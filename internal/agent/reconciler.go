/*
Copyright 2026.

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.gnu.org/licenses/agpl-3.0.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package agent realizes MiroirVolume desired state on one node: it creates,
// resizes, and deletes backing devices via the node's Backend and reports
// observed state back through the CRD status (notes/DESIGN.md §4.2).
package agent

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// VolumeReconciler converges local state for volumes that place a replica on
// NodeName. Level-triggered: safe to restart at any point (notes/DESIGN.md §4.2).
type VolumeReconciler struct {
	client.Client
	NodeName string
	Backend  backend.Backend
	// BackendType gates behavior that depends on the backing implementation
	// (auto rs-discard-granularity is skipped for loopfile).
	BackendType miroirv1alpha1.BackendType
	// DRBD drives the replication layer for multi-replica volumes.
	DRBD *drbd.Driver
	// DRBDEvents delivers kernel state changes (drbdsetup events2) as
	// reconcile triggers, ahead of the poll.
	DRBDEvents <-chan event.GenericEvent
	// Workers bounds concurrent volume reconciles (default 4). Per-key
	// serialization is controller-runtime's guarantee; the storage CLIs
	// are cross-resource-safe and minor allocation holds its own lock.
	Workers int

	// realized caches the last fully realized state per volume so the
	// steady-state poll only re-execs `drbdsetup status` instead of the
	// whole realize/adjust/probe pipeline. See fastPath.
	realizedMu sync.Mutex
	realized   map[string]realizedState
}

// realizedState is the fingerprint of a completed full pass: repeating
// it is pure waste until the spec changes, the kernel reports different
// state, or the deep-check interval elapses (the out-of-band-drift net —
// e.g. a backing device deleted behind the agent's back).
type realizedState struct {
	generation int64
	status     drbd.Status
	replicated bool
	fullPassAt time.Time
}

// deepCheckInterval bounds how long the fast path may skip the full
// realize pipeline; backend drift with no kernel signature (unreplicated
// volumes especially) is caught within one interval.
const deepCheckInterval = 5 * time.Minute

// drbdPollInterval refreshes DRBD state in the CRD: connection/disk state
// changes in the kernel without generating Kubernetes events.
const drbdPollInterval = 30 * time.Second

// errSplitBrain gives the split-brain transition log a real error value.
var errSplitBrain = errors.New("DRBD split-brain detected")

// Reconcile realizes (or tears down) this node's replica of one volume.
func (r *VolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	idx := slices.IndexFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == r.NodeName
	})
	mine := idx >= 0
	localDiskless := mine && vol.Spec.Replicas[idx].Diskless

	if !vol.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, vol, mine)
	}
	if !mine {
		// Not placed here, but a held finalizer means this replica was
		// removed from spec.replicas: tear down the local leg once it is
		// safe (notes/DESIGN.md §4.2).
		return r.reconcileRemoval(ctx, vol)
	}
	if vol.Spec.DRBD != nil && vol.Spec.Replicas[idx].Address == "" {
		// A just-added entry the membership reconciler has not completed
		// yet (no NodeID/address): nothing can be realized safely. Logged
		// so a volume stuck waiting is visible — this wait is normally a
		// single pass, never a steady state.
		log.Info("replica entry incomplete; waiting for membership completion", "volume", vol.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Steady state: when nothing changed since the last full pass, one
	// status exec (or none for unreplicated volumes) replaces the whole
	// realize/adjust/probe pipeline. Peer status writes re-trigger this
	// reconcile every poll cycle; without the short-circuit each of those
	// re-runs ~6 execs per volume.
	if done, res := r.fastPath(ctx, vol, localDiskless); done {
		return res, nil
	}
	// Any pass that reaches the pipeline invalidates the fingerprint; it
	// is re-stored only after the pass completes cleanly.
	r.dropRealized(vol.Name)

	var dev string
	var forceFullSync bool
	var err error
	if !localDiskless {
		// Realize: create (or grow) the backing device — a CoW clone when the
		// volume restores from a snapshot.
		dev, forceFullSync, err = r.realizeBacking(ctx, vol, vol.Spec.Replicas[idx].FullSync)
		if err != nil {
			return ctrl.Result{}, r.reportError(ctx, vol, err)
		}
		// Record the device before growing it: computePhase treats errors
		// with DeviceCreated=false as hard provisioning failures, and a
		// transient resize error must not be one.
		if !vol.Status.PerNode[r.NodeName].DeviceCreated {
			if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
				DeviceCreated: true,
				DevicePath:    dev,
			}); err != nil {
				return ctrl.Result{}, err
			}
			vol.Status.PerNode[r.NodeName] = miroirv1alpha1.ReplicaStatus{
				DeviceCreated: true, DevicePath: dev,
			}
		}
		if err := r.Backend.Resize(ctx, vol.Name, vol.Spec.SizeBytes); err != nil {
			return ctrl.Result{}, r.reportError(ctx, vol, err)
		}
		if vol.Spec.DRBD == nil {
			log.V(1).Info("replica realized", "volume", vol.Name, "device", dev)
			// resyncRatio 1 and quorum true, not the zero values: an
			// unreplicated volume is fully in sync with itself and has no
			// quorum to lose — zeros would perma-fire the alerts.
			recordVolumeMetrics(vol.Name, miroirReplicaView{
				upToDate: true, connected: true, quorum: true, resyncRatio: 1,
			})
			if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
				DeviceCreated: true,
				DevicePath:    dev,
				SizeBytes:     vol.Spec.SizeBytes,
				Connected:     true,
			}); err != nil {
				return ctrl.Result{}, err
			}
			r.storeRealized(vol.Generation, vol.Name, drbd.Status{}, false)
			return ctrl.Result{}, nil
		}
	} else {
		// Diskless tie-breaker: join DRBD for quorum only, no backing.
		// No replica metrics either — the up-to-date/connected series
		// describe data legs, and a tie-breaker is never UpToDate, so a
		// series here would permanently trip up_to_date==0 alerts.
		log.V(1).Info("diskless tie-breaker realized", "volume", vol.Name)
	}

	// Replicated: layer DRBD on the backing device. Pods attach the DRBD
	// device, never the backing LV/zvol directly.
	minor, err := r.assignMinor(ctx, vol)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	// Best-effort: a resync sends zero-runs as discards of this size so a
	// FullSync-joining thin leg stays thin. Never loopfile (loop devices
	// mishandle it) and never worth failing the reconcile over.
	var discardGranularity int64
	if !localDiskless && r.BackendType != miroirv1alpha1.BackendLoopfile {
		if discardGranularity, err = r.DRBD.DiscardGranularity(ctx, dev); err != nil {
			log.V(1).Info("discard granularity probe failed; rendering without it",
				"volume", vol.Name, "error", err)
		}
	}
	if err := r.DRBD.Apply(ctx, drbdResource(vol, r.NodeName, dev, minor, localDiskless, forceFullSync, discardGranularity)); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	// Online growth: once every peer's backing device is at the new size
	// (the local leg was just resized above), the first diskful replica
	// grows the DRBD device over them. It withholds the new size from its
	// status until then — the CSI expansion wait keys on status, and the
	// filesystem must not grow against a still-small DRBD device. A
	// diskless tie-breaker must never be the resize coordinator.
	st, err := r.DRBD.Status(ctx, vol.Name)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	reportSize, requeue, err := r.growIfCoordinator(ctx, vol, st)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	if st.SplitBrain && !vol.Status.PerNode[r.NodeName].SplitBrain {
		log.Error(errSplitBrain,
			"manual resolution required (drbdadm connect --discard-my-data on the losing node)",
			"volume", vol.Name)
	}
	connected := diskfulPeersConnected(st, vol, r.NodeName)
	diskFailed := diskFailedLatch(vol, r.NodeName, st, localDiskless)
	if !localDiskless {
		recordVolumeMetrics(vol.Name, miroirReplicaView{
			upToDate:   st.DiskState == drbd.DiskUpToDate,
			connected:  connected,
			splitBrain: st.SplitBrain,
			suspended:  st.Suspended,
			quorum:     st.Quorum,
			diskFailed: diskFailed,
			// drbdsetup reports percent-in-sync and KiB; the exported
			// gauges are a 0-1 ratio and bytes per Prometheus base units.
			resyncRatio:    st.ResyncPercent / 100,
			outOfSyncBytes: float64(st.OutOfSyncKiB) * 1024,
		})
	}
	if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated: !localDiskless,
		DevicePath:    drbd.DevicePath(minor),
		DRBDMinor:     minor,
		SizeBytes:     reportSize,
		DiskState:     st.DiskState,
		Connected:     connected,
		SplitBrain:    st.SplitBrain,
		Diskless:      localDiskless,
		DiskFailed:    diskFailed,
		Message:       detachedDiskMessage(diskFailed),
	}); err != nil {
		return ctrl.Result{}, err
	}
	// Cache only the settled state: a mid-grow pass (short requeue, size
	// withheld) must keep taking the full pipeline until the grow lands.
	if requeue == drbdPollInterval && reportSize == vol.Spec.SizeBytes {
		r.storeRealized(vol.Generation, vol.Name, st, true)
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// fastPath reports whether this reconcile can settle without the realize
// pipeline: same generation, settled size, a fresh-enough full pass, and —
// for replicated volumes — kernel state identical to the fingerprint. On
// the hit it refreshes the metrics (cheap sets) and skips the status patch
// entirely: the CRD already reflects this state, and phase converges via
// whichever peer actually changed.
func (r *VolumeReconciler) fastPath(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, localDiskless bool) (bool, ctrl.Result) {
	r.realizedMu.Lock()
	entry, ok := r.realized[vol.Name]
	r.realizedMu.Unlock()
	if !ok || entry.generation != vol.Generation ||
		time.Since(entry.fullPassAt) >= deepCheckInterval ||
		(!localDiskless && vol.Status.PerNode[r.NodeName].SizeBytes != vol.Spec.SizeBytes) {
		return false, ctrl.Result{}
	}
	if !entry.replicated {
		recordVolumeMetrics(vol.Name, miroirReplicaView{
			upToDate: true, connected: true, quorum: true, resyncRatio: 1,
		})
		return true, ctrl.Result{}
	}
	st, err := r.DRBD.Status(ctx, vol.Name)
	if err != nil || !statusEqual(st, entry.status) {
		return false, ctrl.Result{}
	}
	if !localDiskless {
		recordVolumeMetrics(vol.Name, miroirReplicaView{
			upToDate:       st.DiskState == drbd.DiskUpToDate,
			connected:      diskfulPeersConnected(st, vol, r.NodeName),
			splitBrain:     st.SplitBrain,
			suspended:      st.Suspended,
			quorum:         st.Quorum,
			diskFailed:     diskFailedLatch(vol, r.NodeName, st, localDiskless),
			resyncRatio:    st.ResyncPercent / 100,
			outOfSyncBytes: float64(st.OutOfSyncKiB) * 1024,
		})
	}
	return true, ctrl.Result{RequeueAfter: drbdPollInterval}
}

func statusEqual(a, b drbd.Status) bool {
	return a.DiskState == b.DiskState &&
		a.Primary == b.Primary &&
		a.PeerPrimary == b.PeerPrimary &&
		a.Suspended == b.Suspended &&
		a.SplitBrain == b.SplitBrain &&
		a.Resyncing == b.Resyncing &&
		a.ResyncPercent == b.ResyncPercent &&
		a.Quorum == b.Quorum &&
		a.OutOfSyncKiB == b.OutOfSyncKiB &&
		maps.Equal(a.PeerConnected, b.PeerConnected)
}

func (r *VolumeReconciler) storeRealized(gen int64, name string, st drbd.Status, replicated bool) {
	r.realizedMu.Lock()
	defer r.realizedMu.Unlock()
	if r.realized == nil {
		r.realized = map[string]realizedState{}
	}
	r.realized[name] = realizedState{
		generation: gen, status: st, replicated: replicated, fullPassAt: time.Now(),
	}
}

func (r *VolumeReconciler) dropRealized(name string) {
	r.realizedMu.Lock()
	defer r.realizedMu.Unlock()
	delete(r.realized, name)
}

// reconcileDeletion tears down the local leg of a deleted volume. Only
// the agent owning a replica may release the finalizer, and only after
// its local teardown succeeded — a foreign agent touching it would race
// the owner and leak the backing device. A pending-removal replica
// (finalizer held, not in spec) takes the same path: volume deletion
// supersedes the removal gates.
func (r *VolumeReconciler) reconcileDeletion(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, mine bool) (ctrl.Result, error) {
	if !mine && !controllerutil.ContainsFinalizer(vol, constants.FinalizerPrefix+r.NodeName) {
		return ctrl.Result{}, nil
	}
	if err := r.teardown(ctx, vol); err != nil {
		if errors.Is(err, backend.ErrBusy) {
			// A force-deleted pod can leave the device open past
			// NodeUnstage; retry until the mount goes away.
			ctrl.LoggerFrom(ctx).Info("device busy during teardown, retrying", "volume", vol.Name)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	// Drop metrics only once the device is gone: a retrying teardown
	// must not blank a volume that still exists.
	dropVolumeMetrics(vol.Name)
	r.dropRealized(vol.Name)
	return ctrl.Result{}, r.removeFinalizer(ctx, vol)
}

// diskfulPeersConnected reports whether this node's replication links to
// every diskful peer are established. A diskless tie-breaker's link is
// excluded: it holds no data leg, so its downtime must not read as
// degraded replication — the snapshot barrier and removal gating key on
// this, and coupling them to the tie-breaker would block both in exactly
// the degraded mode the tie-breaker exists to survive. Entries the
// membership reconciler has not completed (no address) have no
// connection yet and are skipped, matching the rendered config.
func diskfulPeersConnected(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if !st.PeerConnected[rep.NodeID] {
			return false
		}
	}
	return true
}

// peerBackingsGrown reports whether every peer realized the desired size.
// The local leg is excluded: its status entry is stale (just patched).
func peerBackingsGrown(vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless {
			continue
		}
		if vol.Status.PerNode[rep.Node].SizeBytes < vol.Spec.SizeBytes {
			return false
		}
	}
	return true
}

// wipedBacking reports the node-wipe signature: this node's status slot
// says the backing was realized, but the device is gone and so is the
// local seed marker (a reinstall takes both). Re-seeding the day0 GI
// would pose the empty recreated device as the peers' identical twin and
// the partial resync would miss every write since creation — the caller
// forces the just-created-metadata path so the first handshake
// full-syncs this leg instead. Cheap steady-state: the marker check
// short-circuits once Apply has seeded.
func (r *VolumeReconciler) wipedBacking(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (bool, error) {
	if vol.Spec.DRBD == nil || r.DRBD.Seeded(vol.Name) ||
		!vol.Status.PerNode[r.NodeName].DeviceCreated {
		return false, nil
	}
	exists, err := r.Backend.Exists(ctx, vol.Name)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

// realizeBacking creates the backing device: fresh, or cloned from a
// snapshot for restores. Clones are byte-identical on every replica, so
// the day0 GI seed keeps restored volumes from resyncing. forceFullSync
// reports the node-wipe signature (wipedBacking): the recreated device
// must join as a full SyncTarget, never re-seed the day0 GI.
func (r *VolumeReconciler) realizeBacking(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, fullSync bool) (dev string, forceFullSync bool, err error) {
	if forceFullSync, err = r.wipedBacking(ctx, vol); err != nil {
		return "", false, err
	}
	if vol.Spec.Source == nil || fullSync {
		// A FullSync joiner never clones, even on a restored volume: its
		// node holds no source snapshot, and its content arrives over the
		// wire as a full SyncTarget regardless of what the backing holds.
		dev, err = r.Backend.Create(ctx, vol.Name, vol.Spec.SizeBytes)
		return dev, forceFullSync, err
	}
	// The backing is cloned once and survives reboots; the source snapshot
	// may be gone by then, so recover an existing device without it.
	if exists, err := r.Backend.Exists(ctx, vol.Name); err != nil {
		return "", false, err
	} else if exists {
		dev, err = r.Backend.Create(ctx, vol.Name, vol.Spec.SizeBytes)
		return dev, forceFullSync, err
	}
	snap := &miroirv1alpha1.MiroirSnapshot{}
	if err := r.Get(ctx, types.NamespacedName{Name: vol.Spec.Source.SnapshotName}, snap); err != nil {
		return "", false, err
	}
	dev, err = r.Backend.CreateFromSnapshot(ctx, vol.Name, snap.Spec.VolumeName, snap.Name)
	return dev, forceFullSync, err
}

// drbdResource maps the CRD desired state to a render input. Entries the
// membership reconciler has not completed yet (no address) are left out:
// rendering them would produce a config DRBD cannot parse, and the peer
// cannot connect before completion anyway.
func drbdResource(vol *miroirv1alpha1.MiroirVolume, localNode, localDisk string, minor int32, localDiskless, forceFullSync bool, discardGranularity int64) drbd.Resource {
	peers := make([]drbd.Peer, 0, len(vol.Spec.Replicas))
	skipSeed := forceFullSync
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			continue
		}
		if rep.Node == localNode && rep.FullSync {
			skipSeed = true
		}
		peers = append(peers, drbd.Peer{
			Node:     rep.Node,
			NodeID:   rep.NodeID,
			Address:  rep.Address,
			Diskless: rep.Diskless,
		})
	}
	return drbd.Resource{
		Name:          vol.Name,
		Minor:         minor,
		Port:          vol.Spec.DRBD.Port,
		Quorum:        vol.Spec.QuorumPolicy,
		LocalNode:     localNode,
		LocalDisk:     localDisk,
		LocalDiskless: localDiskless,
		Secret:        vol.Spec.DRBD.SharedSecret,
		SkipSeed:      skipSeed,
		// Latched failed: render adjust --skip-disk so the failing disk is
		// not re-attached. Read from prior status, so it lags the detach by
		// one reconcile. Cleared by a replica re-add (removal drops the slot).
		SkipDiskAttach:          !localDiskless && vol.Status.PerNode[localNode].DiskFailed,
		DiscardGranularityBytes: discardGranularity,
		Peers:                   peers,
	}
}

// reconcileRemoval tears down a replica that was removed from
// spec.replicas while the volume lives on. It only proceeds when losing
// this leg cannot lose data: every remaining replica must be UpToDate and
// connected, and no snapshot may reference the volume — snapshots exist as
// backend CoW state on every replica, and restores expect to find them
// wherever the volume is placed.
func (r *VolumeReconciler) reconcileRemoval(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	finalizer := constants.FinalizerPrefix + r.NodeName
	if !controllerutil.ContainsFinalizer(vol, finalizer) {
		return ctrl.Result{}, nil
	}
	if reason := r.removalBlocked(ctx, vol); reason != "" {
		log.Info("replica removal blocked", "volume", vol.Name, "reason", reason)
		st := vol.Status.PerNode[r.NodeName]
		st.Message = "replica removal blocked: " + reason
		if err := r.patchStatus(ctx, vol, st); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.teardown(ctx, vol); err != nil {
		if errors.Is(err, backend.ErrBusy) {
			// A pod still staged here holds the device open; it has to
			// move off this node before the leg can go.
			log.Info("device busy during replica removal, retrying", "volume", vol.Name)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	dropVolumeMetrics(vol.Name)
	r.dropRealized(vol.Name)
	// Drop this node's status slot — merge-patch null deletes the key.
	// Best-effort ordering: a crash here leaves a stale slot, which
	// nothing reads (phase and growth iterate spec.replicas only).
	patch := fmt.Appendf(nil, `{"status":{"perNode":{%q:null}}}`, r.NodeName)
	if err := r.Status().Patch(ctx, vol, client.RawPatch(types.MergePatchType, patch)); err != nil &&
		!apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	log.Info("replica removed", "volume", vol.Name)
	return ctrl.Result{}, r.removeFinalizer(ctx, vol)
}

// removalBlocked reports why this replica must not be torn down yet, or ""
// when it is safe. The remaining replicas' health is read from the CRD
// status the peers report — by removal time the peers have already dropped
// this node from their configs, so the local kernel's view of them is gone.
func (r *VolumeReconciler) removalBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) string {
	if vol.Spec.DRBD == nil {
		// An unreplicated volume's lone entry moved: there is no peer
		// holding the data, so tearing down here is data loss. Unsupported.
		return "volume has no replication layer; refusing to drop the only copy"
	}
	// A diskless tie-breaker holds no backend CoW state, so snapshots do
	// not pin it — only the remaining-replica health gate below applies
	// (pulling the quorum vote while a data leg is down would drop the
	// volume to 1-of-2). The self-reported status marker survives the
	// entry's removal from spec; never key this on the kernel DiskState,
	// which a detached diskful replica also reads.
	if !vol.Status.PerNode[r.NodeName].Diskless {
		snaps := &miroirv1alpha1.MiroirSnapshotList{}
		if err := r.List(ctx, snaps); err != nil {
			return "cannot list snapshots: " + err.Error()
		}
		for _, s := range snaps.Items {
			if s.Spec.VolumeName == vol.Name {
				return "snapshot " + s.Name + " exists; delete the volume's snapshots first"
			}
		}
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			continue
		}
		st, ok := vol.Status.PerNode[rep.Node]
		if !ok || st.DiskState != drbd.DiskUpToDate || !st.Connected {
			return "replica on " + rep.Node + " is not UpToDate and connected"
		}
	}
	return ""
}

func (r *VolumeReconciler) teardown(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Spec.DRBD != nil {
		if err := r.DRBD.Down(ctx, vol.Name); err != nil {
			// A still-staged device answers "held open"; classify it as
			// ErrBusy so teardown takes the 10s retry, not the workqueue's
			// minutes-long backoff (NodeUnstage releases it shortly).
			return backend.Busy(err)
		}
	}
	// Backend.Delete succeeds when the device is already absent, so a
	// diskless tie-breaker (which never created one) needs no special
	// case. Never key this on the kernel DiskState: a diskful replica
	// reads "Diskless" after an I/O-error detach, and skipping its
	// delete would leak the backing device.
	return r.Backend.Delete(ctx, vol.Name)
}

// removeFinalizer releases this node's own finalizer once local teardown
// is done; the volume disappears when the last replica's agent finishes.
func (r *VolumeReconciler) removeFinalizer(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	return removeNodeFinalizer(ctx, r.Client, vol, r.NodeName)
}

// removeNodeFinalizer drops this node's teardown finalizer, swallowing
// conflicts (the watch retriggers) and not-found (the object is already
// gone — the goal state). Shared by the volume and snapshot reconcilers so
// the subtle swallow semantics cannot drift apart.
func removeNodeFinalizer(ctx context.Context, c client.Client, obj client.Object, node string) error {
	finalizer := constants.FinalizerPrefix + node
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(obj, finalizer)
	if err := c.Update(ctx, obj); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *VolumeReconciler) reportError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) error {
	// Preserve the realized state: a transient error (e.g. DRBD peer not
	// up yet) must not erase DeviceCreated, or the volume would read as
	// hard-Failed while the device exists.
	st := vol.Status.PerNode[r.NodeName]
	st.Message = cause.Error()
	if err := r.patchStatus(ctx, vol, st); err != nil {
		return err
	}
	return cause // requeue with backoff
}

// patchStatus applies only this node's slot and the derived phase. A
// full-status apply would force-own peers' slots and Formatted (a CSI
// field) and revert them to this agent's stale read.
func (r *VolumeReconciler) patchStatus(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, mine miroirv1alpha1.ReplicaStatus) error {
	if vol.Status.PerNode == nil {
		vol.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	vol.Status.PerNode[r.NodeName] = mine
	vol.Status.Phase = computePhase(vol)

	ac := acv1alpha1.MiroirVolume(vol.Name).
		WithStatus(acv1alpha1.MiroirVolumeStatus().
			WithPhase(vol.Status.Phase).
			WithPerNode(map[string]acv1alpha1.ReplicaStatusApplyConfiguration{
				r.NodeName: *replicaStatusAC(mine),
			}))
	return r.SubResource("status").Apply(ctx, ac,
		client.FieldOwner("agent-volume-"+r.NodeName),
		client.ForceOwnership)
}

// replicaStatusAC mirrors ReplicaStatus's wire shape: fields without
// omitempty are always set (SSA must own them even at zero — Connected
// false is a statement, not an absence), omitempty fields only when
// non-zero (absent → SSA clears the previous value this manager owned).
func replicaStatusAC(st miroirv1alpha1.ReplicaStatus) *acv1alpha1.ReplicaStatusApplyConfiguration {
	ac := acv1alpha1.ReplicaStatus().
		WithDeviceCreated(st.DeviceCreated).
		WithConnected(st.Connected).
		WithSplitBrain(st.SplitBrain)
	if st.DevicePath != "" {
		ac = ac.WithDevicePath(st.DevicePath)
	}
	if st.SizeBytes != 0 {
		ac = ac.WithSizeBytes(st.SizeBytes)
	}
	if st.DRBDMinor != 0 {
		ac = ac.WithDRBDMinor(st.DRBDMinor)
	}
	if st.DiskState != "" {
		ac = ac.WithDiskState(st.DiskState)
	}
	if st.Diskless {
		ac = ac.WithDiskless(true)
	}
	if st.DiskFailed {
		ac = ac.WithDiskFailed(true)
	}
	if st.Message != "" {
		ac = ac.WithMessage(st.Message)
	}
	return ac
}

// assignMinor returns the DRBD minor for this volume, allocating a free one if unset.
func (r *VolumeReconciler) assignMinor(_ context.Context, vol *miroirv1alpha1.MiroirVolume) (int32, error) {
	if m := vol.Status.PerNode[r.NodeName].DRBDMinor; m > 0 {
		return m, nil
	}
	minor, err := r.DRBD.AllocateMinor(vol.Name)
	if err != nil {
		return 0, err
	}
	return minor, nil
}

// computePhase aggregates per-node states into the volume phase the CSI
// controller waits on (notes/DESIGN.md §4.5.1).
// growIfCoordinator runs drbdadm resize when this node is the resize
// coordinator (first diskful replica) and its realized size still trails
// the spec — first bring-up and expansion. Once its status reaches spec
// the device is grown, so the steady state does nothing (no resize exec
// every poll). Returns the size to report and the requeue interval;
// resize is withheld (short requeue) while a resync is in flight.
func (r *VolumeReconciler) growIfCoordinator(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status) (int64, time.Duration, error) {
	reportSize := vol.Spec.SizeBytes
	coord := vol.Spec.FirstDiskfulReplica()
	behind := vol.Status.PerNode[r.NodeName].SizeBytes < vol.Spec.SizeBytes
	if !behind || coord == nil || coord.Node != r.NodeName {
		return reportSize, drbdPollInterval, nil
	}
	// A latched-failed coordinator has no local disk (Diskless): drbdadm
	// resize would error, and removing the replica re-elects the coordinator
	// to a healthy peer anyway. Withhold and poll rather than error-loop.
	if st.DiskState == drbd.DiskDiskless {
		return vol.Status.PerNode[r.NodeName].SizeBytes, 5 * time.Second, nil
	}
	// drbdadm resize is refused mid-resync, so withhold the size and retry
	// on the next poll instead of failing the reconcile.
	if !peerBackingsGrown(vol, r.NodeName) || st.Resyncing {
		return vol.Status.PerNode[r.NodeName].SizeBytes, 5 * time.Second, nil
	}
	// assumeClean=true: all miroir backends are thin/sparse, so the grown
	// region is zeroed on every replica and a resync of the new extents is
	// unnecessary.
	if err := r.DRBD.Resize(ctx, vol.Name, true); err != nil {
		if !drbd.IsResizeDuringResync(err) {
			return 0, 0, err
		}
		// A resync started between the status read and the resize: withhold.
		return vol.Status.PerNode[r.NodeName].SizeBytes, 5 * time.Second, nil
	}
	return reportSize, drbdPollInterval, nil
}

// diskFailedLatch reports whether this diskful leg is latched failed: DRBD
// detached it to Diskless after a backing-device I/O error (on-io-error
// detach) and it must not be re-attached (drbdResource renders
// adjust --skip-disk while latched). It is sticky — once a prior reconcile
// recorded DiskFailed it stays latched even though prev DiskState is now
// Diskless (the fresh-detach test alone would clear it). It clears only
// when the leg reaches a non-Diskless state again (a re-add re-attaches a
// fresh disk) or the replica is removed (removal drops this status slot).
// Gated on the leg having previously been attached so a normal bring-up
// (briefly Diskless) does not cry wolf.
func diskFailedLatch(vol *miroirv1alpha1.MiroirVolume, self string, st drbd.Status, localDiskless bool) bool {
	if localDiskless || st.DiskState != drbd.DiskDiskless {
		return false
	}
	prev := vol.Status.PerNode[self]
	return prev.DiskFailed || (prev.DiskState != "" && prev.DiskState != drbd.DiskDiskless)
}

// detachedDiskMessage explains a latched-failed leg to the operator, who
// otherwise sees only "DiskState: Diskless" with no cause. Persists as long
// as the leg is latched, not just the reconcile the detach was first seen.
func detachedDiskMessage(diskFailed bool) string {
	if !diskFailed {
		return ""
	}
	return "backing device detached after an I/O error; serving via the peer — " +
		"replace the disk, then remove and re-add this replica"
}

func computePhase(vol *miroirv1alpha1.MiroirVolume) miroirv1alpha1.VolumePhase {
	diskfulReplicas := vol.Spec.DiskfulReplicas()
	ready := 0
	for _, rep := range diskfulReplicas {
		st, ok := vol.Status.PerNode[rep.Node]
		replicated := vol.Spec.DRBD != nil
		switch {
		case ok && st.DeviceCreated && st.SizeBytes >= vol.Spec.SizeBytes &&
			(!replicated || st.DiskState == drbd.DiskUpToDate):
			ready++
		case ok && st.Message != "" && !st.DeviceCreated:
			// Hard failure: the backing device never materialised.
			// Errors after that point (DRBD connect retries, status
			// hiccups) are transient and must not fail provisioning.
			return miroirv1alpha1.VolumeFailed
		}
	}
	switch {
	case ready == len(diskfulReplicas):
		return miroirv1alpha1.VolumeReady
	case ready > 0:
		return miroirv1alpha1.VolumeDegraded
	default:
		return miroirv1alpha1.VolumeCreating
	}
}

// SetupWithManager registers the reconciler.
func (r *VolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// The snapshot controller stays single-worker (its round protocol
	// assumes head-of-line ordering); volumes have no cross-key coupling.
	b := ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{}).
		Named("agent-volume").
		WithOptions(controller.Options{MaxConcurrentReconciles: cmp.Or(r.Workers, 4)})
	if r.DRBDEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.DRBDEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}
