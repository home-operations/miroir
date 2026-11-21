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
// observed state back through the CRD status.
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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/drbd"
)

// VolumeReconciler converges local state for volumes that place a replica on
// NodeName. Level-triggered: safe to restart at any point.
type VolumeReconciler struct {
	client.Client
	NodeName string
	// Pools holds this node's storage pools; each volume's leg resolves
	// its backend through its replica's pool reference.
	Pools Pools
	// DRBD drives the replication layer for multi-replica volumes.
	DRBD *drbd.Driver
	// DRBDEvents delivers kernel state changes (drbdsetup events2) as
	// reconcile triggers, ahead of the poll.
	DRBDEvents <-chan event.GenericEvent
	// Workers bounds concurrent volume reconciles (default 4). Per-key
	// serialization is controller-runtime's guarantee; the storage CLIs
	// are cross-resource-safe and minor allocation holds its own lock.
	Workers int
	// KmsgPath overrides /dev/kmsg for the split-brain kernel-log capture
	// (tests point it at a plain file).
	KmsgPath string
	// ProcDir overrides /proc for the orphaned-hold checks — opener pid
	// liveness and the all-namespaces mount scan (tests point it at a
	// fixture directory).
	ProcDir string
	// Recorder emits the TeardownWedged warning; optional.
	Recorder events.EventRecorder
	// Startup receives each replicated leg's full-pass outcome for the
	// readiness probe; optional.
	Startup *StartupGate

	// realized caches the last fully realized state per volume so the
	// steady-state poll only re-execs `drbdsetup status` instead of the
	// whole realize/adjust/probe pipeline. See fastPath.
	realizedMu sync.Mutex
	realized   map[string]realizedState

	// lastRecovery stamps the last split-brain recovery attempt per volume.
	// Recovery itself flaps connections, and every flap is a DRBD event that
	// requeues the volume — without a floor the agent re-enters recovery
	// several times per second. See recoverSplitBrain.
	recoveryMu   sync.Mutex
	lastRecovery map[string]time.Time
	// stuckSince (same lock) stamps when a stranded-bitmap signature was
	// first seen per volume, keyed to the candidate peer set it was seen
	// on; recovery waits out one poll interval so a status snapshot mid
	// real handshake is never acted on, and a changed set restarts the
	// wait so a newly seen peer never rides an older peer's clock. See
	// recoverStuckResync.
	stuckSince map[string]stuckMark

	// busyFails counts consecutive ErrBusy teardown outcomes per volume.
	// Past busyFailLimit the loop escalates — Warning Event, status
	// Message, parked cadence — instead of silently retrying every 10s
	// forever with the cause swallowed. See reportBusy.
	busyMu    sync.Mutex
	busyFails map[string]int
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

// StaleProbeThreshold bounds how long a replica's LastProbedAt can age
// before computePhase reads it as stale — the agent can no longer reach
// the node-local resources, so the replica is Degraded even if its
// persisted fields still read healthy. Must be longer than deepCheckInterval
// (the full pipeline, which stamps LastProbedAt, runs at most every 5 min):
// the fast path does not stamp LastProbedAt, so a healthy replica parked
// in the fast path can appear stale after one missed deep-check. 2× gives
// one missed-cycle tolerance — the first deep-check expires the fingerprint
// naturally (deepCheckInterval), and only if the second also fails does the
// phase flip Degraded.
const StaleProbeThreshold = 2 * deepCheckInterval

// errRestoreSourceGone marks a restore whose source snapshot was deleted
// before this leg cloned it: the backing can never be created here, so the
// retry parks instead of riding the workqueue backoff forever.
var errRestoreSourceGone = errors.New("restore source snapshot no longer exists")

// legState classifies this node's part in the volume: the replica index
// (-1 when not a replica), whether the local leg is diskless — a
// tie-breaker replica, or a client leg from spec.clients, which realizes
// identically (no backend, DRBD "disk none"; never also a replica, per CRD
// validation) — whether any leg is placed here at all, and whether that
// leg still awaits membership completion (no address).
func legState(vol *miroirv1alpha1.MiroirVolume, node string) (idx int, localDiskless, present, incomplete bool) {
	idx = slices.IndexFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == node
	})
	mine := idx >= 0
	clientLeg := vol.Spec.ClientForNode(node)
	localDiskless = (mine && vol.Spec.Replicas[idx].Diskless) || clientLeg != nil
	present = mine || clientLeg != nil
	incomplete = (mine && vol.Spec.Replicas[idx].Address == "") ||
		(clientLeg != nil && clientLeg.Address == "")
	return idx, localDiskless, present, incomplete
}

// Reconcile realizes (or tears down) this node's replica of one volume.
func (r *VolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, r.volumeGone(req.Name, err)
	}

	idx, localDiskless, present, incomplete := legState(vol, r.NodeName)

	if !vol.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, vol, present)
	}
	if !present {
		// Not placed here, but a held finalizer means this replica (or
		// client leg) was removed from the spec: tear down the local leg
		// once it is safe.
		return r.reconcileRemoval(ctx, vol)
	}
	// A live, placed volume is not tearing down: a busy streak left over
	// from a cancelled removal must not pre-bias the next teardown episode.
	r.clearBusyFails(vol.Name)
	if vol.Spec.DRBD != nil && incomplete {
		// A just-added entry the membership reconciler has not completed
		// yet (no NodeID/address): nothing can be realized safely. Logged
		// so a volume stuck waiting is visible — this wait is normally a
		// single pass, never a steady state.
		log.Info("replica entry incomplete; waiting for membership completion", "volume", vol.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	r.Startup.Baseline(vol.Name, publishedHealthy(vol.Status.PerNode[r.NodeName]))

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
	var err error
	var pool PoolBackend
	poolName := volumePoolOn(vol, r.NodeName)
	if !localDiskless {
		// Resolve the leg's pool first: a replica referencing a pool this
		// node no longer carries is a hard failure, not a wrong-pool guess.
		if pool, err = r.Pools.Get(poolName); err != nil {
			return ctrl.Result{}, r.reportError(ctx, vol, err)
		}
		// Realize: create (or grow) the backing device — a CoW clone when the
		// volume restores from a snapshot.
		if err := r.clearStaleBacking(ctx, vol, pool, poolName); err != nil {
			return ctrl.Result{}, r.reportError(ctx, vol, err)
		}
		dev, err = r.realizeBacking(ctx, pool.Backend, vol, vol.Spec.Replicas[idx].FullSync)
		if err != nil {
			return r.reportRealizeError(ctx, vol, err)
		}
		// Record the device before growing it: computePhase treats errors
		// with DeviceCreated=false as hard provisioning failures, and a
		// transient resize error must not be one.
		if err := r.stampDeviceCreated(ctx, vol, dev, poolName); err != nil {
			return ctrl.Result{}, err
		}
		if vol.Spec.DRBD == nil {
			// A restore clone is born at the snapshot's size; grow the
			// backing to the requested size. No DRBD here, so no internal
			// metadata to relocate; a replicated volume grows only after
			// attach (below), or the clone's stranded metadata wedges it.
			if err := pool.Backend.Resize(ctx, vol.Name, vol.Spec.SizeBytes); err != nil {
				return ctrl.Result{}, r.reportError(ctx, vol, err)
			}
			log.V(1).Info("replica realized", "volume", vol.Name, "device", dev)
			// resyncRatio 1 and quorum true, not the zero values: an
			// unreplicated volume is fully in sync with itself and has no
			// quorum to lose — zeros would perma-fire the alerts.
			recordVolumeMetrics(vol, poolName, miroirReplicaView{
				upToDate: true, connected: true, quorum: true, resyncRatio: 1,
			})
			if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
				DeviceCreated: true,
				DevicePath:    dev,
				SizeBytes:     vol.Spec.SizeBytes,
				Connected:     true,
				Pool:          poolName,
				LastProbedAt:  probeNow(),
			}); err != nil {
				return ctrl.Result{}, err
			}
			r.storeRealized(vol.Generation, vol.Name, drbd.Status{}, false)
			// Unreplicated volumes emit no DRBD events and no status
			// wakeups; without a requeue, out-of-band backend drift (the
			// #88 wipe signature) would only surface at the next watch
			// event. This is the drift net deepCheckInterval promises.
			return ctrl.Result{RequeueAfter: deepCheckInterval}, nil
		}
	} else {
		// Diskless leg (tie-breaker or client): join DRBD without a
		// backing device. No replica metrics either — the
		// up-to-date/connected series describe data legs, and a diskless
		// leg is never UpToDate, so a series here would permanently trip
		// up_to_date==0 alerts.
		log.V(1).Info("diskless leg realized", "volume", vol.Name)
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
	if !localDiskless && pool.Type != miroirv1alpha1.BackendLoopfile {
		if discardGranularity, err = r.DRBD.DiscardGranularity(ctx, dev); err != nil {
			log.V(1).Info("discard granularity probe failed; rendering without it",
				"volume", vol.Name, "error", err)
		}
	}
	resource := drbdResource(vol, r.NodeName, dev, minor, localDiskless, discardGranularity)
	if err := r.DRBD.Apply(ctx, resource); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	st, err := r.DRBD.Status(ctx, vol.Name)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	// Data generations (birth, or the padded-restore seed), ordered
	// before growIfCoordinator so resize never runs against a
	// both-Inconsistent device; status is re-read so this same pass
	// proceeds against UpToDate legs.
	if st, err = r.mintGenerations(ctx, vol, resource, st, localDiskless); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	// Ordered before handleSplitBrain so the same pass cannot auto-discard
	// a leg the latch is about to protect.
	if err := r.latchActivated(ctx, vol, st); err != nil {
		return ctrl.Result{}, err
	}
	// Grow this leg to the spec size now that DRBD has attached it: resize
	// the backing, then (coordinator only) grow the DRBD device over the
	// peers. growLeg documents why the resize must follow the attach.
	reportSize, requeue, err := r.growLeg(ctx, pool.Backend, vol, st, localDiskless)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	connected := diskfulPeersConnected(st, vol, r.NodeName)
	splitActive := r.handleSplitBrain(ctx, vol, resource, st, connected)
	stuckActive := r.recoverStuckResync(ctx, vol, st, localDiskless)
	diskFailed := diskFailedLatch(vol, r.NodeName, st, localDiskless)
	if !localDiskless {
		recordVolumeMetrics(vol, poolName, replicaView(st, vol, r.NodeName, localDiskless))
	} else {
		recordDisklessMetrics(vol, st.Primary)
	}
	statusPool := poolName
	if localDiskless {
		// A diskless leg holds no backing device in any pool.
		statusPool = ""
	}
	if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated:           !localDiskless,
		DevicePath:              drbd.DevicePath(minor),
		DRBDMinor:               minor,
		SizeBytes:               reportSize,
		DiskState:               st.DiskState,
		Connected:               connected,
		SplitBrain:              st.SplitBrain,
		Diskless:                localDiskless,
		DiskFailed:              diskFailed,
		DiscardGranularityBytes: discardGranularity,
		Pool:                    statusPool,
		Message:                 detachedDiskMessage(diskFailed),
		PrimarySince:            primarySince(vol, r.NodeName, st.Primary),
		LastProbedAt:            probeNow(),
	}); err != nil {
		return ctrl.Result{}, err
	}
	r.storeIfSettled(vol, st, requeue, reportSize, splitActive || stuckActive)
	r.Startup.Observe(vol.Name, legSettled(st, vol, r.NodeName, localDiskless))
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// storeIfSettled caches the pass fingerprint only for a settled state: a
// mid-grow pass (short requeue, size withheld) must keep taking the full
// pipeline until the grow lands, and a volume under active recovery must
// re-enter it every poll — a split-brain (locally seen or peer-reported) so
// recoverSplitBrain keeps retrying until the connections re-form, and a
// stranded bitmap (whose kernel state is otherwise steady) so
// recoverStuckResync's confirmation clock is re-read.
func (r *VolumeReconciler) storeIfSettled(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, requeue time.Duration, reportSize int64, recovering bool) {
	if requeue == drbdPollInterval && reportSize == vol.Spec.SizeBytes && !recovering {
		r.storeRealized(vol.Generation, vol.Name, st, true)
	}
}

// fastPath reports whether this reconcile can settle without the realize
// pipeline: same generation, settled size, a fresh-enough full pass, and —
// for replicated volumes — kernel state identical to the fingerprint. On
// the hit it refreshes the metrics (cheap sets) and skips the status patch
// entirely: the CRD already reflects this state, and phase converges via
// whichever peer actually changed, unless a sibling's stale apply already
// overwrote that peer's phase; the recompute below catches that.
func (r *VolumeReconciler) fastPath(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, localDiskless bool) (bool, ctrl.Result) {
	r.realizedMu.Lock()
	entry, ok := r.realized[vol.Name]
	r.realizedMu.Unlock()
	if !ok || entry.generation != vol.Generation ||
		time.Since(entry.fullPassAt) >= deepCheckInterval ||
		(!localDiskless && vol.Status.PerNode[r.NodeName].SizeBytes != vol.Spec.SizeBytes) {
		return false, ctrl.Result{}
	}
	// Phase and ReadyReplicas are co-owned: every leg's full pass
	// force-applies them from its own informer cache, and
	// near-simultaneous passes (a resync completing) can land a stale
	// value last, leaving the CR self-contradictory while every leg parks
	// here until the deep check. No kernel event re-breaks
	// the fingerprint after that, so recompute from the cached CR and take
	// the full pipeline to re-apply them as soon as the informer shows the
	// mismatch.
	if phase, readyReplicas := computePhase(vol); phase != vol.Status.Phase ||
		readyReplicas != vol.Status.ReadyReplicas {
		return false, ctrl.Result{}
	}
	if !entry.replicated {
		recordVolumeMetrics(vol, volumePoolOn(vol, r.NodeName), miroirReplicaView{
			upToDate: true, connected: true, quorum: true, resyncRatio: 1,
		})
		// Same drift net as the full pass: nothing else wakes an
		// unreplicated volume.
		return true, ctrl.Result{RequeueAfter: deepCheckInterval}
	}
	st, err := r.DRBD.Status(ctx, vol.Name)
	if err != nil || !statusEqual(st, entry.status) {
		return false, ctrl.Result{}
	}
	// A peer reporting split-brain while this leg is not fully connected
	// needs the full pipeline: the losing leg's own kernel state is a steady
	// Connecting that never breaks statusEqual, so only the peers' status
	// can route it into recoverSplitBrain.
	if !diskfulPeersConnected(st, vol, r.NodeName) && peerReportedSplitBrain(vol, r.NodeName) {
		return false, ctrl.Result{}
	}
	// A Primary leg without the Activated latch takes the full pipeline,
	// which owns the latch — normally the Primary flip itself breaks
	// statusEqual, but an informer lagging the latch patch must not park
	// the unprotected state here.
	if st.Primary && !vol.Status.Activated {
		return false, ctrl.Result{}
	}
	if !localDiskless {
		recordVolumeMetrics(vol, volumePoolOn(vol, r.NodeName), replicaView(st, vol, r.NodeName, localDiskless))
	} else {
		recordDisklessMetrics(vol, st.Primary)
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
		maps.Equal(a.StuckResyncPeers, b.StuckResyncPeers) &&
		maps.Equal(a.StaleBitmapPeers, b.StaleBitmapPeers) &&
		maps.Equal(a.UnsettledResyncPeers, b.UnsettledResyncPeers) &&
		maps.Equal(a.PeerConnected, b.PeerConnected) &&
		// A peer disk-state flip (DUnknown → Inconsistent at birth) can be
		// the only change in a pass — without it the winner's fingerprint
		// stays valid and the birth-generation trigger never re-runs.
		maps.Equal(a.PeerDiskState, b.PeerDiskState)
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

// volumeGone handles a reconcile whose volume no longer exists in the
// API. A CR can vanish without a final successful teardown here — an
// operator strips the finalizers by hand (the wedge recovery, issue
// #195) — and per-volume state left behind would page forever
// (miroir_volume_wedged is critical) and leak.
func (r *VolumeReconciler) volumeGone(name string, err error) error {
	if !apierrors.IsNotFound(err) {
		return err
	}
	dropVolumeMetrics(name)
	r.dropRealized(name)
	r.dropRecovery(name)
	r.clearBusyFails(name)
	return nil
}

// dropRecovery forgets a torn-down volume's split-brain debounce and
// stuck-resync confirmation stamps so a later volume under the same name
// starts with a clean slate.
func (r *VolumeReconciler) dropRecovery(name string) {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	delete(r.lastRecovery, name)
	delete(r.stuckSince, name)
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

// backingBytes is the size a diskful backing must realize for vol: the
// spec size, plus the internal-metadata overhead when the volume is
// padded (a restore that crossed the replication boundary — see
// VolumeSource.PadForMetadata). Every diskful leg must agree: DRBD sizes
// the device to the smallest leg's usable bytes, and an unpadded joiner
// would shrink the device below the restored filesystem.
func backingBytes(vol *miroirv1alpha1.MiroirVolume) int64 {
	if vol.Spec.DRBD != nil && vol.Spec.Source != nil && vol.Spec.Source.PadForMetadata {
		return vol.Spec.SizeBytes + drbd.InternalMetaOverhead(vol.Spec.SizeBytes)
	}
	return vol.Spec.SizeBytes
}

// realizeBacking creates the backing device: fresh, or cloned from a
// snapshot for restores. Clones of a replicated source are byte-identical
// on every replica and carry the source's live DRBD metadata, so restored
// legs connect with identical inherited generations and skip the resync.
// A wiped node's recreated device gets fresh just-created metadata and
// full-syncs at the first handshake — no special-casing needed. A padded
// clone (unreplicated source restored into a replicated volume) instead
// grows by the metadata overhead here, BEFORE the DRBD apply: there is no
// adopted metadata to strand (the #300 concern), and create-md must land
// in the grown tail rather than over the source filesystem's last bytes.
// The grow runs on the recovery path too — a crash between clone and
// resize must not leave an unpadded backing for create-md to corrupt.
func (r *VolumeReconciler) realizeBacking(ctx context.Context, be backend.Backend, vol *miroirv1alpha1.MiroirVolume, fullSync bool) (dev string, err error) {
	if vol.Spec.Source == nil || fullSync {
		// A FullSync joiner never clones, even on a restored volume: its
		// node holds no source snapshot, and its content arrives over the
		// wire as a full SyncTarget regardless of what the backing holds.
		return be.Create(ctx, vol.Name, backingBytes(vol))
	}
	// The backing is cloned once and survives reboots; the source snapshot
	// may be gone by then, so recover an existing device without it.
	if exists, err := be.Exists(ctx, vol.Name); err != nil {
		return "", err
	} else if exists {
		return r.finishClone(ctx, be, vol)
	}
	snap := &miroirv1alpha1.MiroirSnapshot{}
	if err := r.Get(ctx, types.NamespacedName{Name: vol.Spec.Source.SnapshotName}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			// The source snapshot is gone — deleted by a backup
			// tool (kopiur) after the mover job, or by miroir's own
			// clone cleanup. When a peer already realized its clone,
			// fall back to a fresh backing and let DRBD full-sync
			// from it, the same path a wiped node takes when
			// rejoining. Without a realized clone somewhere the data
			// is truly lost and the volume must park: falling back
			// on the padded-restore seed would satisfy every
			// restoreSeedPending gate and mint the empty backing
			// UpToDate, replicating zeros as a "successful" restore.
			if vol.Spec.DRBD != nil && peerCloneRealized(vol, r.NodeName) {
				ctrl.LoggerFrom(ctx).Info("restore source snapshot gone; creating fresh backing for DRBD full-sync",
					"volume", vol.Name, "snapshot", vol.Spec.Source.SnapshotName)
				return be.Create(ctx, vol.Name, backingBytes(vol))
			}
			return "", fmt.Errorf("snapshot %q: %w", vol.Spec.Source.SnapshotName, errRestoreSourceGone)
		}
		return "", err
	}
	if vol.Spec.DRBD == nil {
		if err := r.DRBD.InvalidateForeignMetadataWipe(vol.Name); err != nil {
			return "", err
		}
	}
	if _, err := be.CreateFromSnapshot(ctx, vol.Name, snap.Spec.VolumeName, snap.Name); err != nil {
		return "", err
	}
	return r.finishClone(ctx, be, vol)
}

// peerCloneRealized reports whether another non-FullSync diskful replica
// has realized its backing — proof a data-bearing clone exists for DRBD
// to full-sync from. FullSync joiners don't count: their backings are
// fresh by construction, DeviceCreated without data. The padded-restore
// seed has only FullSync peers, so it can never pass this gate.
func peerCloneRealized(vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.DiskfulReplicas() {
		if rep.Node == self || rep.FullSync {
			continue
		}
		if vol.Status.PerNode[rep.Node].DeviceCreated {
			return true
		}
	}
	return false
}

// finishClone re-activates an existing clone and settles the shape a
// boundary-crossing restore left it in — both run on the recovery path
// too, so a crash right after the clone cannot skip them. A padded
// volume grows to backingBytes ahead of the DRBD apply (Resize is
// grow-only and idempotent); an unreplicated target sheds any DRBD
// metadata a replicated source's clone carried, or blkid reads the raw
// backing as TYPE=drbd and staging refuses it (see WipeForeignMetadata).
// Unpadded replicated volumes keep today's contract: clones grow only
// after attach (growLeg).
func (r *VolumeReconciler) finishClone(ctx context.Context, be backend.Backend, vol *miroirv1alpha1.MiroirVolume) (string, error) {
	dev, err := be.Create(ctx, vol.Name, vol.Spec.SizeBytes)
	if err != nil {
		return "", err
	}
	if vol.Spec.DRBD == nil {
		return dev, r.DRBD.WipeForeignMetadata(ctx, vol.Name, dev)
	}
	if vol.Spec.Source.PadForMetadata {
		if err := be.Resize(ctx, vol.Name, backingBytes(vol)); err != nil {
			return "", err
		}
	}
	return dev, nil
}

// drbdResource maps the CRD desired state to a render input. Entries the
// membership reconciler has not completed yet (no address) are left out:
// rendering them would produce a config DRBD cannot parse, and the peer
// cannot connect before completion anyway.
func drbdResource(vol *miroirv1alpha1.MiroirVolume, localNode, localDisk string, minor int32, localDiskless bool, discardGranularity int64) drbd.Resource {
	peers := make([]drbd.Peer, 0, len(vol.Spec.Replicas)+len(vol.Spec.Clients))
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			continue
		}
		peers = append(peers, drbd.Peer{
			Node:     rep.Node,
			NodeID:   rep.NodeID,
			Address:  rep.Address,
			Diskless: rep.Diskless,
		})
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Address == "" {
			continue
		}
		peers = append(peers, drbd.Peer{
			Node:     cl.Node,
			NodeID:   cl.NodeID,
			Address:  cl.Address,
			Diskless: true,
			Client:   true,
		})
	}
	// A client leg's device advertises the diskful legs' real discard
	// granularity (max: aligned for the coarsest backing works on all)
	// instead of the 512-byte default DRBD assumes for diskless devices —
	// dm-thin silently drops sub-chunk discards, so trims sized to the
	// default under-free thin pools. 0 (peers not yet published) renders
	// nothing and keeps DRBD's default.
	var clientDiscard int64
	if vol.Spec.ClientForNode(localNode) != nil {
		for _, rep := range vol.Spec.Replicas {
			if !rep.Diskless {
				clientDiscard = max(clientDiscard, vol.Status.PerNode[rep.Node].DiscardGranularityBytes)
			}
		}
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
		// Latched failed: render adjust --skip-disk so the failing disk is
		// not re-attached. Read from prior status, so it lags the detach by
		// one reconcile. Cleared by a replica re-add (removal drops the slot).
		SkipDiskAttach:                !localDiskless && vol.Status.PerNode[localNode].DiskFailed,
		DiscardGranularityBytes:       discardGranularity,
		ClientDiscardGranularityBytes: clientDiscard,
		BitmapGranularityBytes:        vol.Spec.DRBD.BitmapGranularityBytes,
		Peers:                         peers,
	}
}

// clearStaleBacking probes the backing device when the status slot still
// says DeviceCreated. If it is gone (node wipe, out-of-band deletion),
// the stale flag is cleared with a message so computePhase does not treat
// the vanished device as realized. Returns nil when healthy.
func (r *VolumeReconciler) clearStaleBacking(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, pool PoolBackend, poolName string) error {
	prev := vol.Status.PerNode[r.NodeName]
	if !prev.DeviceCreated {
		return nil
	}
	exists, err := pool.Backend.Exists(ctx, vol.Name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	ctrl.LoggerFrom(ctx).Info("backing device missing while status says DeviceCreated; clearing the stale flag",
		"volume", vol.Name)
	// No Message: computePhase treats DeviceCreated=false + Message as a
	// hard failure (VolumeFailed), which would abort any in-flight
	// waitReady poll. Without a Message, the replica reads as
	// not-realized → Phase=Creating, accurately reflecting re-creation.
	return r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated: false,
		Pool:          poolName,
		LastProbedAt:  probeNow(),
	})
}

// stampDeviceCreated patches the per-node slot to mark the backing device
// as created, unless it was already recorded. computePhase treats errors
// with DeviceCreated=false as hard provisioning failures, so the stamp
// must run before any transient operation (e.g. resize) that could error.
func (r *VolumeReconciler) stampDeviceCreated(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dev, poolName string) error {
	if vol.Status.PerNode[r.NodeName].DeviceCreated {
		return nil
	}
	// patchStatus writes the slot back into vol.Status.PerNode, so a later
	// error in the same pass (reportError re-applies the slot) carries the
	// stamp instead of clearing the value just persisted.
	return r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true,
		DevicePath:    dev,
		Pool:          poolName,
		LastProbedAt:  probeNow(),
	})
}

// assignMinor returns the DRBD minor reserved for this volume on this
// node, preferring the one its status already publishes.
func (r *VolumeReconciler) assignMinor(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (int32, error) {
	return r.DRBD.AllocateMinor(ctx, vol.Name, vol.Status.PerNode[r.NodeName].DRBDMinor)
}

// growLeg brings this leg up to the spec size after DRBD has attached it:
// resize the backing, then let the coordinator grow the DRBD device over the
// peers (growIfCoordinator withholds the size until every peer's backing
// reports grown).
//
// The resize must follow the attach for a restore clone. A clone is born at
// the snapshot's size and carries the source's DRBD internal metadata at that
// offset. Attaching first keeps the metadata findable, so the leg adopts it
// and reaches UpToDate like a same-size restore; growIfCoordinator's drbdadm
// resize then relocates it while growing the device. Resizing the backing
// before attach strands the metadata, so create-md mints a fresh Inconsistent
// generation the leg never leaves (birth generation is skipped for clones,
// and both legs come up Inconsistent so neither can be a sync source). A
// fresh volume is already at size, so the resize is a no-op for it.
func (r *VolumeReconciler) growLeg(ctx context.Context, be backend.Backend, vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) (int64, time.Duration, error) {
	if !localDiskless {
		// backingBytes, not the spec size: a padded volume's legs all
		// carry the metadata overhead, and growing one leg to the bare
		// spec would cap the DRBD device below the restored filesystem.
		if err := be.Resize(ctx, vol.Name, backingBytes(vol)); err != nil {
			return 0, 0, err
		}
	}
	return r.growIfCoordinator(ctx, vol, st)
}

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
