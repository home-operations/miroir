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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
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

	// busyFails counts consecutive ErrBusy teardown outcomes per volume.
	// Past busyFailLimit the loop escalates — Warning Event, status
	// Message, parked cadence — instead of silently retrying every 10s
	// forever with the cause swallowed (issue #195). See reportBusy.
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

// probeNow stamps the current time as a probe timestamp for status writes.
func probeNow() *metav1.Time {
	t := metav1.Now()
	return &t
}

// errSplitBrain gives the split-brain transition log a real error value.
var errSplitBrain = errors.New("DRBD split-brain detected")

// errRestoreSourceGone marks a restore whose source snapshot was deleted
// before this leg cloned it: the backing can never be created here, so the
// retry parks instead of riding the workqueue backoff forever (issue #195).
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
	minor, err := r.assignMinor(vol)
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
	// Cache only the settled state: a mid-grow pass (short requeue, size
	// withheld) must keep taking the full pipeline until the grow lands, and
	// a split-brain volume — locally seen or peer-reported — must re-enter it
	// every poll so recoverSplitBrain keeps retrying until the connections
	// re-form.
	if requeue == drbdPollInterval && reportSize == vol.Spec.SizeBytes && !splitActive {
		r.storeRealized(vol.Generation, vol.Name, st, true)
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
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
	// here until the deep check (issue #279). No kernel event re-breaks
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
	// can route it into recoverSplitBrain (issue #144).
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

// dropRecovery forgets a torn-down volume's split-brain debounce stamp so a
// later volume under the same name starts with a clean slate.
func (r *VolumeReconciler) dropRecovery(name string) {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	delete(r.lastRecovery, name)
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
		return r.handleTeardownError(ctx, vol, err)
	}
	// Drop metrics only once the device is gone: a retrying teardown
	// must not blank a volume that still exists.
	dropVolumeMetrics(vol.Name)
	r.dropRealized(vol.Name)
	r.dropRecovery(vol.Name)
	r.clearBusyFails(vol.Name)
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
// replicaView folds one diskful leg's kernel state into the exported
// metric view — the one place the drbdsetup-unit conversions happen
// (percent-in-sync → 0-1 ratio, KiB → bytes, per Prometheus base units).
// Shared by the full pass and the fast path so a gauge added to one can
// never silently serve stale values from the other.
func replicaView(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string, localDiskless bool) miroirReplicaView {
	return miroirReplicaView{
		upToDate:       st.DiskState == drbd.DiskUpToDate,
		connected:      diskfulPeersConnected(st, vol, self),
		splitBrain:     st.SplitBrain,
		suspended:      st.Suspended,
		quorum:         st.Quorum,
		diskFailed:     diskFailedLatch(vol, self, st, localDiskless),
		primary:        st.Primary,
		resyncRatio:    st.ResyncPercent / 100,
		outOfSyncBytes: float64(st.OutOfSyncKiB) * 1024,
	}
}

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

// diskfulPeersUpToDate reports whether every completed diskful peer's disk
// is UpToDate in this node's kernel view. A resyncing (Inconsistent) or
// failed (Diskless) peer cannot cut a snapshot leg, so a barrier raised
// over it is doomed to expire — and it would freeze the workload for the
// whole SuspendDeadline each retry. Same skip rule as
// diskfulPeersConnected.
func diskfulPeersUpToDate(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if st.PeerDiskState[rep.NodeID] != drbd.DiskUpToDate {
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

// recoverSplitBrain reacts to a split-brain, seen locally (a StandAlone
// connection) or reported by a peer's status slot. A volume that never held
// data holds nothing to lose, so it applies ResolveSplitBrain to self-heal —
// the safety net for volumes born under releases whose per-node day0
// seeding could birth-diverge (issue #139; the birth generation now makes
// that impossible for new volumes). "Held data" is either Activated
// (staged for a consumer) or Formatted (carries a filesystem) — Formatted
// latches before the grow-to-fill step, so a formatted volume whose stage
// failed at resize is still covered. Such a volume logs the manual remedy on
// the transition edge (only where the split is locally visible — the losing
// leg's own slot never records it, so it would log every poll) and is left
// for an operator.
//
// Entry is floored to once per poll interval: connection flaps — recovery's
// own, and drbdadm adjust auto-reconnecting a parked StandAlone leg every
// pass — each emit a DRBD event that requeues the volume, re-entering here
// several times per second (issue #144). The floor caps both the recovery
// attempts and the manual-remedy log, whose transition edge the flapping
// status slot keeps re-opening. Failures are retried on a later poll — the
// split state is never fast-path cached.
func (r *VolumeReconciler) recoverSplitBrain(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, localSplit bool) {
	r.recoveryMu.Lock()
	if time.Since(r.lastRecovery[vol.Name]) < drbdPollInterval {
		r.recoveryMu.Unlock()
		return
	}
	if r.lastRecovery == nil {
		r.lastRecovery = map[string]time.Time{}
	}
	r.lastRecovery[vol.Name] = time.Now()
	r.recoveryMu.Unlock()

	log := ctrl.LoggerFrom(ctx)
	// The kernel log carries the handshake's actual verdict ("Split-Brain
	// detected", "Unrelated data, aborting") — the status API only shows
	// the resulting StandAlone. Captured before recovery mutates state,
	// floored with it to once per poll interval.
	kmsgPath := r.KmsgPath
	if kmsgPath == "" {
		kmsgPath = "/dev/kmsg"
	}
	if lines := captureKmsg(kmsgPath, vol.Name, 30); len(lines) > 0 {
		log.Info("kernel log at split-brain detection", "volume", vol.Name, "kmsg", lines)
	} else {
		log.V(1).Info("kernel log unreadable at split-brain detection", "volume", vol.Name, "path", kmsgPath)
	}
	if vol.Status.Activated || vol.Status.Formatted {
		if localSplit && !vol.Status.PerNode[r.NodeName].SplitBrain {
			log.Error(errSplitBrain,
				"manual resolution required (drbdadm connect --discard-my-data on the losing node)",
				"volume", vol.Name)
		}
		return
	}
	log.Info("auto-recovering split-brain on never-written volume", "volume", vol.Name)
	if err := r.DRBD.ResolveSplitBrain(ctx, res); err != nil {
		log.Error(err, "split-brain auto-recovery failed", "volume", vol.Name)
	}
}

// peerReportedSplitBrain reports whether any other node's status slot
// records a split-brain. The losing leg of a birth split never parks
// StandAlone locally — the survivor refuses the handshake and the loser
// returns to Connecting — so the peers' reported state is its only trigger.
// Each slot holds only its own node's kernel state (patchStatus writes
// st.SplitBrain), so the signal clears as soon as the survivor reconnects;
// a leg never echoes a peer's report back into its own slot.
func peerReportedSplitBrain(vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for node, st := range vol.Status.PerNode {
		if node != self && st.SplitBrain {
			return true
		}
	}
	return false
}

// mintBirthUUID creates a fresh volume's birth generation and returns the
// refreshed status. A fresh volume's legs all attach Inconsistent at
// just-created metadata and wait, connected; the winner then mints the one
// UUID every leg adopts over the live connections — a single replicated
// generation cannot birth-diverge (issue #139). A no-op (status returned
// unchanged) on every leg but the winner's, and on any volume not waiting
// on birth.
func (r *VolumeReconciler) mintBirthUUID(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, localDiskless bool) (drbd.Status, error) {
	if !birthInitPending(vol, st, r.NodeName, localDiskless) || !drbd.IsWinner(res) {
		return st, nil
	}
	ctrl.LoggerFrom(ctx).Info("creating birth generation (skip initial sync)", "volume", vol.Name)
	if err := r.DRBD.InitialUUID(ctx, vol.Name); err != nil {
		return st, err
	}
	return r.DRBD.Status(ctx, vol.Name)
}

// mintGenerations runs the mutually exclusive one-time generation mints:
// the birth UUID for a fresh volume, or the restore seed UUID for a
// padded restore (its gates are disjoint from birth's — birth refuses
// FullSync legs and the seed mint requires them).
func (r *VolumeReconciler) mintGenerations(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, localDiskless bool) (drbd.Status, error) {
	st, err := r.mintBirthUUID(ctx, vol, res, st, localDiskless)
	if err != nil {
		return st, err
	}
	return r.mintRestoreSeedUUID(ctx, vol, st, localDiskless)
}

// mintRestoreSeedUUID mints the data generation for the seed leg of a
// restore that crossed the replication boundary (an unreplicated source
// cloned into a replicated volume): create-md left the clone's fresh
// metadata at UUID_JUST_CREATED, the birth flow refuses (the volume holds
// real data, and its FullSync joiners must not be declared identical),
// and there was no metadata to adopt — so nothing else can ever move this
// volume past Inconsistent. ForceFullSyncSource flips the seed UpToDate
// with a full bitmap toward every peer; the just-created joiners then
// elect full SyncTarget on their handshakes. A crash between its promote
// and demote leaves the role Primary with the mint already durable
// (UpToDate), which the pending gate no longer matches — the recovery
// branch demotes it (best-effort: a real consumer holding the device
// refuses harmlessly, and Activated cannot have latched yet because the
// latch runs after this mint).
func (r *VolumeReconciler) mintRestoreSeedUUID(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) (drbd.Status, error) {
	if r.restoreSeedStuckPrimary(vol, st, localDiskless) {
		ctrl.LoggerFrom(ctx).Info("demoting restore seed left Primary by an interrupted mint", "volume", vol.Name)
		if err := r.DRBD.Demote(ctx, vol.Name); err != nil {
			ctrl.LoggerFrom(ctx).V(1).Info("seed demote refused; leaving it to the consumer lifecycle",
				"volume", vol.Name, "error", err)
			return st, nil
		}
		return r.DRBD.Status(ctx, vol.Name)
	}
	if !r.restoreSeedPending(vol, st, localDiskless) {
		return st, nil
	}
	ctrl.LoggerFrom(ctx).Info("seeding restore data generation (forces full sync to joiners)", "volume", vol.Name)
	if err := r.DRBD.ForceFullSyncSource(ctx, vol.Name); err != nil {
		return st, err
	}
	return r.DRBD.Status(ctx, vol.Name)
}

// restoreSeedStuckPrimary reports a padded-restore seed left Primary by a
// mint whose demote never ran: same leg identity as the pending gate, but
// the disk already UpToDate, the role still Primary, and the volume never
// Activated (a consumer's promote latches Activated on the pass that
// observes it, so an unactivated Primary here can only be the mint's).
func (r *VolumeReconciler) restoreSeedStuckPrimary(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) bool {
	if localDiskless || vol.Spec.DRBD == nil ||
		vol.Spec.Source == nil || !vol.Spec.Source.PadForMetadata {
		return false
	}
	seed := vol.Spec.FirstDiskfulReplica()
	if seed == nil || seed.Node != r.NodeName || seed.FullSync {
		return false
	}
	return st.Primary && st.DiskState == drbd.DiskUpToDate && !vol.Status.Activated
}

// restoreSeedPending gates the restore seed mint on proof that this leg
// is the data-bearing clone waiting on its first generation, and nothing
// else: only the padded-restore seed (first diskful, never FullSync — the
// clone), only fresh self-created metadata (adopted metadata carries a
// real generation; see Driver.MetadataAdopted), only while Inconsistent,
// and only while no peer has ever reported UpToDate — a seed leg
// recreated after a node wipe wears the same fresh-metadata signature,
// but its peers hold the data now and it must join as a plain full
// SyncTarget, not declare its blank disk the source.
func (r *VolumeReconciler) restoreSeedPending(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) bool {
	if localDiskless || vol.Spec.DRBD == nil ||
		vol.Spec.Source == nil || !vol.Spec.Source.PadForMetadata {
		return false
	}
	seed := vol.Spec.FirstDiskfulReplica()
	if seed == nil || seed.Node != r.NodeName || seed.FullSync {
		return false
	}
	if st.DiskState != drbd.DiskInconsistent || r.DRBD.MetadataAdopted(vol.Name) {
		return false
	}
	for _, ds := range st.PeerDiskState {
		if ds == drbd.DiskUpToDate {
			return false
		}
	}
	for node, ps := range vol.Status.PerNode {
		if node != r.NodeName && ps.DiskState == drbd.DiskUpToDate {
			return false
		}
	}
	return true
}

// handleSplitBrain detects an active split and routes it into recovery.
// The losing leg of a split never parks StandAlone locally — it sits
// Connecting while the survivor refuses the handshake — so its only signal
// that it must discard is a peer's reported split-brain (issue #144).
// Gated on !connected so a stale peer report cannot churn a healthy
// volume.
func (r *VolumeReconciler) handleSplitBrain(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, connected bool) bool {
	splitActive := st.SplitBrain || (!connected && peerReportedSplitBrain(vol, r.NodeName))
	if splitActive {
		r.recoverSplitBrain(ctx, vol, res, st.SplitBrain)
	}
	return splitActive
}

// birthInitPending reports whether this leg is waiting on the one-time
// birth generation: a never-written replicated volume whose local disk and
// every diskful peer's disk sit Inconsistent over established connections.
// The Activated/Formatted gate means divergent real data can never be
// declared clean; a FullSync joiner means the volume already has a
// generation, so this is birth only. Fail closed: every snapshot-derived
// volume is ineligible, even if its metadata appears Inconsistent.
func birthInitPending(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, self string, localDiskless bool) bool {
	if vol.Spec.DRBD == nil || vol.Spec.Source != nil || localDiskless {
		return false
	}
	if vol.Status.Activated || vol.Status.Formatted {
		return false
	}
	if st.DiskState != drbd.DiskInconsistent {
		return false
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.FullSync {
			return false
		}
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if !st.PeerConnected[rep.NodeID] || st.PeerDiskState[rep.NodeID] != drbd.DiskInconsistent {
			return false
		}
	}
	return true
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
		// The block interrupts the teardown episode: a busy streak from
		// before it must not pre-bias the attempts after it unblocks.
		r.clearBusyFails(vol.Name)
		st := vol.Status.PerNode[r.NodeName]
		st.Message = "replica removal blocked: " + reason
		st.LastProbedAt = probeNow()
		if err := r.patchStatus(ctx, vol, st); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.teardown(ctx, vol); err != nil {
		return r.handleTeardownError(ctx, vol, err)
	}
	dropVolumeMetrics(vol.Name)
	r.dropRealized(vol.Name)
	r.dropRecovery(vol.Name)
	r.clearBusyFails(vol.Name)
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
	slot := vol.Status.PerNode[r.NodeName]
	if vol.Spec.DRBD != nil {
		if err := r.DRBD.Down(ctx, vol.Name); err != nil {
			// A kernel-wedged resource must not classify as ErrBusy: the
			// fast retry would strand another unkillable drbdsetup down
			// every cycle, and nothing but a reboot clears the wedge.
			if errors.Is(err, drbd.ErrWedged) {
				return err
			}
			// A still-staged device answers "held open"; classify it as
			// ErrBusy so teardown takes the 10s retry, not the workqueue's
			// minutes-long backoff (NodeUnstage releases it shortly). An
			// orphaned hold no retry can release routes around the down
			// instead of parking on it forever (issue #319).
			busy := backend.Busy(err)
			if r.orphanHold(ctx, vol, slot, busy) {
				return r.reclaimOrphanHold(ctx, vol, slot, busy)
			}
			// Force-detach escalation after extended busy: when a live
			// opener PID has kept the device busy past busyForceDetachLimit
			// (~10 min), attempt a one-shot force-detach to free the
			// backing — matching Linstor's pattern of escalating teardown
			// strategy instead of parking forever. A mounted device whose
			// openers have exited, a scan error, or a cause without opener
			// pids stays in normal parking (#195: never destroy a backing
			// under a consumer).
			if r.busyFailCount(vol.Name) == busyForceDetachLimit && hasLiveOpener(r.procDir(), err.Error()) {
				return r.forceDetachLive(ctx, vol, slot, busy)
			}
			return busy
		}
		// With the resource down, wipe the DRBD metadata before the sweep
		// removes the device, so freed blocks cannot carry a stale generation
		// identifier into a reuse (issue #139). Best-effort: the sweep
		// destroys the whole device anyway, so a wipe (or pool-resolution)
		// failure must not block the deletion. Never key the DeviceCreated
		// check on the kernel DiskState: a diskful replica reads "Diskless"
		// after an I/O-error detach, and skipping its wipe+delete would
		// leak the backing device.
		if slot.DeviceCreated && !slot.Diskless {
			if pool, err := r.Pools.Get(volumePoolOn(vol, r.NodeName)); err != nil {
				ctrl.LoggerFrom(ctx).V(1).Info("cannot resolve the leg's pool for the metadata wipe; sweep delete destroys metadata with the device",
					"volume", vol.Name, "error", err)
			} else if err := r.DRBD.WipeMetadata(ctx, vol.Name, pool.Backend.DevicePath(vol.Name), slot.DRBDMinor); err != nil {
				ctrl.LoggerFrom(ctx).V(1).Info("wipe-md failed during teardown; backing delete will destroy metadata",
					"volume", vol.Name, "error", err)
			}
		}
	}
	// Delete from every pool instead of resolving one: the leg's pool can
	// be unknowable here (crash before the first status patch, a diskless
	// re-add over a leftover backing), and each backend's Delete succeeds
	// when its device is absent — see Pools.SweepDelete.
	return r.Pools.SweepDelete(ctx, vol.Name)
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

// wedgedRequeue spaces teardown retries of a kernel-wedged resource. The
// 10s ErrBusy cadence would strand another unkillable drbdsetup down every
// cycle; nothing but a node reboot clears the wedge, so the retry only
// needs to notice that the reboot happened.
const wedgedRequeue = 5 * time.Minute

// reportWedged surfaces a teardown the kernel can no longer finish
// (drbd.ErrWedged) — Warning Event, status message, and the wedged gauge
// the shipped alerts page on — then parks the retry at wedgedRequeue.
// The escalation latches on the transition cycle: the park can outlive
// the wedge only through a reboot, and re-emitting an identical Event,
// Error log, and no-op status write every cycle is pure churn (issue
// #319: a wedged volume streamed 123 of them). The gauge is re-raised
// every cycle regardless — it is process state and must survive an agent
// restart into an already-stamped Message. It clears on any non-wedged
// teardown outcome, on teardown success (dropVolumeMetrics), and when
// the CR vanishes (volumeGone).
func (r *VolumeReconciler) reportWedged(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	recordWedged(vol)
	if vol.Status.PerNode[r.NodeName].Message == cause.Error() {
		ctrl.LoggerFrom(ctx).V(1).Info("teardown still wedged in kernel; retry stays parked", "volume", vol.Name)
		return ctrl.Result{RequeueAfter: wedgedRequeue}, nil
	}
	ctrl.LoggerFrom(ctx).Error(cause, "teardown wedged in kernel", "volume", vol.Name)
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownWedged", "Teardown",
			"DRBD cannot tear down %s: device stuck Detaching with connections gone (LINBIT/drbd#137); reboot node %s to clear it",
			vol.Name, r.NodeName)
	}
	return r.parkWithMessage(ctx, vol, cause, wedgedRequeue)
}

// parkWithMessage stamps the cause on this node's status slot and parks
// the retry — the shared tail of every teardown/realize escalation
// (reportWedged, reportBusy, reportRestoreOrphan), kept in one place so
// the park contract cannot drift between them.
func (r *VolumeReconciler) parkWithMessage(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error, requeue time.Duration) (ctrl.Result, error) {
	st := vol.Status.PerNode[r.NodeName]
	st.Message = cause.Error()
	st.LastProbedAt = probeNow()
	if err := r.patchStatus(ctx, vol, st); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// handleTeardownError triages one failed teardown outcome, shared by the
// deletion and removal paths so the busy-streak bookkeeping cannot drift:
// a kernel wedge parks via reportWedged, a held-open device paces via
// reportBusy, anything else surfaces as a hard error. Every non-busy
// outcome resets the busy streak.
func (r *VolumeReconciler) handleTeardownError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, err error) (ctrl.Result, error) {
	if errors.Is(err, drbd.ErrWedged) {
		r.clearBusyFails(vol.Name)
		return r.reportWedged(ctx, vol, err)
	}
	// Any other outcome means a previously reported wedge is gone — e.g.
	// the reboot happened and the device is now merely held open — so the
	// critical alert must stop paging for it.
	clearWedged(vol.Name)
	if errors.Is(err, backend.ErrBusy) {
		// A still-staged (or force-deleted) pod holds the device open;
		// NodeUnstage releases it once the consumer moves off this node.
		return r.reportBusy(ctx, vol, err)
	}
	r.clearBusyFails(vol.Name)
	return ctrl.Result{}, err
}

// busyFailLimit is how many consecutive ErrBusy teardown outcomes ride the
// fast 10s retry — NodeUnstage normally releases the device within a few
// cycles — before the loop escalates; busyRetryAfter is the parked cadence.
// The finalizer is never released on a busy outcome itself: the hold may
// be a live mount, and force-releasing would leak the backing device or
// destroy it under a consumer (issue #195). The one exit is the orphaned-
// hold reclaim (orphanHold), which completes a real teardown of the
// backing first and only fires past this same threshold.
const (
	busyFailLimit  = 30 // ~5 minutes at the 10s cadence
	busyRetryAfter = time.Minute

	// busyForceDetachLimit is the extended busy threshold — ~10 minutes at
	// the 10s cadence — past which teardown escalates from parking to a
	// one-shot force-detach attempt even when a live opener is present,
	// matching Linstor's pattern of escalating the teardown strategy after
	// extended failure rather than parking forever.
	busyForceDetachLimit = busyFailLimit * 2
)

// reportBusy paces one ErrBusy teardown outcome: the fast 10s retry below
// busyFailLimit, then a Warning Event, a status Message naming the cause,
// and the parked cadence. The cause is logged on every fast cycle and on
// the escalation — an ErrBusy from the backend sweep looks identical to a
// held-open device without it — and demotes to V(1) once parked.
func (r *VolumeReconciler) reportBusy(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	r.busyMu.Lock()
	if r.busyFails == nil {
		r.busyFails = map[string]int{}
	}
	r.busyFails[vol.Name]++
	fails := r.busyFails[vol.Name]
	r.busyMu.Unlock()
	if fails < busyFailLimit {
		ctrl.LoggerFrom(ctx).Info("device busy during teardown, retrying",
			"volume", vol.Name, "attempts", fails, "error", cause)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	// Latch the escalation on the crossing cycle: the parked retry can
	// outlive the hold by hours, and re-emitting an identical Event, Error
	// log, and no-op status write every cycle is pure churn (issue #319:
	// one parked volume streamed 648 Error lines).
	if fails > busyFailLimit {
		ctrl.LoggerFrom(ctx).V(1).Info("teardown still busy; retry stays parked",
			"volume", vol.Name, "attempts", fails)
	} else {
		ctrl.LoggerFrom(ctx).Error(cause, "teardown still busy; parking the retry",
			"volume", vol.Name, "attempts", fails)
		if r.Recorder != nil {
			r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownBusy", "Teardown",
				"cannot tear down %s on node %s after %d attempts: %v; something still holds the device (or its backing) open",
				vol.Name, r.NodeName, fails, cause)
		}
	}
	if vol.Status.PerNode[r.NodeName].Message == cause.Error() {
		return ctrl.Result{RequeueAfter: busyRetryAfter}, nil
	}
	return r.parkWithMessage(ctx, vol, cause, busyRetryAfter)
}

// clearBusyFails resets the volume's consecutive busy-teardown count.
func (r *VolumeReconciler) clearBusyFails(name string) {
	r.busyMu.Lock()
	delete(r.busyFails, name)
	r.busyMu.Unlock()
}

// busyFailCount reads the volume's consecutive busy-teardown count.
func (r *VolumeReconciler) busyFailCount(name string) int {
	r.busyMu.Lock()
	defer r.busyMu.Unlock()
	return r.busyFails[name]
}

// procDir resolves the /proc override for the orphaned-hold checks.
func (r *VolumeReconciler) procDir() string {
	if r.ProcDir != "" {
		return r.ProcDir
	}
	return "/proc"
}

// orphanHold reports whether a busy teardown outcome is the unwinnable
// held-open state of issue #319 — the device pinned by a leaked freeze's
// dead superblock, which no retry, restart, or unstage can ever release —
// as opposed to a still-staged device NodeUnstage frees shortly. It
// requires all of: a deletion-targeted volume (a removed replica's volume
// lives on and keeps the parked retry), a known minor, a busy streak past
// the escalation threshold (an in-flight unstage never lasts that long),
// a held-open failure whose reported openers have all exited (a live
// consumer's opener pid is alive; a plain fs mount's dead mount(8) pid is
// disambiguated by the mount scan below), and the device mounted in no
// mount namespace on the node. Any doubt — no opener list, an unreadable
// /proc, a surviving mount — reads as "live consumer" and the park stands.
func (r *VolumeReconciler) orphanHold(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, slot miroirv1alpha1.ReplicaStatus, cause error) bool {
	if vol.DeletionTimestamp.IsZero() || slot.DRBDMinor <= 0 {
		return false
	}
	if r.busyFailCount(vol.Name) < busyFailLimit {
		return false
	}
	if !strings.Contains(cause.Error(), "held open") {
		return false
	}
	pids := openerPids(cause.Error())
	if len(pids) == 0 {
		return false
	}
	for _, pid := range pids {
		if pidAlive(r.procDir(), pid) {
			return false
		}
	}
	mounted, err := deviceMountedAnywhere(r.procDir(), slot.DRBDMinor)
	if err != nil {
		ctrl.LoggerFrom(ctx).V(1).Info("cannot scan mount namespaces for the orphaned-hold check; teardown stays parked",
			"volume", vol.Name, "error", err)
		return false
	}
	return !mounted
}

// reclaimOrphanHold routes a deletion around the pinned minor: the only
// thing an orphaned hold pins is the DRBD minor itself, so force-detach
// frees the backing and the sweep destroys it, letting the caller release
// the finalizer through teardown's normal tail. The rendered config goes
// with the volume — left behind it would hold the volume's DRBD port
// hostage against the next volume handed the recycled number (see
// drbd.RemoveConfig) — while the minor assignment deliberately stays:
// the kernel minor is pinned until a reboot, releasing the number would
// hand it to a new volume, and the startup orphan sweep reaps the
// reservation once the reboot clears the state. On any failure the
// original busy outcome stands and the parked retry re-evaluates
// (ForceDetach no-ops once detached, so a failed sweep or config removal
// converges on the next cycle).
func (r *VolumeReconciler) reclaimOrphanHold(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, slot miroirv1alpha1.ReplicaStatus, cause error) error {
	if err := r.DRBD.ForceDetach(ctx, vol.Name, slot.DRBDMinor); err != nil {
		ctrl.LoggerFrom(ctx).Info("orphaned-hold reclaim could not detach; teardown stays parked",
			"volume", vol.Name, "error", err)
		return cause
	}
	if err := r.Pools.SweepDelete(ctx, vol.Name); err != nil {
		return err
	}
	if err := r.DRBD.RemoveConfig(vol.Name); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("teardown reclaimed from an orphaned device hold; kernel minor stays pinned until reboot",
		"volume", vol.Name, "minor", slot.DRBDMinor, "cause", cause.Error())
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeNormal, "TeardownReclaimed", "Teardown",
			"backing of %s reclaimed on node %s: the DRBD device was held open only by exited processes (leaked freeze, issue #311); orphaned minor %d frees itself on the next reboot",
			vol.Name, r.NodeName, slot.DRBDMinor)
	}
	return nil
}

// forceDetachLive attempts a one-shot force-detach escalation when the
// orphanHold check returns false (a live opener still holds the device)
// and the busy streak has persisted past busyForceDetachLimit (~10 min).
// If the force-detach frees the backing, teardown completes through its
// normal tail (sweep, config removal, finalizer). If the device remains
// busy after the detach, the original busy cause is returned for parking,
// and a TeardownForceDetached Warning event explains the outcome. A failed
// force-detach itself (e.g. DRBD resource already gone) also defers to
// normal busy parking. The force-detach is attempted exactly once at the
// threshold — no looping.
func (r *VolumeReconciler) forceDetachLive(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, slot miroirv1alpha1.ReplicaStatus, cause error) error {
	if err := r.DRBD.ForceDetach(ctx, vol.Name, slot.DRBDMinor); err != nil {
		ctrl.LoggerFrom(ctx).Info("force-detach escalation failed; teardown stays parked",
			"volume", vol.Name, "error", err)
		if r.Recorder != nil {
			r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownForceDetached", "Teardown",
				"force-detach of %s on node %s failed: %v; teardown stays parked",
				vol.Name, r.NodeName, err)
		}
		return cause
	}
	if err := r.Pools.SweepDelete(ctx, vol.Name); err != nil {
		if errors.Is(err, backend.ErrBusy) {
			ctrl.LoggerFrom(ctx).Info("force-detach escalated but device remains busy; teardown stays parked",
				"volume", vol.Name, "error", err)
			if r.Recorder != nil {
				r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownForceDetached", "Teardown",
					"%s force-detached on node %s but the backing device remains busy: %v; teardown stays parked",
					vol.Name, r.NodeName, err)
			}
			return cause
		}
		return err
	}
	if err := r.DRBD.RemoveConfig(vol.Name); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("teardown completed via force-detach escalation; a live opener was holding the device",
		"volume", vol.Name, "minor", slot.DRBDMinor)
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownForceDetached", "Teardown",
			"backing of %s force-detached and destroyed on node %s after %d busy attempts; kernel minor %d stays pinned until reboot",
			vol.Name, r.NodeName, r.busyFailCount(vol.Name), slot.DRBDMinor)
	}
	return nil
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
	return r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated: false,
		Message:       "backing device missing (node wipe or out-of-band deletion); agent will recreate it as a fresh leg",
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
	if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true,
		DevicePath:    dev,
		Pool:          poolName,
		LastProbedAt:  probeNow(),
	}); err != nil {
		return err
	}
	vol.Status.PerNode[r.NodeName] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true, DevicePath: dev, Pool: poolName,
	}
	return nil
}

// reportRealizeError routes a realizeBacking failure: an impossible
// restore parks (reportRestoreOrphan), anything else takes the normal
// status-and-backoff path.
func (r *VolumeReconciler) reportRealizeError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	if errors.Is(cause, errRestoreSourceGone) {
		return r.reportRestoreOrphan(ctx, vol, cause)
	}
	return ctrl.Result{}, r.reportError(ctx, vol, cause)
}

// restoreOrphanRequeue spaces retries of a restore whose source snapshot
// is gone. Only deleting the volume (or recreating the snapshot under the
// same name) unsticks it, so the retry only needs to notice that happened.
const restoreOrphanRequeue = 5 * time.Minute

// reportRestoreOrphan surfaces a restore that can never complete on this
// node (errRestoreSourceGone) — Warning Event and a status Message naming
// the operator's options — then parks the retry at restoreOrphanRequeue.
func (r *VolumeReconciler) reportRestoreOrphan(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	ctrl.LoggerFrom(ctx).Error(cause, "restore source snapshot is gone; parking the volume", "volume", vol.Name)
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "RestoreSourceMissing", "Realize",
			"cannot restore %s on node %s: source snapshot %s no longer exists; delete the volume or recreate the snapshot",
			vol.Name, r.NodeName, vol.Spec.Source.SnapshotName)
	}
	return r.parkWithMessage(ctx, vol, cause, restoreOrphanRequeue)
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

// latchActivated sets the one-way Activated flag when the kernel reports
// the local leg Primary — a Primary is (or was) open for writes. Beyond the
// stage-time twin (csi.markActivated) it covers volumes staged before the
// field existed: a raw-block consumer running since pre-0.4.5 never
// restages, and without the latch recoverSplitBrain would treat its
// data-bearing volume as never-written. MergeFrom, not the agent's SSA
// apply: Activated is CSI-owned and merge patching the single field avoids
// co-owning it.
func (r *VolumeReconciler) latchActivated(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status) error {
	if !st.Primary || vol.Status.Activated {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Activated = true
	return r.Status().Patch(ctx, vol, client.MergeFrom(base))
}

// patchStatus applies only this node's slot and the derived phase. A
// full-status apply would force-own peers' slots and Formatted (a CSI
// field) and revert them to this agent's stale read.
func (r *VolumeReconciler) patchStatus(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, mine miroirv1alpha1.ReplicaStatus) error {
	if vol.Status.PerNode == nil {
		vol.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	vol.Status.PerNode[r.NodeName] = mine
	vol.Status.Phase, vol.Status.ReadyReplicas = computePhase(vol)

	ac := acv1alpha1.MiroirVolume(vol.Name).
		WithStatus(acv1alpha1.MiroirVolumeStatus().
			WithPhase(vol.Status.Phase).
			WithReadyReplicas(vol.Status.ReadyReplicas).
			WithPerNode(map[string]acv1alpha1.ReplicaStatusApplyConfiguration{
				r.NodeName: *replicaStatusAC(mine),
			}))
	return r.SubResource("status").Apply(ctx, ac,
		client.FieldOwner("agent-volume-"+r.NodeName),
		client.ForceOwnership)
}

// primarySince keeps a stable timestamp for how long this leg has been
// Primary: stamped on the first pass that observes the role, carried
// through subsequent passes, dropped on demotion (the device closed). The
// auto-diskful reconciler reads its age for tie-breaker conversion.
func primarySince(vol *miroirv1alpha1.MiroirVolume, node string, primary bool) *metav1.Time {
	if !primary {
		return nil
	}
	if prev := vol.Status.PerNode[node].PrimarySince; prev != nil {
		return prev
	}
	now := metav1.Now()
	return &now
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
	if st.Pool != "" {
		ac = ac.WithPool(st.Pool)
	}
	if st.PrimarySince != nil {
		ac = ac.WithPrimarySince(*st.PrimarySince)
	}
	if st.DiskFailed {
		ac = ac.WithDiskFailed(true)
	}
	if st.DiscardGranularityBytes != 0 {
		ac = ac.WithDiscardGranularityBytes(st.DiscardGranularityBytes)
	}
	if st.Message != "" {
		ac = ac.WithMessage(st.Message)
	}
	if st.LastProbedAt != nil {
		ac = ac.WithLastProbedAt(*st.LastProbedAt)
	}
	return ac
}

// assignMinor returns the DRBD minor for this volume, allocating a free one if unset.
func (r *VolumeReconciler) assignMinor(vol *miroirv1alpha1.MiroirVolume) (int32, error) {
	if m := vol.Status.PerNode[r.NodeName].DRBDMinor; m > 0 {
		return m, nil
	}
	return r.DRBD.AllocateMinor(vol.Name)
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

// computePhase aggregates per-node states into the volume phase the CSI
// controller waits on, plus the "ready/total" diskful summary backing the
// Replicas printcolumn.
func computePhase(vol *miroirv1alpha1.MiroirVolume) (miroirv1alpha1.VolumePhase, string) {
	diskfulReplicas := vol.Spec.DiskfulReplicas()
	ready := 0
	realized := 0
	failed := false
	now := time.Now()
	for _, rep := range diskfulReplicas {
		st, ok := vol.Status.PerNode[rep.Node]
		replicated := vol.Spec.DRBD != nil
		realizedReplica := ok && st.DeviceCreated && st.SizeBytes >= vol.Spec.SizeBytes &&
			(!replicated || st.DiskState == drbd.DiskUpToDate)
		switch {
		case realizedReplica:
			// Keep current data separate from operational readiness so an
			// established but disconnected volume is Degraded, not Creating.
			realized++
			if !replicated || st.Connected {
				// Staleness gate: a replica whose LastProbedAt is set and
				// exceeds StaleProbeThreshold means the agent can no longer
				// reach the node-local resources. Treat it as realized but
				// NOT ready — even if Connected=true — so the phase is
				// Degraded, not Ready. Nil LastProbedAt (legacy volume) uses
				// backward-compatible behavior: trust the persisted fields.
				if st.LastProbedAt != nil && now.Sub(st.LastProbedAt.Time) > StaleProbeThreshold {
					// stale: realized but not ready
				} else {
					ready++
				}
			}
		case ok && st.Message != "" && !st.DeviceCreated:
			// Hard failure: the backing device never materialised.
			// Errors after that point (DRBD connect retries, status
			// hiccups) are transient and must not fail provisioning.
			failed = true
		}
	}
	readyReplicas := fmt.Sprintf("%d/%d", ready, len(diskfulReplicas))
	switch {
	case failed:
		return miroirv1alpha1.VolumeFailed, readyReplicas
	case ready == len(diskfulReplicas):
		return miroirv1alpha1.VolumeReady, readyReplicas
	case realized > 0:
		return miroirv1alpha1.VolumeDegraded, readyReplicas
	default:
		return miroirv1alpha1.VolumeCreating, readyReplicas
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
