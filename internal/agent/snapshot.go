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

package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/drbd"
)

// SuspendDeadline bounds the snapshot write barrier: a peer that never
// snapshots must not freeze the volume forever. Exported so the agent's
// startup sweep can apply the same deadline when lifting barriers left
// raised by a previous crash.
//
// Peers compare the coordinator's SuspendedAt (its wall clock) against
// their own, so this deadline doubles as the cross-node clock-skew
// budget: nodes must run NTP. Skew approaching 60s can prematurely expire
// (or over-extend) a round; homelab nodes are expected well under a second.
const SuspendDeadline = 60 * time.Second

// suspendRetryBackoff spaces barrier retries after a failed round.
const suspendRetryBackoff = 30 * time.Second

// SnapshotReconciler realizes MiroirSnapshots on this node.
//
// Replicated volumes need byte-identical snapshots on both legs (restore
// clones each leg locally and skips the resync), so every replica raises
// a write barrier before any leg is cut — a node promoted mid-round would
// otherwise write into some legs and not others (LINSTOR suspends all
// diskful nodes the same way):
//
//	coordinator: suspend-io → patch ioSuspended + Suspended
//	peer:        sees ioSuspended → suspend-io → Suspended
//	all:         see every replica Suspended → snapshot → Done
//	coordinator: sees all Done (or deadline) → resume-io → readyToUse
//	peer:        sees readyToUse (or barrier cleared) → resume-io
//
// The coordinator is the Primary when one exists (it is where writes
// originate, so its barrier must be first up and last down), else
// replicas[0].
type SnapshotReconciler struct {
	client.Client
	NodeName string
	// Pools holds this node's storage pools; snapshot legs are cut in the
	// pool holding the volume's local replica.
	Pools Pools
	DRBD  *drbd.Driver
	// Recorder emits the BarrierStuck warning; optional.
	Recorder events.EventRecorder

	// barrierFails counts consecutive DRBD failures on the barrier path
	// (the status read and suspend-io) per snapshot. Past barrierFailLimit
	// the round parks at a slow retry instead of riding the workqueue's
	// exponential backoff forever — on a wedged kernel module
	// (LINBIT/drbd#137) neither call ever succeeds, and without a bound
	// the silent retries outlive the incident.
	barrierFailsMu sync.Mutex
	barrierFails   map[string]int
}

// barrierFailLimit is how many consecutive barrier-path failures ride the
// workqueue's fast exponential backoff (transients heal there) before the
// round parks; barrierRetryAfter is the parked cadence.
const (
	barrierFailLimit  = 5
	barrierRetryAfter = time.Minute
)

// drbdBarrierTimeout bounds each DRBD control call the snapshot round makes.
// This reconciler is single-worker (its round protocol assumes head-of-line
// ordering), so a call left on RealExec's 2-minute default would, against a
// wedged resource (LINBIT/drbd#137), stall every other volume's snapshot on
// the node for that long. The teardown path bounds its own DRBD calls the
// same way (drbd.downTimeout); these are metadata ops that finish in
// milliseconds on healthy hardware.
const drbdBarrierTimeout = 30 * time.Second

// barrierFailed classifies one barrier-path failure: fast backoff below
// barrierFailLimit, then a Warning Event and the parked retry.
func (r *SnapshotReconciler) barrierFailed(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	r.barrierFailsMu.Lock()
	if r.barrierFails == nil {
		r.barrierFails = map[string]int{}
	}
	r.barrierFails[snap.Name]++
	fails := r.barrierFails[snap.Name]
	r.barrierFailsMu.Unlock()
	if fails < barrierFailLimit {
		return ctrl.Result{}, cause
	}
	ctrl.LoggerFrom(ctx).Error(cause, "cannot drive the snapshot barrier; parking the retry",
		"snapshot", snap.Name, "volume", vol.Name, "attempts", fails)
	if r.Recorder != nil {
		r.Recorder.Eventf(snap, nil, corev1.EventTypeWarning, "BarrierStuck", "Suspend",
			"cannot drive the snapshot barrier on %s after %d attempts: %v", vol.Name, fails, cause)
	}
	return ctrl.Result{RequeueAfter: barrierRetryAfter}, nil
}

// clearBarrierFails resets the snapshot's consecutive-failure count.
func (r *SnapshotReconciler) clearBarrierFails(name string) {
	r.barrierFailsMu.Lock()
	delete(r.barrierFails, name)
	r.barrierFailsMu.Unlock()
}

// backendFor resolves the backend holding the volume's local leg — the
// spec entry, or the self-reported status slot for a leg pending removal
// (snapshots block replica removal, so the slot is still there).
func (r *SnapshotReconciler) backendFor(vol *miroirv1alpha1.MiroirVolume) (backend.Backend, error) {
	pb, err := r.Pools.Get(volumePoolOn(vol, r.NodeName))
	if err != nil {
		return nil, err
	}
	return pb.Backend, nil
}

// drbdStatus, suspendIO, and resumeIO wrap the driver's DRBD control calls
// with drbdBarrierTimeout so one wedged call cannot pin the single reconcile
// worker. RealExec applies the tighter of its own bound and the parent
// deadline, so the effective bound is drbdBarrierTimeout.
func (r *SnapshotReconciler) drbdStatus(ctx context.Context, name string) (drbd.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, drbdBarrierTimeout)
	defer cancel()
	return r.DRBD.Status(ctx, name)
}

func (r *SnapshotReconciler) suspendIO(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, drbdBarrierTimeout)
	defer cancel()
	return r.DRBD.SuspendIO(ctx, name)
}

func (r *SnapshotReconciler) resumeIO(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, drbdBarrierTimeout)
	defer cancel()
	return r.DRBD.ResumeIO(ctx, name)
}

// Reconcile drives one snapshot's state machine from this node's view.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	snap := &miroirv1alpha1.MiroirSnapshot{}
	if err := r.Get(ctx, req.NamespacedName, snap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: snap.Spec.VolumeName}, vol); err != nil {
		if apierrors.IsNotFound(err) && !snap.DeletionTimestamp.IsZero() {
			// Source volume already gone; nothing local can remain. The
			// failure count dies with the snapshot here too, or a later
			// snapshot reusing the name would inherit it pre-parked.
			r.clearBarrierFails(snap.Name)
			return ctrl.Result{}, r.removeFinalizer(ctx, snap)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !snap.DeletionTimestamp.IsZero() {
		// The failure count dies with the snapshot, so a later snapshot
		// under the same name starts with a clean slate.
		r.clearBarrierFails(snap.Name)
		// Deletion runs before the membership gate: a node that left
		// spec.replicas after the snapshot was cut still holds its
		// finalizer, and skipping it would wedge the snapshot in
		// Terminating forever (which also blocks every later replica
		// removal on the volume).
		//
		// A snapshot deleted mid-round must not strand its barrier. The
		// lift keys on kernel state, not status.ioSuspended: a peer's
		// barrier can outlive the coordinator's void (status already
		// false), and a torn-down resource must not error the path
		// (Status fails → nothing is suspended). Never lift a barrier a
		// sibling snapshot's live round now owns.
		if vol.Spec.DRBD != nil {
			if st, err := r.drbdStatus(ctx, vol.Name); err == nil && st.Suspended {
				if err := r.resumeUnlessSiblingRound(ctx, snap, vol); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		// A diskless replica has no backend snapshot to delete; a
		// departed node deletes whatever leg it still holds. The sweep
		// covers every pool because a departed leg's pool can be
		// unknowable (its status slot is deleted at removal) and each
		// backend's DeleteSnapshot succeeds when it is already absent —
		// guessing one pool could wedge this finalizer forever.
		if r.disklessOn(vol) {
			return ctrl.Result{}, r.removeFinalizer(ctx, snap)
		}
		if err := r.Pools.SweepDeleteSnapshot(ctx, vol.Name, snap.Name); err != nil {
			if errors.Is(err, backend.ErrBusy) {
				// The snapshot device is still open (e.g. a restore in
				// progress); retry until it is released.
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.removeFinalizer(ctx, snap)
	}

	if !replicaOn(vol, r.NodeName) {
		return ctrl.Result{}, nil
	}

	if snap.Status.ReadyToUse {
		// A peer's barrier can outlive the round by a moment; lift it —
		// unless a sibling snapshot's round owns the kernel barrier now.
		// Best-effort: if DRBD cannot report, nothing holds a barrier.
		if vol.Spec.DRBD != nil {
			if st, err := r.drbdStatus(ctx, vol.Name); err == nil && st.Suspended {
				return ctrl.Result{}, r.resumeUnlessSiblingRound(ctx, snap, vol)
			}
		}
		return ctrl.Result{}, nil
	}

	if vol.Spec.DRBD != nil {
		// The volume agent has not brought DRBD up yet; also skip
		// split-brain (snapshotting divergent data is worse than none).
		st := vol.Status.PerNode[r.NodeName]
		switch {
		case st.DiskState == "":
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		case st.SplitBrain:
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	if vol.Spec.DRBD == nil {
		// The volume reconciler has not created the backing device yet —
		// the two reconcilers race at startup, and Sync on the missing
		// device would error-loop until it exists (issue #195). Mirrors
		// the replicated path's DiskState gate above.
		if !vol.Status.PerNode[r.NodeName].DeviceCreated {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		// Single replica: no barrier needed, but queued writes must land.
		be, err := r.backendFor(vol)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := be.Sync(ctx, vol.Name); err != nil {
			return ctrl.Result{}, err
		}
		if err := be.Snapshot(ctx, vol.Name, snap.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchSnap(ctx, snap, func(s *miroirv1alpha1.MiroirSnapshot) {
			s.Status.PerNode = map[string]miroirv1alpha1.SnapshotNodeState{
				r.NodeName: miroirv1alpha1.SnapshotDone,
			}
			s.Status.SizeBytes = vol.Spec.SizeBytes
			s.Status.SourceFormatted = vol.Status.Formatted
			s.Status.ReadyToUse = true
		})
	}
	return r.reconcileReplicated(ctx, snap, vol)
}

func (r *SnapshotReconciler) reconcileReplicated(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, vol *miroirv1alpha1.MiroirVolume) (ctrl.Result, error) {
	st, err := r.drbdStatus(ctx, vol.Name)
	if err != nil {
		// On a wedged module this call is the one that fails (hanging
		// until execTimeout kills it), so it must ride the same bounded
		// retry as suspend-io or the give-up never engages.
		return r.barrierFailed(ctx, snap, vol, err)
	}
	coordinator := r.isCoordinator(vol, st)
	myState := snap.Status.PerNode[r.NodeName]
	expired := snap.Status.SuspendedAt != nil &&
		time.Since(snap.Status.SuspendedAt.Time) > SuspendDeadline
	// Disconnected or resyncing legs have diverged (quorum off lets the
	// survivor write alone) and a barrier over diverged legs cuts
	// diverged legs. Gates raising and cutting only — a degraded volume
	// must still resume. Only diskful peers count: a downed tie-breaker
	// holds no leg, and gating on its link would block every snapshot in
	// exactly the degraded mode the tie-breaker exists to survive.
	healthy := diskfulPeersConnected(st, vol, r.NodeName) && st.DiskState == drbd.DiskUpToDate

	switch {
	case coordinator && !snap.Status.IOSuspended && healthy:
		// A failed previous round (a replica never finished before the
		// deadline) retries with backoff instead of churning the barrier.
		if myState == miroirv1alpha1.SnapshotError &&
			snap.Status.SuspendedAt != nil &&
			time.Since(snap.Status.SuspendedAt.Time) < suspendRetryBackoff {
			return ctrl.Result{RequeueAfter: suspendRetryBackoff}, nil
		}
		// One round per volume: the kernel suspend flag is shared, so a
		// second snapshot's round would tear the first's barrier down
		// mid-cut. Wait for the sibling round to close.
		if active, err := r.otherRoundActive(ctx, snap); err != nil || active {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, err
		}
		return r.raiseBarrier(ctx, snap, vol, true)

	case !coordinator && snap.Status.IOSuspended && !expired && healthy &&
		myState != miroirv1alpha1.SnapshotSuspended && myState != miroirv1alpha1.SnapshotDone:
		return r.raiseBarrier(ctx, snap, vol, false)

	case snap.Status.IOSuspended && !expired && healthy &&
		myState == miroirv1alpha1.SnapshotSuspended && allSuspended(vol, snap):
		return r.cutLeg(ctx, snap, vol)

	case coordinator && snap.Status.IOSuspended:
		return r.collectLegs(ctx, snap, vol, expired)

	case snap.Status.IOSuspended && expired && st.Suspended:
		// Dead round, coordinator gone before voiding it: self-expire
		// the local barrier; the void patch stays the coordinator's.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.resumeIO(ctx, vol.Name)

	case !snap.Status.IOSuspended && st.Suspended:
		// The round ended (voided) while the local barrier was still up —
		// unless a sibling snapshot's round owns the barrier by now.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.resumeUnlessSiblingRound(ctx, snap, vol)
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// raiseBarrier is phase one: freeze local writes and record it. The
// coordinator's barrier opens the round (ioSuspended + deadline clock).
func (r *SnapshotReconciler) raiseBarrier(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, vol *miroirv1alpha1.MiroirVolume, opensRound bool) (ctrl.Result, error) {
	// A leg whose pool cannot be resolved could never cut behind the
	// barrier: fail before the cluster-wide freeze, not after. (A pool
	// dropped mid-round — an agent restart with new values — is still
	// bounded by SuspendDeadline via the expiry paths.)
	if _, err := r.backendFor(vol); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.suspendIO(ctx, vol.Name); err != nil {
		return r.barrierFailed(ctx, snap, vol, err)
	}
	r.clearBarrierFails(snap.Name)
	var err error
	if opensRound {
		// The coordinator owns the round: it sets the barrier fields and
		// resets peers, so it applies the whole status.
		now := metav1.Now()
		err = r.patchSnap(ctx, snap, func(s *miroirv1alpha1.MiroirSnapshot) {
			if s.Status.PerNode == nil {
				s.Status.PerNode = map[string]miroirv1alpha1.SnapshotNodeState{}
			}
			s.Status.IOSuspended = true
			s.Status.SuspendedAt = &now
			// Reset peers: a slow peer's Done from the voided round can
			// land after the void and would pair its stale leg with
			// this round's cuts. Only diskful replicas hold legs — a
			// tie-breaker never joins the round (it can never reach
			// Suspended: raising requires an UpToDate disk).
			for _, rep := range vol.Spec.DiskfulReplicas() {
				if rep.Node != r.NodeName {
					s.Status.PerNode[rep.Node] = miroirv1alpha1.SnapshotPending
				}
			}
			s.Status.PerNode[r.NodeName] = miroirv1alpha1.SnapshotSuspended
		})
	} else {
		// A peer records only its own slot. A full-status apply would
		// force-own the coordinator's barrier fields (ioSuspended,
		// suspendedAt) and revert a resume or void it raced.
		err = r.patchOwnState(ctx, snap, miroirv1alpha1.SnapshotSuspended)
	}
	if err != nil {
		// The barrier is only real once recorded; a failed patch must
		// not leave IO frozen until the retry.
		_ = r.resumeIO(ctx, vol.Name)
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// cutLeg is phase two, entered only once every replica's barrier is up:
// no node can accept writes, so legs cut now are byte-identical
// regardless of order.
//
// Errors do not resume the local barrier: peers still read Suspended and
// keep cutting, so dropping it would let writes into some legs and not
// others. The deadline bounds the freeze instead.
func (r *SnapshotReconciler) cutLeg(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, vol *miroirv1alpha1.MiroirVolume) (ctrl.Result, error) {
	be, err := r.backendFor(vol)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Re-assert the local barrier first (idempotent) — a crash or manual
	// resume-io between phases must not let a leg be cut unprotected.
	if err := r.suspendIO(ctx, vol.Name); err != nil {
		return r.barrierFailed(ctx, snap, vol, err)
	}
	r.clearBarrierFails(snap.Name)
	// suspend-io quiesces new writes only; queued writeback must be
	// drained or the snapshot captures stale content.
	if err := be.Sync(ctx, vol.Name); err != nil {
		return ctrl.Result{}, err
	}
	// Delete-then-cut: the backends treat an existing snapshot as
	// success, which would silently keep a leg from a failed round.
	if err := be.DeleteSnapshot(ctx, vol.Name, snap.Name); err != nil {
		return ctrl.Result{}, err
	}
	if err := be.Snapshot(ctx, vol.Name, snap.Name); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, r.patchOwnState(ctx, snap, miroirv1alpha1.SnapshotDone)
}

// collectLegs is the coordinator's last phase: all legs Done → resume and
// publish; deadline passed → resume and void the round.
func (r *SnapshotReconciler) collectLegs(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, vol *miroirv1alpha1.MiroirVolume, expired bool) (ctrl.Result, error) {
	diskful := vol.Spec.DiskfulReplicas()
	done := 0
	for _, rep := range diskful {
		if snap.Status.PerNode[rep.Node] == miroirv1alpha1.SnapshotDone {
			done++
		}
	}
	if done < len(diskful) && !expired {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	// Resume before reporting anything else: a frozen volume is an
	// outage, a late snapshot is just not ready.
	if err := r.resumeIO(ctx, vol.Name); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.patchSnap(ctx, snap, func(s *miroirv1alpha1.MiroirSnapshot) {
		s.Status.IOSuspended = false
		if done == len(diskful) {
			s.Status.SizeBytes = vol.Spec.SizeBytes
			s.Status.SourceFormatted = vol.Status.Formatted
			s.Status.ReadyToUse = true
		} else {
			// Every leg of this round is void, Done ones included: they
			// were cut under a barrier that failed. The retry recuts.
			for _, rep := range vol.Spec.DiskfulReplicas() {
				s.Status.PerNode[rep.Node] = miroirv1alpha1.SnapshotPending
			}
			s.Status.PerNode[r.NodeName] = miroirv1alpha1.SnapshotError
			// Restamp so the retry backoff counts from this failure —
			// from round start it would always read past the deadline.
			now := metav1.Now()
			s.Status.SuspendedAt = &now
		}
	})
}

// allSuspended reports whether the diskful replicas have raised their
// write barrier (Done implies it did); diskless members vote only quorum.
func allSuspended(vol *miroirv1alpha1.MiroirVolume, snap *miroirv1alpha1.MiroirSnapshot) bool {
	for _, rep := range vol.Spec.DiskfulReplicas() {
		st := snap.Status.PerNode[rep.Node]
		if st != miroirv1alpha1.SnapshotSuspended && st != miroirv1alpha1.SnapshotDone {
			return false
		}
	}
	return true
}

// otherRoundActive reports whether a different MiroirSnapshot of the same
// volume is mid-round (its coordinator holds status.ioSuspended). The
// kernel suspend-io flag is per-resource, not per-snapshot: concurrent
// rounds would lift each other's barrier and cut non-identical legs, so
// every barrier touch outside a round defers to a live sibling.
func (r *SnapshotReconciler) otherRoundActive(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot) (bool, error) {
	list := &miroirv1alpha1.MiroirSnapshotList{}
	if err := r.List(ctx, list); err != nil {
		return false, err
	}
	for i := range list.Items {
		s := &list.Items[i]
		if s.Name != snap.Name && s.Spec.VolumeName == snap.Spec.VolumeName && s.Status.IOSuspended {
			return true, nil
		}
	}
	return false, nil
}

// resumeUnlessSiblingRound lifts the local barrier unless a sibling
// snapshot's live round owns it (that round's protocol lifts it).
func (r *SnapshotReconciler) resumeUnlessSiblingRound(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, vol *miroirv1alpha1.MiroirVolume) error {
	active, err := r.otherRoundActive(ctx, snap)
	if err != nil || active {
		return err
	}
	return r.resumeIO(ctx, vol.Name)
}

// isCoordinator: the Primary owns the barrier (suspend-io only blocks
// writes where they originate); with no Primary anywhere, replicas[0]
// does. A Secondary that sees its peer Primary must defer — both sides
// claiming the role is a livelock: the replicas[0] Secondary re-raises a
// barrier the Primary keeps expiring, and the Primary never takes its
// own leg because coordinators don't. A promotion racing this choice can
// still briefly yield two coordinators — recoverable by construction:
// suspend-io is idempotent, the backends treat an existing snapshot as
// success, and both sides resume.
func (r *SnapshotReconciler) isCoordinator(vol *miroirv1alpha1.MiroirVolume, st drbd.Status) bool {
	if st.Primary {
		return true
	}
	if st.PeerPrimary {
		return false
	}
	// No Primary anywhere: the lowest-ordered diskful replica coordinates,
	// but a dead replicas[0] must not orphan the round (no survivor would
	// ever become coordinator, so it never voids and every sibling snapshot
	// queues behind it forever). Walk diskful replicas in spec order and
	// take the first REACHABLE one: self → we coordinate; a connected peer
	// → it does; a down or not-yet-completed peer → skip to the next. A
	// dead node is in no survivor's connected set, so every mutually
	// connected survivor elects the same present node; a partitioned
	// minority may self-elect, but a degraded volume fails the healthy gate
	// and cannot raise a barrier anyway.
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			continue
		}
		if rep.Node == r.NodeName {
			return true
		}
		// A diskful peer earlier in spec order coordinates unless it is
		// provably down — addressed but its connection is not established.
		// An address-less (not-yet-completed) peer is treated as present so
		// coordination stays deterministic during a membership change.
		if rep.Address == "" || st.PeerConnected[rep.NodeID] {
			return false
		}
	}
	return false
}

func replicaOn(vol *miroirv1alpha1.MiroirVolume, node string) bool {
	return slices.ContainsFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == node
	})
}

// disklessOn checks whether the local replica (if any) is diskless — a
// quorum-only tie-breaker that holds no backend data.
func (r *SnapshotReconciler) disklessOn(vol *miroirv1alpha1.MiroirVolume) bool {
	return slices.ContainsFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == r.NodeName && rep.Diskless
	})
}

// patchOwnState records only this node's slot in the snapshot barrier via a
// merge patch. A peer must not apply the whole status: that would force-own
// the coordinator's round fields (ioSuspended, suspendedAt) and could revert
// a resume or void it raced. The merge patch touches nothing else.
func (r *SnapshotReconciler) patchOwnState(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, state miroirv1alpha1.SnapshotNodeState) error {
	patch := fmt.Appendf(nil, `{"status":{"perNode":{%q:%q}}}`, r.NodeName, state)
	return r.Status().Patch(ctx, snap, client.RawPatch(types.MergePatchType, patch))
}

func (r *SnapshotReconciler) patchSnap(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot, mutate func(*miroirv1alpha1.MiroirSnapshot)) error {
	mutate(snap)
	// Apply name + status only. The previous full-object apply also
	// serialized the live metadata, force-co-owning whatever labels and
	// finalizers this agent had last read — the stale-metadata hazard the
	// volume patchStatus was built to avoid.
	return r.SubResource("status").Apply(ctx,
		acv1alpha1.MiroirSnapshot(snap.Name).WithStatus(snapshotStatusAC(snap.Status)),
		client.FieldOwner("agent-snapshot-"+r.NodeName),
		client.ForceOwnership)
}

// snapshotStatusAC mirrors MiroirSnapshotStatus's wire shape: readyToUse
// and ioSuspended have no omitempty (false is a statement — ioSuspended
// false IS the barrier release), the rest only when non-zero.
func snapshotStatusAC(st miroirv1alpha1.MiroirSnapshotStatus) *acv1alpha1.MiroirSnapshotStatusApplyConfiguration {
	ac := acv1alpha1.MiroirSnapshotStatus().
		WithReadyToUse(st.ReadyToUse).
		WithIOSuspended(st.IOSuspended)
	if len(st.PerNode) > 0 {
		ac = ac.WithPerNode(st.PerNode)
	}
	if st.SizeBytes != 0 {
		ac = ac.WithSizeBytes(st.SizeBytes)
	}
	if st.SourceFormatted {
		ac = ac.WithSourceFormatted(true)
	}
	if st.SuspendedAt != nil {
		ac = ac.WithSuspendedAt(*st.SuspendedAt)
	}
	for _, c := range st.Conditions {
		ac = ac.WithConditions(metav1ac.Condition().
			WithType(c.Type).
			WithStatus(c.Status).
			WithObservedGeneration(c.ObservedGeneration).
			WithLastTransitionTime(c.LastTransitionTime).
			WithReason(c.Reason).
			WithMessage(c.Message))
	}
	return ac
}

func (r *SnapshotReconciler) removeFinalizer(ctx context.Context, snap *miroirv1alpha1.MiroirSnapshot) error {
	return removeNodeFinalizer(ctx, r.Client, snap, r.NodeName)
}

// SetupWithManager registers the reconciler.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirSnapshot{}).
		Named("agent-snapshot").
		Complete(r)
}
