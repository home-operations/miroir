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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/backend"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/drbd"
)

// SuspendDeadline bounds the snapshot write barrier: a peer that never
// snapshots must not freeze the volume forever. Exported so the agent's
// startup sweep can apply the same deadline when lifting barriers left
// raised by a previous crash.
const SuspendDeadline = 60 * time.Second

// suspendRetryBackoff spaces barrier retries after a failed round.
const suspendRetryBackoff = 30 * time.Second

// SnapshotReconciler realizes HomefsSnapshots on this node.
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
	Backend  backend.Backend
	DRBD     *drbd.Driver
}

// Reconcile drives one snapshot's state machine from this node's view.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	snap := &homefsv1alpha1.HomefsSnapshot{}
	if err := r.Get(ctx, req.NamespacedName, snap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	vol := &homefsv1alpha1.HomefsVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: snap.Spec.VolumeName}, vol); err != nil {
		if apierrors.IsNotFound(err) && !snap.DeletionTimestamp.IsZero() {
			// Source volume already gone; nothing local can remain.
			return ctrl.Result{}, r.removeFinalizer(ctx, snap)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	mine := replicaOn(vol, r.NodeName)
	if !mine {
		return ctrl.Result{}, nil
	}

	if !snap.DeletionTimestamp.IsZero() {
		// A snapshot deleted mid-round must not strand its barrier:
		// resume before anything that can fail or requeue. Every replica
		// passes here and resume-io is a no-op when not suspended.
		if snap.Status.IOSuspended && vol.Spec.DRBD != nil {
			if err := r.DRBD.ResumeIO(ctx, vol.Name); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := r.Backend.DeleteSnapshot(ctx, vol.Name, snap.Name); err != nil {
			if isDeviceBusy(err) {
				// ZFS refuses to destroy an origin with live clones;
				// retry until restored volumes are gone.
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.removeFinalizer(ctx, snap)
	}

	if snap.Status.ReadyToUse {
		// A peer's barrier can outlive the round by a moment; lift it.
		// Best-effort: if DRBD cannot report, nothing holds a barrier.
		if vol.Spec.DRBD != nil {
			if st, err := r.DRBD.Status(ctx, vol.Name); err == nil && st.Suspended {
				return ctrl.Result{}, r.DRBD.ResumeIO(ctx, vol.Name)
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
		// Single replica: no barrier needed, but queued writes must land.
		if err := r.Backend.Sync(ctx, vol.Name); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Backend.Snapshot(ctx, vol.Name, snap.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchSnap(ctx, snap, func(s *homefsv1alpha1.HomefsSnapshot) {
			s.Status.PerNode = map[string]homefsv1alpha1.SnapshotNodeState{
				r.NodeName: homefsv1alpha1.SnapshotDone,
			}
			s.Status.SizeBytes = vol.Spec.SizeBytes
			s.Status.ReadyToUse = true
		})
	}
	return r.reconcileReplicated(ctx, snap, vol)
}

func (r *SnapshotReconciler) reconcileReplicated(ctx context.Context, snap *homefsv1alpha1.HomefsSnapshot, vol *homefsv1alpha1.HomefsVolume) (ctrl.Result, error) {
	st, err := r.DRBD.Status(ctx, vol.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	coordinator := r.isCoordinator(vol, st)
	myState := snap.Status.PerNode[r.NodeName]
	expired := snap.Status.SuspendedAt != nil &&
		time.Since(snap.Status.SuspendedAt.Time) > SuspendDeadline
	// Disconnected or resyncing legs have diverged (quorum off lets the
	// survivor write alone) and a barrier over diverged legs cuts
	// diverged legs. Gates raising and cutting only — a degraded volume
	// must still resume.
	healthy := st.Connected && st.DiskState == drbd.DiskUpToDate

	switch {
	case coordinator && !snap.Status.IOSuspended && healthy:
		// A failed previous round (a replica never finished before the
		// deadline) retries with backoff instead of churning the barrier.
		if myState == homefsv1alpha1.SnapshotError &&
			snap.Status.SuspendedAt != nil &&
			time.Since(snap.Status.SuspendedAt.Time) < suspendRetryBackoff {
			return ctrl.Result{RequeueAfter: suspendRetryBackoff}, nil
		}
		return r.raiseBarrier(ctx, snap, vol, true)

	case !coordinator && snap.Status.IOSuspended && !expired && healthy &&
		myState != homefsv1alpha1.SnapshotSuspended && myState != homefsv1alpha1.SnapshotDone:
		return r.raiseBarrier(ctx, snap, vol, false)

	case snap.Status.IOSuspended && !expired && healthy &&
		myState == homefsv1alpha1.SnapshotSuspended && allSuspended(vol, snap):
		return r.cutLeg(ctx, snap, vol)

	case coordinator && snap.Status.IOSuspended:
		return r.collectLegs(ctx, snap, vol, expired)

	case snap.Status.IOSuspended && expired && st.Suspended:
		// Dead round, coordinator gone before voiding it: self-expire
		// the local barrier; the void patch stays the coordinator's.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.DRBD.ResumeIO(ctx, vol.Name)

	case !snap.Status.IOSuspended && st.Suspended:
		// The round ended (voided) while the local barrier was still up.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.DRBD.ResumeIO(ctx, vol.Name)
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// raiseBarrier is phase one: freeze local writes and record it. The
// coordinator's barrier opens the round (ioSuspended + deadline clock).
func (r *SnapshotReconciler) raiseBarrier(ctx context.Context, snap *homefsv1alpha1.HomefsSnapshot, vol *homefsv1alpha1.HomefsVolume, opensRound bool) (ctrl.Result, error) {
	if err := r.DRBD.SuspendIO(ctx, vol.Name); err != nil {
		return ctrl.Result{}, err
	}
	now := metav1.Now()
	err := r.patchSnap(ctx, snap, func(s *homefsv1alpha1.HomefsSnapshot) {
		if s.Status.PerNode == nil {
			s.Status.PerNode = map[string]homefsv1alpha1.SnapshotNodeState{}
		}
		if opensRound {
			s.Status.IOSuspended = true
			s.Status.SuspendedAt = &now
			// Reset peers: a slow peer's Done from the voided round can
			// land after the void and would pair its stale leg with
			// this round's cuts.
			for _, rep := range vol.Spec.Replicas {
				if rep.Node != r.NodeName {
					s.Status.PerNode[rep.Node] = homefsv1alpha1.SnapshotPending
				}
			}
		}
		s.Status.PerNode[r.NodeName] = homefsv1alpha1.SnapshotSuspended
	})
	if err != nil {
		// The barrier is only real once recorded; a failed patch must
		// not leave IO frozen until the retry.
		_ = r.DRBD.ResumeIO(ctx, vol.Name)
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
func (r *SnapshotReconciler) cutLeg(ctx context.Context, snap *homefsv1alpha1.HomefsSnapshot, vol *homefsv1alpha1.HomefsVolume) (ctrl.Result, error) {
	// Re-assert the local barrier first (idempotent) — a crash or manual
	// resume-io between phases must not let a leg be cut unprotected.
	if err := r.DRBD.SuspendIO(ctx, vol.Name); err != nil {
		return ctrl.Result{}, err
	}
	// suspend-io quiesces new writes only; queued writeback must be
	// drained or the snapshot captures stale content.
	if err := r.Backend.Sync(ctx, vol.Name); err != nil {
		return ctrl.Result{}, err
	}
	// Delete-then-cut: the backends treat an existing snapshot as
	// success, which would silently keep a leg from a failed round.
	if err := r.Backend.DeleteSnapshot(ctx, vol.Name, snap.Name); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Backend.Snapshot(ctx, vol.Name, snap.Name); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, r.patchSnap(ctx, snap, func(s *homefsv1alpha1.HomefsSnapshot) {
		s.Status.PerNode[r.NodeName] = homefsv1alpha1.SnapshotDone
	})
}

// collectLegs is the coordinator's last phase: all legs Done → resume and
// publish; deadline passed → resume and void the round.
func (r *SnapshotReconciler) collectLegs(ctx context.Context, snap *homefsv1alpha1.HomefsSnapshot, vol *homefsv1alpha1.HomefsVolume, expired bool) (ctrl.Result, error) {
	done := 0
	for _, rep := range vol.Spec.Replicas {
		if snap.Status.PerNode[rep.Node] == homefsv1alpha1.SnapshotDone {
			done++
		}
	}
	if done < len(vol.Spec.Replicas) && !expired {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	// Resume before reporting anything else: a frozen volume is an
	// outage, a late snapshot is just not ready.
	if err := r.DRBD.ResumeIO(ctx, vol.Name); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.patchSnap(ctx, snap, func(s *homefsv1alpha1.HomefsSnapshot) {
		s.Status.IOSuspended = false
		if done == len(vol.Spec.Replicas) {
			s.Status.SizeBytes = vol.Spec.SizeBytes
			s.Status.ReadyToUse = true
		} else {
			// Every leg of this round is void, Done ones included: they
			// were cut under a barrier that failed. The retry recuts.
			for _, rep := range vol.Spec.Replicas {
				s.Status.PerNode[rep.Node] = homefsv1alpha1.SnapshotPending
			}
			s.Status.PerNode[r.NodeName] = homefsv1alpha1.SnapshotError
			// Restamp so the retry backoff counts from this failure —
			// from round start it would always read past the deadline.
			now := metav1.Now()
			s.Status.SuspendedAt = &now
		}
	})
}

// allSuspended reports whether every replica has raised its write
// barrier (Done implies it did).
func allSuspended(vol *homefsv1alpha1.HomefsVolume, snap *homefsv1alpha1.HomefsSnapshot) bool {
	for _, rep := range vol.Spec.Replicas {
		st := snap.Status.PerNode[rep.Node]
		if st != homefsv1alpha1.SnapshotSuspended && st != homefsv1alpha1.SnapshotDone {
			return false
		}
	}
	return true
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
func (r *SnapshotReconciler) isCoordinator(vol *homefsv1alpha1.HomefsVolume, st drbd.Status) bool {
	if st.Primary {
		return true
	}
	if st.PeerPrimary {
		return false
	}
	return len(vol.Spec.Replicas) > 0 && vol.Spec.Replicas[0].Node == r.NodeName
}

func replicaOn(vol *homefsv1alpha1.HomefsVolume, node string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == node {
			return true
		}
	}
	return false
}

func (r *SnapshotReconciler) patchSnap(ctx context.Context, snap *homefsv1alpha1.HomefsSnapshot, mutate func(*homefsv1alpha1.HomefsSnapshot)) error {
	mutate(snap)
	snap.SetGroupVersionKind(homefsv1alpha1.GroupVersion.WithKind("HomefsSnapshot"))
	snap.ManagedFields = nil
	return r.Status().Patch(ctx, snap, client.Apply, //nolint:staticcheck
		client.FieldOwner("agent-snapshot-"+r.NodeName),
		client.ForceOwnership)
}

func (r *SnapshotReconciler) removeFinalizer(ctx context.Context, snap *homefsv1alpha1.HomefsSnapshot) error {
	finalizer := constants.FinalizerPrefix + r.NodeName
	if !controllerutil.ContainsFinalizer(snap, finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(snap, finalizer)
	if err := r.Update(ctx, snap); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// SetupWithManager registers the reconciler.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&homefsv1alpha1.HomefsSnapshot{}).
		Named("agent-snapshot").
		Complete(r)
}
