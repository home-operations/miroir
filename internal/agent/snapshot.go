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

// suspendDeadline bounds the snapshot write barrier: a peer that never
// snapshots must not freeze the volume forever.
const suspendDeadline = 60 * time.Second

// suspendRetryBackoff spaces barrier retries after a failed round.
const suspendRetryBackoff = 30 * time.Second

// SnapshotReconciler realizes HomefsSnapshots on this node.
//
// Replicated volumes need byte-identical snapshots on both legs (restore
// clones each leg locally and skips the resync), so writes are frozen
// for the duration:
//
//	coordinator: suspend-io → patch ioSuspended → own snapshot → Done
//	peer:        sees ioSuspended + coordinator Done → snapshot → Done
//	coordinator: sees all Done (or deadline) → resume-io → readyToUse
//
// The coordinator is the Primary when one exists (suspend-io only blocks
// writes where they originate), else replicas[0].
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
		return ctrl.Result{}, nil
	}

	if vol.Spec.DRBD == nil {
		// Single replica: no barrier needed.
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

	switch {
	case coordinator && !snap.Status.IOSuspended:
		// A failed previous round (peer never snapshotted before the
		// deadline) retries with backoff instead of churning the barrier.
		if myState == homefsv1alpha1.SnapshotError &&
			snap.Status.SuspendedAt != nil &&
			time.Since(snap.Status.SuspendedAt.Time) < suspendRetryBackoff {
			return ctrl.Result{RequeueAfter: suspendRetryBackoff}, nil
		}
		if err := r.DRBD.SuspendIO(ctx, vol.Name); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Backend.Snapshot(ctx, vol.Name, snap.Name); err != nil {
			// Never leave the volume frozen.
			_ = r.DRBD.ResumeIO(ctx, vol.Name)
			return ctrl.Result{}, err
		}
		now := metav1.Now()
		err := r.patchSnap(ctx, snap, func(s *homefsv1alpha1.HomefsSnapshot) {
			s.Status.IOSuspended = true
			s.Status.SuspendedAt = &now
			if s.Status.PerNode == nil {
				s.Status.PerNode = map[string]homefsv1alpha1.SnapshotNodeState{}
			}
			s.Status.PerNode[r.NodeName] = homefsv1alpha1.SnapshotDone
		})
		if err != nil {
			// The barrier is only real once recorded; a failed patch must
			// not leave IO frozen until the retry.
			_ = r.DRBD.ResumeIO(ctx, vol.Name)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil

	case !coordinator && snap.Status.IOSuspended && myState != homefsv1alpha1.SnapshotDone:
		if err := r.Backend.Snapshot(ctx, vol.Name, snap.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.patchSnap(ctx, snap, func(s *homefsv1alpha1.HomefsSnapshot) {
			if s.Status.PerNode == nil {
				s.Status.PerNode = map[string]homefsv1alpha1.SnapshotNodeState{}
			}
			s.Status.PerNode[r.NodeName] = homefsv1alpha1.SnapshotDone
		})

	case coordinator && snap.Status.IOSuspended:
		done := 0
		for _, rep := range vol.Spec.Replicas {
			if snap.Status.PerNode[rep.Node] == homefsv1alpha1.SnapshotDone {
				done++
			}
		}
		expired := snap.Status.SuspendedAt != nil &&
			time.Since(snap.Status.SuspendedAt.Time) > suspendDeadline
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
				s.Status.PerNode[r.NodeName] = homefsv1alpha1.SnapshotError
			}
		})
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// isCoordinator: the Primary owns the barrier (suspend-io only blocks
// writes where they originate); with no local Primary, replicas[0] does.
// A promotion racing this choice can briefly yield two coordinators —
// recoverable by construction: suspend-io is idempotent, the backends
// treat an existing snapshot as success, and both sides resume.
func (r *SnapshotReconciler) isCoordinator(vol *homefsv1alpha1.HomefsVolume, st drbd.Status) bool {
	if st.Primary {
		return true
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
	base := snap.DeepCopy()
	mutate(snap)
	return r.Status().Patch(ctx, snap,
		client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
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
