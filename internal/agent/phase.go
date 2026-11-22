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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// PhaseReconciler is the only component that gives a volume's Phase and
// ReadyReplicas a value, deriving both from the agents' PerNode slots: an
// agent aggregating them itself would publish a verdict on its peers from
// its own informer cache, and the last of several near-simultaneous writes
// need not be the freshest. An agent can only withdraw them (see
// VolumeReconciler.patchStatus). It runs in the central controller, so probe
// timestamps age the phase even when every agent responsible for a volume
// is down.
type PhaseReconciler struct {
	client.Client
}

func (r *PhaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	now := time.Now()
	phase, readyReplicas := computePhase(vol, now)
	// Every diskful replica reporting a fresh probe contradicts a lingering
	// NodeUnreachable condition. waitReady's success-path clear is one-shot —
	// provisioning succeeded, so no further CreateVolume will retry a failed
	// patch — so this reconciler is the backstop that eventually clears it.
	// Gate on reachability, not phase: recovery can settle at Degraded (a
	// replica still syncing provisions fine) and must still clear.
	clearUnreachable := replicasReachable(vol, now) &&
		meta.IsStatusConditionTrue(vol.Status.Conditions, miroirv1alpha1.ConditionNodeUnreachable)
	if phase != vol.Status.Phase || readyReplicas != vol.Status.ReadyReplicas || clearUnreachable {
		base := vol.DeepCopy()
		vol.Status.Phase = phase
		vol.Status.ReadyReplicas = readyReplicas
		if clearUnreachable {
			meta.SetStatusCondition(&vol.Status.Conditions, metav1.Condition{
				Type:   miroirv1alpha1.ConditionNodeUnreachable,
				Status: metav1.ConditionFalse,
				Reason: "NodeReachable",
			})
		}
		if err := r.Status().Patch(ctx, vol, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: nextProbeExpiry(vol, now)}, nil
}

// replicasReachable reports whether every diskful replica has a PerNode
// entry with a fresh (or legacy-nil) probe — the inverse of the criteria
// the CSI controller brands nodes unreachable by, so the clear cannot
// fight a concurrent set: an absent entry or a stale probe blocks it.
func replicasReachable(vol *miroirv1alpha1.MiroirVolume, now time.Time) bool {
	for _, rep := range vol.Spec.DiskfulReplicas() {
		st, ok := vol.Status.PerNode[rep.Node]
		if !ok {
			return false
		}
		if st.LastProbedAt != nil && now.Sub(st.LastProbedAt.Time) > StaleProbeThreshold {
			return false
		}
	}
	return true
}

// nextProbeExpiry returns the nearest future point at which computePhase can
// change without another Kubernetes event.
func nextProbeExpiry(vol *miroirv1alpha1.MiroirVolume, now time.Time) time.Duration {
	var next time.Duration
	for _, rep := range vol.Spec.DiskfulReplicas() {
		probed := vol.Status.PerNode[rep.Node].LastProbedAt
		if probed == nil {
			continue
		}
		remaining := probed.Add(StaleProbeThreshold).Sub(now)
		if remaining > 0 && (next == 0 || remaining < next) {
			next = remaining
		}
	}
	return next
}

func (r *PhaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{}).
		Named("volume-phase").
		Complete(r)
}

// legRealized reports whether a diskful leg's slot shows a backing device at
// the spec size holding current data. An absent slot is the zero value and
// reads unrealized.
func legRealized(vol *miroirv1alpha1.MiroirVolume, st miroirv1alpha1.ReplicaStatus) bool {
	return st.DeviceCreated && st.SizeBytes >= vol.Spec.SizeBytes &&
		(vol.Spec.DRBD == nil || st.DiskState == drbd.DiskUpToDate)
}

// legServing reports whether a realized leg is also operational: connected
// to its diskful peers when replicated. Probe age is left out; only the
// controller's clock judges it.
func legServing(vol *miroirv1alpha1.MiroirVolume, st miroirv1alpha1.ReplicaStatus) bool {
	return legRealized(vol, st) && (vol.Spec.DRBD == nil || st.Connected)
}

// computePhase aggregates per-node states into the volume phase the CSI
// controller waits on, plus the "ready/total" diskful summary backing the
// Replicas printcolumn.
func computePhase(vol *miroirv1alpha1.MiroirVolume, now time.Time) (miroirv1alpha1.VolumePhase, string) {
	diskfulReplicas := vol.Spec.DiskfulReplicas()
	ready := 0
	realized := 0
	failed := false
	upToDate := false
	for _, rep := range diskfulReplicas {
		st, ok := vol.Status.PerNode[rep.Node]
		// Tracked apart from realized: a leg awaiting an online grow is
		// below spec size yet still holds current data.
		upToDate = upToDate || (ok && st.DiskState == drbd.DiskUpToDate)
		switch {
		case legRealized(vol, st):
			// Keep current data separate from operational readiness so an
			// established but disconnected volume is Degraded, not Creating.
			realized++
			if legServing(vol, st) {
				// Staleness gate: a replica whose LastProbedAt is set and
				// exceeds StaleProbeThreshold means the agent can no longer
				// reach the node-local resources. Treat it as realized but
				// NOT ready — even if Connected=true — so the phase is
				// Degraded, not Ready. Nil LastProbedAt (legacy volume) uses
				// backward-compatible behavior: trust the persisted fields.
				if st.LastProbedAt == nil || now.Sub(st.LastProbedAt.Time) <= StaleProbeThreshold {
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
	case vol.Spec.DRBD != nil && vol.Status.Activated && !upToDate:
		// Activated, not Formatted: a restore inherits Formatted while
		// its legs still sync, and Failed aborts provisioning.
		return miroirv1alpha1.VolumeFailed, readyReplicas
	default:
		return miroirv1alpha1.VolumeCreating, readyReplicas
	}
}
