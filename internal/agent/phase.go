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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// PhaseReconciler runs in the central controller so probe timestamps can age
// the aggregate phase even when every agent responsible for a volume is down.
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
	phase, readyReplicas := computePhaseAt(vol, now)
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
