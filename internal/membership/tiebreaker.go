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

package membership

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/nodemap"
)

// TieBreakerReconciler retrofits a diskless tie-breaker onto 2-replica
// freeze volumes that lack one (#70) — volumes created before a spare
// node existed, or switched to freeze later. It appends a bare
// {node, diskless} entry; the membership Reconciler completes it exactly
// like an operator-added replica. The node map is fixed per controller
// process (a node-map change is a Helm upgrade, hence a restart), so the
// startup reconcile pass covers node additions — no Node watch needed.
type TieBreakerReconciler struct {
	client.Client
	// Nodes is the storage topology — the same map CreateVolume places from.
	Nodes nodemap.Map
}

// Reconcile appends a tie-breaker entry when the volume is a 2-replica
// freeze volume, its entries are complete, and a spare node exists.
func (r *TieBreakerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() || vol.Spec.DRBD == nil ||
		vol.Spec.QuorumPolicy != miroirv1alpha1.QuorumFreeze {
		return ctrl.Result{}, nil
	}
	if len(vol.Spec.Replicas) != 2 || len(vol.Spec.DiskfulReplicas()) != 2 {
		// Already has a tie-breaker, or a membership edit is in flight.
		return ctrl.Result{}, nil
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			// Incomplete entry: membership's completion edit re-triggers us.
			return ctrl.Result{}, nil
		}
	}
	tb := r.Nodes.TieBreakerNode(vol.Spec.Replicas)
	if tb == "" {
		// No spare node; the startup pass after the next node-map change
		// (Helm upgrade → restart) revisits this volume.
		return ctrl.Result{}, nil
	}

	vol.Spec.Replicas = append(vol.Spec.Replicas, miroirv1alpha1.Replica{Node: tb, Diskless: true})
	if err := r.Update(ctx, vol); err != nil {
		return ctrl.Result{}, err
	}
	ctrl.LoggerFrom(ctx).Info("added diskless tie-breaker", "volume", vol.Name, "node", tb)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler. Generation-filtered like the
// membership Reconciler; the initial list delivers every volume once at
// startup, which is the retrofit pass.
func (r *TieBreakerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("tiebreaker").
		Complete(r)
}
