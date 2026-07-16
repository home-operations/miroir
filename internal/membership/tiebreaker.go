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
	"slices"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

// TieBreakerReconciler retrofits a diskless tie-breaker onto 2-replica
// freeze volumes that lack one (#70) — volumes created before a spare
// node existed, or switched to freeze later. It appends a bare
// {node, diskless} entry; the membership Reconciler completes it exactly
// like an operator-added replica. The MiroirNode watch re-runs the
// retrofit when a spare node joins the topology.
type TieBreakerReconciler struct {
	client.Client
	// Nodes yields the storage topology — the same map CreateVolume
	// places from — folded from the MiroirNode CRs per reconcile.
	Nodes nodemap.Source
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
	for _, f := range vol.Finalizers {
		node, ok := strings.CutPrefix(f, constants.FinalizerPrefix)
		if !ok {
			continue
		}
		if !hasLegOn(vol, node) {
			// A removed leg's teardown is still in flight: its agent
			// holds the finalizer until the leg is safely gone. Picking
			// that node now would cancel the teardown, leaking its backing
			// device and stale DRBD metadata that a later diskful re-add
			// would adopt. Releasing the finalizer does not bump the
			// generation, so poll instead of waiting on a watch event.
			// Client legs hold finalizers too — theirs are live legs, not
			// removals in flight.
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}
	nodes, err := r.Nodes.Map(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Client-leg nodes count as used: a replica may not share a node with
	// a client leg (CEL rule), so picking one would only bounce off the
	// apiserver forever.
	legs := slices.Clone(vol.Spec.Replicas)
	for _, cl := range vol.Spec.Clients {
		legs = append(legs, miroirv1alpha1.Replica{Node: cl.Node})
	}
	tb := nodes.TieBreakerNode(legs)
	if tb == "" {
		// No spare node; the MiroirNode watch revisits this volume when
		// one joins the topology.
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
// startup, which is the retrofit pass, and the MiroirNode watch repeats
// it when the topology gains a node.
func (r *TieBreakerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&miroirv1alpha1.MiroirNode{}, enqueueAllVolumes(mgr.GetClient()),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("tiebreaker").
		Complete(r)
}
