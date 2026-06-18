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

// Package membership reconciles replica-set edits on live volumes. An
// operator adds a replica by appending {node, backend} to spec.replicas
// (kubectl edit); this reconciler completes the entry — DRBD node id,
// replication address, teardown finalizer, FullSync marker — after which
// the node's agent realizes it and DRBD full-syncs the new leg. Removal
// needs no spec-side work: the removed node's agent notices its held
// finalizer and tears down once the remaining replicas are safe.
package membership

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

// Reconciler completes operator-added replica entries on replicated
// volumes. It runs in the controller pod: completion needs the node map
// and Node objects, which agents do not have cluster-wide.
type Reconciler struct {
	client.Client
	// Nodes is the storage topology — the same map CreateVolume places from.
	Nodes nodemap.Map
}

// Reconcile fills in NodeID/Address/FullSync for incomplete replica
// entries and ensures every placed node holds its teardown finalizer.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() || vol.Spec.DRBD == nil {
		// Unreplicated volumes cannot change membership: DRBD internal
		// metadata cannot be slipped under a live filesystem (the CRD
		// validation rule already blocks such edits).
		return ctrl.Result{}, nil
	}

	changed := false
	for i := range vol.Spec.Replicas {
		rep := &vol.Spec.Replicas[i]
		if rep.Address != "" {
			continue
		}
		if err := r.complete(ctx, vol, rep); err != nil {
			// Misplacement (unknown node, duplicate) only resolves by
			// another spec edit — surface it and stop without requeueing.
			log.Error(err, "cannot complete added replica",
				"volume", vol.Name, "node", rep.Node)
			return ctrl.Result{}, nil
		}
		changed = true
	}
	for _, rep := range vol.Spec.Replicas {
		if controllerutil.AddFinalizer(vol, constants.FinalizerPrefix+rep.Node) {
			changed = true
		}
	}
	if !changed {
		return ctrl.Result{}, nil
	}
	if err := r.Update(ctx, vol); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("completed replica membership", "volume", vol.Name)
	return ctrl.Result{}, nil
}

// complete fills one added entry in place. NodeID takes the lowest id not
// used by the other entries — freed ids may be reused; the joiner's
// just-created metadata forces a full sync either way, so a stale bitmap
// slot on the peers cannot leak as data.
func (r *Reconciler) complete(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, rep *miroirv1alpha1.Replica) error {
	entry, ok := r.Nodes[rep.Node]
	if !ok {
		return fmt.Errorf("node %s is not in the storage node map", rep.Node)
	}
	dup := 0
	for _, other := range vol.Spec.Replicas {
		if other.Node == rep.Node {
			dup++
		}
	}
	if dup > 1 {
		return fmt.Errorf("node %s appears %d times in spec.replicas", rep.Node, dup)
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: rep.Node}, node); err != nil {
		return fmt.Errorf("get node %s: %w", rep.Node, err)
	}
	addr := ""
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			addr = a.Address
			break
		}
	}
	if addr == "" {
		return fmt.Errorf("node %s has no InternalIP", rep.Node)
	}

	id := int32(0)
	for slices.ContainsFunc(vol.Spec.Replicas, func(other miroirv1alpha1.Replica) bool {
		return other.Node != rep.Node && other.Address != "" && other.NodeID == id
	}) {
		id++
	}

	rep.Backend = entry.Backend
	rep.NodeID = id
	rep.Address = addr
	rep.FullSync = true
	return nil
}

// SetupWithManager registers the reconciler. Generation-filtered: status
// patches from agents arrive every poll interval and carry nothing this
// reconciler reads.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("membership").
		Complete(r)
}
