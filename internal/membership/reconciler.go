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
// operator adds a replica by appending {node} (plus a pool when not the
// default) to spec.replicas (kubectl edit); this reconciler completes the
// entry — backend, DRBD node id, replication address, teardown finalizer,
// FullSync marker — after which
// the node's agent realizes it and DRBD full-syncs the new leg. Removal
// needs no spec-side work: the removed node's agent notices its held
// finalizer and tears down once the remaining replicas are safe.
package membership

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
			if errors.Is(err, errBadPlacement) {
				// Unknown node or duplicate: no Node update or poll
				// re-triggers this generation-filtered controller, but only
				// a spec edit could fix it anyway, so stop without requeue.
				log.Error(err, "cannot complete added replica",
					"volume", vol.Name, "node", rep.Node)
				return ctrl.Result{}, nil
			}
			// Transient (Node not registered yet, InternalIP not posted):
			// requeue with backoff — nothing else wakes this controller.
			return ctrl.Result{}, err
		}
		changed = true
	}
	for i := range vol.Spec.Clients {
		cl := &vol.Spec.Clients[i]
		if cl.Address != "" {
			continue
		}
		if err := r.completeClient(ctx, vol, cl); err != nil {
			if errors.Is(err, errBadPlacement) {
				log.Error(err, "cannot complete client leg",
					"volume", vol.Name, "node", cl.Node)
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		changed = true
	}
	for _, rep := range vol.Spec.Replicas {
		if controllerutil.AddFinalizer(vol, constants.FinalizerPrefix+rep.Node) {
			changed = true
		}
	}
	for _, cl := range vol.Spec.Clients {
		if controllerutil.AddFinalizer(vol, constants.FinalizerPrefix+cl.Node) {
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

// errBadPlacement marks a completion failure that only a spec edit can fix
// (unknown node, duplicate), as opposed to a transient one (Node not
// registered yet) that must be retried.
var errBadPlacement = errors.New("replica placement is invalid")

// complete fills one added entry in place. NodeID takes the lowest id not
// used by the other entries — freed ids may be reused; the joiner's
// just-created metadata forces a full sync either way, so a stale bitmap
// slot on the peers cannot leak as data.
func (r *Reconciler) complete(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, rep *miroirv1alpha1.Replica) error {
	if _, ok := r.Nodes[rep.Node]; !ok {
		return fmt.Errorf("%w: node %s is not in the storage node map", errBadPlacement, rep.Node)
	}
	// A volume's diskful legs all live in one pool: snapshots and restores
	// are pool-local, so a cross-pool leg would make every snapshot of the
	// volume unrestorable (the CRD's uniformity rule also rejects it once
	// completed — refuse here with the reason instead of conflicting
	// forever). An entry naming no pool inherits the volume's.
	targetPool := volumePool(vol)
	if !rep.Diskless {
		if rep.Pool != "" && nodemap.PoolOrDefault(rep.Pool) != targetPool {
			return fmt.Errorf("%w: the volume's replicas live in pool %q; a replica in pool %q would make its snapshots unrestorable (restores are pool-local)",
				errBadPlacement, targetPool, nodemap.PoolOrDefault(rep.Pool))
		}
		if _, ok := r.Nodes.Pool(rep.Node, targetPool); !ok {
			return fmt.Errorf("%w: node %s has no storage pool %q",
				errBadPlacement, rep.Node, targetPool)
		}
	}
	dup := 0
	for _, other := range vol.Spec.Replicas {
		if other.Node == rep.Node {
			dup++
		}
	}
	if dup > 1 {
		return fmt.Errorf("%w: node %s appears %d times in spec.replicas", errBadPlacement, rep.Node, dup)
	}
	addr, err := r.Nodes.ReplicationAddress(ctx, r.Client, rep.Node)
	if err != nil {
		return err
	}
	id := nextNodeID(vol, rep.Node)

	if rep.Diskless {
		// Quorum-only entry: a backend or pool is meaningless, and the
		// node map (not the operator's edit) decides backends — clear any
		// typo.
		rep.Backend = ""
		rep.Pool = ""
	} else {
		pool, _ := r.Nodes.Pool(rep.Node, targetPool)
		rep.Backend = pool.Backend
		rep.Pool = targetPool
		rep.FullSync = true
	}
	rep.NodeID = id
	rep.Address = addr
	return nil
}

// completeClient fills one client leg in place. Unlike a replica it needs
// no node-map entry — any node running an agent can consume remotely, and
// its address resolves like a replica's (map override, else InternalIP).
func (r *Reconciler) completeClient(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cl *miroirv1alpha1.VolumeClient) error {
	dup := 0
	for _, other := range vol.Spec.Clients {
		if other.Node == cl.Node {
			dup++
		}
	}
	if dup > 1 {
		return fmt.Errorf("%w: node %s appears %d times in spec.clients", errBadPlacement, cl.Node, dup)
	}
	addr, err := r.Nodes.ReplicationAddress(ctx, r.Client, cl.Node)
	if err != nil {
		return err
	}
	cl.NodeID = nextNodeID(vol, cl.Node)
	cl.Address = addr
	return nil
}

// nextNodeID returns the lowest DRBD node id unused by any completed
// replica or client leg other than self. Freed ids may be reused; a
// joiner's just-created metadata forces a full sync either way.
func nextNodeID(vol *miroirv1alpha1.MiroirVolume, self string) int32 {
	used := func(id int32) bool {
		return slices.ContainsFunc(vol.Spec.Replicas, func(other miroirv1alpha1.Replica) bool {
			return other.Node != self && other.Address != "" && other.NodeID == id
		}) || slices.ContainsFunc(vol.Spec.Clients, func(other miroirv1alpha1.VolumeClient) bool {
			return other.Node != self && other.Address != "" && other.NodeID == id
		})
	}
	id := int32(0)
	for used(id) {
		id++
	}
	return id
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
