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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

// AutoDiskfulReconciler converts a long-lived diskless leg into a diskful
// replica on its node (LINSTOR's auto-diskful): a consumer that has stayed
// put past the threshold evidently lives there, so give it a local replica
// and stop paying network I/O for every read and write. The node's agent
// attaches a backing device to the live resource and DRBD resyncs it
// online — the pod keeps running throughout.
type AutoDiskfulReconciler struct {
	client.Client
	// Nodes is the storage topology; only nodes in it can become diskful.
	Nodes nodemap.Map
	// After is the conversion threshold; the setup path guards > 0.
	After time.Duration
	// Recorder emits the AutoDiskful event on conversion; optional.
	Recorder events.EventRecorder
}

// Reconcile converts at most one leg per pass — a client leg (replaced by
// a replica entry) or a tie-breaker leg a consumer stages through (flipped
// diskless→diskful in place); the spec update re-triggers this controller
// for anything remaining.
func (r *AutoDiskfulReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() || vol.Spec.DRBD == nil {
		return ctrl.Result{}, nil
	}

	var wait time.Duration
	for _, c := range candidates(vol, r.Nodes) {
		if remaining := r.After - time.Since(c.since); remaining > 0 {
			if wait == 0 || remaining < wait {
				wait = remaining
			}
			continue
		}
		reason, transient := r.conversionBlocked(ctx, vol, c.node)
		if reason != "" {
			ctrl.LoggerFrom(ctx).V(1).Info("auto-diskful conversion blocked",
				"volume", vol.Name, "node", c.node, "reason", reason)
			if transient {
				return ctrl.Result{RequeueAfter: time.Minute}, nil
			}
			// Only a spec edit can change a permanent reason, and spec
			// edits re-trigger the watch — polling would be pure churn.
			return ctrl.Result{}, nil
		}
		c.apply(vol)
		if err := r.Update(ctx, vol); err != nil {
			return ctrl.Result{}, err
		}
		metricConversions.WithLabelValues(c.kind).Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(vol, nil, corev1.EventTypeNormal, "AutoDiskful", "Convert",
				"converting %s leg on node %s to a diskful replica (full sync follows)", c.kind, c.node)
		}
		ctrl.LoggerFrom(ctx).Info("auto-diskful: converting leg to a diskful replica",
			"volume", vol.Name, "node", c.node, "kind", c.kind)
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// candidate is one leg eligible for diskful conversion once aged.
type candidate struct {
	node  string
	kind  string
	since time.Time
	// apply mutates vol to perform this conversion.
	apply func(*miroirv1alpha1.MiroirVolume)
}

// candidates lists the volume's convertible legs: completed client legs
// (their attach time is the age signal) and diskless tie-breaker legs a
// consumer stages through (the agent's PrimarySince stamp — on a
// fully-mapped cluster the volume's non-replica node IS its tie-breaker,
// so no client leg ever exists). Only legs on storage nodes qualify.
func candidates(vol *miroirv1alpha1.MiroirVolume, nodes nodemap.Map) []candidate {
	var out []candidate
	for i := range vol.Spec.Clients {
		cl := vol.Spec.Clients[i]
		entry, storage := nodes[cl.Node]
		if !storage || cl.Address == "" || cl.AddedAt == nil {
			continue
		}
		idx := i
		out = append(out, candidate{node: cl.Node, kind: "client", since: cl.AddedAt.Time,
			apply: func(v *miroirv1alpha1.MiroirVolume) { convertClient(v, idx, entry.Backend) }})
	}
	for i := range vol.Spec.Replicas {
		rep := vol.Spec.Replicas[i]
		if !rep.Diskless {
			continue
		}
		entry, storage := nodes[rep.Node]
		since := vol.Status.PerNode[rep.Node].PrimarySince
		if !storage || since == nil {
			continue
		}
		idx := i
		out = append(out, candidate{node: rep.Node, kind: "tiebreaker", since: since.Time,
			apply: func(v *miroirv1alpha1.MiroirVolume) { convertTieBreaker(v, idx, entry.Backend) }})
	}
	return out
}

// convertClient replaces the client leg with a diskful replica carrying
// the same node-id and address — a DRBD node id is immutable on an up
// resource, and the consumer holds the leg's device open, so its live
// identity must not change. A diskless tie-breaker is dropped in the same
// update: three diskful replicas carry three votes, and MaxItems=3 leaves
// no room for both. FullSync is set here because the entry stays complete
// (address kept), so the membership reconciler never touches it.
func convertClient(vol *miroirv1alpha1.MiroirVolume, clientIdx int, backend miroirv1alpha1.BackendType) {
	cl := vol.Spec.Clients[clientIdx]
	replicas := make([]miroirv1alpha1.Replica, 0, len(vol.Spec.Replicas)+1)
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			continue
		}
		replicas = append(replicas, rep)
	}
	replicas = append(replicas, miroirv1alpha1.Replica{
		Node:     cl.Node,
		Backend:  backend,
		NodeID:   cl.NodeID,
		Address:  cl.Address,
		FullSync: true,
	})
	vol.Spec.Replicas = replicas
	vol.Spec.Clients = append(vol.Spec.Clients[:clientIdx], vol.Spec.Clients[clientIdx+1:]...)
}

// convertTieBreaker flips a tie-breaker leg diskful in place (the CEL
// rules allow exactly this direction): node-id and address are kept, so
// the agent attaches a fresh backing device to the live resource and DRBD
// full-syncs it under the running consumer — LINSTOR's toggle-disk.
func convertTieBreaker(vol *miroirv1alpha1.MiroirVolume, idx int, backend miroirv1alpha1.BackendType) {
	rep := &vol.Spec.Replicas[idx]
	rep.Diskless = false
	rep.Backend = backend
	// FullSync: the fresh backing must join as a full SyncTarget, never
	// pose as a data-bearing twin.
	rep.FullSync = true
}

// conversionBlocked reports why the leg on node may not convert right now
// ("" when it is safe) and whether the reason is transient (worth a
// requeue) or changes only with a spec edit (the watch covers those).
// Conservative: conversion is an optimization, so any doubt defers it.
func (r *AutoDiskfulReconciler) conversionBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, node string) (string, bool) {
	if vol.Status.Phase != miroirv1alpha1.VolumeReady {
		// A FullSync joiner during degradation adds resync load at the
		// worst time; wait for every diskful leg to be UpToDate.
		return "volume is not Ready", true
	}
	if len(vol.Spec.DiskfulReplicas()) >= 3 {
		// Already at full redundancy; converting would mean evicting a
		// replica — a policy decision left to the operator.
		return "volume already has 3 diskful replicas", false
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			// Membership completion is in flight; one spec edit at a time.
			return "a replica change is already in flight", true
		}
	}
	// Capacity: the node must fit the full virtual size per its own fresh
	// stats. Missing or stale stats block — the sync would land blind.
	mn := &miroirv1alpha1.MiroirNode{}
	if err := r.Get(ctx, types.NamespacedName{Name: node}, mn); err != nil {
		return "no pool stats for " + node, true
	}
	if mn.Status.ObservedAt == nil || time.Since(mn.Status.ObservedAt.Time) > constants.StatsStaleAfter {
		return "pool stats for " + node + " are stale", true
	}
	if mn.Status.CapacityBytes-mn.Status.AllocatedBytes < vol.Spec.SizeBytes {
		return "insufficient free space on " + node, true
	}
	return "", false
}

// primarySinceChanged fires the watch on PrimarySince transitions: the
// tie-breaker arm's signal lives in status, which the generation filter
// alone would never see.
func primarySinceChanged(oldVol, newVol *miroirv1alpha1.MiroirVolume) bool {
	for node, st := range newVol.Status.PerNode {
		if (st.PrimarySince == nil) != (oldVol.Status.PerNode[node].PrimarySince == nil) {
			return true
		}
	}
	for node, st := range oldVol.Status.PerNode {
		if _, ok := newVol.Status.PerNode[node]; !ok && st.PrimarySince != nil {
			return true
		}
	}
	return false
}

// SetupWithManager registers the reconciler. Spec changes pass the
// generation filter; PrimarySince edges pass the status predicate; the
// initial list plus RequeueAfter cover the time-based threshold.
func (r *AutoDiskfulReconciler) SetupWithManager(mgr ctrl.Manager) error {
	primaryEdge := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldVol, ok1 := e.ObjectOld.(*miroirv1alpha1.MiroirVolume)
			newVol, ok2 := e.ObjectNew.(*miroirv1alpha1.MiroirVolume)
			return ok1 && ok2 && primarySinceChanged(oldVol, newVol)
		},
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{},
			builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, primaryEdge))).
		Named("autodiskful").
		Complete(r)
}
