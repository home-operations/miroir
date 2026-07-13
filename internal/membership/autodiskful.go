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

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/nodemap"
)

// statsStaleAfter mirrors the CSI placement guard: MiroirNode figures
// older than this are treated as unknown.
const statsStaleAfter = 5 * time.Minute

// AutoDiskfulReconciler converts a long-lived diskless client leg into a
// diskful replica on its node (LINSTOR's auto-diskful): a consumer that has
// stayed put past the threshold evidently lives there, so give it a local
// replica and stop paying network I/O for every read and write. The
// membership Reconciler completes the added entry (FullSync), the node's
// agent attaches a backing device to the live resource, and DRBD resyncs it
// online — the pod keeps running throughout.
type AutoDiskfulReconciler struct {
	client.Client
	// Nodes is the storage topology; only nodes in it can become diskful.
	Nodes nodemap.Map
	// After is the conversion threshold; the setup path guards > 0.
	After time.Duration
}

// Reconcile converts at most one leg per pass — a client leg (attach a
// replica, drop the leg) or a tie-breaker leg a consumer stages through
// (flip diskless→diskful in place); the spec update re-triggers this
// controller for anything remaining.
func (r *AutoDiskfulReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() || vol.Spec.DRBD == nil {
		return ctrl.Result{}, nil
	}

	var wait time.Duration
	track := func(remaining time.Duration) {
		if wait == 0 || remaining < wait {
			wait = remaining
		}
	}
	for i := range vol.Spec.Clients {
		cl := vol.Spec.Clients[i]
		entry, storage := r.Nodes[cl.Node]
		if !storage || cl.Address == "" || cl.AddedAt == nil {
			// Non-storage nodes can never hold a disk; incomplete legs
			// re-trigger via the membership completion edit.
			continue
		}
		if remaining := r.After - time.Since(cl.AddedAt.Time); remaining > 0 {
			track(remaining)
			continue
		}
		if reason := r.conversionBlocked(ctx, vol, cl.Node); reason != "" {
			ctrl.LoggerFrom(ctx).V(1).Info("auto-diskful conversion blocked",
				"volume", vol.Name, "node", cl.Node, "reason", reason)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		return ctrl.Result{}, r.convert(ctx, vol, i, entry.Backend)
	}
	// Tie-breaker arm: on a fully-mapped cluster the volume's non-replica
	// node IS its tie-breaker, so a settled consumer stages through that
	// leg and no client leg ever exists. Its PrimarySince stamp (agent-
	// maintained from the kernel role) is the "in use" signal.
	for i := range vol.Spec.Replicas {
		rep := vol.Spec.Replicas[i]
		if !rep.Diskless {
			continue
		}
		entry, storage := r.Nodes[rep.Node]
		since := vol.Status.PerNode[rep.Node].PrimarySince
		if !storage || since == nil {
			continue
		}
		if remaining := r.After - time.Since(since.Time); remaining > 0 {
			track(remaining)
			continue
		}
		if reason := r.conversionBlocked(ctx, vol, rep.Node); reason != "" {
			ctrl.LoggerFrom(ctx).V(1).Info("auto-diskful conversion blocked",
				"volume", vol.Name, "node", rep.Node, "reason", reason)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		return ctrl.Result{}, r.convertTieBreaker(ctx, vol, i, entry.Backend)
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// conversionBlocked reports why the client leg on node may not convert
// right now, or "" when it is safe. Conservative: conversion is an
// optimization, so any doubt defers it.
func (r *AutoDiskfulReconciler) conversionBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, node string) string {
	if vol.Status.Phase != miroirv1alpha1.VolumeReady {
		// A FullSync joiner during degradation adds resync load at the
		// worst time; wait for every diskful leg to be UpToDate.
		return "volume is not Ready"
	}
	if len(vol.Spec.DiskfulReplicas()) >= 3 {
		// Already at full redundancy; converting would mean evicting a
		// replica — a policy decision left to the operator.
		return "volume already has 3 diskful replicas"
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			// Membership completion is in flight; one spec edit at a time.
			return "a replica change is already in flight"
		}
	}
	// Capacity: the node must fit the full virtual size per its own fresh
	// stats. Missing or stale stats block — the sync would land blind.
	mn := &miroirv1alpha1.MiroirNode{}
	if err := r.Get(ctx, types.NamespacedName{Name: node}, mn); err != nil {
		return "no pool stats for " + node
	}
	if mn.Status.ObservedAt == nil || time.Since(mn.Status.ObservedAt.Time) > statsStaleAfter {
		return "pool stats for " + node + " are stale"
	}
	if mn.Status.CapacityBytes-mn.Status.AllocatedBytes < vol.Spec.SizeBytes {
		return "insufficient free space on " + node
	}
	return ""
}

// convert replaces the client leg with a diskful replica entry in one
// update: the CEL rules forbid a client sharing a node with a replica, and
// a diskless tie-breaker is dropped when present — three diskful replicas
// carry three quorum votes, so it has nothing left to break.
func (r *AutoDiskfulReconciler) convert(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, clientIdx int, backend miroirv1alpha1.BackendType) error {
	node := vol.Spec.Clients[clientIdx].Node
	replicas := make([]miroirv1alpha1.Replica, 0, len(vol.Spec.Replicas)+1)
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			continue
		}
		replicas = append(replicas, rep)
	}
	replicas = append(replicas, miroirv1alpha1.Replica{Node: node, Backend: backend})
	vol.Spec.Replicas = replicas
	vol.Spec.Clients = append(vol.Spec.Clients[:clientIdx], vol.Spec.Clients[clientIdx+1:]...)
	if err := r.Update(ctx, vol); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("auto-diskful: converting client leg to a replica",
		"volume", vol.Name, "node", node)
	return nil
}

// convertTieBreaker flips a tie-breaker leg diskful in place (the CEL
// rules allow exactly this direction): node-id and address are kept, so
// the agent attaches a fresh backing device to the live resource and DRBD
// full-syncs it under the running consumer — LINSTOR's toggle-disk.
func (r *AutoDiskfulReconciler) convertTieBreaker(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, idx int, backend miroirv1alpha1.BackendType) error {
	rep := &vol.Spec.Replicas[idx]
	rep.Diskless = false
	rep.Backend = backend
	// FullSync: the fresh backing must join as a full SyncTarget, never
	// pose as a data-bearing twin.
	rep.FullSync = true
	if err := r.Update(ctx, vol); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("auto-diskful: converting tie-breaker leg to a diskful replica",
		"volume", vol.Name, "node", rep.Node)
	return nil
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
