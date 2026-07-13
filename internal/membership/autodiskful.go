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

// Reconcile converts at most one client leg per pass; the spec update
// re-triggers this generation-filtered controller for any remaining legs.
func (r *AutoDiskfulReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() || vol.Spec.DRBD == nil || len(vol.Spec.Clients) == 0 {
		return ctrl.Result{}, nil
	}

	var wait time.Duration
	for i := range vol.Spec.Clients {
		cl := vol.Spec.Clients[i]
		entry, storage := r.Nodes[cl.Node]
		if !storage || cl.Address == "" || cl.AddedAt == nil {
			// Non-storage nodes can never hold a disk; incomplete legs
			// re-trigger via the membership completion edit.
			continue
		}
		if remaining := r.After - time.Since(cl.AddedAt.Time); remaining > 0 {
			if wait == 0 || remaining < wait {
				wait = remaining
			}
			continue
		}
		if reason := r.conversionBlocked(ctx, vol, cl.Node); reason != "" {
			ctrl.LoggerFrom(ctx).V(1).Info("auto-diskful conversion blocked",
				"volume", vol.Name, "node", cl.Node, "reason", reason)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		return ctrl.Result{}, r.convert(ctx, vol, i, entry.Backend)
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

// SetupWithManager registers the reconciler. Generation-filtered like its
// membership siblings; the initial list plus RequeueAfter cover the
// time-based threshold.
func (r *AutoDiskfulReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("autodiskful").
		Complete(r)
}
