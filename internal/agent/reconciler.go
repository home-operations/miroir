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

// Package agent realizes HomefsVolume desired state on one node: it creates,
// resizes, and deletes backing devices via the node's Backend and reports
// observed state back through the CRD status (notes/DESIGN.md §4.2).
package agent

import (
	"context"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	homefsv1alpha1 "github.com/erwanleboucher/homefs/api/v1alpha1"
	"github.com/erwanleboucher/homefs/internal/backend"
	"github.com/erwanleboucher/homefs/internal/constants"
)

// VolumeReconciler converges local state for volumes that place a replica on
// NodeName. Level-triggered: safe to restart at any point (notes/DESIGN.md §4.2).
type VolumeReconciler struct {
	client.Client
	NodeName string
	Backend  backend.Backend
}

// Reconcile realizes (or tears down) this node's replica of one volume.
func (r *VolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	vol := &homefsv1alpha1.HomefsVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	mine := slices.ContainsFunc(vol.Spec.Replicas, func(rep homefsv1alpha1.Replica) bool {
		return rep.Node == r.NodeName
	})

	if !vol.DeletionTimestamp.IsZero() {
		// Only the agent owning a replica may release the finalizer, and
		// only after its local teardown succeeded — a foreign agent
		// touching it would race the owner and leak the backing device.
		if !mine {
			return ctrl.Result{}, nil
		}
		if err := r.teardown(ctx, vol); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.removeFinalizer(ctx, vol)
	}
	if !mine {
		return ctrl.Result{}, nil
	}

	// Realize: create (or grow) the backing device.
	dev, err := r.Backend.Create(ctx, vol.Name, vol.Spec.SizeBytes)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	if err := r.Backend.Resize(ctx, vol.Name, vol.Spec.SizeBytes); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}

	log.V(1).Info("replica realized", "volume", vol.Name, "device", dev)
	return ctrl.Result{}, r.reportReady(ctx, vol, dev)
}

func (r *VolumeReconciler) teardown(ctx context.Context, vol *homefsv1alpha1.HomefsVolume) error {
	return r.Backend.Delete(ctx, vol.Name)
}

// removeFinalizer releases the volume once local teardown is done. One
// shared finalizer suffices while volumes are single-replica; M2 needs
// per-node finalizers.
func (r *VolumeReconciler) removeFinalizer(ctx context.Context, vol *homefsv1alpha1.HomefsVolume) error {
	if !controllerutil.ContainsFinalizer(vol, constants.VolumeFinalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(vol, constants.VolumeFinalizer)
	if err := r.Update(ctx, vol); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *VolumeReconciler) reportReady(ctx context.Context, vol *homefsv1alpha1.HomefsVolume, dev string) error {
	return r.patchStatus(ctx, vol, homefsv1alpha1.ReplicaStatus{
		DeviceCreated: true,
		DevicePath:    dev,
		SizeBytes:     vol.Spec.SizeBytes,
	})
}

func (r *VolumeReconciler) reportError(ctx context.Context, vol *homefsv1alpha1.HomefsVolume, cause error) error {
	if err := r.patchStatus(ctx, vol, homefsv1alpha1.ReplicaStatus{
		Message: cause.Error(),
	}); err != nil {
		return err
	}
	return cause // requeue with backoff
}

// patchStatus updates this node's slot in status and recomputes the phase.
func (r *VolumeReconciler) patchStatus(ctx context.Context, vol *homefsv1alpha1.HomefsVolume, mine homefsv1alpha1.ReplicaStatus) error {
	base := vol.DeepCopy()
	if vol.Status.PerNode == nil {
		vol.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{}
	}
	vol.Status.PerNode[r.NodeName] = mine
	vol.Status.Phase = computePhase(vol)
	// Optimistic lock: a JSON merge patch replaces the whole perNode map,
	// so a concurrent patch from another node's agent must conflict (and
	// requeue) instead of silently dropping that node's entry.
	return r.Status().Patch(ctx, vol,
		client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// computePhase aggregates per-node states into the volume phase the CSI
// controller waits on (notes/DESIGN.md §4.5.1).
func computePhase(vol *homefsv1alpha1.HomefsVolume) homefsv1alpha1.VolumePhase {
	ready := 0
	for _, rep := range vol.Spec.Replicas {
		st, ok := vol.Status.PerNode[rep.Node]
		switch {
		case ok && st.DeviceCreated && st.SizeBytes >= vol.Spec.SizeBytes:
			ready++
		case ok && st.Message != "":
			return homefsv1alpha1.VolumeFailed
		}
	}
	switch {
	case ready == len(vol.Spec.Replicas):
		return homefsv1alpha1.VolumeReady
	case ready > 0:
		return homefsv1alpha1.VolumeDegraded
	default:
		return homefsv1alpha1.VolumeCreating
	}
}

// SetupWithManager registers the reconciler.
func (r *VolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&homefsv1alpha1.HomefsVolume{}).
		Named("agent-volume").
		Complete(r)
}
