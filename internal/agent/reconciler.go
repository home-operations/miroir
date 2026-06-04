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
	"errors"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/backend"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/drbd"
)

// VolumeReconciler converges local state for volumes that place a replica on
// NodeName. Level-triggered: safe to restart at any point (notes/DESIGN.md §4.2).
type VolumeReconciler struct {
	client.Client
	NodeName string
	Backend  backend.Backend
	// DRBD drives the replication layer for multi-replica volumes.
	DRBD *drbd.Driver
}

// drbdPollInterval refreshes DRBD state in the CRD: connection/disk state
// changes in the kernel without generating Kubernetes events.
const drbdPollInterval = 30 * time.Second

// errSplitBrain gives the split-brain transition log a real error value.
var errSplitBrain = errors.New("DRBD split-brain detected")

// isDeviceBusy matches drbdadm/lvm failures caused by an open device.
func isDeviceBusy(err error) bool {
	s := err.Error()
	return strings.Contains(s, "held open") || strings.Contains(s, "busy") ||
		strings.Contains(s, "in use")
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
			if isDeviceBusy(err) {
				// A force-deleted pod can leave the device open past
				// NodeUnstage; retry until the mount goes away.
				log.Info("device busy during teardown, retrying", "volume", vol.Name)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
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

	if vol.Spec.DRBD == nil {
		log.V(1).Info("replica realized", "volume", vol.Name, "device", dev)
		return ctrl.Result{}, r.patchStatus(ctx, vol, homefsv1alpha1.ReplicaStatus{
			DeviceCreated: true,
			DevicePath:    dev,
			SizeBytes:     vol.Spec.SizeBytes,
			Connected:     true,
		})
	}

	// Replicated: layer DRBD on the backing device. Pods attach the DRBD
	// device, never the backing LV/zvol directly.
	if err := r.DRBD.Apply(ctx, drbdResource(vol, r.NodeName, dev)); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	st, err := r.DRBD.Status(ctx, vol.Name)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	if st.SplitBrain && !vol.Status.PerNode[r.NodeName].SplitBrain {
		log.Error(errSplitBrain,
			"manual resolution required (drbdadm connect --discard-my-data on the losing node)",
			"volume", vol.Name)
	}
	err = r.patchStatus(ctx, vol, homefsv1alpha1.ReplicaStatus{
		DeviceCreated: true,
		DevicePath:    drbd.DevicePath(vol.Spec.DRBD.Minor),
		SizeBytes:     vol.Spec.SizeBytes,
		DiskState:     st.DiskState,
		Connected:     st.Connected,
		SplitBrain:    st.SplitBrain,
	})
	if apierrors.IsConflict(err) {
		// Both agents poll the same volume; a lost optimistic-lock race
		// is routine. A fixed requeue keeps the poll interval bounded
		// instead of growing exponential backoff (and stale DiskState).
		return ctrl.Result{RequeueAfter: drbdPollInterval}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: drbdPollInterval}, nil
}

// drbdResource maps the CRD desired state to a render input.
func drbdResource(vol *homefsv1alpha1.HomefsVolume, localNode, localDisk string) drbd.Resource {
	peers := make([]drbd.Peer, 0, len(vol.Spec.Replicas))
	for _, rep := range vol.Spec.Replicas {
		peers = append(peers, drbd.Peer{
			Node:    rep.Node,
			NodeID:  rep.NodeID,
			Address: rep.Address,
		})
	}
	return drbd.Resource{
		Name:      vol.Name,
		Minor:     vol.Spec.DRBD.Minor,
		Port:      vol.Spec.DRBD.Port,
		Quorum:    vol.Spec.QuorumPolicy,
		LocalNode: localNode,
		LocalDisk: localDisk,
		Peers:     peers,
	}
}

func (r *VolumeReconciler) teardown(ctx context.Context, vol *homefsv1alpha1.HomefsVolume) error {
	if vol.Spec.DRBD != nil {
		if err := r.DRBD.Down(ctx, vol.Name); err != nil {
			return err
		}
	}
	return r.Backend.Delete(ctx, vol.Name)
}

// removeFinalizer releases this node's own finalizer once local teardown
// is done; the volume disappears when the last replica's agent finishes.
func (r *VolumeReconciler) removeFinalizer(ctx context.Context, vol *homefsv1alpha1.HomefsVolume) error {
	finalizer := constants.FinalizerPrefix + r.NodeName
	if !controllerutil.ContainsFinalizer(vol, finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(vol, finalizer)
	if err := r.Update(ctx, vol); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *VolumeReconciler) reportError(ctx context.Context, vol *homefsv1alpha1.HomefsVolume, cause error) error {
	// Preserve the realized state: a transient error (e.g. DRBD peer not
	// up yet) must not erase DeviceCreated, or the volume would read as
	// hard-Failed while the device exists.
	st := vol.Status.PerNode[r.NodeName]
	st.Message = cause.Error()
	if err := r.patchStatus(ctx, vol, st); err != nil {
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
		replicated := vol.Spec.DRBD != nil
		switch {
		case ok && st.DeviceCreated && st.SizeBytes >= vol.Spec.SizeBytes &&
			(!replicated || st.DiskState == "UpToDate"):
			ready++
		case ok && st.Message != "" && !st.DeviceCreated:
			// Hard failure: the backing device never materialised.
			// Errors after that point (DRBD connect retries, status
			// hiccups) are transient and must not fail provisioning.
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
