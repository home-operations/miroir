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

// Package agent realizes MiroirVolume desired state on one node: it creates,
// resizes, and deletes backing devices via the node's Backend and reports
// observed state back through the CRD status (notes/DESIGN.md §4.2).
package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// VolumeReconciler converges local state for volumes that place a replica on
// NodeName. Level-triggered: safe to restart at any point (notes/DESIGN.md §4.2).
type VolumeReconciler struct {
	client.Client
	NodeName string
	Backend  backend.Backend
	// DRBD drives the replication layer for multi-replica volumes.
	DRBD *drbd.Driver
	// DRBDEvents delivers kernel state changes (drbdsetup events2) as
	// reconcile triggers, ahead of the next poll.
	DRBDEvents <-chan event.GenericEvent
}

// drbdPollInterval refreshes DRBD state in the CRD: connection/disk state
// changes in the kernel without generating Kubernetes events.
const drbdPollInterval = 30 * time.Second

// errSplitBrain gives the split-brain transition log a real error value.
var errSplitBrain = errors.New("DRBD split-brain detected")

// isDeviceBusy matches failures that resolve themselves once something
// else goes away: an open device (force-deleted pod), an LVM origin with
// snapshot children, a ZFS origin with clones.
func isDeviceBusy(err error) bool {
	s := err.Error()
	return strings.Contains(s, "held open") || strings.Contains(s, "busy") ||
		strings.Contains(s, "in use") ||
		strings.Contains(s, "snapshot") || // lvremove: origin contains snapshots
		strings.Contains(s, "has children") || // zfs destroy: snapshots exist
		strings.Contains(s, "dependent clones") // zfs destroy: restore clones exist
}

// Reconcile realizes (or tears down) this node's replica of one volume.
func (r *VolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	idx := slices.IndexFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == r.NodeName
	})
	mine := idx >= 0

	if !vol.DeletionTimestamp.IsZero() {
		// Only the agent owning a replica may release the finalizer, and
		// only after its local teardown succeeded — a foreign agent
		// touching it would race the owner and leak the backing device.
		// A pending-removal replica (finalizer held, not in spec) takes
		// the same path: volume deletion supersedes the removal gates.
		if !mine && !controllerutil.ContainsFinalizer(vol, constants.FinalizerPrefix+r.NodeName) {
			return ctrl.Result{}, nil
		}
		dropVolumeMetrics(vol.Name)
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
		// Not placed here, but a held finalizer means this replica was
		// removed from spec.replicas: tear down the local leg once it is
		// safe (notes/DESIGN.md §4.2).
		return r.reconcileRemoval(ctx, vol)
	}
	if vol.Spec.DRBD != nil && vol.Spec.Replicas[idx].Address == "" {
		// A just-added entry the membership reconciler has not completed
		// yet (no NodeID/address): nothing can be realized safely.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Realize: create (or grow) the backing device — a CoW clone when the
	// volume restores from a snapshot.
	dev, err := r.realizeBacking(ctx, vol)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	// Record the device before growing it: computePhase treats errors
	// with DeviceCreated=false as hard provisioning failures, and a
	// transient resize error must not be one.
	if !vol.Status.PerNode[r.NodeName].DeviceCreated {
		if err := r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
			DeviceCreated: true,
			DevicePath:    dev,
		}); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
		vol.Status.PerNode[r.NodeName] = miroirv1alpha1.ReplicaStatus{
			DeviceCreated: true, DevicePath: dev,
		}
	}
	if err := r.Backend.Resize(ctx, vol.Name, vol.Spec.SizeBytes); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}

	if vol.Spec.DRBD == nil {
		log.V(1).Info("replica realized", "volume", vol.Name, "device", dev)
		recordVolumeMetrics(vol.Name, miroirReplicaView{upToDate: true, connected: true})
		return ctrl.Result{}, r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
			DeviceCreated: true,
			DevicePath:    dev,
			SizeBytes:     vol.Spec.SizeBytes,
			Connected:     true,
		})
	}

	// Replicated: layer DRBD on the backing device. Pods attach the DRBD
	// device, never the backing LV/zvol directly.
	minor, err := r.assignMinor(ctx, vol)
	if err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	if err := r.DRBD.Apply(ctx, drbdResource(vol, r.NodeName, dev, minor)); err != nil {
		return ctrl.Result{}, r.reportError(ctx, vol, err)
	}
	// Online growth: once every peer's backing device is at the new size
	// (the local leg was just resized above), replicas[0] grows the DRBD
	// device over them. It withholds the new size from its status until
	// then — the CSI expansion wait keys on status, and the filesystem
	// must not grow against a still-small DRBD device.
	reportSize := vol.Spec.SizeBytes
	requeue := drbdPollInterval
	if vol.Spec.Replicas[0].Node == r.NodeName {
		if peerBackingsGrown(vol, r.NodeName) {
			if err := r.DRBD.Resize(ctx, vol.Name); err != nil {
				return ctrl.Result{}, r.reportError(ctx, vol, err)
			}
		} else {
			reportSize = vol.Status.PerNode[r.NodeName].SizeBytes
			requeue = 5 * time.Second
		}
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
	recordVolumeMetrics(vol.Name, miroirReplicaView{
		upToDate:   st.DiskState == drbd.DiskUpToDate,
		connected:  st.Connected,
		splitBrain: st.SplitBrain,
		suspended:  st.Suspended,
	})
	err = r.patchStatus(ctx, vol, miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true,
		DevicePath:    drbd.DevicePath(minor),
		DRBDMinor:     minor,
		SizeBytes:     reportSize,
		DiskState:     st.DiskState,
		Connected:     st.Connected,
		SplitBrain:    st.SplitBrain,
	})
	if apierrors.IsConflict(err) {
		// Both agents poll the same volume; a lost optimistic-lock race
		// is routine. A fixed requeue keeps the poll interval bounded
		// instead of growing exponential backoff (and stale DiskState).
		return ctrl.Result{RequeueAfter: requeue}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// peerBackingsGrown reports whether every peer realized the desired size.
// The local leg is excluded: its status entry is stale (just patched).
func peerBackingsGrown(vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self {
			continue
		}
		if vol.Status.PerNode[rep.Node].SizeBytes < vol.Spec.SizeBytes {
			return false
		}
	}
	return true
}

// realizeBacking creates the backing device: fresh, or cloned from a
// snapshot for restores. Clones are byte-identical on every replica, so
// the day0 GI seed keeps restored volumes from resyncing.
func (r *VolumeReconciler) realizeBacking(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (string, error) {
	if vol.Spec.Source == nil {
		return r.Backend.Create(ctx, vol.Name, vol.Spec.SizeBytes)
	}
	// The backing is cloned once and survives reboots; the source snapshot
	// may be gone by then, so recover an existing device without it.
	if exists, err := r.Backend.Exists(ctx, vol.Name); err != nil {
		return "", err
	} else if exists {
		return r.Backend.Create(ctx, vol.Name, vol.Spec.SizeBytes)
	}
	snap := &miroirv1alpha1.MiroirSnapshot{}
	if err := r.Get(ctx, types.NamespacedName{Name: vol.Spec.Source.SnapshotName}, snap); err != nil {
		return "", err
	}
	return r.Backend.CreateFromSnapshot(ctx, vol.Name, snap.Spec.VolumeName, snap.Name)
}

// drbdResource maps the CRD desired state to a render input. Entries the
// membership reconciler has not completed yet (no address) are left out:
// rendering them would produce a config DRBD cannot parse, and the peer
// cannot connect before completion anyway.
func drbdResource(vol *miroirv1alpha1.MiroirVolume, localNode, localDisk string, minor int32) drbd.Resource {
	peers := make([]drbd.Peer, 0, len(vol.Spec.Replicas))
	skipSeed := false
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			continue
		}
		if rep.Node == localNode {
			skipSeed = rep.FullSync
		}
		peers = append(peers, drbd.Peer{
			Node:    rep.Node,
			NodeID:  rep.NodeID,
			Address: rep.Address,
		})
	}
	return drbd.Resource{
		Name:      vol.Name,
		Minor:     minor,
		Port:      vol.Spec.DRBD.Port,
		Quorum:    vol.Spec.QuorumPolicy,
		LocalNode: localNode,
		LocalDisk: localDisk,
		Secret:    vol.Spec.DRBD.SharedSecret,
		SkipSeed:  skipSeed,
		Peers:     peers,
	}
}

// reconcileRemoval tears down a replica that was removed from
// spec.replicas while the volume lives on. It only proceeds when losing
// this leg cannot lose data: every remaining replica must be UpToDate and
// connected, and no snapshot may reference the volume — snapshots exist as
// backend CoW state on every replica, and restores expect to find them
// wherever the volume is placed.
func (r *VolumeReconciler) reconcileRemoval(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	finalizer := constants.FinalizerPrefix + r.NodeName
	if !controllerutil.ContainsFinalizer(vol, finalizer) {
		return ctrl.Result{}, nil
	}
	if reason := r.removalBlocked(ctx, vol); reason != "" {
		log.Info("replica removal blocked", "volume", vol.Name, "reason", reason)
		st := vol.Status.PerNode[r.NodeName]
		st.Message = "replica removal blocked: " + reason
		if err := r.patchStatus(ctx, vol, st); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	dropVolumeMetrics(vol.Name)
	if err := r.teardown(ctx, vol); err != nil {
		if isDeviceBusy(err) {
			// A pod still staged here holds the device open; it has to
			// move off this node before the leg can go.
			log.Info("device busy during replica removal, retrying", "volume", vol.Name)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	// Drop this node's status slot — merge-patch null deletes the key.
	// Best-effort ordering: a crash here leaves a stale slot, which
	// nothing reads (phase and growth iterate spec.replicas only).
	patch := fmt.Appendf(nil, `{"status":{"perNode":{%q:null}}}`, r.NodeName)
	if err := r.Status().Patch(ctx, vol, client.RawPatch(types.MergePatchType, patch)); err != nil &&
		!apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	log.Info("replica removed", "volume", vol.Name)
	return ctrl.Result{}, r.removeFinalizer(ctx, vol)
}

// removalBlocked reports why this replica must not be torn down yet, or ""
// when it is safe. The remaining replicas' health is read from the CRD
// status the peers report — by removal time the peers have already dropped
// this node from their configs, so the local kernel's view of them is gone.
func (r *VolumeReconciler) removalBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) string {
	if vol.Spec.DRBD == nil {
		// An unreplicated volume's lone entry moved: there is no peer
		// holding the data, so tearing down here is data loss. Unsupported.
		return "volume has no replication layer; refusing to drop the only copy"
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := r.List(ctx, snaps); err != nil {
		return "cannot list snapshots: " + err.Error()
	}
	for _, s := range snaps.Items {
		if s.Spec.VolumeName == vol.Name {
			return "snapshot " + s.Name + " exists; delete the volume's snapshots first"
		}
	}
	for _, rep := range vol.Spec.Replicas {
		st, ok := vol.Status.PerNode[rep.Node]
		if !ok || st.DiskState != drbd.DiskUpToDate || !st.Connected {
			return "replica on " + rep.Node + " is not UpToDate and connected"
		}
	}
	return ""
}

func (r *VolumeReconciler) teardown(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Spec.DRBD != nil {
		if err := r.DRBD.Down(ctx, vol.Name); err != nil {
			return err
		}
	}
	return r.Backend.Delete(ctx, vol.Name)
}

// removeFinalizer releases this node's own finalizer once local teardown
// is done; the volume disappears when the last replica's agent finishes.
func (r *VolumeReconciler) removeFinalizer(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
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

func (r *VolumeReconciler) reportError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) error {
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
func (r *VolumeReconciler) patchStatus(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, mine miroirv1alpha1.ReplicaStatus) error {
	if vol.Status.PerNode == nil {
		vol.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	vol.Status.PerNode[r.NodeName] = mine
	vol.Status.Phase = computePhase(vol)
	vol.SetGroupVersionKind(miroirv1alpha1.GroupVersion.WithKind("MiroirVolume"))
	vol.ManagedFields = nil
	return r.Status().Patch(ctx, vol, client.Apply, //nolint:staticcheck
		client.FieldOwner("agent-volume-"+r.NodeName),
		client.ForceOwnership)
}

// assignMinor returns the DRBD minor for this volume, allocating a free one if unset.
func (r *VolumeReconciler) assignMinor(_ context.Context, vol *miroirv1alpha1.MiroirVolume) (int32, error) {
	if m := vol.Status.PerNode[r.NodeName].DRBDMinor; m > 0 {
		return m, nil
	}
	minor, err := r.DRBD.AllocateMinor(vol.Name)
	if err != nil {
		return 0, err
	}
	return minor, nil
}

// computePhase aggregates per-node states into the volume phase the CSI
// controller waits on (notes/DESIGN.md §4.5.1).
func computePhase(vol *miroirv1alpha1.MiroirVolume) miroirv1alpha1.VolumePhase {
	ready := 0
	for _, rep := range vol.Spec.Replicas {
		st, ok := vol.Status.PerNode[rep.Node]
		replicated := vol.Spec.DRBD != nil
		switch {
		case ok && st.DeviceCreated && st.SizeBytes >= vol.Spec.SizeBytes &&
			(!replicated || st.DiskState == drbd.DiskUpToDate):
			ready++
		case ok && st.Message != "" && !st.DeviceCreated:
			// Hard failure: the backing device never materialised.
			// Errors after that point (DRBD connect retries, status
			// hiccups) are transient and must not fail provisioning.
			return miroirv1alpha1.VolumeFailed
		}
	}
	switch {
	case ready == len(vol.Spec.Replicas):
		return miroirv1alpha1.VolumeReady
	case ready > 0:
		return miroirv1alpha1.VolumeDegraded
	default:
		return miroirv1alpha1.VolumeCreating
	}
}

// SetupWithManager registers the reconciler.
func (r *VolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{}).
		Named("agent-volume")
	if r.DRBDEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.DRBDEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}
