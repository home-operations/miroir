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

package csi

import (
	"context"
	"slices"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

// GroupControllerGetCapabilities advertises exactly what is implemented.
func (c *Controller) GroupControllerGetCapabilities(_ context.Context, _ *csi.GroupControllerGetCapabilitiesRequest) (*csi.GroupControllerGetCapabilitiesResponse, error) {
	return &csi.GroupControllerGetCapabilitiesResponse{
		Capabilities: []*csi.GroupControllerServiceCapability{{
			Type: &csi.GroupControllerServiceCapability_Rpc{
				Rpc: &csi.GroupControllerServiceCapability_RPC{
					Type: csi.GroupControllerServiceCapability_RPC_CREATE_DELETE_GET_VOLUME_GROUP_SNAPSHOT,
				},
			},
		}},
	}, nil
}

// CreateVolumeGroupSnapshot provisions one MiroirSnapshot per source
// volume plus the MiroirSnapshotGroup that cuts them under one shared
// write barrier, and reports readiness as-is: the external-snapshotter
// polls until ready_to_use. Idempotent by name.
func (c *Controller) CreateVolumeGroupSnapshot(ctx context.Context, req *csi.CreateVolumeGroupSnapshotRequest) (*csi.CreateVolumeGroupSnapshotResponse, error) {
	if req.GetName() == "" || len(req.GetSourceVolumeIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "group snapshot name and source volumes are required")
	}
	// Sorted member order keeps retries byte-identical regardless of the
	// order the CO lists the volumes in.
	volIDs := slices.Sorted(slices.Values(req.GetSourceVolumeIds()))
	if len(slices.Compact(slices.Clone(volIDs))) != len(volIDs) {
		return nil, status.Error(codes.InvalidArgument, "duplicate source volume ids")
	}

	vols := make([]*miroirv1alpha1.MiroirVolume, 0, len(volIDs))
	for _, id := range volIDs {
		vol := &miroirv1alpha1.MiroirVolume{}
		if err := c.Client.Get(ctx, types.NamespacedName{Name: id}, vol); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, status.Errorf(codes.NotFound, "volume %s not found", id)
			}
			return nil, status.Errorf(codes.Internal, "get volume: %v", err)
		}
		if vol.Spec.DRBD == nil {
			// suspend-io is the only write barrier miroir has; an
			// unreplicated volume would keep writing while its group
			// peers freeze, silently voiding the crash-consistency the
			// group exists to provide.
			return nil, status.Errorf(codes.InvalidArgument,
				"volume %s is not replicated: group snapshots need the DRBD write barrier (replicas > 1)", id)
		}
		vols = append(vols, vol)
	}

	snaps := make([]*miroirv1alpha1.MiroirSnapshot, 0, len(vols))
	names := make([]string, 0, len(vols))
	for _, vol := range vols {
		snap, err := c.ensureGroupMember(ctx, req.GetName(), vol)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
		names = append(names, snap.Name)
	}

	grp := &miroirv1alpha1.MiroirSnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: req.GetName()},
		Spec:       miroirv1alpha1.MiroirSnapshotGroupSpec{SnapshotNames: names},
	}
	// One finalizer per node holding any member leg: each agent lifts its
	// local barriers before the round object disappears.
	nodes := map[string]bool{}
	for _, vol := range vols {
		for _, rep := range vol.Spec.Replicas {
			if !nodes[rep.Node] {
				nodes[rep.Node] = true
				grp.Finalizers = append(grp.Finalizers, constants.FinalizerPrefix+rep.Node)
			}
		}
	}
	if err := c.Client.Create(ctx, grp); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, status.Errorf(codes.Internal, "create MiroirSnapshotGroup: %v", err)
		}
		existing := &miroirv1alpha1.MiroirSnapshotGroup{}
		if err := c.reader().Get(ctx, types.NamespacedName{Name: req.GetName()}, existing); err != nil {
			// Cache may lag the just-created object; retryable, not terminal.
			return nil, status.Errorf(codes.Unavailable, "get existing group: %v", err)
		}
		if !slices.Equal(existing.Spec.SnapshotNames, names) {
			return nil, status.Errorf(codes.AlreadyExists,
				"group snapshot %s exists over different members %v", req.GetName(), existing.Spec.SnapshotNames)
		}
		grp = existing
	}
	return &csi.CreateVolumeGroupSnapshotResponse{GroupSnapshot: csiGroupSnapshot(grp, snaps, vols)}, nil
}

// ensureGroupMember creates one member snapshot (<group>-<volumeID>),
// idempotent by name like ensureSnapshot but carrying the group
// reference; grouped members are cut by the group's round, never their
// own.
func (c *Controller) ensureGroupMember(ctx context.Context, group string, vol *miroirv1alpha1.MiroirVolume) (*miroirv1alpha1.MiroirSnapshot, error) {
	name := group + "-" + vol.Name
	snap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: vol.Name, Group: group},
	}
	for _, rep := range vol.Spec.Replicas {
		snap.Finalizers = append(snap.Finalizers, constants.FinalizerPrefix+rep.Node)
	}
	if err := c.Client.Create(ctx, snap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, status.Errorf(codes.Internal, "create member MiroirSnapshot: %v", err)
		}
		existing := &miroirv1alpha1.MiroirSnapshot{}
		if err := c.reader().Get(ctx, types.NamespacedName{Name: name}, existing); err != nil {
			return nil, status.Errorf(codes.Unavailable, "get existing member snapshot: %v", err)
		}
		if existing.Spec.VolumeName != vol.Name || existing.Spec.Group != group {
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot %s exists for volume %s in group %q", name, existing.Spec.VolumeName, existing.Spec.Group)
		}
		snap = existing
	}
	return snap, nil
}

// DeleteVolumeGroupSnapshot removes the members and the group; agents
// drop backend snapshots and barriers via finalizers. Idempotent.
func (c *Controller) DeleteVolumeGroupSnapshot(ctx context.Context, req *csi.DeleteVolumeGroupSnapshotRequest) (*csi.DeleteVolumeGroupSnapshotResponse, error) {
	if req.GetGroupSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group snapshot id is required")
	}
	members, err := c.groupMembers(ctx, req.GetGroupSnapshotId(), req.GetSnapshotIds())
	if err != nil {
		return nil, err
	}
	for _, name := range members {
		snap := &miroirv1alpha1.MiroirSnapshot{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := c.Client.Delete(ctx, snap); err != nil && !apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.Internal, "delete member MiroirSnapshot: %v", err)
		}
	}
	grp := &miroirv1alpha1.MiroirSnapshotGroup{ObjectMeta: metav1.ObjectMeta{Name: req.GetGroupSnapshotId()}}
	if err := c.Client.Delete(ctx, grp); err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "delete MiroirSnapshotGroup: %v", err)
	}
	return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
}

// groupMembers resolves the member list for delete: the group's own
// spec when it exists (cross-checked against the CO's list — a mismatch
// is detectable, so the spec requires reporting it), else the CO's list
// filtered to snapshots that verifiably belong to the group (a partial
// earlier delete can have removed the group first).
func (c *Controller) groupMembers(ctx context.Context, groupID string, snapshotIDs []string) ([]string, error) {
	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	err := c.Client.Get(ctx, types.NamespacedName{Name: groupID}, grp)
	if err == nil {
		if len(snapshotIDs) > 0 && !sameNameSet(snapshotIDs, grp.Spec.SnapshotNames) {
			return nil, status.Errorf(codes.InvalidArgument,
				"snapshot ids %v do not match group %s members %v", snapshotIDs, groupID, grp.Spec.SnapshotNames)
		}
		return grp.Spec.SnapshotNames, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "get MiroirSnapshotGroup: %v", err)
	}
	var members []string
	for _, name := range snapshotIDs {
		snap := &miroirv1alpha1.MiroirSnapshot{}
		if err := c.Client.Get(ctx, types.NamespacedName{Name: name}, snap); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "get member snapshot: %v", err)
		}
		if snap.Spec.Group != groupID {
			return nil, status.Errorf(codes.InvalidArgument,
				"snapshot %s does not belong to group %s", name, groupID)
		}
		members = append(members, name)
	}
	return members, nil
}

// GetVolumeGroupSnapshot reports the group's current state.
func (c *Controller) GetVolumeGroupSnapshot(ctx context.Context, req *csi.GetVolumeGroupSnapshotRequest) (*csi.GetVolumeGroupSnapshotResponse, error) {
	if req.GetGroupSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group snapshot id is required")
	}
	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetGroupSnapshotId()}, grp); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "group snapshot %s not found", req.GetGroupSnapshotId())
		}
		return nil, status.Errorf(codes.Internal, "get MiroirSnapshotGroup: %v", err)
	}
	if ids := req.GetSnapshotIds(); len(ids) > 0 && !sameNameSet(ids, grp.Spec.SnapshotNames) {
		return nil, status.Errorf(codes.InvalidArgument,
			"snapshot ids %v do not match group %s members %v", ids, grp.Name, grp.Spec.SnapshotNames)
	}
	var snaps []*miroirv1alpha1.MiroirSnapshot
	var vols []*miroirv1alpha1.MiroirVolume
	for _, name := range grp.Spec.SnapshotNames {
		snap := &miroirv1alpha1.MiroirSnapshot{}
		if err := c.Client.Get(ctx, types.NamespacedName{Name: name}, snap); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, status.Errorf(codes.NotFound, "member snapshot %s of group %s not found", name, grp.Name)
			}
			return nil, status.Errorf(codes.Internal, "get member snapshot: %v", err)
		}
		snaps = append(snaps, snap)
		vols = append(vols, nil)
	}
	return &csi.GetVolumeGroupSnapshotResponse{GroupSnapshot: csiGroupSnapshot(grp, snaps, vols)}, nil
}

// csiGroupSnapshot maps the group and its members onto the wire shape.
// vols supplies each member's live volume for the pre-cut size fallback
// (nil entries fall back to the snapshot's recorded size alone).
func csiGroupSnapshot(grp *miroirv1alpha1.MiroirSnapshotGroup, snaps []*miroirv1alpha1.MiroirSnapshot, vols []*miroirv1alpha1.MiroirVolume) *csi.VolumeGroupSnapshot {
	ready := grp.Status.ReadyToUse
	members := make([]*csi.Snapshot, 0, len(snaps))
	for i, snap := range snaps {
		size := snap.Status.SizeBytes
		if size == 0 && vols[i] != nil {
			size = vols[i].Spec.SizeBytes
		}
		members = append(members, csiSnapshot(snap, size))
		ready = ready && snap.Status.ReadyToUse
	}
	return &csi.VolumeGroupSnapshot{
		GroupSnapshotId: grp.Name,
		Snapshots:       members,
		CreationTime:    timestamppb.New(grp.CreationTimestamp.Time),
		ReadyToUse:      ready,
	}
}

// sameNameSet reports whether the two name lists contain the same names
// (order-insensitive; both lists are duplicate-free by construction).
func sameNameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := slices.Sorted(slices.Values(a))
	bs := slices.Sorted(slices.Values(b))
	return slices.Equal(as, bs)
}
