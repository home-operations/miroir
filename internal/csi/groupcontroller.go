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
// write barrier. Idempotent by name.
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

	names := make([]string, 0, len(vols))
	for _, vol := range vols {
		names = append(names, req.GetName()+"-"+vol.Name)
	}
	// A same-name group over a different member set is terminal; check
	// before creating members so the common mismatch (a stale retry, a
	// reused name) fails without side effects. Members created here would
	// otherwise be undeletable strays: DeleteSnapshot refuses grouped
	// members and DeleteVolumeGroupSnapshot refuses the set mismatch.
	if err := c.refuseMemberMismatch(ctx, req.GetName(), names); err != nil {
		return nil, err
	}

	snaps := make([]*miroirv1alpha1.MiroirSnapshot, 0, len(vols))
	for i, vol := range vols {
		snap, err := c.ensureSnapshot(ctx, names[i], vol, req.GetName(), false)
		if err != nil {
			c.cleanupPartialMembers(ctx, req.GetName(), names[:i])
			return nil, err
		}
		snaps = append(snaps, snap)
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
			// The pre-check's racing twin: a conflicting group landed
			// between the check and this Create. Members this RPC created
			// that the winner does not own would be undeletable strays —
			// remove them before surfacing the mismatch.
			c.cleanupStrayMembers(ctx, names, existing.Spec.SnapshotNames)
			return nil, status.Errorf(codes.AlreadyExists,
				"group snapshot %s exists over different members %v", req.GetName(), existing.Spec.SnapshotNames)
		}
		grp = existing
	}
	result := csiGroupSnapshot(grp, snaps, vols)
	if !result.GetReadyToUse() {
		// external-snapshotter records member readiness only from the
		// successful Create response; keep the operation pending until it
		// can persist a complete, usable member set.
		return nil, status.Errorf(codes.Aborted, "group snapshot %s is not ready", grp.Name)
	}
	return &csi.CreateVolumeGroupSnapshotResponse{GroupSnapshot: result}, nil
}

// refuseMemberMismatch fails fast when the named group already exists
// over a different member set. Absence is fine (the create proceeds);
// so is an equal set (the idempotent retry proceeds).
func (c *Controller) refuseMemberMismatch(ctx context.Context, group string, names []string) error {
	existing := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: group}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return status.Errorf(codes.Internal, "get MiroirSnapshotGroup: %v", err)
	}
	if !slices.Equal(existing.Spec.SnapshotNames, names) {
		return status.Errorf(codes.AlreadyExists,
			"group snapshot %s exists over different members %v", group, existing.Spec.SnapshotNames)
	}
	return nil
}

// cleanupPartialMembers deletes the members a create had already ensured
// when a later member's create failed: with no group object, nothing
// owns them, DeleteSnapshot refuses grouped members, and a terminal
// per-member failure (an invalid derived name, say) would strand them
// forever. Skipped when the group object exists — then the pre-check
// proved the member set equal and these are a live group's members,
// which the retry will re-adopt. Best-effort, like cleanupStrayMembers.
func (c *Controller) cleanupPartialMembers(ctx context.Context, group string, created []string) {
	if len(created) == 0 {
		return
	}
	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	err := c.Client.Get(ctx, types.NamespacedName{Name: group}, grp)
	if err == nil {
		return
	}
	if !apierrors.IsNotFound(err) {
		log.Error(err, "cannot check group before member cleanup", "group", group)
		return
	}
	c.cleanupStrayMembers(ctx, created, nil)
}

// cleanupStrayMembers deletes the member snapshots this RPC implied that
// the winning group does not list. Only those: an overlapping name (the
// winner snapshots the same volume) belongs to the winner's set and must
// survive. Best-effort — a failed delete leaves a stray the retry (or an
// operator) can remove, which still beats failing the RPC over cleanup.
func (c *Controller) cleanupStrayMembers(ctx context.Context, names, owned []string) {
	for _, name := range names {
		if slices.Contains(owned, name) {
			continue
		}
		snap := &miroirv1alpha1.MiroirSnapshot{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := c.Client.Delete(ctx, snap); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "cannot delete stray group member snapshot", "snapshot", name)
		}
	}
}

// DeleteVolumeGroupSnapshot removes the group and then its members;
// agents drop backend snapshots and barriers via finalizers. The group
// goes first: mid-round, a member's own deletion defers lifting the
// kernel barrier to the live group round, and the group's teardown can
// only lift barriers for members it can still resolve — so members
// deleted first could vanish before a node's group teardown runs and
// strand their volumes suspended (issue #272). Group-first, every agent
// sees the group's deletion while the members still exist, and a member
// deleted after the group is gone lifts its own barrier. Idempotent.
func (c *Controller) DeleteVolumeGroupSnapshot(ctx context.Context, req *csi.DeleteVolumeGroupSnapshotRequest) (*csi.DeleteVolumeGroupSnapshotResponse, error) {
	if req.GetGroupSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group snapshot id is required")
	}
	members, err := c.groupMembers(ctx, req.GetGroupSnapshotId(), req.GetSnapshotIds())
	if err != nil {
		return nil, err
	}
	grp := &miroirv1alpha1.MiroirSnapshotGroup{ObjectMeta: metav1.ObjectMeta{Name: req.GetGroupSnapshotId()}}
	if err := c.Client.Delete(ctx, grp); err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "delete MiroirSnapshotGroup: %v", err)
	}
	for _, name := range members {
		snap := &miroirv1alpha1.MiroirSnapshot{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := c.Client.Delete(ctx, snap); err != nil && !apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.Internal, "delete member MiroirSnapshot: %v", err)
		}
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
