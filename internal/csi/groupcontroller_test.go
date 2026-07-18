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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

const groupGsnap1 = "gsnap-1"

func replicatedVol(name string, nodes ...string) *miroirv1alpha1.MiroirVolume {
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
		},
	}
	for i, n := range nodes {
		v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
			Node: n, Backend: miroirv1alpha1.BackendLVMThin, NodeID: int32(i),
		})
	}
	return v
}

func TestCreateVolumeGroupSnapshotCreatesMembersAndGroup(t *testing.T) {
	s := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(replicatedVol(volSrc, nodeA, nodeB), replicatedVol(volPvc1, nodeB, nodeC)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirSnapshotGroup{}).
		Build()
	c := &Controller{Client: cl}

	// Deliberately unsorted: member order must not depend on it.
	resp, err := c.CreateVolumeGroupSnapshot(t.Context(), &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{volSrc, volPvc1},
	})
	if err != nil {
		t.Fatal(err)
	}
	gs := resp.GetGroupSnapshot()
	if gs.GetGroupSnapshotId() != groupGsnap1 || gs.GetReadyToUse() {
		t.Fatalf("fresh group must report not ready under its id: %+v", gs)
	}
	if len(gs.GetSnapshots()) != 2 {
		t.Fatalf("want 2 member snapshots, got %+v", gs.GetSnapshots())
	}
	for _, m := range gs.GetSnapshots() {
		if m.GetGroupSnapshotId() != groupGsnap1 {
			t.Fatalf("members must carry the group id: %+v", m)
		}
	}

	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: groupGsnap1}, grp); err != nil {
		t.Fatal(err)
	}
	wantMembers := []string{groupGsnap1 + "-" + volPvc1, groupGsnap1 + "-" + volSrc}
	if !slices.Equal(grp.Spec.SnapshotNames, wantMembers) {
		t.Fatalf("members must be sorted by source volume: %v", grp.Spec.SnapshotNames)
	}
	// One finalizer per node holding any member leg: the union a, b, c.
	for _, n := range []string{nodeA, nodeB, nodeC} {
		if !slices.Contains(grp.Finalizers, constants.FinalizerPrefix+n) {
			t.Fatalf("group must carry finalizer for %s: %v", n, grp.Finalizers)
		}
	}
	for _, name := range wantMembers {
		m := &miroirv1alpha1.MiroirSnapshot{}
		if err := cl.Get(t.Context(), types.NamespacedName{Name: name}, m); err != nil {
			t.Fatal(err)
		}
		if m.Spec.Group != groupGsnap1 {
			t.Fatalf("member %s must reference its group: %+v", name, m.Spec)
		}
	}
}

func TestCreateVolumeGroupSnapshotIdempotent(t *testing.T) {
	s := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(replicatedVol(volSrc, nodeA), replicatedVol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirSnapshotGroup{}).
		Build()
	c := &Controller{Client: cl}
	req := &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{volSrc, volPvc1},
	}

	if _, err := c.CreateVolumeGroupSnapshot(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateVolumeGroupSnapshot(t.Context(), req); err != nil {
		t.Fatalf("identical retry must succeed: %v", err)
	}

	_, err := c.CreateVolumeGroupSnapshot(t.Context(), &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{volSrc},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("same name over a different member set must be AlreadyExists, got %v", err)
	}
}

// A same-name group over a different member set must fail before any
// member snapshot exists: members created past that point would be
// undeletable strays (DeleteSnapshot refuses grouped members and
// DeleteVolumeGroupSnapshot refuses the set mismatch).
func TestCreateVolumeGroupSnapshotMismatchCreatesNothing(t *testing.T) {
	s := newScheme(t)
	existing := &miroirv1alpha1.MiroirSnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: groupGsnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotGroupSpec{SnapshotNames: []string{groupGsnap1 + "-" + volNew}},
	}
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(replicatedVol(volSrc, nodeA), existing).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirSnapshotGroup{}).
		Build()
	c := &Controller{Client: cl}

	_, err := c.CreateVolumeGroupSnapshot(t.Context(), &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{volSrc},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("member-set mismatch must be AlreadyExists, got %v", err)
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := cl.List(t.Context(), snaps); err != nil {
		t.Fatal(err)
	}
	if len(snaps.Items) != 0 {
		t.Fatalf("the refused create must leave no member snapshots: %+v", snaps.Items)
	}
}

// The racing twin: the conflicting group lands between the pre-check and
// the group Create. Members this RPC created that the winner does not
// own must be cleaned up; an overlapping member the winner owns must
// survive.
func TestCreateVolumeGroupSnapshotConflictCleansStrayMembers(t *testing.T) {
	s := newScheme(t)
	winner := &miroirv1alpha1.MiroirSnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: groupGsnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotGroupSpec{SnapshotNames: []string{groupGsnap1 + "-" + volSrc}},
	}
	groupGets := 0
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(replicatedVol(volSrc, nodeA), replicatedVol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirSnapshotGroup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if g, ok := obj.(*miroirv1alpha1.MiroirSnapshotGroup); ok && key.Name == groupGsnap1 {
					groupGets++
					if groupGets == 1 {
						// The pre-check races the winner: not visible yet.
						return apierrors.NewNotFound(schema.GroupResource{
							Group: "miroir.home-operations.com", Resource: "miroirsnapshotgroups"}, key.Name)
					}
					winner.DeepCopyInto(g)
					return nil
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*miroirv1alpha1.MiroirSnapshotGroup); ok {
					return apierrors.NewAlreadyExists(schema.GroupResource{
						Group: "miroir.home-operations.com", Resource: "miroirsnapshotgroups"}, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	c := &Controller{Client: cl}

	_, err := c.CreateVolumeGroupSnapshot(t.Context(), &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{volSrc, volPvc1},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("losing the create race to a different member set must be AlreadyExists, got %v", err)
	}
	// Members carry agent finalizers, so the fake client parks the stray
	// in Terminating instead of removing it; deletion-initiated is the
	// controller's whole job here.
	stray := &miroirv1alpha1.MiroirSnapshot{}
	err = cl.Get(t.Context(), types.NamespacedName{Name: groupGsnap1 + "-" + volPvc1}, stray)
	if err == nil && stray.DeletionTimestamp.IsZero() {
		t.Fatalf("the stray member the winner does not own must be cleaned up: %+v", stray.ObjectMeta)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	kept := &miroirv1alpha1.MiroirSnapshot{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: groupGsnap1 + "-" + volSrc}, kept); err != nil || !kept.DeletionTimestamp.IsZero() {
		t.Fatalf("the member the winner owns must survive the cleanup: %v %+v", err, kept.ObjectMeta)
	}
}

func TestCreateVolumeGroupSnapshotRejectsUnreplicated(t *testing.T) {
	s := newScheme(t)
	plain := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendZFS}},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(s).
		WithObjects(replicatedVol(volSrc, nodeA), plain).Build()
	c := &Controller{Client: cl}

	_, err := c.CreateVolumeGroupSnapshot(t.Context(), &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{volSrc, volPvc1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("an unreplicated member must be InvalidArgument, got %v", err)
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := cl.List(t.Context(), snaps); err != nil {
		t.Fatal(err)
	}
	if len(snaps.Items) != 0 {
		t.Fatalf("a refused group must not leave members behind: %+v", snaps.Items)
	}
}

func TestCreateVolumeGroupSnapshotMissingVolume(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).Build()}

	_, err := c.CreateVolumeGroupSnapshot(t.Context(), &csi.CreateVolumeGroupSnapshotRequest{
		Name: groupGsnap1, SourceVolumeIds: []string{"pvc-missing"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("a missing source volume must be NotFound, got %v", err)
	}
}

func TestGetVolumeGroupSnapshotReportsReadiness(t *testing.T) {
	s := newScheme(t)
	member := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: groupGsnap1 + "-" + volSrc},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc, Group: groupGsnap1},
		Status:     miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30},
	}
	grp := &miroirv1alpha1.MiroirSnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: groupGsnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotGroupSpec{SnapshotNames: []string{member.Name}},
		Status:     miroirv1alpha1.MiroirSnapshotGroupStatus{ReadyToUse: true},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(member, grp).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirSnapshotGroup{}).
		Build()
	c := &Controller{Client: cl}

	resp, err := c.GetVolumeGroupSnapshot(t.Context(), &csi.GetVolumeGroupSnapshotRequest{
		GroupSnapshotId: groupGsnap1, SnapshotIds: []string{member.Name},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetGroupSnapshot().GetReadyToUse() {
		t.Fatalf("sealed group with ready members must report ready: %+v", resp.GetGroupSnapshot())
	}

	_, err = c.GetVolumeGroupSnapshot(t.Context(), &csi.GetVolumeGroupSnapshotRequest{
		GroupSnapshotId: groupGsnap1, SnapshotIds: []string{"other-snap"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a member-list mismatch is detectable and must error, got %v", err)
	}

	_, err = c.GetVolumeGroupSnapshot(t.Context(), &csi.GetVolumeGroupSnapshotRequest{
		GroupSnapshotId: "gsnap-missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("a missing group must be NotFound, got %v", err)
	}
}

func TestDeleteVolumeGroupSnapshotDeletesMembersAndGroup(t *testing.T) {
	s := newScheme(t)
	m1 := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: groupGsnap1 + "-" + volSrc},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc, Group: groupGsnap1},
	}
	grp := &miroirv1alpha1.MiroirSnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: groupGsnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotGroupSpec{SnapshotNames: []string{m1.Name}},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(m1, grp).Build()
	c := &Controller{Client: cl}

	if _, err := c.DeleteVolumeGroupSnapshot(t.Context(), &csi.DeleteVolumeGroupSnapshotRequest{
		GroupSnapshotId: groupGsnap1, SnapshotIds: []string{m1.Name},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: m1.Name}, &miroirv1alpha1.MiroirSnapshot{}); !apierrors.IsNotFound(err) {
		t.Fatalf("member must be deleted: %v", err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: groupGsnap1}, &miroirv1alpha1.MiroirSnapshotGroup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("group must be deleted: %v", err)
	}

	// Idempotent retry after the group CR is gone: the CO's snapshot list
	// is the only member source left.
	if _, err := c.DeleteVolumeGroupSnapshot(t.Context(), &csi.DeleteVolumeGroupSnapshotRequest{
		GroupSnapshotId: groupGsnap1, SnapshotIds: []string{m1.Name},
	}); err != nil {
		t.Fatalf("retry after full deletion must succeed: %v", err)
	}
}

func TestDeleteVolumeGroupSnapshotRejectsForeignMember(t *testing.T) {
	s := newScheme(t)
	foreign := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc, Group: "other-group"},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(foreign).Build()
	c := &Controller{Client: cl}

	_, err := c.DeleteVolumeGroupSnapshot(t.Context(), &csi.DeleteVolumeGroupSnapshotRequest{
		GroupSnapshotId: groupGsnap1, SnapshotIds: []string{snapSnap1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a snapshot of another group must be refused, got %v", err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, &miroirv1alpha1.MiroirSnapshot{}); err != nil {
		t.Fatalf("the foreign snapshot must survive: %v", err)
	}
}

func TestDeleteSnapshotRefusesGroupMember(t *testing.T) {
	s := newScheme(t)
	member := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc, Group: groupGsnap1},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(member).Build()
	c := &Controller{Client: cl}

	_, err := c.DeleteSnapshot(t.Context(), &csi.DeleteSnapshotRequest{SnapshotId: snapSnap1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("deleting one leg of an atomic cut must be FailedPrecondition, got %v", err)
	}
}
