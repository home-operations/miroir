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

package agent

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/drbd"
)

func snapObj(name, volume string, nodes ...string) *homefsv1alpha1.HomefsSnapshot {
	finalizers := make([]string, 0, len(nodes))
	for _, n := range nodes {
		finalizers = append(finalizers, constants.FinalizerPrefix+n)
	}
	return &homefsv1alpha1.HomefsSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: finalizers},
		Spec:       homefsv1alpha1.HomefsSnapshotSpec{VolumeName: volume},
	}
}

//nolint:unparam // future tests will vary the name
func reconcileSnap(t *testing.T, r *SnapshotReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestSnapshotUnreplicatedReadyImmediately(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol("pvc-1", "kharkiv"), snapObj("snap-1", "pvc-1", "kharkiv")).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &SnapshotReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	reconcileSnap(t, r, "snap-1")

	got := &homefsv1alpha1.HomefsSnapshot{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.ReadyToUse {
		t.Fatalf("unreplicated snapshot must be ready after one pass: %+v", got.Status)
	}
}

func TestSnapshotReplicatedBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Minor: 1000, Port: 7000}
	v.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{
		"kharkiv": {DeviceCreated: true, DiskState: "UpToDate"},
		"paris":   {DeviceCreated: true, DiskState: "UpToDate"},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj("snap-1", "pvc-1", "kharkiv", "paris")).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	// kharkiv is Primary → coordinator: suspends, snapshots, marks Done.
	feK := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Primary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fbK := newFakeBackend()
	rK := &SnapshotReconciler{Client: c, NodeName: "kharkiv", Backend: fbK,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feK.run}}
	reconcileSnap(t, rK, "snap-1")
	feK.calledWith(t, "drbdadm suspend-io pvc-1")

	got := &homefsv1alpha1.HomefsSnapshot{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if !got.Status.IOSuspended || got.Status.PerNode["kharkiv"] != homefsv1alpha1.SnapshotDone {
		t.Fatalf("coordinator must suspend and snapshot first: %+v", got.Status)
	}
	if got.Status.ReadyToUse {
		t.Fatal("snapshot must not be ready before the peer leg exists")
	}

	// paris is Secondary → snapshots while suspended.
	feP := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fbP := newFakeBackend()
	rP := &SnapshotReconciler{Client: c, NodeName: "paris", Backend: fbP,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	reconcileSnap(t, rP, "snap-1")
	feP.notCalledWith(t, "suspend-io")

	// Coordinator sees both Done → resumes and marks ready.
	reconcileSnap(t, rK, "snap-1")
	feK.calledWith(t, "drbdadm resume-io pvc-1")
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if !got.Status.ReadyToUse || got.Status.IOSuspended {
		t.Fatalf("snapshot must be ready with IO resumed: %+v", got.Status)
	}
}

func TestSnapshotPeerWaitsForBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Minor: 1000, Port: 7000}
	v.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{
		"kharkiv": {DeviceCreated: true, DiskState: "UpToDate"},
		"paris":   {DeviceCreated: true, DiskState: "UpToDate"},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj("snap-1", "pvc-1", "kharkiv", "paris")).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	// paris, Secondary, barrier not raised → must not snapshot yet.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: "paris", Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-1")

	got := &homefsv1alpha1.HomefsSnapshot{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if got.Status.PerNode["paris"] == homefsv1alpha1.SnapshotDone {
		t.Fatal("peer must wait for the IO barrier")
	}
}
