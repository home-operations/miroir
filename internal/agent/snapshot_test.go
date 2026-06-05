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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/drbd"
)

//nolint:unparam // future tests will vary the volume
func snapObj(name, volume string, nodes ...string) *homefsv1alpha1.HomefsSnapshot {
	finalizers := make([]string, 0, len(nodes))
	for _, n := range nodes {
		finalizers = append(finalizers, constants.FinalizerPrefix+n)
	}
	return &homefsv1alpha1.HomefsSnapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: homefsv1alpha1.GroupVersion.String(),
			Kind:       "HomefsSnapshot",
		},
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
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{
		"kharkiv": {DeviceCreated: true, DiskState: "UpToDate"},
		"paris":   {DeviceCreated: true, DiskState: "UpToDate"},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj("snap-1", "pvc-1", "kharkiv", "paris")).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	// kharkiv is Primary → coordinator: raises its barrier first.
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
	if !got.Status.IOSuspended || got.Status.PerNode["kharkiv"] != homefsv1alpha1.SnapshotSuspended {
		t.Fatalf("coordinator must raise the barrier before cutting: %+v", got.Status)
	}
	if len(fbK.snapCalls) != 0 {
		t.Fatalf("no leg may be cut before every barrier is up: %v", fbK.snapCalls)
	}

	// paris (Secondary peer) raises its own barrier too.
	feP := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fbP := newFakeBackend()
	rP := &SnapshotReconciler{Client: c, NodeName: "paris", Backend: fbP,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	reconcileSnap(t, rP, "snap-1")
	feP.calledWith(t, "drbdadm suspend-io pvc-1")

	// All barriers up → each node cuts its leg.
	reconcileSnap(t, rK, "snap-1")
	if len(fbK.snapCalls) != 2 || fbK.snapCalls[1] != "snapshot pvc-1@snap-1" {
		t.Fatalf("coordinator must cut once all barriers are up: %v", fbK.snapCalls)
	}
	reconcileSnap(t, rP, "snap-1")
	if len(fbP.snapCalls) != 2 || fbP.snapCalls[1] != "snapshot pvc-1@snap-1" {
		t.Fatalf("peer must cut once all barriers are up: %v", fbP.snapCalls)
	}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if got.Status.ReadyToUse {
		t.Fatal("snapshot must not be ready before the coordinator collects")
	}

	// Coordinator sees both Done → resumes and marks ready.
	reconcileSnap(t, rK, "snap-1")
	feK.calledWith(t, "drbdadm resume-io pvc-1")
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if !got.Status.ReadyToUse || got.Status.IOSuspended {
		t.Fatalf("snapshot must be ready with IO resumed: %+v", got.Status)
	}

	// The peer's device is still suspended; readyToUse lifts it.
	reconcileSnap(t, rP, "snap-1")
	feP.calledWith(t, "drbdadm resume-io pvc-1")
}

// Regression: a Secondary that is replicas[0] must defer to a peer
// Primary — two coordinators livelock the snapshot.
func TestSnapshotSecondaryDefersToPeerPrimary(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "paris", "kharkiv") // paris is replicas[0]...
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{
		"kharkiv": {DeviceCreated: true, DiskState: "UpToDate"},
		"paris":   {DeviceCreated: true, DiskState: "UpToDate"},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj("snap-1", "pvc-1", "paris", "kharkiv")).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	// ...but kharkiv holds the device open: the barrier only blocks
	// writes where they originate, so kharkiv owns it.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected","peer-role":"Primary"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: "paris", Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-1")

	fe.notCalledWith(t, "suspend-io")
}

// Regression: an expired round voids every leg, the retry backs off,
// re-raises with peers reset, and recuts (delete before snapshot) —
// stale legs must never pair with fresh ones.
func TestSnapshotExpiredRoundResetsPeersAndRecuts(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{
		"kharkiv": {DeviceCreated: true, DiskState: "UpToDate"},
		"paris":   {DeviceCreated: true, DiskState: "UpToDate"},
	}
	snap := snapObj("snap-1", "pvc-1", "kharkiv", "paris")
	expired := metav1.NewTime(time.Now().Add(-2 * SuspendDeadline))
	snap.Status.IOSuspended = true
	snap.Status.SuspendedAt = &expired
	snap.Status.PerNode = map[string]homefsv1alpha1.SnapshotNodeState{
		"kharkiv": homefsv1alpha1.SnapshotDone, // cut under the dead barrier
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	fe := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Primary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: "kharkiv", Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	// Expiry: resume, void every leg, mark the coordinator Error.
	reconcileSnap(t, r, "snap-1")
	fe.calledWith(t, "drbdadm resume-io pvc-1")
	got := &homefsv1alpha1.HomefsSnapshot{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.IOSuspended || got.Status.ReadyToUse {
		t.Fatalf("expired round must resume without going ready: %+v", got.Status)
	}
	if got.Status.PerNode["paris"] != homefsv1alpha1.SnapshotPending ||
		got.Status.PerNode["kharkiv"] != homefsv1alpha1.SnapshotError {
		t.Fatalf("expired legs must be voided: %+v", got.Status.PerNode)
	}

	// The void restamps suspendedAt so the retry backoff is real: an
	// immediate reconcile must not re-raise the barrier.
	reconcileSnap(t, r, "snap-1")
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if got.Status.IOSuspended {
		t.Fatalf("retry must back off before re-raising the barrier: %+v", got.Status)
	}

	// Age past the backoff; a slow peer's Done from the voided round
	// lands late and must be voided again when the next round opens.
	aged := metav1.NewTime(time.Now().Add(-2 * suspendRetryBackoff))
	got.Status.SuspendedAt = &aged
	got.Status.PerNode["paris"] = homefsv1alpha1.SnapshotDone
	if err := c.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}

	// Retry: re-raise (no cut, peers reset) → peer raises → recut.
	reconcileSnap(t, r, "snap-1")
	if len(fb.snapCalls) != 0 {
		t.Fatalf("no recut before every barrier is up: %v", fb.snapCalls)
	}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "snap-1"}, got)
	if got.Status.PerNode["paris"] != homefsv1alpha1.SnapshotPending {
		t.Fatalf("opening a round must void stale peer legs: %+v", got.Status.PerNode)
	}
	feP := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	rP := &SnapshotReconciler{Client: c, NodeName: "paris", Backend: newFakeBackend(),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	reconcileSnap(t, rP, "snap-1")
	reconcileSnap(t, r, "snap-1")
	want := []string{"delete pvc-1@snap-1", "snapshot pvc-1@snap-1"}
	if len(fb.snapCalls) != 2 || fb.snapCalls[0] != want[0] || fb.snapCalls[1] != want[1] {
		t.Fatalf("retry must delete before recutting, got %v", fb.snapCalls)
	}
}

// Regression: a volume whose peer link is down writes alone (quorum
// off) — no barrier and no cut until replication is healthy again.
func TestSnapshotWaitsForHealthyReplication(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]homefsv1alpha1.ReplicaStatus{
		"kharkiv": {DeviceCreated: true, DiskState: "UpToDate"},
		"paris":   {DeviceCreated: true, DiskState: "UpToDate"},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj("snap-1", "pvc-1", "kharkiv", "paris")).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	// Primary and locally UpToDate, but the peer link is still down.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1","role":"Primary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connecting"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: "kharkiv", Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-1")

	fe.notCalledWith(t, "suspend-io")
	if len(fb.snapCalls) != 0 {
		t.Fatalf("no leg may be cut while replication is degraded: %v", fb.snapCalls)
	}
}

// Regression: a snapshot deleted while its barrier is up must resume IO
// on the way out — nothing else ever would.
func TestSnapshotDeleteResumesStrandedBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	snap := snapObj("snap-del", "pvc-1", "kharkiv")
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	snap.Status.IOSuspended = true
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&homefsv1alpha1.HomefsSnapshot{}, &homefsv1alpha1.HomefsVolume{}).
		Build()

	fe := &fakeDRBDExec{}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: "kharkiv", Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-del")

	fe.calledWith(t, "drbdadm resume-io pvc-1")
}

func TestSnapshotPeerWaitsForBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
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
