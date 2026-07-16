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
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

const (
	snapCallSnapshot = "snapshot " + volPvc1 + "@" + snapSnap1
	// cmdStatus keys fakeDRBDExec.errOn for `drbdsetup status`.
	cmdStatus = "status"
)

//nolint:unparam // future tests will vary the volume
func snapObj(name, volume string, nodes ...string) *miroirv1alpha1.MiroirSnapshot {
	finalizers := make([]string, 0, len(nodes))
	for _, n := range nodes {
		finalizers = append(finalizers, constants.FinalizerPrefix+n)
	}
	return &miroirv1alpha1.MiroirSnapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: miroirv1alpha1.GroupVersion.String(),
			Kind:       "MiroirSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: finalizers},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volume},
	}
}

//nolint:unparam // future tests will vary the name
func reconcileSnap(t *testing.T, r *SnapshotReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestSnapshotUnreplicatedReadyImmediately(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA)
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	reconcileSnap(t, r, snapSnap1)

	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.ReadyToUse {
		t.Fatalf("unreplicated snapshot must be ready after one pass: %+v", got.Status)
	}
}

// A snapshot scheduled before the volume reconciler created the backing
// device (the two reconcilers race at startup, issue #195) must wait
// quietly instead of error-looping Sync on the missing device.
func TestSnapshotUnreplicatedWaitsForBackingDevice(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, nodeA), snapObj(snapSnap1, volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	res := reconcileSnap(t, r, snapSnap1)

	if res.RequeueAfter != 5*time.Second {
		t.Fatalf("want a 5s wait for the backing device, got %v", res.RequeueAfter)
	}
	if len(fb.snapCalls) != 0 {
		t.Fatalf("must not touch the backend before the device exists: %v", fb.snapCalls)
	}
	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ReadyToUse {
		t.Fatal("snapshot must not report ready before the device exists")
	}
}

// Regression: replicas[0] (the default coordinator) is dead mid-round. A
// surviving diskful replica must take over as coordinator and void the
// expired round — otherwise status.IOSuspended stays true forever and
// every future snapshot of the volume queues behind it.
func TestSnapshotCoordinatorFailsOverFromDeadReplica0(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB) // node-a = replicas[0]
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	snap := snapObj(snapSnap1, volPvc1, nodeA, nodeB)
	expired := metav1.NewTime(time.Now().Add(-2 * SuspendDeadline))
	snap.Status.IOSuspended = true
	snap.Status.SuspendedAt = &expired
	snap.Status.PerNode = map[string]miroirv1alpha1.SnapshotNodeState{
		nodeB: miroirv1alpha1.SnapshotSuspended,
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// node-b: Secondary, no Primary anywhere, and its link to node-a
	// (node-id 0) is down — node-b is now the lowest reachable diskful leg.
	feP := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connecting"}]}]`}
	rP := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	reconcileSnap(t, rP, snapSnap1)

	feP.calledWith(t, "drbdadm resume-io pvc-1")
	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.IOSuspended {
		t.Fatalf("survivor coordinator must void the dead round: %+v", got.Status)
	}
}

// A suspend-io that fails persistently (e.g. a wedged kernel module,
// LINBIT/drbd#137) must not ride the workqueue's exponential backoff
// forever: after barrierFailLimit consecutive failures the round parks at
// barrierRetryAfter with a BarrierStuck warning.
func TestSnapshotBarrierStuckParksAfterLimit(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{
		statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
			"connections":[{"connection-state":"Connected"}]}]`,
		errOn: map[string]error{"suspend-io": errors.New(
			"exit status 20: Command 'drbdsetup suspend-io 1001' did not terminate within 5 seconds")},
	}
	rec := events.NewFakeRecorder(8)
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}, Recorder: rec}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: snapSnap1}}
	for i := 1; i < barrierFailLimit; i++ {
		if _, err := r.Reconcile(t.Context(), req); err == nil {
			t.Fatalf("failure %d must surface for the fast backoff", i)
		}
	}
	res, err := r.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("failure %d must park the retry, not error: %v", barrierFailLimit, err)
	}
	if res.RequeueAfter != barrierRetryAfter {
		t.Fatalf("want %v parked retry, got %v", barrierRetryAfter, res.RequeueAfter)
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "BarrierStuck") {
			t.Fatalf("want a BarrierStuck warning, got %q", e)
		}
	default:
		t.Fatal("want a BarrierStuck warning event")
	}
}

// On a wedged module the status read is the call that fails (hanging
// until execTimeout kills it); it must ride the same bounded retry as
// suspend-io or the give-up never engages.
func TestSnapshotStatusFailureParksAfterLimit(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{errOn: map[string]error{cmdStatus: errors.New("signal: killed")}}
	rec := events.NewFakeRecorder(8)
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}, Recorder: rec}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: snapSnap1}}
	for i := 1; i < barrierFailLimit; i++ {
		if _, err := r.Reconcile(t.Context(), req); err == nil {
			t.Fatalf("failure %d must surface for the fast backoff", i)
		}
	}
	res, err := r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != barrierRetryAfter {
		t.Fatalf("want parked retry %v, got %v / %v", barrierRetryAfter, res.RequeueAfter, err)
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "BarrierStuck") {
			t.Fatalf("want a BarrierStuck warning, got %q", e)
		}
	default:
		t.Fatal("want a BarrierStuck warning event")
	}
}

// The failure count must die with the snapshot on the volume-already-gone
// finalizer path too, or a later snapshot reusing the name inherits it
// pre-parked.
func TestSnapshotVolGoneClearsBarrierFails(t *testing.T) {
	s := newScheme(t)
	snap := snapObj(snapSnap1, volPvc1, nodeA)
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}).Build()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD:         &drbd.Driver{StateDir: t.TempDir(), Exec: (&fakeDRBDExec{}).run},
		barrierFails: map[string]int{snapSnap1: 3}}

	reconcileSnap(t, r, snapSnap1)

	r.barrierFailsMu.Lock()
	_, leaked := r.barrierFails[snapSnap1]
	r.barrierFailsMu.Unlock()
	if leaked {
		t.Fatal("the failure count must die with the snapshot on the vol-gone path")
	}
}

func TestSnapshotReplicatedBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// node-a is Primary → coordinator: raises its barrier first.
	feK := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fbK := newFakeBackend()
	rK := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fbK),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feK.run}}
	reconcileSnap(t, rK, snapSnap1)
	feK.calledWith(t, "drbdadm suspend-io pvc-1")

	got := &miroirv1alpha1.MiroirSnapshot{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got)
	if !got.Status.IOSuspended || got.Status.PerNode[nodeA] != miroirv1alpha1.SnapshotSuspended {
		t.Fatalf("coordinator must raise the barrier before cutting: %+v", got.Status)
	}
	if len(fbK.snapCalls) != 0 {
		t.Fatalf("no leg may be cut before every barrier is up: %v", fbK.snapCalls)
	}

	// node-b (Secondary peer) raises its own barrier too.
	feP := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fbP := newFakeBackend()
	rP := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fbP),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	reconcileSnap(t, rP, snapSnap1)
	feP.calledWith(t, "drbdadm suspend-io pvc-1")

	// All barriers up → each node cuts its leg.
	reconcileSnap(t, rK, snapSnap1)
	if len(fbK.snapCalls) != 2 || fbK.snapCalls[1] != snapCallSnapshot {
		t.Fatalf("coordinator must cut once all barriers are up: %v", fbK.snapCalls)
	}
	reconcileSnap(t, rP, snapSnap1)
	if len(fbP.snapCalls) != 2 || fbP.snapCalls[1] != snapCallSnapshot {
		t.Fatalf("peer must cut once all barriers are up: %v", fbP.snapCalls)
	}
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got)
	if got.Status.ReadyToUse {
		t.Fatal("snapshot must not be ready before the coordinator collects")
	}

	// Coordinator sees both Done → resumes and marks ready.
	reconcileSnap(t, rK, snapSnap1)
	feK.calledWith(t, "drbdadm resume-io pvc-1")
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got)
	if !got.Status.ReadyToUse || got.Status.IOSuspended {
		t.Fatalf("snapshot must be ready with IO resumed: %+v", got.Status)
	}

	// The peer's device is still suspended; readyToUse lifts it.
	reconcileSnap(t, rP, snapSnap1)
	feP.calledWith(t, "drbdadm resume-io pvc-1")
}

// A snapshot round on a volume with a diskless tie-breaker completes on
// the diskful legs alone: the tie-breaker never raises a barrier or cuts
// a leg (it has none), and the coordinator must not wait for it. Its
// deletion path releases the finalizer without touching the backend.
func TestSnapshotBarrierWithTieBreaker(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, Diskless: true,
	})
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeC: {DiskState: diskStateDiskless},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB, nodeC)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	feK := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"},{"connection-state":"Connected"}]}]`}
	rK := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feK.run}}
	feP := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"},{"connection-state":"Connected"}]}]`}
	rP := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	fbO := newFakeBackend()
	feO := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateDiskless + `"}],
		"connections":[{"connection-state":"Connected"},{"connection-state":"Connected"}]}]`}
	rO := &SnapshotReconciler{Client: c, NodeName: nodeC, Pools: poolsOf(fbO),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feO.run}}

	// Round: coordinator raises, peer raises, both cut, coordinator
	// collects — with the tie-breaker reconciling in between and never
	// contributing a leg.
	reconcileSnap(t, rK, snapSnap1)
	reconcileSnap(t, rO, snapSnap1)
	reconcileSnap(t, rP, snapSnap1)
	reconcileSnap(t, rO, snapSnap1)
	reconcileSnap(t, rK, snapSnap1)
	reconcileSnap(t, rP, snapSnap1)
	reconcileSnap(t, rK, snapSnap1)

	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.ReadyToUse || got.Status.IOSuspended {
		t.Fatalf("round must complete on diskful legs alone: %+v", got.Status)
	}
	if len(fbO.snapCalls) != 0 {
		t.Fatalf("tie-breaker must not touch the backend: %v", fbO.snapCalls)
	}
	feO.notCalledWith(t, "drbdadm suspend-io")

	// Deletion: the tie-breaker's agent releases its finalizer without a
	// backend DeleteSnapshot.
	if err := c.Delete(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcileSnap(t, rO, snapSnap1)
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeC) {
		t.Fatal("tie-breaker must release its snapshot finalizer on delete")
	}
	if len(fbO.snapCalls) != 0 {
		t.Fatalf("tie-breaker deletion must not call the backend: %v", fbO.snapCalls)
	}
}

// Regression: a snapshot round must proceed while the tie-breaker is
// DISCONNECTED — quorum still holds 2/3 and both data legs are healthy,
// which is exactly the degraded mode the tie-breaker exists to survive.
// The old aggregate-Connected gate blocked every round here.
func TestSnapshotProceedsWithTieBreakerDown(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	// Full addresses + node ids so the diskful-connectivity check
	// actually consults the per-peer map (address-less entries are
	// skipped as not-yet-rendered).
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, Diskless: true,
	})
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeC: {DiskState: diskStateDiskless},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB, nodeC)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// Both diskful views: data link Connected, tie-breaker link down.
	feK := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"},
			{"peer-node-id":2,"connection-state":"Connecting"}]}]`}
	rK := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feK.run}}
	feP := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected"},
			{"peer-node-id":2,"connection-state":"Connecting"}]}]`}
	rP := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}

	reconcileSnap(t, rK, snapSnap1)
	feK.calledWith(t, "drbdadm suspend-io pvc-1") // barrier raised despite the down tie-breaker
	reconcileSnap(t, rP, snapSnap1)
	reconcileSnap(t, rK, snapSnap1)
	reconcileSnap(t, rP, snapSnap1)
	reconcileSnap(t, rK, snapSnap1)

	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.ReadyToUse || got.Status.IOSuspended {
		t.Fatalf("round must complete with the tie-breaker down: %+v", got.Status)
	}
}

// A peer raising its barrier must record only its own slot, never the
// coordinator's round fields: a full-status apply could revert a resume or
// void the coordinator raced, re-freezing IO or resurrecting a stale leg.
func TestSnapshotPeerRecordsOnlyOwnSlot(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	// The coordinator has opened the round; node-b has not raised its barrier.
	snap := snapObj(snapSnap1, volPvc1, nodeA, nodeB)
	now := metav1.Now()
	snap.Status.IOSuspended = true
	snap.Status.SuspendedAt = &now
	snap.Status.PerNode = map[string]miroirv1alpha1.SnapshotNodeState{
		nodeA: miroirv1alpha1.SnapshotSuspended,
		nodeB: miroirv1alpha1.SnapshotPending,
	}

	var patchTypes []types.PatchType
	var patchData []string
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				data, _ := patch.Data(obj)
				patchTypes = append(patchTypes, patch.Type())
				patchData = append(patchData, string(data))
				return cl.Status().Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	feP := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	rP := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}

	reconcileSnap(t, rP, snapSnap1)
	feP.calledWith(t, "drbdadm suspend-io pvc-1")

	if len(patchData) == 0 {
		t.Fatal("expected a status patch from the peer")
	}
	for i, data := range patchData {
		if patchTypes[i] != types.MergePatchType {
			t.Errorf("patch %d is %q, want a merge patch (a peer must not apply the whole status)", i, patchTypes[i])
		}
		if strings.Contains(data, "ioSuspended") || strings.Contains(data, "suspendedAt") {
			t.Errorf("patch %d touches the coordinator's barrier fields: %s", i, data)
		}
		if !strings.Contains(data, nodeB) {
			t.Errorf("patch %d does not record this node's slot: %s", i, data)
		}
	}

	// The coordinator's barrier survives the peer's write.
	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.IOSuspended {
		t.Fatal("peer must not clear the coordinator's barrier")
	}
	if got.Status.PerNode[nodeB] != miroirv1alpha1.SnapshotSuspended {
		t.Fatalf("peer's own slot not recorded: %+v", got.Status.PerNode)
	}
}

// Regression: a Secondary that is replicas[0] must defer to a peer
// Primary — two coordinators livelock the snapshot.
func TestSnapshotSecondaryDefersToPeerPrimary(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeB, nodeA) // node-b is replicas[0]...
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeB, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// ...but node-a holds the device open: the barrier only blocks
	// writes where they originate, so node-a owns it.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected","peer-role":"Primary"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, snapSnap1)

	fe.notCalledWith(t, "suspend-io")
}

// Regression: an expired round voids every leg, the retry backs off,
// re-raises with peers reset, and recuts (delete before snapshot) —
// stale legs must never pair with fresh ones.
func TestSnapshotExpiredRoundResetsPeersAndRecuts(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	snap := snapObj(snapSnap1, volPvc1, nodeA, nodeB)
	expired := metav1.NewTime(time.Now().Add(-2 * SuspendDeadline))
	snap.Status.IOSuspended = true
	snap.Status.SuspendedAt = &expired
	snap.Status.PerNode = map[string]miroirv1alpha1.SnapshotNodeState{
		nodeA: miroirv1alpha1.SnapshotDone, // cut under the dead barrier
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	// Expiry: resume, void every leg, mark the coordinator Error.
	reconcileSnap(t, r, snapSnap1)
	fe.calledWith(t, "drbdadm resume-io pvc-1")
	got := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.IOSuspended || got.Status.ReadyToUse {
		t.Fatalf("expired round must resume without going ready: %+v", got.Status)
	}
	if got.Status.PerNode[nodeB] != miroirv1alpha1.SnapshotPending ||
		got.Status.PerNode[nodeA] != miroirv1alpha1.SnapshotError {
		t.Fatalf("expired legs must be voided: %+v", got.Status.PerNode)
	}

	// The void restamps suspendedAt so the retry backoff is real: an
	// immediate reconcile must not re-raise the barrier.
	reconcileSnap(t, r, snapSnap1)
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got)
	if got.Status.IOSuspended {
		t.Fatalf("retry must back off before re-raising the barrier: %+v", got.Status)
	}

	// Age past the backoff; a slow peer's Done from the voided round
	// lands late and must be voided again when the next round opens.
	aged := metav1.NewTime(time.Now().Add(-2 * suspendRetryBackoff))
	got.Status.SuspendedAt = &aged
	got.Status.PerNode[nodeB] = miroirv1alpha1.SnapshotDone
	if err := c.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}

	// Retry: re-raise (no cut, peers reset) → peer raises → recut.
	reconcileSnap(t, r, snapSnap1)
	if len(fb.snapCalls) != 0 {
		t.Fatalf("no recut before every barrier is up: %v", fb.snapCalls)
	}
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got)
	if got.Status.PerNode[nodeB] != miroirv1alpha1.SnapshotPending {
		t.Fatalf("opening a round must void stale peer legs: %+v", got.Status.PerNode)
	}
	feP := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	rP := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}
	reconcileSnap(t, rP, snapSnap1)
	reconcileSnap(t, r, snapSnap1)
	want := []string{"delete pvc-1@snap-1", snapCallSnapshot}
	if len(fb.snapCalls) != 2 || fb.snapCalls[0] != want[0] || fb.snapCalls[1] != want[1] {
		t.Fatalf("retry must delete before recutting, got %v", fb.snapCalls)
	}
}

// Regression: a volume whose peer link is down writes alone (quorum
// off) — no barrier and no cut until replication is healthy again.
func TestSnapshotWaitsForHealthyReplication(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	// Addresses + node ids: the health gate checks the links to diskful
	// peers by node id, and skips entries the membership reconciler has
	// not completed yet.
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// Primary and locally UpToDate, but the data-peer link is still down.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connecting"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, snapSnap1)

	fe.notCalledWith(t, "suspend-io")
	if len(fb.snapCalls) != 0 {
		t.Fatalf("no leg may be cut while replication is degraded: %v", fb.snapCalls)
	}
}

// Regression: a snapshot deleted while its barrier is up must resume IO
// on the way out — nothing else ever would. The lift keys on the kernel
// suspend flag, so it fires even when status.ioSuspended is already false
// (a peer's barrier outliving the coordinator's void).
func TestSnapshotDeleteResumesStrandedBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	snap := snapObj("snap-del", volPvc1, nodeA)
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-del")

	fe.calledWith(t, "drbdadm resume-io pvc-1")
}

// assertFinalizerReleased passes when the node's finalizer is gone —
// including when releasing the last finalizer let the fake client delete
// the object outright.
func assertFinalizerReleased(t *testing.T, c client.Client, node string) {
	const name = "snap-del"
	t.Helper()
	got := &miroirv1alpha1.MiroirSnapshot{}
	err := c.Get(t.Context(), types.NamespacedName{Name: name}, got)
	if apierrors.IsNotFound(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got.Finalizers, constants.FinalizerPrefix+node) {
		t.Fatalf("finalizer must be released: %v", got.Finalizers)
	}
}

// A snapshot deleted after this node's volume teardown already ran Down
// must not wedge on the barrier lift: Status fails on the torn-down
// resource, so nothing is suspended and deletion proceeds — the volume
// teardown's ErrBusy retry then completes. (Deadlock regression.)
func TestSnapshotDeleteToleratesDownedResource(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	snap := snapObj("snap-del", volPvc1, nodeA)
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	snap.Status.IOSuspended = true // stranded by the dead round
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{errOn: map[string]error{cmdStatus: errors.New("no such resource")}}
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-del")

	fe.notCalledWith(t, "resume-io")
	assertFinalizerReleased(t, c, nodeA)
}

// A node that left spec.replicas after the snapshot was cut still holds
// its finalizer; deletion must release it or the snapshot wedges in
// Terminating and blocks every later replica removal on the volume.
func TestSnapshotDeleteReleasesDepartedNode(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB) // node-c already removed
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	snap := snapObj("snap-del", volPvc1, nodeA, nodeB, nodeC)
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{errOn: map[string]error{cmdStatus: errors.New("no such resource")}}
	r := &SnapshotReconciler{Client: c, NodeName: nodeC, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-del")

	assertFinalizerReleased(t, c, nodeC)
}

// Regression (review): the departed leg's pool is unknowable (its status
// slot was deleted at removal), so deletion sweeps every pool — resolving
// one would guess "default" and wedge forever on a node without one.
func TestSnapshotDeleteReleasesDepartedNodeWithoutDefaultPool(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB) // node-c already removed
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	snap := snapObj("snap-del", volPvc1, nodeA, nodeB, nodeC)
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{errOn: map[string]error{cmdStatus: errors.New("no such resource")}}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeC,
		Pools: Pools{"fast": {Backend: fb, Type: miroirv1alpha1.BackendLVMThin}},
		DRBD:  &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-del")

	assertFinalizerReleased(t, c, nodeC)
	if len(fb.snapCalls) == 0 {
		t.Fatal("the sweep must attempt DeleteSnapshot in the node's pools")
	}
}

// The kernel suspend flag is shared per resource: while a sibling
// snapshot's round is live, a deleting snapshot must not lift the
// barrier out from under it — but it still deletes its leg and departs.
func TestSnapshotDeleteSkipsSiblingRoundBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	snap := snapObj("snap-del", volPvc1, nodeA)
	now := metav1.NewTime(time.Now())
	snap.DeletionTimestamp = &now
	sibling := snapObj("snap-live", volPvc1, nodeA, nodeB)
	sibling.Status.IOSuspended = true
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap, sibling).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}]}]`}
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-del")

	fe.notCalledWith(t, "resume-io")
	assertFinalizerReleased(t, c, nodeA)
}

// One round per volume: a coordinator must not open a round while a
// sibling snapshot of the same volume is mid-round.
func TestSnapshotRoundWaitsForSibling(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	fresh := snapObj("snap-b", volPvc1, nodeA, nodeB)
	sibling := snapObj("snap-a", volPvc1, nodeA, nodeB)
	sibling.Status.IOSuspended = true
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, fresh, sibling).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	res := reconcileSnap(t, r, "snap-b")

	fe.notCalledWith(t, "suspend-io")
	if res.RequeueAfter == 0 {
		t.Fatal("must requeue to wait for the sibling round to close")
	}
}

// A voided round's leftover barrier must stay up while a sibling round
// owns it — the sibling's own protocol lifts it.
func TestSnapshotVoidedResumeDefersToSiblingRound(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	voided := snapObj("snap-a", volPvc1, nodeA, nodeB) // round voided
	sibling := snapObj("snap-b", volPvc1, nodeA, nodeB)
	sibling.Status.IOSuspended = true
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, voided, sibling).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// node-b: Secondary, not replicas[0] → not coordinator; barrier up.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	r := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, "snap-a")

	fe.notCalledWith(t, "resume-io")
}

func TestSnapshotPeerWaitsForBarrier(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, volPvc1, nodeA, nodeB)).
		WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}, &miroirv1alpha1.MiroirVolume{}).
		Build()

	// node-b, Secondary, barrier not raised → must not snapshot yet.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	reconcileSnap(t, r, snapSnap1)

	got := &miroirv1alpha1.MiroirSnapshot{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, got)
	if got.Status.PerNode[nodeB] == miroirv1alpha1.SnapshotDone {
		t.Fatal("peer must wait for the IO barrier")
	}
}

// The single-worker reconciler must bound each DRBD control call on the
// barrier path tighter than RealExec's 2-minute default, so one wedged call
// cannot stall every other volume's snapshot on the node. The injected exec
// records the deadline each wrapped call carries.
func TestSnapshotBarrierCallsAreBounded(t *testing.T) {
	var deadlines []time.Duration
	capture := func(ctx context.Context, _ string, _ ...string) (string, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Error("snapshot DRBD call reached exec with no deadline")
			return "[]", nil
		}
		deadlines = append(deadlines, time.Until(dl))
		return "[]", nil
	}
	r := &SnapshotReconciler{DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: capture}}

	ctx := context.Background()
	_, _ = r.drbdStatus(ctx, volPvc1)
	_ = r.suspendIO(ctx, volPvc1)
	_ = r.resumeIO(ctx, volPvc1)

	if len(deadlines) != 3 {
		t.Fatalf("expected 3 bounded DRBD calls, got %d", len(deadlines))
	}
	for _, d := range deadlines {
		if d <= 0 || d > drbdBarrierTimeout+time.Second {
			t.Fatalf("call deadline %s is not bounded by drbdBarrierTimeout (%s)", d, drbdBarrierTimeout)
		}
	}
}
