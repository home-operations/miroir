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
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

const (
	volPvc2   = "pvc-2"
	groupG1   = "gsnap-1"
	memberOf1 = groupG1 + "-" + volPvc1
	memberOf2 = groupG1 + "-" + volPvc2
)

// groupVol is a replicated two-node volume ready for a barrier round.
func groupVol(name string) *miroirv1alpha1.MiroirVolume {
	v := vol(name, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate},
	}
	return v
}

func memberObj(name, volume string) *miroirv1alpha1.MiroirSnapshot {
	m := snapObj(name, volume, nodeA, nodeB)
	m.Spec.Group = groupG1
	return m
}

func groupObj() *miroirv1alpha1.MiroirSnapshotGroup {
	return &miroirv1alpha1.MiroirSnapshotGroup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: miroirv1alpha1.GroupVersion.String(),
			Kind:       "MiroirSnapshotGroup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: groupG1,
			Finalizers: []string{
				constants.FinalizerPrefix + nodeA,
				constants.FinalizerPrefix + nodeB,
			},
		},
		Spec: miroirv1alpha1.MiroirSnapshotGroupSpec{
			SnapshotNames: []string{memberOf1, memberOf2},
		},
	}
}

func groupClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(
			&miroirv1alpha1.MiroirSnapshot{},
			&miroirv1alpha1.MiroirSnapshotGroup{},
			&miroirv1alpha1.MiroirVolume{}).
		Build()
}

func reconcileGroup(t *testing.T, r *GroupSnapshotReconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: groupG1}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

const (
	groupStatusPrimary = `[{"name":"any","role":"Primary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`
	groupStatusPeer = `[{"name":"any","role":"Secondary","suspended-user":true,
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`
)

// The full two-node, two-volume round: every leg of every volume is
// frozen before any leg is cut, no volume resumes before every leg is
// cut, and the members publish ready together.
func TestGroupSnapshotBarrierSpansVolumes(t *testing.T) {
	c := groupClient(t, groupVol(volPvc1), groupVol(volPvc2),
		memberObj(memberOf1, volPvc1), memberObj(memberOf2, volPvc2), groupObj())

	feK := &fakeDRBDExec{statusJSON: groupStatusPrimary}
	fbK := newFakeBackend()
	rK := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fbK),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feK.run}}
	feP := &fakeDRBDExec{statusJSON: groupStatusPeer}
	fbP := newFakeBackend()
	rP := &GroupSnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fbP),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run}}

	// Driver (Primary on the first member volume) opens the round over
	// BOTH volumes.
	reconcileGroup(t, rK)
	feK.calledWith(t, "drbdadm suspend-io "+volPvc1)
	feK.calledWith(t, "drbdadm suspend-io "+volPvc2)
	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: groupG1}, grp)
	if !grp.Status.IOSuspended ||
		grp.Status.PerLeg[slotKey(volPvc1, nodeA)] != miroirv1alpha1.SnapshotSuspended ||
		grp.Status.PerLeg[slotKey(volPvc2, nodeA)] != miroirv1alpha1.SnapshotSuspended ||
		grp.Status.PerLeg[slotKey(volPvc1, nodeB)] != miroirv1alpha1.SnapshotPending {
		t.Fatalf("driver must open the round over every leg: %+v", grp.Status)
	}
	if len(fbK.snapCalls) != 0 {
		t.Fatalf("no leg may be cut before every barrier everywhere is up: %v", fbK.snapCalls)
	}

	// Peer raises its barriers on both volumes.
	reconcileGroup(t, rP)
	feP.calledWith(t, "drbdadm suspend-io "+volPvc1)
	feP.calledWith(t, "drbdadm suspend-io "+volPvc2)
	if len(fbP.snapCalls) != 0 {
		t.Fatalf("peer must not cut while its patch is the newest state: %v", fbP.snapCalls)
	}

	// Every slot Suspended → both nodes cut both legs.
	reconcileGroup(t, rK)
	if len(fbK.snapCalls) != 4 ||
		fbK.snapCalls[1] != "snapshot "+volPvc1+"@"+memberOf1 ||
		fbK.snapCalls[3] != "snapshot "+volPvc2+"@"+memberOf2 {
		t.Fatalf("driver must delete-then-cut each of its legs: %v", fbK.snapCalls)
	}
	reconcileGroup(t, rP)
	if len(fbP.snapCalls) != 4 {
		t.Fatalf("peer must cut each of its legs: %v", fbP.snapCalls)
	}
	_ = c.Get(t.Context(), types.NamespacedName{Name: groupG1}, grp)
	if grp.Status.ReadyToUse {
		t.Fatal("group must not be ready before the driver collects")
	}

	// Driver sees every slot Done → resumes, publishes members, seals.
	reconcileGroup(t, rK)
	feK.calledWith(t, "drbdadm resume-io "+volPvc1)
	feK.calledWith(t, "drbdadm resume-io "+volPvc2)
	_ = c.Get(t.Context(), types.NamespacedName{Name: groupG1}, grp)
	if !grp.Status.ReadyToUse || grp.Status.IOSuspended {
		t.Fatalf("group must seal ready with IO resumed: %+v", grp.Status)
	}
	for _, name := range []string{memberOf1, memberOf2} {
		m := &miroirv1alpha1.MiroirSnapshot{}
		if err := c.Get(t.Context(), types.NamespacedName{Name: name}, m); err != nil {
			t.Fatal(err)
		}
		if !m.Status.ReadyToUse || m.Status.SizeBytes != 1<<30 ||
			m.Status.PerNode[nodeA] != miroirv1alpha1.SnapshotDone ||
			m.Status.PerNode[nodeB] != miroirv1alpha1.SnapshotDone {
			t.Fatalf("member %s must publish with every leg Done: %+v", name, m.Status)
		}
	}

	// The peer's devices are still suspended; the sealed group lifts them.
	reconcileGroup(t, rP)
	feP.calledWith(t, "drbdadm resume-io "+volPvc1)
	feP.calledWith(t, "drbdadm resume-io "+volPvc2)
}

// A round whose deadline passes with legs missing is voided whole: Done
// slots included — they were cut under a barrier that failed.
func TestGroupSnapshotVoidsExpiredRound(t *testing.T) {
	grp := groupObj()
	expired := metav1.NewTime(time.Now().Add(-2 * SuspendDeadline))
	grp.Status.IOSuspended = true
	grp.Status.SuspendedAt = &expired
	grp.Status.PerLeg = map[string]miroirv1alpha1.SnapshotNodeState{
		slotKey(volPvc1, nodeA): miroirv1alpha1.SnapshotDone,
		slotKey(volPvc2, nodeA): miroirv1alpha1.SnapshotDone,
		slotKey(volPvc1, nodeB): miroirv1alpha1.SnapshotSuspended,
		slotKey(volPvc2, nodeB): miroirv1alpha1.SnapshotPending,
	}
	c := groupClient(t, groupVol(volPvc1), groupVol(volPvc2),
		memberObj(memberOf1, volPvc1), memberObj(memberOf2, volPvc2), grp)

	fe := &fakeDRBDExec{statusJSON: groupStatusPrimary}
	r := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	reconcileGroup(t, r)

	fe.calledWith(t, "drbdadm resume-io "+volPvc1)
	fe.calledWith(t, "drbdadm resume-io "+volPvc2)
	got := &miroirv1alpha1.MiroirSnapshotGroup{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: groupG1}, got)
	if got.Status.IOSuspended || got.Status.ReadyToUse {
		t.Fatalf("expired round must be voided, not sealed: %+v", got.Status)
	}
	if got.Status.PerLeg[slotKey(volPvc1, nodeB)] != miroirv1alpha1.SnapshotPending ||
		got.Status.PerLeg[slotKey(volPvc1, nodeA)] != miroirv1alpha1.SnapshotError {
		t.Fatalf("the void must reset every slot (driver's to Error): %+v", got.Status.PerLeg)
	}
}

// The group collect must not lift a shared volume's barrier while a
// standalone sibling round co-holds it — the opens can race past each
// other's guards, and lifting would cut the sibling's remaining legs
// over live writes. The rest resumes, the group still seals, and the
// deferred barrier lifts once the sibling closes.
func TestGroupCollectDefersResumeToSiblingRound(t *testing.T) {
	grp := groupObj()
	now := metav1.Now()
	grp.Status.IOSuspended = true
	grp.Status.SuspendedAt = &now
	grp.Status.PerLeg = map[string]miroirv1alpha1.SnapshotNodeState{
		slotKey(volPvc1, nodeA): miroirv1alpha1.SnapshotDone,
		slotKey(volPvc2, nodeA): miroirv1alpha1.SnapshotDone,
		slotKey(volPvc1, nodeB): miroirv1alpha1.SnapshotDone,
		slotKey(volPvc2, nodeB): miroirv1alpha1.SnapshotDone,
	}
	sibling := snapObj("snap-solo", volPvc1, nodeA, nodeB)
	sibling.Status.IOSuspended = true
	c := groupClient(t, groupVol(volPvc1), groupVol(volPvc2),
		memberObj(memberOf1, volPvc1), memberObj(memberOf2, volPvc2), grp, sibling)

	fe := &fakeDRBDExec{statusJSON: `[{"name":"any","role":"Primary","suspended-user":true,
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	r := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	reconcileGroup(t, r)

	fe.notCalledWith(t, "resume-io "+volPvc1)
	fe.calledWith(t, "drbdadm resume-io "+volPvc2)
	got := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: groupG1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.ReadyToUse || got.Status.IOSuspended {
		t.Fatalf("the group must still seal ready: %+v", got.Status)
	}

	// The sibling closes → the sealed group's stray-lift pass takes it.
	sibling.Status.IOSuspended = false
	if err := c.Status().Update(t.Context(), sibling); err != nil {
		t.Fatal(err)
	}
	reconcileGroup(t, r)
	fe.calledWith(t, "resume-io "+volPvc1)
}

// The group must not open its round while a standalone snapshot's round
// holds any member volume, and vice versa — the kernel suspend flag is
// per-resource and shared between the two protocols.
func TestGroupAndSingleRoundsExclude(t *testing.T) {
	single := snapObj(snapSnap1, volPvc1, nodeA, nodeB)
	single.Status.IOSuspended = true
	c := groupClient(t, groupVol(volPvc1), groupVol(volPvc2),
		memberObj(memberOf1, volPvc1), memberObj(memberOf2, volPvc2), groupObj(), single)

	fe := &fakeDRBDExec{statusJSON: groupStatusPrimary}
	r := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}
	res := reconcileGroup(t, r)
	if res.RequeueAfter != 2*time.Second {
		t.Fatalf("group must wait for the standalone round, got %+v", res)
	}
	fe.notCalledWith(t, "suspend-io")

	// And the other direction: the standalone snapshot defers to a live
	// group round over its volume.
	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: groupG1}, grp)
	grp.Status.IOSuspended = true
	if err := c.Status().Update(t.Context(), grp); err != nil {
		t.Fatal(err)
	}
	single2 := &miroirv1alpha1.MiroirSnapshot{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: snapSnap1}, single2)
	single2.Status.IOSuspended = false
	if err := c.Status().Update(t.Context(), single2); err != nil {
		t.Fatal(err)
	}

	feS := &fakeDRBDExec{statusJSON: groupStatusPrimary}
	rS := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feS.run}}
	resS, err := rS.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: snapSnap1}})
	if err != nil {
		t.Fatal(err)
	}
	if resS.RequeueAfter != 2*time.Second {
		t.Fatalf("standalone snapshot must wait for the group round, got %+v", resS)
	}
	feS.notCalledWith(t, "suspend-io")
}

// A grouped member never runs the per-snapshot round: without the guard
// this unreplicated volume would be cut (and published ready) on the
// first pass, outside any group barrier.
func TestGroupedMemberSkipsOwnRound(t *testing.T) {
	v := vol(volPvc1, nodeA)
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true},
	}
	member := memberObj(memberOf1, volPvc1)
	c := groupClient(t, v, member)
	fb := newFakeBackend()
	r := &SnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	reconcileSnap(t, r, memberOf1)

	if len(fb.snapCalls) != 0 {
		t.Fatalf("a grouped member is the group round's to cut: %v", fb.snapCalls)
	}
	got := &miroirv1alpha1.MiroirSnapshot{}
	_ = c.Get(t.Context(), types.NamespacedName{Name: memberOf1}, got)
	if got.Status.ReadyToUse {
		t.Fatal("a grouped member must not self-publish ready")
	}
}

// A group referencing a not-yet-resolved member waits instead of cutting
// a partial set.
func TestGroupSnapshotWaitsForMissingMember(t *testing.T) {
	c := groupClient(t, groupVol(volPvc1), memberObj(memberOf1, volPvc1), groupObj())
	fe := &fakeDRBDExec{statusJSON: groupStatusPrimary}
	fb := newFakeBackend()
	r := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	res := reconcileGroup(t, r)

	if res.RequeueAfter != 5*time.Second {
		t.Fatalf("a partial group must wait, got %+v", res)
	}
	fe.notCalledWith(t, "suspend-io")
	if len(fb.snapCalls) != 0 {
		t.Fatalf("a partial group must not touch the backend: %v", fb.snapCalls)
	}
}

// Deleting a group mid-round lifts this node's barriers and releases its
// finalizer.
func TestGroupSnapshotDeletionLiftsBarriers(t *testing.T) {
	grp := groupObj()
	now := metav1.NewTime(time.Now())
	grp.DeletionTimestamp = &now
	grp.Status.IOSuspended = true
	c := groupClient(t, groupVol(volPvc1), groupVol(volPvc2),
		memberObj(memberOf1, volPvc1), memberObj(memberOf2, volPvc2), grp)

	fe := &fakeDRBDExec{statusJSON: groupStatusPeer} // kernel: suspended
	r := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	reconcileGroup(t, r)

	fe.calledWith(t, "drbdadm resume-io "+volPvc1)
	fe.calledWith(t, "drbdadm resume-io "+volPvc2)
	got := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: groupG1}, got); err != nil {
		t.Fatal(err)
	}
	for _, f := range got.Finalizers {
		if f == constants.FinalizerPrefix+nodeA {
			t.Fatalf("this node's finalizer must be released: %v", got.Finalizers)
		}
	}
}

// Deleting a group whose members are already gone must still lift this
// node's barriers: mid-round, each member's own deletion defers the lift
// to the live group round, so once the members vanish the perLeg slot
// keys are the only record of the legs this node froze (issue #272).
func TestGroupSnapshotDeletionLiftsBarriersWithoutMembers(t *testing.T) {
	grp := groupObj()
	now := metav1.NewTime(time.Now())
	grp.DeletionTimestamp = &now
	grp.Status.IOSuspended = true
	grp.Status.PerLeg = map[string]miroirv1alpha1.SnapshotNodeState{
		slotKey(volPvc1, nodeA): miroirv1alpha1.SnapshotSuspended,
		slotKey(volPvc2, nodeA): miroirv1alpha1.SnapshotDone,
		slotKey(volPvc1, nodeB): miroirv1alpha1.SnapshotSuspended,
	}
	c := groupClient(t, grp)

	fe := &fakeDRBDExec{statusJSON: groupStatusPeer} // kernel: suspended
	r := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	reconcileGroup(t, r)

	fe.calledWith(t, "drbdadm resume-io "+volPvc1)
	fe.calledWith(t, "drbdadm resume-io "+volPvc2)
	got := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: groupG1}, got); err != nil {
		t.Fatal(err)
	}
	for _, f := range got.Finalizers {
		if f == constants.FinalizerPrefix+nodeA {
			t.Fatalf("this node's finalizer must be released: %v", got.Finalizers)
		}
	}
}

// The group round freezes every locally mounted member filesystem
// before its barrier rises and thaws them all when the round seals; a
// node without mounts (the Secondary) never freezes.
func TestGroupFreezeBracketsRound(t *testing.T) {
	v1, v2 := groupVol(volPvc1), groupVol(volPvc2)
	for i, v := range []*miroirv1alpha1.MiroirVolume{v1, v2} {
		st := v.Status.PerNode[nodeA]
		st.DevicePath = fmt.Sprintf("/dev/drbd100%d", i)
		v.Status.PerNode[nodeA] = st
	}
	c := groupClient(t, v1, v2,
		memberObj(memberOf1, volPvc1), memberObj(memberOf2, volPvc2), groupObj())

	recK := &ioctlRecorder{}
	feK := &fakeDRBDExec{statusJSON: groupStatusPrimary}
	rK := &GroupSnapshotReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: feK.run},
		Freezer: mountedFreezer(recK, map[string]string{
			devDrbd1000: mntStage1, "/dev/drbd1001": "/mnt/stage-2"})}
	recP := &ioctlRecorder{}
	feP := &fakeDRBDExec{statusJSON: groupStatusPeer}
	rP := &GroupSnapshotReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(newFakeBackend()),
		DRBD:    &drbd.Driver{StateDir: t.TempDir(), Exec: feP.run},
		Freezer: mountedFreezer(recP, nil)}

	// Driver opens the round: both mounted members freeze first.
	reconcileGroup(t, rK)
	calls := recK.recorded()
	if len(calls) != 2 || calls[0] != callFreeze1 || calls[1] != "freeze /mnt/stage-2" {
		t.Fatalf("the driver must freeze every mounted member at the raise: %v", calls)
	}
	// Peer raises, both cut, driver collects: every freeze thaws.
	reconcileGroup(t, rP)
	reconcileGroup(t, rK)
	reconcileGroup(t, rP)
	reconcileGroup(t, rK)
	feK.calledWith(t, "drbdadm resume-io "+volPvc1)
	calls = recK.recorded()
	if len(calls) != 4 || calls[2] != callThaw1 || calls[3] != "thaw /mnt/stage-2" {
		t.Fatalf("sealing the round must thaw every freeze: %v", calls)
	}
	reconcileGroup(t, rP)
	if calls := recP.recorded(); len(calls) != 0 {
		t.Fatalf("the unmounted Secondary must never freeze or thaw: %v", calls)
	}
}
