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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

func TestPlanTeardownError(t *testing.T) {
	busy := fmt.Errorf("drbdsetup down pvc-1: held open: %w", backend.ErrBusy)
	wedged := fmt.Errorf("down pvc-1: %w", drbd.ErrWedged)
	stranded := fmt.Errorf("down pvc-1: %w", drbd.ErrDownStranded)
	nodeWedged := fmt.Errorf("lvremove: %w", backend.ErrNodeWedged)
	other := errors.New("lvremove: exit status 5")

	cases := []struct {
		name        string
		err         error
		slotMessage string
		busyFails   int
		want        teardownPlan
	}{
		{
			name: "an unclassified error surfaces and resets the busy streak",
			err:  other, busyFails: busyFailLimit - 1,
			want: teardownPlan{step: teardownFail},
		},
		{
			name: "the first busy outcome takes the fast retry",
			err:  busy,
			want: teardownPlan{step: teardownRetryBusy, busyFails: 1, requeue: 10 * time.Second},
		},
		{
			name: "the last busy outcome inside the budget still takes the fast retry",
			err:  busy, busyFails: busyFailLimit - 2,
			want: teardownPlan{step: teardownRetryBusy, busyFails: busyFailLimit - 1, requeue: 10 * time.Second},
		},
		{
			name: "the busy outcome that crosses the limit parks, announces and stamps",
			err:  busy, busyFails: busyFailLimit - 1,
			want: teardownPlan{
				step: teardownParkBusy, busyFails: busyFailLimit, requeue: busyRetryAfter,
				announce: true, stamp: true,
			},
		},
		{
			name: "a parked busy outcome stays quiet once its cause is on the slot",
			err:  busy, busyFails: busyFailLimit, slotMessage: busy.Error(),
			want: teardownPlan{step: teardownParkBusy, busyFails: busyFailLimit + 1, requeue: busyRetryAfter},
		},
		{
			name: "a parked busy outcome whose cause changed restamps without announcing again",
			err:  busy, busyFails: busyFailLimit, slotMessage: "an older cause",
			want: teardownPlan{
				step: teardownParkBusy, busyFails: busyFailLimit + 1, requeue: busyRetryAfter,
				stamp: true,
			},
		},
		{
			name: "a crossing whose cause is already on the slot announces without a status write",
			err:  busy, busyFails: busyFailLimit - 1, slotMessage: busy.Error(),
			want: teardownPlan{
				step: teardownParkBusy, busyFails: busyFailLimit, requeue: busyRetryAfter,
				announce: true,
			},
		},
		{
			name: "a wedge parks at once, announces, and resets the busy streak",
			err:  wedged, busyFails: busyFailLimit - 1,
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
		{
			name: "a wedge already on the slot stays parked and quiet",
			err:  wedged, slotMessage: wedged.Error(),
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue},
		},
		{
			name: "a stranded down is a wedge",
			err:  stranded,
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
		{
			name: "an error that is both wedged and busy is a wedge",
			err:  fmt.Errorf("%w: %w", backend.ErrBusy, drbd.ErrWedged),
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
		{
			name: "a node-wedged refusal is a wedge, not a recovery",
			err:  nodeWedged, slotMessage: wedged.Error(),
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := planTeardownError(tc.err, tc.slotMessage, tc.busyFails); got != tc.want {
				t.Errorf("plan = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// Regression: a foreign agent must never remove the finalizer — doing so
// races the owning agent's teardown and leaks the backing device.
func TestReconcileForeignAgentLeavesFinalizerOnDelete(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, "node-b") // replica on node-b...
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)} // ...agent on node-a

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatalf("volume must still exist (finalizer held): %v", err)
	}
	if len(got.Finalizers) == 0 {
		t.Fatal("foreign agent removed the finalizer")
	}
}

// Regression (review): teardown must reclaim the backing even when the
// leg's pool is unknowable — no spec entry (removed) and no status slot
// (agent crashed before its first patch). The sweep deletes from every
// pool instead of guessing "default", which would leak the device or, on
// a node without a default pool, wedge the finalizer forever.
func TestReconcileTeardownSweepsAllPools(t *testing.T) {
	s := newScheme(t)
	fast := newFakeBackend()
	v := vol(volPvc1, nodeB) // leg already removed from spec...
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeA)
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: Pools{
		"fast": {Backend: fast, Type: miroirv1alpha1.BackendLVMThin},
	}} // ...and no "default" pool on this node

	// The orphaned device sits in the fast pool with no status slot
	// recording it.
	if _, err := fast.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if fast.existing[volPvc1] {
		t.Fatal("sweep must reclaim the orphaned device from the fast pool")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeA) {
		t.Fatalf("teardown must release this node's finalizer: %v", got.Finalizers)
	}
}

// Regression (review): a leg whose slot reads Diskless can still shadow a
// leftover backing device (diskful leg removed while blocked, node
// re-added as a tie-breaker) — deletion must reclaim it, as the old
// unconditional Backend.Delete did.
func TestReconcileTeardownReclaimsUnderDisklessSlot(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA)
	v.Spec.Replicas[0].Diskless = true
	v.Spec.Replicas[0].Backend = ""
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {Diskless: true},
	}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	// Leftover backing from the leg's earlier diskful incarnation.
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if fb.existing[volPvc1] {
		t.Fatal("deletion must reclaim the leftover backing under a diskless leg")
	}
}

func TestReconcileTeardownOnDelete(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA)
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	// Pre-create the device so teardown has something to remove.
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if len(fb.created) != 0 {
		t.Fatalf("device must be deleted: %+v", fb.created)
	}
	// Finalizer removed → fake client garbage-collects the object.
	got := &miroirv1alpha1.MiroirVolume{}
	err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got)
	if err == nil {
		t.Fatalf("volume should be gone, still has finalizers %v", got.Finalizers)
	}
}

// Regression for the tie-breaker teardown leak: a diskful replica whose
// DRBD detached its backing after an I/O error reports a Diskless disk
// state — teardown must still delete the backend device, or the LV/zvol
// leaks permanently while the finalizer is released.
func TestReconcileTeardownDeletesDespiteDetachedDiskState(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA)
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateDiskless},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if len(fb.created) != 0 {
		t.Fatalf("a detached (DiskState=Diskless) diskful replica must still delete its backing: %+v", fb.created)
	}
}

// Teardown of a diskful DRBD leg explicitly wipes the metadata before the
// backend destroys the device, so the freed blocks can never carry a stale
// generation identifier into a reuse (issue #139).
func TestReconcileTeardownWipesMetadata(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DRBDMinor: 1000},
	}
	stateDir := t.TempDir()
	// A .res must exist or Down short-circuits as never-configured.
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{}
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	fe.calledWith(t, "drbdmeta --force 1000 v09 /dev/fake/pvc-1 internal wipe-md")
	if len(fb.created) != 0 {
		t.Fatalf("device must be deleted after wipe: %+v", fb.created)
	}
}

// A diskless tie-breaker has no backing device: teardown must not attempt a
// metadata wipe against a device that never existed.
func TestReconcileTeardownDisklessSkipsWipe(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {Diskless: true},
	}
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{}
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	reconcile(t, r, volPvc1)

	fe.notCalledWith(t, "wipe-md")
}

// Teardown behind a still-staged device: drbdsetup down answers "held
// open"; the error must classify as ErrBusy so teardown takes the 10s
// retry, not the workqueue's minutes-long backoff, and the finalizer
// stays until NodeUnstage releases the device.
func TestReconcileTeardownDownHeldOpenRetries(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	stateDir := t.TempDir()
	// A .res must exist or Down short-circuits as never-configured.
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{errOn: map[string]error{
		cmdDownPvc1: errors.New("pvc-1: State change failed: Device is held open by someone"),
	}}
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatalf("held-open must be a requeue, not an error: %v", err)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Fatalf("want 10s busy-retry, got %v", res.RequeueAfter)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal("finalizer must be retained while the device is held open")
	}
}

// The 10s busy retry is not unbounded: past busyFailLimit consecutive
// ErrBusy outcomes teardown escalates — TeardownBusy warning, a status
// Message naming the actual cause, parked cadence — while the finalizer
// stays put, and a later successful teardown resets the streak (issue
// #195: a busy loop ran silently for 2+ hours with the cause swallowed).
func TestReconcileTeardownBusyEscalates(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{errOn: map[string]error{
		cmdDownPvc1: errors.New("pvc-1: State change failed: Device is held open by someone"),
	}}
	rec := events.NewFakeRecorder(4)
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb), Recorder: rec,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	for i := 1; i < busyFailLimit; i++ {
		res, err := r.Reconcile(t.Context(), req)
		if err != nil || res.RequeueAfter != 10*time.Second {
			t.Fatalf("attempt %d must ride the 10s busy retry, got %v / %v", i, res.RequeueAfter, err)
		}
	}
	res, err := r.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("the escalation must park, not error: %v", err)
	}
	if res.RequeueAfter != busyRetryAfter {
		t.Fatalf("want %v parked retry, got %v", busyRetryAfter, res.RequeueAfter)
	}
	// The escalation cycle emits both TeardownBusy and (because the
	// cause contains "held open") a one-shot TeardownLiveConsumer.
	if !drainEvents(rec, "TeardownBusy") {
		t.Fatal("want a TeardownBusy warning event")
	}
	if !drainEvents(rec, "TeardownLiveConsumer") {
		t.Fatal("want a TeardownLiveConsumer warning event")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal("finalizer must be retained while the device is held open")
	}
	if msg := got.Status.PerNode[nodeA].Message; !strings.Contains(msg, "held open") {
		t.Fatalf("Message must carry the swallowed cause, got %q", msg)
	}

	// The escalation latches: further parked cycles must not re-emit the
	// Warning Event (the park can outlive the hold by hours).
	res, err = r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != busyRetryAfter {
		t.Fatalf("post-escalation cycle must stay parked, got %v / %v", res.RequeueAfter, err)
	}
	if drainEvents(rec, "TeardownLiveConsumer") {
		t.Fatal("parked cycles must not re-emit TeardownLiveConsumer")
	}

	// The hold clears: teardown completes, this node's finalizer releases,
	// and the failure streak resets.
	fe.errOn = nil
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("teardown after the hold cleared: %v", err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeA) {
		t.Fatalf("this node's finalizer must release once teardown succeeds: %v", got.Finalizers)
	}
	r.memMu.Lock()
	kept, ok := r.mem[volPvc1]
	r.memMu.Unlock()
	if ok {
		t.Fatalf("a successful teardown must forget the volume, busy streak included; kept %+v", kept)
	}
}

// A busy streak is "consecutive attempts of one teardown episode": both a
// volume back on the normal reconcile path (removal cancelled, replica
// re-added) and a blocked removal must reset it, or the stale count
// pre-biases a later teardown toward premature escalation.
func TestReconcileBusyStreakResetsOutsideTeardown(t *testing.T) {
	s := newScheme(t)

	// Live volume on the normal path.
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		mem: map[string]volumeMemory{volPvc1: {busyFails: busyFailLimit - 1}}}
	reconcile(t, r, volPvc1)
	if n := r.recall(volPvc1).busyFails; n != 0 {
		t.Fatalf("normal reconcile must reset the busy streak, still %d", n)
	}

	// Pending-removal replica whose teardown is blocked.
	fb = newFakeBackend()
	v = vol(volPvc1, nodeB) // leg moved to node-b; node-a keeps its finalizer
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeA)
	c = fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	r = &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		mem: map[string]volumeMemory{volPvc1: {busyFails: busyFailLimit - 1}}}
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil || res.RequeueAfter != 30*time.Second {
		t.Fatalf("want the 30s removal-blocked requeue, got %v / %v", res.RequeueAfter, err)
	}
	if n := r.recall(volPvc1).busyFails; n != 0 {
		t.Fatalf("a blocked removal must reset the busy streak, still %d", n)
	}
}

// orphanHoldFixture assembles a deletion whose down is refused by the
// issue #319 signature: held open, all reported openers exited, device
// mounted in no namespace. Callers tweak the proc dir to flip individual
// legs of the orphan proof.
func orphanHoldFixture(t *testing.T, procDir string) (*VolumeReconciler, *fakeBackend, *fakeDRBDExec, *events.FakeRecorder) {
	t.Helper()
	s := newScheme(t)
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DRBDMinor: 1422},
	}
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "minor.assign"), []byte("pvc-1 1422\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{
		statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		errOn: map[string]error{cmdDownPvc1: errors.New(heldOpenWithOpeners)},
	}
	rec := events.NewFakeRecorder(8)
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb), Recorder: rec, ProcDir: procDir,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	return r, fb, fe, rec
}

// drainEvents empties the recorder and reports whether any drained event
// contains substr.
func drainEvents(rec *events.FakeRecorder, substr string) bool {
	for {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, substr) {
				return true
			}
		default:
			return false
		}
	}
}

// An orphaned hold — held open only by exited openers, mounted nowhere,
// past the busy escalation — must not park forever: teardown routes
// around the pinned minor with a force-detach, destroys the backing, and
// releases the finalizer, leaving the rendered config for the post-reboot
// orphan sweep (issue #319).
func TestReconcileTeardownOrphanHoldReclaims(t *testing.T) {
	procDir := procFixture(t, map[int]string{1: unrelatedMountinfo})
	r, fb, fe, rec := orphanHoldFixture(t, procDir)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	// Ride the whole busy escalation first: the orphan proof must not
	// pre-empt a hold NodeUnstage could still release.
	for range busyFailLimit {
		if _, err := r.Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	fe.notCalledWith(t, "drbdsetup detach")

	// The next cycle reclaims instead of parking.
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("the reclaim cycle must complete teardown: %v", err)
	}
	fe.calledWith(t, "drbdsetup detach 1422 --force")
	fe.calledWith(t, "drbdsetup del-peer pvc-1 1")
	fe.notCalledWith(t, "wipe-md")
	if fb.existing[volPvc1] {
		t.Fatal("the backing device must be destroyed by the reclaim")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err == nil {
		if slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeA) {
			t.Fatalf("this node's finalizer must release after the reclaim: %v", got.Finalizers)
		}
	}
	// The rendered config goes with the volume — left behind it would
	// hold the DRBD port hostage against the next volume handed the
	// recycled number — while the minor reservation stays so the zombie
	// kernel minor cannot be re-assigned before the reboot clears it.
	if _, err := os.Stat(filepath.Join(r.DRBD.StateDir, "pvc-1.res")); !os.IsNotExist(err) {
		t.Fatalf("rendered config must be removed by the reclaim: %v", err)
	}
	assign, err := os.ReadFile(filepath.Join(r.DRBD.StateDir, "minor.assign"))
	if err != nil || !strings.Contains(string(assign), volPvc1) {
		t.Fatalf("minor reservation must survive the reclaim, got %q / %v", assign, err)
	}
	if !drainEvents(rec, "TeardownReclaimed") {
		t.Fatal("want a TeardownReclaimed event")
	}
}

// A live opener pid is a consumer, not an orphan: the reclaim must not
// fire and the parked busy retry stands (#195: never destroy a backing
// under a consumer).
func TestReconcileTeardownOrphanHoldLiveOpenerStaysParked(t *testing.T) {
	procDir := procFixture(t, map[int]string{4242424: unrelatedMountinfo})
	r, fb, fe, _ := orphanHoldFixture(t, procDir)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	for range busyFailLimit + 1 {
		if _, err := r.Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	res, err := r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != busyRetryAfter {
		t.Fatalf("a live opener must stay parked, got %v / %v", res.RequeueAfter, err)
	}
	fe.notCalledWith(t, "drbdsetup detach")
	if !fb.existing[volPvc1] {
		t.Fatal("the backing must survive while an opener lives")
	}
}

// A device still mounted in any namespace — even one no host path shows,
// like a force-deleted pod's container — is a live consumer: the reclaim
// must not fire even though every reported opener pid is dead.
func TestReconcileTeardownOrphanHoldMountedStaysParked(t *testing.T) {
	procDir := procFixture(t, map[int]string{
		1:    unrelatedMountinfo,
		4321: drbd1422Mountinfo,
	})
	r, fb, fe, _ := orphanHoldFixture(t, procDir)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	for range busyFailLimit + 1 {
		if _, err := r.Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	res, err := r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != busyRetryAfter {
		t.Fatalf("a mounted device must stay parked, got %v / %v", res.RequeueAfter, err)
	}
	fe.notCalledWith(t, "drbdsetup detach")
	if !fb.existing[volPvc1] {
		t.Fatal("the backing must survive while the device is mounted somewhere")
	}
}

// When teardown is blocked by a live consumer (orphanHold returns false,
// cause contains "held open"), a TeardownLiveConsumer Warning event is
// emitted at the busyFailLimit crossing so operators can see the deletion
// is blocked by a live consumer — not by a dead-held orphan. The teardown
// stays parked (no force-detach, no backing destruction) and the event is
// not re-emitted on subsequent parked cycles.
func TestReconcileTeardownLiveConsumerEmitsEvent(t *testing.T) {
	procDir := procFixture(t, map[int]string{4242424: unrelatedMountinfo})
	r, fb, fe, rec := orphanHoldFixture(t, procDir)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	// Ride up to busyFailLimit — orphanHold returns false (pid 4242424 alive).
	for i := 1; i < busyFailLimit; i++ {
		res, err := r.Reconcile(t.Context(), req)
		if err != nil || res.RequeueAfter != 10*time.Second {
			t.Fatalf("attempt %d must ride the 10s busy retry, got %v / %v", i, res.RequeueAfter, err)
		}
	}

	// The crossing cycle: escalation fires, TeardownLiveConsumer event emitted.
	res, err := r.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("the escalation must park, not error: %v", err)
	}
	if res.RequeueAfter != busyRetryAfter {
		t.Fatalf("want %v parked retry, got %v", busyRetryAfter, res.RequeueAfter)
	}
	// No force-detach must have been attempted.
	fe.notCalledWith(t, "drbdsetup detach")
	if !fb.existing[volPvc1] {
		t.Fatal("the backing must survive while a live consumer holds the device")
	}
	// The TeardownLiveConsumer event must have been emitted.
	if !drainEvents(rec, "TeardownLiveConsumer") {
		t.Fatal("want a TeardownLiveConsumer warning event")
	}

	// One more parked cycle: the event must not re-emit.
	res, err = r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != busyRetryAfter {
		t.Fatalf("post-escalation cycle must stay parked, got %v / %v", res.RequeueAfter, err)
	}
	if drainEvents(rec, "TeardownLiveConsumer") {
		t.Fatal("TeardownLiveConsumer event must not re-emit on parked cycles")
	}
}

// strandedDowns stands in for the node breaker, reporting the given
// resources' drbdsetup down as stranded in the kernel.
type strandedDowns map[string]bool

func (s strandedDowns) StrandedCommand(line string) bool {
	return s[strings.TrimPrefix(line, "drbdsetup down ")]
}

func (s strandedDowns) Commands() []string {
	var out []string
	for name, stuck := range s {
		if stuck {
			out = append(out, "drbdsetup down "+name)
		}
	}
	return out
}

// A down of ours stranded in the kernel wedges the resource while its status
// still reads healthy: teardown must park exactly like the Detaching
// signature does, and the TeardownWedged warning must name that
// fingerprint rather than the drbd#137 one.
func TestReconcileTeardownStrandedDownParksRetry(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],"connections":[]}]`}
	rec := events.NewFakeRecorder(4)
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb), Recorder: rec,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Wedge: strandedDowns{volPvc1: true},
			Mknod: func(string, uint32, int) error { return nil }},
	}
	t.Cleanup(func() { dropVolumeMetrics(volPvc1) })
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	res, err := r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != 10*time.Second {
		t.Fatalf("first sighting must defer on the busy cadence, got %v / %v", res.RequeueAfter, err)
	}
	res, err = r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != wedgedRequeue {
		t.Fatalf("second sighting must park at %v, got %v / %v", wedgedRequeue, res.RequeueAfter, err)
	}
	fe.notCalledWith(t, "drbdsetup down")
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "TeardownWedged") || !strings.Contains(e, "stranded in uninterruptible sleep") {
			t.Fatalf("want a TeardownWedged warning naming the stranded down, got %q", e)
		}
	default:
		t.Fatal("want a TeardownWedged warning event")
	}
	if got := testutil.ToFloat64(metricWedged.WithLabelValues(volPvc1, volPvc1, "")); got != 1 {
		t.Fatalf("wedged gauge = %v, want 1", got)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal("finalizer must be retained while the resource is wedged")
	}
	if msg := got.Status.PerNode[nodeA].Message; !strings.Contains(msg, "reboot") {
		t.Fatalf("Message must say a reboot is required, got %q", msg)
	}
}

// A kernel-wedged resource (stuck Detaching with the connections gone,
// LINBIT/drbd#137) must leave the fast busy-retry without ever spawning
// another down: the first sighting defers on the busy cadence, the second
// parks at wedgedRequeue with the TeardownWedged warning, wedged gauge,
// actionable Message, and the finalizer retained.
func TestReconcileTeardownWedgedParksRetry(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Detaching"}],"connections":[]}]`}
	rec := events.NewFakeRecorder(4)
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb), Recorder: rec,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	t.Cleanup(func() { dropVolumeMetrics(volPvc1) })
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}

	// First sighting: the detach may still be draining — busy retry.
	res, err := r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != 10*time.Second {
		t.Fatalf("first sighting must defer on the busy cadence, got %v / %v", res.RequeueAfter, err)
	}
	// Second sighting: escalate.
	res, err = r.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("a wedge must park the retry, not error: %v", err)
	}
	if res.RequeueAfter != wedgedRequeue {
		t.Fatalf("want %v parked retry, got %v", wedgedRequeue, res.RequeueAfter)
	}
	fe.notCalledWith(t, "drbdsetup down")
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "TeardownWedged") {
			t.Fatalf("want a TeardownWedged warning, got %q", e)
		}
	default:
		t.Fatal("want a TeardownWedged warning event")
	}
	if got := testutil.ToFloat64(metricWedged.WithLabelValues(volPvc1, volPvc1, "")); got != 1 {
		t.Fatalf("wedged gauge = %v, want 1", got)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal("finalizer must be retained while the resource is wedged")
	}
	if msg := got.Status.PerNode[nodeA].Message; !strings.Contains(msg, "reboot") {
		t.Fatalf("Message must say a reboot is required, got %q", msg)
	}

	// The escalation latches: parked cycles must not re-emit the Warning
	// (issue #319: a wedged volume re-emitted it every cycle for hours)
	// while the gauge stays raised.
	res, err = r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != wedgedRequeue {
		t.Fatalf("post-escalation cycle must stay parked, got %v / %v", res.RequeueAfter, err)
	}
	select {
	case e := <-rec.Events:
		t.Fatalf("parked wedge cycles must not re-emit events, got %q", e)
	default:
	}
	if got := testutil.ToFloat64(metricWedged.WithLabelValues(volPvc1, volPvc1, "")); got != 1 {
		t.Fatalf("wedged gauge must stay raised on parked cycles, got %v", got)
	}

	// The wedge clears (reboot happened) but the device is now merely held
	// open: the gauge — and with it the critical alert — must retire.
	fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],"connections":[]}]`
	fe.errOn = map[string]error{
		cmdDownPvc1: errors.New("pvc-1: State change failed: Device is held open by someone"),
	}
	res, err = r.Reconcile(t.Context(), req)
	if err != nil || res.RequeueAfter != 10*time.Second {
		t.Fatalf("held-open after the wedge must take the busy retry, got %v / %v", res.RequeueAfter, err)
	}
	if n := testutil.CollectAndCount(metricWedged); n != 0 {
		t.Fatalf("wedged gauge must clear on a non-wedged outcome, still %d series", n)
	}
}

// Removing a tie-breaker is not pinned by snapshots (it holds no backend
// CoW state); removing a diskful replica still is. The self-reported
// status marker decides — the entry is already gone from spec.
func TestRemovalSnapshotGateSkipsTieBreaker(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB) // node-c already removed from spec
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, Connected: true},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate, Connected: true},
		nodeC: {DiskState: diskStateDiskless, Diskless: true},
	}
	snap := snapObj(snapSnap1, volPvc1, nodeA, nodeB)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}, &miroirv1alpha1.MiroirSnapshot{}).
		Build()

	rO := &VolumeReconciler{Client: c, NodeName: nodeC, Pools: poolsOf(newFakeBackend())}
	if reason := rO.removalBlocked(t.Context(), v); reason != "" {
		t.Fatalf("tie-breaker removal must not be pinned by snapshots: %s", reason)
	}
	// A removed diskful replica (no Diskless marker) stays pinned.
	rK := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}
	if reason := rK.removalBlocked(t.Context(), v); !strings.Contains(reason, "snapshot") {
		t.Fatalf("diskful removal must stay pinned by snapshots, got %q", reason)
	}
}

// removedReplicaVol builds a 2-replica volume on node-b+node-c whose node-a
// leg was just removed from spec.replicas (finalizer still held).
func removedReplicaVol() *miroirv1alpha1.MiroirVolume {
	v := vol(volPvc1, "node-b", nodeC)
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeA)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	for i := range v.Spec.Replicas {
		v.Spec.Replicas[i].NodeID = int32(i)
		v.Spec.Replicas[i].Address = "192.168.1.4" + string(rune('1'+i))
	}
	return v
}

func patchPeersUpToDate(t *testing.T, c client.Client, diskState string) {
	t.Helper()
	err := c.Status().Patch(t.Context(), &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
	}, client.RawPatch(types.MergePatchType, []byte(`{
		"status": {"perNode": {
			"node-b": {"deviceCreated": true, "diskState": "`+diskState+`", "connected": true},
			"`+nodeC+`": {"deviceCreated": true, "diskState": "`+diskStateUpToDate+`", "connected": true},
			"`+nodeA+`": {"deviceCreated": true, "diskState": "`+diskStateUpToDate+`", "connected": true}
		}}
	}`)))
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconcileRemovedReplicaTearsDown(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created[volPvc1] = 1 << 30
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource"), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(removedReplicaVol()).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	patchPeersUpToDate(t, c, diskStateUpToDate)
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run}}

	reconcile(t, r, volPvc1)

	if _, ok := fb.created[volPvc1]; ok {
		t.Fatal("backing device not deleted")
	}
	fe.calledWith(t, cmdDownPvc1)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	for _, f := range got.Finalizers {
		if f == constants.FinalizerPrefix+nodeA {
			t.Fatal("finalizer not released after removal teardown")
		}
	}
	if _, ok := got.Status.PerNode[nodeA]; ok {
		t.Fatal("removed replica's status slot not cleared")
	}
}

func TestReconcileRemovalBlockedBySnapshot(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created[volPvc1] = 1 << 30
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(removedReplicaVol(), &miroirv1alpha1.MiroirSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
			Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volPvc1},
		}).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	patchPeersUpToDate(t, c, diskStateUpToDate)
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("blocked removal must requeue")
	}
	if _, ok := fb.created[volPvc1]; !ok {
		t.Fatal("must not tear down while a snapshot references the volume")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Status.PerNode[nodeA].Message, "snapshot") {
		t.Fatalf("blocked reason not surfaced: %+v", got.Status.PerNode[nodeA])
	}
}

func TestReconcileRemovalBlockedByDegradedPeer(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created[volPvc1] = 1 << 30
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(removedReplicaVol()).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	patchPeersUpToDate(t, c, diskStateInconsistent) // node-b still syncing
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("blocked removal must requeue")
	}
	if _, ok := fb.created[volPvc1]; !ok {
		t.Fatal("must not cut the leg while a remaining replica is not UpToDate")
	}
}
