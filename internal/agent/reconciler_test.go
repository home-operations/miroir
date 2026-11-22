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
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

// Pool names used across this package's tests.
const (
	poolDefault = "default"
	poolFast    = "fast"
)

const (
	nodeA                 = "node-a"
	nodeB                 = "node-b"
	addrA                 = "192.168.1.41"
	addrB                 = "192.168.1.42"
	volPvc1               = "pvc-1"
	snapSnap1             = "snap-1"
	diskStateUpToDate     = "UpToDate"
	diskStateOutdated     = "Outdated"
	diskStateInconsistent = "Inconsistent"
	diskStateDiskless     = "Diskless"
	nodeC                 = "node-c"
	addrC                 = "192.168.1.43"
	// cmdDownPvc1 keys fakeDRBDExec.errOn for `drbdsetup down pvc-1`.
	cmdDownPvc1 = "drbdsetup down pvc-1"
	// snapGone names a deleted source snapshot in restore-source-gone tests.
	snapGone = "snap-gone"
	// statusNotInKernel is the status of a resource that is not up yet — the
	// first --json read of a bring-up pass, which probeMetadata makes before
	// create-md. drbdsetup does not answer that with an empty list: it
	// writes "<name>: No such resource" to stderr and exits 10, which
	// backend.RealExec surfaces as an error. fakeDRBDExec.run turns this
	// sentinel into that error so fixtures exercise the real shape.
	statusNotInKernel = "\x00no-such-resource"
)

// fakeBackend records calls and simulates a thin pool in memory.
type fakeBackend struct {
	created     map[string]int64
	existing    map[string]bool
	failOn      string
	snapCalls   []string
	fromSnapVol []string
	createVol   []string
	stats       backend.PoolStats
	statsErr    error
	// seq, when set, appends a marker on Resize into a log shared with the
	// DRBD exec, so a test can assert the backing resize is ordered after
	// the DRBD attach rather than before it.
	seq *[]string
}

// errBoom is the generic injected failure for fake backends.
var errBoom = errors.New("boom")

// poolsOf wraps a fake backend as this node's single default lvmthin pool —
// the pre-multi-pool shape most tests exercise.
func poolsOf(fb *fakeBackend) Pools {
	return poolsOfType(fb, miroirv1alpha1.BackendLVMThin)
}

func poolsOfType(fb *fakeBackend, typ miroirv1alpha1.BackendType) Pools {
	return Pools{poolDefault: {Backend: fb, Type: typ}}
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		created:  map[string]int64{},
		existing: map[string]bool{},
		stats:    backend.PoolStats{SizeBytes: 1 << 40},
	}
}

func (f *fakeBackend) Exists(_ context.Context, vol string) (bool, error) {
	return f.existing[vol], nil
}

func (f *fakeBackend) Setup(context.Context) error { return nil }

func (f *fakeBackend) Sync(context.Context, string) error { return nil }

func (f *fakeBackend) Create(_ context.Context, vol string, size int64) (string, error) {
	if f.failOn == "create" {
		return "", errors.New("pool exploded")
	}
	if _, ok := f.created[vol]; !ok {
		f.created[vol] = size
	}
	f.existing[vol] = true
	f.createVol = append(f.createVol, vol)
	return f.DevicePath(vol), nil
}

func (f *fakeBackend) Resize(_ context.Context, vol string, size int64) error {
	if f.created[vol] < size {
		f.created[vol] = size
	}
	if f.seq != nil {
		*f.seq = append(*f.seq, "backend-resize "+vol)
	}
	return nil
}

func (f *fakeBackend) Snapshot(_ context.Context, vol, snap string) error {
	f.snapCalls = append(f.snapCalls, "snapshot "+vol+"@"+snap)
	return nil
}

func (f *fakeBackend) CreateFromSnapshot(_ context.Context, vol, _, _ string) (string, error) {
	f.existing[vol] = true
	f.fromSnapVol = append(f.fromSnapVol, vol)
	return f.DevicePath(vol), nil
}

func (f *fakeBackend) Delete(_ context.Context, vol string) error {
	if f.failOn == "delete" || f.failOn == vol+":delete" {
		return backend.ErrBusy
	}
	if f.failOn == vol+":delete-hard" {
		return errBoom
	}
	delete(f.created, vol)
	delete(f.existing, vol)
	return nil
}

func (f *fakeBackend) DeleteSnapshot(_ context.Context, vol, snap string) error {
	f.snapCalls = append(f.snapCalls, "delete "+vol+"@"+snap)
	return nil
}

func (f *fakeBackend) DevicePath(vol string) string { return "/dev/fake/" + vol }

func (f *fakeBackend) Stats(context.Context) (backend.PoolStats, error) {
	if f.statsErr != nil {
		return backend.PoolStats{}, f.statsErr
	}
	return f.stats, nil
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

//nolint:unparam // future tests will vary the name
func vol(name string, nodes ...string) *miroirv1alpha1.MiroirVolume {
	replicas := make([]miroirv1alpha1.Replica, 0, len(nodes))
	for _, n := range nodes {
		replicas = append(replicas, miroirv1alpha1.Replica{
			Node: n, Backend: miroirv1alpha1.BackendLVMThin,
		})
	}
	finalizers := make([]string, 0, len(nodes))
	for _, n := range nodes {
		finalizers = append(finalizers, constants.FinalizerPrefix+n)
	}
	return &miroirv1alpha1.MiroirVolume{
		TypeMeta: metav1.TypeMeta{
			APIVersion: miroirv1alpha1.GroupVersion.String(),
			Kind:       "MiroirVolume",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Finalizers: finalizers,
		},
		Spec: miroirv1alpha1.MiroirVolumeSpec{SizeBytes: 1 << 30, Replicas: replicas},
	}
}

// phaseOf derives the phase the controller would publish from the slots the
// agents wrote.
func phaseOf(vol *miroirv1alpha1.MiroirVolume) miroirv1alpha1.VolumePhase {
	phase, _ := computePhase(vol, time.Now())
	return phase
}

//nolint:unparam // future tests will vary the name
func reconcile(t *testing.T, r *VolumeReconciler, name string) {
	t.Helper()
	_, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconcileRealizesReplica(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	reconcile(t, r, volPvc1)

	if fb.created[volPvc1] != 1<<30 {
		t.Fatalf("device not created: %+v", fb.created)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if phaseOf(got) != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready (status %+v)", phaseOf(got), got.Status.PerNode)
	}
	if got.Status.PerNode[nodeA].DevicePath != fb.DevicePath(volPvc1) {
		t.Fatalf("unexpected status %+v", got.Status.PerNode)
	}
	// No peers means fully in sync: the zero value would perma-fire any
	// <1 resync alert for every unreplicated volume.
	if ratio := testutil.ToFloat64(metricResyncRatio.WithLabelValues(volPvc1, miroirv1alpha1.DefaultPoolName, volPvc1, "")); ratio != 1 {
		t.Fatalf("unreplicated resync_ratio = %v, want 1", ratio)
	}
}

// A replica placed in a named pool realizes through that pool's backend
// and self-reports the pool in its status slot.
func TestReconcileUsesReplicaPool(t *testing.T) {
	s := newScheme(t)
	def, fast := newFakeBackend(), newFakeBackend()
	v := vol(volPvc1, nodeA)
	v.Spec.Replicas[0].Pool = poolFast
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: Pools{
		poolDefault: {Backend: def, Type: miroirv1alpha1.BackendLVMThin},
		poolFast:    {Backend: fast, Type: miroirv1alpha1.BackendLVMThin},
	}}

	reconcile(t, r, volPvc1)

	if len(def.created) != 0 {
		t.Fatalf("default pool must stay untouched: %+v", def.created)
	}
	if fast.created[volPvc1] != 1<<30 {
		t.Fatalf("device must land in the fast pool: %+v", fast.created)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].Pool != poolFast {
		t.Fatalf("status slot must self-report the pool: %+v", got.Status.PerNode[nodeA])
	}
}

// A replica referencing a pool this node no longer carries is a hard
// failure with a pointed message — never a wrong-pool guess.
func TestReconcileUnknownPoolFails(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA)
	v.Spec.Replicas[0].Pool = "gone"
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err == nil {
		t.Fatal("unknown pool must error the reconcile")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if msg := got.Status.PerNode[nodeA].Message; !strings.Contains(msg, `"gone"`) {
		t.Fatalf("status must name the missing pool, got %q", msg)
	}
	if len(fb.created) != 0 {
		t.Fatalf("no device may be created in another pool: %+v", fb.created)
	}
}

func TestReconcileIgnoresForeignVolume(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, "node-b")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	reconcile(t, r, volPvc1)

	if len(fb.created) != 0 {
		t.Fatalf("must not touch foreign volumes: %+v", fb.created)
	}
}

func TestReconcileReportsBackendError(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.failOn = "create"
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	_, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err == nil {
		t.Fatal("expected error to requeue")
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if phaseOf(got) != miroirv1alpha1.VolumeFailed {
		t.Fatalf("phase = %s, want Failed", phaseOf(got))
	}
	if got.Status.PerNode[nodeA].Message == "" {
		t.Fatal("error message must be reported in status")
	}
}

// TestReconcileSourceSnapshotGoneRecoversBacking: a GC'd source snapshot must
// not strand a volume whose backing survived the reboot.
func TestReconcileSourceSnapshotGoneRecoversBacking(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.existing[volPvc1] = true // backing survived the reboot
	v := vol(volPvc1, nodeA)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-deleted"}
	// No MiroirSnapshot object in the client: it has been garbage-collected.
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	// A DRBD driver even though the volume is unreplicated: recovering a
	// restore clone probes it for foreign metadata (finishClone).
	fe := &fakeDRBDExec{}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	if len(fb.fromSnapVol) != 0 {
		t.Fatalf("must not clone (snapshot gone), got CreateFromSnapshot calls %v", fb.fromSnapVol)
	}
	if len(fb.createVol) != 1 || fb.createVol[0] != volPvc1 {
		t.Fatalf("must recover the existing device via Create, got %v", fb.createVol)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if phaseOf(got) != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready (status %+v)", phaseOf(got), got.Status.PerNode)
	}
	if got.Status.PerNode[nodeA].DevicePath != fb.DevicePath(volPvc1) {
		t.Fatalf("backing not recovered: %+v", got.Status.PerNode)
	}
}

// TestReconcileSourceSnapshotGoneAndDeviceMissingParks: no snapshot and no
// device — the restore can never complete, so fail loud (Failed phase,
// RestoreSourceMissing warning, Message) and park the retry rather than
// hot-loop the NotFound through the workqueue backoff (issue #195). Never
// seed an empty device.
func TestReconcileSourceSnapshotGoneAndDeviceMissingParks(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend() // no existing device
	v := vol(volPvc1, nodeA)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-deleted"}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	rec := events.NewFakeRecorder(4)
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb), Recorder: rec}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatalf("an impossible restore must park, not error: %v", err)
	}
	if res.RequeueAfter != restoreOrphanRequeue {
		t.Fatalf("want %v parked retry, got %v", restoreOrphanRequeue, res.RequeueAfter)
	}
	if len(fb.createVol) != 0 || len(fb.fromSnapVol) != 0 {
		t.Fatalf("must not create or clone an empty device: create=%v fromSnap=%v",
			fb.createVol, fb.fromSnapVol)
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "RestoreSourceMissing") {
			t.Fatalf("want a RestoreSourceMissing warning, got %q", e)
		}
	default:
		t.Fatal("want a RestoreSourceMissing warning event")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if phaseOf(got) != miroirv1alpha1.VolumeFailed {
		t.Fatalf("phase = %s, want Failed", phaseOf(got))
	}
	if msg := got.Status.PerNode[nodeA].Message; !strings.Contains(msg, "no longer exists") {
		t.Fatalf("Message must name the missing snapshot, got %q", msg)
	}
}

func TestReconcileReplicatedVolume(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, "node-b")
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB

	stateDir := t.TempDir()
	// Pre-seed .res so assignMinor → AllocateMinor picks minor 1000.
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n"+
			"    on \"node-a\" {\n"+
			"        device minor 1000;\n"+
			"    }\n"+
			"}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD:    &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
		Startup: &StartupGate{Reader: c, NodeName: nodeA, Deadline: time.Now().Add(time.Hour)},
	}
	readyz := httptest.NewRequest("GET", "/readyz", nil)
	if r.Startup.Check(readyz) == nil {
		t.Fatal("agent must not report ready before its replicated leg was reconciled")
	}
	r.Startup.Baseline(volPvc1, true)

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("replicated volumes must requeue to refresh DRBD state")
	}
	if err := r.Startup.Check(readyz); err != nil {
		t.Fatalf("a connected UpToDate leg must open the startup gate: %v", err)
	}
	if fb.created[volPvc1] != 1<<30 {
		t.Fatal("backing device not created")
	}
	fe.calledWith(t, "drbdadm adjust pvc-1")

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	if st.DevicePath != devDrbd1000 {
		t.Fatalf("pods must attach the DRBD device, got %q", st.DevicePath)
	}
	if st.DiskState != diskStateUpToDate || !st.Connected {
		t.Fatalf("unexpected DRBD status %+v", st)
	}
	// replicas[0] withholds its realized size (and the volume stays
	// unready) until the peer's backing reports grown — the size in
	// status is what the expansion wait keys on.
	if st.SizeBytes != 0 {
		t.Fatalf("coordinator must withhold size before the peer reports, got %d", st.SizeBytes)
	}
	fe.notCalledWith(t, "drbdadm resize")

	// Peer reports its leg; the next coordinator pass grows DRBD and
	// publishes the size.
	base := got.DeepCopy()
	got.Status.PerNode["node-b"] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true,
	}
	if err := c.Status().Patch(t.Context(), got, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm resize --assume-clean pvc-1")
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].SizeBytes != 1<<30 {
		t.Fatalf("size must publish after DRBD resize, got %d", got.Status.PerNode[nodeA].SizeBytes)
	}
	if phaseOf(got) != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready with both legs UpToDate", phaseOf(got))
	}
}

// Regression (#290): a restore that grows the volume must resize the backing
// only after DRBD has attached it. The clone is born at the snapshot's size
// carrying the source's internal metadata at that offset; growing the backing
// first strands the metadata and the leg never leaves Inconsistent. The
// backend resize must therefore be ordered after the drbdadm attach, not
// before it.
func TestReconcile_RestoreToLargerResizesBackingAfterAttach(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend() // no existing backing: realizeBacking clones the snapshot
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	// Restore from a snapshot to a size larger than the source.
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapSnap1}
	v.Spec.SizeBytes = 2 << 30

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	fb.seq = &fe.calls // interleave the backing resize into the DRBD call log

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	if len(fb.fromSnapVol) != 1 || fb.fromSnapVol[0] != volPvc1 {
		t.Fatalf("restore must clone the backing, got CreateFromSnapshot %v", fb.fromSnapVol)
	}
	attach, resize := -1, -1
	for i, call := range fe.calls {
		switch {
		case attach < 0 && strings.Contains(call, "drbdadm adjust "+volPvc1):
			attach = i
		case strings.Contains(call, "backend-resize "+volPvc1):
			resize = i
		}
	}
	if attach < 0 {
		t.Fatalf("DRBD never attached the backing, calls: %v", fe.calls)
	}
	if resize < 0 {
		t.Fatalf("backing was never resized to the requested size, calls: %v", fe.calls)
	}
	if resize < attach {
		t.Fatalf("backing resized before DRBD attach (strands clone metadata): attach=%d resize=%d, calls: %v",
			attach, resize, fe.calls)
	}
}

// paddedRestoreVol builds a 2-leg replicated restore that crossed the
// replication boundary (PadForMetadata): node-a is the seed clone,
// node-b the FullSync joiner.
func paddedRestoreVol() *miroirv1alpha1.MiroirVolume {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Spec.Replicas[1].FullSync = true
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapSnap1, PadForMetadata: true}
	return v
}

// A padded restore's clone must grow by the metadata overhead BEFORE the
// DRBD apply: there is no adopted metadata to strand (the source was
// unreplicated), and create-md must land in the grown tail, not over the
// source filesystem's last bytes (issue #305).
func TestReconcilePaddedRestoreGrowsCloneBeforeCreateMD(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	stateDir := t.TempDir()
	// The leading not-in-kernel reads are Apply's peer snapshot and
	// probeMetadata's: the fresh clone must not read as adopted metadata,
	// or create-md never runs.
	up := `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{statusNotInKernel, statusNotInKernel, up, up, up, up}}
	fb.seq = &fe.calls // interleave the backing resize into the DRBD call log

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	if len(fb.fromSnapVol) != 1 {
		t.Fatalf("the seed must clone the snapshot, got %v", fb.fromSnapVol)
	}
	createMD, resize := -1, -1
	for i, call := range fe.calls {
		switch {
		case createMD < 0 && strings.Contains(call, "create-md"):
			createMD = i
		case resize < 0 && strings.Contains(call, "backend-resize "+volPvc1):
			resize = i
		}
	}
	if createMD < 0 || resize < 0 {
		t.Fatalf("want both create-md and a backing resize, calls: %v", fe.calls)
	}
	if resize > createMD {
		t.Fatalf("padded clone must grow before create-md (metadata would overwrite the filesystem tail): resize=%d create-md=%d, calls: %v",
			resize, createMD, fe.calls)
	}
	want := 1<<30 + drbd.InternalMetaOverhead(1<<30)
	if fb.created[volPvc1] != want {
		t.Fatalf("clone backing must realize the padded size %d, got %d", want, fb.created[volPvc1])
	}
}

// A padded restore's FullSync joiner must realize the padded size too:
// DRBD sizes the device to the smallest leg, and a bare-spec joiner
// would cap it below the restored filesystem.
func TestReconcilePaddedRestoreFullSyncJoinerPadsBacking(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateInconsistent + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected"}]}]`}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeB, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	if len(fb.fromSnapVol) != 0 {
		t.Fatal("a FullSync joiner must never clone")
	}
	want := 1<<30 + drbd.InternalMetaOverhead(1<<30)
	if fb.created[volPvc1] != want {
		t.Fatalf("joiner backing must realize the padded size %d, got %d", want, fb.created[volPvc1])
	}
	// The joiner must not mint the seed generation — its content arrives
	// over the wire.
	fe.notCalledWith(t, "primary --force")
}

// A restore that shrank out of replication clones the replicated
// source's DRBD metadata into a raw backing pods stage directly: realize
// must wipe it (blkid otherwise reads the device as TYPE=drbd and
// staging refuses), while a clone of an unreplicated source — no
// metadata — stays untouched.
func TestReconcileUnreplicatedRestoreWipesForeignMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dumpMD   map[string]string
		wantWipe bool
	}{
		{name: "replicated source clone", dumpMD: map[string]string{"dump-md": `version "v09";`}, wantWipe: true},
		{name: "unreplicated source clone", wantWipe: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScheme(t)
			fb := newFakeBackend()
			v := vol(volPvc1, nodeA)
			v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapSnap1}
			fe := &fakeDRBDExec{responses: tc.dumpMD}
			c := fake.NewClientBuilder().WithScheme(s).
				WithObjects(v, snapObj(snapSnap1, "src-vol")).
				WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
				Build()
			r := &VolumeReconciler{
				Client: c, NodeName: nodeA, Pools: poolsOf(fb),
				DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
			}

			reconcile(t, r, volPvc1)

			if len(fb.fromSnapVol) != 1 {
				t.Fatalf("restore must clone, got %v", fb.fromSnapVol)
			}
			if tc.wantWipe {
				fe.calledWith(t, "wipe-md")
			} else {
				fe.notCalledWith(t, "wipe-md")
			}
		})
	}
}

func TestReconcile_SkipResizeDuringResync(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, "node-b")
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}

	// Peer backing grown so peerBackingsGrown is true — only the in-flight
	// sync should withhold the resize.
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected","peer_devices":[{"replication-state":"SyncSource"}]}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	base := got.DeepCopy()
	got.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		"node-b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
	}
	if err := c.Status().Patch(t.Context(), got, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatalf("a resync in progress must not surface as a reconcile error: %v", err)
	}
	fe.notCalledWith(t, "drbdadm resize")
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].SizeBytes != 0 {
		t.Fatalf("size must be withheld while resyncing, got %d", got.Status.PerNode[nodeA].SizeBytes)
	}

	// Resync completes: the next pass grows DRBD and publishes the size.
	fe.statusJSON = `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected","peer_devices":[{"replication-state":"Established"}]}]}]`
	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm resize --assume-clean pvc-1")
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].SizeBytes != 1<<30 {
		t.Fatalf("size must publish once the resync clears, got %d", got.Status.PerNode[nodeA].SizeBytes)
	}
}

func TestReconcile_ResizeRaceWithResyncIsTransient(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, "node-b")
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}

	// Pre-check sees no resync (Established), but the resize itself races one
	// and DRBD refuses it — must be a transient withhold, not a hard error.
	fe := &fakeDRBDExec{
		statusJSON: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected","peer_devices":[{"replication-state":"Established"}]}]}]`,
		errOn: map[string]error{
			"drbdadm resize": errors.New("exit status 10: Resize not allowed during resync."),
		},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	base := got.DeepCopy()
	got.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		"node-b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
	}
	if err := c.Status().Patch(t.Context(), got, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatalf("a resize that raced a resync must not fail the reconcile: %v", err)
	}
	fe.calledWith(t, "drbdadm resize --assume-clean pvc-1") // it was attempted
	if res.RequeueAfter != 5*time.Second {
		t.Fatalf("must requeue soon to retry the resize, got %v", res.RequeueAfter)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].SizeBytes != 0 {
		t.Fatalf("size must be withheld until the resize succeeds, got %d", got.Status.PerNode[nodeA].SizeBytes)
	}
}

type fakeDRBDExec struct {
	calls      []string
	statusJSON string
	// statusSeq is consumed first, one entry per `drbdsetup status --json`
	// call — the birth-generation pass reads status twice (pre/post fire)
	// and needs different answers. Falls back to statusJSON when drained.
	statusSeq []string
	responses map[string]string
	// errOn fails any command line containing the key; a key prefixed
	// with "=" fails only the exact line, so a retry with an extra flag
	// can be told apart from the attempt it follows.
	errOn map[string]error
}

func (f *fakeDRBDExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	for key, err := range f.errOn {
		if exact, ok := strings.CutPrefix(key, "="); ok {
			if line == exact {
				return "", err
			}
		} else if strings.Contains(line, key) {
			return "", err
		}
	}
	if strings.HasPrefix(line, "drbdsetup status --json") && len(f.statusSeq) > 0 {
		out := f.statusSeq[0]
		f.statusSeq = f.statusSeq[1:]
		if out == statusNotInKernel {
			return "", errNoSuchResource
		}
		return out, nil
	}
	if strings.HasPrefix(line, "drbdsetup status") {
		if f.statusJSON == "" {
			// Nothing in the kernel by default: --json prints an empty list.
			return "[]", nil
		}
		return f.statusJSON, nil
	}
	for key, out := range f.responses {
		if strings.Contains(line, key) {
			return out, nil
		}
	}
	if strings.Contains(line, "dump-md") {
		return "", errFreshDevice
	}
	return "", nil
}

var (
	errFreshDevice = errors.New("no valid meta data")
	// errNoSuchResource is drbdsetup's exit-10 answer for a resource that
	// is not up, as backend.RealExec reports it.
	errNoSuchResource = errors.New("drbdsetup status: pvc-1: No such resource")
)

func (f *fakeDRBDExec) calledWith(t *testing.T, substr string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			return
		}
	}
	t.Fatalf("expected call containing %q, got %v", substr, f.calls)
}

func (f *fakeDRBDExec) notCalledWith(t *testing.T, substr string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			t.Fatalf("expected no call containing %q, got %q", substr, c)
		}
	}
}

// A volume whose CR vanished without a final successful teardown here —
// finalizers stripped by hand, the documented wedge recovery — must not
// leave its per-volume metrics behind (miroir_volume_wedged is critical
// and would page forever).
func TestReconcileVolumeGoneDropsMetrics(t *testing.T) {
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}
	metricWedged.WithLabelValues(volPvc1, volPvc1, "").Set(1)
	t.Cleanup(func() { dropVolumeMetrics(volPvc1) })

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(metricWedged); n != 0 {
		t.Fatalf("metrics must drop when the CR is gone, still %d series", n)
	}
}

// Once a leg is latched failed (prior reconcile set DiskFailed), the agent
// renders adjust --skip-disk so DRBD reconciles net/connection state but
// does not re-attach the failing disk — stopping the on-io-error flap
// (#101). The latch is sticky: it stays set though prev DiskState is now
// Diskless, and clears only on a replica re-add.
func TestReconcileLatchedDiskSkipsReAttach(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	// Already latched by a prior reconcile: Diskless and DiskFailed.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateDiskless, DiskFailed: true, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	fe.calledWith(t, "drbdadm adjust --skip-disk "+volPvc1)
	fe.notCalledWith(t, "adjust "+volPvc1) // never a bare re-attach
	fe.notCalledWith(t, "create-md")       // never touch the failing disk
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if st := got.Status.PerNode[nodeA]; !st.DiskFailed {
		t.Fatalf("latch must stay set while the leg is Diskless: %+v", st)
	}
	// The latch is the actionable hardware-failure alert signal.
	if v := testutil.ToFloat64(metricDiskFailed.WithLabelValues(volPvc1, miroirv1alpha1.DefaultPoolName, volPvc1, "")); v != 1 {
		t.Fatalf("miroir_volume_disk_failed = %v, want 1", v)
	}
}

// A latched-failed coordinator (Diskless) cannot drbdadm resize its absent
// local disk. The grow must be withheld, not attempted — otherwise the
// reconcile error-loops on a resize the diskless node can never do.
func TestReconcileLatchedCoordinatorSkipsResize(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.SizeBytes = 2 << 30 // grown; the coordinator is behind
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	// node-a is replicas[0] (coordinator), latched failed and still at the
	// old size.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateDiskless, DiskFailed: true, SizeBytes: 1 << 30},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 2 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdadm resize")
}

// A freeze-policy volume that lost quorum suspends IO while its local
// disk stays UpToDate — quorum is the only signal distinguishing "frozen,
// workloads hanging" from a benign peer outage. The gauge must go 0.
func TestReconcileQuorumLostExportsGauge(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"UpToDate","quorum":false}],
		"connections":[{"peer-node-id":1,"connection-state":"Connecting"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	if v := testutil.ToFloat64(metricQuorum.WithLabelValues(volPvc1, miroirv1alpha1.DefaultPoolName, volPvc1, "")); v != 0 {
		t.Fatalf("miroir_volume_quorum = %v, want 0 on quorum loss", v)
	}
	// Local disk is fine — up_to_date must stay 1 (quorum is the signal).
	if v := testutil.ToFloat64(metricUpToDate.WithLabelValues(volPvc1, miroirv1alpha1.DefaultPoolName, volPvc1, "")); v != 1 {
		t.Fatalf("miroir_volume_up_to_date = %v, want 1", v)
	}
}

// The resize coordinator must not run drbdadm resize once its realized
// size already matches the spec — the steady state, every poll.
func TestReconcileResizeCoordinatorSkipsWhenAtSize(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	// node-a is replicas[0] (the coordinator) and already at spec size.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdadm resize")
}

// A diskless tie-breaker joins DRBD for quorum only: no backend device,
// no metadata seed, DeviceCreated stays false so CSI never stages here.
// A FullSync joiner on a restored volume must create a fresh backing,
// not clone: its node holds no source snapshot (the MiroirSnapshot may
// be gone entirely), and DRBD full-syncs its content anyway.
func TestRealizeBackingFullSyncJoinerCreatesFresh(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapGone}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fb := newFakeBackend()
	r := &VolumeReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fb)}

	if _, err := r.realizeBacking(t.Context(), fb, v, true); err != nil {
		t.Fatal(err)
	}
	if len(fb.fromSnapVol) != 0 {
		t.Fatalf("joiner must not clone: %v", fb.fromSnapVol)
	}
	if !slices.Contains(fb.createVol, volPvc1) {
		t.Fatalf("joiner must create a fresh backing: %v", fb.createVol)
	}
}

// A replicated restore whose source snapshot is gone (deleted by a backup
// tool like kopiur after the mover job) must fall back to a fresh backing
// and let DRBD full-sync from the peer — not park the volume as Failed.
// The peer's realized clone (non-FullSync, DeviceCreated) is what proves
// the data exists somewhere to sync from.
func TestRealizeBackingReplicatedSourceGoneFallsBackToFresh(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapGone}
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true},
	}
	// No MiroirSnapshot in the client — it's been deleted.
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fb := newFakeBackend()
	r := &VolumeReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fb)}

	if _, err := r.realizeBacking(t.Context(), fb, v, false); err != nil {
		t.Fatalf("replicated restore with gone snapshot must not fail: %v", err)
	}
	if len(fb.fromSnapVol) != 0 {
		t.Fatalf("must not clone a gone snapshot: %v", fb.fromSnapVol)
	}
	if !slices.Contains(fb.createVol, volPvc1) {
		t.Fatalf("must create a fresh backing for full-sync: %v", fb.createVol)
	}
}

// A replicated restore whose snapshot is gone before ANY leg cloned has
// nothing to sync from — it must park like the unreplicated case, not
// come up as two blank replicas wearing the restore's Formatted flag.
func TestRealizeBackingReplicatedSourceGoneNoRealizedPeerFails(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapGone}
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fb := newFakeBackend()
	r := &VolumeReconciler{Client: c, NodeName: nodeB, Pools: poolsOf(fb)}

	_, err := r.realizeBacking(t.Context(), fb, v, false)
	if err == nil {
		t.Fatal("no realized peer clone: the fallback has nothing to sync from and must fail")
	}
	if !strings.Contains(err.Error(), "restore source snapshot no longer exists") {
		t.Fatalf("want errRestoreSourceGone, got %v", err)
	}
	if len(fb.createVol) != 0 {
		t.Fatalf("must not create a fresh backing without a data-bearing peer: %v", fb.createVol)
	}
}

// The padded-restore seed is the only data-bearing leg — its joiners are
// FullSync fresh creates. Falling back to a fresh seed would satisfy every
// restoreSeedPending gate and mint the empty backing UpToDate, replicating
// zeros as a "successful" restore. The seed must park instead; a FullSync
// joiner's DeviceCreated must not count as proof of data.
func TestRealizeBackingPaddedSeedSourceGoneFails(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapGone, PadForMetadata: true}
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[1].FullSync = true
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {DeviceCreated: true},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fb := newFakeBackend()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	_, err := r.realizeBacking(t.Context(), fb, v, false)
	if err == nil {
		t.Fatal("padded seed with gone snapshot must fail, not mint an empty full-sync source")
	}
	if !strings.Contains(err.Error(), "restore source snapshot no longer exists") {
		t.Fatalf("want errRestoreSourceGone, got %v", err)
	}
	if len(fb.createVol) != 0 {
		t.Fatalf("must not create a fresh seed backing: %v", fb.createVol)
	}
}

// An unreplicated restore whose source snapshot is gone must still fail —
// the data is truly lost with no peer to sync from.
func TestRealizeBackingUnreplicatedSourceGoneFails(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapGone}
	// No DRBD spec — unreplicated. No MiroirSnapshot — it's been deleted.
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fb := newFakeBackend()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	_, err := r.realizeBacking(t.Context(), fb, v, false)
	if err == nil {
		t.Fatal("unreplicated restore with gone snapshot must fail")
	}
	if !strings.Contains(err.Error(), "restore source snapshot no longer exists") {
		t.Fatalf("want errRestoreSourceGone, got %v", err)
	}
}

func TestRealizeBackingRecloneInvalidatesForeignMetadataWipe(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapSnap1}
	snap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: "source"},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v, snap).Build()
	fb := newFakeBackend()
	fe := &fakeDRBDExec{responses: map[string]string{"dump-md": "version \"v09\";"}}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	if _, err := r.realizeBacking(t.Context(), fb, v, false); err != nil {
		t.Fatal(err)
	}
	delete(fb.created, volPvc1)
	delete(fb.existing, volPvc1)
	if _, err := r.realizeBacking(t.Context(), fb, v, false); err != nil {
		t.Fatal(err)
	}

	var probes, wipes int
	for _, call := range fe.calls {
		if strings.Contains(call, "dump-md") {
			probes++
		}
		if strings.Contains(call, "wipe-md") {
			wipes++
		}
	}
	if probes != 2 || wipes != 2 {
		t.Fatalf("each clone incarnation must be probed and wiped, got %d probes and %d wipes: %v", probes, wipes, fe.calls)
	}
}

// A backing device that vanished after this node had realized it (the
// status slot still says DeviceCreated) is the node-wipe signature: the
// recreated device gets fresh just-created metadata and full-syncs at the
// first handshake instead of posing as the peers' identical twin.
func TestReconcileWipedNodeForcesFullSync(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = "192.168.1.42"
	// The pre-wipe agent had realized the backing and said so in status.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DevicePath: "/dev/mapper/x"},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()

	// The kernel reads before adjust (Apply's peer snapshot, then the
	// metadata probe) find nothing (fresh boot, resource never
	// configured); the post-Apply --json status still answers.
	fe := &fakeDRBDExec{
		statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Inconsistent"}],"connections":[]}]`,
		statusSeq: []string{statusNotInKernel, statusNotInKernel},
	}
	fb := newFakeBackend() // Exists() == false: the wipe took the device
	r := &VolumeReconciler{
		Client:   c,
		NodeName: nodeA,
		Pools:    poolsOf(fb),
		DRBD:     &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	reconcile(t, r, volPvc1)

	fe.calledWith(t, "create-md")
	fe.notCalledWith(t, "new-current-uuid") // just-created metadata → full SyncTarget
}

// A reinstalled node starts with an empty StateDir while the CRs still
// publish its pre-wipe minors. The diskful leg loses its status minor with
// the stale DeviceCreated flag and allocates afresh; the diskless leg keeps
// its status minor. The two must not land on the same minor: drbdadm
// parses every .res in StateDir, so one duplicate fails every resource on
// the node.
func TestReconcileWipedNodeKeepsMinorsDistinct(t *testing.T) {
	for _, order := range [][]string{{volPvc1, volPvc2}, {volPvc2, volPvc1}} {
		t.Run(strings.Join(order, " then "), func(t *testing.T) {
			s := newScheme(t)
			diskful := vol(volPvc1, nodeA, nodeB)
			diskful.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
			diskful.Spec.Replicas[0].NodeID = 0
			diskful.Spec.Replicas[0].Address = addrA
			diskful.Spec.Replicas[1].NodeID = 1
			diskful.Spec.Replicas[1].Address = addrB
			diskful.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {DeviceCreated: true, DevicePath: "/dev/drbd1004", DRBDMinor: 1004},
			}

			tieBreaker := vol(volPvc2, nodeB, nodeC)
			tieBreaker.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7001}
			tieBreaker.Spec.Replicas[0].NodeID = 0
			tieBreaker.Spec.Replicas[0].Address = addrB
			tieBreaker.Spec.Replicas[1].NodeID = 1
			tieBreaker.Spec.Replicas[1].Address = addrC
			tieBreaker.Spec.Replicas = append(tieBreaker.Spec.Replicas, miroirv1alpha1.Replica{
				Node: nodeA, NodeID: 2, Address: addrA, Diskless: true,
			})
			tieBreaker.Finalizers = append(tieBreaker.Finalizers, constants.FinalizerPrefix+nodeA)
			// The minor a fresh allocation hands out first on the empty StateDir.
			tieBreaker.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {Diskless: true, DevicePath: "/dev/drbd1000", DRBDMinor: 1000},
			}

			c := fake.NewClientBuilder().WithScheme(s).WithObjects(diskful, tieBreaker).
				WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
				Build()
			fe := &fakeDRBDExec{}
			r := &VolumeReconciler{
				Client:   c,
				NodeName: nodeA,
				Pools:    poolsOf(newFakeBackend()), // Exists() == false: the wipe took the device
				DRBD:     &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
			}
			for _, name := range order {
				if name == volPvc1 {
					fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
						"devices":[{"disk-state":"Inconsistent"}],"connections":[]}]`
					fe.statusSeq = []string{statusNotInKernel, statusNotInKernel}
				} else {
					fe.statusJSON = `[{"name":"` + volPvc2 + `","role":"Secondary",
						"devices":[{"disk-state":"` + diskStateDiskless + `"}],
						"connections":[{"peer-node-id":0,"connection-state":"Connected"},{"peer-node-id":1,"connection-state":"Connected"}]}]`
				}
				reconcile(t, r, name)
			}

			minors := map[string]int32{}
			for _, name := range order {
				got := &miroirv1alpha1.MiroirVolume{}
				if err := c.Get(t.Context(), types.NamespacedName{Name: name}, got); err != nil {
					t.Fatal(err)
				}
				minors[name] = got.Status.PerNode[nodeA].DRBDMinor
				if minors[name] <= 0 {
					t.Fatalf("%s reports no minor: %+v", name, got.Status.PerNode[nodeA])
				}
			}
			if minors[volPvc1] == minors[volPvc2] {
				t.Fatalf("both volumes rendered minor %d", minors[volPvc1])
			}
		})
	}
}

func TestReconcileDisklessTieBreaker(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, Diskless: true,
	})
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateDiskless + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected"},{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeC, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("tie-breaker must requeue to refresh DRBD state")
	}
	if len(fb.createVol) != 0 {
		t.Fatalf("tie-breaker must not create a backing device: %v", fb.createVol)
	}
	fe.calledWith(t, "drbdadm adjust pvc-1")
	fe.notCalledWith(t, "drbdmeta") // no create-md, no GI seed

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeC]
	if st.DeviceCreated {
		t.Fatal("tie-breaker must not report DeviceCreated (blocks CSI staging)")
	}
	if st.DevicePath == "" || st.DiskState != diskStateDiskless {
		t.Fatalf("tie-breaker status not reported: %+v", st)
	}
}

// Auto-diskful conversion: the spec flips the leg diskful in place while
// the consumer keeps it open, so the resource is up in the kernel as a
// diskless leg over a backing device that does not exist yet. The fresh
// backing must be seeded — reading the live resource as metadata would
// skip create-md and leave adjust attaching a blank device, which fails
// "No valid meta data found" on every pass.
func TestReconcileTieBreakerGainingADiskSeedsMetadata(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	// The converted leg: node-id and address kept, FullSync set — exactly
	// what membership.convertTieBreaker leaves behind.
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, FullSync: true,
	})
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateDiskless + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]},
		{"peer-node-id":1,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`}
	stateDir := t.TempDir()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeC, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	reconcile(t, r, volPvc1)

	if len(fb.createVol) != 1 {
		t.Fatalf("the converted leg must realize a backing device: %v", fb.createVol)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "new-current-uuid") // just-created metadata → full SyncTarget
	if _, err := os.Stat(filepath.Join(stateDir, volPvc1+".md-adopted")); !os.IsNotExist(err) {
		t.Fatal("a blank backing must not be recorded as adopted metadata")
	}
	// A completed seed must leave the marker pair settled: .md-created
	// written and the .md-seeding sentinel cleared. A sentinel left behind
	// authorizes re-seeding a leg that has since full-synced real data.
	if _, err := os.Stat(filepath.Join(stateDir, volPvc1+".md-created")); err != nil {
		t.Fatal("a completed seed must be marked created")
	}
	if _, err := os.Stat(filepath.Join(stateDir, volPvc1+".md-seeding")); !os.IsNotExist(err) {
		t.Fatal("a completed seed must clear the seeding sentinel")
	}
}

func TestReconcileWaitsForIncompleteEntry(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, "node-b")
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	// node-a's entry was just added by an operator: no address yet.
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("must wait for the membership reconciler to complete the entry")
	}
	if len(fb.created) != 0 {
		t.Fatalf("must not realize an incomplete entry: %+v", fb.created)
	}
}

// The probed granularity lands in the local leg's rendered .res for a
// thin backend; loopfile never probes (loop devices mishandle the option).
func TestReconcileRendersDiscardGranularity(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{
		statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"UpToDate","quorum":true}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		responses: map[string]string{"lsblk": "65536\n"},
	}
	stateDir := t.TempDir()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	fe.calledWith(t, "lsblk -bndo DISC-GRAN")
	res, err := os.ReadFile(filepath.Join(stateDir, volPvc1+".res"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(res), "rs-discard-granularity 65536;") {
		t.Fatalf("rendered .res must carry the probed granularity:\n%s", res)
	}
}

func TestReconcileLoopfileSkipsDiscardProbe(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		responses: map[string]string{"lsblk": "65536\n"}}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOfType(fb, miroirv1alpha1.BackendLoopfile),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "lsblk")
}

// countCalls counts fake exec invocations containing substr.
func countCalls(fe *fakeDRBDExec, substr string) int {
	n := 0
	for _, c := range fe.calls {
		if strings.Contains(c, substr) {
			n++
		}
	}
	return n
}

// steadyVolume builds a settled replicated volume + exec fake for the
// fast-path tests: status at spec size, kernel UpToDate and connected.
func steadyVolume(t *testing.T) (*miroirv1alpha1.MiroirVolume, *fakeDRBDExec, *VolumeReconciler) {
	t.Helper()
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}
	return v, fe, r
}

// A second reconcile over unchanged state must cost one status exec, not
// the realize/adjust/probe pipeline — peer status writes re-trigger the
// reconcile every poll cycle and would otherwise re-run everything.
func TestReconcileFastPathSkipsPipeline(t *testing.T) {
	_, fe, r := steadyVolume(t)

	reconcile(t, r, volPvc1) // full pass, stores the fingerprint
	adjusts, statuses := countCalls(fe, "adjust"), countCalls(fe, "drbdsetup status --json")

	reconcile(t, r, volPvc1) // steady state: fast path
	if got := countCalls(fe, "adjust"); got != adjusts {
		t.Fatalf("fast path ran adjust (%d -> %d):\n%v", adjusts, got, fe.calls)
	}
	if got := countCalls(fe, "lsblk"); got != 1 {
		t.Fatalf("fast path must not re-probe discard granularity: %v", fe.calls)
	}
	if got := countCalls(fe, "drbdsetup status --json"); got != statuses+1 {
		t.Fatalf("fast path must still read kernel state once (%d -> %d)", statuses, got)
	}
}

// Kernel drift breaks the fingerprint: the next pass takes the full
// pipeline and re-converges.
func TestReconcileFastPathInvalidatedByStatusDrift(t *testing.T) {
	_, fe, r := steadyVolume(t)
	reconcile(t, r, volPvc1)
	adjusts := countCalls(fe, "adjust")

	// A peer drops: connection state changes under the same generation.
	fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connecting"}]}]`
	reconcile(t, r, volPvc1)
	if got := countCalls(fe, "adjust"); got <= adjusts {
		t.Fatalf("status drift must force the full pipeline: %v", fe.calls)
	}
}

// A spec change (generation bump) breaks the fingerprint.
func TestReconcileFastPathInvalidatedByGeneration(t *testing.T) {
	v, fe, r := steadyVolume(t)
	reconcile(t, r, volPvc1)
	adjusts := countCalls(fe, "adjust")

	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	got.Generation = v.Generation + 1
	if err := r.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("generation bump must force the full pipeline: %v", fe.calls)
	}
}

// A mid-grow pass (reported size withheld) must not be cached: the next
// reconcile keeps taking the full pipeline until the grow settles.
func TestReconcileFastPathSkipsMidGrow(t *testing.T) {
	_, fe, r := steadyVolume(t)
	reconcile(t, r, volPvc1)

	// Spec grows; the local slot lags behind.
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.SizeBytes = 2 << 30
	got.Generation++
	if err := r.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1) // grow pass: withholds size, must not cache
	adjusts := countCalls(fe, "adjust")
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("mid-grow state must keep the full pipeline: %v", fe.calls)
	}
}

// The deep-check interval bounds how long drift with no kernel signature
// (a backing device deleted out-of-band) can hide behind the fast path.
func TestReconcileFastPathDeepCheckExpiry(t *testing.T) {
	_, fe, r := steadyVolume(t)
	reconcile(t, r, volPvc1)
	adjusts := countCalls(fe, "adjust")

	r.remember(volPvc1, func(m *volumeMemory) {
		e := *m.realized
		e.fullPassAt = time.Now().Add(-deepCheckInterval - time.Minute)
		m.realized = &e
	})

	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("expired fingerprint must force the full pipeline: %v", fe.calls)
	}
}

// Regression for the fast-path wedge: a peer disk flipping DUnknown →
// Inconsistent can be the only change in a pass. The fingerprint must
// miss on it, or the winner never re-enters the pipeline and the volume
// parks Creating forever.
func TestFastPathMissesOnPeerDiskStateChange(t *testing.T) {
	r, fe, v := birthSetup(t, nodeA, 1)
	fe.statusJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"Inconsistent"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"DUnknown"}]}]}]`
	st, err := r.DRBD.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	r.storeRealized(v.Generation, volPvc1, st, true)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	got.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, SizeBytes: 1 << 30},
	}
	if err := r.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}

	// The peer's disk turns Inconsistent; everything else is unchanged.
	fe.statusSeq = []string{
		fmt.Sprintf(birthReadyJSON, 1), // fastPath live check → fingerprint miss
		fmt.Sprintf(birthReadyJSON, 1), // Apply's peer snapshot
		fmt.Sprintf(birthReadyJSON, 1), // probeMetadata → attached, adopt
		fmt.Sprintf(birthReadyJSON, 1), // pipeline status → trigger fires
		fmt.Sprintf(birthDoneJSON, 1),  // post-mint re-read
	}
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm new-current-uuid --clear-bitmap pvc-1/0")
}

// The spec's bitmap granularity reaches the render input; create-md picks
// it up from there (drbd.TestApplyBitmapGranularity pins the argv).
func TestDrbdResourceBitmapGranularity(t *testing.T) {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000, BitmapGranularityBytes: 65536}
	r := drbdResource(v, nodeA, "/dev/vg/pvc-1", 1000, false, 0)
	if r.BitmapGranularityBytes != 65536 {
		t.Fatalf("granularity = %d, want 65536", r.BitmapGranularityBytes)
	}
}

// A client leg advertises the max of the diskful legs' published discard
// granularities (mixed backends: aligned for the coarsest works on all);
// replicas and tie-breakers advertise nothing.
func TestDrbdResourceClientDiscardGranularity(t *testing.T) {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeC, NodeID: 2, Address: addrC}}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DiscardGranularityBytes: 262144}, // lvmthin chunk
		nodeB: {DiscardGranularityBytes: 16384},  // zvol block
	}

	if r := drbdResource(v, nodeC, "", 1000, true, 0); r.ClientDiscardGranularityBytes != 262144 {
		t.Fatalf("client granularity = %d, want max(peers) 262144", r.ClientDiscardGranularityBytes)
	}
	if r := drbdResource(v, nodeA, "/dev/vg/pvc-1", 1000, false, 262144); r.ClientDiscardGranularityBytes != 0 {
		t.Fatalf("a replica must not advertise a client granularity, got %d", r.ClientDiscardGranularityBytes)
	}
}

// A client leg (spec.clients) realizes exactly like a diskless
// tie-breaker: no backing device, DRBD adjust with disk none, and a status
// slot marked Diskless so CSI can gate staging on the peers' health.
func TestReconcileClientLegRealizesDiskless(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeC, NodeID: 2, Address: addrC}}
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)
	// A diskful peer has published its backing's discard granularity; the
	// client's device must advertise it (see the .res assertion below).
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DiscardGranularityBytes: 262144},
	}

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateDiskless + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected"},{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeC, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	reconcile(t, r, volPvc1)

	if len(fb.createVol) != 0 {
		t.Fatalf("client leg must not create a backing device: %v", fb.createVol)
	}
	fe.calledWith(t, "drbdadm adjust pvc-1")
	fe.notCalledWith(t, "drbdmeta")

	// The rendered config must exclude the client from quorum voting —
	// exactly once, on the client's own entry, never on the replicas —
	// and advertise the diskful peers' discard granularity (the status
	// fixture publishes node-a's 262144) on the local device.
	res, err := os.ReadFile(filepath.Join(r.DRBD.StateDir, volPvc1+".res"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(res), "tiebreaker no;"); n != 1 {
		t.Fatalf("client leg must render tiebreaker no exactly once, got %d:\n%s", n, res)
	}
	if !strings.Contains(string(res), "discard-granularity 262144;") {
		t.Fatalf("client leg must advertise the peers' discard granularity:\n%s", res)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeC]
	if st.DeviceCreated || !st.Diskless {
		t.Fatalf("client slot must be diskless without a device: %+v", st)
	}
	if st.DevicePath == "" {
		t.Fatal("client slot must record the DRBD device path for staging")
	}
	if v := testutil.ToFloat64(metricDisklessPrimary.WithLabelValues(volPvc1, volPvc1, "")); v != 0 {
		t.Fatalf("diskless_primary = %v, want 0 while Secondary", v)
	}

	// A consumer opens the device: the leg promotes and the remote-consumer
	// gauge flips — the signal the RemoteConsumer alert and auto-diskful
	// dashboards key on.
	fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateDiskless + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected"},{"peer-node-id":1,"connection-state":"Connected"}]}]`
	reconcile(t, r, volPvc1)
	if v := testutil.ToFloat64(metricDisklessPrimary.WithLabelValues(volPvc1, volPvc1, "")); v != 1 {
		t.Fatalf("diskless_primary = %v, want 1 while Primary", v)
	}
}

// An incomplete client leg (membership has not assigned node-id/address)
// must wait, not realize.
func TestReconcileClientLegWaitsForMembership(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeC}}

	fe := &fakeDRBDExec{}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeC, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("incomplete client leg must requeue for membership completion")
	}
	fe.notCalledWith(t, "drbdadm")
}

// An informer lagging the CSI-side Activated latch must not park a
// Primary leg in the fast path: the full pipeline owns the latch, and the
// unprotected state (Primary, no Activated) is what split-brain
// auto-discard safety keys on.
func TestFastPathMissesOnPrimaryWithoutActivatedLatch(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, SizeBytes: 1 << 30}, // settled size
	}

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	// Prime a fingerprint matching the live kernel state: without the
	// Activated guard this reconcile would park in the fast path.
	st, err := r.DRBD.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	r.storeRealized(v.Generation, volPvc1, st, true)

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Activated {
		t.Fatal("the full pipeline must run and latch Activated for a Primary leg")
	}
}

// --- readiness staleness detection reconcile-level tests ---------------------

// When the backing-device probe fails (Exists returns false while status
// says DeviceCreated=true), the agent must clear DeviceCreated and set a
// message so computePhase does not treat the vanished device as realized.
func TestReconcileMissingBackingClearsDeviceCreated(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	// No existing device — simulate the backing being wiped.
	v := vol(volPvc1, nodeA)
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DevicePath: "/dev/fake/pvc-1", SizeBytes: 1 << 30, Connected: true},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	if !st.DeviceCreated {
		t.Fatalf("backing was re-created by the reconcile; DeviceCreated must be true: %+v", st)
	}
	if phaseOf(got) != miroirv1alpha1.VolumeReady {
		t.Fatalf("after realizing the fresh backing, phase = %s, want Ready", phaseOf(got))
	}
	// DeviceCreated reflects the current reality, and LastProbedAt is fresh.
	if st.LastProbedAt == nil {
		t.Fatal("LastProbedAt must be stamped after the successful reconcile")
	}
}

// When the DRBD status probe fails, the agent must NOT stamp a fresh
// LastProbedAt — staleness is detectable because the previous probe
// timestamp ages out. Connected/DiskState are already not updated (the
// error path returns before patchStatus).
func TestReconcileDRBDStatusProbeFailurePreservesLastProbedAt(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	// Pre-populate a fresh probe timestamp so we can detect staleness.
	oldProbe := probeTime(-30 * time.Second)
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true, LastProbedAt: oldProbe},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	// DRBD status probe fails; the adjust succeeds so we get through to Status.
	fe := &fakeDRBDExec{
		errOn: map[string]error{
			"drbdsetup status --json " + volPvc1: errors.New("drbdsetup: connection refused"),
		},
	}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	// The reconcile should fail (status probe error), returning an error.
	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err == nil {
		t.Fatal("DRBD status probe failure must surface as an error")
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	// reportError preserves the old slot fields; LastProbedAt must not have
	// been updated with a fresh timestamp.
	if st.LastProbedAt == nil {
		t.Fatal("LastProbedAt must not be nil after probe failure")
	}
	// The old probe was ~30 seconds ago; a fresh probeNow() would be within
	// a few seconds of now.
	if time.Since(st.LastProbedAt.Time) < 25*time.Second {
		t.Fatalf("LastProbedAt appears freshly stamped (%v ago); want it to preserve the old value from ~30s ago", time.Since(st.LastProbedAt.Time))
	}
	// Connected and DiskState must not have been overwritten with stale values.
	if !st.Connected || st.DiskState != diskStateUpToDate {
		t.Fatalf("Connected/DiskState must not be overwritten on probe failure: %+v", st)
	}
}

// A peer-reported split-brain must break the fast path: the losing leg's own
// kernel state is a steady Connecting that never invalidates the fingerprint,
// so only the peers' status can route it into recovery (issue #144).
func TestFastPathMissesOnPeerReportedSplitBrain(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "Connecting", false)
	var v miroirv1alpha1.MiroirVolume
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &v); err != nil {
		t.Fatal(err)
	}
	// Prime the fast path: settled size in this node's slot, the winner's
	// split-brain report, and a fingerprint matching the live kernel state.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {DeviceCreated: true, SizeBytes: 1 << 30},
		nodeA: {DeviceCreated: true, SplitBrain: true},
	}
	if err := r.Status().Update(t.Context(), &v); err != nil {
		t.Fatal(err)
	}
	st, err := r.DRBD.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	r.storeRealized(v.Generation, volPvc1, st, true)

	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}
