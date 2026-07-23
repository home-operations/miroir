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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
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
	diskStateInconsistent = "Inconsistent"
	diskStateDiskless     = "Diskless"
	nodeC                 = "node-c"
	addrC                 = "192.168.1.43"
	// cmdDownPvc1 keys fakeDRBDExec.errOn for `drbdsetup down pvc-1`.
	cmdDownPvc1 = "drbdsetup down pvc-1"
	// snapGone names a deleted source snapshot in restore-source-gone tests.
	snapGone = "snap-gone"
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
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready (status %+v)", got.Status.Phase, got.Status.PerNode)
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
	if got.Status.Phase != miroirv1alpha1.VolumeFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
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
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready (status %+v)", got.Status.Phase, got.Status.PerNode)
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
	if got.Status.Phase != miroirv1alpha1.VolumeFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
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
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("replicated volumes must requeue to refresh DRBD state")
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
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready with both legs UpToDate", got.Status.Phase)
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
	// Empty statusJSON: probeMetadata's kernel probe must see no attached
	// resource, or the fresh clone reads as adopted metadata and create-md
	// never runs. The --json status reads are served from statusSeq.
	up := `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{up, up, up, up}}
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

// The seed of a padded restore sits on fresh create-md metadata that
// nothing else can promote (the birth flow refuses data-bearing volumes,
// and there was no metadata to adopt): the agent must mint its data
// generation with a full bitmap so the joiners full-sync from it.
func TestReconcilePaddedRestoreSeedMintsGeneration(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	// statusSeq, not statusJSON: the metadata must read as this agent's
	// own fresh create (see the create-md test above) — adopted metadata
	// carries a generation and correctly refuses the mint.
	inc := `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateInconsistent + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{inc, inc, inc, inc}}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	fe.calledWith(t, "drbdadm primary --force "+volPvc1)
	fe.calledWith(t, "drbdadm secondary "+volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// A seed leg recreated after a node wipe wears the same fresh-metadata
// signature as first bring-up, but its peer holds the data now: the mint
// must refuse and let the leg join as a plain full SyncTarget.
func TestReconcilePaddedRestoreSeedMintRefusesWhenPeerHasData(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {DiskState: diskStateUpToDate, DeviceCreated: true},
	}
	// statusSeq like the mint test: with freshly created metadata the
	// peer-data guard must be the thing refusing, not metadata adoption.
	inc := `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateInconsistent + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{inc, inc, inc, inc}}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	fe.notCalledWith(t, "primary --force")
}

// A padded-restore seed found Primary and UpToDate without ever being
// Activated can only be a mint whose demote never ran (the crash window
// between promote and secondary): the next pass must demote it so a
// consumer on the peer node can promote.
func TestReconcilePaddedRestoreSeedDemotesInterruptedMint(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	up := `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{up, up, up, up}}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	fe.calledWith(t, "drbdadm secondary "+volPvc1)
	fe.notCalledWith(t, "primary --force")
}

// Every status apply must name only this node's slot and the phase — never a
// peer's slot or Formatted (a CSI-owned field). A full-status apply would
// force-own those and revert them to this agent's stale read, hot-looping the
// agents against each other; reverting Formatted risks re-mkfs of live data.
// Asserting on the wire payload proves the scope regardless of how faithfully
// the fake client models server-side apply ownership.
func TestReconcile_StatusApplyScopedToOwnSlot(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	// A peer slot and a CSI-owned Formatted flag this agent must not touch.
	v.Status.Formatted = true
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
	}

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}

	var applies []*acv1alpha1.MiroirVolumeStatusApplyConfiguration
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceApply: func(ctx context.Context, cl client.Client, sub string,
				obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				if ac, ok := obj.(*acv1alpha1.MiroirVolumeApplyConfiguration); ok && ac.Status != nil {
					applies = append(applies, ac.Status)
				}
				return cl.SubResource(sub).Apply(ctx, obj, opts...)
			},
		}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	if len(applies) == 0 {
		t.Fatal("expected at least one server-side apply of status")
	}
	for i, st := range applies {
		if _, ok := st.PerNode[nodeA]; !ok {
			t.Errorf("apply %d omits this node's slot: %+v", i, st.PerNode)
		}
		if _, ok := st.PerNode[nodeB]; ok {
			t.Errorf("apply %d names the peer's slot (would force-own it): %+v", i, st.PerNode)
		}
		if st.Formatted != nil {
			t.Errorf("apply %d sets Formatted (would force-own a CSI field)", i)
		}
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
	errOn     map[string]error
}

func (f *fakeDRBDExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	for key, err := range f.errOn {
		if strings.Contains(line, key) {
			return "", err
		}
	}
	if strings.HasPrefix(line, "drbdsetup status --json") && len(f.statusSeq) > 0 {
		out := f.statusSeq[0]
		f.statusSeq = f.statusSeq[1:]
		return out, nil
	}
	if strings.HasPrefix(line, "drbdsetup status") {
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

var errFreshDevice = errors.New("no valid meta data")

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
	r.busyMu.Lock()
	streak := len(r.busyFails)
	r.busyMu.Unlock()
	if streak != 0 {
		t.Fatalf("busy streak must clear on success, still %d entries", streak)
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
		busyFails: map[string]int{volPvc1: busyFailLimit - 1}}
	reconcile(t, r, volPvc1)
	if n := r.busyFails[volPvc1]; n != 0 {
		t.Fatalf("normal reconcile must reset the busy streak, still %d", n)
	}

	// Pending-removal replica whose teardown is blocked.
	fb = newFakeBackend()
	v = vol(volPvc1, nodeB) // leg moved to node-b; node-a keeps its finalizer
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeA)
	c = fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	r = &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		busyFails: map[string]int{volPvc1: busyFailLimit - 1}}
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil || res.RequeueAfter != 30*time.Second {
		t.Fatalf("want the 30s removal-blocked requeue, got %v / %v", res.RequeueAfter, err)
	}
	if n := r.busyFails[volPvc1]; n != 0 {
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

// A diskful leg DRBD detached after an I/O error reads Diskless while the
// volume serves via the peer. The reconcile must explain it (actionable
// Message) and keep the leg DeviceCreated=true so the volume stays
// Degraded, not hard-Failed.
func TestReconcileDetachedDiskGetsActionableMessage(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	// The leg was UpToDate before the disk errored.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	// DRBD reports the local leg detached (Diskless).
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	if !strings.Contains(st.Message, "detached") {
		t.Fatalf("detached leg must carry an actionable message: %q", st.Message)
	}
	if !st.DeviceCreated {
		t.Fatalf("detached leg must stay DeviceCreated (Degraded, not Failed): %+v", st)
	}
	if !st.DiskFailed {
		t.Fatalf("first detach must latch DiskFailed so the next reconcile skips re-attach: %+v", st)
	}
	// The latch is read from the prior reconcile's status, so this first
	// detection still ran a bare adjust (one re-attach), then latched.
	fe.calledWith(t, "drbdadm adjust "+volPvc1)
	fe.notCalledWith(t, "--skip-disk")
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

// A Primary leg latches Activated from the kernel role: a raw-block
// consumer staged before the field existed never restages, and without
// the latch recoverSplitBrain would treat its data-bearing volume as
// never-written and auto-discard a leg.
func TestReconcilePrimaryLatchesActivated(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
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
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Activated {
		t.Fatal("Primary leg must latch Status.Activated")
	}
}

// A Secondary leg must not latch: an unstaged volume stays eligible for
// split-brain auto-recovery.
func TestReconcileSecondaryDoesNotLatchActivated(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Activated {
		t.Fatal("Secondary leg must not latch Status.Activated")
	}
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
func TestRealizeBackingReplicatedSourceGoneFallsBackToFresh(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapGone}
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
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

	// The kernel probe fails (fresh boot, resource never configured);
	// the post-Apply --json status still answers.
	fe := &fakeDRBDExec{
		statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Inconsistent"}],"connections":[]}]`,
		errOn: map[string]error{"drbdsetup status " + volPvc1: errors.New("no such resource")},
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
	fe.notCalledWith(t, "set-gi") // just-created metadata → full SyncTarget
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

// Status.Connected scopes to diskful peers: a downed tie-breaker link
// must not read as degraded replication, while a downed data leg must.
func TestDiskfulPeersConnected(t *testing.T) {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, Diskless: true,
	})

	tieBreakerDown := drbd.Status{PeerConnected: map[int32]bool{1: true, 2: false}}
	if !diskfulPeersConnected(tieBreakerDown, v, nodeA) {
		t.Fatal("a downed tie-breaker link must not count as disconnected")
	}
	dataLegDown := drbd.Status{PeerConnected: map[int32]bool{1: false, 2: true}}
	if diskfulPeersConnected(dataLegDown, v, nodeA) {
		t.Fatal("a downed data leg must count as disconnected")
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

// computePhase is the function the controller's waitReady depends on;
// covering its mixed-state logic here means a regression breaks the
// test that mirrors the live behaviour, not a synthetic helper.
func TestComputePhaseMixing(t *testing.T) {
	const (
		noneOfTwo = "0/2"
		oneOfTwo  = "1/2"
		twoOfTwo  = "2/2"
		zeroOfOne = "0/1"
	)
	cases := []struct {
		name      string
		vol       *miroirv1alpha1.MiroirVolume
		want      miroirv1alpha1.VolumePhase
		wantReady string
	}{
		{
			name: "all replicas ready (unreplicated)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: oneOfOne,
		},
		{
			name: "one ready, one not (replicated, degraded)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "both diskful replicas disconnected (degraded)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: noneOfTwo,
		},
		{
			name: "one diskful replica disconnected (degraded)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "all diskful replicas reconnected (ready)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "no realized replicas (creating)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: noneOfTwo,
		},
		{
			name: "all replicas Inconsistent (creating)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
					},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: noneOfTwo,
		},
		{
			name: "hard failure (no device, message set)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: false, Message: "pool exploded"},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30},
					},
				},
			},
			want:      miroirv1alpha1.VolumeFailed,
			wantReady: oneOfTwo,
		},
		{
			name: "transient error after device exists (stays Degraded, not Failed)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Outdated", Message: "peer not yet up"},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "diskless tie-breaker ignored (ready on diskful legs alone)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas: []miroirv1alpha1.Replica{
						{Node: "a"}, {Node: "b"}, {Node: "tb", Diskless: true},
					},
					DRBD: &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						// The tie-breaker's slot must count toward
						// neither ready nor failed.
						"tb": {DiskState: diskStateDiskless, Message: "whatever"},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		// --- Staleness detection (LastProbedAt) ---
		{
			name: "fresh probe + healthy replicated → Ready",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceCreated, upToDate, connected, sized(1<<30)),
						"b": freshProbe(deviceCreated, upToDate, connected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "stale probe + healthy persisted → Degraded",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": staleProbe(deviceCreated, upToDate, connected, sized(1<<30)),
						"b": staleProbe(deviceCreated, upToDate, connected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: noneOfTwo,
		},
		{
			name: "nil LastProbedAt (legacy) + healthy → Ready (backward compatible)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true, LastProbedAt: nil},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true, LastProbedAt: nil},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "probe aged under deepCheckInterval (4 min) + healthy → Ready (steady-state false-positive guard)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": nearProbe(4*time.Minute, deviceCreated, upToDate, connected, sized(1<<30)),
						"b": nearProbe(4*time.Minute, deviceCreated, upToDate, connected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "fresh probe + disconnected → Degraded (preserves existing behavior)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceCreated, upToDate, connected, sized(1<<30)),
						"b": freshProbe(deviceCreated, upToDate, disconnected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "fresh probe + DeviceCreated=false → not realized",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceNotCreated, "", false, 0),
					},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: zeroOfOne,
		},
		{
			name: "DeviceCreated cleared after missing probe + message → Failed (hard provisioning failure)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: false, SizeBytes: 1 << 30, LastProbedAt: probeNow(), Message: "backing device missing"},
					},
				},
			},
			want:      miroirv1alpha1.VolumeFailed,
			wantReady: zeroOfOne,
		},
		{
			name: "stale probe + unreplicated volume → Degraded",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": staleProbe(deviceCreated, "", true, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: zeroOfOne,
		},
		{
			name: "fresh probe + unreplicated volume → Ready",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceCreated, "", true, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: oneOfOne,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotReady := computePhase(tc.vol)
			if got != tc.want {
				t.Fatalf("phase = %s, want %s", got, tc.want)
			}
			if gotReady != tc.wantReady {
				t.Fatalf("readyReplicas = %q, want %q", gotReady, tc.wantReady)
			}
		})
	}
}

// reportError must not demote Ready volumes on transient errors.
func TestReportErrorPreservesObservedState(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}
	if err := c.Status().Patch(t.Context(), &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
	}, client.RawPatch(types.MergePatchType, []byte(`{
		"status": {"perNode": {"`+nodeA+`": {
			"deviceCreated": true, "sizeBytes": 1073741824, "connected": true
		}}}
	}`))); err != nil {
		t.Fatal(err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if err := r.reportError(t.Context(), got, errors.New("transient K8s blip")); err == nil {
		t.Fatal("expected reportError to requeue with the original cause")
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	if !st.DeviceCreated || st.SizeBytes != 1<<30 || !st.Connected {
		t.Fatalf("reportError wiped observed state: %+v", st)
	}
	if st.Message == "" {
		t.Fatal("reportError must set Message")
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

// --- computePhase staleness helpers ------------------------------------------

const (
	deviceCreated    = true
	deviceNotCreated = false
	upToDate         = diskStateUpToDate
	connected        = true
	disconnected     = false
)

func sized(s int64) int64 { return s }

func probeTime(d time.Duration) *metav1.Time {
	t := metav1.NewTime(time.Now().Add(d))
	return &t
}

func freshProbe(dev bool, disk string, conn bool, size int64) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{
		DeviceCreated: dev,
		DiskState:     disk,
		Connected:     conn,
		SizeBytes:     size,
		LastProbedAt:  probeNow(),
	}
}

func staleProbe(dev bool, disk string, conn bool, size int64) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{
		DeviceCreated: dev,
		DiskState:     disk,
		Connected:     conn,
		SizeBytes:     size,
		LastProbedAt:  probeTime(-11 * time.Minute),
	}
}

// nearProbe is a probe aged by the given (negative) duration — used to test
// probes that are stale-ish but still within the StaleProbeThreshold.
func nearProbe(age time.Duration, dev bool, disk string, conn bool, size int64) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{
		DeviceCreated: dev,
		DiskState:     disk,
		Connected:     conn,
		SizeBytes:     size,
		LastProbedAt:  probeTime(-age),
	}
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

// A phase contradicting the per-node slots breaks the fingerprint: a
// sibling's concurrent full pass can force-apply a phase computed from a
// stale informer cache after this leg's correct write (issue #279), and no
// later kernel event breaks statusEqual, so only the recompute can route
// the repair through the full pipeline.
func TestReconcileFastPathInvalidatedByStalePhase(t *testing.T) {
	_, fe, r := steadyVolume(t)
	reconcile(t, r, volPvc1) // full pass, stores the fingerprint
	adjusts := countCalls(fe, "adjust")

	// The peer's slot reached UpToDate but a stale apply left the phase
	// behind at Degraded.
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	got.Status.PerNode[nodeB] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30, Connected: true,
	}
	got.Status.Phase = miroirv1alpha1.VolumeDegraded
	if err := r.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("stale phase must force the full pipeline: %v", fe.calls)
	}
	after := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, after); err != nil {
		t.Fatal(err)
	}
	if after.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %q, want %q", after.Status.Phase, miroirv1alpha1.VolumeReady)
	}
	if after.Status.ReadyReplicas != "2/2" {
		t.Fatalf("readyReplicas = %q, want %q", after.Status.ReadyReplicas, "2/2")
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

	r.realizedMu.Lock()
	e := r.realized[volPvc1]
	e.fullPassAt = time.Now().Add(-deepCheckInterval - time.Minute)
	r.realized[volPvc1] = e
	r.realizedMu.Unlock()

	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("expired fingerprint must force the full pipeline: %v", fe.calls)
	}
}

// splitBrainSetup builds a 2-replica DRBD volume (node-a node id 0 = seed
// winner, node-b node id 1) whose kernel status reports a connState
// connection to peerNodeID, plus a reconciler running on nodeName. activated
// latches Status.Activated (the auto-recovery gate).
func splitBrainSetup(t *testing.T, nodeName, peerNodeID, connState string, activated bool) (*VolumeReconciler, *fakeDRBDExec) {
	t.Helper()
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Status.Activated = activated

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":` + peerNodeID + `,"connection-state":"` + connState + `"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeName, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	return r, fe
}

// A never-activated volume that comes up split-brain self-heals: the seed
// winner (node-a, node id 0) reconnects as the sync source and never
// discards data (issue #139).
func TestReconcileSplitBrainWinnerReconnectsWhenNeverActivated(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeA, "1", "StandAlone", false)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect pvc-1")
	fe.notCalledWith(t, "discard-my-data")
}

// The losing leg (node-b, node id 1) discards its own generation so it
// full-syncs from the winner.
func TestReconcileSplitBrainLoserDiscardsWhenNeverActivated(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "StandAlone", false)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}

// An activated volume may hold data: split-brain is never auto-resolved, it
// is left for an operator.
func TestReconcileSplitBrainNoAutoRecoveryWhenActivated(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "StandAlone", true)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "drbdadm disconnect")
}

// A formatted-but-not-activated volume (e.g. a clone whose stage failed at
// grow-to-fill after mkfs/mount) carries data and must not be auto-discarded,
// even though Activated is still false.
func TestReconcileSplitBrainNoAutoRecoveryWhenFormatted(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "StandAlone", false)
	var v miroirv1alpha1.MiroirVolume
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &v); err != nil {
		t.Fatal(err)
	}
	v.Status.Formatted = true
	if err := r.Status().Update(t.Context(), &v); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "drbdadm disconnect")
}

// reportPeerSplitBrain marks the given node's status slot split-brain, the
// signal a survivor leaves for the losing leg.
func reportPeerSplitBrain(t *testing.T, r *VolumeReconciler, node string) {
	t.Helper()
	var v miroirv1alpha1.MiroirVolume
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &v); err != nil {
		t.Fatal(err)
	}
	if v.Status.PerNode == nil {
		v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	v.Status.PerNode[node] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true, SplitBrain: true,
	}
	if err := r.Status().Update(t.Context(), &v); err != nil {
		t.Fatal(err)
	}
}

// The losing leg of a birth split never parks StandAlone — its connection
// sits Connecting while the survivor refuses the handshake — so it must take
// the winner's reported split-brain as its trigger and discard (issue #144).
func TestReconcileSplitBrainLoserDiscardsOnPeerReport(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "Connecting", false)
	reportPeerSplitBrain(t, r, nodeA)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}

// A stale peer split-brain report on a healthy, fully connected leg (the
// survivor's status patch lags its recovery) must not churn the volume.
func TestReconcileSplitBrainPeerReportIgnoredWhenConnected(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "Connected", false)
	reportPeerSplitBrain(t, r, nodeA)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "drbdadm disconnect")
}

// Recovery flaps connections, and each flap is a DRBD event that requeues
// the volume — attempts must be floored to one per poll interval or the
// agent thrashes several times per second (issue #144).
func TestReconcileSplitBrainRecoveryDebounced(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeA, "1", "StandAlone", false)
	reconcile(t, r, volPvc1)
	attempts := countCalls(fe, "drbdadm connect pvc-1")
	if attempts != 1 {
		t.Fatalf("first reconcile must attempt recovery once, got %d", attempts)
	}
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "drbdadm connect pvc-1"); n != attempts {
		t.Fatalf("immediate re-reconcile must not re-attempt recovery: %d -> %d", attempts, n)
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

// --- birth generation ---------------------------------------------------

const (
	birthReadyJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"Inconsistent"}],
		"connections":[{"peer-node-id":%d,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"Inconsistent"}]}]}]`
	birthDoneJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"peer-node-id":%d,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"UpToDate"}]}]}]`
)

// birthSetup builds a fresh 2-diskful volume (node-a node id 0 = winner,
// node-b node id 1) and a reconciler on nodeName whose kernel answers the
// queued --json statuses (peerNodeID fills the connection entries).
func birthSetup(t *testing.T, nodeName string, peerNodeID int, statusSeq ...string) (*VolumeReconciler, *fakeDRBDExec, *miroirv1alpha1.MiroirVolume) {
	t.Helper()
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \""+nodeName+"\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	seq := make([]string, len(statusSeq))
	for i, tmpl := range statusSeq {
		seq[i] = fmt.Sprintf(tmpl, peerNodeID)
	}
	fe := &fakeDRBDExec{statusSeq: seq}
	if len(seq) > 0 {
		fe.statusJSON = seq[len(seq)-1] // stable fallback once drained
	}
	// The kernel probe (plain, non-json status) must fail or probeMetadata
	// reads the fallback JSON as "resource attached" and adopts — bypassing
	// the create-md path a fresh volume must take.
	fe.errOn = map[string]error{"drbdsetup status " + volPvc1: errors.New("no such resource")}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeName, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	return r, fe, v
}

// The winner mints the birth generation once every diskful leg sits
// Inconsistent over established connections, and the same pass proceeds
// against the resulting UpToDate state.
func TestReconcileBirthWinnerMintsInitialUUID(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeA, 1, birthReadyJSON, birthDoneJSON)
	reconcile(t, r, volPvc1)
	// The full fresh path: metadata created and left just-created, then the
	// one replicated generation minted over the live connections.
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "set-gi")
	fe.calledWith(t, "drbdadm new-current-uuid --clear-bitmap pvc-1/0")

	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if st := got.Status.PerNode[nodeA]; st.DiskState != diskStateUpToDate {
		t.Fatalf("winner must report the post-birth state, got %+v", st)
	}
}

// The loser waits: only the winner may mint, or two generations could race.
func TestReconcileBirthLoserWaits(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeB, 0, birthReadyJSON)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// No mint while a diskful peer is still disconnected or its disk state is
// unknown — the generation must land on every leg over live connections.
func TestReconcileBirthWaitsForPeers(t *testing.T) {
	for name, status := range map[string]string{
		"peer connecting": `[{"name":"pvc-1","role":"Secondary",
			"devices":[{"disk-state":"Inconsistent"}],
			"connections":[{"peer-node-id":%d,"connection-state":"Connecting",
				"peer_devices":[{"peer-disk-state":"DUnknown"}]}]}]`,
		"peer disk unknown": `[{"name":"pvc-1","role":"Secondary",
			"devices":[{"disk-state":"Inconsistent"}],
			"connections":[{"peer-node-id":%d,"connection-state":"Connected",
				"peer_devices":[{"peer-disk-state":"DUnknown"}]}]}]`,
	} {
		r, fe, _ := birthSetup(t, nodeA, 1, status)
		reconcile(t, r, volPvc1)
		if n := countCalls(fe, "new-current-uuid"); n != 0 {
			t.Fatalf("%s: must not mint, got %d calls", name, n)
		}
	}
}

// A FullSync joiner means the volume already has a generation elsewhere:
// never mint over it.
func TestReconcileBirthSkipsFullSyncJoiner(t *testing.T) {
	r, fe, v := birthSetup(t, nodeA, 1, birthReadyJSON)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: v.Name}, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.Replicas[1].FullSync = true
	if err := r.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// An Activated volume held data: an all-Inconsistent state is then a real
// incident, never something to paper over with a fresh generation.
func TestReconcileBirthSkipsActivatedVolume(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeA, 1, birthReadyJSON)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	got.Status.Activated = true
	if err := r.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// A restored backing contains snapshot data even when its DRBD metadata looks
// freshly created. Never mint a birth UUID over it: that destructive command
// is reserved for volumes born empty.
func TestReconcileBirthSkipsSnapshotDerivedVolume(t *testing.T) {
	r, fe, v := birthSetup(t, nodeA, 1, birthReadyJSON)
	if err := r.Create(t.Context(), snapObj(snapSnap1, "source-volume")); err != nil {
		t.Fatal(err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: v.Name}, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapSnap1}
	if err := r.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// A failed mint surfaces as a reconcile error (retried with backoff) and
// keeps DeviceCreated so the phase never reads as a hard provisioning
// failure.
func TestReconcileBirthMintErrorRetries(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeA, 1, birthReadyJSON)
	fe.errOn = map[string]error{"new-current-uuid": errors.New("exit status 1")}
	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err == nil {
		t.Fatal("a failed mint must surface as a reconcile error")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.PerNode[nodeA].DeviceCreated {
		t.Fatal("DeviceCreated must survive a failed mint")
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

// PrimarySince stamps when the leg becomes Primary, stays stable across
// passes, and clears on demotion — the auto-diskful tie-breaker signal.
func TestReconcilePrimarySinceLifecycle(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	primary := `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`
	fe := &fakeDRBDExec{statusJSON: primary}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	stamped := got.Status.PerNode[nodeA].PrimarySince
	if stamped == nil {
		t.Fatal("Primary leg must stamp PrimarySince")
	}

	// Still Primary: the stamp must not move.
	reconcile(t, r, volPvc1)
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if since := got.Status.PerNode[nodeA].PrimarySince; since == nil || !since.Equal(stamped) {
		t.Fatalf("PrimarySince must be stable while Primary: %v vs %v", since, stamped)
	}

	// Demoted: the stamp clears.
	fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`
	reconcile(t, r, volPvc1)
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].PrimarySince != nil {
		t.Fatal("PrimarySince must clear on demotion")
	}
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
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("after realizing the fresh backing, phase = %s, want Ready", got.Status.Phase)
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
