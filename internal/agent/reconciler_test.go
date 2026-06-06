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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/backend"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/drbd"
)

// fakeBackend records calls and simulates a thin pool in memory.
type fakeBackend struct {
	created   map[string]int64
	failOn    string
	snapCalls []string
}

func newFakeBackend() *fakeBackend { return &fakeBackend{created: map[string]int64{}} }

func (f *fakeBackend) Setup(context.Context) error { return nil }

func (f *fakeBackend) Sync(context.Context, string) error { return nil }

func (f *fakeBackend) Create(_ context.Context, vol string, size int64) (string, error) {
	if f.failOn == "create" {
		return "", errors.New("pool exploded")
	}
	if _, ok := f.created[vol]; !ok {
		f.created[vol] = size
	}
	return f.DevicePath(vol), nil
}

func (f *fakeBackend) Resize(_ context.Context, vol string, size int64) error {
	if f.created[vol] < size {
		f.created[vol] = size
	}
	return nil
}

func (f *fakeBackend) Snapshot(_ context.Context, vol, snap string) error {
	f.snapCalls = append(f.snapCalls, "snapshot "+vol+"@"+snap)
	return nil
}

func (f *fakeBackend) CreateFromSnapshot(_ context.Context, vol, _, _ string) (string, error) {
	return f.DevicePath(vol), nil
}

func (f *fakeBackend) Delete(_ context.Context, vol string) error {
	delete(f.created, vol)
	return nil
}

func (f *fakeBackend) DeleteSnapshot(_ context.Context, vol, snap string) error {
	f.snapCalls = append(f.snapCalls, "delete "+vol+"@"+snap)
	return nil
}

func (f *fakeBackend) DevicePath(vol string) string { return "/dev/fake/" + vol }

func (f *fakeBackend) Stats(context.Context) (backend.PoolStats, error) {
	return backend.PoolStats{SizeBytes: 1 << 40}, nil
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := homefsv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

//nolint:unparam // future tests will vary the name
func vol(name string, nodes ...string) *homefsv1alpha1.HomefsVolume {
	replicas := make([]homefsv1alpha1.Replica, 0, len(nodes))
	for _, n := range nodes {
		replicas = append(replicas, homefsv1alpha1.Replica{
			Node: n, Backend: homefsv1alpha1.BackendLVMThin,
		})
	}
	finalizers := make([]string, 0, len(nodes))
	for _, n := range nodes {
		finalizers = append(finalizers, constants.FinalizerPrefix+n)
	}
	return &homefsv1alpha1.HomefsVolume{
		TypeMeta: metav1.TypeMeta{
			APIVersion: homefsv1alpha1.GroupVersion.String(),
			Kind:       "HomefsVolume",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Finalizers: finalizers,
		},
		Spec: homefsv1alpha1.HomefsVolumeSpec{SizeBytes: 1 << 30, Replicas: replicas},
	}
}

//nolint:unparam // future tests will vary the name
func reconcile(t *testing.T, r *VolumeReconciler, name string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconcileRealizesReplica(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol("pvc-1", "kharkiv")).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	reconcile(t, r, "pvc-1")

	if fb.created["pvc-1"] != 1<<30 {
		t.Fatalf("device not created: %+v", fb.created)
	}
	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != homefsv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready (status %+v)", got.Status.Phase, got.Status.PerNode)
	}
	if got.Status.PerNode["kharkiv"].DevicePath != "/dev/fake/pvc-1" {
		t.Fatalf("unexpected status %+v", got.Status.PerNode)
	}
}

func TestReconcileIgnoresForeignVolume(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol("pvc-1", "paris")).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	reconcile(t, r, "pvc-1")

	if len(fb.created) != 0 {
		t.Fatalf("must not touch foreign volumes: %+v", fb.created)
	}
}

func TestReconcileReportsBackendError(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.failOn = "create"
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol("pvc-1", "kharkiv")).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	_, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}})
	if err == nil {
		t.Fatal("expected error to requeue")
	}

	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != homefsv1alpha1.VolumeFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
	}
	if got.Status.PerNode["kharkiv"].Message == "" {
		t.Fatal("error message must be reported in status")
	}
}

func TestReconcileReplicatedVolume(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.QuorumPolicy = homefsv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = "192.168.1.41"
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = "192.168.1.42"

	stateDir := t.TempDir()
	// Pre-seed .res so assignMinor → AllocateMinor picks minor 1000.
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n"+
			"    on \"kharkiv\" {\n"+
			"        device minor 1000;\n"+
			"    }\n"+
			"}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}

	fe := &fakeDRBDExec{statusJSON: `[{"name":"pvc-1",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"connection-state":"Connected"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: "kharkiv", Backend: fb,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("replicated volumes must requeue to refresh DRBD state")
	}
	if fb.created["pvc-1"] != 1<<30 {
		t.Fatal("backing device not created")
	}
	fe.calledWith(t, "drbdadm adjust pvc-1")

	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode["kharkiv"]
	if st.DevicePath != "/dev/drbd1000" {
		t.Fatalf("pods must attach the DRBD device, got %q", st.DevicePath)
	}
	if st.DiskState != "UpToDate" || !st.Connected {
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
	got.Status.PerNode["paris"] = homefsv1alpha1.ReplicaStatus{
		DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "UpToDate", Connected: true,
	}
	if err := c.Status().Patch(context.Background(), got, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}}); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm resize pvc-1")
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode["kharkiv"].SizeBytes != 1<<30 {
		t.Fatalf("size must publish after DRBD resize, got %d", got.Status.PerNode["kharkiv"].SizeBytes)
	}
	if got.Status.Phase != homefsv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready with both legs UpToDate", got.Status.Phase)
	}
}

type fakeDRBDExec struct {
	calls      []string
	statusJSON string
}

func (f *fakeDRBDExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	if strings.HasPrefix(line, "drbdsetup status") {
		return f.statusJSON, nil
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
	v := vol("pvc-1", "paris") // replica on paris...
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb} // ...agent on kharkiv

	reconcile(t, r, "pvc-1")

	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("volume must still exist (finalizer held): %v", err)
	}
	if len(got.Finalizers) == 0 {
		t.Fatal("foreign agent removed the finalizer")
	}
}

func TestReconcileTeardownOnDelete(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol("pvc-1", "kharkiv")
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	// Pre-create the device so teardown has something to remove.
	if _, err := fb.Create(context.Background(), "pvc-1", 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, "pvc-1")

	if len(fb.created) != 0 {
		t.Fatalf("device must be deleted: %+v", fb.created)
	}
	// Finalizer removed → fake client garbage-collects the object.
	got := &homefsv1alpha1.HomefsVolume{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got)
	if err == nil {
		t.Fatalf("volume should be gone, still has finalizers %v", got.Finalizers)
	}
}

// computePhase is the function the controller's waitReady depends on;
// covering its mixed-state logic here means a regression breaks the
// test that mirrors the live behaviour, not a synthetic helper.
func TestComputePhaseMixing(t *testing.T) {
	cases := []struct {
		name string
		vol  *homefsv1alpha1.HomefsVolume
		want homefsv1alpha1.VolumePhase
	}{
		{
			name: "all replicas ready (unreplicated)",
			vol: &homefsv1alpha1.HomefsVolume{
				Spec: homefsv1alpha1.HomefsVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []homefsv1alpha1.Replica{{Node: "a"}},
				},
				Status: homefsv1alpha1.HomefsVolumeStatus{
					PerNode: map[string]homefsv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30},
					},
				},
			},
			want: homefsv1alpha1.VolumeReady,
		},
		{
			name: "one ready, one not (replicated, degraded)",
			vol: &homefsv1alpha1.HomefsVolume{
				Spec: homefsv1alpha1.HomefsVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []homefsv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &homefsv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: homefsv1alpha1.HomefsVolumeStatus{
					PerNode: map[string]homefsv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "UpToDate"},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Inconsistent"},
					},
				},
			},
			want: homefsv1alpha1.VolumeDegraded,
		},
		{
			name: "all replicas Inconsistent (creating)",
			vol: &homefsv1alpha1.HomefsVolume{
				Spec: homefsv1alpha1.HomefsVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []homefsv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &homefsv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: homefsv1alpha1.HomefsVolumeStatus{
					PerNode: map[string]homefsv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Inconsistent"},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Inconsistent"},
					},
				},
			},
			want: homefsv1alpha1.VolumeCreating,
		},
		{
			name: "hard failure (no device, message set)",
			vol: &homefsv1alpha1.HomefsVolume{
				Spec: homefsv1alpha1.HomefsVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []homefsv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
				},
				Status: homefsv1alpha1.HomefsVolumeStatus{
					PerNode: map[string]homefsv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: false, Message: "pool exploded"},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30},
					},
				},
			},
			want: homefsv1alpha1.VolumeFailed,
		},
		{
			name: "transient error after device exists (stays Degraded, not Failed)",
			vol: &homefsv1alpha1.HomefsVolume{
				Spec: homefsv1alpha1.HomefsVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []homefsv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &homefsv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: homefsv1alpha1.HomefsVolumeStatus{
					PerNode: map[string]homefsv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "UpToDate"},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Outdated", Message: "peer not yet up"},
					},
				},
			},
			want: homefsv1alpha1.VolumeDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := computePhase(tc.vol); got != tc.want {
				t.Fatalf("phase = %s, want %s", got, tc.want)
			}
		})
	}
}

// reportError must not demote Ready volumes on transient errors.
func TestReportErrorPreservesObservedState(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol("pvc-1", "kharkiv")).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}
	if err := c.Status().Patch(context.Background(), &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
	}, client.RawPatch(types.MergePatchType, []byte(`{
		"status": {"perNode": {"kharkiv": {
			"deviceCreated": true, "sizeBytes": 1073741824, "connected": true
		}}}
	}`))); err != nil {
		t.Fatal(err)
	}
	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	if err := r.reportError(context.Background(), got, errors.New("transient K8s blip")); err == nil {
		t.Fatal("expected reportError to requeue with the original cause")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode["kharkiv"]
	if !st.DeviceCreated || st.SizeBytes != 1<<30 || !st.Connected {
		t.Fatalf("reportError wiped observed state: %+v", st)
	}
	if st.Message == "" {
		t.Fatal("reportError must set Message")
	}
}

// removedReplicaVol builds a 2-replica volume on paris+oslo whose kharkiv
// leg was just removed from spec.replicas (finalizer still held).
func removedReplicaVol() *homefsv1alpha1.HomefsVolume {
	v := vol("pvc-1", "paris", "oslo")
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+"kharkiv")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	for i := range v.Spec.Replicas {
		v.Spec.Replicas[i].NodeID = int32(i)
		v.Spec.Replicas[i].Address = "192.168.1.4" + string(rune('1'+i))
	}
	return v
}

func patchPeersUpToDate(t *testing.T, c client.Client, diskState string) {
	t.Helper()
	err := c.Status().Patch(context.Background(), &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
	}, client.RawPatch(types.MergePatchType, []byte(`{
		"status": {"perNode": {
			"paris": {"deviceCreated": true, "diskState": "`+diskState+`", "connected": true},
			"oslo": {"deviceCreated": true, "diskState": "UpToDate", "connected": true},
			"kharkiv": {"deviceCreated": true, "diskState": "UpToDate", "connected": true}
		}}
	}`)))
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconcileRemovedReplicaTearsDown(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created["pvc-1"] = 1 << 30
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte("resource"), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(removedReplicaVol()).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	patchPeersUpToDate(t, c, "UpToDate")
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run}}

	reconcile(t, r, "pvc-1")

	if _, ok := fb.created["pvc-1"]; ok {
		t.Fatal("backing device not deleted")
	}
	fe.calledWith(t, "drbdsetup down pvc-1")
	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	for _, f := range got.Finalizers {
		if f == constants.FinalizerPrefix+"kharkiv" {
			t.Fatal("finalizer not released after removal teardown")
		}
	}
	if _, ok := got.Status.PerNode["kharkiv"]; ok {
		t.Fatal("removed replica's status slot not cleared")
	}
}

func TestReconcileRemovalBlockedBySnapshot(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created["pvc-1"] = 1 << 30
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(removedReplicaVol(), &homefsv1alpha1.HomefsSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-1"},
			Spec:       homefsv1alpha1.HomefsSnapshotSpec{VolumeName: "pvc-1"},
		}).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	patchPeersUpToDate(t, c, "UpToDate")
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("blocked removal must requeue")
	}
	if _, ok := fb.created["pvc-1"]; !ok {
		t.Fatal("must not tear down while a snapshot references the volume")
	}
	got := &homefsv1alpha1.HomefsVolume{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Status.PerNode["kharkiv"].Message, "snapshot") {
		t.Fatalf("blocked reason not surfaced: %+v", got.Status.PerNode["kharkiv"])
	}
}

func TestReconcileRemovalBlockedByDegradedPeer(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created["pvc-1"] = 1 << 30
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(removedReplicaVol()).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	patchPeersUpToDate(t, c, "Inconsistent") // paris still syncing
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("blocked removal must requeue")
	}
	if _, ok := fb.created["pvc-1"]; !ok {
		t.Fatal("must not cut the leg while a remaining replica is not UpToDate")
	}
}

func TestReconcileWaitsForIncompleteEntry(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol("pvc-1", "kharkiv", "paris")
	v.Spec.DRBD = &homefsv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = "192.168.1.42"
	// kharkiv's entry was just added by an operator: no address yet.
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&homefsv1alpha1.HomefsVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: "kharkiv", Backend: fb}

	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}})
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
