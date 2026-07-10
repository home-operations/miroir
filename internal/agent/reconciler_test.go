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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

const (
	nodeKharkiv           = "kharkiv"
	nodeParis             = "paris"
	addrKharkiv           = "192.168.1.41"
	addrParis             = "192.168.1.42"
	volPvc1               = "pvc-1"
	snapSnap1             = "snap-1"
	diskStateUpToDate     = "UpToDate"
	diskStateInconsistent = "Inconsistent"
	diskStateDiskless     = "Diskless"
	nodeOslo              = "oslo"
	addrOslo              = "192.168.1.43"
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
		WithObjects(vol(volPvc1, nodeKharkiv)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	if got.Status.PerNode[nodeKharkiv].DevicePath != "/dev/fake/pvc-1" {
		t.Fatalf("unexpected status %+v", got.Status.PerNode)
	}
	// No peers means fully in sync: the zero value would perma-fire any
	// <1 resync alert for every unreplicated volume.
	if ratio := testutil.ToFloat64(metricResyncRatio.WithLabelValues(volPvc1)); ratio != 1 {
		t.Fatalf("unreplicated resync_ratio = %v, want 1", ratio)
	}
}

func TestReconcileIgnoresForeignVolume(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, "paris")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
		WithObjects(vol(volPvc1, nodeKharkiv)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	if got.Status.PerNode[nodeKharkiv].Message == "" {
		t.Fatal("error message must be reported in status")
	}
}

// TestReconcileSourceSnapshotGoneRecoversBacking: a GC'd source snapshot must
// not strand a volume whose backing survived the reboot.
func TestReconcileSourceSnapshotGoneRecoversBacking(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.existing[volPvc1] = true // backing survived the reboot
	v := vol(volPvc1, nodeKharkiv)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-deleted"}
	// No MiroirSnapshot object in the client: it has been garbage-collected.
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	if got.Status.PerNode[nodeKharkiv].DevicePath != "/dev/fake/pvc-1" {
		t.Fatalf("backing not recovered: %+v", got.Status.PerNode)
	}
}

// TestReconcileSourceSnapshotGoneAndDeviceMissingFails: no snapshot and no
// device — the restore can't complete, so fail loud rather than seed empty.
func TestReconcileSourceSnapshotGoneAndDeviceMissingFails(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend() // no existing device
	v := vol(volPvc1, nodeKharkiv)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-deleted"}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

	_, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err == nil {
		t.Fatal("expected error: restore source is gone and device was never created")
	}
	if len(fb.createVol) != 0 || len(fb.fromSnapVol) != 0 {
		t.Fatalf("must not create or clone an empty device: create=%v fromSnap=%v",
			fb.createVol, fb.fromSnapVol)
	}
}

func TestReconcileReplicatedVolume(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeKharkiv, "paris")
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis

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

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeKharkiv, Backend: fb,
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
	st := got.Status.PerNode[nodeKharkiv]
	if st.DevicePath != "/dev/drbd1000" {
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
	got.Status.PerNode["paris"] = miroirv1alpha1.ReplicaStatus{
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
	if got.Status.PerNode[nodeKharkiv].SizeBytes != 1<<30 {
		t.Fatalf("size must publish after DRBD resize, got %d", got.Status.PerNode[nodeKharkiv].SizeBytes)
	}
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %s, want Ready with both legs UpToDate", got.Status.Phase)
	}
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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis
	// A peer slot and a CSI-owned Formatted flag this agent must not touch.
	v.Status.Formatted = true
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeParis: {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
	}

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"kharkiv\" {\n        device minor 1000;\n    }\n}\n",
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
		Client: c, NodeName: nodeKharkiv, Backend: fb,
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
		if _, ok := st.PerNode[nodeKharkiv]; !ok {
			t.Errorf("apply %d omits this node's slot: %+v", i, st.PerNode)
		}
		if _, ok := st.PerNode[nodeParis]; ok {
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
	v := vol(volPvc1, nodeKharkiv, "paris")
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"kharkiv\" {\n        device minor 1000;\n    }\n}\n",
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
		Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	base := got.DeepCopy()
	got.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		"paris": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
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
	if got.Status.PerNode[nodeKharkiv].SizeBytes != 0 {
		t.Fatalf("size must be withheld while resyncing, got %d", got.Status.PerNode[nodeKharkiv].SizeBytes)
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
	if got.Status.PerNode[nodeKharkiv].SizeBytes != 1<<30 {
		t.Fatalf("size must publish once the resync clears, got %d", got.Status.PerNode[nodeKharkiv].SizeBytes)
	}
}

func TestReconcile_ResizeRaceWithResyncIsTransient(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeKharkiv, "paris")
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"kharkiv\" {\n        device minor 1000;\n    }\n}\n",
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
		Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	base := got.DeepCopy()
	got.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		"paris": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
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
	if got.Status.PerNode[nodeKharkiv].SizeBytes != 0 {
		t.Fatalf("size must be withheld until the resize succeeds, got %d", got.Status.PerNode[nodeKharkiv].SizeBytes)
	}
}

type fakeDRBDExec struct {
	calls      []string
	statusJSON string
	responses  map[string]string
	errOn      map[string]error
}

func (f *fakeDRBDExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	for key, err := range f.errOn {
		if strings.Contains(line, key) {
			return "", err
		}
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
	v := vol(volPvc1, "paris") // replica on paris...
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb} // ...agent on kharkiv

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatalf("volume must still exist (finalizer held): %v", err)
	}
	if len(got.Finalizers) == 0 {
		t.Fatal("foreign agent removed the finalizer")
	}
}

func TestReconcileTeardownOnDelete(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeKharkiv)
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	v := vol(volPvc1, nodeKharkiv)
	now := metav1.NewTime(time.Now())
	v.DeletionTimestamp = &now
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateDiskless},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if len(fb.created) != 0 {
		t.Fatalf("a detached (DiskState=Diskless) diskful replica must still delete its backing: %+v", fb.created)
	}
}

// Teardown behind a still-staged device: drbdsetup down answers "held
// open"; the error must classify as ErrBusy so teardown takes the 10s
// retry, not the workqueue's minutes-long backoff, and the finalizer
// stays until NodeUnstage releases the device.
func TestReconcileTeardownDownHeldOpenRetries(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeKharkiv, nodeParis)
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
		"drbdsetup down pvc-1": errors.New("pvc-1: State change failed: Device is held open by someone"),
	}}
	r := &VolumeReconciler{
		Client: c, NodeName: nodeKharkiv, Backend: fb,
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

// A diskful leg DRBD detached after an I/O error reads Diskless while the
// volume serves via the peer. The reconcile must explain it (actionable
// Message) and keep the leg DeviceCreated=true so the volume stays
// Degraded, not hard-Failed.
func TestReconcileDetachedDiskGetsActionableMessage(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	// The leg was UpToDate before the disk errored.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	// DRBD reports the local leg detached (Diskless).
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeKharkiv]
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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	// Already latched by a prior reconcile: Diskless and DiskFailed.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateDiskless, DiskFailed: true, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	fe.calledWith(t, "drbdadm adjust --skip-disk "+volPvc1)
	fe.notCalledWith(t, "adjust "+volPvc1) // never a bare re-attach
	fe.notCalledWith(t, "create-md")       // never touch the failing disk
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if st := got.Status.PerNode[nodeKharkiv]; !st.DiskFailed {
		t.Fatalf("latch must stay set while the leg is Diskless: %+v", st)
	}
	// The latch is the actionable hardware-failure alert signal.
	if v := testutil.ToFloat64(metricDiskFailed.WithLabelValues(volPvc1)); v != 1 {
		t.Fatalf("miroir_volume_disk_failed = %v, want 1", v)
	}
}

// A latched-failed coordinator (Diskless) cannot drbdadm resize its absent
// local disk. The grow must be withheld, not attempted — otherwise the
// reconcile error-loops on a resize the diskless node can never do.
func TestReconcileLatchedCoordinatorSkipsResize(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.SizeBytes = 2 << 30 // grown; the coordinator is behind
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	// kharkiv is replicas[0] (coordinator), latched failed and still at the
	// old size.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateDiskless, DiskFailed: true, SizeBytes: 1 << 30},
		nodeParis:   {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 2 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdadm resize")
}

// A freeze-policy volume that lost quorum suspends IO while its local
// disk stays UpToDate — quorum is the only signal distinguishing "frozen,
// workloads hanging" from a benign peer outage. The gauge must go 0.
func TestReconcileQuorumLostExportsGauge(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"UpToDate","quorum":false}],
		"connections":[{"peer-node-id":1,"connection-state":"Connecting"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	if v := testutil.ToFloat64(metricQuorum.WithLabelValues(volPvc1)); v != 0 {
		t.Fatalf("miroir_volume_quorum = %v, want 0 on quorum loss", v)
	}
	// Local disk is fine — up_to_date must stay 1 (quorum is the signal).
	if v := testutil.ToFloat64(metricUpToDate.WithLabelValues(volPvc1)); v != 1 {
		t.Fatalf("miroir_volume_up_to_date = %v, want 1", v)
	}
}

// The resize coordinator must not run drbdadm resize once its realized
// size already matches the spec — the steady state, every poll.
func TestReconcileResizeCoordinatorSkipsWhenAtSize(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	// kharkiv is replicas[0] (the coordinator) and already at spec size.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
		nodeParis:   {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-gone"}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fb := newFakeBackend()
	r := &VolumeReconciler{Client: c, NodeName: nodeParis, Backend: fb}

	if _, _, err := r.realizeBacking(t.Context(), v, true); err != nil {
		t.Fatal(err)
	}
	if len(fb.fromSnapVol) != 0 {
		t.Fatalf("joiner must not clone: %v", fb.fromSnapVol)
	}
	if !slices.Contains(fb.createVol, volPvc1) {
		t.Fatalf("joiner must create a fresh backing: %v", fb.createVol)
	}
}

// A backing device that vanished after this node had realized it (the
// status slot still says DeviceCreated) is the node-wipe signature: the
// recreated device must full-sync, never re-seed the day0 GI and pose as
// the peers' identical twin.
func TestReconcileWipedNodeForcesFullSync(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = "192.168.1.42"
	// The pre-wipe agent had realized the backing and said so in status.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DevicePath: "/dev/mapper/x"},
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
		NodeName: nodeKharkiv,
		Backend:  fb,
		DRBD:     &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	reconcile(t, r, volPvc1)

	fe.calledWith(t, "create-md")
	fe.notCalledWith(t, "set-gi") // just-created metadata → full SyncTarget
}

func TestReconcileDisklessTieBreaker(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeOslo, NodeID: 2, Address: addrOslo, Diskless: true,
	})
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeOslo)

	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateDiskless + `"}],
		"connections":[{"peer-node-id":0,"connection-state":"Connected"},{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeOslo, Backend: fb,
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
	st := got.Status.PerNode[nodeOslo]
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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrKharkiv
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeOslo, NodeID: 2, Address: addrOslo, Diskless: true,
	})

	tieBreakerDown := drbd.Status{PeerConnected: map[int32]bool{1: true, 2: false}}
	if !diskfulPeersConnected(tieBreakerDown, v, nodeKharkiv) {
		t.Fatal("a downed tie-breaker link must not count as disconnected")
	}
	dataLegDown := drbd.Status{PeerConnected: map[int32]bool{1: false, 2: true}}
	if diskfulPeersConnected(dataLegDown, v, nodeKharkiv) {
		t.Fatal("a downed data leg must count as disconnected")
	}
}

// Removing a tie-breaker is not pinned by snapshots (it holds no backend
// CoW state); removing a diskful replica still is. The self-reported
// status marker decides — the entry is already gone from spec.
func TestRemovalSnapshotGateSkipsTieBreaker(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeKharkiv, nodeParis) // oslo already removed from spec
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateUpToDate, Connected: true},
		nodeParis:   {DeviceCreated: true, DiskState: diskStateUpToDate, Connected: true},
		nodeOslo:    {DiskState: diskStateDiskless, Diskless: true},
	}
	snap := snapObj(snapSnap1, volPvc1, nodeKharkiv, nodeParis)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snap).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}, &miroirv1alpha1.MiroirSnapshot{}).
		Build()

	rO := &VolumeReconciler{Client: c, NodeName: nodeOslo, Backend: newFakeBackend()}
	if reason := rO.removalBlocked(t.Context(), v); reason != "" {
		t.Fatalf("tie-breaker removal must not be pinned by snapshots: %s", reason)
	}
	// A removed diskful replica (no Diskless marker) stays pinned.
	rK := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: newFakeBackend()}
	if reason := rK.removalBlocked(t.Context(), v); !strings.Contains(reason, "snapshot") {
		t.Fatalf("diskful removal must stay pinned by snapshots, got %q", reason)
	}
}

// computePhase is the function the controller's waitReady depends on;
// covering its mixed-state logic here means a regression breaks the
// test that mirrors the live behaviour, not a synthetic helper.
func TestComputePhaseMixing(t *testing.T) {
	cases := []struct {
		name string
		vol  *miroirv1alpha1.MiroirVolume
		want miroirv1alpha1.VolumePhase
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
			want: miroirv1alpha1.VolumeReady,
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
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
					},
				},
			},
			want: miroirv1alpha1.VolumeDegraded,
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
			want: miroirv1alpha1.VolumeCreating,
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
			want: miroirv1alpha1.VolumeFailed,
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
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Outdated", Message: "peer not yet up"},
					},
				},
			},
			want: miroirv1alpha1.VolumeDegraded,
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
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
						// The tie-breaker's slot must count toward
						// neither ready nor failed.
						"tb": {DiskState: diskStateDiskless, Message: "whatever"},
					},
				},
			},
			want: miroirv1alpha1.VolumeReady,
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
		WithObjects(vol(volPvc1, nodeKharkiv)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}
	if err := c.Status().Patch(t.Context(), &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
	}, client.RawPatch(types.MergePatchType, []byte(`{
		"status": {"perNode": {"`+nodeKharkiv+`": {
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
	st := got.Status.PerNode[nodeKharkiv]
	if !st.DeviceCreated || st.SizeBytes != 1<<30 || !st.Connected {
		t.Fatalf("reportError wiped observed state: %+v", st)
	}
	if st.Message == "" {
		t.Fatal("reportError must set Message")
	}
}

// removedReplicaVol builds a 2-replica volume on paris+oslo whose kharkiv
// leg was just removed from spec.replicas (finalizer still held).
func removedReplicaVol() *miroirv1alpha1.MiroirVolume {
	v := vol(volPvc1, "paris", nodeOslo)
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeKharkiv)
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
			"paris": {"deviceCreated": true, "diskState": "`+diskState+`", "connected": true},
			"`+nodeOslo+`": {"deviceCreated": true, "diskState": "`+diskStateUpToDate+`", "connected": true},
			"`+nodeKharkiv+`": {"deviceCreated": true, "diskState": "`+diskStateUpToDate+`", "connected": true}
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
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run}}

	reconcile(t, r, volPvc1)

	if _, ok := fb.created[volPvc1]; ok {
		t.Fatal("backing device not deleted")
	}
	fe.calledWith(t, "drbdsetup down pvc-1")
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	for _, f := range got.Finalizers {
		if f == constants.FinalizerPrefix+nodeKharkiv {
			t.Fatal("finalizer not released after removal teardown")
		}
	}
	if _, ok := got.Status.PerNode[nodeKharkiv]; ok {
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
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	if !strings.Contains(got.Status.PerNode[nodeKharkiv].Message, "snapshot") {
		t.Fatalf("blocked reason not surfaced: %+v", got.Status.PerNode[nodeKharkiv])
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
	patchPeersUpToDate(t, c, diskStateInconsistent) // paris still syncing
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	v := vol(volPvc1, nodeKharkiv, "paris")
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrParis
	// kharkiv's entry was just added by an operator: no address yet.
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb}

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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
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
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		BackendType: miroirv1alpha1.BackendLVMThin,
		DRBD:        &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		responses: map[string]string{"lsblk": "65536\n"}}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		BackendType: miroirv1alpha1.BackendLoopfile,
		DRBD:        &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

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
	v := vol(volPvc1, nodeKharkiv, nodeParis)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrKharkiv
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrParis
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeKharkiv, Backend: fb,
		BackendType: miroirv1alpha1.BackendLVMThin,
		DRBD:        &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}
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
