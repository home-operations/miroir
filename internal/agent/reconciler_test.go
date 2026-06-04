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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/backend"
	"github.com/eleboucher/homefs/internal/constants"
)

// fakeBackend records calls and simulates a thin pool in memory.
type fakeBackend struct {
	created map[string]int64
	failOn  string
}

func newFakeBackend() *fakeBackend { return &fakeBackend{created: map[string]int64{}} }

func (f *fakeBackend) Setup(context.Context) error { return nil }

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

func (f *fakeBackend) Snapshot(context.Context, string, string) error { return nil }

func (f *fakeBackend) CreateFromSnapshot(_ context.Context, vol, _, _ string) (string, error) {
	return f.DevicePath(vol), nil
}

func (f *fakeBackend) Delete(_ context.Context, vol string) error {
	delete(f.created, vol)
	return nil
}

func (f *fakeBackend) DeleteSnapshot(context.Context, string, string) error { return nil }

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
	return &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Finalizers: []string{constants.VolumeFinalizer},
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
