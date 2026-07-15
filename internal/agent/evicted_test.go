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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// evictedReconciler is a node-a agent with a real DRBD driver over a
// no-op exec: the scrub's Down short-circuits on the missing .res file,
// and the best-effort metadata wipe swallows the fake's empty answers.
func evictedReconciler(t *testing.T, fb *fakeBackend, objs *miroirv1alpha1.MiroirVolume) *VolumeReconciler {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(objs).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	return &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: (&fakeDRBDExec{}).run,
			Mknod: func(string, uint32, int) error { return nil }}}
}

// A returning node holding no finalizer scrubs its leg when — and only
// because — the eviction marker names it: backing gone, status slot and
// marker cleared together.
func TestScrubsEvictedLegOnReturn(t *testing.T) {
	fb := newFakeBackend()
	v := vol(volPvc1, nodeB, nodeC) // node-a evicted: no spec entry, no finalizer
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	now := metav1.NewTime(time.Now())
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DevicePath: "/dev/fake/" + volPvc1},
	}
	v.Status.Evicted = map[string]metav1.Time{nodeA: now}
	r := evictedReconciler(t, fb, v)
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if fb.existing[volPvc1] {
		t.Fatal("scrub must reclaim the abandoned backing device")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Status.PerNode[nodeA]; ok {
		t.Fatalf("scrub must drop this node's status slot: %+v", got.Status.PerNode)
	}
	if _, ok := got.Status.Evicted[nodeA]; ok {
		t.Fatalf("scrub must clear the eviction marker: %+v", got.Status.Evicted)
	}
}

// Without the marker, absence from spec plus a missing finalizer stays
// inert — spec absence alone is never a destruction signal.
func TestNoScrubWithoutMarker(t *testing.T) {
	fb := newFakeBackend()
	v := vol(volPvc1, nodeB, nodeC)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	r := evictedReconciler(t, fb, v)
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if !fb.existing[volPvc1] {
		t.Fatal("no marker, no scrub: the backing must survive")
	}
}

// A leg re-added before the returning node ever scrubbed still wears
// the marker: the FullSync joiner must be scrubbed first, or
// ensureMetadata would adopt the stale pre-eviction generation instead
// of the just-created metadata the full-sync contract assumes.
func TestScrubsBeforeFullSyncRejoin(t *testing.T) {
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[0].FullSync = true
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	now := metav1.NewTime(time.Now())
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DevicePath: "/dev/fake/" + volPvc1},
	}
	v.Status.Evicted = map[string]metav1.Time{nodeA: now}
	r := evictedReconciler(t, fb, v)
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if fb.existing[volPvc1] {
		t.Fatal("stale backing must be scrubbed before the FullSync rejoin")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Status.Evicted[nodeA]; ok {
		t.Fatalf("scrub must clear the eviction marker: %+v", got.Status.Evicted)
	}
}

// An eviction that aborted between the marker stamp and the spec swap
// leaves a live, completed leg wearing a marker: shed the marker, touch
// nothing else — this leg's data is current.
func TestClearsStaleMarkerOnLiveLeg(t *testing.T) {
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	now := metav1.NewTime(time.Now())
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DevicePath: "/dev/fake/" + volPvc1},
	}
	v.Status.Evicted = map[string]metav1.Time{nodeA: now}
	r := evictedReconciler(t, fb, v)
	if _, err := fb.Create(t.Context(), volPvc1, 1<<30); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)

	if !fb.existing[volPvc1] {
		t.Fatal("a live leg's backing must never be scrubbed")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Status.Evicted[nodeA]; ok {
		t.Fatalf("stale marker must be cleared: %+v", got.Status.Evicted)
	}
	if _, ok := got.Status.PerNode[nodeA]; !ok {
		t.Fatalf("the live leg's status slot must survive: %+v", got.Status.PerNode)
	}
}
