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

package membership

import (
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/nodemap"
)

// clientVol is a Ready 2-replica volume with an aged, completed client leg
// on node-c.
func clientVol(age time.Duration) *miroirv1alpha1.MiroirVolume {
	v := replicatedVol()
	v.Spec.Replicas = v.Spec.Replicas[:2]
	added := metav1.NewTime(time.Now().Add(-age))
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{
		{Node: nodeC, NodeID: 2, Address: addrC, AddedAt: &added},
	}
	v.Status.Phase = miroirv1alpha1.VolumeReady
	return v
}

// freshStats is an node-c MiroirNode with room for the volume.
func freshStats(free int64) *miroirv1alpha1.MiroirNode {
	now := metav1.Now()
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeC},
		Status: miroirv1alpha1.MiroirNodeStatus{
			CapacityBytes: 100 << 30, AllocatedBytes: 100<<30 - free, ObservedAt: &now,
		},
	}
}

//nolint:unparam // future tests will vary the name
func reconcileAD(t *testing.T, r *AutoDiskfulReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// An aged client leg on a storage node with capacity converts: the client
// entry is replaced by an incomplete diskful replica entry the membership
// reconciler then completes as a FullSync joiner.
func TestAutoDiskfulConvertsAgedClient(t *testing.T) {
	v := clientVol(15 * time.Minute)
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(v, freshStats(10<<30)).Build()
	r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
		nodeC: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	reconcileAD(t, r, "pvc-1")

	got := get(t, &Reconciler{Client: c}, "pvc-1")
	if len(got.Spec.Clients) != 0 {
		t.Fatalf("client leg must be removed: %+v", got.Spec.Clients)
	}
	idx := slices.IndexFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeC
	})
	if idx < 0 {
		t.Fatalf("node-c must become a replica: %+v", got.Spec.Replicas)
	}
	rep := got.Spec.Replicas[idx]
	if rep.Diskless || rep.Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("converted entry must be a diskful replica: %+v", rep)
	}
	// The leg's live DRBD identity must not change: a node id is immutable
	// on an up resource and the consumer holds the device open.
	if rep.NodeID != 2 || rep.Address != addrC {
		t.Fatalf("conversion must keep the client leg's node-id/address: %+v", rep)
	}
	if !rep.FullSync {
		t.Fatal("converted leg must join as a FullSync joiner")
	}
	if v := testutil.ToFloat64(metricConversions.WithLabelValues("client")); v < 1 {
		t.Fatalf("conversion counter must increment, got %v", v)
	}
}

// Conversion drops a diskless tie-breaker: three diskful replicas carry
// three votes, and MaxItems=3 leaves no room for both.
func TestAutoDiskfulReplacesTieBreaker(t *testing.T) {
	v := clientVol(15 * time.Minute)
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeBergen, NodeID: 3, Address: addrBergen, Diskless: true})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(v, freshStats(10<<30)).Build()
	r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
		nodeC: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	reconcileAD(t, r, "pvc-1")

	got := get(t, &Reconciler{Client: c}, "pvc-1")
	if len(got.Spec.Replicas) != 3 || len(got.Spec.DiskfulReplicas()) != 3 {
		t.Fatalf("want 3 diskful replicas, got %+v", got.Spec.Replicas)
	}
}

// Not-yet-aged legs wait (requeue) without converting.
func TestAutoDiskfulWaitsForThreshold(t *testing.T) {
	v := clientVol(2 * time.Minute)
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(v, freshStats(10<<30)).Build()
	r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
		nodeC: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	res := reconcileAD(t, r, "pvc-1")
	if res.RequeueAfter <= 0 || res.RequeueAfter > 8*time.Minute+time.Second {
		t.Fatalf("must requeue for the remaining age, got %v", res.RequeueAfter)
	}
	if got := get(t, &Reconciler{Client: c}, "pvc-1"); len(got.Spec.Clients) != 1 {
		t.Fatalf("young client leg must not convert: %+v", got.Spec)
	}
}

// Blocked conversions defer: non-storage node, degraded volume, full
// redundancy, and missing/insufficient pool stats all leave the spec alone.
func TestAutoDiskfulBlocks(t *testing.T) {
	cases := map[string]struct {
		mutate func(*miroirv1alpha1.MiroirVolume)
		nodes  nodemap.Map
		stats  *miroirv1alpha1.MiroirNode
	}{
		"non-storage node": {
			mutate: func(*miroirv1alpha1.MiroirVolume) {},
			nodes:  nodemap.Map{},
			stats:  freshStats(10 << 30),
		},
		"degraded volume": {
			mutate: func(v *miroirv1alpha1.MiroirVolume) { v.Status.Phase = miroirv1alpha1.VolumeDegraded },
			nodes:  nodemap.Map{nodeC: {Backend: miroirv1alpha1.BackendLVMThin}},
			stats:  freshStats(10 << 30),
		},
		"no pool stats": {
			mutate: func(*miroirv1alpha1.MiroirVolume) {},
			nodes:  nodemap.Map{nodeC: {Backend: miroirv1alpha1.BackendLVMThin}},
		},
		"insufficient space": {
			mutate: func(*miroirv1alpha1.MiroirVolume) {},
			nodes:  nodemap.Map{nodeC: {Backend: miroirv1alpha1.BackendLVMThin}},
			stats:  freshStats(1 << 20),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			v := clientVol(15 * time.Minute)
			tc.mutate(v)
			b := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
				WithObjects(v)
			if tc.stats != nil {
				b = b.WithObjects(tc.stats)
			}
			r := &AutoDiskfulReconciler{Client: b.Build(), After: 10 * time.Minute, Nodes: tc.nodes}

			reconcileAD(t, r, "pvc-1")

			got := get(t, &Reconciler{Client: r.Client}, "pvc-1")
			if len(got.Spec.Clients) != 1 || len(got.Spec.Replicas) != 2 {
				t.Fatalf("blocked conversion must leave the spec alone: %+v", got.Spec)
			}
		})
	}
}

// tieBreakerVol is a Ready 2+1 volume whose tie-breaker (node-c) has been
// Primary — a consumer staged through it — for the given duration.
func tieBreakerVol(primaryFor time.Duration) *miroirv1alpha1.MiroirVolume {
	v := replicatedVol()
	v.Spec.Replicas = v.Spec.Replicas[:2]
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, Diskless: true,
	})
	since := metav1.NewTime(time.Now().Add(-primaryFor))
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeC: {Diskless: true, PrimarySince: &since},
	}
	v.Status.Phase = miroirv1alpha1.VolumeReady
	return v
}

// A tie-breaker leg a consumer has staged through past the threshold flips
// diskful in place: node-id and address kept (the agent attaches a disk to
// the live leg), FullSync set, backend from the node map.
func TestAutoDiskfulConvertsTieBreaker(t *testing.T) {
	v := tieBreakerVol(15 * time.Minute)
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(v, freshStats(10<<30)).Build()
	r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
		nodeC: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	reconcileAD(t, r, "pvc-1")

	got := get(t, &Reconciler{Client: c}, "pvc-1")
	rep := got.Spec.Replicas[2]
	if rep.Diskless {
		t.Fatalf("tie-breaker must flip diskful: %+v", rep)
	}
	if rep.NodeID != 2 || rep.Address != addrC {
		t.Fatalf("node-id/address must be kept for the in-place attach: %+v", rep)
	}
	if !rep.FullSync || rep.Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("converted leg must be a FullSync joiner with the map's backend: %+v", rep)
	}
}

// A tie-breaker that is not Primary (no consumer staged through it), or
// not Primary long enough, stays diskless.
func TestAutoDiskfulTieBreakerWaits(t *testing.T) {
	young := tieBreakerVol(2 * time.Minute)
	idle := tieBreakerVol(15 * time.Minute)
	idle.Status.PerNode[nodeC] = miroirv1alpha1.ReplicaStatus{Diskless: true} // no PrimarySince
	for name, v := range map[string]*miroirv1alpha1.MiroirVolume{"young": young, "idle": idle} {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
				WithObjects(v, freshStats(10<<30)).Build()
			r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
				nodeC: {Backend: miroirv1alpha1.BackendLVMThin},
			}}
			reconcileAD(t, r, "pvc-1")
			if got := get(t, &Reconciler{Client: c}, "pvc-1"); !got.Spec.Replicas[2].Diskless {
				t.Fatalf("tie-breaker must stay diskless: %+v", got.Spec.Replicas[2])
			}
		})
	}
}

// The watch predicate fires on PrimarySince transitions — the tie-breaker
// signal lives in status, invisible to the generation filter.
func TestPrimarySinceChanged(t *testing.T) {
	now := metav1.Now()
	with := tieBreakerVol(time.Minute)
	without := tieBreakerVol(time.Minute)
	without.Status.PerNode[nodeC] = miroirv1alpha1.ReplicaStatus{Diskless: true}
	if !primarySinceChanged(without, with) || !primarySinceChanged(with, without) {
		t.Fatal("a PrimarySince edge must fire the predicate")
	}
	same := with.DeepCopy()
	same.Status.PerNode[nodeC] = miroirv1alpha1.ReplicaStatus{Diskless: true, PrimarySince: &now}
	if primarySinceChanged(with, same) {
		t.Fatal("presence-stable PrimarySince must not fire the predicate")
	}
}

// A permanently-blocked conversion (3 diskful replicas) must not poll:
// only a spec edit changes it, and spec edits re-trigger the watch.
func TestAutoDiskfulPermanentBlockSkipsRequeue(t *testing.T) {
	v := clientVol(15 * time.Minute)
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeBergen, NodeID: 3, Address: addrBergen,
	})
	v.Spec.Clients[0].NodeID = 4
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(v, freshStats(10<<30)).Build()
	r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
		nodeC: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	res := reconcileAD(t, r, "pvc-1")
	if res.RequeueAfter != 0 {
		t.Fatalf("permanent block must not requeue, got %v", res.RequeueAfter)
	}
	if got := get(t, &Reconciler{Client: c}, "pvc-1"); len(got.Spec.Clients) != 1 {
		t.Fatalf("blocked conversion must leave the spec alone: %+v", got.Spec)
	}
}
