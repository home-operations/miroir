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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/nodemap"
)

// clientVol is a Ready 2-replica volume with an aged, completed client leg
// on oslo.
func clientVol(age time.Duration) *miroirv1alpha1.MiroirVolume {
	v := replicatedVol()
	v.Spec.Replicas = v.Spec.Replicas[:2]
	added := metav1.NewTime(time.Now().Add(-age))
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{
		{Node: nodeOslo, NodeID: 2, Address: addrOslo, AddedAt: &added},
	}
	v.Status.Phase = miroirv1alpha1.VolumeReady
	return v
}

// freshStats is an oslo MiroirNode with room for the volume.
func freshStats(free int64) *miroirv1alpha1.MiroirNode {
	now := metav1.Now()
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeOslo},
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
		nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	reconcileAD(t, r, "pvc-1")

	got := get(t, &Reconciler{Client: c}, "pvc-1")
	if len(got.Spec.Clients) != 0 {
		t.Fatalf("client leg must be removed: %+v", got.Spec.Clients)
	}
	idx := slices.IndexFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeOslo
	})
	if idx < 0 {
		t.Fatalf("oslo must become a replica: %+v", got.Spec.Replicas)
	}
	rep := got.Spec.Replicas[idx]
	if rep.Diskless || rep.Backend != miroirv1alpha1.BackendLVMThin || rep.Address != "" {
		t.Fatalf("converted entry must be a bare diskful replica for membership to complete: %+v", rep)
	}
}

// Conversion drops a diskless tie-breaker: three diskful replicas carry
// three votes, and MaxItems=3 leaves no room for both.
func TestAutoDiskfulReplacesTieBreaker(t *testing.T) {
	v := clientVol(15 * time.Minute)
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeBergen, NodeID: 3, Address: "192.168.1.44", Diskless: true})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(v, freshStats(10<<30)).Build()
	r := &AutoDiskfulReconciler{Client: c, After: 10 * time.Minute, Nodes: nodemap.Map{
		nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin},
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
		nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin},
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
			nodes:  nodemap.Map{nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin}},
			stats:  freshStats(10 << 30),
		},
		"no pool stats": {
			mutate: func(*miroirv1alpha1.MiroirVolume) {},
			nodes:  nodemap.Map{nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin}},
		},
		"insufficient space": {
			mutate: func(*miroirv1alpha1.MiroirVolume) {},
			nodes:  nodemap.Map{nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin}},
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
