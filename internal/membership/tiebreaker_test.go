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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

const (
	nodeKharkiv = "kharkiv"
	nodeParis   = "paris"
	addrOslo    = "192.168.1.43"
	volTB       = "pvc-tb"
)

// freezeVol is a complete 2-replica freeze volume on kharkiv+paris — the
// shape the tie-breaker reconciler retrofits.
func freezeVol() *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volTB,
			Finalizers: []string{
				constants.FinalizerPrefix + nodeKharkiv,
				constants.FinalizerPrefix + nodeParis,
			},
		},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes:    1 << 30,
			QuorumPolicy: miroirv1alpha1.QuorumFreeze,
			DRBD:         &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendZFS, NodeID: 0, Address: "192.168.1.41"},
				{Node: nodeParis, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "192.168.1.42"},
			},
		},
	}
}

func tbReconcile(t *testing.T, r *TieBreakerReconciler) {
	t.Helper()
	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volTB}}); err != nil {
		t.Fatal(err)
	}
}

// The retrofit appends a bare diskless entry, and the membership
// Reconciler completes it exactly like an operator-added replica.
func TestTieBreakerRetrofitsAndMembershipCompletes(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(freezeVol(), node(nodeOslo, addrOslo)).
		Build()
	nodes := nodemap.Map{
		nodeKharkiv: {Backend: miroirv1alpha1.BackendZFS},
		nodeParis:   {Backend: miroirv1alpha1.BackendZFS},
		nodeOslo:    {Backend: miroirv1alpha1.BackendLVMThin},
	}
	tb := &TieBreakerReconciler{Client: c, Nodes: nodes}

	tbReconcile(t, tb)

	mr := &Reconciler{Client: c, Nodes: nodes}
	got := get(t, mr, volTB)
	if len(got.Spec.Replicas) != 3 {
		t.Fatalf("tie-breaker not added: %+v", got.Spec.Replicas)
	}
	if rep := got.Spec.Replicas[2]; rep.Node != nodeOslo || !rep.Diskless || rep.Address != "" {
		t.Fatalf("want a bare diskless oslo entry, got %+v", rep)
	}

	reconcile(t, mr, volTB)
	got = get(t, mr, volTB)
	rep := got.Spec.Replicas[2]
	if rep.NodeID != 2 || rep.Address != addrOslo || !rep.Diskless {
		t.Fatalf("membership must complete the tie-breaker: %+v", rep)
	}
	if rep.FullSync || rep.Backend != "" {
		t.Fatalf("diskless entry must carry no backend or FullSync: %+v", rep)
	}
	if !slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeOslo) {
		t.Fatalf("tie-breaker node needs a teardown finalizer: %v", got.Finalizers)
	}

	// A second pass sees the tie-breaker in place and changes nothing.
	tbReconcile(t, tb)
	if got = get(t, mr, volTB); len(got.Spec.Replicas) != 3 {
		t.Fatalf("retrofit must be idempotent: %+v", got.Spec.Replicas)
	}
}

// A removed replica's node must not be picked as the tie-breaker while
// its teardown finalizer is still held — re-adding it would cancel the
// teardown, leaking the backing device and stale DRBD metadata that a
// later diskful re-add would adopt (partial resync missing writes).
func TestTieBreakerWaitsForInFlightRemoval(t *testing.T) {
	vol := freezeVol()
	// oslo was just removed from spec.replicas; its agent still holds the
	// teardown finalizer while it waits for the remaining legs to be safe.
	vol.Finalizers = append(vol.Finalizers, constants.FinalizerPrefix+nodeOslo)
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(vol, node(nodeOslo, addrOslo)).Build()
	tb := &TieBreakerReconciler{Client: c, Nodes: nodemap.Map{
		nodeKharkiv: {Backend: miroirv1alpha1.BackendZFS},
		nodeParis:   {Backend: miroirv1alpha1.BackendZFS},
		nodeOslo:    {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	res, err := tb.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volTB}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("must requeue: the finalizer release does not bump the generation")
	}
	got := get(t, &Reconciler{Client: c}, volTB)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("no tie-breaker may be added mid-removal: %+v", got.Spec.Replicas)
	}
}

func TestTieBreakerSkips(t *testing.T) {
	spare := nodemap.Map{
		nodeKharkiv: {Backend: miroirv1alpha1.BackendZFS},
		nodeParis:   {Backend: miroirv1alpha1.BackendZFS},
		nodeOslo:    {Backend: miroirv1alpha1.BackendLVMThin},
	}
	cases := map[string]struct {
		mutate func(*miroirv1alpha1.MiroirVolume)
		nodes  nodemap.Map
	}{
		"last-man-standing keeps its policy": {
			mutate: func(v *miroirv1alpha1.MiroirVolume) {
				v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
			},
			nodes: spare,
		},
		"tie-breaker already present": {
			mutate: func(v *miroirv1alpha1.MiroirVolume) {
				v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
					Node: nodeOslo, NodeID: 2, Address: addrOslo, Diskless: true,
				})
			},
			nodes: spare,
		},
		"membership completion in flight": {
			mutate: func(v *miroirv1alpha1.MiroirVolume) {
				v.Spec.Replicas[1].Address = ""
			},
			nodes: spare,
		},
		"no spare node": {
			mutate: func(*miroirv1alpha1.MiroirVolume) {},
			nodes: nodemap.Map{
				nodeKharkiv: {Backend: miroirv1alpha1.BackendZFS},
				nodeParis:   {Backend: miroirv1alpha1.BackendZFS},
			},
		},
		"unreplicated volume": {
			mutate: func(v *miroirv1alpha1.MiroirVolume) { v.Spec.DRBD = nil },
			nodes:  spare,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			vol := freezeVol()
			tc.mutate(vol)
			want := len(vol.Spec.Replicas)
			c := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithObjects(vol, node(nodeOslo, addrOslo)).Build()
			tb := &TieBreakerReconciler{Client: c, Nodes: tc.nodes}

			tbReconcile(t, tb)

			got := get(t, &Reconciler{Client: c}, volTB)
			if len(got.Spec.Replicas) != want {
				t.Fatalf("replicas must stay unchanged: %+v", got.Spec.Replicas)
			}
		})
	}
}
