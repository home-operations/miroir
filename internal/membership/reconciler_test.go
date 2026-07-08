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
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

const nodeOslo = "oslo"

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func node(name, ip string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: ip},
		}},
	}
}

// replicatedVol is a 2-replica volume on kharkiv+paris with an
// operator-added oslo entry awaiting completion.
func replicatedVol() *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-1",
			Finalizers: []string{
				constants.FinalizerPrefix + "kharkiv",
				constants.FinalizerPrefix + "paris",
			},
		},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas: []miroirv1alpha1.Replica{
				{Node: "kharkiv", Backend: miroirv1alpha1.BackendZFS, NodeID: 0, Address: "192.168.1.41"},
				{Node: "paris", Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "192.168.1.42"},
				{Node: nodeOslo, Backend: miroirv1alpha1.BackendZFS},
			},
		},
	}
}

//nolint:unparam // future tests will vary the name
func reconcile(t *testing.T, r *Reconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
		t.Fatal(err)
	}
}

//nolint:unparam // future tests will vary the name
func get(t *testing.T, r *Reconciler, name string) *miroirv1alpha1.MiroirVolume {
	t.Helper()
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name}, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCompletesAddedReplica(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(replicatedVol(), node(nodeOslo, "192.168.1.43")).
		Build()
	r := &Reconciler{Client: c, Nodes: nodemap.Map{
		nodeOslo: {Backend: miroirv1alpha1.BackendLVMThin},
	}}

	reconcile(t, r, "pvc-1")

	got := get(t, r, "pvc-1")
	rep := got.Spec.Replicas[2]
	if rep.NodeID != 2 || rep.Address != "192.168.1.43" {
		t.Fatalf("entry not completed: %+v", rep)
	}
	if !rep.FullSync {
		t.Fatal("late joiner must be marked FullSync — day0 seeding it corrupts data")
	}
	// The node map, not the operator's edit, decides the backend.
	if rep.Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("backend not taken from the node map: %s", rep.Backend)
	}
	if !slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeOslo) {
		t.Fatal("teardown finalizer missing for the added node")
	}
}

func TestReusesLowestFreeNodeID(t *testing.T) {
	v := replicatedVol()
	v.Spec.Replicas[1].NodeID = 2 // id 1 was freed by an earlier removal
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(v, node(nodeOslo, "192.168.1.43")).
		Build()
	r := &Reconciler{Client: c, Nodes: nodemap.Map{
		nodeOslo: {Backend: miroirv1alpha1.BackendZFS},
	}}

	reconcile(t, r, "pvc-1")

	if got := get(t, r, "pvc-1").Spec.Replicas[2]; got.NodeID != 1 {
		t.Fatalf("NodeID = %d, want lowest free id 1", got.NodeID)
	}
}

func TestUnknownNodeLeavesSpecUntouched(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(replicatedVol()).
		Build()
	r := &Reconciler{Client: c, Nodes: nodemap.Map{}} // oslo not a storage node

	reconcile(t, r, "pvc-1")

	if got := get(t, r, "pvc-1").Spec.Replicas[2]; got.Address != "" {
		t.Fatalf("must not complete an entry for a non-storage node: %+v", got)
	}
}

// A replica on a real storage node whose Node object is not ready yet
// (unregistered, or InternalIP not posted) is transient: Reconcile must
// return an error so it requeues. A Node gaining its InternalIP does not
// wake this generation-filtered controller, so swallowing the error would
// wedge the entry at Address="" until the next spec edit.
func TestRequeuesWhenNodeNotReady(t *testing.T) {
	cases := map[string]*corev1.Node{
		"node not registered":     nil,
		"node without InternalIP": {ObjectMeta: metav1.ObjectMeta{Name: nodeOslo}},
	}
	for name, n := range cases {
		t.Run(name, func(t *testing.T) {
			objs := []client.Object{replicatedVol()}
			if n != nil {
				objs = append(objs, n)
			}
			c := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithObjects(objs...).Build()
			r := &Reconciler{Client: c, Nodes: nodemap.Map{
				nodeOslo: {Backend: miroirv1alpha1.BackendZFS},
			}}

			if _, err := r.Reconcile(context.Background(),
				ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1"}}); err == nil {
				t.Fatal("transient completion failure must return an error to requeue")
			}
			if got := get(t, r, "pvc-1").Spec.Replicas[2]; got.Address != "" {
				t.Fatalf("entry must stay incomplete: %+v", got)
			}
		})
	}
}

func TestIgnoresUnreplicatedVolume(t *testing.T) {
	v := replicatedVol()
	v.Spec.DRBD = nil
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(v, node(nodeOslo, "192.168.1.43")).
		Build()
	r := &Reconciler{Client: c, Nodes: nodemap.Map{
		nodeOslo: {Backend: miroirv1alpha1.BackendZFS},
	}}

	reconcile(t, r, "pvc-1")

	if got := get(t, r, "pvc-1").Spec.Replicas[2]; got.Address != "" {
		t.Fatalf("membership changes need a replication layer: %+v", got)
	}
}
