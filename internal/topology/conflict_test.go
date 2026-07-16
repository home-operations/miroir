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

package topology

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	nodeA = "node-a"
	nodeB = "node-b"
	nodeC = "node-c"
)

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&miroirv1alpha1.MiroirNode{}).
		WithObjects(objs...).Build()
}

func addrNode(name, addr string) *miroirv1alpha1.MiroirNode {
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirNodeSpec{
			Address: addr,
			Pools: []miroirv1alpha1.MiroirNodePool{{
				Name:    "default",
				LVMThin: &miroirv1alpha1.LVMThinPool{},
			}},
		},
	}
}

func reconcile(t *testing.T, c client.Client, name string) *miroirv1alpha1.MiroirNode {
	t.Helper()
	r := &ConflictReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	}); err != nil {
		t.Fatal(err)
	}
	n := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: name}, n); err != nil {
		t.Fatal(err)
	}
	return n
}

// One pass covers the whole topology: a single reconcile (any request
// name) must stamp the condition on every node, conflicted or not.
func TestConflictSinglePassCoversAllNodes(t *testing.T) {
	c := newClient(t, addrNode(nodeA, "10.0.100.1"), addrNode(nodeB, "10.0.100.1"),
		addrNode(nodeC, "10.0.100.3"))
	r := &ConflictReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: topologyRequestKey},
	}); err != nil {
		t.Fatal(err)
	}
	for name, conflicted := range map[string]bool{nodeA: true, nodeB: true, nodeC: false} {
		n := &miroirv1alpha1.MiroirNode{}
		if err := c.Get(t.Context(), types.NamespacedName{Name: name}, n); err != nil {
			t.Fatal(err)
		}
		cond := meta.FindStatusCondition(n.Status.Conditions, ConditionAddressConflict)
		if cond == nil {
			t.Fatalf("one pass must stamp the condition on %s", name)
		}
		if got := cond.Status == metav1.ConditionTrue; got != conflicted {
			t.Fatalf("%s: conflicted=%v, want %v (%+v)", name, got, conflicted, cond)
		}
	}
}

func TestConflictConditionRaisedAndNamesPeer(t *testing.T) {
	c := newClient(t, addrNode(nodeA, "10.0.100.1"), addrNode(nodeB, "10.0.100.1"),
		addrNode(nodeC, "10.0.100.3"))
	n := reconcile(t, c, nodeA)
	cond := meta.FindStatusCondition(n.Status.Conditions, ConditionAddressConflict)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected AddressConflict=True, got %+v", cond)
	}
	if !strings.Contains(cond.Message, nodeB) {
		t.Fatalf("the message must name the peer, got %q", cond.Message)
	}
	unique := reconcile(t, c, nodeC)
	if cond := meta.FindStatusCondition(unique.Status.Conditions, ConditionAddressConflict); cond == nil ||
		cond.Status != metav1.ConditionFalse {
		t.Fatalf("a unique address must read False, got %+v", cond)
	}
}

// The fold conflicts on the parsed address; the peer list must group the
// same way, or equal-but-differently-spelled IPv6 addresses raise a
// condition naming no peer.
func TestConflictConditionNamesPeerAcrossIPv6Spellings(t *testing.T) {
	c := newClient(t, addrNode(nodeA, "fd00:1::2"), addrNode(nodeB, "fd00:0001:0:0::2"))
	n := reconcile(t, c, nodeA)
	cond := meta.FindStatusCondition(n.Status.Conditions, ConditionAddressConflict)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected AddressConflict=True, got %+v", cond)
	}
	if !strings.Contains(cond.Message, nodeB) {
		t.Fatalf("the message must name the differently spelled peer, got %q", cond.Message)
	}
}

func TestConflictConditionClearsWhenResolved(t *testing.T) {
	a, b := addrNode(nodeA, "10.0.100.1"), addrNode(nodeB, "10.0.100.1")
	c := newClient(t, a, b)
	if n := reconcile(t, c, nodeA); !meta.IsStatusConditionTrue(n.Status.Conditions, ConditionAddressConflict) {
		t.Fatal("precondition: conflict must be raised first")
	}
	fresh := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeB}, fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Spec.Address = "10.0.100.2"
	if err := c.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if n := reconcile(t, c, nodeA); !meta.IsStatusConditionFalse(n.Status.Conditions, ConditionAddressConflict) {
		t.Fatalf("resolving the peer's address must clear the condition, got %+v", n.Status.Conditions)
	}
}

// The Warning event fires once per fresh conflict — repeat passes over an
// unchanged (or message-only-changed) topology must stay silent.
func TestConflictEventFiresOncePerFreshConflict(t *testing.T) {
	c := newClient(t, addrNode(nodeA, "10.0.100.1"), addrNode(nodeB, "10.0.100.1"))
	rec := events.NewFakeRecorder(8)
	r := &ConflictReconciler{Client: c, Recorder: rec}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "topology"}}

	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}

	close(rec.Events)
	fired := 0
	for range rec.Events {
		fired++
	}
	if fired != 2 { // one per freshly conflicted node, none on the repeat
		t.Fatalf("want exactly one event per fresh conflict (2), got %d", fired)
	}
}
