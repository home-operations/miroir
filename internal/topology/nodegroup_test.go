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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	classLabel   = "storage.miroir.home-operations.com/class"
	classNVMe    = "nvme"
	poolDefault  = "default"
	partLabelDev = "/dev/disk/by-partlabel/r-miroir"
)

func groupScheme(t *testing.T) *runtime.Scheme {
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

func k8sNode(name string, labels map[string]string, annotations map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: name, Labels: labels, Annotations: annotations,
	}}
}

func nvmeGroup() *miroirv1alpha1.MiroirNodeGroup {
	return &miroirv1alpha1.MiroirNodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: classNVMe},
		Spec: miroirv1alpha1.MiroirNodeGroupSpec{
			NodeSelector: metav1.LabelSelector{MatchLabels: map[string]string{classLabel: classNVMe}},
			Template: miroirv1alpha1.MiroirNodeSpec{
				Pools: []miroirv1alpha1.MiroirNodePool{{
					Name:    poolDefault,
					LVMThin: &miroirv1alpha1.LVMThinPool{Device: partLabelDev},
				}},
			},
		},
	}
}

func reconcileGroup(t *testing.T, c client.Client, name string) *miroirv1alpha1.MiroirNodeGroup {
	t.Helper()
	r := &NodeGroupReconciler{Client: c}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	}); err != nil {
		t.Fatal(err)
	}
	g := &miroirv1alpha1.MiroirNodeGroup{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: name}, g); err != nil {
		t.Fatal(err)
	}
	return g
}

func groupClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(groupScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirNodeGroup{}).
		WithObjects(objs...).Build()
}

// One group + labels = the whole fleet: a MiroirNode per matching node,
// provenance-labeled, per-node facts resolved from the Node object.
func TestNodeGroupMaterializesMatchingNodes(t *testing.T) {
	c := groupClient(t, nvmeGroup(),
		k8sNode(nodeA, map[string]string{classLabel: classNVMe, zoneLabel: "rack-1"}, nil),
		k8sNode(nodeB, map[string]string{classLabel: classNVMe},
			map[string]string{miroirv1alpha1.NodeAddressAnnotation: "10.0.100.12"}),
		k8sNode(nodeC, map[string]string{classLabel: "hdd"}, nil))

	g := reconcileGroup(t, c, classNVMe)

	if !slices.Equal(g.Status.Nodes, []string{nodeA, nodeB}) {
		t.Fatalf("members = %v", g.Status.Nodes)
	}
	mn := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatal(err)
	}
	if mn.Labels[miroirv1alpha1.NodeGroupLabel] != classNVMe {
		t.Fatalf("provenance label missing: %v", mn.Labels)
	}
	if mn.Spec.Zone != "rack-1" {
		t.Fatalf("zone must inherit the node's topology label, got %q", mn.Spec.Zone)
	}
	if mn.Spec.Pools[0].LVMThin.Device != partLabelDev {
		t.Fatalf("template pools not applied: %+v", mn.Spec.Pools)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeB}, mn); err != nil {
		t.Fatal(err)
	}
	if mn.Spec.Address != "10.0.100.12" {
		t.Fatalf("address must come from the node annotation, got %q", mn.Spec.Address)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeC}, mn); err == nil {
		t.Fatal("non-matching node must not be materialized")
	}
}

// Managed means managed: hand-edits to a materialized spec revert, and
// template edits converge every member.
func TestNodeGroupRevertsManagedDrift(t *testing.T) {
	c := groupClient(t, nvmeGroup(),
		k8sNode(nodeA, map[string]string{classLabel: classNVMe}, nil))
	reconcileGroup(t, c, classNVMe)

	mn := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatal(err)
	}
	mn.Spec.Pools[0].LVMThin.Device = "/dev/sdz" // hand edit
	if err := c.Update(t.Context(), mn); err != nil {
		t.Fatal(err)
	}

	reconcileGroup(t, c, classNVMe)
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatal(err)
	}
	if mn.Spec.Pools[0].LVMThin.Device != partLabelDev {
		t.Fatalf("managed spec must revert to the template, got %q", mn.Spec.Pools[0].LVMThin.Device)
	}
}

// A direct-authored MiroirNode always wins: the group skips it and
// reports the conflict.
func TestNodeGroupSkipsDirectMiroirNode(t *testing.T) {
	direct := &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeA}, // no provenance label
		Spec: miroirv1alpha1.MiroirNodeSpec{
			Pools: []miroirv1alpha1.MiroirNodePool{{Name: poolDefault, ZFS: &miroirv1alpha1.ZFSPool{Dataset: "tank/miroir"}}},
		},
	}
	c := groupClient(t, nvmeGroup(), direct,
		k8sNode(nodeA, map[string]string{classLabel: classNVMe}, nil))

	g := reconcileGroup(t, c, classNVMe)

	if len(g.Status.Nodes) != 0 {
		t.Fatalf("a direct MiroirNode must not become a member: %v", g.Status.Nodes)
	}
	cond := meta.FindStatusCondition(g.Status.Conditions, miroirv1alpha1.ConditionGroupConflict)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("conflict condition must be raised: %+v", cond)
	}
	mn := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatal(err)
	}
	if mn.Spec.Pools[0].ZFS == nil || mn.Labels[miroirv1alpha1.NodeGroupLabel] != "" {
		t.Fatalf("the direct MiroirNode must be untouched: %+v %v", mn.Spec.Pools, mn.Labels)
	}
}

// A node leaving the selector orphans its MiroirNode in place — topology
// is never deleted out from under live volumes.
func TestNodeGroupOrphansOnUnlabel(t *testing.T) {
	c := groupClient(t, nvmeGroup(),
		k8sNode(nodeA, map[string]string{classLabel: classNVMe}, nil))
	reconcileGroup(t, c, classNVMe)

	node := &corev1.Node{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, node); err != nil {
		t.Fatal(err)
	}
	node.Labels = nil
	if err := c.Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}

	g := reconcileGroup(t, c, classNVMe)
	if len(g.Status.Nodes) != 0 {
		t.Fatalf("membership must drop the unlabeled node: %v", g.Status.Nodes)
	}
	cond := meta.FindStatusCondition(g.Status.Conditions, miroirv1alpha1.ConditionGroupOrphaned)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("orphan condition must be raised: %+v", cond)
	}
	mn := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatalf("orphaned MiroirNode must be left in place: %v", err)
	}
}

// One node, one manager: the second group claiming a node reports
// Conflict and leaves the first group's object alone.
func TestNodeGroupConflictBetweenGroups(t *testing.T) {
	second := nvmeGroup()
	second.Name = "zz-late"
	c := groupClient(t, nvmeGroup(), second,
		k8sNode(nodeA, map[string]string{classLabel: classNVMe}, nil))

	reconcileGroup(t, c, classNVMe)
	g := reconcileGroup(t, c, "zz-late")

	if len(g.Status.Nodes) != 0 {
		t.Fatalf("the losing group must not claim members: %v", g.Status.Nodes)
	}
	cond := meta.FindStatusCondition(g.Status.Conditions, miroirv1alpha1.ConditionGroupConflict)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("conflict condition must be raised: %+v", cond)
	}
	mn := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatal(err)
	}
	if mn.Labels[miroirv1alpha1.NodeGroupLabel] != classNVMe {
		t.Fatalf("first manager must keep the node: %v", mn.Labels)
	}
}
