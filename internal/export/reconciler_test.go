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

package export

import (
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	testNS = "miroir-system"
	nodeA  = "node-a"
	nodeB  = "node-b"
	nodeC  = "node-c"

	testClusterIP = "10.96.1.5"
)

// exportVolume is an RWX volume with a data replica on each named node.
func exportVolume(name string, nodes ...string) *miroirv1alpha1.MiroirVolume {
	reps := make([]miroirv1alpha1.Replica, len(nodes))
	for i, n := range nodes {
		reps[i] = miroirv1alpha1.Replica{Node: n, NodeID: int32(i)}
	}
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			Replicas:  reps,
			Export:    &miroirv1alpha1.ExportSpec{FSType: "ext4"},
		},
	}
}

func newReconciler(objs ...client.Object) (*Reconciler, client.Client) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = miroirv1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(objs...).Build()
	return &Reconciler{
		Client:         cl,
		Namespace:      testNS,
		Image:          "ghcr.io/home-operations/miroir-gateway:test",
		ServiceAccount: "miroir-gateway",
	}, cl
}

func reconcile(t *testing.T, r *Reconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func getDeployment(t *testing.T, cl client.Client, vol string) *appsv1.Deployment {
	t.Helper()
	dep := &appsv1.Deployment{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: shareName(vol), Namespace: testNS}, dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	return dep
}

func TestReconcileCreatesWorkloads(t *testing.T) {
	vol := exportVolume("pvc-abc", nodeA, nodeB)
	r, cl := newReconciler(vol)
	reconcile(t, r, "pvc-abc")

	dep := getDeployment(t, cl, "pvc-abc")
	// Owned by the volume so deleting it garbage-collects the gateway.
	if len(dep.OwnerReferences) != 1 || dep.OwnerReferences[0].Name != "pvc-abc" ||
		dep.OwnerReferences[0].Controller == nil || !*dep.OwnerReferences[0].Controller {
		t.Fatalf("deployment must be controller-owned by the volume, got %+v", dep.OwnerReferences)
	}
	c := dep.Spec.Template.Spec.Containers[0]
	if !slices.Contains(c.Args, "--volume=pvc-abc") || !slices.Contains(c.Args, "--mode=gateway") {
		t.Fatalf("container args = %v", c.Args)
	}
	if c.Image != "ghcr.io/home-operations/miroir-gateway:test" {
		t.Fatalf("image = %q", c.Image)
	}
	if dep.Spec.Template.Spec.ServiceAccountName != "miroir-gateway" {
		t.Fatalf("service account = %q", dep.Spec.Template.Spec.ServiceAccountName)
	}
	// Scheduled onto exactly the volume's diskful replica nodes.
	got := affinityNodes(t, dep)
	slices.Sort(got)
	if want := []string{nodeA, nodeB}; !slices.Equal(got, want) {
		t.Fatalf("affinity nodes = %v, want %v", got, want)
	}
	// Fast eviction on node loss (30s, not the 5m default) so a singleton
	// gateway's failover can start promptly.
	for _, tol := range dep.Spec.Template.Spec.Tolerations {
		if tol.Effect == corev1.TaintEffectNoExecute && (tol.TolerationSeconds == nil || *tol.TolerationSeconds != 30) {
			t.Fatalf("NoExecute toleration %q must be 30s, got %v", tol.Key, tol.TolerationSeconds)
		}
	}

	svc := &corev1.Service{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: shareName("pvc-abc"), Namespace: testNS}, svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP || svc.Spec.Ports[0].Port != nfsPort {
		t.Fatalf("service = %+v", svc.Spec)
	}
}

func TestReconcileAdoptsServiceAndPublishesAddress(t *testing.T) {
	vol := exportVolume("pvc-xyz", nodeA, nodeB)
	// A Service already exists with an apiserver-assigned ClusterIP (as
	// after a controller restart): the reconciler must adopt it, keep the
	// address, and publish it.
	existing := buildService(vol, testNS)
	existing.Spec.ClusterIP = testClusterIP
	r, cl := newReconciler(vol, existing)

	reconcile(t, r, "pvc-xyz")

	svc := &corev1.Service{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: shareName("pvc-xyz"), Namespace: testNS}, svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.ClusterIP != testClusterIP {
		t.Fatalf("adopted Service must keep its ClusterIP, got %q", svc.Spec.ClusterIP)
	}
	if len(svc.OwnerReferences) != 1 {
		t.Fatalf("adopted Service must gain the volume owner ref, got %+v", svc.OwnerReferences)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: "pvc-xyz"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Export == nil || got.Status.Export.Address != testClusterIP {
		t.Fatalf("status.export.address = %+v, want 10.96.1.5", got.Status.Export)
	}
}

func TestReconcileSkipsNonExportVolume(t *testing.T) {
	vol := exportVolume("pvc-plain", nodeA)
	vol.Spec.Export = nil // a plain RWO volume
	r, cl := newReconciler(vol)

	reconcile(t, r, "pvc-plain")

	dep := &appsv1.Deployment{}
	err := cl.Get(t.Context(), types.NamespacedName{Name: shareName("pvc-plain"), Namespace: testNS}, dep)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("a non-export volume must get no gateway, got err=%v", err)
	}
}

func TestReconcileUpdatesAffinityOnReplicaChange(t *testing.T) {
	vol := exportVolume("pvc-move", nodeA, nodeB)
	r, cl := newReconciler(vol)
	reconcile(t, r, "pvc-move")

	// The replica set is edited (a replica moves to node-c); the gateway's
	// affinity must follow so it can still schedule on a data node.
	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: "pvc-move"}, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.Replicas = []miroirv1alpha1.Replica{
		{Node: nodeA, NodeID: 0},
		{Node: nodeC, NodeID: 1},
	}
	if err := cl.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, "pvc-move")

	nodes := affinityNodes(t, getDeployment(t, cl, "pvc-move"))
	slices.Sort(nodes)
	if want := []string{nodeA, nodeC}; !slices.Equal(nodes, want) {
		t.Fatalf("affinity nodes after move = %v, want %v", nodes, want)
	}
}

// exportReadyGauge reads miroir_export_ready for a volume from the global
// registry; ok=false means the series does not exist.
func exportReadyGauge(t *testing.T, volume string) (float64, bool) {
	t.Helper()
	mfs, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "miroir_export_ready" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "volume" && l.GetValue() == volume {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func TestReconcileExportReadyMetric(t *testing.T) {
	vol := exportVolume("pvc-metric", nodeA, nodeB)
	// The Service pre-exists with an apiserver-assigned ClusterIP (the fake
	// client assigns none), so readiness hinges on gateway availability.
	svc := buildService(vol, testNS)
	svc.Spec.ClusterIP = testClusterIP
	r, cl := newReconciler(vol, svc)

	// First reconcile creates the Deployment; no pod is available yet.
	reconcile(t, r, "pvc-metric")
	if v, ok := exportReadyGauge(t, "pvc-metric"); !ok || v != 0 {
		t.Fatalf("gauge = %v (exists=%v), want 0 while no gateway pod is available", v, ok)
	}

	// The gateway pod comes up.
	dep := getDeployment(t, cl, "pvc-metric")
	dep.Status.AvailableReplicas = 1
	if err := cl.Status().Update(t.Context(), dep); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, "pvc-metric")
	if v, ok := exportReadyGauge(t, "pvc-metric"); !ok || v != 1 {
		t.Fatalf("gauge = %v (exists=%v), want 1 with an available gateway and a published address", v, ok)
	}

	// Volume gone: the series must be dropped, not left at a stale value.
	if err := cl.Delete(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, "pvc-metric")
	if _, ok := exportReadyGauge(t, "pvc-metric"); ok {
		t.Fatal("gauge series must be dropped when the volume is deleted")
	}
}

// affinityNodes extracts the node names the Deployment's required node
// affinity pins to.
func affinityNodes(t *testing.T, dep *appsv1.Deployment) []string {
	t.Helper()
	aff := dep.Spec.Template.Spec.Affinity
	if aff == nil || aff.NodeAffinity == nil ||
		aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatal("deployment has no required node affinity")
	}
	terms := aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("unexpected affinity terms: %+v", terms)
	}
	return terms[0].MatchExpressions[0].Values
}
