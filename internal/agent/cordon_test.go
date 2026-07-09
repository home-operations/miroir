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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCordonWatcherTracksUnschedulable(t *testing.T) {
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeKharkiv},
		Spec:       corev1.NodeSpec{Unschedulable: true},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(node).Build()
	w := &CordonWatcher{Client: c, NodeName: nodeKharkiv}

	if w.Cordoned() {
		t.Fatal("must default to not cordoned before any observation")
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeKharkiv}}
	if _, err := w.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if !w.Cordoned() {
		t.Fatal("must observe the node cordoned")
	}

	// Uncordon: the cached state follows the node back to schedulable.
	node.Spec.Unschedulable = false
	if err := c.Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if w.Cordoned() {
		t.Fatal("must follow the node back to schedulable")
	}
}
