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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const oneOfOne = "1/1"

func phaseVolume(probedAt time.Time) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA}},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			Phase:         miroirv1alpha1.VolumeReady,
			ReadyReplicas: oneOfOne,
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {
					DeviceCreated: true,
					SizeBytes:     1 << 30,
					Connected:     true,
					LastProbedAt:  ptrTime(probedAt),
				},
			},
		},
	}
}

func ptrTime(t time.Time) *metav1.Time {
	probed := metav1.NewTime(t)
	return &probed
}

func TestPhaseReconcilerDegradesVolumeWhenAllAgentsAreStale(t *testing.T) {
	vol := phaseVolume(time.Now().Add(-StaleProbeThreshold - time.Minute))
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &PhaseReconciler{Client: cl}

	res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: vol.Name}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("stale probes need no further age requeue, got %v", res.RequeueAfter)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != miroirv1alpha1.VolumeDegraded {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, miroirv1alpha1.VolumeDegraded)
	}
	if got.Status.ReadyReplicas != "0/1" {
		t.Fatalf("readyReplicas = %q, want %q", got.Status.ReadyReplicas, "0/1")
	}
}

func TestPhaseReconcilerSchedulesFreshProbeExpiry(t *testing.T) {
	vol := phaseVolume(time.Now().Add(-time.Minute))
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &PhaseReconciler{Client: cl}

	res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: vol.Name}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter < StaleProbeThreshold-2*time.Minute ||
		res.RequeueAfter > StaleProbeThreshold {
		t.Fatalf("requeue = %v, want the remaining probe lifetime", res.RequeueAfter)
	}
}
