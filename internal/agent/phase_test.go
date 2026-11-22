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
	"context"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
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

// An agent publishes only its own slot; the phase reaches the CR through
// this reconciler's pass over the same object.
func TestPhaseReconcilerDerivesPhaseFromAgentSlot(t *testing.T) {
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	reconcile(t, &VolumeReconciler{Client: cl, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "" || got.Status.ReadyReplicas != "" {
		t.Fatalf("agent wrote phase %q, readyReplicas %q; both are the controller's",
			got.Status.Phase, got.Status.ReadyReplicas)
	}

	r := &PhaseReconciler{Client: cl}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, miroirv1alpha1.VolumeReady)
	}
	if got.Status.ReadyReplicas != oneOfOne {
		t.Fatalf("readyReplicas = %q, want %q", got.Status.ReadyReplicas, oneOfOne)
	}
}

// A phase derived from slots that changed before the write must not land
// over them: an agent may have withdrawn a Ready in between, and writing
// it back would restore the stale value.
func TestPhaseReconcilerSkipsWriteOverChangedSlots(t *testing.T) {
	vol := phaseVolume(time.Now())
	vol.Status.Phase, vol.Status.ReadyReplicas = miroirv1alpha1.VolumeDegraded, "0/1"
	key := types.NamespacedName{Name: vol.Name}
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				// The leg leaves service between the reconciler's read
				// and its write.
				cur := &miroirv1alpha1.MiroirVolume{}
				if err := cl.Get(ctx, key, cur); err != nil {
					return err
				}
				cur.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{}
				if err := cl.Status().Update(ctx, cur); err != nil {
					return err
				}
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	r := &PhaseReconciler{Client: cl}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("a conflict is settled by the next event, not an error: %v", err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != miroirv1alpha1.VolumeDegraded {
		t.Fatalf("phase = %q: the Ready derived from the older slots must not land", got.Status.Phase)
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

// A Ready volume contradicts a lingering NodeUnreachable condition: the
// waitReady success-path clear is one-shot, so when its patch fails the
// PhaseReconciler must clear the condition on the next status event.
func TestPhaseReconcilerClearsLingeringNodeUnreachable(t *testing.T) {
	vol := phaseVolume(time.Now().Add(-time.Minute))
	vol.Status.Conditions = []metav1.Condition{{
		Type:               miroirv1alpha1.ConditionNodeUnreachable,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "AgentUnreachable",
	}}
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &PhaseReconciler{Client: cl}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatal(err)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != miroirv1alpha1.VolumeReady {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, miroirv1alpha1.VolumeReady)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, miroirv1alpha1.ConditionNodeUnreachable)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("NodeUnreachable condition must be cleared to False on a Ready volume, got %+v", cond)
	}
}

// degradedReachableVolume: replicated, both agents probing fresh, but one
// leg disconnected — Degraded yet fully reachable. This is where recovery
// settles when waitReady provisions a Degraded volume, so the backstop
// must clear the condition here too, not only on Ready.
func degradedReachableVolume(name string) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			DRBD:      &miroirv1alpha1.DRBDSpec{},
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA}, {Node: nodeB}},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			Phase:         miroirv1alpha1.VolumeDegraded,
			ReadyReplicas: "1/2",
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {
					DeviceCreated: true,
					SizeBytes:     1 << 30,
					DiskState:     drbd.DiskUpToDate,
					Connected:     true,
					LastProbedAt:  ptrTime(time.Now().Add(-time.Minute)),
				},
				nodeB: {
					DeviceCreated: true,
					SizeBytes:     1 << 30,
					DiskState:     drbd.DiskUpToDate,
					Connected:     false,
					LastProbedAt:  ptrTime(time.Now().Add(-time.Minute)),
				},
			},
			Conditions: []metav1.Condition{{
				Type:               miroirv1alpha1.ConditionNodeUnreachable,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				Reason:             "AgentUnreachable",
			}},
		},
	}
}

func TestPhaseReconcilerClearsNodeUnreachableOnDegradedButReachable(t *testing.T) {
	vol := degradedReachableVolume(volPvc1)
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &PhaseReconciler{Client: cl}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatal(err)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != miroirv1alpha1.VolumeDegraded {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, miroirv1alpha1.VolumeDegraded)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, miroirv1alpha1.ConditionNodeUnreachable)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("NodeUnreachable must clear on a Degraded volume whose replicas all probe fresh, got %+v", cond)
	}
}

func TestPhaseReconcilerKeepsNodeUnreachableWhileProbesStale(t *testing.T) {
	vol := degradedReachableVolume(volPvc1)
	vol.Status.PerNode[nodeB] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true,
		SizeBytes:     1 << 30,
		DiskState:     drbd.DiskUpToDate,
		Connected:     true,
		LastProbedAt:  ptrTime(time.Now().Add(-StaleProbeThreshold - time.Minute)),
	}
	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(vol).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &PhaseReconciler{Client: cl}

	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatal(err)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, miroirv1alpha1.ConditionNodeUnreachable)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("NodeUnreachable must stay True while a replica's probe is stale, got %+v", cond)
	}
}
