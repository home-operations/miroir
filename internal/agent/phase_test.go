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

// computePhase is the function the controller's waitReady depends on;
// covering its mixed-state logic here means a regression breaks the
// test that mirrors the live behaviour, not a synthetic helper.
func TestComputePhaseMixing(t *testing.T) {
	const (
		noneOfTwo = "0/2"
		oneOfTwo  = "1/2"
		twoOfTwo  = "2/2"
		zeroOfOne = "0/1"
	)
	cases := []struct {
		name      string
		vol       *miroirv1alpha1.MiroirVolume
		want      miroirv1alpha1.VolumePhase
		wantReady string
	}{
		{
			name: "all replicas ready (unreplicated)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: oneOfOne,
		},
		{
			name: "one ready, one not (replicated, degraded)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "activated volume with every diskful replica Outdated (failed)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					Activated: true,
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateOutdated, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateOutdated, Connected: true},
					},
				},
			},
			want:      miroirv1alpha1.VolumeFailed,
			wantReady: noneOfTwo,
		},
		{
			name: "activated volume mid-expansion keeps its UpToDate replicas (creating)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 2 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					Activated: true,
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
					},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: noneOfTwo,
		},
		{
			name: "never-activated volume with no UpToDate replica (creating)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					Formatted: true,
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent, Connected: true},
					},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: noneOfTwo,
		},
		{
			name: "both diskful replicas disconnected (degraded)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: noneOfTwo,
		},
		{
			name: "one diskful replica disconnected (degraded)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "all diskful replicas reconnected (ready)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "no realized replicas (creating)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: noneOfTwo,
		},
		{
			name: "all replicas Inconsistent (creating)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateInconsistent},
					},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: noneOfTwo,
		},
		{
			name: "hard failure (no device, message set)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: false, Message: "pool exploded"},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30},
					},
				},
			},
			want:      miroirv1alpha1.VolumeFailed,
			wantReady: oneOfTwo,
		},
		{
			name: "transient error after device exists (stays Degraded, not Failed)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: "Outdated", Message: "peer not yet up"},
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "diskless tie-breaker ignored (ready on diskful legs alone)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas: []miroirv1alpha1.Replica{
						{Node: "a"}, {Node: "b"}, {Node: "tb", Diskless: true},
					},
					DRBD: &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
						// The tie-breaker's slot must count toward
						// neither ready nor failed.
						"tb": {DiskState: diskStateDiskless, Message: "whatever"},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		// --- Staleness detection (LastProbedAt) ---
		{
			name: "fresh probe + healthy replicated → Ready",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceCreated, upToDate, connected, sized(1<<30)),
						"b": freshProbe(deviceCreated, upToDate, connected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "stale probe + healthy persisted → Degraded",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": staleProbe(deviceCreated, upToDate, connected, sized(1<<30)),
						"b": staleProbe(deviceCreated, upToDate, connected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: noneOfTwo,
		},
		{
			name: "nil LastProbedAt (legacy) + healthy → Ready (backward compatible)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true, LastProbedAt: nil},
						"b": {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true, LastProbedAt: nil},
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "probe aged under deepCheckInterval (4 min) + healthy → Ready (steady-state false-positive guard)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": nearProbe(4*time.Minute, deviceCreated, upToDate, connected, sized(1<<30)),
						"b": nearProbe(4*time.Minute, deviceCreated, upToDate, connected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: twoOfTwo,
		},
		{
			name: "fresh probe + disconnected → Degraded (preserves existing behavior)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}, {Node: "b"}},
					DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceCreated, upToDate, connected, sized(1<<30)),
						"b": freshProbe(deviceCreated, upToDate, disconnected, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: oneOfTwo,
		},
		{
			name: "fresh probe + DeviceCreated=false → not realized",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceNotCreated, "", false, 0),
					},
				},
			},
			want:      miroirv1alpha1.VolumeCreating,
			wantReady: zeroOfOne,
		},
		{
			name: "DeviceCreated cleared after missing probe + message → Failed (hard provisioning failure)",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": {DeviceCreated: false, SizeBytes: 1 << 30, LastProbedAt: probeNow(), Message: "backing device missing"},
					},
				},
			},
			want:      miroirv1alpha1.VolumeFailed,
			wantReady: zeroOfOne,
		},
		{
			name: "stale probe + unreplicated volume → Degraded",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": staleProbe(deviceCreated, "", true, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeDegraded,
			wantReady: zeroOfOne,
		},
		{
			name: "fresh probe + unreplicated volume → Ready",
			vol: &miroirv1alpha1.MiroirVolume{
				Spec: miroirv1alpha1.MiroirVolumeSpec{
					SizeBytes: 1 << 30,
					Replicas:  []miroirv1alpha1.Replica{{Node: "a"}},
				},
				Status: miroirv1alpha1.MiroirVolumeStatus{
					PerNode: map[string]miroirv1alpha1.ReplicaStatus{
						"a": freshProbe(deviceCreated, "", true, sized(1<<30)),
					},
				},
			},
			want:      miroirv1alpha1.VolumeReady,
			wantReady: oneOfOne,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotReady := computePhase(tc.vol, time.Now())
			if got != tc.want {
				t.Fatalf("phase = %s, want %s", got, tc.want)
			}
			if gotReady != tc.wantReady {
				t.Fatalf("readyReplicas = %q, want %q", gotReady, tc.wantReady)
			}
		})
	}
}

// --- computePhase staleness helpers ------------------------------------------

const (
	deviceCreated    = true
	deviceNotCreated = false
	upToDate         = diskStateUpToDate
	connected        = true
	disconnected     = false
)

func sized(s int64) int64 { return s }

func probeTime(d time.Duration) *metav1.Time {
	t := metav1.NewTime(time.Now().Add(d))
	return &t
}

func freshProbe(dev bool, disk string, conn bool, size int64) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{
		DeviceCreated: dev,
		DiskState:     disk,
		Connected:     conn,
		SizeBytes:     size,
		LastProbedAt:  probeNow(),
	}
}

func staleProbe(dev bool, disk string, conn bool, size int64) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{
		DeviceCreated: dev,
		DiskState:     disk,
		Connected:     conn,
		SizeBytes:     size,
		LastProbedAt:  probeTime(-11 * time.Minute),
	}
}

// nearProbe is a probe aged by the given (negative) duration — used to test
// probes that are stale-ish but still within the StaleProbeThreshold.
func nearProbe(age time.Duration, dev bool, disk string, conn bool, size int64) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{
		DeviceCreated: dev,
		DiskState:     disk,
		Connected:     conn,
		SizeBytes:     size,
		LastProbedAt:  probeTime(-age),
	}
}

// legBlocksReady applies the controller's own test of a ready leg to this
// node's slot, and only where the node holds a diskful replica: a diskless
// leg's slot never reads as realized, and that must not withdraw a Ready.
func TestLegBlocksReady(t *testing.T) {
	now := time.Now()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.Replicas[1].Diskless = true
	serving := miroirv1alpha1.ReplicaStatus{DeviceCreated: true, SizeBytes: 1 << 30}
	stale := serving
	stale.LastProbedAt = ptrTime(now.Add(-StaleProbeThreshold - time.Minute))
	cases := []struct {
		name string
		node string
		st   miroirv1alpha1.ReplicaStatus
		want bool
	}{
		{"diskful leg in service", nodeA, serving, false},
		{"diskful leg without a device", nodeA, miroirv1alpha1.ReplicaStatus{}, true},
		{"diskful leg below the spec size", nodeA, miroirv1alpha1.ReplicaStatus{DeviceCreated: true, SizeBytes: 1 << 20}, true},
		{"diskful leg whose healthy slot went unprobed", nodeA, stale, true},
		{"diskless leg", nodeB, miroirv1alpha1.ReplicaStatus{Diskless: true}, false},
		{"node without a replica", "node-x", miroirv1alpha1.ReplicaStatus{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := legBlocksReady(v, tc.node, tc.st, now); got != tc.want {
				t.Fatalf("legBlocksReady = %v, want %v", got, tc.want)
			}
		})
	}
}
