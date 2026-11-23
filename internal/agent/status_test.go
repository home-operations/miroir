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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// Every status apply must name only this node's slot and the phase — never a
// peer's slot or Formatted (a CSI-owned field). A full-status apply would
// force-own those and revert them to this agent's stale read, hot-looping the
// agents against each other; reverting Formatted risks re-mkfs of live data.
// Asserting on the wire payload proves the scope regardless of how faithfully
// the fake client models server-side apply ownership.
func TestReconcile_StatusApplyScopedToOwnSlot(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	// A peer slot and a CSI-owned Formatted flag this agent must not touch.
	v.Status.Formatted = true
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {DeviceCreated: true, SizeBytes: 1 << 30, DiskState: diskStateUpToDate, Connected: true},
	}

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}

	var applies []*acv1alpha1.MiroirVolumeStatusApplyConfiguration
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceApply: func(ctx context.Context, cl client.Client, sub string,
				obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				if ac, ok := obj.(*acv1alpha1.MiroirVolumeApplyConfiguration); ok && ac.Status != nil {
					applies = append(applies, ac.Status)
				}
				return cl.SubResource(sub).Apply(ctx, obj, opts...)
			},
		}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	if len(applies) == 0 {
		t.Fatal("expected at least one server-side apply of status")
	}
	for i, st := range applies {
		if _, ok := st.PerNode[nodeA]; !ok {
			t.Errorf("apply %d omits this node's slot: %+v", i, st.PerNode)
		}
		if _, ok := st.PerNode[nodeB]; ok {
			t.Errorf("apply %d names the peer's slot (would force-own it): %+v", i, st.PerNode)
		}
		if st.Formatted != nil {
			t.Errorf("apply %d sets Formatted (would force-own a CSI field)", i)
		}
	}
}

// A diskful leg DRBD detached after an I/O error reads Diskless while the
// volume serves via the peer. The reconcile must explain it (actionable
// Message) and keep the leg DeviceCreated=true so the volume stays
// Degraded, not hard-Failed.
func TestReconcileDetachedDiskGetsActionableMessage(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	// The leg was UpToDate before the disk errored.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	// DRBD reports the local leg detached (Diskless).
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Diskless"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	if !strings.Contains(st.Message, "detached") {
		t.Fatalf("detached leg must carry an actionable message: %q", st.Message)
	}
	if !st.DeviceCreated {
		t.Fatalf("detached leg must stay DeviceCreated (Degraded, not Failed): %+v", st)
	}
	if !st.DiskFailed {
		t.Fatalf("first detach must latch DiskFailed so the next reconcile skips re-attach: %+v", st)
	}
	// The latch is read from the prior reconcile's status, so this first
	// detection still ran a bare adjust (one re-attach), then latched.
	fe.calledWith(t, "drbdadm adjust "+volPvc1)
	fe.notCalledWith(t, "--skip-disk")
}

// A Primary leg latches Activated from the kernel role: a raw-block
// consumer staged before the field existed never restages, and without
// the latch planSplitBrain would treat its data-bearing volume as
// never-written and auto-discard a leg.
func TestReconcilePrimaryLatchesActivated(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
		nodeB: {DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30},
	}
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Activated {
		t.Fatal("Primary leg must latch Status.Activated")
	}
}

// A Secondary leg must not latch: an unstaged volume stays eligible for
// split-brain auto-recovery.
func TestReconcileSecondaryDoesNotLatchActivated(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Activated {
		t.Fatal("Secondary leg must not latch Status.Activated")
	}
}

// Status.Connected scopes to diskful peers: a downed tie-breaker link
// must not read as degraded replication, while a downed data leg must.
func TestDiskfulPeersConnected(t *testing.T) {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{
		Node: nodeC, NodeID: 2, Address: addrC, Diskless: true,
	})

	tieBreakerDown := drbd.Status{PeerConnected: map[int32]bool{1: true, 2: false}}
	if !diskfulPeersConnected(tieBreakerDown, v, nodeA) {
		t.Fatal("a downed tie-breaker link must not count as disconnected")
	}
	dataLegDown := drbd.Status{PeerConnected: map[int32]bool{1: false, 2: true}}
	if diskfulPeersConnected(dataLegDown, v, nodeA) {
		t.Fatal("a downed data leg must count as disconnected")
	}
}

// reportError must not demote Ready volumes on transient errors.
func TestReportErrorPreservesObservedState(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(vol(volPvc1, nodeA)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}
	if err := c.Status().Patch(t.Context(), &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
	}, client.RawPatch(types.MergePatchType, []byte(`{
		"status": {"perNode": {"`+nodeA+`": {
			"deviceCreated": true, "sizeBytes": 1073741824, "connected": true
		}}}
	}`))); err != nil {
		t.Fatal(err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if err := r.reportError(t.Context(), got, errors.New("transient K8s blip")); err == nil {
		t.Fatal("expected reportError to requeue with the original cause")
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	st := got.Status.PerNode[nodeA]
	if !st.DeviceCreated || st.SizeBytes != 1<<30 || !st.Connected {
		t.Fatalf("reportError wiped observed state: %+v", st)
	}
	if st.Message == "" {
		t.Fatal("reportError must set Message")
	}
}

// Phase and ReadyReplicas belong to the controller's PhaseReconciler: a
// full pass publishes this leg's slot and leaves both alone, even where
// they contradict the slots, and a contradiction never breaks the
// fingerprint.
func TestReconcileLeavesPhaseToController(t *testing.T) {
	_, fe, r := steadyVolume(t)
	reconcile(t, r, volPvc1) // full pass, stores the fingerprint

	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "" || got.Status.ReadyReplicas != "" {
		t.Fatalf("agent wrote phase %q, readyReplicas %q; both are the controller's",
			got.Status.Phase, got.Status.ReadyReplicas)
	}
	// nodeB has no slot, so the slots derive Degraded 1/2.
	got.Status.Phase = miroirv1alpha1.VolumeReady
	got.Status.ReadyReplicas = "2/2"
	if err := r.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	adjusts := countCalls(fe, "adjust")

	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n != adjusts {
		t.Fatalf("a phase mismatch must not leave the fast path: %v", fe.calls)
	}

	r.dropRealized(volPvc1)
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("want a full pass after dropping the fingerprint: %v", fe.calls)
	}
	after := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, after); err != nil {
		t.Fatal(err)
	}
	if after.Status.Phase != miroirv1alpha1.VolumeReady || after.Status.ReadyReplicas != "2/2" {
		t.Fatalf("full pass rewrote phase %q, readyReplicas %q",
			after.Status.Phase, after.Status.ReadyReplicas)
	}
}

// A Ready published while this node's diskful leg is out of service is
// withdrawn in the apply that reports the slot: with the controller down
// nothing else would, and a drain gated on Ready would pass on the stale
// value. The rule is on the state, so a Ready that turns up later (a
// controller write derived from older slots) breaks the fingerprint and is
// withdrawn too. This pins what the agent sends, since the fake client
// drops an applied empty string; test/integration covers the apiserver's
// side.
func TestReconcileWithdrawsReadyWhileLegOutOfService(t *testing.T) {
	_, fe, r := steadyVolume(t)
	var last *acv1alpha1.MiroirVolumeStatusApplyConfiguration
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, cl client.Client, sub string,
			obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			if ac, ok := obj.(*acv1alpha1.MiroirVolumeApplyConfiguration); ok && ac.Status != nil {
				last = ac.Status
			}
			return cl.SubResource(sub).Apply(ctx, obj, opts...)
		},
	})
	withdrawn := func() bool {
		return last.Phase != nil && *last.Phase == "" && last.ReadyReplicas != nil && *last.ReadyReplicas == ""
	}
	setPhase := func(phase miroirv1alpha1.VolumePhase) {
		t.Helper()
		got := &miroirv1alpha1.MiroirVolume{}
		if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
			t.Fatal(err)
		}
		got.Status.Phase = phase
		if err := r.Status().Update(t.Context(), got); err != nil {
			t.Fatal(err)
		}
	}

	setPhase(miroirv1alpha1.VolumeReady)
	reconcile(t, r, volPvc1) // UpToDate and connected: in service
	if last == nil || last.Phase != nil || last.ReadyReplicas != nil {
		t.Fatalf("a leg in service must apply its slot alone, got %+v", last)
	}

	fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connecting"}]}]`
	reconcile(t, r, volPvc1)
	if !withdrawn() {
		t.Fatalf("a leg out of service must blank a published Ready, got %+v", last)
	}

	// The pass above stored its fingerprint. A Ready landing afterwards
	// must still leave the fast path and be withdrawn.
	adjusts := countCalls(fe, "adjust")
	last = nil
	setPhase(miroirv1alpha1.VolumeReady)
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n <= adjusts {
		t.Fatalf("a contradicted Ready must force the full pipeline: %v", fe.calls)
	}
	if last == nil || !withdrawn() {
		t.Fatalf("a Ready that lands later must be withdrawn too, got %+v", last)
	}

	// Any other phase is the controller's to keep.
	adjusts = countCalls(fe, "adjust")
	setPhase(miroirv1alpha1.VolumeDegraded)
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "adjust"); n != adjusts {
		t.Fatalf("a Degraded phase must not leave the fast path: %v", fe.calls)
	}
	r.dropRealized(volPvc1)
	reconcile(t, r, volPvc1)
	if last.Phase != nil || last.ReadyReplicas != nil {
		t.Fatalf("a leg out of service must leave a Degraded phase alone, got %+v", last)
	}
}

// A leg whose passes keep failing re-publishes its last good slot with only
// the Message changed. That slot vouches for a Ready until its probe goes
// stale, and not after.
func TestReconcileWithdrawsReadyOnceFailingLegGoesUnprobed(t *testing.T) {
	for _, tc := range []struct {
		name         string
		probedAgo    time.Duration
		wantWithdraw bool
	}{
		{"fresh probe", time.Minute, false},
		{"stale probe", StaleProbeThreshold + time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, fe, r := steadyVolume(t)
			key := types.NamespacedName{Name: volPvc1}
			got := &miroirv1alpha1.MiroirVolume{}
			if err := r.Get(t.Context(), key, got); err != nil {
				t.Fatal(err)
			}
			got.Status.Phase = miroirv1alpha1.VolumeReady
			got.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{
				DeviceCreated: true, DiskState: diskStateUpToDate, SizeBytes: 1 << 30, Connected: true,
				LastProbedAt: ptrTime(time.Now().Add(-tc.probedAgo)),
			}
			if err := r.Status().Update(t.Context(), got); err != nil {
				t.Fatal(err)
			}
			var last *acv1alpha1.MiroirVolumeStatusApplyConfiguration
			r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
				SubResourceApply: func(ctx context.Context, cl client.Client, sub string,
					obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
					if ac, ok := obj.(*acv1alpha1.MiroirVolumeApplyConfiguration); ok && ac.Status != nil {
						last = ac.Status
					}
					return cl.SubResource(sub).Apply(ctx, obj, opts...)
				},
			})

			fe.errOn = map[string]error{"drbdadm adjust": errors.New("adjust failed")}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err == nil {
				t.Fatal("want the adjust failure to requeue")
			}
			if last == nil || last.PerNode[nodeA].Message == nil {
				t.Fatalf("the failing pass must report its error on the slot, got %+v", last)
			}
			if withdrew := last.Phase != nil && *last.Phase == ""; withdrew != tc.wantWithdraw {
				t.Fatalf("withdrew Ready = %v, want %v (apply %+v)", withdrew, tc.wantWithdraw, last)
			}
		})
	}
}

// PrimarySince stamps when the leg becomes Primary, stays stable across
// passes, and clears on demotion — the auto-diskful tie-breaker signal.
func TestReconcilePrimarySinceLifecycle(t *testing.T) {
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	fb := newFakeBackend()
	fb.existing[volPvc1] = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	primary := `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`
	fe := &fakeDRBDExec{statusJSON: primary}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }}}

	reconcile(t, r, volPvc1)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	stamped := got.Status.PerNode[nodeA].PrimarySince
	if stamped == nil {
		t.Fatal("Primary leg must stamp PrimarySince")
	}

	// Still Primary: the stamp must not move.
	reconcile(t, r, volPvc1)
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if since := got.Status.PerNode[nodeA].PrimarySince; since == nil || !since.Equal(stamped) {
		t.Fatalf("PrimarySince must be stable while Primary: %v vs %v", since, stamped)
	}

	// Demoted: the stamp clears.
	fe.statusJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"peer-disk-state":"` + diskStateUpToDate + `"}]}]}]`
	reconcile(t, r, volPvc1)
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PerNode[nodeA].PrimarySince != nil {
		t.Fatal("PrimarySince must clear on demotion")
	}
}

func TestPublishedHealthy(t *testing.T) {
	tests := []struct {
		name string
		st   miroirv1alpha1.ReplicaStatus
		want bool
	}{
		{name: "never realized"},
		{name: "connected and UpToDate", st: miroirv1alpha1.ReplicaStatus{Connected: true, DiskState: drbd.DiskUpToDate}, want: true},
		{name: "disconnected", st: miroirv1alpha1.ReplicaStatus{DiskState: drbd.DiskUpToDate}},
		{name: "not current", st: miroirv1alpha1.ReplicaStatus{Connected: true, DiskState: diskStateOutdated}},
		{name: "split-brain", st: miroirv1alpha1.ReplicaStatus{Connected: true, SplitBrain: true, DiskState: drbd.DiskUpToDate}},
		{name: "connected diskless leg", st: miroirv1alpha1.ReplicaStatus{Connected: true, Diskless: true, DiskState: drbd.DiskDiskless}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := publishedHealthy(tt.st); got != tt.want {
				t.Fatalf("publishedHealthy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLegSettled(t *testing.T) {
	withClient := func() *miroirv1alpha1.MiroirVolume {
		v := replicatedVol()
		v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeC, NodeID: 2, Address: addrC}}
		return v
	}
	connected := map[int32]bool{1: true}
	tests := []struct {
		name          string
		vol           *miroirv1alpha1.MiroirVolume
		st            drbd.Status
		localDiskless bool
		want          bool
	}{
		{
			name: "connected and UpToDate",
			vol:  replicatedVol(),
			st:   drbd.Status{DiskState: drbd.DiskUpToDate, PeerConnected: connected},
			want: true,
		},
		{
			name: "peer disconnected",
			vol:  replicatedVol(),
			st:   drbd.Status{DiskState: drbd.DiskUpToDate},
		},
		{
			name: "local disk not current",
			vol:  replicatedVol(),
			st:   drbd.Status{DiskState: diskStateOutdated, PeerConnected: connected},
		},
		{
			name: "resyncing",
			vol:  replicatedVol(),
			st:   drbd.Status{DiskState: drbd.DiskUpToDate, PeerConnected: connected, Resyncing: true},
		},
		{
			name: "online verify is not a resync",
			vol:  replicatedVol(),
			st: drbd.Status{
				DiskState: drbd.DiskUpToDate, PeerConnected: connected, Resyncing: true, Verifying: true,
			},
			want: true,
		},
		{
			name:          "diskless leg has no disk state to wait for",
			vol:           replicatedVol(),
			st:            drbd.Status{DiskState: drbd.DiskDiskless, PeerConnected: connected},
			localDiskless: true,
			want:          true,
		},
		{
			name: "client peer disconnected",
			vol:  withClient(),
			st:   drbd.Status{DiskState: drbd.DiskUpToDate, PeerConnected: connected},
		},
		{
			name: "client peer connected",
			vol:  withClient(),
			st:   drbd.Status{DiskState: drbd.DiskUpToDate, PeerConnected: map[int32]bool{1: true, 2: true}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legSettled(tt.st, tt.vol, nodeA, tt.localDiskless); got != tt.want {
				t.Fatalf("legSettled() = %v, want %v", got, tt.want)
			}
		})
	}
}
