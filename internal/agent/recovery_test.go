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
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// recoveryVolume is a two-leg replicated volume seen from nodeA (node-id 0)
// with nodeB as peer node-id 1.
func recoveryVolume() *miroirv1alpha1.MiroirVolume {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID, v.Spec.Replicas[0].Address = 0, addrA
	v.Spec.Replicas[1].NodeID, v.Spec.Replicas[1].Address = 1, addrB
	return v
}

// healthyStatus is nodeA's kernel view of a settled volume with a Primary
// on the peer.
func healthyStatus() drbd.Status {
	return drbd.Status{
		DiskState:     drbd.DiskUpToDate,
		Quorum:        true,
		PeerPrimary:   true,
		PeerConnected: map[int32]bool{1: true},
		PeerDiskState: map[int32]string{1: drbd.DiskUpToDate},
	}
}

// recoveryClock is what the two planners remember about a volume between
// passes; recoveryPlan is what they decide for one pass.
type recoveryClock struct {
	lastSplit time.Time
	stuck     *stuckMark
}

type recoveryPlan struct {
	active bool
	split  splitStep
	cycle  []peerCycle
}

// One table for both planners: a pass runs them back to back over the same
// status, and several rules are about what each leaves to the other.
func TestPlanRecovery(t *testing.T) {
	now := time.Now()
	insideFloor := now.Add(-drbdPollInterval + time.Second)
	pastFloor := now.Add(-drbdPollInterval - time.Second)
	armed := func(since time.Time, peers ...int32) *stuckMark {
		set := map[int32]bool{}
		for _, p := range peers {
			set[p] = true
		}
		return &stuckMark{since: since, peers: stuckPeerKey(set, nil)}
	}
	// armedMutual is a clock armed on one mutual-source candidate, with the
	// peer slot stamp it last saw and the publications counted so far.
	armedMutual := func(since time.Time, peer int32, stamp time.Time, advances int) *stuckMark {
		set := map[int32]bool{peer: true}
		return &stuckMark{
			since: since, peers: stuckPeerKey(set, set),
			probes: map[int32]slotProbe{peer: {stamp: stamp, advances: advances}},
		}
	}
	// peerSlot is node-b's status slot reporting disk, last probed at.
	peerSlot := func(disk string, at time.Time) func(*miroirv1alpha1.MiroirVolume) {
		return func(v *miroirv1alpha1.MiroirVolume) {
			v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeB: {DiskState: disk, LastProbedAt: ptrTime(at)}}
		}
	}
	syncSource := func(peer int32) func(*drbd.Status) {
		return func(st *drbd.Status) { st.SyncSourcePeers = map[int32]bool{peer: true} }
	}
	probe0, probe1, probe2 := now.Add(-2*time.Minute), now.Add(-20*time.Second), now

	cases := []struct {
		name string
		// vol and st adjust the healthy baseline.
		vol           func(*miroirv1alpha1.MiroirVolume)
		st            func(*drbd.Status)
		disconnected  bool
		localDiskless bool
		clock         recoveryClock
		want          recoveryPlan
		wantClock     recoveryClock
	}{
		{
			name:  "a healthy volume needs nothing and disarms the clock",
			clock: recoveryClock{stuck: armed(pastFloor, 1)},
		},
		{
			name:      "a local split on a never-written volume is resolved",
			st:        func(st *drbd.Status) { st.SplitBrain = true },
			want:      recoveryPlan{active: true, split: splitResolve},
			wantClock: recoveryClock{lastSplit: now},
		},
		{
			name:      "a split inside the floor stays active and is left alone",
			st:        func(st *drbd.Status) { st.SplitBrain = true },
			clock:     recoveryClock{lastSplit: insideFloor},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{lastSplit: insideFloor},
		},
		{
			name:      "a split past the floor is retried",
			st:        func(st *drbd.Status) { st.SplitBrain = true },
			clock:     recoveryClock{lastSplit: pastFloor},
			want:      recoveryPlan{active: true, split: splitResolve},
			wantClock: recoveryClock{lastSplit: now},
		},
		{
			name:      "an activated volume is reported on the transition edge, never resolved",
			vol:       func(v *miroirv1alpha1.MiroirVolume) { v.Status.Activated = true },
			st:        func(st *drbd.Status) { st.SplitBrain = true },
			want:      recoveryPlan{active: true, split: splitReport},
			wantClock: recoveryClock{lastSplit: now},
		},
		{
			name:      "a formatted volume counts as holding data",
			vol:       func(v *miroirv1alpha1.MiroirVolume) { v.Status.Formatted = true },
			st:        func(st *drbd.Status) { st.SplitBrain = true },
			want:      recoveryPlan{active: true, split: splitReport},
			wantClock: recoveryClock{lastSplit: now},
		},
		{
			name: "a split the slot already records is not reported again",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Status.Activated = true
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeA: {SplitBrain: true}}
			},
			st:        func(st *drbd.Status) { st.SplitBrain = true },
			want:      recoveryPlan{active: true, split: splitWatch},
			wantClock: recoveryClock{lastSplit: now},
		},
		{
			name: "a peer's report cannot churn a connected volume",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeB: {SplitBrain: true}}
			},
		},
		{
			name: "a peer's report routes the disconnected losing leg into recovery",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeB: {SplitBrain: true}}
			},
			disconnected: true,
			want:         recoveryPlan{active: true, split: splitResolve},
			wantClock:    recoveryClock{lastSplit: now},
		},
		{
			name: "a peer-reported split on a volume holding data has no local edge to report",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Status.Activated = true
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeB: {SplitBrain: true}}
			},
			disconnected: true,
			want:         recoveryPlan{active: true, split: splitWatch},
			wantClock:    recoveryClock{lastSplit: now},
		},
		{
			name:      "the first sighting of a stuck resync only arms the clock",
			st:        func(st *drbd.Status) { st.StuckResyncPeers = map[int32]bool{1: true} },
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armed(now, 1)},
		},
		{
			name:      "a stuck resync inside the confirmation window waits",
			st:        func(st *drbd.Status) { st.StuckResyncPeers = map[int32]bool{1: true} },
			clock:     recoveryClock{stuck: armed(insideFloor, 1)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armed(insideFloor, 1)},
		},
		{
			name:      "a confirmed stuck resync is cycled and the window re-armed",
			st:        func(st *drbd.Status) { st.StuckResyncPeers = map[int32]bool{1: true} },
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true, cycle: []peerCycle{{peerID: 1}}},
			wantClock: recoveryClock{stuck: armed(now, 1)},
		},
		{
			name:      "a changed candidate set restarts the wait",
			st:        func(st *drbd.Status) { st.StuckResyncPeers = map[int32]bool{1: true, 2: true} },
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armed(now, 1, 2)},
		},
		{
			name:          "a diskless leg never cycles",
			st:            func(st *drbd.Status) { st.StuckResyncPeers = map[int32]bool{1: true} },
			localDiskless: true,
			clock:         recoveryClock{stuck: armed(pastFloor, 1)},
		},
		{
			name:      "a stale bitmap on an otherwise healthy volume is cycled",
			st:        func(st *drbd.Status) { st.StaleBitmapPeers = map[int32]bool{1: true} },
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true, cycle: []peerCycle{{peerID: 1}}},
			wantClock: recoveryClock{stuck: armed(now, 1)},
		},
		{
			name: "a stale bitmap with no Primary anywhere is left to the kernel",
			st: func(st *drbd.Status) {
				st.StaleBitmapPeers = map[int32]bool{1: true}
				st.PeerPrimary = false
			},
			clock: recoveryClock{stuck: armed(pastFloor, 1)},
		},
		{
			name: "a stale bitmap beside a verify finding stays manual",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
					nodeA: {LastVerifyOutOfSyncBytes: new(int64(4096))},
				}
			},
			st:    func(st *drbd.Status) { st.StaleBitmapPeers = map[int32]bool{1: true} },
			clock: recoveryClock{stuck: armed(pastFloor, 1)},
		},
		{
			name: "a stalled target that sees the Primary takes the forced cycle",
			st: func(st *drbd.Status) {
				st.DiskState = drbd.DiskInconsistent
				st.UnsettledResyncPeers = map[int32]bool{1: true}
			},
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true, cycle: []peerCycle{{peerID: 1, stalledTarget: true}}},
			wantClock: recoveryClock{stuck: armed(now, 1)},
		},
		{
			name: "a stalled target that cannot rule out a hidden Primary stays active and unarmed",
			st: func(st *drbd.Status) {
				st.DiskState = drbd.DiskInconsistent
				st.UnsettledResyncPeers = map[int32]bool{1: true}
				st.PeerPrimary = false
			},
			clock: recoveryClock{stuck: armed(pastFloor, 1)},
			want:  recoveryPlan{active: true},
		},
		{
			name: "a stalled target acts once every other leg's fresh slot rules out a Primary",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeB: {LastProbedAt: ptrTime(now)}}
			},
			st: func(st *drbd.Status) {
				st.DiskState = drbd.DiskInconsistent
				st.UnsettledResyncPeers = map[int32]bool{1: true}
				st.PeerPrimary = false
			},
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true, cycle: []peerCycle{{peerID: 1, stalledTarget: true}}},
			wantClock: recoveryClock{stuck: armed(now, 1)},
		},
		{
			name: "candidates of both kinds cycle in node-id order, each its own way",
			st: func(st *drbd.Status) {
				st.DiskState = drbd.DiskInconsistent
				st.StuckResyncPeers = map[int32]bool{2: true}
				st.UnsettledResyncPeers = map[int32]bool{1: true}
			},
			clock: recoveryClock{stuck: armed(pastFloor, 1, 2)},
			want: recoveryPlan{active: true, cycle: []peerCycle{
				{peerID: 1, stalledTarget: true}, {peerID: 2},
			}},
			wantClock: recoveryClock{stuck: armed(now, 1, 2)},
		},
		{
			name:      "a sync source toward a peer that reads UpToDate itself only arms the clock",
			vol:       peerSlot(drbd.DiskUpToDate, probe0),
			st:        syncSource(1),
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(now, 1, probe0, 0)},
		},
		{
			name:      "a mutual source candidate's new publication is counted inside the window",
			vol:       peerSlot(drbd.DiskUpToDate, probe1),
			st:        syncSource(1),
			clock:     recoveryClock{stuck: armedMutual(insideFloor, 1, probe0, 0)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(insideFloor, 1, probe1, 1)},
		},
		{
			name:      "one publication may predate the resync, so the window alone cycles nothing",
			vol:       peerSlot(drbd.DiskUpToDate, probe1),
			st:        syncSource(1),
			clock:     recoveryClock{stuck: armedMutual(pastFloor, 1, probe0, 0)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
		},
		{
			name:      "a slot the peer has not published again is not counted twice",
			vol:       peerSlot(drbd.DiskUpToDate, probe1),
			st:        syncSource(1),
			clock:     recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
		},
		{
			name:      "a mutual source the peer published twice is cycled and its count reset",
			vol:       peerSlot(drbd.DiskUpToDate, probe2),
			st:        syncSource(1),
			clock:     recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
			want:      recoveryPlan{active: true, cycle: []peerCycle{{peerID: 1}}},
			wantClock: recoveryClock{stuck: armedMutual(now, 1, probe2, 0)},
		},
		{
			name: "the higher node id of a mutual source pair stays active and leaves the cycle to its peer",
			vol: func(v *miroirv1alpha1.MiroirVolume) {
				v.Spec.Replicas[0].NodeID, v.Spec.Replicas[1].NodeID = 1, 0
				peerSlot(drbd.DiskUpToDate, probe2)(v)
			},
			st: func(st *drbd.Status) {
				st.PeerConnected = map[int32]bool{0: true}
				syncSource(0)(st)
			},
			clock:     recoveryClock{stuck: armedMutual(pastFloor, 0, probe1, 1)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(pastFloor, 0, probe2, 2)},
		},
		{
			name:      "a peer that moves from a stale bitmap to a mutual source restarts the wait",
			vol:       peerSlot(drbd.DiskUpToDate, probe2),
			st:        syncSource(1),
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(now, 1, probe2, 0)},
		},
		{
			name:      "a slot stamp that moves back is a republished slot, not a publication",
			vol:       peerSlot(drbd.DiskUpToDate, probe0),
			st:        syncSource(1),
			clock:     recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
			want:      recoveryPlan{active: true},
			wantClock: recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
		},
		{
			name:  "a sync source toward a peer that reads Inconsistent itself is a live resync",
			vol:   peerSlot(drbd.DiskInconsistent, probe2),
			st:    syncSource(1),
			clock: recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
		},
		{
			name:  "a sync source toward a peer with no status slot is left alone",
			st:    syncSource(1),
			clock: recoveryClock{stuck: armedMutual(pastFloor, 1, probe1, 1)},
		},
		{
			name: "a split and a stuck resync are both planned for one pass",
			st: func(st *drbd.Status) {
				st.SplitBrain = true
				st.StuckResyncPeers = map[int32]bool{1: true}
			},
			clock:     recoveryClock{stuck: armed(pastFloor, 1)},
			want:      recoveryPlan{active: true, split: splitResolve, cycle: []peerCycle{{peerID: 1}}},
			wantClock: recoveryClock{lastSplit: now, stuck: armed(now, 1)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := recoveryVolume()
			if tc.vol != nil {
				tc.vol(v)
			}
			st := healthyStatus()
			if tc.st != nil {
				tc.st(&st)
			}
			if tc.disconnected {
				st.PeerConnected = map[int32]bool{}
			}
			connected := diskfulPeersConnected(st, v, nodeA)

			splitActive, split, lastSplit := planSplitBrain(v, nodeA, st, connected, tc.clock.lastSplit, now)
			stuckActive, cycle, stuck := planStuckResync(v, nodeA, st, tc.localDiskless, tc.clock.stuck, now)
			got := recoveryPlan{active: splitActive || stuckActive, split: split, cycle: cycle}
			gotClock := recoveryClock{lastSplit: lastSplit, stuck: stuck}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("plan = %+v, want %+v", got, tc.want)
			}
			if !gotClock.lastSplit.Equal(tc.wantClock.lastSplit) {
				t.Errorf("lastSplit = %v, want %v", gotClock.lastSplit, tc.wantClock.lastSplit)
			}
			if !reflect.DeepEqual(gotClock.stuck, tc.wantClock.stuck) {
				t.Errorf("stuck = %+v, want %+v", gotClock.stuck, tc.wantClock.stuck)
			}
		})
	}
}

// splitBrainSetup builds a 2-replica DRBD volume (node-a node id 0 = seed
// winner, node-b node id 1) whose kernel status reports a connState
// connection to peerNodeID, plus a reconciler running on nodeName. activated
// latches Status.Activated (the auto-recovery gate).
func splitBrainSetup(t *testing.T, nodeName, peerNodeID, connState string, activated bool) (*VolumeReconciler, *fakeDRBDExec) {
	t.Helper()
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Status.Activated = activated

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":` + peerNodeID + `,"connection-state":"` + connState + `"}]}]`}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeName, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	return r, fe
}

// A never-activated volume that comes up split-brain self-heals: the seed
// winner (node-a, node id 0) reconnects as the sync source and never
// discards data (issue #139).
func TestReconcileSplitBrainWinnerReconnectsWhenNeverActivated(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeA, "1", "StandAlone", false)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect pvc-1")
	fe.notCalledWith(t, "discard-my-data")
}

// The losing leg (node-b, node id 1) discards its own generation so it
// full-syncs from the winner.
func TestReconcileSplitBrainLoserDiscardsWhenNeverActivated(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "StandAlone", false)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}

// An activated volume may hold data: split-brain is never auto-resolved, it
// is left for an operator.
func TestReconcileSplitBrainNoAutoRecoveryWhenActivated(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "StandAlone", true)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "drbdadm disconnect")
}

// A formatted-but-not-activated volume (e.g. a clone whose stage failed at
// grow-to-fill after mkfs/mount) carries data and must not be auto-discarded,
// even though Activated is still false.
func TestReconcileSplitBrainNoAutoRecoveryWhenFormatted(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "StandAlone", false)
	var v miroirv1alpha1.MiroirVolume
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &v); err != nil {
		t.Fatal(err)
	}
	v.Status.Formatted = true
	if err := r.Status().Update(t.Context(), &v); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "drbdadm disconnect")
}

// reportPeerSplitBrain marks the given node's status slot split-brain, the
// signal a survivor leaves for the losing leg.
func reportPeerSplitBrain(t *testing.T, r *VolumeReconciler, node string) {
	t.Helper()
	var v miroirv1alpha1.MiroirVolume
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &v); err != nil {
		t.Fatal(err)
	}
	if v.Status.PerNode == nil {
		v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	v.Status.PerNode[node] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true, SplitBrain: true,
	}
	if err := r.Status().Update(t.Context(), &v); err != nil {
		t.Fatal(err)
	}
}

// The losing leg of a birth split never parks StandAlone — its connection
// sits Connecting while the survivor refuses the handshake — so it must take
// the winner's reported split-brain as its trigger and discard (issue #144).
func TestReconcileSplitBrainLoserDiscardsOnPeerReport(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "Connecting", false)
	reportPeerSplitBrain(t, r, nodeA)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}

// A stale peer split-brain report on a healthy, fully connected leg (the
// survivor's status patch lags its recovery) must not churn the volume.
func TestReconcileSplitBrainPeerReportIgnoredWhenConnected(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeB, "0", "Connected", false)
	reportPeerSplitBrain(t, r, nodeA)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "drbdadm disconnect")
}

// Recovery flaps connections, and each flap is a DRBD event that requeues
// the volume — attempts must be floored to one per poll interval or the
// agent thrashes several times per second (issue #144).
func TestReconcileSplitBrainRecoveryDebounced(t *testing.T) {
	r, fe := splitBrainSetup(t, nodeA, "1", "StandAlone", false)
	reconcile(t, r, volPvc1)
	attempts := countCalls(fe, "drbdadm connect pvc-1")
	if attempts != 1 {
		t.Fatalf("first reconcile must attempt recovery once, got %d", attempts)
	}
	reconcile(t, r, volPvc1)
	if n := countCalls(fe, "drbdadm connect pvc-1"); n != attempts {
		t.Fatalf("immediate re-reconcile must not re-attempt recovery: %d -> %d", attempts, n)
	}
}

// --- stuck resync (issue #390) --------------------------------------------

const (
	// stuckStatusJSON is the stranded-bitmap signature: out-of-sync bits
	// toward a connected peer whose replication sits Established and whose
	// disk is parked Consistent — a resync DRBD armed and abandoned.
	stuckStatusJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
			"peer_devices":[{"replication-state":"Established","peer-disk-state":"Consistent","out-of-sync":5171836}]}]}]`
	// stalledTargetStatusJSON is the target side of a resync the source
	// finished alone (issue #576): this leg Inconsistent, parked WFBitMapT
	// toward a connected UpToDate peer with nothing left to sync.
	stalledTargetStatusJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"Inconsistent","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
			"peer_devices":[{"replication-state":"WFBitMapT","peer-disk-state":"UpToDate","out-of-sync":0}]}]}]`
	healthyStatusJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate","quorum":true}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected",
			"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate"}]}]}]`
)

// stuckResyncSetup builds a settled, Activated 2-replica DRBD volume on
// node-a (node id 0) whose kernel wears the stranded-bitmap signature
// toward node-b (node id 1). Activated on purpose: unlike split-brain
// recovery, the connection cycle discards nothing, so held data is no gate.
func stuckResyncSetup(t *testing.T) (*VolumeReconciler, *fakeDRBDExec) {
	t.Helper()
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Status.Activated = true
	// Both legs settled at spec size so the pass is steady-state and the
	// fingerprint gate hinges on the stuck signature alone.
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, SizeBytes: 1 << 30},
		nodeB: {DeviceCreated: true, SizeBytes: 1 << 30},
	}

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \"node-a\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{statusJSON: stuckStatusJSON}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	return r, fe
}

// backdateStuck rewinds the confirmation clock so the next pass treats the
// signature as having persisted through a full poll interval.
func backdateStuck(t *testing.T, r *VolumeReconciler) {
	t.Helper()
	r.remember(volPvc1, func(m *volumeMemory) {
		if m.stuck == nil {
			t.Fatal("confirmation clock not armed")
		}
		mark := *m.stuck
		mark.since = time.Now().Add(-drbdPollInterval - time.Second)
		m.stuck = &mark
	})
}

// The first sighting only arms the confirmation clock — a status snapshot
// can catch a live handshake mid-transition. Once the signature has
// persisted through a poll interval, recovery cycles exactly the stuck
// peer connection, held data notwithstanding (the volume is Activated).
func TestReconcileStuckResyncCyclesAfterConfirmation(t *testing.T) {
	r, fe := stuckResyncSetup(t)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdsetup disconnect")
	backdateStuck(t, r)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.calledWith(t, "drbdsetup connect pvc-1 1")
}

// setPeerSlot rewrites node-b's status slot through mutate.
func setPeerSlot(t *testing.T, r *VolumeReconciler, mutate func(*miroirv1alpha1.ReplicaStatus)) {
	t.Helper()
	v := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), client.ObjectKey{Name: volPvc1}, v); err != nil {
		t.Fatal(err)
	}
	slot := v.Status.PerNode[nodeB]
	mutate(&slot)
	v.Status.PerNode[nodeB] = slot
	if err := r.Status().Update(t.Context(), v); err != nil {
		t.Fatal(err)
	}
}

// stalledTargetSetup is stuckResyncSetup with this leg wearing the
// stalled-target signature (issue #576) and the peer's slot freshly
// probed, reporting no Primary.
func stalledTargetSetup(t *testing.T) (*VolumeReconciler, *fakeDRBDExec) {
	t.Helper()
	r, fe := stuckResyncSetup(t)
	fe.statusJSON = stalledTargetStatusJSON
	setPeerSlot(t, r, func(s *miroirv1alpha1.ReplicaStatus) { s.LastProbedAt = probeNow() })
	return r, fe
}

// The stalled target of a resync the source finished alone (issue #576)
// cycles the peer connection from its own side once the signature
// outlives the confirmation window — gracefully when the kernel lets it.
func TestReconcileStalledTargetCyclesAfterConfirmation(t *testing.T) {
	r, fe := stalledTargetSetup(t)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdsetup disconnect")
	backdateStuck(t, r)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.notCalledWith(t, "--force")
	fe.calledWith(t, "drbdsetup connect pvc-1 1")
}

// A graceful disconnect the kernel refuses (a read-only opener pins the
// Inconsistent device to its last UpToDate peer) is retried forced.
func TestReconcileStalledTargetForcesRefusedDisconnect(t *testing.T) {
	r, fe := stalledTargetSetup(t)
	fe.errOn = map[string]error{"=drbdsetup disconnect pvc-1 1": errors.New("(-2) Need access to UpToDate data")}
	reconcile(t, r, volPvc1)
	backdateStuck(t, r)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1 --force")
	fe.calledWith(t, "drbdsetup connect pvc-1 1")
}

// The stability gate: a Primary this leg cannot see — on the diskful
// peer or on a client leg — makes the same shape the ordinary end of an
// unstable resync, which the kernel re-runs by itself once the Primary is
// back in view, and a peer slot too stale to rule a Primary out keeps the
// case with the kernel too — all uncycled and, since only the peers'
// status can change that, kept out of the fast path. A Primary this leg
// does see is the live-writer case of the hang and is cycled.
func TestReconcileStalledTargetStabilityGate(t *testing.T) {
	const secondary = "Secondary"
	for name, tc := range map[string]struct {
		peerRole string
		slot     func(*miroirv1alpha1.ReplicaStatus)
		client   bool
		cycled   bool
	}{
		"unseen primary": {peerRole: secondary, slot: func(s *miroirv1alpha1.ReplicaStatus) { s.PrimarySince = probeNow() }},
		"stale peer slot": {peerRole: secondary, slot: func(s *miroirv1alpha1.ReplicaStatus) {
			old := metav1.NewTime(time.Now().Add(-StaleProbeThreshold - time.Minute))
			s.LastProbedAt = &old
		}},
		"unseen client primary": {peerRole: secondary, client: true, slot: func(s *miroirv1alpha1.ReplicaStatus) { s.LastProbedAt = probeNow() }},
		"seen primary":          {peerRole: "Primary", slot: func(s *miroirv1alpha1.ReplicaStatus) { s.PrimarySince = probeNow() }, cycled: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, fe := stalledTargetSetup(t)
			fe.statusJSON = strings.Replace(stalledTargetStatusJSON, `"peer-role":"Secondary"`, `"peer-role":"`+tc.peerRole+`"`, 1)
			setPeerSlot(t, r, tc.slot)
			if tc.client {
				// A client leg this target has no connection to, whose
				// own fresh slot reports it Primary.
				v := &miroirv1alpha1.MiroirVolume{}
				if err := r.Get(t.Context(), client.ObjectKey{Name: volPvc1}, v); err != nil {
					t.Fatal(err)
				}
				v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeC, NodeID: 2, Address: addrC}}
				if err := r.Update(t.Context(), v); err != nil {
					t.Fatal(err)
				}
				if err := r.Get(t.Context(), client.ObjectKey{Name: volPvc1}, v); err != nil {
					t.Fatal(err)
				}
				v.Status.PerNode[nodeC] = miroirv1alpha1.ReplicaStatus{Diskless: true, PrimarySince: probeNow(), LastProbedAt: probeNow()}
				if err := r.Status().Update(t.Context(), v); err != nil {
					t.Fatal(err)
				}
			}
			reconcile(t, r, volPvc1)
			if tc.cycled {
				backdateStuck(t, r)
				reconcile(t, r, volPvc1)
				fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
				return
			}
			reconcile(t, r, volPvc1)
			fe.notCalledWith(t, "drbdsetup disconnect")
			// Gated, not merely inside the window: the confirmation clock
			// never arms, so no later pass can cycle either.
			armed := r.recall(volPvc1).stuck != nil
			if armed {
				t.Fatal("a gated stalled target must not arm the confirmation clock")
			}
			parked := r.recall(volPvc1).realized != nil
			if parked {
				t.Fatal("a gated stalled target must not park in the fast path")
			}
		})
	}
}

// mutualSourceStatusJSON is one end of a resync with a source at both ends
// (issue #675): this UpToDate leg SyncSource toward a peer it reads
// Inconsistent, nothing moving.
const mutualSourceStatusJSON = `[{"name":"pvc-1","role":"Secondary",
	"devices":[{"disk-state":"UpToDate","quorum":true}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
		"peer_devices":[{"replication-state":"SyncSource","peer-disk-state":"Inconsistent","percent-in-sync":0.24,"out-of-sync":8366800}]}]}]`

// A sync source whose peer reports its own disk UpToDate is cycled once the
// signature outlives the confirmation window and the peer has published its
// slot twice since this leg armed. One publication is not enough (it can
// carry a kernel reading older than the resync), and a peer that reports
// itself Inconsistent is the target of a live resync.
func TestReconcileMutualSourceCountsPeerPublications(t *testing.T) {
	base := time.Now().Add(-time.Minute)
	for name, tc := range map[string]struct {
		peerDisk string
		// publications is how many times the peer stamps its slot after
		// this leg armed.
		publications  int
		armed, cycled bool
	}{
		"published twice":    {peerDisk: drbd.DiskUpToDate, publications: 2, armed: true, cycled: true},
		"published once":     {peerDisk: drbd.DiskUpToDate, publications: 1, armed: true},
		"never published":    {peerDisk: drbd.DiskUpToDate, armed: true},
		"live resync target": {peerDisk: drbd.DiskInconsistent, publications: 2},
	} {
		t.Run(name, func(t *testing.T) {
			r, fe := stuckResyncSetup(t)
			fe.statusJSON = mutualSourceStatusJSON
			publish := func(n int) {
				setPeerSlot(t, r, func(s *miroirv1alpha1.ReplicaStatus) {
					s.DiskState, s.LastProbedAt = tc.peerDisk, ptrTime(base.Add(time.Duration(n)*10*time.Second))
				})
			}
			publish(0)
			reconcile(t, r, volPvc1)
			armed := r.recall(volPvc1).stuck != nil
			if armed != tc.armed {
				t.Fatalf("confirmation clock armed = %v, want %v", armed, tc.armed)
			}
			parked := r.recall(volPvc1).realized != nil
			if tc.armed && parked {
				t.Fatal("a mutual source candidate must not park in the fast path")
			}
			for n := 1; n <= tc.publications; n++ {
				publish(n)
				reconcile(t, r, volPvc1)
				fe.notCalledWith(t, "drbdsetup disconnect")
			}
			if !tc.armed {
				return
			}
			backdateStuck(t, r)
			reconcile(t, r, volPvc1)
			if !tc.cycled {
				fe.notCalledWith(t, "drbdsetup disconnect")
				return
			}
			fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
			fe.calledWith(t, "drbdsetup connect pvc-1 1")
		})
	}
}

// Within the confirmation window nothing is cycled, however many passes run
// (DRBD events can requeue the volume several times per interval).
func TestReconcileStuckResyncDebounced(t *testing.T) {
	r, fe := stuckResyncSetup(t)
	reconcile(t, r, volPvc1)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdsetup disconnect")
}

// The stuck signature keeps the volume out of the fast path (its kernel
// state is otherwise steady), and a healthy pass parks it again and resets
// the confirmation clock — a signature that clears and returns must
// re-confirm from scratch.
func TestReconcileStuckResyncFingerprintLifecycle(t *testing.T) {
	r, fe := stuckResyncSetup(t)
	reconcile(t, r, volPvc1)
	parked := r.recall(volPvc1).realized != nil
	if parked {
		t.Fatal("stuck volume must not park in the fast path")
	}

	fe.statusJSON = healthyStatusJSON
	reconcile(t, r, volPvc1)
	parked = r.recall(volPvc1).realized != nil
	if !parked {
		t.Fatal("healthy volume must park in the fast path")
	}
	armed := r.recall(volPvc1).stuck != nil
	if armed {
		t.Fatal("healthy pass must reset the confirmation clock")
	}
}

// A leg wearing the stranded-bitmap signature reads connected and UpToDate,
// but its recovery is still pending: the agent must not report ready on it,
// neither while the confirmation clock runs nor on the pass that cycles the
// connection.
func TestReconcileStuckResyncHoldsStartupGate(t *testing.T) {
	r, _ := stuckResyncSetup(t)
	r.Startup = &StartupGate{Reader: r.Client, NodeName: nodeA, Deadline: time.Now().Add(time.Hour)}
	r.Startup.Baseline(volPvc1, true)
	readyz := httptest.NewRequest("GET", "/readyz", nil)

	reconcile(t, r, volPvc1)
	if r.Startup.Check(readyz) == nil {
		t.Fatal("ready while the stuck-resync confirmation clock is running")
	}
	backdateStuck(t, r)
	reconcile(t, r, volPvc1)
	if r.Startup.Check(readyz) == nil {
		t.Fatal("ready on the status read before the connection was cycled")
	}
}

// staleBitmapStatusJSON is the stale one-sided-bitmap signature (issue
// #389): out-of-sync bits toward a connected, UpToDate Primary peer with
// replication Established and this leg Secondary — everything healthy but
// the bits, which nothing drains.
const staleBitmapStatusJSON = `[{"name":"pvc-1","role":"Secondary",
	"devices":[{"disk-state":"UpToDate","quorum":true}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected","peer-role":"Primary",
		"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":5242880}]}]}]`

// A stale one-sided bitmap is cycled once its signature outlives the
// confirmation window, exactly like a stranded after-unstable bitmap. The
// re-handshake takes its direction from generation UUIDs, so any pair but
// Primary/Primary qualifies as long as the volume has a Primary.
func TestReconcileStaleBitmapCyclesAfterConfirmation(t *testing.T) {
	tests := []struct {
		name   string
		status string
		// cycled is the peer node id whose connection is cycled; untouched
		// (0 for none) is one that must be left alone.
		cycled, untouched int
	}{
		{
			name:   "Secondary holding bits toward the Primary",
			status: staleBitmapStatusJSON,
			cycled: 1,
		},
		{
			// After a failover the new Primary holds the bitmap toward a
			// healthy Secondary; only the Primary can see it.
			name: "Primary holding bits toward a Secondary",
			status: `[{"name":"pvc-1","role":"Primary",
				"devices":[{"disk-state":"UpToDate","quorum":true}],
				"connections":[{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":392}]}]}]`,
			cycled: 1,
		},
		{
			// The Primary is up and clean toward both: only the stale
			// Secondary/Secondary connection is cycled.
			name: "Secondary holding bits toward another Secondary",
			status: `[{"name":"pvc-1","role":"Secondary",
				"devices":[{"disk-state":"UpToDate","quorum":true}],
				"connections":[
					{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
						"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":4}]},
					{"peer-node-id":2,"connection-state":"Connected","peer-role":"Primary",
						"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":0}]}]}]`,
			cycled:    1,
			untouched: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, fe := stuckResyncSetup(t)
			fe.statusJSON = tt.status
			reconcile(t, r, volPvc1)
			fe.notCalledWith(t, "drbdsetup disconnect")
			backdateStuck(t, r)
			reconcile(t, r, volPvc1)
			fe.calledWith(t, fmt.Sprintf("drbdsetup disconnect pvc-1 %d", tt.cycled))
			fe.calledWith(t, fmt.Sprintf("drbdsetup connect pvc-1 %d", tt.cycled))
			if tt.untouched != 0 {
				fe.notCalledWith(t, fmt.Sprintf("drbdsetup disconnect pvc-1 %d", tt.untouched))
			}
		})
	}
}

// Shapes that wear the stale-bitmap signature but are not this recovery's
// to fix: nothing is cycled and the confirmation clock never arms.
func TestReconcileStaleBitmapStaysManual(t *testing.T) {
	tests := []struct {
		name   string
		status string
		// slot edits node-a's status slot before the passes: node-a is the
		// first diskful replica, the verify coordinator.
		slot func(*miroirv1alpha1.ReplicaStatus)
	}{
		{
			// The kernel reconciles survivors of a lost Primary with its own
			// resync on reconnect.
			name: "Secondary pair with no Primary anywhere",
			status: `[{"name":"pvc-1","role":"Secondary",
				"devices":[{"disk-state":"UpToDate","quorum":true}],
				"connections":[{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":392}]}]}]`,
		},
		{
			// A genuine data-difference report: auto-resyncing it would
			// destroy the evidence of which leg was wrong.
			name:   "verify finding on record",
			status: staleBitmapStatusJSON,
			slot: func(s *miroirv1alpha1.ReplicaStatus) {
				s.LastVerifyOutOfSyncBytes = new(int64(4096))
			},
		},
		{
			// A verify started but not recorded yet may have left findings
			// in the kernel that no status field accounts for.
			name:   "verify started but not recorded",
			status: staleBitmapStatusJSON,
			slot: func(s *miroirv1alpha1.ReplicaStatus) {
				s.VerifyStartedAt = new(metav1.Now())
			},
		},
		{
			// The state is not steady while any resync runs.
			name: "resync in flight elsewhere on the volume",
			status: `[{"name":"pvc-1","role":"Secondary",
				"devices":[{"disk-state":"UpToDate","quorum":true}],
				"connections":[
					{"peer-node-id":1,"connection-state":"Connected","peer-role":"Primary",
						"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":5242880}]},
					{"peer-node-id":2,"connection-state":"Connected",
						"peer_devices":[{"replication-state":"SyncSource","peer-disk-state":"Inconsistent","percent-in-sync":50,"out-of-sync":1024}]}]}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, fe := stuckResyncSetup(t)
			fe.statusJSON = tt.status
			if tt.slot != nil {
				var v miroirv1alpha1.MiroirVolume
				if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &v); err != nil {
					t.Fatal(err)
				}
				slot := v.Status.PerNode[nodeA]
				tt.slot(&slot)
				v.Status.PerNode[nodeA] = slot
				if err := r.Status().Update(t.Context(), &v); err != nil {
					t.Fatal(err)
				}
			}
			reconcile(t, r, volPvc1)
			reconcile(t, r, volPvc1)
			fe.notCalledWith(t, "drbdsetup disconnect")
			armed := r.recall(volPvc1).stuck != nil
			if armed {
				t.Fatal("the confirmation clock must stay unarmed")
			}
		})
	}
}

// The confirmation clock belongs to the candidate set it armed on: when
// the set changes, a newly seen peer must wait out its own poll interval
// rather than ride the clock an earlier peer armed.
func TestReconcileStaleBitmapCandidateChangeRearms(t *testing.T) {
	r, fe := stuckResyncSetup(t)
	stalePeer1 := `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate","quorum":true}],
		"connections":[
			{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
				"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":4}]},
			{"peer-node-id":2,"connection-state":"Connected","peer-role":"Primary",
				"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":0}]}]}]`
	stalePeer2 := `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate","quorum":true}],
		"connections":[
			{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
				"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":0}]},
			{"peer-node-id":2,"connection-state":"Connected","peer-role":"Primary",
				"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":4}]}]}]`
	fe.statusJSON = stalePeer1
	reconcile(t, r, volPvc1)
	backdateStuck(t, r)
	// Peer 1 cleared itself and peer 2 appeared in the same pass: peer 2
	// must not be cycled on peer 1's expired clock.
	fe.statusJSON = stalePeer2
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "drbdsetup disconnect")
	backdateStuck(t, r)
	reconcile(t, r, volPvc1)
	fe.calledWith(t, "drbdsetup disconnect pvc-1 2")
	fe.notCalledWith(t, "drbdsetup disconnect pvc-1 1")
}
