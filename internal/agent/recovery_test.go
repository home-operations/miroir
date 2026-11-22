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
	"reflect"
	"testing"
	"time"

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
		return &stuckMark{since: since, peers: stuckPeerKey(set)}
	}

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
