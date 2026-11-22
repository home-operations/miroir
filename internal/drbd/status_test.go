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

package drbd

import (
	"testing"
)

func TestStatusParsing(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","suspended-user":true,
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"Connected","peer-role":"Primary"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.DiskState != "UpToDate" || s.SplitBrain || !s.PeerPrimary || !s.Suspended {
		t.Fatalf("unexpected status %+v", s)
	}
}

// Per-peer connection state keys on the DRBD node id, so consumers can
// ignore a diskless tie-breaker's link (snapshot barrier, removal gating).
func TestStatusPerPeerConnected(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected"},
				{"peer-node-id":2,"connection-state":"Connecting"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.PeerConnected[1] || s.PeerConnected[2] {
		t.Fatalf("per-peer state wrong: %+v", s.PeerConnected)
	}
}

// Per-peer disk state keys on the DRBD node id — the birth-generation
// trigger requires every diskful leg to read Inconsistent.
func TestStatusPerPeerDiskState(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"Inconsistent"}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected",
				 "peer_devices":[{"peer-disk-state":"Inconsistent"}]},
				{"peer-node-id":2,"connection-state":"Connecting",
				 "peer_devices":[{"peer-disk-state":"DUnknown"}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.PeerDiskState[1] != DiskInconsistent || s.PeerDiskState[2] != "DUnknown" {
		t.Fatalf("per-peer disk state wrong: %+v", s.PeerDiskState)
	}
}

func TestStatusSplitBrain(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"StandAlone"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.SplitBrain {
		t.Fatalf("StandAlone must surface as split-brain: %+v", s)
	}
}

// percent-in-sync is a peer-device field; Status surfaces the least-synced
// leg as ResyncPercent (100 when fully in sync). Verified against the
// drbdsetup source JSON shape.
func TestStatusResyncPercent(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"Inconsistent","quorum":true}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected",
				"peer_devices":[{"replication-state":"SyncTarget","percent-in-sync":42.5,"out-of-sync":2048}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Resyncing || s.ResyncPercent != 42.5 {
		t.Fatalf("want Resyncing with ResyncPercent 42.5, got %+v", s)
	}
	if !s.Quorum || s.OutOfSyncKiB != 2048 {
		t.Fatalf("want Quorum with OutOfSyncKiB 2048, got %+v", s)
	}
}

// The device quorum flag surfaces a freeze-policy volume whose partition
// lost quorum (IO suspending); out-of-sync tracks the worst peer.
func TestStatusQuorumLost(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate","quorum":false}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connecting",
					"peer_devices":[{"replication-state":"Off","out-of-sync":4096}]},
				{"peer-node-id":2,"connection-state":"Connecting",
					"peer_devices":[{"replication-state":"Off","out-of-sync":1024}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Quorum {
		t.Fatalf("quorum lost must surface as Quorum=false: %+v", s)
	}
	if s.OutOfSyncKiB != 4096 {
		t.Fatalf("OutOfSyncKiB must track the worst peer (4096), got %d", s.OutOfSyncKiB)
	}
	if s.Resyncing {
		t.Fatalf("Off replication must not read as resync: %+v", s)
	}
}

// StuckResyncPeers flags only the stranded-bitmap signature: Connected +
// peer-disk Consistent + out-of-sync, replication Established (issue #390)
// or parked WFBitMapS (issue #397). A running resync, verify findings
// (peer-disk stays UpToDate), a clean Consistent peer, and the WFBitMapT
// target side must all stay unflagged.
func TestStatusStuckResyncPeers(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate","quorum":true}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"Consistent","out-of-sync":5171836}]},
				{"peer-node-id":2,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"SyncSource","peer-disk-state":"Consistent","percent-in-sync":50,"out-of-sync":1024}]},
				{"peer-node-id":3,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":12}]},
				{"peer-node-id":4,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"Consistent","out-of-sync":0}]},
				{"peer-node-id":5,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"WFBitMapS","peer-disk-state":"Consistent","out-of-sync":5171836}]},
				{"peer-node-id":6,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"WFBitMapT","peer-disk-state":"UpToDate","out-of-sync":5171836}]},
				{"peer-node-id":7,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"WFBitMapS","peer-disk-state":"Consistent","out-of-sync":0}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.StuckResyncPeers) != 2 || !s.StuckResyncPeers[1] || !s.StuckResyncPeers[5] {
		t.Fatalf("want peers 1 and 5 flagged stuck, got %v", s.StuckResyncPeers)
	}
}

// UnsettledResyncPeers flags, on an Inconsistent Secondary leg, the
// UpToDate peers it sits connected to with nothing to sync and no resync
// starting (issue #576): replication WFBitMapT or Established with zero
// out-of-sync bits. Bits still pending, an Inconsistent peer, and a
// disconnected peer stay unflagged.
func TestStatusUnsettledResyncPeers(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Inconsistent","quorum":true}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"WFBitMapT","peer-disk-state":"UpToDate","out-of-sync":0}]},
				{"peer-node-id":2,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":0}]},
				{"peer-node-id":3,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"WFBitMapT","peer-disk-state":"UpToDate","out-of-sync":4096}]},
				{"peer-node-id":4,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"Inconsistent","out-of-sync":0}]},
				{"peer-node-id":5,"connection-state":"Connecting",
					"peer_devices":[{"replication-state":"Off","peer-disk-state":"DUnknown","out-of-sync":0}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.UnsettledResyncPeers) != 2 || !s.UnsettledResyncPeers[1] || !s.UnsettledResyncPeers[2] {
		t.Fatalf("want peers 1 and 2 flagged unsettled, got %v", s.UnsettledResyncPeers)
	}
	if s.StuckResyncPeers != nil || s.StaleBitmapPeers != nil {
		t.Fatalf("an Inconsistent leg holds no bitmap signature, got stuck %v stale %v", s.StuckResyncPeers, s.StaleBitmapPeers)
	}
}

// The unsettled signature is void for the whole entry while any leg
// resyncs (a third diskful peer serving this one), on a Primary (its
// consumer, not a probe, would see the forced disconnect), and on a leg
// whose disk is not Inconsistent (nothing left to settle).
func TestStatusUnsettledResyncPeersExclusions(t *testing.T) {
	const peer = `{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"replication-state":"WFBitMapT","peer-disk-state":"UpToDate","out-of-sync":0}]}`
	for name, entry := range map[string]string{
		"resync elsewhere": `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Inconsistent","quorum":true}],
			"connections":[` + peer + `,
				{"peer-node-id":2,"connection-state":"Connected",
					"peer_devices":[{"replication-state":"SyncTarget","peer-disk-state":"UpToDate","percent-in-sync":40,"out-of-sync":8192}]}]}]`,
		"primary": `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"Inconsistent","quorum":true}],
			"connections":[` + peer + `]}]`,
		"uptodate": `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"UpToDate","quorum":true}],
			"connections":[` + peer + `]}]`,
	} {
		t.Run(name, func(t *testing.T) {
			fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: entry}}
			d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
			s, err := d.Status(t.Context(), volPvc1)
			if err != nil {
				t.Fatal(err)
			}
			if s.UnsettledResyncPeers != nil {
				t.Fatalf("want no unsettled peers, got %v", s.UnsettledResyncPeers)
			}
		})
	}
}

// StaleBitmapPeers flags a one-sided bitmap toward a healthy peer:
// Connected + Established + peer-disk UpToDate + out-of-sync, whether the
// peer is the Primary (issue #389) or another Secondary (issue #497). A
// Consistent peer-disk (that is the stuck-resync signature) and a clean
// peer must stay unflagged.
func TestStatusStaleBitmapPeers(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"UpToDate","quorum":true}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected","peer-role":"Primary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":5242880}]},
				{"peer-node-id":2,"connection-state":"Connected","peer-role":"Secondary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":4096}]},
				{"peer-node-id":3,"connection-state":"Connected","peer-role":"Primary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"Consistent","out-of-sync":4096}]},
				{"peer-node-id":4,"connection-state":"Connected","peer-role":"Primary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":0}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.StaleBitmapPeers) != 2 || !s.StaleBitmapPeers[1] || !s.StaleBitmapPeers[2] {
		t.Fatalf("want peers 1 and 2 flagged stale, got %v", s.StaleBitmapPeers)
	}
	if len(s.StuckResyncPeers) != 1 || !s.StuckResyncPeers[3] {
		t.Fatalf("want only peer 3 flagged stuck, got %v", s.StuckResyncPeers)
	}
}

// The inverse pair (issue #469): a Primary leg holding a one-sided bitmap
// toward a healthy Secondary peer is flagged too — the Primary sources the
// re-handshake, so the direction is just as settled. A Primary peer of a
// Primary leg (dual-primary, no unambiguous source) stays unflagged.
func TestStatusStaleBitmapPeersPrimaryLeg(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"UpToDate","quorum":true}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected","peer-role":"Secondary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":392}]},
				{"peer-node-id":2,"connection-state":"Connected","peer-role":"Secondary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":0}]},
				{"peer-node-id":3,"connection-state":"Connected","peer-role":"Primary",
					"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate","out-of-sync":4096}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.StaleBitmapPeers) != 1 || !s.StaleBitmapPeers[1] {
		t.Fatalf("want only peer 1 flagged stale, got %v", s.StaleBitmapPeers)
	}
}

// A healthy volume reports no stuck peers — nil, so fingerprint comparison
// via maps.Equal treats it the same as an empty map.
func TestStatusStuckResyncPeersNilWhenHealthy(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate","quorum":true}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected",
				"peer_devices":[{"replication-state":"Established","peer-disk-state":"UpToDate"}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.StuckResyncPeers != nil {
		t.Fatalf("want nil StuckResyncPeers on a healthy volume, got %v", s.StuckResyncPeers)
	}
	if s.StaleBitmapPeers != nil {
		t.Fatalf("want nil StaleBitmapPeers on a healthy volume, got %v", s.StaleBitmapPeers)
	}
}

// A fully in-sync volume reports ResyncPercent 100, and connection-level
// replication-state (the pre-fix wrong nesting) is correctly ignored.
func TestStatusResyncPercentDefaultsFull(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected","replication-state":"SyncTarget",
				"peer_devices":[{"replication-state":"Established","percent-in-sync":100}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Resyncing {
		t.Fatalf("connection-level replication-state must be ignored (peer-device is Established): %+v", s)
	}
	if s.ResyncPercent != 100 {
		t.Fatalf("in-sync volume must report 100, got %v", s.ResyncPercent)
	}
}

func TestStatusResyncing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		repl  string
		wantR bool
	}{
		{"established", "Established", false},
		{"absent", "", false},
		{"sync-target", "SyncTarget", true},
		{"sync-source", "SyncSource", true},
		{"paused", "PausedSyncT", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// replication-state is a peer-device field, nested under the
			// connection (verified against drbdsetup source).
			peerDevs := ""
			if tc.repl != "" {
				peerDevs = `,"peer_devices":[{"replication-state":"` + tc.repl + `"}]`
			}
			fe := &fakeExec{responses: map[string]string{
				cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
					"devices":[{"disk-state":"UpToDate"}],
					"connections":[{"connection-state":"Connected"` + peerDevs + `}]}]`,
			}}
			d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

			s, err := d.Status(t.Context(), volPvc1)
			if err != nil {
				t.Fatal(err)
			}
			if s.Resyncing != tc.wantR {
				t.Fatalf("replication-state %q: Resyncing = %v, want %v", tc.repl, s.Resyncing, tc.wantR)
			}
			if !s.PeerConnected[0] {
				t.Fatalf("a connected peer must stay connected: %+v", s)
			}
		})
	}
}

func TestStatusVerifying(t *testing.T) {
	for _, tc := range []struct {
		name       string
		repl       string
		wantVerify bool
		wantResync bool
	}{
		{"verify-source", "VerifyS", true, true},
		{"verify-target", "VerifyT", true, true},
		{"data-resync", "SyncTarget", false, true},
		{"established", "Established", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fe := &fakeExec{responses: map[string]string{
				cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
					"devices":[{"disk-state":"UpToDate"}],
					"connections":[{"connection-state":"Connected",
						"peer_devices":[{"replication-state":"` + tc.repl + `","out-of-sync":128}]}]}]`,
			}}
			d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

			s, err := d.Status(t.Context(), volPvc1)
			if err != nil {
				t.Fatal(err)
			}
			if s.Verifying != tc.wantVerify {
				t.Fatalf("replication-state %q: Verifying = %v, want %v", tc.repl, s.Verifying, tc.wantVerify)
			}
			if s.Resyncing != tc.wantResync {
				t.Fatalf("replication-state %q: Resyncing = %v, want %v", tc.repl, s.Resyncing, tc.wantResync)
			}
			if s.OutOfSyncKiB != 128 {
				t.Fatalf("out-of-sync must surface, got %d", s.OutOfSyncKiB)
			}
		})
	}
}
