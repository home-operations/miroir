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
	"context"
	"encoding/json"
	"fmt"
)

// listStatus runs `drbdsetup status --json [name]` and parses the result —
// the one fetch every kernel-view consumer shares.
func (d *Driver) listStatus(ctx context.Context, name ...string) ([]jsonStatus, error) {
	args := append([]string{"status", "--json"}, name...)
	out, err := d.Exec(ctx, "drbdsetup", args...)
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	var parsed []jsonStatus
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	return parsed, nil
}

// DiskUpToDate is the disk state of a replica holding current data.
const DiskUpToDate = "UpToDate"

// DiskInconsistent is the disk state of a replica with no usable data
// generation yet: freshly created metadata before the birth generation, or
// a sync target mid-resync. Never promotable.
const DiskInconsistent = "Inconsistent"

// DiskDiskless is the disk state of a replica with no attached backing
// device — a quorum-only tie-breaker, or a diskful leg DRBD detached
// after an I/O error (on-io-error detach). Do NOT use it to infer
// tie-breaker-ness (that is spec-driven, see Replica.Diskless); it is
// only meaningful for observing whether this leg holds a backing device
// right now — a detach on a leg the spec says is diskful, or the blank
// interval of an auto-diskful conversion (see probeMetadata).
const DiskDiskless = "Diskless"

// diskDetaching is the transient disk state while the kernel drains a
// detach. Seen at teardown entry (with the connections already gone) it
// means a previous down was killed mid-flight — see ErrWedged.
const diskDetaching = "Detaching"

// diskConsistent is the peer-disk state of data that is consistent but not
// yet trusted as current. On a connected peer it is transient — the sync
// handshake resolves it to UpToDate or Outdated — so a peer parked
// Consistent over an up connection with out-of-sync bits is the
// stranded-bitmap signature (see Status.StuckResyncPeers).
const diskConsistent = "Consistent"

// connStandAlone is the connection state of a peer DRBD gave up
// reconnecting to. drbdsetup disconnect refuses it (-9), so a StandAlone
// entry can linger in status through every teardown attempt.
const connStandAlone = "StandAlone"

// connConnected is the fully established connection state.
const connConnected = "Connected"

// rolePrimary is the DRBD role of a node that holds the device open.
const rolePrimary = "Primary"

// Peer-device replication states the status parser distinguishes. Anything
// other than Established/Off means an active resync or verify; VerifyS/VerifyT
// narrow that to an online verify, and WFBitMapS is the source side of a
// bitmap exchange — normally milliseconds, but parked forever when the peer
// discarded the exchange (see Status.StuckResyncPeers).
const (
	replEstablished = "Established"
	replOff         = "Off"
	replVerifyS     = "VerifyS"
	replVerifyT     = "VerifyT"
	replWFBitMapS   = "WFBitMapS"
	replWFBitMapT   = "WFBitMapT"
)

// Status reports this node's view of one resource.
type Status struct {
	// DiskState: UpToDate, Inconsistent, Outdated, Consistent, Diskless…
	DiskState string
	// Primary is true when this node holds the device open (auto-promote).
	Primary bool
	// PeerPrimary is true when a connected peer holds the device open —
	// that peer is where writes originate, hence where a write barrier
	// must be raised.
	PeerPrimary bool
	// Suspended is true while suspend-io holds this node's write barrier.
	Suspended bool
	// PeerConnected maps each peer's DRBD node id to whether its
	// connection is established. There is deliberately no all-peers
	// aggregate: it would couple health decisions to a diskless
	// tie-breaker's link state (the exact bug #78 removed) — filter by
	// the spec's diskful node ids instead (see agent.diskfulPeersConnected).
	PeerConnected map[int32]bool
	// PeerDiskState maps each peer's DRBD node id to that peer's disk
	// state as this node sees it (UpToDate, Inconsistent, Diskless,
	// DUnknown before the handshake reports it…). The birth-generation
	// trigger keys on every diskful leg reading Inconsistent.
	PeerDiskState map[int32]string
	// SplitBrain is true when a connection is StandAlone — DRBD refused
	// to reconnect after detecting divergent data.
	SplitBrain bool
	// Resyncing is true while a peer connection is mid-resync; drbdadm
	// resize is refused in this window. An online verify also sets it (the
	// replication-state leaves Established), so it doubles as "a verify is
	// already running" — see Verifying for the narrower signal.
	Resyncing bool
	// Verifying is true while a peer connection is mid online-verify
	// (replication-state VerifyS/VerifyT). Distinguishes a verify pass from
	// a data resync, both of which set Resyncing.
	Verifying bool
	// ResyncPercent is the least-synced diskful peer's percent-in-sync
	// while resyncing; 100 when fully in sync (nothing to catch up).
	ResyncPercent float64
	// Quorum is the device quorum flag: false while a freeze-policy
	// volume has lost quorum and DRBD is suspending its IO. Always true
	// when quorum is off (last-man-standing).
	Quorum bool
	// OutOfSyncKiB is the largest per-peer out-of-sync amount (KiB, as
	// drbdsetup reports it) — the data-loss exposure if the healthiest
	// peer is lost. Grows while a peer is down with no resync running.
	OutOfSyncKiB int64
	// StuckResyncPeers holds the DRBD node ids of peers wearing the
	// stranded-bitmap signature: connection Connected, replication
	// Established or WFBitMapS, peer-disk Consistent, out-of-sync above
	// zero. DRBD set a bitmap slot toward the peer and then the exchange
	// died without starting the resync (a stale after-unstable handshake,
	// issue #390): either a second after-unstable pass collapsed it back
	// to Established (#390's original shape), or that pass never ran and
	// the leg parked in WFBitMapS waiting for a bitmap reply the peer
	// already discarded. Nothing in-kernel re-examines
	// either while the connection stays up. Only the leg holding the
	// bitmap sees it — the peer still reads this node as UpToDate — so at
	// most one node acts on it. Nil when no peer is stuck.
	StuckResyncPeers map[int32]bool
	// StaleBitmapPeers holds the DRBD node ids of peers this leg carries
	// a one-sided out-of-sync bitmap toward while everything reads
	// healthy: connection Connected, replication Established, peer-disk
	// UpToDate, out-of-sync above zero. Bits stranded by a bitmap clear
	// the kernel refused mid peer-teardown ("bitmap locked") never drain
	// on their own — DRBD only reconciles a dirty bitmap on a connection
	// transition. The same kernel shape is also a verify
	// finding, which must stay manual — the agent gates on the recorded
	// verify outcome before acting. Any pair but Primary/Primary
	// qualifies — a Secondary leg toward the Primary, the Primary
	// toward a Secondary, or a Secondary toward another
	// Secondary: the re-handshake takes its direction from
	// generation UUIDs, not roles, and only dual-primary leaves no
	// settled source. Nil when none.
	StaleBitmapPeers map[int32]bool
	// UnsettledResyncPeers holds the DRBD node ids of UpToDate peers this
	// Inconsistent Secondary leg sits connected to with nothing to sync
	// and no resync starting: replication WFBitMapT or Established, zero
	// out-of-sync bits, no resync in flight on any leg. DRBD 9.3.4 can
	// end a resync on different paths at its two ends when a diskless
	// Primary is writing (issue #576, LINBIT/drbd#148): the source
	// finishes and settles Established while this target never leaves
	// WFBitMapT, or settles Established with its disk still
	// Inconsistent, and nothing re-examines the pair while the
	// connection stays up. Only this leg can act: a graceful disconnect
	// is a cluster-wide transaction, and this leg vetoes the source's
	// while a read-only opener pins it — the state the hang leaves it
	// in. The same shape is also the ordinary end of a resync run while
	// this leg could not see the Primary (DRBD keeps an unstable target
	// Inconsistent and re-runs the handshake once the Primary is back in
	// view), so the agent gates on that before acting. Nil when none.
	UnsettledResyncPeers map[int32]bool
}

// drbdsetup status --json shapes (the fields miroir reads).
type jsonStatus struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	SuspendedUser bool   `json:"suspended-user"`
	Devices       []struct {
		Minor     int32  `json:"minor"`
		DiskState string `json:"disk-state"`
		Quorum    bool   `json:"quorum"`
	} `json:"devices"`
	Connections []struct {
		PeerNodeID      int32  `json:"peer-node-id"`
		ConnectionState string `json:"connection-state"`
		PeerRole        string `json:"peer-role"`
		// replication-state and percent-in-sync are per peer-device,
		// nested under the connection — NOT connection-level fields
		// (verified against drbd-utils user/v9/drbdsetup.c).
		PeerDevices []jsonPeerDevice `json:"peer_devices"`
	} `json:"connections"`
}

type jsonPeerDevice struct {
	ReplicationState string  `json:"replication-state"`
	PeerDiskState    string  `json:"peer-disk-state"`
	PercentInSync    float64 `json:"percent-in-sync"`
	OutOfSyncKiB     int64   `json:"out-of-sync"`
}

// unsettledResyncPeers computes Status.UnsettledResyncPeers. A resync in
// flight on any leg disqualifies the whole entry: a third diskful peer
// serving this one drains the bits toward the others through
// peers-in-sync, so an Established UpToDate peer with zero bits is then
// that resync's tail, and its end flips this disk UpToDate by itself.
func (s jsonStatus) unsettledResyncPeers() map[int32]bool {
	if len(s.Devices) == 0 || s.Devices[0].DiskState != DiskInconsistent || s.Role == rolePrimary {
		return nil
	}
	var peers map[int32]bool
	for _, c := range s.Connections {
		for _, pd := range c.PeerDevices {
			switch pd.ReplicationState {
			case replEstablished, replWFBitMapT:
			case replOff, "":
				continue
			default:
				return nil
			}
			if c.ConnectionState == connConnected && pd.PeerDiskState == DiskUpToDate && pd.OutOfSyncKiB == 0 {
				if peers == nil {
					peers = map[int32]bool{}
				}
				peers[c.PeerNodeID] = true
			}
		}
	}
	return peers
}

// wedgeSignature reports the kernel view of a teardown that cannot make
// progress: device stuck Detaching while every remaining connection is
// one disconnect cannot act on (StandAlone) or already gone. A healthy
// detach completes in milliseconds once the peers are out of the way, and
// nothing but teardown detaches, so this state means a previous down was
// killed mid-flight and never completed — a drain still finishing, or the
// refcount underflow of LINBIT/drbd#137 (see ErrWedged).
func (s jsonStatus) wedgeSignature() bool {
	if len(s.Devices) == 0 || s.Devices[0].DiskState != diskDetaching {
		return false
	}
	for _, c := range s.Connections {
		if c.ConnectionState != connStandAlone {
			return false
		}
	}
	return true
}

// peerIDs lists the entry's connection peer node ids — the handles
// drbdsetup disconnect takes.
func (s jsonStatus) peerIDs() []int32 {
	ids := make([]int32, 0, len(s.Connections))
	for _, c := range s.Connections {
		ids = append(ids, c.PeerNodeID)
	}
	return ids
}

// Status parses `drbdsetup status --json <res>`.
func (d *Driver) Status(ctx context.Context, name string) (Status, error) {
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		return Status{}, fmt.Errorf("%s: %w", name, err)
	}
	if len(parsed) == 0 {
		return Status{}, fmt.Errorf("resource %s not in status output", name)
	}
	res := parsed[0]

	s := Status{
		Primary:       res.Role == rolePrimary,
		Suspended:     res.SuspendedUser,
		PeerConnected: make(map[int32]bool, len(res.Connections)),
		PeerDiskState: make(map[int32]string, len(res.Connections)),
		ResyncPercent: 100,
	}
	if len(res.Devices) > 0 {
		s.DiskState = res.Devices[0].DiskState
		s.Quorum = res.Devices[0].Quorum
	}
	for _, c := range res.Connections {
		if c.PeerRole == rolePrimary {
			s.PeerPrimary = true
		}
		// replication-state / percent-in-sync live per peer-device.
		// Anything other than Established/Off is active resync/verify;
		// track the least-synced leg for progress reporting.
		for _, pd := range c.PeerDevices {
			if rs := pd.ReplicationState; rs != "" && rs != replEstablished && rs != replOff {
				s.Resyncing = true
				if rs == replVerifyS || rs == replVerifyT {
					s.Verifying = true
				}
				if pd.PercentInSync < s.ResyncPercent {
					s.ResyncPercent = pd.PercentInSync
				}
			}
			s.OutOfSyncKiB = max(s.OutOfSyncKiB, pd.OutOfSyncKiB)
			// Out-of-sync bits toward a connected peer with no resync
			// running and the peer-disk parked Consistent: a bitmap DRBD
			// armed and abandoned. The race's terminal shape
			// is a timing lottery — Established when a second
			// after-unstable pass collapsed the exchange, WFBitMapS when
			// it never ran and the peer discarded the bitmap reply (issue
			// #397) — so both count. Every other condition earns its
			// keep: verify findings leave the peer-disk UpToDate, a live
			// WFBitMapS completes in milliseconds (the reconciler's
			// confirmation window filters it), and WFBitMapT stays
			// excluded because the target side never wears the
			// Consistent peer-disk clamp.
			if c.ConnectionState == connConnected &&
				(pd.ReplicationState == replEstablished || pd.ReplicationState == replWFBitMapS) &&
				pd.PeerDiskState == diskConsistent && pd.OutOfSyncKiB > 0 {
				if s.StuckResyncPeers == nil {
					s.StuckResyncPeers = map[int32]bool{}
				}
				s.StuckResyncPeers[c.PeerNodeID] = true
			}
			// A one-sided bitmap toward a healthy peer with no resync
			// running: stale bits from a refused clear (issue #389, held
			// by a Secondary toward the Primary; issue #469, held by the
			// Primary toward a Secondary; issue #497, held by a Secondary
			// toward another Secondary after a connectivity blip) — or a
			// verify finding, which the agent's gates keep manual. The
			// re-handshake settles its direction from generation UUIDs,
			// not roles: equal generations discard the bits, divergent
			// ones sync from the newer history. Only a Primary/Primary
			// pair stays excluded — dual-primary has no settled source.
			if c.ConnectionState == connConnected && (c.PeerRole != rolePrimary || !s.Primary) &&
				pd.ReplicationState == replEstablished &&
				pd.PeerDiskState == DiskUpToDate && pd.OutOfSyncKiB > 0 {
				if s.StaleBitmapPeers == nil {
					s.StaleBitmapPeers = map[int32]bool{}
				}
				s.StaleBitmapPeers[c.PeerNodeID] = true
			}
		}
		// Single-volume resources: the first peer-device carries the peer's
		// disk state.
		if len(c.PeerDevices) > 0 {
			s.PeerDiskState[c.PeerNodeID] = c.PeerDevices[0].PeerDiskState
		}
		s.PeerConnected[c.PeerNodeID] = c.ConnectionState == connConnected
		if c.ConnectionState == connStandAlone {
			s.SplitBrain = true
		}
	}
	s.UnsettledResyncPeers = res.unsettledResyncPeers()
	return s, nil
}
