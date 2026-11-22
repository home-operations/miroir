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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Day0GI derives the deterministic "day 0" generation identifier all
// replicas of a fresh volume stamp into their metadata: 16 hex chars (the
// first 8 bytes of sha256), low bit cleared — DRBD's low bit marks
// "primary writes happened" and a synthetic day0 means none have.
func Day0GI(name string) string {
	h := sha256.Sum256(fmt.Appendf(nil, "miroir-day0:%s/0", name))
	h[7] &^= 0x01
	return strings.ToUpper(hex.EncodeToString(h[:8]))
}

// IsWinner picks the birth-generation winner deterministically: the lowest
// node id among diskful peers. A diskless tie-breaker never holds data, so
// it must never win. Node ids are controller-assigned and stable, and every
// agent reads the same spec, so all nodes elect the same single winner
// without coordinating.
func IsWinner(r Resource) bool {
	lowest := int32(-1)
	var lowestNode string
	for _, p := range r.Peers {
		if p.Diskless {
			continue
		}
		if lowest == -1 || p.NodeID < lowest {
			lowest = p.NodeID
			lowestNode = p.Node
		}
	}
	return lowestNode == r.LocalNode
}

// InitialUUID creates a fresh volume's first data generation — DRBD's
// documented skip-initial-sync: with every leg attached Inconsistent at
// UUID_JUST_CREATED and all connections established, new-current-uuid
// --clear-bitmap mints one UUID and replicates it to every peer, flipping
// all legs UpToDate with zero resync. Valid because thin backings read
// zeros. Run on the winner (IsWinner) only, and only while the volume has
// never held data — the caller gates on both (see agent birthInitPending).
// One replicated UUID cannot birth-diverge, unlike per-node manufactured
// generations.
func (d *Driver) InitialUUID(ctx context.Context, name string) error {
	if _, err := d.adm(ctx, "new-current-uuid", "--clear-bitmap", name+"/0"); err != nil {
		return fmt.Errorf("new-current-uuid %s: %w", name, err)
	}
	return nil
}

// ForceFullSyncSource declares this leg's data the volume's generation —
// DRBD's documented bootstrap for a node that has the data while its
// peers do not: primary --force flips the Inconsistent disk UpToDate and
// mints a current UUID with full bitmaps, every just-created peer elects
// full SyncTarget, and the immediate demote hands the device back to
// auto-promote. Not new-current-uuid: without --clear-bitmap (and with
// --force-resync alike) drbdsetup refuses it on a connected resource
// ("(151) Need to be StandAlone"), and even minted StandAlone it leaves
// the disk Inconsistent with the resync parked on the source's own state
// (PausedSyncS, resync-suspended:dependency) — verified against DRBD 9.3
// on the QEMU e2e. It exists for one consumer: the seed leg of a restore
// that crossed the replication boundary (an unreplicated source's clone)
// holds real data under freshly created metadata, so neither the birth
// flow (data must not be declared identical to blank peers) nor metadata
// adoption (there was none to adopt) can produce its generation. The
// caller gates on the metadata being this agent's own fresh create (see
// agent restoreSeedPending) — forcing a leg with a live generation would
// fork history.
func (d *Driver) ForceFullSyncSource(ctx context.Context, name string) error {
	if _, err := d.adm(ctx, "primary", "--force", name); err != nil {
		return fmt.Errorf("primary --force %s: %w", name, err)
	}
	return d.Demote(ctx, name)
}
