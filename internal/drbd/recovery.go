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
	"fmt"
	"strconv"
)

// ResolveSplitBrain runs DRBD's documented split-brain recovery around the
// winner: every leg disconnects, then the winner (lowest diskful node id)
// and the diskless tie-breaker reconnect as survivors while every other
// diskful leg reconnects with --discard-my-data, becoming SyncTarget.
// Because the winner is derived the same way on every node, the agents
// converge without coordinating.
//
// --discard-my-data drops the loser leg's generation, so this is only valid
// on a volume that never held data; the caller gates on that (see
// planSplitBrain in internal/agent).
func (d *Driver) ResolveSplitBrain(ctx context.Context, r Resource) error {
	unlock, err := d.lockLiveResource(ctx, r.Name)
	if err != nil {
		return fmt.Errorf("resolve split-brain %s: %w", r.Name, err)
	}
	defer unlock()
	// Disconnect first, on every role. A plain connect aborts (exit 10,
	// "Device has a net-config") on any peer that already has one, and after a
	// split-brain the local node holds a net-config toward at least one peer
	// (StandAlone or Connecting); --discard-my-data is likewise only honored on
	// a connect out of StandAlone. Clearing all connections first makes the
	// connect below well-defined. A resource with nothing to disconnect errors
	// here harmlessly, so the result is ignored.
	_, _ = d.adm(ctx, "disconnect", r.Name)

	if r.LocalDiskless || IsWinner(r) {
		if _, err := d.adm(ctx, "connect", r.Name); err != nil {
			return fmt.Errorf("connect %s: %w", r.Name, err)
		}
		return nil
	}
	if _, err := d.adm(ctx, "connect", "--discard-my-data", r.Name); err != nil {
		return fmt.Errorf("connect --discard-my-data %s: %w", r.Name, err)
	}
	return nil
}

// CyclePeerConnection disconnects and reconnects one peer so DRBD re-runs
// that connection's sync handshake — the only trigger for the
// missed-end-of-resync rescue, which is gated on a fresh connect and is
// what resolves a stranded bitmap: equal current UUIDs
// discard it outright, divergent ones start the resync the bits called
// for. drbdsetup on both legs: it takes the peer by node id with no
// config parse, and unlike drbdadm's disconnect it keeps the peer and
// path objects, so the connect below re-activates them in place. Should
// the connect still fail, the next pass's adjust reconnects the parked
// leg.
func (d *Driver) CyclePeerConnection(ctx context.Context, name string, peerID int32) error {
	unlock, err := d.lockLiveResource(ctx, name)
	if err != nil {
		return fmt.Errorf("cycle %s peer %d: %w", name, peerID, err)
	}
	defer unlock()
	id := strconv.Itoa(int(peerID))
	if _, err := d.Exec(ctx, "drbdsetup", "disconnect", name, id); err != nil {
		return fmt.Errorf("disconnect %s peer %s: %w", name, id, err)
	}
	if _, err := d.Exec(ctx, "drbdsetup", "connect", name, id); err != nil {
		return fmt.Errorf("connect %s peer %s: %w", name, id, err)
	}
	return nil
}

// CycleStalledTargetConnection is CyclePeerConnection for an Inconsistent
// Secondary leg (Status.UnsettledResyncPeers), whose graceful disconnect
// the kernel may refuse: DRBD will not drop a device's last access to
// UpToDate data while a read-only opener holds it ("Need access to
// UpToDate data"), and the hang this heals left exactly such an opener
// behind in issue #576 (open:yes, blocked:upper). That opener is never a
// staged consumer — miroir refuses to stage an Inconsistent leg, and a
// writable open would have promoted — so a refused graceful disconnect
// is retried --force, trading the reader's remote data path (its reads
// fail or stall until the reconnect) for the volume's redundancy. The
// leg is re-read first: the same refusal meets a leg promoted since the
// candidate was chosen, whose consumer must keep its path, and forcing
// skips every kernel-side check. No data moves until the reconnect
// handshake decides.
func (d *Driver) CycleStalledTargetConnection(ctx context.Context, name string, peerID int32) error {
	unlock, err := d.lockLiveResource(ctx, name)
	if err != nil {
		return fmt.Errorf("cycle %s peer %d: %w", name, peerID, err)
	}
	defer unlock()
	id := strconv.Itoa(int(peerID))
	if _, err := d.Exec(ctx, "drbdsetup", "disconnect", name, id); err != nil {
		st, serr := d.Status(ctx, name)
		if serr != nil {
			return fmt.Errorf("disconnect %s peer %s: %w (re-read failed: %w)", name, id, err, serr)
		}
		if st.Primary || st.DiskState != DiskInconsistent {
			return fmt.Errorf("disconnect %s peer %s: %w (leg no longer an Inconsistent Secondary; not forcing)", name, id, err)
		}
		if _, ferr := d.Exec(ctx, "drbdsetup", "disconnect", name, id, "--force"); ferr != nil {
			return fmt.Errorf("disconnect %s peer %s: %w; forced: %w", name, id, err, ferr)
		}
	}
	if _, err := d.Exec(ctx, "drbdsetup", "connect", name, id); err != nil {
		return fmt.Errorf("connect %s peer %s: %w", name, id, err)
	}
	return nil
}
