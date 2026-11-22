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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/home-operations/miroir/internal/backend"
)

// disconnectStranded reports ErrBusy while a drbdsetup disconnect against
// any of name's peers (see disconnectPeers) is still stuck in the kernel.
func (d *Driver) disconnectStranded(name string) error {
	if d.Wedge == nil {
		return nil
	}
	prefix := backend.CommandLine("drbdsetup", "disconnect", name) + " "
	for _, line := range d.Wedge.Commands() {
		if strings.HasPrefix(line, prefix) {
			return fmt.Errorf("%s stranded in the kernel; deferring: %w", line, backend.ErrBusy)
		}
	}
	return nil
}

// SweepOrphans tears down kernel resources and rendered config files with
// no owning volume on this node — leftovers of an agent crash between up
// and down, which would otherwise hold backing devices open forever. Best
// effort per resource: errors are joined so one stuck resource cannot
// strand the rest. An unreadable kernel view aborts the whole sweep,
// files included — without it a live resource is indistinguishable from a
// stale file, and stripping its config is the state this sweep's own
// stuck-guard exists to prevent. The sweep runs once per agent start: an
// orphan skipped as wedged (or mid-detach, which wears the same
// signature) stays configured and is retried at the next restart.
func (d *Driver) SweepOrphans(ctx context.Context, owned func(name string) bool) error {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return err
	}
	var errs []error
	stuck := map[string]bool{}
	for _, res := range parsed {
		if owned(res.Name) {
			continue
		}
		// A wedged orphan's down can only hang 30s and strand another
		// unkillable process; the signature is already in
		// hand from this sweep's own parse, so skip without spawning one.
		if cause := d.wedgeCause(res); cause != nil {
			errs = append(errs, fmt.Errorf("orphan %s: %w", res.Name, cause))
			stuck[res.Name] = true
			continue
		}
		if err := d.downResource(ctx, res.Name, res.peerIDs()); err != nil {
			errs = append(errs, fmt.Errorf("down orphan %s: %w", res.Name, err))
			stuck[res.Name] = true
		}
	}
	entries, err := os.ReadDir(d.StateDir)
	if err != nil {
		if !os.IsNotExist(err) {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}
	for _, e := range entries {
		name, found := strings.CutSuffix(e.Name(), ".res")
		// A resource whose down failed is still configured in the kernel;
		// its rendered config stays visible to recovery tooling.
		if !found || owned(name) || stuck[name] {
			continue
		}
		if err := d.removeStateFiles(name); err != nil {
			errs = append(errs, err)
		}
		// The orphaned volume never comes back to Down on this node, so
		// its minor.assign entry would leak — and permanently burn a
		// minor — for the StateDir's lifetime.
		if err := d.releaseMinor(name); err != nil {
			errs = append(errs, fmt.Errorf("release minor of orphan %s: %w", name, err))
		}
	}
	// Assignment entries with no CR, no kernel resource, and no rendered
	// config are reclaim leftovers: RemoveConfig keeps the reservation
	// while the zombie minor lives, and after the reboot that cleared the
	// kernel they are pure leaks. Ordered after the file loop so a plain
	// config orphan is handled (and released) exactly once, above.
	inKernel := map[string]bool{}
	for _, res := range parsed {
		inKernel[res.Name] = true
	}
	if err := d.releaseOrphanedAssignments(owned, inKernel); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// releaseOrphanedAssignments drops minor.assign entries whose volume has
// vanished entirely — not owned, not in the kernel, no rendered config.
func (d *Driver) releaseOrphanedAssignments(owned func(name string) bool, inKernel map[string]bool) error {
	unlock, err := d.lockMinors()
	if err != nil {
		return err
	}
	defer unlock()
	assigned, err := d.readAssignments()
	if err != nil {
		return err
	}
	changed := false
	for name := range assigned {
		if owned(name) || inKernel[name] {
			continue
		}
		if _, err := os.Stat(d.path(name + ".res")); err == nil {
			continue
		}
		delete(assigned, name)
		changed = true
	}
	if !changed {
		return nil
	}
	return d.writeAssignments(assigned)
}

// DemoteAll force-demotes every Primary this node holds, releasing backing
// devices so the OS can tear down storage pools at shutdown. --force
// terminates pending I/O with errors instead of waiting for a metadata
// flush, which is what wedges a reboot in drbd_md_sync_page_io when the
// backing disk is failing. Best effort: errors are joined so one stuck
// resource cannot strand the rest.
func (d *Driver) DemoteAll(ctx context.Context) error {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, res := range parsed {
		if res.Role != rolePrimary {
			continue
		}
		// Never spawn a demote against a wedge fingerprint:
		// it can only hang. DownSecondaries reports these separately.
		if cause := d.wedgeCause(res); cause != nil {
			errs = append(errs, fmt.Errorf("%s: %w", res.Name, cause))
			continue
		}
		if _, err := d.adm(ctx, "secondary", "--force", res.Name); err != nil {
			errs = append(errs, fmt.Errorf("secondary --force %s: %w", res.Name, err))
		}
	}
	return errors.Join(errs...)
}

// DownSecondaries brings down every resource this node holds Secondary,
// releasing its backing device so the backend pool can export on shutdown.
// Primary (open) resources are skipped: drbdsetup down refuses an open
// device, and a local Primary is a leg a workload here still holds.
// Rendered config stays so the successor re-ups. Best effort: errors are
// joined so one stuck resource cannot strand the rest.
func (d *Driver) DownSecondaries(ctx context.Context) error {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, res := range parsed {
		if res.Role == rolePrimary {
			continue
		}
		if err := d.downSecondary(ctx, res.Name); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", res.Name, err))
		}
	}
	return errors.Join(errs...)
}

// downSecondary is one resource's share of DownSecondaries. The early
// signal path runs the sweep while the manager is still stopping, so a
// stage or reconcile may hold the resource; one that does is skipped with
// ErrBusy rather than waited for, so a single slow host command cannot
// spend the sweep's shared deadline on behalf of every later resource.
// The sweep's listing is stale by the time the lock is held: an apply may
// have added a peer whose resync the down must not meet connected, and a
// restart that gave up on its own down leaves a child stranded in the
// kernel. Both come from a re-read under the lock. Never spawn a down
// against a wedge fingerprint: it can only hang until the
// shutdown deadline and strand an unkillable process into the reboot.
func (d *Driver) downSecondary(ctx context.Context, name string) error {
	unlock, ok := d.tryLockResource(name)
	if !ok {
		return fmt.Errorf("held by another operation; skipping: %w", backend.ErrBusy)
	}
	defer unlock()
	// The runner starts a child before noticing cancellation, so an
	// exhausted budget is checked here rather than paid for in kill grace.
	if err := ctx.Err(); err != nil {
		return err
	}
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		return err
	}
	for _, res := range parsed {
		if res.Name != name || res.Role == rolePrimary {
			continue
		}
		if cause := d.wedgeCause(res); cause != nil {
			return cause
		}
		return d.downResource(ctx, name, res.peerIDs())
	}
	return nil
}

// downTimeout tightens RealExec's 2-minute bound for drbdsetup down. A
// device wedged mid-teardown (stuck Detaching, or a receiver thread that
// never exits under del_connection) leaves down in D-state, and teardown
// retries every ~10s — a 2-minute pin per attempt head-of-line-blocks the
// reconcile worker. 30s is generous: down on healthy hardware completes in
// milliseconds. The bound frees the worker, not the device — a D-state
// child ignores the SIGKILL and keeps its holds.
const downTimeout = 30 * time.Second

// disconnectTimeout bounds each best-effort disconnect in downResource,
// which otherwise rides RealExec's 2-minute bound — per peer, per teardown
// attempt — when the kernel resource is wedged. A healthy disconnect
// completes in milliseconds.
const disconnectTimeout = 10 * time.Second

// ErrWedged marks a resource the kernel can no longer tear down, so
// drbdsetup down blocks in uninterruptible sleep and the next attempt hits
// the same wall. Two fingerprints wear it: the device stuck Detaching with
// nothing left to disconnect (refcount underflow, LINBIT/drbd#137), and a
// down of ours still stranded in the kernel (ErrDownStranded, which wraps
// this). Callers must leave the fast retry loop — every extra down attempt
// can strand another unkillable process — and surface that only a node
// reboot clears it. Down escalates to this only on the second consecutive
// sighting of a fingerprint, so a detach still draining gets one retry
// cycle to finish first.
var ErrWedged = errors.New("resource wedged in kernel: node reboot required")

// ErrDownStranded is the ErrWedged fingerprint the kernel's status output
// cannot show: a drbdsetup down killed at its deadline whose task is still
// in uninterruptible sleep. drbd_adm_down sets the resource's
// DOWN_IN_PROGRESS flag before del_connection waits for the receiver
// thread to exit, and clears it only on return, so while the down is stuck
// every open of the minor fails with EAGAIN although status still reads
// healthy (Secondary, UpToDate, quorum). Only the stranded child itself
// identifies the state, via the node breaker's /proc view.
var ErrDownStranded = fmt.Errorf("drbdsetup down stranded in uninterruptible sleep (DOWN_IN_PROGRESS pinned): %w", ErrWedged)

// wedgeCause classifies one kernel status entry: nil when the resource is
// safe to act on, otherwise the ErrWedged fingerprint it wears.
func (d *Driver) wedgeCause(res jsonStatus) error {
	if res.wedgeSignature() {
		return ErrWedged
	}
	return d.downStranded(res.Name)
}

// downStranded reports ErrDownStranded when a drbdsetup down against name
// is still stuck in the kernel, nil otherwise (or with no breaker wired).
func (d *Driver) downStranded(name string) error {
	if d.Wedge == nil || !d.Wedge.StrandedCommand(backend.CommandLine("drbdsetup", "down", name)) {
		return nil
	}
	return ErrDownStranded
}

// disconnectPeers disconnects each peer connection ahead of a down or a
// force-detach. Disconnecting first avoids a kernel deadlock: drbdsetup
// down on a resource with an active resync connection can enter
// uninterruptible sleep negotiating teardown with the peer. Disconnects
// are best-effort (a StandAlone or unconfigured connection errors
// harmlessly) — except when one is killed at its deadline: then the
// connection may be half-torn and acting on the device would race exactly
// that teardown, so the caller's retry (which disconnects again) takes
// over. drbdsetup needs no config and takes peers by node id.
func (d *Driver) disconnectPeers(ctx context.Context, name string, peerIDs []int32) error {
	for _, id := range peerIDs {
		dcCtx, cancel := context.WithTimeout(ctx, disconnectTimeout)
		_, _ = d.Exec(dcCtx, "drbdsetup", "disconnect", name, strconv.Itoa(int(id)))
		expired := dcCtx.Err() != nil
		cancel()
		if expired {
			// ErrBusy: the caller's short retry re-disconnects; the state
			// clears on its own once the kernel finishes the teardown.
			return fmt.Errorf("disconnect %s peer %d killed after %s; deferring teardown: %w",
				name, id, disconnectTimeout, backend.ErrBusy)
		}
	}
	return nil
}

// downResource disconnects each peer connection, then downs the resource.
// drbdsetup down exits 0 for unknown names, keeping teardown idempotent.
// drbdsetup, not drbdadm: drbdadm refuses conflicting .res files, and
// teardown is how a conflict gets removed.
func (d *Driver) downResource(ctx context.Context, name string, peerIDs []int32) error {
	if err := d.disconnectPeers(ctx, name, peerIDs); err != nil {
		return err
	}
	downCtx, cancel := context.WithTimeout(ctx, downTimeout)
	defer cancel()
	if _, err := d.Exec(downCtx, "drbdsetup", "down", name); err != nil {
		return fmt.Errorf("down %s: %w", name, err)
	}
	return nil
}

// liveTeardownView reports the resource's DRBD peer node ids per the
// kernel's live view — the ids downResource must disconnect — and the
// wedge fingerprint the resource wears, if any (see wedgeCause). A
// stranded down counts even when the status lists no entry: the stuck
// child, not the status, is that fingerprint's evidence. A resource the
// kernel does not know has no peers; any other status failure is returned,
// since a down that skipped the disconnects could meet a live connection.
func (d *Driver) liveTeardownView(ctx context.Context, name string) (peerIDs []int32, wedged, err error) {
	wedged = d.downStranded(name)
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		if wedged != nil || isNoSuchResource(err) {
			return nil, wedged, nil
		}
		return nil, nil, err
	}
	for _, res := range parsed {
		if res.Name != name {
			continue
		}
		peerIDs = append(peerIDs, res.peerIDs()...)
		if wedged == nil && res.wedgeSignature() {
			wedged = ErrWedged
		}
	}
	return peerIDs, wedged, nil
}

// isNoSuchResource matches drbdsetup's answer for a resource that is not
// configured in the kernel.
func isNoSuchResource(err error) bool {
	return strings.Contains(err.Error(), "No such resource")
}

// markWedgeSighting records one sighting of a wedge fingerprint and
// reports whether an earlier consecutive sighting was already on record.
func (d *Driver) markWedgeSighting(name string) (again bool) {
	d.wedgeMu.Lock()
	defer d.wedgeMu.Unlock()
	if d.wedgeSeen == nil {
		d.wedgeSeen = map[string]bool{}
	}
	again = d.wedgeSeen[name]
	d.wedgeSeen[name] = true
	return again
}

func (d *Driver) clearWedgeSighting(name string) {
	d.wedgeMu.Lock()
	delete(d.wedgeSeen, name)
	d.wedgeMu.Unlock()
}

// Down stops the resource and removes its rendered state. Idempotent.
// A resource wearing a wedge fingerprint never gets another down spawned:
// the first sighting defers (the detach may still be draining), a second
// consecutive one returns the fingerprint's ErrWedged (wrapped).
func (d *Driver) Down(ctx context.Context, name string) error {
	unlock, err := d.lockResource(ctx, name)
	if err != nil {
		return fmt.Errorf("down %s: %w", name, err)
	}
	defer unlock()
	if _, err := os.Stat(d.path(name + ".res")); os.IsNotExist(err) {
		return nil // never configured here
	}
	peerIDs, wedged, err := d.liveTeardownView(ctx, name)
	if err != nil {
		return fmt.Errorf("down %s: %w", name, err)
	}
	if wedged != nil {
		if d.markWedgeSighting(name) {
			return fmt.Errorf("down %s: %w", name, wedged)
		}
		// ErrBusy: one short retry cycle for a drain to finish before the
		// signature escalates.
		return fmt.Errorf("down %s: detach in flight after a killed down; deferring: %w",
			name, backend.ErrBusy)
	}
	d.clearWedgeSighting(name)
	if err := d.downResource(ctx, name, peerIDs); err != nil {
		return err
	}
	if err := d.removeStateFiles(name); err != nil {
		return err
	}
	// Free the minor for reuse — the .res is gone, so scanUsedMinors no
	// longer covers it, and the assignment would otherwise leak forever.
	return d.releaseMinor(name)
}

// Restart drops and re-registers the resource's kernel state (down + up),
// keeping the rendered config and the minor assignment. It exists for one
// consumer: a filesystem freeze leaked onto an unmounted device pins the
// bdev's freeze count with no mountpoint left for FITHAW, and dropping the
// minor's bdev inode is the only remaining way to clear it.
// The disconnect costs a brief bitmap-based resync on reconnect. A
// resource wearing a teardown wedge fingerprint is refused outright — a
// down spawned against it can only hang (see ErrWedged); the caller's
// retry re-evaluates. The resource lock spans the disconnects, the down
// and the up: the reconciler's adjust, woken by the disconnects' own
// state-change events, must find the resource fully re-registered rather
// than reconnect a peer mid-teardown (see resLocks).
func (d *Driver) Restart(ctx context.Context, name string) error {
	unlock, err := d.lockResource(ctx, name)
	if err != nil {
		return fmt.Errorf("restart %s: %w", name, err)
	}
	defer unlock()
	peerIDs, wedged, err := d.liveTeardownView(ctx, name)
	if err != nil {
		return fmt.Errorf("restart %s: %w", name, err)
	}
	if wedged != nil {
		return fmt.Errorf("restart %s: refusing to spawn a down: %w", name, wedged)
	}
	if err := d.downResource(ctx, name, peerIDs); err != nil {
		return err
	}
	if _, err := d.adm(ctx, "up", name); err != nil {
		return fmt.Errorf("up %s: %w", name, err)
	}
	return nil
}

// ForceDetach releases the backing device from a resource whose down is
// permanently refused by an orphaned opener — a leaked freeze's dead
// superblock pinning open_cnt with no mountpoint left to thaw (issue
// #319). Down can never win that state, but force-detach is legal on an
// open device (dropping a failing disk under live I/O is its purpose),
// and the backing is all a deletion still needs back. The caller must
// keep the minor assignment (see RemoveConfig): the kernel minor stays
// pinned until a reboot, releasing the number would hand it to a new
// volume, and the startup orphan sweep reaps it once the reboot clears
// the state. Remaining peer connections are removed after the detach so
// the zombie resource releases its replication port. Already-detached
// resources still shed those connections, while not-in-kernel resources
// are no-ops so a retry after a failed backing delete converges. A resource
// wearing a teardown wedge fingerprint is refused like everywhere else —
// mid-detach is exactly the state a second detach would race.
func (d *Driver) ForceDetach(ctx context.Context, name string, minor int32) error {
	unlock, err := d.lockResource(ctx, name)
	if err != nil {
		return fmt.Errorf("force-detach %s: %w", name, err)
	}
	defer unlock()
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		return err
	}
	for _, res := range parsed {
		if res.Name != name {
			continue
		}
		if cause := d.wedgeCause(res); cause != nil {
			return fmt.Errorf("force-detach %s: %w", name, cause)
		}
		peerIDs := res.peerIDs()
		if err := d.disconnectPeers(ctx, name, peerIDs); err != nil {
			return err
		}
		if len(res.Devices) > 0 && res.Devices[0].DiskState != DiskDiskless {
			if _, err := d.Exec(ctx, "drbdsetup", "detach", strconv.Itoa(int(minor)), "--force"); err != nil {
				return fmt.Errorf("force-detach %s: %w", name, err)
			}
		}
		for _, id := range peerIDs {
			if _, err := d.Exec(ctx, "drbdsetup", "del-peer", name, strconv.Itoa(int(id))); err != nil {
				return fmt.Errorf("remove %s peer %d: %w", name, id, err)
			}
		}
		return nil
	}
	return nil
}
