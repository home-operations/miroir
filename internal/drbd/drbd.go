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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/home-operations/miroir/internal/backend"
)

// Driver realizes DRBD resources on one node by rendering config into
// StateDir (bind-mounted from a hostPath so rendered state survives pod
// restarts) and driving drbdadm/drbdmeta against it.
type Driver struct {
	// StateDir is where .res files and marker files live; it is
	// drbdadm's include path (/etc/drbd.d) inside the agent container.
	StateDir string
	Exec     backend.Exec
	// Mknod creates device nodes; injectable because the real call needs
	// CAP_MKNOD. Nil means unix.Mknod.
	Mknod func(path string, mode uint32, dev int) error
	// Wedge is the node-scoped breaker Exec reports stranded children to;
	// the driver asks it whether a drbdsetup down against a resource is
	// still stuck in the kernel (see ErrDownStranded). Nil disables that
	// check, leaving the status-derived signature alone.
	Wedge interface {
		StrandedCommand(line string) bool
		Commands() []string
	}

	// wedgeSeen stamps resources whose teardown wore a wedge fingerprint
	// once (see ErrWedged). Down escalates only on the second consecutive
	// sighting: a killed down whose detach is still draining wears the
	// same signature briefly, and a premature ErrWedged pages "reboot
	// required" for a state that resolves itself.
	wedgeMu   sync.Mutex
	wedgeSeen map[string]bool

	// resLocks serialises, per resource, the operations that change its
	// connections or kernel registration (see lockResource) across every
	// caller sharing the Driver. The CSI node service and the volume
	// reconciler hold the same Driver but nothing else orders them: a
	// Restart's disconnects wake the reconciler through events2, and an
	// adjust landing between those disconnects and the down reconnects a
	// peer the down is tearing down. del_connection then waits forever on
	// a receiver thread the connect restarted, stranding drbdsetup down
	// in uninterruptible sleep. Each entry is a one-slot
	// semaphore rather than a mutex so a waiter can give up with its
	// context.
	resMu    sync.Mutex
	resLocks map[string]chan struct{}
}

// lockResource takes name's lock and returns its release, or the context's
// error if that expires first: a cancelled stage or reconcile must not
// start a disconnect/down sequence once nobody is waiting for it. Held
// for the whole sequence of an up, adjust, down, detach, connect or
// disconnect, so a teardown's disconnect-then-down can never interleave
// with a reconnect. Role changes, suspend/resume, resize and verify stay
// outside it: none of them (re)connects a peer. Entries are never
// dropped: one slot per resource name the node has seen is negligible,
// and dropping one under a waiter would split the lock.
func (d *Driver) lockResource(ctx context.Context, name string) (unlock func(), err error) {
	d.resMu.Lock()
	if d.resLocks == nil {
		d.resLocks = map[string]chan struct{}{}
	}
	slot, ok := d.resLocks[name]
	if !ok {
		slot = make(chan struct{}, 1)
		d.resLocks[name] = slot
	}
	d.resMu.Unlock()
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("lock %s: %w", name, ctx.Err())
	}
	// A free slot and an expired context are both ready; select picks
	// either, so the cancellation is re-checked after acquisition.
	if err := ctx.Err(); err != nil {
		<-slot
		return nil, fmt.Errorf("lock %s: %w", name, err)
	}
	return func() { <-slot }, nil
}

// tryLockResource is lockResource without the wait: ok is false when
// another operation holds name. For a sweep over every resource under
// one shared deadline, a held resource is one someone is actively
// working on, and queueing behind it would spend the budget the other
// resources need.
func (d *Driver) tryLockResource(name string) (unlock func(), ok bool) {
	d.resMu.Lock()
	if d.resLocks == nil {
		d.resLocks = map[string]chan struct{}{}
	}
	slot, found := d.resLocks[name]
	if !found {
		slot = make(chan struct{}, 1)
		d.resLocks[name] = slot
	}
	d.resMu.Unlock()
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, true
	default:
		return nil, false
	}
}

// lockLiveResource is lockResource for the operations that connect peers
// (adjust, connect). A teardown child of ours still stranded in the
// kernel outlives the lock its caller held — the runner gives up on the
// child at its deadline — and a connect against that resource restarts
// exactly the receiver thread a down is waiting on, or re-enters a
// connection a disconnect is still tearing down, so those operations are
// refused until the child drains: a stranded down is the ErrDownStranded
// wedge fingerprint, a stranded disconnect the same ErrBusy deferral its
// own caller got. Down and Restart classify the down fingerprint
// themselves (see liveTeardownView).
func (d *Driver) lockLiveResource(ctx context.Context, name string) (unlock func(), err error) {
	unlock, err = d.lockResource(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := d.downStranded(name); err != nil {
		unlock()
		return nil, err
	}
	if err := d.disconnectStranded(name); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

// Apply converges the kernel state for one resource: write config when it
// changed, create metadata once (left at UUID_JUST_CREATED — see
// ensureMetadata for how the birth generation lands), bring the resource
// up / adjust.
func (d *Driver) Apply(ctx context.Context, r Resource) error {
	unlock, err := d.lockLiveResource(ctx, r.Name)
	if err != nil {
		return fmt.Errorf("apply %s: %w", r.Name, err)
	}
	defer unlock()
	// The peers adjust is about to drop, recorded before the kernel forgets
	// them so their metadata slots can be cleared afterwards (see
	// forgetRemovedPeers). The kernel view rather than the previous .res
	// is the source, so a failed adjust retries on the next pass; the
	// marker keeps the set across a failed forget, when the kernel no
	// longer knows the peer either.
	removed, err := d.removedPeers(ctx, r)
	if err != nil {
		return err
	}
	if err := d.writePendingForgets(r.Name, removed); err != nil {
		return err
	}
	if err := d.writeConfig(r); err != nil {
		return err
	}

	// A diskless tie-breaker has no backing device — no metadata, no
	// device-node, no marker. It only needs the .res rendered (writeConfig
	// above) and drbdadm up/adjust so DRBD joins the resource for quorum.
	// A SkipDiskAttach leg is latched failed: leave its backing disk alone
	// (no create-md on a disk DRBD dropped on I/O error). The disk phase is
	// skipped below, so nothing reads or writes the failing device.
	if !r.LocalDiskless && !r.SkipDiskAttach {
		if err := d.ensureMarkedMetadata(ctx, r); err != nil {
			return err
		}
	}

	// adjust is idempotent: up when down, reconfigure on change, no-op
	// otherwise. A half-torn kernel slot can answer "Unknown resource"
	// to adjust but accept a plain up — fall back rather than loop.
	// --skip-disk reconciles net/connection state but leaves the disk
	// detached (drbdadm_adjust.c drops the ADJUST_DISK phase), so a latched
	// leg is not re-attached. It is always up (Diskless is an up-resource
	// state), so the Unknown-resource up fallback below is unreachable.
	adjustArgs := []string{"adjust", r.Name}
	if r.SkipDiskAttach {
		adjustArgs = []string{"adjust", "--skip-disk", r.Name}
	}
	if _, err := d.adm(ctx, adjustArgs...); err != nil {
		// A stale .md-created marker surviving a backing-disk replacement
		// (data disk swapped, /etc/drbd.d intact) makes ensureMarkedMetadata
		// skip create-md, so adjust attaches a blank device and fails "no
		// valid meta-data". Drop the marker and re-run: probeMetadata sees
		// no metadata and just-created metadata makes this a full SyncTarget.
		// Unreachable with --skip-disk (no attach → no metadata read).
		if !r.LocalDiskless && !r.SkipDiskAttach && isMissingMetadata(err) {
			if rmErr := os.Remove(d.path(r.Name + ".md-created")); rmErr != nil && !os.IsNotExist(rmErr) {
				return rmErr
			}
			if mdErr := d.ensureMarkedMetadata(ctx, r); mdErr != nil {
				return mdErr
			}
			if _, err = d.adm(ctx, "adjust", r.Name); err == nil {
				return d.finishApply(ctx, r, removed)
			}
		}
		if !strings.Contains(err.Error(), "Unknown resource") {
			return fmt.Errorf("adjust %s: %w", r.Name, err)
		}
		if _, err := d.adm(ctx, "up", r.Name); err != nil {
			return fmt.Errorf("up %s (after adjust fallback): %w", r.Name, err)
		}
	}

	return d.finishApply(ctx, r, removed)
}

// finishApply is the tail of Apply once adjust has succeeded.
func (d *Driver) finishApply(ctx context.Context, r Resource, removed []int32) error {
	if err := d.forgetRemovedPeers(ctx, r, removed); err != nil {
		return err
	}
	// No udev runs on Talos, so DRBD's rules never create the device
	// node. Without it, the first open(2) would create a regular file
	// under /dev and mkfs would silently write into tmpfs.
	return d.ensureDeviceNode(r.Minor)
}

// pendingForgetSuffix names the marker listing node-ids whose forget-peer
// is still owed, one per line. It is written before adjust drops them from
// the kernel and removed once every forget succeeds: after a failed
// forget the kernel view no longer knows the peer, so the marker is the
// only record that a slot still needs clearing.
const pendingForgetSuffix = ".forget-pending"

// removedPeers is the set of node-ids the rendered config no longer lists
// but that still need a forget-peer: the kernel's current peers plus any
// left pending by an earlier failed forget. A node-id back in the config
// (a consumer returning to the same node) drops out of the set; it is
// live again and gets re-snapshotted when it leaves. A resource that is
// not up answers "No such resource", which means the kernel holds no
// peers; any other status failure aborts the apply, since going on would
// drop peers with no record of them.
func (d *Driver) removedPeers(ctx context.Context, r Resource) ([]int32, error) {
	pending, err := d.readPendingForgets(r.Name)
	if err != nil {
		return nil, err
	}
	parsed, err := d.listStatus(ctx, r.Name)
	if err != nil && !isNoSuchResource(err) {
		return nil, fmt.Errorf("apply %s: %w", r.Name, err)
	}
	for _, res := range parsed {
		if res.Name == r.Name {
			pending = append(pending, res.peerIDs()...)
		}
	}
	removed := slices.DeleteFunc(pending, func(id int32) bool {
		return slices.ContainsFunc(r.Peers, func(p Peer) bool { return p.NodeID == id })
	})
	slices.Sort(removed)
	return slices.Compact(removed), nil
}

func (d *Driver) readPendingForgets(name string) ([]int32, error) {
	data, err := os.ReadFile(d.path(name + pendingForgetSuffix))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []int32
	for field := range strings.FieldsSeq(string(data)) {
		id, err := strconv.ParseInt(field, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("pending forgets %s: %w", name, err)
		}
		ids = append(ids, int32(id))
	}
	return ids, nil
}

// writePendingForgets records ids as owed forgets; an empty set removes
// the marker.
func (d *Driver) writePendingForgets(name string, ids []int32) error {
	path := d.path(name + pendingForgetSuffix)
	if len(ids) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b, "%d\n", id)
	}
	if err := os.MkdirAll(d.StateDir, 0o750); err != nil {
		return err
	}
	return writeFileAtomic(path, []byte(b.String()), 0o640)
}

// forgetRemovedPeers clears the metadata slot of every peer adjust
// dropped from the resource. del-peer leaves MDF_NODE_EXISTS behind in
// each diskful leg's metadata, and DRBD's quorum code counts an
// unconfigured slot wearing it as a missing diskless tiebreaker: on a
// 2-replica freeze volume that phantom doubles the tiebreaker electorate,
// so the next single diskful loss drops quorum with the real tiebreaker
// still connected. Only forget-peer clears the slot. It
// requires the peer to be unconfigured, hence after adjust, and is a
// no-op on a diskless leg, which has no metadata to hold the flag. The
// pending marker is cleared only once every forget has succeeded.
func (d *Driver) forgetRemovedPeers(ctx context.Context, r Resource, removed []int32) error {
	for _, id := range removed {
		if _, err := d.Exec(ctx, "drbdsetup", "forget-peer", r.Name, strconv.Itoa(int(id))); err != nil {
			return fmt.Errorf("forget-peer %s %d: %w", r.Name, id, err)
		}
	}
	return d.writePendingForgets(r.Name, nil)
}

// RemoveConfig removes the resource's rendered config and markers while
// leaving its minor.assign reservation in place. It exists for the
// orphaned-hold reclaim: the zombie kernel minor outlives its volume
// until a reboot, and a rendered config left behind holds the volume's
// DRBD port hostage — the controller re-allocates ports from CRs alone,
// and drbdadm refuses to parse a config directory whose resources share
// an endpoint, wedging the next replicated volume handed the recycled
// port on this node (caught by the e2e). The minor reservation must
// outlive the config (a fresh volume handed the zombie's minor could
// never register it); the startup orphan sweep releases it once a reboot
// clears the kernel state.
func (d *Driver) RemoveConfig(name string) error {
	return d.removeStateFiles(name)
}

// Demote releases an explicitly held Primary role back to auto-promote.
// Used by ForceFullSyncSource and by its crash recovery (a mint whose
// demote never ran); a device a consumer holds open refuses harmlessly.
func (d *Driver) Demote(ctx context.Context, name string) error {
	if _, err := d.adm(ctx, "secondary", name); err != nil {
		return fmt.Errorf("secondary %s: %w", name, err)
	}
	return nil
}

// DiscardGranularity probes the backing device's discard granularity
// (lsblk DISC-GRAN, bytes), clamped to [4096, 1MiB] — DRBD's sane range
// for rs-discard-granularity. 0 means the device does not support
// discards and nothing should be rendered.
func (d *Driver) DiscardGranularity(ctx context.Context, dev string) (int64, error) {
	out, err := d.Exec(ctx, "lsblk", "-bndo", "DISC-GRAN", dev)
	if err != nil {
		return 0, fmt.Errorf("probe discard granularity of %s: %w", dev, err)
	}
	gran, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse discard granularity of %s (%q): %w", dev, out, err)
	}
	if gran <= 0 {
		return 0, nil
	}
	return min(max(gran, 4096), 1<<20), nil
}

// UserSuspended lists resources whose IO is frozen by suspend-io. The
// kernel is the authority here: a crash between suspend-io and the status
// patch leaves a frozen device that no snapshot status records.
func (d *Driver) UserSuspended(ctx context.Context) ([]string, error) {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, r := range parsed {
		if r.SuspendedUser {
			names = append(names, r.Name)
		}
	}
	return names, nil
}

// SuspendIO freezes writes on this node's device — the cluster-wide write
// barrier for crash-consistent snapshots when run on the Primary. Goes
// through drbdadm: drbdsetup's suspend-io wants a minor, not a name.
func (d *Driver) SuspendIO(ctx context.Context, name string) error {
	_, err := d.adm(ctx, "suspend-io", name)
	return err
}

// ResumeIO lifts the barrier. Callers run it unconditionally after a
// snapshot attempt: a volume left suspended is an outage.
func (d *Driver) ResumeIO(ctx context.Context, name string) error {
	_, err := d.adm(ctx, "resume-io", name)
	return err
}

// Resize grows the DRBD device to the (already grown) backing devices.
// Callers must gate it on Status().Resyncing == false: DRBD refuses a
// resize mid-resync. assumeClean passes --assume-clean so DRBD marks the
// grown region UpToDate on every replica without a resync — correct for
// thin/sparse backends where the grown bytes are deterministically zeroed.
func (d *Driver) Resize(ctx context.Context, name string, assumeClean bool) error {
	if assumeClean {
		_, err := d.adm(ctx, "resize", "--assume-clean", name)
		return err
	}
	_, err := d.adm(ctx, "resize", name)
	return err
}

// Verify kicks off an online verify against every connected peer — the only
// cross-leg integrity check DRBD offers. It returns as soon as the pass is
// armed; the kernel runs the scan in the background and reports out-of-sync
// blocks in the replication status (Status.OutOfSyncKiB) and the kernel log.
// Requires verify-alg in the DRBD config; callers gate it on a connected,
// non-resyncing volume (drbdadm refuses verify otherwise).
func (d *Driver) Verify(ctx context.Context, name string) error {
	_, err := d.adm(ctx, "verify", name)
	return err
}

// IsResizeDuringResync reports whether a Resize failed only because a resync
// was in flight — DRBD refuses resize then, and the caller retries instead
// of failing the reconcile.
func IsResizeDuringResync(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Resize not allowed during resync")
}

func (d *Driver) writeConfig(r Resource) error {
	rendered := []byte(Render(r))
	path := d.path(r.Name + ".res")
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, rendered) {
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(d.StateDir, 0o750); err != nil {
		return err
	}
	// Atomic write: StateDir is drbdadm's include directory, and a crash
	// mid-write would leave a truncated .res that makes drbdadm adjust fail
	// for EVERY resource on the node, not just this one.
	return writeFileAtomic(path, rendered, 0o640)
}

// writeFileAtomic writes data to path via a temp file, fsync, and rename, so a
// crash never leaves a partially-written file at path.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }() // no-op after the explicit Close below; guards early returns
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *Driver) path(file string) string {
	return filepath.Join(d.StateDir, file)
}

func (d *Driver) adm(ctx context.Context, args ...string) (string, error) {
	return d.Exec(ctx, "drbdadm", args...)
}
