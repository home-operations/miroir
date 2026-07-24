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

package backend

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// zfsBackend provisions sparse zvols under a dedicated dataset (paris's
// backend; pool shared with OpenEBS LocalPV-ZFS).
type zfsBackend struct {
	dataset      string
	volBlockSize int64
	compression  string
	exec         Exec
	// readyTimeout bounds awaitDevice; a field so tests need not wait it out.
	readyTimeout time.Duration
}

func newZFS(cfg Config, e Exec) *zfsBackend {
	blockSize := cfg.ZFSVolBlockSize
	if blockSize == 0 {
		blockSize = defaultZFSVolBlockSize
	}
	compression := cfg.ZFSCompression
	if compression == "" {
		compression = defaultZFSCompression
	}
	return &zfsBackend{
		dataset:      cfg.Dataset,
		volBlockSize: blockSize,
		compression:  compression,
		exec:         e,
		readyTimeout: zvolReadyTimeout,
	}
}

// Setup creates the parent dataset (e.g. tank/miroir) if absent — the
// namespace separating miroir zvols from OpenEBS datasets in the shared
// pool.
func (z *zfsBackend) Setup(ctx context.Context) error {
	ok, err := z.exists(ctx, z.dataset)
	if err != nil || ok {
		return err
	}
	_, err = z.exec(ctx, "zfs", "create", "-p", z.dataset)
	return err
}

func (z *zfsBackend) name(vol string) string {
	return z.dataset + "/" + vol
}

// pool is the top-level zpool of the dataset (tank/miroir → tank): the scope
// whose capacity the overcommit guard reads, since it is shared with OpenEBS.
func (z *zfsBackend) pool() string {
	p, _, _ := strings.Cut(z.dataset, "/")
	return p
}

func (z *zfsBackend) DevicePath(vol string) string {
	return "/dev/zvol/" + z.name(vol)
}

func (z *zfsBackend) exists(ctx context.Context, name string) (bool, error) {
	_, err := z.exec(ctx, "zfs", "list", "-H", name)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (z *zfsBackend) Exists(ctx context.Context, vol string) (bool, error) {
	return z.exists(ctx, z.name(vol))
}

const (
	defaultZFSVolBlockSize int64 = 4 << 10
	defaultZFSCompression        = "lz4"
	// zvolReadyTimeout bounds the wait for a fresh zvol's device node.
	// zfs create returns once the volume exists in the pool, but
	// /dev/zvol/… is a udev-managed symlink that lands asynchronously —
	// the reason OpenZFS ships zvol_wait. udev normally settles in
	// milliseconds; the budget covers a node whose queue is backed up.
	zvolReadyTimeout  = 30 * time.Second
	zvolReadyInterval = 100 * time.Millisecond
)

// awaitDevice returns vol's device path once the node can be opened.
// blockdev is the probe rather than a stat: the /dev/zvol symlink appears
// before its target is guaranteed openable, and every consumer of this
// path (drbdadm create-md, mkfs) opens the device.
func (z *zfsBackend) awaitDevice(ctx context.Context, vol string) (string, error) {
	dev := z.DevicePath(vol)
	deadline := time.Now().Add(z.readyTimeout)
	for {
		_, err := z.exec(ctx, "blockdev", "--getsize64", dev)
		if err == nil {
			return dev, nil
		}
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("zvol %s: %s not usable after %s (udev may be stuck): %w",
				vol, dev, z.readyTimeout, err)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(zvolReadyInterval):
		}
	}
}

// alignSize rounds sizeBytes up to blockSize. PVC sizes are not necessarily
// block-size multiples (1G = 10^9), and the other backends already realize
// at least the requested size, so rounding up is harmless.
func alignSize(sizeBytes, blockSize int64) int64 {
	return (sizeBytes + blockSize - 1) &^ (blockSize - 1)
}

func (z *zfsBackend) Create(ctx context.Context, vol string, sizeBytes int64) (string, error) {
	ok, err := z.exists(ctx, z.name(vol))
	if err != nil {
		return "", err
	}
	if !ok {
		args := []string{
			"create",
			"-s", // sparse: thin semantics, matching the lvm-thin leg
			"-b", strconv.FormatInt(z.volBlockSize, 10),
		}
		if z.compression != "inherit" {
			args = append(args, "-o", "compression="+z.compression)
		}
		args = append(args, "-V", strconv.FormatInt(alignSize(sizeBytes, z.volBlockSize), 10), z.name(vol))
		_, err = z.exec(ctx, "zfs", args...)
		if err != nil {
			return "", fmt.Errorf("zfs create %s: %w", vol, err)
		}
	}
	// Also on the already-exists path: a node that just imported the pool
	// has the zvol without its device node yet.
	return z.awaitDevice(ctx, vol)
}

func (z *zfsBackend) Resize(ctx context.Context, vol string, sizeBytes int64) error {
	cur, blockSize, err := z.volGeometry(ctx, vol)
	if err != nil {
		return err
	}
	if cur >= sizeBytes {
		return nil // already big enough (idempotent retry)
	}
	if _, err := z.exec(ctx, "zfs", "set",
		fmt.Sprintf("volsize=%d", alignSize(sizeBytes, blockSize)), z.name(vol)); err != nil {
		return fmt.Errorf("zfs set volsize %s to %d: %w", vol, sizeBytes, err)
	}
	return nil
}

func (z *zfsBackend) Sync(ctx context.Context, vol string) error {
	// No global sync(2): it waits on dirty pages of filesystems mounted
	// on the suspended DRBD device above this zvol, which cannot flush
	// while the barrier is up — deadlock. Crash-consistent is the contract.
	if _, err := z.exec(ctx, "blockdev", "--flushbufs", z.DevicePath(vol)); err != nil {
		return fmt.Errorf("flush %s: %w", vol, err)
	}
	// Commit pending transaction groups before the snapshot dataset is cut.
	pool := z.pool()
	if _, err := z.exec(ctx, "zpool", "sync", pool); err != nil {
		return fmt.Errorf("zpool sync: %w", err)
	}
	return nil
}

func (z *zfsBackend) Snapshot(ctx context.Context, vol, snap string) error {
	ok, err := z.exists(ctx, z.name(vol)+"@"+snap)
	if err != nil || ok {
		return err
	}
	if _, err := z.exec(ctx, "zfs", "snapshot", z.name(vol)+"@"+snap); err != nil {
		return fmt.Errorf("zfs snapshot %s@%s: %w", vol, snap, err)
	}
	return nil
}

func (z *zfsBackend) CreateFromSnapshot(ctx context.Context, vol, sourceVol, snap string) (string, error) {
	ok, err := z.exists(ctx, z.name(vol))
	if err != nil {
		return "", err
	}
	if !ok {
		_, err = z.exec(ctx, "zfs", "clone",
			z.name(sourceVol)+"@"+snap, z.name(vol))
		if err != nil {
			return "", err
		}
	}
	return z.awaitDevice(ctx, vol)
}

func (z *zfsBackend) Delete(ctx context.Context, vol string) error {
	ok, err := z.exists(ctx, z.name(vol))
	if err != nil || !ok {
		return err
	}
	// The zvol's kernel device can still be releasing after DRBD detached
	// — zfs destroy races the cleanup and returns "dataset is busy". A
	// few short retries let the kernel finish within this call instead
	// of escalating to the agent's 10s cadence, which parks at 30
	// attempts when many volumes tear down at once.
	_, derr := z.exec(ctx, "zfs", "destroy", z.name(vol))
	for attempt := 0; derr != nil && strings.Contains(derr.Error(), "dataset is busy") && attempt < 3; attempt++ {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return Busy(derr)
		}
		_, derr = z.exec(ctx, "zfs", "destroy", z.name(vol))
	}
	if derr == nil ||
		(!strings.Contains(derr.Error(), "has children") &&
			!strings.Contains(derr.Error(), "dependent clones")) {
		return Busy(derr)
	}
	// Restore clones pin this volume's snapshot chain — promoting
	// reparents each clone's origin snapshot onto the clone, freeing the
	// volume. Snapshots without clones still block the retry, which is
	// the intended ordering: they go through their own delete lifecycle.
	if err := z.promoteClones(ctx, vol); err != nil {
		return err
	}
	_, err = z.exec(ctx, "zfs", "destroy", z.name(vol))
	return Busy(err)
}

func (z *zfsBackend) promoteClones(ctx context.Context, vol string) error {
	out, err := z.exec(ctx, "zfs", "get", "-Hpo", "value", "clones",
		"-r", "-t", "snapshot", z.name(vol))
	if err != nil {
		return err
	}
	for clones := range strings.FieldsSeq(out) {
		if clones == "-" {
			continue
		}
		for clone := range strings.SplitSeq(clones, ",") {
			if _, err := z.exec(ctx, "zfs", "promote", clone); err != nil {
				return err
			}
		}
	}
	return nil
}

// snapshotsMatching lists every snapshot under the parent dataset whose
// name ends in "@"+snap. The snapshot may have migrated: deleting a volume
// with restore clones promotes them, and zfs promote reparents every
// snapshot at-or-older than the clone's origin onto the clone. Snapshot
// names are CR names — cluster-unique — so matching by @suffix anywhere
// under the dataset finds the migrated copy; without this, a migrated
// snapshot's deletion false-succeeds and the leftover blocks its clone's
// destroy forever.
func (z *zfsBackend) snapshotsMatching(ctx context.Context, snap string) ([]string, error) {
	out, err := z.exec(ctx, "zfs", "list", "-Hpo", "name", "-t", "snapshot", "-r", z.dataset)
	if err != nil {
		return nil, err
	}
	var names []string
	for line := range strings.SplitSeq(out, "\n") {
		name := strings.TrimSpace(line)
		if strings.HasSuffix(name, "@"+snap) {
			names = append(names, name)
		}
	}
	return names, nil
}

func (z *zfsBackend) DeleteSnapshot(ctx context.Context, vol, snap string) error {
	names, err := z.snapshotsMatching(ctx, snap)
	if err != nil {
		return err
	}
	for _, name := range names {
		// -d defers destruction while restore clones still reference the
		// snapshot (ZFS removes it with the last clone); immediate otherwise.
		_, destroyErr := z.exec(ctx, "zfs", "destroy", "-d", name)
		if destroyErr == nil {
			continue
		}
		// The listed name can be stale by destroy time: a concurrent clone
		// teardown removes a deferred snapshot with its last clone, and a
		// concurrent promote reparents it under the clone. Re-list to tell
		// gone (the contract's "succeeds if already absent") from migrated:
		// a survivor means the destroy genuinely failed or the snapshot
		// lives under a new name, so surface the error and let the retry
		// find it.
		still, err := z.snapshotsMatching(ctx, snap)
		if err != nil {
			return err
		}
		if len(still) > 0 {
			return destroyErr
		}
	}
	return nil
}

func (z *zfsBackend) volGeometry(ctx context.Context, vol string) (sizeBytes, blockSize int64, err error) {
	out, err := z.exec(ctx, "zfs", "get", "-Hpo", "value", "volsize,volblocksize", z.name(vol))
	if err != nil {
		return 0, 0, err
	}
	values := strings.Fields(out)
	if len(values) != 2 {
		return 0, 0, fmt.Errorf("unexpected zfs volume geometry %q", out)
	}
	sizeBytes, err = strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	blockSize, err = strconv.ParseInt(values[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return sizeBytes, blockSize, nil
}

func (z *zfsBackend) Stats(ctx context.Context) (PoolStats, error) {
	// Pool-level stats: the pool is shared with OpenEBS, so headroom must
	// account for everything in it, not only miroir zvols.
	pool := z.pool()
	out, err := z.exec(ctx, "zpool", "get", "-Hpo", "value", "size,allocated", pool)
	if err != nil {
		return PoolStats{}, err
	}
	lines := strings.Fields(strings.TrimSpace(out))
	if len(lines) != 2 {
		return PoolStats{}, fmt.Errorf("unexpected zpool output %q", out)
	}
	size, err := strconv.ParseInt(lines[0], 10, 64)
	if err != nil {
		return PoolStats{}, err
	}
	used, err := strconv.ParseInt(lines[1], 10, 64)
	if err != nil {
		return PoolStats{}, err
	}
	return PoolStats{SizeBytes: size, UsedBytes: used}, nil
}
