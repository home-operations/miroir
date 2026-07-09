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
)

// zfsBackend provisions sparse zvols under a dedicated dataset (paris's
// backend; pool shared with OpenEBS LocalPV-ZFS — notes/DESIGN.md §4.1a).
// volblocksize is fixed at 4k to align with the LVM-thin peer leg.
type zfsBackend struct {
	dataset string
	exec    Exec
}

func newZFS(cfg Config, e Exec) *zfsBackend {
	return &zfsBackend{dataset: cfg.Dataset, exec: e}
}

// Setup creates the parent dataset (e.g. tank/miroir) if absent — the
// namespace separating miroir zvols from OpenEBS datasets in the shared
// pool (notes/DESIGN.md §4.1a).
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

// zvolBlockSize is the volblocksize passed to zfs create (-b); volsize
// must be a multiple of it or OpenZFS rejects the create/set with EINVAL.
const zvolBlockSize = 4096

// alignSize rounds sizeBytes up to the zvol block size: PVC sizes are not
// necessarily 4KiB-multiples (1G = 10^9), and the other backends already
// realize >= the requested size, so rounding up is harmless — DRBD sizes
// the device to the smallest leg.
func alignSize(sizeBytes int64) int64 {
	return (sizeBytes + zvolBlockSize - 1) &^ (zvolBlockSize - 1)
}

func (z *zfsBackend) Create(ctx context.Context, vol string, sizeBytes int64) (string, error) {
	ok, err := z.exists(ctx, z.name(vol))
	if err != nil {
		return "", err
	}
	if !ok {
		_, err = z.exec(ctx, "zfs", "create",
			"-s", // sparse: thin semantics, matching the lvm-thin leg
			"-b", strconv.Itoa(zvolBlockSize),
			// lz4 early-aborts on incompressible data (≈free) and cuts
			// physical I/O; a near-universal default for zvols.
			"-o", "compression=lz4",
			"-V", strconv.FormatInt(alignSize(sizeBytes), 10),
			z.name(vol))
		if err != nil {
			return "", fmt.Errorf("zfs create %s: %w", vol, err)
		}
	}
	return z.DevicePath(vol), nil
}

func (z *zfsBackend) Resize(ctx context.Context, vol string, sizeBytes int64) error {
	cur, err := z.volSize(ctx, vol)
	if err != nil {
		return err
	}
	if cur >= sizeBytes {
		return nil // already big enough (idempotent retry)
	}
	if _, err := z.exec(ctx, "zfs", "set",
		fmt.Sprintf("volsize=%d", alignSize(sizeBytes)), z.name(vol)); err != nil {
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
	return z.DevicePath(vol), nil
}

func (z *zfsBackend) Delete(ctx context.Context, vol string) error {
	ok, err := z.exists(ctx, z.name(vol))
	if err != nil || !ok {
		return err
	}
	_, derr := z.exec(ctx, "zfs", "destroy", z.name(vol))
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
	for _, clones := range strings.Fields(out) {
		if clones == "-" {
			continue
		}
		for _, clone := range strings.Split(clones, ",") {
			if _, err := z.exec(ctx, "zfs", "promote", clone); err != nil {
				return err
			}
		}
	}
	return nil
}

func (z *zfsBackend) DeleteSnapshot(ctx context.Context, vol, snap string) error {
	// The snapshot may have migrated: deleting a volume with restore
	// clones promotes them, and zfs promote reparents every snapshot
	// at-or-older than the clone's origin onto the clone. Snapshot names
	// are CR names — cluster-unique — so match by @suffix anywhere under
	// the parent dataset; without this, a migrated snapshot's deletion
	// false-succeeds and the leftover blocks its clone's destroy forever.
	out, err := z.exec(ctx, "zfs", "list", "-Hpo", "name", "-t", "snapshot", "-r", z.dataset)
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(out, "\n") {
		name := strings.TrimSpace(line)
		if !strings.HasSuffix(name, "@"+snap) {
			continue
		}
		// -d defers destruction while restore clones still reference the
		// snapshot (ZFS removes it with the last clone); immediate otherwise.
		if _, err := z.exec(ctx, "zfs", "destroy", "-d", name); err != nil {
			return err
		}
	}
	return nil
}

func (z *zfsBackend) volSize(ctx context.Context, vol string) (int64, error) {
	out, err := z.exec(ctx, "zfs", "get", "-Hpo", "value", "volsize", z.name(vol))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

func (z *zfsBackend) Stats(ctx context.Context) (PoolStats, error) {
	// Pool-level stats: the pool is shared with OpenEBS, so headroom must
	// account for everything in it, not only miroir zvols (notes/DESIGN.md §4.6).
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
