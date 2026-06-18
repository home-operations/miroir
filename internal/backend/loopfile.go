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
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// loopfile provisions volumes as sparse files on a node's existing
// filesystem, each attached as a loop device — no dedicated disk and no
// LVM/ZFS pool to format (notes/DESIGN.md §4.1a). It is the "use whatever's
// already mounted" backend: Longhorn's sparse-file replicas, but replicated
// by in-kernel DRBD instead of a userspace engine.
//
// Files are thin (sparse via truncate), matching the lvm-thin/zfs legs.
// CoW snapshots use reflink (cp --reflink=always → FICLONE), so the base
// directory must sit on a reflink-capable filesystem — XFS with reflink=1
// (Talos /var) or btrfs. Setup probes this and fails fast otherwise, rather
// than letting the first snapshot silently fall back to a full copy.
//
// Loop device numbers are assigned by the kernel and not stable, but
// Backend.DevicePath must return a fixed path. Each attach therefore
// maintains a symlink under <baseDir>/dev/<vol> → /dev/loopN; that symlink
// is the device path pods/DRBD see, repointed on every Create so it
// survives a reboot (the agent re-attaches before layering DRBD).
type loopfile struct {
	baseDir string
	exec    Exec
}

func newLoopfile(cfg Config, e Exec) *loopfile {
	return &loopfile{baseDir: cfg.BaseDir, exec: e}
}

func (lf *loopfile) volDir() string  { return filepath.Join(lf.baseDir, "volumes") }
func (lf *loopfile) snapDir() string { return filepath.Join(lf.baseDir, "snapshots") }
func (lf *loopfile) devDir() string  { return filepath.Join(lf.baseDir, "dev") }

func (lf *loopfile) imgPath(vol string) string   { return filepath.Join(lf.volDir(), vol+".img") }
func (lf *loopfile) snapPath(snap string) string { return filepath.Join(lf.snapDir(), snap+".img") }

// DevicePath returns the stable symlink to the volume's loop device,
// created by attach. Pure (no exec) by interface contract.
func (lf *loopfile) DevicePath(vol string) string {
	return filepath.Join(lf.devDir(), vol)
}

// Setup creates the layout directories and verifies the base directory is
// on a reflink-capable filesystem — without it CoW snapshots are
// impossible, and we refuse rather than degrade to full copies.
func (lf *loopfile) Setup(ctx context.Context) error {
	for _, d := range []string{lf.volDir(), lf.snapDir(), lf.devDir()} {
		if _, err := lf.exec(ctx, "mkdir", "-p", d); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	src := filepath.Join(lf.baseDir, ".reflink-probe")
	dst := src + ".clone"
	if _, err := lf.exec(ctx, "truncate", "-s", "4096", src); err != nil {
		return fmt.Errorf("reflink probe (create): %w", err)
	}
	_, cloneErr := lf.exec(ctx, "cp", "--reflink=always", src, dst)
	if _, err := lf.exec(ctx, "rm", "-f", src, dst); err != nil {
		return fmt.Errorf("reflink probe (cleanup): %w", err)
	}
	if cloneErr != nil {
		return fmt.Errorf("base dir %s is not on a reflink-capable filesystem "+
			"(need XFS reflink=1 or btrfs for CoW snapshots): %w", lf.baseDir, cloneErr)
	}
	return nil
}

func (lf *loopfile) exists(ctx context.Context, path string) (bool, error) {
	if _, err := lf.exec(ctx, "stat", path); err != nil {
		if strings.Contains(err.Error(), "No such file") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// loopDev returns the loop device currently backing file, or "" if none.
func (lf *loopfile) loopDev(ctx context.Context, file string) (string, error) {
	out, err := lf.exec(ctx, "losetup", "-j", file, "-O", "NAME", "-n")
	if err != nil {
		return "", fmt.Errorf("losetup -j %s: %w", file, err)
	}
	line := strings.TrimSpace(out)
	if line == "" {
		return "", nil
	}
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i] // a file backs at most one loop here; first wins
	}
	return strings.TrimSpace(line), nil
}

// attach ensures file is bound to a loop device and (re)points the volume's
// stable symlink at it, returning the symlink path. Idempotent: an
// already-attached file reuses its loop device.
func (lf *loopfile) attach(ctx context.Context, vol, file string) (string, error) {
	dev, err := lf.loopDev(ctx, file)
	if err != nil {
		return "", err
	}
	if dev == "" {
		out, aerr := lf.exec(ctx, "losetup", "--find", "--show", file)
		if aerr != nil {
			return "", fmt.Errorf("losetup --find %s: %w", file, aerr)
		}
		dev = strings.TrimSpace(out)
	}
	link := lf.DevicePath(vol)
	if _, err := lf.exec(ctx, "ln", "-sfn", dev, link); err != nil {
		return "", fmt.Errorf("symlink %s -> %s: %w", link, dev, err)
	}
	return link, nil
}

func (lf *loopfile) Create(ctx context.Context, vol string, sizeBytes int64) (string, error) {
	file := lf.imgPath(vol)
	ok, err := lf.exists(ctx, file)
	if err != nil {
		return "", err
	}
	if !ok {
		// Sparse (thin) backing file, like the zfs -s zvol / lvm-thin LV.
		if _, err := lf.exec(ctx, "truncate", "-s", strconv.FormatInt(sizeBytes, 10), file); err != nil {
			return "", fmt.Errorf("create backing file %s: %w", file, err)
		}
	}
	return lf.attach(ctx, vol, file)
}

func (lf *loopfile) Resize(ctx context.Context, vol string, sizeBytes int64) error {
	cur, err := lf.sizeOf(ctx, lf.imgPath(vol))
	if err != nil {
		return err
	}
	if cur >= sizeBytes {
		return nil // already big enough (idempotent retry)
	}
	if _, err := lf.exec(ctx, "truncate", "-s", strconv.FormatInt(sizeBytes, 10), lf.imgPath(vol)); err != nil {
		return fmt.Errorf("grow backing file %s to %d: %w", vol, sizeBytes, err)
	}
	// Refresh the loop device's capacity to the grown file.
	dev, err := lf.loopDev(ctx, lf.imgPath(vol))
	if err != nil {
		return err
	}
	if dev != "" {
		if _, err := lf.exec(ctx, "losetup", "-c", dev); err != nil {
			return fmt.Errorf("losetup -c %s: %w", dev, err)
		}
	}
	return nil
}

func (lf *loopfile) Sync(_ context.Context, vol string) error {
	// fsync the backing file, not the loop device: DRBD (and a directly
	// mounted fs on replicas:1) submit bios that the loop driver writes into
	// the *backing file's* page cache, but blockdev --flushbufs on the loop
	// device only syncs that block device's own cache — it never reaches the
	// file. With in-flight writes already drained by the DRBD suspend
	// barrier, fsyncing the file makes the snapshot source durable
	// regardless of whether reflink happens to flush it.
	//
	// fsync of one fd, not global sync(2): the latter waits on dirty pages
	// of the filesystem mounted on the suspended DRBD device, which cannot
	// flush under the barrier — deadlock (see zfsBackend.Sync). The backing
	// file lives on the node's own filesystem, unaffected.
	f, err := os.OpenFile(lf.imgPath(vol), os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s for sync: %w", vol, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", vol, err)
	}
	return nil
}

func (lf *loopfile) Snapshot(ctx context.Context, vol, snap string) error {
	ok, err := lf.exists(ctx, lf.snapPath(snap))
	if err != nil || ok {
		return err
	}
	if _, err := lf.exec(ctx, "cp", "--reflink=always", lf.imgPath(vol), lf.snapPath(snap)); err != nil {
		return fmt.Errorf("reflink snapshot %s of %s: %w", snap, vol, err)
	}
	return nil
}

func (lf *loopfile) CreateFromSnapshot(ctx context.Context, vol, _ /* sourceVol */, snap string) (string, error) {
	file := lf.imgPath(vol)
	ok, err := lf.exists(ctx, file)
	if err != nil {
		return "", err
	}
	if !ok {
		// A reflink copy of the snapshot is the clone: instant CoW within
		// the same filesystem, writes break sharing per-extent.
		if _, err := lf.exec(ctx, "cp", "--reflink=always", lf.snapPath(snap), file); err != nil {
			return "", fmt.Errorf("reflink clone %s from %s: %w", vol, snap, err)
		}
	}
	return lf.attach(ctx, vol, file)
}

func (lf *loopfile) Delete(ctx context.Context, vol string) error {
	file := lf.imgPath(vol)
	ok, err := lf.exists(ctx, file)
	if err != nil || !ok {
		return err
	}
	dev, err := lf.loopDev(ctx, file)
	if err != nil {
		return err
	}
	if dev != "" {
		if _, err := lf.exec(ctx, "losetup", "-d", dev); err != nil {
			return fmt.Errorf("detach %s: %w", dev, err)
		}
	}
	if _, err := lf.exec(ctx, "rm", "-f", lf.DevicePath(vol)); err != nil {
		return fmt.Errorf("remove symlink %s: %w", lf.DevicePath(vol), err)
	}
	if _, err := lf.exec(ctx, "rm", "-f", file); err != nil {
		return fmt.Errorf("remove backing file %s: %w", file, err)
	}
	return nil
}

func (lf *loopfile) DeleteSnapshot(ctx context.Context, _ /* vol */, snap string) error {
	ok, err := lf.exists(ctx, lf.snapPath(snap))
	if err != nil || !ok {
		return err
	}
	if _, err := lf.exec(ctx, "rm", "-f", lf.snapPath(snap)); err != nil {
		return fmt.Errorf("remove snapshot %s: %w", snap, err)
	}
	return nil
}

func (lf *loopfile) sizeOf(ctx context.Context, file string) (int64, error) {
	out, err := lf.exec(ctx, "stat", "-c", "%s", file)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

func (lf *loopfile) Stats(ctx context.Context) (PoolStats, error) {
	// The "pool" is the filesystem holding the base directory; headroom must
	// account for everything on it, not only miroir files (notes/DESIGN.md §4.6).
	out, err := lf.exec(ctx, "df", "-B1", "-P", lf.baseDir)
	if err != nil {
		return PoolStats{}, err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return PoolStats{}, fmt.Errorf("unexpected df output %q", out)
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 3 {
		return PoolStats{}, fmt.Errorf("unexpected df line %q", lines[len(lines)-1])
	}
	size, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return PoolStats{}, err
	}
	used, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return PoolStats{}, err
	}
	return PoolStats{SizeBytes: size, UsedBytes: used}, nil
}
