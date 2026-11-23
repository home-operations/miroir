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
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	ctrl "sigs.k8s.io/controller-runtime"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// FIFREEZE/FITHAW from linux/fs.h (_IOWR('X', 119/120, int)); x/sys/unix
// does not export them.
const (
	fiFreeze = 0xc0045877
	fiThaw   = 0xc0045878
)

// freezeTimeout bounds the FIFREEZE ioctl: it blocks until the
// filesystem's dirty pages are written back, which on a struggling
// device can outlast the whole barrier round. A freeze abandoned on
// timeout is self-canceling (see Freeze), so the round can retry
// without risking a filesystem left frozen with no owner.
const freezeTimeout = 30 * time.Second

// Freezer freezes and thaws the filesystem mounted on a block device,
// upgrading a snapshot cut from crash-consistent to
// filesystem-consistent: FIFREEZE writes back all dirty pages and
// quiesces new writes above the block layer, so the data an
// application wrote before the cut — synced or not — is on the device
// when the barrier rises.
//
// The device is matched against live mounts by device number, so any
// alias path (/dev/drbd1000, /dev/mapper/…, /dev/zvol/…) finds its
// mount; btrfs mounts carry an anonymous st_dev and are matched by
// their mount source's number instead. A device mounted more than once
// (staging plus pod binds) shares one superblock; freezing any
// mountpoint freezes them all.
type Freezer struct {
	// mountinfo, devNumber and ioctl are the syscall surface, injectable
	// in tests.
	mountinfo func() ([]byte, error)
	devNumber func(path string) (uint64, bool, error)
	ioctl     func(mountpoint string, req uint) error

	// devLocks orders a freeze against a staging unmount of the same
	// device (see HoldFreezes), one slot per device number. FIFREEZE first
	// writes back every dirty page, which can take seconds after a consumer
	// that just exited; an unstage whose own thaw ran inside that window
	// found nothing frozen, and its unmount then tore down a filesystem the
	// freeze froze a moment later — the leak of issue #311, permanent on an
	// unreplicated volume. Each slot is a one-element channel so a waiter
	// can give up with its context. Entries are never dropped: one per
	// device number the node has seen is negligible.
	devMu    sync.Mutex
	devLocks map[uint64]chan struct{}
}

// lockDev takes rdev's slot, or returns the context's error if that
// expires first.
func (f *Freezer) lockDev(ctx context.Context, rdev uint64) (unlock func(), err error) {
	f.devMu.Lock()
	if f.devLocks == nil {
		f.devLocks = map[uint64]chan struct{}{}
	}
	slot, ok := f.devLocks[rdev]
	if !ok {
		slot = make(chan struct{}, 1)
		f.devLocks[rdev] = slot
	}
	f.devMu.Unlock()
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// A free slot and an expired context are both ready; select picks
	// either, so the cancellation is re-checked after acquisition.
	if err := ctx.Err(); err != nil {
		<-slot
		return nil, err
	}
	return func() { <-slot }, nil
}

// NewFreezer returns a Freezer backed by /proc/self/mountinfo and real
// ioctls. The agent's mount namespace sees the kubelet staging mounts
// it created (the kubelet dir hostPath propagates them).
func NewFreezer() *Freezer {
	return &Freezer{
		mountinfo: func() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") },
		devNumber: realDevNumber,
		ioctl:     freezeIoctl,
	}
}

// Freeze freezes the filesystem mounted from device, returning the
// mountpoint it froze ("" when the device is not fs-mounted here — a
// Secondary leg, a raw-block volume, or an unstaged device). A
// filesystem already frozen (EBUSY) counts as success: the freeze is
// shared state exactly like the kernel's suspend-io flag, and the
// paired Thaw tolerates the mirror case. On timeout the ioctl cannot
// be interrupted, so a watcher thaws the late freeze the moment it
// lands: a failed Freeze never leaves the filesystem frozen. The device's
// slot is held until the freeze is either answered or thawed by that
// watcher, so a staging unmount can never run between a late freeze and
// its thaw.
func (f *Freezer) Freeze(ctx context.Context, device string) (string, error) {
	rdev, isDev, err := f.devNumber(device)
	if err != nil || !isDev {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(ctx, freezeTimeout)
	defer cancel()
	unlock, err := f.lockDev(ctx, rdev)
	if err != nil {
		return "", fmt.Errorf("freeze %s: %w", device, err)
	}
	watcherOwnsLock := false
	defer func() {
		if !watcherOwnsLock {
			unlock()
		}
	}()
	mp, err := f.mountpointOf(device)
	if err != nil || mp == "" {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- f.ioctl(mp, fiFreeze) }()
	select {
	case err := <-done:
		if errors.Is(err, unix.EBUSY) {
			return mp, nil
		}
		if err != nil {
			return "", fmt.Errorf("freeze %s (%s): %w", mp, device, err)
		}
		return mp, nil
	case <-ctx.Done():
		watcherOwnsLock = true
		go func() {
			defer unlock()
			if err := <-done; err == nil {
				_ = f.ioctl(mp, fiThaw)
			}
		}()
		return "", fmt.Errorf("freeze %s (%s): %w", mp, device, ctx.Err())
	}
}

// Thaw lifts the freeze on the filesystem mounted from device, returning
// the mountpoint it thawed. Not mounted (mountpoint "") or not frozen
// (EINVAL) are successes: thaw is called from every path that closes or
// abandons a barrier round, most of which never froze anything. But ""
// also means an actually-leaked freeze was NOT lifted — FITHAW needs a
// mountpoint, and a frozen device refuses every new mount —
// so callers who suspect a leak must not read "" as thawed; the
// stage-time recovery (stage.EnsureFilesystem) is what clears those.
func (f *Freezer) Thaw(device string) (string, error) {
	mp, err := f.mountpointOf(device)
	if err != nil || mp == "" {
		return "", err
	}
	if err := f.ioctl(mp, fiThaw); err != nil && !errors.Is(err, unix.EINVAL) {
		return "", fmt.Errorf("thaw %s (%s): %w", mp, device, err)
	}
	return mp, nil
}

// HoldFreezes blocks freezes of the filesystem mounted at target until the
// returned release is called, or returns the context's error if a freeze in
// progress outlasts it. A staging unmount holds it across its
// ThawMountpoint and the unmount, so a snapshot round's freeze either lands
// before the thaw (which lifts it) or runs after the unmount (and finds
// nothing mounted to freeze). A target that is not a block-device mount
// (not mounted, or NFS) needs no ordering and gets a no-op release.
func (f *Freezer) HoldFreezes(ctx context.Context, target string) (release func(), err error) {
	rdev, ok, err := f.mountDev(target)
	if err != nil || !ok {
		return func() {}, err
	}
	return f.lockDev(ctx, rdev)
}

// HoldDevice blocks freezes and staging unmounts of device until the
// returned release is called, or returns the context's error if one in
// progress outlasts it. It keys the same slot as Freeze and HoldFreezes. A
// path that does not resolve to a block device is an error, not a reason
// to skip the ordering: a mounted device whose node vanished is still
// mounted.
func (f *Freezer) HoldDevice(ctx context.Context, device string) (release func(), err error) {
	rdev, isDev, err := f.devNumber(device)
	if err != nil {
		return nil, err
	}
	if !isDev {
		return nil, fmt.Errorf("%s is not a block device", device)
	}
	return f.lockDev(ctx, rdev)
}

// mountDev returns the device number backing the filesystem mounted at
// target, as Freeze keys it: mountinfo's major:minor, or for btrfs (whose
// st_dev is anonymous) its mount source's number. ok is false when target
// is not a block-device mount, and for a btrfs mount whose source node is
// missing: refusing the unstage instead would wedge it for as long as the
// node stays missing, which is worse than the narrow race left open.
func (f *Freezer) mountDev(target string) (rdev uint64, ok bool, err error) {
	data, err := f.mountinfo()
	if err != nil {
		return 0, false, err
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) < 5 || fields[4] != target {
			continue
		}
		majs, mins, found := strings.Cut(fields[2], ":")
		maj, majErr := strconv.ParseUint(majs, 10, 32)
		minor, minErr := strconv.ParseUint(mins, 10, 32)
		if found && majErr == nil && minErr == nil && maj != 0 {
			return unix.Mkdev(uint32(maj), uint32(minor)), true, nil
		}
		if fstype, _ := mountFS(fields); fstype == "btrfs" {
			src, isDev := f.sourceDev(fields)
			return src, isDev, nil
		}
		return 0, false, nil
	}
	return 0, false, nil
}

// ThawMountpoint lifts a freeze on the block-device-backed filesystem
// mounted at target; not a mountpoint and not frozen (EINVAL) are
// successes, as it guards every staging unmount and those almost never
// tear down a frozen filesystem. The unmount must run after it: once the
// last mountpoint is gone, FITHAW has nothing left to open while the
// frozen device refuses every new mount — the catch-22 of issue #311.
// Only block-device mounts (mountinfo major != 0, or btrfs, whose st_dev
// is anonymous) are touched: opening a dead hard-NFS mountpoint for the
// ioctl would hang exactly where the unstage path's forced unmount
// cannot. It does not take the device's slot: callers hold HoldFreezes
// across it and the unmount.
func (f *Freezer) ThawMountpoint(target string) error {
	data, err := f.mountinfo()
	if err != nil {
		return err
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) < 5 || fields[4] != target {
			continue
		}
		if maj, _, ok := strings.Cut(fields[2], ":"); !ok || maj == "0" {
			if fstype, _ := mountFS(fields); fstype != "btrfs" {
				return nil
			}
		}
		if err := f.ioctl(target, fiThaw); err != nil && !errors.Is(err, unix.EINVAL) {
			return fmt.Errorf("thaw %s: %w", target, err)
		}
		return nil
	}
	return nil
}

// mountpointOf finds the first mountpoint whose filesystem is backed by
// device, matching by device number so alias paths resolve. Filesystems
// not backed by a block device (devtmpfs bind mounts of the device node
// included) never match: their mountinfo st_dev is not the device's
// rdev, and their mount source is not a device node.
func (f *Freezer) mountpointOf(device string) (string, error) {
	rdev, isDev, err := f.devNumber(device)
	if err != nil || !isDev {
		// A device that vanished mid-round has nothing mounted left to
		// freeze or thaw.
		return "", nil
	}
	data, err := f.mountinfo()
	if err != nil {
		return "", err
	}
	want := fmt.Sprintf("%d:%d", unix.Major(rdev), unix.Minor(rdev))
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		if fields[2] == want {
			return fields[4], nil
		}
		if strings.HasPrefix(fields[2], "0:") {
			if src, ok := f.sourceDev(fields); ok && src == rdev {
				return fields[4], nil
			}
		}
	}
	return "", nil
}

// sourceDev returns the device number of a mountinfo row's mount source
// when it is a block device node. btrfs reports an anonymous st_dev (0:N)
// in field 3, so its source is the only link back to the device. Sources
// outside /dev (NFS's host:/export, pseudo-filesystems) are never stat'ed.
func (f *Freezer) sourceDev(fields []string) (uint64, bool) {
	_, src := mountFS(fields)
	if !strings.HasPrefix(src, "/dev/") {
		return 0, false
	}
	rdev, isDev, err := f.devNumber(src)
	return rdev, err == nil && isDev
}

// mountFS returns a mountinfo row's filesystem type and mount source: the
// two fields after the "-" that ends the variable-length optional fields,
// or "" for a malformed row.
func mountFS(fields []string) (fstype, source string) {
	for i := 6; i+2 < len(fields); i++ {
		if fields[i] == "-" {
			return fields[i+1], fields[i+2]
		}
	}
	return "", ""
}

// freezeMounted freezes the volume's locally mounted filesystem (a
// no-op on nodes where it is not mounted — only the Primary mounts).
// Ordering is the whole point: the freeze's writeback happens before
// this node's own suspend-io, and suspend-io only blocks writes where
// they originate, so the flushed pages replicate to every leg before
// any barrier that matters to them rises.
func freezeMounted(ctx context.Context, f *Freezer, node string, vol *miroirv1alpha1.MiroirVolume) error {
	if f == nil {
		return nil
	}
	device := vol.Status.PerNode[node].DevicePath
	if device == "" {
		return nil
	}
	_, err := f.Freeze(ctx, device)
	return err
}

// holdUnmounts keeps a staging unmount of the volume's local device from
// running until release is called. An unreplicated volume's backend cut
// freezes the filesystem itself: lvmthin's snapshot suspends the device
// the filesystem sits on, and dm suspend freezes it as a kernel freeze
// holder. An unmount landing inside that suspend leaves the device frozen
// with nothing left to thaw it through, so every later mount fails
// ("Can't mount, blockdev is frozen"). The wait is bounded by
// freezeTimeout: an unmount stuck in the kernel must not pin the snapshot
// worker. A nil freezer or a leg without a device path holds nothing.
func holdUnmounts(ctx context.Context, f *Freezer, node string, vol *miroirv1alpha1.MiroirVolume) (release func(), err error) {
	device := vol.Status.PerNode[node].DevicePath
	if f == nil || device == "" {
		return func() {}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, freezeTimeout)
	defer cancel()
	release, err = f.HoldDevice(ctx, device)
	if err != nil {
		return nil, fmt.Errorf("order the cut of %s against unmounts: %w", vol.Name, err)
	}
	return release, nil
}

// thawMounted lifts the local filesystem freeze; best-effort (not
// mounted or not frozen are successes), logged so a persistently
// failing thaw — a frozen workload — is never silent.
func thawMounted(ctx context.Context, f *Freezer, node string, vol *miroirv1alpha1.MiroirVolume) {
	if f == nil {
		return
	}
	device := vol.Status.PerNode[node].DevicePath
	if device == "" {
		return
	}
	if _, err := f.Thaw(device); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "cannot thaw the snapshot filesystem freeze", "volume", vol.Name)
	}
}

// realDevNumber stats path and returns its device number; isDev is
// false for anything but a block device (including a missing path).
func realDevNumber(path string) (uint64, bool, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return 0, false, nil
	}
	if st.Mode&unix.S_IFMT != unix.S_IFBLK {
		return 0, false, nil
	}
	return st.Rdev, true, nil
}

// freezeIoctl opens the mountpoint and issues the freeze/thaw ioctl.
func freezeIoctl(mountpoint string, req uint) error {
	fd, err := unix.Open(mountpoint, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	return unix.IoctlSetInt(fd, req, 0)
}
