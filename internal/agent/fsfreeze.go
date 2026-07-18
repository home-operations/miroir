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
	"strings"
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
// when the barrier rises (issue #291).
//
// The device is matched against live mounts by device number, so any
// alias path (/dev/drbd1000, /dev/mapper/…, /dev/zvol/…) finds its
// mount. A device mounted more than once (staging plus pod binds)
// shares one superblock; freezing any mountpoint freezes them all.
type Freezer struct {
	// mountinfo, devNumber and ioctl are the syscall surface, injectable
	// in tests.
	mountinfo func() ([]byte, error)
	devNumber func(path string) (uint64, bool, error)
	ioctl     func(mountpoint string, req uint) error
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
// lands: a failed Freeze never leaves the filesystem frozen.
func (f *Freezer) Freeze(ctx context.Context, device string) (string, error) {
	mp, err := f.mountpointOf(device)
	if err != nil || mp == "" {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, freezeTimeout)
	defer cancel()
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
		go func() {
			if err := <-done; err == nil {
				_ = f.ioctl(mp, fiThaw)
			}
		}()
		return "", fmt.Errorf("freeze %s (%s): %w", mp, device, ctx.Err())
	}
}

// Thaw lifts the freeze on the filesystem mounted from device. Not
// mounted or not frozen (EINVAL) are successes: thaw is called from
// every path that closes or abandons a barrier round, most of which
// never froze anything.
func (f *Freezer) Thaw(device string) error {
	mp, err := f.mountpointOf(device)
	if err != nil || mp == "" {
		return err
	}
	if err := f.ioctl(mp, fiThaw); err != nil && !errors.Is(err, unix.EINVAL) {
		return fmt.Errorf("thaw %s (%s): %w", mp, device, err)
	}
	return nil
}

// mountpointOf finds the first mountpoint whose filesystem is backed by
// device, matching by device number so alias paths resolve. Filesystems
// not backed by a block device (devtmpfs bind mounts of the device node
// included) never match: their mountinfo st_dev is not the device's
// rdev.
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
	}
	return "", nil
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
	if err := f.Thaw(device); err != nil {
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
