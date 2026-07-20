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
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	devDrbd1000 = "/dev/drbd1000"
	mntStage1   = "/mnt/stage-1"
	callFreeze1 = "freeze " + mntStage1
	callThaw1   = "thaw " + mntStage1
)

// testMountinfo mirrors /proc/self/mountinfo: the drbd device (147:0)
// carries an ext4 staging mount plus a pod bind of the same superblock;
// devtmpfs (0:6) hosts the raw-block bind of a device node.
const testMountinfo = `21 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw
36 21 147:0 / /var/lib/kubelet/plugins/kubernetes.io/csi/miroir/abc/globalmount rw,relatime - ext4 /dev/drbd1000 rw
37 21 147:0 / /var/lib/kubelet/pods/x/volumes/kubernetes.io~csi/pvc-1/mount rw,relatime - ext4 /dev/drbd1000 rw
40 21 0:6 /drbd1001 /var/lib/kubelet/pods/y/volumeDevices/pvc-2 rw - devtmpfs devtmpfs rw
`

// ioctlRecorder captures freeze/thaw ioctls and answers from a scripted
// error queue (nil when exhausted).
type ioctlRecorder struct {
	mu    sync.Mutex
	calls []string
	errs  []error
}

func (r *ioctlRecorder) ioctl(mountpoint string, req uint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := "thaw"
	if req == fiFreeze {
		op = "freeze"
	}
	r.calls = append(r.calls, op+" "+mountpoint)
	if len(r.errs) == 0 {
		return nil
	}
	err := r.errs[0]
	r.errs = r.errs[1:]
	return err
}

func (r *ioctlRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// mountedFreezer builds a Freezer that sees each device mounted at its
// mountpoint, recording freezes and thaws into rec. An empty map is a
// node where nothing is mounted (a Secondary).
func mountedFreezer(rec *ioctlRecorder, mounts map[string]string) *Freezer {
	devs := map[string]uint64{}
	var info strings.Builder
	i := uint32(0)
	for dev, mp := range mounts {
		i++
		devs[dev] = unix.Mkdev(147, i)
		fmt.Fprintf(&info, "%d 1 147:%d / %s rw - ext4 %s rw\n", 35+i, i, mp, dev)
	}
	lines := info.String()
	return &Freezer{
		mountinfo: func() ([]byte, error) { return []byte(lines), nil },
		devNumber: func(path string) (uint64, bool, error) {
			d, ok := devs[path]
			return d, ok, nil
		},
		ioctl: rec.ioctl,
	}
}

func testFreezer(rec *ioctlRecorder) *Freezer {
	return &Freezer{
		mountinfo: func() ([]byte, error) { return []byte(testMountinfo), nil },
		devNumber: func(path string) (uint64, bool, error) {
			if path == devDrbd1000 {
				return unix.Mkdev(147, 0), true, nil
			}
			return 0, false, nil
		},
		ioctl: rec.ioctl,
	}
}

func TestFreezeFreezesTheStagingMount(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	mp, err := f.Freeze(t.Context(), devDrbd1000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(mp, "/globalmount") {
		t.Fatalf("must freeze the first mount of the device, got %q", mp)
	}
	if calls := rec.recorded(); len(calls) != 1 || calls[0] != "freeze "+mp {
		t.Fatalf("unexpected ioctls: %v", calls)
	}
}

func TestFreezeSkipsUnmountedDevice(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	// Not a block device here (a torn-down volume) — never an error.
	mp, err := f.Freeze(t.Context(), "/dev/drbd9999")
	if err != nil || mp != "" {
		t.Fatalf("unmounted device must be a no-op, got %q, %v", mp, err)
	}
	if len(rec.recorded()) != 0 {
		t.Fatalf("no ioctl may run without a mount: %v", rec.recorded())
	}
}

func TestFreezeTreatsAlreadyFrozenAsSuccess(t *testing.T) {
	rec := &ioctlRecorder{errs: []error{unix.EBUSY}}
	f := testFreezer(rec)
	mp, err := f.Freeze(t.Context(), devDrbd1000)
	if err != nil || mp == "" {
		t.Fatalf("EBUSY (already frozen) must count as success, got %q, %v", mp, err)
	}
}

func TestFreezeTimeoutSelfCancels(t *testing.T) {
	rec := &ioctlRecorder{}
	release := make(chan struct{})
	thawed := make(chan struct{})
	f := testFreezer(rec)
	inner := f.ioctl
	f.ioctl = func(mp string, req uint) error {
		if req == fiFreeze {
			<-release
		}
		err := inner(mp, req)
		if req == fiThaw {
			close(thawed)
		}
		return err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := f.Freeze(ctx, devDrbd1000); err == nil {
		t.Fatal("a freeze past the deadline must fail the round")
	}
	// The abandoned ioctl lands later; its success must be thawed away.
	close(release)
	select {
	case <-thawed:
	case <-time.After(5 * time.Second):
		t.Fatal("late freeze was never self-canceled")
	}
}

func TestThawToleratesNotFrozen(t *testing.T) {
	rec := &ioctlRecorder{errs: []error{unix.EINVAL}}
	f := testFreezer(rec)
	mp, err := f.Thaw(devDrbd1000)
	if err != nil {
		t.Fatalf("EINVAL (not frozen) must be a success: %v", err)
	}
	if !strings.HasSuffix(mp, "/globalmount") {
		t.Fatalf("Thaw must report the mountpoint it reached, got %q", mp)
	}
}

func TestThawReportsNoMountpoint(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	// Not mounted: a no-op, but the empty mountpoint must be visible to
	// callers — a freeze leaked on an unmounted device was NOT lifted.
	mp, err := f.Thaw("/dev/drbd9999")
	if err != nil || mp != "" {
		t.Fatalf("unmounted device must report no mountpoint, got %q, %v", mp, err)
	}
	if len(rec.recorded()) != 0 {
		t.Fatalf("no ioctl may run without a mount: %v", rec.recorded())
	}
}

func TestThawSurfacesRealErrors(t *testing.T) {
	rec := &ioctlRecorder{errs: []error{errors.New("io error")}}
	f := testFreezer(rec)
	if _, err := f.Thaw(devDrbd1000); err == nil {
		t.Fatal("a real thaw failure must surface (a frozen workload)")
	}
}

func TestThawMountpointThawsTheStagingMount(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	target := "/var/lib/kubelet/plugins/kubernetes.io/csi/miroir/abc/globalmount"
	if err := f.ThawMountpoint(target); err != nil {
		t.Fatal(err)
	}
	if calls := rec.recorded(); len(calls) != 1 || calls[0] != "thaw "+target {
		t.Fatalf("unexpected ioctls: %v", calls)
	}
}

func TestThawMountpointSkipsNonMountpoint(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	// An unstage retry after the unmount already happened: the target is
	// no longer in mountinfo, and an ioctl against a plain directory would
	// reach whatever filesystem holds it.
	if err := f.ThawMountpoint("/var/lib/kubelet/plugins/kubernetes.io/csi/miroir/gone/globalmount"); err != nil {
		t.Fatal(err)
	}
	if len(rec.recorded()) != 0 {
		t.Fatalf("no ioctl may run against a non-mountpoint: %v", rec.recorded())
	}
}

func TestThawMountpointSkipsNonBlockMounts(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	// A devtmpfs/NFS-style mount (major 0) can never wear a bdev freeze,
	// and opening a dead hard-NFS mountpoint would hang — never touch it.
	if err := f.ThawMountpoint("/var/lib/kubelet/pods/y/volumeDevices/pvc-2"); err != nil {
		t.Fatal(err)
	}
	if len(rec.recorded()) != 0 {
		t.Fatalf("no ioctl may run against a non-block mount: %v", rec.recorded())
	}
}

func TestThawMountpointToleratesNotFrozen(t *testing.T) {
	rec := &ioctlRecorder{errs: []error{unix.EINVAL}}
	f := testFreezer(rec)
	if err := f.ThawMountpoint("/var/lib/kubelet/plugins/kubernetes.io/csi/miroir/abc/globalmount"); err != nil {
		t.Fatalf("EINVAL (not frozen) must be a success: %v", err)
	}
}

func TestThawMountpointSurfacesRealErrors(t *testing.T) {
	rec := &ioctlRecorder{errs: []error{errors.New("io error")}}
	f := testFreezer(rec)
	if err := f.ThawMountpoint("/var/lib/kubelet/plugins/kubernetes.io/csi/miroir/abc/globalmount"); err == nil {
		t.Fatal("a real thaw failure must surface")
	}
}

func TestRawBlockBindNeverMatches(t *testing.T) {
	rec := &ioctlRecorder{}
	f := testFreezer(rec)
	f.devNumber = func(path string) (uint64, bool, error) {
		// The raw-block device node exists, but no filesystem is backed
		// by it — the devtmpfs bind's st_dev is devtmpfs's, not 147:1.
		return unix.Mkdev(147, 1), true, nil
	}
	mp, err := f.Freeze(t.Context(), "/dev/drbd1001")
	if err != nil || mp != "" {
		t.Fatalf("raw-block volumes have nothing to freeze, got %q, %v", mp, err)
	}
}
