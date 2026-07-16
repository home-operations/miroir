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
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var lcfg = Config{BaseDir: "/var/lib/miroir"}

func TestLoopfileCreateAttachesNewFile(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/miroir/volumes/pvc-1.img", "",
		errors.New("stat: cannot stat: No such file or directory"))
	fe.respond("losetup -j", "", nil) // not yet attached
	fe.respond("losetup --find --show", "/dev/loop0\n", nil)
	b := newLoopfile(lcfg, fe.run)

	dev, err := b.Create(t.Context(), "pvc-1", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/var/lib/miroir/dev/pvc-1" {
		t.Fatalf("unexpected device path %q", dev)
	}
	// Temp path + rename: a crash between truncate's open and ftruncate
	// must never leave a 0-byte image the retry's exists() accepts.
	fe.calledWith(t, "truncate -s 10737418240 /var/lib/miroir/volumes/pvc-1.img.tmp")
	fe.calledWith(t, "mv /var/lib/miroir/volumes/pvc-1.img.tmp /var/lib/miroir/volumes/pvc-1.img")
	fe.calledWith(t, "losetup --find --show /var/lib/miroir/volumes/pvc-1.img")
	fe.calledWith(t, "losetup --direct-io=on /dev/loop0")
	fe.calledWith(t, "ln -sfn /dev/loop0 /var/lib/miroir/dev/pvc-1")
}

func TestLoopfileCreateIdempotentReusesLoop(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → file exists
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	b := newLoopfile(lcfg, fe.run)

	dev, err := b.Create(t.Context(), "pvc-1", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/var/lib/miroir/dev/pvc-1" {
		t.Fatalf("unexpected device path %q", dev)
	}
	// Existing file is not recreated, and the existing loop is reused
	// (no fresh --find) — only the stable symlink is repointed, which is
	// what makes Create idempotent across reboots.
	fe.notCalledWith(t, "truncate")
	fe.notCalledWith(t, "losetup --find")
	fe.calledWith(t, "ln -sfn /dev/loop3 /var/lib/miroir/dev/pvc-1")
}

func TestLoopfileResizeGrowsAndRefreshesLoop(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat -c %s", "1073741824\n", nil) // 1Gi currently
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Resize(t.Context(), "pvc-1", 10<<30); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "truncate -s 10737418240 /var/lib/miroir/volumes/pvc-1.img")
	fe.calledWith(t, "losetup -c /dev/loop3")
}

func TestLoopfileResizeSkipsWhenBigEnough(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat -c %s", "10737418240\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Resize(t.Context(), "pvc-1", 10<<30); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "truncate")
	fe.notCalledWith(t, "losetup -c")
}

func TestLoopfileSnapshotReflinks(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/miroir/snapshots/snap-1.img", "",
		errors.New("No such file or directory"))
	b := newLoopfile(lcfg, fe.run)

	if err := b.Snapshot(t.Context(), "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "cp --reflink=always /var/lib/miroir/volumes/pvc-1.img /var/lib/miroir/snapshots/snap-1.img.tmp")
	fe.calledWith(t, "mv /var/lib/miroir/snapshots/snap-1.img.tmp /var/lib/miroir/snapshots/snap-1.img")
}

func TestLoopfileSnapshotIdempotent(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → snapshot file exists
	b := newLoopfile(lcfg, fe.run)

	if err := b.Snapshot(t.Context(), "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "cp --reflink")
}

func TestLoopfileCreateFromSnapshot(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/miroir/volumes/pvc-2.img", "",
		errors.New("No such file or directory"))
	fe.respond("losetup -j", "", nil)
	fe.respond("losetup --find --show", "/dev/loop0\n", nil)
	b := newLoopfile(lcfg, fe.run)

	dev, err := b.CreateFromSnapshot(t.Context(), "pvc-2", "pvc-1", "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/var/lib/miroir/dev/pvc-2" {
		t.Fatalf("unexpected device path %q", dev)
	}
	fe.calledWith(t, "cp --reflink=always /var/lib/miroir/snapshots/snap-1.img /var/lib/miroir/volumes/pvc-2.img.tmp")
	fe.calledWith(t, "mv /var/lib/miroir/volumes/pvc-2.img.tmp /var/lib/miroir/volumes/pvc-2.img")
}

func TestLoopfileDeleteDetachesAndRemoves(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → file exists
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "losetup -d /dev/loop3")
	fe.calledWith(t, "rm -f /var/lib/miroir/dev/pvc-1")
	fe.calledWith(t, "rm -f /var/lib/miroir/volumes/pvc-1.img")
}

// A held loop device must be detected BEFORE losetup -d: on modern
// kernels detaching a held device does not fail — it sets lazy AUTOCLEAR
// and returns success — so an error-based guard never fires and Delete
// would destroy an in-use volume's backing file.
func TestLoopfileDeleteBusyWhenStacked(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → file exists
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	// A DRBD attach shows as a stacked child of the loop device.
	fe.respond("lsblk -rno NAME,MOUNTPOINT /dev/loop3", "loop3\ndrbd1000\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy while a stacked device holds the loop, got %v", err)
	}
	fe.notCalledWith(t, "losetup -d")
	fe.notCalledWith(t, "rm -f")
}

func TestLoopfileDeleteBusyWhenMounted(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → file exists
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	fe.respond("lsblk -rno NAME,MOUNTPOINT /dev/loop3",
		"loop3 /var/lib/kubelet/plugins/miroir/staging/pvc-1\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy while the loop device is mounted, got %v", err)
	}
	fe.notCalledWith(t, "losetup -d")
	fe.notCalledWith(t, "rm -f")
}

func TestLoopfileDeleteAbsentIsNoop(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/miroir/volumes/pvc-1.img", "",
		errors.New("No such file or directory"))
	b := newLoopfile(lcfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "losetup -d")
	// Only the crash-leftover .tmp is reaped; the backing file is gone.
	fe.calledWith(t, "rm -f /var/lib/miroir/volumes/pvc-1.img.tmp")
}

func TestLoopfileSetupProbesReflink(t *testing.T) {
	fe := &fakeExec{}
	b := newLoopfile(lcfg, fe.run)

	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "mkdir -p /var/lib/miroir/volumes")
	fe.calledWith(t, "mkdir -p /var/lib/miroir/snapshots")
	fe.calledWith(t, "mkdir -p /var/lib/miroir/dev")
	fe.calledWith(t, "cp --reflink=always /var/lib/miroir/.reflink-probe /var/lib/miroir/.reflink-probe.clone")
}

func TestLoopfileSetupFailsWithoutReflink(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("cp --reflink", "", errors.New("cp: failed to clone: Operation not supported"))
	b := newLoopfile(lcfg, fe.run)

	if err := b.Setup(t.Context()); err == nil {
		t.Fatal("expected Setup to fail on a non-reflink filesystem")
	}
	// The probe must be cleaned up even when the clone fails.
	fe.calledWith(t, "rm -f /var/lib/miroir/.reflink-probe /var/lib/miroir/.reflink-probe.clone")
}

func TestLoopfileStats(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("df -B1 -P", "Filesystem 1B-blocks Used Available Capacity Mounted on\n"+
		"/dev/sda2 2000000000000 500000000000 1500000000000 25% /var/lib/miroir\n", nil)
	b := newLoopfile(lcfg, fe.run)

	s, err := b.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.SizeBytes != 2000000000000 || s.UsedBytes != 500000000000 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestNewSelectsLoopfile(t *testing.T) {
	fe := &fakeExec{}
	if _, err := New("loopfile", lcfg, fe.run); err != nil {
		t.Fatal(err)
	}
}

// After a reboot the persistent symlinks can point at loop devices the
// kernel has since handed to other volumes; Setup prunes the mismatches
// so a stale link can never stage another volume's data.
func TestLoopfileSetupPrunesStaleSymlinks(t *testing.T) {
	base := t.TempDir()
	fe := &fakeExec{}
	// pvc-stale's image is attached nowhere; pvc-live's matches its link.
	fe.respond("losetup -j "+filepath.Join(base, "volumes", "pvc-stale.img"), "", nil)
	fe.respond("losetup -j "+filepath.Join(base, "volumes", "pvc-live.img"),
		"/dev/loop3\n", nil)
	b := newLoopfile(Config{BaseDir: base}, fe.run)

	devDir := filepath.Join(base, "dev")
	if err := os.MkdirAll(devDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{
		"pvc-stale": "/dev/loop0",
		"pvc-live":  "/dev/loop3",
	} {
		if err := os.Symlink(target, filepath.Join(devDir, name)); err != nil {
			t.Fatal(err)
		}
	}

	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(devDir, "pvc-stale")); !os.IsNotExist(err) {
		t.Fatal("stale symlink must be pruned")
	}
	if _, err := os.Lstat(filepath.Join(devDir, "pvc-live")); err != nil {
		t.Fatal("matching symlink must survive")
	}
}
