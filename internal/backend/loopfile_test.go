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
	"errors"
	"testing"
)

var lcfg = Config{BaseDir: "/var/lib/homefs"}

func TestLoopfileCreateAttachesNewFile(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/homefs/volumes/pvc-1.img", "",
		errors.New("stat: cannot stat: No such file or directory"))
	fe.respond("losetup -j", "", nil) // not yet attached
	fe.respond("losetup --find --show", "/dev/loop0\n", nil)
	b := newLoopfile(lcfg, fe.run)

	dev, err := b.Create(context.Background(), "pvc-1", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/var/lib/homefs/dev/pvc-1" {
		t.Fatalf("unexpected device path %q", dev)
	}
	fe.calledWith(t, "truncate -s 10737418240 /var/lib/homefs/volumes/pvc-1.img")
	fe.calledWith(t, "losetup --find --show /var/lib/homefs/volumes/pvc-1.img")
	fe.calledWith(t, "ln -sfn /dev/loop0 /var/lib/homefs/dev/pvc-1")
}

func TestLoopfileCreateIdempotentReusesLoop(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → file exists
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	b := newLoopfile(lcfg, fe.run)

	dev, err := b.Create(context.Background(), "pvc-1", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/var/lib/homefs/dev/pvc-1" {
		t.Fatalf("unexpected device path %q", dev)
	}
	// Existing file is not recreated, and the existing loop is reused
	// (no fresh --find) — only the stable symlink is repointed, which is
	// what makes Create idempotent across reboots.
	fe.notCalledWith(t, "truncate")
	fe.notCalledWith(t, "losetup --find")
	fe.calledWith(t, "ln -sfn /dev/loop3 /var/lib/homefs/dev/pvc-1")
}

func TestLoopfileResizeGrowsAndRefreshesLoop(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat -c %s", "1073741824\n", nil) // 1Gi currently
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Resize(context.Background(), "pvc-1", 10<<30); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "truncate -s 10737418240 /var/lib/homefs/volumes/pvc-1.img")
	fe.calledWith(t, "losetup -c /dev/loop3")
}

func TestLoopfileResizeSkipsWhenBigEnough(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat -c %s", "10737418240\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Resize(context.Background(), "pvc-1", 10<<30); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "truncate")
	fe.notCalledWith(t, "losetup -c")
}

func TestLoopfileSnapshotReflinks(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/homefs/snapshots/snap-1.img", "",
		errors.New("No such file or directory"))
	b := newLoopfile(lcfg, fe.run)

	if err := b.Snapshot(context.Background(), "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "cp --reflink=always /var/lib/homefs/volumes/pvc-1.img /var/lib/homefs/snapshots/snap-1.img")
}

func TestLoopfileSnapshotIdempotent(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → snapshot file exists
	b := newLoopfile(lcfg, fe.run)

	if err := b.Snapshot(context.Background(), "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "cp --reflink")
}

func TestLoopfileCreateFromSnapshot(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/homefs/volumes/pvc-2.img", "",
		errors.New("No such file or directory"))
	fe.respond("losetup -j", "", nil)
	fe.respond("losetup --find --show", "/dev/loop0\n", nil)
	b := newLoopfile(lcfg, fe.run)

	dev, err := b.CreateFromSnapshot(context.Background(), "pvc-2", "pvc-1", "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/var/lib/homefs/dev/pvc-2" {
		t.Fatalf("unexpected device path %q", dev)
	}
	fe.calledWith(t, "cp --reflink=always /var/lib/homefs/snapshots/snap-1.img /var/lib/homefs/volumes/pvc-2.img")
}

func TestLoopfileDeleteDetachesAndRemoves(t *testing.T) {
	fe := &fakeExec{} // stat succeeds → file exists
	fe.respond("losetup -j", "/dev/loop3\n", nil)
	b := newLoopfile(lcfg, fe.run)

	if err := b.Delete(context.Background(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "losetup -d /dev/loop3")
	fe.calledWith(t, "rm -f /var/lib/homefs/dev/pvc-1")
	fe.calledWith(t, "rm -f /var/lib/homefs/volumes/pvc-1.img")
}

func TestLoopfileDeleteAbsentIsNoop(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("stat /var/lib/homefs/volumes/pvc-1.img", "",
		errors.New("No such file or directory"))
	b := newLoopfile(lcfg, fe.run)

	if err := b.Delete(context.Background(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "losetup -d")
	fe.notCalledWith(t, "rm -f /var/lib/homefs/volumes/pvc-1.img")
}

func TestLoopfileSetupProbesReflink(t *testing.T) {
	fe := &fakeExec{}
	b := newLoopfile(lcfg, fe.run)

	if err := b.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "mkdir -p /var/lib/homefs/volumes")
	fe.calledWith(t, "mkdir -p /var/lib/homefs/snapshots")
	fe.calledWith(t, "mkdir -p /var/lib/homefs/dev")
	fe.calledWith(t, "cp --reflink=always /var/lib/homefs/.reflink-probe /var/lib/homefs/.reflink-probe.clone")
}

func TestLoopfileSetupFailsWithoutReflink(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("cp --reflink", "", errors.New("cp: failed to clone: Operation not supported"))
	b := newLoopfile(lcfg, fe.run)

	if err := b.Setup(context.Background()); err == nil {
		t.Fatal("expected Setup to fail on a non-reflink filesystem")
	}
	// The probe must be cleaned up even when the clone fails.
	fe.calledWith(t, "rm -f /var/lib/homefs/.reflink-probe /var/lib/homefs/.reflink-probe.clone")
}

func TestLoopfileStats(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("df -B1 -P", "Filesystem 1B-blocks Used Available Capacity Mounted on\n"+
		"/dev/sda2 2000000000000 500000000000 1500000000000 25% /var/lib/homefs\n", nil)
	b := newLoopfile(lcfg, fe.run)

	s, err := b.Stats(context.Background())
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
