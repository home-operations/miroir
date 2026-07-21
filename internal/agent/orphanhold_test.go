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
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// heldOpenWithOpeners is the drbdsetup down failure shape from the issue
// #319 field report: the -12 state-change error plus the kernel's opener
// list relayed on stderr.
const heldOpenWithOpeners = `pvc-1: State change failed: (-12) Device is held open by someone
/dev/drbd1422 open_cnt:1, writable:1; list of openers follows
drbd1422 opened by mount (pid 4242424) at 2026-07-20 22:48:32.925`

// unrelatedMountinfo is a mountinfo table that references no DRBD device.
const unrelatedMountinfo = "36 25 0:35 / /run rw - tmpfs tmpfs rw\n"

func TestOpenerPidsParsesReportedOpeners(t *testing.T) {
	got := openerPids(heldOpenWithOpeners + "\ndrbd1422 opened by qemu-system (pid 555) at 2026-07-20 23:00:00.000")
	if len(got) != 2 || got[0] != 4242424 || got[1] != 555 {
		t.Fatalf("want [4242424 555], got %v", got)
	}
	// No opener list (older drbd-utils, or a non-DRBD busy cause) must
	// parse to nothing — the caller then assumes a live consumer.
	if got := openerPids("pvc-1: State change failed: Device is held open by someone"); len(got) != 0 {
		t.Fatalf("want no pids without an opener list, got %v", got)
	}
}

func TestMountinfoListsDevice(t *testing.T) {
	fsMount := []byte("824 25 147:1422 / /var/lib/kubelet/plugins/kubernetes.io/csi/x/globalmount rw - ext4 /dev/drbd1422 rw\n")
	if !mountinfoListsDevice(fsMount, 1422) {
		t.Fatal("a filesystem mount backed by the device must match")
	}
	// A raw-block publish is a devtmpfs bind whose root is the device node.
	bind := []byte("910 25 0:6 /drbd1422 /var/lib/kubelet/pods/u/volumeDevices/x rw - devtmpfs devtmpfs rw\n")
	if !mountinfoListsDevice(bind, 1422) {
		t.Fatal("a raw-block bind of the device node must match")
	}
	other := []byte("36 25 0:35 / /run rw - tmpfs tmpfs rw\n824 25 147:1423 / /mnt rw - ext4 /dev/drbd1423 rw\n")
	if mountinfoListsDevice(other, 1422) {
		t.Fatal("unrelated mounts must not match")
	}
}

// procFixture builds a fake /proc: one numbered dir per pid, each with
// the given mountinfo table.
func procFixture(t *testing.T, mountinfoByPid map[int]string) string {
	t.Helper()
	dir := t.TempDir()
	for pid, mi := range mountinfoByPid {
		pidDir := filepath.Join(dir, strconv.Itoa(pid))
		if err := os.MkdirAll(pidDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(pidDir, "mountinfo"), []byte(mi), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// The scan must see a mount held only inside a container's namespace —
// the force-deleted-pod hazard (#195): the host table is clean while the
// container still runs with the staged filesystem mounted.
func TestDeviceMountedAnywhereSeesContainerNamespaces(t *testing.T) {
	host := unrelatedMountinfo
	container := "824 25 147:1422 / /data rw - ext4 /dev/drbd1422 rw\n"
	dir := procFixture(t, map[int]string{1: host, 4321: container})

	mounted, err := deviceMountedAnywhere(dir, 1422)
	if err != nil || !mounted {
		t.Fatalf("want mounted=true from the container table, got %v / %v", mounted, err)
	}

	clean := procFixture(t, map[int]string{1: host, 4321: host})
	mounted, err = deviceMountedAnywhere(clean, 1422)
	if err != nil || mounted {
		t.Fatalf("want mounted=false with no table listing the device, got %v / %v", mounted, err)
	}
}

func TestDeviceMountedAnywhereSurfacesUnreadableProc(t *testing.T) {
	if _, err := deviceMountedAnywhere(filepath.Join(t.TempDir(), "missing"), 1422); err == nil {
		t.Fatal("an unreadable proc dir must surface as an error, not read as unmounted")
	}
}

func TestPidAlive(t *testing.T) {
	dir := procFixture(t, map[int]string{4321: ""})
	if !pidAlive(dir, 4321) {
		t.Fatal("existing pid dir must read alive")
	}
	if pidAlive(dir, 9999) {
		t.Fatal("missing pid dir must read dead")
	}
}
