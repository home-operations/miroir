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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// drbdMajor is DRBD's registered block major (LANANA, drivers/block/drbd).
// Mountinfo rows are matched against it by number so no device node needs
// to be stat'ed.
const drbdMajor = 147

// openerPidRE pulls the pids out of the opener list drbdsetup prints when
// a state change fails with "Device is held open by someone" (-12), e.g.
// "drbd1422 opened by mount (pid 149800) at 2026-07-20 22:48:32". The
// format is the kernel's drbd_report_openers line, relayed on stderr by
// drbd-utils; the same lines appear in the issue #319 field report.
var openerPidRE = regexp.MustCompile(`\bopened by .* \(pid (\d+)\)`)

// openerPids parses the opener pids out of a held-open failure. Empty
// when the tooling did not report openers — callers must then assume a
// live consumer.
func openerPids(msg string) []int {
	var pids []int
	for _, m := range openerPidRE.FindAllStringSubmatch(msg, -1) {
		if pid, err := strconv.Atoi(m[1]); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}

// pidAlive reports whether pid still exists under procDir. A recycled pid
// reads as alive — the safe direction: the caller keeps waiting instead
// of reclaiming.
func pidAlive(procDir string, pid int) bool {
	_, err := os.Stat(filepath.Join(procDir, strconv.Itoa(pid)))
	return err == nil
}

// deviceMountedAnywhere reports whether /dev/drbd<minor> backs a
// filesystem mount or appears as a raw-block bind in any mount namespace
// visible under procDir. The agent runs with hostPID, so walking every
// process's mountinfo covers container namespaces too — a force-deleted
// pod's container can keep the staged filesystem mounted in its own
// namespace after the host paths are gone, and that is a live consumer,
// not an orphan (#195: never destroy a backing under a consumer).
// Namespaces are deduped via ns/mnt so each table is read once. Every
// error fails closed — "could not check" must never read as "not
// mounted" — except a process gone mid-scan: reaped reads ENOENT, a
// zombie EINVAL (its mount namespace is already released), and neither
// can hold a mount.
func deviceMountedAnywhere(procDir string, minor int32) (bool, error) {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return false, err
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		ns, nsErr := os.Readlink(filepath.Join(procDir, e.Name(), "ns", "mnt"))
		if nsErr == nil && seen[ns] {
			continue
		}
		data, err := os.ReadFile(filepath.Join(procDir, e.Name(), "mountinfo"))
		if err != nil {
			if os.IsNotExist(err) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ESRCH) {
				continue
			}
			return false, err
		}
		// Mark the namespace covered only now: a process that exited
		// between the readlink and the read must not retire its shared
		// namespace unread, or a surviving sibling's table — possibly the
		// one listing the device — would be skipped as a duplicate.
		if nsErr == nil {
			seen[ns] = true
		}
		if mountinfoListsDevice(data, minor) {
			return true, nil
		}
	}
	return false, nil
}

// hasLiveOpener reports whether the busy-teardown cause originates from a
// live opener — at least one reported opener PID is still alive — as opposed
// to a still-mounted device whose openers have exited, a scan error, or a
// cause without opener information. It is the live-opener gate for the
// force-detach escalation: force-detaching under a mounted device destroys a
// backing under a consumer (#195), so only a live opener PID warrants the
// escalation.
func hasLiveOpener(procDir, msg string) bool {
	if !strings.Contains(msg, "held open") {
		return false
	}
	pids := openerPids(msg)
	if len(pids) == 0 {
		return false
	}
	for _, pid := range pids {
		if pidAlive(procDir, pid) {
			return true
		}
	}
	return false
}

// /dev/drbd<minor>: a filesystem mount carries the device's major:minor
// in field 3 (the mounted fs's st_dev), and a raw-block publish is a
// devtmpfs bind whose root (field 4) is the device node's path.
func mountinfoListsDevice(data []byte, minor int32) bool {
	fsDev := fmt.Sprintf("%d:%d", drbdMajor, minor)
	bindRoot := fmt.Sprintf("/drbd%d", minor)
	for line := range strings.Lines(string(data)) {
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		if f[2] == fsDev || f[3] == bindRoot {
			return true
		}
	}
	return false
}
