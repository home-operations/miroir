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

package drbd

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// drbdMajor is DRBD's fixed block-device major number.
const drbdMajor = 147

// minorBase is the first DRBD minor number the agent assigns to miroir
// volumes. Lower numbers may be reserved for system DRBD resources.
const minorBase int32 = 1000

// lockMinors serialises minor.assign access. flock, not an O_EXCL
// sentinel: a sentinel orphaned by a crash between create and remove would
// deadlock every future allocation (nothing sweeps minor.lock). LOCK_EX
// blocks until acquired and the kernel drops it when the fd closes —
// including on SIGKILL/OOM — so a crash cannot wedge allocation. The file
// is intentionally left on disk as the stable lock target.
func (d *Driver) lockMinors() (func(), error) {
	f, err := os.OpenFile(d.path("minor.lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open minor lock: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock minor allocation: %w", err)
	}
	return func() { _ = f.Close() }, nil
}

// releaseMinor drops a volume's minor.assign entry so the minor can be
// reused. Called on teardown; without it the map grows for the lifetime of
// the StateDir. Absent entry (unreplicated volume, or already released) is
// a no-op.
func (d *Driver) releaseMinor(volume string) error {
	unlock, err := d.lockMinors()
	if err != nil {
		return err
	}
	defer unlock()
	assigned, err := d.readAssignments()
	if err != nil {
		return err
	}
	if _, ok := assigned[volume]; !ok {
		return nil
	}
	delete(assigned, volume)
	return d.writeAssignments(assigned)
}

// AllocateMinor assigns a DRBD minor to a volume, returning the same
// minor on future calls. Serialised by an advisory flock the kernel
// releases on close or process death, then persisted via atomic rename.
//
// preferred (0 for none) is the minor the volume's status publishes. It
// outlives StateDir (a node reinstall wipes the directory, not the CR),
// so it is honoured only while no other volume on this node holds it,
// and is recorded like any other assignment so a later allocation cannot
// hand it out again.
func (d *Driver) AllocateMinor(ctx context.Context, volume string, preferred int32) (int32, error) {
	unlock, err := d.lockMinors()
	if err != nil {
		return 0, err
	}
	defer unlock()

	assigned, err := d.readAssignments()
	if err != nil {
		return 0, err
	}
	// The volume's own record stands unless another volume's record holds
	// the same minor; that one is contested like any other claim below.
	m, recorded := assigned[volume]
	delete(assigned, volume)
	if recorded && !slices.Contains(slices.Collect(maps.Values(assigned)), m) {
		return m, nil
	}
	// A .res without an assignment record is our own partially-recorded
	// state (crash between render and record, or a lost minor.assign):
	// recover the volume's minor from its own file instead of assigning a
	// fresh one — the kernel resource may still be running on it.
	if !recorded {
		var ok bool
		if m, ok = d.minorFromOwnRes(volume); !ok {
			m = preferred
		}
	}
	used, err := d.scanUsedMinors(assigned, volume)
	if err != nil {
		return 0, fmt.Errorf("allocate minor for %s: %w", volume, err)
	}
	if m > 0 && used[m] {
		// Two volumes claim the minor, and drbdadm refuses to parse a
		// config directory holding a duplicate, failing every resource on
		// the node. The kernel settles it: the volume running on the
		// minor keeps it and the other claim is dropped, to be replaced
		// when that volume next allocates. Renumbering the running one
		// instead would have adjust delete and recreate its device.
		live, err := d.liveOnMinor(ctx, volume, m)
		if err != nil {
			return 0, fmt.Errorf("allocate minor for %s: %w", volume, err)
		}
		if live {
			maps.DeleteFunc(assigned, func(_ string, held int32) bool { return held == m })
		} else {
			m = 0
		}
	}
	if m <= 0 {
		m = minorBase
		for used[m] {
			m++
		}
	}
	assigned[volume] = m
	if err := d.writeAssignments(assigned); err != nil {
		return 0, err
	}
	return m, nil
}

// liveOnMinor reports whether the kernel runs the volume on the minor.
func (d *Driver) liveOnMinor(ctx context.Context, volume string, minor int32) (bool, error) {
	parsed, err := d.listStatus(ctx, volume)
	if err != nil {
		if isNoSuchResource(err) {
			return false, nil
		}
		return false, err
	}
	for _, res := range parsed {
		if res.Name != volume {
			continue
		}
		for _, dev := range res.Devices {
			if dev.Minor == minor {
				return true, nil
			}
		}
	}
	return false, nil
}

// minorsInRes parses the device minors out of a rendered .res: miroir's
// own form ("device minor N;") and the classic named form
// ("device drbdX minor N;").
func minorsInRes(raw []byte) []int32 {
	var minors []int32
	for line := range strings.SplitSeq(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) >= 3 && f[0] == "device" && f[len(f)-2] == "minor" {
			if n, err := strconv.Atoi(strings.TrimSuffix(f[len(f)-1], ";")); err == nil {
				minors = append(minors, int32(n)) //nolint:gosec // drbd minors are small
			}
		}
	}
	return minors
}

// minorFromOwnRes recovers the minor a previous render of this volume
// already claimed, if its .res survives.
func (d *Driver) minorFromOwnRes(volume string) (int32, bool) {
	raw, err := os.ReadFile(d.path(volume + ".res"))
	if err != nil {
		return 0, false
	}
	if minors := minorsInRes(raw); len(minors) > 0 {
		return minors[0], true
	}
	return 0, false
}

func (d *Driver) readAssignments() (map[string]int32, error) {
	raw, err := os.ReadFile(d.path("minor.assign"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]int32{}, nil
		}
		return nil, err
	}
	m := map[string]int32{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(val)
		if err == nil {
			m[name] = int32(n)
		}
	}
	return m, nil
}

func (d *Driver) writeAssignments(m map[string]int32) error {
	var buf bytes.Buffer
	for name, minor := range m {
		fmt.Fprintf(&buf, "%s %d\n", name, minor)
	}
	return writeFileAtomic(d.path("minor.assign"), buf.Bytes(), 0o640)
}

// scanUsedMinors collects the minors held by every volume but the named
// one: all assignment records (the caller has removed the volume's own)
// and every other volume's rendered .res.
// An unreadable .res is an error, not a free minor: handing its number out
// again would give drbdadm a duplicate.
func (d *Driver) scanUsedMinors(assigned map[string]int32, volume string) (map[int32]bool, error) {
	used := map[int32]bool{}
	for _, m := range assigned {
		used[m] = true
	}
	entries, err := os.ReadDir(d.StateDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, e := range entries {
		if name, found := strings.CutSuffix(e.Name(), ".res"); !found || name == volume {
			continue
		}
		raw, err := os.ReadFile(d.path(e.Name()))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, m := range minorsInRes(raw) {
			used[m] = true
		}
	}
	return used, nil
}

// ensureDeviceNode creates /dev/drbd<minor> if absent (mknod; the agent
// is privileged). Idempotent; refuses to touch a path that exists but is
// not the expected block device.
func (d *Driver) ensureDeviceNode(minor int32) error {
	path := DevicePath(minor)
	want := unix.Mkdev(drbdMajor, uint32(minor)) //nolint:gosec // minor is a small allocator value
	var st unix.Stat_t
	err := unix.Stat(path, &st)
	if err == nil {
		// unix.Stat_t.Rdev is uint64 on linux but int32 on darwin; the cast is a
		// no-op on linux (hence the nolint) but required for the package to build
		// on a macOS dev host.
		if st.Mode&unix.S_IFMT == unix.S_IFBLK && uint64(st.Rdev) == want { //nolint:unconvert
			return nil
		}
		// Wrong rdev or a leftover regular file (e.g. a pre-mknod open
		// landed in tmpfs): replace it.
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale %s: %w", path, err)
		}
	} else if err != unix.ENOENT {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	mknod := d.Mknod
	if mknod == nil {
		mknod = unix.Mknod
	}
	if err := mknod(path, unix.S_IFBLK|0o660, int(want)); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mknod %s: %w", path, err)
	}
	// Enforce perms regardless of umask.
	if err := os.Chmod(path, 0o660); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}
