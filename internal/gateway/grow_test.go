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

package gateway

import (
	"testing"

	"github.com/go-logr/logr"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	fakeexec "k8s.io/utils/exec/testing"
)

// scriptedExec returns a FakeExec whose commands answer, in order, with
// the given actions.
func scriptedExec(script []fakeexec.FakeAction) *fakeexec.FakeExec {
	fcmd := &fakeexec.FakeCmd{CombinedOutputScript: script}
	fe := &fakeexec.FakeExec{}
	for range script {
		fe.CommandScript = append(fe.CommandScript,
			func(cmd string, args ...string) utilexec.Cmd {
				return fakeexec.InitFakeCmd(fcmd, cmd, args...)
			})
	}
	return fe
}

func execOut(s string) fakeexec.FakeAction {
	return func() ([]byte, []byte, error) { return []byte(s), nil, nil }
}

func execExit(code int) fakeexec.FakeAction {
	return func() ([]byte, []byte, error) { return nil, nil, fakeexec.FakeExitError{Status: code} }
}

// newTestGrower builds a grower over a scripted exec.
func newTestGrower(script []fakeexec.FakeAction) (*grower, *fakeexec.FakeExec) {
	fe := scriptedExec(script)
	m := mount.NewSafeFormatAndMount(mount.NewFakeMounter(nil), fe)
	return newGrower(m, "/dev/drbd1000", "/export/pvc-rwx", logr.Discard()), fe
}

// The first tick grows once (blkid + resize2fs) to establish the size the
// filesystem fills; every later tick is one blockdev call and nothing else
// while the device holds its size. The pre-fix loop ran resize2fs every
// poll because NeedResize answers true unconditionally for ext4/xfs.
func TestGrowerUnchangedDeviceResizesOnlyOnce(t *testing.T) {
	g, fe := newTestGrower([]fakeexec.FakeAction{
		execOut("1073741824\n"), // tick 1: blockdev
		execOut("TYPE=ext4\n"),  // Resize: blkid
		execOut(""),             // Resize: resize2fs
		execOut("1073741824\n"), // tick 2
		execOut("1073741824\n"), // tick 3
	})
	g.tick()
	if g.size != 1073741824 || fe.CommandCalls != 3 {
		t.Fatalf("first tick must grow once: size=%d calls=%d", g.size, fe.CommandCalls)
	}
	g.tick()
	g.tick()
	if fe.CommandCalls != 5 {
		t.Fatalf("exec ran %d commands, want 5: unchanged ticks are blockdev only", fe.CommandCalls)
	}
}

// A grown device gets exactly one resize, after which the new size is the
// baseline and later ticks are blockdev-only again.
func TestGrowerGrownDeviceResizesOnce(t *testing.T) {
	g, fe := newTestGrower([]fakeexec.FakeAction{
		execOut("2147483648\n"), // tick 1: device doubled
		execOut("TYPE=ext4\n"),  // Resize: blkid
		execOut(""),             // Resize: resize2fs
		execOut("2147483648\n"), // tick 2: unchanged
	})
	g.size = 1073741824
	g.tick()
	if fe.CommandCalls != 3 {
		t.Fatalf("exec ran %d commands after the grow, want 3", fe.CommandCalls)
	}
	if g.size != 2147483648 {
		t.Fatalf("size = %d, want the grown device size recorded", g.size)
	}
	g.tick()
	if fe.CommandCalls != 4 {
		t.Fatalf("exec ran %d commands, want 4: the second tick must not resize again", fe.CommandCalls)
	}
}

// A failed grow keeps the old baseline so the next tick retries; a failed
// size read is skipped without touching the filesystem.
func TestGrowerRetriesAfterFailure(t *testing.T) {
	g, fe := newTestGrower([]fakeexec.FakeAction{
		execOut("2147483648\n"), // tick 1
		execOut("TYPE=ext4\n"),  // blkid
		execExit(1),             // resize2fs fails
		execExit(1),             // tick 2: blockdev fails
		execOut("2147483648\n"), // tick 3
		execOut("TYPE=ext4\n"),  // blkid
		execOut(""),             // resize2fs succeeds
	})
	g.size = 1073741824
	g.tick()
	if g.size != 1073741824 {
		t.Fatalf("a failed grow must keep the baseline, got %d", g.size)
	}
	g.tick()
	if fe.CommandCalls != 4 {
		t.Fatalf("a failed size read must not resize, ran %d commands want 4", fe.CommandCalls)
	}
	g.tick()
	if g.size != 2147483648 || fe.CommandCalls != 7 {
		t.Fatalf("retry must grow and record: size=%d calls=%d", g.size, fe.CommandCalls)
	}
}

// Resize answers (false, nil) when blkid cannot read a filesystem type
// (exit 2): nothing was grown, so the size must not advance or the
// expansion would be lost until the next one.
func TestGrowerUnreadableTypeKeepsRetrying(t *testing.T) {
	g, fe := newTestGrower([]fakeexec.FakeAction{
		execOut("2147483648\n"), // tick 1
		execExit(2),             // blkid: no type read
		execOut("2147483648\n"), // tick 2
		execOut("TYPE=ext4\n"),  // blkid
		execOut(""),             // resize2fs
	})
	g.size = 1073741824
	g.tick()
	if g.size != 1073741824 || fe.CommandCalls != 2 {
		t.Fatalf("a no-op Resize must keep the baseline: size=%d calls=%d", g.size, fe.CommandCalls)
	}
	g.tick()
	if g.size != 2147483648 || fe.CommandCalls != 5 {
		t.Fatalf("retry must grow and record: size=%d calls=%d", g.size, fe.CommandCalls)
	}
}
