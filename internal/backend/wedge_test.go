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
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// stuckSet builds a Wedge whose liveness check reports exactly pids as stuck,
// so a test can retire one by removing it from the set.
func stuckSet(limit int, pids ...int) (*Wedge, map[int]bool) {
	stuck := map[int]bool{}
	for _, p := range pids {
		stuck[p] = true
	}
	w := NewWedge(limit)
	w.isStranded = func(pid int) bool { return stuck[pid] }
	return w, stuck
}

func TestWedgeTripsOnlyAtLimit(t *testing.T) {
	w, _ := stuckSet(3, 1, 2, 3)
	w.record(1, "drbdsetup down pvc-a")
	if w.Tripped() {
		t.Fatalf("one stranded child must not trip the breaker: a per-resource guard already parks that volume")
	}
	w.record(2, "umount /var/lib/kubelet/a")
	if w.Tripped() {
		t.Fatalf("two stranded children must not trip yet, limit is 3")
	}
	w.record(3, "lvm lvcreate --snapshot")
	if !w.Tripped() {
		t.Fatalf("third stranded child must trip: the jam is no longer resource-local")
	}
	if got := w.Stranded(); got != 3 {
		t.Fatalf("Stranded() = %d, want 3", got)
	}
}

func TestWedgeResetsWhenChildrenDrain(t *testing.T) {
	w, stuck := stuckSet(2, 10, 11)
	w.record(10, "drbdsetup down pvc-a")
	w.record(11, "drbdsetup secondary pvc-b")
	if !w.Tripped() {
		t.Fatalf("breaker must be open with 2 stranded children at limit 2")
	}
	// The kernel finally completes one of the two.
	delete(stuck, 11)
	if w.Tripped() {
		t.Fatalf("breaker must reset once a child drains, without an agent restart")
	}
	if got := w.Stranded(); got != 1 {
		t.Fatalf("Stranded() = %d, want 1 after pruning the drained child", got)
	}
}

func TestWedgeRecordIsIdempotentPerPid(t *testing.T) {
	w, _ := stuckSet(2, 7)
	w.record(7, "drbdsetup down pvc-a")
	w.record(7, "drbdsetup down pvc-a")
	w.record(7, "drbdsetup down pvc-a")
	if got := w.Stranded(); got != 1 {
		t.Fatalf("Stranded() = %d, want 1: retries against one pid must not inflate the count", got)
	}
	if w.Tripped() {
		t.Fatalf("re-recording one pid must not trip a limit-2 breaker")
	}
}

func TestWedgeErrNamesStuckCommandsSorted(t *testing.T) {
	w, _ := stuckSet(2, 1, 2)
	w.record(2, "umount /var/lib/kubelet/b")
	w.record(1, "drbdsetup down pvc-a")
	err := w.Err()
	if !errors.Is(err, ErrNodeWedged) {
		t.Fatalf("Err() = %v, want it to wrap ErrNodeWedged", err)
	}
	// Sorted, so a re-emitted Event carries no spurious change.
	want := "drbdsetup down pvc-a; umount /var/lib/kubelet/b"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Err() = %q, want it to name the stuck commands as %q", err, want)
	}
}

// StrandedCommand is the per-resource view: an exact command line, pruned
// like the count so a drained child stops matching.
func TestWedgeStrandedCommandMatchesExactLineUntilDrained(t *testing.T) {
	w, stuck := stuckSet(3, 1, 2)
	w.record(1, "drbdsetup down pvc-a")
	w.record(2, "drbdsetup down pvc-ab")
	if !w.StrandedCommand("drbdsetup down pvc-a") {
		t.Fatal("a recorded stranded down must match its own line")
	}
	if w.StrandedCommand("drbdsetup down pvc-") {
		t.Fatal("a prefix must not match: pvc-a and pvc-ab are different resources")
	}
	delete(stuck, 1)
	if w.StrandedCommand("drbdsetup down pvc-a") {
		t.Fatal("a drained child must stop matching")
	}
	if !w.StrandedCommand("drbdsetup down pvc-ab") {
		t.Fatal("the still-stuck sibling must keep matching")
	}
	var nilWedge *Wedge
	if nilWedge.StrandedCommand("drbdsetup down pvc-a") {
		t.Fatal("a nil breaker reports nothing stranded")
	}
}

func TestWedgeZeroLimitNeverTrips(t *testing.T) {
	w, _ := stuckSet(0, 1, 2, 3, 4)
	for pid := 1; pid <= 4; pid++ {
		w.record(pid, "drbdsetup down")
	}
	if w.Tripped() {
		t.Fatalf("limit 0 must disable tripping while still counting")
	}
	if got := w.Stranded(); got != 4 {
		t.Fatalf("Stranded() = %d, want 4: a disabled breaker still reports", got)
	}
	if err := w.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil while the breaker is closed", err)
	}
}

func TestNilWedgeIsInert(t *testing.T) {
	var w *Wedge
	w.record(1, "drbdsetup down") // must not panic
	if w.Tripped() || w.Stranded() != 0 || w.Err() != nil || w.Commands() != nil {
		t.Fatalf("a nil Wedge must be fully inert, so RealExec keeps working without one")
	}
}

func TestRunnerRefusesToSpawnOnceTripped(t *testing.T) {
	w, _ := stuckSet(1, 5)
	w.record(5, "drbdsetup down pvc-a")
	r := &Runner{Wedge: w}
	// /bin/true would succeed; the breaker must stop it before the fork.
	out, err := r.Run(context.Background(), "true")
	if !errors.Is(err, ErrNodeWedged) {
		t.Fatalf("Run() error = %v, want ErrNodeWedged: a jammed node must not spawn more children", err)
	}
	if out != "" {
		t.Fatalf("Run() out = %q, want empty when refused before the fork", out)
	}
}

func TestRunnerRunsNormallyWhenClosed(t *testing.T) {
	r := &Runner{Wedge: NewWedge(DefaultWedgeLimit)}
	out, err := r.Run(context.Background(), "echo", "ok")
	if err != nil {
		t.Fatalf("Run() error = %v, want nil on a healthy node", err)
	}
	if strings.TrimSpace(out) != "ok" {
		t.Fatalf("Run() out = %q, want %q", out, "ok")
	}
	if got := r.Wedge.Stranded(); got != 0 {
		t.Fatalf("Stranded() = %d, want 0: a clean exit strands nothing", got)
	}
}

// A command killed at its deadline whose child dies on the SIGKILL must NOT
// count, and Run must return as soon as it does: only a child that outlives
// the kill is one the kernel swallowed. The child really is spawned and
// killed here.
func TestRunnerDoesNotRecordChildrenThatDieOnKill(t *testing.T) {
	w := NewWedge(1)
	w.isStranded = func(int) bool { return true }
	r := &Runner{Wedge: w}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := r.Run(ctx, "sleep", "30")
	if err == nil {
		t.Fatalf("Run() error = nil, want the deadline to fail the command")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Run() took %v: a killed child must not wait out the grace", elapsed)
	}
	if got := w.Stranded(); got != 0 {
		t.Fatalf("Stranded() = %d, want 0: a child that died is not stranded", got)
	}
}

// A child that ignores the SIGKILL (uninterruptible sleep, simulated by a
// no-op kill) is recorded once the grace expires and Run returns without
// waiting for it — os/exec's own Wait would block until the child exits.
// The record retires when the child finally exits and is reaped, not on a
// /proc probe: the pid is that child's until then, so it cannot name an
// unrelated task that reused the number.
func TestRunnerRecordsChildrenThatOutliveKillUntilReaped(t *testing.T) {
	w := NewWedge(1)
	w.isStranded = func(int) bool { return true } // only reaping may retire
	r := &Runner{Wedge: w, kill: func(*os.Process) error { return nil }, grace: 50 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	out, err := r.Run(ctx, "sleep", "30")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "still alive") {
		t.Fatalf("Run() error = %v, want the deadline error naming the surviving child", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Run() took %v: a stranded child must not pin the caller", elapsed)
	}
	if out != "" {
		t.Fatalf("Run() out = %q, want empty: the child still owns its output", out)
	}
	if got := w.Stranded(); got != 1 {
		t.Fatalf("Stranded() = %d, want 1", got)
	}
	if !w.StrandedCommand("sleep 30") {
		t.Fatal("the stranded child must be findable by its command line")
	}

	w.mu.Lock()
	var pid int
	for pid = range w.children {
	}
	w.mu.Unlock()
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill %d: %v", pid, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for w.Stranded() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("record must retire once the child is reaped")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A command that succeeds must never record, whatever the kernel state of
// the pid it happened to use.
func TestRunnerDoesNotRecordSuccessfulCommands(t *testing.T) {
	w := NewWedge(1)
	w.isStranded = func(int) bool { return true }
	if _, err := (&Runner{Wedge: w}).Run(context.Background(), "true"); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if got := w.Stranded(); got != 0 {
		t.Fatalf("Stranded() = %d, want 0", got)
	}
}

// comm is unquoted and may contain spaces and parentheses; the state is the
// first field after the FINAL ')'.
func TestParseProcState(t *testing.T) {
	for _, tc := range []struct {
		name string
		line string
		want byte
	}{
		{"plain comm", "1234 (sleep) D 1 1234 1234 0 -1 4194560", 'D'},
		{"comm with parens", "1234 (weird) name) D 1 1234", 'D'},
		{"comm with spaces and parens", "1234 (a ) b (c) R 1 1234", 'R'},
		{"empty comm", "1234 () S 1 1234", 'S'},
		{"zombie", "1234 (gone) Z 1 1234", 'Z'},
		{"no closing paren", "1234 sleep D 1", 0},
		{"nothing after comm", "1234 (sleep)", 0},
		{"empty", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseProcState([]byte(tc.line)); got != tc.want {
				t.Fatalf("parseProcState(%q) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}

func TestProcStateOnRealTasks(t *testing.T) {
	if st := procState(os.Getpid()); st == 0 {
		t.Fatalf("procState(self) = 0, want a real state byte")
	}
	if stranded(os.Getpid()) {
		t.Fatalf("stranded(self) = true, want false: the test process is runnable")
	}
	if st := procState(-1); st != 0 {
		t.Fatalf("procState(-1) = %q, want 0 for a task that cannot exist", st)
	}
}
