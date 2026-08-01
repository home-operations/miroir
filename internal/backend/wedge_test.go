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

// A command killed at its deadline whose child dies normally must NOT count:
// only uninterruptible sleep means the kernel swallowed it. Without this
// distinction any command that outran its deadline would trip the breaker.
// The child really is spawned and killed here — the liveness check is stubbed,
// not the process.
func TestRunnerRecordsOnlyChildrenLeftInDState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		inDState bool
		want     int
	}{
		{"child left in D-state is recorded", true, 1},
		{"child that died is not recorded", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var probed []int
			w := NewWedge(1)
			w.isStranded = func(pid int) bool {
				probed = append(probed, pid)
				return tc.inDState
			}
			r := &Runner{Wedge: w}
			// A real child, killed by a deadline it cannot meet.
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if _, err := r.Run(ctx, "sleep", "30"); err == nil {
				t.Fatalf("Run() error = nil, want the deadline to fail the command")
			}
			if len(probed) == 0 {
				t.Fatalf("a killed command must have its child's state probed")
			}
			if got := w.Stranded(); got != tc.want {
				t.Fatalf("Stranded() = %d, want %d", got, tc.want)
			}
		})
	}
}

// A command that succeeds must never probe or record, whatever the kernel
// state of the pid it happened to use.
func TestRunnerDoesNotProbeSuccessfulCommands(t *testing.T) {
	w := NewWedge(1)
	probed := false
	w.isStranded = func(int) bool { probed = true; return true }
	if _, err := (&Runner{Wedge: w}).Run(context.Background(), "true"); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if probed {
		t.Fatalf("a successful command must not be probed for stranding")
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
