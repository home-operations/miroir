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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Exec runs a CLI command and returns its combined output. Injectable so
// backends are unit-testable without lvm/zfs installed.
type Exec func(ctx context.Context, name string, args ...string) (string, error)

// execTimeout bounds a single host command. Every miroir CLI call (lvm,
// zfs, drbdadm, drbdsetup, losetup, blockdev) is a metadata operation
// that completes in well under a second on healthy hardware; the only way
// to exceed this is a genuinely wedged device or pool. Reconcile contexts
// have no deadline of their own, so without this bound a child stuck in
// D-state pins the single reconcile worker forever and head-of-line-blocks
// every other volume on the node.
const execTimeout = 2 * time.Minute

// CommandLine is the line a Wedge records a stranded child under; callers
// asking Wedge.StrandedCommand about a specific command build it here so
// the two cannot drift.
func CommandLine(name string, args ...string) string {
	return strings.TrimSpace(name + " " + strings.Join(args, " "))
}

// RealExec executes commands on the host without wedge tracking. The agent
// container runs with the host namespaces, so lvm/zfs act on the node's
// devices directly. Callers with a breaker to share use Runner instead.
func RealExec(ctx context.Context, name string, args ...string) (string, error) {
	return (&Runner{}).Run(ctx, name, args...)
}

// killGrace is how long a child killed at its deadline gets to actually
// die before Run stops waiting for it. A killable child is gone within
// milliseconds of SIGKILL; only a task in uninterruptible sleep outlives
// it, and that is the stranded child the breaker exists to count.
const killGrace = 10 * time.Second

// Runner executes host commands, reporting every child the kernel refuses to
// let die to a node-scoped Wedge. Its Run method has the Exec signature, so
// it drops in wherever RealExec is injected.
type Runner struct {
	// Wedge both gates and records: commands are refused once it has
	// tripped, and stranded children are reported to it. Nil disables both.
	Wedge *Wedge

	// kill is injectable for tests, where a no-op stands in for a child
	// that ignores SIGKILL; nil means (*os.Process).Kill.
	kill func(*os.Process) error
	// grace overrides killGrace in tests; zero means killGrace.
	grace time.Duration
}

// Run executes name with args and returns its combined output. Once the
// breaker is open it refuses to spawn: the new child would strand too, and
// each one holds more locks and pushes the node further from a graceful
// reboot.
//
// Wait is not bounded by the context: os/exec waits for the child to exit,
// and a child in uninterruptible sleep never acts on the SIGKILL, so a
// stranded child would pin the caller for as long as the kernel holds it.
// Run therefore waits in a goroutine, and once the deadline's kill has gone
// unanswered for killGrace it records the child as stranded and returns.
// The goroutine keeps waiting and retires the record when the child finally
// exits: the pid belongs to that unreaped child until then, so the record
// can never describe some unrelated task that reused the number.
func (r *Runner) Run(ctx context.Context, name string, args ...string) (string, error) {
	line := CommandLine(name, args...)
	if err := r.Wedge.Err(); err != nil {
		return "", fmt.Errorf("%s: %w", line, err)
	}
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	// Bounds the pipe drain after the child exits: a grandchild it left
	// behind holding the pipes would otherwise keep Wait from returning.
	cmd.WaitDelay = killGrace
	// Force the C locale: the delete/exists classifiers match lvm/zfs error
	// text ("in use", "Failed to find", …), which the tools localise.
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("%s: %w", line, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	finish := func(err error) (string, error) {
		if err != nil {
			return out.String(), fmt.Errorf("%s: %w: %s", line, err, strings.TrimSpace(out.String()))
		}
		return out.String(), nil
	}
	select {
	case err := <-done:
		return finish(err)
	case <-ctx.Done():
	}
	// ErrProcessDone here means the child won the race with the deadline;
	// Wait then returns immediately below.
	_ = r.doKill(cmd.Process)
	select {
	case err := <-done:
		return finish(err)
	case <-time.After(r.killGrace()):
	}
	pid := cmd.Process.Pid
	r.Wedge.record(pid, line)
	go func() {
		<-done
		r.Wedge.retire(pid)
	}()
	// out is not read here: the copy goroutine still owns it until Wait
	// returns.
	return "", fmt.Errorf("%s: %w: child %d still alive after SIGKILL (uninterruptible sleep)", line, ctx.Err(), pid)
}

func (r *Runner) doKill(p *os.Process) error {
	if r.kill != nil {
		return r.kill(p)
	}
	return p.Kill()
}

func (r *Runner) killGrace() time.Duration {
	if r.grace > 0 {
		return r.grace
	}
	return killGrace
}

// Busy classifies a delete/destroy/down failure: it wraps err as ErrBusy
// when the cause clears on its own — the device is still open, or (zfs)
// snapshots or restore clones must go first — so the caller retries. Other
// errors pass through unchanged and are treated as permanent. Returns nil
// for nil. Exported so the agent can classify drbdsetup down failures the
// same way (a still-staged device answers "held open").
func Busy(err error) error {
	if err == nil {
		return nil
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "held open"),
		strings.Contains(s, "busy"),
		strings.Contains(s, "in use"),
		strings.Contains(s, "has children"),     // zfs: snapshots exist
		strings.Contains(s, "dependent clones"): // zfs: restore clones exist
		return fmt.Errorf("%w: %v", ErrBusy, err)
	}
	return err
}
