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
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// ErrNodeWedged marks a command refused because this node's storage stack
// already swallowed enough children to stop making progress. Unlike
// drbd.ErrWedged, which names one resource the kernel cannot tear down, this
// is node-scoped: once the DRBD/LVM/filesystem path jams, commands against
// healthy resources wedge too, so the only safe move is to stop spawning.
// Only a reboot clears the kernel state behind it; an agent restart just
// forgets the count and re-trips once more children strand.
var ErrNodeWedged = errors.New("node storage stack wedged: node reboot required")

// DefaultWedgeLimit is how many concurrently-stranded children trip the
// breaker. Above one because the per-resource guards already park a single
// wedged volume, and tripping the node on it would turn one bad volume into
// a node-wide outage; by the third the jam is no longer resource-local.
const DefaultWedgeLimit = 3

// procState reads a task's scheduler state from /proc, or 0 when the task is
// gone.
func procState(pid int) byte {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	return parseProcState(data)
}

// parseProcState pulls the state character out of a /proc/<pid>/stat line.
// comm (field 2) is unquoted and may contain spaces and parentheses, but
// every field after it is numeric, so the state is the first field following
// the final ')'. Returns 0 for anything malformed.
func parseProcState(data []byte) byte {
	i := bytes.LastIndexByte(data, ')')
	if i < 0 {
		return 0
	}
	fields := strings.Fields(string(data[i+1:]))
	if len(fields) == 0 {
		return 0
	}
	return fields[0][0]
}

// stranded reports whether pid is in uninterruptible sleep, which is what
// separates a swallowed child from a merely slow one: a slow child dies on
// the SIGKILL its deadline sends, a blocked one ignores it and keeps its
// locks. Only 'D' qualifies — a killed child reads 'Z' until Wait collects
// it.
func stranded(pid int) bool { return procState(pid) == 'D' }

// Wedge is a node-scoped circuit breaker over host commands. It counts
// children killed at their deadline whose task never died, and once enough
// are outstanding it refuses to spawn more.
//
// The per-resource guards cannot see this: drbd.Down already refuses a
// second down against a resource wearing the wedge signature (issue #195),
// but that signature belongs to one resource, and a jammed kernel storage
// path strands commands against healthy resources too. Each retry then adds
// a task, so the node degrades instead of failing, until even kubelet's own
// unmounts cannot complete and a graceful reboot is no longer possible.
//
// The count is re-verified from /proc on every read, so a child that drains
// retires itself and the breaker resets without an agent restart.
type Wedge struct {
	// Limit is how many outstanding stranded children trip the breaker;
	// zero disables tripping on stranded children (the count is still
	// tracked and reported). A Latch trips regardless of Limit.
	Limit int

	// isStranded is injectable for tests; nil means the real /proc check.
	isStranded func(pid int) bool

	mu       sync.Mutex
	children map[int]string // pid -> command line that stranded it

	// latched is a pinned-open reason that is not a stranded child.
	latched string
}

// NewWedge returns a breaker tripping at limit outstanding stranded children.
func NewWedge(limit int) *Wedge {
	return &Wedge{Limit: limit, children: map[int]string{}}
}

// Latch pins the breaker open for a fault no draining child can clear —
// a DRBD kernel assertion means a damaged refcount that only a node
// reboot resets. First reason wins: an assertion storm repeats the same
// line endlessly, and the cause does not change after the first.
func (w *Wedge) Latch(reason string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.latched == "" {
		w.latched = reason
	}
}

func (w *Wedge) alive(pid int) bool {
	if w.isStranded != nil {
		return w.isStranded(pid)
	}
	return stranded(pid)
}

// note records pid as stranded if it really is in uninterruptible sleep.
// Runner goes through here rather than calling stranded directly so the
// detection shares the seam Stranded uses and can be tested.
func (w *Wedge) note(pid int, cmd string) {
	if w == nil || !w.alive(pid) {
		return
	}
	w.record(pid, cmd)
}

// record notes that cmd's child stranded. Safe to call for a pid already
// recorded: the map keys on pid, so a retry cannot inflate the count.
func (w *Wedge) record(pid int, cmd string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.children == nil {
		w.children = map[int]string{}
	}
	w.children[pid] = cmd
}

// Stranded reports how many children are still stuck, pruning any that have
// since drained. Pruning on read is what makes the breaker reset itself.
//
// The /proc reads happen outside the lock. This mutex sits in front of every
// host command and every staging unmount on the node, and the tasks being
// read are by construction stuck in the kernel — holding it across their
// /proc files would serialize all storage work behind them.
func (w *Wedge) Stranded() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	pids := make([]int, 0, len(w.children))
	for pid := range w.children {
		pids = append(pids, pid)
	}
	w.mu.Unlock()

	var drained []int
	for _, pid := range pids {
		if !w.alive(pid) {
			drained = append(drained, pid)
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, pid := range drained {
		delete(w.children, pid)
	}
	return len(w.children)
}

// StrandedTripped reports whether the breaker tripped on stranded children,
// not on a latch. cleanupMount consults this so a latched node can still
// drain.
func (w *Wedge) StrandedTripped() bool {
	if w == nil || w.Limit <= 0 {
		return false
	}
	return w.Stranded() >= w.Limit
}

// Tripped reports whether the breaker is open, i.e. new host commands must
// be refused with ErrNodeWedged.
func (w *Wedge) Tripped() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	latched := w.latched
	w.mu.Unlock()
	if latched != "" {
		return true
	}
	return w.StrandedTripped()
}

// Commands lists the stuck commands, for the Event and status message that
// tell an operator which node to reboot and why. Sorted for a stable
// message: an Event that reorders every cycle reads as new information.
func (w *Wedge) Commands() []string {
	if w == nil {
		return nil
	}
	w.Stranded() // prune first, so the list matches the count
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.children)+1)
	for _, cmd := range w.children {
		out = append(out, cmd)
	}
	if w.latched != "" {
		out = append(out, w.latched)
	}
	slices.Sort(out)
	return out
}

// Err returns an ErrNodeWedged naming the stuck commands, or nil when the
// breaker is closed.
func (w *Wedge) Err() error {
	if !w.Tripped() {
		return nil
	}
	return fmt.Errorf("%w (stuck: %s)", ErrNodeWedged, strings.Join(w.Commands(), "; "))
}
