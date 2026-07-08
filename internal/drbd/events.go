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
	"bufio"
	"context"
	"io"
	"os/exec"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// EventWatcher streams `drbdsetup events2 all` and reports which resource
// each kernel state change belongs to, so role/disk/connection transitions
// reach the reconciler without waiting for the next poll.
type EventWatcher struct {
	// Notify runs on the scanning goroutine and must honor ctx — a
	// blocked delivery (full channel at shutdown) would wedge the scan.
	Notify func(ctx context.Context, resource string)
}

// restartBackoff spaces respawns of a dying events2 process (e.g. the
// DRBD module not loaded yet) so the agent doesn't hot-loop.
const restartBackoff = 5 * time.Second

// Start runs the events2 stream until ctx is done, restarting the process
// when it exits. Implements manager.Runnable.
func (w *EventWatcher) Start(ctx context.Context) error {
	log := ctrl.Log.WithName("drbd-events")
	for ctx.Err() == nil {
		cmd := exec.CommandContext(ctx, "drbdsetup", "events2", "all")
		out, err := cmd.StdoutPipe()
		if err == nil {
			if err = cmd.Start(); err != nil {
				// Unstarted: the restart loop would leak one pipe fd
				// per attempt.
				_ = out.Close()
			}
		}
		if err == nil {
			err = w.scan(ctx, out)
			if werr := cmd.Wait(); err == nil {
				err = werr
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Error(err, "events2 stream ended; restarting")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(restartBackoff):
		}
	}
	return nil
}

// scan reads events2 lines until EOF and returns the scanner error, if any.
// The 1 MiB buffer lifts bufio.Scanner's 64 KiB default token cap so an
// unusually long line cannot silently end the stream; a surfaced error lets
// Start log it and respawn rather than the pipe filling and wedging Wait.
func (w *EventWatcher) scan(ctx context.Context, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if name := parseEvent2(scanner.Text()); name != "" {
			w.Notify(ctx, name)
		}
	}
	return scanner.Err()
}

// parseEvent2 extracts the resource name from one events2 line, or ""
// for lines carrying no resource state. Lines look like:
//
//	exists resource name:pvc-1 role:Secondary suspended:no
//	change connection name:pvc-1 peer-node-id:1 connection:StandAlone
//	exists -
//
// helper invocations (call/response) and the initial-dump terminator
// ("exists -") are skipped.
func parseEvent2(line string) string {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return ""
	}
	switch fields[0] {
	case "exists", "create", "change", "destroy":
	default:
		return ""
	}
	for _, f := range fields[2:] {
		if name, ok := strings.CutPrefix(f, "name:"); ok {
			return name
		}
	}
	return ""
}
