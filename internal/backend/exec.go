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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Exec runs a CLI command and returns its combined output. Injectable so
// backends are unit-testable without lvm/zfs installed.
type Exec func(ctx context.Context, name string, args ...string) (string, error)

// RealExec executes commands on the host. The agent container runs with the
// host namespaces, so lvm/zfs act on the node's devices directly.
func RealExec(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	// A child stuck in D-state (wedged pool, frozen dm device) ignores the
	// SIGKILL from ctx cancellation and would pin CombinedOutput on its
	// open pipes forever; WaitDelay forces Wait to give up on them.
	cmd.WaitDelay = 10 * time.Second
	// Force the C locale: the delete/exists classifiers match lvm/zfs error
	// text ("in use", "Failed to find", …), which the tools localise.
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s",
			name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// busy classifies a delete/destroy failure: it wraps err as ErrBusy when the
// cause clears on its own — the device is still open, or (zfs) snapshots or
// restore clones must go first — so the caller retries. Other errors pass
// through unchanged and are treated as permanent. Returns nil for nil.
func busy(err error) error {
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
