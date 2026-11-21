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
	"context"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

// The scanner's 1 MiB buffer lifts bufio's 64 KiB token cap: a single
// overlong line must not end the stream, and events on both sides of it
// must still be delivered.
func TestScanSurvivesLongLines(t *testing.T) {
	var got []string
	w := &EventWatcher{Notify: func(_ context.Context, name string) {
		got = append(got, name)
	}}
	input := "exists resource name:" + volPvc1 + " role:Secondary\n" +
		"change - " + strings.Repeat("x", 500*1024) + "\n" +
		"exists resource name:" + volPvc2 + " role:Secondary\n"
	if err := w.scan(t.Context(), strings.NewReader(input)); err != nil {
		t.Fatalf("a long line within the buffer must not error: %v", err)
	}
	if !slices.Equal(got, []string{volPvc1, volPvc2}) {
		t.Fatalf("events on both sides of the long line must deliver: %v", got)
	}

	// Past the 1 MiB cap the scanner errors, so Start respawns the stream.
	if err := w.scan(t.Context(), strings.NewReader(strings.Repeat("y", 2<<20))); err == nil {
		t.Fatal("an over-cap line must surface the scanner error")
	}
}

// A scanner error leaves nobody reading the pipe: the child must be killed,
// or it blocks on its next write and the watcher never respawns.
func TestStreamKillsChildAfterScanError(t *testing.T) {
	w := &EventWatcher{Notify: func(context.Context, string) {}}
	cmd := exec.CommandContext(t.Context(), "sh", "-c",
		`head -c 3000000 /dev/zero | tr '\0' y; exec sleep 60`)
	done := make(chan error, 1)
	go func() { done <- w.stream(t.Context(), cmd) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("an over-cap line must surface the scanner error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not return after the scanner error")
	}
}
