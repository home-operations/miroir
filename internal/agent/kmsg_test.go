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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// newKmsgFIFO stands in for /dev/kmsg: a FIFO is pollable like the char
// device, so os.File registers it with the netpoller, and the writer held
// open means a drained read parks instead of seeing EOF. A regular file
// never reaches that path.
func newKmsgFIFO(t *testing.T) (string, *os.File) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kmsg")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return path, w
}

func TestCaptureKmsgFiltersAndCaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	var b strings.Builder
	b.WriteString("6,100,1;drbd pvc-other: Connection established\n")
	for i := range 40 {
		fmt.Fprintf(&b, "6,%d,1;drbd pvc-1: record %d\n", 200+i, i)
	}
	b.WriteString("6,300,1;drbd pvc-1: Split-Brain detected but unresolved, dropping connection!\n")
	if err := os.WriteFile(path, []byte(b.String()), 0o640); err != nil {
		t.Fatal(err)
	}

	lines := captureKmsg(path, "pvc-1", 30)
	if len(lines) != 30 {
		t.Fatalf("want 30 lines (cap), got %d", len(lines))
	}
	for _, l := range lines {
		if strings.Contains(l, "pvc-other") {
			t.Fatalf("foreign resource leaked through the filter: %q", l)
		}
	}
	if last := lines[len(lines)-1]; !strings.Contains(last, "Split-Brain detected") {
		t.Fatalf("the newest record must survive the cap, got %q", last)
	}
}

func TestCaptureKmsgMissingFile(t *testing.T) {
	if lines := captureKmsg(filepath.Join(t.TempDir(), "absent"), "pvc-1", 30); lines != nil {
		t.Fatalf("missing file must yield nil, got %v", lines)
	}
}

func TestCaptureKmsgReturnsOnDrainedPollableSource(t *testing.T) {
	path, w := newKmsgFIFO(t)
	if _, err := w.WriteString("6,1,1,-;drbd pvc-1: conn( Connecting -> StandAlone )\n" +
		"6,2,2,-;drbd pvc-1: Split-Brain detected but unresolved, dropping connection!\n"); err != nil {
		t.Fatal(err)
	}

	done := make(chan []string, 1)
	go func() { done <- captureKmsg(path, "pvc-1", 1) }()
	select {
	case lines := <-done:
		if len(lines) != 1 || !strings.Contains(lines[0], "Split-Brain detected") {
			t.Fatalf("want the newest buffered record, got %v", lines)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("captureKmsg must return once the buffered records are drained, not wait for the next one")
	}
}
