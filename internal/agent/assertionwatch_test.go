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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/home-operations/miroir/internal/backend"
)

func TestIsFatalDRBDAssertion(t *testing.T) {
	cases := []struct {
		line string
		want bool
	}{
		{"7,158805,16504921346,-;drbd pvc-7ddfe328/0 drbd1006: ASSERTION i >= 0 FAILED in put_ldev", true},
		{"3,155901,15315301767,-;drbd pvc-1/0 drbd1004: ASSERTION atomic_read(&device->local_cnt) > 0 FAILED in drbd_al_begin_io_fastpath", true},
		{"6,158312,-,-;drbd pvc-1: role( Primary -> Secondary ) [mount:100 auto-promote]", false},
		{"7,12157,2155463424,-;audit: rate limit exceeded", false},
		{"6,12157,-,-;FAT-fs (nvme0n1p1): Filesystem has been set read-only", false},
		{"3,1,1,-;drbd pvc-1/0 drbd1000: ASSERTION i >= 0 in put_ldev", false},
		{"3,1,1,-;drbd pvc-2/0 drbd1001: ASSERTION j < max FAILED in some_other_fn", false},
		{"8,1,1,-;drbd pvc-x: ASSERTION i >= 0 FAILED in put_ldev", false},
		{"14,1,1,-;drbd pvc-x: ASSERTION i >= 0 FAILED in drbd_al_begin_io_fastpath", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isFatalDRBDAssertion(tc.line); got != tc.want {
			t.Errorf("isFatalDRBDAssertion(%q) = %v, want %v", tc.line, got, tc.want)
		}
	}
}

func TestAssertionWatcherTripsTheWedge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	initial := "6,1,1,-;drbd pvc-a: Connection established\n" +
		"3,2,2,-;drbd pvc-a/0 drbd1000: ASSERTION i >= 0 FAILED in put_ldev\n"
	if err := os.WriteFile(path, []byte(initial), 0o640); err != nil {
		t.Fatal(err)
	}

	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_drbd_assertions"})
	w := backend.NewWedge(backend.DefaultWedgeLimit)
	aw := &AssertionWatcher{
		Path:     path,
		Wedge:    w,
		Interval: 10 * time.Millisecond,
		metric:   counter,
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- aw.Start(ctx) }()

	deadline := time.After(5 * time.Second)
	for !w.Tripped() {
		select {
		case <-deadline:
			t.Fatal("the watcher must latch the wedge on a boot-history assertion")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := w.Err(); err == nil || !strings.Contains(err.Error(), "ASSERTION") {
		t.Fatalf("the breaker refusal must quote the assertion, got %v", err)
	}
	if got := testutil.ToFloat64(counter); got != 0 {
		t.Fatalf("assertion counter = %v, want 0: boot-history replay must not count", got)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("3,3,3,-;drbd pvc-b/0 drbd1001: ASSERTION j < max FAILED in drbd_al_begin_io_fastpath\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for testutil.ToFloat64(counter) != 1 {
		select {
		case <-deadline:
			t.Fatal("the watcher must count assertions sighted after Start")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := w.Commands(); len(got) != 1 || !strings.Contains(got[0], "put_ldev") {
		t.Fatalf("Commands() = %v, want the first assertion kept as the reason", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start() = %v, want nil on shutdown", err)
	}
}
