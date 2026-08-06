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
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/home-operations/miroir/internal/backend"
)

const (
	assertionWatchInterval = time.Second
	defaultKmsgPath        = "/dev/kmsg"
)

var metricDRBDAssertions = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "miroir_node_drbd_assertions_total",
	Help: "Fatal DRBD kernel assertions sighted in the node kernel log since the agent started. Boot-history replays latch without counting; only assertions sighted after Start increment. Any latch holds the node-scoped command breaker open until reboot.",
})

func init() {
	metrics.Registry.MustRegister(metricDRBDAssertions)
}

// fatalDRBDAssertions are the D_ASSERT signatures from issue #414.
var fatalDRBDAssertions = []string{
	"FAILED in put_ldev",
	"FAILED in drbd_al_begin_io_fastpath",
}

// isFatalDRBDAssertion matches a kernel-origin DRBD assertion with one of
// the fatal signatures. The first kmsg field is (facility<<3)|severity;
// kernel messages have facility 0 (priority 0-7), userspace gets 8+, so
// the priority gate rejects forged lines — a false positive is reboot-only.
func isFatalDRBDAssertion(line string) bool {
	prio, rest, ok := strings.Cut(line, ",")
	if !ok {
		return false
	}
	p, err := strconv.Atoi(prio)
	if err != nil || p >= 8 {
		return false
	}
	if !strings.Contains(rest, "ASSERTION") {
		return false
	}
	for _, sig := range fatalDRBDAssertions {
		if strings.Contains(rest, sig) {
			return true
		}
	}
	return false
}

// AssertionWatcher latches the node breaker on fatal DRBD kernel
// assertions read from /dev/kmsg. Only a reboot clears the underlying
// state, so the latch is permanent for the process.
type AssertionWatcher struct {
	Path     string
	Wedge    *backend.Wedge
	Interval time.Duration
	metric   prometheus.Counter
}

// Start drains the kmsg ring on entry so an assertion from before the
// agent started still latches, then polls until ctx is cancelled.
// Replayed lines latch but do not increment the counter.
func (w *AssertionWatcher) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("drbd-assertions")
	if w.metric == nil {
		w.metric = metricDRBDAssertions
	}
	interval := w.Interval
	if interval <= 0 {
		interval = assertionWatchInterval
	}
	path := w.Path
	if path == "" {
		path = defaultKmsgPath
	}

	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		log.Error(err, "kmsg unavailable; assertions will not trip the breaker this run")
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()
	var partial string
	replay := true
	for {
		if f != nil {
			partial = w.drain(log, f, partial, replay)
			replay = false
		}
		select {
		case <-ctx.Done():
			if f != nil {
				_ = f.Close()
			}
			return nil
		case <-tick.C:
		}
		if f == nil {
			if nf, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
				f = nf
			}
		}
	}
}

func (w *AssertionWatcher) drain(log logr.Logger, f *os.File, partial string, replay bool) string {
	buf := make([]byte, 8192)
	var b strings.Builder
	b.WriteString(partial)
	for range kmsgReadCap {
		n, err := f.Read(buf)
		if n > 0 {
			b.Write(buf[:n])
		}
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				continue
			}
			break
		}
	}
	data := b.String()
	lines := strings.Split(data, "\n")
	if strings.HasSuffix(data, "\n") {
		partial = ""
	} else {
		partial = lines[len(lines)-1]
		lines = lines[:len(lines)-1]
	}
	for _, line := range lines {
		if !isFatalDRBDAssertion(line) {
			continue
		}
		if !replay {
			w.metric.Inc()
		}
		if w.Wedge != nil {
			w.Wedge.Latch("kernel log assertion: " + kmsgPayload(line))
		}
		log.Info("DRBD kernel assertion sighted; node command breaker latched until reboot",
			"record", kmsgPayload(line))
	}
	return partial
}

func kmsgPayload(line string) string {
	if _, rest, ok := strings.Cut(line, ";"); ok {
		return rest
	}
	return line
}
