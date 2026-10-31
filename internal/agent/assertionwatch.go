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
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/home-operations/miroir/internal/backend"
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
// the fatal signatures; kernelPayload's priority gate rejects forged lines,
// since a false positive is reboot-only.
func isFatalDRBDAssertion(line string) bool {
	rest, ok := kernelPayload(line)
	if !ok || !strings.Contains(rest, "ASSERTION") {
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
	tailKmsg(ctx, log, w.Path, w.Interval, func(lines []string, replay bool) {
		w.handle(log, lines, replay)
	})
	return nil
}

func (w *AssertionWatcher) handle(log logr.Logger, lines []string, replay bool) {
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
}
