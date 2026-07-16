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

package membership

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Leg kinds labelling the auto-diskful and auto-evict counters.
const (
	kindReplica    = "replica"
	kindTieBreaker = "tiebreaker"
	kindClient     = "client"
)

// metricConversions counts auto-diskful conversions by leg kind. Counter,
// not gauge: conversions are one-way topology changes, and the rate view
// ("how often do workloads settle remotely") is the operational question.
var metricConversions = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "miroir_autodiskful_conversions_total",
	Help: "Auto-diskful conversions performed, by leg kind (client: a spec.clients leg replaced by a replica; tiebreaker: a diskless tie-breaker flipped diskful in place).",
}, []string{"kind"})

// metricEvictions counts auto-evict swaps by evicted leg kind. Each one
// is a topology change an operator would otherwise have made by hand —
// worth a durable trail beyond the per-volume event.
var metricEvictions = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "miroir_autoevict_evictions_total",
	Help: "Auto-evict swaps performed, by evicted leg kind (replica: a diskful replica re-placed; tiebreaker: a diskless tie-breaker re-placed; client: a dead consumer's leg dropped).",
}, []string{"kind"})

// metricEvictStanddown counts passes where eviction refused to act on a
// stale node. A rising multiple_stale rate flags an observer-side
// problem; peer_connected flags a node cut off from the API server but
// not from its peers; node_ready flags a broken agent on a live node.
var metricEvictStanddown = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "miroir_autoevict_standdown_total",
	Help: "Auto-evict passes that stood down instead of evicting, by reason (multiple_stale: more than one node's heartbeat is stale; peer_connected: a survivor still holds DRBD links to the stale node; node_ready: the node's kubelet still heartbeats, only the agent is gone).",
}, []string{"reason"})

func init() {
	metrics.Registry.MustRegister(metricConversions, metricEvictions, metricEvictStanddown)
}
