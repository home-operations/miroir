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

// metricConversions counts auto-diskful conversions by leg kind. Counter,
// not gauge: conversions are one-way topology changes, and the rate view
// ("how often do workloads settle remotely") is the operational question.
var metricConversions = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "miroir_autodiskful_conversions_total",
	Help: "Auto-diskful conversions performed, by leg kind (client: a spec.clients leg replaced by a replica; tiebreaker: a diskless tie-breaker flipped diskful in place).",
}, []string{"kind"})

func init() {
	metrics.Registry.MustRegister(metricConversions)
}
