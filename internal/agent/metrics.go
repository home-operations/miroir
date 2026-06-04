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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Exposed on the agent's metrics endpoint; the split-brain gauge is the
// alerting hook for the last-man-standing failure mode (DESIGN §3.2).
var (
	metricUpToDate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "homefs_volume_up_to_date",
		Help: "1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created).",
	}, []string{"volume"})
	metricConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "homefs_volume_connected",
		Help: "1 when all replication links from this node are established.",
	}, []string{"volume"})
	metricSplitBrain = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "homefs_volume_split_brain",
		Help: "1 when DRBD refused to reconnect after divergence; manual resolution required.",
	}, []string{"volume"})
)

func init() {
	metrics.Registry.MustRegister(metricUpToDate, metricConnected, metricSplitBrain)
}

func recordVolumeMetrics(volume string, st homefsReplicaView) {
	metricUpToDate.WithLabelValues(volume).Set(boolGauge(st.upToDate))
	metricConnected.WithLabelValues(volume).Set(boolGauge(st.connected))
	metricSplitBrain.WithLabelValues(volume).Set(boolGauge(st.splitBrain))
}

func dropVolumeMetrics(volume string) {
	metricUpToDate.DeleteLabelValues(volume)
	metricConnected.DeleteLabelValues(volume)
	metricSplitBrain.DeleteLabelValues(volume)
}

type homefsReplicaView struct {
	upToDate   bool
	connected  bool
	splitBrain bool
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
