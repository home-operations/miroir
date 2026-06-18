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
const volumeLabel = "volume"

var (
	metricUpToDate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_up_to_date",
		Help: "1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created).",
	}, []string{volumeLabel})
	metricConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_connected",
		Help: "1 when all replication links from this node are established.",
	}, []string{volumeLabel})
	metricSplitBrain = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_split_brain",
		Help: "1 when DRBD refused to reconnect after divergence; manual resolution required.",
	}, []string{volumeLabel})
	metricSuspended = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_suspended",
		Help: "1 while suspend-io holds this node's write barrier; sustained means a stranded barrier (snapshot rounds last seconds).",
	}, []string{volumeLabel})
)

func init() {
	metrics.Registry.MustRegister(metricUpToDate, metricConnected, metricSplitBrain, metricSuspended)
}

func recordVolumeMetrics(volume string, st miroirReplicaView) {
	metricUpToDate.WithLabelValues(volume).Set(boolGauge(st.upToDate))
	metricConnected.WithLabelValues(volume).Set(boolGauge(st.connected))
	metricSplitBrain.WithLabelValues(volume).Set(boolGauge(st.splitBrain))
	metricSuspended.WithLabelValues(volume).Set(boolGauge(st.suspended))
}

func dropVolumeMetrics(volume string) {
	metricUpToDate.DeleteLabelValues(volume)
	metricConnected.DeleteLabelValues(volume)
	metricSplitBrain.DeleteLabelValues(volume)
	metricSuspended.DeleteLabelValues(volume)
}

type miroirReplicaView struct {
	upToDate   bool
	connected  bool
	splitBrain bool
	suspended  bool
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
