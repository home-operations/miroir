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

package export

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

// The RWX serving signal. The agents' miroir_volume_* gauges track DRBD
// replica health and stay green while the gateway — a per-volume singleton —
// is down and every NFS client hangs, so export health needs its own gauge.
// Published from the controller (this reconciler owns the gateway workloads
// and already watches them): the gateway's own /metrics vanishes exactly
// when the gateway is down, which is the state this gauge must report.
const volumeLabel = "volume"

var metricExportReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "miroir_export_ready",
	Help: "1 when the RWX volume's NFS gateway is serving (gateway pod available and export address published); 0 while clients cannot reach the export — a failover in progress, or a gateway that cannot run.",
}, []string{volumeLabel, "pvc", "pvc_namespace"})

func init() {
	metrics.Registry.MustRegister(metricExportReady)
}

func recordExportReady(vol *miroirv1alpha1.MiroirVolume, ready bool) {
	var v float64
	if ready {
		v = 1
	}
	// Retire any series recorded before the PVC-ref labels landed on the
	// volume (the controller backfill changes the series identity); one
	// series per RWX volume, so the sweep is cheap.
	dropExportMetrics(vol.Name)
	pvc, namespace := constants.PVCRef(vol.Name, vol.Labels)
	metricExportReady.WithLabelValues(vol.Name, pvc, namespace).Set(v)
}

// dropExportMetrics removes a volume's series; a deleted volume must not
// leave a stale 0 behind (it would fire MiroirExportUnavailable forever).
func dropExportMetrics(volume string) {
	metricExportReady.DeletePartialMatch(prometheus.Labels{volumeLabel: volume})
}
