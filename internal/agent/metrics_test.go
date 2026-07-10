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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// hasSeries reports whether the controller-runtime registry currently
// exposes a series of the named family labelled with the volume — the
// wire-level view a scrape would see, so DeleteLabelValues regressions
// (a Set recreating a dropped series) cannot hide.
func hasSeries(t *testing.T, family, volume string) bool {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == volumeLabel && l.GetValue() == volume {
					return true
				}
			}
		}
	}
	return false
}

// The gauges must track the recorded view exactly and vanish from the
// scrape once dropped — a replica removed from this node must not keep
// serving stale health series.
func TestVolumeMetricsLifecycle(t *testing.T) {
	const volume = "pvc-metrics-lifecycle"
	recordVolumeMetrics(volume, miroirReplicaView{
		upToDate:      true,
		connected:     false,
		splitBrain:    true,
		suspended:     false,
		resyncPercent: 42.5,
	})

	if got := testutil.ToFloat64(metricUpToDate.WithLabelValues(volume)); got != 1 {
		t.Fatalf("up_to_date = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricConnected.WithLabelValues(volume)); got != 0 {
		t.Fatalf("connected = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metricSplitBrain.WithLabelValues(volume)); got != 1 {
		t.Fatalf("split_brain = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricSuspended.WithLabelValues(volume)); got != 0 {
		t.Fatalf("suspended = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metricResyncPercent.WithLabelValues(volume)); got != 42.5 {
		t.Fatalf("resync_percent = %v, want 42.5", got)
	}

	dropVolumeMetrics(volume)
	for _, family := range []string{
		"miroir_volume_up_to_date",
		"miroir_volume_connected",
		"miroir_volume_split_brain",
		"miroir_volume_suspended",
		"miroir_volume_resync_percent",
	} {
		if hasSeries(t, family, volume) {
			t.Fatalf("%s{volume=%q} still exposed after drop", family, volume)
		}
	}
}
