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
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
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

// metricsVol is an unlabeled volume: its series fall back to the CR name
// as the pvc label.
func metricsVol(name string) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

const familyUpToDate = "miroir_volume_up_to_date"

// The gauges must track the recorded view exactly and vanish from the
// scrape once dropped — a replica removed from this node must not keep
// serving stale health series.
func TestVolumeMetricsLifecycle(t *testing.T) {
	const volume = "pvc-metrics-lifecycle"
	const pool = "fast"
	vol := metricsVol(volume)
	recordVolumeMetrics(vol, pool, miroirReplicaView{
		upToDate:       true,
		connected:      false,
		splitBrain:     true,
		suspended:      false,
		quorum:         true,
		diskFailed:     true,
		primary:        true,
		resyncRatio:    0.425,
		outOfSyncBytes: 2048 * 1024,
	})

	recordVerifyMetrics(vol, pool, time.Unix(1700000000, 0), 512)
	recordDisklessMetrics(vol, true)

	if got := testutil.ToFloat64(metricUpToDate.WithLabelValues(volume, pool, volume, "")); got != 1 {
		t.Fatalf("up_to_date = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricConnected.WithLabelValues(volume, pool, volume, "")); got != 0 {
		t.Fatalf("connected = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metricSplitBrain.WithLabelValues(volume, pool, volume, "")); got != 1 {
		t.Fatalf("split_brain = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricSuspended.WithLabelValues(volume, pool, volume, "")); got != 0 {
		t.Fatalf("suspended = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metricResyncRatio.WithLabelValues(volume, pool, volume, "")); got != 0.425 {
		t.Fatalf("resync_ratio = %v, want 0.425", got)
	}
	if got := testutil.ToFloat64(metricQuorum.WithLabelValues(volume, pool, volume, "")); got != 1 {
		t.Fatalf("quorum = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricDiskFailed.WithLabelValues(volume, pool, volume, "")); got != 1 {
		t.Fatalf("disk_failed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricOutOfSyncBytes.WithLabelValues(volume, pool, volume, "")); got != 2048*1024 {
		t.Fatalf("out_of_sync_bytes = %v, want %v", got, 2048*1024)
	}
	if got := testutil.ToFloat64(metricPrimary.WithLabelValues(volume, pool, volume, "")); got != 1 {
		t.Fatalf("primary = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricVerifyTimestamp.WithLabelValues(volume, pool, volume, "")); got != 1700000000 {
		t.Fatalf("verify_last_timestamp_seconds = %v, want 1700000000", got)
	}
	if got := testutil.ToFloat64(metricVerifyOutOfSyncBytes.WithLabelValues(volume, pool, volume, "")); got != 512 {
		t.Fatalf("verify_out_of_sync_bytes = %v, want 512", got)
	}
	if got := testutil.ToFloat64(metricDisklessPrimary.WithLabelValues(volume, volume, "")); got != 1 {
		t.Fatalf("diskless_primary = %v, want 1", got)
	}

	// Drop knows only the volume, not the pool the series were recorded
	// under — the partial-match delete must still clear every family.
	dropVolumeMetrics(volume)
	for _, family := range []string{
		familyUpToDate,
		"miroir_volume_connected",
		"miroir_volume_split_brain",
		"miroir_volume_suspended",
		"miroir_volume_resync_ratio",
		"miroir_volume_quorum",
		"miroir_volume_disk_failed",
		"miroir_volume_out_of_sync_bytes",
		"miroir_volume_primary",
		"miroir_volume_verify_last_timestamp_seconds",
		"miroir_volume_verify_out_of_sync_bytes",
		"miroir_volume_diskless_primary",
	} {
		if hasSeries(t, family, volume) {
			t.Fatalf("%s{volume=%q} still exposed after drop", family, volume)
		}
	}
}

// The kernel info gauge must expose the probed versions as labels with a
// constant 1 — the shape "up{version=...}"-style queries and the fleet
// skew table depend on.
func TestRecordDRBDVersions(t *testing.T) {
	RecordDRBDVersions("9.3.2", "9.34.3")
	if got := testutil.ToFloat64(metricDRBDKernelInfo.WithLabelValues("9.3.2", "9.34.3")); got != 1 {
		t.Fatalf("drbd_kernel_info{version=9.3.2,utils_version=9.34.3} = %v, want 1", got)
	}
}

// A leg converted from diskless to diskful in place (auto-diskful /
// auto-evict, which never pass through the removal path that drops metrics)
// must not keep exposing its diskless-primary series at a stale value;
// recording the diskful view clears it.
func TestDisklessMetricClearedWhenLegBecomesDiskful(t *testing.T) {
	const volume = "pvc-diskless-to-diskful"
	const pool = "default"
	vol := metricsVol(volume)

	// Diskless tie-breaker / client leg: only the diskless-primary series.
	recordDisklessMetrics(vol, true)
	if !hasSeries(t, "miroir_volume_diskless_primary", volume) {
		t.Fatal("diskless_primary series missing after recordDisklessMetrics")
	}

	// Converted to a diskful replica: it now publishes the diskful view, and
	// the diskless-primary series must be gone from the scrape.
	recordVolumeMetrics(vol, pool, miroirReplicaView{
		upToDate: true, connected: true, quorum: true, resyncRatio: 1,
	})
	if hasSeries(t, "miroir_volume_diskless_primary", volume) {
		t.Fatal("diskless_primary series still exposed after the leg became diskful")
	}
	if got := testutil.ToFloat64(metricUpToDate.WithLabelValues(volume, pool, volume, "")); got != 1 {
		t.Fatalf("up_to_date = %v, want 1", got)
	}

	dropVolumeMetrics(volume)
}

// When the PVC-ref labels land on a volume whose series already exist
// unlabeled (the controller's backfill), the old identity must leave the
// scrape — otherwise it would keep reporting its last values alongside
// the new series and double-count in aggregations.
func TestPVCRefBackfillRetiresUnlabeledSeries(t *testing.T) {
	const volume = "pvc-backfill-transition"
	const pool = "default"
	vol := metricsVol(volume)
	recordVolumeMetrics(vol, pool, miroirReplicaView{
		upToDate: true, connected: true, quorum: true, resyncRatio: 1,
	})

	vol.Labels = map[string]string{
		constants.LabelPVCName:      "config-jellyfin",
		constants.LabelPVCNamespace: "media",
	}
	recordVolumeMetrics(vol, pool, miroirReplicaView{
		upToDate: true, connected: true, quorum: true, resyncRatio: 1,
	})

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != familyUpToDate {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels[volumeLabel] != volume {
				continue
			}
			if labels[pvcLabel] != "config-jellyfin" || labels[pvcNamespaceLabel] != "media" {
				t.Fatalf("stale identity still exposed after backfill: %v", labels)
			}
		}
	}

	dropVolumeMetrics(volume)
}
