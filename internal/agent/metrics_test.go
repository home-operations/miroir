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
	const pool = "fast"
	recordVolumeMetrics(volume, pool, miroirReplicaView{
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

	recordVerifyMetrics(volume, pool, time.Unix(1700000000, 0), 512)
	recordDisklessMetrics(volume, true)

	if got := testutil.ToFloat64(metricUpToDate.WithLabelValues(volume, pool)); got != 1 {
		t.Fatalf("up_to_date = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricConnected.WithLabelValues(volume, pool)); got != 0 {
		t.Fatalf("connected = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metricSplitBrain.WithLabelValues(volume, pool)); got != 1 {
		t.Fatalf("split_brain = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricSuspended.WithLabelValues(volume, pool)); got != 0 {
		t.Fatalf("suspended = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metricResyncRatio.WithLabelValues(volume, pool)); got != 0.425 {
		t.Fatalf("resync_ratio = %v, want 0.425", got)
	}
	if got := testutil.ToFloat64(metricQuorum.WithLabelValues(volume, pool)); got != 1 {
		t.Fatalf("quorum = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricDiskFailed.WithLabelValues(volume, pool)); got != 1 {
		t.Fatalf("disk_failed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricOutOfSyncBytes.WithLabelValues(volume, pool)); got != 2048*1024 {
		t.Fatalf("out_of_sync_bytes = %v, want %v", got, 2048*1024)
	}
	if got := testutil.ToFloat64(metricPrimary.WithLabelValues(volume, pool)); got != 1 {
		t.Fatalf("primary = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricVerifyTimestamp.WithLabelValues(volume, pool)); got != 1700000000 {
		t.Fatalf("verify_last_timestamp_seconds = %v, want 1700000000", got)
	}
	if got := testutil.ToFloat64(metricVerifyOutOfSyncBytes.WithLabelValues(volume, pool)); got != 512 {
		t.Fatalf("verify_out_of_sync_bytes = %v, want 512", got)
	}
	if got := testutil.ToFloat64(metricDisklessPrimary.WithLabelValues(volume)); got != 1 {
		t.Fatalf("diskless_primary = %v, want 1", got)
	}

	// Drop knows only the volume, not the pool the series were recorded
	// under — the partial-match delete must still clear every family.
	dropVolumeMetrics(volume)
	for _, family := range []string{
		"miroir_volume_up_to_date",
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

// The kernel info gauge must expose the probed version as a label with a
// constant 1 — the shape "up{version=...}"-style queries and the fleet
// skew table depend on.
func TestRecordDRBDKernelVersion(t *testing.T) {
	RecordDRBDKernelVersion("9.3.2")
	if got := testutil.ToFloat64(metricDRBDKernelInfo.WithLabelValues("9.3.2")); got != 1 {
		t.Fatalf("drbd_kernel_info{version=9.3.2} = %v, want 1", got)
	}
}
