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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Exposed on the agent's metrics endpoint; the split-brain gauge is the
// alerting hook for the last-man-standing failure mode.
const (
	volumeLabel = "volume"
	poolLabel   = "pool"
)

var (
	// Diskful volume gauges carry the pool backing this node's leg (pools
	// are per-node, so peers' legs of the same volume may report different
	// pool values). Diskless legs have no backing pool, so
	// miroir_volume_diskless_primary stays volume-only.
	metricUpToDate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_up_to_date",
		Help: "1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created).",
	}, []string{volumeLabel, poolLabel})
	metricConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_connected",
		Help: "1 when all replication links from this node to diskful peers are established (a diskless tie-breaker's link is excluded).",
	}, []string{volumeLabel, poolLabel})
	metricSplitBrain = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_split_brain",
		Help: "1 when DRBD refused to reconnect after divergence; manual resolution required.",
	}, []string{volumeLabel, poolLabel})
	metricSuspended = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_suspended",
		Help: "1 while a user suspend-io (the snapshot write barrier) freezes this node's IO; sustained means a stranded barrier (snapshot rounds last seconds).",
	}, []string{volumeLabel, poolLabel})
	metricResyncRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_resync_ratio",
		Help: "Fraction in sync (0-1) of the least-synced diskful peer while resyncing; 1 when fully in sync (unreplicated volumes report 1).",
	}, []string{volumeLabel, poolLabel})
	metricQuorum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_quorum",
		Help: "1 when this node's replica sees DRBD quorum; 0 while a freeze-policy volume has lost quorum and refuses writes with I/O errors (on-no-quorum io-error; always 1 under last-man-standing, and for unreplicated volumes).",
	}, []string{volumeLabel, poolLabel})
	metricDiskFailed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_disk_failed",
		Help: "1 when this leg's backing disk was detached after an I/O error and is latched failed — replace the disk, then remove and re-add the replica.",
	}, []string{volumeLabel, poolLabel})
	metricOutOfSyncBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_out_of_sync_bytes",
		Help: "Largest per-peer out-of-sync amount in bytes: the exposure if the healthiest peer is lost. Grows while a peer is down with no resync running; also counts online-verify findings.",
	}, []string{volumeLabel, poolLabel})
	metricVerifyTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_verify_last_timestamp_seconds",
		Help: "Unix time of the last completed scheduled online verify for this volume; absent until the first verify runs. Alert on staleness to catch a verify schedule that stopped firing.",
	}, []string{volumeLabel, poolLabel})
	metricVerifyOutOfSyncBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_verify_out_of_sync_bytes",
		Help: "Out-of-sync bytes the last scheduled verify found (0 = clean). Findings also surface in miroir_volume_out_of_sync_bytes until a disconnect/connect resync clears them.",
	}, []string{volumeLabel, poolLabel})
	metricPrimary = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_primary",
		Help: "1 while this node's diskful leg is DRBD Primary: the consumer pod or the RWX gateway runs here and this leg serves the I/O. Diskless legs report miroir_volume_diskless_primary instead; unreplicated volumes have no DRBD role and always report 0.",
	}, []string{volumeLabel, poolLabel})
	metricDisklessPrimary = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_diskless_primary",
		Help: "1 while this node's diskless leg (client or tie-breaker) is DRBD Primary: a consumer runs here and every read and write crosses the replication network. Sustained 1 means the workload pays network I/O — auto-diskful (autoDiskfulAfter) converts the leg to a local replica when the node has storage capacity.",
	}, []string{volumeLabel})
	// Volume-only, like miroir_volume_diskless_primary: the pool a wedged
	// teardown reported under can be unknowable (see dropVolumeMetrics).
	metricWedged = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_wedged",
		Help: "1 when the kernel can no longer tear down this volume's DRBD resource (device stuck Detaching after a refcount underflow, LINBIT/drbd#137); teardown is parked at a slow retry and only a node reboot clears the state.",
	}, []string{volumeLabel})

	// Pool gauges carry the pool name; the PodMonitor stamps a node label
	// on every series, so (node, pool) identifies one pool.
	metricPoolCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_pool_capacity_bytes",
		Help: "Total capacity of this node's named storage pool.",
	}, []string{poolLabel})
	metricPoolAllocated = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_pool_allocated_bytes",
		Help: "Bytes allocated from this node's named storage pool.",
	}, []string{poolLabel})
	metricPoolMetaUsedRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_pool_meta_used_ratio",
		Help: "Fraction (0-1) of dm-thin metadata used; 0 for backends without a metadata pool.",
	}, []string{poolLabel})

	// Info-style: constant 1, the payload is the version label. Exported
	// from client-only nodes too — unlike MiroirNode status, which only
	// the pool stats publisher (storage nodes) updates.
	metricDRBDKernelInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_node_drbd_kernel_info",
		Help: "Always 1, labelled with the DRBD kernel module version probed at agent startup; absent on nodes without the module. Query it for fleet version skew before a kernel floor raise.",
	}, []string{"version"})
)

func init() {
	metrics.Registry.MustRegister(
		metricUpToDate, metricConnected, metricSplitBrain, metricSuspended,
		metricResyncRatio, metricQuorum, metricDiskFailed, metricOutOfSyncBytes,
		metricVerifyTimestamp, metricVerifyOutOfSyncBytes, metricPrimary, metricDisklessPrimary,
		metricWedged, metricPoolCapacity, metricPoolAllocated, metricPoolMetaUsedRatio,
		metricDRBDKernelInfo,
	)
}

func recordVolumeMetrics(volume, pool string, st miroirReplicaView) {
	metricUpToDate.WithLabelValues(volume, pool).Set(boolGauge(st.upToDate))
	metricConnected.WithLabelValues(volume, pool).Set(boolGauge(st.connected))
	metricSplitBrain.WithLabelValues(volume, pool).Set(boolGauge(st.splitBrain))
	metricSuspended.WithLabelValues(volume, pool).Set(boolGauge(st.suspended))
	metricResyncRatio.WithLabelValues(volume, pool).Set(st.resyncRatio)
	metricQuorum.WithLabelValues(volume, pool).Set(boolGauge(st.quorum))
	metricDiskFailed.WithLabelValues(volume, pool).Set(boolGauge(st.diskFailed))
	metricOutOfSyncBytes.WithLabelValues(volume, pool).Set(st.outOfSyncBytes)
	metricPrimary.WithLabelValues(volume, pool).Set(boolGauge(st.primary))
}

// recordPoolMetrics publishes one pool's sample. The pool set is fixed per
// agent process (a node-map change is a Helm upgrade, hence a restart), so
// series are never deleted.
func recordPoolMetrics(pool string, capacityBytes, allocatedBytes int64, metaUsedRatio float64) {
	metricPoolCapacity.WithLabelValues(pool).Set(float64(capacityBytes))
	metricPoolAllocated.WithLabelValues(pool).Set(float64(allocatedBytes))
	metricPoolMetaUsedRatio.WithLabelValues(pool).Set(metaUsedRatio)
}

// dropVolumeMetrics deletes by volume alone (partial match): the pool a
// torn-down leg reported under can be unknowable here, for the same reason
// deletions sweep every pool — see Pools.SweepDelete.
func dropVolumeMetrics(volume string) {
	byVolume := prometheus.Labels{volumeLabel: volume}
	metricUpToDate.DeletePartialMatch(byVolume)
	metricConnected.DeletePartialMatch(byVolume)
	metricSplitBrain.DeletePartialMatch(byVolume)
	metricSuspended.DeletePartialMatch(byVolume)
	metricResyncRatio.DeletePartialMatch(byVolume)
	metricQuorum.DeletePartialMatch(byVolume)
	metricDiskFailed.DeletePartialMatch(byVolume)
	metricOutOfSyncBytes.DeletePartialMatch(byVolume)
	metricPrimary.DeletePartialMatch(byVolume)
	metricVerifyTimestamp.DeletePartialMatch(byVolume)
	metricVerifyOutOfSyncBytes.DeletePartialMatch(byVolume)
	metricDisklessPrimary.DeleteLabelValues(volume)
	metricWedged.DeleteLabelValues(volume)
}

// recordDisklessMetrics publishes a diskless leg's view. Deliberately not
// the diskful gauges: a tie-breaker reading up_to_date=0 or losing its own
// quorum would fire the data-leg alerts for a leg that holds no data.
func recordDisklessMetrics(volume string, primary bool) {
	metricDisklessPrimary.WithLabelValues(volume).Set(boolGauge(primary))
}

// RecordDRBDKernelVersion publishes the module version probed at agent
// startup. Set once per process: a module change means a node reboot and
// thus a fresh agent, so startup is current enough (same contract as
// PoolStatsPublisher.DRBDVersion).
func RecordDRBDKernelVersion(version string) {
	metricDRBDKernelInfo.WithLabelValues(version).Set(1)
}

// recordVerifyMetrics publishes the outcome of a completed verify pass. The
// coordinator sets these directly (they are event-driven, not part of the
// per-poll recordVolumeMetrics view).
func recordVerifyMetrics(volume, pool string, at time.Time, outOfSyncBytes int64) {
	metricVerifyTimestamp.WithLabelValues(volume, pool).Set(float64(at.Unix()))
	metricVerifyOutOfSyncBytes.WithLabelValues(volume, pool).Set(float64(outOfSyncBytes))
}

type miroirReplicaView struct {
	upToDate       bool
	connected      bool
	splitBrain     bool
	suspended      bool
	quorum         bool
	diskFailed     bool
	primary        bool
	resyncRatio    float64
	outOfSyncBytes float64
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
