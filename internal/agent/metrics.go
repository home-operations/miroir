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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

// Exposed on the agent's metrics endpoint; the split-brain gauge is the
// alerting hook for the last-man-standing failure mode.
//
// Every volume series carries pvc/pvc_namespace alongside the CR name:
// dashboards and alerts read the claim the volume serves, not the opaque
// pvc-<uuid>. The pair comes from the CreateVolume-stamped CR labels; a
// volume without them falls back to its CR name (see refOf).
const (
	volumeLabel       = "volume"
	poolLabel         = "pool"
	pvcLabel          = "pvc"
	pvcNamespaceLabel = "pvc_namespace"
)

var (
	diskfulLabels    = []string{volumeLabel, poolLabel, pvcLabel, pvcNamespaceLabel}
	volumeOnlyLabels = []string{volumeLabel, pvcLabel, pvcNamespaceLabel}
)

// volumeRef is the label identity a volume's series carry. pvc falls back
// to the CR name (and pvc_namespace to empty) when the volume has no
// PVC-ref labels — created before the labels existed and not yet
// backfilled, or a PVC name too long for a label value — so legends never
// go blank.
type volumeRef struct {
	name, pvc, namespace string
}

func refOf(vol *miroirv1alpha1.MiroirVolume) volumeRef {
	pvc, namespace := constants.PVCRef(vol.Name, vol.Labels)
	return volumeRef{name: vol.Name, pvc: pvc, namespace: namespace}
}

func (r volumeRef) with(pool string) prometheus.Labels {
	return prometheus.Labels{
		volumeLabel: r.name, poolLabel: pool,
		pvcLabel: r.pvc, pvcNamespaceLabel: r.namespace,
	}
}

func (r volumeRef) labels() prometheus.Labels {
	return prometheus.Labels{
		volumeLabel: r.name, pvcLabel: r.pvc, pvcNamespaceLabel: r.namespace,
	}
}

// seenRefs tracks the identity each volume's series were last recorded
// under. The controller backfills the PVC-ref labels onto legacy volumes
// after their series already exist, and without a drop on that transition
// the old identity would keep reporting its last values alongside the new
// series. The one-time drop also clears the verify series; they reappear
// at the next verify pass.
var (
	seenRefsMu sync.Mutex
	seenRefs   = map[string]volumeRef{}
)

// syncRef resolves a volume's label identity, retiring series recorded
// under a previous one.
func syncRef(vol *miroirv1alpha1.MiroirVolume) volumeRef {
	ref := refOf(vol)
	seenRefsMu.Lock()
	prev, ok := seenRefs[ref.name]
	seenRefs[ref.name] = ref
	seenRefsMu.Unlock()
	if ok && prev != ref {
		deleteVolumeSeries(ref.name)
	}
	return ref
}

var (
	// Diskful volume gauges carry the pool backing this node's leg (pools
	// are per-node, so peers' legs of the same volume may report different
	// pool values). Diskless legs have no backing pool, so
	// miroir_volume_diskless_primary carries no pool label.
	metricUpToDate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_up_to_date",
		Help: "1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created).",
	}, diskfulLabels)
	metricConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_connected",
		Help: "1 when all replication links from this node to diskful peers are established (a diskless tie-breaker's link is excluded).",
	}, diskfulLabels)
	metricSplitBrain = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_split_brain",
		Help: "1 when DRBD refused to reconnect after divergence; manual resolution required.",
	}, diskfulLabels)
	metricSuspended = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_suspended",
		Help: "1 while a user suspend-io (the snapshot write barrier) freezes this node's IO; sustained means a stranded barrier (snapshot rounds last seconds).",
	}, diskfulLabels)
	metricResyncRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_resync_ratio",
		Help: "Fraction in sync (0-1) of the least-synced diskful peer while resyncing; 1 when fully in sync (unreplicated volumes report 1).",
	}, diskfulLabels)
	metricQuorum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_quorum",
		Help: "1 when this node's replica sees DRBD quorum; 0 while a freeze-policy volume has lost quorum and refuses writes with I/O errors (on-no-quorum io-error; always 1 under last-man-standing, and for unreplicated volumes).",
	}, diskfulLabels)
	metricDiskFailed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_disk_failed",
		Help: "1 when this leg's backing disk was detached after an I/O error and is latched failed — replace the disk, then remove and re-add the replica.",
	}, diskfulLabels)
	metricOutOfSyncBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_out_of_sync_bytes",
		Help: "Largest per-peer out-of-sync amount in bytes: the exposure if the healthiest peer is lost. Grows while a peer is down with no resync running; also counts online-verify findings.",
	}, diskfulLabels)
	metricVerifyTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_verify_last_timestamp_seconds",
		Help: "Unix time of the last completed scheduled online verify for this volume; absent until the first verify runs. Alert on staleness to catch a verify schedule that stopped firing.",
	}, diskfulLabels)
	metricVerifyOutOfSyncBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_verify_out_of_sync_bytes",
		Help: "Out-of-sync bytes the last scheduled verify found (0 = clean). Findings also surface in miroir_volume_out_of_sync_bytes until a disconnect/connect resync clears them.",
	}, diskfulLabels)
	metricPrimary = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_primary",
		Help: "1 while this node's diskful leg is DRBD Primary: the consumer pod or the RWX gateway runs here and this leg serves the I/O. Diskless legs report miroir_volume_diskless_primary instead; unreplicated volumes have no DRBD role and always report 0.",
	}, diskfulLabels)
	metricDisklessPrimary = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_diskless_primary",
		Help: "1 while this node's diskless leg (client or tie-breaker) is DRBD Primary: a consumer runs here and every read and write crosses the replication network. Sustained 1 means the workload pays network I/O — auto-diskful (autoDiskfulAfter) converts the leg to a local replica when the node has storage capacity.",
	}, volumeOnlyLabels)
	// Pool-less, like miroir_volume_diskless_primary: the pool a wedged
	// teardown reported under can be unknowable (see dropVolumeMetrics).
	metricWedged = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_volume_wedged",
		Help: "1 when the kernel can no longer tear down this volume's DRBD resource (device stuck Detaching after a refcount underflow, LINBIT/drbd#137); teardown is parked at a slow retry and only a node reboot clears the state.",
	}, volumeOnlyLabels)

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
		Help: "Always 1, labelled with the DRBD kernel module version probed at agent startup (version) and the agent image's drbd-utils version (utils_version); absent on nodes without the module. Query it for fleet version skew before a kernel floor raise.",
	}, []string{"version", "utils_version"})
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

func recordVolumeMetrics(vol *miroirv1alpha1.MiroirVolume, pool string, st miroirReplicaView) {
	ref := syncRef(vol)
	// This leg is diskful, so clear any diskless-primary series a prior life
	// of it left behind: auto-diskful and auto-evict convert a diskless
	// tie-breaker/client leg into a diskful replica in place, without the
	// removal path (dropVolumeMetrics) that would otherwise clear it. Left
	// alone the stale series reads its last value until the volume is deleted.
	// The reverse (diskful to diskless) only happens via removal, which drops
	// every series, so no symmetric clear is needed. Cheap no-op when absent.
	metricDisklessPrimary.DeletePartialMatch(prometheus.Labels{volumeLabel: ref.name})
	l := ref.with(pool)
	metricUpToDate.With(l).Set(boolGauge(st.upToDate))
	metricConnected.With(l).Set(boolGauge(st.connected))
	metricSplitBrain.With(l).Set(boolGauge(st.splitBrain))
	metricSuspended.With(l).Set(boolGauge(st.suspended))
	metricResyncRatio.With(l).Set(st.resyncRatio)
	metricQuorum.With(l).Set(boolGauge(st.quorum))
	metricDiskFailed.With(l).Set(boolGauge(st.diskFailed))
	metricOutOfSyncBytes.With(l).Set(st.outOfSyncBytes)
	metricPrimary.With(l).Set(boolGauge(st.primary))
}

// recordPoolMetrics publishes one pool's sample. The pool set is fixed per
// agent process (the TopologyWatcher restarts the agent on a pool-spec
// change), so series are never deleted.
func recordPoolMetrics(pool string, capacityBytes, allocatedBytes int64, metaUsedRatio float64) {
	metricPoolCapacity.WithLabelValues(pool).Set(float64(capacityBytes))
	metricPoolAllocated.WithLabelValues(pool).Set(float64(allocatedBytes))
	metricPoolMetaUsedRatio.WithLabelValues(pool).Set(metaUsedRatio)
}

// dropVolumeMetrics deletes by volume alone (partial match): the pool and
// PVC ref a torn-down leg reported under can be unknowable here, for the
// same reason deletions sweep every pool — see Pools.SweepDelete.
func dropVolumeMetrics(volume string) {
	seenRefsMu.Lock()
	delete(seenRefs, volume)
	seenRefsMu.Unlock()
	deleteVolumeSeries(volume)
}

func deleteVolumeSeries(volume string) {
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
	metricDisklessPrimary.DeletePartialMatch(byVolume)
	metricWedged.DeletePartialMatch(byVolume)
}

// recordDisklessMetrics publishes a diskless leg's view. Deliberately not
// the diskful gauges: a tie-breaker reading up_to_date=0 or losing its own
// quorum would fire the data-leg alerts for a leg that holds no data.
func recordDisklessMetrics(vol *miroirv1alpha1.MiroirVolume, primary bool) {
	metricDisklessPrimary.With(syncRef(vol).labels()).Set(boolGauge(primary))
}

// recordWedged raises the wedged gauge; clearWedged retires it by volume
// alone, so a wedge recorded before a PVC-ref backfill still clears.
func recordWedged(vol *miroirv1alpha1.MiroirVolume) {
	metricWedged.With(syncRef(vol).labels()).Set(1)
}

func clearWedged(volume string) {
	metricWedged.DeletePartialMatch(prometheus.Labels{volumeLabel: volume})
}

// RecordDRBDVersions publishes the module and drbd-utils versions probed
// at agent startup. Set once per process: a module change means a node
// reboot and thus a fresh agent, and the utils only change with the agent
// image, so startup is current enough (same contract as
// PoolStatsPublisher.DRBDVersion).
func RecordDRBDVersions(kernel, utils string) {
	metricDRBDKernelInfo.WithLabelValues(kernel, utils).Set(1)
}

// recordVerifyMetrics publishes the outcome of a completed verify pass. The
// coordinator sets these directly (they are event-driven, not part of the
// per-poll recordVolumeMetrics view).
func recordVerifyMetrics(vol *miroirv1alpha1.MiroirVolume, pool string, at time.Time, outOfSyncBytes int64) {
	l := syncRef(vol).with(pool)
	metricVerifyTimestamp.With(l).Set(float64(at.Unix()))
	metricVerifyOutOfSyncBytes.With(l).Set(float64(outOfSyncBytes))
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
