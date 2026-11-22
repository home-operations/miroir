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
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// probeNow stamps the current time as a probe timestamp for status writes.
func probeNow() *metav1.Time {
	t := metav1.Now()
	return &t
}

// replicaView folds one diskful leg's kernel state into the exported
// metric view — the one place the drbdsetup-unit conversions happen
// (percent-in-sync → 0-1 ratio, KiB → bytes, per Prometheus base units).
// Shared by the full pass and the fast path so a gauge added to one can
// never silently serve stale values from the other.
func replicaView(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string, localDiskless bool) miroirReplicaView {
	return miroirReplicaView{
		upToDate:       st.DiskState == drbd.DiskUpToDate,
		connected:      diskfulPeersConnected(st, vol, self),
		splitBrain:     st.SplitBrain,
		suspended:      st.Suspended,
		quorum:         st.Quorum,
		diskFailed:     diskFailedLatch(vol, self, st, localDiskless),
		primary:        st.Primary,
		resyncRatio:    st.ResyncPercent / 100,
		outOfSyncBytes: float64(st.OutOfSyncKiB) * 1024,
	}
}

// diskfulPeersConnected reports whether this node's replication links to
// every diskful peer are established. A diskless tie-breaker's link is
// excluded: it holds no data leg, so its downtime must not read as
// degraded replication — the snapshot barrier and removal gating key on
// this, and coupling them to the tie-breaker would block both in exactly
// the degraded mode the tie-breaker exists to survive. Entries the
// membership reconciler has not completed (no address) have no
// connection yet and are skipped, matching the rendered config.
func diskfulPeersConnected(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if !st.PeerConnected[rep.NodeID] {
			return false
		}
	}
	return true
}

// publishedHealthy reports whether a leg's published status shows it
// connected with current data. StartupGate reads it off the status the
// previous agent process left behind.
func publishedHealthy(st miroirv1alpha1.ReplicaStatus) bool {
	return st.Connected && !st.SplitBrain && (st.Diskless || st.DiskState == drbd.DiskUpToDate)
}

// legSettled reports whether this leg has nothing left to converge: every
// completed peer connected, no resync running, and current data on a
// diskful leg. Unlike diskfulPeersConnected it counts diskless peers: it
// paces agent rollouts (see StartupGate), where a dropped tie-breaker or
// client link is one of the transitions to wait out.
func legSettled(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string, localDiskless bool) bool {
	if st.Resyncing && !st.Verifying {
		return false
	}
	if !localDiskless && st.DiskState != drbd.DiskUpToDate {
		return false
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Node != self && rep.Address != "" && !st.PeerConnected[rep.NodeID] {
			return false
		}
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Node != self && cl.Address != "" && !st.PeerConnected[cl.NodeID] {
			return false
		}
	}
	return true
}

// diskfulPeersUpToDate reports whether every completed diskful peer's disk
// is UpToDate in this node's kernel view. A resyncing (Inconsistent) or
// failed (Diskless) peer cannot cut a snapshot leg, so a barrier raised
// over it is doomed to expire — and it would freeze the workload for the
// whole SuspendDeadline each retry. Same skip rule as
// diskfulPeersConnected.
func diskfulPeersUpToDate(st drbd.Status, vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if st.PeerDiskState[rep.NodeID] != drbd.DiskUpToDate {
			return false
		}
	}
	return true
}

// reportRealizeError routes a realizeBacking failure: an impossible
// restore parks (reportRestoreOrphan), anything else takes the normal
// status-and-backoff path.
func (r *VolumeReconciler) reportRealizeError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	if errors.Is(cause, errRestoreSourceGone) {
		return r.reportRestoreOrphan(ctx, vol, cause)
	}
	return ctrl.Result{}, r.reportError(ctx, vol, cause)
}

// restoreOrphanRequeue spaces retries of a restore whose source snapshot
// is gone. Only deleting the volume (or recreating the snapshot under the
// same name) unsticks it, so the retry only needs to notice that happened.
const restoreOrphanRequeue = 5 * time.Minute

// reportRestoreOrphan surfaces a restore that can never complete on this
// node (errRestoreSourceGone) — Warning Event and a status Message naming
// the operator's options — then parks the retry at restoreOrphanRequeue.
func (r *VolumeReconciler) reportRestoreOrphan(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	ctrl.LoggerFrom(ctx).Error(cause, "restore source snapshot is gone; parking the volume", "volume", vol.Name)
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "RestoreSourceMissing", "Realize",
			"cannot restore %s on node %s: source snapshot %s no longer exists; delete the volume or recreate the snapshot",
			vol.Name, r.NodeName, vol.Spec.Source.SnapshotName)
	}
	return r.parkWithMessage(ctx, vol, cause, restoreOrphanRequeue)
}

func (r *VolumeReconciler) reportError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error) error {
	// Preserve the realized state: a transient error (e.g. DRBD peer not
	// up yet) must not erase DeviceCreated, or the volume would read as
	// hard-Failed while the device exists.
	st := vol.Status.PerNode[r.NodeName]
	st.Message = cause.Error()
	if err := r.patchStatus(ctx, vol, st); err != nil {
		return err
	}
	return cause // requeue with backoff
}

// latchActivated sets the one-way Activated flag when the kernel reports
// the local leg Primary — a Primary is (or was) open for writes. Beyond the
// stage-time twin (csi.markActivated) it covers volumes staged before the
// field existed: a raw-block consumer running since pre-0.4.5 never
// restages, and without the latch planSplitBrain would treat its
// data-bearing volume as never-written. MergeFrom, not the agent's SSA
// apply: Activated is CSI-owned and merge patching the single field avoids
// co-owning it.
func (r *VolumeReconciler) latchActivated(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status) error {
	if !st.Primary || vol.Status.Activated {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Activated = true
	return r.Status().Patch(ctx, vol, client.MergeFrom(base))
}

// patchStatus applies only this node's slot. A full-status apply would
// force-own peers' slots, Formatted (a CSI field), and the Phase and
// ReadyReplicas the controller's PhaseReconciler derives, and revert them to
// this agent's stale read.
//
// Phase and ReadyReplicas it can only withdraw: a Ready published while this
// node's diskful leg is not ready is untrue, so the same apply blanks both.
// A volume whose controller is down then reads as underived, never as a
// stale Ready that an operator's drain gate would pass. The test is on the
// state, not on the transition, so a failed apply, a lagging informer or a
// late controller write is withdrawn by whichever pass sees it next (see
// fastPath), and it uses the controller's own definition of a ready leg
// (legReady), probe age included: a leg whose passes keep failing
// re-publishes its last good slot with only the Message changed
// (reportError), and that slot stops counting once its probe goes stale.
// The controller takes the fields back with the phase it derives.
func (r *VolumeReconciler) patchStatus(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, mine miroirv1alpha1.ReplicaStatus) error {
	if vol.Status.PerNode == nil {
		vol.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	vol.Status.PerNode[r.NodeName] = mine

	status := acv1alpha1.MiroirVolumeStatus().
		WithPerNode(map[string]acv1alpha1.ReplicaStatusApplyConfiguration{
			r.NodeName: *replicaStatusAC(mine),
		})
	withdraw := vol.Status.Phase == miroirv1alpha1.VolumeReady && legBlocksReady(vol, r.NodeName, mine, time.Now())
	if withdraw {
		status = status.WithPhase("").WithReadyReplicas("")
	}
	ac := acv1alpha1.MiroirVolume(vol.Name).WithStatus(status)
	if err := r.SubResource("status").Apply(ctx, ac,
		client.FieldOwner("agent-volume-"+r.NodeName),
		client.ForceOwnership); err != nil {
		return err
	}
	if withdraw {
		vol.Status.Phase, vol.Status.ReadyReplicas = "", ""
	}
	return nil
}

// primarySince keeps a stable timestamp for how long this leg has been
// Primary: stamped on the first pass that observes the role, carried
// through subsequent passes, dropped on demotion (the device closed). The
// auto-diskful reconciler reads its age for tie-breaker conversion.
func primarySince(vol *miroirv1alpha1.MiroirVolume, node string, primary bool) *metav1.Time {
	if !primary {
		return nil
	}
	if prev := vol.Status.PerNode[node].PrimarySince; prev != nil {
		return prev
	}
	now := metav1.Now()
	return &now
}

// replicaStatusAC mirrors ReplicaStatus's wire shape: fields without
// omitempty are always set (SSA must own them even at zero — Connected
// false is a statement, not an absence), omitempty fields only when
// non-zero (absent → SSA clears the previous value this manager owned).
func replicaStatusAC(st miroirv1alpha1.ReplicaStatus) *acv1alpha1.ReplicaStatusApplyConfiguration {
	ac := acv1alpha1.ReplicaStatus().
		WithDeviceCreated(st.DeviceCreated).
		WithConnected(st.Connected).
		WithSplitBrain(st.SplitBrain)
	if st.DevicePath != "" {
		ac = ac.WithDevicePath(st.DevicePath)
	}
	if st.SizeBytes != 0 {
		ac = ac.WithSizeBytes(st.SizeBytes)
	}
	if st.DRBDMinor != 0 {
		ac = ac.WithDRBDMinor(st.DRBDMinor)
	}
	if st.DiskState != "" {
		ac = ac.WithDiskState(st.DiskState)
	}
	if st.Diskless {
		ac = ac.WithDiskless(true)
	}
	if st.Pool != "" {
		ac = ac.WithPool(st.Pool)
	}
	if st.PrimarySince != nil {
		ac = ac.WithPrimarySince(*st.PrimarySince)
	}
	if st.DiskFailed {
		ac = ac.WithDiskFailed(true)
	}
	if st.DiscardGranularityBytes != 0 {
		ac = ac.WithDiscardGranularityBytes(st.DiscardGranularityBytes)
	}
	if st.Message != "" {
		ac = ac.WithMessage(st.Message)
	}
	if st.LastProbedAt != nil {
		ac = ac.WithLastProbedAt(*st.LastProbedAt)
	}
	return ac
}

// diskFailedLatch reports whether this diskful leg is latched failed: DRBD
// detached it to Diskless after a backing-device I/O error (on-io-error
// detach) and it must not be re-attached (drbdResource renders
// adjust --skip-disk while latched). It is sticky — once a prior reconcile
// recorded DiskFailed it stays latched even though prev DiskState is now
// Diskless (the fresh-detach test alone would clear it). In practice only
// removing the replica clears it: the latch renders adjust --skip-disk, so
// a latched leg is never re-attached and can never reach a non-Diskless
// state on its own.
// Gated on the leg having previously been attached so a normal bring-up
// (briefly Diskless) does not cry wolf. That gate reads CRD status, which
// outlives the agent process — so a leg recorded attached before a restart
// and read Diskless after one latches whether or not a disk actually
// failed, and only a replica re-add gets it back. Auto-diskful conversions
// are the population most exposed to that, being the legs that go
// Diskless -> attached in place.
func diskFailedLatch(vol *miroirv1alpha1.MiroirVolume, self string, st drbd.Status, localDiskless bool) bool {
	if localDiskless || st.DiskState != drbd.DiskDiskless {
		return false
	}
	prev := vol.Status.PerNode[self]
	return prev.DiskFailed || (prev.DiskState != "" && prev.DiskState != drbd.DiskDiskless)
}

// detachedDiskMessage explains a latched-failed leg to the operator, who
// otherwise sees only "DiskState: Diskless" with no cause. Persists as long
// as the leg is latched, not just the reconcile the detach was first seen.
func detachedDiskMessage(diskFailed bool) string {
	if !diskFailed {
		return ""
	}
	return "backing device detached after an I/O error; serving via the peer — " +
		"replace the disk, then remove and re-add this replica"
}
