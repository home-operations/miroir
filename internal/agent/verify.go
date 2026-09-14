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
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// defaultVerifyPollInterval is how often the scheduler checks whether a
// running verify has finished. Verify reads the whole disk, so a pass takes
// minutes to hours — a coarse poll is fine.
const defaultVerifyPollInterval = 15 * time.Second

// VerifyScheduler runs `drbdadm verify` on a cron schedule for the volumes
// this node coordinates. Online verify is the only cross-leg integrity check
// DRBD offers (a scrub/fsck validates one leg against itself); this turns
// "remember to run it" into a declared cadence, and surfaces findings in the
// MiroirVolume status, a metric, and an Event instead of only the kernel log.
//
// Only the coordinator (first diskful replica, the same election resize uses)
// initiates a verify per volume, so a volume is not verified from both legs.
// The run is serialized: volumes are verified one at a time, because a verify
// fully reads both legs and a parallel storm would swamp the pool.
type VerifyScheduler struct {
	Client   client.Client
	NodeName string
	DRBD     *drbd.Driver
	// Schedule fires the verify sweep; parsed in main, never nil here.
	Schedule cron.Schedule
	// Recorder emits the VerifyOutOfSync warning; optional.
	Recorder events.EventRecorder
	// PollInterval between completion checks; defaultVerifyPollInterval when zero.
	PollInterval time.Duration
}

// Start sleeps until each scheduled fire, then runs one sweep. The next fire
// is computed after the sweep returns, so an overrun skips missed fires
// rather than queueing them.
func (v *VerifyScheduler) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("verify")
	for {
		next := v.Schedule.Next(time.Now())
		log.Info("scheduled next online verify sweep", "at", next)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
		}
		if err := v.runOnce(ctx); err != nil {
			log.Error(err, "online verify sweep failed")
		}
	}
}

// runOnce verifies every volume this node coordinates, sequentially. A single
// volume's failure is logged and the sweep continues — never fatal.
func (v *VerifyScheduler) runOnce(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("verify")
	var list miroirv1alpha1.MiroirVolumeList
	if err := v.Client.List(ctx, &list); err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	for i := range list.Items {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		vol := &list.Items[i]
		if !v.coordinates(vol) {
			continue
		}
		if err := v.verifyVolume(ctx, vol); err != nil {
			log.Error(err, "verify volume", "volume", vol.Name)
		}
	}
	return nil
}

// coordinates reports whether this node initiates verify for the volume: it
// is replicated, this node is the first diskful replica, and the local
// backing device exists.
func (v *VerifyScheduler) coordinates(vol *miroirv1alpha1.MiroirVolume) bool {
	if vol.Spec.DRBD == nil {
		return false
	}
	coord := vol.Spec.FirstDiskfulReplica()
	if coord == nil || coord.Node != v.NodeName {
		return false
	}
	return vol.Status.PerNode[v.NodeName].DeviceCreated
}

// verifyVolume kicks off a verify when the volume is healthy and idle, waits
// for it to finish, and records the out-of-sync outcome. It skips (no error)
// any volume that is resyncing, already verifying, suspended, split-brain,
// not UpToDate, or missing a peer link — verify needs a quiet, fully
// connected pair, and drbdadm refuses it otherwise.
func (v *VerifyScheduler) verifyVolume(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	st, err := v.DRBD.Status(ctx, vol.Name)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	// Resyncing covers both a data resync and a verify already running (from
	// this node or a peer that reached our leg).
	if st.Resyncing || st.Suspended || st.SplitBrain ||
		st.DiskState != drbd.DiskUpToDate || !diskfulPeersConnected(st, vol, v.NodeName) {
		return nil
	}
	// The marker lands before the kick so no window exists in which the
	// kernel can hold findings the status does not yet account for.
	if err := v.applyVerifySlot(ctx, vol, v.carriedVerifySlot(vol).WithVerifyStartedAt(metav1.Now())); err != nil {
		return fmt.Errorf("record verify start: %w", err)
	}
	if err := v.DRBD.Verify(ctx, vol.Name); err != nil {
		if cerr := v.applyVerifySlot(ctx, vol, v.carriedVerifySlot(vol)); cerr != nil {
			ctrl.LoggerFrom(ctx).WithName("verify").Error(cerr,
				"cannot clear the verify-started marker after a failed kick", "volume", vol.Name)
		}
		return fmt.Errorf("kick verify: %w", err)
	}

	poll := v.PollInterval
	if poll <= 0 {
		poll = defaultVerifyPollInterval
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	// drbdadm verify initiates synchronously, so the first poll observes the
	// pass either in flight (large volume) or already complete with the
	// out-of-sync count settled (small/clean volume) — no start race.
	for {
		select {
		case <-ctx.Done():
			// Shutdown: the kernel verify continues harmlessly and the next
			// sweep re-checks. Do not stop it — that needs a disconnect.
			// The started marker stays: its findings are unrecorded until
			// that sweep, and the stale-bitmap self-heal must keep off.
			return nil
		case <-t.C:
		}
		st, err := v.DRBD.Status(ctx, vol.Name)
		if err != nil {
			return fmt.Errorf("poll verify status: %w", err)
		}
		if !st.Verifying {
			// A pair that broke mid-pass aborts the verify, and the
			// out-of-sync count then mixes findings with bits accrued while
			// the peer was away — the latter heal on an ordinary reconnect
			// resync, unlike findings. Unattributable: discard and let a
			// later sweep re-verify.
			if !diskfulPeersConnected(st, vol, v.NodeName) {
				ctrl.LoggerFrom(ctx).WithName("verify").Info(
					"verify interrupted by a peer disconnect; result discarded", "volume", vol.Name)
				// The bits accrued while the peer was away heal on the
				// reconnect resync, so nothing needs the marker's guard.
				return v.applyVerifySlot(ctx, vol, v.carriedVerifySlot(vol))
			}
			return v.recordResult(ctx, vol, st.OutOfSyncKiB*1024)
		}
	}
}

// recordResult writes the outcome to the coordinator's status slot, the
// verify metrics, and — on a dirty result — a Warning event. The applied
// slot omits VerifyStartedAt, which is what removes the marker: the
// verify field owner declares the whole set of fields it wants, and a
// field it stops declaring is dropped.
func (v *VerifyScheduler) recordResult(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, oosBytes int64) error {
	now := metav1.Now()
	recordVerifyMetrics(vol, volumePoolOn(vol, v.NodeName), now.Time, oosBytes)
	if oosBytes > 0 && v.Recorder != nil {
		v.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "VerifyOutOfSync", "Verify",
			"online verify found %d out-of-sync bytes; a disconnect/connect cycle is needed to resync", oosBytes)
	}
	slot := acv1alpha1.ReplicaStatus().
		WithLastVerifyTime(now).
		WithLastVerifyOutOfSyncBytes(oosBytes)
	if err := v.applyVerifySlot(ctx, vol, slot); err != nil {
		return fmt.Errorf("record verify result: %w", err)
	}
	return nil
}

// carriedVerifySlot rebuilds the last recorded verify outcome from the
// coordinator's slot, so an apply that only adds or drops the started
// marker does not also drop the record it sits next to.
func (v *VerifyScheduler) carriedVerifySlot(vol *miroirv1alpha1.MiroirVolume) *acv1alpha1.ReplicaStatusApplyConfiguration {
	slot := acv1alpha1.ReplicaStatus()
	prev := vol.Status.PerNode[v.NodeName]
	if prev.LastVerifyTime != nil {
		slot.WithLastVerifyTime(*prev.LastVerifyTime)
	}
	if prev.LastVerifyOutOfSyncBytes != nil {
		slot.WithLastVerifyOutOfSyncBytes(*prev.LastVerifyOutOfSyncBytes)
	}
	return slot
}

// applyVerifySlot applies the verify-owned fields of the coordinator's
// status slot under the verify field owner, which is what keeps them
// from colliding with the reconciler's per-node slot apply.
func (v *VerifyScheduler) applyVerifySlot(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, slot *acv1alpha1.ReplicaStatusApplyConfiguration) error {
	ac := acv1alpha1.MiroirVolume(vol.Name).
		WithStatus(acv1alpha1.MiroirVolumeStatus().
			WithPerNode(map[string]acv1alpha1.ReplicaStatusApplyConfiguration{v.NodeName: *slot}))
	return v.Client.SubResource("status").Apply(ctx, ac,
		client.FieldOwner("agent-verify-"+v.NodeName),
		client.ForceOwnership)
}
