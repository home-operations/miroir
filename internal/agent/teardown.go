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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// reconcileDeletion tears down the local leg of a deleted volume. Only
// the agent owning a replica may release the finalizer, and only after
// its local teardown succeeded — a foreign agent touching it would race
// the owner and leak the backing device. A pending-removal replica
// (finalizer held, not in spec) takes the same path: volume deletion
// supersedes the removal gates.
func (r *VolumeReconciler) reconcileDeletion(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, mine bool) (ctrl.Result, error) {
	if !mine && !controllerutil.ContainsFinalizer(vol, constants.FinalizerPrefix+r.NodeName) {
		return ctrl.Result{}, nil
	}
	if err := r.teardown(ctx, vol); err != nil {
		return r.handleTeardownError(ctx, vol, err)
	}
	// Only once the device is gone: a retrying teardown must not blank the
	// metrics of a volume that still exists.
	r.forget(vol.Name)
	return ctrl.Result{}, r.removeFinalizer(ctx, vol)
}

// reconcileRemoval tears down a replica that was removed from
// spec.replicas while the volume lives on. It only proceeds when losing
// this leg cannot lose data: every remaining replica must be UpToDate and
// connected, and no snapshot may reference the volume — snapshots exist as
// backend CoW state on every replica, and restores expect to find them
// wherever the volume is placed.
func (r *VolumeReconciler) reconcileRemoval(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	finalizer := constants.FinalizerPrefix + r.NodeName
	if !controllerutil.ContainsFinalizer(vol, finalizer) {
		return ctrl.Result{}, nil
	}
	if reason := r.removalBlocked(ctx, vol); reason != "" {
		log.Info("replica removal blocked", "volume", vol.Name, "reason", reason)
		// The block interrupts the teardown episode: a busy streak from
		// before it must not pre-bias the attempts after it unblocks.
		r.clearBusyFails(vol.Name)
		st := vol.Status.PerNode[r.NodeName]
		st.Message = "replica removal blocked: " + reason
		st.LastProbedAt = probeNow()
		if err := r.patchStatus(ctx, vol, st); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.teardown(ctx, vol); err != nil {
		return r.handleTeardownError(ctx, vol, err)
	}
	r.forget(vol.Name)
	// Drop this node's status slot — merge-patch null deletes the key.
	// Best-effort ordering: a crash here leaves a stale slot, which
	// nothing reads (phase and growth iterate spec.replicas only).
	patch := fmt.Appendf(nil, `{"status":{"perNode":{%q:null}}}`, r.NodeName)
	if err := r.Status().Patch(ctx, vol, client.RawPatch(types.MergePatchType, patch)); err != nil &&
		!apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	log.Info("replica removed", "volume", vol.Name)
	return ctrl.Result{}, r.removeFinalizer(ctx, vol)
}

// removalBlocked reports why this replica must not be torn down yet, or ""
// when it is safe. The remaining replicas' health is read from the CRD
// status the peers report — by removal time the peers have already dropped
// this node from their configs, so the local kernel's view of them is gone.
func (r *VolumeReconciler) removalBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) string {
	if vol.Spec.DRBD == nil {
		// An unreplicated volume's lone entry moved: there is no peer
		// holding the data, so tearing down here is data loss. Unsupported.
		return "volume has no replication layer; refusing to drop the only copy"
	}
	// A diskless tie-breaker holds no backend CoW state, so snapshots do
	// not pin it — only the remaining-replica health gate below applies
	// (pulling the quorum vote while a data leg is down would drop the
	// volume to 1-of-2). The self-reported status marker survives the
	// entry's removal from spec; never key this on the kernel DiskState,
	// which a detached diskful replica also reads.
	if !vol.Status.PerNode[r.NodeName].Diskless {
		snaps := &miroirv1alpha1.MiroirSnapshotList{}
		if err := r.List(ctx, snaps); err != nil {
			return "cannot list snapshots: " + err.Error()
		}
		for _, s := range snaps.Items {
			if s.Spec.VolumeName == vol.Name {
				return "snapshot " + s.Name + " exists; delete the volume's snapshots first"
			}
		}
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			continue
		}
		st, ok := vol.Status.PerNode[rep.Node]
		if !ok || st.DiskState != drbd.DiskUpToDate || !st.Connected {
			return "replica on " + rep.Node + " is not UpToDate and connected"
		}
	}
	return ""
}

func (r *VolumeReconciler) teardown(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	slot := vol.Status.PerNode[r.NodeName]
	if vol.Spec.DRBD != nil {
		if err := r.DRBD.Down(ctx, vol.Name); err != nil {
			// A kernel-wedged resource must not classify as ErrBusy: the
			// fast retry would strand another unkillable drbdsetup down
			// every cycle, and nothing but a reboot clears the wedge.
			if errors.Is(err, drbd.ErrWedged) {
				return err
			}
			// A still-staged device answers "held open"; classify it as
			// ErrBusy so teardown takes the 10s retry, not the workqueue's
			// minutes-long backoff (NodeUnstage releases it shortly). An
			// orphaned hold no retry can release routes around the down
			// instead of parking on it forever.
			busy := backend.Busy(err)
			if r.orphanHold(ctx, vol, slot, busy) {
				return r.reclaimOrphanHold(ctx, vol, slot, busy)
			}
			// Anything else parks as a busy outcome with no force-detach:
			// destroying a backing under a live consumer loses its data.
			return busy
		}
		// With the resource down, wipe the DRBD metadata before the sweep
		// removes the device, so freed blocks cannot carry a stale generation
		// identifier into a reuse. Best-effort: the sweep
		// destroys the whole device anyway, so a wipe (or pool-resolution)
		// failure must not block the deletion. Never key the DeviceCreated
		// check on the kernel DiskState: a diskful replica reads "Diskless"
		// after an I/O-error detach, and skipping its wipe+delete would
		// leak the backing device.
		if slot.DeviceCreated && !slot.Diskless {
			if pool, err := r.Pools.Get(volumePoolOn(vol, r.NodeName)); err != nil {
				ctrl.LoggerFrom(ctx).V(1).Info("cannot resolve the leg's pool for the metadata wipe; sweep delete destroys metadata with the device",
					"volume", vol.Name, "error", err)
			} else if err := r.DRBD.WipeMetadata(ctx, vol.Name, pool.Backend.DevicePath(vol.Name), slot.DRBDMinor); err != nil {
				ctrl.LoggerFrom(ctx).V(1).Info("wipe-md failed during teardown; backing delete will destroy metadata",
					"volume", vol.Name, "error", err)
			}
		}
	}
	// Delete from every pool instead of resolving one: the leg's pool can
	// be unknowable here (crash before the first status patch, a diskless
	// re-add over a leftover backing), and each backend's Delete succeeds
	// when its device is absent — see Pools.SweepDelete.
	return r.Pools.SweepDelete(ctx, vol.Name)
}

// removeFinalizer releases this node's own finalizer once local teardown
// is done; the volume disappears when the last replica's agent finishes.
func (r *VolumeReconciler) removeFinalizer(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	return removeNodeFinalizer(ctx, r.Client, vol, r.NodeName)
}

// removeNodeFinalizer drops this node's teardown finalizer, swallowing
// conflicts (the watch retriggers) and not-found (the object is already
// gone — the goal state). Shared by the volume and snapshot reconcilers so
// the subtle swallow semantics cannot drift apart.
func removeNodeFinalizer(ctx context.Context, c client.Client, obj client.Object, node string) error {
	finalizer := constants.FinalizerPrefix + node
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(obj, finalizer)
	if err := c.Update(ctx, obj); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// wedgedRequeue spaces teardown retries of a kernel-wedged resource. The
// 10s ErrBusy cadence would strand another unkillable drbdsetup down every
// cycle; nothing but a node reboot clears the wedge, so the retry only
// needs to notice that the reboot happened.
const wedgedRequeue = 5 * time.Minute

// parkWithMessage stamps the cause on this node's status slot and parks
// the retry — the shared tail of every teardown/realize escalation
// (handleTeardownError, reportRestoreOrphan), kept in one place so
// the park contract cannot drift between them.
func (r *VolumeReconciler) parkWithMessage(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cause error, requeue time.Duration) (ctrl.Result, error) {
	st := vol.Status.PerNode[r.NodeName]
	st.Message = cause.Error()
	st.LastProbedAt = probeNow()
	if err := r.patchStatus(ctx, vol, st); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// busyFailLimit is how many consecutive ErrBusy teardown outcomes ride the
// fast 10s retry — NodeUnstage normally releases the device within a few
// cycles — before the loop escalates; busyRetryAfter is the parked cadence.
// The finalizer is never released on a busy outcome itself: the hold may
// be a live mount, and force-releasing would leak the backing device or
// destroy it under a consumer. The one exit is the orphaned-
// hold reclaim (orphanHold), which completes a real teardown of the
// backing first and only fires past this same threshold.
const (
	busyFailLimit  = 30 // ~5 minutes at the 10s cadence
	busyRetryAfter = time.Minute
)

// teardownStep is how the agent answers one failed teardown.
type teardownStep int

const (
	// teardownFail: an error of no known class. Surface it and let the
	// workqueue back off.
	teardownFail teardownStep = iota
	// teardownRetryBusy: a device held open, inside the fast-retry budget.
	// A still-staged (or force-deleted) pod holds it; NodeUnstage releases
	// it once the consumer moves off this node.
	teardownRetryBusy
	// teardownParkBusy: a device held open past busyFailLimit.
	teardownParkBusy
	// teardownParkWedged: the kernel can no longer finish the teardown
	// (drbd.ErrWedged), or the node-scoped breaker refused the command
	// (backend.ErrNodeWedged). Nothing but a node reboot clears either.
	teardownParkWedged
)

// teardownPlan is the verdict on one failed teardown.
type teardownPlan struct {
	step teardownStep
	// busyFails is the busy streak to carry into the next outcome; zero
	// clears it. Every non-busy outcome resets the streak.
	busyFails int
	// requeue is the retry cadence; unused for teardownFail.
	requeue time.Duration
	// announce is set on the cycle an escalation starts, and only then: the
	// parked retry can outlive its cause by hours, and re-emitting an
	// identical Event and Error log every cycle is pure churn (issue #319:
	// one parked volume streamed 648 Error lines, a wedged one 123 Events).
	announce bool
	// stamp is set while the cause is not yet on the status slot, so a
	// parked cycle repeats no status write either.
	stamp bool
}

// planTeardownError triages one failed teardown outcome from the error, the
// Message already on this node's status slot and the busy streak so far.
// It is shared by the deletion and removal paths so the streak bookkeeping
// cannot drift, and it reads only its arguments, so the escalation rules can
// be tested without running a teardown; handleTeardownError carries the plan
// out.
//
// A wedge escalates on the cycle its cause first reaches the slot. A busy
// outcome rides the fast 10s retry below busyFailLimit, escalates on the
// crossing cycle, and stays parked after it.
func planTeardownError(err error, slotMessage string, busyFails int) teardownPlan {
	stamp := slotMessage != err.Error()
	switch {
	// backend.ErrNodeWedged rides the same path as drbd.ErrWedged: both mean
	// this node cannot finish the teardown until it reboots.
	case errors.Is(err, drbd.ErrWedged) || errors.Is(err, backend.ErrNodeWedged):
		return teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: stamp, stamp: stamp}
	case errors.Is(err, backend.ErrBusy):
		fails := busyFails + 1
		if fails < busyFailLimit {
			return teardownPlan{step: teardownRetryBusy, busyFails: fails, requeue: 10 * time.Second}
		}
		return teardownPlan{
			step: teardownParkBusy, busyFails: fails, requeue: busyRetryAfter,
			announce: fails == busyFailLimit, stamp: stamp,
		}
	default:
		return teardownPlan{step: teardownFail}
	}
}

// handleTeardownError carries out planTeardownError's verdict on one failed
// teardown: the busy streak, the wedged gauge, the logs and Warning Events of
// an escalation, the status Message, and the retry cadence.
func (r *VolumeReconciler) handleTeardownError(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, err error) (ctrl.Result, error) {
	plan := planTeardownError(err, vol.Status.PerNode[r.NodeName].Message, r.recall(vol.Name).busyFails)
	r.remember(vol.Name, func(m *volumeMemory) { m.busyFails = plan.busyFails })
	log := ctrl.LoggerFrom(ctx)
	if plan.step != teardownParkWedged {
		// Any non-wedged outcome means a previously reported wedge is gone
		// — e.g. the reboot happened and the device is now merely held
		// open — so the critical alert must stop paging for it.
		clearWedged(vol.Name)
	}
	switch plan.step {
	case teardownParkWedged:
		// Raised every cycle, announced or not: the gauge is process state
		// and must survive an agent restart into an already-stamped
		// Message. It clears on any non-wedged teardown outcome, on
		// teardown success (dropVolumeMetrics), and when the CR vanishes
		// (volumeGone). A node-wedged outcome must not clear it either:
		// the node breaker refuses the status read that Down needs to see
		// the per-resource signature, so every wedged volume would
		// otherwise report as recovered and delete the series the shipped
		// critical alert pages on, exactly when the node is worst off.
		recordWedged(vol)
		if !plan.announce {
			log.V(1).Info("teardown still wedged in kernel; retry stays parked", "volume", vol.Name)
			break
		}
		log.Error(err, "teardown wedged in kernel", "volume", vol.Name)
		if r.Recorder != nil {
			detail := "device stuck Detaching with connections gone (LINBIT/drbd#137)"
			switch {
			case errors.Is(err, drbd.ErrDownStranded):
				detail = "a drbdsetup down is stranded in uninterruptible sleep, pinning the resource's DOWN_IN_PROGRESS flag so every open fails with EAGAIN"
			case errors.Is(err, backend.ErrNodeWedged):
				detail = "the node's storage stack has stranded enough commands that miroir stopped spawning them"
			}
			r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownWedged", "Teardown",
				"DRBD cannot tear down %s: %s; reboot node %s to clear it",
				vol.Name, detail, r.NodeName)
		}
	case teardownRetryBusy:
		// The cause is logged on every fast cycle, and again on the
		// escalation below: an ErrBusy from the backend sweep looks
		// identical to a held-open device without it.
		log.Info("device busy during teardown, retrying",
			"volume", vol.Name, "attempts", plan.busyFails, "error", err)
	case teardownParkBusy:
		if !plan.announce {
			log.V(1).Info("teardown still busy; retry stays parked",
				"volume", vol.Name, "attempts", plan.busyFails)
			break
		}
		log.Error(err, "teardown still busy; parking the retry",
			"volume", vol.Name, "attempts", plan.busyFails)
		if r.Recorder != nil {
			r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownBusy", "Teardown",
				"cannot tear down %s on node %s after %d attempts: %v; something still holds the device (or its backing) open",
				vol.Name, r.NodeName, plan.busyFails, err)
			// A held-open device that reached here was not reclaimed as an
			// orphaned hold, so a live consumer (or a still-mounted device)
			// pins it: only that consumer releasing the device ends the wait.
			if strings.Contains(err.Error(), "held open") {
				r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "TeardownLiveConsumer", "Teardown",
					"deletion of %s on node %s is blocked by a live consumer still holding the device open; "+
						"teardown will retry once the consumer releases it — no force-detach will be attempted",
					vol.Name, r.NodeName)
			}
		}
	default:
		return ctrl.Result{}, err
	}
	if !plan.stamp {
		return ctrl.Result{RequeueAfter: plan.requeue}, nil
	}
	return r.parkWithMessage(ctx, vol, err, plan.requeue)
}

// clearBusyFails resets the volume's consecutive busy-teardown count.
func (r *VolumeReconciler) clearBusyFails(name string) {
	r.remember(name, func(m *volumeMemory) { m.busyFails = 0 })
}

// procDir resolves the /proc override for the orphaned-hold checks.
func (r *VolumeReconciler) procDir() string {
	if r.ProcDir != "" {
		return r.ProcDir
	}
	return "/proc"
}

// orphanHold reports whether a busy teardown outcome is the unwinnable
// held-open state of issue #319 — the device pinned by a leaked freeze's
// dead superblock, which no retry, restart, or unstage can ever release —
// as opposed to a still-staged device NodeUnstage frees shortly. It
// requires all of: a deletion-targeted volume (a removed replica's volume
// lives on and keeps the parked retry), a known minor, a busy streak past
// the escalation threshold (an in-flight unstage never lasts that long),
// a held-open failure whose reported openers have all exited (a live
// consumer's opener pid is alive; a plain fs mount's dead mount(8) pid is
// disambiguated by the mount scan below), and the device mounted in no
// mount namespace on the node. Any doubt — no opener list, an unreadable
// /proc, a surviving mount — reads as "live consumer" and the park stands.
func (r *VolumeReconciler) orphanHold(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, slot miroirv1alpha1.ReplicaStatus, cause error) bool {
	if vol.DeletionTimestamp.IsZero() || slot.DRBDMinor <= 0 {
		return false
	}
	if r.recall(vol.Name).busyFails < busyFailLimit {
		return false
	}
	if !strings.Contains(cause.Error(), "held open") {
		return false
	}
	pids := openerPids(cause.Error())
	if len(pids) == 0 {
		return false
	}
	for _, pid := range pids {
		if pidAlive(r.procDir(), pid) {
			return false
		}
	}
	mounted, err := deviceMountedAnywhere(r.procDir(), slot.DRBDMinor)
	if err != nil {
		ctrl.LoggerFrom(ctx).V(1).Info("cannot scan mount namespaces for the orphaned-hold check; teardown stays parked",
			"volume", vol.Name, "error", err)
		return false
	}
	return !mounted
}

// reclaimOrphanHold routes a deletion around the pinned minor: the only
// thing an orphaned hold pins is the DRBD minor itself, so force-detach
// frees the backing and the sweep destroys it, letting the caller release
// the finalizer through teardown's normal tail. The rendered config goes
// with the volume — left behind it would hold the volume's DRBD port
// hostage against the next volume handed the recycled number (see
// drbd.RemoveConfig) — while the minor assignment deliberately stays:
// the kernel minor is pinned until a reboot, releasing the number would
// hand it to a new volume, and the startup orphan sweep reaps the
// reservation once the reboot clears the state. On any failure the
// original busy outcome stands and the parked retry re-evaluates
// (ForceDetach no-ops once detached, so a failed sweep or config removal
// converges on the next cycle).
func (r *VolumeReconciler) reclaimOrphanHold(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, slot miroirv1alpha1.ReplicaStatus, cause error) error {
	if err := r.DRBD.ForceDetach(ctx, vol.Name, slot.DRBDMinor); err != nil {
		ctrl.LoggerFrom(ctx).Info("orphaned-hold reclaim could not detach; teardown stays parked",
			"volume", vol.Name, "error", err)
		return cause
	}
	if err := r.Pools.SweepDelete(ctx, vol.Name); err != nil {
		return err
	}
	if err := r.DRBD.RemoveConfig(vol.Name); err != nil {
		return err
	}
	ctrl.LoggerFrom(ctx).Info("teardown reclaimed from an orphaned device hold; kernel minor stays pinned until reboot",
		"volume", vol.Name, "minor", slot.DRBDMinor, "cause", cause.Error())
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeNormal, "TeardownReclaimed", "Teardown",
			"backing of %s reclaimed on node %s: the DRBD device was held open only by exited processes (leaked freeze, issue #311); orphaned minor %d frees itself on the next reboot",
			vol.Name, r.NodeName, slot.DRBDMinor)
	}
	return nil
}
