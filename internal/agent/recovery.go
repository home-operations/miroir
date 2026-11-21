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
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// errSplitBrain gives the split-brain transition log a real error value.
var errSplitBrain = errors.New("DRBD split-brain detected")

// recoverSplitBrain reacts to a split-brain, seen locally (a StandAlone
// connection) or reported by a peer's status slot. A volume that never held
// data holds nothing to lose, so it applies ResolveSplitBrain to self-heal —
// the safety net for volumes born under releases whose per-node day0
// seeding could birth-diverge (issue #139; the birth generation now makes
// that impossible for new volumes). "Held data" is either Activated
// (staged for a consumer) or Formatted (carries a filesystem) — Formatted
// latches before the grow-to-fill step, so a formatted volume whose stage
// failed at resize is still covered. Such a volume logs the manual remedy on
// the transition edge (only where the split is locally visible — the losing
// leg's own slot never records it, so it would log every poll) and is left
// for an operator.
//
// Entry is floored to once per poll interval: connection flaps — recovery's
// own, and drbdadm adjust auto-reconnecting a parked StandAlone leg every
// pass — each emit a DRBD event that requeues the volume, re-entering here
// several times per second. The floor caps both the recovery
// attempts and the manual-remedy log, whose transition edge the flapping
// status slot keeps re-opening. Failures are retried on a later poll — the
// split state is never fast-path cached.
func (r *VolumeReconciler) recoverSplitBrain(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, localSplit bool) {
	r.recoveryMu.Lock()
	if time.Since(r.lastRecovery[vol.Name]) < drbdPollInterval {
		r.recoveryMu.Unlock()
		return
	}
	if r.lastRecovery == nil {
		r.lastRecovery = map[string]time.Time{}
	}
	r.lastRecovery[vol.Name] = time.Now()
	r.recoveryMu.Unlock()

	log := ctrl.LoggerFrom(ctx)
	// The kernel log carries the handshake's actual verdict ("Split-Brain
	// detected", "Unrelated data, aborting") — the status API only shows
	// the resulting StandAlone. Captured before recovery mutates state,
	// floored with it to once per poll interval.
	kmsgPath := cmp.Or(r.KmsgPath, defaultKmsgPath)
	if lines := captureKmsg(kmsgPath, vol.Name, 30); len(lines) > 0 {
		log.Info("kernel log at split-brain detection", "volume", vol.Name, "kmsg", lines)
	} else {
		log.V(1).Info("kernel log unreadable at split-brain detection", "volume", vol.Name, "path", kmsgPath)
	}
	if vol.Status.Activated || vol.Status.Formatted {
		if localSplit && !vol.Status.PerNode[r.NodeName].SplitBrain {
			log.Error(errSplitBrain,
				"manual resolution required (drbdadm connect --discard-my-data on the losing node)",
				"volume", vol.Name)
		}
		return
	}
	log.Info("auto-recovering split-brain on never-written volume", "volume", vol.Name)
	if err := r.DRBD.ResolveSplitBrain(ctx, res); err != nil {
		log.Error(err, "split-brain auto-recovery failed", "volume", vol.Name)
	}
}

// peerReportedSplitBrain reports whether any other node's status slot
// records a split-brain. The losing leg of a birth split never parks
// StandAlone locally — the survivor refuses the handshake and the loser
// returns to Connecting — so the peers' reported state is its only trigger.
// Each slot holds only its own node's kernel state (patchStatus writes
// st.SplitBrain), so the signal clears as soon as the survivor reconnects;
// a leg never echoes a peer's report back into its own slot.
func peerReportedSplitBrain(vol *miroirv1alpha1.MiroirVolume, self string) bool {
	for node, st := range vol.Status.PerNode {
		if node != self && st.SplitBrain {
			return true
		}
	}
	return false
}

// recoverStuckResync heals the resync states DRBD leaves behind with
// nothing to drain them:
//
//   - A resync armed but never started: a rapid
//     promote/demote/promote of a diskless primary mints two current UUIDs
//     back to back, a diskful leg handshakes against a one-generation-stale
//     view of its peer and sets a full bitmap slot (WFBitMapS, peer-disk
//     clamped Consistent). From there a timing lottery picks the terminal
//     shape: a second after-unstable pass — equal UUIDs by then — collapses
//     the pending exchange back to Established without undoing it, or that
//     pass never runs and the leg parks in WFBitMapS waiting for a bitmap
//     reply the peer already discarded. Both wear
//     StuckResyncPeers.
//   - A resync whose two ends disagree on how it ended (issue #576,
//     LINBIT/drbd#148): with a diskless Primary writing, the source
//     finishes and settles Established while the target never leaves
//     WFBitMapT (or settles Established) with its disk Inconsistent and
//     nothing left to sync. Only the target can act — a graceful
//     disconnect is a cluster-wide transaction the target vetoes while a
//     read-only opener pins it, from either side — so it wears
//     UnsettledResyncPeers, behind unsettledResyncActionable's gate, and
//     is cycled with a forced fallback.
//   - Bits stranded by a bitmap clear the kernel refused mid peer-teardown
//     ("bitmap locked", issue #389), left on a freshly promoted Primary
//     toward a Secondary after a failover, or left between
//     two Secondaries after a connectivity blip, everything
//     otherwise healthy (StaleBitmapPeers, behind staleBitmapActionable's
//     gates — the same kernel shape is a verify finding, which stays
//     manual).
//
// None drains in place: the kernel only reconciles a dirty bitmap on a
// connection transition (its missed-end-of-resync rescue runs at connect),
// so the volume sits with degraded redundancy accounting until the
// connection cycles.
//
// Recovery cycles exactly the affected peer connections, re-running their
// handshakes. Safe on any volume, held data or not: identical data
// reconnects with zero movement — the re-handshake classifies the stale
// bitmap as a missed resync end and discards it — and genuinely divergent
// data starts the resync the bits called for. No Activated gate, unlike
// split-brain recovery, because nothing is ever discarded.
//
// The signature must persist through a full poll interval before recovery
// runs: a status snapshot can catch a live handshake between setting the
// bitmap and starting its resync, and cycling that would abort real work.
// Each attempt re-arms the window, flooring retries. Returns true while the
// signature is live so the caller keeps the volume out of the fast path —
// the stuck state is otherwise steady, and a stored fingerprint would park
// the confirmation clock unread.
func (r *VolumeReconciler) recoverStuckResync(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) bool {
	peers := st.StuckResyncPeers
	if r.staleBitmapActionable(vol, st) {
		peers = mergePeers(peers, st.StaleBitmapPeers)
	}
	var stalled map[int32]bool
	if r.unsettledResyncActionable(vol, st) {
		stalled = st.UnsettledResyncPeers
		peers = mergePeers(peers, stalled)
	}
	// A diskless leg holds no bitmap, so it can never be the stranded
	// side; whatever it sees is a diskful pair's business to resolve.
	if localDiskless || len(peers) == 0 {
		r.recoveryMu.Lock()
		delete(r.stuckSince, vol.Name)
		r.recoveryMu.Unlock()
		// A stalled target its gate holds back stays out of the fast
		// path: the gate reads the other legs' status slots, which no
		// kernel event re-breaks the fingerprint for.
		return len(st.UnsettledResyncPeers) > 0
	}
	key := stuckPeerKey(peers)
	r.recoveryMu.Lock()
	mark, seen := r.stuckSince[vol.Name]
	if !seen || mark.peers != key {
		if r.stuckSince == nil {
			r.stuckSince = map[string]stuckMark{}
		}
		r.stuckSince[vol.Name] = stuckMark{since: time.Now(), peers: key}
		r.recoveryMu.Unlock()
		return true
	}
	if time.Since(mark.since) < drbdPollInterval {
		r.recoveryMu.Unlock()
		return true
	}
	r.stuckSince[vol.Name] = stuckMark{since: time.Now(), peers: key}
	r.recoveryMu.Unlock()

	log := ctrl.LoggerFrom(ctx)
	for peerID := range peers {
		log.Info("cycling peer connection to re-run the sync handshake (resync never settled, no resync running)",
			"volume", vol.Name, "peerNodeID", peerID, "outOfSyncKiB", st.OutOfSyncKiB)
		cycle := r.DRBD.CyclePeerConnection
		if stalled[peerID] {
			cycle = r.DRBD.CycleStalledTargetConnection
		}
		if err := cycle(ctx, vol.Name, peerID); err != nil {
			log.Error(err, "stuck-resync connection cycle failed", "volume", vol.Name, "peerNodeID", peerID)
			continue
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "StuckResyncRecovered", "CycleConnection",
				"DRBD left the resync toward peer node %d unsettled with no resync running; cycled the connection so the handshake resolves it", peerID)
		}
	}
	return true
}

// mergePeers unions two candidate sets without mutating either.
func mergePeers(a, b map[int32]bool) map[int32]bool {
	if a == nil {
		return b
	}
	a = maps.Clone(a)
	maps.Copy(a, b)
	return a
}

// stuckMark is one volume's stranded-bitmap confirmation clock: when the
// signature was first seen, and on which candidate peers.
type stuckMark struct {
	since time.Time
	peers string
}

// stuckPeerKey renders a candidate set in a stable form for stuckMark.
func stuckPeerKey(peers map[int32]bool) string {
	return fmt.Sprint(slices.Sorted(maps.Keys(peers)))
}

// staleBitmapActionable reports whether the stale-bitmap candidates in
// StaleBitmapPeers are safe to cycle (issues #389 and #469). Their kernel
// shape is indistinguishable from a verify finding, so everything else
// must read healthy — an UpToDate disk with quorum, every diskful peer
// connected and UpToDate, no resync, verify, or split-brain in flight —
// and the coordinator's verify record must be settled and clean: a
// recorded finding, or a verify started and not yet recorded (findings
// the kernel may hold that no status field accounts for), keeps the
// case manual — auto-resyncing a real finding would destroy the evidence
// of which leg was wrong (the record clears once a later verify reports
// 0 after the operator's own cycle). Roles do
// not settle the direction — the re-handshake takes it from generation
// UUIDs, and equal generations discard the bits outright — so the pair's
// roles are not a gate. A live Primary somewhere in the volume is: with
// no Primary at all, two Secondaries holding bits may be survivors of a
// lost Primary, a state the kernel reconciles with its own resync on the
// reconnect, so that case is left to the kernel rather than cycled here.
func (r *VolumeReconciler) staleBitmapActionable(vol *miroirv1alpha1.MiroirVolume, st drbd.Status) bool {
	if len(st.StaleBitmapPeers) == 0 || st.DiskState != drbd.DiskUpToDate ||
		!st.Quorum || st.Resyncing || st.SplitBrain || (!st.Primary && !st.PeerPrimary) {
		return false
	}
	if !diskfulPeersConnected(st, vol, r.NodeName) || !diskfulPeersUpToDate(st, vol, r.NodeName) {
		return false
	}
	if coord := vol.Spec.FirstDiskfulReplica(); coord != nil {
		slot := vol.Status.PerNode[coord.Node]
		if slot.VerifyStartedAt != nil {
			return false
		}
		if v := slot.LastVerifyOutOfSyncBytes; v != nil && *v > 0 {
			return false
		}
	}
	return true
}

// unsettledResyncActionable reports whether the stalled-target candidates
// in UnsettledResyncPeers are safe to cycle. Their kernel
// shape — this leg Inconsistent, nothing to sync, no resync starting — is
// also the ordinary end of a resync run while this leg could not see the
// Primary: DRBD keeps an unstable target Inconsistent and re-runs the
// handshake by itself once the Primary is back in view, and a cycle in
// the meantime only repeats the unstable resync. So a Primary this leg
// does not see keeps the case with the kernel: act only when a connected
// peer holds the role, or every other placed leg's status slot — client
// legs included, the usual home of an RWO consumer's Primary — is fresh
// and reports not holding it. A missing or stale slot (StaleProbeThreshold,
// the same bar computePhase uses) is an agent that cannot report — a
// Primary may have come up behind it — and a crashed Primary's lingering
// PrimarySince holds the case until its agent returns: both err toward
// leaving the kernel to it.
func (r *VolumeReconciler) unsettledResyncActionable(vol *miroirv1alpha1.MiroirVolume, st drbd.Status) bool {
	if len(st.UnsettledResyncPeers) == 0 || st.SplitBrain {
		return false
	}
	if st.PeerPrimary {
		return true
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Node != r.NodeName && rep.Address != "" && !slotRulesOutPrimary(vol, rep.Node) {
			return false
		}
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Address != "" && !slotRulesOutPrimary(vol, cl.Node) {
			return false
		}
	}
	return true
}

// slotRulesOutPrimary reports whether a leg's status slot is fresh and
// reports it not holding the Primary role.
func slotRulesOutPrimary(vol *miroirv1alpha1.MiroirVolume, node string) bool {
	slot, ok := vol.Status.PerNode[node]
	return ok && slot.PrimarySince == nil && slot.LastProbedAt != nil &&
		time.Since(slot.LastProbedAt.Time) <= StaleProbeThreshold
}

// handleSplitBrain detects an active split and routes it into recovery.
// The losing leg of a split never parks StandAlone locally — it sits
// Connecting while the survivor refuses the handshake — so its only signal
// that it must discard is a peer's reported split-brain.
// Gated on !connected so a stale peer report cannot churn a healthy
// volume.
func (r *VolumeReconciler) handleSplitBrain(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, connected bool) bool {
	splitActive := st.SplitBrain || (!connected && peerReportedSplitBrain(vol, r.NodeName))
	if splitActive {
		r.recoverSplitBrain(ctx, vol, res, st.SplitBrain)
	}
	return splitActive
}
