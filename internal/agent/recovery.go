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

// splitStep is what one pass does about a split-brain.
type splitStep int

const (
	// splitIdle: no split, or one still inside the floor.
	splitIdle splitStep = iota
	// splitWatch: the volume held data and the operator has been told.
	splitWatch
	// splitReport: the volume held data and this is the transition edge;
	// log the manual remedy.
	splitReport
	// splitResolve: the volume never held data; discard one generation.
	splitResolve
)

// peerCycle is one peer connection whose handshake a pass re-runs.
type peerCycle struct {
	peerID int32
	// stalledTarget selects the forced fallback a stalled resync target
	// needs (drbd.Driver.CycleStalledTargetConnection).
	stalledTarget bool
}

// planSplitBrain decides what one pass does about a split-brain, seen
// locally (a StandAlone connection) or reported by a peer's status slot. Like
// planStuckResync it reads only its arguments, so its rules can be tested
// against a status and a clock without a kernel or an exec fake;
// runRecovery carries both plans out. active keeps the volume out of the
// fast path, which a split does not break by itself.
//
// The losing leg of a split never parks StandAlone locally (it sits
// Connecting while the survivor refuses the handshake), so its only signal
// that it must discard is a peer's report; that one is gated on !connected
// so a stale report cannot churn a healthy volume.
//
// A volume that never held data holds nothing to lose, so the step is
// splitResolve: ResolveSplitBrain self-heals it, the safety net for volumes
// born under releases whose per-node day0 seeding could birth-diverge (issue
// #139; the birth generation now makes that impossible for new volumes).
// "Held data" is either Activated (staged for a consumer) or Formatted
// (carries a filesystem). Formatted latches before the grow-to-fill step, so
// a formatted volume whose stage failed at resize is still covered. Such a
// volume reports the manual remedy on the transition edge (only where the
// split is locally visible: the losing leg's own slot never records it, so
// it would report every poll) and is left for an operator.
//
// Entry is floored to once per poll interval: connection flaps (recovery's
// own, and drbdadm adjust auto-reconnecting a parked StandAlone leg every
// pass) each emit a DRBD event that requeues the volume, re-entering here
// several times per second. The floor caps both the recovery attempts and
// the manual-remedy log, whose transition edge the flapping status slot
// keeps re-opening. Failures are retried on a later poll; the split state is
// never fast-path cached.
func planSplitBrain(vol *miroirv1alpha1.MiroirVolume, node string, st drbd.Status, connected bool, last, now time.Time) (active bool, step splitStep, next time.Time) {
	if !st.SplitBrain && (connected || !peerReportedSplitBrain(vol, node)) {
		return false, splitIdle, last
	}
	if now.Sub(last) < drbdPollInterval {
		return true, splitIdle, last
	}
	switch {
	case !vol.Status.Activated && !vol.Status.Formatted:
		return true, splitResolve, now
	case st.SplitBrain && !vol.Status.PerNode[node].SplitBrain:
		return true, splitReport, now
	default:
		return true, splitWatch, now
	}
}

// runRecovery works this pass's replication faults: for the split-brain and
// then for the stranded bitmaps it plans against the volume's clock, stores
// the clock and carries the plan out. The second plan is made only after the
// first has run, so its clock starts once split-brain recovery is done with
// the connections. It reports whether recovery is live, which keeps the
// volume out of the fast path.
func (r *VolumeReconciler) runRecovery(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, connected, localDiskless bool) bool {
	splitActive := r.runSplitBrain(ctx, vol, res, st, connected)
	stuckActive := r.runStuckResync(ctx, vol, st, localDiskless)
	return splitActive || stuckActive
}

func (r *VolumeReconciler) runSplitBrain(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, connected bool) bool {
	var (
		active bool
		step   splitStep
	)
	r.remember(vol.Name, func(m *volumeMemory) {
		active, step, m.lastSplit = planSplitBrain(vol, r.NodeName, st, connected, m.lastSplit, time.Now())
	})
	if step == splitIdle {
		return active
	}

	log := ctrl.LoggerFrom(ctx)
	// The kernel log carries the handshake's actual verdict ("Split-Brain
	// detected", "Unrelated data, aborting"); the status API only shows
	// the resulting StandAlone. Captured before recovery mutates state,
	// floored with it to once per poll interval.
	kmsgPath := cmp.Or(r.KmsgPath, defaultKmsgPath)
	if lines := captureKmsg(kmsgPath, vol.Name, 30); len(lines) > 0 {
		log.Info("kernel log at split-brain detection", "volume", vol.Name, "kmsg", lines)
	} else {
		log.V(1).Info("kernel log unreadable at split-brain detection", "volume", vol.Name, "path", kmsgPath)
	}
	switch step {
	case splitReport:
		log.Error(errSplitBrain,
			"manual resolution required (drbdadm connect --discard-my-data on the losing node)",
			"volume", vol.Name)
	case splitResolve:
		log.Info("auto-recovering split-brain on never-written volume", "volume", vol.Name)
		if err := r.DRBD.ResolveSplitBrain(ctx, res); err != nil {
			log.Error(err, "split-brain auto-recovery failed", "volume", vol.Name)
		}
	}
	return active
}

func (r *VolumeReconciler) runStuckResync(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) bool {
	var (
		active bool
		cycles []peerCycle
	)
	r.remember(vol.Name, func(m *volumeMemory) {
		active, cycles, m.stuck = planStuckResync(vol, r.NodeName, st, localDiskless, m.stuck, time.Now())
	})

	log := ctrl.LoggerFrom(ctx)
	for _, c := range cycles {
		log.Info("cycling peer connection to re-run the sync handshake (resync never settled, no resync running)",
			"volume", vol.Name, "peerNodeID", c.peerID, "outOfSyncKiB", st.OutOfSyncKiB)
		cycle := r.DRBD.CyclePeerConnection
		if c.stalledTarget {
			cycle = r.DRBD.CycleStalledTargetConnection
		}
		if err := cycle(ctx, vol.Name, c.peerID); err != nil {
			log.Error(err, "stuck-resync connection cycle failed", "volume", vol.Name, "peerNodeID", c.peerID)
			continue
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "StuckResyncRecovered", "CycleConnection",
				"DRBD left the resync toward peer node %d unsettled with no resync running; cycled the connection so the handshake resolves it", c.peerID)
		}
	}
	return active
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

// planStuckResync heals the resync states DRBD leaves behind with
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
//   - A resync with a source at both ends (issue #675): the first case's
//     stale handshake runs on both diskful legs at once, each takes the
//     other's bitmap for a target's reply, and both settle SyncSource
//     waiting for resync requests only a target sends. Each leg sees a
//     live resync's source side, so the proof is the peer's status slot
//     (mutualSourcePeers, mutualSourceProbes), and one leg of the pair
//     acts (lowerNodeID).
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
// mark is that confirmation clock: a changed candidate set restarts the wait
// so a newly seen peer never rides an older peer's clock, and each attempt
// re-arms the window, flooring retries. active is true while the signature
// is live so the caller keeps the volume out of the fast path: the stuck
// state is otherwise steady, and a stored fingerprint would park the
// confirmation clock unread.
func planStuckResync(vol *miroirv1alpha1.MiroirVolume, node string, st drbd.Status, localDiskless bool, mark *stuckMark, now time.Time) (active bool, cycle []peerCycle, next *stuckMark) {
	peers := st.StuckResyncPeers
	if staleBitmapActionable(vol, node, st) {
		peers = mergePeers(peers, st.StaleBitmapPeers)
	}
	var stalled map[int32]bool
	if unsettledResyncActionable(vol, node, st, now) {
		stalled = st.UnsettledResyncPeers
		peers = mergePeers(peers, stalled)
	}
	mutual := mutualSourcePeers(vol, node, st)
	peers = mergePeers(peers, mutual)
	// A diskless leg holds no bitmap, so it can never be the stranded
	// side; whatever it sees is a diskful pair's business to resolve.
	if localDiskless || len(peers) == 0 {
		// A stalled target its gate holds back stays out of the fast
		// path: the gate reads the other legs' status slots, which no
		// kernel event re-breaks the fingerprint for.
		return len(st.UnsettledResyncPeers) > 0, nil, nil
	}
	key := stuckPeerKey(peers, mutual)
	if mark == nil || mark.peers != key {
		return true, nil, &stuckMark{since: now, peers: key, probes: observeProbes(vol, mutual, nil)}
	}
	next = &stuckMark{since: mark.since, peers: key, probes: observeProbes(vol, mutual, mark.probes)}
	if now.Sub(mark.since) < drbdPollInterval {
		return true, nil, next
	}
	for _, peerID := range slices.Sorted(maps.Keys(peers)) {
		if mutual[peerID] {
			probe := next.probes[peerID]
			if probe.advances < mutualSourceProbes || !lowerNodeID(vol, node, peerID) {
				continue
			}
			next.probes[peerID] = slotProbe{stamp: probe.stamp}
		}
		cycle = append(cycle, peerCycle{peerID: peerID, stalledTarget: stalled[peerID]})
	}
	if len(cycle) > 0 {
		next.since = now
	}
	return true, cycle, next
}

// mutualSourcePeers returns the SyncSourcePeers whose own status slot
// reports an UpToDate disk. DRBD holds a resync target's disk below
// UpToDate from the bitmap exchange until the resync ends, so a peer this
// leg syncs toward that reads UpToDate itself is no target: it is the
// other source of a resync with none (issue #675). The slot may predate
// the resync, so this only makes the peer a candidate; the slotProbe count
// decides the cycle.
//
// Both legs of the pair see each other this way, and both must: a
// candidate keeps a leg out of the fast path, whose full passes are what
// publish the slot the other leg counts. A sync source whose peer reports
// anything else is a live resync and parks as before, so a fault that
// begins while the peer's slot lags is picked up by the next deep check.
func mutualSourcePeers(vol *miroirv1alpha1.MiroirVolume, node string, st drbd.Status) map[int32]bool {
	var peers map[int32]bool
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == node || !st.SyncSourcePeers[rep.NodeID] ||
			vol.Status.PerNode[rep.Node].DiskState != drbd.DiskUpToDate {
			continue
		}
		if peers == nil {
			peers = map[int32]bool{}
		}
		peers[rep.NodeID] = true
	}
	return peers
}

// mutualSourceProbes is how many later LastProbedAt stamps this leg must
// see on a mutualSourcePeers candidate's slot, still UpToDate, past the
// stamp it armed on. A pass reads the kernel first and stamps LastProbedAt
// last, so one publication can carry a reading older than the resync; a
// second has to come from a later pass. That narrows the stale case
// without closing it: nothing orders the peer's kernel reads against this
// leg's, so a peer agent that samples just before a real resync starts
// and then stops reporting for the rest of the window can still be
// counted. The cost is a graceful cycle of a healthy resync, which resumes
// from its bitmap. Only the peer's stamps are compared, each to the one
// before, which keeps the two nodes' clocks out of it.
const mutualSourceProbes = 2

// slotProbe counts one candidate's status publications.
type slotProbe struct {
	// stamp is the LastProbedAt last seen, zero when the slot has none.
	stamp time.Time
	// advances counts the later stamps seen since the leg armed. A stamp
	// that moves back is a republished slot (reportError), not a probe.
	advances int
}

// observeProbes carries prev forward over this pass's view of the mutual
// candidates' slots. Nil when there are none.
func observeProbes(vol *miroirv1alpha1.MiroirVolume, mutual map[int32]bool, prev map[int32]slotProbe) map[int32]slotProbe {
	if len(mutual) == 0 {
		return nil
	}
	probes := make(map[int32]slotProbe, len(mutual))
	for _, rep := range vol.Spec.Replicas {
		if !mutual[rep.NodeID] {
			continue
		}
		var stamp time.Time
		if probed := vol.Status.PerNode[rep.Node].LastProbedAt; probed != nil {
			stamp = probed.Time
		}
		probe, seen := prev[rep.NodeID]
		switch {
		case !seen:
			probe = slotProbe{stamp: stamp}
		case stamp.After(probe.stamp):
			probe = slotProbe{stamp: stamp, advances: probe.advances + 1}
		}
		probes[rep.NodeID] = probe
	}
	return probes
}

// lowerNodeID reports whether this leg holds the lower DRBD node id of its
// pair with peerID. Both legs of a mutual-source pair wear the signature,
// and this picks the one that cycles, the same on both without
// coordinating.
func lowerNodeID(vol *miroirv1alpha1.MiroirVolume, node string, peerID int32) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == node {
			return rep.NodeID < peerID
		}
	}
	return false
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
	// probes holds a slotProbe per mutualSourcePeers candidate; nil
	// without one.
	probes map[int32]slotProbe
}

// stuckPeerKey renders a candidate set in a stable form for stuckMark.
// mutual, the candidates among peers that are there as a mutual source, is
// part of the key: a peer that moves from another signature to that one
// must not ride the clock the first armed.
func stuckPeerKey(peers, mutual map[int32]bool) string {
	return fmt.Sprint(slices.Sorted(maps.Keys(peers)), slices.Sorted(maps.Keys(mutual)))
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
func staleBitmapActionable(vol *miroirv1alpha1.MiroirVolume, node string, st drbd.Status) bool {
	if len(st.StaleBitmapPeers) == 0 || st.DiskState != drbd.DiskUpToDate ||
		!st.Quorum || st.Resyncing || st.SplitBrain || (!st.Primary && !st.PeerPrimary) {
		return false
	}
	if !diskfulPeersConnected(st, vol, node) || !diskfulPeersUpToDate(st, vol, node) {
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
func unsettledResyncActionable(vol *miroirv1alpha1.MiroirVolume, node string, st drbd.Status, now time.Time) bool {
	if len(st.UnsettledResyncPeers) == 0 || st.SplitBrain {
		return false
	}
	if st.PeerPrimary {
		return true
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Node != node && rep.Address != "" && !slotRulesOutPrimary(vol, rep.Node, now) {
			return false
		}
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Address != "" && !slotRulesOutPrimary(vol, cl.Node, now) {
			return false
		}
	}
	return true
}

// slotRulesOutPrimary reports whether a leg's status slot is fresh and
// reports it not holding the Primary role.
func slotRulesOutPrimary(vol *miroirv1alpha1.MiroirVolume, node string, now time.Time) bool {
	slot, ok := vol.Status.PerNode[node]
	return ok && slot.PrimarySince == nil && slot.LastProbedAt != nil &&
		now.Sub(slot.LastProbedAt.Time) <= StaleProbeThreshold
}
