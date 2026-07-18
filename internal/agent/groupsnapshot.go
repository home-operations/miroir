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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	acv1alpha1 "github.com/home-operations/miroir/api/v1alpha1/applyconfiguration/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/drbd"
)

// GroupSnapshotReconciler realizes MiroirSnapshotGroups on this node.
//
// A group is ONE write-barrier round spanning every diskful leg of every
// member volume, so the member snapshots are crash-consistent with each
// other: every member volume is frozen before any leg anywhere is cut,
// and no volume resumes until every leg everywhere is cut — there is an
// instant at which all volumes were simultaneously frozen, and each cut
// equals its volume's content at that instant. The state machine is the
// MiroirSnapshot barrier generalized from per-node slots to
// per-(volume, node) slots, kept on the single group object so that
// completing and voiding a round serialize on one resourceVersion
// instead of racing across objects:
//
//	driver: suspend-io (its local member legs) → patch ioSuspended + Suspended
//	peer:   sees ioSuspended → suspend-io (its legs) → Suspended
//	all:    see every slot Suspended → cut local legs → Done
//	driver: sees all Done (or deadline) → resume-io → readyToUse (or void)
//	peer:   sees readyToUse (or barrier cleared) → resume-io
//
// The driver is elected by the first member volume's coordinator rule
// (its Primary when one exists, else the first reachable diskful
// replica). Every participating node freezes and cuts its own legs, so
// the driver's identity matters for liveness only, never for the
// freeze's correctness.
//
// Only replicated (DRBD) volumes can join a group: suspend-io is the
// only write barrier miroir has, and a 1-replica volume without DRBD
// would keep writing while its peers freeze — a cut that silently
// breaks the group's whole promise. The CSI layer refuses such members
// at creation.
type GroupSnapshotReconciler struct {
	client.Client
	NodeName string
	// Pools holds this node's storage pools; each member leg is cut in
	// the pool holding that volume's local replica.
	Pools Pools
	DRBD  *drbd.Driver
	// Reader is the uncached API reader for cross-round checks (see
	// SnapshotReconciler.Reader). Falls back to the cached client when
	// unset (tests).
	Reader client.Reader
	// Recorder emits the BarrierStuck warning; optional.
	Recorder events.EventRecorder

	// barrierFails parks a group whose barrier path fails persistently,
	// mirroring SnapshotReconciler.barrierFails.
	barrierFailsMu sync.Mutex
	barrierFails   map[string]int
}

// groupMember pairs one member snapshot with its source volume.
type groupMember struct {
	snap *miroirv1alpha1.MiroirSnapshot
	vol  *miroirv1alpha1.MiroirVolume
}

// slotKey identifies one diskful leg of one member volume in the group
// round's status: "<volume>/<node>".
func slotKey(volume, node string) string { return volume + "/" + node }

// Reconcile drives one group round from this node's view.
func (r *GroupSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	grp := &miroirv1alpha1.MiroirSnapshotGroup{}
	if err := r.Get(ctx, req.NamespacedName, grp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	members, missing, err := r.members(ctx, grp)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !grp.DeletionTimestamp.IsZero() {
		r.clearBarrierFails(grp.Name)
		// A group deleted mid-round must not strand barriers. Lift every
		// local member barrier the kernel still holds — keyed on kernel
		// state, and never one a sibling round now owns.
		for _, m := range members {
			if m.vol.Spec.DRBD == nil || !diskfulOn(m.vol, r.NodeName) {
				continue
			}
			if st, err := r.drbdStatus(ctx, m.vol.Name); err == nil && st.Suspended {
				if err := r.resumeUnlessOtherRound(ctx, grp, m.vol.Name); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		// The members' own finalizers tear down the backend snapshots;
		// the group holds no node-local state beyond its barriers.
		return ctrl.Result{}, removeNodeFinalizer(ctx, r.Client, grp, r.NodeName)
	}

	if missing {
		// The CSI layer creates members before the group, so a missing
		// member snapshot or source volume here is either creation still
		// settling or a member torn out from under a live group; wait —
		// cutting a partial group would misrepresent what it covers.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	mine := myLegs(members, r.NodeName)
	if len(mine) == 0 {
		return ctrl.Result{}, nil
	}

	if grp.Status.ReadyToUse {
		// A barrier can outlive the round by a moment; lift stray local
		// ones — unless a sibling round owns the kernel flag by now.
		for _, m := range mine {
			if st, err := r.drbdStatus(ctx, m.vol.Name); err == nil && st.Suspended {
				if err := r.resumeUnlessOtherRound(ctx, grp, m.vol.Name); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		return ctrl.Result{}, nil
	}

	for _, m := range mine {
		if m.vol.Spec.DRBD == nil {
			// Defense in depth: the CSI layer refuses unreplicated members
			// (no write barrier — see the type comment). Waiting on one
			// would freeze the rest of the group for nothing.
			return ctrl.Result{}, nil
		}
		st := m.vol.Status.PerNode[r.NodeName]
		switch {
		case st.DiskState == "":
			// The volume agent has not brought DRBD up yet.
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		case st.SplitBrain:
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	return r.reconcileRound(ctx, grp, members, mine)
}

// reconcileRound is the group barrier state machine proper, entered once
// this node verifiably holds diskful legs of a live, fully-resolved
// group.
func (r *GroupSnapshotReconciler) reconcileRound(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, members, mine []groupMember) (ctrl.Result, error) {
	sts, healthy, kernelSuspended, res, err := r.localView(ctx, grp, mine)
	if err != nil || res.RequeueAfter > 0 {
		return res, err
	}

	driver := r.isDriver(members, sts)
	expired := grp.Status.SuspendedAt != nil &&
		time.Since(grp.Status.SuspendedAt.Time) > SuspendDeadline

	switch {
	case driver && !grp.Status.IOSuspended && healthy && !kernelSuspended:
		// The !kernelSuspended guard is the kernel-truth half of the
		// one-round-per-volume rule (see reconcileReplicated's twin).
		return r.openRound(ctx, grp, members, mine)

	case !driver && grp.Status.IOSuspended && !expired && healthy &&
		!myLegsAll(grp, mine, r.NodeName, miroirv1alpha1.SnapshotSuspended):
		return r.raiseBarriers(ctx, grp, members, mine, false)

	case grp.Status.IOSuspended && !expired && healthy &&
		allSlotsSuspended(grp, members) && !myLegsAll(grp, mine, r.NodeName, miroirv1alpha1.SnapshotDone):
		return r.cutLegs(ctx, grp, mine)

	case driver && grp.Status.IOSuspended:
		return r.collectSlots(ctx, grp, members, mine, expired)

	case grp.Status.IOSuspended && expired && kernelSuspended:
		// Dead round, driver gone before voiding it: self-expire the
		// local barriers; the void patch stays the driver's.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.resumeMine(ctx, grp, mine)

	case !grp.Status.IOSuspended && kernelSuspended:
		// The round ended (voided) while local barriers were still up —
		// unless a sibling round owns one of them by now.
		for _, m := range mine {
			if sts[m.vol.Name].Suspended {
				if err := r.resumeUnlessOtherRound(ctx, grp, m.vol.Name); err != nil {
					return ctrl.Result{RequeueAfter: 2 * time.Second}, err
				}
			}
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// localView reads this node's DRBD state for every local member leg:
// per-volume statuses, whether every local volume passes the same
// health gate as the per-snapshot round (a barrier over diverged or
// resyncing legs cuts diverged legs, and a peer that can never raise
// its own barrier only freezes the group until the deadline voids it),
// and whether any local barrier is up in the kernel. A failing status
// read rides the bounded barrier-failure retry.
func (r *GroupSnapshotReconciler) localView(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, mine []groupMember) (sts map[string]drbd.Status, healthy, kernelSuspended bool, res ctrl.Result, err error) {
	sts = map[string]drbd.Status{}
	healthy = true
	for _, m := range mine {
		st, err := r.drbdStatus(ctx, m.vol.Name)
		if err != nil {
			res, err := r.barrierFailed(ctx, grp, m.vol, err)
			return nil, false, false, res, err
		}
		sts[m.vol.Name] = st
		if !diskfulPeersConnected(st, m.vol, r.NodeName) ||
			!diskfulPeersUpToDate(st, m.vol, r.NodeName) ||
			st.DiskState != drbd.DiskUpToDate {
			healthy = false
		}
		if st.Suspended {
			kernelSuspended = true
		}
	}
	return sts, healthy, kernelSuspended, ctrl.Result{}, nil
}

// openRound starts a new group round as driver: a fresh raise, unless
// the previous round's failure is still inside its retry backoff or any
// member volume has a live sibling round (a standalone snapshot's or
// another group's).
func (r *GroupSnapshotReconciler) openRound(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, members, mine []groupMember) (ctrl.Result, error) {
	if myState := grp.Status.PerLeg[slotKey(mine[0].vol.Name, r.NodeName)]; myState == miroirv1alpha1.SnapshotError &&
		grp.Status.SuspendedAt != nil &&
		time.Since(grp.Status.SuspendedAt.Time) < suspendRetryBackoff {
		return ctrl.Result{RequeueAfter: suspendRetryBackoff}, nil
	}
	for _, m := range members {
		if active, err := volumeRoundActive(ctx, r.reader(), m.vol.Name, "", grp.Name); err != nil || active {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, err
		}
	}
	return r.raiseBarriers(ctx, grp, members, mine, true)
}

// raiseBarriers freezes every local member leg and records it. The
// driver's raise opens the round (ioSuspended + deadline clock + a slot
// reset across EVERY member, so a slow node's Done from a voided round
// cannot pair with this round's cuts); a peer records only its own
// slots.
func (r *GroupSnapshotReconciler) raiseBarriers(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, members, mine []groupMember, opensRound bool) (ctrl.Result, error) {
	// A leg whose pool cannot be resolved could never cut behind the
	// barrier: fail before the group-wide freeze, not after.
	for _, m := range mine {
		if _, err := r.backendFor(m.vol); err != nil {
			return ctrl.Result{}, err
		}
	}
	suspended := make([]groupMember, 0, len(mine))
	for _, m := range mine {
		if err := r.suspendIO(ctx, m.vol.Name); err != nil {
			// A half-raised node must not stay half-frozen behind a failed
			// raise; the retry re-raises the lot.
			_ = r.resumeMine(ctx, grp, suspended)
			return r.barrierFailed(ctx, grp, m.vol, err)
		}
		suspended = append(suspended, m)
	}
	r.clearBarrierFails(grp.Name)
	var err error
	if opensRound {
		now := metav1.Now()
		err = r.patchGroup(ctx, grp, func(g *miroirv1alpha1.MiroirSnapshotGroup) {
			if g.Status.PerLeg == nil {
				g.Status.PerLeg = map[string]miroirv1alpha1.SnapshotNodeState{}
			}
			g.Status.IOSuspended = true
			g.Status.SuspendedAt = &now
			for _, m := range members {
				for _, rep := range m.vol.Spec.DiskfulReplicas() {
					g.Status.PerLeg[slotKey(m.vol.Name, rep.Node)] = miroirv1alpha1.SnapshotPending
				}
			}
			for _, m := range mine {
				g.Status.PerLeg[slotKey(m.vol.Name, r.NodeName)] = miroirv1alpha1.SnapshotSuspended
			}
		})
	} else {
		err = r.patchMySlots(ctx, grp, mine, miroirv1alpha1.SnapshotSuspended)
	}
	if err != nil {
		// The barrier is only real once recorded; a failed patch must not
		// leave IO frozen until the retry.
		_ = r.resumeMine(ctx, grp, mine)
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// cutLegs is phase two, entered only once EVERY slot of EVERY member is
// Suspended: no member volume can accept writes anywhere, so legs cut
// now are mutually consistent regardless of order. Errors do not resume
// the local barriers (peers keep cutting behind theirs); the deadline
// bounds the freeze instead.
func (r *GroupSnapshotReconciler) cutLegs(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, mine []groupMember) (ctrl.Result, error) {
	for _, m := range mine {
		be, err := r.backendFor(m.vol)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Re-assert the barrier first (idempotent) — a crash or manual
		// resume-io between phases must not let a leg be cut unprotected.
		if err := r.suspendIO(ctx, m.vol.Name); err != nil {
			return r.barrierFailed(ctx, grp, m.vol, err)
		}
		if err := be.Sync(ctx, m.vol.Name); err != nil {
			return ctrl.Result{}, err
		}
		// Delete-then-cut: an existing backend snapshot is a leftover
		// from a failed round and would silently survive as a stale leg.
		if err := be.DeleteSnapshot(ctx, m.vol.Name, m.snap.Name); err != nil {
			return ctrl.Result{}, err
		}
		if err := be.Snapshot(ctx, m.vol.Name, m.snap.Name); err != nil {
			return ctrl.Result{}, err
		}
	}
	r.clearBarrierFails(grp.Name)
	return ctrl.Result{RequeueAfter: time.Second}, r.patchMySlots(ctx, grp, mine, miroirv1alpha1.SnapshotDone)
}

// collectSlots is the driver's last phase: every slot Done → resume,
// publish the members, and seal; deadline passed → resume and void the
// round (Done slots included: they were cut under a barrier that
// failed, and the retry recuts them).
func (r *GroupSnapshotReconciler) collectSlots(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, members, mine []groupMember, expired bool) (ctrl.Result, error) {
	done, total := 0, 0
	for _, m := range members {
		for _, rep := range m.vol.Spec.DiskfulReplicas() {
			total++
			if grp.Status.PerLeg[slotKey(m.vol.Name, rep.Node)] == miroirv1alpha1.SnapshotDone {
				done++
			}
		}
	}
	if done < total && !expired {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	// Resume before reporting anything else: a frozen volume is an
	// outage, a late group snapshot is just not ready. A barrier a
	// sibling round co-holds stays for the sibling (see resumeMine).
	if err := r.resumeMine(ctx, grp, mine); err != nil {
		return ctrl.Result{}, err
	}
	if done == total {
		// Publish the members before sealing the group: a crash in
		// between re-enters this branch (ioSuspended still true, all
		// slots Done) and re-publishes idempotently — the group is never
		// ready while a member is not.
		if err := r.publishMembers(ctx, members); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, r.patchGroup(ctx, grp, func(g *miroirv1alpha1.MiroirSnapshotGroup) {
		g.Status.IOSuspended = false
		if done == total {
			g.Status.ReadyToUse = true
			return
		}
		for _, m := range members {
			for _, rep := range m.vol.Spec.DiskfulReplicas() {
				g.Status.PerLeg[slotKey(m.vol.Name, rep.Node)] = miroirv1alpha1.SnapshotPending
			}
		}
		for _, m := range mine {
			g.Status.PerLeg[slotKey(m.vol.Name, r.NodeName)] = miroirv1alpha1.SnapshotError
		}
		// Restamp so the retry backoff counts from this failure.
		now := metav1.Now()
		g.Status.SuspendedAt = &now
	})
}

// publishMembers writes each member snapshot's status once the group's
// cut is complete: ready, sized, and with every diskful leg Done so a
// member restore follows the same path as a standalone snapshot's. The
// driver is the only writer (grouped members never run the per-snapshot
// round), so the full-status apply cannot fight another owner.
func (r *GroupSnapshotReconciler) publishMembers(ctx context.Context, members []groupMember) error {
	for _, m := range members {
		st := miroirv1alpha1.MiroirSnapshotStatus{
			ReadyToUse:      true,
			SizeBytes:       m.vol.Spec.SizeBytes,
			SourceFormatted: m.vol.Status.Formatted,
			PerNode:         map[string]miroirv1alpha1.SnapshotNodeState{},
		}
		for _, rep := range m.vol.Spec.DiskfulReplicas() {
			st.PerNode[rep.Node] = miroirv1alpha1.SnapshotDone
		}
		if err := r.SubResource("status").Apply(ctx,
			acv1alpha1.MiroirSnapshot(m.snap.Name).WithStatus(snapshotStatusAC(st)),
			client.FieldOwner("agent-group-"+r.NodeName),
			client.ForceOwnership); err != nil {
			return err
		}
	}
	return nil
}

// members resolves the group's member snapshots and their source
// volumes. missing is true while any member or volume cannot be
// resolved (creation settling, or a member torn out from under the
// group).
func (r *GroupSnapshotReconciler) members(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup) (members []groupMember, missing bool, err error) {
	for _, name := range grp.Spec.SnapshotNames {
		snap := &miroirv1alpha1.MiroirSnapshot{}
		if err := r.Get(ctx, types.NamespacedName{Name: name}, snap); err != nil {
			if apierrors.IsNotFound(err) {
				missing = true
				continue
			}
			return nil, false, err
		}
		vol := &miroirv1alpha1.MiroirVolume{}
		if err := r.Get(ctx, types.NamespacedName{Name: snap.Spec.VolumeName}, vol); err != nil {
			if apierrors.IsNotFound(err) {
				missing = true
				continue
			}
			return nil, false, err
		}
		members = append(members, groupMember{snap: snap, vol: vol})
	}
	return members, missing, nil
}

// myLegs filters the members whose volume has a diskful leg on this node.
func myLegs(members []groupMember, node string) []groupMember {
	var mine []groupMember
	for _, m := range members {
		if diskfulOn(m.vol, node) {
			mine = append(mine, m)
		}
	}
	return mine
}

// diskfulOn reports whether the volume has a diskful replica on node.
func diskfulOn(vol *miroirv1alpha1.MiroirVolume, node string) bool {
	for _, rep := range vol.Spec.DiskfulReplicas() {
		if rep.Node == node {
			return true
		}
	}
	return false
}

// isDriver elects this node by the first member volume's coordinator
// rule. A node without a diskful leg of that volume never drives — it
// cannot even observe that volume's DRBD state — but still raises and
// cuts its own legs as a peer.
func (r *GroupSnapshotReconciler) isDriver(members []groupMember, sts map[string]drbd.Status) bool {
	vol0 := members[0].vol
	st, ok := sts[vol0.Name]
	if !ok {
		return false
	}
	return coordinatorFor(r.NodeName, vol0, st)
}

// myLegsAll reports whether every one of this node's slots has reached
// the given state; Done also satisfies Suspended.
func myLegsAll(grp *miroirv1alpha1.MiroirSnapshotGroup, mine []groupMember, node string, want miroirv1alpha1.SnapshotNodeState) bool {
	for _, m := range mine {
		st := grp.Status.PerLeg[slotKey(m.vol.Name, node)]
		if st == want || (want == miroirv1alpha1.SnapshotSuspended && st == miroirv1alpha1.SnapshotDone) {
			continue
		}
		return false
	}
	return true
}

// allSlotsSuspended reports whether every diskful leg of every member
// volume has raised its barrier (Done implies it did).
func allSlotsSuspended(grp *miroirv1alpha1.MiroirSnapshotGroup, members []groupMember) bool {
	for _, m := range members {
		for _, rep := range m.vol.Spec.DiskfulReplicas() {
			st := grp.Status.PerLeg[slotKey(m.vol.Name, rep.Node)]
			if st != miroirv1alpha1.SnapshotSuspended && st != miroirv1alpha1.SnapshotDone {
				return false
			}
		}
	}
	return true
}

// resumeMine lifts this node's barriers on the given members' volumes —
// except one a sibling round co-holds. Two opens over a shared volume
// can race past each other's guards (a round is invisible between its
// suspend-io and its status patch landing); both then freeze the volume
// through the one kernel flag, and the first round to close must leave
// the flag for the sibling to lift at its own close, or the sibling's
// remaining legs are cut over live writes. The ReadyToUse and
// voided-round branches re-check and lift once nothing holds it.
func (r *GroupSnapshotReconciler) resumeMine(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, mine []groupMember) error {
	for _, m := range mine {
		if err := r.resumeUnlessOtherRound(ctx, grp, m.vol.Name); err != nil {
			return err
		}
	}
	return nil
}

// resumeUnlessOtherRound lifts the local barrier on volume unless a
// sibling round — a standalone snapshot's or another group's — owns it.
func (r *GroupSnapshotReconciler) resumeUnlessOtherRound(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, volume string) error {
	active, err := volumeRoundActive(ctx, r.reader(), volume, "", grp.Name)
	if err != nil || active {
		return err
	}
	return r.resumeIO(ctx, volume)
}

// backendFor resolves the backend holding the volume's local leg.
func (r *GroupSnapshotReconciler) backendFor(vol *miroirv1alpha1.MiroirVolume) (backend.Backend, error) {
	pb, err := r.Pools.Get(volumePoolOn(vol, r.NodeName))
	if err != nil {
		return nil, err
	}
	return pb.Backend, nil
}

// drbdStatus, suspendIO, and resumeIO mirror SnapshotReconciler's
// bounded DRBD control calls.
func (r *GroupSnapshotReconciler) drbdStatus(ctx context.Context, name string) (drbd.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, drbdBarrierTimeout)
	defer cancel()
	return r.DRBD.Status(ctx, name)
}

func (r *GroupSnapshotReconciler) suspendIO(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, drbdBarrierTimeout)
	defer cancel()
	return r.DRBD.SuspendIO(ctx, name)
}

func (r *GroupSnapshotReconciler) resumeIO(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, drbdBarrierTimeout)
	defer cancel()
	return r.DRBD.ResumeIO(ctx, name)
}

// barrierFailed mirrors SnapshotReconciler.barrierFailed for the group
// round: fast backoff below barrierFailLimit, then a Warning Event and
// the parked retry.
func (r *GroupSnapshotReconciler) barrierFailed(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, vol *miroirv1alpha1.MiroirVolume, cause error) (ctrl.Result, error) {
	r.barrierFailsMu.Lock()
	if r.barrierFails == nil {
		r.barrierFails = map[string]int{}
	}
	r.barrierFails[grp.Name]++
	fails := r.barrierFails[grp.Name]
	r.barrierFailsMu.Unlock()
	if fails < barrierFailLimit {
		return ctrl.Result{}, cause
	}
	ctrl.LoggerFrom(ctx).Error(cause, "cannot drive the group snapshot barrier; parking the retry",
		"group", grp.Name, "volume", vol.Name, "attempts", fails)
	if r.Recorder != nil {
		r.Recorder.Eventf(grp, nil, corev1.EventTypeWarning, "BarrierStuck", "Suspend",
			"cannot drive the group snapshot barrier on %s after %d attempts: %v", vol.Name, fails, cause)
	}
	return ctrl.Result{RequeueAfter: barrierRetryAfter}, nil
}

func (r *GroupSnapshotReconciler) clearBarrierFails(name string) {
	r.barrierFailsMu.Lock()
	delete(r.barrierFails, name)
	r.barrierFailsMu.Unlock()
}

// patchMySlots records only this node's slots via a merge patch; a peer
// must not apply the whole status (see patchOwnState's twin).
func (r *GroupSnapshotReconciler) patchMySlots(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, mine []groupMember, state miroirv1alpha1.SnapshotNodeState) error {
	slots := make([]string, 0, len(mine))
	for _, m := range mine {
		slots = append(slots, fmt.Sprintf("%q:%q", slotKey(m.vol.Name, r.NodeName), state))
	}
	patch := fmt.Appendf(nil, `{"status":{"perLeg":{%s}}}`, strings.Join(slots, ","))
	return r.Status().Patch(ctx, grp, client.RawPatch(types.MergePatchType, patch))
}

func (r *GroupSnapshotReconciler) patchGroup(ctx context.Context, grp *miroirv1alpha1.MiroirSnapshotGroup, mutate func(*miroirv1alpha1.MiroirSnapshotGroup)) error {
	mutate(grp)
	ac := acv1alpha1.MiroirSnapshotGroupStatus().
		WithReadyToUse(grp.Status.ReadyToUse).
		WithIOSuspended(grp.Status.IOSuspended)
	if len(grp.Status.PerLeg) > 0 {
		ac = ac.WithPerLeg(grp.Status.PerLeg)
	}
	if grp.Status.SuspendedAt != nil {
		ac = ac.WithSuspendedAt(*grp.Status.SuspendedAt)
	}
	return r.SubResource("status").Apply(ctx,
		acv1alpha1.MiroirSnapshotGroup(grp.Name).WithStatus(ac),
		client.FieldOwner("agent-group-"+r.NodeName),
		client.ForceOwnership)
}

// reader returns the uncached API reader, falling back to the cached
// client when unset (tests).
func (r *GroupSnapshotReconciler) reader() client.Reader {
	if r.Reader != nil {
		return r.Reader
	}
	return r.Client
}

// SetupWithManager registers the reconciler.
func (r *GroupSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirSnapshotGroup{}).
		Named("agent-group-snapshot").
		Complete(r)
}
