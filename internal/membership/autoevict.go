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

package membership

import (
	"context"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
	"github.com/home-operations/miroir/internal/nodemap"
)

// AutoEvictReconciler re-places the legs of a permanently dead storage
// node (LINSTOR's auto-evict): once a node's MiroirNode heartbeat
// (Status.ObservedAt, refreshed ~60s by its pool-stats publisher) has
// been stale for After, each volume with a leg there gets one atomic
// spec update — dead entry out, replacement in.
//
// The dead node's teardown finalizer is deliberately left in place: it
// is the durable record that the leg was never torn down there. When
// the node returns, its agent runs the ordinary removal flow
// (reconcileRemoval) against it — safety-gated, wiping metadata and
// reclaiming the backing device — exactly as if an operator had removed
// the entry. Until then the finalizer also keeps the volume deletable
// only once that cleanup can actually happen, and keeps the tie-breaker
// retrofit from treating the dead node as a free spare.
//
// Deliberately conservative: it evicts only when the remaining legs are
// clean, stands down entirely when more than one node looks dead (an
// observer-side problem is more likely than a multi-node failure), and
// stands down when any survivor still sees the "dead" node's DRBD links
// up (a node partitioned from the API server but not from its peers is
// alive; kicking it would discard a healthy replica).
type AutoEvictReconciler struct {
	client.Client
	// Nodes is the storage topology — placement candidates and the
	// per-node opt-out come from it.
	Nodes nodemap.Map
	// After is the heartbeat staleness that declares a node dead; the
	// setup path guards > 0.
	After time.Duration
	// Recorder emits per-volume AutoEvict events; optional.
	Recorder events.EventRecorder
}

// evictRecheckInterval paces re-examination of a dead node whose volumes
// could not all be evicted (gates blocked, no spare capacity, safety
// valve engaged). Nothing event-driven re-triggers those cases: the
// blockers live in volume status, snapshots, or other nodes' heartbeats.
const evictRecheckInterval = 5 * time.Minute

// evictPass is the cluster state gathered once per reconcile pass and
// shared by every per-volume decision — listing snapshots or re-reading
// node stats per volume would multiply two collection sizes for answers
// that cannot change mid-pass.
type evictPass struct {
	// pinned holds the names of volumes referenced by any snapshot.
	pinned map[string]bool
	// nodes indexes the listed MiroirNodes by name.
	nodes map[string]*miroirv1alpha1.MiroirNode
}

// Reconcile checks one node's heartbeat and, once it has been stale for
// After, evicts that node's legs volume by volume.
func (r *AutoEvictReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	if !r.Nodes.AutoEvictAllowed(req.Name) {
		// Not a storage node, or opted out via the node map.
		return ctrl.Result{}, nil
	}
	mn := &miroirv1alpha1.MiroirNode{}
	if err := r.Get(ctx, req.NamespacedName, mn); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if mn.Status.ObservedAt == nil {
		// Never heartbeated: there is no "was alive, went dark" signal to
		// act on (a node added to the map but not yet provisioned).
		return ctrl.Result{}, nil
	}
	if remaining := r.After - time.Since(mn.Status.ObservedAt.Time); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	pass, stale, err := r.gather(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(stale) > 1 {
		// Safety valve: several nodes going dark together points at the
		// observer side (API server, network, this controller), not at
		// that many simultaneous hardware deaths. Do nothing.
		metricEvictStanddown.WithLabelValues("multiple_stale").Inc()
		log.Info("auto-evict standing down: multiple stale heartbeats", "nodes", stale)
		return ctrl.Result{RequeueAfter: evictRecheckInterval}, nil
	}

	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := r.List(ctx, vols); err != nil {
		return ctrl.Result{}, err
	}
	blocked := false
	for i := range vols.Items {
		vol := &vols.Items[i]
		if !hasLegOn(vol, req.Name) {
			continue
		}
		outcome, err := r.evictVolume(ctx, vol, req.Name, pass)
		if err != nil {
			return ctrl.Result{}, err
		}
		switch outcome {
		case evictedOutcome:
		case peerConnectedOutcome:
			// A survivor still holds an established DRBD link to the
			// "dead" node: it is alive and only its API-server path is
			// broken. Evicting a live replica discards good data — stand
			// down for the whole node, not just this volume.
			metricEvictStanddown.WithLabelValues("peer_connected").Inc()
			log.Info("auto-evict standing down: a survivor reports the node's DRBD links up",
				"node", req.Name, "volume", vol.Name)
			return ctrl.Result{RequeueAfter: evictRecheckInterval}, nil
		default:
			blocked = true
		}
	}
	if blocked {
		return ctrl.Result{RequeueAfter: evictRecheckInterval}, nil
	}
	return ctrl.Result{}, nil
}

// gather lists the cluster state one eviction pass needs — MiroirNodes
// (the safety-valve census and the capacity views) and snapshots (the
// pin set) — exactly once.
func (r *AutoEvictReconciler) gather(ctx context.Context) (*evictPass, []string, error) {
	nodes := &miroirv1alpha1.MiroirNodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return nil, nil, err
	}
	pass := &evictPass{
		pinned: map[string]bool{},
		nodes:  make(map[string]*miroirv1alpha1.MiroirNode, len(nodes.Items)),
	}
	var stale []string
	for i := range nodes.Items {
		n := &nodes.Items[i]
		pass.nodes[n.Name] = n
		if _, ok := r.Nodes[n.Name]; !ok {
			continue
		}
		if n.Status.ObservedAt != nil && time.Since(n.Status.ObservedAt.Time) >= r.After {
			stale = append(stale, n.Name)
		}
	}
	slices.Sort(stale)
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := r.List(ctx, snaps); err != nil {
		return nil, nil, err
	}
	for i := range snaps.Items {
		pass.pinned[snaps.Items[i].Spec.VolumeName] = true
	}
	return pass, stale, nil
}

// evictOutcome classifies one volume's eviction attempt.
type evictOutcome int

const (
	// evictedOutcome: the swap was applied.
	evictedOutcome evictOutcome = iota
	// blockedOutcome: a gate refused for now; retry on the recheck tick.
	blockedOutcome
	// peerConnectedOutcome: a survivor sees the dead node's DRBD links
	// established — the node is alive, abort the whole pass.
	peerConnectedOutcome
)

// evictVolume applies one volume's eviction: gate checks, replacement
// choice, then the atomic spec swap. The dead node's finalizer stays.
func (r *AutoEvictReconciler) evictVolume(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dead string, pass *evictPass) (evictOutcome, error) {
	log := ctrl.LoggerFrom(ctx)

	// -1 when the dead leg is a client leg, not a replica.
	deadIdx := slices.IndexFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == dead
	})

	reason, outcome := evictBlocked(vol, dead, deadIdx, pass)
	if reason != "" {
		if outcome == blockedOutcome {
			log.Info("auto-evict blocked", "volume", vol.Name, "node", dead, "reason", reason)
		}
		return outcome, nil
	}

	kind, apply := r.plan(vol, dead, deadIdx, pass)
	if apply == nil {
		log.Info("auto-evict blocked", "volume", vol.Name, "node", dead,
			"reason", "no spare node qualifies for the replacement leg")
		return blockedOutcome, nil
	}

	apply(vol)
	if err := r.Update(ctx, vol); err != nil {
		if apierrors.IsConflict(err) {
			return blockedOutcome, nil
		}
		return blockedOutcome, err
	}
	metricEvictions.WithLabelValues(kind).Inc()
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "AutoEvict", "Evict",
			"node %s has been unreachable past the eviction threshold; re-placed its %s leg (its teardown finalizer stays until the node returns and cleans up)", dead, kind)
	}
	log.Info("auto-evict: re-placed a dead node's leg",
		"volume", vol.Name, "node", dead, "kind", kind)
	return evictedOutcome, nil
}

// evictBlocked reports why the volume must not be evicted right now (""
// when it may proceed) and the outcome class for a non-empty reason.
func evictBlocked(vol *miroirv1alpha1.MiroirVolume, dead string, deadIdx int, pass *evictPass) (string, evictOutcome) {
	if !vol.DeletionTimestamp.IsZero() {
		// Deletion has its own teardown flow; re-placing legs under it
		// only adds sync work the deletion immediately undoes.
		return "volume is being deleted", blockedOutcome
	}
	if vol.Spec.DRBD == nil {
		// The dead node holds the only copy; re-placing is data loss.
		return "volume is unreplicated", blockedOutcome
	}
	if incompleteChange(vol) {
		return "a membership change is already in flight", blockedOutcome
	}
	deadDiskful := deadIdx >= 0 && !vol.Spec.Replicas[deadIdx].Diskless
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == dead || rep.Diskless {
			continue
		}
		st, ok := vol.Status.PerNode[rep.Node]
		if !ok || st.DiskState != drbd.DiskUpToDate || st.SplitBrain {
			// Losing the dead leg must not cost data: every survivor has
			// to hold a full clean copy first. (Connected is not required
			// here — with a diskful peer down it reads false on every
			// survivor by definition.)
			return "replica on " + rep.Node + " is not UpToDate", blockedOutcome
		}
		if deadDiskful && st.Connected {
			// Connected means links to *every* diskful peer, including
			// the dead one: proof of life.
			return "survivor " + rep.Node + " still sees the node's DRBD links up", peerConnectedOutcome
		}
	}
	if deadDiskful && pass.pinned[vol.Name] {
		// Snapshots are per-replica CoW state; a replacement leg would
		// not carry them, leaving restores dependent on which leg serves
		// them.
		return "snapshots pin the volume's replicas", blockedOutcome
	}
	return "", evictedOutcome
}

// incompleteChange reports whether a membership edit is mid-completion:
// any replica or client leg still lacking its address. One spec edit at
// a time — acting on a half-completed spec races the membership
// reconciler's completion update.
func incompleteChange(vol *miroirv1alpha1.MiroirVolume) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			return true
		}
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Address == "" {
			return true
		}
	}
	return false
}

// plan picks the replacement and returns the leg kind plus a mutation
// applying the swap, or a nil mutation when no replacement exists.
func (r *AutoEvictReconciler) plan(vol *miroirv1alpha1.MiroirVolume, dead string, deadIdx int, pass *evictPass) (string, func(*miroirv1alpha1.MiroirVolume)) {
	if deadIdx < 0 {
		// A dead consumer needs no replacement: the pod is gone; drop the leg.
		return kindClient, func(v *miroirv1alpha1.MiroirVolume) {
			v.Spec.Clients = slices.DeleteFunc(v.Spec.Clients, func(c miroirv1alpha1.VolumeClient) bool {
				return c.Node == dead
			})
		}
	}
	if vol.Spec.Replicas[deadIdx].Diskless {
		// TieBreakerNode sees the dead entry as used, so it can never hand
		// the dead node back. Client-leg nodes count as used too: a
		// replica may not share a node with a client leg (CEL rule).
		legs := slices.Clone(vol.Spec.Replicas)
		for _, cl := range vol.Spec.Clients {
			legs = append(legs, miroirv1alpha1.Replica{Node: cl.Node})
		}
		tb := r.Nodes.TieBreakerNode(legs)
		if tb == "" {
			return kindTieBreaker, nil
		}
		return kindTieBreaker, func(v *miroirv1alpha1.MiroirVolume) {
			swapReplica(v, dead, miroirv1alpha1.Replica{Node: tb, Diskless: true})
		}
	}
	poolName := volumePool(vol)
	fits := func(node string) bool {
		if _, ok := r.Nodes.Pool(node, poolName); !ok {
			return false
		}
		return poolRoom(pass.nodes[node], poolName, vol.Spec.SizeBytes) == ""
	}
	if repl := replacementNode(r.Nodes, vol, dead, fits); repl != "" {
		return kindReplica, func(v *miroirv1alpha1.MiroirVolume) {
			swapReplica(v, dead, miroirv1alpha1.Replica{Node: repl})
		}
	}
	// No spare node: flipping a live tie-breaker diskful (auto-diskful's
	// toggle-disk) restores 2 data copies at the cost of the quorum leg —
	// strictly better than staying at 1 clean copy.
	for i, rep := range vol.Spec.Replicas {
		if !rep.Diskless || rep.Node == dead || !fits(rep.Node) {
			continue
		}
		pool, ok := r.Nodes.Pool(rep.Node, poolName)
		if !ok {
			continue
		}
		return kindReplica, func(v *miroirv1alpha1.MiroirVolume) {
			convertTieBreaker(v, i, pool.Backend, poolName)
			v.Spec.Replicas = slices.DeleteFunc(v.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
				return rep.Node == dead
			})
		}
	}
	return kindReplica, nil
}

// swapReplica drops the dead node's entry and inserts the bare
// replacement, diskful entries first so the CEL first-replica-diskful
// rule holds no matter which entry died. The membership reconciler
// completes the new entry (address, node id, FullSync, finalizer).
func swapReplica(vol *miroirv1alpha1.MiroirVolume, dead string, replacement miroirv1alpha1.Replica) {
	out := make([]miroirv1alpha1.Replica, 0, len(vol.Spec.Replicas))
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == dead || rep.Diskless {
			continue
		}
		out = append(out, rep)
	}
	if !replacement.Diskless {
		out = append(out, replacement)
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == dead || !rep.Diskless {
			continue
		}
		out = append(out, rep)
	}
	if replacement.Diskless {
		out = append(out, replacement)
	}
	vol.Spec.Replicas = out
}

// replacementNode picks the node for a re-placed diskful leg: one
// passing fits (pool present, fresh stats, room), holding no leg of the
// volume; zones not already covered by the surviving legs win, ties by
// name. Empty when no node qualifies. The zone preference and tie-break
// are nodemap.PickSpare — the same policy tie-breaker placement uses.
func replacementNode(nodes nodemap.Map, vol *miroirv1alpha1.MiroirVolume, dead string, fits func(string) bool) string {
	used := make(map[string]bool, len(vol.Spec.Replicas)+len(vol.Spec.Clients))
	usedZone := map[string]bool{}
	for _, rep := range vol.Spec.Replicas {
		used[rep.Node] = true
		if rep.Node == dead {
			// The dead node's zone is genuinely free again.
			continue
		}
		if z := nodes[rep.Node].Zone; z != "" {
			usedZone[z] = true
		}
	}
	for _, cl := range vol.Spec.Clients {
		used[cl.Node] = true
	}
	return nodes.PickSpare(used, usedZone, fits)
}

// poolRoom reports why the node's pool cannot take a full-size sync (""
// when it can): stats missing or stale, pool absent, or not enough free
// space. The one bar auto-diskful and auto-evict share — a full sync
// must never land blind.
func poolRoom(mn *miroirv1alpha1.MiroirNode, pool string, sizeBytes int64) string {
	if mn == nil {
		return "no pool stats"
	}
	if mn.Status.ObservedAt == nil || time.Since(mn.Status.ObservedAt.Time) > constants.StatsStaleAfter {
		return "pool stats are stale"
	}
	st := mn.Status.Pool(pool)
	if st == nil {
		return "no stats for pool " + pool
	}
	if st.CapacityBytes-st.AllocatedBytes < sizeBytes {
		return "insufficient free space in pool " + pool
	}
	return ""
}

// hasLegOn reports whether any replica or client leg of the volume lives
// on the node.
func hasLegOn(vol *miroirv1alpha1.MiroirVolume, node string) bool {
	return slices.ContainsFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == node
	}) || vol.Spec.ClientForNode(node) != nil
}

// SetupWithManager registers the reconciler on MiroirNode events. No
// predicate: heartbeats are status patches (generation never changes),
// and with a handful of nodes at one patch per minute the event volume
// is negligible.
func (r *AutoEvictReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirNode{}).
		Named("autoevict").
		Complete(r)
}
