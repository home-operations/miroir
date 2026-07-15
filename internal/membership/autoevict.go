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
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
	"github.com/home-operations/miroir/internal/nodemap"
)

// AutoEvictReconciler re-places the legs of a permanently dead storage
// node (LINSTOR's auto-evict): once a node's MiroirNode heartbeat
// (Status.ObservedAt, refreshed ~60s by its pool-stats publisher) has
// been stale for After, each volume with a leg there gets one atomic
// spec update — dead entry out, replacement in — and the dead node's
// teardown finalizer is force-released (its agent cannot run). A status
// marker records the force-release so the node, if it ever returns,
// scrubs its leftover local state instead of leaking it.
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

	stale, err := r.staleNodes(ctx)
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
	remaining := false
	for i := range vols.Items {
		vol := &vols.Items[i]
		if !hasLegOn(vol, req.Name) {
			continue
		}
		outcome, err := r.evictVolume(ctx, vol, req.Name)
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
			remaining = true
		}
	}
	if remaining {
		return ctrl.Result{RequeueAfter: evictRecheckInterval}, nil
	}
	return ctrl.Result{}, nil
}

// staleNodes lists map nodes whose heartbeat is older than After —
// the safety-valve census.
func (r *AutoEvictReconciler) staleNodes(ctx context.Context) ([]string, error) {
	nodes := &miroirv1alpha1.MiroirNodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return nil, err
	}
	var stale []string
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if _, ok := r.Nodes[n.Name]; !ok {
			continue
		}
		if n.Status.ObservedAt != nil && time.Since(n.Status.ObservedAt.Time) >= r.After {
			stale = append(stale, n.Name)
		}
	}
	slices.Sort(stale)
	return stale, nil
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
// choice, the eviction marker, then the atomic spec swap plus finalizer
// force-release.
func (r *AutoEvictReconciler) evictVolume(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dead string) (evictOutcome, error) {
	log := ctrl.LoggerFrom(ctx)

	reason, outcome := r.evictBlocked(ctx, vol, dead)
	if reason != "" {
		if outcome == blockedOutcome {
			log.Info("auto-evict blocked", "volume", vol.Name, "node", dead, "reason", reason)
		}
		return outcome, nil
	}

	kind, apply := r.plan(ctx, vol, dead)
	if apply == nil {
		log.Info("auto-evict blocked", "volume", vol.Name, "node", dead,
			"reason", "no spare node qualifies for the replacement leg")
		return blockedOutcome, nil
	}

	// Stamp the marker before the swap: if the node ever returns, the
	// marker is its agent's only permission to scrub the abandoned leg
	// (reconcileRemoval skips nodes holding no finalizer). A marker
	// without a completed swap is harmless — the agent ignores it while
	// the node is still placed, and a later pass re-stamps.
	if err := r.stampEvicted(ctx, vol, dead); err != nil {
		return blockedOutcome, err
	}
	apply(vol)
	controllerutil.RemoveFinalizer(vol, constants.FinalizerPrefix+dead)
	if err := r.Update(ctx, vol); err != nil {
		if apierrors.IsConflict(err) {
			return blockedOutcome, nil
		}
		return blockedOutcome, err
	}
	metricEvictions.WithLabelValues(kind).Inc()
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "AutoEvict", "Evict",
			"node %s has been unreachable past the eviction threshold; re-placed its %s leg and force-released its teardown finalizer", dead, kind)
	}
	log.Info("auto-evict: re-placed a dead node's leg",
		"volume", vol.Name, "node", dead, "kind", kind)
	return evictedOutcome, nil
}

// evictBlocked reports why the volume must not be evicted right now (""
// when it may proceed) and the outcome class for a non-empty reason.
func (r *AutoEvictReconciler) evictBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dead string) (string, evictOutcome) {
	if !vol.DeletionTimestamp.IsZero() {
		// Deletion has its own teardown flow; force-releasing here would
		// let the object vanish before the returning node could scrub.
		return "volume is being deleted", blockedOutcome
	}
	if vol.Spec.DRBD == nil {
		// The dead node holds the only copy; re-placing is data loss.
		return "volume is unreplicated", blockedOutcome
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Address == "" {
			return "a replica change is already in flight", blockedOutcome
		}
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Address == "" {
			return "a client-leg change is already in flight", blockedOutcome
		}
	}
	deadIdx := slices.IndexFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == dead
	})
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
	if deadDiskful {
		snaps := &miroirv1alpha1.MiroirSnapshotList{}
		if err := r.List(ctx, snaps); err != nil {
			return "cannot list snapshots: " + err.Error(), blockedOutcome
		}
		for _, s := range snaps.Items {
			if s.Spec.VolumeName == vol.Name {
				// Snapshots are per-replica CoW state; a replacement leg
				// would not carry them, leaving restores dependent on
				// which leg serves them.
				return "snapshot " + s.Name + " pins the volume's replicas", blockedOutcome
			}
		}
	}
	return "", evictedOutcome
}

// plan picks the replacement and returns the leg kind plus a mutation
// applying the swap, or a nil mutation when no replacement exists.
func (r *AutoEvictReconciler) plan(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dead string) (string, func(*miroirv1alpha1.MiroirVolume)) {
	if cl := vol.Spec.ClientForNode(dead); cl != nil {
		// A dead consumer needs no replacement: the pod is gone; drop the leg.
		return kindClient, func(v *miroirv1alpha1.MiroirVolume) {
			v.Spec.Clients = slices.DeleteFunc(v.Spec.Clients, func(c miroirv1alpha1.VolumeClient) bool {
				return c.Node == dead
			})
		}
	}
	deadIdx := slices.IndexFunc(vol.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == dead
	})
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
	if repl := r.replacementNode(ctx, vol, dead); repl != "" {
		return kindReplica, func(v *miroirv1alpha1.MiroirVolume) {
			swapReplica(v, dead, miroirv1alpha1.Replica{Node: repl})
		}
	}
	// No spare node: flipping a live tie-breaker diskful (auto-diskful's
	// toggle-disk) restores 2 data copies at the cost of the quorum leg —
	// strictly better than staying at 1 clean copy.
	poolName := volumePool(vol)
	for i, rep := range vol.Spec.Replicas {
		if !rep.Diskless || rep.Node == dead {
			continue
		}
		if !r.nodeFits(ctx, rep.Node, poolName, vol.Spec.SizeBytes) {
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

// replacementNode picks the node for a re-placed diskful leg: carries
// the volume's pool, holds no leg of the volume, has fresh stats with
// room for the full virtual size; zones not already covered by the
// surviving legs win, ties by name. Empty when no node qualifies.
func (r *AutoEvictReconciler) replacementNode(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dead string) string {
	pool := volumePool(vol)
	used := make(map[string]bool, len(vol.Spec.Replicas)+len(vol.Spec.Clients))
	usedZone := map[string]bool{}
	for _, rep := range vol.Spec.Replicas {
		used[rep.Node] = true
		if rep.Node == dead {
			continue
		}
		if z := r.Nodes[rep.Node].Zone; z != "" {
			usedZone[z] = true
		}
	}
	for _, cl := range vol.Spec.Clients {
		used[cl.Node] = true
	}
	var candidates []string
	for name := range r.Nodes {
		if used[name] {
			continue
		}
		if _, ok := r.Nodes.Pool(name, pool); !ok {
			continue
		}
		if !r.nodeFits(ctx, name, pool, vol.Spec.SizeBytes) {
			continue
		}
		candidates = append(candidates, name)
	}
	slices.Sort(candidates)
	for _, name := range candidates {
		if z := r.Nodes[name].Zone; z == "" || !usedZone[z] {
			return name
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	return ""
}

// nodeFits reports whether the node's pool has fresh stats and room for
// the volume's full virtual size — the same bar auto-diskful sets: a
// full sync must never land blind.
func (r *AutoEvictReconciler) nodeFits(ctx context.Context, node, pool string, sizeBytes int64) bool {
	mn := &miroirv1alpha1.MiroirNode{}
	if err := r.Get(ctx, types.NamespacedName{Name: node}, mn); err != nil {
		return false
	}
	if mn.Status.ObservedAt == nil || time.Since(mn.Status.ObservedAt.Time) > constants.StatsStaleAfter {
		return false
	}
	st := mn.Status.Pool(pool)
	return st != nil && st.CapacityBytes-st.AllocatedBytes >= sizeBytes
}

// stampEvicted records the force-release in status before it happens —
// the returning node's permission slip to scrub its abandoned leg.
func (r *AutoEvictReconciler) stampEvicted(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, dead string) error {
	patch := fmt.Appendf(nil, `{"status":{"evicted":{%q:%q}}}`,
		dead, time.Now().UTC().Format(time.RFC3339))
	return r.Status().Patch(ctx, vol, client.RawPatch(types.MergePatchType, patch))
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
