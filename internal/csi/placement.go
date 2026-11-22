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

package csi

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

// placeVolume decides the volume's replica layout inside the allocMu
// critical section: fresh volumes are placed (plus tie-breaker), a
// restore follows its source, and a restore whose class requests a
// different diskful count than the source had is reshaped.
// One topology snapshot serves placement and the tie-breaker pick;
// separate resolves could observe different topologies mid-RPC.
func (c *Controller) placeVolume(ctx context.Context, req *csi.CreateVolumeRequest, snapID string, srcReplicas []miroirv1alpha1.Replica, replicas int, sizeBytes int64, vols []miroirv1alpha1.MiroirVolume, remoteAccess bool, pool string, quorum miroirv1alpha1.QuorumPolicy) ([]miroirv1alpha1.Replica, error) {
	if snapID != "" && len(diskfulOf(srcReplicas)) == replicas {
		return srcReplicas, nil
	}
	nodes, err := c.nodes(ctx)
	if err != nil {
		return nil, err
	}
	if snapID != "" {
		return c.reshapeRestore(ctx, nodes, req.GetAccessibilityRequirements(),
			srcReplicas, replicas, sizeBytes, req.GetName(), vols, remoteAccess, pool, quorum)
	}
	p, err := c.place(ctx, nodes, req.GetAccessibilityRequirements(), replicas, sizeBytes, req.GetName(), vols, remoteAccess, pool, nil)
	if err != nil {
		return nil, err
	}
	return c.withTieBreaker(ctx, nodes, p, replicas, quorum)
}

// place selects count replica nodes carrying the class's pool: the
// scheduler's preference first (WaitForFirstConsumer), then the remaining
// eligible storage nodes by that pool's free space — capacity-aware
// spread. Nodes whose projected provisioned total for the pool would
// breach the overcommit ratio are excluded, and a chosen node breaching it
// (e.g. a topology-pinned one) fails the request so the scheduler retries
// elsewhere. Pools without fresh stats are treated as unknown and allowed,
// so a cold cluster still provisions. For replicated volumes it resolves
// each node's InternalIP and assigns DRBD node ids by slice position —
// replicas[0] is the GI-seed winner (internal/drbd).
func (c *Controller) place(ctx context.Context, nodes nodemap.Map, reqs *csi.TopologyRequirement, count int, sizeBytes int64, name string, vols []miroirv1alpha1.MiroirVolume, remoteAccess bool, pool string, excluded map[string]bool) ([]miroirv1alpha1.Replica, error) {
	// Every diskful leg needs the pool on its own node; a class naming a
	// pool too few nodes carry must say so instead of a generic refusal.
	// Unplaceable nodes (address conflict) are excluded outright — but
	// counted, so the refusal names the real cause instead of blaming the
	// pool declarations. Callers placing joiners for a reshaped restore
	// pass the nodes already holding a leg as excluded.
	candidates := map[string]nodemap.Pool{}
	conflicted := 0
	for n := range nodes {
		if excluded[n] {
			continue
		}
		p, ok := nodes.Pool(n, pool)
		if !ok {
			continue
		}
		if !nodes.Placeable(n) {
			conflicted++
			continue
		}
		candidates[n] = p
	}
	if len(candidates) < count {
		if conflicted > 0 {
			return nil, status.Errorf(codes.ResourceExhausted,
				"storage pool %q exists on %d of %d storage nodes, but %d carrying it are excluded by a replication "+
					"address conflict; a %d-replica class needs %d (kubectl get miroirnodes; see the AddressConflict condition)",
				pool, len(candidates)+conflicted, len(nodes), conflicted, count, count)
		}
		return nil, status.Errorf(codes.ResourceExhausted,
			"storage pool %q exists on %d of %d storage nodes; a %d-replica class needs %d (Helm values: nodes.<name>.spec.pools)",
			pool, len(candidates), len(nodes), count, count)
	}

	stats, err := c.poolStats(ctx)
	if err != nil {
		return nil, err
	}
	provisioned := provisionedPerPool(vols, name)
	// overcommitted reports whether the pool on node lacks the headroom for
	// sizeBytes, using fresh stats only — a node that has published none is
	// admitted (GetCapacity is the one that steers the scheduler away).
	overcommitted := func(node string) bool {
		room, known := c.poolHeadroom(stats[node].Pool(pool), provisioned[nodePool{node, pool}])
		if !known {
			return false
		}
		return sizeBytes > room
	}
	// headroom ranks candidates by the same allowance the admission guard
	// computes: provisioned counts volumes committed moments ago under
	// allocMu, so a burst spreads across nodes instead of piling onto
	// whichever one the ~minutely pool stats still call emptiest. Unknown
	// stats rank last (0) but stay admitted.
	headroom := func(node string) int64 {
		room, known := c.poolHeadroom(stats[node].Pool(pool), provisioned[nodePool{node, pool}])
		if !known {
			return 0
		}
		return room
	}
	// physicalFree breaks headroom ties: the virtual overcommit bound
	// usually binds first on freshly provisioned pools, flattening real
	// free-space differences that should still steer placement.
	physicalFree := func(node string) int64 {
		st := stats[node].Pool(pool)
		if st == nil {
			return 0
		}
		return max(0, st.CapacityBytes-st.AllocatedBytes)
	}

	// Pin only the scheduler-selected node: with delayed binding the
	// provisioner sends the whole cluster topology rotated so the selected
	// node leads the preferred list — everything behind it is rotation
	// artifact, not scheduler intent. Honoring the full list replayed the
	// rotation verbatim: every second replica landed on selected+1 and the
	// capacity ranking below never ran, starving the rotation's tail node.
	// The pinned node is kept even if it later fails the
	// overcommit guard, so a topology-pinned volume refuses rather than
	// silently landing on a node the pod can't reach.
	ordered := make([]string, 0, len(candidates))
	pin, topologyMatched := topologyPick(reqs, candidates, excluded)
	if pin != "" {
		ordered = append(ordered, pin)
	}
	if reqs != nil && len(reqs.GetRequisite()) > 0 && !topologyMatched && !remoteAccess {
		// On a remote-access volume the scheduler may pick a non-storage
		// node for the first consumer (the PV will carry no affinity);
		// fall through to capacity-ranked placement — the pod attaches
		// through a diskless client leg. Everything else refuses: the pod
		// could never reach a volume placed off its node.
		return nil, status.Errorf(codes.ResourceExhausted,
			"no node carrying storage pool %q satisfies the requested topology", pool)
	}
	// Remaining eligible nodes, largest headroom first; ties break by
	// physical free space, then least provisioned (all a node has before
	// its first stats publish), then name.
	rest := make([]string, 0, len(candidates))
	for n := range candidates {
		if slices.Contains(ordered, n) || overcommitted(n) {
			continue
		}
		rest = append(rest, n)
	}
	slices.SortFunc(rest, func(a, b string) int {
		if d := cmp.Compare(headroom(b), headroom(a)); d != 0 {
			return d
		}
		if d := cmp.Compare(physicalFree(b), physicalFree(a)); d != 0 {
			return d
		}
		if d := cmp.Compare(provisioned[nodePool{a, pool}], provisioned[nodePool{b, pool}]); d != 0 {
			return d
		}
		return cmp.Compare(a, b)
	})
	pinned := len(ordered) // topology-selected nodes, honored unconditionally
	ordered = append(ordered, rest...)
	overcommit, freeSpace := c.ratios()
	if len(ordered) < count {
		return nil, status.Errorf(codes.ResourceExhausted,
			"only %d of the %d nodes carrying pool %q can host a %d-byte volume within its capacity guardrails "+
				"(capacity×%g overcommit, free×%g free-space)",
			len(ordered), len(candidates), pool, sizeBytes, overcommit, freeSpace)
	}
	ordered = spreadByZone(ordered, pinned, count, func(n string) string { return nodes[n].Zone })
	for _, n := range ordered {
		if overcommitted(n) {
			room, _ := c.poolHeadroom(stats[n].Pool(pool), provisioned[nodePool{n, pool}])
			return nil, status.Errorf(codes.ResourceExhausted,
				"pool %q on node %s has room for %d of the requested %d bytes "+
					"(capacity×%g overcommit, free×%g free-space)",
				pool, n, room, sizeBytes, overcommit, freeSpace)
		}
	}

	replicas := make([]miroirv1alpha1.Replica, 0, count)
	for i, name := range ordered {
		r := miroirv1alpha1.Replica{Node: name, Backend: candidates[name].Backend, Pool: pool}
		if count > 1 {
			addr, err := c.replicationAddress(ctx, nodes, name)
			if err != nil {
				return nil, err
			}
			r.NodeID = int32(i) //nolint:gosec // count <= 3
			r.Address = addr
		}
		replicas = append(replicas, r)
	}
	return replicas, nil
}

// withTieBreaker appends a diskless tie-breaker to a freshly placed
// 2-replica freeze volume, so majority quorum survives a single node loss.
// It picks from the same topology snapshot the caller placed with.
// Unchanged when disabled, not applicable, or no spare node exists —
// the tie-breaker reconciler retrofits skipped volumes once one joins.
func (c *Controller) withTieBreaker(ctx context.Context, nodes nodemap.Map, placed []miroirv1alpha1.Replica, replicas int, quorum miroirv1alpha1.QuorumPolicy) ([]miroirv1alpha1.Replica, error) {
	if !c.AutoTieBreaker || replicas != 2 || quorum != miroirv1alpha1.QuorumFreeze {
		return placed, nil
	}
	tb := nodes.TieBreakerNode(placed)
	if tb == "" {
		return placed, nil
	}
	addr, err := c.replicationAddress(ctx, nodes, tb)
	if err != nil {
		return nil, err
	}
	return append(placed, miroirv1alpha1.Replica{
		Node: tb,
		// Smallest unused, not positional: a reshaped restore preserves
		// the source legs' ids (clone metadata is keyed by them), so the
		// kept set can be sparse. Identical to len(placed) for a freshly
		// placed contiguous layout.
		NodeID:   nextFreeNodeID(placed),
		Address:  addr,
		Diskless: true,
	}), nil
}

// topologyPick resolves the scheduler's topology selection against the
// placeable candidates: the first candidate named by the requirement is
// pinned. A named node that instead already holds a kept leg (a reshaped
// restore placing joiners passes those as excluded) satisfies the
// topology without a pin — scanning on would pin a rotation artifact,
// the exact #258 bug — and must not trip the topology refusal.
func topologyPick(reqs *csi.TopologyRequirement, candidates map[string]nodemap.Pool, excluded map[string]bool) (pin string, matched bool) {
	for _, t := range slices.Concat(reqs.GetPreferred(), reqs.GetRequisite()) {
		n, ok := t.GetSegments()[constants.TopologyKey]
		if !ok {
			continue
		}
		if excluded[n] {
			return "", true
		}
		if _, ok := candidates[n]; ok {
			return n, true
		}
	}
	return "", false
}

// nextFreeNodeID returns the smallest DRBD node id no replica uses.
func nextFreeNodeID(reps []miroirv1alpha1.Replica) int32 {
	used := map[int32]bool{}
	for _, r := range reps {
		used[r.NodeID] = true
	}
	for id := int32(0); ; id++ {
		if !used[id] {
			return id
		}
	}
}

// diskfulOf filters a replica layout down to its diskful legs, order
// preserved.
func diskfulOf(reps []miroirv1alpha1.Replica) []miroirv1alpha1.Replica {
	diskful := make([]miroirv1alpha1.Replica, 0, len(reps))
	for _, rep := range reps {
		if !rep.Diskless {
			diskful = append(diskful, rep)
		}
	}
	return diskful
}

// reshapeRestore adapts a restore's replica layout to a class requesting
// a different diskful count than the source volume had.
// Growing appends freshly placed FullSync joiners — their content
// arrives over the wire, so unlike the CoW legs they may land anywhere
// the pool reaches. Shrinking keeps a subset of the source legs — the
// scheduler-selected leg when complete, else a relocation decided by
// shrinkPin — then seed first and Done legs preferred. The source tie-breaker (if any) is
// discarded and re-derived for the final shape, and node ids are
// re-numbered around the preserved legs — a leg cloning DRBD metadata
// keeps the id that metadata is keyed by, everything else takes the
// smallest unused id. A 1-replica target ends unreplicated: the sole
// kept leg sheds the replication-only fields (the clone's embedded
// metadata becomes inert tail bytes past the filesystem).
func (c *Controller) reshapeRestore(ctx context.Context, nodes nodemap.Map, reqs *csi.TopologyRequirement, base []miroirv1alpha1.Replica, target int, sizeBytes int64, name string, vols []miroirv1alpha1.MiroirVolume, remoteAccess bool, pool string, quorum miroirv1alpha1.QuorumPolicy) ([]miroirv1alpha1.Replica, error) {
	diskful := diskfulOf(base)
	switch {
	case len(diskful) > target:
		pin := ""
		if !remoteAccess {
			selected := selectedTopologyNode(reqs)
			var err error
			if pin, err = shrinkPin(reqs, diskful, selected); err != nil {
				return nil, err
			}
			if selected != "" && pin != selected {
				log.Info("shrink restore relocating off a selected node with no complete snapshot leg",
					"volume", name, "selected", selected, "kept", cmp.Or(pin, diskful[0].Node))
			}
		}
		diskful = keepRestoreLegs(diskful, target, pin)
	case len(diskful) < target:
		excluded := map[string]bool{}
		for _, rep := range diskful {
			excluded[rep.Node] = true
		}
		extra, err := c.place(ctx, nodes, reqs, target-len(diskful), sizeBytes, name, vols, remoteAccess, pool, excluded)
		if err != nil {
			return nil, err
		}
		for i := range extra {
			extra[i].FullSync = true
		}
		diskful = append(diskful, extra...)
	}
	if target == 1 {
		diskful[0].NodeID, diskful[0].Address, diskful[0].FullSync = 0, "", false
		return diskful[:1], nil
	}
	// Preserved clone legs (recognizable by their replicated-source
	// address) keep their ids; everything else — an unreplicated source's
	// seed, placement's positional ids — is re-numbered and addressed.
	used := map[int32]bool{}
	assign := make([]int, 0, len(diskful))
	for i := range diskful {
		if diskful[i].Address != "" && !diskful[i].FullSync {
			used[diskful[i].NodeID] = true
			continue
		}
		assign = append(assign, i)
	}
	for _, i := range assign {
		id := int32(0)
		for used[id] {
			id++
		}
		used[id] = true
		diskful[i].NodeID = id
		if diskful[i].Address == "" {
			addr, err := c.replicationAddress(ctx, nodes, diskful[i].Node)
			if err != nil {
				return nil, err
			}
			diskful[i].Address = addr
		}
	}
	return c.withTieBreaker(ctx, nodes, diskful, target, quorum)
}

// keepRestoreLegs selects target legs from the source's diskful layout.
// A pinned completed leg is guaranteed inclusion; for an unreplicated
// target it replaces the source seed, which no longer has a DRBD generation
// to bootstrap. Replicated targets keep the seed first, then the pin, then
// the remaining legs in order with Done before FullSync.
func keepRestoreLegs(diskful []miroirv1alpha1.Replica, target int, pin string) []miroirv1alpha1.Replica {
	kept := make([]miroirv1alpha1.Replica, 0, target)
	kept = append(kept, diskful[0])
	if pin != "" {
		pinned := diskful[slices.IndexFunc(diskful, func(rep miroirv1alpha1.Replica) bool {
			return rep.Node == pin
		})]
		if target == 1 {
			return []miroirv1alpha1.Replica{pinned}
		}
		if pin != diskful[0].Node {
			kept = append(kept, pinned)
		}
	}
	for _, fullSync := range []bool{false, true} {
		for _, rep := range diskful[1:] {
			if rep.Node != pin && rep.FullSync == fullSync && len(kept) < target {
				kept = append(kept, rep)
			}
		}
	}
	return kept
}

// shrinkPin resolves the topology selection against a shrink-restore's
// complete legs. A selected node holding a complete leg is honored. A
// legless selection is a scheduling artifact, not a constraint: preferred
// topology is best-effort, and the scheduler cannot see leg placement
// (capacity tracking has no per-volume dimension), so refusing would only
// replay the same selection forever and strand the PVC in
// ProvisioningFailed. The shrink instead relocates to a complete
// leg within requisite — the hard bound (allowedTopologies,
// strict-topology) — and the response's AccessibleTopology re-places the
// consumer. Only when requisite admits no complete leg does the refusal
// stand: that volume cannot exist where the request demands it.
func shrinkPin(reqs *csi.TopologyRequirement, diskful []miroirv1alpha1.Replica, selected string) (string, error) {
	complete := func(node string) bool {
		return slices.ContainsFunc(diskful, func(rep miroirv1alpha1.Replica) bool {
			return rep.Node == node && !rep.FullSync
		})
	}
	if selected == "" || complete(selected) {
		return selected, nil
	}
	requisite := map[string]bool{}
	for _, top := range reqs.GetRequisite() {
		if node := top.GetSegments()[constants.TopologyKey]; node != "" {
			requisite[node] = true
		}
	}
	// The seed is always complete (resolveSource refuses otherwise) and
	// keepRestoreLegs keeps it first, so it needs no pin.
	if len(requisite) == 0 || requisite[diskful[0].Node] {
		return "", nil
	}
	for _, rep := range diskful {
		if !rep.FullSync && requisite[rep.Node] {
			return rep.Node, nil
		}
	}
	return "", status.Errorf(codes.ResourceExhausted,
		"no complete source snapshot leg exists on topology node %q or within the requested topology", selected)
}

// selectedTopologyNode returns the scheduler-selected node from a
// WaitForFirstConsumer request. The provisioner puts it first in preferred;
// strict-topology requests also carry it as the sole requisite entry.
func selectedTopologyNode(reqs *csi.TopologyRequirement) string {
	for _, tops := range [][]*csi.Topology{reqs.GetPreferred(), reqs.GetRequisite()} {
		for _, top := range tops {
			if node := top.GetSegments()[constants.TopologyKey]; node != "" {
				return node
			}
		}
	}
	return ""
}

// spreadByZone selects count nodes from ordered (already ranked by topology
// then free space), preferring distinct failure domains. The first `pinned`
// entries are topology-selected and taken unconditionally; the rest fill
// remaining slots, nodes in a not-yet-used zone first, then — only if zones
// run short — the leftovers in rank order. A node with an empty zone is
// unconstrained, so when no node declares a zone this returns ordered[:count]
// unchanged. Callers guarantee len(ordered) >= count.
func spreadByZone(ordered []string, pinned, count int, zoneOf func(string) string) []string {
	picked := make([]string, 0, count)
	used := map[string]bool{}
	take := func(n string) {
		picked = append(picked, n)
		if z := zoneOf(n); z != "" {
			used[z] = true
		}
	}
	for i := 0; i < pinned && len(picked) < count; i++ {
		take(ordered[i])
	}
	for i := pinned; i < len(ordered) && len(picked) < count; i++ {
		if z := zoneOf(ordered[i]); z == "" || !used[z] {
			take(ordered[i])
		}
	}
	for i := pinned; i < len(ordered) && len(picked) < count; i++ {
		if !slices.Contains(picked, ordered[i]) {
			take(ordered[i])
		}
	}
	return picked
}

// replicationAddress resolves a node's replication endpoint — the node
// map's address override, or the node's InternalIP when unset.
func (c *Controller) replicationAddress(ctx context.Context, nodes nodemap.Map, name string) (string, error) {
	addr, err := nodes.ReplicationAddress(ctx, c.Client, name)
	if err != nil {
		return "", status.Errorf(codes.Internal, "%v", err)
	}
	return addr, nil
}

// poolStats returns the fresh pool capacity each storage node's agent
// published. Stale or never-published nodes are
// omitted — placement treats them as unknown (eligible, no free-space
// signal) so a cold cluster still provisions.
func (c *Controller) poolStats(ctx context.Context) (map[string]miroirv1alpha1.MiroirNodeStatus, error) {
	list := &miroirv1alpha1.MiroirNodeList{}
	if err := c.reader().List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "list MiroirNodes: %v", err)
	}
	out := make(map[string]miroirv1alpha1.MiroirNodeStatus, len(list.Items))
	for _, n := range list.Items {
		if n.Status.ObservedAt == nil || time.Since(n.Status.ObservedAt.Time) > constants.StatsStaleAfter {
			continue
		}
		out[n.Name] = n.Status
	}
	return out, nil
}

// poolHeadroom reports the largest volume a pool can still admit: its
// virtual overcommit allowance (capacity×OvercommitRatio − provisioned)
// bounded by its physical room (free×FreeSpaceRatio). Thin legs consume
// the pool only as they fill, so the virtual bound alone keeps admitting
// onto a pool with no physical space left — and ENOSPC under a live
// volume surfaces as DRBD I/O errors and a detached leg rather than a
// clean refusal at provision time.
//
// known is false when the node published no fresh stats; the two callers
// deliberately disagree on what that means, so neither gets a default
// here (place() admits anyway, GetCapacity reports zero).
func (c *Controller) poolHeadroom(st *miroirv1alpha1.MiroirNodePoolStatus, provisioned int64) (room int64, known bool) {
	if st == nil || st.CapacityBytes <= 0 {
		return 0, false
	}
	overcommit, freeSpace := c.ratios()
	virtual := int64(float64(st.CapacityBytes)*overcommit) - provisioned
	physical := int64(float64(max(0, st.CapacityBytes-st.AllocatedBytes)) * freeSpace)
	return max(0, min(virtual, physical)), true
}

// ratios reports the effective admission ratios with defaults applied, so
// a refusal can name the knobs an operator would turn.
func (c *Controller) ratios() (overcommit, freeSpace float64) {
	overcommit, freeSpace = c.OvercommitRatio, c.FreeSpaceRatio
	if overcommit <= 0 {
		overcommit = defaultOvercommitRatio
	}
	if freeSpace <= 0 {
		freeSpace = defaultFreeSpaceRatio
	}
	return overcommit, freeSpace
}

// nodePool keys per-pool bookkeeping: one pool on one node. Replica pool
// names are normalized (empty → default) before keying.
type nodePool struct {
	node string
	pool string
}

// provisionedPerPool sums the provisioned (virtual) bytes per (node, pool)
// from a pre-fetched volume list, excluding the named volume (the one being
// (re)created, so an idempotent retry does not count itself). Clones share
// backing on disk but are counted in full — a conservative overcommit guard.
func provisionedPerPool(vols []miroirv1alpha1.MiroirVolume, exclude string) map[nodePool]int64 {
	out := map[nodePool]int64{}
	for _, v := range vols {
		if v.Name == exclude {
			continue
		}
		for _, r := range v.Spec.Replicas {
			if r.Diskless {
				continue
			}
			out[nodePool{r.Node, nodemap.PoolOrDefault(r.Pool)}] += v.Spec.SizeBytes
		}
	}
	return out
}
