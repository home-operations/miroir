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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

// Controller implements csi.ControllerServer (notes/DESIGN.md §6.1). It translates
// CSI RPCs into MiroirVolume objects and waits for node agents to realize
// them — the Kubernetes API is the only channel to the data plane (§4.2).
type Controller struct {
	csi.UnimplementedControllerServer

	Client client.Client
	// APIReader reads straight from the API server, bypassing the
	// informer cache. Port allocation needs read-your-writes: the cache
	// can lag a just-created volume, handing its port out twice.
	APIReader client.Reader
	// Nodes is the storage topology from the Helm-rendered node map —
	// which nodes hold storage and with which backend.
	Nodes nodemap.Map
	// ProvisionTimeout bounds the wait for agents to realize a volume.
	ProvisionTimeout time.Duration
	// OvercommitRatio bounds thin-provisioning overcommit: CreateVolume is
	// refused when a node's provisioned total would exceed
	// capacity×ratio (notes/DESIGN.md §4.6). Zero → defaultOvercommitRatio.
	OvercommitRatio float64
	// AutoTieBreaker adds a diskless tie-breaker replica to new 2-replica
	// freeze volumes when a spare storage node exists (#70).
	AutoTieBreaker bool
	// DRBDPortBase is the lowest TCP port the allocator hands to replicated
	// volumes (one per resource, ascending). Zero → defaultDRBDPortBase.
	// Configurable so operators can move the range off host-network tenants
	// like the Ceph mgr dashboard (default 7000). See issue #148.
	DRBDPortBase int32

	// allocMu serialises CreateVolume RPCs that run concurrently within
	// the single controller pod: two interleaved List→Create spans would
	// hand out the same port. The port must be cluster-wide unique per
	// DRBD resource.
	allocMu sync.Mutex
}

const (
	defaultProvisionTimeout = 120 * time.Second // matches sidecars.provisioner.timeout
	// defaultOvercommitRatio caps provisioned-over-capacity per pool
	// (notes/DESIGN.md §4.6); 2× is the documented default.
	defaultOvercommitRatio = 2.0
	// defaultDRBDPortBase is the lowest DRBD replication port when
	// DRBDPortBase is unset (zero). Ceph mgr dashboard's non-SSL default
	// is also 7000; operators co-locating with Rook host-network Ceph can
	// move this via the --drbd-port-base flag / drbd.portBase Helm value.
	defaultDRBDPortBase = 7000
	// statsStaleAfter ignores MiroirNode figures older than this as
	// unknown — the agent republishes every ~60s, so a few missed polls
	// mean the node is down and its stats can't be trusted for placement.
	statsStaleAfter = 5 * time.Minute
)

// ControllerGetCapabilities advertises exactly what is implemented.
func (c *Controller) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
	}
	resp := &csi.ControllerGetCapabilitiesResponse{}
	for _, t := range caps {
		resp.Capabilities = append(resp.Capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: t},
			},
		})
	}
	return resp, nil
}

// CreateVolume provisions a MiroirVolume and waits until its agents report
// Ready (notes/DESIGN.md §4.5.1). Idempotent by volume name.
func (c *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}

	sizeBytes := req.GetCapacityRange().GetRequiredBytes()
	limitBytes := req.GetCapacityRange().GetLimitBytes()
	if sizeBytes == 0 {
		sizeBytes = 1 << 30 // spec allows omitting capacity_range; pick a sane floor
		if limitBytes > 0 && limitBytes < sizeBytes {
			sizeBytes = limitBytes
		}
	}
	if limitBytes > 0 && sizeBytes > limitBytes {
		return nil, status.Errorf(codes.OutOfRange,
			"required %d exceeds limit %d", sizeBytes, limitBytes)
	}

	replicas, err := parseReplicas(req.GetParameters())
	if err != nil {
		return nil, err
	}
	quorum, err := parseQuorum(req.GetParameters())
	if err != nil {
		return nil, err
	}

	snapID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
	source, srcReplicas, sourceFormatted, err := c.resolveSource(ctx, snapID, sizeBytes, replicas)
	if err != nil {
		return nil, err
	}

	// Serialise placement, port allocation, and Create as one critical
	// section: the overcommit guard and the DRBD port scan read fresh
	// cluster state, and a concurrent CreateVolume that has not committed
	// yet would otherwise be invisible to both — two RPCs could pass the
	// guard or claim the same port. waitReady is deliberately left outside.
	c.allocMu.Lock()
	// One volume List serves both the overcommit guard (place) and the
	// DRBD port scan (allocateDRBD); they need identical data and both run
	// under the lock. Fetched only when a placing or replicated path needs
	// it, so a 1-replica restore still does zero volume Lists.
	vols, err := c.allocVolumes(ctx, snapID == "" || replicas > 1)
	if err != nil {
		c.allocMu.Unlock()
		return nil, err
	}
	placed := srcReplicas
	if snapID == "" {
		p, err := c.place(ctx, req.GetAccessibilityRequirements(), replicas, sizeBytes, req.GetName(), vols)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
		placed, err = c.withTieBreaker(ctx, p, replicas, quorum)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
	}
	vol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: req.GetName()},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: sizeBytes,
			Replicas:  placed,
			Source:    source,
		},
	}
	for _, r := range placed {
		vol.Finalizers = append(vol.Finalizers, constants.FinalizerPrefix+r.Node)
	}
	if replicas > 1 {
		vol.Spec.QuorumPolicy = quorum
		drbdSpec, err := c.allocateDRBD(vols)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
		vol.Spec.DRBD = drbdSpec
	}
	createErr := c.Client.Create(ctx, vol)
	c.allocMu.Unlock()

	sourceSnapshot := ""
	if source != nil {
		sourceSnapshot = source.SnapshotName
	}
	if err := c.handleCreateErr(ctx, createErr, vol, sizeBytes, replicas, quorum, sourceSnapshot); err != nil {
		return nil, err
	}
	// A clone carries the source's filesystem: inherit Formatted before
	// any pod stages it, so a blank clone is refused instead of mkfs'd.
	if sourceFormatted {
		if err := c.markVolumeFormatted(ctx, vol.Name); err != nil {
			return nil, status.Errorf(codes.Unavailable, "record formatted flag on %s: %v", vol.Name, err)
		}
	}
	if err := c.waitReady(ctx, vol.Name); err != nil {
		return nil, err
	}

	topology := make([]*csi.Topology, 0, len(vol.Spec.Replicas))
	for _, r := range vol.Spec.Replicas {
		if r.Diskless {
			continue
		}
		topology = append(topology, &csi.Topology{
			Segments: map[string]string{constants.TopologyKey: r.Node},
		})
	}
	var contentSource *csi.VolumeContentSource
	if source != nil && source.SnapshotName != "" {
		contentSource = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: source.SnapshotName,
				},
			},
		}
	}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           vol.Name,
			CapacityBytes:      sizeBytes,
			AccessibleTopology: topology,
			ContentSource:      contentSource,
		},
	}, nil
}

// DeleteVolume removes the MiroirVolume; agents tear down local state via
// the finalizer before it disappears (notes/DESIGN.md §4.5.7). Idempotent.
func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	vol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: req.GetVolumeId()},
	}
	if err := c.Client.Delete(ctx, vol); err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "delete MiroirVolume: %v", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities confirms RWO/RWOP mount and block support.
func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get MiroirVolume: %v", err)
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil //nolint:nilerr
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// resolveSource resolves a restore's source: on a clone (snapID set) it
// validates the snapshot and size, and returns the placement replicas
// (following the source, FullSyncing post-snapshot legs). Returns zero
// values for a fresh volume (no content source).
func (c *Controller) resolveSource(ctx context.Context, snapID string, sizeBytes int64, replicas int) (*miroirv1alpha1.VolumeSource, []miroirv1alpha1.Replica, bool, error) {
	if snapID == "" {
		return nil, nil, false, nil
	}
	// Clones are local CoW, so replicas must live on the nodes holding the
	// snapshot — placement follows the source.
	srcVol, snap, err := c.snapshotSource(ctx, snapID)
	if err != nil {
		return nil, nil, false, err
	}
	if sizeBytes < snap.Status.SizeBytes {
		return nil, nil, false, status.Errorf(codes.InvalidArgument,
			"requested %d below snapshot size %d", sizeBytes, snap.Status.SizeBytes)
	}
	if len(srcVol.Spec.DiskfulReplicas()) != replicas {
		return nil, nil, false, status.Errorf(codes.InvalidArgument,
			"restore replica count %d must match source diskful replicas %d",
			replicas, len(srcVol.Spec.DiskfulReplicas()))
	}
	srcReplicas, err := c.restoreReplicas(ctx, srcVol, snap)
	if err != nil {
		return nil, nil, false, err
	}
	return &miroirv1alpha1.VolumeSource{SnapshotName: snapID}, srcReplicas, snap.Status.SourceFormatted, nil
}

// allocVolumes lists every volume once// allocVolumes lists every volume once for the allocMu critical section
// (the overcommit guard and the DRBD port scan share it). Skipped when
// unneeded — a 1-replica restore does zero volume Lists.
func (c *Controller) allocVolumes(ctx context.Context, needed bool) ([]miroirv1alpha1.MiroirVolume, error) {
	if !needed {
		return nil, nil
	}
	list := &miroirv1alpha1.MiroirVolumeList{}
	if err := c.reader().List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "list MiroirVolumes: %v", err)
	}
	return list.Items, nil
}

// place selects count replica nodes:// place selects count replica nodes: the scheduler's preference first
// (WaitForFirstConsumer), then the remaining eligible storage nodes by
// free space — capacity-aware spread (notes/DESIGN.md §4.6). Nodes whose
// projected provisioned total would breach the overcommit ratio are
// excluded, and a chosen node breaching it (e.g. a topology-pinned one)
// fails the request so the scheduler retries elsewhere. Pools without
// fresh stats are treated as unknown and allowed, so a cold cluster still
// provisions. For replicated volumes it resolves each node's InternalIP
// and assigns DRBD node ids by slice position — replicas[0] is the GI-seed
// winner (internal/drbd).
func (c *Controller) place(ctx context.Context, reqs *csi.TopologyRequirement, count int, sizeBytes int64, name string, vols []miroirv1alpha1.MiroirVolume) ([]miroirv1alpha1.Replica, error) {
	if len(c.Nodes) < count {
		return nil, status.Errorf(codes.ResourceExhausted,
			"need %d storage nodes, have %d (Helm values: nodes)", count, len(c.Nodes))
	}

	stats, err := c.poolStats(ctx)
	if err != nil {
		return nil, err
	}
	provisioned := provisionedPerNode(vols, name)
	ratio := c.OvercommitRatio
	if ratio <= 0 {
		ratio = defaultOvercommitRatio
	}
	// overcommitted reports whether placing sizeBytes on node would push
	// its provisioned total past capacity×ratio, using fresh stats only.
	overcommitted := func(node string) bool {
		st, ok := stats[node]
		if !ok || st.CapacityBytes <= 0 {
			return false
		}
		return float64(provisioned[node]+sizeBytes) > float64(st.CapacityBytes)*ratio
	}
	// freeBytes is the pool headroom used to rank candidates; 0 (sorts
	// last) when stats are unknown.
	freeBytes := func(node string) int64 {
		st, ok := stats[node]
		if !ok {
			return 0
		}
		if free := st.CapacityBytes - st.AllocatedBytes; free > 0 {
			return free
		}
		return 0
	}

	ordered := make([]string, 0, len(c.Nodes))
	// Scheduler-selected topology first — kept in place even if it later
	// fails the overcommit guard, so a topology-pinned volume refuses
	// rather than silently landing on a node the pod can't reach.
	for _, t := range append(reqs.GetPreferred(), reqs.GetRequisite()...) {
		if n, ok := t.GetSegments()[constants.TopologyKey]; ok {
			if _, ok := c.Nodes[n]; ok && !slices.Contains(ordered, n) {
				ordered = append(ordered, n)
			}
		}
	}
	if reqs != nil && len(reqs.GetRequisite()) > 0 && len(ordered) == 0 {
		return nil, status.Error(codes.ResourceExhausted,
			"no storage node satisfies the requested topology")
	}
	// Remaining eligible nodes, most free space first (ties by name).
	rest := make([]string, 0, len(c.Nodes))
	for n := range c.Nodes {
		if slices.Contains(ordered, n) || overcommitted(n) {
			continue
		}
		rest = append(rest, n)
	}
	slices.SortFunc(rest, func(a, b string) int {
		if d := cmp.Compare(freeBytes(b), freeBytes(a)); d != 0 {
			return d
		}
		return cmp.Compare(a, b)
	})
	pinned := len(ordered) // topology-selected nodes, honored unconditionally
	ordered = append(ordered, rest...)
	if len(ordered) < count {
		return nil, status.Errorf(codes.ResourceExhausted,
			"only %d of %d storage nodes can host a %d-byte volume within the %gx overcommit ratio",
			len(ordered), len(c.Nodes), sizeBytes, ratio)
	}
	ordered = spreadByZone(ordered, pinned, count, func(n string) string { return c.Nodes[n].Zone })
	for _, n := range ordered {
		if overcommitted(n) {
			return nil, status.Errorf(codes.ResourceExhausted,
				"node %s would exceed the %gx overcommit ratio for a %d-byte volume", n, ratio, sizeBytes)
		}
	}

	replicas := make([]miroirv1alpha1.Replica, 0, count)
	for i, name := range ordered {
		r := miroirv1alpha1.Replica{Node: name, Backend: c.Nodes[name].Backend}
		if count > 1 {
			addr, err := c.nodeInternalIP(ctx, name)
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
// 2-replica freeze volume, so majority quorum survives a single node loss
// (#70). Unchanged when disabled, not applicable, or no spare node exists —
// the tie-breaker reconciler retrofits skipped volumes once one joins.
func (c *Controller) withTieBreaker(ctx context.Context, placed []miroirv1alpha1.Replica, replicas int, quorum miroirv1alpha1.QuorumPolicy) ([]miroirv1alpha1.Replica, error) {
	if !c.AutoTieBreaker || replicas != 2 || quorum != miroirv1alpha1.QuorumFreeze {
		return placed, nil
	}
	tb := c.Nodes.TieBreakerNode(placed)
	if tb == "" {
		return placed, nil
	}
	addr, err := c.nodeInternalIP(ctx, tb)
	if err != nil {
		return nil, err
	}
	return append(placed, miroirv1alpha1.Replica{
		Node:     tb,
		NodeID:   int32(len(placed)), //nolint:gosec // ≤3 replicas
		Address:  addr,
		Diskless: true,
	}), nil
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

// nodeInternalIP resolves a node's replication endpoint from its Node
// object — no addresses to maintain in Helm values.
func (c *Controller) nodeInternalIP(ctx context.Context, name string) (string, error) {
	node := &corev1.Node{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
		return "", status.Errorf(codes.Internal, "get node %s: %v", name, err)
	}
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address, nil
		}
	}
	return "", status.Errorf(codes.Internal, "node %s has no InternalIP", name)
}

// reader returns the API-server reader for read-your-writes, falling back
// to the cached client (in tests, or before the manager wires APIReader).
// The overcommit guard and DRBD port scan run under allocMu and must see a
// concurrent CreateVolume's just-committed object, which the cache can lag.
func (c *Controller) reader() client.Reader {
	if c.APIReader != nil {
		return c.APIReader
	}
	return c.Client
}

// poolStats returns the fresh pool capacity each storage node's agent
// published (notes/DESIGN.md §4.6). Stale or never-published nodes are
// omitted — placement treats them as unknown (eligible, no free-space
// signal) so a cold cluster still provisions.
func (c *Controller) poolStats(ctx context.Context) (map[string]miroirv1alpha1.MiroirNodeStatus, error) {
	list := &miroirv1alpha1.MiroirNodeList{}
	if err := c.reader().List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "list MiroirNodes: %v", err)
	}
	out := make(map[string]miroirv1alpha1.MiroirNodeStatus, len(list.Items))
	for _, n := range list.Items {
		if n.Status.ObservedAt == nil || time.Since(n.Status.ObservedAt.Time) > statsStaleAfter {
			continue
		}
		out[n.Name] = n.Status
	}
	return out, nil
}

// provisionedPerNode sums the provisioned (virtual) size of every volume
// with a replica on each node, excluding exclude (the volume being
// (re)created, so an idempotent retry does not count itself). Clones share
// backing on disk but are counted in full — a conservative overcommit guard.
// provisionedPerNode sums the provisioned (virtual) bytes per node from a
// pre-fetched volume list, excluding the named volume (the one being
// (re)created, so an idempotent retry does not count itself).
func provisionedPerNode(vols []miroirv1alpha1.MiroirVolume, exclude string) map[string]int64 {
	out := map[string]int64{}
	for _, v := range vols {
		if v.Name == exclude {
			continue
		}
		for _, r := range v.Spec.Replicas {
			if r.Diskless {
				continue
			}
			out[r.Node] += v.Spec.SizeBytes
		}
	}
	return out
}

// allocateDRBD picks the lowest free TCP port by scanning existing
// volumes, and mints the resource's peer-auth secret. Callers hold
// allocMu across allocate+Create — CreateVolume RPCs run concurrently
// within the pod. The minor is per-node and allocated locally by each
// agent.
func (c *Controller) allocateDRBD(vols []miroirv1alpha1.MiroirVolume) (*miroirv1alpha1.DRBDSpec, error) {
	portBase := c.DRBDPortBase
	if portBase == 0 {
		portBase = defaultDRBDPortBase
	}
	usedPort := map[int32]bool{}
	for _, v := range vols {
		if v.Spec.DRBD != nil {
			usedPort[v.Spec.DRBD.Port] = true
		}
	}
	spec := &miroirv1alpha1.DRBDSpec{Port: portBase}
	for usedPort[spec.Port] {
		spec.Port++
	}
	// 24 random bytes → 48 hex chars, under DRBD's 64-character cap.
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return nil, status.Errorf(codes.Internal, "generate shared secret: %v", err)
	}
	spec.SharedSecret = hex.EncodeToString(raw)
	return spec, nil
}

// handleCreateErr resolves Create conflicts: nil for success, nil after a
// compatible AlreadyExists (mutating vol to the existing object), and a
// gRPC error otherwise. Idempotency: same name must mean same request.
func (c *Controller) handleCreateErr(ctx context.Context, err error, vol *miroirv1alpha1.MiroirVolume, sizeBytes int64, replicas int, quorum miroirv1alpha1.QuorumPolicy, sourceSnapshot string) error {
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return status.Errorf(codes.Internal, "create MiroirVolume: %v", err)
	}
	// AlreadyExists proves the object is on the API server; the informer
	// cache can still lag it, so read through APIReader. A read failure is
	// in-flight, not terminal — Unavailable keeps the provisioner retrying
	// (Internal is a final error → it would record "volume not created"
	// for a volume that demonstrably exists, and leak the CR).
	existing := &miroirv1alpha1.MiroirVolume{}
	if err := c.reader().Get(ctx, types.NamespacedName{Name: vol.Name}, existing); err != nil {
		return status.Errorf(codes.Unavailable, "get existing MiroirVolume: %v", err)
	}
	existingSource := ""
	if existing.Spec.Source != nil {
		existingSource = existing.Spec.Source.SnapshotName
	}
	existingDiskful := len(existing.Spec.DiskfulReplicas())
	if existing.Spec.SizeBytes != sizeBytes || existingDiskful != replicas ||
		(replicas > 1 && existing.Spec.QuorumPolicy != quorum) ||
		existingSource != sourceSnapshot {
		return status.Errorf(codes.AlreadyExists,
			"volume %s exists with size=%d diskful=%d quorum=%s source=%q (requested size=%d replicas=%d quorum=%s source=%q)",
			vol.Name, existing.Spec.SizeBytes, existingDiskful, existing.Spec.QuorumPolicy, existingSource,
			sizeBytes, replicas, quorum, sourceSnapshot)
	}
	*vol = *existing
	return nil
}

// errVolumeFailed marks a hard provisioning failure reported by an agent,
// as opposed to "not ready yet".
type errVolumeFailed struct{ detail string }

func (e *errVolumeFailed) Error() string { return e.detail }

// waitReady blocks until the volume is usable. Degraded counts as success: one
// replica is UpToDate and serving while the rest finish their initial sync in
// the background. Requiring full redundancy would blow the sidecar timeout on
// large volumes (a 50Gi initial sync far exceeds the 120s deadline, so the call
// retries forever and the PVC never binds). devicePath/NodeStage separately
// refuses an Inconsistent local replica, so no pod lands on stale data.
func (c *Controller) waitReady(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, cmp.Or(c.ProvisionTimeout, defaultProvisionTimeout))
	defer cancel()

	err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true,
		func(ctx context.Context) (bool, error) {
			vol := &miroirv1alpha1.MiroirVolume{}
			if err := c.Client.Get(ctx, types.NamespacedName{Name: name}, vol); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil // informer cache not warm yet; retry
				}
				return false, err
			}
			switch vol.Status.Phase {
			case miroirv1alpha1.VolumeReady, miroirv1alpha1.VolumeDegraded:
				return true, nil
			case miroirv1alpha1.VolumeFailed:
				return false, &errVolumeFailed{detail: fmt.Sprintf("%+v", vol.Status.PerNode)}
			default:
				return false, nil
			}
		})
	if err == nil {
		return nil
	}
	if failed, ok := errors.AsType[*errVolumeFailed](err); ok {
		// Hard agent failure (e.g. pool out of space). DeadlineExceeded
		// would make the provisioner retry forever.
		return status.Errorf(codes.ResourceExhausted, "volume %s failed: %s", name, failed.detail)
	}
	// Genuine timeout: the provisioner retries CreateVolume and finds the
	// existing CR, so the CR is deliberately left in place.
	return status.Errorf(codes.DeadlineExceeded, "volume %s not ready: %v", name, err)
}

// ControllerExpandVolume grows the volume online: bump the desired size
// and wait for every agent to realize it (backing devices + DRBD).
func (c *Controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	newSize := req.GetCapacityRange().GetRequiredBytes()
	if newSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity range is required")
	}
	nodeExpansion := req.GetVolumeCapability().GetBlock() == nil

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}
	if newSize > vol.Spec.SizeBytes {
		base := vol.DeepCopy()
		vol.Spec.SizeBytes = newSize
		if err := c.Client.Patch(ctx, vol, client.MergeFrom(base)); err != nil {
			return nil, status.Errorf(codes.Internal, "grow volume: %v", err)
		}
	}
	// Never shrink, and never advertise less capacity than the spec holds.
	respBytes := max(newSize, vol.Spec.SizeBytes)

	// Wait for every diskful replica to realize at least this RPC's size —
	// including idempotent retries whose earlier attempt already patched
	// the spec but timed out. Returning success before the device grew
	// would let kubelet run the node expansion against the old size: the
	// filesystem resize no-ops, the PVC is recorded expanded, and nothing
	// ever retriggers the grow. The csi-resizer retries this RPC, so a
	// slow grow (node rebooting) just re-waits each attempt.
	timeout := cmp.Or(c.ProvisionTimeout, defaultProvisionTimeout)
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := wait.PollUntilContextCancel(waitCtx, 500*time.Millisecond, true,
		func(ctx context.Context) (bool, error) {
			v := &miroirv1alpha1.MiroirVolume{}
			if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, v); err != nil {
				return false, client.IgnoreNotFound(err) // cache lag → retry
			}
			for _, rep := range v.Spec.Replicas {
				if rep.Diskless {
					continue
				}
				if v.Status.PerNode[rep.Node].SizeBytes < newSize {
					return false, nil
				}
			}
			return true, nil
		})
	if err != nil {
		return nil, status.Errorf(codes.DeadlineExceeded, "volume %s not grown yet: %v", req.GetVolumeId(), err)
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         respBytes,
		NodeExpansionRequired: nodeExpansion,
	}, nil
}

// CreateSnapshot provisions a MiroirSnapshot and reports readiness as-is:
// the external-snapshotter polls until ready_to_use. Idempotent by name.
func (c *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetName() == "" || req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name and source volume are required")
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetSourceVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetSourceVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}

	snap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: req.GetName()},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: req.GetSourceVolumeId()},
	}
	for _, rep := range vol.Spec.Replicas {
		snap.Finalizers = append(snap.Finalizers, constants.FinalizerPrefix+rep.Node)
	}
	if err := c.Client.Create(ctx, snap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, status.Errorf(codes.Internal, "create MiroirSnapshot: %v", err)
		}
		existing := &miroirv1alpha1.MiroirSnapshot{}
		if err := c.reader().Get(ctx, types.NamespacedName{Name: req.GetName()}, existing); err != nil {
			// Cache may lag the just-created object; retryable, not terminal.
			return nil, status.Errorf(codes.Unavailable, "get existing snapshot: %v", err)
		}
		if existing.Spec.VolumeName != req.GetSourceVolumeId() {
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot %s exists for volume %s", req.GetName(), existing.Spec.VolumeName)
		}
		snap = existing
	}
	// Report the size captured at snapshot time once known; the live
	// volume may have been expanded since.
	size := cmp.Or(snap.Status.SizeBytes, vol.Spec.SizeBytes)
	return &csi.CreateSnapshotResponse{Snapshot: csiSnapshot(snap, size)}, nil
}

// DeleteSnapshot removes the MiroirSnapshot; agents drop the backend
// snapshots via finalizers. Idempotent.
func (c *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot id is required")
	}
	snap := &miroirv1alpha1.MiroirSnapshot{ObjectMeta: metav1.ObjectMeta{Name: req.GetSnapshotId()}}
	if err := c.Client.Delete(ctx, snap); err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "delete MiroirSnapshot: %v", err)
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots reports existing snapshots (no pagination: home scale).
func (c *Controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := c.Client.List(ctx, snaps); err != nil {
		return nil, status.Errorf(codes.Internal, "list snapshots: %v", err)
	}
	resp := &csi.ListSnapshotsResponse{}
	for i := range snaps.Items {
		s := &snaps.Items[i]
		if req.GetSnapshotId() != "" && s.Name != req.GetSnapshotId() {
			continue
		}
		if req.GetSourceVolumeId() != "" && s.Spec.VolumeName != req.GetSourceVolumeId() {
			continue
		}
		resp.Entries = append(resp.Entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: csiSnapshot(s, s.Status.SizeBytes),
		})
	}
	return resp, nil
}

// ListVolumes returns all provisioned volumes for external-provisioner reconciliation.
func (c *Controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := c.Client.List(ctx, vols); err != nil {
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	// Tokens are positional; the listing order must be stable across calls
	// or pages skip and duplicate entries.
	slices.SortFunc(vols.Items, func(a, b miroirv1alpha1.MiroirVolume) int {
		return cmp.Compare(a.Name, b.Name)
	})

	start := 0
	if req.GetStartingToken() != "" {
		n, err := strconv.Atoi(req.GetStartingToken())
		if err != nil || n < 0 || n >= len(vols.Items) {
			return nil, status.Errorf(codes.Aborted, "invalid starting_token: %s", req.GetStartingToken())
		}
		start = n
	}

	maxEntries := int(req.GetMaxEntries())
	if maxEntries <= 0 {
		maxEntries = len(vols.Items) - start
	}

	resp := &csi.ListVolumesResponse{}
	end := min(start+maxEntries, len(vols.Items))

	for i := start; i < end; i++ {
		v := &vols.Items[i]
		topology := make([]*csi.Topology, 0, len(v.Spec.Replicas))
		for _, r := range v.Spec.Replicas {
			if r.Diskless {
				continue
			}
			topology = append(topology, &csi.Topology{
				Segments: map[string]string{constants.TopologyKey: r.Node},
			})
		}
		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:           v.Name,
				CapacityBytes:      v.Spec.SizeBytes,
				AccessibleTopology: topology,
			},
		})
	}

	if end < len(vols.Items) {
		resp.NextToken = strconv.Itoa(end)
	}
	return resp, nil
}

func csiSnapshot(snap *miroirv1alpha1.MiroirSnapshot, sizeBytes int64) *csi.Snapshot {
	return &csi.Snapshot{
		SnapshotId:     snap.Name,
		SourceVolumeId: snap.Spec.VolumeName,
		SizeBytes:      sizeBytes,
		CreationTime:   timestamppb.New(snap.CreationTimestamp.Time),
		ReadyToUse:     snap.Status.ReadyToUse,
	}
}

// restoreReplicas copies the source volume's replica layout for a clone,
// cleaned: FullSync is stripped — every leg clones its byte-identical
// local snapshot, so a carried flag would full-resync one for nothing —
// and replication addresses are re-resolved from the live Node objects
// (the source's were captured at its creation and can be stale).
func (c *Controller) restoreReplicas(ctx context.Context, srcVol *miroirv1alpha1.MiroirVolume, snap *miroirv1alpha1.MiroirSnapshot) ([]miroirv1alpha1.Replica, error) {
	reps := slices.Clone(srcVol.Spec.Replicas)
	for i := range reps {
		// A leg clones from that node's local snapshot only if the node
		// actually cut one. A replica added AFTER the snapshot was taken
		// has no local snapshot: mark it FullSync so the agent creates a
		// fresh backing and DRBD full-syncs it from a Done peer, instead
		// of failing CreateFromSnapshot and flipping the clone to Failed.
		done := snap.Status.PerNode[reps[i].Node] == miroirv1alpha1.SnapshotDone
		reps[i].FullSync = !reps[i].Diskless && !done
		if reps[i].Address == "" {
			continue
		}
		addr, err := c.nodeInternalIP(ctx, reps[i].Node)
		if err != nil {
			return nil, err
		}
		reps[i].Address = addr
	}
	// The GI-seed winner (first diskful replica) is the clone's sync
	// source, so it must hold real data — refuse a restore whose seed leg
	// was added after the snapshot (no complete leg to seed from). A
	// ReadyToUse snapshot has every pre-existing diskful leg Done, so this
	// only rejects the pathological post-snapshot seed case.
	if seed := firstDiskful(reps); seed != nil && seed.FullSync {
		return nil, status.Errorf(codes.FailedPrecondition,
			"snapshot %s has no complete leg on seed node %s; cannot restore", snap.Name, seed.Node)
	}
	return reps, nil
}

// firstDiskful returns the first non-diskless replica, or nil.
func firstDiskful(reps []miroirv1alpha1.Replica) *miroirv1alpha1.Replica {
	for i := range reps {
		if !reps[i].Diskless {
			return &reps[i]
		}
	}
	return nil
}

// snapshotSource resolves a ready snapshot and its source volume.
func (c *Controller) snapshotSource(ctx context.Context, snapID string) (*miroirv1alpha1.MiroirVolume, *miroirv1alpha1.MiroirSnapshot, error) {
	snap := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: snapID}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, status.Errorf(codes.NotFound, "snapshot %s not found", snapID)
		}
		return nil, nil, status.Errorf(codes.Internal, "get snapshot: %v", err)
	}
	if !snap.Status.ReadyToUse {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "snapshot %s not ready", snapID)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: snap.Spec.VolumeName}, vol); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "get snapshot source volume: %v", err)
	}
	return vol, snap, nil
}

// markVolumeFormatted records that the volume carries a filesystem. Reads
// fresh and patches so it works for both just-created and pre-existing
// volumes (idempotent CreateVolume retries).
func (c *Controller) markVolumeFormatted(ctx context.Context, name string) error {
	vol := &miroirv1alpha1.MiroirVolume{}
	// Read through APIReader: this runs right after Create, and the
	// informer cache reliably lags a just-created object — a cached Get
	// here 404s and fails the whole CreateVolume into a retry loop.
	if err := c.reader().Get(ctx, types.NamespacedName{Name: name}, vol); err != nil {
		return err
	}
	return markFormatted(ctx, c.Client, vol)
}

// markFormatted flips the Formatted status flag once; shared by the
// controller (clone inheritance) and the node service (post-mkfs).
func markFormatted(ctx context.Context, cl client.Client, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Status.Formatted {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Formatted = true
	return cl.Status().Patch(ctx, vol, client.MergeFrom(base))
}

// markActivated latches the Activated status flag once, the first time a
// node stages the volume for a consumer. It gates split-brain auto-recovery
// (see agent VolumeReconciler.recoverSplitBrain): a staged volume may hold
// data, so its divergence is never auto-discarded.
func markActivated(ctx context.Context, cl client.Client, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Status.Activated {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Activated = true
	return cl.Status().Patch(ctx, vol, client.MergeFrom(base))
}

func parseReplicas(params map[string]string) (int, error) {
	raw, ok := params[constants.ParamReplicas]
	if !ok {
		return 1, nil
	}
	// Ceiling: DRBD9 metadata reservation (--max-peers=7) and
	// last-man-standing quorum only make sense for ≤3 replicas.
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 3 {
		return 0, status.Errorf(codes.InvalidArgument,
			"invalid %s=%q (want 1..3)", constants.ParamReplicas, raw)
	}
	return n, nil
}

// parseQuorum defaults to freeze: with the auto-added tie-breaker a
// 2-replica volume gets majority quorum without split-brain exposure (#70).
func parseQuorum(params map[string]string) (miroirv1alpha1.QuorumPolicy, error) {
	switch raw := params[constants.ParamQuorum]; raw {
	case "", string(miroirv1alpha1.QuorumFreeze):
		return miroirv1alpha1.QuorumFreeze, nil
	case string(miroirv1alpha1.QuorumLastManStanding):
		return miroirv1alpha1.QuorumLastManStanding, nil
	default:
		return "", status.Errorf(codes.InvalidArgument,
			"invalid %s=%q (want last-man-standing | freeze)", constants.ParamQuorum, raw)
	}
}

func validateCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		switch c.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
		default:
			return status.Errorf(codes.InvalidArgument,
				"unsupported access mode %s (miroir is RWO/RWOP only)",
				c.GetAccessMode().GetMode())
		}
		if c.GetMount() == nil && c.GetBlock() == nil {
			return status.Error(codes.InvalidArgument, "capability must be mount or block")
		}
	}
	return nil
}
