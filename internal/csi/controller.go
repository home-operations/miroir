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
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
	"github.com/home-operations/miroir/internal/stage"
)

// Controller implements csi.ControllerServer. It translates
// CSI RPCs into MiroirVolume objects and waits for node agents to realize
// them — the Kubernetes API is the only channel to the data plane (§4.2).
type Controller struct {
	csi.UnimplementedControllerServer
	csi.UnimplementedGroupControllerServer

	Client client.Client
	// APIReader reads straight from the API server, bypassing the
	// informer cache. Port allocation needs read-your-writes: the cache
	// can lag a just-created volume, handing its port out twice.
	APIReader client.Reader
	// Nodes yields the storage topology — which nodes hold storage and
	// with which backend — folded from the MiroirNode CRs per RPC, so a
	// chart-applied topology edit takes effect without a restart.
	Nodes nodemap.Source
	// ProvisionTimeout bounds the wait for agents to realize a volume.
	ProvisionTimeout time.Duration
	// OvercommitRatio bounds thin-provisioning overcommit: CreateVolume is
	// refused when a node's provisioned total would exceed
	// capacity×ratio. Zero → defaultOvercommitRatio.
	OvercommitRatio float64
	// FreeSpaceRatio bounds provisioning against a pool's *physical* room:
	// CreateVolume is refused when the request would exceed
	// free×ratio. Zero → defaultFreeSpaceRatio. See poolHeadroom.
	FreeSpaceRatio float64
	// AutoTieBreaker adds a diskless tie-breaker replica to new 2-replica
	// freeze volumes when a spare storage node exists (#70).
	AutoTieBreaker bool
	// RWXEnabled mirrors whether the export reconciler is running (a
	// gateway image is configured). When false, RWX CreateVolume requests
	// are rejected at provision time — otherwise the volume would be
	// created with a spec.export no reconciler ever serves and its
	// consumers would hang on a gateway that never comes.
	RWXEnabled bool
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
	// defaultOvercommitRatio caps provisioned-over-capacity per pool;
	// 2× is the documented default.
	defaultOvercommitRatio = 2.0
	// defaultFreeSpaceRatio caps provisioned-over-physically-free per
	// pool, matching LINSTOR's and BlockStor's 20× thin default. It only
	// binds once a pool is ~90% physically full — below that the 2×
	// virtual bound above is always the tighter of the two. The two are
	// complements: the virtual bound governs a pool filling with
	// provisioned-but-empty volumes, this one a pool whose volumes have
	// actually filled it.
	defaultFreeSpaceRatio = 20.0
	// defaultDRBDPortBase is the lowest DRBD replication port when
	// DRBDPortBase is unset (zero). Ceph mgr dashboard's non-SSL default
	// is also 7000; operators co-locating with Rook host-network Ceph can
	// move this via the --drbd-port-base flag / drbd.portBase Helm value.
	defaultDRBDPortBase = 7000
)

// ControllerGetCapabilities advertises exactly what is implemented.
func (c *Controller) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
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
// Ready. Idempotent by volume name.
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

	params, err := parseClassParams(req.GetParameters())
	if err != nil {
		return nil, err
	}
	replicas, quorum, remoteAccess := params.replicas, params.quorum, params.remoteAccess
	shared := isShared(req.GetVolumeCapabilities())
	if err := validateSharedRequest(c.RWXEnabled, shared, replicas, quorum); err != nil {
		return nil, err
	}

	snapID, cloneSrc, err := c.resolveContentSource(ctx, req, sizeBytes, replicas, params.pool)
	if err != nil {
		return nil, err
	}
	source, srcReplicas, sourceFormatted, err := c.resolveSource(ctx, snapID, sizeBytes, replicas, params.pool)
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
		// One topology snapshot serves both placement and the tie-breaker
		// pick; separate resolves could observe different topologies
		// mid-RPC.
		nodes, err := c.nodes(ctx)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
		p, err := c.place(ctx, nodes, req.GetAccessibilityRequirements(), replicas, sizeBytes, req.GetName(), vols, remoteAccess, params.pool)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
		placed, err = c.withTieBreaker(ctx, nodes, p, replicas, quorum)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
	}
	vol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: req.GetName(),
			Labels: constants.PVCRefLabels(
				req.GetParameters()[paramPVCName],
				req.GetParameters()[paramPVCNamespace]),
		},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: sizeBytes,
			Replicas:  placed,
			Source:    source,
			// Export volumes are consumed over NFS, never through DRBD
			// legs: leaving AllowRemoteAccess unset keeps every client-leg
			// path closed for them (their PV is unpinned via Export
			// already), so a stray device resolution on a consumer node
			// fails loudly instead of attaching a leg.
			AllowRemoteAccess: remoteAccess && !shared,
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
		drbdSpec.BitmapGranularityBytes = params.bitmapGranularity
		vol.Spec.DRBD = drbdSpec
	}
	if shared {
		vol.Spec.Export = &miroirv1alpha1.ExportSpec{FSType: exportFSType(req.GetVolumeCapabilities())}
	}
	createErr := c.Client.Create(ctx, vol)
	c.allocMu.Unlock()

	sourceSnapshot := ""
	if source != nil {
		sourceSnapshot = source.SnapshotName
	}
	if err := c.handleCreateErr(ctx, createErr, vol, sizeBytes, replicas, quorum, sourceSnapshot, params.pool); err != nil {
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

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           vol.Name,
			CapacityBytes:      sizeBytes,
			AccessibleTopology: accessibleTopology(vol),
			ContentSource:      responseContentSource(cloneSrc, source),
		},
	}, nil
}

// responseContentSource echoes the request's content source: the source
// volume for a clone (its internal snapshot is an implementation
// detail), else the snapshot the volume was restored from, else nil.
func responseContentSource(cloneSrc string, source *miroirv1alpha1.VolumeSource) *csi.VolumeContentSource {
	if cloneSrc != "" {
		return &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: cloneSrc},
			},
		}
	}
	if source != nil && source.SnapshotName != "" {
		return &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: source.SnapshotName,
				},
			},
		}
	}
	return nil
}

// DeleteVolume removes the MiroirVolume; agents tear down local state via
// the finalizer before it disappears. Idempotent.
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
	// A clone's internal source snapshot dies with the clone. Its name is
	// deterministic, so no volume read is needed even on an idempotent
	// retry whose volume CR is already gone; the ownership guard keeps a
	// legacy user snapshot that merely wears the prefix (created before
	// the reservation) out of reach. The backends order the on-disk
	// teardown themselves (ZFS defers the snapshot's destruction until
	// the clone zvol goes).
	cloneSnap := &miroirv1alpha1.MiroirSnapshot{}
	err := c.Client.Get(ctx, types.NamespacedName{Name: constants.CloneSnapshotPrefix + req.GetVolumeId()}, cloneSnap)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return nil, status.Errorf(codes.Internal, "get clone-source MiroirSnapshot: %v", err)
	case isCloneSourceSnapshot(cloneSnap):
		if err := c.Client.Delete(ctx, cloneSnap); err != nil && !apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.Internal, "delete clone-source MiroirSnapshot: %v", err)
		}
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// volumeKind is the owner-reference kind stamped on internal
// clone-source snapshots.
const volumeKind = "MiroirVolume"

// isCloneSourceSnapshot reports whether the snapshot is an internal
// clone-source snapshot: the reserved name prefix AND the MiroirVolume
// owner reference stamped at creation. The prefix alone is not proof —
// a user snapshot named clone-<x> can predate the prefix reservation,
// and treating it as internal would delete or hide user data.
func isCloneSourceSnapshot(snap *miroirv1alpha1.MiroirSnapshot) bool {
	if !strings.HasPrefix(snap.Name, constants.CloneSnapshotPrefix) {
		return false
	}
	for _, ref := range snap.OwnerReferences {
		if ref.Kind == volumeKind &&
			strings.HasPrefix(ref.APIVersion, miroirv1alpha1.GroupVersion.Group) {
			return true
		}
	}
	return false
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
	// Multi-node access is only real if the volume was provisioned with an
	// NFS gateway; confirming RWX on an RWO volume (or vice versa) would
	// promise access the mount path cannot deliver.
	if isShared(req.GetVolumeCapabilities()) != (vol.Spec.Export != nil) {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Message: "requested access mode does not match the volume's provisioning",
		}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// resolveSource resolves a restore's source: on a clone (snapID set) it
// validates the snapshot, size, and pool, and returns the placement
// replicas (following the source, FullSyncing post-snapshot legs). Returns
// zero values for a fresh volume (no content source).
func (c *Controller) resolveSource(ctx context.Context, snapID string, sizeBytes int64, replicas int, pool string) (*miroirv1alpha1.VolumeSource, []miroirv1alpha1.Replica, bool, error) {
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
	// CoW clones cannot cross pools: each leg clones its node-local
	// snapshot inside the pool that holds it.
	for _, rep := range srcVol.Spec.DiskfulReplicas() {
		if src := nodemap.PoolOrDefault(rep.Pool); src != pool {
			return nil, nil, false, status.Errorf(codes.InvalidArgument,
				"restore must stay in the source volume's pool %q (CoW clones cannot cross pools); the class requests pool %q",
				src, pool)
		}
	}
	srcReplicas, err := c.restoreReplicas(ctx, srcVol, snap)
	if err != nil {
		return nil, nil, false, err
	}
	return &miroirv1alpha1.VolumeSource{SnapshotName: snapID}, srcReplicas, snap.Status.SourceFormatted, nil
}

// allocVolumes lists every volume once for the allocMu critical section
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
func (c *Controller) place(ctx context.Context, nodes nodemap.Map, reqs *csi.TopologyRequirement, count int, sizeBytes int64, name string, vols []miroirv1alpha1.MiroirVolume, remoteAccess bool, pool string) ([]miroirv1alpha1.Replica, error) {
	// Every diskful leg needs the pool on its own node; a class naming a
	// pool too few nodes carry must say so instead of a generic refusal.
	// Unplaceable nodes (address conflict) are excluded outright — but
	// counted, so the refusal names the real cause instead of blaming the
	// pool declarations.
	candidates := map[string]nodemap.Pool{}
	conflicted := 0
	for n := range nodes {
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
	// capacity ranking below never ran, starving the rotation's tail node
	// (#258). The pinned node is kept even if it later fails the
	// overcommit guard, so a topology-pinned volume refuses rather than
	// silently landing on a node the pod can't reach.
	ordered := make([]string, 0, len(candidates))
	topologyMatched := false
	for _, t := range append(reqs.GetPreferred(), reqs.GetRequisite()...) {
		n, ok := t.GetSegments()[constants.TopologyKey]
		if !ok {
			continue
		}
		if _, ok := candidates[n]; !ok {
			continue
		}
		topologyMatched = true
		ordered = append(ordered, n)
		break
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
// 2-replica freeze volume, so majority quorum survives a single node loss
// (#70). It picks from the same topology snapshot the caller placed with.
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

// replicationAddress resolves a node's replication endpoint — the node
// map's address override, or the node's InternalIP when unset.
func (c *Controller) replicationAddress(ctx context.Context, nodes nodemap.Map, name string) (string, error) {
	addr, err := nodes.ReplicationAddress(ctx, c.Client, name)
	if err != nil {
		return "", status.Errorf(codes.Internal, "%v", err)
	}
	return addr, nil
}

// nodes resolves the current topology from the Source, mapping a resolve
// failure to the gRPC error every caller would otherwise repeat.
func (c *Controller) nodes(ctx context.Context) (nodemap.Map, error) {
	nodes, err := c.Nodes.Map(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return nodes, nil
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
func (c *Controller) handleCreateErr(ctx context.Context, err error, vol *miroirv1alpha1.MiroirVolume, sizeBytes int64, replicas int, quorum miroirv1alpha1.QuorumPolicy, sourceSnapshot, pool string) error {
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
	existingPool := pool
	if first := existing.Spec.FirstDiskfulReplica(); first != nil {
		existingPool = nodemap.PoolOrDefault(first.Pool)
	}
	if existing.Spec.SizeBytes != sizeBytes || existingDiskful != replicas ||
		(replicas > 1 && existing.Spec.QuorumPolicy != quorum) ||
		existingSource != sourceSnapshot || existingPool != pool ||
		(existing.Spec.Export != nil) != (vol.Spec.Export != nil) {
		return status.Errorf(codes.AlreadyExists,
			"volume %s exists with size=%d diskful=%d quorum=%s source=%q pool=%s rwx=%t (requested size=%d replicas=%d quorum=%s source=%q pool=%s rwx=%t)",
			vol.Name, existing.Spec.SizeBytes, existingDiskful, existing.Spec.QuorumPolicy, existingSource, existingPool, existing.Spec.Export != nil,
			sizeBytes, replicas, quorum, sourceSnapshot, pool, vol.Spec.Export != nil)
	}
	*vol = *existing
	return nil
}

// resolveContentSource maps the request's content source onto the
// snapshot the volume restores from: the named snapshot as-is, or for a
// volume clone the internal snapshot this call cuts on the source
// volume (waiting until every leg holds it). cloneSrc is the clone's
// source volume id, "" for snapshot and blank sources.
func (c *Controller) resolveContentSource(ctx context.Context, req *csi.CreateVolumeRequest, sizeBytes int64, replicas int, pool string) (snapID, cloneSrc string, err error) {
	snapID, cloneSrc, err = contentSourceIDs(req)
	if err != nil || cloneSrc == "" {
		return snapID, cloneSrc, err
	}
	snapID, err = c.ensureCloneSnapshot(ctx, cloneSrc, req.GetName(), sizeBytes, replicas, pool)
	return snapID, cloneSrc, err
}

// contentSourceIDs extracts the request's content source ids. An empty
// id or an unrecognized source type is refused: falling through would
// provision a blank volume under the requested name and then block a
// corrected retry on AlreadyExists.
func contentSourceIDs(req *csi.CreateVolumeRequest) (snapID, cloneVolID string, err error) {
	switch src := req.GetVolumeContentSource(); {
	case src == nil:
		return "", "", nil
	case src.GetSnapshot() != nil:
		if src.GetSnapshot().GetSnapshotId() == "" {
			return "", "", status.Error(codes.InvalidArgument, "snapshot content source needs a snapshot id")
		}
		return src.GetSnapshot().GetSnapshotId(), "", nil
	case src.GetVolume() != nil:
		if src.GetVolume().GetVolumeId() == "" {
			return "", "", status.Error(codes.InvalidArgument, "volume content source needs a volume id")
		}
		return "", src.GetVolume().GetVolumeId(), nil
	default:
		return "", "", status.Error(codes.InvalidArgument,
			"unsupported volume content source: only snapshot and volume sources are supported")
	}
}

// ensureCloneSnapshot realizes a volume-clone content source as an
// internal snapshot of the source volume, so the clone rides the whole
// snapshot-restore path (local CoW clone per leg, placement following
// the source). The name is deterministic (clone-<cloneVolumeID>): retries
// converge on one snapshot and DeleteVolume cleans it up by name alone.
// The shape checks run before the cut — a clone whose class can never
// match the source must fail without freezing the source volume for a
// snapshot round it would then leak.
func (c *Controller) ensureCloneSnapshot(ctx context.Context, srcVolID, cloneVolID string, sizeBytes int64, replicas int, pool string) (string, error) {
	snapName := constants.CloneSnapshotPrefix + cloneVolID
	// A taken volume name that is not this clone can only end in
	// handleCreateErr's AlreadyExists — refuse it BEFORE the cut, or the
	// doomed request freezes the source for a snapshot round and leaks
	// its result until the name's owner (or the source) is deleted. A
	// name owned by this same clone proceeds: that is the idempotent
	// retry, and any other parameter mismatch still surfaces through
	// handleCreateErr exactly as before.
	existing := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: cloneVolID}, existing); err == nil {
		if existing.Spec.Source == nil || existing.Spec.Source.SnapshotName != snapName {
			existingSource := ""
			if existing.Spec.Source != nil {
				existingSource = existing.Spec.Source.SnapshotName
			}
			return "", status.Errorf(codes.AlreadyExists,
				"volume %s exists with source %q (requested a clone of volume %s)",
				cloneVolID, existingSource, srcVolID)
		}
	} else if !apierrors.IsNotFound(err) {
		return "", status.Errorf(codes.Internal, "get existing volume: %v", err)
	}

	srcVol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: srcVolID}, srcVol); err != nil {
		if apierrors.IsNotFound(err) {
			return "", status.Errorf(codes.NotFound, "clone source volume %s not found", srcVolID)
		}
		return "", status.Errorf(codes.Internal, "get clone source volume: %v", err)
	}
	if sizeBytes < srcVol.Spec.SizeBytes {
		return "", status.Errorf(codes.InvalidArgument,
			"requested %d below clone source volume size %d", sizeBytes, srcVol.Spec.SizeBytes)
	}
	if got := len(srcVol.Spec.DiskfulReplicas()); got != replicas {
		return "", status.Errorf(codes.InvalidArgument,
			"clone replica count %d must match source diskful replicas %d", replicas, got)
	}
	for _, rep := range srcVol.Spec.DiskfulReplicas() {
		if src := nodemap.PoolOrDefault(rep.Pool); src != pool {
			return "", status.Errorf(codes.InvalidArgument,
				"clone must stay in the source volume's pool %q (CoW clones cannot cross pools); the class requests pool %q",
				src, pool)
		}
	}
	if _, err := c.ensureSnapshot(ctx, snapName, srcVol, true); err != nil {
		return "", err
	}
	if err := c.waitSnapshotReady(ctx, snapName); err != nil {
		return "", err
	}
	return snapName, nil
}

// waitSnapshotReady blocks until the clone-source snapshot is cut on
// every leg. DeadlineExceeded keeps the provisioner retrying: the
// snapshot round continues in the background and the retry finds it
// ready (or still converging).
func (c *Controller) waitSnapshotReady(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, cmp.Or(c.ProvisionTimeout, defaultProvisionTimeout))
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true,
		func(ctx context.Context) (bool, error) {
			snap := &miroirv1alpha1.MiroirSnapshot{}
			if err := c.Client.Get(ctx, types.NamespacedName{Name: name}, snap); err != nil {
				return false, client.IgnoreNotFound(err) // cache lag → retry
			}
			return snap.Status.ReadyToUse, nil
		})
	if err != nil {
		return status.Errorf(codes.DeadlineExceeded, "clone-source snapshot %s not ready: %v", name, err)
	}
	return nil
}

// expandWithinHeadroom applies CreateVolume's capacity guardrails to a
// grow: expansion otherwise bypasses them entirely, and one PVC grown past
// the pool ENOSPCs every thin volume sharing it — surfacing as DRBD I/O
// errors and detached legs, not a clean refusal. Same accounting as
// place(): the volume's own current size is excluded, its projected whole
// size must fit; a node without fresh stats admits. Not under allocMu —
// this is a guardrail against runaway growth, not an exact admission (a
// concurrent CreateVolume can overshoot by one request, the same window
// expansion always had).
func (c *Controller) expandWithinHeadroom(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, newSize int64) error {
	stats, err := c.poolStats(ctx)
	if err != nil {
		return err
	}
	list := &miroirv1alpha1.MiroirVolumeList{}
	if err := c.Client.List(ctx, list); err != nil {
		return status.Errorf(codes.Internal, "list MiroirVolumes: %v", err)
	}
	provisioned := provisionedPerPool(list.Items, vol.Name)
	pool := nodemap.PoolOrDefault("")
	if first := vol.Spec.FirstDiskfulReplica(); first != nil {
		pool = nodemap.PoolOrDefault(first.Pool)
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			continue
		}
		room, known := c.poolHeadroom(stats[rep.Node].Pool(pool), provisioned[nodePool{rep.Node, pool}])
		if !known || newSize <= room {
			continue
		}
		overcommit, freeSpace := c.ratios()
		return status.Errorf(codes.ResourceExhausted,
			"pool %q on node %s has room for %d of the requested %d bytes "+
				"(capacity×%g overcommit, free×%g free-space)",
			pool, rep.Node, room, newSize, overcommit, freeSpace)
	}
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
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}
	// RWX volumes are grown by the gateway (it holds the only mount), and a
	// block volume needs no filesystem grow — neither triggers node expansion.
	nodeExpansion := req.GetVolumeCapability().GetBlock() == nil && vol.Spec.Export == nil
	if newSize > vol.Spec.SizeBytes {
		if err := c.expandWithinHeadroom(ctx, vol, newSize); err != nil {
			return nil, err
		}
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
	if strings.HasPrefix(req.GetName(), constants.CloneSnapshotPrefix) {
		// Reserved so DeleteVolume's blind clone-<volumeID> delete can
		// never hit a user snapshot.
		return nil, status.Errorf(codes.InvalidArgument,
			"snapshot name prefix %q is reserved for clone-source snapshots", constants.CloneSnapshotPrefix)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetSourceVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetSourceVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}
	snap, err := c.ensureSnapshot(ctx, req.GetName(), vol, false)
	if err != nil {
		return nil, err
	}
	// Report the size captured at snapshot time once known; the live
	// volume may have been expanded since.
	size := cmp.Or(snap.Status.SizeBytes, vol.Spec.SizeBytes)
	return &csi.CreateSnapshotResponse{Snapshot: csiSnapshot(snap, size)}, nil
}

// ensureSnapshot creates a MiroirSnapshot of vol, idempotent by name:
// AlreadyExists resolves to the existing object when it captures the
// same volume and is a terminal error otherwise. owned marks an
// internal clone-source snapshot: it must not outlive its source
// volume, and an abandoned provisioning (PVC deleted before
// CreateVolume ever succeeded) never reaches DeleteVolume's cleanup —
// the owner reference has garbage collection reap it with the source.
func (c *Controller) ensureSnapshot(ctx context.Context, name string, vol *miroirv1alpha1.MiroirVolume, owned bool) (*miroirv1alpha1.MiroirSnapshot, error) {
	snap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: vol.Name},
	}
	if owned {
		snap.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(vol, miroirv1alpha1.GroupVersion.WithKind(volumeKind)),
		}
	}
	for _, rep := range vol.Spec.Replicas {
		snap.Finalizers = append(snap.Finalizers, constants.FinalizerPrefix+rep.Node)
	}
	if err := c.Client.Create(ctx, snap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, status.Errorf(codes.Internal, "create MiroirSnapshot: %v", err)
		}
		existing := &miroirv1alpha1.MiroirSnapshot{}
		if err := c.reader().Get(ctx, types.NamespacedName{Name: name}, existing); err != nil {
			// Cache may lag the just-created object; retryable, not terminal.
			return nil, status.Errorf(codes.Unavailable, "get existing snapshot: %v", err)
		}
		if existing.Spec.VolumeName != vol.Name {
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot %s exists for volume %s", name, existing.Spec.VolumeName)
		}
		if owned && !isCloneSourceSnapshot(existing) {
			// A pre-reservation user snapshot occupying the reserved name:
			// adopting it would clone whatever it captured back then
			// instead of cutting fresh source data.
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot %s exists and is not an internal clone-source snapshot", name)
		}
		snap = existing
	}
	return snap, nil
}

// DeleteSnapshot removes the MiroirSnapshot; agents drop the backend
// snapshots via finalizers. Idempotent.
func (c *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot id is required")
	}
	// A group member only dies with its group: deleting one leg of an
	// atomic cut would leave the group claiming a consistency it no
	// longer has.
	existing := &miroirv1alpha1.MiroirSnapshot{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetSnapshotId()}, existing); err == nil {
		if existing.Spec.Group != "" {
			return nil, status.Errorf(codes.FailedPrecondition,
				"snapshot %s belongs to group snapshot %s; delete the group", req.GetSnapshotId(), existing.Spec.Group)
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "get MiroirSnapshot: %v", err)
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
		// Internal clone-source snapshots are an implementation detail of
		// volume clones; listing them would invite the snapshotter to
		// adopt objects whose lifecycle DeleteVolume owns. Legacy user
		// snapshots that merely wear the prefix stay listed.
		if isCloneSourceSnapshot(s) {
			continue
		}
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

// ControllerGetVolume reports one volume's replication health to the
// external-health-monitor, which raises it as an event on the PVC. The
// condition is derived from the same aggregated status the agents publish.
func (c *Controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}
	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           vol.Name,
			CapacityBytes:      vol.Spec.SizeBytes,
			AccessibleTopology: accessibleTopology(vol),
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: volumeCondition(vol),
		},
	}, nil
}

// GetCapacity reports a storage node's provisionable headroom so the
// kube-scheduler (via the external-provisioner's CSIStorageCapacity objects)
// steers a WaitForFirstConsumer pod onto a node whose pool can hold the volume,
// instead of landing there and having CreateVolume refuse it. The number is the
// same headroom place() guards on — both go through poolHeadroom — so the
// scheduler filters a node exactly when placement would reject it.
//
// The provisioner calls this once per (StorageClass, topology segment) with
// the class's parameters, and the agent registers the driver on every node —
// so segments arrive for non-storage nodes too. Those mirror place(): a
// remote-access class (replicated, allowRemoteVolumeAccess not false) serves
// consumers anywhere through a diskless client leg, so a non-storage segment
// answers "can the volume be placed at all" instead of 0, which would wrongly
// filter pods off nodes place() accepts. A storage segment is a node place()
// will pin (and hold to the overcommit guard), remote access or not, so it
// contributes its own headroom. Every diskful replica needs its own node's
// pool, so a replicated class is additionally bounded by the peers' headroom
// (see capacityFor). A node without fresh stats reports zero (the scheduler
// avoids it) until its agent publishes — self-healing within
// poolStatsInterval; place() stays the authority and still allows an
// unknown-stats node if the pod lands there anyway.
func (c *Controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	replicas, err := parseReplicas(req.GetParameters())
	if err != nil {
		return nil, err
	}
	remoteAccess, err := parseAllowRemoteAccess(req.GetParameters(), replicas)
	if err != nil {
		return nil, err
	}
	pool := parsePool(req.GetParameters())
	nodes, err := c.nodes(ctx)
	if err != nil {
		return nil, err
	}
	stats, err := c.poolStats(ctx)
	if err != nil {
		return nil, err
	}
	list := &miroirv1alpha1.MiroirVolumeList{}
	if err := c.reader().List(ctx, list); err != nil {
		return nil, status.Errorf(codes.Internal, "list MiroirVolumes: %v", err)
	}
	provisioned := provisionedPerPool(list.Items, "")
	available := func(node string) int64 {
		if _, ok := nodes.Pool(node, pool); !ok || !nodes.Placeable(node) {
			return 0 // no pool here, or the node is unplaceable (address conflict)
		}
		// Unknown stats fall out as zero — excluded until the agent
		// publishes, where place() would still admit.
		room, _ := c.poolHeadroom(stats[node].Pool(pool), provisioned[nodePool{node, pool}])
		return room
	}

	// capacityFor is the largest volume the class can still place: each of
	// its `replicas` diskful legs needs the class's pool on a distinct
	// node, so the answer is the replicas-th largest per-node headroom — 0
	// when fewer nodes have room, matching place()'s refusal. A pinned
	// scheduler-preferred storage node consumes one leg slot and bounds the
	// answer with its own headroom (place() honors the pin
	// unconditionally); empty means unpinned.
	capacityFor := func(pinned string) int64 {
		need := replicas
		bound := int64(-1) // no bound yet; always set before returning
		if pinned != "" {
			bound = available(pinned)
			need--
		}
		if need == 0 {
			return bound
		}
		peers := make([]int64, 0, len(nodes))
		for node := range nodes {
			if node != pinned {
				peers = append(peers, available(node))
			}
		}
		slices.SortFunc(peers, func(a, b int64) int { return cmp.Compare(b, a) })
		if len(peers) < need {
			return 0
		}
		if bound < 0 || peers[need-1] < bound {
			bound = peers[need-1]
		}
		return bound
	}

	// Topology-aware provisioner: one segment per call. A node that
	// carries the pool but is unplaceable forks like one without the pool:
	// place() never admits it as a candidate, so with remote access the
	// volume lands elsewhere and without it the request refuses.
	if seg := req.GetAccessibleTopology().GetSegments(); seg != nil {
		node := seg[constants.TopologyKey]
		if _, hasPool := nodes.Pool(node, pool); !hasPool || !nodes.Placeable(node) {
			if remoteAccess {
				// Consumers here attach a diskless client leg; the volume
				// lands wherever place() ranks best.
				return &csi.GetCapacityResponse{AvailableCapacity: capacityFor("")}, nil
			}
			return &csi.GetCapacityResponse{}, nil // no placeable pool here, class pinned to replica nodes
		}
		return &csi.GetCapacityResponse{AvailableCapacity: capacityFor(node)}, nil
	}
	// No segment: cluster-wide answer.
	return &csi.GetCapacityResponse{AvailableCapacity: capacityFor("")}, nil
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
		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:           v.Name,
				CapacityBytes:      v.Spec.SizeBytes,
				AccessibleTopology: accessibleTopology(v),
			},
			Status: &csi.ListVolumesResponse_VolumeStatus{
				VolumeCondition: volumeCondition(v),
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
		SnapshotId:      snap.Name,
		SourceVolumeId:  snap.Spec.VolumeName,
		SizeBytes:       sizeBytes,
		CreationTime:    timestamppb.New(snap.CreationTimestamp.Time),
		ReadyToUse:      snap.Status.ReadyToUse,
		GroupSnapshotId: snap.Spec.Group,
	}
}

// restoreReplicas copies the source volume's replica layout for a clone,
// cleaned: FullSync is stripped — every leg clones its byte-identical
// local snapshot, so a carried flag would full-resync one for nothing —
// and replication addresses are re-resolved (node-map override, else the
// live Node's InternalIP; the source's were captured at its creation and
// can be stale).
func (c *Controller) restoreReplicas(ctx context.Context, srcVol *miroirv1alpha1.MiroirVolume, snap *miroirv1alpha1.MiroirSnapshot) ([]miroirv1alpha1.Replica, error) {
	nodes, err := c.nodes(ctx)
	if err != nil {
		return nil, err
	}
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
		addr, err := c.replicationAddress(ctx, nodes, reps[i].Node)
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
	return stage.MarkFormatted(ctx, c.Client, vol)
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

// validateSharedRequest rejects RWX shapes the NFS gateway cannot serve.
// FailedPrecondition (not InvalidArgument) for the disabled case: the
// request is well-formed and provisioning self-heals on the provisioner's
// retry once an operator enables the gateway.
func validateSharedRequest(rwxEnabled, shared bool, replicas int, quorum miroirv1alpha1.QuorumPolicy) error {
	if !shared {
		return nil
	}
	if !rwxEnabled {
		return status.Error(codes.FailedPrecondition,
			"RWX (ReadWriteMany) volumes are disabled: no NFS gateway is configured (set gateway.enabled=true in Helm values)")
	}
	if replicas < 2 {
		return status.Error(codes.InvalidArgument,
			"RWX volumes need at least 2 replicas so the NFS gateway can fail over to a surviving node")
	}
	if quorum == miroirv1alpha1.QuorumLastManStanding {
		return status.Error(codes.InvalidArgument,
			"RWX volumes require freeze quorum: last-man-standing risks two gateways writing during a partition")
	}
	return nil
}

// parseAllowRemoteAccess reads the remote-access StorageClass parameter.
// Absent defaults to allowed on replicated classes (matching linstor-csi);
// set "false" to pin pods to replica nodes. Only replicated volumes can
// serve remote consumers (a diskless leg needs DRBD peers to read from):
// replicas:1 classes default off and reject an explicit "true" rather
// than silently ignoring it.
func parseAllowRemoteAccess(params map[string]string, replicas int) (bool, error) {
	raw := params[constants.ParamAllowRemoteAccess]
	if raw == "" {
		return replicas > 1, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return false, status.Errorf(codes.InvalidArgument,
			"invalid %s=%q (want true | false)", constants.ParamAllowRemoteAccess, raw)
	}
	if enabled && replicas <= 1 {
		return false, status.Errorf(codes.InvalidArgument,
			"%s requires a replicated class (replicas > 1)", constants.ParamAllowRemoteAccess)
	}
	return enabled, nil
}

// The external-provisioner injects these parameters when it runs with
// --extra-create-metadata (the chart always passes it); they identify the
// PVC a CreateVolume serves.
const (
	paramPVCName      = "csi.storage.k8s.io/pvc/name"
	paramPVCNamespace = "csi.storage.k8s.io/pvc/namespace"
)

// classParams is the StorageClass-driven shape of a CreateVolume request.
type classParams struct {
	replicas          int
	quorum            miroirv1alpha1.QuorumPolicy
	remoteAccess      bool
	bitmapGranularity int64
	pool              string
}

// parseClassParams validates the StorageClass parameters as one unit;
// CreateVolume sits at the gocyclo limit, so the per-parameter error
// branches live here.
func parseClassParams(raw map[string]string) (classParams, error) {
	var p classParams
	var err error
	if p.replicas, err = parseReplicas(raw); err != nil {
		return p, err
	}
	if p.quorum, err = parseQuorum(raw); err != nil {
		return p, err
	}
	if p.remoteAccess, err = parseAllowRemoteAccess(raw, p.replicas); err != nil {
		return p, err
	}
	p.pool = parsePool(raw)
	p.bitmapGranularity, err = parseBitmapGranularity(raw, p.replicas)
	return p, err
}

// parsePool reads the StorageClass pool parameter; absent means the
// default pool. Never fails: a pool no node carries surfaces as place()'s
// explicit "exists on N of M nodes" refusal, which also covers typos.
func parsePool(params map[string]string) string {
	return nodemap.PoolOrDefault(params[constants.ParamPool])
}

// parseBitmapGranularity reads the DRBD bitmap block size in bytes; 0
// (absent) leaves DRBD's default. drbdmeta constrains it to a power of two
// in [4k, 1M] — rejected here so a bad class fails the RPC, not create-md
// on the node.
func parseBitmapGranularity(params map[string]string, replicas int) (int64, error) {
	raw := params[constants.ParamBitmapGranularity]
	if raw == "" {
		return 0, nil
	}
	gran, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || gran < 4096 || gran > 1<<20 || gran&(gran-1) != 0 {
		return 0, status.Errorf(codes.InvalidArgument,
			"invalid %s=%q (want a power of two in [4096, 1048576])",
			constants.ParamBitmapGranularity, raw)
	}
	if replicas <= 1 {
		return 0, status.Errorf(codes.InvalidArgument,
			"%s requires a replicated class (replicas > 1)", constants.ParamBitmapGranularity)
	}
	return gran, nil
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
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
			// RWX is served over NFS from a shared filesystem; a block
			// device mounted on two nodes has no such coordination.
			if c.GetBlock() != nil {
				return status.Error(codes.InvalidArgument,
					"multi-node access is filesystem-only; block volumes are single-node")
			}
		default:
			return status.Errorf(codes.InvalidArgument,
				"unsupported access mode %s", c.GetAccessMode().GetMode())
		}
		if c.GetMount() == nil && c.GetBlock() == nil {
			return status.Error(codes.InvalidArgument, "capability must be mount or block")
		}
	}
	return nil
}

// isShared reports whether the request is for an RWX (multi-node) volume.
func isShared(caps []*csi.VolumeCapability) bool {
	for _, c := range caps {
		switch c.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
			return true
		}
	}
	return false
}

// defaultFSType is the filesystem used when a mount capability names none.
const defaultFSType = "ext4"

// exportFSType is the filesystem the gateway formats an RWX volume with,
// taken from the first mount capability (ext4 when unset).
func exportFSType(caps []*csi.VolumeCapability) string {
	for _, c := range caps {
		if fs := c.GetMount().GetFsType(); fs != "" {
			return fs
		}
	}
	return defaultFSType
}

// accessibleTopology reports the nodes a volume can be published from. It
// is nil for an RWX volume: its NFS gateway is reachable cluster-wide, so
// the PV carries no node-affinity constraint and consumers schedule
// anywhere.
func accessibleTopology(vol *miroirv1alpha1.MiroirVolume) []*csi.Topology {
	// Unpinned either way: an RWX volume's NFS gateway is reachable
	// cluster-wide, and a remote-access volume's consumers attach a
	// diskless client leg on whatever node they land on.
	if vol.Spec.Export != nil || vol.Spec.AllowRemoteAccess {
		return nil
	}
	top := make([]*csi.Topology, 0, len(vol.Spec.Replicas))
	for _, r := range vol.Spec.Replicas {
		if r.Diskless {
			continue
		}
		top = append(top, &csi.Topology{
			Segments: map[string]string{constants.TopologyKey: r.Node},
		})
	}
	return top
}
