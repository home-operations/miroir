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
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/nodemap"
)

// Controller implements csi.ControllerServer (notes/DESIGN.md §6.1). It translates
// CSI RPCs into HomefsVolume objects and waits for node agents to realize
// them — the Kubernetes API is the only channel to the data plane (§4.2).
type Controller struct {
	csi.UnimplementedControllerServer

	Client client.Client
	// Nodes is the storage topology from the Helm-rendered node map —
	// which nodes hold storage and with which backend.
	Nodes nodemap.Map
	// ProvisionTimeout bounds the wait for agents to realize a volume.
	ProvisionTimeout time.Duration

	// allocMu serialises allocateDRBD+Create: CreateVolume RPCs run
	// concurrently within the single controller pod, and two interleaved
	// List→Create spans would hand out the same minor/port.
	allocMu sync.Mutex
}

const defaultProvisionTimeout = 60 * time.Second

// ControllerGetCapabilities advertises exactly what is implemented (M1:
// provisioning only; snapshots and expansion arrive in M3/M4).
func (c *Controller) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
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

// CreateVolume provisions a HomefsVolume and waits until its agents report
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

	placed, err := c.place(ctx, req.GetAccessibilityRequirements(), replicas)
	if err != nil {
		return nil, err
	}

	vol := &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: req.GetName(),
		},
		Spec: homefsv1alpha1.HomefsVolumeSpec{
			SizeBytes: sizeBytes,
			Replicas:  placed,
		},
	}
	for _, r := range placed {
		vol.Finalizers = append(vol.Finalizers, constants.FinalizerPrefix+r.Node)
	}
	if replicas > 1 {
		vol.Spec.QuorumPolicy = quorum
		c.allocMu.Lock()
		drbdSpec, err := c.allocateDRBD(ctx)
		if err != nil {
			c.allocMu.Unlock()
			return nil, err
		}
		vol.Spec.DRBD = drbdSpec
		err = c.Client.Create(ctx, vol)
		c.allocMu.Unlock()
		if err2 := c.handleCreateErr(ctx, err, vol, sizeBytes, replicas, quorum); err2 != nil {
			return nil, err2
		}
	} else if err := c.Client.Create(ctx, vol); err != nil {
		if err2 := c.handleCreateErr(ctx, err, vol, sizeBytes, replicas, quorum); err2 != nil {
			return nil, err2
		}
	}
	if err := c.waitReady(ctx, vol.Name); err != nil {
		return nil, err
	}

	topology := make([]*csi.Topology, 0, len(vol.Spec.Replicas))
	for _, r := range vol.Spec.Replicas {
		topology = append(topology, &csi.Topology{
			Segments: map[string]string{constants.TopologyKey: r.Node},
		})
	}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           vol.Name,
			CapacityBytes:      sizeBytes,
			AccessibleTopology: topology,
		},
	}, nil
}

// DeleteVolume removes the HomefsVolume; agents tear down local state via
// the finalizer before it disappears (notes/DESIGN.md §4.5.7). Idempotent.
func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	vol := &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{Name: req.GetVolumeId()},
	}
	if err := c.Client.Delete(ctx, vol); err != nil && !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "delete HomefsVolume: %v", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities confirms RWO/RWOP mount and block support.
func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	vol := &homefsv1alpha1.HomefsVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "get HomefsVolume: %v", err)
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil //nolint:nilerr // spec: unsupported caps are a non-error response
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// place selects count replica nodes: the scheduler's preference first
// (WaitForFirstConsumer), then the remaining storage nodes by name
// (capacity-aware spread is future work). For replicated volumes it
// resolves each node's InternalIP and assigns DRBD node ids by slice
// position — replicas[0] is the GI-seed winner (internal/drbd).
func (c *Controller) place(ctx context.Context, reqs *csi.TopologyRequirement, count int) ([]homefsv1alpha1.Replica, error) {
	if len(c.Nodes) < count {
		return nil, status.Errorf(codes.ResourceExhausted,
			"need %d storage nodes, have %d (Helm values: nodes)", count, len(c.Nodes))
	}

	ordered := make([]string, 0, len(c.Nodes))
	// Scheduler-selected topology first.
	for _, t := range append(reqs.GetPreferred(), reqs.GetRequisite()...) {
		if name, ok := t.GetSegments()[constants.TopologyKey]; ok {
			if _, ok := c.Nodes[name]; ok && !slices.Contains(ordered, name) {
				ordered = append(ordered, name)
			}
		}
	}
	if reqs != nil && len(reqs.GetRequisite()) > 0 && len(ordered) == 0 {
		return nil, status.Error(codes.ResourceExhausted,
			"no storage node satisfies the requested topology")
	}
	for _, name := range slices.Sorted(maps.Keys(c.Nodes)) {
		if !slices.Contains(ordered, name) {
			ordered = append(ordered, name)
		}
	}
	ordered = ordered[:count]

	replicas := make([]homefsv1alpha1.Replica, 0, count)
	for i, name := range ordered {
		r := homefsv1alpha1.Replica{Node: name, Backend: c.Nodes[name].Backend}
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

// allocateDRBD picks the lowest free minor and TCP port by scanning
// existing volumes. Safe without locking: the controller is a single
// replica with Recreate strategy.
func (c *Controller) allocateDRBD(ctx context.Context) (*homefsv1alpha1.DRBDSpec, error) {
	const (
		minorBase = 1000
		portBase  = 7000
	)
	vols := &homefsv1alpha1.HomefsVolumeList{}
	if err := c.Client.List(ctx, vols); err != nil {
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	usedMinor := map[int32]bool{}
	usedPort := map[int32]bool{}
	for _, v := range vols.Items {
		if v.Spec.DRBD != nil {
			usedMinor[v.Spec.DRBD.Minor] = true
			usedPort[v.Spec.DRBD.Port] = true
		}
	}
	spec := &homefsv1alpha1.DRBDSpec{Minor: minorBase, Port: portBase}
	for usedMinor[spec.Minor] {
		spec.Minor++
	}
	for usedPort[spec.Port] {
		spec.Port++
	}
	return spec, nil
}

// handleCreateErr resolves Create conflicts: nil for success, nil after a
// compatible AlreadyExists (mutating vol to the existing object), and a
// gRPC error otherwise. Idempotency: same name must mean same request.
func (c *Controller) handleCreateErr(ctx context.Context, err error, vol *homefsv1alpha1.HomefsVolume, sizeBytes int64, replicas int, quorum homefsv1alpha1.QuorumPolicy) error {
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return status.Errorf(codes.Internal, "create HomefsVolume: %v", err)
	}
	existing := &homefsv1alpha1.HomefsVolume{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: vol.Name}, existing); err != nil {
		return status.Errorf(codes.Internal, "get existing HomefsVolume: %v", err)
	}
	if existing.Spec.SizeBytes != sizeBytes || len(existing.Spec.Replicas) != replicas ||
		(replicas > 1 && existing.Spec.QuorumPolicy != quorum) {
		return status.Errorf(codes.AlreadyExists,
			"volume %s exists with size=%d replicas=%d quorum=%s (requested size=%d replicas=%d quorum=%s)",
			vol.Name, existing.Spec.SizeBytes, len(existing.Spec.Replicas), existing.Spec.QuorumPolicy,
			sizeBytes, replicas, quorum)
	}
	*vol = *existing
	return nil
}

// errVolumeFailed marks a hard provisioning failure reported by an agent,
// as opposed to "not ready yet".
type errVolumeFailed struct{ detail string }

func (e *errVolumeFailed) Error() string { return e.detail }

// waitReady polls the volume status until agents report Ready.
func (c *Controller) waitReady(ctx context.Context, name string) error {
	timeout := c.ProvisionTimeout
	if timeout == 0 {
		timeout = defaultProvisionTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true,
		func(ctx context.Context) (bool, error) {
			vol := &homefsv1alpha1.HomefsVolume{}
			if err := c.Client.Get(ctx, types.NamespacedName{Name: name}, vol); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil // informer cache not warm yet; retry
				}
				return false, err
			}
			switch vol.Status.Phase {
			case homefsv1alpha1.VolumeReady:
				return true, nil
			case homefsv1alpha1.VolumeFailed:
				return false, &errVolumeFailed{detail: fmt.Sprintf("%+v", vol.Status.PerNode)}
			default:
				return false, nil
			}
		})
	if err == nil {
		return nil
	}
	failed := &errVolumeFailed{}
	if errors.As(err, &failed) {
		// Hard agent failure (e.g. pool out of space). DeadlineExceeded
		// would make the provisioner retry forever.
		return status.Errorf(codes.ResourceExhausted, "volume %s failed: %s", name, failed.detail)
	}
	// Genuine timeout: the provisioner retries CreateVolume and finds the
	// existing CR, so the CR is deliberately left in place.
	return status.Errorf(codes.DeadlineExceeded, "volume %s not ready: %v", name, err)
}

func parseReplicas(params map[string]string) (int, error) {
	raw, ok := params[constants.ParamReplicas]
	if !ok {
		return 1, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 3 {
		return 0, status.Errorf(codes.InvalidArgument,
			"invalid %s=%q (want 1..3)", constants.ParamReplicas, raw)
	}
	return n, nil
}

func parseQuorum(params map[string]string) (homefsv1alpha1.QuorumPolicy, error) {
	switch raw := params[constants.ParamQuorum]; raw {
	case "", string(homefsv1alpha1.QuorumLastManStanding):
		return homefsv1alpha1.QuorumLastManStanding, nil
	case string(homefsv1alpha1.QuorumFreeze):
		return homefsv1alpha1.QuorumFreeze, nil
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
				"unsupported access mode %s (homefs is RWO/RWOP only)",
				c.GetAccessMode().GetMode())
		}
		if c.GetMount() == nil && c.GetBlock() == nil {
			return status.Error(codes.InvalidArgument, "capability must be mount or block")
		}
	}
	return nil
}
