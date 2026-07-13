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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
	"github.com/home-operations/miroir/internal/stage"
)

// Node implements csi.NodeServer (notes/DESIGN.md §4.5.2). It looks the volume up
// in the CRD (the source of truth) and stages its node-local device.
type Node struct {
	csi.UnimplementedNodeServer

	Client   client.Client
	NodeName string
	Mounter  *mount.SafeFormatAndMount
	// DRBD answers from the kernel, not the CRD: status written by the
	// reconciler lags, and staging on a stale UpToDate mounts (or worse,
	// formats) a diverged replica.
	DRBD stage.DRBDStatus
}

// NewNode wires a Node service with the host mount/format tooling.
func NewNode(c client.Client, nodeName string, d stage.DRBDStatus) *Node {
	return &Node{
		Client:   c,
		NodeName: nodeName,
		Mounter:  mount.NewSafeFormatAndMount(mount.New(""), utilexec.New()),
		DRBD:     d,
	}
}

// deps bundles the node's tooling for the shared staging pipeline.
func (n *Node) deps() stage.Deps {
	return stage.Deps{Client: n.Client, NodeName: n.NodeName, Mounter: n.Mounter, DRBD: n.DRBD}
}

// NodeGetInfo reports this node's name and topology segment (§6.5).
func (n *Node) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: n.NodeName,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{constants.TopologyKey: n.NodeName},
		},
	}, nil
}

// NodeGetCapabilities advertises staging, expansion and stats.
func (n *Node) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	caps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	}
	resp := &csi.NodeGetCapabilitiesResponse{}
	for _, t := range caps {
		resp.Capabilities = append(resp.Capabilities, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: t},
			},
		})
	}
	return resp, nil
}

// devicePath resolves the volume's local device and gates it against
// divergent replicas (see stage.Device). Kept as a method so the node
// service's call sites and tests read unchanged.
func (n *Node) devicePath(ctx context.Context, volumeID string) (string, *miroirv1alpha1.MiroirVolume, error) {
	// The client-leg decision is deliberately node-service-only: the RWX
	// gateway stages through stage.Device on a replica node and must never
	// attach a client leg when misplaced.
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(ctx, types.NamespacedName{Name: volumeID}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
		}
		return "", nil, status.Errorf(codes.Unavailable, "volume %s lookup: %v", volumeID, err)
	}
	i := slices.IndexFunc(vol.Spec.Replicas, func(r miroirv1alpha1.Replica) bool {
		return r.Node == n.NodeName
	})
	if i < 0 && vol.Spec.AllowRemoteAccess {
		// No replica here, but the volume serves remote consumers: attach
		// (or use) an ephemeral diskless client leg.
		return n.clientDevicePath(ctx, vol)
	}
	if i >= 0 && vol.Spec.Replicas[i].Diskless && vol.Spec.AllowRemoteAccess {
		// The tie-breaker's diskless leg serves I/O the same way a client
		// leg does; without PV node affinity the scheduler may
		// legitimately land a pod here.
		return n.disklessDevicePath(ctx, vol)
	}
	return stage.Device(ctx, n.deps(), volumeID)
}

// clientDevicePath resolves the device for an ephemeral diskless client
// leg on this node, creating the spec entry on first use. The membership
// reconciler completes the entry and the agent realizes it; until then the
// stage returns Unavailable and the CO retries.
func (n *Node) clientDevicePath(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (string, *miroirv1alpha1.MiroirVolume, error) {
	if vol.Spec.ClientForNode(n.NodeName) == nil {
		if err := n.addClientLeg(ctx, vol); err != nil {
			return "", nil, err
		}
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s: attaching diskless client leg on node %s", vol.Name, n.NodeName)
	}
	return n.disklessDevicePath(ctx, vol)
}

// disklessDevicePath verifies a local diskless leg (client or tie-breaker)
// can serve I/O: the leg is realized, the volume has quorum, and at least
// one diskful peer with current data is reachable — all reads and writes
// cross the replication network to it.
func (n *Node) disklessDevicePath(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (string, *miroirv1alpha1.MiroirVolume, error) {
	st, ok := vol.Status.PerNode[n.NodeName]
	if !ok || st.DevicePath == "" {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s diskless leg not realized on node %s", vol.Name, n.NodeName)
	}
	live, err := n.DRBD.Status(ctx, vol.Name)
	if err != nil {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s DRBD state unreadable on node %s: %v", vol.Name, n.NodeName, err)
	}
	if live.SplitBrain {
		return "", nil, status.Errorf(codes.FailedPrecondition,
			"volume %s is split-brain on node %s — manual resolution required", vol.Name, n.NodeName)
	}
	if !live.Quorum {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s has no quorum on node %s", vol.Name, n.NodeName)
	}
	if !anyUpToDatePeerLive(vol, n.NodeName, live) {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s has no reachable UpToDate replica from node %s", vol.Name, n.NodeName)
	}
	return st.DevicePath, vol, nil
}

// diskfulPeerReplicas yields the completed diskful replicas excluding
// self — the one peers-walk both live gates below share, so their skip
// rules (diskless excluded per the bug #78 class, incomplete membership
// entries skipped) cannot drift apart.
func diskfulPeerReplicas(vol *miroirv1alpha1.MiroirVolume, self string) []miroirv1alpha1.Replica {
	out := make([]miroirv1alpha1.Replica, 0, len(vol.Spec.Replicas))
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		out = append(out, rep)
	}
	return out
}

// anyUpToDatePeerLive reports whether at least one diskful replica is
// connected and UpToDate per the live kernel view — the minimum for a
// diskless leg to serve I/O.
func anyUpToDatePeerLive(vol *miroirv1alpha1.MiroirVolume, self string, live drbd.Status) bool {
	for _, rep := range diskfulPeerReplicas(vol, self) {
		if live.PeerConnected[rep.NodeID] && live.PeerDiskState[rep.NodeID] == drbd.DiskUpToDate {
			return true
		}
	}
	return false
}

// addClientLeg appends a bare client entry for this node; membership
// completes it (node-id, address, finalizer) and the local agent realizes
// the diskless leg.
func (n *Node) addClientLeg(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Spec.DRBD == nil {
		return status.Errorf(codes.FailedPrecondition,
			"volume %s is unreplicated; it cannot serve remote consumers", vol.Name)
	}
	if len(vol.Spec.Clients) >= 2 {
		// MaxItems=2: one consumer plus a pod-move overlap. A third means a
		// stale leg (e.g. a lost node that never unstaged) needs removal.
		return status.Errorf(codes.ResourceExhausted,
			"volume %s already has %d client legs (%v) — remove a stale one to attach on %s",
			vol.Name, len(vol.Spec.Clients), clientNodes(vol), n.NodeName)
	}
	now := metav1.Now()
	vol.Spec.Clients = append(vol.Spec.Clients, miroirv1alpha1.VolumeClient{Node: n.NodeName, AddedAt: &now})
	if err := n.Client.Update(ctx, vol); err != nil {
		return status.Errorf(codes.Unavailable, "add client leg for %s on %s: %v", vol.Name, n.NodeName, err)
	}
	return nil
}

func clientNodes(vol *miroirv1alpha1.MiroirVolume) []string {
	nodes := make([]string, 0, len(vol.Spec.Clients))
	for _, cl := range vol.Spec.Clients {
		nodes = append(nodes, cl.Node)
	}
	return nodes
}

// removeClientLeg drops this node's client leg after unstage; the agent
// tears the local DRBD leg down via the removal path and releases the
// finalizer. No-op when the node holds no client leg or the volume is
// already gone.
func (n *Node) removeClientLeg(ctx context.Context, volumeID string) error {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(ctx, types.NamespacedName{Name: volumeID}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return status.Errorf(codes.Unavailable, "volume %s lookup: %v", volumeID, err)
	}
	i := slices.IndexFunc(vol.Spec.Clients, func(c miroirv1alpha1.VolumeClient) bool {
		return c.Node == n.NodeName
	})
	if i < 0 {
		return nil
	}
	vol.Spec.Clients = slices.Delete(vol.Spec.Clients, i, i+1)
	if err := n.Client.Update(ctx, vol); err != nil && !apierrors.IsNotFound(err) {
		// Conflict or transient API failure: the CO retries NodeUnstage.
		return status.Errorf(codes.Unavailable, "remove client leg for %s on %s: %v", volumeID, n.NodeName, err)
	}
	return nil
}

// NodeStageVolume makes the device usable at the staging path: filesystem
// volumes get mkfs-if-blank + mount; block volumes only need the device to
// exist (publish bind-mounts it directly).
func (n *Node) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging path are required")
	}
	if err := validateCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}

	// RWX volumes are served over NFS by a gateway pod; the device lives on
	// the gateway's node, not here, so this path never touches it.
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Unavailable, "volume %s lookup: %v", req.GetVolumeId(), err)
	}
	if vol.Spec.Export != nil {
		return n.stageNFS(req, vol)
	}

	dev, vol, err := n.devicePath(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	if req.GetVolumeCapability().GetBlock() != nil {
		// Nothing to mount for raw block, but the device node must exist —
		// an LV present in metadata yet not activated would otherwise fail
		// later at publish with a confusing ENOENT.
		if _, err := os.Stat(dev); err != nil {
			return nil, status.Errorf(codes.Unavailable, "block device %s not ready: %v", dev, err)
		}
		// Stage succeeded: publish will hand the device to a consumer that may
		// write. Latch activated so split-brain auto-recovery, which discards a
		// leg, no longer touches this volume.
		if err := stage.MarkActivated(ctx, n.Client, vol); err != nil {
			return nil, status.Errorf(codes.Internal, "record activated flag: %v", err)
		}
		return &csi.NodeStageVolumeResponse{}, nil
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	if fsType == "" {
		fsType = defaultFSType
	}
	flags := req.GetVolumeCapability().GetMount().GetMountFlags()
	if err := stage.EnsureFilesystem(ctx, n.deps(), vol, dev, req.GetStagingTargetPath(), fsType, flags); err != nil {
		return nil, err
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// stageNFS mounts an RWX volume's NFS export at the staging path. The
// gateway pod owns the device, the filesystem, and the Formatted/Activated
// latches, so a consumer node only mounts.
func (n *Node) stageNFS(req *csi.NodeStageVolumeRequest, vol *miroirv1alpha1.MiroirVolume) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "RWX volumes are filesystem-only")
	}
	if vol.Status.Export == nil || vol.Status.Export.Address == "" {
		return nil, status.Errorf(codes.Unavailable, "volume %s NFS gateway not ready", vol.Name)
	}

	target := req.GetStagingTargetPath()
	notMnt, err := n.Mounter.IsLikelyNotMountPoint(target)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(target, 0o750); err != nil {
			return nil, status.Errorf(codes.Internal, "mkdir staging path: %v", err)
		}
		notMnt = true
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect staging path: %v", err)
	}
	if !notMnt {
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// hard: a gateway failover must stall I/O, never surface EIO into the
	// app. noresvport: reconnect from a fresh source port so the mount
	// survives the Service endpoint moving to the replacement gateway pod.
	opts := append([]string{"vers=4.1", "hard", "noresvport", "timeo=600", "retrans=5"},
		req.GetVolumeCapability().GetMount().GetMountFlags()...)
	source := fmt.Sprintf("%s:/%s", vol.Status.Export.Address, vol.Name)
	if err := n.Mounter.Mount(source, target, "nfs4", opts); err != nil {
		return nil, status.Errorf(codes.Unavailable, "mount %s at %s: %v", source, target, err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeExpandVolume grows the filesystem to the (already grown) device,
// online. Raw block volumes need nothing.
func (n *Node) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and path are required")
	}
	if req.GetVolumeCapability().GetBlock() != nil {
		return &csi.NodeExpandVolumeResponse{}, nil
	}
	dev, _, err := n.devicePath(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	resizer := mount.NewResizeFs(n.Mounter.Exec)
	if _, err := resizer.Resize(dev, req.GetVolumePath()); err != nil {
		return nil, status.Errorf(codes.Internal, "grow filesystem on %s: %v", dev, err)
	}
	return &csi.NodeExpandVolumeResponse{CapacityBytes: req.GetCapacityRange().GetRequiredBytes()}, nil
}

// nfsUnmountTimeout bounds a forced NFS unmount so a dead gateway cannot
// wedge NodeUnstageVolume indefinitely.
const nfsUnmountTimeout = 30 * time.Second

// NodeUnstageVolume unmounts the staging path. Idempotent. An export
// volume's staging mount is NFS: if its gateway has died a plain unmount
// blocks, so force it with a deadline. The volume lookup is best-effort —
// a volume already deleted falls back to the normal cleanup.
func (n *Node) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging path are required")
	}
	target := req.GetStagingTargetPath()

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(ctx, types.NamespacedName{Name: req.GetVolumeId()}, vol); err == nil && vol.Spec.Export != nil {
		if forcer, ok := n.Mounter.Interface.(mount.MounterForceUnmounter); ok {
			if err := mount.CleanupMountWithForce(target, forcer, true, nfsUnmountTimeout); err != nil {
				return nil, status.Errorf(codes.Internal, "unstage: %v", err)
			}
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
	}
	if err := mount.CleanupMountPoint(target, n.Mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "unstage: %v", err)
	}
	// A client leg follows its consumer: with the device released, drop the
	// spec entry so peers stop dialing it and the local agent tears it down.
	if err := n.removeClientLeg(ctx, req.GetVolumeId()); err != nil {
		return nil, err
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staged volume (or the raw device) into
// the pod's target path.
func (n *Node) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	if err := validateCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}

	options := []string{"bind"}
	if req.GetReadonly() {
		options = append(options, "ro")
	}

	var source string
	if req.GetVolumeCapability().GetBlock() != nil {
		dev, _, err := n.devicePath(ctx, req.GetVolumeId())
		if err != nil {
			return nil, err
		}
		source = dev
		// Bind target for a block device is a file, not a directory.
		if err := os.MkdirAll(filepath.Dir(req.GetTargetPath()), 0o750); err != nil {
			return nil, status.Errorf(codes.Internal, "mkdir target dir: %v", err)
		}
		f, err := os.OpenFile(req.GetTargetPath(), os.O_CREATE, 0o640)
		if err != nil && !os.IsExist(err) {
			return nil, status.Errorf(codes.Internal, "create target file: %v", err)
		}
		if f != nil {
			_ = f.Close()
		}
	} else {
		if req.GetStagingTargetPath() == "" {
			return nil, status.Error(codes.InvalidArgument, "staging path is required for mount volumes")
		}
		source = req.GetStagingTargetPath()
		if err := os.MkdirAll(req.GetTargetPath(), 0o750); err != nil {
			return nil, status.Errorf(codes.Internal, "mkdir target path: %v", err)
		}
	}

	mounted, err := n.Mounter.IsMountPoint(req.GetTargetPath())
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "inspect target path: %v", err)
	}
	if mounted {
		return &csi.NodePublishVolumeResponse{}, nil // idempotent
	}
	if err := n.Mounter.Mount(source, req.GetTargetPath(), "", options); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount: %v", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume removes the pod bind mount. Idempotent.
func (n *Node) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	if err := mount.CleanupMountPoint(req.GetTargetPath(), n.Mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "unpublish: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats reports capacity on a published volume via statfs.
func (n *Node) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and path are required")
	}
	// A raw-block publish path is a bind-mounted device file: statfs
	// there reports the host filesystem backing the target dir, not the
	// volume. No filesystem, no usage to report.
	if fi, err := os.Stat(req.GetVolumePath()); err == nil && !fi.IsDir() {
		return &csi.NodeGetVolumeStatsResponse{}, nil
	}
	stats, err := statfsAt(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "volume stats: %v", err)
	}
	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{Unit: csi.VolumeUsage_BYTES, Total: stats.total, Used: stats.used, Available: stats.available},
			{Unit: csi.VolumeUsage_INODES, Total: stats.inodes, Used: stats.inodesUsed, Available: stats.inodesAvail},
		},
	}, nil
}

type fsStatResult struct {
	total, used, available          int64
	inodes, inodesUsed, inodesAvail int64
}

// statfsAt wraps unix.Statfs — no shelling out.
func statfsAt(path string) (fsStatResult, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return fsStatResult{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	// Fragment and block sizes are in units defined by the filesystem;
	// the kernel returns them as int64 and the math is straight.
	bsize := st.Bsize
	total := int64(st.Blocks) * bsize
	free := int64(st.Bavail) * bsize // Bavail: blocks free to non-root
	used := total - int64(st.Bfree)*bsize
	return fsStatResult{
		total:       total,
		used:        used,
		available:   free,
		inodes:      int64(st.Files),
		inodesAvail: int64(st.Ffree),
		inodesUsed:  int64(st.Files) - int64(st.Ffree),
	}, nil
}
