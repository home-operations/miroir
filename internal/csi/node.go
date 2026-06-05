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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"sigs.k8s.io/controller-runtime/pkg/client"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/drbd"
)

// Node implements csi.NodeServer (notes/DESIGN.md §4.5.2). It looks the volume up
// in the CRD (the source of truth) and stages its node-local device.
type Node struct {
	csi.UnimplementedNodeServer

	Client   client.Client
	NodeName string
	Mounter  *mount.SafeFormatAndMount
}

// NewNode wires a Node service with the host mount/format tooling.
func NewNode(c client.Client, nodeName string) *Node {
	return &Node{
		Client:   c,
		NodeName: nodeName,
		Mounter:  mount.NewSafeFormatAndMount(mount.New(""), utilexec.New()),
	}
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

// devicePath resolves the volume's local device from the CRD and verifies
// this node holds a replica.
func (n *Node) devicePath(ctx context.Context, volumeID string) (string, error) {
	vol := &homefsv1alpha1.HomefsVolume{}
	if err := n.Client.Get(ctx, types.NamespacedName{Name: volumeID}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return "", status.Errorf(codes.NotFound, "volume %s not found", volumeID)
		}
		return "", status.Errorf(codes.Unavailable, "volume %s lookup: %v", volumeID, err)
	}
	if !slices.ContainsFunc(vol.Spec.Replicas, func(r homefsv1alpha1.Replica) bool {
		return r.Node == n.NodeName
	}) {
		return "", status.Errorf(codes.FailedPrecondition,
			"volume %s has no replica on node %s", volumeID, n.NodeName)
	}
	st, ok := vol.Status.PerNode[n.NodeName]
	if !ok || !st.DeviceCreated || st.DevicePath == "" {
		return "", status.Errorf(codes.Unavailable,
			"volume %s device not ready on node %s", volumeID, n.NodeName)
	}
	// Replicated volumes must not be formatted or mounted before this
	// replica holds current data — mkfs on an Inconsistent secondary
	// would race the initial handshake.
	if vol.Spec.DRBD != nil && st.DiskState != drbd.DiskUpToDate {
		return "", status.Errorf(codes.Unavailable,
			"volume %s is %s on node %s (want UpToDate)", volumeID, st.DiskState, n.NodeName)
	}
	if st.SplitBrain {
		return "", status.Errorf(codes.FailedPrecondition,
			"volume %s is split-brain on node %s — manual resolution required", volumeID, n.NodeName)
	}
	return st.DevicePath, nil
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

	dev, err := n.devicePath(ctx, req.GetVolumeId())
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
		return &csi.NodeStageVolumeResponse{}, nil
	}

	notMnt, err := n.Mounter.IsLikelyNotMountPoint(req.GetStagingTargetPath())
	if os.IsNotExist(err) {
		if err := os.MkdirAll(req.GetStagingTargetPath(), 0o750); err != nil {
			return nil, status.Errorf(codes.Internal, "mkdir staging path: %v", err)
		}
		notMnt = true
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect staging path: %v", err)
	}
	if !notMnt {
		return &csi.NodeStageVolumeResponse{}, nil // already staged (idempotent)
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	if fsType == "" {
		fsType = "ext4"
	}
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	// Open-for-write probe: the first open(2) auto-promotes DRBD, and a
	// refused promotion (peer already Primary) otherwise surfaces as
	// mkfs "Wrong medium type".
	f, err := os.OpenFile(dev, os.O_RDWR, 0)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable,
			"device %s not writable (is the volume in use on another node?): %v", dev, err)
	}
	_ = f.Close()

	// FormatAndMount formats only when the device has no filesystem —
	// the mkfs-if-blank step of notes/DESIGN.md §4.5.2.
	if err := n.Mounter.FormatAndMount(dev, req.GetStagingTargetPath(), fsType, mountFlags); err != nil {
		return nil, status.Errorf(codes.Internal, "format/mount %s: %v", dev, err)
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
	dev, err := n.devicePath(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	resizer := mount.NewResizeFs(n.Mounter.Exec)
	if _, err := resizer.Resize(dev, req.GetVolumePath()); err != nil {
		return nil, status.Errorf(codes.Internal, "grow filesystem on %s: %v", dev, err)
	}
	return &csi.NodeExpandVolumeResponse{CapacityBytes: req.GetCapacityRange().GetRequiredBytes()}, nil
}

// NodeUnstageVolume unmounts the staging path. Idempotent.
func (n *Node) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging path are required")
	}
	if err := mount.CleanupMountPoint(req.GetStagingTargetPath(), n.Mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "unstage: %v", err)
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
		dev, err := n.devicePath(ctx, req.GetVolumeId())
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
