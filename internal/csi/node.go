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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// DRBDStatus reports this node's live view of a DRBD resource.
type DRBDStatus interface {
	Status(ctx context.Context, name string) (drbd.Status, error)
}

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
	DRBD DRBDStatus
}

// NewNode wires a Node service with the host mount/format tooling.
func NewNode(c client.Client, nodeName string, d DRBDStatus) *Node {
	return &Node{
		Client:   c,
		NodeName: nodeName,
		Mounter:  mount.NewSafeFormatAndMount(mount.New(""), utilexec.New()),
		DRBD:     d,
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
// this node holds a replica with current data.
func (n *Node) devicePath(ctx context.Context, volumeID string) (string, *miroirv1alpha1.MiroirVolume, error) {
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
	if i < 0 {
		if vol.Spec.AllowRemoteAccess {
			// No replica here, but the volume serves remote consumers:
			// attach (or use) an ephemeral diskless client leg.
			return n.clientDevicePath(ctx, vol)
		}
		return "", nil, status.Errorf(codes.FailedPrecondition,
			"volume %s has no replica on node %s", volumeID, n.NodeName)
	}
	if vol.Spec.Replicas[i].Diskless {
		if vol.Spec.AllowRemoteAccess {
			// The tie-breaker's diskless leg serves I/O the same way a
			// client leg does; without PV node affinity the scheduler may
			// legitimately land a pod here.
			return n.disklessDevicePath(ctx, vol)
		}
		return "", nil, status.Errorf(codes.FailedPrecondition,
			"node %s is a diskless tie-breaker; cannot stage volume %s", n.NodeName, volumeID)
	}
	st, ok := vol.Status.PerNode[n.NodeName]
	if !ok || !st.DeviceCreated || st.DevicePath == "" {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s device not ready on node %s", volumeID, n.NodeName)
	}
	// Replicated volumes must not be formatted or mounted before this
	// replica holds current data — mkfs on an Inconsistent secondary
	// would race the initial handshake. Ask the kernel, not the CRD:
	// status lags behind a link flap by a reconcile interval.
	if vol.Spec.DRBD != nil {
		live, err := n.DRBD.Status(ctx, volumeID)
		if err != nil {
			return "", nil, status.Errorf(codes.Unavailable,
				"volume %s DRBD state unreadable on node %s: %v", volumeID, n.NodeName, err)
		}
		if live.SplitBrain {
			return "", nil, status.Errorf(codes.FailedPrecondition,
				"volume %s is split-brain on node %s — manual resolution required", volumeID, n.NodeName)
		}
		if live.DiskState != drbd.DiskUpToDate {
			return "", nil, status.Errorf(codes.Unavailable,
				"volume %s is %s on node %s (want UpToDate)", volumeID, live.DiskState, n.NodeName)
		}
		// Mid-recovery a birth-split volume can pass the live checks: the
		// survivor and tie-breaker reconnect first (quorum restores, the
		// device turns writable) while the losing leg is still divergent. A
		// stage completing in that window latches Activated and closes the
		// auto-recovery that would have healed the loser (issue #144). Hold
		// staging while a split is recorded AND a diskful link is down —
		// only while the volume is still auto-recovery-eligible. The live
		// connectivity corroboration keeps a stale slot from a dead peer
		// from blocking a volume whose data legs are all established, and
		// releases the hold the moment the loser reconnects, ahead of the
		// slots clearing on the next status patch.
		if !vol.Status.Activated && !vol.Status.Formatted &&
			!diskfulPeersLive(vol, n.NodeName, live) {
			for node, rep := range vol.Status.PerNode {
				if rep.SplitBrain {
					return "", nil, status.Errorf(codes.Unavailable,
						"volume %s is recovering from split-brain (reported by node %s)", volumeID, node)
				}
			}
		}
	}
	return st.DevicePath, vol, nil
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
	if !anyUpToDatePeerLive(vol, live) {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s has no reachable UpToDate replica from node %s", vol.Name, n.NodeName)
	}
	return st.DevicePath, vol, nil
}

// anyUpToDatePeerLive reports whether at least one diskful replica is
// connected and UpToDate per the live kernel view — the minimum for a
// diskless leg to serve I/O.
func anyUpToDatePeerLive(vol *miroirv1alpha1.MiroirVolume, live drbd.Status) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless || rep.Address == "" {
			continue
		}
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

// diskfulPeersLive reports whether this node's replication links to every
// diskful peer are established, per the live kernel view. Mirrors the
// agent's diskfulPeersConnected: a diskless tie-breaker's link is excluded
// so its state never gates a data leg (the bug #78 class), and entries the
// membership reconciler has not completed are skipped.
func diskfulPeersLive(vol *miroirv1alpha1.MiroirVolume, self string, live drbd.Status) bool {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if !live.PeerConnected[rep.NodeID] {
			return false
		}
	}
	return true
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
		if err := n.markActivated(ctx, vol); err != nil {
			return nil, status.Errorf(codes.Internal, "record activated flag: %v", err)
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
	if notMnt {
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

		// mkfs-if-blank is allowed exactly once per volume: a blank device on
		// a volume that ever carried a filesystem is data loss (diverged
		// replica, torn clone), and reformatting would silently finish it.
		format, err := n.Mounter.GetDiskFormat(dev)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "probe filesystem on %s: %v", dev, err)
		}
		if format == "" && vol.Status.Formatted {
			return nil, status.Errorf(codes.DataLoss,
				"volume %s was formatted before but %s reads blank — refusing to reformat", req.GetVolumeId(), dev)
		}
		if format != "" {
			// Record before mounting so a clone that arrived with a
			// filesystem is protected from then on.
			if err := n.markFormatted(ctx, vol); err != nil {
				return nil, status.Errorf(codes.Internal, "record formatted flag: %v", err)
			}
		}

		// FormatAndMount formats only when the device has no filesystem —
		// the mkfs-if-blank step of notes/DESIGN.md §4.5.2.
		if err := n.Mounter.FormatAndMount(dev, req.GetStagingTargetPath(), fsType, mountFlags); err != nil {
			return nil, status.Errorf(codes.Internal, "format/mount %s: %v", dev, err)
		}
		if format == "" {
			// First mkfs. A failed patch fails the stage; the retry lands in
			// the format != "" path above and records it then.
			if err := n.markFormatted(ctx, vol); err != nil {
				return nil, status.Errorf(codes.Internal, "record formatted flag: %v", err)
			}
		}
	} else {
		// Already staged: a mounted device carries a filesystem, so a
		// missed Formatted patch from an earlier stage heals here.
		if err := n.markFormatted(ctx, vol); err != nil {
			return nil, status.Errorf(codes.Internal, "record formatted flag: %v", err)
		}
	}

	// Grow-to-fill runs on every stage, not only a fresh mount: a restored
	// clone carries the snapshot's smaller filesystem, and a resize that
	// failed after the mount already succeeded must be retried on the next
	// stage, not skipped by the already-staged fast path. NeedResize is a
	// no-op once the filesystem fills the device.
	resizer := mount.NewResizeFs(n.Mounter.Exec)
	need, err := resizer.NeedResize(dev, req.GetStagingTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check filesystem size on %s: %v", dev, err)
	}
	if need {
		if _, err := resizer.Resize(dev, req.GetStagingTargetPath()); err != nil {
			return nil, status.Errorf(codes.Internal, "grow filesystem on %s: %v", dev, err)
		}
	}
	// Stage fully succeeded (mkfs + mount + grow): the volume now carries a
	// filesystem and a consumer may write. Latch activated only here, never on
	// a stage that failed the write probe or mkfs — a volume that never
	// completed staging holds no data and must stay eligible for split-brain
	// auto-recovery.
	if err := n.markActivated(ctx, vol); err != nil {
		return nil, status.Errorf(codes.Internal, "record activated flag: %v", err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// markFormatted records that the volume carries a filesystem. No-op when
// already recorded.
func (n *Node) markFormatted(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	return markFormatted(ctx, n.Client, vol)
}

// markActivated latches that the volume has been staged for a consumer at
// least once. No-op when already recorded.
func (n *Node) markActivated(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	return markActivated(ctx, n.Client, vol)
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

// NodeUnstageVolume unmounts the staging path. Idempotent.
func (n *Node) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging path are required")
	}
	if err := mount.CleanupMountPoint(req.GetStagingTargetPath(), n.Mounter, true); err != nil {
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
