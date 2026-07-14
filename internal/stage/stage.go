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

// Package stage holds the node-local pipeline that turns a volume's DRBD
// (or backing) device into a mounted, grown filesystem, with the safety
// gates that guard divergent replicas. It is shared by the CSI node
// service and the RWX NFS gateway, which both stage the same device the
// same way. Errors are gRPC status errors so the node service can pass
// them straight through; other callers use their Error() text.
package stage

import (
	"context"
	"os"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	mount "k8s.io/mount-utils"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// DRBDStatus reports a node's live view of a DRBD resource.
type DRBDStatus interface {
	Status(ctx context.Context, name string) (drbd.Status, error)
}

// Deps carries the node-local tooling the staging pipeline needs.
type Deps struct {
	Client   client.Client
	NodeName string
	Mounter  *mount.SafeFormatAndMount
	// DRBD answers from the kernel, not the CRD: status written by the
	// reconciler lags, and staging on a stale UpToDate mounts (or worse,
	// formats) a diverged replica.
	DRBD DRBDStatus
}

// Device resolves the volume's local device from the CRD and verifies
// this node holds a replica with current data.
func Device(ctx context.Context, d Deps, volumeID string) (string, *miroirv1alpha1.MiroirVolume, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := d.Client.Get(ctx, types.NamespacedName{Name: volumeID}, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
		}
		return "", nil, status.Errorf(codes.Unavailable, "volume %s lookup: %v", volumeID, err)
	}
	i := slices.IndexFunc(vol.Spec.Replicas, func(r miroirv1alpha1.Replica) bool {
		return r.Node == d.NodeName
	})
	if i < 0 {
		return "", nil, status.Errorf(codes.FailedPrecondition,
			"volume %s has no replica on node %s", volumeID, d.NodeName)
	}
	if vol.Spec.Replicas[i].Diskless {
		return "", nil, status.Errorf(codes.FailedPrecondition,
			"node %s is a diskless tie-breaker; cannot stage volume %s", d.NodeName, volumeID)
	}
	st, ok := vol.Status.PerNode[d.NodeName]
	if !ok || !st.DeviceCreated || st.DevicePath == "" {
		return "", nil, status.Errorf(codes.Unavailable,
			"volume %s device not ready on node %s", volumeID, d.NodeName)
	}
	// Replicated volumes must not be formatted or mounted before this
	// replica holds current data — mkfs on an Inconsistent secondary
	// would race the initial handshake. Ask the kernel, not the CRD:
	// status lags behind a link flap by a reconcile interval.
	if vol.Spec.DRBD != nil {
		live, err := d.DRBD.Status(ctx, volumeID)
		if err != nil {
			return "", nil, status.Errorf(codes.Unavailable,
				"volume %s DRBD state unreadable on node %s: %v", volumeID, d.NodeName, err)
		}
		if live.SplitBrain {
			return "", nil, status.Errorf(codes.FailedPrecondition,
				"volume %s is split-brain on node %s — manual resolution required", volumeID, d.NodeName)
		}
		if live.DiskState != drbd.DiskUpToDate {
			return "", nil, status.Errorf(codes.Unavailable,
				"volume %s is %s on node %s (want UpToDate)", volumeID, live.DiskState, d.NodeName)
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
			!diskfulPeersLive(vol, d.NodeName, live) {
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

// EnsureFilesystem makes dev usable at target: mkfs-if-blank (exactly once
// per volume), mount, grow-to-fill, and latch Formatted/Activated. It is
// idempotent — a device already mounted at target only heals the flags and
// grows — so both a re-issued NodeStageVolume and a gateway restart land
// here safely.
func EnsureFilesystem(ctx context.Context, d Deps, vol *miroirv1alpha1.MiroirVolume, dev, target, fsType string, flags []string) error {
	notMnt, err := d.Mounter.IsLikelyNotMountPoint(target)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(target, 0o750); err != nil {
			return status.Errorf(codes.Internal, "mkdir staging path: %v", err)
		}
		notMnt = true
	} else if err != nil {
		return status.Errorf(codes.Internal, "inspect staging path: %v", err)
	}
	if notMnt {
		// Open-for-write probe: the first open(2) auto-promotes DRBD, and a
		// refused promotion (peer already Primary) otherwise surfaces as
		// mkfs "Wrong medium type".
		f, err := os.OpenFile(dev, os.O_RDWR, 0)
		if err != nil {
			return status.Errorf(codes.Unavailable,
				"device %s not writable (is the volume in use on another node?): %v", dev, err)
		}
		_ = f.Close()

		// mkfs-if-blank is allowed exactly once per volume: a blank device on
		// a volume that ever carried a filesystem is data loss (diverged
		// replica, torn clone), and reformatting would silently finish it.
		format, err := d.Mounter.GetDiskFormat(dev)
		if err != nil {
			return status.Errorf(codes.Internal, "probe filesystem on %s: %v", dev, err)
		}
		if format == "" && vol.Status.Formatted {
			return status.Errorf(codes.DataLoss,
				"volume %s was formatted before but %s reads blank — refusing to reformat", vol.Name, dev)
		}
		if format != "" {
			// Record before mounting so a clone that arrived with a
			// filesystem is protected from then on.
			if err := MarkFormatted(ctx, d.Client, vol); err != nil {
				return status.Errorf(codes.Internal, "record formatted flag: %v", err)
			}
		}

		// FormatAndMount formats only when the device has no filesystem —
		// the mkfs-if-blank step.
		if err := d.Mounter.FormatAndMount(dev, target, fsType, flags); err != nil {
			return status.Errorf(codes.Internal, "format/mount %s: %v", dev, err)
		}
		if format == "" {
			// First mkfs. A failed patch fails the stage; the retry lands in
			// the format != "" path above and records it then.
			if err := MarkFormatted(ctx, d.Client, vol); err != nil {
				return status.Errorf(codes.Internal, "record formatted flag: %v", err)
			}
		}
	} else {
		// Already staged: a mounted device carries a filesystem, so a
		// missed Formatted patch from an earlier stage heals here.
		if err := MarkFormatted(ctx, d.Client, vol); err != nil {
			return status.Errorf(codes.Internal, "record formatted flag: %v", err)
		}
	}

	// Grow-to-fill runs on every stage, not only a fresh mount: a restored
	// clone carries the snapshot's smaller filesystem, and a resize that
	// failed after the mount already succeeded must be retried on the next
	// stage, not skipped by the already-staged fast path. NeedResize is a
	// no-op once the filesystem fills the device.
	resizer := mount.NewResizeFs(d.Mounter.Exec)
	need, err := resizer.NeedResize(dev, target)
	if err != nil {
		return status.Errorf(codes.Internal, "check filesystem size on %s: %v", dev, err)
	}
	if need {
		if _, err := resizer.Resize(dev, target); err != nil {
			return status.Errorf(codes.Internal, "grow filesystem on %s: %v", dev, err)
		}
	}
	// Stage fully succeeded (mkfs + mount + grow): the volume now carries a
	// filesystem and a consumer may write. Latch activated only here, never on
	// a stage that failed the write probe or mkfs — a volume that never
	// completed staging holds no data and must stay eligible for split-brain
	// auto-recovery.
	if err := MarkActivated(ctx, d.Client, vol); err != nil {
		return status.Errorf(codes.Internal, "record activated flag: %v", err)
	}
	return nil
}

// MarkFormatted flips the Formatted status flag once; shared by the
// controller (clone inheritance) and the staging pipeline (post-mkfs).
func MarkFormatted(ctx context.Context, cl client.Client, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Status.Formatted {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Formatted = true
	return cl.Status().Patch(ctx, vol, client.MergeFrom(base))
}

// MarkActivated latches the Activated status flag once, the first time a
// node stages the volume for a consumer. It gates split-brain auto-recovery
// (see agent VolumeReconciler.recoverSplitBrain): a staged volume may hold
// data, so its divergence is never auto-discarded.
func MarkActivated(ctx context.Context, cl client.Client, vol *miroirv1alpha1.MiroirVolume) error {
	if vol.Status.Activated {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Activated = true
	return cl.Status().Patch(ctx, vol, client.MergeFrom(base))
}
