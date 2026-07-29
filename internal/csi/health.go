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
	"fmt"
	"slices"

	"github.com/container-storage-interface/spec/lib/go/csi"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// Reasons a volume is reported unhealthy. The CSI spec has COs match on
// these strings, so they are part of the driver's contract: rename one and
// anything keyed off it stops firing.
const (
	reasonSplitBrain           = "SplitBrain"
	reasonSplitBrainRecovering = "SplitBrainRecovering"
	reasonBackingDiskFailed    = "BackingDiskFailed"
	reasonProvisioningFailed   = "ProvisioningFailed"
	reasonBackingDeviceLost    = "BackingDeviceLost"
	reasonReplicaUnrealized    = "ReplicaUnrealized"
	reasonReplicaOutOfSync     = "ReplicaOutOfSync"
	reasonNoQuorum             = "NoQuorum"
)

// volumeHealth maps a volume's aggregated CRD status (the same signals the
// agent folds into Phase and the miroir_volume_* gauges) to a CSI
// VolumeHealth, so operators see split-brain/degraded without scraping
// Prometheus. This is the cluster-wide view, every condition on every node,
// which is what ControllerGetVolumeHealth and ControllerListVolumeHealth are
// asked for; NodeGetVolumeHealth answers with nodeVolumeHealth instead.
func volumeHealth(vol *miroirv1alpha1.MiroirVolume) *csi.VolumeHealth {
	carried := carriedData(vol)
	var entries []*csi.VolumeHealth_VolumeHealthEntry
	if node, ok := firstNodeWhere(vol, func(s miroirv1alpha1.ReplicaStatus) bool { return s.SplitBrain }); ok {
		entries = append(entries, splitBrainEntry(node, carried))
	}
	if node, ok := firstNodeWhere(vol, func(s miroirv1alpha1.ReplicaStatus) bool { return s.DiskFailed }); ok {
		entries = append(entries, diskFailedEntry(node))
	}
	return healthOf(vol, append(entries, phaseHealth(vol, carried)))
}

// nodeVolumeHealth is volumeHealth from one node's perspective, which is what
// NodeGetVolumeHealth is asked for. A fault latched on a peer is that node's
// to report, so only this node's replica status feeds the per-replica signals.
// What this node adds instead is the live kernel view: quorum is node-local
// and absent from the CRD, and a diskless client or tie-breaker leg feeds
// nothing into Phase, so a volume reading Ready cluster-wide can still refuse
// to stage here (see disklessDevicePath). live is read only when liveOK: the
// leg exists here and drbdsetup answered.
func nodeVolumeHealth(vol *miroirv1alpha1.MiroirVolume, node string, live drbd.Status, liveOK bool) *csi.VolumeHealth {
	carried := carriedData(vol)
	st := vol.Status.PerNode[node]
	var entries []*csi.VolumeHealth_VolumeHealthEntry
	if st.SplitBrain || (liveOK && live.SplitBrain) {
		entries = append(entries, splitBrainEntry(node, carried))
	}
	if st.DiskFailed {
		entries = append(entries, diskFailedEntry(node))
	}
	if liveOK && !live.Quorum {
		entries = append(entries, entryf(csi.VolumeHealthErrorType_INACCESSIBLE, reasonNoQuorum,
			"no quorum on node %s; DRBD is suspending I/O until a replica returns", node))
	}
	return healthOf(vol, append(entries, phaseHealth(vol, carried)))
}

// carriedData latches "this volume may hold data" off the same pair
// split-brain auto-recovery keys on (see recoverSplitBrain). It separates a
// volume that never provisioned from one that provisioned, served, and later
// lost legs: the phase is identical, the operator's action is not.
func carriedData(vol *miroirv1alpha1.MiroirVolume) bool {
	return vol.Status.Activated || vol.Status.Formatted
}

// splitBrainEntry reports legs DRBD refused to reconnect after detecting
// divergent data. Once the volume has carried data an operator must pick the
// loser, whose writes are lost; before that the reconciler discards one
// generation itself, so the window is visible but self-healing; calling it
// DATA_LOSS would send a CO restoring a snapshot of a volume with nothing in
// it yet.
func splitBrainEntry(node string, carried bool) *csi.VolumeHealth_VolumeHealthEntry {
	if !carried {
		return entryf(csi.VolumeHealthErrorType_DEGRADED, reasonSplitBrainRecovering,
			"split-brain on node %s; the volume has never carried data, so the agent resolves it automatically", node)
	}
	return entryf(csi.VolumeHealthErrorType_DATA_LOSS, reasonSplitBrain,
		"split-brain on node %s; manual resolution required", node)
}

// diskFailedEntry reports a backing disk DRBD detached on I/O error
// (on-io-error detach): serving continues via the peer, but this leg is
// latched failed and redundancy is gone until the disk is replaced.
func diskFailedEntry(node string) *csi.VolumeHealth_VolumeHealthEntry {
	return entryf(csi.VolumeHealthErrorType_DEGRADED, reasonBackingDiskFailed,
		"backing disk failed on node %s; replace the disk, then remove and re-add the replica", node)
}

// phaseHealth maps the cluster-wide phase to the condition it carries, or nil
// for none. Creating and Failed both mean "a diskful leg has no backing
// device", which is a provisioning verdict on a fresh volume and a regression
// on one that has already served (issue #88's stale-backing sweep clears
// DeviceCreated on an established leg): carriedData tells those apart, and
// whether any leg still holds its device decides between lost access and lost
// redundancy.
func phaseHealth(vol *miroirv1alpha1.MiroirVolume, carried bool) *csi.VolumeHealth_VolumeHealthEntry {
	switch vol.Status.Phase {
	case miroirv1alpha1.VolumeDegraded:
		return entryf(csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync,
			"degraded: a replica is missing current data (resyncing or its peer is unreachable)")
	case miroirv1alpha1.VolumeFailed:
		// Always adverse; which condition it is falls out below.
	case miroirv1alpha1.VolumeCreating, "":
		// The expected state while a fresh volume provisions, and the empty
		// phase of one no agent has reconciled yet. Adverse only once the
		// volume has carried data, where it means a leg lost the device it
		// already had.
		if !carried {
			return nil
		}
	default:
		return nil
	}
	if anyBackingDevice(vol) {
		// A leg still holds its device, so a peer keeps serving: redundancy
		// is reduced, access is not lost.
		return entryf(csi.VolumeHealthErrorType_DEGRADED, reasonReplicaUnrealized,
			"a replica's backing device could not be created; the volume is serving from its remaining legs")
	}
	if carried {
		return entryf(csi.VolumeHealthErrorType_INACCESSIBLE, reasonBackingDeviceLost,
			"no diskful replica has a backing device left; the volume cannot be staged")
	}
	return entryf(csi.VolumeHealthErrorType_INACCESSIBLE, reasonProvisioningFailed,
		"provisioning failed; a backing device never materialized")
}

// anyBackingDevice reports whether any diskful replica still holds its backing
// device, i.e. whether the volume can be served at all. Phase cannot say:
// it short-circuits to Failed on the first unrealized leg, however many others
// are up.
func anyBackingDevice(vol *miroirv1alpha1.MiroirVolume) bool {
	for _, rep := range vol.Spec.DiskfulReplicas() {
		if st, ok := vol.Status.PerNode[rep.Node]; ok && st.DeviceCreated {
			return true
		}
	}
	return false
}

// healthOf assembles the response from what the mappers produced, dropping the
// nils they return for "no condition" and ordering what is left most-urgent
// first, so a CO surfacing only the head names the condition to act on. An
// empty list means healthy.
func healthOf(vol *miroirv1alpha1.MiroirVolume, entries []*csi.VolumeHealth_VolumeHealthEntry) *csi.VolumeHealth {
	entries = slices.DeleteFunc(entries, func(e *csi.VolumeHealth_VolumeHealthEntry) bool { return e == nil })
	slices.SortStableFunc(entries, func(a, b *csi.VolumeHealth_VolumeHealthEntry) int {
		return cmp.Compare(b.GetStatus(), a.GetStatus())
	})
	return &csi.VolumeHealth{VolumeId: vol.Name, HealthStatuses: entries}
}

// firstNodeWhere returns the lexically-first node whose status satisfies pred,
// so a condition message stays stable across reconciles when several nodes
// match.
func firstNodeWhere(vol *miroirv1alpha1.MiroirVolume, pred func(miroirv1alpha1.ReplicaStatus) bool) (string, bool) {
	best, found := "", false
	for node, st := range vol.Status.PerNode {
		if pred(st) && (!found || node < best) {
			best, found = node, true
		}
	}
	return best, found
}

func entryf(errorType csi.VolumeHealthErrorType, reason, format string, args ...any) *csi.VolumeHealth_VolumeHealthEntry {
	return &csi.VolumeHealth_VolumeHealthEntry{
		Status:  errorType,
		Reason:  reason,
		Message: fmt.Sprintf(format, args...),
	}
}
