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
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// Reasons a volume is reported unhealthy. The CSI spec has COs match on
// these strings, so they are part of the driver's contract: rename one and
// anything keyed off it stops firing.
const (
	reasonSplitBrain         = "SplitBrain"
	reasonBackingDiskFailed  = "BackingDiskFailed"
	reasonProvisioningFailed = "ProvisioningFailed"
	reasonReplicaOutOfSync   = "ReplicaOutOfSync"
)

// volumeHealth maps a volume's aggregated CRD status (the same signals the
// agent folds into Phase and the miroir_volume_* gauges) to a CSI
// VolumeHealth, which a CO fetches through ControllerGetVolumeHealth or
// NodeGetVolumeHealth — so operators see split-brain/degraded without
// scraping Prometheus. An empty status list means healthy. Every condition
// that holds is reported, ordered most-urgent first so a CO surfacing only
// the head names the condition to act on.
func volumeHealth(vol *miroirv1alpha1.MiroirVolume) *csi.VolumeHealth {
	health := &csi.VolumeHealth{VolumeId: vol.Name}
	// Split-brain: DRBD refused to reconnect diverged legs. Nothing heals it
	// automatically — an operator must pick the loser, whose writes are lost.
	if node, ok := firstNodeWhere(vol, func(s miroirv1alpha1.ReplicaStatus) bool { return s.SplitBrain }); ok {
		health.HealthStatuses = append(health.HealthStatuses,
			entryf(csi.VolumeHealthErrorType_DATA_LOSS, reasonSplitBrain,
				"split-brain on node %s; manual resolution required", node))
	}
	// A backing disk DRBD detached on I/O error (on-io-error detach): serving
	// continues via the peer, but this leg is latched failed and redundancy is
	// gone until the disk is replaced.
	if node, ok := firstNodeWhere(vol, func(s miroirv1alpha1.ReplicaStatus) bool { return s.DiskFailed }); ok {
		health.HealthStatuses = append(health.HealthStatuses,
			entryf(csi.VolumeHealthErrorType_DEGRADED, reasonBackingDiskFailed,
				"backing disk failed on node %s; replace the disk, then remove and re-add the replica", node))
	}
	switch vol.Status.Phase {
	case miroirv1alpha1.VolumeFailed:
		health.HealthStatuses = append(health.HealthStatuses,
			entryf(csi.VolumeHealthErrorType_INACCESSIBLE, reasonProvisioningFailed,
				"provisioning failed; a backing device never materialized"))
	case miroirv1alpha1.VolumeDegraded:
		health.HealthStatuses = append(health.HealthStatuses,
			entryf(csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync,
				"degraded: a replica is missing current data (resyncing or its peer is unreachable)"))
	}
	return health
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

func entryf(status csi.VolumeHealthErrorType, reason, format string, args ...any) *csi.VolumeHealth_VolumeHealthEntry {
	return &csi.VolumeHealth_VolumeHealthEntry{
		Status:  status,
		Reason:  reason,
		Message: fmt.Sprintf(format, args...),
	}
}
