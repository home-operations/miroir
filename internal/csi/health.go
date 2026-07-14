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

// volumeCondition maps a volume's aggregated CRD status (the same signals the
// agent folds into Phase and the miroir_volume_* gauges) to a CSI
// VolumeCondition. The external-health-monitor surfaces it as a PVC event and
// kubelet as the volume-health metric, so operators see split-brain/degraded
// without scraping Prometheus. Abnormal states are ranked most-urgent first so
// the single message names the condition to act on.
func volumeCondition(vol *miroirv1alpha1.MiroirVolume) *csi.VolumeCondition {
	// Split-brain: DRBD refused to reconnect diverged legs. Nothing heals it
	// automatically — an operator must pick the loser.
	if node, ok := firstNodeWhere(vol, func(s miroirv1alpha1.ReplicaStatus) bool { return s.SplitBrain }); ok {
		return abnormalf("split-brain on node %s; manual resolution required", node)
	}
	// A backing disk DRBD detached on I/O error (on-io-error detach): serving
	// continues via the peer, but this leg is latched failed and redundancy is
	// gone until the disk is replaced.
	if node, ok := firstNodeWhere(vol, func(s miroirv1alpha1.ReplicaStatus) bool { return s.DiskFailed }); ok {
		return abnormalf("backing disk failed on node %s; replace the disk, then remove and re-add the replica", node)
	}
	switch vol.Status.Phase {
	case miroirv1alpha1.VolumeFailed:
		return abnormalf("provisioning failed; a backing device never materialized")
	case miroirv1alpha1.VolumeDegraded:
		return abnormalf("degraded: a replica is missing current data (resyncing or its peer is unreachable)")
	}
	return &csi.VolumeCondition{Abnormal: false, Message: "healthy"}
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

func abnormalf(format string, args ...any) *csi.VolumeCondition {
	return &csi.VolumeCondition{Abnormal: true, Message: fmt.Sprintf(format, args...)}
}
