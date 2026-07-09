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

// Package nodemap loads the per-node storage topology from a config file
// (a ConfigMap rendered from the Helm release's `nodes` values). It is the
// single source of truth for which nodes hold storage and how: the
// controller places replicas from it, agents pick their backend from it.
package nodemap

import (
	"fmt"
	"os"
	"slices"

	"sigs.k8s.io/yaml"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// Node describes one storage node's backend. Replication addresses are
// not configured here: the controller resolves each node's InternalIP
// from its Node object at volume creation and persists it in the CRD.
type Node struct {
	// Backend selects the storage implementation: "lvmthin" | "zfs".
	Backend miroirv1alpha1.BackendType `json:"backend"`
	// Zone is an optional failure domain (rack, host group, AZ). When set,
	// the controller spreads a volume's replicas across distinct zones;
	// empty means unconstrained.
	Zone string `json:"zone,omitempty"`
	// Device is the block device backing the LVM VG (lvmthin).
	Device string `json:"device,omitempty"`
	// ZFSDataset is the parent dataset for zvols (zfs).
	ZFSDataset string `json:"zfsDataset,omitempty"`
	// ThinPoolSize bounds the thin pool (lvm size spec, e.g. "400g");
	// empty claims all free VG space.
	ThinPoolSize string `json:"thinPoolSize,omitempty"`
	// BaseDir is the directory on the node's existing filesystem holding the
	// loop-backed sparse files (loopfile), e.g. "/var/lib/miroir".
	BaseDir string `json:"baseDir,omitempty"`
}

// Map is node name → storage config. Nodes absent from the map hold no
// replicas.
type Map map[string]Node

// TieBreakerNode picks a storage node to host a diskless tie-breaker for
// the given replicas: one not already holding a replica, preferring a zone
// none of them occupy, ties by name. Empty when no spare node exists.
func (m Map) TieBreakerNode(replicas []miroirv1alpha1.Replica) string {
	usedNode := make(map[string]bool, len(replicas))
	usedZone := make(map[string]bool, len(replicas))
	for _, r := range replicas {
		usedNode[r.Node] = true
		if z := m[r.Node].Zone; z != "" {
			usedZone[z] = true
		}
	}
	spare := make([]string, 0, len(m))
	for n := range m {
		if !usedNode[n] {
			spare = append(spare, n)
		}
	}
	slices.Sort(spare)
	for _, n := range spare {
		if z := m[n].Zone; z == "" || !usedZone[z] {
			return n
		}
	}
	if len(spare) > 0 {
		return spare[0]
	}
	return ""
}

// Load reads and validates the node map from a YAML file.
func Load(path string) (Map, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read node map: %w", err)
	}
	m := Map{}
	if err := yaml.UnmarshalStrict(raw, &m); err != nil {
		return nil, fmt.Errorf("parse node map %s: %w", path, err)
	}
	for name, n := range m {
		switch n.Backend {
		case miroirv1alpha1.BackendLVMThin, miroirv1alpha1.BackendZFS, miroirv1alpha1.BackendLoopfile:
		default:
			return nil, fmt.Errorf("node %s: invalid backend %q", name, n.Backend)
		}
		if n.Backend == miroirv1alpha1.BackendZFS && n.ZFSDataset == "" {
			return nil, fmt.Errorf("node %s: zfs backend requires zfsDataset", name)
		}
		if n.Backend == miroirv1alpha1.BackendLoopfile && n.BaseDir == "" {
			return nil, fmt.Errorf("node %s: loopfile backend requires baseDir", name)
		}
	}
	return m, nil
}
