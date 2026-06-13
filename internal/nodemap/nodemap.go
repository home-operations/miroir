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

	"sigs.k8s.io/yaml"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
)

// Node describes one storage node's backend. Replication addresses are
// not configured here: the controller resolves each node's InternalIP
// from its Node object at volume creation and persists it in the CRD.
type Node struct {
	// Backend selects the storage implementation: "lvmthin" | "zfs".
	Backend homefsv1alpha1.BackendType `json:"backend"`
	// Device is the block device backing the LVM VG (lvmthin).
	Device string `json:"device,omitempty"`
	// ZFSDataset is the parent dataset for zvols (zfs).
	ZFSDataset string `json:"zfsDataset,omitempty"`
	// ThinPoolSize bounds the thin pool (lvm size spec, e.g. "400g");
	// empty claims all free VG space.
	ThinPoolSize string `json:"thinPoolSize,omitempty"`
	// BaseDir is the directory on the node's existing filesystem holding the
	// loop-backed sparse files (loopfile), e.g. "/var/lib/homefs".
	BaseDir string `json:"baseDir,omitempty"`
}

// Map is node name → storage config. Nodes absent from the map hold no
// replicas.
type Map map[string]Node

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
		case homefsv1alpha1.BackendLVMThin, homefsv1alpha1.BackendZFS, homefsv1alpha1.BackendLoopfile:
		default:
			return nil, fmt.Errorf("node %s: invalid backend %q", name, n.Backend)
		}
		if n.Backend == homefsv1alpha1.BackendZFS && n.ZFSDataset == "" {
			return nil, fmt.Errorf("node %s: zfs backend requires zfsDataset", name)
		}
		if n.Backend == homefsv1alpha1.BackendLoopfile && n.BaseDir == "" {
			return nil, fmt.Errorf("node %s: loopfile backend requires baseDir", name)
		}
	}
	return m, nil
}
