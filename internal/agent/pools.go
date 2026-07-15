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

package agent

import (
	"fmt"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/nodemap"
)

// PoolBackend is one storage pool's backend on this node.
type PoolBackend struct {
	Backend backend.Backend
	Type    miroirv1alpha1.BackendType
}

// Pools maps pool name → that pool's backend on this node, built from the
// node map at agent start. Volume reconciles resolve their replica's pool
// here.
type Pools map[string]PoolBackend

// Get resolves a pool by name (empty → default). The error names the pool
// so a volume referencing a pool this node no longer carries fails loudly
// instead of landing in the wrong pool.
func (p Pools) Get(name string) (PoolBackend, error) {
	resolved := nodemap.PoolOrDefault(name)
	pb, ok := p[resolved]
	if !ok {
		return PoolBackend{}, fmt.Errorf("storage pool %q is not configured on this node", resolved)
	}
	return pb, nil
}

// volumePoolOn resolves which pool holds (or held) the volume's local leg
// on node: the spec entry while the leg is placed here, else the agent's
// self-reported status slot — a replica removed from spec still needs its
// teardown to target the right pool. Returns the normalized pool name.
func volumePoolOn(vol *miroirv1alpha1.MiroirVolume, node string) string {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == node {
			return nodemap.PoolOrDefault(rep.Pool)
		}
	}
	return nodemap.PoolOrDefault(vol.Status.PerNode[node].Pool)
}
