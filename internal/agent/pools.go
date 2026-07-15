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
	"context"
	"fmt"
	"maps"
	"slices"

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
// Only a hint for delete paths: a leg torn down after a crash may have
// neither source, so deletions sweep every pool instead of trusting this.
func volumePoolOn(vol *miroirv1alpha1.MiroirVolume, node string) string {
	for _, rep := range vol.Spec.Replicas {
		if rep.Node == node {
			return nodemap.PoolOrDefault(rep.Pool)
		}
	}
	return nodemap.PoolOrDefault(vol.Status.PerNode[node].Pool)
}

// sorted returns the pool names in stable order for deterministic sweeps.
func (p Pools) sorted() []string {
	return slices.Sorted(maps.Keys(p))
}

// SweepDelete removes the volume's backing device from every pool on this
// node. Volume names are cluster-unique and a node holds at most one leg
// of a volume, so at most one pool has the device and the rest no-op —
// the same always-safe semantics the pre-multi-pool unconditional delete
// had. Delete paths use this instead of resolving one pool: the leg's
// pool can be unknowable (agent crashed before its first status patch, a
// slot deleted at removal, a leg re-added diskless over a leftover
// backing), and guessing wrong would leak the device or wedge the
// finalizer. Every pool is attempted; the first error is returned after
// the sweep so one bad pool cannot shadow the others' cleanup.
func (p Pools) SweepDelete(ctx context.Context, vol string) error {
	var firstErr error
	for _, name := range p.sorted() {
		if err := p[name].Backend.Delete(ctx, vol); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SweepDeleteSnapshot removes a snapshot from every pool on this node,
// with the same rationale and semantics as SweepDelete.
func (p Pools) SweepDeleteSnapshot(ctx context.Context, vol, snap string) error {
	var firstErr error
	for _, name := range p.sorted() {
		if err := p[name].Backend.DeleteSnapshot(ctx, vol, snap); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
