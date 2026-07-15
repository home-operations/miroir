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
// controller places replicas from it, agents pick their backends from it.
package nodemap

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

// Pool describes one named storage pool on a node: the backend
// implementation and its backing location. Pool names are cluster-wide
// identities — a StorageClass selects a pool by name and the controller
// places each replica on a node carrying that pool.
type Pool struct {
	// Backend selects the storage implementation: "lvmthin" | "zfs" |
	// "loopfile".
	Backend miroirv1alpha1.BackendType `json:"backend"`
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

// Node describes one storage node: its named pools plus node-level
// replication settings. Replication endpoints default to the node's
// InternalIP, resolved from its Node object at volume creation and
// persisted in the CRD; `address` overrides that for a dedicated
// replication NIC.
type Node struct {
	// Zone is an optional failure domain (rack, host group, AZ). When set,
	// the controller spreads a volume's replicas across distinct zones;
	// empty means unconstrained.
	Zone string `json:"zone,omitempty"`
	// Address optionally pins the node's DRBD replication endpoint to a
	// dedicated storage NIC/VLAN IP (IPv4 or IPv6); empty falls back to
	// the node's InternalIP.
	Address string `json:"address,omitempty"`
	// Pools maps pool name → pool config. The pre-multi-pool single pool
	// is the pool named "default" — volumes and classes that name no pool
	// resolve there.
	Pools map[string]Pool `json:"pools"`
}

// Map is node name → storage config. Nodes absent from the map hold no
// replicas.
type Map map[string]Node

// Pool resolves a named pool on a node; the second return mirrors map
// lookup. Empty name means the default pool, matching CRD adoption
// (replicas persisted before pools carry no pool field).
func (m Map) Pool(node, pool string) (Pool, bool) {
	p, ok := m[node].Pools[PoolOrDefault(pool)]
	return p, ok
}

// PoolOrDefault maps the empty pool name to the default pool. Replicas
// and StorageClasses created before named pools carry no pool reference;
// they all mean the pool now called "default".
func PoolOrDefault(pool string) string {
	if pool == "" {
		return constants.DefaultPoolName
	}
	return pool
}

// TieBreakerNode picks a storage node to host a diskless tie-breaker for
// the given replicas: one not already holding a replica, preferring a zone
// none of them occupy, ties by name. Any storage node qualifies — a
// tie-breaker holds no data, so it needs no particular pool. Empty when no
// spare node exists.
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

// poolNameRe bounds pool names: they become LVM VG names
// (vg-miroir-<pool>), metric label values, and StorageClass parameters, so
// keep them short lowercase DNS-label-style identifiers.
var poolNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

// Load reads and validates the node map from a YAML file.
func Load(path string) (Map, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read node map: %w", err)
	}
	m := Map{}
	if err := yaml.UnmarshalStrict(raw, &m); err != nil {
		if legacy := legacyFlatNode(raw); legacy != "" {
			return nil, fmt.Errorf("node %s uses the pre-0.10 flat single-pool shape; "+
				"move backend/device/zfsDataset/baseDir/thinPoolSize under `pools: {%s: {...}}` "+
				"(zone and address stay node-level)", legacy, constants.DefaultPoolName)
		}
		return nil, fmt.Errorf("parse node map %s: %w", path, err)
	}
	// Keyed by the parsed form so differently written but equal IPs
	// (IPv6 zero-compression) still collide.
	addrOwner := map[string]string{}
	for name, n := range m {
		if len(n.Pools) == 0 {
			return nil, fmt.Errorf("node %s: no pools defined (declare at least pools.%s)",
				name, constants.DefaultPoolName)
		}
		if err := validatePools(name, n.Pools); err != nil {
			return nil, err
		}
		if n.Address != "" {
			ip := net.ParseIP(n.Address)
			if ip == nil {
				return nil, fmt.Errorf("node %s: invalid address %q", name, n.Address)
			}
			// Two nodes dialing one endpoint makes every shared volume's
			// DRBD connections ambiguous at connect time — fail at load.
			if other, dup := addrOwner[ip.String()]; dup {
				return nil, fmt.Errorf("nodes %s and %s share replication address %s",
					min(name, other), max(name, other), n.Address)
			}
			addrOwner[ip.String()] = name
		}
	}
	return m, nil
}

// validatePools checks one node's pools: valid names and backends, the
// per-backend required field, and no two pools sharing a backing location
// (one device/dataset/dir belongs to exactly one pool).
func validatePools(node string, pools map[string]Pool) error {
	backingOwner := map[string]string{}
	for poolName, p := range pools {
		if !poolNameRe.MatchString(poolName) {
			return fmt.Errorf("node %s: invalid pool name %q (lowercase alphanumerics and dashes, max 32 chars)",
				node, poolName)
		}
		var backing string
		switch p.Backend {
		case miroirv1alpha1.BackendLVMThin:
			backing = p.Device
		case miroirv1alpha1.BackendZFS:
			if p.ZFSDataset == "" {
				return fmt.Errorf("node %s pool %s: zfs backend requires zfsDataset", node, poolName)
			}
			backing = p.ZFSDataset
		case miroirv1alpha1.BackendLoopfile:
			if p.BaseDir == "" {
				return fmt.Errorf("node %s pool %s: loopfile backend requires baseDir", node, poolName)
			}
			backing = p.BaseDir
		default:
			return fmt.Errorf("node %s pool %s: invalid backend %q", node, poolName, p.Backend)
		}
		if backing == "" {
			continue
		}
		if other, dup := backingOwner[backing]; dup {
			return fmt.Errorf("node %s: pools %s and %s share backing %s",
				node, min(poolName, other), max(poolName, other), backing)
		}
		backingOwner[backing] = poolName
	}
	return nil
}

// legacyFlatNode reports the first node still written in the pre-0.10
// flat single-pool shape (backend at the node level), or "" — so the load
// error names the actual migration instead of a strict-unmarshal field
// complaint.
func legacyFlatNode(raw []byte) string {
	probe := map[string]struct {
		Backend string `json:"backend"`
	}{}
	if yaml.Unmarshal(raw, &probe) != nil {
		return ""
	}
	names := make([]string, 0, len(probe))
	for name, n := range probe {
		if n.Backend != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return ""
	}
	return slices.Min(names)
}

// ReplicationAddress resolves a node's DRBD replication endpoint: the node
// map's address override when set (dedicated storage NIC/VLAN), otherwise
// the Node object's InternalIP. An override needs no Node lookup, so it
// resolves even before the kubelet posts the node's addresses.
func (m Map) ReplicationAddress(ctx context.Context, r client.Reader, name string) (string, error) {
	if a := m[name].Address; a != "" {
		return a, nil
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
		return "", fmt.Errorf("get node %s: %w", name, err)
	}
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address, nil
		}
	}
	return "", fmt.Errorf("node %s has no InternalIP", name)
}
