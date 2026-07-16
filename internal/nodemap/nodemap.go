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

// Package nodemap folds the per-node storage topology — MiroirNode custom
// resources rendered by the Helm chart from the release's `nodes` values —
// into the flat placement map the controller and agents consume: the
// controller places replicas from it, agents pick their backends from it.
// Per-node validation lives in the MiroirNode CRD (schema + CEL); this
// package adds only the cross-object rule a CRD cannot see (duplicate
// replication addresses).
package nodemap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// Pool describes one named storage pool on a node: the backend
// implementation and its backing location, flattened from the CR's
// per-backend block. Pool names are cluster-wide identities — a
// StorageClass selects a pool by name and the controller places each
// replica on a node carrying that pool.
type Pool struct {
	// Backend selects the storage implementation: "lvmthin" | "zfs" |
	// "loopfile".
	Backend miroirv1alpha1.BackendType
	// Device is the block device backing the LVM VG (lvmthin).
	Device string
	// ZFSDataset is the parent dataset for zvols (zfs).
	ZFSDataset string
	// ZFSVolBlockSize is the block size for newly created zvols (zfs).
	ZFSVolBlockSize string
	// ZFSCompression is the compression algorithm for newly created zvols
	// (zfs), or "inherit" to use the parent dataset's setting.
	ZFSCompression string
	// ThinPoolSize bounds the thin pool (lvm size spec, e.g. "400g");
	// empty claims all free VG space.
	ThinPoolSize string
	// BaseDir is the directory on the node's existing filesystem holding the
	// loop-backed sparse files (loopfile), e.g. "/var/lib/miroir".
	BaseDir string
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
	Zone string
	// Address optionally pins the node's DRBD replication endpoint to a
	// dedicated storage NIC/VLAN IP (IPv4 or IPv6); empty falls back to
	// the node's InternalIP.
	Address string
	// AutoEvict, when explicitly false, exempts this node from auto-evict:
	// its legs are never re-placed while its heartbeat is stale (a node
	// with known long outages). Absent means eligible.
	AutoEvict *bool
	// AddressConflict marks a node whose Address is shared with another
	// node — two nodes dialing one endpoint makes every shared volume's
	// DRBD connections ambiguous at connect time. Conflicted nodes are
	// skipped by placement and refuse address resolution until the
	// operator resolves the clash; a CRD validates one object at a time,
	// so this cross-object rule lives here.
	AddressConflict bool
	// Pools maps pool name → pool config. The pre-multi-pool single pool
	// is the pool named "default" — volumes and classes that name no pool
	// resolve there.
	Pools map[string]Pool
}

// Map is node name → storage config. Nodes absent from the map hold no
// replicas.
type Map map[string]Node

// Source yields the current storage topology. The controller resolves it
// per RPC or reconcile from the MiroirNode cache (CRSource); tests hand a
// fixed Static map.
type Source interface {
	Map(ctx context.Context) (Map, error)
}

// CRSource folds the MiroirNode CRs from a (cached) reader on every call,
// so the topology follows chart-applied edits without a process restart.
type CRSource struct {
	Reader client.Reader
}

// Map lists the MiroirNodes and folds them.
func (s *CRSource) Map(ctx context.Context) (Map, error) {
	list := &miroirv1alpha1.MiroirNodeList{}
	if err := s.Reader.List(ctx, list); err != nil {
		return nil, fmt.Errorf("list MiroirNodes: %w", err)
	}
	return FromNodes(list.Items), nil
}

// Map implements Source with a fixed topology, so a literal Map serves
// anywhere a Source is expected (tests, single-shot callers).
func (m Map) Map(context.Context) (Map, error) { return m, nil }

// FromSpec flattens one MiroirNode spec into the internal node shape. The
// backend is the configuration block that is present; the CRD guarantees
// exactly one is set.
func FromSpec(spec miroirv1alpha1.MiroirNodeSpec) Node {
	n := Node{
		Zone:      spec.Zone,
		Address:   spec.Address,
		AutoEvict: spec.AutoEvict,
		Pools:     make(map[string]Pool, len(spec.Pools)),
	}
	for _, p := range spec.Pools {
		var pool Pool
		switch {
		case p.LVMThin != nil:
			pool = Pool{
				Backend:      miroirv1alpha1.BackendLVMThin,
				Device:       p.LVMThin.Device,
				ThinPoolSize: p.LVMThin.PoolSize,
			}
		case p.ZFS != nil:
			pool = Pool{
				Backend:         miroirv1alpha1.BackendZFS,
				ZFSDataset:      p.ZFS.Dataset,
				ZFSCompression:  p.ZFS.Compression,
				ZFSVolBlockSize: p.ZFS.VolBlockSize,
			}
		case p.Loopfile != nil:
			pool = Pool{
				Backend: miroirv1alpha1.BackendLoopfile,
				BaseDir: p.Loopfile.BaseDir,
			}
		}
		n.Pools[p.Name] = pool
	}
	return n
}

// ConflictKey canonicalizes a replication address for conflict grouping:
// parseable IPs collapse to their canonical spelling (IPv6 zero-compression),
// anything else — reachable only through a stale CRD or a writer bypassing
// the isIP rule — groups on the raw string, so duplicated junk still
// conflicts instead of slipping into persisted replica specs. Empty means
// no address (InternalIP fallback) and never conflicts.
func ConflictKey(address string) string {
	if ip := net.ParseIP(address); ip != nil {
		return ip.String()
	}
	return address
}

// FromNodes folds MiroirNode CRs into the placement Map. Nodes sharing a
// replication address are all marked AddressConflict — keyed by ConflictKey
// so differently written but equal IPs (IPv6 zero-compression) still
// collide.
func FromNodes(items []miroirv1alpha1.MiroirNode) Map {
	m := make(Map, len(items))
	owners := map[string][]string{}
	for i := range items {
		node := FromSpec(items[i].Spec)
		if key := ConflictKey(node.Address); key != "" {
			owners[key] = append(owners[key], items[i].Name)
		}
		m[items[i].Name] = node
	}
	for _, names := range owners {
		if len(names) < 2 {
			continue
		}
		for _, name := range names {
			node := m[name]
			node.AddressConflict = true
			m[name] = node
		}
	}
	return m
}

// Pool resolves a named pool on a node; the second return mirrors map
// lookup. Empty name means the default pool, matching CRD adoption
// (replicas persisted before pools carry no pool field).
func (m Map) Pool(node, pool string) (Pool, bool) {
	p, ok := m[node].Pools[PoolOrDefault(pool)]
	return p, ok
}

// Placeable reports whether a node may receive new legs: it is in the
// topology and its replication endpoint is unambiguous (no address
// conflict). Every candidate enumeration — place, GetCapacity, PickSpare —
// flows through this one predicate so exclusion reasons cannot drift
// between them; ReplicationAddress refuses conflicted nodes separately
// because client legs on nodes outside the topology still resolve there.
func (m Map) Placeable(node string) bool {
	n, ok := m[node]
	return ok && !n.AddressConflict
}

// AutoEvictAllowed reports whether auto-evict may re-place the node's
// legs: the node is in the map and has not opted out.
func (m Map) AutoEvictAllowed(node string) bool {
	n, ok := m[node]
	return ok && (n.AutoEvict == nil || *n.AutoEvict)
}

// PoolOrDefault maps the empty pool name to the default pool. Replicas
// and StorageClasses created before named pools carry no pool reference;
// they all mean the pool now called "default".
func PoolOrDefault(pool string) string {
	if pool == "" {
		return miroirv1alpha1.DefaultPoolName
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
	return m.PickSpare(usedNode, usedZone, nil)
}

// PickSpare is the one spare-picking policy: the lowest-named node not
// in usedNodes that passes keep (nil accepts all), preferring a node
// whose zone is not in usedZones; the first candidate when every zone is
// taken; "" when none qualifies. Address-conflicted nodes never qualify.
// Tie-breaker placement and auto-evict re-placement both resolve through
// it so their spread rules cannot drift apart.
func (m Map) PickSpare(usedNodes, usedZones map[string]bool, keep func(node string) bool) string {
	spare := make([]string, 0, len(m))
	for n := range m {
		if !usedNodes[n] && m.Placeable(n) && (keep == nil || keep(n)) {
			spare = append(spare, n)
		}
	}
	slices.Sort(spare)
	for _, n := range spare {
		if z := m[n].Zone; z == "" || !usedZones[z] {
			return n
		}
	}
	if len(spare) > 0 {
		return spare[0]
	}
	return ""
}

const (
	DefaultZFSVolBlockSize = "4K"
	DefaultZFSCompression  = "lz4"
)

// zfsVolBlockSizes maps the CRD's volBlockSize enum to bytes. The CRD is
// the validation surface; spellings are canonical (no case folding).
var zfsVolBlockSizes = map[string]int64{
	"4K": 4 << 10, "8K": 8 << 10, "16K": 16 << 10,
	"32K": 32 << 10, "64K": 64 << 10, "128K": 128 << 10,
}

// ZFSVolBlockSizeBytes returns the zvol block size in bytes; the CRD
// default when unset.
func (p Pool) ZFSVolBlockSizeBytes() int64 {
	if p.ZFSVolBlockSize == "" {
		return zfsVolBlockSizes[DefaultZFSVolBlockSize]
	}
	return zfsVolBlockSizes[p.ZFSVolBlockSize]
}

// ErrAddressConflict marks an address resolution refused because the node
// shares its replication address with another MiroirNode. Callers that
// distinguish unfixable-by-retry failures (membership's errBadPlacement)
// test for it with errors.Is.
var ErrAddressConflict = errors.New("replication address conflict")

// ReplicationAddress resolves a node's DRBD replication endpoint: the node
// map's address override when set (dedicated storage NIC/VLAN), otherwise
// the Node object's InternalIP. An override needs no Node lookup, so it
// resolves even before the kubelet posts the node's addresses. A node in
// an address conflict refuses — a leg persisted with an ambiguous endpoint
// would outlive the misconfiguration.
func (m Map) ReplicationAddress(ctx context.Context, r client.Reader, name string) (string, error) {
	if m[name].AddressConflict {
		return "", fmt.Errorf("%w: node %s shares its replication address %s with another MiroirNode; resolve the conflict first",
			ErrAddressConflict, name, m[name].Address)
	}
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
