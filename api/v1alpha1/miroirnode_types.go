package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LVMThinPool configures an lvmthin pool: a dm-thin pool on the LVM VG
// vg-miroir (default pool) or vg-miroir-<pool>.
type LVMThinPool struct {
	// Device is the block device backing the pool's VG. Optional when the
	// VG already exists (a pre-provisioned or shared VG); required for the
	// agent to create it on first start.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Device string `json:"device,omitempty"`
	// PoolSize bounds the thin pool (an lvm size spec, e.g. "400g");
	// empty claims all free VG space.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	PoolSize string `json:"poolSize,omitempty"`
}

// ZFSPool configures a zfs pool: zvols under a parent dataset. Values are
// OpenZFS property spellings, accepted verbatim by zfs(8) — canonical case
// is required (4K, lz4), not folded.
type ZFSPool struct {
	// Dataset is the parent dataset for zvols.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Dataset string `json:"dataset"`
	// Compression is the compression property for newly created zvols;
	// "inherit" uses the parent dataset's setting. Existing zvols are not
	// mutated, and snapshot clones retain their source properties.
	// +optional
	// +kubebuilder:default=lz4
	// +kubebuilder:validation:Pattern=`^(inherit|on|off|lz4|lzjb|zle|gzip(-[1-9])?|zstd(-([1-9]|1[0-9]))?|zstd-fast(-(10|[1-9]|[2-9]0|100|500|1000))?)$`
	Compression string `json:"compression,omitempty"`
	// VolBlockSize is the volblocksize property for newly created zvols.
	// +optional
	// +kubebuilder:default="4K"
	// +kubebuilder:validation:Enum="4K";"8K";"16K";"32K";"64K";"128K"
	VolBlockSize string `json:"volBlockSize,omitempty"`
}

// LoopfilePool configures a loopfile pool: loop-backed sparse files on the
// node's existing filesystem — no dedicated disk or pool.
type LoopfilePool struct {
	// BaseDir is the directory holding the sparse files, e.g.
	// "/var/lib/miroir". Must be reflink-capable (XFS reflink=1 or btrfs)
	// for CoW snapshots; the agent refuses the pool otherwise.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	BaseDir string `json:"baseDir"`
}

// MiroirNodePool is one named storage pool on a node: the backend
// implementation plus that backend's configuration block. Pool names are
// cluster-wide identities — a StorageClass selects a pool by name and the
// controller places each replica on a node carrying that pool.
// +kubebuilder:validation:XValidation:rule="(self.backend == 'lvmthin') == has(self.lvmthin) && (self.backend == 'zfs') == has(self.zfs) && (self.backend == 'loopfile') == has(self.loopfile)",message="exactly the selected backend's configuration block must be set (lvmthin: {} when the VG already exists)"
type MiroirNodePool struct {
	// Name is the pool name ("default" for the pre-multi-pool single
	// pool). It becomes an LVM VG name suffix, a metric label value, and a
	// StorageClass parameter, so it stays a short lowercase identifier.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`
	Name string `json:"name"`
	// Backend is the storage implementation backing this pool. An explicit
	// discriminator (not inferred from the block below): it is also
	// persisted per Replica, beyond pool config.
	Backend BackendType `json:"backend"`
	// LVMThin configures the pool when backend is lvmthin.
	// +optional
	LVMThin *LVMThinPool `json:"lvmthin,omitempty"`
	// ZFS configures the pool when backend is zfs.
	// +optional
	ZFS *ZFSPool `json:"zfs,omitempty"`
	// Loopfile configures the pool when backend is loopfile.
	// +optional
	Loopfile *LoopfilePool `json:"loopfile,omitempty"`
}

// MiroirNodeSpec is one storage node's desired topology: its named pools
// plus node-level replication settings. Rendered by the Helm chart from
// the release's `nodes` values; read by the controller for placement and
// by the node's agent for backend selection.
type MiroirNodeSpec struct {
	// Zone is an optional failure domain (rack, host group, AZ). When set,
	// the controller spreads a volume's replicas across distinct zones;
	// empty means unconstrained.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Zone string `json:"zone,omitempty"`
	// Address optionally pins the node's DRBD replication endpoint to a
	// dedicated storage NIC/VLAN IP (IPv4 or IPv6); empty falls back to
	// the node's InternalIP. It applies to volumes created afterwards —
	// existing volumes keep the address persisted at creation.
	// +optional
	// +kubebuilder:validation:MaxLength=45
	// +kubebuilder:validation:XValidation:rule="isIP(self)",message="address must be a plain IPv4 or IPv6 address"
	Address string `json:"address,omitempty"`
	// AutoEvict, when explicitly false, exempts this node from auto-evict:
	// its legs are never re-placed while its heartbeat is stale (a node
	// with known long outages). Absent means eligible.
	// +optional
	AutoEvict *bool `json:"autoEvict,omitempty"`
	// Pools lists this node's storage pools, one entry per pool. Every
	// node in the topology carries at least one.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:XValidation:rule="self.filter(p, has(p.lvmthin) && has(p.lvmthin.device)).map(p, p.lvmthin.device).all(d, self.filter(p, has(p.lvmthin) && has(p.lvmthin.device) && p.lvmthin.device == d).size() == 1)",message="no two pools may share a device"
	// +kubebuilder:validation:XValidation:rule="self.filter(p, has(p.zfs)).map(p, p.zfs.dataset).all(d, self.filter(p, has(p.zfs) && p.zfs.dataset == d).size() == 1)",message="no two pools may share a dataset"
	// +kubebuilder:validation:XValidation:rule="self.filter(p, has(p.loopfile)).map(p, p.loopfile.baseDir).all(d, self.filter(p, has(p.loopfile) && p.loopfile.baseDir == d).size() == 1)",message="no two pools may share a baseDir"
	Pools []MiroirNodePool `json:"pools"`
}

// MiroirNodePoolStatus is one pool's capacity figures.
// On a shared pool (e.g. ZFS shared with OpenEBS) the figures are
// pool-level, so a co-tenant's growth correctly shrinks miroir's headroom.
type MiroirNodePoolStatus struct {
	// Name is the pool name from the node map.
	Name string `json:"name"`
	// CapacityBytes is the total size of this node-local pool.
	// +optional
	CapacityBytes int64 `json:"capacityBytes,omitempty"`
	// AllocatedBytes is the pool capacity currently used (all tenants).
	// +optional
	AllocatedBytes int64 `json:"allocatedBytes,omitempty"`
	// MetaUsedPercent is the dm-thin metadata usage (lvmthin only; 0
	// otherwise), rounded — exhausting metadata wedges the pool
	// independently of data space.
	// +optional
	MetaUsedPercent int32 `json:"metaUsedPercent,omitempty"`
	// Message carries the last stats-sampling error for this pool, if
	// any — a pool whose backend cannot be read stays visible instead of
	// silently dropping out of the list.
	// +optional
	Message string `json:"message,omitempty"`
}

// MiroirNodeStatus is the pool capacity this node's agent publishes for
// capacity-aware placement and overcommit guardrails.
type MiroirNodeStatus struct {
	// Pools carries one capacity entry per storage pool on this node.
	// +optional
	// +listType=map
	// +listMapKey=name
	Pools []MiroirNodePoolStatus `json:"pools,omitempty"`
	// CapacityBytes is the pre-multi-pool single-pool figure. Kept in the
	// schema so a mixed-version rollout (new controller, old agent) does
	// not prune the old agent's stats into "fresh but empty" — Pool()
	// folds these into the default pool entry. Never written by current
	// agents; drop after one release.
	//
	// Deprecated: superseded by Pools.
	// +optional
	CapacityBytes int64 `json:"capacityBytes,omitempty"`
	// AllocatedBytes is the pre-multi-pool twin of CapacityBytes.
	//
	// Deprecated: superseded by Pools.
	// +optional
	AllocatedBytes int64 `json:"allocatedBytes,omitempty"`
	// MetaUsedPercent is the pre-multi-pool twin of CapacityBytes.
	//
	// Deprecated: superseded by Pools.
	// +optional
	MetaUsedPercent int32 `json:"metaUsedPercent,omitempty"`
	// DRBDVersion is the DRBD kernel module version the agent probed at
	// startup (e.g. "9.3.2"); absent on nodes without the module. The
	// module ships with the host, not the agent image, so this is the
	// per-node view that makes mixed clusters visible mid-upgrade.
	// +optional
	DRBDVersion string `json:"drbdVersion,omitempty"`
	// ObservedAt is when these figures were last sampled; the controller
	// ignores stats older than a few poll intervals as unknown.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
	// Conditions follow the standard Kubernetes condition conventions;
	// PoolUsageHigh fires at the 80% data/metadata warn line (any pool).
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Pool returns the named pool's capacity entry, or nil. Empty means the
// default pool, matching the CRD adoption rule for pre-multi-pool objects.
// A status still published in the pre-multi-pool flat shape (an agent not
// yet rolled to this release) reads as the default pool.
func (s MiroirNodeStatus) Pool(name string) *MiroirNodePoolStatus {
	if name == "" {
		name = DefaultPoolName
	}
	for i := range s.Pools {
		if s.Pools[i].Name == name {
			return &s.Pools[i]
		}
	}
	if name == DefaultPoolName && len(s.Pools) == 0 && s.CapacityBytes > 0 {
		return &MiroirNodePoolStatus{
			Name:            DefaultPoolName,
			CapacityBytes:   s.CapacityBytes,
			AllocatedBytes:  s.AllocatedBytes,
			MetaUsedPercent: s.MetaUsedPercent,
		}
	}
	return nil
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=min
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pools",type=string,JSONPath=`.spec.pools[*].name`
// +kubebuilder:printcolumn:name="Zone",type=string,JSONPath=`.spec.zone`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.pools[*].capacityBytes`
// +kubebuilder:printcolumn:name="Allocated",type=string,JSONPath=`.status.pools[*].allocatedBytes`
// +kubebuilder:printcolumn:name="DRBD",type=string,JSONPath=`.status.drbdVersion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MiroirNode declares one storage node's topology and publishes its pool
// capacities. Named after the node; the spec is authored through the Helm
// chart's `nodes` values, the status is written by that node's agent, and
// the controller reads both at placement.
type MiroirNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MiroirNodeSpec   `json:"spec,omitempty"`
	Status MiroirNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MiroirNodeList contains a list of MiroirNode.
type MiroirNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MiroirNode `json:"items"`
}
