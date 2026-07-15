package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MiroirNodePool is the per-pool slice of a node's spec: which backend
// implementation the named pool runs, mirrored from the node map so
// `kubectl get miroirnodes` reads without the ConfigMap.
type MiroirNodePool struct {
	// Name is the pool name from the node map ("default" for the
	// pre-multi-pool single pool).
	Name string `json:"name"`
	// Backend is the storage implementation backing this pool.
	// +optional
	Backend BackendType `json:"backend,omitempty"`
}

// MiroirNodeSpec records which pools a storage node runs.
type MiroirNodeSpec struct {
	// Pools lists this node's storage pools, one entry per pool.
	// +optional
	// +listType=map
	// +listMapKey=name
	Pools []MiroirNodePool `json:"pools,omitempty"`
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
func (s MiroirNodeStatus) Pool(name string) *MiroirNodePoolStatus {
	if name == "" {
		name = "default"
	}
	for i := range s.Pools {
		if s.Pools[i].Name == name {
			return &s.Pools[i]
		}
	}
	return nil
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=min
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pools",type=string,JSONPath=`.spec.pools[*].name`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.pools[*].capacityBytes`
// +kubebuilder:printcolumn:name="Allocated",type=string,JSONPath=`.status.pools[*].allocatedBytes`
// +kubebuilder:printcolumn:name="DRBD",type=string,JSONPath=`.status.drbdVersion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MiroirNode publishes one storage node's pool capacities. Named after the
// node; written by that node's agent, read by the controller at placement.
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
