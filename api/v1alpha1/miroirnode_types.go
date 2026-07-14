package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MiroirNodeSpec records which backend a storage node runs, mirrored from
// the node map so `kubectl get miroirnodes` reads without the ConfigMap.
type MiroirNodeSpec struct {
	// Backend is the storage implementation backing this node's pool.
	// +optional
	Backend BackendType `json:"backend,omitempty"`
}

// MiroirNodeStatus is the pool capacity this node's agent publishes for
// capacity-aware placement and overcommit guardrails.
// On a shared pool (e.g. ZFS shared with OpenEBS) the figures are
// pool-level, so a co-tenant's growth correctly shrinks miroir's headroom.
type MiroirNodeStatus struct {
	// CapacityBytes is the total size of the node-local pool.
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
	// ObservedAt is when these figures were last sampled; the controller
	// ignores stats older than a few poll intervals as unknown.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
	// Conditions follow the standard Kubernetes condition conventions;
	// PoolUsageHigh fires at the 80% data/metadata warn line.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=min
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.backend`
// +kubebuilder:printcolumn:name="Capacity",type=integer,JSONPath=`.status.capacityBytes`
// +kubebuilder:printcolumn:name="Allocated",type=integer,JSONPath=`.status.allocatedBytes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MiroirNode publishes one storage node's pool capacity. Named after the
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
