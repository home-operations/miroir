package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackendType selects the node-local storage backend for a replica.
// +kubebuilder:validation:Enum=lvmthin;zfs
type BackendType string

const (
	BackendLVMThin BackendType = "lvmthin"
	BackendZFS     BackendType = "zfs"
)

// QuorumPolicy selects the 2-node consistency mode (DESIGN.md §3.2).
// +kubebuilder:validation:Enum=last-man-standing;freeze
type QuorumPolicy string

const (
	QuorumLastManStanding QuorumPolicy = "last-man-standing"
	QuorumFreeze          QuorumPolicy = "freeze"
)

// VolumePhase is the coarse lifecycle state of a volume.
type VolumePhase string

const (
	VolumeCreating VolumePhase = "Creating"
	VolumeReady    VolumePhase = "Ready"
	VolumeDegraded VolumePhase = "Degraded"
	VolumeFailed   VolumePhase = "Failed"
)

// Replica is one placement of the volume's data (or, later, a DRBD peer).
type Replica struct {
	// Node is the Kubernetes node name hosting this replica.
	Node string `json:"node"`
	// Backend selects how the backing device is provisioned on this node.
	Backend BackendType `json:"backend"`
}

// DRBDSpec carries the cluster-unique DRBD identifiers allocated by the
// controller. Unset for replicas:1 volumes (no replication layer).
type DRBDSpec struct {
	// Minor is the DRBD device minor number (device /dev/drbd<minor>).
	// +kubebuilder:validation:Minimum=0
	Minor int32 `json:"minor"`
	// Port is the TCP port for this resource's replication links.
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// VolumeSource provisions the volume's content from an existing snapshot.
type VolumeSource struct {
	// SnapshotName references a HomefsSnapshot by name.
	SnapshotName string `json:"snapshotName"`
}

// HomefsVolumeSpec is the desired state, written by the controller at
// CreateVolume time and reconciled by node agents.
type HomefsVolumeSpec struct {
	// SizeBytes is the provisioned (virtual, thin) size of the volume.
	// +kubebuilder:validation:Minimum=1
	SizeBytes int64 `json:"sizeBytes"`
	// Replicas lists the placement of the volume. Exactly one entry for
	// unreplicated volumes; two or more once DRBD replication lands (M2).
	// +kubebuilder:validation:MinItems=1
	Replicas []Replica `json:"replicas"`
	// QuorumPolicy applies only when len(Replicas) > 1.
	// +optional
	QuorumPolicy QuorumPolicy `json:"quorumPolicy,omitempty"`
	// DRBD is set by the controller when len(Replicas) > 1.
	// +optional
	DRBD *DRBDSpec `json:"drbd,omitempty"`
	// Source, if set, provisions content from a snapshot (CoW clone).
	// +optional
	Source *VolumeSource `json:"source,omitempty"`
}

// ReplicaStatus is the per-node observed state, written by that node's agent.
type ReplicaStatus struct {
	// DeviceCreated is true once the backing device exists on the node.
	DeviceCreated bool `json:"deviceCreated,omitempty"`
	// DevicePath is the node-local path pods attach to (backing device for
	// replicas:1; /dev/drbd<minor> once replicated).
	DevicePath string `json:"devicePath,omitempty"`
	// SizeBytes is the currently realized size on this node, used to
	// acknowledge expansion.
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// Message carries the last reconcile error, if any.
	Message string `json:"message,omitempty"`
}

// HomefsVolumeStatus is the observed state aggregated from node agents.
type HomefsVolumeStatus struct {
	// Phase summarizes the volume state for the controller and humans.
	// +optional
	Phase VolumePhase `json:"phase,omitempty"`
	// PerNode maps node name to that agent's observed state.
	// +optional
	PerNode map[string]ReplicaStatus `json:"perNode,omitempty"`
	// Conditions follow the standard Kubernetes condition conventions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=hfv
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.spec.sizeBytes`
// +kubebuilder:printcolumn:name="Replicas",type=string,JSONPath=`.spec.replicas[*].node`,priority=1
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HomefsVolume is one provisioned volume (1:1 with a PV).
type HomefsVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HomefsVolumeSpec   `json:"spec"`
	Status HomefsVolumeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HomefsVolumeList contains a list of HomefsVolume.
type HomefsVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HomefsVolume `json:"items"`
}
