package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotNodeState tracks one node's progress through the snapshot barrier.
type SnapshotNodeState string

const (
	SnapshotPending SnapshotNodeState = "Pending"
	SnapshotDone    SnapshotNodeState = "Done"
	SnapshotError   SnapshotNodeState = "Error"
)

// HomefsSnapshotSpec is the desired state, created by the controller on
// CSI CreateSnapshot.
type HomefsSnapshotSpec struct {
	// VolumeName references the HomefsVolume this snapshot captures.
	VolumeName string `json:"volumeName"`
}

// HomefsSnapshotStatus is the observed state aggregated from node agents.
// The snapshot exists as a backend CoW snapshot on every replica of the
// source volume (DESIGN.md §4.5.4).
type HomefsSnapshotStatus struct {
	// ReadyToUse is true once every replica holds the snapshot.
	// +optional
	ReadyToUse bool `json:"readyToUse"`
	// PerNode maps node name to that agent's snapshot progress.
	// +optional
	PerNode map[string]SnapshotNodeState `json:"perNode,omitempty"`
	// SizeBytes is the virtual size of the snapshotted volume, used by
	// the CSI layer to size restored volumes.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// IOSuspended is the replicated-snapshot write barrier: set by the
	// coordinator after suspend-io, cleared on resume. Peers snapshot
	// only while it holds.
	// +optional
	IOSuspended bool `json:"ioSuspended"`
	// SuspendedAt bounds the barrier: the coordinator resumes IO at the
	// deadline even if a peer never snapshots.
	// +optional
	SuspendedAt *metav1.Time `json:"suspendedAt,omitempty"`
	// Conditions follow the standard Kubernetes condition conventions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=hfs
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Volume",type=string,JSONPath=`.spec.volumeName`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.readyToUse`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HomefsSnapshot is one crash-consistent point-in-time snapshot of a
// HomefsVolume (1:1 with a VolumeSnapshotContent).
type HomefsSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HomefsSnapshotSpec   `json:"spec"`
	Status HomefsSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HomefsSnapshotList contains a list of HomefsSnapshot.
type HomefsSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HomefsSnapshot `json:"items"`
}
