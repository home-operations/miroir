package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MiroirSnapshotGroupSpec is the desired state, created by the controller
// on CSI CreateVolumeGroupSnapshot.
type MiroirSnapshotGroupSpec struct {
	// SnapshotNames lists the member MiroirSnapshots (one per source
	// volume, each carrying this group's name in spec.group). Immutable:
	// the group barrier is cut atomically over exactly this member set,
	// so growing or shrinking it after the fact would misrepresent what
	// the cut covered.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="snapshotNames is immutable: the group barrier was cut over exactly this member set"
	SnapshotNames []string `json:"snapshotNames"`
}

// MiroirSnapshotGroupStatus is the shared state of the group's write
// barrier round. One MiroirSnapshotGroup is one round spanning every
// diskful leg of every member volume — the multi-volume generalization
// of the MiroirSnapshot per-node round, kept on a single object so that
// sealing and voiding the round serialize on one resourceVersion.
type MiroirSnapshotGroupStatus struct {
	// ReadyToUse is true once every diskful leg of every member volume
	// holds its snapshot, all cut inside one shared write barrier.
	// +optional
	ReadyToUse bool `json:"readyToUse"`
	// PerLeg maps "<volume>/<node>" to that leg's progress through the
	// group barrier round.
	// +optional
	PerLeg map[string]SnapshotNodeState `json:"perLeg,omitempty"`
	// IOSuspended is the group-wide write barrier: set by the round
	// driver after suspending its local member legs, cleared on resume.
	// Legs are cut only while it holds and every slot reports Suspended.
	// +optional
	IOSuspended bool `json:"ioSuspended"`
	// SuspendedAt bounds the barrier: the driver voids the round at the
	// deadline even if a leg never cuts, so a stuck member cannot freeze
	// every volume in the group forever.
	// +optional
	SuspendedAt *metav1.Time `json:"suspendedAt,omitempty"`
	// Conditions follow the standard Kubernetes condition conventions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=misg
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.readyToUse`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MiroirSnapshotGroup is one crash-consistent point-in-time snapshot cut
// across several MiroirVolumes at once (1:1 with a
// VolumeGroupSnapshotContent). Its members are ordinary MiroirSnapshots
// that restore individually; the group coordinates their cut under one
// shared write barrier.
type MiroirSnapshotGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MiroirSnapshotGroupSpec   `json:"spec"`
	Status MiroirSnapshotGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MiroirSnapshotGroupList contains a list of MiroirSnapshotGroup.
type MiroirSnapshotGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MiroirSnapshotGroup `json:"items"`
}
