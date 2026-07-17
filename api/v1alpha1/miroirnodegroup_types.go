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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeGroupLabel marks a MiroirNode as materialized by the named
// MiroirNodeGroup. It is provenance, not ownership for garbage
// collection: removing a node from a group (or deleting the group)
// orphans its MiroirNodes in place — topology is never deleted out from
// under live volumes.
const NodeGroupLabel = "miroir.home-operations.com/node-group"

// NodeAddressAnnotation on a corev1.Node supplies that node's dedicated
// replication address (storage NIC/VLAN IP) to group-materialized
// MiroirNodes. Addresses are per-node facts, so a group template cannot
// carry them; direct MiroirNodes set spec.address instead.
const NodeAddressAnnotation = "miroir.home-operations.com/address"

// MiroirNodeGroupSpec selects a set of nodes and the MiroirNode spec they
// share.
// +kubebuilder:validation:XValidation:rule="!has(self.template.address) || size(self.template.address) == 0",message="address is a per-node fact: annotate the Node with miroir.home-operations.com/address, or author a direct MiroirNode"
type MiroirNodeGroupSpec struct {
	// NodeSelector picks the member nodes by label. An empty selector
	// matches every node in the cluster (the Kubernetes convention).
	NodeSelector metav1.LabelSelector `json:"nodeSelector"`
	// Template is the MiroirNode spec applied to every member, with two
	// per-node facts resolved from the Node object: an empty zone
	// inherits the node's topology.kubernetes.io/zone label, and the
	// replication address comes from the node's
	// miroir.home-operations.com/address annotation.
	Template MiroirNodeSpec `json:"template"`
}

// Condition types reported on a MiroirNodeGroup.
const (
	// ConditionGroupConflict is True while another manager (a direct
	// MiroirNode or another group) already holds a matching node's
	// MiroirNode; the group skips those nodes.
	ConditionGroupConflict = "Conflict"
	// ConditionGroupOrphaned is True while MiroirNodes this group
	// materialized no longer match its selector. They are deliberately
	// left in place; decommissioning is an explicit
	// `kubectl delete miroirnode <name>`.
	ConditionGroupOrphaned = "OrphanedMembers"
)

// MiroirNodeGroupStatus reports the group's materialized membership.
type MiroirNodeGroupStatus struct {
	// Nodes lists the member nodes whose MiroirNode this group currently
	// manages, sorted by name.
	// +optional
	Nodes []string `json:"nodes,omitempty"`
	// ObservedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions follow the standard Kubernetes condition conventions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=ming
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Members",type=string,JSONPath=`.status.nodes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 63",message="the group name becomes a label value on materialized MiroirNodes: 63 characters maximum"

// MiroirNodeGroup materializes one MiroirNode per label-matched node, so a
// fleet sharing a storage layout (a device-path convention, a common ZFS
// dataset) is one object and joining it is labeling the node. Direct
// MiroirNodes always win over groups, and membership changes never delete
// a MiroirNode — members that stop matching are orphaned in place.
type MiroirNodeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MiroirNodeGroupSpec   `json:"spec,omitempty"`
	Status MiroirNodeGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MiroirNodeGroupList contains a list of MiroirNodeGroup.
type MiroirNodeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MiroirNodeGroup `json:"items"`
}
