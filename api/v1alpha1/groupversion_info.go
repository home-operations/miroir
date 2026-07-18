// Package v1alpha1 contains API Schema definitions for the miroir v1alpha1 API group
// +kubebuilder:object:generate=true
// +kubebuilder:ac:generate=true
// +groupName=miroir.home-operations.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "miroir.home-operations.com", Version: "v1alpha1"}

	// SchemeGroupVersion is the client-gen-convention alias the generated
	// applyconfiguration package references.
	SchemeGroupVersion = GroupVersion

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&MiroirVolume{},
		&MiroirVolumeList{},
		&MiroirSnapshot{},
		&MiroirSnapshotList{},
		&MiroirSnapshotGroup{},
		&MiroirSnapshotGroupList{},
		&MiroirNode{},
		&MiroirNodeList{},
		&MiroirNodeGroup{},
		&MiroirNodeGroupList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
