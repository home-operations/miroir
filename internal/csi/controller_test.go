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

package csi

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/constants"
	"github.com/eleboucher/homefs/internal/nodemap"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := homefsv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

var testNodes = nodemap.Map{
	"kharkiv": nodemap.Node{Backend: homefsv1alpha1.BackendLVMThin, Device: "/dev/disk/by-partlabel/r-homefs"},
	"paris":   nodemap.Node{Backend: homefsv1alpha1.BackendZFS, ZFSDataset: "data-pool/homefs"},
}

// readyOnGet flips a created volume to Ready, simulating the agent.
// NB: depends on the fake client returning the same object pointer.
func readyOnGet(s *runtime.Scheme) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if vol, ok := obj.(*homefsv1alpha1.HomefsVolume); ok {
					vol.Status.Phase = homefsv1alpha1.VolumeReady
				}
				return nil
			},
		}).
		Build()
}

func volCaps() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}}
}

func TestCreateVolumePlacesOnPreferredNode(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	resp, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-1",
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: "kharkiv"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.VolumeId != "pvc-1" || resp.Volume.CapacityBytes != 5<<30 {
		t.Fatalf("unexpected volume %+v", resp.Volume)
	}
	if got := resp.Volume.AccessibleTopology[0].Segments[constants.TopologyKey]; got != "kharkiv" {
		t.Fatalf("expected placement on kharkiv, got %s", got)
	}

	// The CR must exist with the right backend for the chosen node.
	vol := &homefsv1alpha1.HomefsVolume{}
	if err := c.Client.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.Replicas[0].Backend != homefsv1alpha1.BackendLVMThin {
		t.Fatalf("expected lvmthin backend, got %s", vol.Spec.Replicas[0].Backend)
	}
}

func TestCreateVolumeIdempotent(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}
	req := &csi.CreateVolumeRequest{
		Name:               "pvc-1",
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	}

	if _, err := c.CreateVolume(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("second identical CreateVolume must succeed: %v", err)
	}

	req.CapacityRange = &csi.CapacityRange{RequiredBytes: 6 << 30}
	_, err := c.CreateVolume(context.Background(), req)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("size mismatch must be ALREADY_EXISTS, got %v", err)
	}
}

func TestCreateVolumeRejectsRWX(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes}

	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-1",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RWX must be rejected, got %v", err)
	}
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes}

	if _, err := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "nope"}); err != nil {
		t.Fatalf("deleting absent volume must succeed: %v", err)
	}
}

func nodeObj(name, ip string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}},
		},
	}
}

func TestCreateVolumeReplicated(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(nodeObj("kharkiv", "192.168.1.41"), nodeObj("paris", "192.168.1.42")).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if vol, ok := obj.(*homefsv1alpha1.HomefsVolume); ok {
						vol.Status.Phase = homefsv1alpha1.VolumeReady
					}
					return nil
				},
			}).Build(),
		Nodes:            testNodes,
		ProvisionTimeout: 2 * time.Second,
	}

	resp, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-r1",
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		Parameters: map[string]string{
			constants.ParamReplicas: "2",
			constants.ParamQuorum:   "last-man-standing",
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: "paris"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Volume.AccessibleTopology) != 2 {
		t.Fatalf("topology must cover both replica nodes: %+v", resp.Volume.AccessibleTopology)
	}

	vol := &homefsv1alpha1.HomefsVolume{}
	if err := c.Client.Get(context.Background(), types.NamespacedName{Name: "pvc-r1"}, vol); err != nil {
		t.Fatal(err)
	}
	if len(vol.Spec.Replicas) != 2 {
		t.Fatalf("replicas = %d", len(vol.Spec.Replicas))
	}
	// Scheduler preference first → paris is replicas[0] (the GI winner).
	if vol.Spec.Replicas[0].Node != "paris" || vol.Spec.Replicas[0].NodeID != 0 {
		t.Fatalf("unexpected first replica %+v", vol.Spec.Replicas[0])
	}
	if vol.Spec.Replicas[0].Address != "192.168.1.42" ||
		vol.Spec.Replicas[1].Address != "192.168.1.41" {
		t.Fatalf("InternalIPs not resolved: %+v", vol.Spec.Replicas)
	}
	if vol.Spec.DRBD == nil || vol.Spec.DRBD.Port != 7000 {
		t.Fatalf("unexpected DRBD allocation %+v", vol.Spec.DRBD)
	}
	if vol.Spec.QuorumPolicy != homefsv1alpha1.QuorumLastManStanding {
		t.Fatalf("quorum = %s", vol.Spec.QuorumPolicy)
	}
	if len(vol.Finalizers) != 2 {
		t.Fatalf("want per-node finalizers, got %v", vol.Finalizers)
	}

	// A second volume gets the next minor/port.
	if _, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-r2",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
	}); err != nil {
		t.Fatal(err)
	}
	vol2 := &homefsv1alpha1.HomefsVolume{}
	if err := c.Client.Get(context.Background(), types.NamespacedName{Name: "pvc-r2"}, vol2); err != nil {
		t.Fatal(err)
	}
	if vol2.Spec.DRBD.Port != 7001 {
		t.Fatalf("allocator must advance: %+v", vol2.Spec.DRBD)
	}
}

func TestPickNodeNoStorageNodes(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).Build(), Nodes: nodemap.Map{}}

	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-1",
		VolumeCapabilities: volCaps(),
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected RESOURCE_EXHAUSTED, got %v", err)
	}
}

func TestCreateVolumeFromSnapshotEchoesContentSource(t *testing.T) {
	s := newScheme(t)
	srcVol := &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-src"},
		Spec: homefsv1alpha1.HomefsVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []homefsv1alpha1.Replica{{Node: "kharkiv", Backend: homefsv1alpha1.BackendLVMThin}},
		},
	}
	srcSnap := &homefsv1alpha1.HomefsSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1"},
		Spec:       homefsv1alpha1.HomefsSnapshotSpec{VolumeName: "pvc-src"},
		Status:     homefsv1alpha1.HomefsSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30},
	}
	cl := readyOnGet(s)
	if err := cl.Create(context.Background(), srcVol); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(context.Background(), srcSnap); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	resp, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-new",
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.ContentSource == nil {
		t.Fatal("ContentSource must be set on snapshot-restore responses")
	}
	if got := resp.Volume.ContentSource.GetSnapshot().GetSnapshotId(); got != "snap-1" {
		t.Fatalf("ContentSource.Snapshot.SnapshotId = %q, want snap-1", got)
	}
	vol := &homefsv1alpha1.HomefsVolume{}
	if err := c.Client.Get(context.Background(), types.NamespacedName{Name: "pvc-new"}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.Source == nil || vol.Spec.Source.SnapshotName != "snap-1" {
		t.Fatalf("new volume must record source snapshot, got %+v", vol.Spec.Source)
	}
	if len(vol.Spec.Replicas) != 1 || vol.Spec.Replicas[0].Node != "kharkiv" {
		t.Fatalf("placement must follow source volume, got %+v", vol.Spec.Replicas)
	}
}

func TestCreateVolumeRejectsFourReplicas(t *testing.T) {
	// Exceeding --max-peers headroom; fail loudly.
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}
	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-1",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "4"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("replicas=4 must be rejected, got %v", err)
	}
}

// ALREADY_EXISTS spec check: a re-issued CreateVolume with a different
// source snapshot must reject, not silently re-point the existing CR.
func TestCreateVolumeIdempotentRejectsSourceChange(t *testing.T) {
	s := newScheme(t)
	srcVol := &homefsv1alpha1.HomefsVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-src"},
		Spec: homefsv1alpha1.HomefsVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []homefsv1alpha1.Replica{{Node: "kharkiv", Backend: homefsv1alpha1.BackendLVMThin}},
		},
	}
	cl := readyOnGet(s)
	if err := cl.Create(context.Background(), srcVol); err != nil {
		t.Fatal(err)
	}
	snap1 := &homefsv1alpha1.HomefsSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1"},
		Spec:       homefsv1alpha1.HomefsSnapshotSpec{VolumeName: "pvc-src"},
		Status:     homefsv1alpha1.HomefsSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30},
	}
	snap2 := &homefsv1alpha1.HomefsSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-2"},
		Spec:       homefsv1alpha1.HomefsSnapshotSpec{VolumeName: "pvc-src"},
		Status:     homefsv1alpha1.HomefsSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30},
	}
	if err := cl.Create(context.Background(), snap1); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(context.Background(), snap2); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}
	mk := func(snapID string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:               "pvc-r",
			VolumeCapabilities: volCaps(),
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
			VolumeContentSource: &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapID},
				},
			},
		}
	}
	if _, err := c.CreateVolume(context.Background(), mk("snap-1")); err != nil {
		t.Fatal(err)
	}
	// Same name, different source snapshot → ALREADY_EXISTS, not silent
	// re-pointing of the existing CR's source field.
	_, err := c.CreateVolume(context.Background(), mk("snap-2"))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("source change must be ALREADY_EXISTS, got %v", err)
	}
}
