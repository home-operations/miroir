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
	"maps"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/nodemap"
)

const (
	nodeKharkiv = "kharkiv"
	nodeParis   = "paris"
	nodeOslo    = "oslo"
	addrKharkiv = "192.168.1.41"
	addrParis   = "192.168.1.42"
	addrOslo    = "192.168.1.43"
	volPvc1     = "pvc-1"
	volSrc      = "pvc-src"
	volNew      = "pvc-new"
	snapSnap1   = "snap-1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

var testNodes = nodemap.Map{
	nodeKharkiv: nodemap.Node{Backend: miroirv1alpha1.BackendLVMThin, Device: "/dev/disk/by-partlabel/r-miroir"},
	nodeParis:   nodemap.Node{Backend: miroirv1alpha1.BackendZFS, ZFSDataset: "data-pool/miroir"},
}

// readyOnGet flips a created volume to Ready, simulating the agent.
// NB: depends on the fake client returning the same object pointer.
func readyOnGet(s *runtime.Scheme) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}, &miroirv1alpha1.MiroirSnapshot{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if vol, ok := obj.(*miroirv1alpha1.MiroirVolume); ok {
					vol.Status.Phase = miroirv1alpha1.VolumeReady
				}
				return nil
			},
		}).
		Build()
}

// degradedOnGet flips a created volume to Degraded, simulating a replicated
// volume whose primary is UpToDate while a secondary still runs its initial
// sync. NB: depends on the fake client returning the same object pointer.
func degradedOnGet(s *runtime.Scheme) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}, &miroirv1alpha1.MiroirSnapshot{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if vol, ok := obj.(*miroirv1alpha1.MiroirVolume); ok {
					vol.Status.Phase = miroirv1alpha1.VolumeDegraded
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

	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeKharkiv}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.VolumeId != volPvc1 || resp.Volume.CapacityBytes != 5<<30 {
		t.Fatalf("unexpected volume %+v", resp.Volume)
	}
	if got := resp.Volume.AccessibleTopology[0].Segments[constants.TopologyKey]; got != nodeKharkiv {
		t.Fatalf("expected placement on kharkiv, got %s", got)
	}

	// The CR must exist with the right backend for the chosen node.
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: volPvc1}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.Replicas[0].Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("expected lvmthin backend, got %s", vol.Spec.Replicas[0].Backend)
	}
}

func TestCreateVolumeIdempotent(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}
	req := &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	}

	if _, err := c.CreateVolume(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateVolume(t.Context(), req); err != nil {
		t.Fatalf("second identical CreateVolume must succeed: %v", err)
	}

	req.CapacityRange = &csi.CapacityRange{RequiredBytes: 6 << 30}
	_, err := c.CreateVolume(t.Context(), req)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("size mismatch must be ALREADY_EXISTS, got %v", err)
	}
}

func TestCreateVolumeSucceedsWhenDegraded(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: degradedOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	// Degraded means the primary is UpToDate and serving; CreateVolume must
	// return without waiting for the secondary's initial sync.
	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
	if err != nil {
		t.Fatalf("Degraded volume must provision successfully: %v", err)
	}
	if resp.Volume.VolumeId != volPvc1 {
		t.Fatalf("unexpected volume %+v", resp.Volume)
	}
}

func rwxCaps() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"}},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}}
}

func TestCreateVolumeRWXSetsExport(t *testing.T) {
	s := newScheme(t)
	// RWX is replicated (>=2), so placement resolves each replica's address
	// from its Node's InternalIP — the fake client needs the Node objects.
	cl := readyOnGet(s)
	for _, n := range []*corev1.Node{nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, addrParis)} {
		if err := cl.Create(t.Context(), n); err != nil {
			t.Fatal(err)
		}
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second, DRBDPortBase: 7000}

	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: rwxCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	// An RWX PV carries no node affinity — consumers mount NFS from any node.
	if len(resp.Volume.AccessibleTopology) != 0 {
		t.Fatalf("RWX volume must have no accessible topology, got %v", resp.Volume.AccessibleTopology)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: volPvc1}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.Export == nil || vol.Spec.Export.FSType != "xfs" {
		t.Fatalf("expected export spec fsType=xfs, got %+v", vol.Spec.Export)
	}
}

func TestCreateVolumeRejectsRWXSingleReplica(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes}

	// RWX needs a second replica node for the gateway to fail over to.
	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: rwxCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "1"},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("single-replica RWX must be rejected, got %v", err)
	}
}

func TestCreateVolumeRejectsRWXBlock(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name: volPvc1,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RWX block must be rejected, got %v", err)
	}
}

func TestCreateVolumeRejectsRWXLastManStanding(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, DRBDPortBase: 7000}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: rwxCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2", constants.ParamQuorum: "last-man-standing"},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RWX with last-man-standing quorum must be rejected, got %v", err)
	}
}

// Confirming RWX access against a volume provisioned without an export
// gateway would promise access the mount path cannot deliver.
func TestValidateVolumeCapabilitiesRWXMismatch(t *testing.T) {
	s := newScheme(t)
	vol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv}},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(vol).Build()
	c := &Controller{Client: cl, Nodes: testNodes}

	resp, err := c.ValidateVolumeCapabilities(t.Context(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           volPvc1,
		VolumeCapabilities: rwxCaps(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Confirmed != nil {
		t.Fatalf("RWX must not be confirmed for a non-export volume, got %+v", resp.Confirmed)
	}
	if resp.Message == "" {
		t.Fatal("expected a rejection message")
	}
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes}

	if _, err := c.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: "nope"}); err != nil {
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
			WithObjects(nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, "192.168.1.42")).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if vol, ok := obj.(*miroirv1alpha1.MiroirVolume); ok {
						vol.Status.Phase = miroirv1alpha1.VolumeReady
					}
					return nil
				},
			}).Build(),
		Nodes:            testNodes,
		ProvisionTimeout: 2 * time.Second,
	}

	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-r1",
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		Parameters: map[string]string{
			constants.ParamReplicas: "2",
			constants.ParamQuorum:   string(miroirv1alpha1.QuorumLastManStanding),
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeParis}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Volume.AccessibleTopology) != 2 {
		t.Fatalf("topology must cover both replica nodes: %+v", resp.Volume.AccessibleTopology)
	}

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-r1"}, vol); err != nil {
		t.Fatal(err)
	}
	if len(vol.Spec.Replicas) != 2 {
		t.Fatalf("replicas = %d", len(vol.Spec.Replicas))
	}
	// Scheduler preference first → paris is replicas[0] (the GI winner).
	if vol.Spec.Replicas[0].Node != nodeParis || vol.Spec.Replicas[0].NodeID != 0 {
		t.Fatalf("unexpected first replica %+v", vol.Spec.Replicas[0])
	}
	if vol.Spec.Replicas[0].Address != "192.168.1.42" ||
		vol.Spec.Replicas[1].Address != addrKharkiv {
		t.Fatalf("InternalIPs not resolved: %+v", vol.Spec.Replicas)
	}
	if vol.Spec.DRBD == nil || vol.Spec.DRBD.Port != 7000 {
		t.Fatalf("unexpected DRBD allocation %+v", vol.Spec.DRBD)
	}
	if vol.Spec.QuorumPolicy != miroirv1alpha1.QuorumLastManStanding {
		t.Fatalf("quorum = %s", vol.Spec.QuorumPolicy)
	}
	if len(vol.Finalizers) != 2 {
		t.Fatalf("want per-node finalizers, got %v", vol.Finalizers)
	}

	// A second volume gets the next minor/port.
	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-r2",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
	}); err != nil {
		t.Fatal(err)
	}
	vol2 := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-r2"}, vol2); err != nil {
		t.Fatal(err)
	}
	if vol2.Spec.DRBD.Port != 7001 {
		t.Fatalf("allocator must advance: %+v", vol2.Spec.DRBD)
	}
}

// A node map address override pins that replica's endpoint; a node without
// one still falls back to its InternalIP — both can appear in one volume.
func TestCreateVolumeReplicatedAddressOverride(t *testing.T) {
	s := newScheme(t)
	nodes := maps.Clone(testNodes)
	paris := nodes[nodeParis]
	paris.Address = "10.0.100.42"
	nodes[nodeParis] = paris
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, addrParis)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if vol, ok := obj.(*miroirv1alpha1.MiroirVolume); ok {
						vol.Status.Phase = miroirv1alpha1.VolumeReady
					}
					return nil
				},
			}).Build(),
		Nodes:            nodes,
		ProvisionTimeout: 2 * time.Second,
	}

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-ovr",
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeParis}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-ovr"}, vol); err != nil {
		t.Fatal(err)
	}
	// paris preferred → replicas[0]; its override wins, kharkiv falls back.
	if vol.Spec.Replicas[0].Address != "10.0.100.42" ||
		vol.Spec.Replicas[1].Address != addrKharkiv {
		t.Fatalf("override/fallback not applied: %+v", vol.Spec.Replicas)
	}
}

// tieBreakerController is a 3-node controller whose fake client flips
// created volumes to Ready, for exercising the auto-tie-breaker path.
func tieBreakerController(t *testing.T, autoTieBreaker bool) *Controller {
	t.Helper()
	return &Controller{
		Client: fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, addrParis), nodeObj(nodeOslo, addrOslo)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if vol, ok := obj.(*miroirv1alpha1.MiroirVolume); ok {
						vol.Status.Phase = miroirv1alpha1.VolumeReady
					}
					return nil
				},
			}).Build(),
		Nodes: nodemap.Map{
			nodeKharkiv: {Backend: miroirv1alpha1.BackendLVMThin},
			nodeParis:   {Backend: miroirv1alpha1.BackendZFS, ZFSDataset: "data-pool/miroir"},
			nodeOslo:    {Backend: miroirv1alpha1.BackendLVMThin},
		},
		ProvisionTimeout: 2 * time.Second,
		AutoTieBreaker:   autoTieBreaker,
	}
}

// A 2-replica volume with the default quorum (now freeze) gets a diskless
// tie-breaker on the spare node (#70).
func TestCreateVolumeAutoTieBreaker(t *testing.T) {
	c := tieBreakerController(t, true)

	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-tb",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeKharkiv}},
				{Segments: map[string]string{constants.TopologyKey: nodeParis}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Volume.AccessibleTopology) != 2 {
		t.Fatalf("topology must cover only diskful nodes: %+v", resp.Volume.AccessibleTopology)
	}

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-tb"}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.QuorumPolicy != miroirv1alpha1.QuorumFreeze {
		t.Fatalf("default quorum must be freeze, got %s", vol.Spec.QuorumPolicy)
	}
	if len(vol.Spec.Replicas) != 3 {
		t.Fatalf("want 2 diskful + 1 tie-breaker, got %+v", vol.Spec.Replicas)
	}
	tb := vol.Spec.Replicas[2]
	if tb.Node != nodeOslo || !tb.Diskless || tb.NodeID != 2 || tb.Address != addrOslo {
		t.Fatalf("unexpected tie-breaker %+v", tb)
	}
	if tb.Backend != "" {
		t.Fatalf("tie-breaker must carry no backend: %+v", tb)
	}
	if len(vol.Finalizers) != 3 {
		t.Fatalf("tie-breaker node needs a teardown finalizer: %v", vol.Finalizers)
	}
}

// The diskless tie-breaker resolves its endpoint through the same path, so
// a node map override pins its replication address too.
func TestCreateVolumeAutoTieBreakerAddressOverride(t *testing.T) {
	c := tieBreakerController(t, true)
	c.Nodes[nodeOslo] = nodemap.Node{Backend: miroirv1alpha1.BackendLVMThin, Address: "10.0.100.44"}

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-tbo",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeKharkiv}},
				{Segments: map[string]string{constants.TopologyKey: nodeParis}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-tbo"}, vol); err != nil {
		t.Fatal(err)
	}
	if tb := vol.Spec.Replicas[2]; tb.Node != nodeOslo || tb.Address != "10.0.100.44" {
		t.Fatalf("tie-breaker did not take the override address: %+v", tb)
	}
}

func TestCreateVolumeAutoTieBreakerSkips(t *testing.T) {
	cases := map[string]struct {
		controller *Controller
		params     map[string]string
	}{
		"opted out": {
			controller: tieBreakerController(t, false),
			params:     map[string]string{constants.ParamReplicas: "2"},
		},
		"last-man-standing": {
			controller: tieBreakerController(t, true),
			params: map[string]string{
				constants.ParamReplicas: "2",
				constants.ParamQuorum:   string(miroirv1alpha1.QuorumLastManStanding),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := tc.controller.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
				Name:               "pvc-plain",
				VolumeCapabilities: volCaps(),
				Parameters:         tc.params,
			}); err != nil {
				t.Fatal(err)
			}
			vol := &miroirv1alpha1.MiroirVolume{}
			if err := tc.controller.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-plain"}, vol); err != nil {
				t.Fatal(err)
			}
			if len(vol.Spec.Replicas) != 2 {
				t.Fatalf("no tie-breaker expected: %+v", vol.Spec.Replicas)
			}
		})
	}
}

// With every storage node holding a replica there is nothing to place the
// tie-breaker on — the volume is created without one.
func TestCreateVolumeAutoTieBreakerNoSpareNode(t *testing.T) {
	c := tieBreakerController(t, true)
	c.Nodes = testNodes // kharkiv + paris only

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-nospare",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
	}); err != nil {
		t.Fatal(err)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-nospare"}, vol); err != nil {
		t.Fatal(err)
	}
	if len(vol.Spec.Replicas) != 2 || vol.Spec.QuorumPolicy != miroirv1alpha1.QuorumFreeze {
		t.Fatalf("want 2 diskful freeze replicas, got %+v (quorum %s)",
			vol.Spec.Replicas, vol.Spec.QuorumPolicy)
	}
}

// A retried expand whose earlier attempt already patched the spec but
// timed out must keep waiting on realized sizes — returning success
// early lets kubelet no-op the filesystem grow against the old device
// size and record the PVC expanded while the fs stays small.
func TestControllerExpandRetryWaitsForRealization(t *testing.T) {
	s := newScheme(t)
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 10 << 30, // a prior attempt already grew the spec
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin},
				{Node: nodeParis, Backend: miroirv1alpha1.BackendZFS},
			},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeKharkiv: {SizeBytes: 5 << 30},
				nodeParis:   {SizeBytes: 5 << 30},
			},
		},
	}
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(v).
			WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build(),
		ProvisionTimeout: time.Second,
	}

	_, err := c.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      volPvc1,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 10 << 30},
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("retry must wait for realization, got %v", err)
	}
}

func TestControllerExpandSucceedsOnceRealized(t *testing.T) {
	s := newScheme(t)
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 10 << 30,
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin},
			},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeKharkiv: {SizeBytes: 10 << 30},
			},
		},
	}
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(v).
			WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build(),
		ProvisionTimeout: time.Second,
	}

	// The idempotent retry (spec already at size, devices realized).
	resp, err := c.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      volPvc1,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 10 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.CapacityBytes != 10<<30 {
		t.Fatalf("capacity = %d", resp.CapacityBytes)
	}

	// A stale smaller request must never shrink and reports the spec size.
	resp, err = c.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      volPvc1,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.CapacityBytes != 10<<30 {
		t.Fatalf("stale request must report the larger spec size, got %d", resp.CapacityBytes)
	}
}

// Restore copies the source's replica layout but must clean the entries:
// FullSync stripped (clones are byte-identical on every leg) and the
// replication addresses re-resolved from the live Node objects.
func TestCreateVolumeFromSnapshotCleansReplicas(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes:    5 << 30,
			QuorumPolicy: miroirv1alpha1.QuorumFreeze,
			DRBD:         &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0, Address: "10.0.0.1"},
				// paris joined after creation: FullSync stuck in the spec.
				{Node: nodeParis, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "10.0.0.2", FullSync: true},
			},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{
				nodeKharkiv: miroirv1alpha1.SnapshotDone,
				nodeParis:   miroirv1alpha1.SnapshotDone,
			}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap,
		nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, addrParis)} {
		if err := cl.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volNew,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapSnap1},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: volNew}, got); err != nil {
		t.Fatal(err)
	}
	for _, rep := range got.Spec.Replicas {
		if rep.FullSync {
			t.Fatalf("clone must not inherit FullSync: %+v", rep)
		}
	}
	if got.Spec.Replicas[0].Address != addrKharkiv || got.Spec.Replicas[1].Address != addrParis {
		t.Fatalf("addresses must be re-resolved: %+v", got.Spec.Replicas)
	}
}

// A clone re-resolves addresses through the node map, so an override that
// was configured after the source volume was created supersedes the stale
// address the source persisted.
func TestCreateVolumeFromSnapshotAddressOverride(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes:    5 << 30,
			QuorumPolicy: miroirv1alpha1.QuorumFreeze,
			DRBD:         &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0, Address: "192.0.2.1"},
				{Node: nodeParis, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "192.0.2.2"},
			},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{
				nodeKharkiv: miroirv1alpha1.SnapshotDone,
				nodeParis:   miroirv1alpha1.SnapshotDone,
			}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap,
		nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, addrParis)} {
		if err := cl.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	nodes := maps.Clone(testNodes)
	kharkiv := nodes[nodeKharkiv]
	kharkiv.Address = "10.0.100.1"
	nodes[nodeKharkiv] = kharkiv
	c := &Controller{Client: cl, ProvisionTimeout: 2 * time.Second, Nodes: nodes}

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volNew,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapSnap1},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: volNew}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas[0].Address != "10.0.100.1" || got.Spec.Replicas[1].Address != addrParis {
		t.Fatalf("clone did not re-resolve through the override: %+v", got.Spec.Replicas)
	}
}

// A restore whose source gained a replica AFTER the snapshot was cut: the
// new node holds no local snapshot, so its clone leg must be marked
// FullSync (fresh backing, synced from a Done peer) instead of failing
// CreateFromSnapshot and flipping the clone to Failed.
func TestCreateVolumeFromSnapshotFullSyncsPostSnapshotReplica(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes:    5 << 30,
			QuorumPolicy: miroirv1alpha1.QuorumFreeze,
			DRBD:         &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0, Address: "10.0.0.1"},
				{Node: nodeParis, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "10.0.0.2"},
			},
		},
	}
	// The snapshot captured only kharkiv Done; paris was added afterward.
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeKharkiv: miroirv1alpha1.SnapshotDone}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap,
		nodeObj(nodeKharkiv, addrKharkiv), nodeObj(nodeParis, addrParis)} {
		if err := cl.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volNew,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapSnap1},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := cl.Get(t.Context(), types.NamespacedName{Name: volNew}, got); err != nil {
		t.Fatal(err)
	}
	byNode := map[string]miroirv1alpha1.Replica{}
	for _, r := range got.Spec.Replicas {
		byNode[r.Node] = r
	}
	if byNode[nodeKharkiv].FullSync {
		t.Fatalf("the Done seed leg must clone, not full-sync: %+v", byNode[nodeKharkiv])
	}
	if !byNode[nodeParis].FullSync {
		t.Fatalf("the post-snapshot leg must full-sync (no local snapshot): %+v", byNode[nodeParis])
	}
}

// A lagging informer after AlreadyExists must surface as Unavailable
// (retryable) — not Internal, which the provisioner treats as final and
// would record "volume not created" for a volume that exists.
func TestCreateVolumeAlreadyExistsCacheLagIsUnavailable(t *testing.T) {
	s := newScheme(t)
	// APIReader has the volume; the cached Client (interceptor) 404s it,
	// simulating an informer that has not caught up.
	existing := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	apiReader := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	cached := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return apierrors.NewAlreadyExists(
					miroirv1alpha1.GroupVersion.WithResource("miroirvolumes").GroupResource(), volPvc1)
			},
		}).Build()
	c := &Controller{Client: cached, APIReader: apiReader, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	// Same shape as the existing volume, so handleCreateErr reaches the Get.
	err := c.handleCreateErr(t.Context(),
		apierrors.NewAlreadyExists(miroirv1alpha1.GroupVersion.WithResource("miroirvolumes").GroupResource(), volPvc1),
		&miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: volPvc1}},
		5<<30, 1, miroirv1alpha1.QuorumLastManStanding, "")
	if err != nil {
		t.Fatalf("APIReader has the volume; compatible retry must succeed: %v", err)
	}
}

// markVolumeFormatted runs right after Create, when the informer cache
// reliably lags the just-created object: it must read through APIReader,
// or every fresh clone fails CreateVolume into a retry loop.
func TestMarkVolumeFormattedReadsThroughAPIReader(t *testing.T) {
	s := newScheme(t)
	existing := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	// One store: reads through the cached client 404 (informer lag), while
	// APIReader and the write path see the real object.
	real := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build()
	cached := interceptor.NewClient(real, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return apierrors.NewNotFound(
				miroirv1alpha1.GroupVersion.WithResource("miroirvolumes").GroupResource(), volPvc1)
		},
	})
	c := &Controller{Client: cached, APIReader: real, Nodes: testNodes}

	if err := c.markVolumeFormatted(t.Context(), volPvc1); err != nil {
		t.Fatalf("APIReader has the volume; formatted flag must be recorded: %v", err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := real.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Formatted {
		t.Fatal("formatted flag must be recorded on the volume")
	}
}

func TestPickNodeNoStorageNodes(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).Build(), Nodes: nodemap.Map{}}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: volCaps(),
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected RESOURCE_EXHAUSTED, got %v", err)
	}
}

func TestCreateVolumeFromSnapshotEchoesContentSource(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeKharkiv: miroirv1alpha1.SnapshotDone}},
	}
	cl := readyOnGet(s)
	if err := cl.Create(t.Context(), srcVol); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	// The status subresource strips status on create.
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volNew,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapSnap1},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.ContentSource == nil {
		t.Fatal("ContentSource must be set on snapshot-restore responses")
	}
	if got := resp.Volume.ContentSource.GetSnapshot().GetSnapshotId(); got != snapSnap1 {
		t.Fatalf("ContentSource.Snapshot.SnapshotId = %q, want snap-1", got)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: volNew}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.Source == nil || vol.Spec.Source.SnapshotName != snapSnap1 {
		t.Fatalf("new volume must record source snapshot, got %+v", vol.Spec.Source)
	}
	if len(vol.Spec.Replicas) != 1 || vol.Spec.Replicas[0].Node != nodeKharkiv {
		t.Fatalf("placement must follow source volume, got %+v", vol.Spec.Replicas)
	}
}

// A restore of a formatted source must inherit Formatted before any pod
// stages it — a blank clone is then refused instead of mkfs'd.
func TestCreateVolumeFromSnapshotInheritsFormatted(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{
			ReadyToUse: true, SizeBytes: 5 << 30, SourceFormatted: true,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeKharkiv: miroirv1alpha1.SnapshotDone},
		},
	}
	cl := readyOnGet(s)
	if err := cl.Create(t.Context(), srcVol); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	// The status subresource strips status on create.
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volNew,
		VolumeCapabilities: volCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapSnap1},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: volNew}, vol); err != nil {
		t.Fatal(err)
	}
	if !vol.Status.Formatted {
		t.Fatal("restored volume must inherit Formatted from the snapshot source")
	}
}

func TestCreateVolumeRejectsFourReplicas(t *testing.T) {
	// Exceeding --max-peers headroom; fail loudly.
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}
	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
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
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	cl := readyOnGet(s)
	if err := cl.Create(t.Context(), srcVol); err != nil {
		t.Fatal(err)
	}
	snap1 := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeKharkiv: miroirv1alpha1.SnapshotDone}},
	}
	snap2 := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-2"},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeKharkiv: miroirv1alpha1.SnapshotDone}},
	}
	if err := cl.Create(t.Context(), snap1); err != nil {
		t.Fatal(err)
	}
	if err := cl.Status().Update(t.Context(), snap1); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(t.Context(), snap2); err != nil {
		t.Fatal(err)
	}
	if err := cl.Status().Update(t.Context(), snap2); err != nil {
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
	if _, err := c.CreateVolume(t.Context(), mk(snapSnap1)); err != nil {
		t.Fatal(err)
	}
	// Same name, different source snapshot → ALREADY_EXISTS, not silent
	// re-pointing of the existing CR's source field.
	_, err := c.CreateVolume(t.Context(), mk("snap-2"))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("source change must be ALREADY_EXISTS, got %v", err)
	}
}
