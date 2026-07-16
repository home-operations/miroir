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
	"strings"
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

// Pool names used across this package's tests.
const (
	poolDefault = "default"
	poolFast    = "fast"
)

const (
	nodeA      = "node-a"
	nodeB      = "node-b"
	nodeC      = "node-c"
	addrA      = "192.168.1.41"
	addrB      = "192.168.1.42"
	addrC      = "192.168.1.43"
	volPvc1    = "pvc-1"
	paramTrue  = "true"
	paramFalse = "false"
	volSrc     = "pvc-src"
	volNew     = "pvc-new"
	snapSnap1  = "snap-1"
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
	nodeA: storageNode(nodemap.Pool{Backend: miroirv1alpha1.BackendLVMThin, Device: "/dev/disk/by-partlabel/r-miroir"}),
	nodeB: storageNode(nodemap.Pool{Backend: miroirv1alpha1.BackendZFS, ZFSDataset: "data-pool/miroir"}),
}

// storageNode is a node-map entry with the given config as its single
// default pool — the pre-multi-pool shape most tests exercise.
func storageNode(pool nodemap.Pool) nodemap.Node {
	return nodemap.Node{Pools: map[string]nodemap.Pool{poolDefault: pool}}
}

// readyOnGet flips a created volume to Ready, simulating the agent.
// NB: depends on the fake client returning the same object pointer.
func readyOnGet(s *runtime.Scheme, objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
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
				{Segments: map[string]string{constants.TopologyKey: nodeA}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.VolumeId != volPvc1 || resp.Volume.CapacityBytes != 5<<30 {
		t.Fatalf("unexpected volume %+v", resp.Volume)
	}
	if got := resp.Volume.AccessibleTopology[0].Segments[constants.TopologyKey]; got != nodeA {
		t.Fatalf("expected placement on node-a, got %s", got)
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
	// Node objects back the replication-address resolution place() runs
	// for the 2-replica placement.
	c := &Controller{
		Client:           readyOnGet(s, nodeObj(nodeA, addrA), nodeObj(nodeB, addrB)),
		Nodes:            testNodes,
		ProvisionTimeout: 2 * time.Second,
		RWXEnabled:       true,
		DRBDPortBase:     7000,
	}

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
	// NFS consumers must never grow DRBD client legs: the remote-access
	// flag stays unset on export volumes even though the class default is
	// on, closing every client-leg path.
	if vol.Spec.AllowRemoteAccess {
		t.Fatal("export volumes must not capture allowRemoteAccess")
	}
}

func TestCreateVolumeRejectsRWXSingleReplica(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, RWXEnabled: true}

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
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, RWXEnabled: true, DRBDPortBase: 7000}

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

// With no gateway configured an RWX volume would carry a spec.export no
// reconciler ever serves; the request must fail at provision time instead.
// FailedPrecondition, not InvalidArgument: provisioning self-heals on retry
// once the gateway is enabled.
func TestCreateVolumeRejectsRWXWhenDisabled(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, DRBDPortBase: 7000}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: rwxCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 5 << 30},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RWX with the gateway disabled must be rejected with FailedPrecondition, got %v", err)
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
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA}},
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
		Client:           readyOnGet(s, nodeObj(nodeA, addrA), nodeObj(nodeB, "192.168.1.42")),
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
			// Strict locality: this test asserts the affinity-carrying path.
			constants.ParamAllowRemoteAccess: paramFalse,
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeB}},
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
	// Scheduler preference first → node-b is replicas[0] (the GI winner).
	if vol.Spec.Replicas[0].Node != nodeB || vol.Spec.Replicas[0].NodeID != 0 {
		t.Fatalf("unexpected first replica %+v", vol.Spec.Replicas[0])
	}
	if vol.Spec.Replicas[0].Address != "192.168.1.42" ||
		vol.Spec.Replicas[1].Address != addrA {
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
	entry := nodes[nodeB]
	entry.Address = "10.0.100.42"
	nodes[nodeB] = entry
	c := &Controller{
		Client:           readyOnGet(s, nodeObj(nodeA, addrA), nodeObj(nodeB, addrB)),
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
				{Segments: map[string]string{constants.TopologyKey: nodeB}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-ovr"}, vol); err != nil {
		t.Fatal(err)
	}
	// node-b preferred → replicas[0]; its override wins, node-a falls back.
	if vol.Spec.Replicas[0].Address != "10.0.100.42" ||
		vol.Spec.Replicas[1].Address != addrA {
		t.Fatalf("override/fallback not applied: %+v", vol.Spec.Replicas)
	}
}

// tieBreakerController is a 3-node controller whose fake client flips
// created volumes to Ready, for exercising the auto-tie-breaker path.
func tieBreakerController(t *testing.T, autoTieBreaker bool) *Controller {
	t.Helper()
	return &Controller{
		Client: readyOnGet(newScheme(t),
			nodeObj(nodeA, addrA), nodeObj(nodeB, addrB), nodeObj(nodeC, addrC)),
		Nodes: nodemap.Map{
			nodeA: storageNode(nodemap.Pool{Backend: miroirv1alpha1.BackendLVMThin}),
			nodeB: storageNode(nodemap.Pool{Backend: miroirv1alpha1.BackendZFS, ZFSDataset: "data-pool/miroir"}),
			nodeC: storageNode(nodemap.Pool{Backend: miroirv1alpha1.BackendLVMThin}),
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
		Parameters: map[string]string{
			constants.ParamReplicas: "2",
			// Strict locality: this test asserts the affinity-carrying path.
			constants.ParamAllowRemoteAccess: paramFalse,
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeA}},
				{Segments: map[string]string{constants.TopologyKey: nodeB}},
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
	if tb.Node != nodeC || !tb.Diskless || tb.NodeID != 2 || tb.Address != addrC {
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
	withAddr := storageNode(nodemap.Pool{Backend: miroirv1alpha1.BackendLVMThin})
	withAddr.Address = "10.0.100.44"
	c.Nodes.(nodemap.Map)[nodeC] = withAddr

	if _, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-tbo",
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKey: nodeA}},
				{Segments: map[string]string{constants.TopologyKey: nodeB}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-tbo"}, vol); err != nil {
		t.Fatal(err)
	}
	if tb := vol.Spec.Replicas[2]; tb.Node != nodeC || tb.Address != "10.0.100.44" {
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
	c.Nodes = testNodes // node-a + node-b only

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
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin},
				{Node: nodeB, Backend: miroirv1alpha1.BackendZFS},
			},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {SizeBytes: 5 << 30},
				nodeB: {SizeBytes: 5 << 30},
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
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin},
			},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {SizeBytes: 10 << 30},
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
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0, Address: "10.0.0.1"},
				// node-b joined after creation: FullSync stuck in the spec.
				{Node: nodeB, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "10.0.0.2", FullSync: true},
			},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{
				nodeA: miroirv1alpha1.SnapshotDone,
				nodeB: miroirv1alpha1.SnapshotDone,
			}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap,
		nodeObj(nodeA, addrA), nodeObj(nodeB, addrB)} {
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
	if got.Spec.Replicas[0].Address != addrA || got.Spec.Replicas[1].Address != addrB {
		t.Fatalf("addresses must be re-resolved: %+v", got.Spec.Replicas)
	}
}

// A restore whose class names a different pool than the source's replicas
// is refused with a pointed message: CoW clones cannot cross pools.
func TestCreateVolumeFromSnapshotRefusesCrossPool(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin, Pool: poolFast},
			},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeA: miroirv1alpha1.SnapshotDone}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap} {
		if err := cl.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Client: cl, Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	// The class names no pool → default, but the source lives in poolFast.
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
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-pool restore must be InvalidArgument, got %v", err)
	}
	if !strings.Contains(err.Error(), `pool "fast"`) {
		t.Fatalf("error must name the source pool: %v", err)
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
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0, Address: "192.0.2.1"},
				{Node: nodeB, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "192.0.2.2"},
			},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{
				nodeA: miroirv1alpha1.SnapshotDone,
				nodeB: miroirv1alpha1.SnapshotDone,
			}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap,
		nodeObj(nodeA, addrA), nodeObj(nodeB, addrB)} {
		if err := cl.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.Status().Update(t.Context(), srcSnap); err != nil {
		t.Fatal(err)
	}
	nodes := maps.Clone(testNodes)
	entry := nodes[nodeA]
	entry.Address = "10.0.100.1"
	nodes[nodeA] = entry
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
	if got.Spec.Replicas[0].Address != "10.0.100.1" || got.Spec.Replicas[1].Address != addrB {
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
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0, Address: "10.0.0.1"},
				{Node: nodeB, Backend: miroirv1alpha1.BackendZFS, NodeID: 1, Address: "10.0.0.2"},
			},
		},
	}
	// The snapshot captured only node-a Done; node-b was added afterward.
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeA: miroirv1alpha1.SnapshotDone}},
	}
	cl := readyOnGet(s)
	for _, obj := range []client.Object{srcVol, srcSnap,
		nodeObj(nodeA, addrA), nodeObj(nodeB, addrB)} {
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
	if byNode[nodeA].FullSync {
		t.Fatalf("the Done seed leg must clone, not full-sync: %+v", byNode[nodeA])
	}
	if !byNode[nodeB].FullSync {
		t.Fatalf("the post-snapshot leg must full-sync (no local snapshot): %+v", byNode[nodeB])
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
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin}},
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
		5<<30, 1, miroirv1alpha1.QuorumLastManStanding, "", poolDefault)
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
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin}},
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

// Nodes excluded by an address conflict still carry the pool; the refusal
// must name the conflict instead of blaming pool declarations the operator
// would find correct.
func TestCreateVolumeRefusalNamesAddressConflict(t *testing.T) {
	s := newScheme(t)
	conflicted := func(pool nodemap.Pool) nodemap.Node {
		n := storageNode(pool)
		n.Address, n.AddressConflict = "10.0.100.9", true
		return n
	}
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build(),
		Nodes: nodemap.Map{
			nodeA: conflicted(nodemap.Pool{Backend: miroirv1alpha1.BackendLVMThin}),
			nodeB: conflicted(nodemap.Pool{Backend: miroirv1alpha1.BackendLVMThin}),
		},
	}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: volCaps(),
		Parameters:         map[string]string{constants.ParamReplicas: "2"},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected RESOURCE_EXHAUSTED, got %v", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "address conflict") ||
		!strings.Contains(msg, "2 carrying it are excluded") {
		t.Fatalf("the refusal must name the address-conflict exclusion, got %q", msg)
	}
}

func TestCreateVolumeFromSnapshotEchoesContentSource(t *testing.T) {
	s := newScheme(t)
	srcVol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volSrc},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 5 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeA: miroirv1alpha1.SnapshotDone}},
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
	if len(vol.Spec.Replicas) != 1 || vol.Spec.Replicas[0].Node != nodeA {
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
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
	srcSnap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapSnap1},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{
			ReadyToUse: true, SizeBytes: 5 << 30, SourceFormatted: true,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeA: miroirv1alpha1.SnapshotDone},
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
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin}},
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
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeA: miroirv1alpha1.SnapshotDone}},
	}
	snap2 := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-2"},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volSrc},
		Status: miroirv1alpha1.MiroirSnapshotStatus{ReadyToUse: true, SizeBytes: 5 << 30,
			PerNode: map[string]miroirv1alpha1.SnapshotNodeState{nodeA: miroirv1alpha1.SnapshotDone}},
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

// allowRemoteVolumeAccess=true drops the PV's accessible topology (pods
// schedule anywhere; non-replica nodes attach a diskless client leg) and
// records the opt-in on the volume spec.
func TestCreateVolumeRemoteAccessDropsTopology(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client:           readyOnGet(s, nodeObj(nodeA, addrA), nodeObj(nodeB, addrB)),
		Nodes:            testNodes,
		ProvisionTimeout: 2 * time.Second,
	}

	resp, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               "pvc-remote",
		VolumeCapabilities: volCaps(),
		Parameters: map[string]string{
			constants.ParamReplicas:          "2",
			constants.ParamAllowRemoteAccess: paramTrue,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Volume.AccessibleTopology) != 0 {
		t.Fatalf("remote-access volume must carry no topology: %+v", resp.Volume.AccessibleTopology)
	}
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: "pvc-remote"}, vol); err != nil {
		t.Fatal(err)
	}
	if !vol.Spec.AllowRemoteAccess {
		t.Fatal("spec.allowRemoteAccess not recorded")
	}
}

func TestParseBitmapGranularity(t *testing.T) {
	cases := map[string]struct {
		raw      string
		replicas int
		want     int64
		wantErr  bool
	}{
		"absent means default":      {raw: "", replicas: 2, want: 0},
		"absent ok on unreplicated": {raw: "", replicas: 1, want: 0},
		"valid 64k":                 {raw: "65536", replicas: 2, want: 65536},
		"floor 4k":                  {raw: "4096", replicas: 2, want: 4096},
		"ceiling 1M":                {raw: "1048576", replicas: 2, want: 1048576},
		"rejected below 4k":         {raw: "2048", replicas: 2, wantErr: true},
		"rejected above 1M":         {raw: "2097152", replicas: 2, wantErr: true},
		"rejected non power of two": {raw: "65537", replicas: 2, wantErr: true},
		"rejected non-numeric":      {raw: "64k", replicas: 2, wantErr: true},
		"rejected on unreplicated":  {raw: "65536", replicas: 1, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			params := map[string]string{}
			if tc.raw != "" {
				params[constants.ParamBitmapGranularity] = tc.raw
			}
			got, err := parseBitmapGranularity(params, tc.replicas)
			if tc.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("want InvalidArgument, got %v", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("parseBitmapGranularity = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestParseAllowRemoteAccess(t *testing.T) {
	cases := map[string]struct {
		raw      string
		replicas int
		want     bool
		wantErr  bool
	}{
		"absent defaults on (replicated)":    {raw: "", replicas: 2, want: true},
		"absent defaults off (unreplicated)": {raw: "", replicas: 1, want: false},
		"explicit false":                     {raw: paramFalse, replicas: 2, want: false},
		"enabled on replicated":              {raw: paramTrue, replicas: 2, want: true},
		"rejected on unreplicated":           {raw: paramTrue, replicas: 1, wantErr: true},
		"rejected on invalid value":          {raw: "yes", replicas: 2, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			params := map[string]string{}
			if tc.raw != "" {
				params[constants.ParamAllowRemoteAccess] = tc.raw
			}
			got, err := parseAllowRemoteAccess(params, tc.replicas)
			if tc.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("want InvalidArgument, got %v", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("parseAllowRemoteAccess = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

// Expansion must honor the same capacity guardrails as CreateVolume: one
// PVC grown past the pool ENOSPCs every thin volume sharing it.
func TestControllerExpandRefusesBeyondHeadroom(t *testing.T) {
	s := newScheme(t)
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 10 << 30,
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin},
			},
		},
	}
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(v, miroirNodeObj(nodeA, 100*gib, 95*gib)).
			WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build(),
		ProvisionTimeout: time.Second,
	}

	_, err := c.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      volPvc1,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 10 << 40}, // 10 TiB
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a grow past the pool guardrails must refuse, got %v", err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Client.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.SizeBytes != 10<<30 {
		t.Fatalf("a refused grow must not touch the spec: %d", got.Spec.SizeBytes)
	}
}

// A node without fresh stats admits the grow, matching place().
func TestControllerExpandAdmitsWithoutStats(t *testing.T) {
	s := newScheme(t)
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 10 << 30,
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin},
			},
		},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {SizeBytes: 20 << 30}, // realization already caught up
			},
		},
	}
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(v).
			WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).Build(),
		ProvisionTimeout: time.Second,
	}

	if _, err := c.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      volPvc1,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 20 << 30},
	}); err != nil {
		t.Fatalf("no fresh stats must admit: %v", err)
	}
}

// A Volume-type content source (PVC clone) is not implemented; silently
// ignoring it would provision a blank volume under a clone's name.
func TestCreateVolumeRejectsVolumeContentSource(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: readyOnGet(s), Nodes: testNodes, ProvisionTimeout: 2 * time.Second}

	_, err := c.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name:               volPvc1,
		VolumeCapabilities: volCaps(),
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "pvc-0"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a volume content source must be InvalidArgument, got %v", err)
	}
}

// CreateSnapshot idempotency: a same-name retry with the same source
// returns the existing snapshot; the same name for a different source is
// AlreadyExists.
func TestCreateSnapshotIdempotent(t *testing.T) {
	s := newScheme(t)
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 10 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendZFS}},
		},
	}
	other := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-2"},
		Spec:       miroirv1alpha1.MiroirVolumeSpec{SizeBytes: 1 << 30},
	}
	c := &Controller{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(v, other).
			WithStatusSubresource(&miroirv1alpha1.MiroirSnapshot{}).Build(),
	}

	first, err := c.CreateSnapshot(t.Context(), &csi.CreateSnapshotRequest{
		Name: snapSnap1, SourceVolumeId: volPvc1,
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := c.CreateSnapshot(t.Context(), &csi.CreateSnapshotRequest{
		Name: snapSnap1, SourceVolumeId: volPvc1,
	})
	if err != nil {
		t.Fatalf("same-name same-source retry must succeed: %v", err)
	}
	if retry.Snapshot.SnapshotId != first.Snapshot.SnapshotId {
		t.Fatalf("retry must return the existing snapshot: %+v", retry.Snapshot)
	}

	_, err = c.CreateSnapshot(t.Context(), &csi.CreateSnapshotRequest{
		Name: snapSnap1, SourceVolumeId: "pvc-2",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("same name for a different source must be AlreadyExists, got %v", err)
	}
}

// Tokens are positional: pages must neither skip nor duplicate, and a
// garbage or out-of-range token is Aborted per the CSI spec.
func TestListVolumesPagination(t *testing.T) {
	s := newScheme(t)
	objs := make([]client.Object, 0, 3)
	for _, name := range []string{"pvc-b", "pvc-a", "pvc-c"} {
		objs = append(objs, &miroirv1alpha1.MiroirVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       miroirv1alpha1.MiroirVolumeSpec{SizeBytes: 1 << 30},
		})
	}
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()}

	page1, err := c.ListVolumes(t.Context(), &csi.ListVolumesRequest{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Entries) != 2 || page1.Entries[0].Volume.VolumeId != "pvc-a" ||
		page1.Entries[1].Volume.VolumeId != "pvc-b" {
		t.Fatalf("page 1 must be the first two in stable order: %+v", page1.Entries)
	}
	if page1.NextToken == "" {
		t.Fatal("a truncated page must return a token")
	}

	page2, err := c.ListVolumes(t.Context(), &csi.ListVolumesRequest{
		MaxEntries: 2, StartingToken: page1.NextToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Entries) != 1 || page2.Entries[0].Volume.VolumeId != "pvc-c" ||
		page2.NextToken != "" {
		t.Fatalf("page 2 must hold the remainder with no token: %+v", page2)
	}

	for _, token := range []string{"9", "x", "-1"} {
		if _, err := c.ListVolumes(t.Context(), &csi.ListVolumesRequest{StartingToken: token}); status.Code(err) != codes.Aborted {
			t.Fatalf("token %q must be Aborted, got %v", token, err)
		}
	}
}
