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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	homefsv1alpha1 "github.com/erwanleboucher/homefs/api/v1alpha1"
	"github.com/erwanleboucher/homefs/internal/constants"
	"github.com/erwanleboucher/homefs/internal/nodemap"
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
