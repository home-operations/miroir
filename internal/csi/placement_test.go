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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

const gib = 1 << 30

// miroirNodeObj builds a freshly-observed MiroirNode so the controller
// treats its stats as current.
func miroirNodeObj(name string, capacity, allocated int64) *miroirv1alpha1.MiroirNode {
	now := metav1.Now()
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: miroirv1alpha1.MiroirNodeStatus{
			CapacityBytes:  capacity,
			AllocatedBytes: allocated,
			ObservedAt:     &now,
		},
	}
}

// volOn builds a MiroirVolume placing one replica of the given size on node.
func volOn(name, node string, sizeBytes int64) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: sizeBytes,
			Replicas:  []miroirv1alpha1.Replica{{Node: node, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
}

func placementClient(s *runtime.Scheme, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func topologyPref(node string) *csi.TopologyRequirement {
	return &csi.TopologyRequirement{
		Preferred: []*csi.Topology{{Segments: map[string]string{constants.TopologyKey: node}}},
	}
}

func TestPlaceWeightsByFreeSpace(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 100*gib, 90*gib), // 10 GiB free
			miroirNodeObj(nodeParis, 100*gib, 10*gib),   // 90 GiB free
		),
		Nodes: testNodes,
	}

	got, err := c.place(context.Background(), nil, 1, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Node != nodeParis {
		t.Fatalf("expected placement on paris (most free), got %+v", got)
	}
}

func TestPlaceRefusesOvercommit(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			miroirNodeObj(nodeParis, 10*gib, 0),
			volOn("existing-k", nodeKharkiv, 15*gib),
			volOn("existing-p", nodeParis, 15*gib),
		),
		Nodes: testNodes,
	}

	// Default 2× ratio: 15 + 10 = 25 GiB provisioned > 20 GiB cap on both.
	_, err := c.place(context.Background(), nil, 1, 10*gib, volNew)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("overcommit must be ResourceExhausted, got %v", err)
	}
}

func TestPlaceTopologyPinnedRefusedWhenOvercommitted(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			miroirNodeObj(nodeParis, 100*gib, 0), // roomy, but not the pod's node
			volOn("existing-k", nodeKharkiv, 15*gib),
		),
		Nodes: testNodes,
	}

	_, err := c.place(context.Background(), topologyPref(nodeKharkiv), 1, 10*gib, volNew)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("pinned overcommitted node must be ResourceExhausted, got %v", err)
	}
}

func TestPlaceFallsBackWithoutStats(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: placementClient(s), Nodes: testNodes}

	got, err := c.place(context.Background(), nil, 1, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Node != nodeKharkiv {
		t.Fatalf("expected by-name fallback to kharkiv, got %+v", got)
	}
}

func TestPlaceHonoursConfiguredRatio(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			miroirNodeObj(nodeParis, 10*gib, 0),
		),
		Nodes:           testNodes,
		OvercommitRatio: 1, // no overcommit allowed
	}

	// 11 GiB on a 10 GiB pool breaches a 1× ratio on every node.
	_, err := c.place(context.Background(), nil, 1, 11*gib, volNew)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("1x ratio must refuse an over-capacity volume, got %v", err)
	}
}
