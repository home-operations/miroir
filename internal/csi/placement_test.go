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
	"slices"
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
	"github.com/home-operations/miroir/internal/nodemap"
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

func TestSpreadByZone(t *testing.T) {
	zoneOf := func(m map[string]string) func(string) string {
		return func(n string) string { return m[n] }
	}
	cases := []struct {
		name    string
		ordered []string
		zones   map[string]string
		pinned  int
		count   int
		want    []string
	}{{
		name:    "no zones declared keeps the ranked prefix",
		ordered: []string{"a", "b", "c"},
		count:   2,
		want:    []string{"a", "b"},
	}, {
		name:    "prefers a distinct zone over rank",
		ordered: []string{"a", "b", "c"},
		zones:   map[string]string{"a": "z1", "b": "z1", "c": "z2"},
		count:   2,
		want:    []string{"a", "c"},
	}, {
		name:    "falls back to a used zone when zones run short",
		ordered: []string{"a", "b", "c"},
		zones:   map[string]string{"a": "z1", "b": "z1", "c": "z1"},
		count:   2,
		want:    []string{"a", "b"},
	}, {
		name:    "topology-pinned nodes are kept even in a used zone",
		ordered: []string{"a", "b", "c"},
		zones:   map[string]string{"a": "z1", "b": "z1", "c": "z2"},
		pinned:  2,
		count:   2,
		want:    []string{"a", "b"},
	}, {
		name:    "an empty zone is unconstrained",
		ordered: []string{"a", "b", "c"},
		zones:   map[string]string{"a": "z1", "c": "z1"},
		count:   2,
		want:    []string{"a", "b"},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := spreadByZone(tc.ordered, tc.pinned, tc.count, zoneOf(tc.zones)); !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// kharkiv and paris share zone a with the most free space; oslo is alone in
// zone b with the least. Free-space ranking alone picks kharkiv+paris — zone
// spread must instead reach into zone b so the replicas span failure domains.
func TestPlaceSpreadsAcrossZones(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 100*gib, 10*gib), // 90 free, zone a
			miroirNodeObj(nodeParis, 100*gib, 20*gib),   // 80 free, zone a
			miroirNodeObj(nodeOslo, 100*gib, 90*gib),    // 10 free, zone b
			nodeObj(nodeKharkiv, addrKharkiv),
			nodeObj(nodeParis, "192.168.1.42"),
			nodeObj(nodeOslo, "192.168.1.43"),
		),
		Nodes: nodemap.Map{
			nodeKharkiv: {Backend: miroirv1alpha1.BackendLVMThin, Zone: "a", Device: "/dev/x"},
			nodeParis:   {Backend: miroirv1alpha1.BackendZFS, Zone: "a", ZFSDataset: "p/m"},
			nodeOslo:    {Backend: miroirv1alpha1.BackendLVMThin, Zone: "b", Device: "/dev/y"},
		},
	}

	got, err := c.place(context.Background(), nil, 2, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	nodes := []string{got[0].Node, got[1].Node}
	slices.Sort(nodes)
	if !slices.Equal(nodes, []string{nodeKharkiv, nodeOslo}) {
		t.Fatalf("replicas must span zones a and b (kharkiv+oslo), got %v", nodes)
	}
}

// threeNodeController builds a controller with kharkiv+paris in zone a
// and oslo in zone b. All have fresh pool stats and node IP objects.
func threeNodeController(s *runtime.Scheme) *Controller {
	return &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 100*gib, 10*gib),
			miroirNodeObj(nodeParis, 100*gib, 20*gib),
			miroirNodeObj("oslo", 100*gib, 90*gib),
			nodeObj(nodeKharkiv, "192.168.1.41"),
			nodeObj(nodeParis, "192.168.1.42"),
			nodeObj("oslo", "192.168.1.43"),
		),
		Nodes: nodemap.Map{
			nodeKharkiv: {Backend: miroirv1alpha1.BackendLVMThin, Zone: "a", Device: "/dev/x"},
			nodeParis:   {Backend: miroirv1alpha1.BackendZFS, Zone: "a", ZFSDataset: "p/m"},
			"oslo":      {Backend: miroirv1alpha1.BackendLVMThin, Zone: "b", Device: "/dev/y"},
		},
	}
}

func TestMaybeAddTieBreaker_freezeWithSpare(t *testing.T) {
	s := newScheme(t)
	c := threeNodeController(s)

	placed, err := c.place(context.Background(), nil, 2, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	// 2 diskful placed; defaulted LMS should flip to freeze + tie-breaker.
	// place() zone-spreads onto kharkiv(zone a)+oslo(zone b), so paris is spare.
	got, q := c.maybeAddTieBreaker(context.Background(), placed, miroirv1alpha1.QuorumLastManStanding, false, 2)
	if q != miroirv1alpha1.QuorumFreeze {
		t.Fatalf("quorum should flip to freeze, got %s", q)
	}
	if len(got) != 3 || !got[2].Diskless {
		t.Fatalf("expected 3rd diskless replica, got %+v", got)
	}
	if got[2].Node != nodeParis {
		t.Fatalf("tie-breaker should be paris (spare), got %s", got[2].Node)
	}
}

func TestMaybeAddTieBreaker_explicitLMS(t *testing.T) {
	s := newScheme(t)
	c := threeNodeController(s)

	placed, err := c.place(context.Background(), nil, 2, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	// Explicitly LMS: no tie-breaker.
	got, q := c.maybeAddTieBreaker(context.Background(), placed, miroirv1alpha1.QuorumLastManStanding, true, 2)
	if q != miroirv1alpha1.QuorumLastManStanding {
		t.Fatalf("explicit LMS must stay, got %s", q)
	}
	if len(got) != 2 {
		t.Fatalf("explicit LMS must not add tie-breaker, got %d replicas", len(got))
	}
}

func TestMaybeAddTieBreaker_explicitFreezeNoSpare(t *testing.T) {
	s := newScheme(t)
	// Only 2 nodes — no spare for a tie-breaker.
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			miroirNodeObj(nodeParis, 10*gib, 0),
			nodeObj(nodeKharkiv, "192.168.1.41"),
			nodeObj(nodeParis, "192.168.1.42"),
		),
		Nodes: testNodes,
	}
	placed, err := c.place(context.Background(), nil, 2, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	got, q := c.maybeAddTieBreaker(context.Background(), placed, miroirv1alpha1.QuorumFreeze, true, 2)
	if q != miroirv1alpha1.QuorumFreeze {
		t.Fatalf("explicit freeze must stay, got %s", q)
	}
	if len(got) != 2 {
		t.Fatalf("no spare → no tie-breaker, got %d replicas", len(got))
	}
}

func TestMaybeAddTieBreaker_defaultLMSNoSpareFallsBack(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			miroirNodeObj(nodeParis, 10*gib, 0),
			nodeObj(nodeKharkiv, "192.168.1.41"),
			nodeObj(nodeParis, "192.168.1.42"),
		),
		Nodes: testNodes,
	}
	placed, err := c.place(context.Background(), nil, 2, 5*gib, volNew)
	if err != nil {
		t.Fatal(err)
	}
	// Defaulted LMS, no spare: stays LMS.
	got, q := c.maybeAddTieBreaker(context.Background(), placed, miroirv1alpha1.QuorumLastManStanding, false, 2)
	if q != miroirv1alpha1.QuorumLastManStanding {
		t.Fatalf("no spare → stay LMS, got %s", q)
	}
	if len(got) != 2 {
		t.Fatalf("no spare → no tie-breaker, got %d replicas", len(got))
	}
}

func TestMaybeAddTieBreaker_oddDiskfulNoop(t *testing.T) {
	// 3 diskful replicas (odd count) → no tie-breaker needed.
	placed := []miroirv1alpha1.Replica{
		{Node: nodeKharkiv, NodeID: 0, Address: "10.0.0.1"},
		{Node: nodeParis, NodeID: 1, Address: "10.0.0.2"},
		{Node: "oslo", NodeID: 2, Address: "10.0.0.3"},
	}
	s := newScheme(t)
	c := threeNodeController(s)
	got, q := c.maybeAddTieBreaker(context.Background(), placed, miroirv1alpha1.QuorumFreeze, true, 3)
	if q != miroirv1alpha1.QuorumFreeze || len(got) != 3 {
		t.Fatalf("odd diskful → no tie-breaker, got %d replicas quorum=%s", len(got), q)
	}
}

func TestFindTieBreakerNode_prefersZoneDistinct(t *testing.T) {
	c := &Controller{
		Nodes: nodemap.Map{
			"a1": {Backend: miroirv1alpha1.BackendLVMThin, Zone: "z1"},
			"a2": {Backend: miroirv1alpha1.BackendLVMThin, Zone: "z1"},
			"b1": {Backend: miroirv1alpha1.BackendLVMThin, Zone: "z2"},
		},
	}
	// Both diskful in z1: must pick b1 (z2).
	got := c.findTieBreakerNode([]miroirv1alpha1.Replica{
		{Node: "a1"}, {Node: "a2"},
	})
	if got != "b1" {
		t.Fatalf("expected zone-distinct b1, got %s", got)
	}
}

func TestFindTieBreakerNode_noSpare(t *testing.T) {
	c := &Controller{
		Nodes: nodemap.Map{
			"a": {Backend: miroirv1alpha1.BackendLVMThin, Zone: "z1"},
			"b": {Backend: miroirv1alpha1.BackendLVMThin, Zone: "z2"},
		},
	}
	got := c.findTieBreakerNode([]miroirv1alpha1.Replica{
		{Node: "a"}, {Node: "b"},
	})
	if got != "" {
		t.Fatalf("expected no spare, got %s", got)
	}
}
