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

// placementVols lists the seeded volumes the way CreateVolume does, to
// feed place()'s overcommit accounting.
func placementVols(t *testing.T, c client.Client) []miroirv1alpha1.MiroirVolume {
	t.Helper()
	list := &miroirv1alpha1.MiroirVolumeList{}
	if err := c.List(t.Context(), list); err != nil {
		t.Fatal(err)
	}
	return list.Items
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

	got, err := c.place(t.Context(), nil, 1, 5*gib, volNew, placementVols(t, c.Client), false)
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
	_, err := c.place(t.Context(), nil, 1, 10*gib, volNew, placementVols(t, c.Client), false)
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

	_, err := c.place(t.Context(), topologyPref(nodeKharkiv), 1, 10*gib, volNew, placementVols(t, c.Client), false)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("pinned overcommitted node must be ResourceExhausted, got %v", err)
	}
}

func TestPlaceFallsBackWithoutStats(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: placementClient(s), Nodes: testNodes}

	got, err := c.place(t.Context(), nil, 1, 5*gib, volNew, placementVols(t, c.Client), false)
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
	_, err := c.place(t.Context(), nil, 1, 11*gib, volNew, placementVols(t, c.Client), false)
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

	got, err := c.place(t.Context(), nil, 2, 5*gib, volNew, placementVols(t, c.Client), false)
	if err != nil {
		t.Fatal(err)
	}
	nodes := []string{got[0].Node, got[1].Node}
	slices.Sort(nodes)
	if !slices.Equal(nodes, []string{nodeKharkiv, nodeOslo}) {
		t.Fatalf("replicas must span zones a and b (kharkiv+oslo), got %v", nodes)
	}
}

// topologyReq mirrors a strict-topology provisioner request: the
// scheduler-selected node as both requisite and preferred.
func topologyReq(node string) *csi.TopologyRequirement {
	t := topologyPref(node)
	t.Requisite = t.Preferred
	return t
}

// A remote-access volume whose first consumer sits on a non-storage node
// must not wedge: the scheduler-selected topology is unsatisfiable by
// design (the pod will attach through a diskless client leg), so placement
// falls back to capacity ranking instead of refusing.
func TestPlaceRemoteAccessIgnoresNonStorageTopology(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 100*gib, 10*gib),
			miroirNodeObj(nodeParis, 100*gib, 10*gib),
			nodeObj(nodeKharkiv, addrKharkiv),
			nodeObj(nodeParis, addrParis),
		),
		Nodes: testNodes,
	}

	got, err := c.place(t.Context(), topologyReq("edge-node"), 2, 5*gib, volNew, placementVols(t, c.Client), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 replicas on storage nodes, got %+v", got)
	}
	for _, rep := range got {
		if rep.Node == "edge-node" {
			t.Fatalf("non-storage node must not receive a replica: %+v", got)
		}
	}
}

// Without remote access the same request keeps refusing — a pod pinned to
// a non-storage node could never reach its volume.
func TestPlaceStrictRefusesNonStorageTopology(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 100*gib, 10*gib),
			miroirNodeObj(nodeParis, 100*gib, 10*gib),
		),
		Nodes: testNodes,
	}

	if _, err := c.place(t.Context(), topologyReq("edge-node"), 2, 5*gib, volNew, placementVols(t, c.Client), false); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("non-storage topology without remote access must refuse, got %v", err)
	}
}

// topologySegment builds a single-segment GetCapacity request the way the
// topology-aware external-provisioner does.
func topologySegment(node string) *csi.GetCapacityRequest {
	return &csi.GetCapacityRequest{
		AccessibleTopology: &csi.Topology{Segments: map[string]string{constants.TopologyKey: node}},
	}
}

// GetCapacity reports capacity×ratio − provisioned for the requested node, so
// the scheduler filters a node exactly when place() would refuse it.
func TestGetCapacityPerNode(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			miroirNodeObj(nodeParis, 10*gib, 0),
			volOn("existing-p", nodeParis, 5*gib),
		),
		Nodes: testNodes,
	}
	// Default 2× ratio: kharkiv headroom = 20 - 0 = 20 GiB; paris = 20 - 5 = 15 GiB.
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeKharkiv))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("kharkiv capacity = %d, want %d", resp.GetAvailableCapacity(), 20*gib)
	}
	resp, err = c.GetCapacity(t.Context(), topologySegment(nodeParis))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 15*gib {
		t.Fatalf("paris capacity = %d, want %d", resp.GetAvailableCapacity(), 15*gib)
	}
}

// A node whose pool is provisioned past capacity×ratio reports zero, matching
// place()'s overcommit refusal.
func TestGetCapacityOvercommittedIsZero(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),
			volOn("existing-k", nodeKharkiv, 25*gib), // 25 > 2×10
		),
		Nodes: testNodes,
	}
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeKharkiv))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 0 {
		t.Fatalf("overcommitted node must report 0, got %d", resp.GetAvailableCapacity())
	}
}

// A node without fresh stats, an unknown segment, and a non-storage node all
// report zero so the scheduler steers elsewhere until stats land.
func TestGetCapacityUnknownIsZero(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s, miroirNodeObj(nodeKharkiv, 10*gib, 0)),
		Nodes:  testNodes,
	}
	// paris is a storage node but has published no stats yet.
	if resp, _ := c.GetCapacity(t.Context(), topologySegment(nodeParis)); resp.GetAvailableCapacity() != 0 {
		t.Fatalf("statless node must report 0, got %d", resp.GetAvailableCapacity())
	}
	// oslo is not in the storage map at all.
	if resp, _ := c.GetCapacity(t.Context(), topologySegment(nodeOslo)); resp.GetAvailableCapacity() != 0 {
		t.Fatalf("non-storage node must report 0, got %d", resp.GetAvailableCapacity())
	}
}

// With no topology segment, GetCapacity reports the roomiest node — the
// largest volume the cluster can still place.
func TestGetCapacityNoTopologyReportsBestNode(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0), // 20 GiB headroom
			miroirNodeObj(nodeParis, 100*gib, 0),  // 200 GiB headroom
		),
		Nodes: testNodes,
	}
	resp, err := c.GetCapacity(t.Context(), &csi.GetCapacityRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 200*gib {
		t.Fatalf("no-topology capacity = %d, want roomiest node %d", resp.GetAvailableCapacity(), 200*gib)
	}
}

// A remote-access class (replicated; allowRemoteVolumeAccess defaults true)
// leaves the PV unpinned and place() accepts a first consumer on a
// non-storage node (diskless client leg) — that segment must not answer 0,
// or the scheduler filters pods off nodes CreateVolume would accept. Each
// diskful leg needs its own pool, so a 2-replica class is bounded by the
// 2nd-largest headroom. A storage segment answers its own headroom bounded
// by the peers': place() pins a scheduler-preferred storage node and holds
// it to the overcommit guard, remote access or not.
func TestGetCapacityRemoteAccessSegments(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0), // 20 GiB headroom
			miroirNodeObj(nodeParis, 100*gib, 0),  // 200 GiB headroom
		),
		Nodes: testNodes,
	}
	remoteParams := map[string]string{constants.ParamReplicas: "2"}

	req := topologySegment(nodeOslo) // not a storage node
	req.Parameters = remoteParams
	resp, err := c.GetCapacity(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("non-storage segment of a 2-replica class = %d, want 2nd-largest headroom %d",
			resp.GetAvailableCapacity(), 20*gib)
	}

	req = topologySegment(nodeParis)
	req.Parameters = remoteParams
	resp, err = c.GetCapacity(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	// paris pinned (200 GiB) + one peer leg on kharkiv (20 GiB) → 20 GiB.
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("storage segment = %d, want min(own, peer) %d",
			resp.GetAvailableCapacity(), 20*gib)
	}
}

// A replicated class must not publish capacity a placement cannot honor:
// with one storage node roomy and the other full, a 2-replica volume has
// nowhere for its second leg — place() refuses, so the segment reports 0.
func TestGetCapacityReplicatedNeedsAllLegs(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeKharkiv, 10*gib, 0),  // 20 GiB headroom
			miroirNodeObj(nodeParis, 10*gib, 0),    // full below
			volOn("existing-p", nodeParis, 25*gib), // 25 > 2×10 — overcommitted
		),
		Nodes: testNodes,
	}
	req := topologySegment(nodeKharkiv)
	req.Parameters = map[string]string{constants.ParamReplicas: "2"}
	resp, err := c.GetCapacity(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 0 {
		t.Fatalf("2-replica class with one full peer must report 0, got %d",
			resp.GetAvailableCapacity())
	}
}
