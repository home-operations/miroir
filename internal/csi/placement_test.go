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
	"strings"
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
			Pools: []miroirv1alpha1.MiroirNodePoolStatus{{
				Name: poolDefault, CapacityBytes: capacity, AllocatedBytes: allocated,
			}},
			ObservedAt: &now,
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
			miroirNodeObj(nodeA, 100*gib, 90*gib), // 10 GiB free
			miroirNodeObj(nodeB, 100*gib, 10*gib), // 90 GiB free
		),
		Nodes: testNodes,
	}

	got, err := c.place(t.Context(), nil, 1, 5*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Node != nodeB {
		t.Fatalf("expected placement on node-b (most free), got %+v", got)
	}
}

func TestPlaceRefusesOvercommit(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 10*gib, 0),
			miroirNodeObj(nodeB, 10*gib, 0),
			volOn("existing-k", nodeA, 15*gib),
			volOn("existing-p", nodeB, 15*gib),
		),
		Nodes: testNodes,
	}

	// Default 2× ratio: 15 + 10 = 25 GiB provisioned > 20 GiB cap on both.
	_, err := c.place(t.Context(), nil, 1, 10*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("overcommit must be ResourceExhausted, got %v", err)
	}
}

func TestPlaceTopologyPinnedRefusedWhenOvercommitted(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 10*gib, 0),
			miroirNodeObj(nodeB, 100*gib, 0), // roomy, but not the pod's node
			volOn("existing-k", nodeA, 15*gib),
		),
		Nodes: testNodes,
	}

	_, err := c.place(t.Context(), topologyPref(nodeA), 1, 10*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("pinned overcommitted node must be ResourceExhausted, got %v", err)
	}
}

func TestPlaceFallsBackWithoutStats(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: placementClient(s), Nodes: testNodes}

	got, err := c.place(t.Context(), nil, 1, 5*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Node != nodeA {
		t.Fatalf("expected by-name fallback to node-a, got %+v", got)
	}
}

func TestPlaceHonoursConfiguredRatio(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 10*gib, 0),
			miroirNodeObj(nodeB, 10*gib, 0),
		),
		Nodes:           testNodes,
		OvercommitRatio: 1, // no overcommit allowed
	}

	// 11 GiB on a 10 GiB pool breaches a 1× ratio on every node.
	_, err := c.place(t.Context(), nil, 1, 11*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("1x ratio must refuse an over-capacity volume, got %v", err)
	}
}

// The gap the free-space bound closes: nothing is provisioned here, so the
// 2× virtual allowance (200 GiB) would happily admit onto a pool with 1 GiB
// of physical room left. Filling a thin pool surfaces as I/O errors under
// live volumes, so the refusal belongs at provision time.
func TestPlaceRefusesPhysicallyFullPool(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 100*gib, 99*gib),
			miroirNodeObj(nodeB, 100*gib, 99*gib),
		),
		Nodes: testNodes,
	}

	// Default 20× free-space ratio: 1 GiB free admits at most 20 GiB.
	_, err := c.place(t.Context(), nil, 1, 25*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a physically full pool must be ResourceExhausted, got %v", err)
	}
}

func TestPlaceHonoursConfiguredFreeSpaceRatio(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 100*gib, 90*gib),
			miroirNodeObj(nodeB, 100*gib, 90*gib),
		),
		Nodes:          testNodes,
		FreeSpaceRatio: 1, // no overcommit against the 10 GiB still free
	}

	// The default 20× would admit this out of the same 10 GiB of free space,
	// as would the untouched 2× virtual bound.
	_, err := c.place(t.Context(), nil, 1, 50*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("1x free-space ratio must refuse a volume past the physical headroom, got %v", err)
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

// node-a and node-b share zone a with the most free space; node-c is alone in
// zone b with the least. Free-space ranking alone picks node-a+node-b — zone
// spread must instead reach into zone b so the replicas span failure domains.
func TestPlaceSpreadsAcrossZones(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 100*gib, 10*gib), // 90 free, zone a
			miroirNodeObj(nodeB, 100*gib, 20*gib), // 80 free, zone a
			miroirNodeObj(nodeC, 100*gib, 90*gib), // 10 free, zone b
			nodeObj(nodeA, addrA),
			nodeObj(nodeB, "192.168.1.42"),
			nodeObj(nodeC, "192.168.1.43"),
		),
		Nodes: nodemap.Map{
			nodeA: {Zone: "a", Pools: map[string]nodemap.Pool{poolDefault: {Backend: miroirv1alpha1.BackendLVMThin, Device: "/dev/x"}}},
			nodeB: {Zone: "a", Pools: map[string]nodemap.Pool{poolDefault: {Backend: miroirv1alpha1.BackendZFS, ZFSDataset: "p/m"}}},
			nodeC: {Zone: "b", Pools: map[string]nodemap.Pool{poolDefault: {Backend: miroirv1alpha1.BackendLVMThin, Device: "/dev/y"}}},
		},
	}

	got, err := c.place(t.Context(), nil, 2, 5*gib, volNew, placementVols(t, c.Client), false, poolDefault)
	if err != nil {
		t.Fatal(err)
	}
	nodes := []string{got[0].Node, got[1].Node}
	slices.Sort(nodes)
	if !slices.Equal(nodes, []string{nodeA, nodeC}) {
		t.Fatalf("replicas must span zones a and b (node-a+node-c), got %v", nodes)
	}
}

// multiPoolNodes is a 3-node map where only node-a and node-b carry the
// poolFast pool; every node carries the default pool.
func multiPoolNodes() nodemap.Map {
	withFast := func() nodemap.Node {
		return nodemap.Node{Pools: map[string]nodemap.Pool{
			poolDefault: {Backend: miroirv1alpha1.BackendLVMThin},
			poolFast:    {Backend: miroirv1alpha1.BackendLVMThin},
		}}
	}
	return nodemap.Map{
		nodeA: withFast(),
		nodeB: withFast(),
		nodeC: {Pools: map[string]nodemap.Pool{poolDefault: {Backend: miroirv1alpha1.BackendLVMThin}}},
	}
}

// miroirNodePools builds a freshly-observed MiroirNode with one capacity
// entry per named pool.
func miroirNodePools(name string, pools ...miroirv1alpha1.MiroirNodePoolStatus) *miroirv1alpha1.MiroirNode {
	now := metav1.Now()
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     miroirv1alpha1.MiroirNodeStatus{Pools: pools, ObservedAt: &now},
	}
}

// A class naming a pool only some nodes carry places onto exactly those
// nodes, ranked by that pool's own headroom.
func TestPlaceFiltersAndRanksByPool(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodePools(nodeA,
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolDefault, CapacityBytes: 100 * gib, AllocatedBytes: 10 * gib},
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolFast, CapacityBytes: 50 * gib, AllocatedBytes: 40 * gib}),
			miroirNodePools(nodeB,
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolDefault, CapacityBytes: 100 * gib, AllocatedBytes: 90 * gib},
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolFast, CapacityBytes: 50 * gib, AllocatedBytes: 5 * gib}),
			miroirNodePools(nodeC,
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolDefault, CapacityBytes: 500 * gib}),
		),
		Nodes: multiPoolNodes(),
	}

	// node-c has the roomiest default pool but no fast pool at all; node-b's
	// fast pool has more headroom than node-a's.
	got, err := c.place(t.Context(), nil, 1, 5*gib, volNew, placementVols(t, c.Client), false, poolFast)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Node != nodeB {
		t.Fatalf("expected the fast pool to rank node-b first, got %+v", got)
	}
	if got[0].Pool != poolFast {
		t.Fatalf("placed replica must persist its pool, got %+v", got[0])
	}
}

// A class naming a pool that exists on too few nodes for its replica count
// fails with an explicit message, not a generic placement refusal.
func TestPlaceRefusesPoolOnTooFewNodes(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: placementClient(s), Nodes: multiPoolNodes()}

	_, err := c.place(t.Context(), nil, 3, 5*gib, volNew, placementVols(t, c.Client), false, poolFast)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("want ResourceExhausted, got %v", err)
	}
	if !strings.Contains(err.Error(), `pool "fast" exists on 2 of 3 storage nodes`) {
		t.Fatalf("error must name the pool and its node count: %v", err)
	}
}

// Overcommit accounting is (node, pool)-scoped: volumes provisioned from
// the default pool must not consume the fast pool's headroom.
func TestPlaceOvercommitIsPoolScoped(t *testing.T) {
	s := newScheme(t)
	defaultVol := volOn("existing-default", nodeA, 15*gib) // default pool (empty = default)
	fastVol := volOn("existing-fast", nodeA, 15*gib)
	fastVol.Spec.Replicas[0].Pool = poolFast
	c := &Controller{
		Client: placementClient(s,
			miroirNodePools(nodeA,
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolDefault, CapacityBytes: 10 * gib},
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolFast, CapacityBytes: 10 * gib}),
			defaultVol, fastVol,
		),
		Nodes: nodemap.Map{nodeA: {Pools: map[string]nodemap.Pool{
			poolDefault: {Backend: miroirv1alpha1.BackendLVMThin},
			poolFast:    {Backend: miroirv1alpha1.BackendLVMThin},
		}}},
	}

	// Each pool holds 15 GiB provisioned of its 2×10 GiB budget: another
	// 10 GiB breaches either pool alone (25 > 20), so per-pool accounting
	// refuses — but 5 GiB still fits (20 > 15+5 is false; 20 >= 20 passes).
	if _, err := c.place(t.Context(), nil, 1, 10*gib, volNew, placementVols(t, c.Client), false, poolFast); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("fast pool at 15/20 GiB must refuse 10 GiB more, got %v", err)
	}
	got, err := c.place(t.Context(), nil, 1, 5*gib, volNew, placementVols(t, c.Client), false, poolFast)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Pool != poolFast {
		t.Fatalf("5 GiB must still fit the fast pool, got %+v", got)
	}
}

// GetCapacity answers per (segment, class): the same node reports each
// pool's own headroom, and a node without the class's pool reports zero.
func TestGetCapacityPerPool(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodePools(nodeA,
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolDefault, CapacityBytes: 10 * gib},
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolFast, CapacityBytes: 5 * gib}),
			miroirNodePools(nodeC,
				miroirv1alpha1.MiroirNodePoolStatus{Name: poolDefault, CapacityBytes: 10 * gib}),
		),
		Nodes: multiPoolNodes(),
	}
	fastParams := map[string]string{constants.ParamPool: poolFast}

	req := topologySegment(nodeA)
	req.Parameters = fastParams
	if resp, _ := c.GetCapacity(t.Context(), req); resp.GetAvailableCapacity() != 10*gib {
		t.Fatalf("fast pool headroom = %d, want %d", resp.GetAvailableCapacity(), 10*gib)
	}
	if resp, _ := c.GetCapacity(t.Context(), topologySegment(nodeA)); resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("default pool headroom = %d, want %d", resp.GetAvailableCapacity(), 20*gib)
	}
	// node-c carries no fast pool: a fast-class RWO segment reports zero.
	req = topologySegment(nodeC)
	req.Parameters = map[string]string{
		constants.ParamPool:              poolFast,
		constants.ParamAllowRemoteAccess: "false",
		constants.ParamReplicas:          "2",
	}
	if resp, _ := c.GetCapacity(t.Context(), req); resp.GetAvailableCapacity() != 0 {
		t.Fatalf("segment without the class's pool must report 0, got %d", resp.GetAvailableCapacity())
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
			miroirNodeObj(nodeA, 100*gib, 10*gib),
			miroirNodeObj(nodeB, 100*gib, 10*gib),
			nodeObj(nodeA, addrA),
			nodeObj(nodeB, addrB),
		),
		Nodes: testNodes,
	}

	got, err := c.place(t.Context(), topologyReq("edge-node"), 2, 5*gib, volNew, placementVols(t, c.Client), true, poolDefault)
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
			miroirNodeObj(nodeA, 100*gib, 10*gib),
			miroirNodeObj(nodeB, 100*gib, 10*gib),
		),
		Nodes: testNodes,
	}

	if _, err := c.place(t.Context(), topologyReq("edge-node"), 2, 5*gib, volNew, placementVols(t, c.Client), false, poolDefault); status.Code(err) != codes.ResourceExhausted {
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
			miroirNodeObj(nodeA, 10*gib, 0),
			miroirNodeObj(nodeB, 10*gib, 0),
			volOn("existing-p", nodeB, 5*gib),
		),
		Nodes: testNodes,
	}
	// Default 2× ratio: node-a headroom = 20 - 0 = 20 GiB; node-b = 20 - 5 = 15 GiB.
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeA))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("node-a capacity = %d, want %d", resp.GetAvailableCapacity(), 20*gib)
	}
	resp, err = c.GetCapacity(t.Context(), topologySegment(nodeB))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 15*gib {
		t.Fatalf("node-b capacity = %d, want %d", resp.GetAvailableCapacity(), 15*gib)
	}
}

// A node whose pool is provisioned past capacity×ratio reports zero, matching
// place()'s overcommit refusal.
func TestGetCapacityOvercommittedIsZero(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 10*gib, 0),
			volOn("existing-k", nodeA, 25*gib), // 25 > 2×10
		),
		Nodes: testNodes,
	}
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeA))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 0 {
		t.Fatalf("overcommitted node must report 0, got %d", resp.GetAvailableCapacity())
	}
}

// GetCapacity must bound by physical room too, or the scheduler would keep
// steering pods onto a node whose pool CreateVolume then refuses.
func TestGetCapacityBoundedByFreeSpace(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s, miroirNodeObj(nodeA, 100*gib, 99*gib)),
		Nodes:  testNodes,
	}
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeA))
	if err != nil {
		t.Fatal(err)
	}
	// 1 GiB free × 20 = 20 GiB, well under the 200 GiB virtual allowance.
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("capacity = %d, want the physical bound %d", resp.GetAvailableCapacity(), 20*gib)
	}
}

// Acceptance criterion (#228): placement and GetCapacity must enforce the
// same headroom, so the scheduler filters a node exactly when place() would
// reject it. The reported figure is the largest volume place() still admits.
func TestGetCapacityAgreesWithPlacement(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s, miroirNodeObj(nodeA, 100*gib, 95*gib)),
		Nodes:  testNodes,
	}
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeA))
	if err != nil {
		t.Fatal(err)
	}
	room := resp.GetAvailableCapacity()

	vols := placementVols(t, c.Client)
	if _, err := c.place(t.Context(), topologyPref(nodeA), 1, room, volNew, vols, false, poolDefault); err != nil {
		t.Fatalf("place must admit exactly the reported capacity (%d): %v", room, err)
	}
	_, err = c.place(t.Context(), topologyPref(nodeA), 1, room+1, volNew, vols, false, poolDefault)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("place must refuse one byte over the reported capacity, got %v", err)
	}
}

// Regression (review): a not-yet-upgraded agent publishes the flat
// pre-multi-pool figures; the controller folds them into the default pool
// so a mixed-version rollout does not zero the node's capacity.
func TestGetCapacityReadsLegacyFlatStatus(t *testing.T) {
	s := newScheme(t)
	now := metav1.Now()
	legacy := &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeA},
		Status: miroirv1alpha1.MiroirNodeStatus{
			CapacityBytes: 10 * gib, ObservedAt: &now,
		},
	}
	c := &Controller{Client: placementClient(s, legacy), Nodes: testNodes}
	resp, err := c.GetCapacity(t.Context(), topologySegment(nodeA))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("legacy flat status must fold into the default pool, got %d", resp.GetAvailableCapacity())
	}
}

// A node without fresh stats, an unknown segment, and a non-storage node all
// report zero so the scheduler steers elsewhere until stats land.
func TestGetCapacityUnknownIsZero(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s, miroirNodeObj(nodeA, 10*gib, 0)),
		Nodes:  testNodes,
	}
	// node-b is a storage node but has published no stats yet.
	if resp, _ := c.GetCapacity(t.Context(), topologySegment(nodeB)); resp.GetAvailableCapacity() != 0 {
		t.Fatalf("statless node must report 0, got %d", resp.GetAvailableCapacity())
	}
	// node-c is not in the storage map at all.
	if resp, _ := c.GetCapacity(t.Context(), topologySegment(nodeC)); resp.GetAvailableCapacity() != 0 {
		t.Fatalf("non-storage node must report 0, got %d", resp.GetAvailableCapacity())
	}
}

// With no topology segment, GetCapacity reports the roomiest node — the
// largest volume the cluster can still place.
func TestGetCapacityNoTopologyReportsBestNode(t *testing.T) {
	s := newScheme(t)
	c := &Controller{
		Client: placementClient(s,
			miroirNodeObj(nodeA, 10*gib, 0),  // 20 GiB headroom
			miroirNodeObj(nodeB, 100*gib, 0), // 200 GiB headroom
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
			miroirNodeObj(nodeA, 10*gib, 0),  // 20 GiB headroom
			miroirNodeObj(nodeB, 100*gib, 0), // 200 GiB headroom
		),
		Nodes: testNodes,
	}
	remoteParams := map[string]string{constants.ParamReplicas: "2"}

	req := topologySegment(nodeC) // not a storage node
	req.Parameters = remoteParams
	resp, err := c.GetCapacity(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 20*gib {
		t.Fatalf("non-storage segment of a 2-replica class = %d, want 2nd-largest headroom %d",
			resp.GetAvailableCapacity(), 20*gib)
	}

	req = topologySegment(nodeB)
	req.Parameters = remoteParams
	resp, err = c.GetCapacity(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	// node-b pinned (200 GiB) + one peer leg on node-a (20 GiB) → 20 GiB.
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
			miroirNodeObj(nodeA, 10*gib, 0),    // 20 GiB headroom
			miroirNodeObj(nodeB, 10*gib, 0),    // full below
			volOn("existing-p", nodeB, 25*gib), // 25 > 2×10 — overcommitted
		),
		Nodes: testNodes,
	}
	req := topologySegment(nodeA)
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
