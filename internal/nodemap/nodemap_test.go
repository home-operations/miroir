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

package nodemap

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	nodeA      = "node-a"
	nodeB      = "node-b"
	nodeC      = "node-c"
	nodeBergen = "bergen"

	poolDefault = "default"
	zoneRack1   = "rack-1"
	volBlock16K = "16K"
	datasetTank = "tank/miroir"
)

func miroirNode(name string, spec miroirv1alpha1.MiroirNodeSpec) miroirv1alpha1.MiroirNode {
	return miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: spec}
}

func TestFromSpecFlattensBackendBlocks(t *testing.T) {
	autoEvict := false
	n := FromSpec(miroirv1alpha1.MiroirNodeSpec{
		Zone:      zoneRack1,
		Address:   "10.0.100.1",
		AutoEvict: &autoEvict,
		Pools: []miroirv1alpha1.MiroirNodePool{
			{Name: poolDefault,
				LVMThin: &miroirv1alpha1.LVMThinPool{Device: "/dev/disk/by-partlabel/r-miroir", PoolSize: "400g"}},
			{Name: "fast",
				ZFS: &miroirv1alpha1.ZFSPool{Dataset: datasetTank, Compression: "inherit", VolBlockSize: volBlock16K}},
			{Name: "scratch",
				Loopfile: &miroirv1alpha1.LoopfilePool{BaseDir: "/var/lib/miroir"}},
		},
	})
	if n.Zone != zoneRack1 || n.Address != "10.0.100.1" || n.AutoEvict == nil || *n.AutoEvict {
		t.Fatalf("node-level settings wrong: %+v", n)
	}
	def := n.Pools[poolDefault]
	if def.Backend != miroirv1alpha1.BackendLVMThin ||
		def.Device != "/dev/disk/by-partlabel/r-miroir" || def.ThinPoolSize != "400g" {
		t.Fatalf("lvmthin pool flattened wrong: %+v", def)
	}
	zfs := n.Pools["fast"]
	if zfs.ZFSDataset != datasetTank || zfs.ZFSCompression != "inherit" ||
		zfs.ZFSVolBlockSize != volBlock16K || zfs.ZFSVolBlockSizeBytes() != 16<<10 {
		t.Fatalf("zfs pool flattened wrong: %+v", zfs)
	}
	if n.Pools["scratch"].BaseDir != "/var/lib/miroir" {
		t.Fatalf("loopfile pool flattened wrong: %+v", n.Pools["scratch"])
	}
}

// The CRD defaults compression/volBlockSize on write, but a spec read
// before defaulting (or built by hand) must still resolve sane values.
func TestZFSVolBlockSizeDefaults(t *testing.T) {
	p := Pool{Backend: miroirv1alpha1.BackendZFS}
	if p.ZFSVolBlockSizeBytes() != 4<<10 {
		t.Fatalf("empty volBlockSize should default to 4K, got %d bytes", p.ZFSVolBlockSizeBytes())
	}
}

// A pool whose backend block is missing (an old object written before the
// CRD required it, mid-upgrade) folds to an empty-config pool instead of
// panicking; the agent's backend setup reports the missing config.
func TestFromSpecToleratesMissingBlock(t *testing.T) {
	// Unreachable through the CRD (exactly one block is required), but a
	// stored pre-validation object must fold to an empty pool, not panic.
	n := FromSpec(miroirv1alpha1.MiroirNodeSpec{Pools: []miroirv1alpha1.MiroirNodePool{
		{Name: poolDefault},
	}})
	if p := n.Pools[poolDefault]; p.Backend != "" || p.ZFSDataset != "" {
		t.Fatalf("missing block should fold empty, got %+v", p)
	}
}

// FromNodes marks every node sharing a replication address, keyed by the
// parsed form so IPv6 zero-compression variants still collide, and
// PickSpare / ReplicationAddress both refuse conflicted nodes.
func TestFromNodesMarksAddressConflicts(t *testing.T) {
	pools := []miroirv1alpha1.MiroirNodePool{{Name: poolDefault,
		LVMThin: &miroirv1alpha1.LVMThinPool{}}}
	m := FromNodes([]miroirv1alpha1.MiroirNode{
		miroirNode(nodeA, miroirv1alpha1.MiroirNodeSpec{Address: "fd00:1::2", Pools: pools}),
		miroirNode(nodeB, miroirv1alpha1.MiroirNodeSpec{Address: "fd00:0001:0:0::2", Pools: pools}),
		miroirNode(nodeC, miroirv1alpha1.MiroirNodeSpec{Address: "10.0.100.3", Pools: pools}),
	})
	if !m[nodeA].AddressConflict || !m[nodeB].AddressConflict {
		t.Fatalf("both holders of the shared address must be conflicted: %+v", m)
	}
	if m[nodeC].AddressConflict {
		t.Fatal("a unique address must not be conflicted")
	}
	if got := m.PickSpare(map[string]bool{nodeC: true}, nil, nil); got != "" {
		t.Fatalf("PickSpare must skip conflicted nodes, picked %q", got)
	}
	if _, err := m.ReplicationAddress(t.Context(), nil, nodeA); !errors.Is(err, ErrAddressConflict) {
		t.Fatalf("ReplicationAddress must refuse a conflicted node with ErrAddressConflict, got %v", err)
	}
	if m.Placeable(nodeA) || m.Placeable(nodeB) {
		t.Fatal("conflicted nodes must not be placeable")
	}
	if !m.Placeable(nodeC) {
		t.Fatal("a unique-address node must be placeable")
	}
	if m.Placeable("absent") {
		t.Fatal("a node outside the topology must not be placeable")
	}
}

// A non-IP address string is reachable only through a stale CRD (no isIP
// rule) or a writer bypassing it; duplicated junk must still conflict so an
// ambiguous endpoint never reaches persisted replica specs, and empty
// addresses (the InternalIP fallback) must never conflict with each other.
func TestFromNodesConflictsNonIPDuplicates(t *testing.T) {
	pools := []miroirv1alpha1.MiroirNodePool{{Name: poolDefault,
		LVMThin: &miroirv1alpha1.LVMThinPool{}}}
	m := FromNodes([]miroirv1alpha1.MiroirNode{
		miroirNode(nodeA, miroirv1alpha1.MiroirNodeSpec{Address: "storage-vlan", Pools: pools}),
		miroirNode(nodeB, miroirv1alpha1.MiroirNodeSpec{Address: "storage-vlan", Pools: pools}),
		miroirNode(nodeC, miroirv1alpha1.MiroirNodeSpec{Pools: pools}),
	})
	if !m[nodeA].AddressConflict || !m[nodeB].AddressConflict {
		t.Fatalf("duplicated non-IP addresses must be conflicted: %+v", m)
	}
	if m[nodeC].AddressConflict {
		t.Fatal("an empty address must never be conflicted")
	}
}

func TestFromNodesEmptyAddressesNeverConflict(t *testing.T) {
	pools := []miroirv1alpha1.MiroirNodePool{{Name: poolDefault,
		LVMThin: &miroirv1alpha1.LVMThinPool{}}}
	m := FromNodes([]miroirv1alpha1.MiroirNode{
		miroirNode(nodeA, miroirv1alpha1.MiroirNodeSpec{Pools: pools}),
		miroirNode(nodeB, miroirv1alpha1.MiroirNodeSpec{Pools: pools}),
	})
	if m[nodeA].AddressConflict || m[nodeB].AddressConflict {
		t.Fatalf("nodes on the InternalIP fallback share no explicit address: %+v", m)
	}
}

func TestCRSourceFoldsMiroirNodes(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	node := miroirNode(nodeA, miroirv1alpha1.MiroirNodeSpec{
		Zone: zoneRack1,
		Pools: []miroirv1alpha1.MiroirNodePool{{Name: poolDefault,
			ZFS: &miroirv1alpha1.ZFSPool{Dataset: datasetTank}}},
	})
	src := &CRSource{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(&node).Build()}
	m, err := src.Map(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := m.Pool(nodeA, ""); !ok || p.ZFSDataset != datasetTank {
		t.Fatalf("CRSource folded wrong: %+v %t", p, ok)
	}
	if m[nodeA].Zone != zoneRack1 {
		t.Fatalf("zone lost in fold: %+v", m[nodeA])
	}
}

func TestAutoEvictOptOut(t *testing.T) {
	optOut := false
	m := Map{
		nodeA: {AutoEvict: &optOut},
		nodeB: {},
	}
	if m.AutoEvictAllowed(nodeA) {
		t.Fatal("autoEvict: false must opt the node out")
	}
	if !m.AutoEvictAllowed(nodeB) {
		t.Fatal("absent autoEvict must leave the node eligible")
	}
	if m.AutoEvictAllowed("unmapped") {
		t.Fatal("a node outside the map is never eligible")
	}
}

func TestPoolLookup(t *testing.T) {
	m := Map{nodeA: {Pools: map[string]Pool{
		"default": {Backend: miroirv1alpha1.BackendLVMThin},
		"fast":    {Backend: miroirv1alpha1.BackendZFS},
	}}}
	if p, ok := m.Pool(nodeA, ""); !ok || p.Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("empty pool name should resolve the default pool, got %+v %t", p, ok)
	}
	if p, ok := m.Pool(nodeA, "fast"); !ok || p.Backend != miroirv1alpha1.BackendZFS {
		t.Fatalf("named pool lookup wrong: %+v %t", p, ok)
	}
	if _, ok := m.Pool(nodeA, "absent"); ok {
		t.Fatal("absent pool should not resolve")
	}
	if _, ok := m.Pool(nodeB, "default"); ok {
		t.Fatal("absent node should not resolve")
	}
}

func TestTieBreakerNode(t *testing.T) {
	replicas := []miroirv1alpha1.Replica{{Node: nodeA}, {Node: nodeB}}
	cases := map[string]struct {
		m    Map
		want string
	}{
		"prefers a zone no replica occupies": {
			m: Map{
				nodeA: {Zone: "a"}, nodeB: {Zone: "b"},
				nodeC: {Zone: "a"}, nodeBergen: {Zone: "c"},
			},
			want: nodeBergen,
		},
		"zoneless spare is unconstrained": {
			m:    Map{nodeA: {Zone: "a"}, nodeB: {Zone: "b"}, nodeC: {}},
			want: nodeC,
		},
		"falls back to an occupied zone when none is free": {
			m:    Map{nodeA: {Zone: "a"}, nodeB: {Zone: "b"}, nodeC: {Zone: "a"}},
			want: nodeC,
		},
		"name order breaks ties": {
			m:    Map{nodeA: {}, nodeB: {}, nodeC: {}, nodeBergen: {}},
			want: nodeBergen,
		},
		"no spare node": {
			m:    Map{nodeA: {}, nodeB: {}},
			want: "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.m.TieBreakerNode(replicas); got != tc.want {
				t.Fatalf("TieBreakerNode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReplicationAddress(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	withNode := func(objs ...client.Object) client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	}
	internalIP := func(name, ip string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: ip},
			}},
		}
	}
	cases := map[string]struct {
		m       Map
		client  client.Client
		want    string
		wantErr bool
	}{
		"override skips the node lookup": {
			m:      Map{nodeA: {Address: "10.0.100.5"}},
			client: withNode(), // no Node object registered
			want:   "10.0.100.5",
		},
		"falls back to InternalIP": {
			m:      Map{nodeA: {}},
			client: withNode(internalIP(nodeA, "192.168.1.41")),
			want:   "192.168.1.41",
		},
		"node without InternalIP errors": {
			m:       Map{nodeA: {}},
			client:  withNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeA}}),
			wantErr: true,
		},
		"absent node errors": {
			m:       Map{nodeA: {}},
			client:  withNode(),
			wantErr: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := tc.m.ReplicationAddress(t.Context(), tc.client, nodeA)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("ReplicationAddress = %q, want %q", got, tc.want)
			}
		})
	}
}
