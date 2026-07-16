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
	"os"
	"path/filepath"
	"strings"
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
)

func writeMap(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nodes.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValid(t *testing.T) {
	m, err := Load(writeMap(t, `
node-a:
  zone: rack-1
  address: 10.0.100.1
  pools:
    default:
      backend: lvmthin
      device: /dev/disk/by-partlabel/r-miroir
      thinPoolSize: 400g
    fast:
      backend: lvmthin
      device: /dev/disk/by-id/nvme-fast
node-b:
  address: "fd00:1::2"
  pools:
    default:
      backend: zfs
      zfsDataset: tank/miroir
      zfsVolBlockSize: 16K
      zfsCompression: inherit
node-c:
  pools:
    default:
      backend: loopfile
      baseDir: /var/lib/miroir
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 {
		t.Fatalf("want 3 nodes, got %d", len(m))
	}
	def := m["node-a"].Pools["default"]
	if def.Backend != miroirv1alpha1.BackendLVMThin || def.ThinPoolSize != "400g" {
		t.Fatalf("node-a default pool parsed wrong: %+v", def)
	}
	if fast := m["node-a"].Pools["fast"]; fast.Device != "/dev/disk/by-id/nvme-fast" {
		t.Fatalf("node-a fast pool parsed wrong: %+v", fast)
	}
	if m["node-a"].Zone != "rack-1" {
		t.Fatalf("node-a zone not parsed: %+v", m["node-a"])
	}
	zfs := m["node-b"].Pools["default"]
	if zfs.ZFSDataset != "tank/miroir" || zfs.ZFSVolBlockSize != "16K" ||
		zfs.ZFSVolBlockSizeBytes() != 16<<10 || zfs.ZFSCompression != "inherit" {
		t.Fatalf("node-b ZFS pool parsed wrong: %+v", zfs)
	}
	if m["node-c"].Pools["default"].BaseDir != "/var/lib/miroir" {
		t.Fatalf("node-c baseDir wrong: %+v", m["node-c"])
	}
	if m["node-a"].Address != "10.0.100.1" || m["node-b"].Address != "fd00:1::2" {
		t.Fatalf("addresses parsed wrong: %q %q", m["node-a"].Address, m["node-b"].Address)
	}
}

func TestZFSDefaults(t *testing.T) {
	m, err := Load(writeMap(t, `
node-a:
  pools:
    default:
      backend: zfs
      zfsDataset: tank/miroir
`))
	if err != nil {
		t.Fatal(err)
	}
	p := m[nodeA].Pools[miroirv1alpha1.DefaultPoolName]
	if p.ZFSVolBlockSize != DefaultZFSVolBlockSize {
		t.Fatalf("zfsVolBlockSize = %q, want %q", p.ZFSVolBlockSize, DefaultZFSVolBlockSize)
	}
	if p.ZFSVolBlockSizeBytes() != 4<<10 {
		t.Fatalf("zfsVolBlockSize bytes = %d, want %d", p.ZFSVolBlockSizeBytes(), 4<<10)
	}
	if p.ZFSCompression != DefaultZFSCompression {
		t.Fatalf("zfsCompression = %q, want %q", p.ZFSCompression, DefaultZFSCompression)
	}
}

// Load canonicalizes the zfs settings' casing: the block size has to match
// a zfsVolBlockSizes key, and the compression value reaches `zfs create -o`
// verbatim, which OpenZFS accepts only in its own lowercase spelling.
func TestZFSSettingsCanonicalizeCase(t *testing.T) {
	m, err := Load(writeMap(t, `
node-a:
  pools:
    default:
      backend: zfs
      zfsDataset: tank/miroir
      zfsVolBlockSize: 16k
      zfsCompression: ZSTD-3
`))
	if err != nil {
		t.Fatal(err)
	}
	p := m[nodeA].Pools[miroirv1alpha1.DefaultPoolName]
	if p.ZFSVolBlockSizeBytes() != 16<<10 {
		t.Fatalf("zfsVolBlockSizeBytes = %d, want %d", p.ZFSVolBlockSizeBytes(), 16<<10)
	}
	if p.ZFSCompression != "zstd-3" {
		t.Fatalf("zfsCompression = %q, want %q", p.ZFSCompression, "zstd-3")
	}
}

// autoEvict: false parses (UnmarshalStrict rejects unknown fields, so
// this guards the schema) and flips AutoEvictAllowed; absence and
// unmapped nodes resolve as documented.
func TestAutoEvictOptOut(t *testing.T) {
	m, err := Load(writeMap(t, `
node-a:
  autoEvict: false
  pools:
    default:
      backend: zfs
      zfsDataset: tank/miroir
node-b:
  pools:
    default:
      backend: zfs
      zfsDataset: tank/miroir
`))
	if err != nil {
		t.Fatal(err)
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

func TestLoadErrors(t *testing.T) {
	pool := func(body string) string {
		return "node-a:\n  pools:\n    default:\n" + body
	}
	cases := map[string]string{
		"unknown backend":          pool("      backend: btrfs\n"),
		"zfs without dataset":      pool("      backend: zfs\n"),
		"loopfile without baseDir": pool("      backend: loopfile\n"),
		"unknown field":            pool("      backend: lvmthin\n      bogus: x\n"),
		"malformed yaml":           "node-a: : :\n",
		"no pools":                 "node-a:\n  zone: rack-1\n",
		"invalid zfs block size":   pool("      backend: zfs\n      zfsDataset: tank/miroir\n      zfsVolBlockSize: 12K\n"),
		"oversized zfs block size": pool("      backend: zfs\n      zfsDataset: tank/miroir\n      zfsVolBlockSize: 256K\n"),
		"invalid zfs compression":  pool("      backend: zfs\n      zfsDataset: tank/miroir\n      zfsCompression: snappy\n"),
		"empty pools":              "node-a:\n  pools: {}\n",
		"invalid pool name": "node-a:\n  pools:\n    Fast_NVMe:\n" +
			"      backend: lvmthin\n",
		"pools sharing a device": "node-a:\n  pools:\n" +
			"    a:\n      backend: lvmthin\n      device: /dev/sda\n" +
			"    b:\n      backend: lvmthin\n      device: /dev/sda\n",
		"invalid address":   pool("      backend: lvmthin\n") + "  address: not-an-ip\n",
		"address is a CIDR": pool("      backend: lvmthin\n") + "  address: 10.0.0.0/24\n",
		"duplicate address": pool("      backend: lvmthin\n") + "  address: 10.0.100.1\n" +
			"node-b:\n  address: 10.0.100.1\n  pools:\n    default:\n      backend: lvmthin\n",
		"duplicate address ipv6": pool("      backend: lvmthin\n") + "  address: fd00:1::2\n" +
			"node-b:\n  address: fd00:0001:0:0::2\n  pools:\n    default:\n      backend: lvmthin\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeMap(t, body)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadLegacyFlatShape(t *testing.T) {
	_, err := Load(writeMap(t, `
node-a:
  backend: lvmthin
  device: /dev/disk/by-partlabel/r-miroir
`))
	if err == nil {
		t.Fatal("expected an error for the pre-0.10 flat shape")
	}
	if !strings.Contains(err.Error(), "pools") {
		t.Fatalf("error should point at the pools migration, got: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("expected an error for a missing file")
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
