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
  backend: lvmthin
  device: /dev/disk/by-partlabel/r-miroir
  thinPoolSize: 400g
  zone: rack-1
  address: 10.0.100.1
node-b:
  backend: zfs
  zfsDataset: tank/miroir
  address: "fd00:1::2"
node-c:
  backend: loopfile
  baseDir: /var/lib/miroir
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 {
		t.Fatalf("want 3 nodes, got %d", len(m))
	}
	if m["node-a"].Backend != miroirv1alpha1.BackendLVMThin || m["node-a"].ThinPoolSize != "400g" {
		t.Fatalf("node-a parsed wrong: %+v", m["node-a"])
	}
	if m["node-a"].Zone != "rack-1" {
		t.Fatalf("node-a zone not parsed: %+v", m["node-a"])
	}
	if m["node-b"].ZFSDataset != "tank/miroir" {
		t.Fatalf("node-b dataset wrong: %+v", m["node-b"])
	}
	if m["node-c"].BaseDir != "/var/lib/miroir" {
		t.Fatalf("node-c baseDir wrong: %+v", m["node-c"])
	}
	if m["node-a"].Address != "10.0.100.1" || m["node-b"].Address != "fd00:1::2" {
		t.Fatalf("addresses parsed wrong: %q %q", m["node-a"].Address, m["node-b"].Address)
	}
}

func TestLoadErrors(t *testing.T) {
	cases := map[string]string{
		"unknown backend":          "node-a:\n  backend: btrfs\n",
		"zfs without dataset":      "node-b:\n  backend: zfs\n",
		"loopfile without baseDir": "node-c:\n  backend: loopfile\n",
		"unknown field":            "node-a:\n  backend: lvmthin\n  bogus: x\n",
		"malformed yaml":           "node-a: : :\n",
		"invalid address":          "node-a:\n  backend: lvmthin\n  address: not-an-ip\n",
		"address is a CIDR":        "node-a:\n  backend: lvmthin\n  address: 10.0.0.0/24\n",
		"duplicate address":        "node-a:\n  backend: lvmthin\n  address: 10.0.100.1\nnode-b:\n  backend: lvmthin\n  address: 10.0.100.1\n",
		"duplicate address ipv6":   "node-a:\n  backend: lvmthin\n  address: fd00:1::2\nnode-b:\n  backend: lvmthin\n  address: fd00:0001:0:0::2\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeMap(t, body)); err == nil {
				t.Fatal("expected an error")
			}
		})
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
