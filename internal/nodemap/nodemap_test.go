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
	nodeKharkiv = "kharkiv"
	nodeParis   = "paris"
	nodeOslo    = "oslo"
	nodeBergen  = "bergen"
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
kharkiv:
  backend: lvmthin
  device: /dev/disk/by-partlabel/r-miroir
  thinPoolSize: 400g
  zone: rack-1
  address: 10.0.100.1
paris:
  backend: zfs
  zfsDataset: tank/miroir
  address: "fd00:1::2"
oslo:
  backend: loopfile
  baseDir: /var/lib/miroir
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 {
		t.Fatalf("want 3 nodes, got %d", len(m))
	}
	if m["kharkiv"].Backend != miroirv1alpha1.BackendLVMThin || m["kharkiv"].ThinPoolSize != "400g" {
		t.Fatalf("kharkiv parsed wrong: %+v", m["kharkiv"])
	}
	if m["kharkiv"].Zone != "rack-1" {
		t.Fatalf("kharkiv zone not parsed: %+v", m["kharkiv"])
	}
	if m["paris"].ZFSDataset != "tank/miroir" {
		t.Fatalf("paris dataset wrong: %+v", m["paris"])
	}
	if m["oslo"].BaseDir != "/var/lib/miroir" {
		t.Fatalf("oslo baseDir wrong: %+v", m["oslo"])
	}
	if m["kharkiv"].Address != "10.0.100.1" || m["paris"].Address != "fd00:1::2" {
		t.Fatalf("addresses parsed wrong: %q %q", m["kharkiv"].Address, m["paris"].Address)
	}
}

func TestLoadErrors(t *testing.T) {
	cases := map[string]string{
		"unknown backend":          "kharkiv:\n  backend: btrfs\n",
		"zfs without dataset":      "paris:\n  backend: zfs\n",
		"loopfile without baseDir": "oslo:\n  backend: loopfile\n",
		"unknown field":            "kharkiv:\n  backend: lvmthin\n  bogus: x\n",
		"malformed yaml":           "kharkiv: : :\n",
		"invalid address":          "kharkiv:\n  backend: lvmthin\n  address: not-an-ip\n",
		"address is a CIDR":        "kharkiv:\n  backend: lvmthin\n  address: 10.0.0.0/24\n",
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
	replicas := []miroirv1alpha1.Replica{{Node: nodeKharkiv}, {Node: nodeParis}}
	cases := map[string]struct {
		m    Map
		want string
	}{
		"prefers a zone no replica occupies": {
			m: Map{
				nodeKharkiv: {Zone: "a"}, nodeParis: {Zone: "b"},
				nodeOslo: {Zone: "a"}, nodeBergen: {Zone: "c"},
			},
			want: nodeBergen,
		},
		"zoneless spare is unconstrained": {
			m:    Map{nodeKharkiv: {Zone: "a"}, nodeParis: {Zone: "b"}, nodeOslo: {}},
			want: nodeOslo,
		},
		"falls back to an occupied zone when none is free": {
			m:    Map{nodeKharkiv: {Zone: "a"}, nodeParis: {Zone: "b"}, nodeOslo: {Zone: "a"}},
			want: nodeOslo,
		},
		"name order breaks ties": {
			m:    Map{nodeKharkiv: {}, nodeParis: {}, nodeOslo: {}, nodeBergen: {}},
			want: nodeBergen,
		},
		"no spare node": {
			m:    Map{nodeKharkiv: {}, nodeParis: {}},
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
			m:      Map{nodeKharkiv: {Address: "10.0.100.5"}},
			client: withNode(), // no Node object registered
			want:   "10.0.100.5",
		},
		"falls back to InternalIP": {
			m:      Map{nodeKharkiv: {}},
			client: withNode(internalIP(nodeKharkiv, "192.168.1.41")),
			want:   "192.168.1.41",
		},
		"node without InternalIP errors": {
			m:       Map{nodeKharkiv: {}},
			client:  withNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeKharkiv}}),
			wantErr: true,
		},
		"absent node errors": {
			m:       Map{nodeKharkiv: {}},
			client:  withNode(),
			wantErr: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := tc.m.ReplicationAddress(t.Context(), tc.client, nodeKharkiv)
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
