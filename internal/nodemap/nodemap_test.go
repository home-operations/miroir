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

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
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
paris:
  backend: zfs
  zfsDataset: tank/miroir
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
}

func TestLoadErrors(t *testing.T) {
	cases := map[string]string{
		"unknown backend":          "kharkiv:\n  backend: btrfs\n",
		"zfs without dataset":      "paris:\n  backend: zfs\n",
		"loopfile without baseDir": "oslo:\n  backend: loopfile\n",
		"unknown field":            "kharkiv:\n  backend: lvmthin\n  bogus: x\n",
		"malformed yaml":           "kharkiv: : :\n",
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
