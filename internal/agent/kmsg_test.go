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

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCaptureKmsgFiltersAndCaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	var b strings.Builder
	b.WriteString("6,100,1;drbd pvc-other: Connection established\n")
	for i := range 40 {
		fmt.Fprintf(&b, "6,%d,1;drbd pvc-1: record %d\n", 200+i, i)
	}
	b.WriteString("6,300,1;drbd pvc-1: Split-Brain detected but unresolved, dropping connection!\n")
	if err := os.WriteFile(path, []byte(b.String()), 0o640); err != nil {
		t.Fatal(err)
	}

	lines := captureKmsg(path, "pvc-1", 30)
	if len(lines) != 30 {
		t.Fatalf("want 30 lines (cap), got %d", len(lines))
	}
	for _, l := range lines {
		if strings.Contains(l, "pvc-other") {
			t.Fatalf("foreign resource leaked through the filter: %q", l)
		}
	}
	if last := lines[len(lines)-1]; !strings.Contains(last, "Split-Brain detected") {
		t.Fatalf("the newest record must survive the cap, got %q", last)
	}
}

func TestCaptureKmsgMissingFile(t *testing.T) {
	if lines := captureKmsg(filepath.Join(t.TempDir(), "absent"), "pvc-1", 30); lines != nil {
		t.Fatalf("missing file must yield nil, got %v", lines)
	}
}
