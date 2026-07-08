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

package drbd

import (
	"os"
	"path/filepath"
	"testing"
)

// AllocateMinor hands out the base minor first, increments for new volumes,
// returns the same minor on repeat (idempotent), and survives a fresh Driver
// over the same StateDir (the assignment is persisted).
func TestAllocateMinorStableSequentialPersistent(t *testing.T) {
	dir := t.TempDir()
	d := &Driver{StateDir: dir}

	a, err := d.AllocateMinor("pvc-a")
	if err != nil {
		t.Fatalf("allocate pvc-a: %v", err)
	}
	if a != minorBase {
		t.Errorf("first minor = %d, want %d", a, minorBase)
	}

	b, err := d.AllocateMinor("pvc-b")
	if err != nil {
		t.Fatalf("allocate pvc-b: %v", err)
	}
	if b != minorBase+1 {
		t.Errorf("second minor = %d, want %d", b, minorBase+1)
	}

	// Repeat call is stable, not a new allocation.
	if again, _ := d.AllocateMinor("pvc-a"); again != a {
		t.Errorf("repeat pvc-a = %d, want stable %d", again, a)
	}

	// A fresh Driver over the same StateDir reads the persisted assignment.
	fresh := &Driver{StateDir: dir}
	if got, _ := fresh.AllocateMinor("pvc-a"); got != a {
		t.Errorf("after reload pvc-a = %d, want persisted %d", got, a)
	}
	if got, _ := fresh.AllocateMinor("pvc-c"); got != minorBase+2 {
		t.Errorf("new volume after reload = %d, want %d", got, minorBase+2)
	}
}

// A minor already claimed by a .res file on disk (a resource created out of
// band, or a partially-recorded assignment) must be skipped so two resources
// never collide on /dev/drbd<minor>.
func TestAllocateMinorSkipsMinorsUsedInResFiles(t *testing.T) {
	dir := t.TempDir()
	d := &Driver{StateDir: dir}

	// A .res claiming minorBase and minorBase+1, with no matching assignment.
	res := "resource legacy {\n" +
		"  device drbd0 minor 1000;\n" +
		"  device drbd1 minor 1001;\n" +
		"}\n"
	if err := os.WriteFile(filepath.Join(dir, "legacy.res"), []byte(res), 0o640); err != nil {
		t.Fatal(err)
	}

	got, err := d.AllocateMinor("pvc-new")
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if got != minorBase+2 {
		t.Errorf("minor = %d, want %d (must skip 1000/1001 held by legacy.res)", got, minorBase+2)
	}
}
