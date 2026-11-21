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
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// AllocateMinor hands out the base minor first, increments for new volumes,
// returns the same minor on repeat (idempotent), and survives a fresh Driver
// over the same StateDir (the assignment is persisted).
func TestAllocateMinorStableSequentialPersistent(t *testing.T) {
	dir := t.TempDir()
	d := &Driver{StateDir: dir}

	a, err := d.AllocateMinor(t.Context(), "pvc-a", 0)
	if err != nil {
		t.Fatalf("allocate pvc-a: %v", err)
	}
	if a != minorBase {
		t.Errorf("first minor = %d, want %d", a, minorBase)
	}

	b, err := d.AllocateMinor(t.Context(), "pvc-b", 0)
	if err != nil {
		t.Fatalf("allocate pvc-b: %v", err)
	}
	if b != minorBase+1 {
		t.Errorf("second minor = %d, want %d", b, minorBase+1)
	}

	// Repeat call is stable, not a new allocation.
	if again, _ := d.AllocateMinor(t.Context(), "pvc-a", 0); again != a {
		t.Errorf("repeat pvc-a = %d, want stable %d", again, a)
	}

	// A fresh Driver over the same StateDir reads the persisted assignment.
	fresh := &Driver{StateDir: dir}
	if got, _ := fresh.AllocateMinor(t.Context(), "pvc-a", 0); got != a {
		t.Errorf("after reload pvc-a = %d, want persisted %d", got, a)
	}
	if got, _ := fresh.AllocateMinor(t.Context(), "pvc-c", 0); got != minorBase+2 {
		t.Errorf("new volume after reload = %d, want %d", got, minorBase+2)
	}
}

// A minor already claimed by a .res file on disk (a resource created out of
// band, or a partially-recorded assignment) must be skipped so two resources
// never collide on /dev/drbd<minor>.
// The scan must parse miroir's OWN rendered device form ("device minor
// N;", no device name) — a fixture built from Render itself, so the two
// cannot drift apart again.
func TestAllocateMinorSkipsMinorsFromOwnRender(t *testing.T) {
	dir := t.TempDir()
	d := &Driver{StateDir: dir}

	res := Render(Resource{
		Name:      "legacy",
		Minor:     minorBase,
		Port:      7000,
		LocalNode: nodeA,
		LocalDisk: "/dev/mapper/x",
		Peers: []Peer{
			{Node: nodeA, NodeID: 0, Address: addrA},
			{Node: nodeB, NodeID: 1, Address: addrB},
		},
	})
	if err := os.WriteFile(filepath.Join(dir, "legacy.res"), []byte(res), 0o640); err != nil {
		t.Fatal(err)
	}

	got, err := d.AllocateMinor(t.Context(), "pvc-new", 0)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if got != minorBase+1 {
		t.Errorf("minor = %d, want %d (must skip the rendered minor)", got, minorBase+1)
	}
}

// A volume whose .res survived but whose assignment record was lost must
// recover its own minor, not claim a fresh one — the kernel resource may
// still be running on it.
// Down releases the volume's minor.assign entry so the minor is reused,
// not leaked for the lifetime of the StateDir.
func TestDownReleasesMinorAssignment(t *testing.T) {
	dir := t.TempDir()
	fe := &fakeExec{}
	d := &Driver{StateDir: dir, Exec: fe.run, Mknod: fakeMknod}

	first, err := d.AllocateMinor(t.Context(), "pvc-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	// A .res must exist or Down short-circuits as never-configured.
	if err := os.WriteFile(filepath.Join(dir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := d.Down(t.Context(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	// A fresh volume reclaims the freed minor instead of advancing past it.
	reused, err := d.AllocateMinor(t.Context(), volPvc2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if reused != first {
		t.Fatalf("freed minor %d must be reused, got %d", first, reused)
	}
}

func TestAllocateMinorRecoversOwnMinorFromRes(t *testing.T) {
	dir := t.TempDir()
	d := &Driver{StateDir: dir}

	if err := os.WriteFile(filepath.Join(dir, "pvc-1.res"),
		[]byte("resource \"pvc-1\" {\n    device minor 1004;\n}\n"), 0o640); err != nil {
		t.Fatal(err)
	}

	got, err := d.AllocateMinor(t.Context(), "pvc-1", 0)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if got != 1004 {
		t.Errorf("minor = %d, want the recovered 1004", got)
	}
}

// A preferred minor is the one the volume's status publishes; it can
// outlive StateDir, so it yields to whatever this node recorded since,
// unless the kernel still runs the volume on it.
func TestAllocateMinorPreferred(t *testing.T) {
	const other = "pvc-other"
	resWithMinor := func(name string, minor int32) string {
		return "resource \"" + name + "\" {\n    device minor " + strconv.Itoa(int(minor)) + ";\n}\n"
	}
	liveOn1013 := `[{"name":"` + volPvc1 + `","devices":[{"minor":1013}]}]`
	notUp := errors.New("exit status 10: pvc-1: No such resource")
	tests := []struct {
		name      string
		assigned  string           // minor.assign content
		res       map[string]int32 // rendered .res per volume
		status    string           // kernel view of the volume
		statusErr error
		preferred int32
		want      int32
		wantErr   bool
	}{
		{name: "free preferred minor is taken", preferred: 1013, want: 1013},
		{name: "no preference takes the lowest free minor", want: minorBase},
		{
			name:      "preferred minor assigned to another volume",
			assigned:  other + " 1013\n",
			preferred: 1013,
			want:      minorBase,
		},
		{
			name:      "preferred minor rendered by another volume",
			res:       map[string]int32{other: 1013},
			statusErr: notUp,
			preferred: 1013,
			want:      minorBase,
		},
		{
			name:      "own assignment wins over the preference",
			assigned:  volPvc1 + " 1002\n",
			preferred: 1013,
			want:      1002,
		},
		{
			name:      "own render wins over the preference",
			res:       map[string]int32{volPvc1: 1004},
			preferred: 1013,
			want:      1004,
		},
		{
			name:      "own render yields to another volume's assignment",
			assigned:  other + " 1013\n",
			res:       map[string]int32{volPvc1: 1013, other: 1013},
			preferred: 1013,
			want:      minorBase,
		},
		{
			name:      "own render yields to another volume's render",
			res:       map[string]int32{volPvc1: 1013, other: 1013},
			statusErr: notUp,
			preferred: 1013,
			want:      minorBase,
		},
		{
			name:      "running on the contested own render keeps it",
			assigned:  other + " 1013\n",
			res:       map[string]int32{volPvc1: 1013, other: 1013},
			status:    liveOn1013,
			preferred: 1013,
			want:      1013,
		},
		{
			name:      "running on the contested preferred minor keeps it",
			assigned:  other + " 1013\n",
			status:    liveOn1013,
			preferred: 1013,
			want:      1013,
		},
		{
			name:      "running on another minor yields the contested one",
			assigned:  other + " 1013\n",
			status:    `[{"name":"` + volPvc1 + `","devices":[{"minor":1002}]}]`,
			preferred: 1013,
			want:      minorBase,
		},
		{
			name:      "unreadable kernel view defers",
			assigned:  other + " 1013\n",
			statusErr: errors.New("exit status 20"),
			preferred: 1013,
			wantErr:   true,
		},
		{
			name:      "own assignment yields to another volume's assignment",
			assigned:  volPvc1 + " 1013\n" + other + " 1013\n",
			res:       map[string]int32{volPvc1: 1013, other: 1013},
			statusErr: notUp,
			preferred: 1013,
			want:      minorBase,
		},
		{
			name:      "running on the contested own assignment keeps it",
			assigned:  volPvc1 + " 1013\n" + other + " 1013\n",
			res:       map[string]int32{volPvc1: 1013, other: 1013},
			status:    liveOn1013,
			preferred: 1013,
			want:      1013,
		},
		{
			name:      "contested own assignment with an unreadable kernel view defers",
			assigned:  volPvc1 + " 1013\n" + other + " 1013\n",
			statusErr: errors.New("exit status 20"),
			wantErr:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			statusCmd := cmdDrbdsetupStatus + " --json " + volPvc1
			fe := &fakeExec{}
			if tc.status != "" {
				fe.responses = map[string]string{statusCmd: tc.status}
			}
			if tc.statusErr != nil {
				fe.errOn = map[string]error{statusCmd: tc.statusErr}
			}
			d := &Driver{StateDir: dir, Exec: fe.run}
			if tc.assigned != "" {
				if err := os.WriteFile(filepath.Join(dir, "minor.assign"), []byte(tc.assigned), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			for name, minor := range tc.res {
				if err := os.WriteFile(filepath.Join(dir, name+".res"), []byte(resWithMinor(name, minor)), 0o640); err != nil {
					t.Fatal(err)
				}
			}

			got, err := d.AllocateMinor(t.Context(), volPvc1, tc.preferred)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("minor = %d, want an error", got)
				}
				raw, err := os.ReadFile(filepath.Join(dir, "minor.assign"))
				if err != nil {
					t.Fatal(err)
				}
				if string(raw) != tc.assigned {
					t.Fatalf("failed allocation rewrote minor.assign: %q", raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("allocate: %v", err)
			}
			if got != tc.want {
				t.Fatalf("minor = %d, want %d", got, tc.want)
			}
			// The allocation is recorded, and a dropped claim is replaced:
			// the other volume never ends up on the same minor.
			next, err := d.AllocateMinor(t.Context(), other, 0)
			if err != nil {
				t.Fatalf("allocate %s: %v", other, err)
			}
			if next == got {
				t.Fatalf("minor %d handed out twice", got)
			}
		})
	}
}

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

	got, err := d.AllocateMinor(t.Context(), "pvc-new", 0)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if got != minorBase+2 {
		t.Errorf("minor = %d, want %d (must skip 1000/1001 held by legacy.res)", got, minorBase+2)
	}
}

// The orphan sweep is the last owner of a crash leftover's minor: the
// volume never comes back to Down on this node, so the sweep must release
// the assignment or the minor is burned for the StateDir's lifetime.
func TestSweepOrphansReleasesMinorAssignment(t *testing.T) {
	dir := t.TempDir()
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"pvc-1","role":"Secondary","connections":[]}]`,
	}}
	d := &Driver{StateDir: dir, Exec: fe.run, Mknod: fakeMknod}

	first, err := d.AllocateMinor(t.Context(), "pvc-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pvc-1.res"), []byte("resource \"pvc-1\" {}\n"), 0o640); err != nil {
		t.Fatal(err)
	}

	if err := d.SweepOrphans(t.Context(), func(string) bool { return false }); err != nil {
		t.Fatal(err)
	}
	reused, err := d.AllocateMinor(t.Context(), volPvc2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if reused != first {
		t.Fatalf("swept orphan's minor %d must be reused, got %d", first, reused)
	}
}

// A rendered config that cannot be read may hold any minor, so allocation
// must fail instead of risking a duplicate.
func TestAllocateMinorFailsOnUnreadableRes(t *testing.T) {
	d := &Driver{StateDir: t.TempDir(), Exec: (&fakeExec{}).run}
	if err := os.Mkdir(d.path("pvc-other.res"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AllocateMinor(t.Context(), volPvc1, 0); err == nil {
		t.Fatal("allocation must fail while another volume's config is unreadable")
	}
}
