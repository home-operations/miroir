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
	"context"
	"errors"
	"os"
	"testing"
)

func TestWipeMetadata(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := d.WipeMetadata(context.Background(), volPvc1, "/dev/vg-miroir/pvc-1", 1000); err != nil {
		t.Fatalf("WipeMetadata: %v", err)
	}
	fe.calledWith(t, "drbdmeta --force 1000 v09 /dev/vg-miroir/pvc-1 internal wipe-md")
}

func TestVirginMetadata(t *testing.T) {
	day0 := "current-uuid 0x" + Day0GI(volPvc1) + ";"
	cases := []struct {
		name, dump string
		want       bool
	}{
		{"day0 seed", day0, true},
		{"just created", "current-uuid 0x0000000000000004;", true},
		{"primary wrote", mockCurrentUUID, false},
		{"divergence tracked", day0 + "\n    bitmap-uuid 0x00000000DEAD0000;", false},
		{"unparseable", "garbage", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		if got := virginMetadata(tc.dump, volPvc1); got != tc.want {
			t.Errorf("%s: virginMetadata = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The foreign-metadata wipe must fire only on proof: metadata present
// (or unclean — still metadata) wipes and marks; a bare device marks
// without wiping (wipe-md there would zero the filesystem tail); the
// marker short-circuits every later call so the probe can never run
// against a device a consumer may have mounted.
func TestWipeForeignMetadataProbesThenWipesOnce(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{"dump-md": "version \"v09\";"}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path("used.res"), []byte("device minor 1000;\n"), 0o640); err != nil {
		t.Fatal(err)
	}

	if err := d.WipeForeignMetadata(t.Context(), volPvc1, "/dev/x"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdmeta 1001 v09 /dev/x internal dump-md")
	fe.calledWith(t, "drbdmeta --force 1001 v09 /dev/x internal wipe-md")
	assigned, err := d.readAssignments()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := assigned["@foreign-metadata-probe/"+volPvc1]; ok {
		t.Fatalf("temporary probe minor must be released: %v", assigned)
	}
	fe.calls = nil
	if err := d.WipeForeignMetadata(t.Context(), volPvc1, "/dev/x"); err != nil {
		t.Fatal(err)
	}
	if len(fe.calls) != 0 {
		t.Fatalf("marker must short-circuit later calls, got %v", fe.calls)
	}
}

func TestWipeForeignMetadataSkipsBareDevice(t *testing.T) {
	fe := &fakeExec{} // default dump-md answer: no valid meta data
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.WipeForeignMetadata(t.Context(), volPvc1, "/dev/x"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "wipe-md")
	if _, err := os.Stat(d.path(volPvc1 + ".md-wiped")); err != nil {
		t.Fatalf("a bare device must still mark as settled: %v", err)
	}
}

func TestWipeForeignMetadataWipesUncleanMetadata(t *testing.T) {
	fe := &fakeExec{errOn: map[string]error{"dump-md": errors.New("unclean meta data; apply-al first")}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.WipeForeignMetadata(t.Context(), volPvc1, "/dev/x"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "wipe-md")
}

// The overhead bound must dominate the bitmap arithmetic it stands in
// for (1 bit per 4KiB per peer slot) with real slack for the superblock
// and activity log, stay extent-aligned, and grow monotonically.
func TestInternalMetaOverheadBounds(t *testing.T) {
	prev := int64(0)
	for _, size := range []int64{1 << 30, 5 << 30, 100 << 30, 1 << 40} {
		got := InternalMetaOverhead(size)
		if bitmaps := size / 32768 * maxPeers; got <= bitmaps {
			t.Fatalf("overhead %d for %d must exceed the raw bitmap bytes %d", got, size, bitmaps)
		}
		if got%(4<<20) != 0 {
			t.Fatalf("overhead %d for %d must be 4MiB-aligned", got, size)
		}
		if got < prev {
			t.Fatalf("overhead must grow with size: %d then %d", prev, got)
		}
		prev = got
	}
}

// MetadataAdopted reflects the adoption marker ensureMetadata writes for
// metadata inherited from a previous life — the restore seed mint must
// never fire on such a leg.
func TestMetadataAdoptedTracksMarker(t *testing.T) {
	d := &Driver{StateDir: t.TempDir(), Exec: (&fakeExec{}).run, Mknod: fakeMknod}
	if d.MetadataAdopted(volPvc1) {
		t.Fatal("no marker must read as not adopted")
	}
	if err := d.markAdopted(volPvc1); err != nil {
		t.Fatal(err)
	}
	if !d.MetadataAdopted(volPvc1) {
		t.Fatal("marker must read as adopted")
	}
}
