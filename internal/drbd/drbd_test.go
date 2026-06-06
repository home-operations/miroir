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
	"path/filepath"
	"strings"
	"testing"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
)

func testResource(local string) Resource {
	return Resource{
		Name:      "pvc-1",
		Minor:     1000,
		Port:      7000,
		Quorum:    homefsv1alpha1.QuorumLastManStanding,
		LocalNode: local,
		LocalDisk: "/dev/vg-homefs/pvc-1",
		Peers: []Peer{
			{Node: "kharkiv", NodeID: 0, Address: "192.168.1.41"},
			{Node: "paris", NodeID: 1, Address: "192.168.1.42"},
		},
	}
}

func TestRenderDeterministicAndLocalDisk(t *testing.T) {
	r := testResource("kharkiv")
	a, b := Render(r), Render(r)
	if a != b {
		t.Fatal("render is not deterministic")
	}
	if !strings.Contains(a, `disk "/dev/vg-homefs/pvc-1";`) {
		t.Fatalf("local disk path missing:\n%s", a)
	}
	if !strings.Contains(a, `disk "/dev/drbd/this/is/not/used";`) {
		t.Fatalf("peer placeholder missing:\n%s", a)
	}
	if !strings.Contains(a, "quorum off;") {
		t.Fatalf("last-man-standing must render quorum off:\n%s", a)
	}
	if !strings.Contains(a, `address ipv4 192.168.1.42:7000;`) {
		t.Fatalf("peer address missing:\n%s", a)
	}
}

func TestRenderSharedSecret(t *testing.T) {
	r := testResource("kharkiv")
	if out := Render(r); strings.Contains(out, "shared-secret") {
		t.Fatalf("no auth must render for secretless volumes (pre-secret CRs):\n%s", out)
	}
	r.Secret = "0123456789abcdef"
	out := Render(r)
	if !strings.Contains(out, "cram-hmac-alg sha1;") ||
		!strings.Contains(out, `shared-secret "0123456789abcdef";`) {
		t.Fatalf("peer auth missing:\n%s", out)
	}
}

func TestParseEvent2(t *testing.T) {
	cases := []struct{ line, want string }{
		{"exists resource name:pvc-1 role:Secondary suspended:no", "pvc-1"},
		{"change connection name:pvc-1 peer-node-id:1 connection:StandAlone", "pvc-1"},
		{"change device name:pvc-2 volume:0 minor:1000 disk:UpToDate", "pvc-2"},
		{"destroy resource name:pvc-3", "pvc-3"},
		{"change peer-device name:pvc-1 peer-node-id:1 replication:SyncTarget", "pvc-1"},
		{"exists -", ""},
		{"call helper name:pvc-1 helper:before-resync-target", ""},
		{"", ""},
		{"garbage", ""},
	}
	for _, tc := range cases {
		if got := parseEvent2(tc.line); got != tc.want {
			t.Errorf("parseEvent2(%q) = %q, want %q", tc.line, got, tc.want)
		}
	}
}

func TestRenderFreezeQuorum(t *testing.T) {
	r := testResource("kharkiv")
	r.Quorum = homefsv1alpha1.QuorumFreeze
	out := Render(r)
	if !strings.Contains(out, "quorum majority;") || !strings.Contains(out, "on-no-quorum io-error;") {
		t.Fatalf("freeze policy not rendered:\n%s", out)
	}
}

func TestRenderNoAutoSplitBrainResolution(t *testing.T) {
	out := Render(testResource("kharkiv"))
	for _, directive := range []string{
		"after-sb-0pri disconnect;",
		"after-sb-1pri disconnect;",
		"after-sb-2pri disconnect;",
	} {
		if !strings.Contains(out, directive) {
			t.Fatalf("missing %q:\n%s", directive, out)
		}
	}
}

func TestDay0GIDeterministicAndEven(t *testing.T) {
	a, b := Day0GI("pvc-1"), Day0GI("pvc-1")
	if a != b || len(a) != 16 {
		t.Fatalf("unstable or malformed day0 GI: %q %q", a, b)
	}
	if Day0GI("pvc-2") == a {
		t.Fatal("different volumes must get different GIs")
	}
	last := a[len(a)-1]
	if !strings.ContainsRune("02468ACE", rune(last)) {
		t.Fatalf("day0 GI must be even (primary-writes bit clear), got %q", a)
	}
}

func fakeMknod(string, uint32, int) error { return nil }

type fakeExec struct {
	calls     []string
	responses map[string]string
	errOn     map[string]error
	errOnce   map[string]error
}

func (f *fakeExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	for key, err := range f.errOnce {
		if strings.Contains(line, key) {
			delete(f.errOnce, key)
			return "", err
		}
	}
	for key, err := range f.errOn {
		if strings.Contains(line, key) {
			return "", err
		}
	}
	for key, out := range f.responses {
		if strings.Contains(line, key) {
			return out, nil
		}
	}
	if strings.Contains(line, "dump-md") {
		// Fresh backing device by default.
		return "", errors.New("Exclusive open failed. no valid meta data")
	}
	return "", nil
}

func (f *fakeExec) calledWith(t *testing.T, substr string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			return
		}
	}
	t.Fatalf("expected call containing %q, got %v", substr, f.calls)
}

func (f *fakeExec) notCalledWith(t *testing.T, substr string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			t.Fatalf("expected no call containing %q, got %q", substr, c)
		}
	}
}

func TestApplyFreshResource(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource("kharkiv") // node-id 0 → winner

	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	// Winner seeds Consistent+UpToDate flags into EVERY slot, its own
	// included — partial seeding leaves the local current-UUID random and
	// the first handshake aborts with "unrelated data" (blockstor Bug 284).
	fe.calledWith(t, "set-gi --node-id 0 "+Day0GI("pvc-1")+":0:0:0:1:1")
	fe.calledWith(t, "set-gi --node-id 1 "+Day0GI("pvc-1")+":0:0:0:1:1")
	fe.calledWith(t, "set-gi --node-id 31 "+Day0GI("pvc-1")+":0:0:0:1:1")
	// drbdmeta is addressed by minor: the resource/volume form is drbdadm
	// syntax and makes drbdmeta consult kernel state through a malformed
	// drbdsetup invocation with undefined output.
	fe.calledWith(t, "drbdmeta --force 1000 v09 /dev/vg-homefs/pvc-1 internal set-gi")
	fe.notCalledWith(t, "drbdmeta --force pvc-1/0")
	fe.calledWith(t, "drbdadm adjust pvc-1")

	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.res")); err != nil {
		t.Fatal("res file not written")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written")
	}
}

func TestApplyNonWinnerSeedsBareGI(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource("paris") // node-id 1 → not winner

	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	// Non-winner seeds the bare day0 GI (no flags) into every slot and
	// reaches UpToDate at the first handshake.
	fe.calledWith(t, "set-gi --node-id 0 "+Day0GI("pvc-1")+":0:0:0")
	fe.calledWith(t, "set-gi --node-id 1 "+Day0GI("pvc-1")+":0:0:0")
	fe.notCalledWith(t, ":1:1")
}

func TestApplySkipSeedLeavesJustCreatedMetadata(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource("kharkiv")
	r.SkipSeed = true // late joiner: must full-sync, not pose as a day0 twin

	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "set-gi")
	fe.calledWith(t, "drbdadm adjust pvc-1")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written")
	}
}

func TestApplyIdempotent(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource("kharkiv")

	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	before := len(fe.calls)
	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	// Second pass: only adjust — no create-md, no re-seeding (would
	// clobber runtime generations / live bitmaps).
	for _, c := range fe.calls[before:] {
		if strings.Contains(c, "create-md") || strings.Contains(c, "set-gi") {
			t.Fatalf("second apply must not re-init metadata: %q", c)
		}
	}
	fe.calledWith(t, "drbdadm adjust pvc-1")
}

func TestApplyReseedsAfterMidSeedCrash(t *testing.T) {
	fe := &fakeExec{errOn: map[string]error{
		"set-gi --node-id 1": errors.New("exit status 20: Unexpected output from drbdsetup"),
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource("paris")

	if err := d.Apply(context.Background(), r); err == nil {
		t.Fatal("expected mid-seed failure")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); !os.IsNotExist(err) {
		t.Fatal("marker must not be written after a failed seed")
	}

	// Retry: metadata now exists on disk (create-md ran), but the seeding
	// sentinel proves it is our own half-seeded attempt — it must be
	// re-seeded in full, never adopted (adoption deadlocks the handshake:
	// both replicas Inconsistent, no sync source). The dump is
	// deliberately non-virgin so only the sentinel can explain the
	// re-seed.
	fe.errOn = nil
	fe.responses = map[string]string{"dump-md": "current-uuid 0xDEADBEEF00000001;"}
	fe.calls = nil
	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "set-gi --node-id 1 "+Day0GI("pvc-1"))
	fe.calledWith(t, "set-gi --node-id 31 "+Day0GI("pvc-1"))
	fe.notCalledWith(t, "create-md")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written after successful re-seed")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-seeding")); !os.IsNotExist(err) {
		t.Fatal("seeding sentinel must be removed after success")
	}
}

func TestApplyAdoptsAttachedDevice(t *testing.T) {
	// Markers lost but the kernel holds the minor: a previous life
	// completed seeding and adjust — metadata is live, never touch it.
	// dump-md succeeds read-only on an attached minor with a stale-output
	// warning; the refusal form covers metadata-modifying probes.
	for name, fe := range map[string]*fakeExec{
		"kernel has resource": {responses: map[string]string{
			"drbdsetup status pvc-1": "pvc-1 role:Secondary\n  disk:Inconsistent",
		}},
		"stale warning": {responses: map[string]string{
			"dump-md": "# Output might be stale, since minor 1000 is attached\ncurrent-uuid 0x0000000000000004;",
		}},
		"configured refusal": {errOn: map[string]error{
			"dump-md": errors.New("Device 'pvc-1' is configured!"),
		}},
	} {
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
		if err := d.Apply(context.Background(), testResource("kharkiv")); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		fe.notCalledWith(t, "create-md")
		fe.notCalledWith(t, "set-gi")
		if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
			t.Fatalf("%s: adopted device must be marked created", name)
		}
		if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); err != nil {
			t.Fatalf("%s: adoption must leave a breadcrumb", name)
		}
	}
}

func TestApplyAppliesALOnUncleanClone(t *testing.T) {
	// A clone of an attached volume inherits a mid-flight activity log;
	// drbdmeta refuses to read it until apply-al replays it. The clone's
	// inherited GI (the source's, not this volume's day0) must then be
	// adopted, never re-seeded.
	fe := &fakeExec{
		errOnce: map[string]error{
			"dump-md": errors.New(`Found meta data is "unclean", please apply-al first`),
		},
		responses: map[string]string{"dump-md": "current-uuid 0xDEADBEEF00000001;"},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(context.Background(), testResource("kharkiv")); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm apply-al pvc-1/0")
	fe.notCalledWith(t, "create-md")
	fe.notCalledWith(t, "set-gi")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); err != nil {
		t.Fatal("clone adoption must leave a breadcrumb")
	}
}

func TestApplySurfacesNonDRBDBusyDevice(t *testing.T) {
	// A backing device held open by something other than DRBD (stale
	// mount, LVM) is not an attachment — it must error, not adopt.
	fe := &fakeExec{errOn: map[string]error{
		"dump-md": errors.New("open(/dev/vg-homefs/pvc-1) failed: Device or resource busy\n" +
			"Exclusive open failed. Do it anyways?\nOperation canceled."),
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(context.Background(), testResource("kharkiv")); err == nil {
		t.Fatal("busy backing device must surface as an error")
	}
	fe.notCalledWith(t, "create-md")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); !os.IsNotExist(err) {
		t.Fatal("busy device must not be marked created")
	}
}

func TestApplyFastPathCleansStaleSentinel(t *testing.T) {
	// marker + sentinel coexisting (crash in a past life, failed Down):
	// the fast path must clear the sentinel — left stale, it would
	// authorize re-seeding live metadata the moment the marker is lost.
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	for _, f := range []string{"pvc-1.md-created", "pvc-1.md-seeding"} {
		if err := os.WriteFile(filepath.Join(d.StateDir, f), nil, 0o640); err != nil {
			t.Fatal(err)
		}
	}

	if err := d.Apply(context.Background(), testResource("kharkiv")); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "set-gi")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-seeding")); !os.IsNotExist(err) {
		t.Fatal("fast path must remove a stale seeding sentinel")
	}
}

func TestApplyAdoptsLiveMetadataWithoutMarkers(t *testing.T) {
	// Markers lost, device detached, but the GI shows a Primary wrote
	// (current UUID moved off day0): live volume — adopt, no re-seed.
	fe := &fakeExec{responses: map[string]string{
		"dump-md": "current-uuid 0xDEADBEEF00000001;\nbitmap-uuid 0x0000000000000000;",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(context.Background(), testResource("kharkiv")); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "create-md")
	fe.notCalledWith(t, "set-gi")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); err != nil {
		t.Fatal("adoption must leave a breadcrumb")
	}
}

func TestApplyReseedsVirginMetadataWithoutMarkers(t *testing.T) {
	// Markers lost, device detached, GI still the day0 seed with clean
	// bitmaps: provably no data — re-seeding is safe and unsticks a
	// partially seeded volume.
	fe := &fakeExec{responses: map[string]string{
		"dump-md": "current-uuid 0x" + Day0GI("pvc-1") + ";\nbitmap-uuid 0x0000000000000000;",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(context.Background(), testResource("kharkiv")); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "set-gi --node-id 31")
	fe.notCalledWith(t, "create-md")
}

func TestVirginMetadata(t *testing.T) {
	day0 := "current-uuid 0x" + Day0GI("pvc-1") + ";"
	cases := []struct {
		name, dump string
		want       bool
	}{
		{"day0 seed", day0, true},
		{"just created", "current-uuid 0x0000000000000004;", true},
		{"primary wrote", "current-uuid 0xDEADBEEF00000001;", false},
		{"divergence tracked", day0 + "\n    bitmap-uuid 0x00000000DEAD0000;", false},
		{"unparseable", "garbage", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		if got := virginMetadata(tc.dump, "pvc-1"); got != tc.want {
			t.Errorf("%s: virginMetadata = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDownRemovesState(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource("kharkiv")

	if err := d.Apply(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if err := d.Down(context.Background(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup down pvc-1")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.res")); !os.IsNotExist(err) {
		t.Fatal("res file must be removed")
	}

	// Down on never-configured resource is a no-op.
	before := len(fe.calls)
	if err := d.Down(context.Background(), "pvc-other"); err != nil {
		t.Fatal(err)
	}
	if len(fe.calls) != before {
		t.Fatal("down on unknown resource must not call drbdadm")
	}
}

func TestStatusParsing(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		"drbdsetup status": `[{"name":"pvc-1","suspended-user":true,
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"Connected","peer-role":"Primary"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(context.Background(), "pvc-1")
	if err != nil {
		t.Fatal(err)
	}
	if s.DiskState != "UpToDate" || !s.Connected || s.SplitBrain || !s.PeerPrimary || !s.Suspended {
		t.Fatalf("unexpected status %+v", s)
	}
}

func TestUserSuspendedListsFrozenResources(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		"drbdsetup status": `[
			{"name":"pvc-1","suspended":true,"suspended-user":true},
			{"name":"pvc-2","suspended":false,"suspended-user":false}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	got, err := d.UserSuspended(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "pvc-1" {
		t.Fatalf("want [pvc-1], got %v", got)
	}
}

func TestStatusSplitBrain(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		"drbdsetup status": `[{"name":"pvc-1",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"StandAlone"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(context.Background(), "pvc-1")
	if err != nil {
		t.Fatal(err)
	}
	if !s.SplitBrain || s.Connected {
		t.Fatalf("StandAlone must surface as split-brain: %+v", s)
	}
}
