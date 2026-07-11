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

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	volPvc1            = "pvc-1"
	cmdDrbdsetupStatus = "drbdsetup status"
	cmdDumpMD          = "dump-md"
	mockCurrentUUID    = "current-uuid 0xDEADBEEF00000001;"
	addrKharkiv        = "192.168.1.41"
	addrParis          = "192.168.1.42"
)

func testResource(local string) Resource {
	return Resource{
		Name:      volPvc1,
		Minor:     1000,
		Port:      7000,
		Quorum:    miroirv1alpha1.QuorumLastManStanding,
		LocalNode: local,
		LocalDisk: "/dev/vg-miroir/pvc-1",
		Peers: []Peer{
			{Node: nodeKharkiv, NodeID: 0, Address: addrKharkiv},
			{Node: nodeParis, NodeID: 1, Address: addrParis},
		},
	}
}

func TestRenderDeterministicAndLocalDisk(t *testing.T) {
	r := testResource(nodeKharkiv)
	a, b := Render(r), Render(r)
	if a != b {
		t.Fatal("render is not deterministic")
	}
	if !strings.Contains(a, `disk "/dev/vg-miroir/pvc-1";`) {
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
	r := testResource(nodeKharkiv)
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
		{"exists resource name:pvc-1 role:Secondary suspended:no", volPvc1},
		{"change connection name:pvc-1 peer-node-id:1 connection:StandAlone", volPvc1},
		{"change device name:pvc-2 volume:0 minor:1000 disk:UpToDate", "pvc-2"},
		{"destroy resource name:pvc-3", "pvc-3"},
		{"change peer-device name:pvc-1 peer-node-id:1 replication:SyncTarget", volPvc1},
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
	r := testResource(nodeKharkiv)
	r.Quorum = miroirv1alpha1.QuorumFreeze
	out := Render(r)
	if !strings.Contains(out, "quorum majority;") || !strings.Contains(out, "on-no-quorum io-error;") {
		t.Fatalf("freeze policy not rendered:\n%s", out)
	}
}

func TestRenderNoAutoSplitBrainResolution(t *testing.T) {
	out := Render(testResource(nodeKharkiv))
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
	a, b := Day0GI(volPvc1), Day0GI(volPvc1)
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

func TestResolveSplitBrainWinnerReconnects(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	// kharkiv is node id 0 — the seed winner and the survivor.
	if err := d.ResolveSplitBrain(context.Background(), testResource(nodeKharkiv)); err != nil {
		t.Fatalf("ResolveSplitBrain: %v", err)
	}
	fe.calledWith(t, "drbdadm connect pvc-1")
	fe.notCalledWith(t, "discard-my-data")
	fe.notCalledWith(t, "disconnect")
}

func TestResolveSplitBrainLoserDiscards(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	// paris is node id 1 — the loser: it must disconnect then reconnect
	// discarding its own generation so it becomes SyncTarget.
	if err := d.ResolveSplitBrain(context.Background(), testResource(nodeParis)); err != nil {
		t.Fatalf("ResolveSplitBrain: %v", err)
	}
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}

func TestResolveSplitBrainDisklessReconnects(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeParis)
	r.LocalDiskless = true // a tie-breaker never discards data
	if err := d.ResolveSplitBrain(context.Background(), r); err != nil {
		t.Fatalf("ResolveSplitBrain: %v", err)
	}
	fe.calledWith(t, "drbdadm connect pvc-1")
	fe.notCalledWith(t, "discard-my-data")
}

func TestWipeMetadata(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := d.WipeMetadata(context.Background(), volPvc1, "/dev/vg-miroir/pvc-1", 1000); err != nil {
		t.Fatalf("WipeMetadata: %v", err)
	}
	fe.calledWith(t, "drbdmeta --force 1000 v09 /dev/vg-miroir/pvc-1 internal wipe-md")
}

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
	if strings.Contains(line, cmdDumpMD) {
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
	r := testResource(nodeKharkiv) // node-id 0 → winner

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	// Winner seeds Consistent+UpToDate flags into the local slot and
	// every peer slot — partial seeding leaves the local current-UUID
	// random and the first handshake aborts "unrelated data".
	fe.calledWith(t, "set-gi --node-id 0 "+Day0GI(volPvc1)+":0:0:0:1:1")
	fe.calledWith(t, "set-gi --node-id 1 "+Day0GI(volPvc1)+":0:0:0:1:1")
	// Only actual peer slots are seeded, not all 32 metadata slots.
	fe.notCalledWith(t, "set-gi --node-id 31")
	// drbdmeta is addressed by minor: the resource/volume form is drbdadm
	// syntax and makes drbdmeta consult kernel state through a malformed
	// drbdsetup invocation with undefined output.
	fe.calledWith(t, "drbdmeta --force 1000 v09 /dev/vg-miroir/pvc-1 internal set-gi")
	fe.notCalledWith(t, "drbdmeta --force pvc-1/0")
	fe.calledWith(t, "drbdadm adjust pvc-1")

	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.res")); err != nil {
		t.Fatal("res file not written")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written")
	}
}

func TestApplyNonWinnerSeedsWasUpToDate(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeParis) // node-id 1 → not winner

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	// Non-winner seeds WasUpToDate (without Consistent): attaches
	// Inconsistent but keeps the bitmap clean so no full resync fires.
	// Reaches UpToDate at the first handshake.
	fe.calledWith(t, "set-gi --node-id 0 "+Day0GI(volPvc1)+":0:0:0:0:1")
	fe.calledWith(t, "set-gi --node-id 1 "+Day0GI(volPvc1)+":0:0:0:0:1")
	fe.notCalledWith(t, ":1:1")
	fe.notCalledWith(t, "set-gi --node-id 31")
}

func TestApplySkipSeedLeavesJustCreatedMetadata(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeKharkiv)
	r.SkipSeed = true // late joiner: must full-sync, not pose as a day0 twin

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "set-gi")
	fe.calledWith(t, "drbdadm adjust pvc-1")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written")
	}
}

// KernelAvailable keys on the kernel answering, not the binary being on
// PATH — the image always ships drbdsetup; a local-only node lacks the
// module and answers exit 20 to everything.
func TestKernelAvailable(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if !d.KernelAvailable(t.Context()) {
		t.Fatal("kernel answering must read as available")
	}
	fe.calledWith(t, "modprobe drbd") // proactive load through /lib/modules

	fe = &fakeExec{errOn: map[string]error{
		"drbdsetup status": errors.New("exit status 20: Failed to modprobe drbd"),
	}}
	d = &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if d.KernelAvailable(t.Context()) {
		t.Fatal("module-less node must read as unavailable")
	}
}

// DiscardGranularity parses lsblk bytes output and clamps to DRBD's sane
// range; 0 (no discard support) and garbage both mean "render nothing".
func TestDiscardGranularity(t *testing.T) {
	cases := map[string]struct {
		out  string
		want int64
		err  bool
	}{
		"typical thin chunk": {out: "65536\n", want: 65536},
		"clamped up":         {out: "512\n", want: 4096},
		"clamped down":       {out: "4194304\n", want: 1 << 20},
		"no discard support": {out: "0\n", want: 0},
		"unparseable output": {out: "DISC-GRAN\n", err: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fe := &fakeExec{responses: map[string]string{"lsblk": tc.out}}
			d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
			got, err := d.DiscardGranularity(t.Context(), "/dev/vg-miroir/pvc-1")
			if tc.err != (err != nil) {
				t.Fatalf("err = %v, want err=%v", err, tc.err)
			}
			if got != tc.want {
				t.Fatalf("granularity = %d, want %d", got, tc.want)
			}
		})
	}
}

// A latched-failed leg (SkipDiskAttach) renders adjust --skip-disk and
// leaves the backing disk untouched: no create-md, no bare adjust that
// would re-attach the failing disk and re-trigger the I/O error (#101).
func TestApplySkipDiskAttachLeavesDiskDetached(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeKharkiv)
	r.SkipDiskAttach = true

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm adjust --skip-disk pvc-1")
	// The failing disk is never re-attached or re-created.
	fe.notCalledWith(t, "adjust pvc-1")
	fe.notCalledWith(t, "create-md")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); !os.IsNotExist(err) {
		t.Fatal("skip-disk leg must not create metadata")
	}
}

// A backing disk replaced under a surviving .md-created marker makes the
// first adjust fail "no valid meta-data"; Apply drops the stale marker,
// recreates metadata (SkipSeed → full SyncTarget), and retries adjust.
func TestApplyReseedsOnMissingMetadata(t *testing.T) {
	dir := t.TempDir()
	// Pre-existing marker from the pre-replacement life of the volume.
	if err := os.WriteFile(filepath.Join(dir, "pvc-1.md-created"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeExec{errOnce: map[string]error{
		"adjust pvc-1": errors.New("drbdadm: no valid meta-data found"),
	}}
	d := &Driver{StateDir: dir, Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeKharkiv)
	r.SkipSeed = true // the recreated leg must full-sync, never re-seed day0

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "set-gi")
	// adjust attempted twice: the failing first, the succeeding retry.
	var adjusts int
	for _, c := range fe.calls {
		if strings.Contains(c, "adjust pvc-1") {
			adjusts++
		}
	}
	if adjusts != 2 {
		t.Fatalf("want 2 adjust attempts (fail + retry), got %d: %v", adjusts, fe.calls)
	}
}

func TestApplyIdempotent(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeKharkiv)

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	before := len(fe.calls)
	if err := d.Apply(t.Context(), r); err != nil {
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
	r := testResource(nodeParis)

	if err := d.Apply(t.Context(), r); err == nil {
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
	fe.responses = map[string]string{cmdDumpMD: mockCurrentUUID}
	fe.calls = nil
	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "set-gi --node-id 1 "+Day0GI(volPvc1))
	fe.notCalledWith(t, "set-gi --node-id 31")
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
			cmdDumpMD: "# Output might be stale, since minor 1000 is attached\ncurrent-uuid 0x0000000000000004;",
		}},
		"configured refusal": {errOn: map[string]error{
			cmdDumpMD: errors.New("Device 'pvc-1' is configured!"),
		}},
	} {
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
		if err := d.Apply(t.Context(), testResource(nodeKharkiv)); err != nil {
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
			cmdDumpMD: errors.New(`Found meta data is "unclean", please apply-al first`),
		},
		responses: map[string]string{cmdDumpMD: mockCurrentUUID},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeKharkiv)); err != nil {
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
		cmdDumpMD: errors.New("open(/dev/vg-miroir/pvc-1) failed: Device or resource busy\n" +
			"Exclusive open failed. Do it anyways?\nOperation canceled."),
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeKharkiv)); err == nil {
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

	if err := d.Apply(t.Context(), testResource(nodeKharkiv)); err != nil {
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
		cmdDumpMD: "current-uuid 0xDEADBEEF00000001;\nbitmap-uuid 0x0000000000000000;",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeKharkiv)); err != nil {
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
		cmdDumpMD: "current-uuid 0x" + Day0GI(volPvc1) + ";\nbitmap-uuid 0x0000000000000000;",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeKharkiv)); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "set-gi --node-id 1")
	fe.notCalledWith(t, "set-gi --node-id 31")
	fe.notCalledWith(t, "create-md")
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

func TestDownRemovesState(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeKharkiv)

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if err := d.Down(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup down pvc-1")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.res")); !os.IsNotExist(err) {
		t.Fatal("res file must be removed")
	}

	// Down on never-configured resource is a no-op.
	before := len(fe.calls)
	if err := d.Down(t.Context(), "pvc-other"); err != nil {
		t.Fatal(err)
	}
	if len(fe.calls) != before {
		t.Fatal("down on unknown resource must not call drbdadm")
	}
}

func TestStatusParsing(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","suspended-user":true,
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"Connected","peer-role":"Primary"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.DiskState != "UpToDate" || s.SplitBrain || !s.PeerPrimary || !s.Suspended {
		t.Fatalf("unexpected status %+v", s)
	}
}

// Per-peer connection state keys on the DRBD node id, so consumers can
// ignore a diskless tie-breaker's link (snapshot barrier, removal gating).
func TestStatusPerPeerConnected(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connected"},
				{"peer-node-id":2,"connection-state":"Connecting"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.PeerConnected[1] || s.PeerConnected[2] {
		t.Fatalf("per-peer state wrong: %+v", s.PeerConnected)
	}
}

func TestDownSecondariesSkipsPrimary(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Primary"},
			{"name":"pvc-2","role":"Secondary"}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.DownSecondaries(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Primary legs are still open — skipped.
	fe.notCalledWith(t, "drbdsetup down pvc-1")
	fe.calledWith(t, "drbdsetup down pvc-2")
}

func TestDownSecondariesContinuesOnError(t *testing.T) {
	fe := &fakeExec{
		responses: map[string]string{
			cmdDrbdsetupStatus: `[
				{"name":"pvc-2","role":"Secondary"},
				{"name":"pvc-3","role":"Secondary"}]`,
		},
		errOn: map[string]error{"down pvc-2": errors.New("Device is held open by someone")},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	err := d.DownSecondaries(t.Context())
	if err == nil || !strings.Contains(err.Error(), "pvc-2") {
		t.Fatalf("want a joined error naming pvc-2, got %v", err)
	}
	// One stuck resource must not strand the rest of the sweep.
	fe.calledWith(t, "drbdsetup down pvc-3")
}

func TestIsResizeDuringResync(t *testing.T) {
	if !IsResizeDuringResync(errors.New("exit status 10: Resize not allowed during resync.")) {
		t.Fatal("must match DRBD's resync refusal")
	}
	if IsResizeDuringResync(errors.New("some other drbd failure")) {
		t.Fatal("must not match unrelated errors")
	}
	if IsResizeDuringResync(nil) {
		t.Fatal("nil is not a resync refusal")
	}
}

func TestUserSuspendedListsFrozenResources(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"` + volPvc1 + `","suspended":true,"suspended-user":true},
			{"name":"pvc-2","suspended":false,"suspended-user":false}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	got, err := d.UserSuspended(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != volPvc1 {
		t.Fatalf("want [pvc-1], got %v", got)
	}
}

func TestStatusSplitBrain(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"StandAlone"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.SplitBrain {
		t.Fatalf("StandAlone must surface as split-brain: %+v", s)
	}
}

// percent-in-sync is a peer-device field; Status surfaces the least-synced
// leg as ResyncPercent (100 when fully in sync). Verified against the
// drbdsetup source JSON shape.
func TestStatusResyncPercent(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"Inconsistent","quorum":true}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected",
				"peer_devices":[{"replication-state":"SyncTarget","percent-in-sync":42.5,"out-of-sync":2048}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Resyncing || s.ResyncPercent != 42.5 {
		t.Fatalf("want Resyncing with ResyncPercent 42.5, got %+v", s)
	}
	if !s.Quorum || s.OutOfSyncKiB != 2048 {
		t.Fatalf("want Quorum with OutOfSyncKiB 2048, got %+v", s)
	}
}

// The device quorum flag surfaces a freeze-policy volume whose partition
// lost quorum (IO suspending); out-of-sync tracks the worst peer.
func TestStatusQuorumLost(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate","quorum":false}],
			"connections":[
				{"peer-node-id":1,"connection-state":"Connecting",
					"peer_devices":[{"replication-state":"Off","out-of-sync":4096}]},
				{"peer-node-id":2,"connection-state":"Connecting",
					"peer_devices":[{"replication-state":"Off","out-of-sync":1024}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Quorum {
		t.Fatalf("quorum lost must surface as Quorum=false: %+v", s)
	}
	if s.OutOfSyncKiB != 4096 {
		t.Fatalf("OutOfSyncKiB must track the worst peer (4096), got %d", s.OutOfSyncKiB)
	}
	if s.Resyncing {
		t.Fatalf("Off replication must not read as resync: %+v", s)
	}
}

// A fully in-sync volume reports ResyncPercent 100, and connection-level
// replication-state (the pre-fix wrong nesting) is correctly ignored.
func TestStatusResyncPercentDefaultsFull(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected","replication-state":"SyncTarget",
				"peer_devices":[{"replication-state":"Established","percent-in-sync":100}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	s, err := d.Status(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Resyncing {
		t.Fatalf("connection-level replication-state must be ignored (peer-device is Established): %+v", s)
	}
	if s.ResyncPercent != 100 {
		t.Fatalf("in-sync volume must report 100, got %v", s.ResyncPercent)
	}
}

func TestStatusResyncing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		repl  string
		wantR bool
	}{
		{"established", "Established", false},
		{"absent", "", false},
		{"sync-target", "SyncTarget", true},
		{"sync-source", "SyncSource", true},
		{"paused", "PausedSyncT", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// replication-state is a peer-device field, nested under the
			// connection (verified against drbdsetup source).
			peerDevs := ""
			if tc.repl != "" {
				peerDevs = `,"peer_devices":[{"replication-state":"` + tc.repl + `"}]`
			}
			fe := &fakeExec{responses: map[string]string{
				cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `",
					"devices":[{"disk-state":"UpToDate"}],
					"connections":[{"connection-state":"Connected"` + peerDevs + `}]}]`,
			}}
			d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

			s, err := d.Status(t.Context(), volPvc1)
			if err != nil {
				t.Fatal(err)
			}
			if s.Resyncing != tc.wantR {
				t.Fatalf("replication-state %q: Resyncing = %v, want %v", tc.repl, s.Resyncing, tc.wantR)
			}
			if !s.PeerConnected[0] {
				t.Fatalf("a connected peer must stay connected: %+v", s)
			}
		})
	}
}
