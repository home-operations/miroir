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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
)

const (
	volPvc1            = "pvc-1"
	volPvc2            = "pvc-2"
	cmdDrbdsetupStatus = "drbdsetup status"
	cmdAdmVersion      = "drbdadm --version"
	cmdDumpMD          = "dump-md"
	mockCurrentUUID    = "current-uuid 0xDEADBEEF00000001;"
	addrA              = "192.168.1.41"
	addrB              = "192.168.1.42"
	addrC              = "192.168.1.43"
	nodeWorker1        = "worker-1"
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
			{Node: nodeA, NodeID: 0, Address: addrA},
			{Node: nodeB, NodeID: 1, Address: addrB},
		},
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
	if strings.Contains(line, cmdDumpMD) {
		// Fresh backing device by default.
		return "", errors.New("Exclusive open failed. no valid meta data")
	}
	if strings.HasPrefix(line, cmdDrbdsetupStatus) {
		// Nothing in the kernel by default: --json prints an empty list.
		return "[]", nil
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
	r := testResource(nodeA)

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	// Metadata stays at UUID_JUST_CREATED: the birth generation is minted
	// once by the winner over live connections (InitialUUID), never
	// manufactured per node.
	fe.notCalledWith(t, "new-current-uuid")
	fe.calledWith(t, "drbdadm adjust pvc-1")
	fe.notCalledWith(t, "forget-peer")

	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.res")); err != nil {
		t.Fatal("res file not written")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written")
	}
}

// A bitmap granularity on the resource reaches create-md as
// --bitmap-block-size; the default (0) must add nothing — TestApplyFresh-
// Resource above pins the exact default argv.
func TestApplyBitmapGranularity(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeA)
	r.BitmapGranularityBytes = 65536

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 --bitmap-block-size 65536 pvc-1/0")
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
	r := testResource(nodeA)
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
// recreates metadata (just-created → full SyncTarget), and retries adjust.
func TestApplyRecreatesOnMissingMetadata(t *testing.T) {
	dir := t.TempDir()
	// Pre-existing marker from the pre-replacement life of the volume.
	if err := os.WriteFile(filepath.Join(dir, "pvc-1.md-created"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeExec{errOnce: map[string]error{
		"adjust pvc-1": errors.New("drbdadm: no valid meta-data found"),
	}}
	d := &Driver{StateDir: dir, Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "new-current-uuid")
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

// An auto-diskful conversion is already up in the kernel as a diskless
// leg while the backing it is gaining is still blank. The resource's
// presence must not be read as live metadata: create-md has to run, or
// adjust attaches a device with no metadata on it and the leg never
// materializes — Apply's missing-metadata recovery re-probes into the
// same adoption, so it error-loops "No valid meta data found" forever.
func TestApplySeedsMetadataForDisklessLegGainingADisk(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"` + DiskDiskless + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"` + DiskUpToDate + `"}]}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.notCalledWith(t, "new-current-uuid")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); !os.IsNotExist(err) {
		t.Fatal("a blank backing must not be recorded as adopted metadata")
	}
	// The marker pair is the whole point of seeding under a live resource:
	// .md-created must land and the .md-seeding sentinel must be gone, or
	// the next restart re-seeds a leg that has since full-synced real data.
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("a completed seed must be marked created")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-seeding")); !os.IsNotExist(err) {
		t.Fatal("a completed seed must clear the seeding sentinel")
	}
}

// The converse of the seeding case: the same up-but-Diskless kernel state,
// but the backing already carries live metadata. dump-md — not the kernel
// probe — is the authority that keeps create-md --force off real data, so
// this is the one path where that gate is load-bearing.
func TestApplyAdoptsLiveMetadataOnDisklessLegGainingADisk(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"` + DiskDiskless + `"}],"connections":[]}]`,
		// Neither day0 nor UUID_JUST_CREATED: a generation a Primary
		// minted, i.e. real data.
		cmdDumpMD: "current-uuid 0xB0FFEEB0FFEEB0FF;\nbitmap-uuid 0x0000000000000000;",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "create-md")
	fe.notCalledWith(t, "new-current-uuid")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); err != nil {
		t.Fatal("live metadata under a diskless leg must be adopted, not re-seeded")
	}
}

func TestApplyIdempotent(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeA)

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

// A peer adjust drops (del-peer) keeps its MDF_NODE_EXISTS metadata slot,
// which DRBD's quorum code counts as a missing diskless tiebreaker (issue
// #478). Apply must forget-peer every node-id the kernel had before the
// config change and the new config no longer lists — after adjust, since
// forget-peer refuses a still-configured peer — and only those.
func TestApplyForgetsRemovedPeers(t *testing.T) {
	status := `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + DiskUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"},
		{"peer-node-id":3,"connection-state":"Connected"}]}]`

	t.Run("forgets the dropped peer only", func(t *testing.T) {
		fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: status}}
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

		if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
			t.Fatal(err)
		}
		fe.calledWith(t, "drbdsetup forget-peer pvc-1 3")
		fe.notCalledWith(t, "forget-peer pvc-1 1")
		adjust := slices.Index(fe.calls, "drbdadm adjust pvc-1")
		forget := slices.Index(fe.calls, "drbdsetup forget-peer pvc-1 3")
		if adjust < 0 || forget < adjust {
			t.Fatalf("forget-peer must follow adjust: %v", fe.calls)
		}
	})

	t.Run("nothing to forget when the peer set is unchanged", func(t *testing.T) {
		fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: status}}
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
		r := testResource(nodeA)
		r.Peers = append(r.Peers, Peer{Node: nodeWorker1, NodeID: 3, Address: addrC, Diskless: true, Client: true})

		if err := d.Apply(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		fe.notCalledWith(t, "forget-peer")
	})

	t.Run("a failed forget fails the apply and is retried", func(t *testing.T) {
		fe := &fakeExec{
			responses: map[string]string{cmdDrbdsetupStatus: status},
			errOnce:   map[string]error{"forget-peer": errors.New("exit status 1")},
		}
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

		if err := d.Apply(t.Context(), testResource(nodeA)); err == nil {
			t.Fatal("a failed forget-peer must surface so the next pass retries it")
		}
		if _, err := os.Stat(d.path(volPvc1 + pendingForgetSuffix)); err != nil {
			t.Fatalf("a failed forget must leave the pending marker: %v", err)
		}
		// adjust already dropped the peer: the kernel view no longer lists
		// it, so only the marker can carry it to this pass.
		fe.responses[cmdDrbdsetupStatus] = `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"` + DiskUpToDate + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
		fe.calls = nil
		if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
			t.Fatal(err)
		}
		fe.calledWith(t, "drbdsetup forget-peer pvc-1 3")
		if _, err := os.Stat(d.path(volPvc1 + pendingForgetSuffix)); !os.IsNotExist(err) {
			t.Fatal("a completed forget must clear the pending marker")
		}
	})

	t.Run("a pending peer back in the config is dropped without a forget", func(t *testing.T) {
		fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: status}}
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
		if err := d.writePendingForgets(volPvc1, []int32{3}); err != nil {
			t.Fatal(err)
		}
		r := testResource(nodeA)
		r.Peers = append(r.Peers, Peer{Node: nodeWorker1, NodeID: 3, Address: addrC, Diskless: true, Client: true})

		if err := d.Apply(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		fe.notCalledWith(t, "forget-peer")
		if _, err := os.Stat(d.path(volPvc1 + pendingForgetSuffix)); !os.IsNotExist(err) {
			t.Fatal("a re-configured peer must leave the pending marker")
		}
	})

	t.Run("a resource not in the kernel has nothing to forget", func(t *testing.T) {
		fe := &fakeExec{errOn: map[string]error{
			cmdDrbdsetupStatus: errors.New("exit status 10: pvc-1: No such resource"),
		}}
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

		if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
			t.Fatal(err)
		}
		fe.notCalledWith(t, "forget-peer")
	})

	t.Run("any other status failure fails the apply", func(t *testing.T) {
		fe := &fakeExec{errOn: map[string]error{
			cmdDrbdsetupStatus: errors.New("exit status 20: Failed to modprobe drbd"),
		}}
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

		if err := d.Apply(t.Context(), testResource(nodeA)); err == nil {
			t.Fatal("an unreadable kernel view must not let adjust drop peers unrecorded")
		}
		fe.notCalledWith(t, "drbdadm adjust")
	})
}

func TestApplyRetriesAfterCreateMDCrash(t *testing.T) {
	fe := &fakeExec{errOn: map[string]error{
		"create-md": errors.New("exit status 20: open failed"),
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeB)

	if err := d.Apply(t.Context(), r); err == nil {
		t.Fatal("expected create-md failure")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); !os.IsNotExist(err) {
		t.Fatal("marker must not be written after a failed create")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-seeding")); err != nil {
		t.Fatal("sentinel must survive the failed attempt")
	}

	// Retry: metadata landed on disk before the crash surfaced (still the
	// just-created UUID), and the sentinel proves it is our own attempt —
	// the retry completes without another create-md and lands the marker.
	fe.errOn = nil
	fe.responses = map[string]string{cmdDumpMD: "current-uuid 0x" + justCreatedUUID + ";"}
	fe.calls = nil
	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "create-md")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("marker not written after successful retry")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-seeding")); !os.IsNotExist(err) {
		t.Fatal("sentinel must be removed after success")
	}
}

func TestApplyAdoptsAttachedDevice(t *testing.T) {
	// Markers lost but the kernel holds the minor: a previous life
	// completed seeding and adjust — metadata is live, never touch it.
	// dump-md succeeds read-only on an attached minor with a stale-output
	// warning. There is deliberately no "Device is configured!" case:
	// drbdmeta gates that refusal on `minor_attached && modifies_md`
	// (drbd-utils user/shared/drbdmeta.c) and dump-md declares
	// modifies_md = 0, so the probe never sees it. A busy open surfaces as
	// EBUSY instead, which must error rather than adopt — see
	// TestApplySurfacesNonDRBDBusyDevice.
	for name, fe := range map[string]*fakeExec{
		"local disk attached": {responses: map[string]string{
			cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
				"devices":[{"disk-state":"` + DiskInconsistent + `"}],"connections":[]}]`,
		}},
		"stale warning": {responses: map[string]string{
			cmdDumpMD: "# Output might be stale, since minor 1000 is attached\ncurrent-uuid 0x0000000000000004;",
		}},
	} {
		d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
		if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		fe.notCalledWith(t, "create-md")
		fe.notCalledWith(t, "new-current-uuid")
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

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm apply-al pvc-1/0")
	fe.notCalledWith(t, "create-md")
	fe.notCalledWith(t, "new-current-uuid")
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

	if err := d.Apply(t.Context(), testResource(nodeA)); err == nil {
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

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "new-current-uuid")
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

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "create-md")
	fe.notCalledWith(t, "new-current-uuid")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); err != nil {
		t.Fatal("adoption must leave a breadcrumb")
	}
}

func TestApplyClaimsVirginMetadataWithoutMarkers(t *testing.T) {
	// Markers lost, device detached, GI still a day0 seed (older release)
	// with clean bitmaps: provably no data — claim it as our own (marker
	// lands) without recreating; a written volume would be adopted instead.
	fe := &fakeExec{responses: map[string]string{
		cmdDumpMD: "current-uuid 0x" + Day0GI(volPvc1) + ";\nbitmap-uuid 0x0000000000000000;",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Apply(t.Context(), testResource(nodeA)); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "create-md")
	fe.notCalledWith(t, "new-current-uuid")
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-created")); err != nil {
		t.Fatal("virgin metadata must be claimed with the marker")
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.md-adopted")); !os.IsNotExist(err) {
		t.Fatal("virgin metadata is ours, not an adoption")
	}
}

// gatedExec is a goroutine-safe exec whose first drbdsetup disconnect
// parks until released, standing in for a teardown the kernel is still
// draining: everything a restart does after that first disconnect is
// what must stay ordered ahead of a concurrent adjust.
type gatedExec struct {
	mu      sync.Mutex
	calls   []string
	gated   sync.Once
	parked  chan struct{} // closed once the disconnect is in flight
	release chan struct{} // the disconnect returns once closed
	status  string
}

func (g *gatedExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	g.mu.Lock()
	g.calls = append(g.calls, line)
	g.mu.Unlock()
	switch {
	case strings.HasPrefix(line, "drbdsetup disconnect "):
		g.gated.Do(func() {
			close(g.parked)
			<-g.release
		})
	case strings.HasPrefix(line, cmdDrbdsetupStatus):
		return g.status, nil
	case strings.Contains(line, cmdDumpMD):
		return "", errors.New("Exclusive open failed. no valid meta data")
	}
	return "", nil
}

func (g *gatedExec) snapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.calls)
}

func indexOf(calls []string, substr string) int {
	return slices.IndexFunc(calls, func(c string) bool { return strings.Contains(c, substr) })
}

// The runner gives up on a down at its deadline while the child stays
// stranded in the kernel, so the restart's lock is released with the
// teardown still outstanding. Apply must refuse to adjust (a connect
// would restart the receiver thread the down waits on) until the child
// drains, and resume on its own once it has.
func TestApplyRefusesAdjustWhileDownStranded(t *testing.T) {
	r := testResource(nodeA)
	fe := &fakeExec{}
	w := &fakeWedge{stuck: map[string]bool{"drbdsetup down " + r.Name: true}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod, Wedge: w}

	err := d.Apply(t.Context(), r)
	if !errors.Is(err, ErrDownStranded) {
		t.Fatalf("Apply = %v, want ErrDownStranded", err)
	}
	fe.notCalledWith(t, "adjust")

	w.stuck = nil
	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatalf("Apply after the child drained: %v", err)
	}
	fe.calledWith(t, "adjust "+r.Name)
}

// A disconnect killed at its deadline can likewise outlive its caller's
// lock (disconnectPeers defers with ErrBusy and the restart returns
// without a down). A connect into that half-torn connection is the same
// hazard, so Apply defers with the same ErrBusy until the child drains;
// the name match is exact, so pvc-10's stranded disconnect does not
// park pvc-1.
func TestApplyDefersAdjustWhileDisconnectStranded(t *testing.T) {
	r := testResource(nodeA)
	fe := &fakeExec{}
	w := &fakeWedge{stuck: map[string]bool{"drbdsetup disconnect " + r.Name + "0 1": true}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod, Wedge: w}

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatalf("Apply with another resource's disconnect stranded: %v", err)
	}
	fe.calledWith(t, "adjust "+r.Name)

	fe.calls = nil
	w.stuck["drbdsetup disconnect "+r.Name+" 1"] = true
	err := d.Apply(t.Context(), r)
	if !errors.Is(err, backend.ErrBusy) || errors.Is(err, ErrWedged) {
		t.Fatalf("Apply = %v, want ErrBusy and not a wedge", err)
	}
	fe.notCalledWith(t, "adjust")
}

// A caller whose context has expired never gets the lock, whether the slot
// is held (it stops waiting) or free (select may pick the ready slot, so
// the cancellation is re-checked and the slot handed back): a teardown
// nobody waits for must not start. The holder is unaffected either way.
func TestLockResourceHonoursContext(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name string
		held bool
	}{
		{"slot held", true},
		{"slot free", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &Driver{}
			if tc.held {
				unlock, err := d.lockResource(t.Context(), volPvc1)
				if err != nil {
					t.Fatal(err)
				}
				defer unlock()
			}
			for range 3 {
				if _, err := d.lockResource(cancelled, volPvc1); !errors.Is(err, context.Canceled) {
					t.Fatalf("lockResource with a cancelled context = %v, want context.Canceled", err)
				}
			}
			if tc.held {
				return
			}
			// The slot must have been handed back, not leaked.
			unlock, err := d.lockResource(t.Context(), volPvc1)
			if err != nil {
				t.Fatalf("slot leaked by a refused acquisition: %v", err)
			}
			unlock()
		})
	}
}

// The resource lock is per name: holding one resource's lock must not
// park an operation on another.
func TestLockResourceIsPerResource(t *testing.T) {
	d := &Driver{}
	unlock, err := d.lockResource(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	other, err := d.lockResource(ctx, volPvc2)
	if err != nil {
		t.Fatalf("locking pvc-2 parked behind pvc-1's lock: %v", err)
	}
	other()
}

// fakeWedge stands in for the node breaker: it reports exactly the command
// lines in stuck as stranded.
type fakeWedge struct{ stuck map[string]bool }

func (f *fakeWedge) StrandedCommand(line string) bool { return f.stuck[line] }

func (f *fakeWedge) Commands() []string {
	var out []string
	for line, stuck := range f.stuck {
		if stuck {
			out = append(out, line)
		}
	}
	return out
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

func TestVerify(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Verify(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm verify pvc-1")
}
