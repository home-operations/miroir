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
		"after-sb-0pri discard-zero-changes;",
		"after-sb-1pri consensus;",
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
}

func (f *fakeExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
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
	fe.calledWith(t, "drbdadm down pvc-1")
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
		"drbdsetup status": `[{"name":"pvc-1",
			"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	s, err := d.Status(context.Background(), "pvc-1")
	if err != nil {
		t.Fatal(err)
	}
	if s.DiskState != "UpToDate" || !s.Connected || s.SplitBrain {
		t.Fatalf("unexpected status %+v", s)
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
