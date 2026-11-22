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
	"testing"
)

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

// KernelVersion pulls DRBD_KERNEL_VERSION out of drbdadm --version; the
// other lines (utils version, api codes) must not be mistaken for it.
func TestKernelVersion(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdAdmVersion: "DRBDADM_BUILDTAG=GIT-hash\n" +
			"DRBDADM_API_VERSION=2\n" +
			"DRBD_KERNEL_VERSION_CODE=0x090302\n" +
			"DRBD_KERNEL_VERSION=9.3.2\n" +
			"DRBDADM_VERSION_CODE=0x092203\n" +
			"DRBDADM_VERSION=9.34.3\n",
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	v, err := d.KernelVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if v != "9.3.2" {
		t.Fatalf("version = %q, want 9.3.2", v)
	}
	u, err := d.UtilsVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if u != "9.34.3" {
		t.Fatalf("utils version = %q, want 9.34.3 (must not match DRBDADM_VERSION_CODE)", u)
	}

	fe = &fakeExec{responses: map[string]string{cmdAdmVersion: "DRBDADM_VERSION=9.34.3\n"}}
	d = &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if _, err := d.KernelVersion(t.Context()); err == nil {
		t.Fatal("missing DRBD_KERNEL_VERSION line must error, not match DRBDADM_VERSION")
	}

	fe = &fakeExec{errOn: map[string]error{cmdAdmVersion: errors.New("exit status 1")}}
	d = &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if _, err := d.KernelVersion(t.Context()); err == nil {
		t.Fatal("exec failure must surface as an error")
	}
}

// The floor compare is semver-based — "9.10" is newer than "9.3", a
// short "9.3" reads as "9.3.0" — and anything unparseable reads as
// below the floor.
func TestBelowKernelFloor(t *testing.T) {
	cases := map[string]bool{
		"9.3.1":      false, // the floor itself
		"9.3.2":      false,
		"9.4.0":      false,
		"9.10.0":     false, // numeric, not lexicographic
		"10.0.0":     false,
		"9.3.1.1":    false, // 4-component vendor build: compared on x.y.z
		"v9.3.2":     false, // leading v tolerated
		"9.3.2-1":    false, // suffixed build of a version above the floor
		"9.3.2+ptf1": false, // LINBIT PTF-style build metadata
		"9.3.0":      true,
		"9.2.18":     true,
		"9.2.18.4":   true, // 4-component below the floor stays below
		"8.4.11":     true,
		"9.3":        true, // shorter than the floor
		"":           true,
		"unknown":    true,
	}
	for version, want := range cases {
		if got := BelowKernelFloor(version); got != want {
			t.Errorf("BelowKernelFloor(%q) = %v, want %v", version, got, want)
		}
	}
}
