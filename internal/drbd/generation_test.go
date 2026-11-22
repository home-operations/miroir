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
	"strings"
	"testing"
)

func TestDay0GIDeterministicAndEven(t *testing.T) {
	a, b := Day0GI(volPvc1), Day0GI(volPvc1)
	if a != b || len(a) != 16 {
		t.Fatalf("unstable or malformed day0 GI: %q %q", a, b)
	}
	if Day0GI(volPvc2) == a {
		t.Fatal("different volumes must get different GIs")
	}
	last := a[len(a)-1]
	if !strings.ContainsRune("02468ACE", rune(last)) {
		t.Fatalf("day0 GI must be even (primary-writes bit clear), got %q", a)
	}
}

func TestInitialUUID(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := d.InitialUUID(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm new-current-uuid --clear-bitmap pvc-1/0")
}

// The seed bootstrap is force-promote then demote — never
// new-current-uuid, which a connected resource refuses ("Need to be
// StandAlone") and which leaves the disk Inconsistent even when minted,
// and never --clear-bitmap, which would declare blank joiners identical
// to data they do not hold.
func TestForceFullSyncSourcePromotesAndDemotes(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.ForceFullSyncSource(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm primary --force pvc-1")
	fe.calledWith(t, "drbdadm secondary pvc-1")
	fe.notCalledWith(t, "new-current-uuid")
}

// A failed promote must not be followed by the demote; the error is the
// caller's retry signal.
func TestForceFullSyncSourceSkipsDemoteOnPromoteFailure(t *testing.T) {
	fe := &fakeExec{errOn: map[string]error{"primary --force": errors.New("boom")}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.ForceFullSyncSource(t.Context(), volPvc1); err == nil {
		t.Fatal("a failed promote must surface")
	}
	fe.notCalledWith(t, "drbdadm secondary")
}

// The seed winner is the lowest diskful node id: a diskless tie-breaker
// holding the lowest id must never win, or no diskful node would seed
// UpToDate and the first handshake would deadlock all-Inconsistent.
func TestWinnerSkipsDiskless(t *testing.T) {
	r := Resource{
		LocalNode: nodeA,
		Peers: []Peer{
			{Node: nodeC, NodeID: 0, Diskless: true},
			{Node: nodeA, NodeID: 1},
			{Node: nodeB, NodeID: 2},
		},
	}
	if !IsWinner(r) {
		t.Fatal("node-a (lowest diskful id) must win, not the diskless tie-breaker")
	}
	r.LocalNode = nodeC
	if IsWinner(r) {
		t.Fatal("a diskless tie-breaker must never be the winner")
	}
}
