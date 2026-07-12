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
	"strings"
	"testing"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	nodeKharkiv = "kharkiv"
	nodeParis   = "paris"
	nodeOslo    = "oslo"
)

func tieBreakerResource(localNode string) Resource {
	return Resource{
		Name:      "pvc-1",
		Minor:     1000,
		Port:      7000,
		Quorum:    miroirv1alpha1.QuorumFreeze,
		LocalNode: localNode,
		LocalDisk: "/dev/vg-miroir/pvc-1",
		Peers: []Peer{
			{Node: nodeKharkiv, NodeID: 0, Address: addrKharkiv},
			{Node: nodeParis, NodeID: 1, Address: addrParis},
			{Node: nodeOslo, NodeID: 2, Address: "192.168.1.43", Diskless: true},
		},
	}
}

// rs-discard-granularity renders only in the LOCAL leg's volume disk{}
// (each node renders its own .res, so per-leg granularity), and only when
// probed non-zero — the resource-level option overrides the common{} knob.
func TestRenderDiscardGranularityLocalOnly(t *testing.T) {
	r := tieBreakerResource(nodeKharkiv)
	r.DiscardGranularityBytes = 65536
	out := Render(r)
	if !strings.Contains(out, "rs-discard-granularity 65536;") {
		t.Fatalf("local leg must render the granularity:\n%s", out)
	}
	// Exactly one render: the local volume block, not the peers'.
	if strings.Count(out, "rs-discard-granularity") != 1 {
		t.Fatalf("granularity must render once (local leg only):\n%s", out)
	}

	r.DiscardGranularityBytes = 0
	if out := Render(r); strings.Contains(out, "rs-discard-granularity") {
		t.Fatalf("zero granularity must render nothing:\n%s", out)
	}
}

// A diskless tie-breaker peer renders "disk none" with no meta-disk; the
// diskful peers keep the placeholder/local-disk + internal metadata.
func TestRenderDisklessTieBreaker(t *testing.T) {
	out := Render(tieBreakerResource(nodeKharkiv))

	oslo := out[strings.Index(out, "on \""+nodeOslo+"\""):]
	oslo = oslo[:strings.Index(oslo, "}\n    }")]
	if !strings.Contains(oslo, "disk none;") {
		t.Fatalf("tie-breaker peer must render disk none:\n%s", oslo)
	}
	if strings.Contains(oslo, "meta-disk") {
		t.Fatalf("tie-breaker peer must not render a meta-disk:\n%s", oslo)
	}
	if !strings.Contains(out, `disk "/dev/vg-miroir/pvc-1";`) {
		t.Fatal("local diskful peer must render the real backing path")
	}
	if !strings.Contains(out, `disk "`+peerDiskPlaceholder+`";`) {
		t.Fatal("remote diskful peer must render the placeholder")
	}
	if !strings.Contains(out, "hosts \""+nodeKharkiv+"\" \""+nodeParis+"\" \""+nodeOslo+"\";") {
		t.Fatal("the tie-breaker must be in the connection mesh")
	}
}

// The freeze policy renders majority quorum without
// on-suspended-primary-outdated: that option only acts under
// on-no-quorum suspend-io, and keying off the suspended state would
// entangle it with the snapshot barrier's suspend-io.
func TestRenderFreezeQuorumOptions(t *testing.T) {
	out := Render(tieBreakerResource(nodeKharkiv))
	if !strings.Contains(out, "quorum majority;") || !strings.Contains(out, "on-no-quorum io-error;") {
		t.Fatal("freeze must render quorum majority + on-no-quorum io-error")
	}
	if strings.Contains(out, "on-suspended-primary-outdated") {
		t.Fatal("on-suspended-primary-outdated is inert with io-error and must not render")
	}
}

// The seed winner is the lowest diskful node id: a diskless tie-breaker
// holding the lowest id must never win, or no diskful node would seed
// UpToDate and the first handshake would deadlock all-Inconsistent.
func TestWinnerSkipsDiskless(t *testing.T) {
	r := Resource{
		LocalNode: nodeKharkiv,
		Peers: []Peer{
			{Node: nodeOslo, NodeID: 0, Diskless: true},
			{Node: nodeKharkiv, NodeID: 1},
			{Node: nodeParis, NodeID: 2},
		},
	}
	if !IsWinner(r) {
		t.Fatal("kharkiv (lowest diskful id) must win, not the diskless tie-breaker")
	}
	r.LocalNode = nodeOslo
	if IsWinner(r) {
		t.Fatal("a diskless tie-breaker must never be the winner")
	}
}

func TestRenderIPv6Address(t *testing.T) {
	out := Render(Resource{
		Name:      "pvc-1",
		Minor:     1000,
		Port:      7000,
		LocalNode: nodeKharkiv,
		LocalDisk: "/dev/mapper/x",
		Peers: []Peer{
			{Node: nodeKharkiv, NodeID: 0, Address: "fd00::41"},
			{Node: nodeOslo, NodeID: 1, Address: "192.168.1.43"},
		},
	})
	if !strings.Contains(out, "address ipv6 [fd00::41]:7000;") {
		t.Fatalf("IPv6 peer must render family + brackets:\n%s", out)
	}
	if !strings.Contains(out, "address ipv4 192.168.1.43:7000;") {
		t.Fatalf("IPv4 peer must stay ipv4:\n%s", out)
	}
}
