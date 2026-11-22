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
	nodeA = "node-a"
	nodeB = "node-b"
	nodeC = "node-c"
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
			{Node: nodeA, NodeID: 0, Address: addrA},
			{Node: nodeB, NodeID: 1, Address: addrB},
			{Node: nodeC, NodeID: 2, Address: addrC, Diskless: true},
		},
	}
}

// rs-discard-granularity renders only in the LOCAL leg's volume disk{}
// (each node renders its own .res, so per-leg granularity), and only when
// probed non-zero — the resource-level option overrides the common{} knob.
func TestRenderDiscardGranularityLocalOnly(t *testing.T) {
	r := tieBreakerResource(nodeA)
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
	out := Render(tieBreakerResource(nodeA))

	tieBreaker := out[strings.Index(out, "on \""+nodeC+"\""):]
	tieBreaker = tieBreaker[:strings.Index(tieBreaker, "}\n    }")]
	if !strings.Contains(tieBreaker, "disk none;") {
		t.Fatalf("tie-breaker peer must render disk none:\n%s", tieBreaker)
	}
	if strings.Contains(tieBreaker, "meta-disk") {
		t.Fatalf("tie-breaker peer must not render a meta-disk:\n%s", tieBreaker)
	}
	if !strings.Contains(out, `disk "/dev/vg-miroir/pvc-1";`) {
		t.Fatal("local diskful peer must render the real backing path")
	}
	if !strings.Contains(out, `disk "`+peerDiskPlaceholder+`";`) {
		t.Fatal("remote diskful peer must render the placeholder")
	}
	if !strings.Contains(out, "hosts \""+nodeA+"\" \""+nodeB+"\" \""+nodeC+"\";") {
		t.Fatal("the tie-breaker must be in the connection mesh")
	}
}

// The freeze policy renders majority quorum without
// on-suspended-primary-outdated: that option only acts under
// on-no-quorum suspend-io, and keying off the suspended state would
// entangle it with the snapshot barrier's suspend-io.
func TestRenderFreezeQuorumOptions(t *testing.T) {
	out := Render(tieBreakerResource(nodeA))
	if !strings.Contains(out, "quorum majority;") || !strings.Contains(out, "on-no-quorum io-error;") {
		t.Fatal("freeze must render quorum majority + on-no-quorum io-error")
	}
	if strings.Contains(out, "on-suspended-primary-outdated") {
		t.Fatal("on-suspended-primary-outdated is inert with io-error and must not render")
	}
}

func TestRenderIPv6Address(t *testing.T) {
	out := Render(Resource{
		Name:      "pvc-1",
		Minor:     1000,
		Port:      7000,
		LocalNode: nodeA,
		LocalDisk: "/dev/mapper/x",
		Peers: []Peer{
			{Node: nodeA, NodeID: 0, Address: "fd00::41"},
			{Node: nodeC, NodeID: 1, Address: addrC},
		},
	})
	if !strings.Contains(out, "address ipv6 [fd00::41]:7000;") {
		t.Fatalf("IPv6 peer must render family + brackets:\n%s", out)
	}
	if !strings.Contains(out, "address ipv4 "+addrC+":7000;") {
		t.Fatalf("IPv4 peer must stay ipv4:\n%s", out)
	}
}

func TestRenderDeterministicAndLocalDisk(t *testing.T) {
	r := testResource(nodeA)
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
	r := testResource(nodeA)
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

func TestRenderFreezeQuorum(t *testing.T) {
	r := testResource(nodeA)
	r.Quorum = miroirv1alpha1.QuorumFreeze
	out := Render(r)
	if !strings.Contains(out, "quorum majority;") || !strings.Contains(out, "on-no-quorum io-error;") {
		t.Fatalf("freeze policy not rendered:\n%s", out)
	}
}

// A client leg renders "tiebreaker no" (it must not shift quorum math);
// a placed tie-breaker replica keeps the default — voting is its purpose.
func TestRenderClientLegNoTiebreaker(t *testing.T) {
	r := testResource(nodeA)
	r.Peers = append(r.Peers,
		Peer{Node: "tiebreak-1", NodeID: 2, Address: addrC, Diskless: true},
		Peer{Node: nodeWorker1, NodeID: 3, Address: "192.168.1.44", Diskless: true, Client: true},
	)
	out := Render(r)
	if n := strings.Count(out, "tiebreaker no;"); n != 1 {
		t.Fatalf("tiebreaker no must render exactly once (the client leg), got %d:\n%s", n, out)
	}
	client := out[strings.Index(out, `on "worker-1"`):]
	if !strings.Contains(client[:strings.Index(client, "}")+1], "tiebreaker no;") {
		t.Fatalf("tiebreaker no must sit in the client's volume block:\n%s", out)
	}
}

// A client leg rendered on its own node advertises the diskful legs'
// discard granularity; rendered anywhere else (or unset) it must not.
func TestRenderClientDiscardGranularity(t *testing.T) {
	r := testResource(nodeA)
	r.Peers = append(r.Peers,
		Peer{Node: nodeWorker1, NodeID: 2, Address: addrC, Diskless: true, Client: true},
	)
	r.ClientDiscardGranularityBytes = 65536

	// Rendered on a replica node: the option belongs to the client's
	// device, not this one — nothing renders even with the value set.
	if out := Render(r); strings.Contains(out, "discard-granularity") {
		t.Fatalf("non-client local must not render discard-granularity:\n%s", out)
	}

	r.LocalNode = nodeWorker1
	r.LocalDiskless = true
	r.LocalDisk = ""
	// The resync knob must stay diskful-only even when its value is set.
	r.DiscardGranularityBytes = 4096
	out := Render(r)
	if !strings.Contains(out, "discard-granularity 65536;") {
		t.Fatalf("client local must advertise the peers' granularity:\n%s", out)
	}
	if strings.Contains(out, "rs-discard-granularity") {
		t.Fatalf("diskless local must not render the resync knob:\n%s", out)
	}
	r.DiscardGranularityBytes = 0

	r.ClientDiscardGranularityBytes = 0
	if out := Render(r); strings.Contains(out, "discard-granularity") {
		t.Fatalf("zero must render nothing (keep the kernel heuristic):\n%s", out)
	}
}

func TestRenderNoAutoSplitBrainResolution(t *testing.T) {
	out := Render(testResource(nodeA))
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
