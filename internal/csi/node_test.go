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

package csi

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// devDrbd1000 is the staged DRBD device path shared by the fixtures.
const devDrbd1000 = "/dev/drbd1000"

type fakeDRBDStatus struct {
	st  drbd.Status
	err error
}

func (f fakeDRBDStatus) Status(context.Context, string) (drbd.Status, error) {
	return f.st, f.err
}

// stagedVolume is a single-replica-on-kharkiv replicated volume whose agent
// has already created the local DRBD device.
func stagedVolume() *miroirv1alpha1.MiroirVolume {
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeKharkiv, Address: addrKharkiv}},
		},
	}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DeviceCreated: true, DevicePath: devDrbd1000},
	}
	return v
}

func newNode(t *testing.T, vol *miroirv1alpha1.MiroirVolume, d DRBDStatus) *Node {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(vol).Build()
	return &Node{Client: c, NodeName: nodeKharkiv, DRBD: d}
}

// A split-brain leg must never be staged: mkfs/mount on divergent data
// would finalize the loser's copy. The kernel's live view decides, not the
// lagging CRD status.
func TestDevicePathRefusesSplitBrain(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{
		st: drbd.Status{DiskState: drbd.DiskUpToDate, SplitBrain: true},
	})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("split-brain must be FailedPrecondition, got %v", err)
	}
}

// A leg that is not UpToDate is still resyncing or diverged; staging it
// could mount stale data or race the initial handshake.
func TestDevicePathRefusesNotUpToDate(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{
		st: drbd.Status{DiskState: "Inconsistent"},
	})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("a non-UpToDate leg must be Unavailable, got %v", err)
	}
}

// The gate reads the kernel, not the CRD: an unreadable DRBD state must not
// fall through to staging.
func TestDevicePathRefusesUnreadableDRBD(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{err: context.DeadlineExceeded})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("unreadable DRBD state must be Unavailable, got %v", err)
	}
}

func TestDevicePathHealthyReturnsDevice(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{
		st: drbd.Status{DiskState: drbd.DiskUpToDate},
	})
	dev, _, err := n.devicePath(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if dev != devDrbd1000 {
		t.Fatalf("dev = %q, want /dev/drbd1000", dev)
	}
}

// A diskless tie-breaker node must never stage the volume: it holds no
// data leg, only a quorum vote.
func TestDevicePathRefusesDisklessNode(t *testing.T) {
	v := stagedVolume()
	// paris + oslo hold the data; kharkiv (this node) is the tie-breaker.
	v.Spec.Replicas = []miroirv1alpha1.Replica{
		{Node: nodeParis, NodeID: 0, Address: addrParis},
		{Node: nodeOslo, NodeID: 1, Address: "192.168.1.43"},
		{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv, Diskless: true},
	}
	n := newNode(t, v, fakeDRBDStatus{
		st: drbd.Status{DiskState: "Diskless"},
	})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a diskless tie-breaker node must be FailedPrecondition, got %v", err)
	}
}

// twoLegVolume extends stagedVolume with a second diskful replica on paris
// (node id 1) whose slot records a split-brain — the recovery-in-progress
// signal the staging hold keys on.
func twoLegVolume() *miroirv1alpha1.MiroirVolume {
	v := stagedVolume()
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeParis, NodeID: 1, Address: addrParis})
	v.Status.PerNode[nodeKharkiv] = miroirv1alpha1.ReplicaStatus{
		DeviceCreated: true, DevicePath: devDrbd1000, SplitBrain: true,
	}
	return v
}

// Mid-recovery a never-activated volume can read healthy locally (survivor
// and tie-breaker reconnected, quorum back) while the losing leg is still
// divergent and disconnected. Staging then would latch Activated and close
// the auto-recovery that heals the loser — hold it.
func TestDevicePathHoldsNeverActivatedRecoveringSplitBrain(t *testing.T) {
	n := newNode(t, twoLegVolume(), fakeDRBDStatus{
		st: drbd.Status{DiskState: drbd.DiskUpToDate}, // paris link down: no PeerConnected entry
	})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("recovering split-brain must hold staging as Unavailable, got %v", err)
	}
}

// A stale split-brain slot (e.g. left by a dead tie-breaker) must not hold
// staging when every diskful link is live — the kernel corroboration is
// what keeps the hold from wedging a healthy volume.
func TestDevicePathStaleSplitSlotIgnoredWhenPeersLive(t *testing.T) {
	n := newNode(t, twoLegVolume(), fakeDRBDStatus{
		st: drbd.Status{
			DiskState:     drbd.DiskUpToDate,
			PeerConnected: map[int32]bool{1: true},
		},
	})
	if _, _, err := n.devicePath(t.Context(), volPvc1); err != nil {
		t.Fatalf("connected volume must stage despite a stale slot: %v", err)
	}
}

// An activated volume is past auto-recovery: the hold must not apply at all,
// even with a split recorded and a link down.
func TestDevicePathActivatedIgnoresSplitSlot(t *testing.T) {
	v := twoLegVolume()
	v.Status.Activated = true
	n := newNode(t, v, fakeDRBDStatus{st: drbd.Status{DiskState: drbd.DiskUpToDate}})
	if _, _, err := n.devicePath(t.Context(), volPvc1); err != nil {
		t.Fatalf("activated volume must stage despite a split slot: %v", err)
	}
}

// A node holding no replica of the volume must be refused before any DRBD
// or device lookup.
func TestDevicePathRefusesForeignNode(t *testing.T) {
	v := stagedVolume()
	v.Spec.Replicas[0].Node = nodeParis
	n := newNode(t, v, fakeDRBDStatus{st: drbd.Status{DiskState: drbd.DiskUpToDate}})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a node without a replica must be FailedPrecondition, got %v", err)
	}
}
