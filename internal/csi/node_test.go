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
		nodeKharkiv: {DeviceCreated: true, DevicePath: "/dev/drbd1000"},
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
		st: drbd.Status{DiskState: drbd.DiskUpToDate, Connected: true, SplitBrain: true},
	})
	if _, _, err := n.devicePath(context.Background(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("split-brain must be FailedPrecondition, got %v", err)
	}
}

// A leg that is not UpToDate is still resyncing or diverged; staging it
// could mount stale data or race the initial handshake.
func TestDevicePathRefusesNotUpToDate(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{
		st: drbd.Status{DiskState: "Inconsistent", Connected: true},
	})
	if _, _, err := n.devicePath(context.Background(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("a non-UpToDate leg must be Unavailable, got %v", err)
	}
}

// The gate reads the kernel, not the CRD: an unreadable DRBD state must not
// fall through to staging.
func TestDevicePathRefusesUnreadableDRBD(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{err: context.DeadlineExceeded})
	if _, _, err := n.devicePath(context.Background(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("unreadable DRBD state must be Unavailable, got %v", err)
	}
}

func TestDevicePathHealthyReturnsDevice(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{
		st: drbd.Status{DiskState: drbd.DiskUpToDate, Connected: true},
	})
	dev, _, err := n.devicePath(context.Background(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/dev/drbd1000" {
		t.Fatalf("dev = %q, want /dev/drbd1000", dev)
	}
}

// A diskless tie-breaker node must never stage the volume: it holds no
// data leg, only a quorum vote.
func TestDevicePathRefusesDisklessNode(t *testing.T) {
	v := stagedVolume()
	// paris + oslo hold the data; kharkiv (this node) is the tie-breaker.
	v.Spec.Replicas = []miroirv1alpha1.Replica{
		{Node: nodeParis, NodeID: 0, Address: "192.168.1.42"},
		{Node: nodeOslo, NodeID: 1, Address: "192.168.1.43"},
		{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv, Diskless: true},
	}
	n := newNode(t, v, fakeDRBDStatus{
		st: drbd.Status{DiskState: "Diskless", Connected: true},
	})
	if _, _, err := n.devicePath(context.Background(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a diskless tie-breaker node must be FailedPrecondition, got %v", err)
	}
}

// A node holding no replica of the volume must be refused before any DRBD
// or device lookup.
func TestDevicePathRefusesForeignNode(t *testing.T) {
	v := stagedVolume()
	v.Spec.Replicas[0].Node = nodeParis
	n := newNode(t, v, fakeDRBDStatus{st: drbd.Status{DiskState: drbd.DiskUpToDate}})
	if _, _, err := n.devicePath(context.Background(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a node without a replica must be FailedPrecondition, got %v", err)
	}
}
