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
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
	"github.com/home-operations/miroir/internal/stage"
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

func newNode(t *testing.T, vol *miroirv1alpha1.MiroirVolume, d stage.DRBDStatus) *Node {
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

// NodeGetVolumeStats attaches the volume's replication health so kubelet's
// volume-health metric reflects a degraded leg alongside the capacity stats.
func TestNodeGetVolumeStatsReportsCondition(t *testing.T) {
	v := stagedVolume()
	v.Status.Phase = miroirv1alpha1.VolumeDegraded
	n := newNode(t, v, fakeDRBDStatus{st: drbd.Status{DiskState: drbd.DiskUpToDate}})

	resp, err := n.NodeGetVolumeStats(t.Context(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   volPvc1,
		VolumePath: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetVolumeCondition().GetAbnormal() {
		t.Fatalf("degraded volume must report abnormal condition, got %+v", resp.GetVolumeCondition())
	}
	if len(resp.GetUsage()) == 0 {
		t.Fatal("expected capacity usage alongside the condition")
	}
}

// A stats call for a volume that has been deleted must still succeed — the
// condition is best-effort, capacity is the contract.
func TestNodeGetVolumeStatsMissingVolume(t *testing.T) {
	n := newNode(t, stagedVolume(), fakeDRBDStatus{})
	resp, err := n.NodeGetVolumeStats(t.Context(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "missing",
		VolumePath: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetVolumeCondition() != nil {
		t.Fatalf("missing volume should carry no condition, got %+v", resp.GetVolumeCondition())
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

// exportVolume is an RWX volume; address is set only once the gateway
// Service has a ClusterIP.
func exportVolume(address string) *miroirv1alpha1.MiroirVolume {
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec:       miroirv1alpha1.MiroirVolumeSpec{Export: &miroirv1alpha1.ExportSpec{FSType: "ext4"}},
	}
	if address != "" {
		v.Status.Export = &miroirv1alpha1.ExportStatus{Address: address}
	}
	return v
}

func nfsStageReq(vc *csi.VolumeCapability) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          volPvc1,
		StagingTargetPath: "/var/lib/kubelet/stage",
		VolumeCapability:  vc,
	}
}

func mountCap() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}
}

// Until the gateway Service has an address, staging must fail retryably so
// the CSI sidecar keeps retrying rather than failing the pod.
func TestStageNFSGatewayNotReady(t *testing.T) {
	n := &Node{NodeName: nodeKharkiv}
	if _, err := n.stageNFS(nfsStageReq(mountCap()), exportVolume("")); status.Code(err) != codes.Unavailable {
		t.Fatalf("unready gateway must be Unavailable, got %v", err)
	}
}

// RWX is filesystem-only; a block capability on an export volume is a
// misconfiguration, not something to mount.
func TestStageNFSRejectsBlock(t *testing.T) {
	n := &Node{NodeName: nodeKharkiv}
	block := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}
	if _, err := n.stageNFS(nfsStageReq(block), exportVolume("10.96.0.7")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("block on an RWX volume must be InvalidArgument, got %v", err)
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

// remoteVolume is a 2-replica volume on paris+oslo with remote access
// allowed; this node (kharkiv) holds no replica.
func remoteVolume() *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes:         1 << 30,
			DRBD:              &miroirv1alpha1.DRBDSpec{Port: 7000},
			AllowRemoteAccess: true,
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeParis, NodeID: 0, Address: addrParis},
				{Node: nodeOslo, NodeID: 1, Address: "192.168.1.43"},
			},
		},
	}
}

// healthyRemoteStatus is the live view of a diskless leg with quorum and
// both diskful peers reachable and current.
func healthyRemoteStatus() drbd.Status {
	return drbd.Status{
		DiskState:     drbd.DiskDiskless,
		Quorum:        true,
		PeerConnected: map[int32]bool{0: true, 1: true},
		PeerDiskState: map[int32]string{0: drbd.DiskUpToDate, 1: drbd.DiskUpToDate},
	}
}

// First stage on a non-replica node: the leg is attached (spec.clients
// gains this node) and the stage retries until the agent realizes it.
func TestDevicePathRemoteAttachAddsClientLeg(t *testing.T) {
	n := newNode(t, remoteVolume(), fakeDRBDStatus{})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("first remote stage must be Unavailable (leg attaching), got %v", err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	cl := got.Spec.ClientForNode(nodeKharkiv)
	if cl == nil {
		t.Fatalf("client leg not added: %+v", got.Spec.Clients)
	}
	if cl.AddedAt == nil {
		t.Fatal("client leg must be stamped with AddedAt (auto-diskful keys on it)")
	}
}

// Without the StorageClass opt-in a non-replica node stays refused.
func TestDevicePathRemoteRefusedWithoutOptIn(t *testing.T) {
	v := remoteVolume()
	v.Spec.AllowRemoteAccess = false
	n := newNode(t, v, fakeDRBDStatus{})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("non-replica node must be FailedPrecondition, got %v", err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Spec.Clients) != 0 {
		t.Fatalf("no client leg may be added without opt-in: %+v", got.Spec.Clients)
	}
}

// A realized client leg serves once the volume has quorum and a current
// diskful peer is reachable.
func TestDevicePathClientLegServes(t *testing.T) {
	v := remoteVolume()
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv}}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DevicePath: devDrbd1000, Diskless: true},
	}
	n := newNode(t, v, fakeDRBDStatus{st: healthyRemoteStatus()})
	dev, _, err := n.devicePath(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if dev != devDrbd1000 {
		t.Fatalf("dev = %q, want %s", dev, devDrbd1000)
	}
}

// A diskless leg without quorum, or with no current peer to read from,
// must not stage: all its I/O rides the peers.
func TestDevicePathClientLegRefusesUnhealthy(t *testing.T) {
	noQuorum := healthyRemoteStatus()
	noQuorum.Quorum = false
	stalePeers := healthyRemoteStatus()
	stalePeers.PeerDiskState = map[int32]string{0: "Inconsistent", 1: "DUnknown"}
	for name, st := range map[string]drbd.Status{"no quorum": noQuorum, "no UpToDate peer": stalePeers} {
		t.Run(name, func(t *testing.T) {
			v := remoteVolume()
			v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv}}
			v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
				nodeKharkiv: {DevicePath: devDrbd1000, Diskless: true},
			}
			n := newNode(t, v, fakeDRBDStatus{st: st})
			if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.Unavailable {
				t.Fatalf("unhealthy diskless leg must be Unavailable, got %v", err)
			}
		})
	}
}

// On a remote-access volume the tie-breaker's own diskless leg serves I/O
// — without PV affinity the scheduler may legitimately land a pod there.
func TestDevicePathTieBreakerServesRemoteVolume(t *testing.T) {
	v := remoteVolume()
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv, Diskless: true})
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DevicePath: devDrbd1000, Diskless: true},
	}
	n := newNode(t, v, fakeDRBDStatus{st: healthyRemoteStatus()})
	dev, _, err := n.devicePath(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	if dev != devDrbd1000 {
		t.Fatalf("dev = %q, want %s", dev, devDrbd1000)
	}
}

// Unstage drops the client leg so peers stop dialing it and the local
// agent tears it down.
func TestNodeUnstageRemovesClientLeg(t *testing.T) {
	v := remoteVolume()
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv}}
	n := newNode(t, v, fakeDRBDStatus{})
	n.Mounter = mount.NewSafeFormatAndMount(mount.NewFakeMounter(nil), utilexec.New())

	if _, err := n.NodeUnstageVolume(t.Context(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          volPvc1,
		StagingTargetPath: filepath.Join(t.TempDir(), "absent"),
	}); err != nil {
		t.Fatal(err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := n.Client.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Spec.Clients) != 0 {
		t.Fatalf("client leg must be removed at unstage: %+v", got.Spec.Clients)
	}
}

// The #144 staging hold applies to diskless legs too: a never-activated
// birth-split volume mid-recovery (quorum back, survivor UpToDate, loser
// divergent with its link down) must not be staged through a client or
// tie-breaker leg — that would latch Activated and close auto-recovery.
func TestDevicePathDisklessHoldsRecoveringSplitBrain(t *testing.T) {
	v := remoteVolume()
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv}}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DevicePath: devDrbd1000, Diskless: true},
		nodeOslo:    {DeviceCreated: true, SplitBrain: true},
	}
	st := healthyRemoteStatus()
	delete(st.PeerConnected, 1) // the losing leg's link is down
	n := newNode(t, v, fakeDRBDStatus{st: st})
	if _, _, err := n.devicePath(t.Context(), volPvc1); status.Code(err) != codes.Unavailable {
		t.Fatalf("recovering split-brain must hold diskless staging, got %v", err)
	}
}

// A stale split slot must not hold a diskless leg whose data links are all
// live — mirroring the diskful hold's corroboration.
func TestDevicePathDisklessStaleSplitSlotIgnored(t *testing.T) {
	v := remoteVolume()
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeKharkiv, NodeID: 2, Address: addrKharkiv}}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeKharkiv: {DevicePath: devDrbd1000, Diskless: true},
		nodeOslo:    {DeviceCreated: true, SplitBrain: true}, // stale
	}
	n := newNode(t, v, fakeDRBDStatus{st: healthyRemoteStatus()})
	if _, _, err := n.devicePath(t.Context(), volPvc1); err != nil {
		t.Fatalf("live diskless leg must stage despite a stale slot: %v", err)
	}
}
