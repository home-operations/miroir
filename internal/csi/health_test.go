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
	"slices"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

func volWithStatus(name string, phase miroirv1alpha1.VolumePhase, perNode map[string]miroirv1alpha1.ReplicaStatus) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     miroirv1alpha1.MiroirVolumeStatus{Phase: phase, PerNode: perNode},
	}
}

// activated marks the volume as having been staged for a consumer, which is
// what turns a self-healing birth split into operator-resolved data loss and a
// provisioning verdict into a regression.
func activated(vol *miroirv1alpha1.MiroirVolume) *miroirv1alpha1.MiroirVolume {
	vol.Status.Activated = true
	return vol
}

// withServingLeg adds a diskful replica whose backing device the agent
// created, i.e. a leg that can still serve.
func withServingLeg(vol *miroirv1alpha1.MiroirVolume, node string) *miroirv1alpha1.MiroirVolume {
	vol.Spec.Replicas = append(vol.Spec.Replicas, miroirv1alpha1.Replica{Node: node})
	if vol.Status.PerNode == nil {
		vol.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
	}
	st := vol.Status.PerNode[node]
	st.DeviceCreated = true
	vol.Status.PerNode[node] = st
	return vol
}

// healthEntry is a status/reason pair flattened for comparison.
type healthEntry struct {
	status csi.VolumeHealthErrorType
	reason string
}

func entriesOf(h *csi.VolumeHealth) []healthEntry {
	out := make([]healthEntry, 0, len(h.GetHealthStatuses()))
	for _, e := range h.GetHealthStatuses() {
		out = append(out, healthEntry{e.GetStatus(), e.GetReason()})
	}
	return out
}

// headMessage is the message a CO surfacing only the most urgent condition
// shows, or "" when nothing is reported.
func headMessage(h *csi.VolumeHealth) string {
	if len(h.GetHealthStatuses()) == 0 {
		return ""
	}
	return h.GetHealthStatuses()[0].GetMessage()
}

func TestVolumeHealth(t *testing.T) {
	tests := []struct {
		name         string
		vol          *miroirv1alpha1.MiroirVolume
		want         []healthEntry
		wantContains string
	}{
		{
			name: "ready reports no adverse status",
			vol:  volWithStatus("v", miroirv1alpha1.VolumeReady, nil),
			want: []healthEntry{},
		},
		{
			name: "creating is the expected state while provisioning",
			vol:  volWithStatus("v", miroirv1alpha1.VolumeCreating, nil),
			want: []healthEntry{},
		},
		{
			name: "an un-reconciled volume has no phase and no condition",
			vol:  volWithStatus("v", "", nil),
			want: []healthEntry{},
		},
		{
			name: "split-brain ranks above the degraded phase it implies",
			vol: activated(volWithStatus("v", miroirv1alpha1.VolumeDegraded, map[string]miroirv1alpha1.ReplicaStatus{
				nodeB: {DiskFailed: true},
				nodeA: {SplitBrain: true},
			})),
			want: []healthEntry{
				{csi.VolumeHealthErrorType_DATA_LOSS, reasonSplitBrain},
				{csi.VolumeHealthErrorType_DEGRADED, reasonBackingDiskFailed},
				{csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync},
			},
			wantContains: "split-brain on node " + nodeA,
		},
		{
			// The reconciler auto-resolves this one (recoverSplitBrain);
			// DATA_LOSS would send a CO restoring a snapshot of an empty
			// volume.
			name: "a birth split-brain is degraded, not data loss",
			vol: volWithStatus("v", miroirv1alpha1.VolumeCreating, map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {SplitBrain: true},
			}),
			want:         []healthEntry{{csi.VolumeHealthErrorType_DEGRADED, reasonSplitBrainRecovering}},
			wantContains: "resolves it automatically",
		},
		{
			name: "disk failed without split-brain",
			vol: volWithStatus("v", miroirv1alpha1.VolumeReady, map[string]miroirv1alpha1.ReplicaStatus{
				nodeB: {DiskFailed: true},
			}),
			want:         []healthEntry{{csi.VolumeHealthErrorType_DEGRADED, reasonBackingDiskFailed}},
			wantContains: "backing disk failed on node " + nodeB,
		},
		{
			name:         "failed phase is inaccessible",
			vol:          volWithStatus("v", miroirv1alpha1.VolumeFailed, nil),
			want:         []healthEntry{{csi.VolumeHealthErrorType_INACCESSIBLE, reasonProvisioningFailed}},
			wantContains: "provisioning failed",
		},
		{
			// Phase short-circuits to Failed on the first unrealized leg, so
			// it says nothing about the peers still serving the pod.
			name:         "failed phase with a surviving leg is degraded, not inaccessible",
			vol:          withServingLeg(activated(volWithStatus("v", miroirv1alpha1.VolumeFailed, nil)), nodeA),
			want:         []healthEntry{{csi.VolumeHealthErrorType_DEGRADED, reasonReplicaUnrealized}},
			wantContains: "serving from its remaining legs",
		},
		{
			name:         "creating on a volume that has carried data is a regression",
			vol:          activated(volWithStatus("v", miroirv1alpha1.VolumeCreating, nil)),
			want:         []healthEntry{{csi.VolumeHealthErrorType_INACCESSIBLE, reasonBackingDeviceLost}},
			wantContains: "cannot be staged",
		},
		{
			name:         "degraded phase",
			vol:          volWithStatus("v", miroirv1alpha1.VolumeDegraded, nil),
			want:         []healthEntry{{csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync}},
			wantContains: "degraded",
		},
		{
			// The disk-failure entry is built first; severity, not build
			// order, decides what a CO reading only the head sees.
			name: "a lesser condition found first does not head the list",
			vol: volWithStatus("v", miroirv1alpha1.VolumeFailed, map[string]miroirv1alpha1.ReplicaStatus{
				nodeB: {DiskFailed: true},
			}),
			want: []healthEntry{
				{csi.VolumeHealthErrorType_INACCESSIBLE, reasonProvisioningFailed},
				{csi.VolumeHealthErrorType_DEGRADED, reasonBackingDiskFailed},
			},
			wantContains: "provisioning failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := volumeHealth(tt.vol)
			if got.GetVolumeId() != tt.vol.Name {
				t.Fatalf("volume id = %q, want %q", got.GetVolumeId(), tt.vol.Name)
			}
			if !slices.Equal(entriesOf(got), tt.want) {
				t.Fatalf("statuses = %v, want %v", entriesOf(got), tt.want)
			}
			if msg := headMessage(got); !strings.Contains(msg, tt.wantContains) {
				t.Fatalf("message %q does not contain %q", msg, tt.wantContains)
			}
		})
	}
}

// TestVolumeHealthDeterministic pins the message when several nodes match,
// so a health event doesn't flap between reconciles.
func TestVolumeHealthDeterministic(t *testing.T) {
	vol := activated(volWithStatus("v", miroirv1alpha1.VolumeReady, map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {SplitBrain: true},
		nodeA: {SplitBrain: true},
		nodeC: {SplitBrain: true},
	}))
	for range 5 {
		if msg := headMessage(volumeHealth(vol)); !strings.Contains(msg, nodeA) {
			t.Fatalf("expected lexically-first node %s, got %q", nodeA, msg)
		}
	}
}

func TestControllerGetVolume(t *testing.T) {
	s := newScheme(t)
	vol := volWithStatus(volPvc1, miroirv1alpha1.VolumeDegraded, nil)
	vol.Spec.SizeBytes = 1 << 30
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(vol).Build()}

	resp, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: volPvc1})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetVolume().GetVolumeId() != volPvc1 {
		t.Fatalf("volume id = %q, want %q", resp.GetVolume().GetVolumeId(), volPvc1)
	}
	if resp.GetVolume().GetCapacityBytes() != 1<<30 {
		t.Fatalf("capacity = %d, want %d", resp.GetVolume().GetCapacityBytes(), 1<<30)
	}
	// status is REQUIRED even though v1.13 reserved the condition inside it;
	// a non-Go CO dereferences the message rather than a nil-safe getter.
	if resp.GetStatus() == nil {
		t.Fatal("status is a REQUIRED field, got nil")
	}

	if _, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty volume id must be InvalidArgument, got %v", err)
	}
	if _, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: volMissing}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing volume must be NotFound, got %v", err)
	}
}

func TestControllerGetVolumeHealth(t *testing.T) {
	s := newScheme(t)
	vol := volWithStatus(volPvc1, miroirv1alpha1.VolumeDegraded, nil)
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(vol).Build()}

	resp, err := c.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{VolumeId: volPvc1})
	if err != nil {
		t.Fatal(err)
	}
	if got := entriesOf(resp.GetVolumeHealth()); !slices.Equal(got, []healthEntry{{csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync}}) {
		t.Fatalf("statuses = %v, want a single degraded entry", got)
	}

	if _, err := c.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty volume id must be InvalidArgument, got %v", err)
	}
	if _, err := c.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{VolumeId: volMissing}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing volume must be NotFound, got %v", err)
	}
}

// ControllerListVolumeHealth reports only the abnormal volumes, in a stable
// order, and pages through them with positional tokens.
func TestControllerListVolumeHealth(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
		volWithStatus("vol-c", miroirv1alpha1.VolumeFailed, nil),
		volWithStatus("vol-a", miroirv1alpha1.VolumeReady, nil),
		volWithStatus("vol-b", miroirv1alpha1.VolumeDegraded, nil),
	).Build()}

	all, err := c.ControllerListVolumeHealth(context.Background(), &csi.ControllerListVolumeHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.GetEntries()) != 2 {
		t.Fatalf("entries = %d, want the 2 abnormal volumes", len(all.GetEntries()))
	}
	if id := all.GetEntries()[0].GetVolumeId(); id != "vol-b" {
		t.Fatalf("first entry = %q, want vol-b (healthy vol-a omitted, order stable)", id)
	}
	if all.GetNextToken() != "" {
		t.Fatalf("next token = %q, want none on a complete listing", all.GetNextToken())
	}

	first, err := c.ControllerListVolumeHealth(context.Background(), &csi.ControllerListVolumeHealthRequest{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetEntries()) != 1 || first.GetNextToken() != "1" {
		t.Fatalf("page = %d entries, next %q; want 1 and \"1\"", len(first.GetEntries()), first.GetNextToken())
	}
	second, err := c.ControllerListVolumeHealth(context.Background(), &csi.ControllerListVolumeHealthRequest{StartingToken: first.GetNextToken()})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.GetEntries()) != 1 {
		t.Fatalf("second page = %d entries, want 1", len(second.GetEntries()))
	}
	if id := second.GetEntries()[0].GetVolumeId(); id != "vol-c" {
		t.Fatalf("second page starts at %q, want vol-c", id)
	}

	if _, err := c.ControllerListVolumeHealth(context.Background(),
		&csi.ControllerListVolumeHealthRequest{StartingToken: "nope"}); status.Code(err) != codes.Aborted {
		t.Fatalf("an unparseable starting token must be Aborted, got %v", err)
	}
}

// The abnormal set shrinks whenever a volume recovers, so a token this RPC
// issued one poll earlier routinely lands past the end of the next listing.
// That page is empty; aborting would make the CO restart the whole listing
// every time a volume resynced mid-page.
func TestControllerListVolumeHealthTokenPastEnd(t *testing.T) {
	s := newScheme(t)
	c := &Controller{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
		volWithStatus("vol-a", miroirv1alpha1.VolumeReady, nil),
	).Build()}

	resp, err := c.ControllerListVolumeHealth(context.Background(), &csi.ControllerListVolumeHealthRequest{StartingToken: "2"})
	if err != nil {
		t.Fatalf("a stale token must not abort the listing: %v", err)
	}
	if len(resp.GetEntries()) != 0 || resp.GetNextToken() != "" {
		t.Fatalf("entries = %d, next %q; want an empty final page", len(resp.GetEntries()), resp.GetNextToken())
	}
	// ListVolumes pages a set that only moves on create/delete, where the
	// same token means the caller must restart.
	if _, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{StartingToken: "2"}); status.Code(err) != codes.Aborted {
		t.Fatalf("ListVolumes must still abort a past-the-end token, got %v", err)
	}
}
