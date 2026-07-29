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

// healthEntry is a status/reason/message triple flattened for comparison.
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
			name: "split-brain ranks above the degraded phase it implies",
			vol: volWithStatus("v", miroirv1alpha1.VolumeDegraded, map[string]miroirv1alpha1.ReplicaStatus{
				nodeB: {DiskFailed: true},
				nodeA: {SplitBrain: true},
			}),
			want: []healthEntry{
				{csi.VolumeHealthErrorType_DATA_LOSS, reasonSplitBrain},
				{csi.VolumeHealthErrorType_DEGRADED, reasonBackingDiskFailed},
				{csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync},
			},
			wantContains: "split-brain on node " + nodeA,
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
			name:         "degraded phase",
			vol:          volWithStatus("v", miroirv1alpha1.VolumeDegraded, nil),
			want:         []healthEntry{{csi.VolumeHealthErrorType_DEGRADED, reasonReplicaOutOfSync}},
			wantContains: "degraded",
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
			if tt.wantContains == "" {
				return
			}
			if msg := got.GetHealthStatuses()[0].GetMessage(); !strings.Contains(msg, tt.wantContains) {
				t.Fatalf("message %q does not contain %q", msg, tt.wantContains)
			}
		})
	}
}

// TestVolumeHealthDeterministic pins the message when several nodes match,
// so a health event doesn't flap between reconciles.
func TestVolumeHealthDeterministic(t *testing.T) {
	vol := volWithStatus("v", miroirv1alpha1.VolumeReady, map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {SplitBrain: true},
		nodeA: {SplitBrain: true},
		nodeC: {SplitBrain: true},
	})
	for range 5 {
		if msg := volumeHealth(vol).GetHealthStatuses()[0].GetMessage(); !strings.Contains(msg, nodeA) {
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

	if _, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{}); err == nil {
		t.Fatal("expected error for empty volume id")
	}
	if _, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: volMissing}); err == nil {
		t.Fatal("expected NotFound for missing volume")
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

	if _, err := c.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{}); err == nil {
		t.Fatal("expected error for empty volume id")
	}
	if _, err := c.ControllerGetVolumeHealth(context.Background(), &csi.ControllerGetVolumeHealthRequest{VolumeId: volMissing}); err == nil {
		t.Fatal("expected NotFound for missing volume")
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
	if id := second.GetEntries()[0].GetVolumeId(); id != "vol-c" {
		t.Fatalf("second page starts at %q, want vol-c", id)
	}

	if _, err := c.ControllerListVolumeHealth(context.Background(), &csi.ControllerListVolumeHealthRequest{StartingToken: "nope"}); err == nil {
		t.Fatal("expected Aborted for an invalid starting token")
	}
}
