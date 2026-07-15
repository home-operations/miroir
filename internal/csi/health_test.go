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

func TestVolumeCondition(t *testing.T) {
	tests := []struct {
		name         string
		vol          *miroirv1alpha1.MiroirVolume
		wantAbnormal bool
		wantContains string
	}{
		{
			name:         "ready is healthy",
			vol:          volWithStatus("v", miroirv1alpha1.VolumeReady, nil),
			wantAbnormal: false,
			wantContains: "healthy",
		},
		{
			name: "split-brain wins over everything",
			vol: volWithStatus("v", miroirv1alpha1.VolumeDegraded, map[string]miroirv1alpha1.ReplicaStatus{
				nodeB: {DiskFailed: true},
				nodeA: {SplitBrain: true},
			}),
			wantAbnormal: true,
			wantContains: "split-brain on node " + nodeA,
		},
		{
			name: "disk failed when no split-brain",
			vol: volWithStatus("v", miroirv1alpha1.VolumeReady, map[string]miroirv1alpha1.ReplicaStatus{
				nodeB: {DiskFailed: true},
			}),
			wantAbnormal: true,
			wantContains: "backing disk failed on node " + nodeB,
		},
		{
			name:         "failed phase",
			vol:          volWithStatus("v", miroirv1alpha1.VolumeFailed, nil),
			wantAbnormal: true,
			wantContains: "provisioning failed",
		},
		{
			name:         "degraded phase",
			vol:          volWithStatus("v", miroirv1alpha1.VolumeDegraded, nil),
			wantAbnormal: true,
			wantContains: "degraded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := volumeCondition(tt.vol)
			if got.GetAbnormal() != tt.wantAbnormal {
				t.Fatalf("abnormal = %v, want %v (msg %q)", got.GetAbnormal(), tt.wantAbnormal, got.GetMessage())
			}
			if !strings.Contains(got.GetMessage(), tt.wantContains) {
				t.Fatalf("message %q does not contain %q", got.GetMessage(), tt.wantContains)
			}
		})
	}
}

// TestVolumeConditionDeterministic pins the message when several nodes match,
// so a health event doesn't flap between reconciles.
func TestVolumeConditionDeterministic(t *testing.T) {
	vol := volWithStatus("v", miroirv1alpha1.VolumeReady, map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {SplitBrain: true},
		nodeA: {SplitBrain: true},
		nodeC: {SplitBrain: true},
	})
	for range 5 {
		if msg := volumeCondition(vol).GetMessage(); !strings.Contains(msg, nodeA) {
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
	if !resp.GetStatus().GetVolumeCondition().GetAbnormal() {
		t.Fatal("expected degraded volume to report abnormal")
	}

	if _, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{}); err == nil {
		t.Fatal("expected error for empty volume id")
	}
	if _, err := c.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "missing"}); err == nil {
		t.Fatal("expected NotFound for missing volume")
	}
}
