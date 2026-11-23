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

package agent

import (
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestStartupGateCheck(t *testing.T) {
	unreplicated := vol("local", nodeA)
	elsewhere := replicatedVol()
	elsewhere.Name = "elsewhere"
	elsewhere.Spec.Replicas[0].Node = nodeC
	incomplete := replicatedVol()
	incomplete.Name = "incomplete"
	incomplete.Spec.Replicas[0].Address = ""
	deleting := replicatedVol()
	deleting.Name = "deleting"
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	future := time.Now().Add(time.Hour)
	tests := []struct {
		name string
		objs []client.Object
		// baseline and observed are the reconciler's reports for pvc-1:
		// healthy before the restart, and settled on a pass since.
		baseline *bool
		observed *bool
		deadline time.Time
		wantOpen bool
	}{
		{name: "no volumes", deadline: future, wantOpen: true},
		{
			name:     "volumes with no DRBD leg to wait for here",
			objs:     []client.Object{unreplicated, elsewhere, incomplete, deleting},
			deadline: future,
			wantOpen: true,
		},
		{
			name:     "local leg not reconciled yet",
			objs:     []client.Object{replicatedVol()},
			deadline: future,
		},
		{
			name:     "healthy before the restart, first pass still running",
			objs:     []client.Object{replicatedVol()},
			baseline: new(true),
			deadline: future,
		},
		{
			name:     "healthy before the restart, unsettled since",
			objs:     []client.Object{replicatedVol()},
			baseline: new(true),
			observed: new(false),
			deadline: future,
		},
		{
			name:     "healthy before the restart, settled since",
			objs:     []client.Object{replicatedVol()},
			baseline: new(true),
			observed: new(true),
			deadline: future,
			wantOpen: true,
		},
		{
			name:     "already unhealthy before the restart",
			objs:     []client.Object{replicatedVol()},
			baseline: new(false),
			observed: new(false),
			deadline: future,
			wantOpen: true,
		},
		{
			name:     "unsettled leg past the deadline",
			objs:     []client.Object{replicatedVol()},
			baseline: new(true),
			observed: new(false),
			deadline: time.Now().Add(-time.Second),
			wantOpen: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &StartupGate{Reader: newClient(t, tt.objs...), NodeName: nodeA, Deadline: tt.deadline}
			if tt.baseline != nil {
				g.Baseline(volPvc1, *tt.baseline)
			}
			if tt.observed != nil {
				g.Observe(volPvc1, *tt.observed)
			}
			err := g.Check(httptest.NewRequest("GET", "/readyz", nil))
			if (err == nil) != tt.wantOpen {
				t.Fatalf("Check() = %v, want open %v", err, tt.wantOpen)
			}
		})
	}
}

// Only the status the previous agent process left behind is the baseline:
// a later pass reporting the leg unhealthy must not exempt it.
func TestStartupGateBaselineFirstCallWins(t *testing.T) {
	g := &StartupGate{Reader: newClient(t, replicatedVol()), NodeName: nodeA, Deadline: time.Now().Add(time.Hour)}
	g.Baseline(volPvc1, true)
	g.Baseline(volPvc1, false)
	g.Observe(volPvc1, false)
	if g.Check(httptest.NewRequest("GET", "/readyz", nil)) == nil {
		t.Fatal("a leg healthy before the restart and unsettled since must hold the gate")
	}
}

// The gate paces rollouts only: a leg that degrades after the agent went
// ready must not flip the pod back to not-ready.
func TestStartupGateStaysOpen(t *testing.T) {
	g := &StartupGate{Reader: newClient(t, replicatedVol()), NodeName: nodeA, Deadline: time.Now().Add(time.Hour)}
	req := httptest.NewRequest("GET", "/readyz", nil)
	g.Baseline(volPvc1, true)
	g.Observe(volPvc1, true)
	if err := g.Check(req); err != nil {
		t.Fatal(err)
	}
	g.Observe(volPvc1, false)
	if err := g.Check(req); err != nil {
		t.Fatalf("gate closed again after opening: %v", err)
	}
}
