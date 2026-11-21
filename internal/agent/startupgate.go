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
	"fmt"
	"net/http"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// StartupSettleTimeout bounds how long a restarted agent withholds
// readiness: an unsettled leg must not hold a rollout forever.
const StartupSettleTimeout = 2 * time.Minute

// StartupGate is the agent's readiness check. It stays closed until every
// replicated leg on this node that was healthy before the restart has been
// reconciled and found settled (see legSettled), so a DaemonSet rollout
// waits for this node's replication before restarting the next agent.
// Once open it stays open: the probe only paces rollouts.
type StartupGate struct {
	// Reader lists the volumes to wait for; the manager's cached client.
	Reader   client.Reader
	NodeName string
	// Deadline is when the gate opens regardless of unsettled legs.
	Deadline time.Time

	mu sync.Mutex
	// healthyBefore is keyed by volume reconciled since startup.
	healthyBefore map[string]bool
	settled       map[string]bool
	open          bool
}

// Baseline records whether a volume's local leg was healthy per the status
// it published before this agent first reconciled it. Only the first call
// per volume counts. A nil gate ignores it.
func (g *StartupGate) Baseline(volume string, healthy bool) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.open {
		return
	}
	if _, seen := g.healthyBefore[volume]; seen {
		return
	}
	if g.healthyBefore == nil {
		g.healthyBefore = map[string]bool{}
	}
	g.healthyBefore[volume] = healthy
}

// Observe records the outcome of one full reconcile pass over a replicated
// volume's local leg. A nil gate ignores it.
func (g *StartupGate) Observe(volume string, settled bool) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.open {
		return
	}
	if g.settled == nil {
		g.settled = map[string]bool{}
	}
	g.settled[volume] = settled
}

// Check is the healthz.Checker behind the agent's /readyz.
func (g *StartupGate) Check(req *http.Request) error {
	if g.isOpen() {
		return nil
	}
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := g.Reader.List(req.Context(), vols); err != nil {
		return fmt.Errorf("agent: list volumes: %w", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range vols.Items {
		vol := &vols.Items[i]
		if vol.Spec.DRBD == nil || !vol.DeletionTimestamp.IsZero() {
			continue
		}
		if _, _, present, incomplete := legState(vol, g.NodeName); !present || incomplete {
			continue
		}
		healthyBefore, reconciled := g.healthyBefore[vol.Name]
		if !reconciled || (healthyBefore && !g.settled[vol.Name]) {
			return fmt.Errorf("agent: volume %s has not settled since the agent started", vol.Name)
		}
	}
	g.openLocked()
	return nil
}

// isOpen reports whether the gate has opened, opening it at the deadline.
func (g *StartupGate) isOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open && !time.Now().Before(g.Deadline) {
		ctrl.Log.WithName("startup-gate").Info(
			"DRBD legs still unsettled at the startup deadline; reporting ready anyway",
			"node", g.NodeName)
		g.openLocked()
	}
	return g.open
}

func (g *StartupGate) openLocked() {
	g.open = true
	g.healthyBefore, g.settled = nil, nil
}
