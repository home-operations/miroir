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
	"context"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/home-operations/miroir/internal/drbd"
)

// drbdShutdownTimeout bounds the Secondary-teardown sweep at shutdown.
const drbdShutdownTimeout = 15 * time.Second

// ShutdownDownSecondaries demotes all Primaries then brings down every
// Secondary on a cordoned node, releasing backing devices so the OS can tear
// down storage pools without EIO. The early signal path deliberately does not
// perform API access, so it can start before manager shutdown has completed.
func ShutdownDownSecondaries(cordon *CordonWatcher, driver *drbd.Driver) {
	if cordon.Cordoned() {
		ctx, cancel := context.WithTimeout(context.Background(), drbdShutdownTimeout)
		defer cancel()
		setupLog.Info("node cordoned; demoting Primaries and releasing DRBD backings for shutdown")
		// Demote first: --force terminates pending I/O so the subsequent
		// down and the OS teardown cannot wedge on a metadata flush.
		if err := driver.DemoteAll(ctx); err != nil {
			setupLog.Error(err, "DRBD shutdown demote failed; attempting down sweep anyway")
		}
		if err := driver.DownSecondaries(ctx); err != nil {
			setupLog.Error(err, "DRBD shutdown teardown failed; node reboot may stall")
		}
	}
}

func ShutdownBarrierSweep(c client.Reader, driver *drbd.Driver, nodeName string) {
	// Short API budget: the chart grants 60s of termination grace and the
	// manager stop + DownSecondaries already spend up to 45s of it. A
	// stranded barrier missed here is lifted by the startup sweep on the
	// next boot.
	if err := ResumeStaleBarriers(context.Background(), c, driver,
		NewFreezer(), nodeName, apiShutdownWait); err != nil {
		setupLog.Error(err, "shutdown barrier sweep failed")
	}
}

// ShutdownSweep is the final idempotent shutdown pass.
func ShutdownSweep(c client.Reader, cordon *CordonWatcher, driver *drbd.Driver, nodeName string) {
	ShutdownDownSecondaries(cordon, driver)
	ShutdownBarrierSweep(c, driver, nodeName)
}
