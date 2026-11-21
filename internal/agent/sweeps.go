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
	"errors"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// barrierSweepInterval paces the periodic stranded-barrier sweep (AddBarrierSweep).
const barrierSweepInterval = 5 * time.Minute

// AddBarrierSweep runs the startup/shutdown stranded-barrier sweep
// mid-runtime too: a wedged suspend-io (LINBIT/drbd#137) can set the kernel
// flag after drbdadm reported failure, leaving a frozen volume no recorded
// round owns until the agent restarts. ResumeStaleBarriers skips live rounds
// (fresh ioSuspended timestamp); a round past SuspendDeadline is void
// by protocol anyway. A round is briefly invisible between its suspend-io
// and its status patch, so a tick can in theory lift a live coordinator's
// barrier — bounded: the cut phase re-asserts suspend-io per leg (though
// not the thawed freeze, so that round's cut degrades from fs-consistent
// to crash-consistent). API-list failures are logged, not fatal: the next
// tick retries.
func AddBarrierSweep(mgr manager.Manager, drbdReady bool, driver *drbd.Driver, freezer *Freezer,
	nodeName string) error {
	if !drbdReady {
		return nil
	}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(barrierSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := ResumeStaleBarriers(ctx, mgr.GetAPIReader(), driver, freezer, nodeName, apiShutdownWait); err != nil {
					setupLog.Error(err, "periodic barrier sweep incomplete")
				}
			}
		}
	})); err != nil {
		return fmt.Errorf("unable to add periodic barrier sweep: %w", err)
	}
	return nil
}

// wedgeSampleInterval paces the node-scoped breaker's gauge: short enough to
// show the jam building rather than only its aftermath, and cheap at one
// /proc/<pid>/stat read per outstanding child.
const wedgeSampleInterval = 30 * time.Second

// AddWedgeSampler publishes the node-scoped breaker's state on a timer and
// logs its transitions. Sampled rather than event-driven because the count
// self-heals: a gauge written only when a child strands would page forever
// after the node recovered.
func AddWedgeSampler(mgr manager.Manager, w *backend.Wedge) error {
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(wedgeSampleInterval)
		defer ticker.Stop()
		open := false
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				RecordNodeWedge(w)
				// Edges only: a jammed node stays jammed until it reboots,
				// and repeating the line every 30s buries it.
				if tripped := w.Tripped(); tripped != open {
					open = tripped
					if tripped {
						setupLog.Error(w.Err(), "node storage stack wedged; refusing further storage commands until this node reboots",
							"stranded", w.Stranded())
					} else {
						setupLog.Info("node storage stack recovered; resuming storage commands")
					}
				}
			}
		}
	})); err != nil {
		return fmt.Errorf("unable to add the wedge sampler: %w", err)
	}
	return nil
}

// SweepOrphans removes DRBD state with no owning volume on this node. c
// must be an uncached reader: the manager has not started yet.
// Returns an error only when the volume list cannot be fetched; the sweep
// itself is best-effort and its failures are logged here.
func SweepOrphans(ctx context.Context, c client.Reader, nodeName string, driver *drbd.Driver) error {
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := ListWithRetry(ctx, c, vols, APIStartupWait); err != nil {
		return err
	}
	owned := map[string]bool{}
	for _, v := range vols.Items {
		for _, rep := range v.Spec.Replicas {
			if rep.Node == nodeName {
				owned[v.Name] = true
			}
		}
		// A held finalizer without a spec entry is a replica pending
		// removal: its teardown is the reconciler's, gated on the
		// remaining replicas' health — not the orphan sweep's.
		for _, f := range v.Finalizers {
			if f == constants.FinalizerPrefix+nodeName {
				owned[v.Name] = true
			}
		}
	}
	if err := driver.SweepOrphans(ctx,
		func(name string) bool { return owned[name] }); err != nil {
		setupLog.Error(err, "orphan sweep incomplete")
	}
	return nil
}

// ResumeStaleBarriers lifts suspend-io left behind by a previous crash.
// The kernel's view drives the sweep: a crash between suspend-io and the
// status patch leaves a frozen device no snapshot records. It is read
// before the rounds are listed, so a barrier is never judged against a
// listing older than itself. Barriers whose round is still within the
// deadline are the reconciler's to drive. c must be an uncached reader.
// Returns an error only when a round list cannot be fetched; resume
// failures are per-resource and logged here.
func ResumeStaleBarriers(ctx context.Context, c client.Reader, driver *drbd.Driver, freezer *Freezer,
	nodeName string, apiBudget time.Duration) error {
	suspended, err := driver.UserSuspended(ctx)
	if err != nil {
		// No kernel view (e.g. module not loaded yet) also means nothing
		// can be suspended — don't block agent startup on it.
		setupLog.Error(err, "cannot list suspended resources; skipping barrier sweep")
		return nil
	}
	if len(suspended) == 0 {
		return nil
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := ListWithRetry(ctx, c, snaps, apiBudget); err != nil {
		return err
	}
	fresh := map[string]bool{}
	for _, s := range snaps.Items {
		if s.Status.IOSuspended && s.Status.SuspendedAt != nil &&
			time.Since(s.Status.SuspendedAt.Time) < SuspendDeadline {
			fresh[s.Spec.VolumeName] = true
		}
	}
	// A group round's ioSuspended lives on the group object, not on its
	// member snapshots — without this, every volume of a live group round
	// reads as stale and the sweep resumes (and thaws) mid-cut. Member
	// volumes come from the member snapshots; perLeg slot keys
	// ("<volume>/<node>") cover a member already torn out of the list.
	groups := &miroirv1alpha1.MiroirSnapshotGroupList{}
	if err := ListWithRetry(ctx, c, groups, apiBudget); err != nil {
		return err
	}
	snapVol := map[string]string{}
	for _, s := range snaps.Items {
		snapVol[s.Name] = s.Spec.VolumeName
	}
	for _, g := range groups.Items {
		if !g.Status.IOSuspended || g.Status.SuspendedAt == nil ||
			time.Since(g.Status.SuspendedAt.Time) >= SuspendDeadline {
			continue
		}
		for _, name := range g.Spec.SnapshotNames {
			if vol := snapVol[name]; vol != "" {
				fresh[vol] = true
			}
		}
		for key := range g.Status.PerLeg {
			if vol, _, ok := strings.Cut(key, "/"); ok {
				fresh[vol] = true
			}
		}
	}
	var errs []error
	var stale []string
	for _, vol := range suspended {
		if fresh[vol] {
			continue
		}
		stale = append(stale, vol)
		setupLog.Info("lifting stale IO barrier", "volume", vol)
		if err := driver.ResumeIO(ctx, vol); err != nil {
			errs = append(errs, fmt.Errorf("resume stale barrier on %s: %w", vol, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		setupLog.Error(err, "barrier resume sweep incomplete")
	}
	// A crash mid-round can leave the filesystem freeze behind exactly
	// like the barrier; the reconcilers only thaw volumes whose snapshot
	// objects still exist, so the sweep covers the rest. Fresh rounds
	// keep their freeze — their own close lifts it.
	if len(stale) > 0 && freezer != nil {
		vols := &miroirv1alpha1.MiroirVolumeList{}
		if err := ListWithRetry(ctx, c, vols, apiBudget); err != nil {
			setupLog.Error(err, "cannot list volumes; skipping freeze thaw sweep")
			return nil
		}
		paths := map[string]string{}
		for i := range vols.Items {
			paths[vols.Items[i].Name] = vols.Items[i].Status.PerNode[nodeName].DevicePath
		}
		for _, name := range stale {
			if device := paths[name]; device != "" {
				mp, err := freezer.Thaw(device)
				switch {
				case err != nil:
					setupLog.Error(err, "cannot thaw stale filesystem freeze", "volume", name)
				case mp == "":
					// Usually a leg that never froze (only mounted legs do),
					// but a freeze leaked before its mount went away is
					// unreachable now — mount refuses the frozen device and
					// FITHAW needs a mountpoint. Not silent, so
					// the leak never looks like success; the stage-time
					// recovery is what clears it.
					setupLog.Info("stale barrier volume is not mounted; nothing to thaw here",
						"volume", name, "device", device)
				}
			}
		}
	}
	return nil
}
