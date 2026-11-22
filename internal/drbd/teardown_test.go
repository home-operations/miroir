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

package drbd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/home-operations/miroir/internal/backend"
)

func TestDownRemovesState(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"pvc-1","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeA)

	if err := d.Apply(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if err := d.Down(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	// Disconnect must precede down, with the peer_node_id from status.
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.calledWith(t, "drbdsetup down pvc-1")
	discIdx, downIdx := -1, -1
	for i, c := range fe.calls {
		if strings.Contains(c, "drbdsetup disconnect pvc-1") {
			discIdx = i
		}
		if strings.Contains(c, "drbdsetup down pvc-1") {
			downIdx = i
		}
	}
	if discIdx < 0 || downIdx < 0 || discIdx > downIdx {
		t.Fatalf("disconnect must precede down, got calls: %v", fe.calls)
	}
	if _, err := os.Stat(filepath.Join(d.StateDir, "pvc-1.res")); !os.IsNotExist(err) {
		t.Fatal("res file must be removed")
	}

	// Down on never-configured resource is a no-op.
	before := len(fe.calls)
	if err := d.Down(t.Context(), "pvc-other"); err != nil {
		t.Fatal(err)
	}
	if len(fe.calls) != before {
		t.Fatal("down on unknown resource must not call drbdadm")
	}
}

func TestDownSecondariesSkipsPrimary(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Primary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]},
			{"name":"pvc-2","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.DownSecondaries(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Primary legs are still open — skipped.
	fe.notCalledWith(t, "drbdsetup down pvc-1")
	fe.notCalledWith(t, "drbdsetup disconnect pvc-1")
	fe.calledWith(t, "drbdsetup disconnect pvc-2 1")
	fe.calledWith(t, "drbdsetup down pvc-2")
	// One listing for the sweep, one re-read under the lock per resource
	// actually downed; the skipped Primary costs no re-read.
	statusCalls := 0
	for _, c := range fe.calls {
		if strings.Contains(c, cmdDrbdsetupStatus) {
			statusCalls++
		}
	}
	if statusCalls != 2 {
		t.Fatalf("want exactly 2 status calls, got %d: %v", statusCalls, fe.calls)
	}
}

func TestDownSecondariesContinuesOnError(t *testing.T) {
	fe := &fakeExec{
		responses: map[string]string{
			cmdDrbdsetupStatus: `[
				{"name":"pvc-2","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]},
				{"name":"pvc-3","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		},
		errOn: map[string]error{"down pvc-2": errors.New("Device is held open by someone")},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	err := d.DownSecondaries(t.Context())
	if err == nil || !strings.Contains(err.Error(), volPvc2) {
		t.Fatalf("want a joined error naming pvc-2, got %v", err)
	}
	// One stuck resource must not strand the rest of the sweep.
	fe.calledWith(t, "drbdsetup disconnect pvc-3 1")
	fe.calledWith(t, "drbdsetup down pvc-3")
}

// DemoteAll force-demotes every Primary so the OS can release backing
// devices during shutdown. Secondaries are skipped (already not Primary).
func TestDemoteAllForceDemotesPrimaries(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Primary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]},
			{"name":"pvc-2","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.DemoteAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Primary is demoted with --force (terminates pending I/O, no metadata flush wait).
	fe.calledWith(t, "drbdadm secondary --force pvc-1")
	// Secondary is already not Primary — skipped.
	fe.notCalledWith(t, "drbdadm secondary pvc-2")
	// No down calls — DemoteAll only demotes; DownSecondaries does the down.
	fe.notCalledWith(t, "drbdsetup down")
}

// A wedged resource is skipped, not force-demoted: a demote against the
// wedge signature can only hang (issue #195), same as down does.
func TestDemoteAllSkipsWedged(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Primary","devices":[{"disk-state":"Detaching"}],"connections":[{"peer-node-id":1,"connection-state":"StandAlone"}]},
			{"name":"pvc-2","role":"Primary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	err := d.DemoteAll(t.Context())
	if err == nil || !strings.Contains(err.Error(), "wedged") {
		t.Fatalf("want a joined error naming the wedged resource, got %v", err)
	}
	// Wedged pvc-1 is not touched.
	fe.notCalledWith(t, "drbdadm secondary pvc-1")
	// Healthy pvc-2 is still demoted.
	fe.calledWith(t, "drbdadm secondary --force pvc-2")
}

// One stuck demote must not strand the rest of the sweep.
func TestDemoteAllContinuesOnError(t *testing.T) {
	fe := &fakeExec{
		responses: map[string]string{
			cmdDrbdsetupStatus: `[
				{"name":"pvc-1","role":"Primary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]},
				{"name":"pvc-2","role":"Primary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		},
		errOn: map[string]error{"secondary --force pvc-1": errors.New("device busy")},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	err := d.DemoteAll(t.Context())
	if err == nil || !strings.Contains(err.Error(), "pvc-1") {
		t.Fatalf("want a joined error naming pvc-1, got %v", err)
	}
	// The failed demote of pvc-1 must not strand pvc-2.
	fe.calledWith(t, "drbdadm secondary --force pvc-2")
}

func TestSweepOrphansContinuesOnError(t *testing.T) {
	fe := &fakeExec{
		responses: map[string]string{
			cmdDrbdsetupStatus: `[
				{"name":"pvc-1","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]},
				{"name":"pvc-2","role":"Secondary","connections":[]}]`,
		},
		errOn: map[string]error{"down pvc-1": errors.New("signal: killed")},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	for _, name := range []string{volPvc1, volPvc2} {
		if err := os.WriteFile(d.path(name+".res"), nil, 0o640); err != nil {
			t.Fatal(err)
		}
	}

	err := d.SweepOrphans(t.Context(), func(string) bool { return false })
	if err == nil || !strings.Contains(err.Error(), volPvc1) {
		t.Fatalf("want a joined error naming pvc-1, got %v", err)
	}
	// One wedged orphan must not strand the rest of the sweep.
	fe.calledWith(t, "drbdsetup down pvc-2")
	// The wedged resource is still configured in the kernel: its rendered
	// config must survive, while the downed orphan's is removed.
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("wedged resource's config must remain: %v", err)
	}
	if _, err := os.Stat(d.path("pvc-2.res")); !os.IsNotExist(err) {
		t.Fatalf("downed orphan's config must be removed, got %v", err)
	}
}

// A resource stuck Detaching with the connections gone is wedged in the
// kernel (LINBIT/drbd#137): Down must never spawn another down — each
// attempt can strand another unkillable process. The first sighting
// defers (a killed down's detach may still be draining); the second
// consecutive sighting escalates to ErrWedged.
func TestDownWedgedSkipsDown(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Detaching"}],"connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	err := d.Down(t.Context(), volPvc1)
	if err == nil || errors.Is(err, ErrWedged) {
		t.Fatalf("first sighting must defer without escalating, got %v", err)
	}
	if err := d.Down(t.Context(), volPvc1); !errors.Is(err, ErrWedged) {
		t.Fatalf("second consecutive sighting must return ErrWedged, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down")
	// The rendered config stays: the resource is still configured in the
	// kernel, and a post-reboot retry finishes the teardown.
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("wedged resource's config must remain: %v", err)
	}
}

// A lingering StandAlone connection — which disconnect refuses (-9) and
// cannot remove — must not defeat the wedge signature.
func TestDownWedgedWithStandAlonePeer(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Detaching"}],
			"connections":[{"peer-node-id":1,"connection-state":"StandAlone"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	_ = d.Down(t.Context(), volPvc1)
	if err := d.Down(t.Context(), volPvc1); !errors.Is(err, ErrWedged) {
		t.Fatalf("StandAlone peer must not defeat the signature, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down")
}

// A completed teardown between two sightings resets the escalation: the
// signature vanishing means the drain finished, so a later teardown of a
// same-named resource must start from a clean slate.
func TestDownWedgeSightingResetsWhenSignatureClears(t *testing.T) {
	wedged := `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"Detaching"}],"connections":[]}]`
	healthy := `[{"name":"` + volPvc1 + `","role":"Secondary",
		"devices":[{"disk-state":"` + DiskUpToDate + `"}],"connections":[]}]`
	fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: wedged}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	_ = d.Down(t.Context(), volPvc1) // first sighting recorded
	fe.responses[cmdDrbdsetupStatus] = healthy
	if err := d.Down(t.Context(), volPvc1); err != nil {
		t.Fatalf("signature gone: down must proceed, got %v", err)
	}
	fe.calledWith(t, "drbdsetup down pvc-1")
	// Re-render and wedge again: escalation must need two fresh sightings.
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	fe.responses[cmdDrbdsetupStatus] = wedged
	if err := d.Down(t.Context(), volPvc1); errors.Is(err, ErrWedged) {
		t.Fatal("a cleared sighting must not carry over to a fresh wedge")
	}
}

// Detaching with a peer connection still up is a normal teardown
// transient, not the wedge signature — down must proceed.
func TestDownDetachingWithPeersStillDowns(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Detaching"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	if err := d.Down(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.calledWith(t, "drbdsetup down pvc-1")
}

// Restart drops and re-registers the kernel state — the only lever that
// clears a filesystem freeze leaked onto an unmounted device (issue #311)
// — while the rendered config and minor assignment stay for the up.
func TestRestartDownsAndUps(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"` + DiskUpToDate + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	if err := d.Restart(t.Context(), volPvc1); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.calledWith(t, "drbdsetup down pvc-1")
	fe.calledWith(t, "drbdadm up pvc-1")
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("rendered config must survive a restart: %v", err)
	}
}

// Restart's disconnect-then-down must never interleave with Apply's
// adjust: the disconnects' own state-change events wake the reconciler,
// and an adjust reconnecting a peer the down is tearing down strands
// drbdsetup down in del_connection forever (issue #582). Apply parks on
// the resource lock for the whole restart, so its adjust lands on the
// re-registered resource, after the up. synctest.Wait observes the
// parked Apply deterministically: it returns once every goroutine in
// the bubble is durably blocked, the restart on the exec gate and Apply
// on the lock.
func TestRestartSerializesApplyAdjust(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := testResource(nodeA)
		ge := &gatedExec{
			parked:  make(chan struct{}),
			release: make(chan struct{}),
			status: `[{"name":"` + r.Name + `","role":"Secondary",
			"devices":[{"disk-state":"` + DiskUpToDate + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
		}
		d := &Driver{StateDir: t.TempDir(), Exec: ge.run, Mknod: fakeMknod}
		if err := os.WriteFile(d.path(r.Name+".res"), nil, 0o640); err != nil {
			t.Fatal(err)
		}

		// Released on every exit: a failed assertion must not leave the
		// restart parked on the gate, which would deadlock the bubble.
		var once sync.Once
		release := func() { once.Do(func() { close(ge.release) }) }
		defer release()

		restartDone := make(chan error, 1)
		go func() { restartDone <- d.Restart(t.Context(), r.Name) }()
		<-ge.parked

		applyDone := make(chan error, 1)
		go func() { applyDone <- d.Apply(t.Context(), r) }()
		synctest.Wait()
		select {
		case err := <-applyDone:
			t.Fatalf("Apply returned (%v) while the teardown was still in flight", err)
		default:
		}
		if calls := ge.snapshot(); indexOf(calls, "adjust") >= 0 {
			t.Fatalf("adjust ran while the teardown was in flight: %v", calls)
		}

		release()
		if err := <-restartDone; err != nil {
			t.Fatalf("Restart: %v", err)
		}
		if err := <-applyDone; err != nil {
			t.Fatalf("Apply: %v", err)
		}
		calls := ge.snapshot()
		down, up, adjust := indexOf(calls, "drbdsetup down "+r.Name), indexOf(calls, "drbdadm up "+r.Name), indexOf(calls, "adjust "+r.Name)
		if down < 0 || up < down || adjust < up {
			t.Fatalf("want disconnect, down, up, then adjust; got %v", calls)
		}
	})
}

// Once the sweep's budget is spent, no resource spawns another command:
// the runner would start it before noticing and then wait out its kill
// grace. The slot is handed back, so the final sweep still gets it.
func TestDownSecondariesStopsSpawningOnceCancelled(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"pvc-1","role":"Secondary",
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := d.downSecondary(ctx, volPvc1); !errors.Is(err, context.Canceled) {
		t.Fatalf("downSecondary with a spent budget = %v, want context.Canceled", err)
	}
	if len(fe.calls) != 0 {
		t.Fatalf("no command may spawn after cancellation, got %v", fe.calls)
	}
	unlock, ok := d.tryLockResource(volPvc1)
	if !ok {
		t.Fatal("slot leaked by the cancelled sweep")
	}
	unlock()
}

// The shutdown sweep runs every resource under one deadline, so a
// resource another operation holds is skipped as ErrBusy, never waited
// for: the rest of the node's backings still get released.
func TestDownSecondariesSkipsHeldResource(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]},
			{"name":"pvc-2","role":"Secondary","connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	unlock, err := d.lockResource(t.Context(), volPvc1)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	err = d.DownSecondaries(t.Context())
	if !errors.Is(err, backend.ErrBusy) || !strings.Contains(err.Error(), volPvc1) {
		t.Fatalf("want the held resource reported as ErrBusy, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down pvc-1")
	fe.calledWith(t, "drbdsetup disconnect pvc-2 1")
	fe.calledWith(t, "drbdsetup down pvc-2")
}

// What the sweep listed can be stale by the time it holds a resource's
// lock: an apply that ran meanwhile may have added a peer, and a down
// meeting that peer's resync still connected is the hang disconnectPeers
// exists for. The peers to disconnect come from the re-read under the
// lock, never from the listing.
func TestDownSecondariesDisconnectsPeersReadUnderLock(t *testing.T) {
	listed := `[{"name":"pvc-1","role":"Secondary",
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fresh := `[{"name":"pvc-1","role":"Secondary",
		"connections":[{"peer-node-id":1,"connection-state":"Connected"},
			{"peer-node-id":2,"connection-state":"Connected"}]}]`
	fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: fresh}}
	// The listing is the stale one; every later status read is fresh.
	d := &Driver{StateDir: t.TempDir(), Mknod: fakeMknod}
	first := true
	d.Exec = func(ctx context.Context, name string, args ...string) (string, error) {
		out, err := fe.run(ctx, name, args...)
		if first && strings.HasPrefix(name+" "+strings.Join(args, " "), cmdDrbdsetupStatus) {
			first = false
			return listed, nil
		}
		return out, err
	}

	if err := d.DownSecondaries(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup disconnect pvc-1 2")
	fe.calledWith(t, "drbdsetup down pvc-1")
}

// A drbdsetup down of ours still stranded in the kernel is a wedge
// fingerprint the status cannot show: the resource reads healthy while its
// DOWN_IN_PROGRESS flag is pinned. Down must not spawn another down against
// it, escalating to ErrDownStranded (an ErrWedged) on the second sighting,
// and must resume once the stranded child drains.
func TestDownStrandedDownSkipsDown(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"UpToDate"}],"connections":[]}]`,
	}}
	w := &fakeWedge{stuck: map[string]bool{"drbdsetup down " + volPvc1: true}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod, Wedge: w}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	err := d.Down(t.Context(), volPvc1)
	if !errors.Is(err, backend.ErrBusy) || errors.Is(err, ErrWedged) {
		t.Fatalf("first sighting must defer without escalating, got %v", err)
	}
	err = d.Down(t.Context(), volPvc1)
	if !errors.Is(err, ErrDownStranded) || !errors.Is(err, ErrWedged) {
		t.Fatalf("second consecutive sighting must return ErrDownStranded wrapping ErrWedged, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down")
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("wedged resource's config must remain: %v", err)
	}

	// The child drained: the fingerprint is gone and the down proceeds.
	delete(w.stuck, "drbdsetup down "+volPvc1)
	if err := d.Down(t.Context(), volPvc1); err != nil {
		t.Fatalf("down after the stranded child drained: %v", err)
	}
	fe.calledWith(t, "drbdsetup down "+volPvc1)
}

// The stranded-down fingerprint is keyed on the exact resource: a stuck
// down for another resource must not park this one.
func TestDownIgnoresStrandedDownOfOtherResource(t *testing.T) {
	fe := &fakeExec{}
	w := &fakeWedge{stuck: map[string]bool{"drbdsetup down " + volPvc1 + "-other": true}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod, Wedge: w}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := d.Down(t.Context(), volPvc1); err != nil {
		t.Fatalf("down: %v", err)
	}
	fe.calledWith(t, "drbdsetup down "+volPvc1)
}

// A stranded down is a wedge fingerprint for Restart too: the frozen-bdev
// recovery is the path that spawned the stuck down in the first place, and
// a second one can only strand another unkillable process.
func TestRestartRefusesStrandedDown(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"UpToDate"}],"connections":[]}]`,
	}}
	w := &fakeWedge{stuck: map[string]bool{"drbdsetup down " + volPvc1: true}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod, Wedge: w}

	if err := d.Restart(t.Context(), volPvc1); !errors.Is(err, ErrDownStranded) {
		t.Fatalf("want ErrDownStranded, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down")
	fe.notCalledWith(t, "drbdadm up")
}

// The shutdown and orphan sweeps classify from their own status parse; the
// stranded-down fingerprint must reach them the same way the signature does.
func TestDownSecondariesSkipsStrandedDown(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"UpToDate"}],"connections":[]}]`,
	}}
	w := &fakeWedge{stuck: map[string]bool{"drbdsetup down " + volPvc1: true}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod, Wedge: w}

	if err := d.DownSecondaries(t.Context()); !errors.Is(err, ErrDownStranded) {
		t.Fatalf("want the stranded resource surfaced as ErrDownStranded, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down")
}

// A resource wearing the teardown wedge signature must be refused: a down
// spawned against it can only hang and strand an unkillable process.
func TestRestartRefusesWedged(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Detaching"}],"connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Restart(t.Context(), volPvc1); err == nil {
		t.Fatal("a wedged resource must refuse restart")
	}
	fe.notCalledWith(t, "drbdsetup down")
	fe.notCalledWith(t, "drbdadm up")
}

// A reclaim leftover — an assignment with no CR, no kernel resource, no
// rendered config — must be released by the startup sweep after the
// reboot cleared the zombie; while the zombie still lives in the kernel
// the reservation must survive, or a new volume could be handed a minor
// it can never register.
func TestSweepOrphansReleasesReclaimLeftoverAssignments(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: `[]`}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if _, err := d.AllocateMinor(t.Context(), "pvc-zombie", 0); err != nil {
		t.Fatal(err)
	}

	if err := d.SweepOrphans(t.Context(), func(string) bool { return false }); err != nil {
		t.Fatal(err)
	}
	assigned, err := d.readAssignments()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := assigned["pvc-zombie"]; ok {
		t.Fatalf("post-reboot sweep must release the leftover reservation: %v", assigned)
	}
}

func TestSweepOrphansKeepsAssignmentWhileZombieLives(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{cmdDrbdsetupStatus: `[
		{"name":"pvc-zombie","role":"Secondary","devices":[{"disk-state":"Diskless"}],"connections":[]}]`}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if _, err := d.AllocateMinor(t.Context(), "pvc-zombie", 0); err != nil {
		t.Fatal(err)
	}

	// The sweep tries (and here fails to matter) to down the unowned
	// kernel zombie; the reservation must survive regardless.
	_ = d.SweepOrphans(t.Context(), func(string) bool { return false })
	assigned, err := d.readAssignments()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := assigned["pvc-zombie"]; !ok {
		t.Fatalf("reservation must survive while the kernel holds the zombie: %v", assigned)
	}
}

// ForceDetach disconnects the peers, force-detaches the backing by minor,
// then removes the peer definitions. It is the escape hatch for a deletion
// whose down is permanently refused by an orphaned opener (issue #319).
// No down is spawned and the rendered config survives: the caller keeps
// the zombie minor reserved
// until the post-reboot orphan sweep reaps it.
func TestForceDetachDisconnectsAndDetaches(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"` + DiskUpToDate + `"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	if err := d.ForceDetach(t.Context(), volPvc1, 1422); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.calledWith(t, "drbdsetup detach 1422 --force")
	fe.calledWith(t, "drbdsetup del-peer pvc-1 1")
	fe.notCalledWith(t, "drbdsetup down")
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("rendered config must survive a force-detach: %v", err)
	}
}

// An already-detached (or vanished) resource is a no-op, so a retry after
// a failed backing delete converges instead of erroring on the re-detach.
func TestForceDetachAlreadyDisklessNoop(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"Diskless"}],"connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.ForceDetach(t.Context(), volPvc1, 1422); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "drbdsetup detach")

	fe.responses[cmdDrbdsetupStatus] = `[]`
	if err := d.ForceDetach(t.Context(), volPvc1, 1422); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "drbdsetup detach")
}

// A retry after the backing was detached must still remove connections
// that reserve the deleted volume's endpoint in the kernel.
func TestForceDetachDisklessRemovesPeers(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Primary",
			"devices":[{"disk-state":"Diskless"}],
			"connections":[{"peer-node-id":1,"connection-state":"StandAlone"}]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.ForceDetach(t.Context(), volPvc1, 1422); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdsetup disconnect pvc-1 1")
	fe.calledWith(t, "drbdsetup del-peer pvc-1 1")
	fe.notCalledWith(t, "drbdsetup detach")
}

// A resource wearing the teardown wedge signature must be refused: a
// detach is already in flight in the kernel and racing it is exactly what
// the wedge containment exists to prevent.
func TestForceDetachRefusesWedged(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary",
			"devices":[{"disk-state":"Detaching"}],"connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.ForceDetach(t.Context(), volPvc1, 1422); !errors.Is(err, ErrWedged) {
		t.Fatalf("want ErrWedged, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup detach")
}

// A failed down must not be followed by an up: the resource is still
// registered, and the error is the caller's retry signal.
func TestRestartSkipsUpWhenDownFails(t *testing.T) {
	fe := &fakeExec{
		responses: map[string]string{
			cmdDrbdsetupStatus: `[{"name":"` + volPvc1 + `","role":"Secondary","connections":[]}]`,
		},
		errOn: map[string]error{"down pvc-1": errors.New("Device is held open by someone")},
	}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	if err := d.Restart(t.Context(), volPvc1); err == nil {
		t.Fatal("a failed down must surface")
	}
	fe.notCalledWith(t, "drbdadm up")
}

// A wedged orphan must be skipped from the sweep's own status parse —
// spawning a down against it can only hang 30s and strand another
// unkillable process per agent boot.
func TestSweepOrphansSkipsWedgedWithoutDown(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Secondary","devices":[{"disk-state":"Detaching"}],"connections":[]},
			{"name":"pvc-2","role":"Secondary","connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	err := d.SweepOrphans(t.Context(), func(string) bool { return false })
	if !errors.Is(err, ErrWedged) {
		t.Fatalf("want the wedged orphan surfaced as ErrWedged, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down pvc-1")
	fe.calledWith(t, "drbdsetup down pvc-2")
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("wedged orphan's config must remain: %v", err)
	}
}

// An unreadable kernel view must abort the sweep before any file is
// removed: a live resource is then indistinguishable from a stale file,
// and stripping its config is the state the stuck-guard exists to prevent.
func TestSweepOrphansKeepsFilesWhenStatusUnreadable(t *testing.T) {
	fe := &fakeExec{errOn: map[string]error{
		cmdDrbdsetupStatus: errors.New("signal: killed"),
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	if err := d.SweepOrphans(t.Context(), func(string) bool { return false }); err == nil {
		t.Fatal("an unreadable status must surface an error")
	}
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("no file may be removed without the kernel view: %v", err)
	}
}

// DownSecondaries must not spawn a down against a wedged resource either:
// at shutdown it can only hang until the deadline and strand an
// unkillable process into the reboot.
func TestDownSecondariesSkipsWedged(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Secondary","devices":[{"disk-state":"Detaching"}],"connections":[]},
			{"name":"pvc-2","role":"Secondary","connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}

	err := d.DownSecondaries(t.Context())
	if !errors.Is(err, ErrWedged) {
		t.Fatalf("want the wedged resource surfaced as ErrWedged, got %v", err)
	}
	fe.notCalledWith(t, "drbdsetup down pvc-1")
	fe.calledWith(t, "drbdsetup down pvc-2")
}

func TestSweepOrphansSkipsOwned(t *testing.T) {
	fe := &fakeExec{responses: map[string]string{
		cmdDrbdsetupStatus: `[
			{"name":"pvc-1","role":"Secondary","connections":[]},
			{"name":"pvc-2","role":"Secondary","connections":[]}]`,
	}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	owned := func(name string) bool { return name == volPvc1 }
	if err := d.SweepOrphans(t.Context(), owned); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "drbdsetup down pvc-1")
	fe.calledWith(t, "drbdsetup down pvc-2")
	if _, err := os.Stat(d.path(volPvc1 + ".res")); err != nil {
		t.Fatalf("owned resource's config must remain: %v", err)
	}
}

// A down that cannot read the kernel's peer list must not run: skipping
// the disconnects would meet a live connection. Only a resource the kernel
// does not know is safe to down blind.
func TestTeardownNeedsReadableStatus(t *testing.T) {
	ops := map[string]func(*Driver) error{
		"down":    func(d *Driver) error { return d.Down(t.Context(), volPvc1) },
		"restart": func(d *Driver) error { return d.Restart(t.Context(), volPvc1) },
	}
	tests := []struct {
		name      string
		statusErr error
		wantErr   bool
	}{
		{name: "status times out", statusErr: errors.New("signal: killed"), wantErr: true},
		{name: "resource not in kernel", statusErr: errors.New("pvc-1: No such resource")},
	}
	for op, run := range ops {
		for _, tt := range tests {
			t.Run(op+"/"+tt.name, func(t *testing.T) {
				fe := &fakeExec{errOn: map[string]error{cmdDrbdsetupStatus: tt.statusErr}}
				d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
				if err := os.WriteFile(d.path(volPvc1+".res"), nil, 0o640); err != nil {
					t.Fatal(err)
				}
				err := run(d)
				if (err != nil) != tt.wantErr {
					t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
				}
				if tt.wantErr {
					fe.notCalledWith(t, "drbdsetup down")
				} else {
					fe.calledWith(t, "drbdsetup down pvc-1")
				}
			})
		}
	}
}
