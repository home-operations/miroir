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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"golang.org/x/sys/unix"

	"github.com/home-operations/miroir/internal/backend"
)

// Driver realizes DRBD resources on one node by rendering config into
// StateDir (bind-mounted from a hostPath so rendered state survives pod
// restarts) and driving drbdadm/drbdmeta against it.
type Driver struct {
	// StateDir is where .res files and marker files live; it is
	// drbdadm's include path (/etc/drbd.d) inside the agent container.
	StateDir string
	Exec     backend.Exec
	// Mknod creates device nodes; injectable because the real call needs
	// CAP_MKNOD. Nil means unix.Mknod.
	Mknod func(path string, mode uint32, dev int) error

	// wedgeSeen stamps resources whose teardown wore the wedge signature
	// once (see ErrWedged). Down escalates only on the second consecutive
	// sighting: a killed down whose detach is still draining wears the
	// same signature briefly, and a premature ErrWedged pages "reboot
	// required" for a state that resolves itself.
	wedgeMu   sync.Mutex
	wedgeSeen map[string]bool
}

// isMissingMetadata matches drbdadm's complaint when a backing device
// carries no DRBD metadata (attach against a blank/replaced disk).
func isMissingMetadata(err error) bool {
	s := err.Error()
	return strings.Contains(s, "no valid meta") || strings.Contains(s, "No valid meta")
}

// ensureMarkedMetadata creates metadata once per backing device, gated on
// the .md-created marker: the marker lands only after a fully successful
// create, so a retry never adopts a half-written attempt. The all-legs
// Inconsistent state this leaves behind is deliberate — the winner resolves
// it with the birth generation once every leg is connected (InitialUUID).
func (d *Driver) ensureMarkedMetadata(ctx context.Context, r Resource) error {
	marker := d.path(r.Name + ".md-created")
	if _, err := os.Stat(marker); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := d.ensureMetadata(ctx, r); err != nil {
			return err
		}
	}
	// A sentinel must never outlive a completed seed — left stale, it
	// would authorize re-seeding live metadata the moment the marker is
	// lost. Removed before the marker lands so a crash in between leaves
	// "no markers" (re-probed, re-seeded only if virgin).
	if err := os.Remove(d.path(r.Name + ".md-seeding")); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(marker, nil, 0o640)
}

// Apply converges the kernel state for one resource: write config when it
// changed, create metadata once (left at UUID_JUST_CREATED — see
// ensureMetadata for how the birth generation lands), bring the resource
// up / adjust.
func (d *Driver) Apply(ctx context.Context, r Resource) error {
	if err := d.writeConfig(r); err != nil {
		return err
	}

	// A diskless tie-breaker has no backing device — no metadata, no
	// device-node, no marker. It only needs the .res rendered (writeConfig
	// above) and drbdadm up/adjust so DRBD joins the resource for quorum.
	// A SkipDiskAttach leg is latched failed: leave its backing disk alone
	// (no create-md on a disk DRBD dropped on I/O error). The disk phase is
	// skipped below, so nothing reads or writes the failing device.
	if !r.LocalDiskless && !r.SkipDiskAttach {
		if err := d.ensureMarkedMetadata(ctx, r); err != nil {
			return err
		}
	}

	// adjust is idempotent: up when down, reconfigure on change, no-op
	// otherwise. A half-torn kernel slot can answer "Unknown resource"
	// to adjust but accept a plain up — fall back rather than loop.
	// --skip-disk reconciles net/connection state but leaves the disk
	// detached (drbdadm_adjust.c drops the ADJUST_DISK phase), so a latched
	// leg is not re-attached. It is always up (Diskless is an up-resource
	// state), so the Unknown-resource up fallback below is unreachable.
	adjustArgs := []string{"adjust", r.Name}
	if r.SkipDiskAttach {
		adjustArgs = []string{"adjust", "--skip-disk", r.Name}
	}
	if _, err := d.adm(ctx, adjustArgs...); err != nil {
		// A stale .md-created marker surviving a backing-disk replacement
		// (data disk swapped, /etc/drbd.d intact) makes ensureMarkedMetadata
		// skip create-md, so adjust attaches a blank device and fails "no
		// valid meta-data". Drop the marker and re-run: probeMetadata sees
		// no metadata and just-created metadata makes this a full SyncTarget.
		// Unreachable with --skip-disk (no attach → no metadata read).
		if !r.LocalDiskless && !r.SkipDiskAttach && isMissingMetadata(err) {
			if rmErr := os.Remove(d.path(r.Name + ".md-created")); rmErr != nil && !os.IsNotExist(rmErr) {
				return rmErr
			}
			if mdErr := d.ensureMarkedMetadata(ctx, r); mdErr != nil {
				return mdErr
			}
			if _, err = d.adm(ctx, "adjust", r.Name); err == nil {
				return d.ensureDeviceNode(r.Minor)
			}
		}
		if !strings.Contains(err.Error(), "Unknown resource") {
			return fmt.Errorf("adjust %s: %w", r.Name, err)
		}
		if _, err := d.adm(ctx, "up", r.Name); err != nil {
			return fmt.Errorf("up %s (after adjust fallback): %w", r.Name, err)
		}
	}

	// No udev runs on Talos, so DRBD's rules never create the device
	// node. Without it, the first open(2) would create a regular file
	// under /dev and mkfs would silently write into tmpfs.
	return d.ensureDeviceNode(r.Minor)
}

// ensureMetadata creates the backing metadata, surviving a crash at any
// point: the .md-seeding sentinel written before create-md proves on-disk
// metadata is our own unfinished attempt and safe to recreate. The on-disk
// probe stays the authority for "metadata exists" — markers are hostPath
// state that can vanish while the LV still holds a live GI, and a blind
// create-md --force would wipe it.
//
// Metadata is deliberately left at create-md's UUID_JUST_CREATED. A fresh
// volume's legs connect Inconsistent (unpromotable) and the winner then
// creates the birth generation over the live connections (InitialUUID) —
// one UUID replicated to every peer, so birth divergence is impossible. A
// leg joining an existing volume takes the same just-created metadata into
// its first handshake and elects full SyncTarget — the only correct
// outcome, since the peers' bitmaps never tracked writes against a replica
// that did not exist yet.
func (d *Driver) ensureMetadata(ctx context.Context, r Resource) error {
	hasMD, attached, dump, err := d.probeMetadata(ctx, r.Name)
	if err != nil {
		return err
	}
	if attached {
		// A previous life completed bring-up and adjust; the metadata is
		// live — never touch it.
		return d.markAdopted(r.Name)
	}
	sentinel := d.path(r.Name + ".md-seeding")
	_, serr := os.Stat(sentinel)
	if serr != nil && !os.IsNotExist(serr) {
		return serr
	}
	if hasMD && os.IsNotExist(serr) {
		// Metadata without any marker: lost hostPath state. Recreate only
		// when the GI proves no Primary ever wrote; anything else is a
		// live volume — adopt it untouched.
		if !virginMetadata(dump, r.Name) {
			return d.markAdopted(r.Name)
		}
	}
	if err := os.WriteFile(sentinel, nil, 0o640); err != nil {
		return err
	}
	if !hasMD {
		// --max-peers reserves metadata slots up-front: it cannot
		// be raised later without recreating metadata, so leave
		// headroom beyond the current 2-node shape.
		args := []string{"create-md", "--force", "--max-peers", strconv.Itoa(maxPeers)}
		if r.BitmapGranularityBytes > 0 {
			// Like max-peers, fixed for this leg's lifetime: adjust
			// never revisits it, only a metadata recreate could.
			args = append(args, "--bitmap-block-size",
				strconv.FormatInt(r.BitmapGranularityBytes, 10))
		}
		if _, err := d.adm(ctx, append(args, r.Name+"/0")...); err != nil {
			return fmt.Errorf("create-md %s: %w", r.Name, err)
		}
	}
	return nil
}

// maxPeers is the metadata peer-slot count create-md reserves up-front
// (it cannot be raised later without recreating metadata) and the input
// InternalMetaOverhead sizes bitmaps for.
const maxPeers = 7

// InternalMetaOverhead returns how many extra bytes a backing device
// needs beyond its nominal size so DRBD internal metadata (meta-disk
// internal, created with --max-peers) fits without the usable device
// sizing below nominal. Consumed by restores that cross the replication
// boundary (VolumeSource.PadForMetadata): an unreplicated source's
// filesystem spans the full nominal size, so the clone must grow by at
// least the metadata size before create-md, and every full-sync joiner
// must match or the DRBD device sizes to the smallest leg.
//
// Deliberately a generous upper bound rather than drbdmeta's exact
// arithmetic: one bitmap bit covers 4KiB per peer slot (the finest
// granularity DRBD accepts; a coarser BitmapGranularityBytes only
// shrinks it), rounded up per slot, plus slack for the superblock and
// activity log, rounded to 4MiB for backend extent alignment. The pad
// is virtual on every miroir backend (thin/sparse), so over-reserving
// costs nothing; the e2e restore spec verifies the bound against a real
// create-md.
func InternalMetaOverhead(sizeBytes int64) int64 {
	const block = 4096
	perPeer := (sizeBytes/32768 + block) &^ (block - 1) // 1 bit / 4KiB, per peer, rounded up
	overhead := perPeer*(maxPeers+1) + (8 << 20)
	const extent = 4 << 20
	return (overhead + extent - 1) &^ (extent - 1)
}

// probeMetadata reports the backing device's DRBD metadata state.
// attached means the kernel has the resource (which necessarily implies
// metadata exists); dump is the raw dump-md output when readable.
//
// Kernel state is probed first: an attached resource claims its backing
// device exclusively, so dump-md only reports EBUSY — the same error a
// foreign holder (stale mount, LVM) produces. Asking the kernel
// disambiguates: resource present → attached; absent → any remaining
// busy error is a foreign holder and surfaces.
func (d *Driver) probeMetadata(ctx context.Context, name string) (hasMD, attached bool, dump string, err error) {
	if out, err := d.Exec(ctx, "drbdsetup", "status", name); err == nil && strings.Contains(out, name) {
		return true, true, "", nil
	}
	out, err := d.adm(ctx, "dump-md", name+"/0")
	if err != nil && strings.Contains(err.Error(), "unclean") {
		// A snapshot of an attached volume captures its activity log
		// mid-flight; the kernel replays it on attach, but drbdmeta
		// refuses to read unclean metadata. Replay it the same way.
		if _, aerr := d.adm(ctx, "apply-al", name+"/0"); aerr != nil {
			return false, false, "", fmt.Errorf("apply-al %s: %w", name, aerr)
		}
		out, err = d.adm(ctx, "dump-md", name+"/0")
	}
	switch {
	case err == nil && strings.Contains(out, "might be stale"):
		// dump-md succeeded against an attached minor (possible when the
		// open is not exclusive), flagging "# Output might be stale".
		return true, true, "", nil
	case err == nil:
		return true, false, out, nil
	case strings.Contains(err.Error(), "is configured!"):
		// drbdmeta's own refusal — covers a resource attached between
		// the kernel probe and the dump.
		return true, true, "", nil
	case strings.Contains(err.Error(), "no valid meta data") ||
		strings.Contains(err.Error(), "No valid meta data"):
		return false, false, "", nil
	default:
		return false, false, "", fmt.Errorf("dump-md %s: %w", name, err)
	}
}

// markAdopted leaves a durable breadcrumb when metadata is taken over
// without local provenance (markers lost): a volume that adopted a
// deadlocked half-seed is otherwise indistinguishable from one in a
// normal first sync during triage.
func (d *Driver) markAdopted(name string) error {
	return os.WriteFile(d.path(name+".md-adopted"), nil, 0o640)
}

// MetadataAdopted reports whether this leg's metadata was adopted from a
// previous life (an attached resource, or a dump proving live data)
// rather than created fresh by this agent. The restore seed mint keys on
// it: adopted metadata carries a real generation and must never be
// re-seeded.
func (d *Driver) MetadataAdopted(name string) bool {
	_, err := os.Stat(d.path(name + ".md-adopted"))
	return err == nil
}

// justCreatedUUID is drbdmeta's constant current-uuid for metadata that
// was created but never promoted (UUID_JUST_CREATED).
const justCreatedUUID = "0000000000000004"

// virginMetadata reports whether a dump-md proves the volume never held
// data: the current UUID is still the day0 seed or create-md's
// just-created constant, and no slot carries a bitmap UUID (no
// divergence was ever tracked). Anything else — including unparseable
// output — counts as live: adopting a deadlocked volume is
// operator-recoverable, wiping a live one is not.
func virginMetadata(dump, name string) bool {
	current := ""
	for line := range strings.SplitSeq(dump, "\n") {
		fields := strings.Fields(strings.TrimSuffix(strings.TrimSpace(line), ";"))
		if len(fields) != 2 {
			continue
		}
		val := strings.TrimPrefix(strings.ToUpper(fields[1]), "0X")
		switch fields[0] {
		case "current-uuid":
			if current == "" {
				current = val
			}
		case "bitmap-uuid":
			if strings.Trim(val, "0") != "" {
				return false
			}
		}
	}
	return current == Day0GI(name) || current == justCreatedUUID
}

// stateSuffixes are the per-resource files in StateDir that teardown
// removes — keep in sync with everything Apply and ensureMetadata write.
var stateSuffixes = []string{".res", ".md-created", ".md-seeding", ".md-adopted"}

// removeStateFiles deletes the resource's rendered config and markers.
func (d *Driver) removeStateFiles(name string) error {
	for _, suffix := range stateSuffixes {
		if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// RemoveConfig removes the resource's rendered config and markers while
// leaving its minor.assign reservation in place. It exists for the
// orphaned-hold reclaim: the zombie kernel minor outlives its volume
// until a reboot, and a rendered config left behind holds the volume's
// DRBD port hostage — the controller re-allocates ports from CRs alone,
// and drbdadm refuses to parse a config directory whose resources share
// an endpoint, wedging the next replicated volume handed the recycled
// port on this node (caught by the e2e). The minor reservation must
// outlive the config (a fresh volume handed the zombie's minor could
// never register it); the startup orphan sweep releases it once a reboot
// clears the kernel state.
func (d *Driver) RemoveConfig(name string) error {
	return d.removeStateFiles(name)
}

// listStatus runs `drbdsetup status --json [name]` and parses the result —
// the one fetch every kernel-view consumer shares.
func (d *Driver) listStatus(ctx context.Context, name ...string) ([]jsonStatus, error) {
	args := append([]string{"status", "--json"}, name...)
	out, err := d.Exec(ctx, "drbdsetup", args...)
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	var parsed []jsonStatus
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	return parsed, nil
}

// SweepOrphans tears down kernel resources and rendered config files with
// no owning volume on this node — leftovers of an agent crash between up
// and down, which would otherwise hold backing devices open forever. Best
// effort per resource: errors are joined so one stuck resource cannot
// strand the rest. An unreadable kernel view aborts the whole sweep,
// files included — without it a live resource is indistinguishable from a
// stale file, and stripping its config is the state this sweep's own
// stuck-guard exists to prevent. The sweep runs once per agent start: an
// orphan skipped as wedged (or mid-detach, which wears the same
// signature) stays configured and is retried at the next restart.
func (d *Driver) SweepOrphans(ctx context.Context, owned func(name string) bool) error {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return err
	}
	var errs []error
	stuck := map[string]bool{}
	for _, res := range parsed {
		if owned(res.Name) {
			continue
		}
		// A wedged orphan's down can only hang 30s and strand another
		// unkillable process (issue #195); the signature is already in
		// hand from this sweep's own parse, so skip without spawning one.
		if res.wedgeSignature() {
			errs = append(errs, fmt.Errorf("orphan %s: %w", res.Name, ErrWedged))
			stuck[res.Name] = true
			continue
		}
		if err := d.downResource(ctx, res.Name, res.peerIDs()); err != nil {
			errs = append(errs, fmt.Errorf("down orphan %s: %w", res.Name, err))
			stuck[res.Name] = true
		}
	}
	entries, err := os.ReadDir(d.StateDir)
	if err != nil {
		if !os.IsNotExist(err) {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}
	for _, e := range entries {
		name, found := strings.CutSuffix(e.Name(), ".res")
		// A resource whose down failed is still configured in the kernel;
		// its rendered config stays visible to recovery tooling.
		if !found || owned(name) || stuck[name] {
			continue
		}
		if err := d.removeStateFiles(name); err != nil {
			errs = append(errs, err)
		}
		// The orphaned volume never comes back to Down on this node, so
		// its minor.assign entry would leak — and permanently burn a
		// minor — for the StateDir's lifetime.
		if err := d.releaseMinor(name); err != nil {
			errs = append(errs, fmt.Errorf("release minor of orphan %s: %w", name, err))
		}
	}
	// Assignment entries with no CR, no kernel resource, and no rendered
	// config are reclaim leftovers: RemoveConfig keeps the reservation
	// while the zombie minor lives, and after the reboot that cleared the
	// kernel they are pure leaks. Ordered after the file loop so a plain
	// config orphan is handled (and released) exactly once, above.
	inKernel := map[string]bool{}
	for _, res := range parsed {
		inKernel[res.Name] = true
	}
	if err := d.releaseOrphanedAssignments(owned, inKernel); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// releaseOrphanedAssignments drops minor.assign entries whose volume has
// vanished entirely — not owned, not in the kernel, no rendered config.
func (d *Driver) releaseOrphanedAssignments(owned func(name string) bool, inKernel map[string]bool) error {
	unlock, err := d.lockMinors()
	if err != nil {
		return err
	}
	defer unlock()
	assigned, err := d.readAssignments()
	if err != nil {
		return err
	}
	changed := false
	for name := range assigned {
		if owned(name) || inKernel[name] {
			continue
		}
		if _, err := os.Stat(d.path(name + ".res")); err == nil {
			continue
		}
		delete(assigned, name)
		changed = true
	}
	if !changed {
		return nil
	}
	return d.writeAssignments(assigned)
}

// DownSecondaries brings down every resource this node holds Secondary,
// releasing its backing device so the backend pool can export on shutdown.
// Primary (open) resources are skipped: drbdsetup down refuses an open
// device, and a local Primary is a leg a workload here still holds.
// Rendered config stays so the successor re-ups. Best effort: errors are
// joined so one stuck resource cannot strand the rest.
func (d *Driver) DownSecondaries(ctx context.Context) error {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, res := range parsed {
		if res.Role == rolePrimary {
			continue
		}
		// Never spawn a down against the wedge signature (issue #195):
		// it can only hang until the shutdown deadline and strand an
		// unkillable process into the reboot.
		if res.wedgeSignature() {
			errs = append(errs, fmt.Errorf("%s: %w", res.Name, ErrWedged))
			continue
		}
		if err := d.downResource(ctx, res.Name, res.peerIDs()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// drbdMajor is DRBD's fixed block-device major number.
const drbdMajor = 147

// minorBase is the first DRBD minor number the agent assigns to miroir
// volumes. Lower numbers may be reserved for system DRBD resources.
const minorBase int32 = 1000

// lockMinors serialises minor.assign access. flock, not an O_EXCL
// sentinel: a sentinel orphaned by a crash between create and remove would
// deadlock every future allocation (nothing sweeps minor.lock). LOCK_EX
// blocks until acquired and the kernel drops it when the fd closes —
// including on SIGKILL/OOM — so a crash cannot wedge allocation. The file
// is intentionally left on disk as the stable lock target.
func (d *Driver) lockMinors() (func(), error) {
	f, err := os.OpenFile(d.path("minor.lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open minor lock: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock minor allocation: %w", err)
	}
	return func() { _ = f.Close() }, nil
}

// releaseMinor drops a volume's minor.assign entry so the minor can be
// reused. Called on teardown; without it the map grows for the lifetime of
// the StateDir. Absent entry (unreplicated volume, or already released) is
// a no-op.
func (d *Driver) releaseMinor(volume string) error {
	unlock, err := d.lockMinors()
	if err != nil {
		return err
	}
	defer unlock()
	assigned, err := d.readAssignments()
	if err != nil {
		return err
	}
	if _, ok := assigned[volume]; !ok {
		return nil
	}
	delete(assigned, volume)
	return d.writeAssignments(assigned)
}

// AllocateMinor assigns a DRBD minor to a volume, returning the same
// minor on future calls. Serialised by an advisory flock the kernel
// releases on close or process death, then persisted via atomic rename.
func (d *Driver) AllocateMinor(volume string) (int32, error) {
	unlock, err := d.lockMinors()
	if err != nil {
		return 0, err
	}
	defer unlock()

	assigned, err := d.readAssignments()
	if err != nil {
		return 0, err
	}
	if m, ok := assigned[volume]; ok {
		return m, nil
	}
	// A .res without an assignment record is our own partially-recorded
	// state (crash between render and record, or a lost minor.assign):
	// recover the volume's minor from its own file instead of assigning a
	// fresh one — the kernel resource may still be running on it.
	if m, ok := d.minorFromOwnRes(volume); ok {
		assigned[volume] = m
		if err := d.writeAssignments(assigned); err != nil {
			return 0, err
		}
		return m, nil
	}
	used := d.scanUsedMinors(assigned)
	m := minorBase
	for used[m] {
		m++
	}
	assigned[volume] = m
	if err := d.writeAssignments(assigned); err != nil {
		return 0, err
	}
	return m, nil
}

// minorsInRes parses the device minors out of a rendered .res: miroir's
// own form ("device minor N;") and the classic named form
// ("device drbdX minor N;").
func minorsInRes(raw []byte) []int32 {
	var minors []int32
	for line := range strings.SplitSeq(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) >= 3 && f[0] == "device" && f[len(f)-2] == "minor" {
			if n, err := strconv.Atoi(strings.TrimSuffix(f[len(f)-1], ";")); err == nil {
				minors = append(minors, int32(n)) //nolint:gosec // drbd minors are small
			}
		}
	}
	return minors
}

// minorFromOwnRes recovers the minor a previous render of this volume
// already claimed, if its .res survives.
func (d *Driver) minorFromOwnRes(volume string) (int32, bool) {
	raw, err := os.ReadFile(d.path(volume + ".res"))
	if err != nil {
		return 0, false
	}
	if minors := minorsInRes(raw); len(minors) > 0 {
		return minors[0], true
	}
	return 0, false
}

func (d *Driver) readAssignments() (map[string]int32, error) {
	raw, err := os.ReadFile(d.path("minor.assign"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]int32{}, nil
		}
		return nil, err
	}
	m := map[string]int32{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(val)
		if err == nil {
			m[name] = int32(n)
		}
	}
	return m, nil
}

func (d *Driver) writeAssignments(m map[string]int32) error {
	var buf bytes.Buffer
	for name, minor := range m {
		fmt.Fprintf(&buf, "%s %d\n", name, minor)
	}
	return writeFileAtomic(d.path("minor.assign"), buf.Bytes(), 0o640)
}

func (d *Driver) scanUsedMinors(assigned map[string]int32) map[int32]bool {
	used := map[int32]bool{}
	for _, m := range assigned {
		used[m] = true
	}
	entries, err := os.ReadDir(d.StateDir)
	if err != nil {
		return used
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".res") {
			continue
		}
		raw, err := os.ReadFile(d.path(e.Name()))
		if err != nil {
			continue
		}
		for _, m := range minorsInRes(raw) {
			used[m] = true
		}
	}
	return used
}

// ensureDeviceNode creates /dev/drbd<minor> if absent (mknod; the agent
// is privileged). Idempotent; refuses to touch a path that exists but is
// not the expected block device.
func (d *Driver) ensureDeviceNode(minor int32) error {
	path := DevicePath(minor)
	want := unix.Mkdev(drbdMajor, uint32(minor)) //nolint:gosec // minor is a small allocator value
	var st unix.Stat_t
	err := unix.Stat(path, &st)
	if err == nil {
		// unix.Stat_t.Rdev is uint64 on linux but int32 on darwin; the cast is a
		// no-op on linux (hence the nolint) but required for the package to build
		// on a macOS dev host.
		if st.Mode&unix.S_IFMT == unix.S_IFBLK && uint64(st.Rdev) == want { //nolint:unconvert
			return nil
		}
		// Wrong rdev or a leftover regular file (e.g. a pre-mknod open
		// landed in tmpfs): replace it.
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale %s: %w", path, err)
		}
	} else if err != unix.ENOENT {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	mknod := d.Mknod
	if mknod == nil {
		mknod = unix.Mknod
	}
	if err := mknod(path, unix.S_IFBLK|0o660, int(want)); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mknod %s: %w", path, err)
	}
	// Enforce perms regardless of umask.
	if err := os.Chmod(path, 0o660); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}

// Day0GI derives the deterministic "day 0" generation identifier all
// replicas of a fresh volume stamp into their metadata: 16 hex chars (the
// first 8 bytes of sha256), low bit cleared — DRBD's low bit marks
// "primary writes happened" and a synthetic day0 means none have.
func Day0GI(name string) string {
	h := sha256.Sum256(fmt.Appendf(nil, "miroir-day0:%s/0", name))
	h[7] &^= 0x01
	return strings.ToUpper(hex.EncodeToString(h[:8]))
}

// IsWinner picks the birth-generation winner deterministically: the lowest
// node id among diskful peers. A diskless tie-breaker never holds data, so
// it must never win. Node ids are controller-assigned and stable, and every
// agent reads the same spec, so all nodes elect the same single winner
// without coordinating.
func IsWinner(r Resource) bool {
	lowest := int32(-1)
	var lowestNode string
	for _, p := range r.Peers {
		if p.Diskless {
			continue
		}
		if lowest == -1 || p.NodeID < lowest {
			lowest = p.NodeID
			lowestNode = p.Node
		}
	}
	return lowestNode == r.LocalNode
}

// InitialUUID creates a fresh volume's first data generation — DRBD's
// documented skip-initial-sync: with every leg attached Inconsistent at
// UUID_JUST_CREATED and all connections established, new-current-uuid
// --clear-bitmap mints one UUID and replicates it to every peer, flipping
// all legs UpToDate with zero resync. Valid because thin backings read
// zeros. Run on the winner (IsWinner) only, and only while the volume has
// never held data — the caller gates on both (see agent birthInitPending).
// One replicated UUID cannot birth-diverge, unlike per-node manufactured
// generations (issue #139).
func (d *Driver) InitialUUID(ctx context.Context, name string) error {
	if _, err := d.adm(ctx, "new-current-uuid", "--clear-bitmap", name+"/0"); err != nil {
		return fmt.Errorf("new-current-uuid %s: %w", name, err)
	}
	return nil
}

// ForceFullSyncSource declares this leg's data the volume's generation —
// DRBD's documented bootstrap for a node that has the data while its
// peers do not: primary --force flips the Inconsistent disk UpToDate and
// mints a current UUID with full bitmaps, every just-created peer elects
// full SyncTarget, and the immediate demote hands the device back to
// auto-promote. Not new-current-uuid: without --clear-bitmap (and with
// --force-resync alike) drbdsetup refuses it on a connected resource
// ("(151) Need to be StandAlone"), and even minted StandAlone it leaves
// the disk Inconsistent with the resync parked on the source's own state
// (PausedSyncS, resync-suspended:dependency) — verified against DRBD 9.3
// on the QEMU e2e. It exists for one consumer: the seed leg of a restore
// that crossed the replication boundary (an unreplicated source's clone)
// holds real data under freshly created metadata, so neither the birth
// flow (data must not be declared identical to blank peers) nor metadata
// adoption (there was none to adopt) can produce its generation. The
// caller gates on the metadata being this agent's own fresh create (see
// agent restoreSeedPending) — forcing a leg with a live generation would
// fork history.
func (d *Driver) ForceFullSyncSource(ctx context.Context, name string) error {
	if _, err := d.adm(ctx, "primary", "--force", name); err != nil {
		return fmt.Errorf("primary --force %s: %w", name, err)
	}
	return d.Demote(ctx, name)
}

// Demote releases an explicitly held Primary role back to auto-promote.
// Used by ForceFullSyncSource and by its crash recovery (a mint whose
// demote never ran); a device a consumer holds open refuses harmlessly.
func (d *Driver) Demote(ctx context.Context, name string) error {
	if _, err := d.adm(ctx, "secondary", name); err != nil {
		return fmt.Errorf("secondary %s: %w", name, err)
	}
	return nil
}

// ResolveSplitBrain runs DRBD's documented split-brain recovery around the
// winner: every leg disconnects, then the winner (lowest diskful node id)
// and the diskless tie-breaker reconnect as survivors while every other
// diskful leg reconnects with --discard-my-data, becoming SyncTarget.
// Because the winner is derived the same way on every node, the agents
// converge without coordinating.
//
// --discard-my-data drops the loser leg's generation, so this is only valid
// on a volume that never held data; the caller gates on that (see
// VolumeReconciler.recoverSplitBrain).
func (d *Driver) ResolveSplitBrain(ctx context.Context, r Resource) error {
	// Disconnect first, on every role. A plain connect aborts (exit 10,
	// "Device has a net-config") on any peer that already has one, and after a
	// split-brain the local node holds a net-config toward at least one peer
	// (StandAlone or Connecting); --discard-my-data is likewise only honored on
	// a connect out of StandAlone. Clearing all connections first makes the
	// connect below well-defined. A resource with nothing to disconnect errors
	// here harmlessly, so the result is ignored.
	_, _ = d.adm(ctx, "disconnect", r.Name)

	if r.LocalDiskless || IsWinner(r) {
		if _, err := d.adm(ctx, "connect", r.Name); err != nil {
			return fmt.Errorf("connect %s: %w", r.Name, err)
		}
		return nil
	}
	if _, err := d.adm(ctx, "connect", "--discard-my-data", r.Name); err != nil {
		return fmt.Errorf("connect --discard-my-data %s: %w", r.Name, err)
	}
	return nil
}

// WipeMetadata destroys the DRBD metadata on a backing device so it cannot
// carry a stale generation identifier into a reuse. The resource must be
// down first — drbdmeta refuses an attached device. Callers treat it as
// best-effort: teardown's Backend.Delete destroys the whole device anyway.
func (d *Driver) WipeMetadata(ctx context.Context, name, disk string, minor int32) error {
	// drbdmeta wants the minor as its DEVICE handle — fed a name the shipped
	// utils derive minor -1 and probe it with a malformed drbdsetup call
	// whose output is version-dependent (observed flaking per-invocation on
	// real hardware). The resource is down, so the minor probes as
	// unconfigured and wipe-md proceeds.
	if _, err := d.Exec(ctx, "drbdmeta", "--force", strconv.Itoa(int(minor)),
		"v09", disk, "internal", "wipe-md"); err != nil {
		return fmt.Errorf("wipe-md %s: %w", name, err)
	}
	return nil
}

// downTimeout tightens RealExec's 2-minute bound for drbdsetup down. A
// device wedged mid-teardown (stuck Detaching) leaves down in D-state, and
// teardown retries every ~10s — a 2-minute pin per attempt head-of-line-
// blocks the reconcile worker. 30s is generous: down on healthy hardware
// completes in milliseconds. The bound frees the worker, not the device —
// a D-state child ignores the SIGKILL and keeps its holds.
const downTimeout = 30 * time.Second

// disconnectTimeout bounds each best-effort disconnect in downResource,
// which otherwise rides RealExec's 2-minute bound — per peer, per teardown
// attempt — when the kernel resource is wedged. A healthy disconnect
// completes in milliseconds.
const disconnectTimeout = 10 * time.Second

// ErrWedged marks a resource the kernel can no longer tear down: the
// device is stuck Detaching with nothing left to disconnect, so drbdsetup
// down blocks in uninterruptible sleep and the next attempt hits the same
// wall (refcount underflow, LINBIT/drbd#137). Callers must leave the fast
// retry loop — every extra down attempt can strand another unkillable
// process — and surface that only a node reboot clears it. Down escalates
// to this only on the second consecutive sighting of the signature, so a
// detach still draining gets one retry cycle to finish first.
var ErrWedged = errors.New("resource wedged in kernel (LINBIT/drbd#137): node reboot required")

// disconnectPeers disconnects each peer connection ahead of a down or a
// force-detach. Disconnecting first avoids a kernel deadlock: drbdsetup
// down on a resource with an active resync connection can enter
// uninterruptible sleep negotiating teardown with the peer. Disconnects
// are best-effort (a StandAlone or unconfigured connection errors
// harmlessly) — except when one is killed at its deadline: then the
// connection may be half-torn and acting on the device would race exactly
// that teardown, so the caller's retry (which disconnects again) takes
// over. drbdsetup needs no config and takes peers by node id.
func (d *Driver) disconnectPeers(ctx context.Context, name string, peerIDs []int32) error {
	for _, id := range peerIDs {
		dcCtx, cancel := context.WithTimeout(ctx, disconnectTimeout)
		_, _ = d.Exec(dcCtx, "drbdsetup", "disconnect", name, strconv.Itoa(int(id)))
		expired := dcCtx.Err() != nil
		cancel()
		if expired {
			// ErrBusy: the caller's short retry re-disconnects; the state
			// clears on its own once the kernel finishes the teardown.
			return fmt.Errorf("disconnect %s peer %d killed after %s; deferring teardown: %w",
				name, id, disconnectTimeout, backend.ErrBusy)
		}
	}
	return nil
}

// downResource disconnects each peer connection, then downs the resource.
// drbdsetup down exits 0 for unknown names, keeping teardown idempotent.
// drbdsetup, not drbdadm: drbdadm refuses conflicting .res files, and
// teardown is how a conflict gets removed.
func (d *Driver) downResource(ctx context.Context, name string, peerIDs []int32) error {
	if err := d.disconnectPeers(ctx, name, peerIDs); err != nil {
		return err
	}
	downCtx, cancel := context.WithTimeout(ctx, downTimeout)
	defer cancel()
	if _, err := d.Exec(downCtx, "drbdsetup", "down", name); err != nil {
		return fmt.Errorf("down %s: %w", name, err)
	}
	return nil
}

// liveTeardownView reports the resource's DRBD peer node ids per the
// kernel's live view — the ids downResource must disconnect — and whether
// the resource wears the wedge signature (see jsonStatus.wedgeSignature).
// Empty/false when the resource is down or the status is unreadable
// (nothing to disconnect).
func (d *Driver) liveTeardownView(ctx context.Context, name string) (peerIDs []int32, wedged bool) {
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		return nil, false
	}
	for _, res := range parsed {
		if res.Name != name {
			continue
		}
		peerIDs = append(peerIDs, res.peerIDs()...)
		if res.wedgeSignature() {
			wedged = true
		}
	}
	return peerIDs, wedged
}

// markWedgeSighting records one sighting of the wedge signature and
// reports whether an earlier consecutive sighting was already on record.
func (d *Driver) markWedgeSighting(name string) (again bool) {
	d.wedgeMu.Lock()
	defer d.wedgeMu.Unlock()
	if d.wedgeSeen == nil {
		d.wedgeSeen = map[string]bool{}
	}
	again = d.wedgeSeen[name]
	d.wedgeSeen[name] = true
	return again
}

func (d *Driver) clearWedgeSighting(name string) {
	d.wedgeMu.Lock()
	delete(d.wedgeSeen, name)
	d.wedgeMu.Unlock()
}

// Down stops the resource and removes its rendered state. Idempotent.
// A resource wearing the wedge signature never gets another down spawned:
// the first sighting defers (the detach may still be draining), a second
// consecutive one returns ErrWedged (wrapped).
func (d *Driver) Down(ctx context.Context, name string) error {
	if _, err := os.Stat(d.path(name + ".res")); os.IsNotExist(err) {
		return nil // never configured here
	}
	peerIDs, wedged := d.liveTeardownView(ctx, name)
	if wedged {
		if d.markWedgeSighting(name) {
			return fmt.Errorf("down %s: %w", name, ErrWedged)
		}
		// ErrBusy: one short retry cycle for a drain to finish before the
		// signature escalates.
		return fmt.Errorf("down %s: detach in flight after a killed down; deferring: %w",
			name, backend.ErrBusy)
	}
	d.clearWedgeSighting(name)
	if err := d.downResource(ctx, name, peerIDs); err != nil {
		return err
	}
	if err := d.removeStateFiles(name); err != nil {
		return err
	}
	// Free the minor for reuse — the .res is gone, so scanUsedMinors no
	// longer covers it, and the assignment would otherwise leak forever.
	return d.releaseMinor(name)
}

// Restart drops and re-registers the resource's kernel state (down + up),
// keeping the rendered config and the minor assignment. It exists for one
// consumer: a filesystem freeze leaked onto an unmounted device pins the
// bdev's freeze count with no mountpoint left for FITHAW, and dropping the
// minor's bdev inode is the only remaining way to clear it (issue #311).
// The disconnect costs a brief bitmap-based resync on reconnect. A
// resource wearing the teardown wedge signature is refused outright — a
// down spawned against it can only hang (see ErrWedged); the caller's
// retry re-evaluates.
func (d *Driver) Restart(ctx context.Context, name string) error {
	peerIDs, wedged := d.liveTeardownView(ctx, name)
	if wedged {
		return fmt.Errorf("restart %s: teardown wedge signature; refusing to spawn a down", name)
	}
	if err := d.downResource(ctx, name, peerIDs); err != nil {
		return err
	}
	if _, err := d.adm(ctx, "up", name); err != nil {
		return fmt.Errorf("up %s: %w", name, err)
	}
	return nil
}

// ForceDetach releases the backing device from a resource whose down is
// permanently refused by an orphaned opener — a leaked freeze's dead
// superblock pinning open_cnt with no mountpoint left to thaw (issue
// #319). Down can never win that state, but force-detach is legal on an
// open device (dropping a failing disk under live I/O is its purpose),
// and the backing is all a deletion still needs back. The caller must
// keep the minor assignment (see RemoveConfig): the kernel minor stays
// pinned until a reboot, releasing the number would hand it to a new
// volume, and the startup orphan sweep reaps it once the reboot clears
// the state. Already-detached (Diskless) and not-in-kernel are
// no-ops so a retry after a failed backing delete converges. A resource
// wearing the teardown wedge signature is refused like everywhere else —
// mid-detach is exactly the state a second detach would race.
func (d *Driver) ForceDetach(ctx context.Context, name string, minor int32) error {
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		return err
	}
	for _, res := range parsed {
		if res.Name != name {
			continue
		}
		if res.wedgeSignature() {
			return fmt.Errorf("force-detach %s: %w", name, ErrWedged)
		}
		if len(res.Devices) == 0 || res.Devices[0].DiskState == DiskDiskless {
			return nil
		}
		if err := d.disconnectPeers(ctx, name, res.peerIDs()); err != nil {
			return err
		}
		if _, err := d.Exec(ctx, "drbdsetup", "detach", strconv.Itoa(int(minor)), "--force"); err != nil {
			return fmt.Errorf("force-detach %s: %w", name, err)
		}
		return nil
	}
	return nil
}

// DiskUpToDate is the disk state of a replica holding current data.
const DiskUpToDate = "UpToDate"

// DiskInconsistent is the disk state of a replica with no usable data
// generation yet: freshly created metadata before the birth generation, or
// a sync target mid-resync. Never promotable.
const DiskInconsistent = "Inconsistent"

// DiskDiskless is the disk state of a replica with no attached backing
// device — a quorum-only tie-breaker, or a diskful leg DRBD detached
// after an I/O error (on-io-error detach). Do NOT use it to infer
// tie-breaker-ness (that is spec-driven, see Replica.Diskless); it is
// only meaningful for observing a detach on a leg the spec says is diskful.
const DiskDiskless = "Diskless"

// diskDetaching is the transient disk state while the kernel drains a
// detach. Seen at teardown entry (with the connections already gone) it
// means a previous down was killed mid-flight — see ErrWedged.
const diskDetaching = "Detaching"

// connStandAlone is the connection state of a peer DRBD gave up
// reconnecting to. drbdsetup disconnect refuses it (-9), so a StandAlone
// entry can linger in status through every teardown attempt.
const connStandAlone = "StandAlone"

// connConnected is the fully established connection state.
const connConnected = "Connected"

// rolePrimary is the DRBD role of a node that holds the device open.
const rolePrimary = "Primary"

// Peer-device replication states the status parser distinguishes. Anything
// other than Established/Off means an active resync or verify; VerifyS/VerifyT
// narrow that to an online verify.
const (
	replEstablished = "Established"
	replOff         = "Off"
	replVerifyS     = "VerifyS"
	replVerifyT     = "VerifyT"
)

// Status reports this node's view of one resource.
type Status struct {
	// DiskState: UpToDate, Inconsistent, Outdated, Consistent, Diskless…
	DiskState string
	// Primary is true when this node holds the device open (auto-promote).
	Primary bool
	// PeerPrimary is true when a connected peer holds the device open —
	// that peer is where writes originate, hence where a write barrier
	// must be raised.
	PeerPrimary bool
	// Suspended is true while suspend-io holds this node's write barrier.
	Suspended bool
	// PeerConnected maps each peer's DRBD node id to whether its
	// connection is established. There is deliberately no all-peers
	// aggregate: it would couple health decisions to a diskless
	// tie-breaker's link state (the exact bug #78 removed) — filter by
	// the spec's diskful node ids instead (see agent.diskfulPeersConnected).
	PeerConnected map[int32]bool
	// PeerDiskState maps each peer's DRBD node id to that peer's disk
	// state as this node sees it (UpToDate, Inconsistent, Diskless,
	// DUnknown before the handshake reports it…). The birth-generation
	// trigger keys on every diskful leg reading Inconsistent.
	PeerDiskState map[int32]string
	// SplitBrain is true when a connection is StandAlone — DRBD refused
	// to reconnect after detecting divergent data.
	SplitBrain bool
	// Resyncing is true while a peer connection is mid-resync; drbdadm
	// resize is refused in this window. An online verify also sets it (the
	// replication-state leaves Established), so it doubles as "a verify is
	// already running" — see Verifying for the narrower signal.
	Resyncing bool
	// Verifying is true while a peer connection is mid online-verify
	// (replication-state VerifyS/VerifyT). Distinguishes a verify pass from
	// a data resync, both of which set Resyncing.
	Verifying bool
	// ResyncPercent is the least-synced diskful peer's percent-in-sync
	// while resyncing; 100 when fully in sync (nothing to catch up).
	ResyncPercent float64
	// Quorum is the device quorum flag: false while a freeze-policy
	// volume has lost quorum and DRBD is suspending its IO. Always true
	// when quorum is off (last-man-standing).
	Quorum bool
	// OutOfSyncKiB is the largest per-peer out-of-sync amount (KiB, as
	// drbdsetup reports it) — the data-loss exposure if the healthiest
	// peer is lost. Grows while a peer is down with no resync running.
	OutOfSyncKiB int64
}

// drbdsetup status --json shapes (the fields miroir reads).
type jsonStatus struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	SuspendedUser bool   `json:"suspended-user"`
	Devices       []struct {
		DiskState string `json:"disk-state"`
		Quorum    bool   `json:"quorum"`
	} `json:"devices"`
	Connections []struct {
		PeerNodeID      int32  `json:"peer-node-id"`
		ConnectionState string `json:"connection-state"`
		PeerRole        string `json:"peer-role"`
		// replication-state and percent-in-sync are per peer-device,
		// nested under the connection — NOT connection-level fields
		// (verified against drbd-utils user/v9/drbdsetup.c).
		PeerDevices []struct {
			ReplicationState string  `json:"replication-state"`
			PeerDiskState    string  `json:"peer-disk-state"`
			PercentInSync    float64 `json:"percent-in-sync"`
			OutOfSyncKiB     int64   `json:"out-of-sync"`
		} `json:"peer_devices"`
	} `json:"connections"`
}

// wedgeSignature reports the kernel view of a teardown that cannot make
// progress: device stuck Detaching while every remaining connection is
// one disconnect cannot act on (StandAlone) or already gone. A healthy
// detach completes in milliseconds once the peers are out of the way, and
// nothing but teardown detaches, so this state means a previous down was
// killed mid-flight and never completed — a drain still finishing, or the
// refcount underflow of LINBIT/drbd#137 (see ErrWedged).
func (s jsonStatus) wedgeSignature() bool {
	if len(s.Devices) == 0 || s.Devices[0].DiskState != diskDetaching {
		return false
	}
	for _, c := range s.Connections {
		if c.ConnectionState != connStandAlone {
			return false
		}
	}
	return true
}

// peerIDs lists the entry's connection peer node ids — the handles
// drbdsetup disconnect takes.
func (s jsonStatus) peerIDs() []int32 {
	ids := make([]int32, 0, len(s.Connections))
	for _, c := range s.Connections {
		ids = append(ids, c.PeerNodeID)
	}
	return ids
}

// Status parses `drbdsetup status --json <res>`.
func (d *Driver) Status(ctx context.Context, name string) (Status, error) {
	parsed, err := d.listStatus(ctx, name)
	if err != nil {
		return Status{}, fmt.Errorf("%s: %w", name, err)
	}
	if len(parsed) == 0 {
		return Status{}, fmt.Errorf("resource %s not in status output", name)
	}
	res := parsed[0]

	s := Status{
		Primary:       res.Role == rolePrimary,
		Suspended:     res.SuspendedUser,
		PeerConnected: make(map[int32]bool, len(res.Connections)),
		PeerDiskState: make(map[int32]string, len(res.Connections)),
		ResyncPercent: 100,
	}
	if len(res.Devices) > 0 {
		s.DiskState = res.Devices[0].DiskState
		s.Quorum = res.Devices[0].Quorum
	}
	for _, c := range res.Connections {
		if c.PeerRole == rolePrimary {
			s.PeerPrimary = true
		}
		// replication-state / percent-in-sync live per peer-device.
		// Anything other than Established/Off is active resync/verify;
		// track the least-synced leg for progress reporting.
		for _, pd := range c.PeerDevices {
			if rs := pd.ReplicationState; rs != "" && rs != replEstablished && rs != replOff {
				s.Resyncing = true
				if rs == replVerifyS || rs == replVerifyT {
					s.Verifying = true
				}
				if pd.PercentInSync < s.ResyncPercent {
					s.ResyncPercent = pd.PercentInSync
				}
			}
			s.OutOfSyncKiB = max(s.OutOfSyncKiB, pd.OutOfSyncKiB)
		}
		// Single-volume resources: the first peer-device carries the peer's
		// disk state.
		if len(c.PeerDevices) > 0 {
			s.PeerDiskState[c.PeerNodeID] = c.PeerDevices[0].PeerDiskState
		}
		s.PeerConnected[c.PeerNodeID] = c.ConnectionState == connConnected
		if c.ConnectionState == connStandAlone {
			s.SplitBrain = true
		}
	}
	return s, nil
}

// KernelAvailable reports whether the DRBD kernel module is usable on
// this node. The module ships with the host (the Talos drbd extension),
// not the container, so probe by loading: the explicit modprobe pulls it
// in through the pod's /lib/modules hostPath on nodes that have it but
// have not loaded it yet, and fails harmlessly on local-only nodes.
// `drbdsetup status` then answers exit 0 iff the kernel side is up.
func (d *Driver) KernelAvailable(ctx context.Context) bool {
	_, _ = d.Exec(ctx, "modprobe", "drbd")
	_, err := d.Exec(ctx, "drbdsetup", "status")
	return err == nil
}

// KernelFloor is the minimum DRBD kernel module version the agent runs
// against (Talos ≥ 1.13.0 ships it via the siderolabs/drbd extension).
// Rendering a 9.3.1-era option (peer-tiebreaker, bitmap granularity)
// against an older module errors drbdadm for every resource on the node,
// so the floor is enforced once at startup instead of gating each option.
const KernelFloor = "9.3.1"

// KernelVersion reports the loaded module's version via drbdadm's
// DRBD_KERNEL_VERSION (sourced from /proc/drbd). Only meaningful after
// KernelAvailable: with no module loaded drbdadm falls back to a
// compile-time guess.
func (d *Driver) KernelVersion(ctx context.Context) (string, error) {
	return d.admVersionField(ctx, "DRBD_KERNEL_VERSION")
}

// UtilsVersion reports the drbd-utils userland version via drbdadm's
// DRBDADM_VERSION. Unlike the kernel module, the utils ship in the agent
// image, so this is the container's toolchain version, not the host's.
func (d *Driver) UtilsVersion(ctx context.Context) (string, error) {
	return d.admVersionField(ctx, "DRBDADM_VERSION")
}

// admVersionField pulls one KEY=value line out of drbdadm --version. The
// "=" is part of the match so DRBDADM_VERSION never picks up the
// DRBDADM_VERSION_CODE line.
func (d *Driver) admVersionField(ctx context.Context, key string) (string, error) {
	out, err := d.Exec(ctx, "drbdadm", "--version")
	if err != nil {
		return "", fmt.Errorf("drbdadm --version: %w", err)
	}
	for line := range strings.SplitSeq(out, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+"="); ok {
			return strings.TrimSpace(v), nil
		}
	}
	return "", fmt.Errorf("no %s in drbdadm --version output", key)
}

// kernelFloor is KernelFloor pre-parsed for comparisons.
var kernelFloor = semver.MustParse(KernelFloor)

// BelowKernelFloor reports whether a module version sorts below
// KernelFloor ("9.10" is newer than "9.3"; a short "9.3" reads as
// "9.3.0"). A version that does not parse reads as below: refusing an
// unknown platform beats rendering options it may reject.
func BelowKernelFloor(version string) bool {
	trimmed := strings.TrimSpace(version)
	v, err := semver.NewVersion(trimmed)
	if err != nil {
		// semver rejects a 4th dotted component (vendor module builds
		// like "9.3.1.1") that the floor never needs; compare on the
		// leading x.y.z before giving up.
		if parts := strings.SplitN(trimmed, ".", 4); len(parts) == 4 {
			v, err = semver.NewVersion(strings.Join(parts[:3], "."))
		}
		if err != nil {
			return true
		}
	}
	return v.LessThan(kernelFloor)
}

// DiscardGranularity probes the backing device's discard granularity
// (lsblk DISC-GRAN, bytes), clamped to [4096, 1MiB] — DRBD's sane range
// for rs-discard-granularity. 0 means the device does not support
// discards and nothing should be rendered.
func (d *Driver) DiscardGranularity(ctx context.Context, dev string) (int64, error) {
	out, err := d.Exec(ctx, "lsblk", "-bndo", "DISC-GRAN", dev)
	if err != nil {
		return 0, fmt.Errorf("probe discard granularity of %s: %w", dev, err)
	}
	gran, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse discard granularity of %s (%q): %w", dev, out, err)
	}
	if gran <= 0 {
		return 0, nil
	}
	return min(max(gran, 4096), 1<<20), nil
}

// UserSuspended lists resources whose IO is frozen by suspend-io. The
// kernel is the authority here: a crash between suspend-io and the status
// patch leaves a frozen device that no snapshot status records.
func (d *Driver) UserSuspended(ctx context.Context) ([]string, error) {
	parsed, err := d.listStatus(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, r := range parsed {
		if r.SuspendedUser {
			names = append(names, r.Name)
		}
	}
	return names, nil
}

// SuspendIO freezes writes on this node's device — the cluster-wide write
// barrier for crash-consistent snapshots when run on the Primary. Goes
// through drbdadm: drbdsetup's suspend-io wants a minor, not a name.
func (d *Driver) SuspendIO(ctx context.Context, name string) error {
	_, err := d.adm(ctx, "suspend-io", name)
	return err
}

// ResumeIO lifts the barrier. Callers run it unconditionally after a
// snapshot attempt: a volume left suspended is an outage.
func (d *Driver) ResumeIO(ctx context.Context, name string) error {
	_, err := d.adm(ctx, "resume-io", name)
	return err
}

// Resize grows the DRBD device to the (already grown) backing devices.
// Callers must gate it on Status().Resyncing == false: DRBD refuses a
// resize mid-resync. assumeClean passes --assume-clean so DRBD marks the
// grown region UpToDate on every replica without a resync — correct for
// thin/sparse backends where the grown bytes are deterministically zeroed.
func (d *Driver) Resize(ctx context.Context, name string, assumeClean bool) error {
	if assumeClean {
		_, err := d.adm(ctx, "resize", "--assume-clean", name)
		return err
	}
	_, err := d.adm(ctx, "resize", name)
	return err
}

// Verify kicks off an online verify against every connected peer — the only
// cross-leg integrity check DRBD offers. It returns as soon as the pass is
// armed; the kernel runs the scan in the background and reports out-of-sync
// blocks in the replication status (Status.OutOfSyncKiB) and the kernel log.
// Requires verify-alg in the DRBD config; callers gate it on a connected,
// non-resyncing volume (drbdadm refuses verify otherwise).
func (d *Driver) Verify(ctx context.Context, name string) error {
	_, err := d.adm(ctx, "verify", name)
	return err
}

// IsResizeDuringResync reports whether a Resize failed only because a resync
// was in flight — DRBD refuses resize then, and the caller retries instead
// of failing the reconcile.
func IsResizeDuringResync(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Resize not allowed during resync")
}

func (d *Driver) writeConfig(r Resource) error {
	rendered := []byte(Render(r))
	path := d.path(r.Name + ".res")
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, rendered) {
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(d.StateDir, 0o750); err != nil {
		return err
	}
	// Atomic write: StateDir is drbdadm's include directory, and a crash
	// mid-write would leave a truncated .res that makes drbdadm adjust fail
	// for EVERY resource on the node, not just this one.
	return writeFileAtomic(path, rendered, 0o640)
}

// writeFileAtomic writes data to path via a temp file, fsync, and rename, so a
// crash never leaves a partially-written file at path.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }() // no-op after the explicit Close below; guards early returns
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *Driver) path(file string) string {
	return filepath.Join(d.StateDir, file)
}

func (d *Driver) adm(ctx context.Context, args ...string) (string, error) {
	return d.Exec(ctx, "drbdadm", args...)
}
