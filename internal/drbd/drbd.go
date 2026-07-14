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
	"time"

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
		if _, err := d.adm(ctx, "create-md", "--force", "--max-peers", "7", r.Name+"/0"); err != nil {
			return fmt.Errorf("create-md %s: %w", r.Name, err)
		}
	}
	return nil
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

// SweepOrphans tears down kernel resources and rendered config files with
// no owning volume on this node — leftovers of an agent crash between up
// and down, which would otherwise hold backing devices open forever.
func (d *Driver) SweepOrphans(ctx context.Context, owned func(name string) bool) error {
	out, err := d.Exec(ctx, "drbdsetup", "status", "--json")
	if err == nil {
		var parsed []jsonStatus
		if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr == nil {
			for _, res := range parsed {
				if owned(res.Name) {
					continue
				}
				if err := d.downResource(ctx, res.Name); err != nil {
					return fmt.Errorf("down orphan %s: %w", res.Name, err)
				}
			}
		}
	}
	entries, err := os.ReadDir(d.StateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		name, found := strings.CutSuffix(e.Name(), ".res")
		if !found || owned(name) {
			continue
		}
		for _, suffix := range stateSuffixes {
			if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// DownSecondaries brings down every resource this node holds Secondary,
// releasing its backing device so the backend pool can export on shutdown.
// Primary (open) resources are skipped: drbdsetup down refuses an open
// device, and downing a leg a workload still holds — here, or on a peer this
// node feeds as SyncSource — would drop live redundancy. Rendered config
// stays so the successor re-ups. Best effort: errors are joined so one stuck
// resource cannot strand the rest.
func (d *Driver) DownSecondaries(ctx context.Context) error {
	out, err := d.Exec(ctx, "drbdsetup", "status", "--json")
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	var parsed []jsonStatus
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return fmt.Errorf("parse status: %w", err)
	}
	var errs []error
	for _, res := range parsed {
		if res.Role == rolePrimary {
			continue
		}
		if err := d.downResource(ctx, res.Name); err != nil {
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

// downTimeout bounds drbdsetup down. A device stuck in Detaching (kernel
// ref-count corruption, put_ldev assertion) makes down hang in D-state
// forever; without a bound it pins the reconcile worker and head-of-line-
// blocks every other volume on the node. Shorter than execTimeout because
// down on healthy hardware completes in milliseconds.
const downTimeout = 30 * time.Second

// downResource disconnects each peer connection then downs the resource.
// Disconnecting first avoids a kernel deadlock when the peer is mid-resync:
// drbdsetup down can enter uninterruptible sleep negotiating teardown with
// an active connection. drbdsetup disconnect requires a peer_node_id, so
// the peers are enumerated from status. Best-effort: a resource already
// StandAlone or not configured has no connections to disconnect.
// drbdsetup, not drbdadm: drbdadm refuses conflicting .res files, and
// teardown is how a conflict gets removed.
func (d *Driver) downResource(ctx context.Context, name string) error {
	if out, err := d.Exec(ctx, "drbdsetup", "status", "--json", name); err == nil {
		var parsed []jsonStatus
		if json.Unmarshal([]byte(out), &parsed) == nil {
			for _, res := range parsed {
				if res.Name != name {
					continue
				}
				for _, conn := range res.Connections {
					_, _ = d.Exec(ctx, "drbdsetup", "disconnect", name,
						strconv.Itoa(int(conn.PeerNodeID)))
				}
			}
		}
	}
	downCtx, cancel := context.WithTimeout(ctx, downTimeout)
	defer cancel()
	if _, err := d.Exec(downCtx, "drbdsetup", "down", name); err != nil {
		return fmt.Errorf("down %s: %w", name, err)
	}
	return nil
}

// Down stops the resource and removes its rendered state. Idempotent.
func (d *Driver) Down(ctx context.Context, name string) error {
	if _, err := os.Stat(d.path(name + ".res")); os.IsNotExist(err) {
		return nil // never configured here
	}
	if err := d.downResource(ctx, name); err != nil {
		return err
	}
	for _, suffix := range stateSuffixes {
		if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// Free the minor for reuse — the .res is gone, so scanUsedMinors no
	// longer covers it, and the assignment would otherwise leak forever.
	return d.releaseMinor(name)
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

// Status parses `drbdsetup status --json <res>`.
func (d *Driver) Status(ctx context.Context, name string) (Status, error) {
	out, err := d.Exec(ctx, "drbdsetup", "status", "--json", name)
	if err != nil {
		return Status{}, fmt.Errorf("status %s: %w", name, err)
	}
	var parsed []jsonStatus
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return Status{}, fmt.Errorf("parse status %s: %w", name, err)
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
		s.PeerConnected[c.PeerNodeID] = c.ConnectionState == "Connected"
		if c.ConnectionState == "StandAlone" {
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
	out, err := d.Exec(ctx, "drbdsetup", "status", "--json")
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	var parsed []jsonStatus
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
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
