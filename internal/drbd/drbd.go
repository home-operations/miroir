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

// Apply converges the kernel state for one resource: write config when it
// changed, create and GI-seed metadata once (see seedGI for how fresh
// volumes skip the initial sync), bring the resource up / adjust.
func (d *Driver) Apply(ctx context.Context, r Resource) error {
	if _, err := d.writeConfig(r); err != nil {
		return err
	}

	// create-md + seed exactly once per backing device: the .md-created
	// marker lands only after a fully successful seed, so a retry never
	// adopts half-seeded metadata (which deadlocks the first handshake:
	// both sides Inconsistent, no sync source).
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
	if err := os.WriteFile(marker, nil, 0o640); err != nil {
		return err
	}

	// adjust is idempotent: up when down, reconfigure on change, no-op
	// otherwise. A half-torn kernel slot can answer "Unknown resource"
	// to adjust but accept a plain up — fall back rather than loop.
	if _, err := d.adm(ctx, "adjust", r.Name); err != nil {
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

// ensureMetadata creates and GI-seeds the backing metadata, surviving a
// crash at any point: the .md-seeding sentinel written before create-md
// proves on-disk metadata is our own unfinished attempt and safe to
// re-seed. The on-disk probe stays the authority for "metadata exists" —
// markers are hostPath state that can vanish while the LV still holds a
// live GI, and a blind create-md --force would wipe it.
func (d *Driver) ensureMetadata(ctx context.Context, r Resource) error {
	hasMD, attached, dump, err := d.probeMetadata(ctx, r.Name)
	if err != nil {
		return err
	}
	if attached {
		// A previous life completed seeding and adjust; the metadata is
		// live — never touch it.
		return d.markAdopted(r.Name)
	}
	sentinel := d.path(r.Name + ".md-seeding")
	_, serr := os.Stat(sentinel)
	if serr != nil && !os.IsNotExist(serr) {
		return serr
	}
	if hasMD && os.IsNotExist(serr) {
		// Metadata without any marker: lost hostPath state. Re-seed only
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
	if r.SkipSeed {
		// Late joiner: just-created metadata (UUID_JUST_CREATED) makes
		// the first handshake elect this node full SyncTarget — the only
		// correct outcome, since the peers' bitmaps never tracked writes
		// against a replica that did not exist yet.
		return nil
	}
	return d.seedGI(ctx, r)
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
	for _, line := range strings.Split(dump, "\n") {
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
				// drbdsetup down works without a .res file.
				if _, err := d.Exec(ctx, "drbdsetup", "down", res.Name); err != nil {
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
		if _, err := d.Exec(ctx, "drbdsetup", "down", res.Name); err != nil {
			errs = append(errs, fmt.Errorf("down %s: %w", res.Name, err))
		}
	}
	return errors.Join(errs...)
}

// drbdMajor is DRBD's fixed block-device major number.
const drbdMajor = 147

// minorBase is the first DRBD minor number the agent assigns to miroir
// volumes. Lower numbers may be reserved for system DRBD resources.
const minorBase int32 = 1000

// AllocateMinor assigns a DRBD minor to a volume, returning the same
// minor on future calls. Thread-safe via lock file + atomic rename.
func (d *Driver) AllocateMinor(volume string) (int32, error) {
	lockPath := d.path("minor.lock")
	for tries := 0; tries < 10; tries++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return 0, err
		}
		assigned, err := d.readAssignments()
		if err != nil {
			_ = f.Close()
			_ = os.Remove(lockPath)
			return 0, err
		}
		if m, ok := assigned[volume]; ok {
			_ = f.Close()
			_ = os.Remove(lockPath)
			return m, nil
		}
		used := d.scanUsedMinors(assigned)
		m := minorBase
		for used[m] {
			m++
		}
		assigned[volume] = m
		if err := d.writeAssignments(assigned); err != nil {
			_ = f.Close()
			_ = os.Remove(lockPath)
			return 0, err
		}
		_ = f.Close()
		_ = os.Remove(lockPath)
		return m, nil
	}
	return 0, fmt.Errorf("minor lock held after 10 retries")
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
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		n, err := strconv.Atoi(parts[1])
		if err == nil {
			m[parts[0]] = int32(n)
		}
	}
	return m, nil
}

func (d *Driver) writeAssignments(m map[string]int32) error {
	path := d.path("minor.assign")
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for name, minor := range m {
		if _, err := fmt.Fprintf(f, "%s %d\n", name, minor); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) >= 4 && f[0] == "device" && f[2] == "minor" {
				n, err := strconv.Atoi(strings.TrimSuffix(f[3], ";"))
				if err == nil {
					used[int32(n)] = true
				}
			}
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
		if st.Mode&unix.S_IFMT == unix.S_IFBLK && st.Rdev == want {
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

// seedGI lets fresh volumes skip the initial sync: every replica stamps
// the same deterministic day0 generation identifier into the metadata
// slots of the local node and every peer — the winner node (lowest node
// id) with Consistent+UpToDate flags, every other node with
// WasUpToDate — so the first handshake sees identical generations and
// clean bitmaps. Valid because thin backings read zeros.
//
// The non-winner stamps WasUpToDate (MDF_WAS_UP_TO_DATE) without
// Consistent: the kernel's attach-time "new region assumed zeroed" path
// requires either WasUpToDate or a non-zero rs-discard-granularity to
// keep the bitmap clean on a freshly-grown thin device. A flagless seed
// attaches with the full device marked out-of-sync, triggering a full
// initial sync. WasUpToDate without Consistent still attaches
// Inconsistent (cannot be promoted); it reaches UpToDate at the first
// handshake with zero resync.
//
// Only the local node's slot and actual peer slots are seeded (not all
// 32 metadata slots): unoccupied slots are never read during the
// handshake, and 32 subprocess calls add unnecessary cold-start
// latency. The local slot MUST be included: seeding only peer slots
// leaves the local current-UUID at create-md's random value and the
// handshake aborts "unrelated data" → permanent StandAlone. Peers
// includes the local node, so iterating r.Peers covers both.
//
// GI string is positional: current:bitmap:hist0:hist1[:consistent:uptodate].
// Bitmap base stays 0 ("no out-of-sync bits") — a non-zero base reads as
// a live resync anchor and triggers a full SyncTarget.
func (d *Driver) seedGI(ctx context.Context, r Resource) error {
	gi := Day0GI(r.Name) + ":0:0:0"
	if isWinner(r) {
		// The winner is UpToDate from metadata alone; everyone else
		// reaches it at the first handshake.
		gi += ":1:1"
	} else {
		// WasUpToDate without Consistent: attaches Inconsistent
		// (safe — cannot be promoted), but the kernel's "new region
		// assumed zeroed" attach path keeps the bitmap clean so no
		// full resync is triggered.
		gi += ":0:1"
	}
	// drbdmeta's DEVICE argument is the minor — resource/volume syntax
	// is drbdadm-only. Fed a name, the shipped utils derive minor -1 and
	// probe it with a malformed drbdsetup call whose output is
	// version-dependent (observed flaking per-invocation on real
	// hardware). The real minor keeps the in-use probe well-defined:
	// unconfigured proceeds, configured refuses.
	minor := strconv.Itoa(int(r.Minor))
	for _, p := range r.Peers {
		_, err := d.Exec(ctx, "drbdmeta", "--force", minor, "v09",
			r.LocalDisk, "internal",
			"set-gi", "--node-id", strconv.Itoa(int(p.NodeID)), gi)
		if err != nil {
			return fmt.Errorf("set-gi %s node-id %d: %w", r.Name, p.NodeID, err)
		}
	}
	return nil
}

// isWinner picks the seed winner deterministically: the lowest node id.
func isWinner(r Resource) bool {
	lowest := int32(-1)
	var lowestNode string
	for _, p := range r.Peers {
		if lowest == -1 || p.NodeID < lowest {
			lowest = p.NodeID
			lowestNode = p.Node
		}
	}
	return lowestNode == r.LocalNode
}

// Down stops the resource and removes its rendered state. Idempotent.
// drbdsetup, not drbdadm: drbdadm refuses to do anything while any two
// .res files in the directory conflict, and teardown is how a conflict
// gets removed. drbdsetup needs no config and exits 0 for unknown names.
func (d *Driver) Down(ctx context.Context, name string) error {
	if _, err := os.Stat(d.path(name + ".res")); os.IsNotExist(err) {
		return nil // never configured here
	}
	if _, err := d.Exec(ctx, "drbdsetup", "down", name); err != nil {
		return fmt.Errorf("down %s: %w", name, err)
	}
	for _, suffix := range stateSuffixes {
		if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// DiskUpToDate is the disk state of a replica holding current data.
const DiskUpToDate = "UpToDate"

// rolePrimary is the DRBD role of a node that holds the device open.
const rolePrimary = "Primary"

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
	// Connected is true when every peer connection is established.
	Connected bool
	// SplitBrain is true when a connection is StandAlone — DRBD refused
	// to reconnect after detecting divergent data.
	SplitBrain bool
	// Resyncing is true while a peer connection is mid-resync; drbdadm
	// resize is refused in this window.
	Resyncing bool
}

// drbdsetup status --json shapes (the fields miroir reads).
type jsonStatus struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	SuspendedUser bool   `json:"suspended-user"`
	Devices       []struct {
		DiskState string `json:"disk-state"`
	} `json:"devices"`
	Connections []struct {
		ConnectionState  string `json:"connection-state"`
		PeerRole         string `json:"peer-role"`
		ReplicationState string `json:"replication-state"`
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

	s := Status{Connected: true, Primary: res.Role == "Primary", Suspended: res.SuspendedUser}
	if len(res.Devices) > 0 {
		s.DiskState = res.Devices[0].DiskState
	}
	for _, c := range res.Connections {
		if c.PeerRole == "Primary" {
			s.PeerPrimary = true
		}
		// Anything other than Established/Off is an active resync.
		if rs := c.ReplicationState; rs != "" && rs != "Established" && rs != "Off" {
			s.Resyncing = true
		}
		switch c.ConnectionState {
		case "Connected":
		case "StandAlone":
			s.SplitBrain = true
			s.Connected = false
		default:
			s.Connected = false
		}
	}
	return s, nil
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

// IsResizeDuringResync reports whether a Resize failed only because a resync
// was in flight — DRBD refuses resize then, and the caller retries instead
// of failing the reconcile.
func IsResizeDuringResync(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Resize not allowed during resync")
}

func (d *Driver) writeConfig(r Resource) (bool, error) {
	rendered := []byte(Render(r))
	path := d.path(r.Name + ".res")
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, rendered) {
		return false, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err := os.MkdirAll(d.StateDir, 0o750); err != nil {
		return false, err
	}
	return true, os.WriteFile(path, rendered, 0o640)
}

func (d *Driver) path(file string) string {
	return filepath.Join(d.StateDir, file)
}

func (d *Driver) adm(ctx context.Context, args ...string) (string, error) {
	return d.Exec(ctx, "drbdadm", args...)
}
