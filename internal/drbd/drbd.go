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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/eleboucher/homefs/internal/backend"
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

	// create-md + seed exactly once per backing device. The authority is
	// the on-disk metadata itself (dump-md probe) — a marker file alone
	// is hostPath state that can vanish while the LV still holds live
	// metadata, and a blind create-md --force would then wipe a live GI
	// and resync bitmap. The marker remains as a cheap fast path.
	marker := d.path(r.Name + ".md-created")
	if _, err := os.Stat(marker); os.IsNotExist(err) {
		hasMD, err := d.hasMetadata(ctx, r.Name)
		if err != nil {
			return err
		}
		if !hasMD {
			// --max-peers reserves metadata slots up-front: it cannot
			// be raised later without recreating metadata, so leave
			// headroom beyond the current 2-node shape.
			if _, err := d.adm(ctx, "create-md", "--force", "--max-peers", "7", r.Name+"/0"); err != nil {
				return fmt.Errorf("create-md %s: %w", r.Name, err)
			}
			if err := d.seedGI(ctx, r); err != nil {
				return err
			}
		}
		if err := os.WriteFile(marker, nil, 0o640); err != nil {
			return err
		}
	} else if err != nil {
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

// hasMetadata probes the backing device for existing DRBD metadata.
// An attached (busy) device necessarily has metadata.
func (d *Driver) hasMetadata(ctx context.Context, name string) (bool, error) {
	_, err := d.adm(ctx, "dump-md", name+"/0")
	switch {
	case err == nil:
		return true, nil
	case strings.Contains(err.Error(), "busy") ||
		strings.Contains(err.Error(), "configured"):
		// Device attached to the kernel — metadata exists.
		return true, nil
	case strings.Contains(err.Error(), "no valid meta data") ||
		strings.Contains(err.Error(), "No valid meta data"):
		return false, nil
	default:
		return false, fmt.Errorf("dump-md %s: %w", name, err)
	}
}

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
		for _, suffix := range []string{".res", ".md-created"} {
			if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// drbdMajor is DRBD's fixed block-device major number.
const drbdMajor = 147

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
	return nil
}

// Day0GI derives the deterministic "day 0" generation identifier all
// replicas of a fresh volume stamp into their metadata: 16 hex chars (the
// first 8 bytes of sha256), low bit cleared — DRBD's low bit marks
// "primary writes happened" and a synthetic day0 means none have.
func Day0GI(name string) string {
	h := sha256.Sum256(fmt.Appendf(nil, "homefs-day0:%s/0", name))
	h[7] &^= 0x01
	return strings.ToUpper(hex.EncodeToString(h[:8]))
}

// maxNodeID is DRBD9's highest metadata node-id slot.
const maxNodeID = 31

// seedGI lets fresh volumes skip the initial sync: every replica stamps
// the same deterministic day0 generation identifier (winner = lowest node
// id, flagged Consistent+UpToDate; others bare), so the first handshake
// sees identical generations and clean bitmaps — valid because thin
// backings read zeros.
//
// Runs between create-md and the first adjust, into every node-id slot
// (0..maxNodeID, own slot included): seeding only peer slots would leave
// the local current-UUID at create-md's random value and the handshake
// aborts "unrelated data" → permanent StandAlone.
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
	}
	for nodeID := 0; nodeID <= maxNodeID; nodeID++ {
		_, err := d.Exec(ctx, "drbdmeta", "--force", r.Name+"/0", "v09",
			r.LocalDisk, "internal",
			"set-gi", "--node-id", strconv.Itoa(nodeID), gi)
		if err != nil {
			return fmt.Errorf("set-gi %s node-id %d: %w", r.Name, nodeID, err)
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
func (d *Driver) Down(ctx context.Context, name string) error {
	if _, err := os.Stat(d.path(name + ".res")); os.IsNotExist(err) {
		return nil // never configured here
	}
	if _, err := d.adm(ctx, "down", name); err != nil &&
		!strings.Contains(err.Error(), "no resources defined") &&
		!strings.Contains(err.Error(), "not defined in your config") {
		return fmt.Errorf("down %s: %w", name, err)
	}
	for _, suffix := range []string{".res", ".md-created"} {
		if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Status reports this node's view of one resource.
type Status struct {
	// DiskState: UpToDate, Inconsistent, Outdated, Consistent, Diskless…
	DiskState string
	// Connected is true when every peer connection is established.
	Connected bool
	// SplitBrain is true when a connection is StandAlone — DRBD refused
	// to reconnect after detecting divergent data.
	SplitBrain bool
}

// drbdsetup status --json shapes (the fields homefs reads).
type jsonStatus struct {
	Name    string `json:"name"`
	Devices []struct {
		DiskState string `json:"disk-state"`
	} `json:"devices"`
	Connections []struct {
		ConnectionState string `json:"connection-state"`
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

	s := Status{Connected: true}
	if len(res.Devices) > 0 {
		s.DiskState = res.Devices[0].DiskState
	}
	for _, c := range res.Connections {
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
