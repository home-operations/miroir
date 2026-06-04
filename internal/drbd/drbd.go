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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/erwanleboucher/homefs/internal/backend"
)

// Driver realizes DRBD resources on one node by rendering config into
// StateDir (bind-mounted from a hostPath so rendered state survives pod
// restarts) and driving drbdadm against it.
type Driver struct {
	// StateDir is where .res files and marker files live; drbdadm's
	// default include path (/etc/drbd.d) inside the agent container.
	StateDir string
	Exec     backend.Exec
}

// Apply converges the kernel state for one resource: write config when it
// changed, create metadata once, bring the resource up / adjust it.
// Returns true when the config file changed (callers may log it).
func (d *Driver) Apply(ctx context.Context, r Resource) (bool, error) {
	changed, err := d.writeConfig(r)
	if err != nil {
		return false, err
	}

	// create-md exactly once per backing device: a marker file in the
	// state dir guards re-runs (drbdadm create-md prompts on existing
	// metadata; --force on every pass would wipe a resync bitmap).
	marker := d.path(r.Name + ".md-created")
	if _, err := os.Stat(marker); os.IsNotExist(err) {
		if _, err := d.adm(ctx, "create-md", "--force", r.Name+"/0"); err != nil {
			return changed, fmt.Errorf("create-md %s: %w", r.Name, err)
		}
		if err := os.WriteFile(marker, nil, 0o640); err != nil {
			return changed, err
		}
	} else if err != nil {
		return changed, err
	}

	// adjust is idempotent: up when down, reconfigure on change, no-op
	// otherwise.
	if _, err := d.adm(ctx, "adjust", r.Name); err != nil {
		return changed, fmt.Errorf("adjust %s: %w", r.Name, err)
	}
	return changed, nil
}

// SkipInitialSync marks a fresh, empty resource UpToDate everywhere
// without copying data. Valid only for new volumes on thin backing (both
// legs read zeros). Exactly one node must call it, once, with all peers
// connected.
func (d *Driver) SkipInitialSync(ctx context.Context, name string) error {
	marker := d.path(name + ".synced")
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	if _, err := d.adm(ctx, "new-current-uuid", "--clear-bitmap", name+"/0"); err != nil {
		return fmt.Errorf("new-current-uuid %s: %w", name, err)
	}
	return os.WriteFile(marker, nil, 0o640)
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
	for _, suffix := range []string{".res", ".md-created", ".synced"} {
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
	// PeersUpToDate is true when every connected peer reports UpToDate.
	PeersUpToDate bool
	// AllPeersInconsistent is true when this node and every peer are
	// Inconsistent — the fresh-volume state before the initial-sync skip.
	AllPeersInconsistent bool
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
		PeerDevices     []struct {
			PeerDiskState string `json:"peer-disk-state"`
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
		return Status{}, fmt.Errorf("resource %s not found in status output", name)
	}
	res := parsed[0]

	s := Status{Connected: true, PeersUpToDate: true, AllPeersInconsistent: true}
	if len(res.Devices) > 0 {
		s.DiskState = res.Devices[0].DiskState
	}
	if s.DiskState != "Inconsistent" {
		s.AllPeersInconsistent = false
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
		for _, pd := range c.PeerDevices {
			if pd.PeerDiskState != "UpToDate" {
				s.PeersUpToDate = false
			}
			if pd.PeerDiskState != "Inconsistent" {
				s.AllPeersInconsistent = false
			}
		}
	}
	if !s.Connected {
		s.AllPeersInconsistent = false
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
