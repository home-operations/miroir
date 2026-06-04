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

package backend

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// lvmThin provisions thin LVs in a dm-thin pool.
type lvmThin struct {
	vg       string
	pool     string
	device   string
	poolSize string
	exec     Exec
}

func newLVMThin(cfg Config, e Exec) *lvmThin {
	return &lvmThin{
		vg: cfg.VolumeGroup, pool: cfg.ThinPool,
		device: cfg.Device, poolSize: cfg.PoolSize,
		exec: e,
	}
}

// Setup creates PV → VG → thin pool on the configured device if the VG does
// not exist yet (notes/DESIGN.md §7.2: kharkiv's r-homefs raw partition). Metadata
// LV is sized 1 GiB per dm-thin guidance (§4.6).
func (l *lvmThin) Setup(ctx context.Context) error {
	if _, err := l.lvmRead(ctx, "vgs", l.vg); err == nil {
		// VG exists; ensure the thin pool does too (it might not after a
		// partial first start).
		ok, perr := l.exists(ctx, l.pool)
		if perr != nil {
			return fmt.Errorf("check thin pool %s/%s: %w", l.vg, l.pool, perr)
		}
		if ok {
			return nil
		}
	} else if l.device == "" {
		return fmt.Errorf("VG %s absent and no --lvm-device configured to create it", l.vg)
	} else {
		if _, err := l.lvmWrite(ctx, "pvcreate", l.device); err != nil &&
			!strings.Contains(err.Error(), "already") {
			return fmt.Errorf("pvcreate %s: %w", l.device, err)
		}
		if _, err := l.lvmWrite(ctx, "vgcreate", l.vg, l.device); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("vgcreate %s: %w", l.vg, err)
		}
	}
	// Sizing: all free space when homefs owns the VG; an explicit bound
	// when the VG is shared (other provisioners need room for their own
	// pools/LVs).
	sizeArgs := []string{"--extents", "100%FREE"}
	if l.poolSize != "" {
		sizeArgs = []string{"--size", l.poolSize}
	}
	args := append([]string{"lvcreate", "--type", "thin-pool"}, sizeArgs...)
	args = append(args, "--poolmetadatasize", "1g", "--name", l.pool, l.vg)
	if _, err := l.lvmWrite(ctx, args...); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("create thin pool %s/%s: %w", l.vg, l.pool, err)
	}
	return nil
}

// lvmWrite runs state-modifying commands with udev sync disabled (the agent
// container has no udev daemon). --noudevsync is invalid on read commands.
func (l *lvmThin) lvmWrite(ctx context.Context, args ...string) (string, error) {
	return l.exec(ctx, "lvm", append(args, "--noudevsync")...)
}

func (l *lvmThin) lvmRead(ctx context.Context, args ...string) (string, error) {
	return l.exec(ctx, "lvm", args...)
}

func (l *lvmThin) DevicePath(vol string) string {
	return fmt.Sprintf("/dev/%s/%s", l.vg, vol)
}

func (l *lvmThin) exists(ctx context.Context, lv string) (bool, error) {
	_, err := l.lvmRead(ctx, "lvs", fmt.Sprintf("%s/%s", l.vg, lv))
	if err != nil {
		if strings.Contains(err.Error(), "Failed to find") ||
			strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (l *lvmThin) Create(ctx context.Context, vol string, sizeBytes int64) (string, error) {
	ok, err := l.exists(ctx, vol)
	if err != nil {
		return "", err
	}
	if !ok {
		_, err = l.lvmWrite(ctx, "lvcreate",
			"--type", "thin",
			"--virtualsize", fmt.Sprintf("%db", sizeBytes),
			"--thinpool", l.pool,
			"--name", vol,
			"--setactivationskip", "n",
			l.vg)
		if err != nil {
			return "", fmt.Errorf("lvcreate %s: %w", vol, err)
		}
		return l.DevicePath(vol), nil
	}
	// Talos does not activate foreign LVs at boot, so an LV surviving a
	// node reboot exists in metadata but has no device node until
	// activated. Idempotent on an already-active LV.
	if _, err := l.lvmWrite(ctx, "lvchange", "--activate", "y",
		fmt.Sprintf("%s/%s", l.vg, vol)); err != nil {
		return "", fmt.Errorf("activate %s: %w", vol, err)
	}
	return l.DevicePath(vol), nil
}

func (l *lvmThin) Resize(ctx context.Context, vol string, sizeBytes int64) error {
	cur, err := l.sizeOf(ctx, vol)
	if err != nil {
		return err
	}
	if cur >= sizeBytes {
		return nil // already big enough (idempotent retry)
	}
	_, err = l.lvmWrite(ctx, "lvextend",
		"--size", fmt.Sprintf("%db", sizeBytes),
		fmt.Sprintf("%s/%s", l.vg, vol))
	return err
}

func (l *lvmThin) Snapshot(ctx context.Context, vol, snap string) error {
	ok, err := l.exists(ctx, snap)
	if err != nil || ok {
		return err
	}
	_, err = l.lvmWrite(ctx, "lvcreate",
		"--snapshot",
		"--name", snap,
		"--setactivationskip", "n",
		fmt.Sprintf("%s/%s", l.vg, vol))
	return err
}

func (l *lvmThin) CreateFromSnapshot(ctx context.Context, vol, _ /* sourceVol */, snap string) (string, error) {
	ok, err := l.exists(ctx, vol)
	if err != nil {
		return "", err
	}
	if !ok {
		// A writable thin snapshot of the snapshot is the clone: instant
		// CoW within the same pool, no data copy (notes/DESIGN.md §4.5.5).
		_, err = l.lvmWrite(ctx, "lvcreate",
			"--snapshot",
			"--name", vol,
			"--setactivationskip", "n",
			fmt.Sprintf("%s/%s", l.vg, snap))
		if err != nil {
			return "", err
		}
	}
	return l.DevicePath(vol), nil
}

func (l *lvmThin) Delete(ctx context.Context, vol string) error {
	ok, err := l.exists(ctx, vol)
	if err != nil || !ok {
		return err
	}
	_, err = l.lvmWrite(ctx, "lvremove", "--yes", fmt.Sprintf("%s/%s", l.vg, vol))
	return err
}

func (l *lvmThin) DeleteSnapshot(ctx context.Context, _ /* vol */, snap string) error {
	return l.Delete(ctx, snap)
}

func (l *lvmThin) sizeOf(ctx context.Context, lv string) (int64, error) {
	out, err := l.lvmRead(ctx, "lvs", "--noheadings", "--units", "b", "--nosuffix",
		"-o", "lv_size", fmt.Sprintf("%s/%s", l.vg, lv))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}

func (l *lvmThin) Stats(ctx context.Context) (PoolStats, error) {
	out, err := l.lvmRead(ctx, "lvs", "--noheadings", "--units", "b", "--nosuffix",
		"--separator", "|",
		"-o", "lv_size,data_percent,metadata_percent",
		fmt.Sprintf("%s/%s", l.vg, l.pool))
	if err != nil {
		return PoolStats{}, err
	}
	fields := strings.Split(strings.TrimSpace(out), "|")
	if len(fields) != 3 {
		return PoolStats{}, fmt.Errorf("unexpected lvs output %q", out)
	}
	size, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 64)
	if err != nil {
		return PoolStats{}, err
	}
	dataPct, err := strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
	if err != nil {
		return PoolStats{}, err
	}
	metaPct, err := strconv.ParseFloat(strings.TrimSpace(fields[2]), 64)
	if err != nil {
		return PoolStats{}, err
	}
	return PoolStats{
		SizeBytes:       size,
		UsedBytes:       int64(float64(size) * dataPct / 100),
		MetaUsedPercent: metaPct,
	}, nil
}
