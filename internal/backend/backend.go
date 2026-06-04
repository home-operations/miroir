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

// Package backend abstracts the node-local thin-provisioned storage layer
// (notes/DESIGN.md §4.1a). DRBD replicates whatever block device a Backend
// provides, so replicas of one volume may use different backends on
// different nodes (ZFS zvol on paris, LVM thin LV on kharkiv).
package backend

import (
	"context"
	"fmt"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
)

// PoolStats reports capacity of the node-local pool backing this Backend.
type PoolStats struct {
	// SizeBytes is the total pool capacity.
	SizeBytes int64
	// UsedBytes is the currently allocated capacity.
	UsedBytes int64
	// MetaUsedPercent is the metadata usage (dm-thin only; 0 for ZFS).
	MetaUsedPercent float64
}

// Backend provisions and manages thin block devices on one node.
// Implementations are thin wrappers over the lvm/zfs CLIs; all methods are
// idempotent so reconcilers can retry safely.
type Backend interface {
	// Setup bootstraps the node-local pool on first start if absent
	// (notes/DESIGN.md §7.2): lvmthin creates PV/VG/thin-pool on the configured
	// device; zfs creates the parent dataset. Idempotent.
	Setup(ctx context.Context) error
	// Create provisions a thin device of the given virtual size and
	// returns its device path. Succeeds if the device already exists at
	// (at least) the requested size.
	Create(ctx context.Context, vol string, sizeBytes int64) (devPath string, err error)
	// Resize grows the device to sizeBytes (online). Shrinking errors.
	Resize(ctx context.Context, vol string, sizeBytes int64) error
	// Sync drains in-flight writes down to the backing store: dirty
	// pages, DRBD's writeback queue, and (ZFS) pending transaction
	// groups. Snapshots taken without it can capture stale content even
	// under a DRBD suspend-io barrier — suspend stops new writes, not
	// queued ones.
	Sync(ctx context.Context, vol string) error
	// Snapshot takes a local CoW snapshot of the device.
	Snapshot(ctx context.Context, vol, snap string) error
	// CreateFromSnapshot provisions a new writable device as a CoW clone
	// of an existing snapshot and returns its device path.
	CreateFromSnapshot(ctx context.Context, vol, sourceVol, snap string) (devPath string, err error)
	// Delete removes the device. Succeeds if already absent.
	Delete(ctx context.Context, vol string) error
	// DeleteSnapshot removes a snapshot. Succeeds if already absent.
	DeleteSnapshot(ctx context.Context, vol, snap string) error
	// DevicePath returns the path the device has (or would have).
	DevicePath(vol string) string
	// Stats reports pool capacity for placement and guardrails (§4.6).
	Stats(ctx context.Context) (PoolStats, error)
}

// New returns the Backend implementation selected by typ.
func New(typ homefsv1alpha1.BackendType, cfg Config, exec Exec) (Backend, error) {
	switch typ {
	case homefsv1alpha1.BackendLVMThin:
		return newLVMThin(cfg, exec), nil
	case homefsv1alpha1.BackendZFS:
		return newZFS(cfg, exec), nil
	default:
		return nil, fmt.Errorf("unknown backend type %q", typ)
	}
}

// Config carries the node-local pool locations, from agent flags.
type Config struct {
	// VolumeGroup is the LVM VG holding the thin pool (lvmthin).
	VolumeGroup string
	// ThinPool is the thin pool LV name inside VolumeGroup (lvmthin).
	ThinPool string
	// Device is the block device backing the VG (lvmthin), e.g.
	// /dev/disk/by-partlabel/r-homefs (Talos RawVolumeConfig partition).
	// Required only for Setup when the VG does not exist yet.
	Device string
	// PoolSize bounds the thin pool created by Setup (lvm size spec,
	// e.g. "400g"). Empty means all free space — set it when the VG is
	// shared with another provisioner (e.g. OpenEBS LVM-LocalPV).
	PoolSize string
	// Dataset is the parent ZFS dataset for zvols (zfs), e.g. "tank/homefs".
	Dataset string
}
