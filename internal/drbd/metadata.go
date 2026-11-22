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
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
		// Metadata without any marker: lost hostPath state. Treat it as
		// freshly created only when the GI proves no Primary ever wrote;
		// anything else is a live volume — adopt it untouched.
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
// attached means this leg holds its backing device in the kernel (which
// necessarily implies metadata exists); dump is the raw dump-md output
// when readable.
//
// Kernel state is probed first: an attached resource claims its backing
// device exclusively, so dump-md only reports EBUSY — the same error a
// foreign holder (stale mount, LVM) produces. Asking the kernel
// disambiguates: local disk attached → attached; otherwise any remaining
// busy error is a foreign holder and surfaces.
//
// The probe keys on this node's own disk state, not the resource's mere
// presence in the kernel — the same predicate drbdmeta itself uses to
// refuse a busy device (is_attached() is dstate > Diskless, drbd-utils
// user/shared/drbdmeta.c). An auto-diskful conversion (a tie-breaker or
// client leg flipped diskful in place, keeping its node-id) is already up
// as a diskless leg while the backing it is gaining is still blank: it
// holds no backing device, so nothing claims the disk, dump-md reads it
// fine and create-md is allowed. Reading presence alone as live metadata
// would skip create-md and leave adjust attaching a blank device — which
// fails "No valid meta data found", and Apply's recovery re-probes into
// the same answer, so the leg never materializes.
func (d *Driver) probeMetadata(ctx context.Context, name string) (hasMD, attached bool, dump string, err error) {
	if parsed, perr := d.listStatus(ctx, name); perr == nil {
		for _, res := range parsed {
			// Positive test on purpose: disk-state is a plain string, so a
			// device entry missing the key unmarshals to "" — and reading
			// that as attached would adopt a blank backing and reinstate
			// the error loop above. Unknown means "ask dump-md", which is
			// authoritative either way.
			if res.Name == name && len(res.Devices) > 0 &&
				res.Devices[0].DiskState != "" &&
				res.Devices[0].DiskState != DiskDiskless {
				return true, true, "", nil
			}
		}
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
// removes — keep in sync with everything Apply, ensureMetadata, and
// WipeForeignMetadata write.
var stateSuffixes = []string{".res", ".md-created", ".md-seeding", ".md-adopted", ".md-wiped", pendingForgetSuffix}

// removeStateFiles deletes the resource's rendered config and markers.
func (d *Driver) removeStateFiles(name string) error {
	for _, suffix := range stateSuffixes {
		if err := os.Remove(d.path(name + suffix)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// WipeForeignMetadata probes an unreplicated volume's backing for DRBD
// internal metadata and wipes it when present — the state a restore that
// shrank out of replication clones in: the replicated source's metadata
// rides the tail of the byte-identical clone, and with pods staging the
// raw backing (no DRBD layer above it), blkid identifies the device as
// TYPE=drbd and staging's filesystem checks refuse it (found by the
// e2e). Probing first keeps a metadata-less clone (an unreplicated
// source's) untouched — wipe-md against a bare device would zero the
// filesystem tail instead. The .md-wiped marker makes it one-shot: the
// probe must never run against the device once a consumer can have it
// mounted, and the crash window between clone and wipe is covered by the
// caller re-running until the marker lands. A temporary minor assignment
// keeps drbdmeta's DEVICE handle clear of both system resources below
// minorBase and concurrently realized miroir resources.
func (d *Driver) WipeForeignMetadata(ctx context.Context, name, disk string) (retErr error) {
	marker := d.path(name + ".md-wiped")
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	probeName := "@foreign-metadata-probe/" + name
	minor, err := d.AllocateMinor(ctx, probeName, 0)
	if err != nil {
		return fmt.Errorf("reserve foreign metadata probe minor for %s: %w", name, err)
	}
	defer func() {
		retErr = errors.Join(retErr, d.releaseMinor(probeName))
	}()

	// "unclean" is metadata too: a snapshot cut on an attached source
	// captures its activity log mid-flight and dump-md refuses to read
	// it, but wipe-md --force does not care.
	_, err = d.Exec(ctx, "drbdmeta", strconv.Itoa(int(minor)), "v09", disk, "internal", "dump-md")
	switch {
	case err == nil || strings.Contains(err.Error(), "unclean"):
		if err := d.WipeMetadata(ctx, name, disk, minor); err != nil {
			return err
		}
	case !isMissingMetadata(err):
		return fmt.Errorf("probe foreign metadata on %s: %w", name, err)
	}
	return os.WriteFile(marker, nil, 0o640)
}

// InvalidateForeignMetadataWipe forgets the one-shot result before a
// backing is recreated from its source snapshot. The replacement clone
// carries the source's metadata again even when its device path and volume
// name are unchanged.
func (d *Driver) InvalidateForeignMetadataWipe(name string) error {
	err := os.Remove(d.path(name + ".md-wiped"))
	if err != nil && !os.IsNotExist(err) {
		return err
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
