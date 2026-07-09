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
	"errors"
	"strings"
	"testing"
)

// fakeExec records invocations and replays scripted responses keyed by a
// substring of the full command line.
type fakeExec struct {
	calls     []string
	responses map[string]struct {
		out string
		err error
	}
}

func (f *fakeExec) run(_ context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, line)
	for key, r := range f.responses {
		if strings.Contains(line, key) {
			return r.out, r.err
		}
	}
	return "", nil
}

func (f *fakeExec) respond(key, out string, err error) {
	if f.responses == nil {
		f.responses = map[string]struct {
			out string
			err error
		}{}
	}
	f.responses[key] = struct {
		out string
		err error
	}{out, err}
}

func (f *fakeExec) calledWith(t *testing.T, substr string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			return
		}
	}
	t.Fatalf("expected a call containing %q, got %v", substr, f.calls)
}

func (f *fakeExec) notCalledWith(t *testing.T, substr string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c, substr) {
			t.Fatalf("expected no call containing %q, got %q", substr, c)
		}
	}
}

const (
	thinPoolName = "thinpool"
	volumeGroup  = "vg-miroir"
)

var cfg = Config{VolumeGroup: volumeGroup, ThinPool: thinPoolName, Dataset: "tank/miroir"}

func TestLVMThinCreate(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("lvs vg-miroir/pvc-1", "", errors.New("Failed to find logical volume"))
	b := newLVMThin(cfg, fe.run)

	dev, err := b.Create(t.Context(), "pvc-1", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/dev/vg-miroir/pvc-1" {
		t.Fatalf("unexpected device path %q", dev)
	}
	fe.calledWith(t, "lvcreate --type thin --virtualsize 10737418240b --thinpool thinpool --name pvc-1")
}

func TestLVMThinCreateIdempotent(t *testing.T) {
	fe := &fakeExec{} // lvs succeeds → LV exists
	b := newLVMThin(cfg, fe.run)

	if _, err := b.Create(t.Context(), "pvc-1", 10<<30); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "lvcreate")
	// Existing LVs are activated: Talos does not run vgchange -ay at boot,
	// so post-reboot the LV has no device node until activated.
	fe.calledWith(t, "lvchange --activate y vg-miroir/pvc-1")
}

func TestLVMThinResizeSkipsWhenBigEnough(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("lv_size", "  10737418240\n", nil)
	b := newLVMThin(cfg, fe.run)

	if err := b.Resize(t.Context(), "pvc-1", 10<<30); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "lvextend")
}

func TestLVMThinDeleteAbsentIsNoop(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("lvs vg-miroir/pvc-1", "", errors.New("Failed to find logical volume"))
	b := newLVMThin(cfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "lvremove")
}

func TestLVMThinStats(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("lv_size,data_percent,metadata_percent", "  751619276800|10.50|1.20\n", nil)
	b := newLVMThin(cfg, fe.run)

	s, err := b.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.SizeBytes != 751619276800 {
		t.Fatalf("size = %d", s.SizeBytes)
	}
	if s.UsedBytes != int64(float64(751619276800)*0.105) {
		t.Fatalf("used = %d", s.UsedBytes)
	}
	if s.MetaUsedPercent != 1.2 {
		t.Fatalf("meta%% = %f", s.MetaUsedPercent)
	}
}

func TestZFSCreate(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs list -H tank/miroir/pvc-1", "", errors.New("dataset does not exist"))
	b := newZFS(cfg, fe.run)

	dev, err := b.Create(t.Context(), "pvc-1", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/dev/zvol/tank/miroir/pvc-1" {
		t.Fatalf("unexpected device path %q", dev)
	}
	// Sparse + 4k volblocksize per notes/DESIGN.md §4.1a / SPIKE P0-1, lz4.
	fe.calledWith(t, "zfs create -s -b 4096 -o compression=lz4 -V 10737418240 tank/miroir/pvc-1")
}

func TestZFSSnapshotIdempotent(t *testing.T) {
	fe := &fakeExec{} // list succeeds → snapshot exists
	b := newZFS(cfg, fe.run)

	if err := b.Snapshot(t.Context(), "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "zfs snapshot")
}

func TestZFSCreateFromSnapshot(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs list -H tank/miroir/pvc-2", "", errors.New("dataset does not exist"))
	b := newZFS(cfg, fe.run)

	dev, err := b.CreateFromSnapshot(t.Context(), "pvc-2", "pvc-1", "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	if dev != "/dev/zvol/tank/miroir/pvc-2" {
		t.Fatalf("unexpected device path %q", dev)
	}
	fe.calledWith(t, "zfs clone tank/miroir/pvc-1@snap-1 tank/miroir/pvc-2")
}

func TestLVMThinCloneReactivates(t *testing.T) {
	// Existing clone (post-reboot reconcile) must be re-activated: Talos
	// does not activate foreign LVs at boot.
	fe := &fakeExec{}
	b := newLVMThin(cfg, fe.run)

	if _, err := b.CreateFromSnapshot(t.Context(), "pvc-2", "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "lvcreate")
	fe.calledWith(t, "lvchange --activate y vg-miroir/pvc-2")
}

func TestLVMThinSnapshotAvoidsReservedName(t *testing.T) {
	// LVM rejects LV names starting "snapshot"; CSI snapshot names start
	// exactly there — the LV gets a prefix, end to end.
	fe := &fakeExec{}
	fe.respond("lvs vg-miroir/miroir-snapshot-1", "", errors.New("Failed to find logical volume"))
	b := newLVMThin(cfg, fe.run)

	if err := b.Snapshot(t.Context(), "pvc-1", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "--name miroir-snapshot-1")
	fe.notCalledWith(t, "--name snapshot-1")

	fe.respond("lvs vg-miroir/pvc-2", "", errors.New("Failed to find logical volume"))
	if _, err := b.CreateFromSnapshot(t.Context(), "pvc-2", "pvc-1", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "vg-miroir/miroir-snapshot-1")

	fe.respond("lvs vg-miroir/miroir-snapshot-1", "", nil) // now exists
	if err := b.DeleteSnapshot(t.Context(), "pvc-1", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "lvremove --yes vg-miroir/miroir-snapshot-1")
}

func TestZFSDeleteSnapshotDefersForClones(t *testing.T) {
	// A restore clone pins its origin snapshot: -d lets ZFS remove it
	// with the last clone instead of wedging the delete in retries.
	fe := &fakeExec{}
	fe.respond("zfs list -Hpo name -t snapshot", "tank/miroir/pvc-1@snap-1\n", nil)
	b := newZFS(cfg, fe.run)

	if err := b.DeleteSnapshot(t.Context(), "pvc-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "zfs destroy -d tank/miroir/pvc-1@snap-1")
}

func TestZFSDeletePromotesDependentClones(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs destroy tank/miroir/pvc-1",
		"", errors.New("cannot destroy 'tank/miroir/pvc-1': volume has dependent clones"))
	fe.respond("zfs get -Hpo value clones",
		"-\ntank/miroir/pvc-2,tank/miroir/pvc-3\n", nil)
	b := newZFS(cfg, fe.run)

	// The retry hits the same canned destroy error; what matters is that
	// every dependent clone was promoted first.
	if err := b.Delete(t.Context(), "pvc-1"); err == nil {
		t.Fatal("expected destroy error to propagate")
	}
	fe.calledWith(t, "zfs promote tank/miroir/pvc-2")
	fe.calledWith(t, "zfs promote tank/miroir/pvc-3")
}

func TestZFSDeleteWithoutClonesDoesNotPromote(t *testing.T) {
	fe := &fakeExec{}
	b := newZFS(cfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "zfs promote")
	fe.notCalledWith(t, "zfs get -Hpo value clones")
}

func TestZFSDeleteBusyWhileSnapshotsPresent(t *testing.T) {
	// Snapshots (children, not clones) block destroy and cannot be
	// promoted away: the volume must report ErrBusy so the agent retries
	// until its snapshots are deleted, not fail teardown permanently.
	fe := &fakeExec{}
	fe.respond("zfs destroy tank/miroir/pvc-1",
		"", errors.New("cannot destroy 'tank/miroir/pvc-1': filesystem has children"))
	fe.respond("zfs get -Hpo value clones", "-\n", nil) // no clones to promote
	b := newZFS(cfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy while snapshots pin the volume, got %v", err)
	}
}

func TestZFSDeleteSnapshotSurfacesPermanentError(t *testing.T) {
	// Deferred destroy (-d) never blocks on clones, so a DeleteSnapshot
	// error is permanent and must surface — never ErrBusy, or the agent
	// would silently retry it forever. The message contains "snapshot",
	// which the old substring matcher wrongly treated as busy.
	fe := &fakeExec{}
	fe.respond("zfs list -Hpo name -t snapshot", "tank/miroir/pvc-1@snap-1\n", nil)
	fe.respond("zfs destroy -d tank/miroir/pvc-1@snap-1", "",
		errors.New("cannot destroy snapshot tank/miroir/pvc-1@snap-1: permission denied"))
	b := newZFS(cfg, fe.run)

	err := b.DeleteSnapshot(t.Context(), "pvc-1", "snap-1")
	if err == nil {
		t.Fatal("a permanent DeleteSnapshot error must surface")
	}
	if errors.Is(err, ErrBusy) {
		t.Fatalf("permanent snapshot error must not be ErrBusy, got %v", err)
	}
}

func TestLVMThinDeleteBusyWhenInUse(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("lvremove --yes vg-miroir/pvc-1", "",
		errors.New("Logical volume vg-miroir/pvc-1 in use."))
	b := newLVMThin(cfg, fe.run)

	if err := b.Delete(t.Context(), "pvc-1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy while the LV is open, got %v", err)
	}
}

func TestZFSStatsUsesPool(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zpool get", "2000000000000\n500000000000\n", nil)
	b := newZFS(cfg, fe.run)

	s, err := b.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.SizeBytes != 2000000000000 || s.UsedBytes != 500000000000 {
		t.Fatalf("stats = %+v", s)
	}
	fe.calledWith(t, "zpool get -Hpo value size,allocated tank")
}

func TestLVMThinSetupBootstrapsPool(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("vgs vg-miroir", "", errors.New("Volume group \"vg-miroir\" not found"))
	fe.respond("lvs vg-miroir/thinpool", "", errors.New("Failed to find logical volume"))
	b := newLVMThin(Config{VolumeGroup: volumeGroup, ThinPool: thinPoolName,
		Device: "/dev/disk/by-partlabel/r-miroir"}, fe.run)

	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "pvcreate /dev/disk/by-partlabel/r-miroir")
	fe.calledWith(t, "vgcreate vg-miroir /dev/disk/by-partlabel/r-miroir")
	fe.calledWith(t, "lvcreate --type thin-pool --extents 100%FREE --poolmetadatasize 1g --name thinpool vg-miroir")
}

// A bounded pool leaves VG free space for co-tenant provisioners
// (e.g. OpenEBS LVM-LocalPV creating <vg>_thinpool alongside).
func TestLVMThinSetupBoundedPoolSize(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("vgs vg-miroir", "", errors.New("Volume group \"vg-miroir\" not found"))
	fe.respond("lvs vg-miroir/thinpool", "", errors.New("Failed to find logical volume"))
	b := newLVMThin(Config{VolumeGroup: volumeGroup, ThinPool: thinPoolName,
		Device: "/dev/disk/by-partlabel/r-miroir", PoolSize: "400g"}, fe.run)

	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "lvcreate --type thin-pool --size 400g --poolmetadatasize 1g --name thinpool vg-miroir")
	fe.notCalledWith(t, "100%FREE")
}

func TestLVMThinSetupIdempotent(t *testing.T) {
	fe := &fakeExec{} // vgs + lvs succeed → VG and pool exist
	b := newLVMThin(cfg, fe.run)

	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "pvcreate")
	fe.notCalledWith(t, "lvcreate")
}

func TestLVMThinSetupNoDeviceFails(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("vgs vg-miroir", "", errors.New("Volume group \"vg-miroir\" not found"))
	b := newLVMThin(Config{VolumeGroup: volumeGroup, ThinPool: thinPoolName}, fe.run)

	if err := b.Setup(t.Context()); err == nil {
		t.Fatal("expected error when VG absent and no device configured")
	}
}

func TestZFSSetupCreatesParentDataset(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs list -H tank/miroir", "", errors.New("dataset does not exist"))
	b := newZFS(cfg, fe.run)

	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "zfs create -p tank/miroir")
}

// Regression: --noudevsync is invalid on several lvm subcommands
// (pvcreate rejects it outright); udev is disabled via lvmlocal.conf in
// the image instead, so no command may carry the flag.
func TestLVMCommandsHaveNoUdevSyncFlag(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("lv_size", "  10737418240\n", nil)
	b := newLVMThin(cfg, fe.run)

	_, _ = b.Create(t.Context(), "pvc-1", 1<<30)
	_ = b.Resize(t.Context(), "pvc-1", 1<<30)
	_, _ = b.Stats(t.Context())
	_ = b.Setup(t.Context())

	for _, call := range fe.calls {
		if strings.Contains(call, "--noudevsync") {
			t.Fatalf("lvm command carries --noudevsync: %q", call)
		}
	}
}

func TestNewSelectsBackend(t *testing.T) {
	fe := &fakeExec{}
	if _, err := New("lvmthin", cfg, fe.run); err != nil {
		t.Fatal(err)
	}
	if _, err := New("zfs", cfg, fe.run); err != nil {
		t.Fatal(err)
	}
	if _, err := New("bogus", cfg, fe.run); err == nil {
		t.Fatal("expected error for unknown backend")
	}
}

// PVC sizes are not necessarily 4KiB-multiples (1G = 10^9); OpenZFS
// rejects a volsize that is not a multiple of volblocksize.
func TestZFSAlignsVolsize(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs list -H tank/miroir/pvc-1", "", errors.New("dataset does not exist"))
	fe.respond("zfs get -Hpo value volsize", "1000001536\n", nil)
	b := newZFS(cfg, fe.run)

	if _, err := b.Create(t.Context(), "pvc-1", 1_000_000_000); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "-V 1000001536") // 10^9 rounded up to the 4096 boundary

	if err := b.Resize(t.Context(), "pvc-1", 2_000_000_000); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "volsize=2000003072")
}

// Deleting a volume with restore clones promotes them, and zfs promote
// reparents older snapshots onto the clone. DeleteSnapshot must find the
// migrated copy by its cluster-unique @name — a false success here leaks
// the snapshot onto the clone and blocks the clone's destroy forever.
func TestZFSDeleteSnapshotFindsMigratedSnapshot(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs list -Hpo name -t snapshot -r tank/miroir",
		"tank/miroir/pvc-clone@snap-1\n", nil)
	b := newZFS(cfg, fe.run)

	if err := b.DeleteSnapshot(t.Context(), "pvc-src", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "zfs destroy -d tank/miroir/pvc-clone@snap-1")
}

func TestZFSDeleteSnapshotAbsentIsNoop(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("zfs list -Hpo name -t snapshot -r tank/miroir",
		"tank/miroir/other@unrelated\n", nil)
	b := newZFS(cfg, fe.run)

	if err := b.DeleteSnapshot(t.Context(), "pvc-src", "snap-1"); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "destroy")
}

// A transient vgs failure must surface, not fall into the bootstrap
// branch and confusingly fail pvcreate against an in-use device.
func TestLVMThinSetupSurfacesTransientVGSError(t *testing.T) {
	fe := &fakeExec{}
	fe.respond("vgs vg-miroir", "", errors.New("global/lvmetad lock contention"))
	b := newLVMThin(Config{VolumeGroup: volumeGroup, ThinPool: thinPoolName, Device: "/dev/sdz"}, fe.run)

	if err := b.Setup(t.Context()); err == nil {
		t.Fatal("transient vgs error must fail Setup")
	}
	fe.notCalledWith(t, "pvcreate")
}
