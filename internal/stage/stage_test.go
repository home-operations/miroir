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

package stage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	fakeexec "k8s.io/utils/exec/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

const snapName = "snap-1"

type statusOnlyDRBD struct{}

func (statusOnlyDRBD) Status(context.Context, string) (drbd.Status, error) {
	return drbd.Status{}, nil
}

// restartingDRBD is the resourceRestarter upgrade the recovery needs.
type restartingDRBD struct {
	statusOnlyDRBD
	restarted []string
	err       error
}

func (r *restartingDRBD) Restart(_ context.Context, name string) error {
	r.restarted = append(r.restarted, name)
	return r.err
}

func replicatedVolume() *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			DRBD: &miroirv1alpha1.DRBDSpec{Port: 7000},
		},
	}
}

// errFrozenMount mirrors what FormatAndMount surfaces when the kernel
// refuses the mount over a pinned bdev freeze count (fs/super.c via
// mount(8)'s output).
var errFrozenMount = errors.New(`mount failed: exit status 32
Output: mount: /stage/globalmount: /dev/drbd1378 already mounted or mount point busy.
mount warning:
      * drbd1378: Can't mount, blockdev is frozen`)

func TestRecoverFrozenBdevRestartsAndRetries(t *testing.T) {
	r := &restartingDRBD{}
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, replicatedVolume(), "/dev/drbd1378", errFrozenMount)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("recovery must hand kubelet an Unavailable retry, got %v", err)
	}
	if len(r.restarted) != 1 || r.restarted[0] != "pvc-1" {
		t.Fatalf("the volume's resource must be restarted: %v", r.restarted)
	}
}

func TestRecoverFrozenBdevSurfacesRestartFailure(t *testing.T) {
	r := &restartingDRBD{err: errors.New("resource wedged")}
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, replicatedVolume(), "/dev/drbd1378", errFrozenMount)
	if status.Code(err) != codes.Internal {
		t.Fatalf("a failed restart must surface, got %v", err)
	}
}

func TestRecoverFrozenBdevIgnoresOtherMountErrors(t *testing.T) {
	r := &restartingDRBD{}
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, replicatedVolume(), "/dev/drbd1378",
		errors.New("mount failed: wrong fs type"))
	if err != nil {
		t.Fatalf("an ordinary mount failure is not the recovery's, got %v", err)
	}
	if len(r.restarted) != 0 {
		t.Fatalf("no restart may run for an ordinary mount failure: %v", r.restarted)
	}
}

func TestRecoverFrozenBdevSkipsLocalVolumes(t *testing.T) {
	r := &restartingDRBD{}
	vol := replicatedVolume()
	vol.Spec.DRBD = nil
	if err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, vol, "/dev/vg-miroir/pvc-1", errFrozenMount); err != nil {
		t.Fatalf("a local volume's device is no DRBD resource to restart, got %v", err)
	}
	if len(r.restarted) != 0 {
		t.Fatalf("no restart may run for a local volume: %v", r.restarted)
	}
}

func TestRecoverFrozenBdevNeedsRestarter(t *testing.T) {
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: statusOnlyDRBD{}}, replicatedVolume(), "/dev/drbd1378", errFrozenMount)
	if err != nil {
		t.Fatalf("a status-only DRBD dep must fall through to the generic wrap, got %v", err)
	}
}

// apiReader answers the formatted-flag confirmation from one volume and
// counts the reads, standing in for the uncached API reader.
type apiReader struct {
	vol  *miroirv1alpha1.MiroirVolume
	err  error
	gets int
}

func (r *apiReader) Get(_ context.Context, key client.ObjectKey, obj client.Object,
	_ ...client.GetOption) error {
	r.gets++
	if r.err != nil {
		return r.err
	}
	v, ok := obj.(*miroirv1alpha1.MiroirVolume)
	if !ok || r.vol == nil || key.Name != r.vol.Name {
		return errors.New("no such volume")
	}
	r.vol.DeepCopyInto(v)
	return nil
}

func (r *apiReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

func restoredVolume() *miroirv1alpha1.MiroirVolume {
	vol := replicatedVolume()
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapName}
	return vol
}

// The cache carries a restore as never-formatted for as long as the watch
// takes to deliver the controller's inherited flag; a blank clone staged
// inside that window must be refused, not reformatted.
func TestFormattedBeforeConfirmsRestoreAgainstTheAPI(t *testing.T) {
	live := restoredVolume()
	live.Status.Formatted = true
	r := &apiReader{vol: live}
	formatted, err := formattedBefore(t.Context(), Deps{Reader: r}, restoredVolume())
	if err != nil {
		t.Fatal(err)
	}
	if !formatted {
		t.Fatal("a stale cached flag must lose to the API server's")
	}
	if r.gets != 1 {
		t.Fatalf("the confirmation must read once, read %d times", r.gets)
	}
}

func TestFormattedBeforeFormatsARestoreOfAnUnformattedSource(t *testing.T) {
	r := &apiReader{vol: restoredVolume()}
	formatted, err := formattedBefore(t.Context(), Deps{Reader: r}, restoredVolume())
	if err != nil {
		t.Fatal(err)
	}
	if formatted {
		t.Fatal("a source that never carried a filesystem must still mkfs")
	}
}

func TestFormattedBeforeSkipsTheReadForFreshVolumes(t *testing.T) {
	r := &apiReader{err: errors.New("must not be read")}
	formatted, err := formattedBefore(t.Context(), Deps{Reader: r}, replicatedVolume())
	if err != nil || formatted {
		t.Fatalf("a volume with no content source answers from the cache, got %v %v", formatted, err)
	}
	if r.gets != 0 {
		t.Fatalf("no confirmation read may run for a fresh volume, ran %d", r.gets)
	}
}

func TestFormattedBeforeRefusesToGuessOnAReadFailure(t *testing.T) {
	r := &apiReader{err: errors.New("apiserver unreachable")}
	_, err := formattedBefore(t.Context(), Deps{Reader: r}, restoredVolume())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("an unreadable flag must hand kubelet a retry, got %v", err)
	}
}

// scriptedExec answers each command, in order, from script.
func scriptedExec(script []fakeexec.FakeAction) *fakeexec.FakeExec {
	fcmd := &fakeexec.FakeCmd{CombinedOutputScript: script}
	fe := &fakeexec.FakeExec{}
	for range script {
		fe.CommandScript = append(fe.CommandScript,
			func(cmd string, args ...string) utilexec.Cmd {
				return fakeexec.InitFakeCmd(fcmd, cmd, args...)
			})
	}
	return fe
}

func execOut(s string) fakeexec.FakeAction {
	return func() ([]byte, []byte, error) { return []byte(s), nil, nil }
}

func execExit(code int) fakeexec.FakeAction {
	return func() ([]byte, []byte, error) { return nil, nil, fakeexec.FakeExitError{Status: code} }
}

// ensureDeps builds Deps around a fake mounter, a scripted exec, and a
// fake client holding vol.
func ensureDeps(t *testing.T, vol *miroirv1alpha1.MiroirVolume, fm *mount.FakeMounter, fe *fakeexec.FakeExec) Deps {
	t.Helper()
	s := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(vol).Build()
	return Deps{Client: c, NodeName: "node-a", DRBD: statusOnlyDRBD{},
		Mounter: mount.NewSafeFormatAndMount(fm, fe)}
}

// tempDev stands in for the block device: EnsureFilesystem's write probe
// only needs something openable O_RDWR.
func tempDev(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "dev")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// EnsureFilesystem's own probe is the format decision: a device that reads
// a filesystem is mounted without re-entering FormatAndMount, whose internal
// re-probe answers a momentarily unreadable device with mkfs (issue #444).
// The bait entries would satisfy exactly that re-probe and mkfs; the stage
// must never reach them.
func TestEnsureFilesystemNeverReformatsAFormattedDevice(t *testing.T) {
	vol := replicatedVolume()
	fm := mount.NewFakeMounter(nil)
	fe := scriptedExec([]fakeexec.FakeAction{
		execOut("TYPE=ext4\n"), // blkid: the device carries a filesystem
		execOut(""),            // fsck -a: clean
		execOut("1\n"),         // blockdev --getro: read-only skips the resize path
		execExit(2),            // bait: a re-probe reading the device blank
		execOut(""),            // bait: the mkfs that must never run
	})
	d := ensureDeps(t, vol, fm, fe)
	if err := EnsureFilesystem(t.Context(), d, vol, tempDev(t),
		filepath.Join(t.TempDir(), "staging"), "ext4", nil); err != nil {
		t.Fatal(err)
	}
	if fe.CommandCalls != 3 {
		t.Fatalf("exec ran %d commands, want 3 (blkid, fsck, blockdev) and no mkfs", fe.CommandCalls)
	}
	log := fm.GetLog()
	if len(log) != 1 || log[0].Action != mount.FakeActionMount {
		t.Fatalf("expected exactly one mount, got %+v", log)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := d.Client.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Formatted || !got.Status.Activated {
		t.Fatalf("flags must latch: formatted=%v activated=%v",
			got.Status.Formatted, got.Status.Activated)
	}
}

// A blank probe on a volume that ever carried a filesystem is refused, not
// formatted — blkid cannot tell blank from unreadable.
func TestEnsureFilesystemRefusesBlankFormattedVolume(t *testing.T) {
	vol := replicatedVolume()
	vol.Status.Formatted = true
	fm := mount.NewFakeMounter(nil)
	fe := scriptedExec([]fakeexec.FakeAction{
		execExit(2), // blkid: blank (or unreadable — indistinguishable)
		execOut(""), // bait: the mkfs that must never run
	})
	err := EnsureFilesystem(t.Context(), ensureDeps(t, vol, fm, fe), vol, tempDev(t),
		filepath.Join(t.TempDir(), "staging"), "ext4", nil)
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("a formatted volume reading blank must refuse with DataLoss, got %v", err)
	}
	if fe.CommandCalls != 1 {
		t.Fatalf("exec ran %d commands, want 1 (blkid only)", fe.CommandCalls)
	}
	if log := fm.GetLog(); len(log) != 0 {
		t.Fatalf("nothing may be mounted: %+v", log)
	}
}

// A volume that never carried a filesystem still gets its one mkfs.
func TestEnsureFilesystemFormatsNeverFormattedVolume(t *testing.T) {
	vol := replicatedVolume()
	fm := mount.NewFakeMounter(nil)
	fe := scriptedExec([]fakeexec.FakeAction{
		execExit(2),    // blkid: blank
		execExit(2),    // FormatAndMount's internal blkid: still blank
		execOut(""),    // mkfs.ext4
		execOut("1\n"), // blockdev --getro: read-only skips the resize path
	})
	d := ensureDeps(t, vol, fm, fe)
	if err := EnsureFilesystem(t.Context(), d, vol, tempDev(t),
		filepath.Join(t.TempDir(), "staging"), "ext4", nil); err != nil {
		t.Fatal(err)
	}
	log := fm.GetLog()
	if len(log) != 1 || log[0].Action != mount.FakeActionMount {
		t.Fatalf("expected exactly one mount, got %+v", log)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := d.Client.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Formatted || !got.Status.Activated {
		t.Fatalf("flags must latch after first mkfs: formatted=%v activated=%v",
			got.Status.Formatted, got.Status.Activated)
	}
}

// Damage fsck cannot correct fails the stage before anything is mounted,
// matching FormatAndMount's refusal.
func TestEnsureFilesystemFailsStageOnUncorrectableFsck(t *testing.T) {
	vol := replicatedVolume()
	fm := mount.NewFakeMounter(nil)
	fe := scriptedExec([]fakeexec.FakeAction{
		execOut("TYPE=ext4\n"),
		execExit(4), // fsck: errors left uncorrected
	})
	err := EnsureFilesystem(t.Context(), ensureDeps(t, vol, fm, fe), vol, tempDev(t),
		filepath.Join(t.TempDir(), "staging"), "ext4", nil)
	if status.Code(err) != codes.Internal {
		t.Fatalf("uncorrectable fsck errors must fail the stage, got %v", err)
	}
	if log := fm.GetLog(); len(log) != 0 {
		t.Fatalf("nothing may be mounted: %+v", log)
	}
}

func TestXFSCloneMountFlags(t *testing.T) {
	const noatime = "noatime"
	vol := replicatedVolume()
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapName}
	original := []string{noatime}
	got := xfsCloneMountFlags(vol, "xfs", original)
	if !slices.Equal(got, []string{noatime, "nouuid"}) {
		t.Fatalf("flags = %v, want noatime,nouuid", got)
	}
	if !slices.Equal(original, []string{noatime}) {
		t.Fatalf("input flags were mutated: %v", original)
	}
}

func TestXFSCloneMountFlagsSkipsOtherFilesystemsAndSources(t *testing.T) {
	vol := replicatedVolume()
	flags := []string{"relatime"}
	if got := xfsCloneMountFlags(vol, "xfs", flags); !slices.Equal(got, flags) {
		t.Fatalf("non-clone flags = %v, want %v", got, flags)
	}
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapName}
	if got := xfsCloneMountFlags(vol, "ext4", flags); !slices.Equal(got, flags) {
		t.Fatalf("ext4 clone flags = %v, want %v", got, flags)
	}
	withNoUUID := []string{"nouuid"}
	if got := xfsCloneMountFlags(vol, "xfs", withNoUUID); !slices.Equal(got, withNoUUID) {
		t.Fatalf("existing nouuid flags = %v, want %v", got, withNoUUID)
	}
}
