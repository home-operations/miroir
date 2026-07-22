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
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

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

func TestXFSCloneMountFlags(t *testing.T) {
	const noatime = "noatime"
	vol := replicatedVolume()
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-1"}
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
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-1"}
	if got := xfsCloneMountFlags(vol, "ext4", flags); !slices.Equal(got, flags) {
		t.Fatalf("ext4 clone flags = %v, want %v", got, flags)
	}
	withNoUUID := []string{"nouuid"}
	if got := xfsCloneMountFlags(vol, "xfs", withNoUUID); !slices.Equal(got, withNoUUID) {
		t.Fatalf("existing nouuid flags = %v, want %v", got, withNoUUID)
	}
}
