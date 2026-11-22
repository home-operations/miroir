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
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
)

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

// KernelFloor is the minimum DRBD kernel module version the agent runs
// against (Talos ≥ 1.13.0 ships it via the siderolabs/drbd extension).
// Rendering a 9.3.1-era option (peer-tiebreaker, bitmap granularity)
// against an older module errors drbdadm for every resource on the node,
// so the floor is enforced once at startup instead of gating each option.
const KernelFloor = "9.3.1"

// KernelVersion reports the loaded module's version via drbdadm's
// DRBD_KERNEL_VERSION (sourced from /proc/drbd). Only meaningful after
// KernelAvailable: with no module loaded drbdadm falls back to a
// compile-time guess.
func (d *Driver) KernelVersion(ctx context.Context) (string, error) {
	return d.admVersionField(ctx, "DRBD_KERNEL_VERSION")
}

// UtilsVersion reports the drbd-utils userland version via drbdadm's
// DRBDADM_VERSION. Unlike the kernel module, the utils ship in the agent
// image, so this is the container's toolchain version, not the host's.
func (d *Driver) UtilsVersion(ctx context.Context) (string, error) {
	return d.admVersionField(ctx, "DRBDADM_VERSION")
}

// admVersionField pulls one KEY=value line out of drbdadm --version. The
// "=" is part of the match so DRBDADM_VERSION never picks up the
// DRBDADM_VERSION_CODE line.
func (d *Driver) admVersionField(ctx context.Context, key string) (string, error) {
	out, err := d.Exec(ctx, "drbdadm", "--version")
	if err != nil {
		return "", fmt.Errorf("drbdadm --version: %w", err)
	}
	for line := range strings.SplitSeq(out, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+"="); ok {
			return strings.TrimSpace(v), nil
		}
	}
	return "", fmt.Errorf("no %s in drbdadm --version output", key)
}

// kernelFloor is KernelFloor pre-parsed for comparisons.
var kernelFloor = semver.MustParse(KernelFloor)

// BelowKernelFloor reports whether a module version sorts below
// KernelFloor ("9.10" is newer than "9.3"; a short "9.3" reads as
// "9.3.0"). A version that does not parse reads as below: refusing an
// unknown platform beats rendering options it may reject.
func BelowKernelFloor(version string) bool {
	trimmed := strings.TrimSpace(version)
	v, err := semver.NewVersion(trimmed)
	if err != nil {
		// semver rejects a 4th dotted component (vendor module builds
		// like "9.3.1.1") that the floor never needs; compare on the
		// leading x.y.z before giving up.
		if parts := strings.SplitN(trimmed, ".", 4); len(parts) == 4 {
			v, err = semver.NewVersion(strings.Join(parts[:3], "."))
		}
		if err != nil {
			return true
		}
	}
	return v.LessThan(kernelFloor)
}
