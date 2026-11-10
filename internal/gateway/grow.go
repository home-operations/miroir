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

package gateway

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

// grower grows the mounted filesystem to its device once the device has
// expanded underneath it. It keys off the device size rather than
// mount-utils' NeedResize, which answers true unconditionally for ext4 and
// xfs (it defers the size check to resize2fs/xfs_growfs), so polling that
// would fork a resize on every tick for the life of the pod.
type grower struct {
	resizer   *mount.ResizeFs
	exec      utilexec.Interface
	dev       string
	mountPath string
	// size is the device size the filesystem is known to fill: 0 until a
	// grow succeeds, so the first tick always grows once. Sampling the
	// device at startup instead would miss an expansion that landed
	// between the stage's grow and that sample.
	size uint64
	log  logr.Logger
}

func newGrower(mounter *mount.SafeFormatAndMount, dev, mountPath string, log logr.Logger) *grower {
	return &grower{
		resizer:   mount.NewResizeFs(mounter.Exec),
		exec:      mounter.Exec,
		dev:       dev,
		mountPath: mountPath,
		log:       log,
	}
}

// tick grows the filesystem if the device is larger than the size the
// filesystem last filled. The size advances only on a confirmed grow: a
// failed one, or a Resize that found nothing to grow (blkid could not read
// a filesystem type), leaves it alone so the next tick retries.
func (g *grower) tick() {
	size, err := deviceSize(g.exec, g.dev)
	if err != nil {
		g.log.Info("device size check failed", "reason", err.Error())
		return
	}
	if size <= g.size {
		return
	}
	resized, err := g.resizer.Resize(g.dev, g.mountPath)
	if err != nil {
		g.log.Info("filesystem grow failed", "reason", err.Error())
		return
	}
	if !resized {
		g.log.Info("filesystem grow skipped: no filesystem type read from the device")
		return
	}
	g.size = size
	g.log.Info("filesystem grown to device", "mount", g.mountPath, "bytes", size)
}

// deviceSize reads the block device's size in bytes.
func deviceSize(exec utilexec.Interface, dev string) (uint64, error) {
	out, err := exec.Command("blockdev", "--getsize64", dev).CombinedOutput()
	s := strings.TrimSpace(string(out))
	if err != nil {
		return 0, fmt.Errorf("blockdev --getsize64 %s: %w: %s", dev, err, s)
	}
	size, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size of %s %q: %w", dev, s, err)
	}
	return size, nil
}
