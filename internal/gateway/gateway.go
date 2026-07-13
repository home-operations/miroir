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

// Package gateway is the RWX NFS share manager: one instance per RWX
// volume, running on a replica node, that stages the volume's device the
// same way the CSI node service does (internal/stage) and serves it over
// NFSv4 with NFS-Ganesha. It is the volume's single writer — the device is
// only ever mounted here — so DRBD single-primary fencing is unchanged.
// On failover the Deployment reschedules this onto another replica node;
// the staging retry below is the wait for DRBD to release the dead peer's
// Primary claim.
package gateway

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/home-operations/miroir/internal/stage"
)

// ganeshaBin is the NFS-Ganesha server; a package var so tests can point
// it at a stub.
var ganeshaBin = "ganesha.nfsd"

// Config parameterises one gateway instance.
type Config struct {
	// VolumeID is the MiroirVolume this gateway serves.
	VolumeID string
	// NodeName is this pod's node (downward API); stage.Device uses it to
	// find the local replica.
	NodeName string
	// ExportDir is the parent of the per-volume mount point (the device is
	// mounted at ExportDir/VolumeID). Default /export.
	ExportDir string
	// GaneshaConf is where the rendered config is written. Default
	// /etc/ganesha/ganesha.conf.
	GaneshaConf string
	// StageRetry is how often to retry staging while DRBD promotion is
	// refused (the failover wait). Default 5s.
	StageRetry time.Duration
	// ResizePoll is how often to grow the filesystem to a device that
	// expanded underneath it. Default 30s.
	ResizePoll time.Duration
}

func (c *Config) withDefaults() {
	if c.ExportDir == "" {
		c.ExportDir = "/export"
	}
	if c.GaneshaConf == "" {
		c.GaneshaConf = "/etc/ganesha/ganesha.conf"
	}
	if c.StageRetry == 0 {
		c.StageRetry = 5 * time.Second
	}
	if c.ResizePoll == 0 {
		c.ResizePoll = 30 * time.Second
	}
}

const (
	ganeshaPort   = 2049
	gracePeriod   = 60
	leaseLifetime = 30
)

// Run stages the volume's device, exports it over NFS, and supervises the
// server until ctx is cancelled (clean stop, nil) or the server dies
// (non-nil error → pod restart). It blocks.
func Run(ctx context.Context, cl client.Client, drbdStatus stage.DRBDStatus, cfg Config, log logr.Logger) error {
	cfg.withDefaults()
	deps := stage.Deps{
		Client:   cl,
		NodeName: cfg.NodeName,
		Mounter:  mount.NewSafeFormatAndMount(mount.New(""), utilexec.New()),
		DRBD:     drbdStatus,
	}
	mountPath := filepath.Join(cfg.ExportDir, cfg.VolumeID)

	// Stage the device. This retries forever until it succeeds: on failover
	// stage.Device passes as soon as the local leg is UpToDate, but
	// EnsureFilesystem's open-for-write probe is refused until DRBD drops
	// the dead gateway's Primary claim (its connection timeout). Polling
	// both is the whole failover wait.
	var dev string
	err := wait.PollUntilContextCancel(ctx, cfg.StageRetry, true, func(ctx context.Context) (bool, error) {
		d, vol, err := stage.Device(ctx, deps, cfg.VolumeID)
		if err != nil {
			log.Info("waiting for device", "reason", err.Error())
			return false, nil
		}
		if vol.Spec.Export == nil {
			// Not an RWX volume — a misprovisioned gateway. Fail hard, no
			// retry can fix a missing export spec.
			return false, fmt.Errorf("volume %s has no spec.export (not an RWX volume)", cfg.VolumeID)
		}
		if err := stage.EnsureFilesystem(ctx, deps, vol, d, mountPath, vol.Spec.Export.FSType, nil); err != nil {
			log.Info("waiting to stage filesystem", "reason", err.Error())
			return false, nil
		}
		dev = d
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("stage volume %s: %w", cfg.VolumeID, err)
	}
	log.Info("volume staged", "device", dev, "mount", mountPath)

	if err := writeGaneshaConf(cfg, mountPath); err != nil {
		return err
	}
	return supervise(ctx, deps.Mounter, cfg, dev, mountPath, log)
}

// writeGaneshaConf renders and writes the export config and ensures the
// recovery directory exists on the volume.
func writeGaneshaConf(cfg Config, mountPath string) error {
	recoveryRoot := filepath.Join(mountPath, ".ganesha-recovery")
	if err := os.MkdirAll(recoveryRoot, 0o750); err != nil {
		return fmt.Errorf("create recovery dir: %w", err)
	}
	conf := renderGanesha(ganeshaParams{
		Path:          mountPath,
		Pseudo:        "/" + cfg.VolumeID,
		RecoveryRoot:  recoveryRoot,
		GracePeriod:   gracePeriod,
		LeaseLifetime: leaseLifetime,
		Port:          ganeshaPort,
	})
	if err := os.MkdirAll(filepath.Dir(cfg.GaneshaConf), 0o750); err != nil {
		return fmt.Errorf("create ganesha config dir: %w", err)
	}
	if err := os.WriteFile(cfg.GaneshaConf, []byte(conf), 0o644); err != nil {
		return fmt.Errorf("write ganesha config: %w", err)
	}
	return nil
}

// supervise runs ganesha in the foreground and, on a ticker, grows the
// filesystem to a device that expanded underneath it (online expand lands
// on the diskful legs; the gateway is where the fs is mounted, so it does
// the fs grow). It returns nil on a clean ctx-cancelled stop and an error
// if ganesha exits on its own — a dead server must restart the pod.
func supervise(ctx context.Context, mounter *mount.SafeFormatAndMount, cfg Config, dev, mountPath string, log logr.Logger) error {
	cmd := exec.Command(ganeshaBin, "-F", "-f", cfg.GaneshaConf, "-N", "NIV_EVENT")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ganesha: %w", err)
	}
	log.Info("ganesha serving", "export", "/"+cfg.VolumeID, "port", ganeshaPort)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	ticker := time.NewTicker(cfg.ResizePoll)
	defer ticker.Stop()
	resizer := mount.NewResizeFs(mounter.Exec)

	for {
		select {
		case <-ctx.Done():
			// Graceful stop: ask ganesha to exit, then reap it. The
			// deferred device unmount happens as the pod's mount namespace
			// tears down; DRBD auto-demotes on the last close.
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
			return nil
		case err := <-done:
			return fmt.Errorf("ganesha exited: %w", err)
		case <-ticker.C:
			need, err := resizer.NeedResize(dev, mountPath)
			if err != nil {
				log.Info("resize check failed", "reason", err.Error())
				continue
			}
			if need {
				if _, err := resizer.Resize(dev, mountPath); err != nil {
					log.Info("filesystem grow failed", "reason", err.Error())
				} else {
					log.Info("filesystem grown to device", "mount", mountPath)
				}
			}
		}
	}
}
