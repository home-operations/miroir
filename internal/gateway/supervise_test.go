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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

// stubGanesha points ganeshaBin at a script standing in for the server;
// the script touches a "ready" file first, which awaitStub polls, so no
// test races the stub's startup. Restored on cleanup.
func stubGanesha(t *testing.T, script string) string {
	t.Helper()
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	stub := filepath.Join(dir, "ganesha-stub")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\ntouch "+ready+"\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	orig := ganeshaBin
	ganeshaBin = stub
	t.Cleanup(func() { ganeshaBin = orig })
	return ready
}

// awaitStub blocks until the stub signalled readiness.
func awaitStub(t *testing.T, ready string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("stub never signalled readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func superviseCfg() (Config, *mount.SafeFormatAndMount) {
	return Config{VolumeID: "pvc-rwx", ResizePoll: time.Hour},
		mount.NewSafeFormatAndMount(mount.NewFakeMounter(nil), utilexec.New())
}

// A ganesha that exits on its own must surface an error: a dead server
// restarts the pod, silence would leave every NFS client hanging.
func TestSuperviseFailsWhenGaneshaExits(t *testing.T) {
	stubGanesha(t, `test "$*" = "-F -p /run/ganesha.pid -L STDOUT -f /etc/ganesha/ganesha.conf -N NIV_EVENT" || exit 4
exit 3`)
	cfg, m := superviseCfg()
	cfg.GaneshaConf = "/etc/ganesha/ganesha.conf"

	err := supervise(t.Context(), m, cfg, "/dev/null", t.TempDir(), &health{},
		make(chan error), logr.Discard())
	if err == nil || !strings.Contains(err.Error(), "ganesha exited: exit status 3") {
		t.Fatalf("self-exit must error, got %v", err)
	}
}

// A cancelled context is the clean shutdown: SIGTERM, reap, nil. The stub
// reaps its own child on TERM so no sleep outlives the test.
func TestSuperviseStopsCleanOnCancel(t *testing.T) {
	ready := stubGanesha(t, "trap 'kill $child 2>/dev/null; exit 0' TERM; sleep 60 & child=$!; wait $child")
	cfg, m := superviseCfg()
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- supervise(ctx, m, cfg, "/dev/null", t.TempDir(), &health{},
			make(chan error), logr.Discard())
	}()
	awaitStub(t, ready)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancel must stop clean, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervise did not stop on cancel")
	}
}

// A dead liveness endpoint kills ganesha and propagates the reason —
// the kubelet would probe-kill the pod anyway; exiting names why. exec
// makes the sleep BE the stub process, so the kill reaps it.
func TestSuperviseKillsGaneshaOnHealthError(t *testing.T) {
	ready := stubGanesha(t, "exec sleep 60")
	cfg, m := superviseCfg()
	healthErr := make(chan error, 1)
	boom := errors.New("healthz bind failed")

	done := make(chan error, 1)
	go func() {
		done <- supervise(t.Context(), m, cfg, "/dev/null", t.TempDir(), &health{},
			healthErr, logr.Discard())
	}()
	awaitStub(t, ready)
	healthErr <- boom
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("health failure must propagate, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervise did not exit on health error")
	}
}
