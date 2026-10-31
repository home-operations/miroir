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

package agent

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
)

const (
	kmsgPollInterval = time.Second
	defaultKmsgPath  = "/dev/kmsg"
)

// kmsgReadCap bounds the drain loop: /dev/kmsg yields one record per read
// and a busy logger could otherwise keep the loop alive indefinitely.
const kmsgReadCap = 8192

// kmsgReadTimeout bounds a single drain of /dev/kmsg. The device is
// pollable, so os.File registers it with the runtime netpoller and Read
// parks on a drained ring until the kernel logs another record instead of
// surfacing EAGAIN: O_NONBLOCK alone does not make the read return. A
// read deadline turns that park into os.ErrDeadlineExceeded; on a quiet
// node the next record can be hours away (issue #501).
const kmsgReadTimeout = 250 * time.Millisecond

// captureKmsg returns the last limit kernel-log records mentioning
// resource. The DRBD handshake verdict ("Split-Brain detected",
// "Unrelated data, aborting") is kernel-log only — the status API reports
// just the resulting StandAlone — so this is the one place the actual
// failure reason is visible. The agent runs privileged with the host /dev
// mounted, so /dev/kmsg is readable; any failure returns nil and the
// caller degrades to logging without the kernel context.
func captureKmsg(path, resource string, limit int) []string {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	// A regular file (tests) is not pollable and refuses the deadline with
	// os.ErrNoDeadline; its reads cannot park, so the error is moot.
	_ = f.SetReadDeadline(time.Now().Add(kmsgReadTimeout))

	// Drain everything buffered: /dev/kmsg returns one record per read and
	// the deadline fires once caught up; EPIPE marks a record overwritten
	// mid-read and the next read continues. A regular file (tests) arrives
	// in chunks holding several newline-separated records.
	var raw strings.Builder
	buf := make([]byte, 8192)
	for range kmsgReadCap {
		n, err := f.Read(buf)
		if n > 0 {
			raw.Write(buf[:n])
			if buf[n-1] != '\n' {
				raw.WriteByte('\n')
			}
		}
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				continue
			}
			break // deadline (drained), EOF, or anything else
		}
	}

	var lines []string
	for line := range strings.SplitSeq(strings.TrimSuffix(raw.String(), "\n"), "\n") {
		if strings.Contains(line, resource) {
			lines = append(lines, line)
		}
	}
	if len(lines) > limit {
		lines = lines[len(lines)-limit:]
	}
	return lines
}

// tailKmsg drains the kernel log ring on entry, then every interval until
// ctx is cancelled, handing each drain's complete records to handle.
// replay marks the first drain: the boot history before the caller
// started. handle runs on every tick, even with kmsg unopenable.
func tailKmsg(ctx context.Context, log logr.Logger, path string, interval time.Duration, handle func(lines []string, replay bool)) {
	if path == "" {
		path = defaultKmsgPath
	}
	if interval <= 0 {
		interval = kmsgPollInterval
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		log.Error(err, "kmsg unavailable; retrying on every poll")
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()
	var partial string
	replay := true
	for {
		var lines []string
		drained := false
		if f != nil {
			lines, partial = drainKmsg(f, partial)
			drained = true
		}
		handle(lines, replay && drained)
		if drained {
			replay = false
		}
		select {
		case <-ctx.Done():
			if f != nil {
				_ = f.Close()
			}
			return
		case <-tick.C:
		}
		if f == nil {
			if nf, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
				f = nf
			}
		}
	}
}

// drainKmsg reads every buffered record. partial carries a record split
// across drains, which a regular file in tests can produce.
func drainKmsg(f *os.File, partial string) (lines []string, rest string) {
	// Re-armed per drain: the deadline is absolute, and without it a
	// drained ring parks Read until the next kernel record, which also
	// holds the caller's ctx check hostage. See kmsgReadTimeout.
	_ = f.SetReadDeadline(time.Now().Add(kmsgReadTimeout))
	buf := make([]byte, 8192)
	var b strings.Builder
	b.WriteString(partial)
	for range kmsgReadCap {
		n, err := f.Read(buf)
		if n > 0 {
			b.Write(buf[:n])
		}
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				continue
			}
			break
		}
	}
	data := b.String()
	lines = strings.Split(data, "\n")
	if strings.HasSuffix(data, "\n") {
		rest = ""
	} else {
		rest = lines[len(lines)-1]
	}
	return lines[:len(lines)-1], rest
}

// kernelPayload returns a kmsg record's message when the record comes from
// the kernel. The first field is (facility<<3)|severity; kernel messages
// have facility 0 (priority 0-7), userspace gets 8+, so the gate rejects
// lines forged through /dev/kmsg writes.
func kernelPayload(line string) (string, bool) {
	prio, rest, ok := strings.Cut(line, ",")
	if !ok {
		return "", false
	}
	p, err := strconv.Atoi(prio)
	if err != nil || p >= 8 {
		return "", false
	}
	return kmsgPayload(rest), true
}

func kmsgPayload(line string) string {
	if _, rest, ok := strings.Cut(line, ";"); ok {
		return rest
	}
	return line
}
