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
	"errors"
	"os"
	"strings"
	"syscall"
)

// kmsgReadCap bounds the drain loop: /dev/kmsg yields one record per read
// and a busy logger could otherwise keep the loop alive indefinitely.
const kmsgReadCap = 8192

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

	// Drain everything buffered: /dev/kmsg returns one record per read and
	// EAGAIN once caught up; EPIPE marks a record overwritten mid-read and
	// the next read continues. A regular file (tests) arrives in chunks
	// holding several newline-separated records.
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
			break // EAGAIN (drained), EOF, or anything else
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
