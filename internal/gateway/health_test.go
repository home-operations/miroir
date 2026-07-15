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
	"encoding/binary"
	"io"
	"net"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeNFS listens on a loopback port and answers each connection according
// to mode: "reply" sends a well-formed accepted NULL reply, "wedge" reads
// the call and never answers (the wedged-but-listening ganesha), "garbage"
// answers with a non-RPC byte stream.
func fakeNFS(t *testing.T, mode string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close() //nolint:errcheck // test server
				var mark [4]byte
				if _, err := io.ReadFull(c, mark[:]); err != nil {
					return
				}
				n := binary.BigEndian.Uint32(mark[:]) & 0x7fffffff
				call := make([]byte, n)
				if _, err := io.ReadFull(c, call); err != nil {
					return
				}
				switch mode {
				case "wedge":
					time.Sleep(10 * time.Second)
				case "garbage":
					_, _ = c.Write([]byte("definitely not sunrpc"))
				default:
					// Accepted NULL reply: xid, REPLY, MSG_ACCEPTED,
					// AUTH_NONE verf, SUCCESS.
					var reply [28]byte
					binary.BigEndian.PutUint32(reply[0:], 0x80000000|24)
					copy(reply[4:8], call[0:4]) // echo xid
					binary.BigEndian.PutUint32(reply[8:], 1)
					_, _ = c.Write(reply[:])
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

func TestNFSNull(t *testing.T) {
	if err := nfsNull(fakeNFS(t, "reply"), time.Second); err != nil {
		t.Fatalf("healthy server: %v", err)
	}
	if err := nfsNull(fakeNFS(t, "wedge"), 200*time.Millisecond); err == nil {
		t.Fatal("wedged server (accepts TCP, never answers RPC) reported healthy")
	}
	if err := nfsNull(fakeNFS(t, "garbage"), time.Second); err == nil {
		t.Fatal("non-RPC responder reported healthy")
	}
	if err := nfsNull("127.0.0.1:1", 200*time.Millisecond); err == nil {
		t.Fatal("closed port reported healthy")
	}
}

// The liveness contract: pass unconditionally while staging (a failover
// wait must not be probe-killed), then answer with the real NFS probe once
// serving.
func TestHealthzPhases(t *testing.T) {
	h := &health{volume: "pvc-health", nfsAddr: fakeNFS(t, "wedge"), timeout: 200 * time.Millisecond}

	rec := httptest.NewRecorder()
	h.healthz(rec, nil)
	if rec.Code != 200 {
		t.Fatalf("staging healthz = %d, want 200", rec.Code)
	}

	h.serving.Store(true)
	rec = httptest.NewRecorder()
	h.healthz(rec, nil)
	if rec.Code != 503 {
		t.Fatalf("wedged serving healthz = %d, want 503", rec.Code)
	}

	h.nfsAddr = fakeNFS(t, "reply")
	rec = httptest.NewRecorder()
	h.healthz(rec, nil)
	if rec.Code != 200 {
		t.Fatalf("healthy serving healthz = %d, want 200", rec.Code)
	}
}
