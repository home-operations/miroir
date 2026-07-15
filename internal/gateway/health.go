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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The gateway runs without the controller-runtime manager (see cmd/main.go),
// so it carries its own registry instead of the global one the controller
// and agent share.
var (
	gatewayRegistry  = prometheus.NewRegistry()
	metricNFSHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "miroir_gateway_nfs_healthy",
		Help: "1 when the last liveness probe's NFS NULL call succeeded against the local ganesha; 0 when ganesha accepted the TCP connection but did not answer RPC (wedged). Sampled at the kubelet's probe cadence.",
	}, []string{"volume"})
)

func init() {
	gatewayRegistry.MustRegister(metricNFSHealthy)
}

// health serves the gateway's operational endpoint: /healthz for the pod's
// liveness probe and /metrics for the gateway PodMonitor.
type health struct {
	volume  string
	nfsAddr string
	timeout time.Duration
	// serving flips once ganesha has started. Before that /healthz always
	// passes: staging legitimately blocks for minutes on failover (waiting
	// for DRBD to release the dead gateway's Primary claim), and a liveness
	// kill during that wait would restart the pod and reset the wait.
	serving atomic.Bool
}

func (h *health) healthz(w http.ResponseWriter, _ *http.Request) {
	if !h.serving.Load() {
		_, _ = io.WriteString(w, "staging")
		return
	}
	if err := nfsNull(h.nfsAddr, h.timeout); err != nil {
		metricNFSHealthy.WithLabelValues(h.volume).Set(0)
		http.Error(w, "nfs probe: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	metricNFSHealthy.WithLabelValues(h.volume).Set(1)
	_, _ = io.WriteString(w, "ok")
}

// serve runs the HTTP endpoint until ctx is cancelled. Failure to bind is
// fatal via the returned channel: a gateway whose liveness endpoint never
// answers would be killed by the kubelet anyway, so fail loudly at startup.
func (h *health) serve(ctx context.Context, addr string, log logr.Logger) <-chan error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	mux.Handle("/metrics", promhttp.HandlerFor(gatewayRegistry, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		log.Info("health endpoint serving", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errc <- fmt.Errorf("health endpoint: %w", err)
		}
	}()
	return errc
}

// nfsNull performs an ONC RPC NULL call (program 100003, NFSv4, procedure
// 0) over TCP — the protocol-level "are you alive" ping every RPC server
// must answer. This is what distinguishes a healthy ganesha from a wedged
// one: a wedged server still accepts TCP connections (so the readiness
// TCPSocket probe stays green) but never answers RPC.
func nfsNull(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // read-side probe; nothing to flush
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}

	// RFC 5531 call body (40 bytes) behind a record mark: xid, CALL(0),
	// RPC v2, NFS program, v4, NULL proc, AUTH_NONE cred + verf.
	xid := uint32(time.Now().UnixNano()) //nolint:gosec // wrap-around is fine for an xid
	var call [44]byte
	binary.BigEndian.PutUint32(call[0:], 0x80000000|40) // last fragment, 40-byte body
	binary.BigEndian.PutUint32(call[4:], xid)
	binary.BigEndian.PutUint32(call[12:], 2)      // RPC version
	binary.BigEndian.PutUint32(call[16:], 100003) // NFS program number
	binary.BigEndian.PutUint32(call[20:], 4)      // NFSv4
	// msg_type CALL, proc NULL, and the two AUTH_NONE blocks are all zeros.
	if _, err := conn.Write(call[:]); err != nil {
		return fmt.Errorf("send: %w", err)
	}

	var mark [4]byte
	if _, err := io.ReadFull(conn, mark[:]); err != nil {
		return fmt.Errorf("read record mark: %w", err)
	}
	rm := binary.BigEndian.Uint32(mark[:])
	if rm&0x80000000 == 0 {
		return fmt.Errorf("fragmented reply")
	}
	n := rm & 0x7fffffff
	if n < 16 || n > 1024 {
		return fmt.Errorf("implausible reply length %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		return fmt.Errorf("read reply: %w", err)
	}
	if got := binary.BigEndian.Uint32(body[0:]); got != xid {
		return fmt.Errorf("xid mismatch: sent %d, got %d", xid, got)
	}
	if mt := binary.BigEndian.Uint32(body[4:]); mt != 1 {
		return fmt.Errorf("not a reply (msg_type %d)", mt)
	}
	if rs := binary.BigEndian.Uint32(body[8:]); rs != 0 {
		return fmt.Errorf("rpc denied (reply_stat %d)", rs)
	}
	// Skip the opaque verifier (flavor + length + padded body), then check
	// accept_stat.
	vlen := int(binary.BigEndian.Uint32(body[16:]))
	if vlen > 400 {
		return fmt.Errorf("implausible verifier length %d", vlen)
	}
	off := 20 + ((vlen + 3) &^ 3)
	if len(body) < off+4 {
		return fmt.Errorf("short reply (%d bytes)", len(body))
	}
	if as := binary.BigEndian.Uint32(body[off:]); as != 0 {
		return fmt.Errorf("call not accepted (accept_stat %d)", as)
	}
	return nil
}
