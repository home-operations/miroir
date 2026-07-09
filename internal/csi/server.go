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

// Package csi implements the miroir.home-operations.com CSI driver services (notes/DESIGN.md §6).
package csi

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	ctrl "sigs.k8s.io/controller-runtime"
)

var log = ctrl.Log.WithName("csi")

// gracefulStopTimeout bounds the drain of in-flight CSI RPCs at shutdown;
// kept under the manager's runnable shutdown grace so a hung RPC fails a
// single call, not the whole process teardown.
const gracefulStopTimeout = 10 * time.Second

// Serve listens on a unix socket and serves the given CSI services until ctx
// is cancelled. controller and node may each be nil (controller pod serves
// Identity+Controller; agent pod serves Identity+Node).
func Serve(ctx context.Context, socketPath string, identity csi.IdentityServer, controller csi.ControllerServer, node csi.NodeServer) error {
	// Remove a stale socket from a previous run; fail if the path is a
	// foreign file rather than a socket.
	if info, err := os.Stat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refusing to remove non-socket %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return err
		}
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socketPath, err)
	}

	srv := grpc.NewServer(grpc.UnaryInterceptor(logInterceptor))
	if identity != nil {
		csi.RegisterIdentityServer(srv, identity)
	}
	if controller != nil {
		csi.RegisterControllerServer(srv, controller)
	}
	if node != nil {
		csi.RegisterNodeServer(srv, node)
	}

	go func() {
		<-ctx.Done()
		// An RPC blocked in the kernel (a stuck mount, a device frozen
		// under a stranded barrier) can hang GracefulStop past the
		// manager's runnable grace, failing the whole shutdown. Fall
		// back to a hard stop so Serve always returns.
		done := make(chan struct{})
		go func() { srv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(gracefulStopTimeout):
			srv.Stop()
		}
	}()
	log.Info("serving CSI", "socket", socketPath)
	return srv.Serve(lis)
}

func logInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		log.Error(err, "rpc failed", "method", info.FullMethod)
	} else {
		log.V(1).Info("rpc ok", "method", info.FullMethod)
	}
	return resp, err
}
