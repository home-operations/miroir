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

package csi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A panicking handler must surface as codes.Internal, not unwind past the
// interceptor and crash the process.
func TestRecoverInterceptorRecoversPanic(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodePublishVolume"}
	resp, err := recoverInterceptor(context.Background(), nil, info,
		func(context.Context, any) (any, error) { panic("boom") })
	if resp != nil {
		t.Fatalf("expected nil response after panic, got %v", resp)
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (err=%v)", got, err)
	}
}

// captureLog swaps the package logger for one that records emitted lines at
// verbosity 0, so V(1) output is dropped and only error-level lines land.
func captureLog(t *testing.T) *[]string {
	t.Helper()
	var lines []string
	orig := log
	log = funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{})
	t.Cleanup(func() { log = orig })
	return &lines
}

// Retryable codes are flow control (a settling group snapshot answers
// Aborted until ready), so they must not log at error level; the error
// itself still passes through to the CO unchanged.
func TestLogInterceptorRetryableNotErrorLevel(t *testing.T) {
	for _, code := range []codes.Code{codes.Aborted, codes.Unavailable, codes.DeadlineExceeded} {
		lines := captureLog(t)
		info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.GroupController/CreateVolumeGroupSnapshot"}
		want := status.Errorf(code, "not ready")
		_, err := logInterceptor(context.Background(), nil, info,
			func(context.Context, any) (any, error) { return nil, want })
		if !errors.Is(err, want) {
			t.Fatalf("%v: error not passed through: got %v", code, err)
		}
		if len(*lines) != 0 {
			t.Fatalf("%v: expected no error-level lines, got %v", code, *lines)
		}
	}
}

// Genuinely unexpected codes keep the error-level log.
func TestLogInterceptorUnexpectedIsErrorLevel(t *testing.T) {
	lines := captureLog(t)
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/CreateVolume"}
	_, err := logInterceptor(context.Background(), nil, info,
		func(context.Context, any) (any, error) { return nil, status.Errorf(codes.Internal, "boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("error not passed through: got %v", err)
	}
	if len(*lines) != 1 || !strings.Contains((*lines)[0], "rpc failed") {
		t.Fatalf("expected one rpc failed line, got %v", *lines)
	}
}

// The healthy path is untouched: the handler's response and error pass through.
func TestRecoverInterceptorPassesThrough(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodeGetInfo"}
	want := &struct{}{}
	resp, err := recoverInterceptor(context.Background(), nil, info,
		func(context.Context, any) (any, error) { return want, nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != want {
		t.Fatalf("response not passed through: got %v, want %v", resp, want)
	}
}
