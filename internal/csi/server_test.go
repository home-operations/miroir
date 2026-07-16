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
	"testing"

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
