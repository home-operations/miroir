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

package main

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

func TestCSIServerRunsOnEveryReplica(t *testing.T) {
	// Under leader election (#132) the CSI server must not wait for the
	// Lease: each pod's sidecars reach the driver over the pod-local
	// socket, so a standby whose gRPC server never comes up would strand
	// its sidecars the moment one of them wins its own lease.
	var r manager.LeaderElectionRunnable = csiRunnable{}
	if r.NeedLeaderElection() {
		t.Fatal("csiRunnable must opt out of leader election")
	}
}

func TestTransientAPIError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unauthorized", apierrors.NewUnauthorized("no creds"), false},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{}, "x", errors.New("rbac")), false},
		{"notfound", apierrors.NewNotFound(schema.GroupResource{}, "x"), false},
		// The failure that crashed the agent on startup: a dial error while
		// the control plane is still recovering — must be retried.
		{"dial error", errors.New("dial tcp 10.43.0.1:443: connect: no route to host"), true},
		{"service unavailable", apierrors.NewServiceUnavailable("starting"), true},
	}
	for _, tc := range cases {
		if got := transientAPIError(tc.err); got != tc.want {
			t.Errorf("%s: transientAPIError = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestListWithRetrySucceeds(t *testing.T) {
	s := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	if err := listWithRetry(c, &miroirv1alpha1.MiroirVolumeList{}, apiStartupWait); err != nil {
		t.Fatal(err)
	}
}

func TestListWithRetryReturnsTerminalErrorImmediately(t *testing.T) {
	s := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	// A terminal (non-transient) error must abort the wait at once rather
	// than block for apiStartupWait — otherwise a misconfiguration would
	// look like a slow startup.
	c := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return apierrors.NewUnauthorized("no creds")
			},
		}).Build()
	if err := listWithRetry(c, &miroirv1alpha1.MiroirVolumeList{}, apiStartupWait); !apierrors.IsUnauthorized(err) {
		t.Fatalf("want the terminal Unauthorized returned, got %v", err)
	}
}
