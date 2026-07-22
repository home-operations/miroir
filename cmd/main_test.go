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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/agent"
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

// byObjectFor resolves a type's entry in Options.ByObject: the map is
// keyed by object pointers, so lookups must match by type, not identity.
func byObjectFor[T client.Object](opts cache.Options) (cache.ByObject, bool) {
	for obj, cfg := range opts.ByObject {
		if _, ok := obj.(T); ok {
			return cfg, true
		}
	}
	return cache.ByObject{}, false
}

func TestCacheOptionsAgentPinsOwnMiroirNode(t *testing.T) {
	opts := cacheOptions("agent", "", "node-a")
	byNode, ok := byObjectFor[*miroirv1alpha1.MiroirNode](opts)
	if !ok {
		t.Fatal("agent mode must scope the MiroirNode informer")
	}
	if byNode.Field == nil || byNode.Field.String() != "metadata.name=node-a" {
		t.Fatalf("MiroirNode informer must be pinned to the node's own object, got %v", byNode.Field)
	}
}

// nodeName is validated after the manager (and its cache config) is
// built; an empty name must not produce a match-nothing selector.
func TestCacheOptionsAgentWithoutNodeNameStaysUnscoped(t *testing.T) {
	if opts := cacheOptions("agent", "", ""); opts.ByObject != nil {
		t.Fatalf("no scoping without a node name, got %v", opts.ByObject)
	}
}

func TestCacheOptionsControllerStaysClusterScopedForMiroirNodes(t *testing.T) {
	opts := cacheOptions(modeController, "miroir-system", "")
	if _, ok := byObjectFor[*appsv1.Deployment](opts); !ok {
		t.Fatal("controller mode must namespace the gateway Deployment informer")
	}
	if _, ok := byObjectFor[*corev1.Service](opts); !ok {
		t.Fatal("controller mode must namespace the gateway Service informer")
	}
	if _, ok := byObjectFor[*miroirv1alpha1.MiroirNode](opts); ok {
		t.Fatal("the controller reads every MiroirNode; its informer must stay cluster-scoped")
	}
}

func TestCacheOptionsAgentPinsOwnNode(t *testing.T) {
	opts := cacheOptions("agent", "", "node-a")
	byNode, ok := byObjectFor[*corev1.Node](opts)
	if !ok {
		t.Fatal("agent mode must scope the corev1.Node informer")
	}
	if byNode.Field == nil || byNode.Field.String() != "metadata.name=node-a" {
		t.Fatalf("Node informer must be pinned to the node's own object, got %v", byNode.Field)
	}
}

func TestArmSignalShutdownRunsCleanupOnSignal(t *testing.T) {
	signalCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleaned := make(chan struct{})
	wait, _ := armSignalShutdown(signalCtx, func() { close(cleaned) })
	cancel()
	wait()
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("signal cleanup did not run")
	}
}

func TestArmSignalShutdownDisarmsForInternalCancellation(t *testing.T) {
	signalCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleaned := make(chan struct{})
	wait, disarm := armSignalShutdown(signalCtx, func() { close(cleaned) })
	disarm()
	wait()
	select {
	case <-cleaned:
		t.Fatal("internal manager cancellation ran shutdown cleanup")
	default:
	}
}

func TestArmSignalShutdownDisarmAfterSignalRunsCleanup(t *testing.T) {
	signalCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleaned := make(chan struct{})
	wait, disarm := armSignalShutdown(signalCtx, func() { close(cleaned) })
	cancel()
	disarm()
	wait()
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("signal cleanup did not run when disarmed after signal")
	}
}

func TestAgentShutdownDownSecondariesRequiresCordon(t *testing.T) {
	// A nil driver is intentional: an uncordoned node must return before any
	// teardown is attempted.
	agentShutdownDownSecondaries(&agent.CordonWatcher{}, nil)
}
