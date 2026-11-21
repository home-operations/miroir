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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/agent"
	"github.com/home-operations/miroir/internal/export"
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

// discoveryServer serves the discovery endpoints the way an API server
// with only the core group does, answering each request with the status
// code respond returns for its path.
func discoveryServer(t *testing.T, respond func(path string) int) *rest.Config {
	t.Helper()
	bodies := map[string]string{
		"/api":  `{"kind":"APIVersions","versions":["v1"]}`,
		"/apis": `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`,
		"/api/v1": `{"kind":"APIResourceList","groupVersion":"v1",` +
			`"resources":[{"name":"nodes","namespaced":false,"kind":"Node","verbs":["get","list","watch"]}]}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		code := respond(r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if code == http.StatusOK {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}
}

// warmNodeMapping runs warmRESTMapper for the agent's corev1.Node cache
// entry against a fresh dynamic mapper on cfg.
func warmNodeMapping(t *testing.T, cfg *rest.Config, objs map[client.Object]cache.ByObject) error {
	t.Helper()
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mapper, err := apiutil.NewDynamicRESTMapper(cfg, hc)
	if err != nil {
		t.Fatal(err)
	}
	return warmRESTMapper(mapper, scheme, objs, agent.APIStartupWait)
}

var nodeByObject = map[client.Object]cache.ByObject{&corev1.Node{}: {}}

func TestWarmRESTMapperRetriesTransientFailure(t *testing.T) {
	// The reboot race: the first discovery call fails while the node's
	// networking is still coming up, and the process must retry rather
	// than exit — this is the manager's own first API access.
	var probes atomic.Int32
	cfg := discoveryServer(t, func(path string) int {
		if path == "/api" && probes.Add(1) == 1 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})
	if err := warmNodeMapping(t, cfg, nodeByObject); err != nil {
		t.Fatal(err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("want one retry after the transient failure, got %d probe(s)", got)
	}
}

func TestWarmRESTMapperReturnsTerminalErrorImmediately(t *testing.T) {
	cfg := discoveryServer(t, func(string) int { return http.StatusUnauthorized })
	start := time.Now()
	err := warmNodeMapping(t, cfg, nodeByObject)
	if !apierrors.IsUnauthorized(err) {
		t.Fatalf("want the terminal Unauthorized returned, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("terminal error took %v to surface; must not wait out the budget", elapsed)
	}
}

func TestWarmRESTMapperMissingCRDIsTerminal(t *testing.T) {
	// A kind the server does not serve means the CRDs are not installed:
	// fail at once, as NewManager did before the mapper was warmed.
	cfg := discoveryServer(t, func(string) int { return http.StatusOK })
	start := time.Now()
	err := warmNodeMapping(t, cfg, map[client.Object]cache.ByObject{&miroirv1alpha1.MiroirNode{}: {}})
	if !meta.IsNoMatchError(err) {
		t.Fatalf("want a NoMatch error returned, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("terminal error took %v to surface; must not wait out the budget", elapsed)
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
	shutdown := armSignalShutdown(signalCtx, func() { close(cleaned) })
	cancel()
	if !shutdown.finish() {
		t.Fatal("signal cleanup must win after signal cancellation")
	}
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("signal cleanup did not run")
	}
}

func TestArmSignalShutdownFinishesForInternalRestart(t *testing.T) {
	signalCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleaned := make(chan struct{})
	shutdown := armSignalShutdown(signalCtx, func() { close(cleaned) })
	if shutdown.finish() {
		t.Fatal("internal restart must win before signal cancellation")
	}
	select {
	case <-cleaned:
		t.Fatal("internal manager cancellation ran shutdown cleanup")
	default:
	}
}

func TestFinishManagerShutdownInternalRestartOnlySweepsBarrier(t *testing.T) {
	signalCtx, cancelSignal := context.WithCancel(t.Context())
	defer cancelSignal()
	managerCtx, cancelManager := context.WithCancel(t.Context())
	cancelManager()
	var full, barrier int
	unexpectedCleanup := make(chan struct{}, 1)
	shutdown := armSignalShutdown(signalCtx, func() { unexpectedCleanup <- struct{}{} })
	if err := finishManagerShutdown(nil, signalCtx, managerCtx,
		func() { full++ }, func() { barrier++ }, shutdown); err != nil {
		t.Fatalf("finish manager shutdown: %v", err)
	}
	if full != 0 || barrier != 1 {
		t.Fatalf("full = %d, barrier = %d; want 0, 1", full, barrier)
	}
	select {
	case <-unexpectedCleanup:
		t.Fatal("internal restart ran early cleanup")
	default:
	}
}

func TestFinishManagerShutdownSignalBeforeManagerStopRunsFullSweep(t *testing.T) {
	signalCtx, cancelSignal := context.WithCancel(t.Context())
	managerCtx, cancelManager := context.WithCancel(t.Context())
	cancelSignal()
	cancelManager()
	var full, barrier int
	shutdown := armSignalShutdown(signalCtx, func() {})
	if err := finishManagerShutdown(nil, signalCtx, managerCtx,
		func() { full++ }, func() { barrier++ }, shutdown); err != nil {
		t.Fatalf("finish manager shutdown: %v", err)
	}
	if full != 1 || barrier != 0 {
		t.Fatalf("full = %d, barrier = %d; want 1, 0", full, barrier)
	}
}

func TestFinishManagerShutdownSignalDuringBarrierRunsFullSweep(t *testing.T) {
	signalCtx, cancelSignal := context.WithCancel(t.Context())
	defer cancelSignal()
	managerCtx, cancelManager := context.WithCancel(t.Context())
	cancelManager()
	earlyCleaned := make(chan struct{})
	var full, barrier int
	shutdown := armSignalShutdown(signalCtx, func() { close(earlyCleaned) })
	if err := finishManagerShutdown(nil, signalCtx, managerCtx, func() { full++ }, func() {
		barrier++
		cancelSignal()
	}, shutdown); err != nil {
		t.Fatalf("finish manager shutdown: %v", err)
	}
	if full != 1 || barrier != 1 {
		t.Fatalf("full = %d, barrier = %d; want 1, 1", full, barrier)
	}
	select {
	case <-earlyCleaned:
	default:
		t.Fatal("signal cleanup did not complete")
	}
}

func TestParseGatewayResources(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    corev1.ResourceRequirements
		wantErr bool
	}{
		{
			name: "empty selects the default without a memory limit",
			raw:  "",
			want: export.DefaultResources(),
		},
		{
			name: "JSON from the chart is decoded as given",
			raw:  `{"requests":{"cpu":"10m","memory":"64Mi"},"limits":{"memory":"1Gi"}}`,
			want: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("10m"),
					corev1.ResourceMemory: resource.MustParse("64Mi"),
				},
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
			},
		},
		{
			name: "an explicit empty object means no requests or limits at all",
			raw:  `{}`,
			want: corev1.ResourceRequirements{},
		},
		{name: "malformed JSON is rejected", raw: `{"requests":`, wantErr: true},
		{name: "an unknown field is rejected rather than silently dropped", raw: `{"limit":{"memory":"1Gi"}}`, wantErr: true},
		{name: "trailing data is rejected", raw: `{} {}`, wantErr: true},
		{name: "a stray closing brace is rejected", raw: `{}}`, wantErr: true},
		{name: "a stray closing bracket is rejected", raw: `{}]`, wantErr: true},
		{name: "a bad quantity is rejected", raw: `{"limits":{"memory":"lots"}}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGatewayResources(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !apiequality.Semantic.DeepEqual(got, tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
