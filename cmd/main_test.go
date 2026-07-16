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
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

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

// crdScheme builds a scheme carrying the apiextensions types the CRD guard
// reads.
func crdScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func miroirNodeCRD(annotations map[string]string) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        miroirv1alpha1.MiroirNodeCRDName,
			Annotations: annotations,
		},
	}
}

func TestCheckMiroirNodeCRD(t *testing.T) {
	cases := map[string]struct {
		crd     *apiextensionsv1.CustomResourceDefinition
		wantErr bool
	}{
		"current revision passes": {
			crd: miroirNodeCRD(map[string]string{
				miroirv1alpha1.SchemaRevisionAnnotation: miroirv1alpha1.MiroirNodeSchemaRevision,
			}),
		},
		"stale revision refuses": {
			crd: miroirNodeCRD(map[string]string{
				miroirv1alpha1.SchemaRevisionAnnotation: "0",
			}),
			wantErr: true,
		},
		"pre-revision CRD (no annotation) refuses": {
			crd:     miroirNodeCRD(nil),
			wantErr: true,
		},
		"missing CRD refuses": {wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(crdScheme(t))
			if tc.crd != nil {
				b = b.WithObjects(tc.crd)
			}
			err := checkMiroirNodeCRD(b.Build(), apiStartupWait)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
		})
	}
}

// The guard compares the served CRD against MiroirNodeSchemaRevision, so
// the CRD the chart ships must carry exactly that revision — this pins the
// constant and the +kubebuilder:metadata marker together.
func TestChartCRDCarriesCurrentSchemaRevision(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "charts", "miroir", "crds",
		"miroir.home-operations.com_miroirnodes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, crd); err != nil {
		t.Fatal(err)
	}
	if got := crd.Annotations[miroirv1alpha1.SchemaRevisionAnnotation]; got != miroirv1alpha1.MiroirNodeSchemaRevision {
		t.Fatalf("chart CRD carries schema revision %q, the binary expects %q — regenerate with `mise run helm-crds` "+
			"or bump the +kubebuilder:metadata marker with the constant", got, miroirv1alpha1.MiroirNodeSchemaRevision)
	}
}
