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

// Package integration runs against a real (envtest) apiserver. It exists
// for behavior only the apiserver exhibits — CRD structural schemas, CEL
// validation rules, and watch delivery semantics — which no fake client or
// unit test exercises.
package integration

import (
	"context"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	scheme    *runtime.Scheme
	k8sClient client.Client
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "integration suite (envtest apiserver: CRD schema + CEL)")
}

var _ = BeforeSuite(func() {
	ctx, cancel = context.WithCancel(context.Background())
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())

	scheme = runtime.NewScheme()
	Expect(miroirv1alpha1.AddToScheme(scheme)).To(Succeed())
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	cancel()
	Expect(testEnv.Stop()).To(Succeed())
})
