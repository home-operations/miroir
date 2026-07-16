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

package integration

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/agent"
)

// The startup race only a real apiserver reproduces: a MiroirNode deleted
// between the agent's direct startup read and the informer's initial list
// yields no watch event at all — the object is simply absent from the
// list, so neither an Add nor a Delete is ever delivered. The watcher's
// synthetic boot event is what guarantees one comparison anyway; without
// it this spec hangs and the agent would serve the stale topology
// indefinitely.
var _ = Describe("TopologyWatcher boot event", func() {
	It("restarts a storage agent whose MiroirNode vanished before the cache synced", func() {
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
		})
		Expect(err).NotTo(HaveOccurred())

		stopped := make(chan struct{})
		var once sync.Once
		w := &agent.TopologyWatcher{
			Client:   mgr.GetClient(),
			NodeName: "boot-race-node", // no MiroirNode of this name exists
			BootedPools: []miroirv1alpha1.MiroirNodePool{{
				Name: poolDefault, Backend: miroirv1alpha1.BackendLVMThin,
				LVMThin: &miroirv1alpha1.LVMThinPool{Device: deviceSDB},
			}},
			Stop: func() { once.Do(func() { close(stopped) }) },
		}
		Expect(w.SetupWithManager(mgr)).To(Succeed())

		mgrCtx, mgrCancel := context.WithCancel(ctx)
		DeferCleanup(mgrCancel)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()

		// Generous deadline: covers manager startup and cache sync, not
		// just the event delivery itself.
		Eventually(stopped, "30s").Should(BeClosed(),
			"the boot event must drive one comparison even though the apiserver delivers no event for the vanished object")
	})
})
