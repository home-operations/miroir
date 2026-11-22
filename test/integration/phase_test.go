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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/agent"
	"github.com/home-operations/miroir/internal/backend"
)

// memBackend is a pool whose devices exist only as map entries.
type memBackend struct {
	backend.Backend
	devices map[string]bool
	// createErr, when set, fails every Create.
	createErr error
}

func (b *memBackend) Create(_ context.Context, vol string, _ int64) (string, error) {
	if b.createErr != nil {
		return "", b.createErr
	}
	b.devices[vol] = true
	return b.DevicePath(vol), nil
}

func (b *memBackend) Resize(context.Context, string, int64) error { return nil }

func (b *memBackend) Exists(_ context.Context, vol string) (bool, error) {
	return b.devices[vol], nil
}

func (b *memBackend) DevicePath(vol string) string { return "/dev/mem/" + vol }

// Phase and ReadyReplicas change hands between field managers: the
// controller sets them, an agent may only blank a Ready its leg contradicts,
// and the agent's ordinary applies must then leave the controller's value
// alone. Only a real apiserver tracks that ownership, and the unit tests'
// fake client drops an applied empty string.
var _ = Describe("MiroirVolume phase ownership", func() {
	It("lets an agent withdraw a Ready and the controller derive the phase again", func() {
		const name = "phase-ownership"
		key := types.NamespacedName{Name: name}
		req := ctrl.Request{NamespacedName: key}
		Expect(k8sClient.Create(ctx, unreplicatedVolume(name))).To(Succeed())
		DeferCleanup(func() {
			vol := &miroirv1alpha1.MiroirVolume{}
			Expect(k8sClient.Get(ctx, key, vol)).To(Succeed())
			vol.Finalizers = nil
			Expect(k8sClient.Update(ctx, vol)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, vol))).To(Succeed())
		})

		pool := &memBackend{devices: map[string]bool{}}
		// A fresh reconciler per pass, as after an agent restart: nothing
		// cached lets a pass skip the backing-device probe.
		agentPass := func() error {
			r := &agent.VolumeReconciler{
				Client:   k8sClient,
				NodeName: nodeA,
				Pools: agent.Pools{miroirv1alpha1.DefaultPoolName: {
					Backend: pool, Type: miroirv1alpha1.BackendLVMThin,
				}},
			}
			_, err := r.Reconcile(ctx, req)
			return err
		}
		controllerPass := func() {
			_, err := (&agent.PhaseReconciler{Client: k8sClient}).Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
		}
		status := func() miroirv1alpha1.MiroirVolumeStatus {
			vol := &miroirv1alpha1.MiroirVolume{}
			Expect(k8sClient.Get(ctx, key, vol)).To(Succeed())
			return vol.Status
		}

		Expect(agentPass()).To(Succeed())
		Expect(status().PerNode[nodeA].DeviceCreated).To(BeTrue())
		Expect(status().Phase).To(BeEmpty(), "an agent never sets a phase")

		controllerPass()
		Expect(status().Phase).To(Equal(miroirv1alpha1.VolumeReady))
		Expect(status().ReadyReplicas).To(Equal("1/1"))

		Expect(agentPass()).To(Succeed())
		Expect(status().Phase).To(Equal(miroirv1alpha1.VolumeReady),
			"an agent whose leg stays in service leaves the phase alone")

		// The backing device vanishes and the pool cannot re-create it:
		// the leg is out of service.
		delete(pool.devices, name)
		pool.createErr = errors.New("pool offline")
		Expect(agentPass()).To(MatchError(pool.createErr))
		Expect(status().PerNode[nodeA].DeviceCreated).To(BeFalse())
		Expect(status().Phase).To(BeEmpty(), "the Ready must not survive the leg leaving service")
		Expect(status().ReadyReplicas).To(BeEmpty())

		// A Ready derived from the older slots lands late. The leg is
		// still out of service, so the next pass withdraws that one too,
		// in its only apply, and is left owning the blanked fields.
		vol := &miroirv1alpha1.MiroirVolume{}
		Expect(k8sClient.Get(ctx, key, vol)).To(Succeed())
		base := vol.DeepCopy()
		vol.Status.Phase, vol.Status.ReadyReplicas = miroirv1alpha1.VolumeReady, "1/1"
		Expect(k8sClient.Status().Patch(ctx, vol, client.MergeFrom(base))).To(Succeed())
		Expect(agentPass()).To(MatchError(pool.createErr))
		Expect(status().Phase).To(BeEmpty())
		Expect(status().ReadyReplicas).To(BeEmpty())

		controllerPass()
		Expect(status().Phase).To(Equal(miroirv1alpha1.VolumeFailed),
			"the controller takes the blanked fields back from the agent")
		Expect(status().ReadyReplicas).To(Equal("0/1"))

		Expect(agentPass()).To(MatchError(pool.createErr))
		Expect(status().Phase).To(Equal(miroirv1alpha1.VolumeFailed),
			"an apply that no longer carries the fields must not drop the controller's value")
		Expect(status().ReadyReplicas).To(Equal("0/1"))

		pool.createErr = nil
		Expect(agentPass()).To(Succeed())
		Expect(status().PerNode[nodeA].DeviceCreated).To(BeTrue())
		Expect(status().Phase).To(Equal(miroirv1alpha1.VolumeFailed), "an agent never promotes the phase")

		controllerPass()
		Expect(status().Phase).To(Equal(miroirv1alpha1.VolumeReady))
		Expect(status().ReadyReplicas).To(Equal("1/1"))
	})
})
