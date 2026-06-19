//go:build e2e

package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// Capacity-aware placement surface (DESIGN.md §4.6): each storage agent
// publishes its pool capacity to a MiroirNode the controller reads at
// placement. Provisioning itself exercises the placement path in the
// lifecycle spec; here we assert the stats the decision rests on.
var _ = Describe("capacity-aware placement", func() {
	ctx := context.Background()

	It("publishes a MiroirNode with fresh pool stats per storage node", func() {
		Eventually(func(g Gomega) {
			var nodes miroirv1alpha1.MiroirNodeList
			g.Expect(k8s.List(ctx, &nodes)).To(Succeed())
			g.Expect(nodes.Items).NotTo(BeEmpty(), "no MiroirNodes published")
			for _, n := range nodes.Items {
				g.Expect(n.Status.CapacityBytes).To(BeNumerically(">", 0), "node %s has no capacity", n.Name)
				g.Expect(n.Status.AllocatedBytes).To(BeNumerically(">=", 0))
				g.Expect(n.Status.ObservedAt).NotTo(BeNil(), "node %s never sampled", n.Name)
				g.Expect(time.Since(n.Status.ObservedAt.Time)).To(BeNumerically("<", 5*time.Minute),
					"node %s stats are stale", n.Name)
			}
		}).Should(Succeed())
	})
})
