//go:build e2e && replicated

// Replicated (DRBD) e2e coverage — issue #139 / #125. Gated behind the extra
// `replicated` build tag so the default `local` and `conformance` suites
// (single-node kind, no DRBD module) never compile or run it. Drive it with
// `mise run e2e-run-replicated` against a cluster that has the DRBD kernel
// module and >=2 diskful nodes.
package e2e

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// replicatedClass is the chart's default 2-diskful + tie-breaker class.
const replicatedClass = "miroir-replicated"

// eventuallyTwoUpToDateLegs asserts the volume converges to at least two
// diskful legs that are UpToDate and connected, with no leg split-brain —
// the health signal smoke.sh checks at the phase level, made per-leg.
func eventuallyTwoUpToDateLegs(ctx context.Context, pvName string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var v miroirv1alpha1.MiroirVolume
		g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pvName}, &v)).To(Succeed())
		upToDate := 0
		for node, st := range v.Status.PerNode {
			g.Expect(st.SplitBrain).To(BeFalse(), "leg on %s is split-brain (%s)", node, pvName)
			if !st.Diskless && st.DiskState == "UpToDate" && st.Connected {
				upToDate++
			}
		}
		g.Expect(upToDate).To(BeNumerically(">=", 2),
			"want >=2 UpToDate+connected diskful legs, got PerNode=%+v", v.Status.PerNode)
	}).Should(Succeed())
}

var _ = Describe("miroir-replicated volume health", Ordered, func() {
	const ns = "miroir-e2e-repl"
	ctx := context.Background()
	var failed bool

	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}))).To(Succeed())
	})
	AfterEach(func() { failed = failed || CurrentSpecReport().Failed() })
	AfterAll(func() {
		if failed {
			GinkgoWriter.Printf("specs failed — leaving namespace %q for diagnostics\n", ns)
			return
		}
		_ = k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})

	// Regression for the concurrent-bootstrap burst (#139 case B): several
	// replicated volumes provisioned at once must all reach two UpToDate legs.
	It("provisions several replicated PVCs that each reach two UpToDate legs", func() {
		const n = 3
		claims := make([]string, n)
		for i := range claims {
			claims[i] = fmt.Sprintf("burst-%d", i)
			Expect(k8s.Create(ctx, pvc(ns, claims[i], replicatedClass, "1Gi", nil))).To(Succeed())
			Expect(k8s.Create(ctx, pod(ns, "pod-"+claims[i], claims[i]))).To(Succeed())
		}
		for _, claim := range claims {
			eventuallyPodReady(ctx, ns, "pod-"+claim)
			pv := boundPV(ctx, ns, claim)
			eventuallyMiroirVolumeReady(ctx, pv)
			eventuallyTwoUpToDateLegs(ctx, pv)
		}
	})

	// Regression for the deterministic delete→recreate (#139 case A): a fresh
	// volume created under a reused claim name must come up healthy, i.e.
	// teardown fully released the prior volume's DRBD state.
	It("recreates a replicated PVC under the same claim without split-brain", func() {
		const claim = "recycle"
		create := func() string {
			Expect(k8s.Create(ctx, pvc(ns, claim, replicatedClass, "1Gi", nil))).To(Succeed())
			Expect(k8s.Create(ctx, pod(ns, "pod-"+claim, claim))).To(Succeed())
			eventuallyPodReady(ctx, ns, "pod-"+claim)
			pv := boundPV(ctx, ns, claim)
			eventuallyTwoUpToDateLegs(ctx, pv)
			return pv
		}
		destroy := func(pv string) {
			Expect(k8s.Delete(ctx, pod(ns, "pod-"+claim, claim))).To(Succeed())
			eventuallyGone(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pod-" + claim}})
			Expect(k8s.Delete(ctx, pvc(ns, claim, replicatedClass, "1Gi", nil))).To(Succeed())
			eventuallyGone(ctx, &miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: pv}})
		}

		first := create()
		destroy(first)
		second := create()
		Expect(second).NotTo(Equal(first), "recreate must be a distinct PV")
		destroy(second)
	})
})
