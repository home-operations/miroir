//go:build e2e

package e2e

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// Issue #305: a restore may request a different replica count than its
// source. Growing crosses the replication boundary (miroir-local →
// miroir-replicated): the clone gains fresh DRBD metadata on a padded
// backing, mints its data generation, and full-syncs the joiner — this
// spec is the real-hardware proof of that whole chain, including that
// the metadata-overhead bound leaves the restored filesystem intact and
// writable. Shrinking (miroir-replicated → miroir-local) drops the DRBD
// layer, leaving the clone's embedded metadata as inert tail bytes.
var _ = Describe("restore replica reshape", Ordered, func() {
	const ns = "miroir-e2e-shape"
	ctx := context.Background()

	var upSum, downSum string
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

	It("expands an unreplicated volume into a replicated restore (1→2)", func() {
		Expect(k8s.Create(ctx, pvc(ns, "shape-src", storageClass, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "shape-src-writer", "shape-src"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "shape-src-writer")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "shape-src"))
		upSum = writeSeed(ns, "shape-src-writer", "/data/seed")

		Expect(k8s.Create(ctx, volumeSnapshot(ns, "shape-src-snap", "shape-src"))).To(Succeed())
		eventuallySnapshotReady(ctx, ns, "shape-src-snap")

		Expect(k8s.Create(ctx, pvc(ns, "shape-up", replicatedClass, "1Gi", ptr("shape-src-snap")))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "shape-up-reader", "shape-up"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "shape-up-reader")

		pv := boundPV(ctx, ns, "shape-up")
		eventuallyMiroirVolumeReady(ctx, pv)
		var v miroirv1alpha1.MiroirVolume
		Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
		Expect(v.Spec.DRBD).NotTo(BeNil(), "expanded restore must be replicated")
		Expect(v.Spec.DiskfulReplicas()).To(HaveLen(2))
		Expect(v.Spec.Source.PadForMetadata).To(BeTrue(), "boundary-crossing restore must pad its backings")

		// Point-in-time content survived the boundary crossing, and the
		// padded device still accepts writes past the source's fill line —
		// the on-hardware proof that create-md landed in the pad, not over
		// the filesystem tail.
		Expect(sha(ns, "shape-up-reader", "/data/seed")).To(Equal(upSum))
		kubectlExec(ns, "shape-up-reader", "dd if=/dev/zero of=/data/extra bs=1M count=64 2>/dev/null && sync")
	})

	It("shrinks a replicated volume into an unreplicated restore (2→1)", func() {
		Expect(k8s.Create(ctx, pvc(ns, "shape-rep", replicatedClass, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "shape-rep-writer", "shape-rep"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "shape-rep-writer")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "shape-rep"))
		downSum = writeSeed(ns, "shape-rep-writer", "/data/seed")

		Expect(k8s.Create(ctx, volumeSnapshot(ns, "shape-rep-snap", "shape-rep"))).To(Succeed())
		eventuallySnapshotReady(ctx, ns, "shape-rep-snap")

		Expect(k8s.Create(ctx, pvc(ns, "shape-down", storageClass, "1Gi", ptr("shape-rep-snap")))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "shape-down-reader", "shape-down"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "shape-down-reader")

		pv := boundPV(ctx, ns, "shape-down")
		eventuallyMiroirVolumeReady(ctx, pv)
		var v miroirv1alpha1.MiroirVolume
		Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
		Expect(v.Spec.DRBD).To(BeNil(), "shrunk restore must be unreplicated")
		Expect(v.Spec.Replicas).To(HaveLen(1))

		Expect(sha(ns, "shape-down-reader", "/data/seed")).To(Equal(downSum))
	})
})
