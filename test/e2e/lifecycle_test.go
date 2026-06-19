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

// Full CSI lifecycle on miroir-local (lvmthin, replicas:1) — the local half of
// smoke.sh; the DRBD-only steps (failover, barrier) need a real cluster.
var _ = Describe("miroir-local volume lifecycle", Ordered, func() {
	const ns = "miroir-e2e"
	ctx := context.Background()

	var sumA, sumB string // /data/seed sha at the snapshot point (A) and after mutation (B)
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

	It("provisions, mounts, and reports the MiroirVolume Ready", func() {
		Expect(k8s.Create(ctx, pvc(ns, "appdata", storageClass, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "writer", "appdata"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "writer")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "appdata"))
	})

	It("persists written data", func() {
		sumA = writeSeed(ns, "writer", "/data/seed")
		Expect(sumA).NotTo(BeEmpty())
	})

	It("takes a snapshot, then mutates the source past it", func() {
		Expect(k8s.Create(ctx, volumeSnapshot(ns, "appdata-snap", "appdata"))).To(Succeed())
		eventuallySnapshotReady(ctx, ns, "appdata-snap")

		// Overwrite after the snapshot: the snapshot must NOT see this.
		sumB = writeSeed(ns, "writer", "/data/seed")
		Expect(sumB).NotTo(Equal(sumA))
	})

	It("restores the snapshot into a new volume at the point-in-time state", func() {
		src := "appdata-snap"
		Expect(k8s.Create(ctx, pvc(ns, "restored", storageClass, "1Gi", &src))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "reader", "restored"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "reader")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "restored"))

		// The clone holds A (snapshot time), not B; the source still holds B.
		Expect(sha(ns, "reader", "/data/seed")).To(Equal(sumA))
		Expect(sha(ns, "writer", "/data/seed")).To(Equal(sumB))
	})

	It("keeps data across a pod restart (NodeUnstage/NodeStage)", func() {
		recreatePod(ctx, pod(ns, "writer", "appdata"))
		Expect(sha(ns, "writer", "/data/seed")).To(Equal(sumB))
	})

	It("expands the volume online without restart", func() {
		before := dataKB(ns, "writer")
		var p corev1.PersistentVolumeClaim
		Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "appdata"}, &p)).To(Succeed())
		Expect(k8s.Patch(ctx, &p, client.RawPatch(client.Merge.Type(),
			[]byte(`{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}`)))).To(Succeed())
		Eventually(func() int { return dataKB(ns, "writer") }).
			Should(BeNumerically(">", before), "filesystem did not grow online")
	})

	It("deletes everything and cleans up MiroirVolumes and MiroirSnapshots", func() {
		appPV := boundPV(ctx, ns, "appdata")
		restoredPV := boundPV(ctx, ns, "restored")
		Expect(k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())

		// Finalizers run backend.Delete() before the cluster-scoped objects go.
		eventuallyGone(ctx, &miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: appPV}})
		eventuallyGone(ctx, &miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: restoredPV}})
		Eventually(func(g Gomega) {
			var snaps miroirv1alpha1.MiroirSnapshotList
			g.Expect(k8s.List(ctx, &snaps)).To(Succeed())
			g.Expect(snaps.Items).To(BeEmpty(), "MiroirSnapshots not cleaned up")
		}).Should(Succeed())
	})
})
