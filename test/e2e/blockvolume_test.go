//go:build e2e

package e2e

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Block volumeMode: the device is published raw (no mkfs/mount), so this
// exercises the block NodePublish path the Filesystem specs don't.
var _ = Describe("miroir-local block volume", Ordered, func() {
	const ns = "miroir-e2e-block"
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

	It("provisions a raw block device and round-trips data", func() {
		Expect(k8s.Create(ctx, blockPVC(ns, "blk", storageClass, "1Gi"))).To(Succeed())
		Expect(k8s.Create(ctx, blockPod(ns, "blkpod", "blk"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "blkpod")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "blk"))

		want := kubectlExec(ns, "blkpod",
			"dd if=/dev/urandom of=/tmp/pat bs=1M count=16 2>/dev/null && "+
				"dd if=/tmp/pat of=/dev/xvda bs=1M count=16 conv=fsync 2>/dev/null && "+
				"sha256sum /tmp/pat | cut -d' ' -f1")
		got := kubectlExec(ns, "blkpod",
			"dd if=/dev/xvda bs=1M count=16 2>/dev/null | sha256sum | cut -d' ' -f1")
		Expect(got).To(Equal(want), "data read back from the raw block device differs")
	})
})
