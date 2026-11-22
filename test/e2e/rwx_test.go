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

// Issue #650: an RWX volume reaches the pod as an NFS mount from the gateway,
// and mkfs leaves its root 0:0 0755, so a non-root pod can write to it only if
// the kubelet applies the pod's fsGroup. The owners the pod reads back must be
// the real numeric IDs, not the 4294967294 an unmapped NFSv4 owner name
// becomes.
var _ = Describe("RWX fsGroup", Ordered, Label("rwx"), func() {
	const ns = "miroir-e2e-rwx"
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

	It("lets a non-root pod write to the volume root", func() {
		claim := pvc(ns, "shared", replicatedClass, "1Gi", nil)
		claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		Expect(k8s.Create(ctx, claim)).To(Succeed())

		writer := pod(ns, "writer", "shared")
		writer.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsUser:           new(int64(1000)),
			RunAsGroup:          new(int64(1000)),
			FSGroup:             new(int64(1000)),
			FSGroupChangePolicy: new(corev1.FSGroupChangeOnRootMismatch),
		}
		Expect(k8s.Create(ctx, writer)).To(Succeed())
		eventuallyPodReady(ctx, ns, "writer")

		kubectlExec(ns, "writer", "mkdir /data/dir && echo ok > /data/dir/file")
		Expect(kubectlExec(ns, "writer", "stat -c %u:%g /data")).To(Equal("0:1000"))
		Expect(kubectlExec(ns, "writer", "stat -c %u:%g /data/dir/file")).To(Equal("1000:1000"))
	})
})
