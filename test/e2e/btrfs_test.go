//go:build e2e

package e2e

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// btrfs on miroir-local's lvmthin pool. The upstream storage suite has no
// btrfs patterns, so this is the only on-hardware proof of the format,
// mount-option, grow, and restore paths for it. The restore lands on the
// source's node (a local snapshot never leaves it), so it also proves the
// kernel mounts a block-level copy beside its still-mounted source despite
// the shared filesystem UUID.
var _ = Describe("btrfs volume lifecycle", Ordered, func() {
	const (
		ns    = "miroir-e2e-btrfs"
		class = "miroir-e2e-btrfs"
	)
	ctx := context.Background()

	var sumA string
	var failed bool

	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}))).To(Succeed())
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, btrfsClass(class, "1")))).To(Succeed())
	})
	AfterEach(func() { failed = failed || CurrentSpecReport().Failed() })
	AfterAll(func() {
		if failed {
			GinkgoWriter.Printf("specs failed — leaving namespace %q for diagnostics\n", ns)
			return
		}
		_ = k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		_ = k8s.Delete(ctx, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class}})
	})

	It("formats btrfs and mounts it with the class's mount options", func() {
		Expect(k8s.Create(ctx, pvc(ns, "btrfs-data", class, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "btrfs-writer", "btrfs-data"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "btrfs-writer")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "btrfs-data"))

		mnt := kubectlExec(ns, "btrfs-writer", "awk '$2 == \"/data\"' /proc/mounts")
		Expect(mnt).To(ContainSubstring(" btrfs "))
		Expect(mnt).To(ContainSubstring("compress=zstd"))
	})

	It("restores a snapshot beside its mounted source", func() {
		sumA = writeSeed(ns, "btrfs-writer", "/data/seed")
		Expect(k8s.Create(ctx, volumeSnapshot(ns, "btrfs-snap", "btrfs-data"))).To(Succeed())
		eventuallySnapshotReady(ctx, ns, "btrfs-snap")
		sumB := writeSeed(ns, "btrfs-writer", "/data/seed")

		Expect(k8s.Create(ctx, pvc(ns, "btrfs-restored", class, "1Gi", ptr("btrfs-snap")))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "btrfs-reader", "btrfs-restored"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "btrfs-reader")

		var writer, reader corev1.Pod
		Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "btrfs-writer"}, &writer)).To(Succeed())
		Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "btrfs-reader"}, &reader)).To(Succeed())
		Expect(reader.Spec.NodeName).To(Equal(writer.Spec.NodeName))

		Expect(sha(ns, "btrfs-reader", "/data/seed")).To(Equal(sumA))
		Expect(sha(ns, "btrfs-writer", "/data/seed")).To(Equal(sumB))
	})

	It("expands the volume online", func() {
		before := dataKB(ns, "btrfs-writer")
		var p corev1.PersistentVolumeClaim
		Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "btrfs-data"}, &p)).To(Succeed())
		Expect(k8s.Patch(ctx, &p, client.RawPatch(client.Merge.Type(),
			[]byte(`{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}`)))).To(Succeed())
		Eventually(func() int { return dataKB(ns, "btrfs-writer") }).
			Should(BeNumerically(">", before), "filesystem did not grow online")
	})

	It("deletes everything and cleans up the MiroirVolumes", func() {
		dataPV := boundPV(ctx, ns, "btrfs-data")
		restoredPV := boundPV(ctx, ns, "btrfs-restored")
		Expect(k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
		eventuallyGone(ctx, &miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: dataPV}})
		eventuallyGone(ctx, &miroirv1alpha1.MiroirVolume{ObjectMeta: metav1.ObjectMeta{Name: restoredPV}})
	})
})

// A replicated btrfs snapshot must capture writes still in the page
// cache: the cut lands well inside the kernel's 30s dirty-expiry window,
// so only the barrier's freeze and DRBD suspend-io writeback get them to
// the legs. The restore then mounts the copy, whose filesystem UUID
// matches its source's, wherever the scheduler puts the reader.
var _ = Describe("replicated btrfs snapshot barrier", Ordered, func() {
	const (
		ns    = "miroir-e2e-btrfs-rep"
		class = "miroir-e2e-btrfs-rep"
	)
	ctx := context.Background()
	var failed bool

	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}))).To(Succeed())
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, btrfsClass(class, "2")))).To(Succeed())
	})
	AfterEach(func() { failed = failed || CurrentSpecReport().Failed() })
	AfterAll(func() {
		if failed {
			GinkgoWriter.Printf("specs failed — leaving namespace %q for diagnostics\n", ns)
			return
		}
		_ = k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		_ = k8s.Delete(ctx, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class}})
	})

	It("captures page-cache writes in a replicated snapshot", func() {
		Expect(k8s.Create(ctx, pvc(ns, "rep-data", class, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "rep-writer", "rep-data"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "rep-writer")
		eventuallyMiroirVolumeReady(ctx, boundPV(ctx, ns, "rep-data"))

		want := kubectlExec(ns, "rep-writer",
			"dd if=/dev/urandom of=/data/unsynced bs=1M count=16 2>/dev/null && "+
				"sha256sum /data/unsynced | cut -d' ' -f1")
		Expect(k8s.Create(ctx, volumeSnapshot(ns, "rep-snap", "rep-data"))).To(Succeed())
		eventuallySnapshotReady(ctx, ns, "rep-snap")

		Expect(k8s.Create(ctx, pvc(ns, "rep-restored", class, "1Gi", ptr("rep-snap")))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "rep-reader", "rep-restored"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "rep-reader")
		Expect(sha(ns, "rep-reader", "/data/unsynced")).To(Equal(want),
			"page-cache writes missed the replicated snapshot")
	})
})

// btrfsClass builds a btrfs StorageClass on the default lvmthin pool with
// zstd compression, the configuration docs/configuration.md documents.
func btrfsClass(name, replicas string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: name},
		Provisioner:          "miroir.home-operations.com",
		VolumeBindingMode:    ptr(storagev1.VolumeBindingWaitForFirstConsumer),
		AllowVolumeExpansion: ptr(true),
		ReclaimPolicy:        ptr(corev1.PersistentVolumeReclaimDelete),
		MountOptions:         []string{"compress=zstd"},
		Parameters: map[string]string{
			"miroir.home-operations.com/replicas": replicas,
			"csi.storage.k8s.io/fstype":           "btrfs",
		},
	}
}
