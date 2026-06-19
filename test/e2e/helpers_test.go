//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

var (
	volumeSnapshotGVK = schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}
	snapshotAPIGroup  = "snapshot.storage.k8s.io"
	blockMode         = corev1.PersistentVolumeBlock
)

// pvc builds a Filesystem PVC; pass a non-nil src to clone from a VolumeSnapshot.
func pvc(ns, name, sc, size string, src *string) *corev1.PersistentVolumeClaim {
	p := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
	if src != nil {
		p.Spec.DataSource = &corev1.TypedLocalObjectReference{
			APIGroup: &snapshotAPIGroup, Kind: "VolumeSnapshot", Name: *src,
		}
	}
	return p
}

// blockPVC builds a raw-block (volumeMode: Block) PVC.
func blockPVC(ns, name, sc, size string) *corev1.PersistentVolumeClaim {
	p := pvc(ns, name, sc, size, nil)
	p.Spec.VolumeMode = &blockMode
	return p
}

// pod builds a sleeper that mounts pvcName at /data.
func pod(ns, name, pvcName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: ptr(int64(1)),
			Containers: []corev1.Container{{
				Name:         "app",
				Image:        "alpine:3.23",
				Command:      []string{"sleep", "infinity"},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName}},
			}},
		},
	}
}

// blockPod builds a sleeper with pvcName as a raw block device at /dev/xvda.
func blockPod(ns, name, pvcName string) *corev1.Pod {
	p := pod(ns, name, pvcName)
	p.Spec.Containers[0].VolumeMounts = nil
	p.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: "data", DevicePath: "/dev/xvda"}}
	return p
}

func volumeSnapshot(ns, name, srcPVC string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(volumeSnapshotGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	_ = unstructured.SetNestedField(u.Object, snapshotClass, "spec", "volumeSnapshotClassName")
	_ = unstructured.SetNestedField(u.Object, srcPVC, "spec", "source", "persistentVolumeClaimName")
	return u
}

func ptr[T any](v T) *T { return &v }

// --- waits -----------------------------------------------------------------

func eventuallyPodReady(ctx context.Context, ns, name string) {
	Eventually(func(g Gomega) {
		var p corev1.Pod
		g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &p)).To(Succeed())
		g.Expect(podReady(&p)).To(BeTrue(), "pod %s phase=%s", name, p.Status.Phase)
	}).Should(Succeed())
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func boundPV(ctx context.Context, ns, name string) string {
	var pv string
	Eventually(func(g Gomega) {
		var p corev1.PersistentVolumeClaim
		g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &p)).To(Succeed())
		g.Expect(p.Status.Phase).To(Equal(corev1.ClaimBound), "PVC %s not Bound", name)
		pv = p.Spec.VolumeName
		g.Expect(pv).NotTo(BeEmpty())
	}).Should(Succeed())
	return pv
}

func eventuallyMiroirVolumeReady(ctx context.Context, pvName string) {
	Eventually(func(g Gomega) {
		var v miroirv1alpha1.MiroirVolume
		g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pvName}, &v)).To(Succeed())
		g.Expect(string(v.Status.Phase)).To(Equal("Ready"), "MiroirVolume %s not Ready", pvName)
	}).Should(Succeed())
}

func eventuallySnapshotReady(ctx context.Context, ns, name string) {
	Eventually(func(g Gomega) {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(volumeSnapshotGVK)
		g.Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, u)).To(Succeed())
		ready, found, _ := unstructured.NestedBool(u.Object, "status", "readyToUse")
		g.Expect(found && ready).To(BeTrue(), "VolumeSnapshot %s not readyToUse", name)
	}).Should(Succeed())
}

func eventuallyGone(ctx context.Context, obj client.Object) {
	key := client.ObjectKeyFromObject(obj)
	Eventually(func() bool {
		return apierrorNotFound(k8s.Get(ctx, key, obj))
	}).Should(BeTrue(), "%T %s still present", obj, key.Name)
}

func apierrorNotFound(err error) bool {
	return err != nil && client.IgnoreNotFound(err) == nil
}

// recreatePod deletes a pod and recreates it from the same spec, forcing a
// NodeUnstage/NodeStage cycle on its volume.
func recreatePod(ctx context.Context, p *corev1.Pod) {
	Expect(k8s.Delete(ctx, p)).To(Succeed())
	eventuallyGone(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name}})
	fresh := p.DeepCopy()
	fresh.ResourceVersion = ""
	Expect(k8s.Create(ctx, fresh)).To(Succeed())
	eventuallyPodReady(ctx, p.Namespace, p.Name)
}

// --- in-pod data ops (kubectl exec, like smoke.sh) -------------------------

func kubectlExec(ns, podName, sh string) string {
	GinkgoHelper()
	out, err := exec.Command("kubectl", "exec", "-n", ns, podName, "--", "sh", "-c", sh).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kubectl exec %s/%s: %s", ns, podName, string(out))
	return strings.TrimSpace(string(out))
}

// writeSeed writes 32MiB of random data to path and returns its sha256.
func writeSeed(ns, podName, path string) string {
	kubectlExec(ns, podName, fmt.Sprintf(
		"dd if=/dev/urandom of=%s bs=1M count=32 2>/dev/null && sha256sum %s && sync", path, path))
	return kubectlExec(ns, podName, fmt.Sprintf("sha256sum %s | cut -d' ' -f1", path))
}

// sha returns the sha256 of path inside the pod.
func sha(ns, podName, path string) string {
	return kubectlExec(ns, podName, fmt.Sprintf("sha256sum %s | cut -d' ' -f1", path))
}

// dataKB returns the size (1K-blocks) of the /data filesystem. A long LVM
// device path wraps df onto two lines, so the size is read as the 5th-from-last
// field of the data line (size, used, avail, use%, mount).
func dataKB(ns, podName string) int {
	out := kubectlExec(ns, podName, "df -k /data | awk 'END{print $(NF-4)}'")
	var kb int
	_, err := fmt.Sscanf(out, "%d", &kb)
	Expect(err).NotTo(HaveOccurred(), "parse df size %q", out)
	return kb
}
