//go:build e2e

package e2e

import (
	"context"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	// The conversion waits out --auto-diskful-after, then full-syncs a 1Gi
	// device end to end before the leg reads UpToDate.
	conversionTimeout = 10 * time.Minute
)

// controllerArgs returns the miroir controller container's argv, so a spec
// can assert the option it depends on is actually installed rather than
// time out on a controller that was never asked to convert anything.
func controllerArgs() string {
	GinkgoHelper()
	out, err := exec.Command("kubectl", "get", "deployment", "-n", agentNamespace,
		"-l", "app.kubernetes.io/component=controller",
		"-o", "jsonpath={.items[0].spec.template.spec.containers[*].args}").CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "read controller args: %s", string(out))
	return string(out)
}

// localLegStatus returns this node's own two status lines for a resource
// (the resource line and its device line). Scoped deliberately: the peer
// lines below carry peer-disk: states that a whole-output substring match
// would confuse for the local disk.
func localLegStatus(ctx context.Context, node, name string) string {
	return agentExec(ctx, node, "drbdsetup status "+name+" | awk 'NR<=2'")
}

// disklessLegNode returns the node carrying the volume's diskless leg —
// a tie-breaker replica, or a client leg on a cluster with no spare node
// for one. Empty when every leg is diskful.
func disklessLegNode(vol *miroirv1alpha1.MiroirVolume) string {
	for _, rep := range vol.Spec.Replicas {
		if rep.Diskless {
			return rep.Node
		}
	}
	for _, cl := range vol.Spec.Clients {
		return cl.Node
	}
	return ""
}

// Auto-diskful on real DRBD: a consumer that settles on the node carrying
// the volume's diskless tie-breaker gets a local replica there, converted
// in place under the running pod (LINSTOR's toggle-disk).
//
// The conversion keeps the leg's node-id and address, so it is already up
// in the kernel as a diskless Primary while the backing it is gaining is
// still blank — the state that made the agent adopt a blank device as
// live metadata, skip create-md, and error-loop "No valid meta data
// found" forever. Only real DRBD proves the whole chain: create-md
// against an up-but-diskless resource, the attach, and a full sync that
// carries the data the pod wrote over the network before it had a disk.
//
// Needs a spare storage node (cluster-spare.yaml) and a controller
// installed with autoDiskfulAfter set — the autodiskful CI leg.
var _ = Describe("auto-diskful conversion", Ordered, Label("autodiskful"), func() {
	const ns = "miroir-e2e-diskful"
	ctx := context.Background()

	var pv, tieNode, seedSum string
	var roamer, settled *corev1.Pod
	var failed bool

	BeforeAll(func() {
		Expect(controllerArgs()).To(ContainSubstring("--auto-diskful-after="),
			"the controller must be installed with autoDiskfulAfter set")
		Expect(len(replicaNodes(ctx))).To(BeNumerically(">=", 3),
			"a 2-replica volume only gets a diskless tie-breaker when a third storage node is spare")
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

	It("provisions a replicated volume with a diskless tie-breaker", func() {
		Expect(k8s.Create(ctx, pvc(ns, "convdata", replicatedClass, "1Gi", nil))).To(Succeed())
		roamer = pod(ns, "roamer", "convdata")
		Expect(k8s.Create(ctx, roamer)).To(Succeed())
		eventuallyPodReady(ctx, ns, roamer.Name)
		pv = boundPV(ctx, ns, "convdata")
		eventuallyMiroirVolumeReady(ctx, pv)
		seedSum = writeSeed(ns, roamer.Name, "/data/seed")

		// The tie-breaker lands on whichever node placement left spare.
		Eventually(func(g Gomega) {
			var v miroirv1alpha1.MiroirVolume
			g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
			g.Expect(v.Spec.DiskfulReplicas()).To(HaveLen(2))
			tieNode = disklessLegNode(&v)
			g.Expect(tieNode).NotTo(BeEmpty(), "no diskless leg on %s", pv)
		}).Should(Succeed())
	})

	It("moves the consumer onto the diskless leg's node", func() {
		Expect(k8s.Delete(ctx, roamer)).To(Succeed())
		eventuallyGone(ctx, roamer)

		settled = pod(ns, "settled", "convdata")
		settled.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": tieNode}
		Expect(k8s.Create(ctx, settled)).To(Succeed())
		eventuallyPodReady(ctx, ns, settled.Name)

		// The leg the conversion is about to act on: up, Primary for the
		// pod, and serving every read and write over the network. The
		// threshold clock is PrimarySince, stamped back at NodeStageVolume,
		// so this races the conversion by however long the pod took to
		// start — bounded tightly, because a leg that already converted
		// only gets further from Diskless and waiting cannot recover it.
		Eventually(func(g Gomega) {
			g.Expect(localLegStatus(ctx, tieNode, pv)).To(ContainSubstring("disk:Diskless"))
			var v miroirv1alpha1.MiroirVolume
			g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
			g.Expect(v.Status.PerNode[tieNode].DiskState).To(Equal("Diskless"))
		}).WithTimeout(20*time.Second).Should(Succeed(),
			"the leg must still be diskless here; if it already converted, "+
				"pod startup ate the whole --auto-diskful-after budget")
		Expect(sha(ns, settled.Name, "/data/seed")).To(Equal(seedSum),
			"the diskless client must serve the peers' data")
	})

	It("converts the settled leg into a diskful replica", func() {
		Eventually(func(g Gomega) {
			var v miroirv1alpha1.MiroirVolume
			g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
			g.Expect(v.Spec.DiskfulReplicas()).To(HaveLen(3))
			g.Expect(hasDiskfulReplicaOn(&v, tieNode)).To(BeTrue(),
				"the settled leg on %s must be the one converted", tieNode)
			g.Expect(disklessLegNode(&v)).To(BeEmpty(), "no diskless leg may survive the conversion")
		}).WithTimeout(conversionTimeout).Should(Succeed())

		Eventually(func(g Gomega) {
			found, err := eventWithReason(ctx, "AutoDiskful", pv)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(found).To(BeTrue(), "want an AutoDiskful event for %s", pv)
		}).Should(Succeed())
	})

	It("seeds the fresh backing and full-syncs it under the running pod", func() {
		// The regression: with the blank backing adopted as live metadata,
		// create-md never ran, the attach failed "No valid meta data found"
		// on every pass, and this leg stayed Diskless forever.
		Eventually(func(g Gomega) {
			var v miroirv1alpha1.MiroirVolume
			g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
			g.Expect(v.Status.PerNode[tieNode].DiskState).To(Equal("UpToDate"),
				"converted leg must attach its backing and finish its full sync")
			g.Expect(string(v.Status.Phase)).To(Equal("Ready"))
		}).WithTimeout(conversionTimeout).Should(Succeed())

		// On the node itself: a backing LV exists and the kernel attached it.
		Expect(agentExec(ctx, tieNode, "lvs --noheadings -o lv_name")).To(ContainSubstring(pv),
			"the conversion must leave a real backing device behind")
		Expect(localLegStatus(ctx, tieNode, pv)).To(ContainSubstring("disk:UpToDate"))

		// The pod rode the conversion out, and it now reads the seed it
		// wrote before the node had a disk — off the local replica this
		// time. Phase and readiness, not RestartCount: these pods are
		// RestartPolicy: Never, so a container the conversion killed would
		// move the pod to Failed with its restart count still zero.
		var p corev1.Pod
		Expect(k8s.Get(ctx, client.ObjectKeyFromObject(settled), &p)).To(Succeed())
		Expect(p.Status.Phase).To(Equal(corev1.PodRunning),
			"the conversion must be transparent to the consumer")
		Expect(podReady(&p)).To(BeTrue(), "consumer must still be Ready after the conversion")
		Expect(sha(ns, settled.Name, "/data/seed")).To(Equal(seedSum))
	})
})
