//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	replicatedClass = "miroir-replicated"
	agentNamespace  = "miroir-system"

	// The reclaim only fires after the full busy escalation (30 attempts
	// at 10s) plus at least one parked 60s cycle, so the deletion needs a
	// far longer wait than the suite default.
	reclaimTimeout = 12 * time.Minute
)

// agentPodOn returns the miroir agent pod running on node.
func agentPodOn(ctx context.Context, node string) string {
	GinkgoHelper()
	var pods corev1.PodList
	Expect(k8s.List(ctx, &pods, client.InNamespace(agentNamespace),
		client.MatchingLabels{"app.kubernetes.io/component": "agent"})).To(Succeed())
	for _, p := range pods.Items {
		if p.Spec.NodeName == node {
			return p.Name
		}
	}
	Fail("no agent pod on node " + node)
	return ""
}

// agentExec runs sh -c inside the privileged agent container on node —
// the harness's stand-in for node-level storage commands (same pattern as
// smoke.sh). The agent shares the host mount namespaces via bidirectional
// propagation, so mounts and unmounts issued here land on the host.
func agentExec(ctx context.Context, node, sh string) string {
	GinkgoHelper()
	out, err := exec.Command("kubectl", "exec", "-n", agentNamespace, agentPodOn(ctx, node),
		"-c", "agent", "--", "sh", "-c", sh).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "agent exec on %s: %s", node, string(out))
	return strings.TrimSpace(string(out))
}

// Reproduces issue #319's shape on real DRBD and asserts the
// orphaned-hold reclaim: a kernel object with no living process behind it
// (in the field, a leaked freeze's dead superblock; here, a loop device —
// see the planting step for why) holds the unmounted DRBD device open, so
// drbdsetup down is refused forever. The reclaim must force-detach the
// backing, finish the deletion, and leave only the pinned kernel minor
// for the post-reboot orphan sweep.
var _ = Describe("orphaned-hold teardown reclaim", Ordered, func() {
	const ns = "miroir-e2e-orphan"
	ctx := context.Background()

	var pv, node string
	var minor int32
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

	It("provisions a replicated volume and mounts it", func() {
		Expect(k8s.Create(ctx, pvc(ns, "holddata", replicatedClass, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "holder", "holddata"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "holder")
		pv = boundPV(ctx, ns, "holddata")
		eventuallyMiroirVolumeReady(ctx, pv)
		writeSeed(ns, "holder", "/data/seed")

		var p corev1.Pod
		Expect(k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "holder"}, &p)).To(Succeed())
		node = p.Spec.NodeName
		var v miroirv1alpha1.MiroirVolume
		Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
		minor = v.Status.PerNode[node].DRBDMinor
		Expect(minor).To(BeNumerically(">", 0), "staged leg must report its DRBD minor")
	})

	It("plants a dead-opener hold on the unmounted device", func() {
		// Clean unstage first: the pod goes, kubelet unmounts, and the
		// #312 thaw guard runs — no mount survives in any namespace.
		Expect(k8s.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "holder"}})).To(Succeed())
		eventuallyGone(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "holder"}})

		// Then pin the device with a kernel object whose opener is dead: a
		// loop device holds the DRBD device open (auto-promoted, writable)
		// after losetup itself has exited — the same shape as #319's field
		// state, where a leaked freeze's dead superblock held open_cnt with
		// every recorded opener long gone. Deliberately NOT reproduced via
		// fsfreeze here: on Talos, machined's device prober race-opens a
		// frozen bdev and wedges in the open, becoming a live opener the
		// reclaim gates then (correctly) refuse — the freeze repro is
		// nondeterministic, the loop hold is not.
		dev := fmt.Sprintf("/dev/drbd%d", minor)
		agentExec(ctx, node, "losetup -f "+dev)

		// The hold must actually pin the device: no mounts anywhere, yet
		// the kernel counts a writable opener — exactly what drbdsetup
		// down is about to be refused over.
		Eventually(func(g Gomega) {
			g.Expect(agentExec(ctx, node, fmt.Sprintf("findmnt -rn -S %s -o TARGET || true", dev))).To(BeEmpty())
			g.Expect(agentExec(ctx, node, "drbdsetup status "+pv+" --verbose")).To(ContainSubstring("open:yes"))
		}).Should(Succeed())
	})

	It("reclaims the deletion past the pinned minor", func() {
		Expect(k8s.Delete(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "holddata"},
		})).To(Succeed())

		Eventually(func() bool {
			return apierrorNotFound(k8s.Get(ctx, client.ObjectKey{Name: pv}, &miroirv1alpha1.MiroirVolume{}))
		}).WithTimeout(reclaimTimeout).Should(BeTrue(), "MiroirVolume %s must finish deleting despite the hold", pv)
		Eventually(func() bool {
			return apierrorNotFound(k8s.Get(ctx, client.ObjectKey{Name: pv}, &corev1.PersistentVolume{}))
		}).Should(BeTrue(), "PV %s must be released", pv)
	})

	It("frees the backing and leaves only the zombie minor with a reclaim event", func() {
		// The backing LV is gone — the space came back without a reboot.
		lvs := agentExec(ctx, node, "lvs --noheadings -o lv_name || true")
		Expect(lvs).NotTo(ContainSubstring(pv), "backing LV must be destroyed by the reclaim")

		// The kernel minor is the expected residue: still registered,
		// diskless, pinned by the dead-opener hold until the node reboots
		// (the startup orphan sweep then releases its minor reservation).
		status := agentExec(ctx, node, "drbdsetup status "+pv+" --verbose || true")
		Expect(status).To(ContainSubstring(pv), "zombie minor must remain until reboot")
		Expect(status).To(ContainSubstring("Diskless"), "zombie minor must have no backing attached")

		var evs eventsv1.EventList
		Expect(k8s.List(ctx, &evs, client.InNamespace("default"))).To(Succeed())
		found := false
		for _, e := range evs.Items {
			if e.Reason == "TeardownReclaimed" && e.Regarding.Name == pv {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue(), "want a TeardownReclaimed event for %s", pv)
	})
})
