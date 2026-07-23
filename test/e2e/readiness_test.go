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

// replicaNodes returns the node names of all schedulable nodes that have
// an agent pod running, or all cluster nodes if agents are absent.
func replicaNodes(ctx context.Context) []string {
	var nodes corev1.NodeList
	Expect(k8s.List(ctx, &nodes)).To(Succeed())
	var out []string
	for _, n := range nodes.Items {
		// Exclude nodes that are unschedulable (control-planes) unless
		// they run an agent — a 2-node home-lab cluster may have only
		// control-planes. In that case count every node.
		if n.Spec.Unschedulable {
			continue
		}
		out = append(out, n.Name)
	}
	if len(out) < 2 {
		out = nil
		for _, n := range nodes.Items {
			out = append(out, n.Name)
		}
	}
	Expect(len(out)).To(BeNumerically(">=", 2),
		"need at least 2 cluster nodes for replicated e2e tests")
	return out
}

// agentDaemonSetName discovers the agent DaemonSet name dynamically (it
// varies with the Helm release name).
func agentDaemonSetName() string {
	out, err := exec.Command("kubectl", "get", "daemonset", "-n", agentNamespace,
		"-l", "app.kubernetes.io/component=agent",
		"-o", "jsonpath={.items[0].metadata.name}").CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "find agent DaemonSet: %s", string(out))
	name := strings.TrimSpace(string(out))
	Expect(name).NotTo(BeEmpty(), "no agent DaemonSet found in namespace "+agentNamespace)
	return name
}

// kubectlPatch patches a resource in-cluster.
func kubectlPatch(kind, ns, name, patch string) {
	GinkgoHelper()
	out, err := exec.Command("kubectl", "patch", kind, "-n", ns, name,
		"--type=merge", "-p", patch).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kubectl patch %s %s/%s: %s", kind, ns, name, string(out))
}

// kubectlLabelNode patches a node label in-cluster.
func kubectlLabelNode(node, labels, verb string) {
	GinkgoHelper()
	out, err := exec.Command("kubectl", verb, "node", node, labels).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kubectl %s node %s: %s", verb, node, string(out))
}

// mivCondition returns the condition with the given type from a MiroirVolume, or nil.
func mivCondition(vol *miroirv1alpha1.MiroirVolume, condType string) *metav1.Condition {
	for _, c := range vol.Status.Conditions {
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// eventWithReason returns true if there is a core/v1 Event with the given
// reason for the named regarding object.
func eventWithReason(ctx context.Context, reason, name string) bool {
	var evs eventsv1.EventList
	Expect(k8s.List(ctx, &evs, client.InNamespace("default"))).To(Succeed())
	for _, e := range evs.Items {
		if e.Reason == reason && e.Regarding.Name == name {
			return true
		}
	}
	return false
}

// ————————————————————————————————————————————————————————————————
// Test 1: Stale readiness detection (Phase 1 — LastProbedAt)
// ————————————————————————————————————————————————————————————————

var _ = Describe("stale readiness detection (LastProbedAt)", Ordered, func() {
	const ns = "miroir-e2e-readiness"
	ctx := context.Background()

	var pv, nodeA, nodeB string
	var probedBefore map[string]metav1.Time
	var failed bool

	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}))).To(Succeed())
		nodes := replicaNodes(ctx)
		nodeA = nodes[0]
		nodeB = nodes[1]
	})
	AfterEach(func() { failed = failed || CurrentSpecReport().Failed() })
	AfterAll(func() {
		if failed {
			GinkgoWriter.Printf("specs failed — leaving namespace %q for diagnostics\n", ns)
			return
		}
		_ = k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})

	It("provisions a replicated volume and reports LastProbedAt on both nodes", func() {
		Expect(k8s.Create(ctx, pvc(ns, "data", replicatedClass, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "consumer", "data"))).To(Succeed())
		eventuallyPodReady(ctx, ns, "consumer")
		pv = boundPV(ctx, ns, "data")
		eventuallyMiroirVolumeReady(ctx, pv)

		var v miroirv1alpha1.MiroirVolume
		Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())

		// Both nodes must have PerNode entries with non-nil LastProbedAt.
		psA, okA := v.Status.PerNode[nodeA]
		psB, okB := v.Status.PerNode[nodeB]
		Expect(okA).To(BeTrue(), "PerNode entry missing for node %s", nodeA)
		Expect(okB).To(BeTrue(), "PerNode entry missing for node %s", nodeB)
		Expect(psA.LastProbedAt).NotTo(BeNil(), "LastProbedAt missing on %s", nodeA)
		Expect(psB.LastProbedAt).NotTo(BeNil(), "LastProbedAt missing on %s", nodeB)
		Expect(string(v.Status.Phase)).To(Equal("Ready"))

		probedBefore = map[string]metav1.Time{
			nodeA: *psA.LastProbedAt,
			nodeB: *psB.LastProbedAt,
		}
	})

	It("refreshes LastProbedAt after agent restart", func() {
		// Clean unmount first.
		Expect(k8s.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "consumer"},
		})).To(Succeed())
		eventuallyGone(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "consumer"}})

		// Restart the agent on one node by deleting its pod; the
		// DaemonSet recreates it. After restart the agent re-probes
		// and stamps a fresh LastProbedAt.
		origPod := agentPodOn(ctx, nodeA)
		Expect(k8s.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: agentNamespace,
				Name:      origPod,
			},
		})).To(Succeed())
		// Wait for the new agent pod to be ready.
		Eventually(func(g Gomega) {
			var p corev1.Pod
			g.Expect(k8s.Get(ctx, client.ObjectKey{
				Namespace: agentNamespace,
				Name:      agentPodOn(ctx, nodeA),
			}, &p)).To(Succeed())
			g.Expect(podReady(&p)).To(BeTrue(), "new agent pod on %s not ready yet", nodeA)
		}).WithTimeout(2 * time.Minute).Should(Succeed())

		// Now check LastProbedAt was refreshed.
		Eventually(func(g Gomega) {
			var v miroirv1alpha1.MiroirVolume
			g.Expect(k8s.Get(ctx, client.ObjectKey{Name: pv}, &v)).To(Succeed())
			ps, ok := v.Status.PerNode[nodeA]
			g.Expect(ok).To(BeTrue())
			g.Expect(ps.LastProbedAt).NotTo(BeNil())
			// The new probe time must be strictly after the pre-restart one.
			g.Expect(ps.LastProbedAt.Time.After(probedBefore[nodeA].Time)).
				To(BeTrue(), "LastProbedAt not refreshed on %s after agent restart", nodeA)
		}).WithTimeout(3 * time.Minute).Should(Succeed())
	})

	It("returns to Ready after agent restart", func() {
		eventuallyMiroirVolumeReady(ctx, pv)
	})
})

// ————————————————————————————————————————————————————————————————
// Test 2: CSI node unreachability surfacing (Phase 3)
// ————————————————————————————————————————————————————————————————

var _ = Describe("CSI node unreachability surfacing", Ordered, func() {
	const ns = "miroir-e2e-csi-unreach"
	ctx := context.Background()

	var dsName, excludeNode string
	var failed bool

	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8s.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}))).To(Succeed())
		nodes := replicaNodes(ctx)
		excludeNode = nodes[0]
		dsName = agentDaemonSetName()
	})
	AfterEach(func() { failed = failed || CurrentSpecReport().Failed() })
	AfterAll(func() {
		// Restore the DaemonSet to schedule everywhere — remove any
		// nodeSelector we added. Best-effort: check if a nodeSelector
		// was patched, then delete the key.
		_ = exec.Command("kubectl", "patch", "daemonset", "-n", agentNamespace, dsName,
			"--type=json", "-p",
			`[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]`).Run()
		// Remove the exclusion label from the node.
		_ = exec.Command("kubectl", "label", "node", excludeNode,
			"miroir-e2e-test/excluded-").Run()

		if failed {
			GinkgoWriter.Printf("specs failed — leaving namespace %q for diagnostics\n", ns)
			return
		}
		_ = k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})

	It("evicts the agent from one node and verifies NodeUnreachable surfaces", func() {
		// 1. Label the node to exclude the DaemonSet and patch the
		//    DaemonSet's nodeSelector to skip labeled nodes.
		kubectlLabelNode(excludeNode,
			"miroir-e2e-test/excluded=true", "label")
		kubectlPatch("daemonset", agentNamespace, dsName,
			fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"miroir-e2e-test/excluded":"false"}}}}}`))

		// 2. Delete the agent pod on the excluded node. The DaemonSet
		//    won't restart it because the nodeSelector excludes it.
		agentName := agentPodOn(ctx, excludeNode)
		Expect(k8s.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: agentNamespace,
				Name:      agentName,
			},
		})).To(Succeed())
		eventuallyGone(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: agentNamespace, Name: agentName},
		})

		// 3. Create a replicated PVC and a pod. The pod will stay
		//    Pending because the volume cannot provision — one
		//    replica's agent is absent. The CSI controller's waitReady
		//    will time out and surface a NodeUnreachable condition.
		Expect(k8s.Create(ctx, pvc(ns, "data", replicatedClass, "1Gi", nil))).To(Succeed())
		Expect(k8s.Create(ctx, pod(ns, "consumer", "data"))).To(Succeed())

		// 4. Wait for the NodeUnreachable condition to appear on the
		//    MiroirVolume. The CSI controller's waitReady times out
		//    after provisionTimeout (~120s) and then checkNodeUnreachable
		//    fires if a replica node's PerNode is absent.
		var mvName string
		Eventually(func(g Gomega) {
			var v miroirv1alpha1.MiroirVolume
			// Try by the bound PV name first.
			var pvc corev1.PersistentVolumeClaim
			if k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "data"}, &pvc) == nil &&
				pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
				mvName = pvc.Spec.VolumeName
				err := k8s.Get(ctx, client.ObjectKey{Name: mvName}, &v)
				g.Expect(err).NotTo(HaveOccurred())
			} else {
				// Fall back: list all MiroirVolumes with the PVC
				// reference labels.
				var mvs miroirv1alpha1.MiroirVolumeList
				g.Expect(k8s.List(ctx, &mvs,
					client.MatchingLabels{
						"miroir.home-operations.com/pvc-name":      "data",
						"miroir.home-operations.com/pvc-namespace": ns,
					})).To(Succeed())
				g.Expect(mvs.Items).NotTo(BeEmpty(),
					"no MiroirVolume found for PVC %s/%s", ns, "data")
				v = mvs.Items[0]
				mvName = v.Name
			}
			cond := mivCondition(&v, miroirv1alpha1.ConditionNodeUnreachable)
			g.Expect(cond).NotTo(BeNil(),
				"NodeUnreachable condition not yet set on %s", mvName)
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}).WithTimeout(4*time.Minute).Should(Succeed(),
			"NodeUnreachable condition must appear within provision timeout window")

		// 5. Assert a NodeUnreachable event was emitted.
		Expect(eventWithReason(ctx, "NodeUnreachable", mvName)).
			To(BeTrue(), "want a NodeUnreachable event for %s", mvName)
	})
})
