//go:build e2e

// Package e2e drives a Helm-deployed miroir through the CSI volume lifecycle on
// the kind cluster $KUBECONFIG points at (see the mise test-e2e task). It is
// build-tagged so `mise run test` never compiles it.
//
// Scope: the local backend (miroir-local / lvmthin / replicas:1). The DRBD path
// needs modules absent from kind; it stays gated by smoke.sh + conformance.
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	// Defaults match the chart's miroir-local class and miroir-snap snapclass.
	storageClass  = "miroir-local"
	snapshotClass = "miroir-snap"

	// Generous because a cold lvmthin pool (pvcreate/vgcreate/lvcreate on
	// first volume) plus WaitForFirstConsumer binding can take a while.
	provisionTimeout = 3 * time.Minute
	pollInterval     = 3 * time.Second
)

// k8s is the cluster client shared by the specs.
var k8s client.Client

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "miroir e2e suite")
}

var _ = BeforeSuite(func() {
	SetDefaultEventuallyTimeout(provisionTimeout)
	SetDefaultEventuallyPollingInterval(pollInterval)

	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(miroirv1alpha1.AddToScheme(scheme)).To(Succeed())

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath())
	Expect(err).NotTo(HaveOccurred(), "load kubeconfig (set KUBECONFIG)")

	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred(), "build cluster client")

	// Fail fast with a clear message if the harness didn't install the chart.
	var sc corev1.NamespaceList
	Expect(k8s.List(context.Background(), &sc)).To(Succeed(), "cluster unreachable")
})

func kubeconfigPath() string {
	if p := os.Getenv("KUBECONFIG"); p != "" {
		return p
	}
	return clientcmd.RecommendedHomeFile
}
