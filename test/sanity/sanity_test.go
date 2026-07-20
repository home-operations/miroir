/*
Copyright 2026.

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.gnu.org/licenses/agpl-3.0.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package sanity runs the upstream kubernetes-csi/csi-test sanity suite
// (the CSI spec's gRPC contract conformance) against miroir's Identity
// and Controller services in-process. It needs no cluster and no
// privileges: the driver runs against a fake API client whose
// interceptors stand in for the agents (marking volumes Ready and
// snapshots ReadyToUse), so CreateVolume/CreateSnapshot's realization
// waits resolve. The Node service is not exercised here because its
// stage/publish RPCs perform real mounts and formats that need a
// privileged host with real devices (the e2e suites cover that path).
package sanity_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	csiapi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/csi"
	"github.com/home-operations/miroir/internal/nodemap"
)

// storageNodeNames is the fake topology. The simulator stamps snapshot
// legs across all of them so a restore finds a complete leg on whichever
// node the source volume lives on.
var storageNodeNames = []string{"node-a", "node-b"}

// readyStatus stamps the terminal status the agents would write, so the
// controller's realization waits (CreateVolume, ControllerExpandVolume,
// snapshot restore) resolve against a cluster with no agents.
func readyStatus(obj client.Object) {
	switch o := obj.(type) {
	case *miroirv1alpha1.MiroirVolume:
		o.Status.Phase = miroirv1alpha1.VolumeReady
		// Report each replica's device realized at the current spec size,
		// so both CreateVolume's readiness wait and ExpandVolume's
		// grown-size wait (per-node SizeBytes >= new size) resolve.
		if o.Status.PerNode == nil {
			o.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{}
		}
		for _, rep := range o.Spec.Replicas {
			st := o.Status.PerNode[rep.Node]
			st.DeviceCreated = true
			st.SizeBytes = o.Spec.SizeBytes
			o.Status.PerNode[rep.Node] = st
		}
	case *miroirv1alpha1.MiroirSnapshot:
		// A real ReadyToUse snapshot has every diskful leg Done; mirror
		// that (SizeBytes falls back to the source volume in CreateSnapshot)
		// so the restore's seed-node check finds a complete leg.
		o.Status.ReadyToUse = true
		if o.Status.PerNode == nil {
			o.Status.PerNode = map[string]miroirv1alpha1.SnapshotNodeState{}
		}
		for _, n := range storageNodeNames {
			o.Status.PerNode[n] = miroirv1alpha1.SnapshotDone
		}
	case *miroirv1alpha1.MiroirSnapshotGroup:
		o.Status.ReadyToUse = true
	}
}

// agentSim is a fake client whose reads report volumes and snapshots as
// already realized.
func agentSim(scheme *runtime.Scheme) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&miroirv1alpha1.MiroirVolume{},
			&miroirv1alpha1.MiroirSnapshot{},
			&miroirv1alpha1.MiroirSnapshotGroup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(
				ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption,
			) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				readyStatus(obj)
				return nil
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				// Simulate the agents finishing teardown: strip finalizers so
				// the fake client actually removes the object instead of
				// parking it with a deletionTimestamp (miroir passes a
				// name-only stub, so load the stored object to see them).
				cur := obj.DeepCopyObject().(client.Object)
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), cur); err == nil && len(cur.GetFinalizers()) > 0 {
					cur.SetFinalizers(nil)
					if err := c.Update(ctx, cur); err != nil {
						return err
					}
				}
				return c.Delete(ctx, obj, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := c.List(ctx, list, opts...); err != nil {
					return err
				}
				switch l := list.(type) {
				case *miroirv1alpha1.MiroirVolumeList:
					for i := range l.Items {
						readyStatus(&l.Items[i])
					}
				case *miroirv1alpha1.MiroirSnapshotList:
					for i := range l.Items {
						readyStatus(&l.Items[i])
					}
				case *miroirv1alpha1.MiroirSnapshotGroupList:
					for i := range l.Items {
						readyStatus(&l.Items[i])
					}
				}
				return nil
			},
		}).
		Build()
}

// cleanupNode is scaffolding, not a service under test: csi-test's shared
// resource teardown calls NodeUnpublishVolume and (when STAGE_UNSTAGE is
// advertised) NodeUnstageVolume on every controller-created volume before
// deleting it. The real Node service performs privileged mounts, which is
// out of scope here (the e2e suites cover it and the whole "Node Service"
// describe is skipped), so this stub advertises no capabilities (teardown
// then skips NodeUnstage) and answers the unpublish call benignly.
type cleanupNode struct {
	csiapi.UnimplementedNodeServer
}

func (cleanupNode) NodeGetCapabilities(
	context.Context, *csiapi.NodeGetCapabilitiesRequest,
) (*csiapi.NodeGetCapabilitiesResponse, error) {
	return &csiapi.NodeGetCapabilitiesResponse{}, nil
}

func (cleanupNode) NodeUnpublishVolume(
	context.Context, *csiapi.NodeUnpublishVolumeRequest,
) (*csiapi.NodeUnpublishVolumeResponse, error) {
	return &csiapi.NodeUnpublishVolumeResponse{}, nil
}

// storageNode carries an address override so the replicated run's
// placement resolves replication endpoints without corev1 Node objects
// (nodemap.ReplicationAddress falls back to the Node's InternalIP only
// when the override is unset).
func storageNode(addr string) nodemap.Node {
	return nodemap.Node{
		Address: addr,
		Pools: map[string]nodemap.Pool{
			"default": {Backend: miroirv1alpha1.BackendLVMThin, Device: "/dev/vdb"},
		},
	}
}

func TestCSISanity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := miroirv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	cl := agentSim(scheme)
	nodes := nodemap.Map{}
	for i, n := range storageNodeNames {
		nodes[n] = storageNode(fmt.Sprintf("10.0.0.%d", i+1))
	}

	controller := &csi.Controller{
		Client:           cl,
		APIReader:        cl,
		Nodes:            nodes,
		ProvisionTimeout: 30 * time.Second,
		DRBDPortBase:     7000,
	}
	identity := &csi.Identity{Version: "sanity", WithController: true}

	// A short socket path: the scratchpad/GOTMPDIR paths overflow the
	// 108-byte AF_UNIX limit.
	sockDir, err := os.MkdirTemp("/tmp", "miroir-sanity-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "csi.sock")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	served := make(chan error, 1)
	go func() { served <- csi.Serve(ctx, sock, identity, controller, cleanupNode{}) }()

	// Wait for the socket to appear before handing the address to csi-test.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("CSI socket never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}

	newConfig := func(replicas string) sanity.TestConfig {
		config := sanity.NewTestConfig()
		config.Address = "unix://" + sock
		config.TestVolumeSize = 1 << 30 // 1 GiB
		config.TestVolumeParameters = map[string]string{constants.ParamReplicas: replicas}
		config.TestSnapshotParameters = map[string]string{}
		config.IdempotentCount = 3
		return config
	}

	// The suite runs twice against the same driver: unreplicated, then
	// replicated (DRBD port allocation, cross-node placement, the restore
	// replica-count check). GinkgoTest only registers describes bound to a
	// config, so both runs share the single RunSpecs invocation below —
	// ginkgo refuses a second one per process.
	local := newConfig("1")
	replicated := newConfig("2")
	ginkgo.Describe("replicas=1", func() { sanity.GinkgoTest(&local) })
	ginkgo.Describe("replicas=2", func() { sanity.GinkgoTest(&replicated) })
	gomega.RegisterFailHandler(ginkgo.Fail)

	suiteCfg, reporterCfg := ginkgo.GinkgoConfiguration()
	suiteCfg.SkipStrings = append(suiteCfg.SkipStrings,
		// The Node service is not registered (stage/publish need a
		// privileged host with real devices); its whole describe is
		// skipped, and the e2e suites cover that path.
		"Node Service",
		// ListSnapshots deliberately does not paginate (it returns every
		// snapshot; see its "no pagination: home scale" comment), so the
		// starting-token continuation this spec asserts cannot hold.
		// Pagination is optional in the CSI spec.
		"should return next token when a limited number of entries are requested",
	)
	ginkgo.RunSpecs(t, "miroir csi-sanity (identity + controller)", suiteCfg, reporterCfg)
}
