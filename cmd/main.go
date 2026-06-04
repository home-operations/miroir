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

// homefs is a low-resource replicated block storage driver for Kubernetes.
// One binary, two modes (notes/DESIGN.md §4.2):
//
//	--mode=controller  CSI Identity+Controller services (Deployment)
//	--mode=agent       CSI Identity+Node services + node reconciler (DaemonSet)
package main

import (
	"context"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	homefsv1alpha1 "github.com/eleboucher/homefs/api/v1alpha1"
	"github.com/eleboucher/homefs/internal/agent"
	"github.com/eleboucher/homefs/internal/backend"
	"github.com/eleboucher/homefs/internal/csi"
	"github.com/eleboucher/homefs/internal/drbd"
	"github.com/eleboucher/homefs/internal/nodemap"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// Populated via -ldflags at build time.
var (
	version = "dev"
	commit  = "unknown"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(homefsv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		mode        string
		csiSocket   string
		metricsAddr string
		probeAddr   string
		nodesConfig string

		// agent mode
		nodeName     string
		vg           string
		thinPool     string
		drbdStateDir string
	)
	flag.StringVar(&mode, "mode", "", "controller | agent")
	flag.StringVar(&csiSocket, "csi-socket", "/csi/csi.sock", "CSI gRPC unix socket path")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint (0 to disable)")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "this node's name (agent)")
	flag.StringVar(&nodesConfig, "nodes-config", "/etc/homefs/nodes.yaml",
		"per-node storage topology (rendered from Helm values)")
	flag.StringVar(&vg, "lvm-vg", "vg-homefs", "LVM volume group (agent, lvmthin)")
	flag.StringVar(&thinPool, "lvm-thinpool", "thinpool", "LVM thin pool LV (agent, lvmthin)")
	flag.StringVar(&drbdStateDir, "drbd-state-dir", "/etc/drbd.d",
		"rendered DRBD config dir (agent; hostPath-backed)")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog.Info("starting homefs", "mode", mode, "version", version, "commit", commit)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		// No leader election: the controller is a 1-replica Deployment and
		// agents are per-node singletons (notes/DESIGN.md §4.2).
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	identity := &csi.Identity{Version: version, WithController: mode == "controller"}

	switch mode {
	case "controller":
		nodes, err := nodemap.Load(nodesConfig)
		if err != nil {
			setupLog.Error(err, "unable to load node map")
			os.Exit(1)
		}
		controller := &csi.Controller{Client: mgr.GetClient(), Nodes: nodes}
		serveCSI(mgr, csiSocket, identity, controller, nil)

	case "agent":
		if nodeName == "" {
			setupLog.Error(nil, "--node-name (or NODE_NAME) is required in agent mode")
			os.Exit(1)
		}
		nodes, err := nodemap.Load(nodesConfig)
		if err != nil {
			setupLog.Error(err, "unable to load node map")
			os.Exit(1)
		}
		entry, ok := nodes[nodeName]
		if !ok {
			// Not a storage node: serve the CSI node service (pods may
			// still mount remote volumes in future modes) but no backend.
			setupLog.Error(nil, "node absent from the node map; agent requires a storage entry",
				"node", nodeName)
			os.Exit(1)
		}
		be, err := backend.New(entry.Backend, backend.Config{
			VolumeGroup: vg,
			ThinPool:    thinPool,
			Device:      entry.Device,
			Dataset:     entry.ZFSDataset,
			PoolSize:    entry.ThinPoolSize,
		}, backend.RealExec)
		if err != nil {
			setupLog.Error(err, "invalid backend for node", "node", nodeName)
			os.Exit(1)
		}
		// Bootstrap the node-local pool before serving anything
		// (notes/DESIGN.md §7.2): first start on a fresh node creates
		// PV/VG/thin-pool (lvmthin) or the parent dataset (zfs).
		if err := be.Setup(context.Background()); err != nil {
			setupLog.Error(err, "backend pool setup failed")
			os.Exit(1)
		}
		drbdDriver := &drbd.Driver{StateDir: drbdStateDir, Exec: backend.RealExec}
		// Reap kernel resources and rendered config orphaned by a crash
		// between up and down — they hold backing devices open forever.
		if err := sweepOrphans(nodeName, drbdDriver); err != nil {
			setupLog.Error(err, "orphan sweep failed")
			os.Exit(1)
		}
		reconciler := &agent.VolumeReconciler{
			Client:   mgr.GetClient(),
			NodeName: nodeName,
			Backend:  be,
			DRBD:     drbdDriver,
		}
		if err := reconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up agent reconciler")
			os.Exit(1)
		}
		snapReconciler := &agent.SnapshotReconciler{
			Client:   mgr.GetClient(),
			NodeName: nodeName,
			Backend:  be,
			DRBD:     drbdDriver,
		}
		if err := snapReconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up snapshot reconciler")
			os.Exit(1)
		}
		node := csi.NewNode(mgr.GetClient(), nodeName)
		serveCSI(mgr, csiSocket, identity, nil, node)

	default:
		setupLog.Error(nil, "--mode must be controller or agent")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

// sweepOrphans removes DRBD state with no owning volume on this node,
// using a direct (uncached) client — the manager has not started yet.
func sweepOrphans(nodeName string, driver *drbd.Driver) error {
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	vols := &homefsv1alpha1.HomefsVolumeList{}
	if err := c.List(context.Background(), vols); err != nil {
		return err
	}
	owned := map[string]bool{}
	for _, v := range vols.Items {
		for _, rep := range v.Spec.Replicas {
			if rep.Node == nodeName {
				owned[v.Name] = true
			}
		}
	}
	return driver.SweepOrphans(context.Background(),
		func(name string) bool { return owned[name] })
}

// serveCSI runs the CSI gRPC server alongside the manager; controller and
// node are mutually exclusive (one per mode).
func serveCSI(mgr ctrl.Manager, socket string, identity *csi.Identity, controller *csi.Controller, node *csi.Node) {
	err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		// CSI RPCs read CRs through the manager's cache; wait for sync so
		// early kubelet/sidecar calls don't race a cold cache.
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return context.Canceled
		}
		if controller != nil {
			return csi.Serve(ctx, socket, identity, controller, nil)
		}
		return csi.Serve(ctx, socket, identity, nil, node)
	}))
	if err != nil {
		setupLog.Error(err, "unable to add CSI server to manager")
		os.Exit(1)
	}
}
