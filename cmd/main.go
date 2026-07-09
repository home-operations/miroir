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

// miroir is a low-resource replicated block storage driver for Kubernetes.
// One binary, two modes (notes/DESIGN.md §4.2):
//
//	--mode=controller  CSI Identity+Controller services (Deployment)
//	--mode=agent       CSI Identity+Node services + node reconciler (DaemonSet)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/agent"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/csi"
	"github.com/home-operations/miroir/internal/drbd"
	"github.com/home-operations/miroir/internal/membership"
	"github.com/home-operations/miroir/internal/nodemap"
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
	utilruntime.Must(miroirv1alpha1.AddToScheme(scheme))
}

// setupMembership registers the membership reconciler (completes
// operator-added replica entries) and, when enabled, the tie-breaker
// retrofit for pre-existing 2-replica freeze volumes (#70).
func setupMembership(mgr ctrl.Manager, nodes nodemap.Map, autoTieBreaker bool) error {
	r := &membership.Reconciler{Client: mgr.GetClient(), Nodes: nodes}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("membership reconciler: %w", err)
	}
	if !autoTieBreaker {
		return nil
	}
	tb := &membership.TieBreakerReconciler{Client: mgr.GetClient(), Nodes: nodes}
	if err := tb.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("tie-breaker reconciler: %w", err)
	}
	return nil
}

// backendFor resolves this node's storage entry from the node map and
// builds its backend — shared by setup and agent mode so the two can
// never wire Config differently.
func backendFor(nodeName, nodesConfig, vg, thinPool string) (backend.Backend, miroirv1alpha1.BackendType, error) {
	nodes, err := nodemap.Load(nodesConfig)
	if err != nil {
		return nil, "", fmt.Errorf("load node map: %w", err)
	}
	entry, ok := nodes[nodeName]
	if !ok {
		return nil, "", fmt.Errorf("node %s absent from the node map (Helm values: nodes)", nodeName)
	}
	be, err := backend.New(entry.Backend, backend.Config{
		VolumeGroup: vg,
		ThinPool:    thinPool,
		Device:      entry.Device,
		Dataset:     entry.ZFSDataset,
		PoolSize:    entry.ThinPoolSize,
		BaseDir:     entry.BaseDir,
	}, backend.RealExec)
	if err != nil {
		return nil, "", fmt.Errorf("backend for node %s: %w", nodeName, err)
	}
	return be, entry.Backend, nil
}

func main() {
	var (
		mode             string
		csiSocket        string
		metricsAddr      string
		nodesConfig      string
		provisionTimeout time.Duration
		overcommitRatio  float64
		autoTieBreaker   bool

		// agent mode
		nodeName          string
		vg                string
		thinPool          string
		drbdStateDir      string
		poolStatsInterval time.Duration
	)
	flag.StringVar(&mode, "mode", "", "controller | agent | setup")
	flag.StringVar(&csiSocket, "csi-socket", "/csi/csi.sock", "CSI gRPC unix socket path")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8081",
		"single operational endpoint: /metrics plus the /healthz and /readyz probes (org port standard)")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "this node's name (agent)")
	flag.StringVar(&nodesConfig, "nodes-config", "/etc/miroir/nodes.yaml",
		"per-node storage topology (rendered from Helm values)")
	flag.DurationVar(&provisionTimeout, "provision-timeout", 0,
		"wait for agents to realise a new volume (controller; 0 → default)")
	flag.Float64Var(&overcommitRatio, "overcommit-ratio", 0,
		"max provisioned-over-capacity per pool before CreateVolume is refused (controller; 0 → default 2.0)")
	flag.BoolVar(&autoTieBreaker, "auto-tie-breaker", true,
		"add a diskless tie-breaker to 2-replica freeze volumes when a spare node exists (controller)")
	flag.DurationVar(&poolStatsInterval, "pool-stats-interval", 0,
		"how often the agent republishes pool capacity (agent; 0 → default 60s)")
	flag.StringVar(&vg, "lvm-vg", "vg-miroir", "LVM volume group (agent, lvmthin)")
	flag.StringVar(&thinPool, "lvm-thinpool", "thinpool", "LVM thin pool LV (agent, lvmthin)")
	flag.StringVar(&drbdStateDir, "drbd-state-dir", "/etc/drbd.d",
		"rendered DRBD config dir (agent; hostPath-backed)")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog.Info("starting miroir", "mode", mode, "version", version, "commit", commit)

	// Setup mode provisions the node-local pool and exits. It reads the node
	// map from a file and drives lvm/zfs directly, so it needs neither the
	// controller-runtime manager nor an API connection — build neither.
	if mode == "setup" {
		if nodeName == "" {
			setupLog.Error(nil, "--node-name (or NODE_NAME) is required in setup mode")
			os.Exit(1)
		}
		be, _, err := backendFor(nodeName, nodesConfig, vg, thinPool)
		if err != nil {
			setupLog.Error(err, "unable to build the node's backend")
			os.Exit(1)
		}
		if err := be.Setup(context.Background()); err != nil {
			setupLog.Error(err, "backend pool setup failed", "node", nodeName)
			os.Exit(1)
		}
		setupLog.Info("pool ready", "node", nodeName)
		return
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: metricsAddr},
		// The dedicated health-probe server is disabled; the probes are
		// co-hosted on the (plain HTTP) metrics listener so each workload
		// exposes a single operational port — the agent runs hostNetwork,
		// so every listener occupies a real node port.
		HealthProbeBindAddress: "0",
		// No leader election: the controller is a 1-replica Deployment and
		// agents are per-node singletons (notes/DESIGN.md §4.2).
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}
	// healthz.CheckHandler returns 200 when the checker passes and 500
	// otherwise — the contract a kubelet HTTP probe expects.
	if err := mgr.AddMetricsServerExtraHandler("/healthz", healthz.CheckHandler{Checker: healthz.Ping}); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddMetricsServerExtraHandler("/readyz", healthz.CheckHandler{Checker: healthz.Ping}); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	identity := &csi.Identity{Version: version, WithController: mode == "controller"}

	// Agent mode only, run after the manager stops (SIGTERM): release DRBD
	// backings when the node is going down and lift any leftover write
	// barrier — both are kernel state that outlives the process.
	var shutdownSweep func()

	switch mode {
	case "controller":
		nodes, err := nodemap.Load(nodesConfig)
		if err != nil {
			setupLog.Error(err, "unable to load node map")
			os.Exit(1)
		}
		controller := &csi.Controller{
			Client:           mgr.GetClient(),
			APIReader:        mgr.GetAPIReader(),
			Nodes:            nodes,
			ProvisionTimeout: provisionTimeout,
			OvercommitRatio:  overcommitRatio,
			AutoTieBreaker:   autoTieBreaker,
		}
		if err := setupMembership(mgr, nodes, autoTieBreaker); err != nil {
			setupLog.Error(err, "unable to set up membership reconcilers")
			os.Exit(1)
		}
		serveCSI(mgr, csiSocket, identity, controller, nil)

	case "agent":
		if nodeName == "" {
			setupLog.Error(nil, "--node-name (or NODE_NAME) is required in agent mode")
			os.Exit(1)
		}
		// Agents refuse to start on nodes absent from the node map: the
		// DaemonSet's chart-side scope is every schedulable node, but only
		// storage nodes run an agent-backed backend.
		be, backendType, err := backendFor(nodeName, nodesConfig, vg, thinPool)
		if err != nil {
			setupLog.Error(err, "unable to build the node's backend")
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
		// Lift any IO barrier left by a previous agent crash.
		if err := resumeStaleBarriers(drbdDriver, apiStartupWait); err != nil {
			setupLog.Error(err, "barrier resume sweep failed")
			os.Exit(1)
		}
		// Tracks this node's cordon state so shutdownSweep can tell a node
		// reboot/upgrade (drained, so cordoned) from a routine pod restart.
		cordon := &agent.CordonWatcher{Client: mgr.GetClient(), NodeName: nodeName}
		if err := cordon.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up cordon watcher")
			os.Exit(1)
		}
		shutdownSweep = func() { agentShutdownSweep(cordon, drbdDriver) }
		// events2 turns kernel state changes into immediate reconciles;
		// the 30s poll remains as the safety net.
		drbdEvents := make(chan event.GenericEvent, 64)
		watcher := &drbd.EventWatcher{Notify: func(ctx context.Context, resource string) {
			ev := event.GenericEvent{Object: &miroirv1alpha1.MiroirVolume{
				ObjectMeta: metav1.ObjectMeta{Name: resource},
			}}
			select {
			case drbdEvents <- ev:
			case <-ctx.Done():
			}
		}}
		if err := mgr.Add(watcher); err != nil {
			setupLog.Error(err, "unable to add DRBD event watcher")
			os.Exit(1)
		}
		reconciler := &agent.VolumeReconciler{
			Client:     mgr.GetClient(),
			NodeName:   nodeName,
			Backend:    be,
			DRBD:       drbdDriver,
			DRBDEvents: drbdEvents,
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
		// Publishes this node's pool capacity for capacity-aware placement
		// (notes/DESIGN.md §4.6).
		if err := mgr.Add(&agent.PoolStatsPublisher{
			Client:      mgr.GetClient(),
			NodeName:    nodeName,
			Backend:     be,
			BackendType: backendType,
			Interval:    poolStatsInterval,
			Recorder:    mgr.GetEventRecorder("miroir-agent"),
		}); err != nil {
			setupLog.Error(err, "unable to add pool stats publisher")
			os.Exit(1)
		}
		node := csi.NewNode(mgr.GetClient(), nodeName, drbdDriver)
		serveCSI(mgr, csiSocket, identity, nil, node)

	default:
		setupLog.Error(nil, "--mode must be controller, agent, or setup")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	err = mgr.Start(ctrl.SetupSignalHandler())
	// The sweep must run even when the manager exits with an error — a
	// runnable blowing the shutdown grace is exactly the case where a
	// cordoned node still needs its DRBD backings released for reboot.
	if shutdownSweep != nil {
		shutdownSweep()
	}
	if err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

// apiStartupWait bounds how long the startup sweeps wait for the API server,
// so a reboot that races control-plane recovery does not exit on the first
// dial error and churn through CrashLoopBackOff. Kept under the liveness
// kill window: the probe endpoints are not up until the manager starts.
const apiStartupWait = 45 * time.Second

// drbdShutdownTimeout bounds the Secondary-teardown sweep at shutdown.
const drbdShutdownTimeout = 15 * time.Second

// apiShutdownWait bounds the shutdown barrier sweep's API access: the
// termination grace budget is 60s and the manager stop plus
// DownSecondaries can already spend 45s of it. apiStartupWait would
// guarantee a SIGKILL mid-sweep.
const apiShutdownWait = 5 * time.Second

// listWithRetry retries an API list until it succeeds, hits a terminal
// (non-transient) error, or apiStartupWait elapses — so a control plane still
// coming back up does not crash the agent on startup.
func listWithRetry(c client.Client, list client.ObjectList, budget time.Duration) error {
	var lastErr error
	waitErr := wait.PollUntilContextTimeout(context.Background(), 2*time.Second, budget, true,
		func(ctx context.Context) (bool, error) {
			lastErr = c.List(ctx, list)
			if lastErr == nil {
				return true, nil
			}
			if !transientAPIError(lastErr) {
				return false, lastErr
			}
			setupLog.Info("API server not ready; retrying", "error", lastErr.Error())
			return false, nil
		})
	if waitErr != nil && lastErr != nil {
		return lastErr
	}
	return waitErr
}

// transientAPIError reports whether an API error is worth retrying. Dial
// failures during control-plane recovery (connection refused, no route to
// host) arrive as non-APIStatus errors; only explicit terminal statuses
// (auth, not-found, invalid) are treated as permanent.
func transientAPIError(err error) bool {
	switch {
	case err == nil:
		return false
	case apierrors.IsUnauthorized(err), apierrors.IsForbidden(err),
		apierrors.IsNotFound(err), apierrors.IsInvalid(err):
		return false
	default:
		return true
	}
}

// agentShutdownSweep runs after the agent's manager stops (SIGTERM). A
// cordoned node is being drained for a reboot or upgrade: release Secondary
// backings so the backend pool can export. Gated on cordon because an
// ungated teardown would disconnect idle replicas on every pod rollout. A
// leftover write barrier is also kernel state that must not outlive the
// process.
func agentShutdownSweep(cordon *agent.CordonWatcher, driver *drbd.Driver) {
	if cordon.Cordoned() {
		ctx, cancel := context.WithTimeout(context.Background(), drbdShutdownTimeout)
		defer cancel()
		setupLog.Info("node cordoned; releasing Secondary DRBD backings for shutdown")
		if err := driver.DownSecondaries(ctx); err != nil {
			setupLog.Error(err, "DRBD shutdown teardown failed; node reboot may stall")
		}
	}
	// Short API budget: the chart grants 60s of termination grace and the
	// manager stop + DownSecondaries already spend up to 45s of it. A
	// stranded barrier missed here is lifted by the startup sweep on the
	// next boot.
	if err := resumeStaleBarriers(driver, apiShutdownWait); err != nil {
		setupLog.Error(err, "shutdown barrier sweep failed")
	}
}

// sweepOrphans removes DRBD state with no owning volume on this node,
// using a direct (uncached) client — the manager has not started yet.
func sweepOrphans(nodeName string, driver *drbd.Driver) error {
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := listWithRetry(c, vols, apiStartupWait); err != nil {
		return err
	}
	owned := map[string]bool{}
	for _, v := range vols.Items {
		for _, rep := range v.Spec.Replicas {
			if rep.Node == nodeName {
				owned[v.Name] = true
			}
		}
		// A held finalizer without a spec entry is a replica pending
		// removal: its teardown is the reconciler's, gated on the
		// remaining replicas' health — not the orphan sweep's.
		for _, f := range v.Finalizers {
			if f == constants.FinalizerPrefix+nodeName {
				owned[v.Name] = true
			}
		}
	}
	return driver.SweepOrphans(context.Background(),
		func(name string) bool { return owned[name] })
}

// resumeStaleBarriers lifts suspend-io left behind by a previous crash.
// The kernel's view drives the sweep: a crash between suspend-io and the
// status patch leaves a frozen device no snapshot records. Barriers whose
// round is still within the deadline are the reconciler's to drive.
func resumeStaleBarriers(driver *drbd.Driver, apiBudget time.Duration) error {
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := listWithRetry(c, snaps, apiBudget); err != nil {
		return err
	}
	fresh := map[string]bool{}
	for _, s := range snaps.Items {
		if s.Status.IOSuspended && s.Status.SuspendedAt != nil &&
			time.Since(s.Status.SuspendedAt.Time) < agent.SuspendDeadline {
			fresh[s.Spec.VolumeName] = true
		}
	}
	suspended, err := driver.UserSuspended(context.Background())
	if err != nil {
		// No kernel view (e.g. module not loaded yet) also means nothing
		// can be suspended — don't block agent startup on it.
		setupLog.Error(err, "cannot list suspended resources; skipping barrier sweep")
		return nil
	}
	for _, vol := range suspended {
		if fresh[vol] {
			continue
		}
		if err := driver.ResumeIO(context.Background(), vol); err != nil {
			return fmt.Errorf("resume stale barrier on %s: %w", vol, err)
		}
	}
	return nil
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
