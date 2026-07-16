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
// One binary, several modes:
//
//	--mode=controller  CSI Identity+Controller services (Deployment)
//	--mode=agent       CSI Identity+Node services + node reconciler (DaemonSet)
//	--mode=gateway     NFS-Ganesha share manager for one RWX volume (per-volume Deployment)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/robfig/cron/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
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
	"github.com/home-operations/miroir/internal/export"
	"github.com/home-operations/miroir/internal/gateway"
	"github.com/home-operations/miroir/internal/membership"
	"github.com/home-operations/miroir/internal/nodemap"
	"github.com/home-operations/miroir/internal/topology"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

const modeController = "controller"

// Populated via -ldflags at build time.
var (
	version = "dev"
	commit  = "unknown"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(miroirv1alpha1.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
}

// setupMembership registers the membership reconciler (completes
// operator-added replica entries), the tie-breaker retrofit for
// pre-existing 2-replica freeze volumes (#70) when enabled, the
// auto-diskful converter for long-lived client legs when a threshold is
// set, and the auto-evict reconciler for dead nodes when its threshold
// is set.
func setupMembership(mgr ctrl.Manager, nodes nodemap.Source, autoTieBreaker bool,
	autoDiskfulAfter, autoEvictAfter time.Duration,
) error {
	r := &membership.Reconciler{Client: mgr.GetClient(), Nodes: nodes}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("membership reconciler: %w", err)
	}
	if autoDiskfulAfter > 0 {
		ad := &membership.AutoDiskfulReconciler{
			Client:   mgr.GetClient(),
			Nodes:    nodes,
			After:    autoDiskfulAfter,
			Recorder: mgr.GetEventRecorder("miroir-controller"),
		}
		if err := ad.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("auto-diskful reconciler: %w", err)
		}
	}
	if autoEvictAfter > 0 {
		ae := &membership.AutoEvictReconciler{
			Client:   mgr.GetClient(),
			Nodes:    nodes,
			After:    autoEvictAfter,
			Recorder: mgr.GetEventRecorder("miroir-controller"),
		}
		if err := ae.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("auto-evict reconciler: %w", err)
		}
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

// setupExport registers the RWX gateway reconciler, which maintains the
// per-volume NFS-Ganesha Deployment and Service. It is skipped when no
// gateway image is configured — RWX is off until the chart wires one.
func setupExport(mgr ctrl.Manager, namespace, image, serviceAccount string) error {
	if image == "" {
		setupLog.Info("no --gateway-image set; RWX (ReadWriteMany) volumes are disabled")
		return nil
	}
	r := &export.Reconciler{
		Client:         mgr.GetClient(),
		Namespace:      namespace,
		Image:          image,
		ServiceAccount: serviceAccount,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("export reconciler: %w", err)
	}
	return nil
}

// fetchMiroirNode reads this node's MiroirNode straight from the API
// server (the cache has not started), retrying transient errors within the
// startup budget so a reboot that races control-plane recovery does not
// churn through CrashLoopBackOff. found is false when no MiroirNode names
// this node — it holds no storage and runs a client-only agent.
func fetchMiroirNode(r client.Reader, name string, budget time.Duration) (*miroirv1alpha1.MiroirNode, bool, error) {
	node := &miroirv1alpha1.MiroirNode{}
	err := apiWithRetry(budget, func(ctx context.Context) error {
		return r.Get(ctx, types.NamespacedName{Name: name}, node)
	})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return node, true, nil
}

// verifyMiroirNodeCRD refuses to run against an out-of-date MiroirNode CRD.
// Helm applies crds/ only on install, never on upgrade, and the failure
// mode is silent: the API server prunes the spec fields a stale schema does
// not know, so chart-rendered MiroirNodes lose pools, zone, and address
// with no error anywhere. The generated CRD carries a schema-revision
// annotation; a mismatch — or its absence, a pre-revision CRD — means the
// CRDs were not refreshed alongside the chart, so fail loudly instead.
func verifyMiroirNodeCRD(r client.Reader, budget time.Duration) {
	if err := checkMiroirNodeCRD(r, budget); err != nil {
		setupLog.Error(err, "refusing to start on an out-of-date MiroirNode CRD")
		os.Exit(1)
	}
}

func checkMiroirNodeCRD(r client.Reader, budget time.Duration) error {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	err := apiWithRetry(budget, func(ctx context.Context) error {
		return r.Get(ctx, types.NamespacedName{Name: miroirv1alpha1.MiroirNodeCRDName}, crd)
	})
	if err != nil {
		return fmt.Errorf("read the MiroirNode CRD: %w", err)
	}
	if got := crd.Annotations[miroirv1alpha1.SchemaRevisionAnnotation]; got != miroirv1alpha1.MiroirNodeSchemaRevision {
		return fmt.Errorf("the installed MiroirNode CRD carries schema revision %q, this release needs %q: "+
			"an old schema silently prunes spec fields, and Helm never applies crds/ on upgrade — "+
			"apply the chart's CRDs first (see https://miroir.home-operations.com/upgrading/)",
			got, miroirv1alpha1.MiroirNodeSchemaRevision)
	}
	return nil
}

// cacheOptions builds the manager cache config. SSA-heavy objects grow a
// managedFields entry per field manager (every agent + the CSI controller
// write each volume), so strip them from cached copies — nothing reads
// them locally (conflict detection is server-side; SSA patches build fresh
// objects). In controller mode the export reconciler manages gateway
// Deployments and Services only in its own namespace, so scope those
// informers there: Owns() otherwise lists them cluster-wide, which the
// namespaced Role neither grants nor should. Other types stay cluster-scoped.
func cacheOptions(mode, namespace string) cache.Options {
	opts := cache.Options{DefaultTransform: cache.TransformStripManagedFields()}
	if mode == modeController && namespace != "" {
		opts.ByObject = map[client.Object]cache.ByObject{
			&appsv1.Deployment{}: {Namespaces: map[string]cache.Config{namespace: {}}},
			&corev1.Service{}:    {Namespaces: map[string]cache.Config{namespace: {}}},
		}
	}
	return opts
}

// volumeGroupFor names the LVM VG backing a pool. The default pool keeps
// the pre-multi-pool name so existing VGs keep working across the upgrade;
// every other pool gets its own suffixed VG.
func volumeGroupFor(pool string) string {
	if pool == miroirv1alpha1.DefaultPoolName {
		return "vg-miroir"
	}
	return "vg-miroir-" + pool
}

// agentTopology reads this node's MiroirNode (with the startup retry
// budget) and arms the watcher that restarts the agent when the pool spec
// drifts from this snapshot — or appears at all — so a chart-applied pool
// edit reaches the agent without the ConfigMap-checksum pod roll it used
// to ride. found is false for a client-only node (no MiroirNode).
func agentTopology(mgr manager.Manager, nodeName string, stop context.CancelFunc) (*miroirv1alpha1.MiroirNode, bool) {
	miroirNode, found, err := fetchMiroirNode(mgr.GetAPIReader(), nodeName, apiStartupWait)
	if err != nil {
		setupLog.Error(err, "unable to read this node's MiroirNode")
		os.Exit(1)
	}
	var bootedPools []miroirv1alpha1.MiroirNodePool
	if found {
		bootedPools = miroirNode.Spec.Pools
	}
	if err := (&agent.TopologyWatcher{
		Client:      mgr.GetClient(),
		NodeName:    nodeName,
		BootedPools: bootedPools,
		Stop:        stop,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up topology watcher")
		os.Exit(1)
	}
	return miroirNode, found
}

// poolBackendsFor builds one backend per pool from this node's flattened
// MiroirNode spec.
func poolBackendsFor(nodeName string, entry nodemap.Node) (agent.Pools, error) {
	pools := agent.Pools{}
	for name, p := range entry.Pools {
		be, err := backend.New(p.Backend, backend.Config{
			VolumeGroup:     volumeGroupFor(name),
			ThinPool:        "thinpool",
			Device:          p.Device,
			Dataset:         p.ZFSDataset,
			ZFSVolBlockSize: p.ZFSVolBlockSizeBytes(),
			ZFSCompression:  p.ZFSCompression,
			PoolSize:        p.ThinPoolSize,
			BaseDir:         p.BaseDir,
		}, backend.RealExec)
		if err != nil {
			return nil, fmt.Errorf("backend for node %s pool %s: %w", nodeName, name, err)
		}
		pools[name] = agent.PoolBackend{Backend: be, Type: p.Backend}
	}
	return pools, nil
}

// setupAgentPools bootstraps the node-local pools before the agent serves
// anything: first start on a fresh node creates PV/VG/thin-pool (lvmthin)
// or the parent dataset (zfs). One bad pool must not take the good ones
// down — its volumes fail their reconciles with real errors while the
// rest of the node keeps serving. All pools failing means the node is
// misconfigured wholesale; exit like the single-pool agent always did so
// the CrashLoopBackOff is impossible to miss.
func setupAgentPools(pools agent.Pools) {
	failed := 0
	for _, name := range slices.Sorted(maps.Keys(pools)) {
		if err := pools[name].Backend.Setup(context.Background()); err != nil {
			setupLog.Error(err, "backend pool setup failed; volumes in this pool will fail until it is fixed",
				"pool", name)
			failed++
		}
	}
	if failed == len(pools) {
		setupLog.Error(nil, "every pool failed setup", "pools", len(pools))
		os.Exit(1)
	}
}

// validateDRBDPortBase exits on an out-of-range base: the allocator hands
// out ports ascending from it, so a base near 65535 overflows the port
// space only once volumes accumulate — fail at startup instead.
func validateDRBDPortBase(base int) {
	if base < 1024 || base > 64000 {
		setupLog.Error(nil, "--drbd-port-base must be within 1024-64000", "value", base)
		os.Exit(1)
	}
}

// addVerifyScheduler registers the online-verify scheduler when a schedule is
// set and the DRBD kernel side is present. An invalid cron spec is a
// misconfiguration — fail at startup rather than silently never verifying.
func addVerifyScheduler(mgr manager.Manager, nodeName string, drbdReady bool, schedule string, d *drbd.Driver) {
	if !drbdReady || schedule == "" {
		return
	}
	parsed, err := cron.ParseStandard(schedule)
	if err != nil {
		setupLog.Error(err, "invalid --verify-schedule", "value", schedule)
		os.Exit(1)
	}
	if err := mgr.Add(&agent.VerifyScheduler{
		Client:   mgr.GetClient(),
		NodeName: nodeName,
		DRBD:     d,
		Schedule: parsed,
		Recorder: mgr.GetEventRecorder("miroir-agent"),
	}); err != nil {
		setupLog.Error(err, "unable to add verify scheduler")
		os.Exit(1)
	}
}

// runGateway serves one RWX volume over NFS and blocks until the process
// is signalled. It builds a direct client (no manager/cache) and drives
// the host's DRBD/mount tooling like the agent, exiting non-zero if the
// export ever fails so the pod restarts.
func runGateway(nodeName, volumeName, exportDir, ganeshaConf, drbdStateDir, httpAddr string) {
	if nodeName == "" {
		setupLog.Error(nil, "--node-name (or NODE_NAME) is required in gateway mode")
		os.Exit(1)
	}
	if volumeName == "" {
		setupLog.Error(nil, "--volume is required in gateway mode")
		os.Exit(1)
	}
	cl, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to build client")
		os.Exit(1)
	}
	drbdDriver := &drbd.Driver{StateDir: drbdStateDir, Exec: backend.RealExec}
	if err := gateway.Run(ctrl.SetupSignalHandler(), cl, drbdDriver, gateway.Config{
		VolumeID:    volumeName,
		NodeName:    nodeName,
		ExportDir:   exportDir,
		GaneshaConf: ganeshaConf,
		HTTPAddr:    httpAddr,
	}, setupLog.WithName("gateway")); err != nil {
		setupLog.Error(err, "gateway exited")
		os.Exit(1)
	}
}

func main() {
	var (
		mode             string
		csiSocket        string
		metricsAddr      string
		provisionTimeout time.Duration
		overcommitRatio  float64
		freeSpaceRatio   float64
		autoTieBreaker   bool
		autoDiskfulAfter time.Duration
		autoEvictAfter   time.Duration
		drbdPortBase     int
		leaderElect      bool
		leaderElectionID string
		leaderElectionNS string
		podNamespace     string
		gatewayImage     string
		gatewaySA        string

		// agent mode
		nodeName          string
		drbdStateDir      string
		poolStatsInterval time.Duration
		volumeWorkers     int
		verifySchedule    string

		// gateway mode
		volumeName  string
		exportDir   string
		ganeshaConf string
	)
	flag.StringVar(&mode, "mode", "", "controller | agent | gateway")
	flag.StringVar(&csiSocket, "csi-socket", "/csi/csi.sock", "CSI gRPC unix socket path")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8081",
		"single operational endpoint: /metrics plus the /healthz and /readyz probes (org port standard)")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "this node's name (agent)")
	flag.DurationVar(&provisionTimeout, "provision-timeout", 0,
		"wait for agents to realise a new volume (controller; 0 → default)")
	flag.Float64Var(&overcommitRatio, "overcommit-ratio", 0,
		"max provisioned-over-capacity per pool before CreateVolume is refused (controller; 0 → default 2.0)")
	flag.Float64Var(&freeSpaceRatio, "free-space-ratio", 0,
		"max provisioned-over-physically-free per pool before CreateVolume is refused (controller; 0 → default 20.0)")
	flag.DurationVar(&autoDiskfulAfter, "auto-diskful-after", 0,
		"convert a diskless client leg into a diskful replica once it has been attached this long "+
			"(controller; 0 disables; needs a storage node with capacity — see LINSTOR auto-diskful)")
	flag.DurationVar(&autoEvictAfter, "auto-evict-after", 0,
		"re-place a dead storage node's replicas once its MiroirNode heartbeat has been stale this long "+
			"(controller; 0 disables; needs a spare storage node — see LINSTOR auto-evict)")
	flag.BoolVar(&autoTieBreaker, "auto-tie-breaker", true,
		"add a diskless tie-breaker to 2-replica freeze volumes when a spare node exists (controller)")
	flag.IntVar(&drbdPortBase, "drbd-port-base", 7000,
		"lowest TCP port for DRBD replication links, one per replicated volume ascending "+
			"(controller; raise to avoid host-network tenants like Ceph mgr dashboard on 7000)")
	flag.BoolVar(&leaderElect, "leader-elect", false,
		"elect a leader via a coordination.k8s.io Lease so extra replicas stand by warm (controller)")
	flag.StringVar(&leaderElectionID, "leader-election-id", "miroir-controller",
		"leader-election Lease name; keep it stable across upgrades (controller)")
	flag.StringVar(&leaderElectionNS, "leader-election-namespace", "",
		"leader-election Lease namespace; empty auto-detects the pod's namespace in-cluster (controller)")
	flag.StringVar(&podNamespace, "namespace", os.Getenv("POD_NAMESPACE"),
		"the controller's own namespace, where per-RWX-volume gateway workloads are created (controller)")
	flag.StringVar(&gatewayImage, "gateway-image", "",
		"container image for per-RWX-volume NFS gateway pods; empty disables RWX (controller)")
	flag.StringVar(&gatewaySA, "gateway-service-account", "",
		"ServiceAccount for gateway pods, with the RBAC the gateway needs (controller)")
	flag.IntVar(&volumeWorkers, "volume-workers", 4,
		"concurrent volume reconciles per agent (agent)")
	flag.DurationVar(&poolStatsInterval, "pool-stats-interval", 0,
		"how often the agent republishes pool capacity (agent; 0 → default 60s)")
	flag.StringVar(&verifySchedule, "verify-schedule", "",
		"cron spec (5-field, agent-local time) for scheduled online verify of the volumes this "+
			"node coordinates (agent; empty disables; requires verify-alg in the DRBD common config)")
	flag.StringVar(&drbdStateDir, "drbd-state-dir", "/etc/drbd.d",
		"rendered DRBD config dir (agent; hostPath-backed)")
	flag.StringVar(&volumeName, "volume", "", "MiroirVolume to export over NFS (gateway)")
	flag.StringVar(&exportDir, "export-dir", "/export",
		"parent directory for the per-volume mount point (gateway)")
	flag.StringVar(&ganeshaConf, "ganesha-conf", "/etc/ganesha/ganesha.conf",
		"path the rendered NFS-Ganesha config is written to (gateway)")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog.Info("starting miroir", "mode", mode, "version", version, "commit", commit)

	validateDRBDPortBase(drbdPortBase)

	// Gateway mode drives the host directly and needs no controller-runtime
	// manager, so it builds none and exits before one is constructed.
	if mode == "gateway" {
		// The gateway skips the manager but serves the same operational
		// endpoint itself (/healthz liveness + /metrics) on metricsAddr.
		runGateway(nodeName, volumeName, exportDir, ganeshaConf, drbdStateDir, metricsAddr)
		return
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: metricsAddr},
		Cache:   cacheOptions(mode, podNamespace),
		// The dedicated health-probe server is disabled; the probes are
		// co-hosted on the (plain HTTP) metrics listener so each workload
		// exposes a single operational port — the agent runs hostNetwork,
		// so every listener occupies a real node port.
		HealthProbeBindAddress: "0",
		// Leader election is the opt-in controller HA mode (#132): extra
		// replicas stand by warm and only the reconcilers wait on the
		// Lease — the cache, metrics server, and CSI socket run on every
		// replica because each pod's CSI sidecars elect independently and
		// reach the driver over the pod-local socket. Gated on controller
		// mode: agents are per-node singletons, and a shared Lease would
		// serialize the whole DaemonSet down to one working node.
		LeaderElection:          mode == modeController && leaderElect,
		LeaderElectionID:        leaderElectionID,
		LeaderElectionNamespace: leaderElectionNS,
		// Safe because nothing runs after mgr.Start returns in controller
		// mode (the shutdown sweep is agent-only), so the released Lease
		// can't be beaten to by a still-writing old leader.
		LeaderElectionReleaseOnCancel: true,
		Controller: config.Controller{
			// The priority queue (default-on since controller-runtime
			// v0.22) enqueues initial-list events at low priority, and a
			// steadily busy queue never drains them: a volume created
			// moments before an agent start is delivered only through the
			// initial list, so its realization starves indefinitely —
			// silently, and again after every restart. FIFO restores the
			// guarantee that startup work eventually runs.
			UsePriorityQueue: new(false),
		},
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

	identity := &csi.Identity{Version: version, WithController: mode == modeController}

	// Agent mode only, run after the manager stops (SIGTERM): release DRBD
	// backings when the node is going down and lift any leftover write
	// barrier — both are kernel state that outlives the process.
	var shutdownSweep func()

	// The signal context is created before the mode switch so the agent's
	// topology watcher can stop the manager gracefully (restart-to-reload)
	// through the same cancellation path a SIGTERM takes.
	ctx, stop := context.WithCancel(ctrl.SetupSignalHandler())
	defer stop()

	switch mode {
	case modeController:
		verifyMiroirNodeCRD(mgr.GetAPIReader(), apiStartupWait)
		// The topology is watch-driven: placement and membership fold the
		// MiroirNode CRs from the cache on every RPC/reconcile, so a
		// chart-applied topology edit takes effect without a restart.
		nodes := &nodemap.CRSource{Reader: mgr.GetClient()}
		controller := &csi.Controller{
			Client:           mgr.GetClient(),
			APIReader:        mgr.GetAPIReader(),
			Nodes:            nodes,
			ProvisionTimeout: provisionTimeout,
			OvercommitRatio:  overcommitRatio,
			FreeSpaceRatio:   freeSpaceRatio,
			AutoTieBreaker:   autoTieBreaker,
			RWXEnabled:       gatewayImage != "",
			DRBDPortBase:     int32(drbdPortBase),
		}
		if err := setupMembership(mgr, nodes, autoTieBreaker, autoDiskfulAfter, autoEvictAfter); err != nil {
			setupLog.Error(err, "unable to set up membership reconcilers")
			os.Exit(1)
		}
		// Cross-object topology rules (duplicate replication address) are
		// reported as MiroirNode conditions — a CRD validates one object at
		// a time, and placement already refuses conflicted nodes.
		if err := (&topology.ConflictReconciler{
			Client:   mgr.GetClient(),
			Recorder: mgr.GetEventRecorder("miroir-controller"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up topology conflict reconciler")
			os.Exit(1)
		}
		if err := setupExport(mgr, podNamespace, gatewayImage, gatewaySA); err != nil {
			setupLog.Error(err, "unable to set up export reconciler")
			os.Exit(1)
		}
		serveCSI(mgr, csiSocket, identity, controller, nil)

	case "agent":
		if nodeName == "" {
			setupLog.Error(nil, "--node-name (or NODE_NAME) is required in agent mode")
			os.Exit(1)
		}
		verifyMiroirNodeCRD(mgr.GetAPIReader(), apiStartupWait)
		// The DaemonSet's chart-side scope is every schedulable node, but
		// only storage nodes run agent-backed backends. A node with no
		// MiroirNode holds no volumes and runs a client-only node service so
		// pods there can still mount RWX (NFS) volumes.
		miroirNode, found := agentTopology(mgr, nodeName, stop)
		if !found {
			setupLog.Info("no MiroirNode for this node; running client-only node service", "node", nodeName)
			// Client legs get DRBD configs rendered here too, so the
			// kernel floor binds on client-only nodes as well.
			clientDRBD := &drbd.Driver{StateDir: drbdStateDir, Exec: backend.RealExec}
			probeDRBD(clientDRBD)
			node := csi.NewNode(mgr.GetClient(), nodeName, clientDRBD)
			serveCSI(mgr, csiSocket, identity, nil, node)
			break
		}
		pools, err := poolBackendsFor(nodeName, nodemap.FromSpec(miroirNode.Spec))
		if err != nil {
			setupLog.Error(err, "unable to build the node's backends")
			os.Exit(1)
		}
		setupAgentPools(pools)
		drbdDriver := &drbd.Driver{StateDir: drbdStateDir, Exec: backend.RealExec}
		// The binary is always in the image; what a local-only node lacks
		// is the kernel module. Probe once (the modprobe inside also loads
		// it proactively on nodes that ship it) and run without the DRBD
		// machinery when the kernel side is absent — otherwise the events
		// watcher hot-loops "exit status 20" every 5s forever.
		drbdKernel, drbdReady := probeDRBD(drbdDriver)
		if !drbdReady {
			setupLog.Info("DRBD kernel module unavailable; running local-only " +
				"(no events watcher, no orphan/barrier/shutdown sweeps)")
		}
		if drbdReady {
			// Reap kernel resources and rendered config orphaned by a crash
			// between up and down — they hold backing devices open forever.
			// The sweeps return an error only when the API list fails: exit
			// so the restart retries it (without the list they cannot tell
			// orphaned from owned, and nothing else re-runs them). Sweep
			// execution itself is best-effort and logged inside — a wedged
			// resource (LINBIT/drbd#137) must not keep the agent from
			// serving the node's healthy volumes (issue #195).
			if err := sweepOrphans(nodeName, drbdDriver); err != nil {
				setupLog.Error(err, "orphan sweep failed")
				os.Exit(1)
			}
			// Lift any IO barrier left by a previous agent crash; same
			// fatal-only-on-API-failure contract.
			if err := resumeStaleBarriers(drbdDriver, apiStartupWait); err != nil {
				setupLog.Error(err, "barrier resume sweep failed")
				os.Exit(1)
			}
		}
		// Tracks this node's cordon state so shutdownSweep can tell a node
		// reboot/upgrade (drained, so cordoned) from a routine pod restart.
		cordon := &agent.CordonWatcher{Client: mgr.GetClient(), NodeName: nodeName}
		if err := cordon.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up cordon watcher")
			os.Exit(1)
		}
		var drbdEvents chan event.GenericEvent
		if drbdReady {
			shutdownSweep = func() { agentShutdownSweep(cordon, drbdDriver) }
			// events2 turns kernel state changes into immediate reconciles;
			// the 30s poll remains as the safety net.
			drbdEvents = make(chan event.GenericEvent, 64)
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
		}
		reconciler := &agent.VolumeReconciler{
			Client:     mgr.GetClient(),
			NodeName:   nodeName,
			Pools:      pools,
			DRBD:       drbdDriver,
			DRBDEvents: drbdEvents,
			Workers:    volumeWorkers,
			Recorder:   mgr.GetEventRecorder("miroir-agent"),
		}
		if err := reconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up agent reconciler")
			os.Exit(1)
		}
		snapReconciler := &agent.SnapshotReconciler{
			Client:   mgr.GetClient(),
			NodeName: nodeName,
			Pools:    pools,
			DRBD:     drbdDriver,
			Recorder: mgr.GetEventRecorder("miroir-agent"),
		}
		if err := snapReconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to set up snapshot reconciler")
			os.Exit(1)
		}
		// Publishes this node's pool capacities for capacity-aware placement.
		if err := mgr.Add(&agent.PoolStatsPublisher{
			Client:      mgr.GetClient(),
			NodeName:    nodeName,
			Pools:       pools,
			Interval:    poolStatsInterval,
			Recorder:    mgr.GetEventRecorder("miroir-agent"),
			DRBDVersion: drbdKernel,
		}); err != nil {
			setupLog.Error(err, "unable to add pool stats publisher")
			os.Exit(1)
		}
		// Scheduled online verify — the only cross-leg integrity check. Needs
		// the DRBD kernel side, so it is gated on drbdReady like the sweeps.
		addVerifyScheduler(mgr, nodeName, drbdReady, verifySchedule, drbdDriver)
		node := csi.NewNode(mgr.GetClient(), nodeName, drbdDriver)
		serveCSI(mgr, csiSocket, identity, nil, node)

	default:
		setupLog.Error(nil, "--mode must be controller, agent, or gateway")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	err = mgr.Start(ctx)
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

// apiWithRetry retries one API call until it succeeds, hits a terminal
// (non-transient) error, or the budget elapses — so a control plane still
// coming back up does not crash the process on startup. It is the one
// retry policy every pre-manager API access shares.
func apiWithRetry(budget time.Duration, op func(ctx context.Context) error) error {
	var lastErr error
	waitErr := wait.PollUntilContextTimeout(context.Background(), 2*time.Second, budget, true,
		func(ctx context.Context) (bool, error) {
			lastErr = op(ctx)
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

// listWithRetry retries an API list with the shared startup retry policy.
func listWithRetry(c client.Client, list client.ObjectList, budget time.Duration) error {
	return apiWithRetry(budget, func(ctx context.Context) error { return c.List(ctx, list) })
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
// Returns an error only when the volume list cannot be fetched; the sweep
// itself is best-effort and its failures are logged here (issue #195).
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
	if err := driver.SweepOrphans(context.Background(),
		func(name string) bool { return owned[name] }); err != nil {
		setupLog.Error(err, "orphan sweep incomplete")
	}
	return nil
}

// resumeStaleBarriers lifts suspend-io left behind by a previous crash.
// The kernel's view drives the sweep: a crash between suspend-io and the
// status patch leaves a frozen device no snapshot records. Barriers whose
// round is still within the deadline are the reconciler's to drive.
// Returns an error only when the snapshot list cannot be fetched; resume
// failures are per-resource, logged here — one wedged resource must not
// strand the other frozen volumes' barriers (issue #195).
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
	var errs []error
	for _, vol := range suspended {
		if fresh[vol] {
			continue
		}
		if err := driver.ResumeIO(context.Background(), vol); err != nil {
			errs = append(errs, fmt.Errorf("resume stale barrier on %s: %w", vol, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		setupLog.Error(err, "barrier resume sweep incomplete")
	}
	return nil
}

// probeDRBD reports whether the DRBD kernel side is usable and, when it
// is, the module version — exiting below drbd.KernelFloor: a 9.3.1-era
// option rendered against an older module errors drbdadm for every
// resource on the node, so failing fast here beats poisoning them all
// later. Talos ≥ 1.13.0 ships a module at the floor.
func probeDRBD(driver *drbd.Driver) (version string, ready bool) {
	if !driver.KernelAvailable(context.Background()) {
		return "", false
	}
	v, err := driver.KernelVersion(context.Background())
	if err != nil {
		// The module answered but the version read flaked; running
		// unchecked beats refusing a working node.
		setupLog.Error(err, "cannot read DRBD kernel module version; skipping floor check")
		return "", true
	}
	if drbd.BelowKernelFloor(v) {
		setupLog.Error(nil, "DRBD kernel module is below the supported floor; upgrade the node (Talos >= 1.13.0)",
			"version", v, "floor", drbd.KernelFloor)
		os.Exit(1)
	}
	agent.RecordDRBDKernelVersion(v)
	return v, true
}

// csiRunnable marks the CSI server as running on every replica rather than
// only the elected leader: each pod's sidecars hold their own Leases and
// reach the driver over the pod-local socket, so a standby's gRPC server
// must be up for its sidecars to probe (and to act the moment one of them
// wins its lease). Without this, mgr.Add defaults a plain Runnable into the
// leader-election group.
type csiRunnable struct{ manager.Runnable }

func (csiRunnable) NeedLeaderElection() bool { return false }

// serveCSI runs the CSI gRPC server alongside the manager; controller and
// node are mutually exclusive (one per mode).
func serveCSI(mgr ctrl.Manager, socket string, identity *csi.Identity, controller *csi.Controller, node *csi.Node) {
	err := mgr.Add(csiRunnable{manager.RunnableFunc(func(ctx context.Context) error {
		// CSI RPCs read CRs through the manager's cache; wait for sync so
		// early kubelet/sidecar calls don't race a cold cache.
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return context.Canceled
		}
		if controller != nil {
			return csi.Serve(ctx, socket, identity, controller, nil)
		}
		return csi.Serve(ctx, socket, identity, nil, node)
	})})
	if err != nil {
		setupLog.Error(err, "unable to add CSI server to manager")
		os.Exit(1)
	}
}
