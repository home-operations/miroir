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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
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

const (
	modeController = "controller"
	modeAgent      = "agent"
	modeGateway    = "gateway"
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
// operator-added replica entries), the tie-breaker retrofit for
// pre-existing 2-replica freeze volumes when enabled, the
// auto-diskful converter for long-lived client legs when a threshold is
// set, and the auto-evict reconciler for dead nodes when its threshold
// is set.
func setupMembership(mgr ctrl.Manager, nodes nodemap.Source, autoTieBreaker bool,
	autoDiskfulAfter, autoEvictAfter time.Duration,
) error {
	r := &membership.Reconciler{Client: mgr.GetClient(), Nodes: nodes, PVs: mgr.GetAPIReader()}
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

// setupTopology registers the cross-object topology reconcilers: the
// conflict pass (duplicate replication addresses reported as MiroirNode
// conditions — a CRD validates one object at a time, and placement
// already refuses conflicted nodes) and the node-group materializer (one
// MiroirNode per label-matched node, so a homogeneous fleet is one object
// and joining it is labeling the node).
func setupTopology(mgr ctrl.Manager) error {
	if err := (&topology.ConflictReconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorder("miroir-controller"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("conflict reconciler: %w", err)
	}
	if err := (&topology.NodeGroupReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("node group reconciler: %w", err)
	}
	return nil
}

// setupExport registers the RWX gateway reconciler, which maintains the
// per-volume NFS-Ganesha Deployment and Service. It is skipped when no
// gateway image is configured — RWX is off until the chart wires one.
func setupExport(mgr ctrl.Manager, namespace, image, serviceAccount string,
	resources corev1.ResourceRequirements) error {
	if image == "" {
		setupLog.Info("no --gateway-image set; RWX (ReadWriteMany) volumes are disabled")
		return nil
	}
	r := &export.Reconciler{
		Client:         mgr.GetClient(),
		Namespace:      namespace,
		Image:          image,
		ServiceAccount: serviceAccount,
		Resources:      resources,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("export reconciler: %w", err)
	}
	return nil
}

// parseGatewayResources decodes --gateway-resources, a JSON
// ResourceRequirements (the chart renders its gateway.resources value with
// toJson). Empty selects export.DefaultResources. Unknown fields are an
// error: the chart schema is permissive, so a misspelt "limit" would
// otherwise leave gateways unlimited without a word.
func parseGatewayResources(raw string) (corev1.ResourceRequirements, error) {
	if raw == "" {
		return export.DefaultResources(), nil
	}
	var res corev1.ResourceRequirements
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("--gateway-resources: %w", err)
	}
	// More() stops at a stray closing delimiter, so decode again and
	// require EOF to reject anything after the object.
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return corev1.ResourceRequirements{}, errors.New("--gateway-resources: trailing data after the JSON object")
	}
	return res, nil
}

// fetchMiroirNode reads this node's MiroirNode straight from the API
// server (the cache has not started), retrying transient errors within the
// startup budget so a reboot that races control-plane recovery does not
// churn through CrashLoopBackOff. found is false when no MiroirNode names
// this node — it holds no storage and runs a client-only agent.
func fetchMiroirNode(r client.Reader, name string, budget time.Duration) (*miroirv1alpha1.MiroirNode, bool, error) {
	node := &miroirv1alpha1.MiroirNode{}
	err := apiWithRetry(context.Background(), budget, func(ctx context.Context) error {
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

// cacheOptions builds the manager cache config. SSA-heavy objects grow a
// managedFields entry per field manager (every agent + the CSI controller
// write each volume), so strip them from cached copies — nothing reads
// them locally (conflict detection is server-side; SSA patches build fresh
// objects). In controller mode the export reconciler manages gateway
// Deployments and Services only in its own namespace, so scope those
// informers there: Owns() otherwise lists them cluster-wide, which the
// namespaced Role neither grants nor should. In agent mode the MiroirNode
// informer is pinned to the node's own object — no agent consumer reads
// another node's (the topology watcher and poolstats are self-only), and
// an unscoped watch would deliver every node's per-minute status heartbeat
// to every agent: N cached objects and N events/min per agent, N² across
// the cluster, for reconciles a name predicate then discards. Other types
// stay cluster-scoped.
func cacheOptions(mode, namespace, nodeName string) cache.Options {
	opts := cache.Options{DefaultTransform: cache.TransformStripManagedFields()}
	if mode == modeController && namespace != "" {
		opts.ByObject = map[client.Object]cache.ByObject{
			&appsv1.Deployment{}: {Namespaces: map[string]cache.Config{namespace: {}}},
			&corev1.Service{}:    {Namespaces: map[string]cache.Config{namespace: {}}},
		}
	}
	if mode == modeAgent && nodeName != "" {
		// The corev1.Node informer gets the same pin: its only agent
		// consumer is the CordonWatcher, which reads the node's own object.
		opts.ByObject = map[client.Object]cache.ByObject{
			&miroirv1alpha1.MiroirNode{}: {
				Field: fields.OneTermEqualSelector("metadata.name", nodeName),
			},
			&corev1.Node{}: {
				Field: fields.OneTermEqualSelector("metadata.name", nodeName),
			},
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

// clientOnlyReason explains why this node's agent serves client-only, or
// returns "" when the MiroirNode carries usable pools.
func clientOnlyReason(found bool, miroirNode *miroirv1alpha1.MiroirNode, flat nodemap.Node) string {
	switch {
	case !found:
		return "no MiroirNode for this node; running client-only node service"
	case len(flat.Pools) > 0:
		return ""
	case len(miroirNode.Spec.Pools) > 0:
		// Pools exist in the spec but none flattened: block-less entries,
		// which only a pre-0.11 stored object can carry.
		return "MiroirNode pools carry no backend block (a pre-0.11 stored object?): " +
			"apply this node's MiroirNode manifest; running client-only node service"
	default:
		return "MiroirNode declares no usable pools; running client-only node service"
	}
}

// agentTopology reads this node's MiroirNode (with the startup retry
// budget) and arms the watcher that restarts the agent when the pool spec
// drifts from this snapshot — or appears at all — so a chart-applied pool
// edit reaches the agent without the ConfigMap-checksum pod roll it used
// to ride. found is false for a client-only node (no MiroirNode).
func agentTopology(mgr manager.Manager, nodeName string, stop context.CancelFunc,
) (*miroirv1alpha1.MiroirNode, bool, error) {
	miroirNode, found, err := fetchMiroirNode(mgr.GetAPIReader(), nodeName, apiStartupWait)
	if err != nil {
		return nil, false, fmt.Errorf("unable to read this node's MiroirNode: %w", err)
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
		return nil, false, fmt.Errorf("unable to set up topology watcher: %w", err)
	}
	return miroirNode, found, nil
}

// poolBackendsFor builds one backend per pool from this node's flattened
// MiroirNode spec. run is the shared wedge-tracking executor, so a kernel
// that strands an lvm or zfs child trips the same node-scoped breaker the
// DRBD driver reports to.
func poolBackendsFor(nodeName string, entry nodemap.Node, run backend.Exec) (agent.Pools, error) {
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
		}, run)
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
func setupAgentPools(pools agent.Pools) error {
	failed := 0
	for _, name := range slices.Sorted(maps.Keys(pools)) {
		if err := pools[name].Backend.Setup(context.Background()); err != nil {
			setupLog.Error(err, "backend pool setup failed; volumes in this pool will fail until it is fixed",
				"pool", name)
			failed++
		}
	}
	if failed == len(pools) {
		return fmt.Errorf("every pool failed setup (%d pools)", len(pools))
	}
	return nil
}

// validateMode rejects an unknown --mode before anything reaches the API.
func validateMode(mode string) error {
	if !slices.Contains([]string{modeController, modeAgent, modeGateway}, mode) {
		return fmt.Errorf("--mode must be controller, agent, or gateway, got %q", mode)
	}
	return nil
}

// validateDRBDPortBase rejects an out-of-range base: the allocator hands
// out ports ascending from it, so a base near 65535 overflows the port
// space only once volumes accumulate — fail at startup instead.
func validateDRBDPortBase(base int) error {
	if base < 1024 || base > 64000 {
		return fmt.Errorf("--drbd-port-base must be within 1024-64000, got %d", base)
	}
	return nil
}

// addVerifyScheduler registers the online-verify scheduler when a schedule is
// set and the DRBD kernel side is present. An invalid cron spec is a
// misconfiguration — fail at startup rather than silently never verifying.
func addVerifyScheduler(mgr manager.Manager, nodeName string, drbdReady bool, schedule string, d *drbd.Driver) error {
	if !drbdReady || schedule == "" {
		return nil
	}
	parsed, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid --verify-schedule %q: %w", schedule, err)
	}
	if err := mgr.Add(&agent.VerifyScheduler{
		Client:   mgr.GetClient(),
		NodeName: nodeName,
		DRBD:     d,
		Schedule: parsed,
		Recorder: mgr.GetEventRecorder("miroir-agent"),
	}); err != nil {
		return fmt.Errorf("unable to add verify scheduler: %w", err)
	}
	return nil
}

// runGateway serves one RWX volume over NFS and blocks until the process
// is signalled. It builds a direct client (no manager/cache) and drives
// the host's DRBD/mount tooling like the agent; a failed export is an
// error so the pod restarts.
func runGateway(o options) error {
	if o.nodeName == "" {
		return errors.New("--node-name (or NODE_NAME) is required in gateway mode")
	}
	if o.volumeName == "" {
		return errors.New("--volume is required in gateway mode")
	}
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("unable to load the kube config: %w", err)
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("unable to build client: %w", err)
	}
	drbdDriver := &drbd.Driver{StateDir: o.drbdStateDir, Exec: backend.RealExec}
	if err := gateway.Run(ctrl.SetupSignalHandler(), cl, drbdDriver, gateway.Config{
		VolumeID:    o.volumeName,
		NodeName:    o.nodeName,
		ExportDir:   o.exportDir,
		GaneshaConf: o.ganeshaConf,
		HTTPAddr:    o.metricsAddr,
	}, setupLog.WithName("gateway")); err != nil {
		return fmt.Errorf("gateway exited: %w", err)
	}
	return nil
}

// options holds the parsed command line.
type options struct {
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
	gatewayResources string

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
}

func parseFlags() (options, zap.Options) {
	var o options
	flag.StringVar(&o.mode, "mode", "", "controller | agent | gateway")
	flag.StringVar(&o.csiSocket, "csi-socket", "/csi/csi.sock", "CSI gRPC unix socket path")
	flag.StringVar(&o.metricsAddr, "metrics-bind-address", ":8081",
		"single operational endpoint: /metrics plus the /healthz and /readyz probes (org port standard)")
	flag.StringVar(&o.nodeName, "node-name", os.Getenv("NODE_NAME"), "this node's name (agent)")
	flag.DurationVar(&o.provisionTimeout, "provision-timeout", 0,
		"wait for agents to realise a new volume (controller; 0 → default)")
	flag.Float64Var(&o.overcommitRatio, "overcommit-ratio", 0,
		"max provisioned-over-capacity per pool before CreateVolume is refused (controller; 0 → default 2.0)")
	flag.Float64Var(&o.freeSpaceRatio, "free-space-ratio", 0,
		"max provisioned-over-physically-free per pool before CreateVolume is refused (controller; 0 → default 20.0)")
	flag.DurationVar(&o.autoDiskfulAfter, "auto-diskful-after", 0,
		"convert a diskless client leg into a diskful replica once it has been attached this long "+
			"(controller; 0 disables; needs a storage node with capacity — see LINSTOR auto-diskful)")
	flag.DurationVar(&o.autoEvictAfter, "auto-evict-after", 0,
		"re-place a dead storage node's replicas once its MiroirNode heartbeat has been stale this long "+
			"(controller; 0 disables; needs a spare storage node — see LINSTOR auto-evict)")
	flag.BoolVar(&o.autoTieBreaker, "auto-tie-breaker", true,
		"add a diskless tie-breaker to 2-replica freeze volumes when a spare node exists (controller)")
	flag.IntVar(&o.drbdPortBase, "drbd-port-base", 7000,
		"lowest TCP port for DRBD replication links, one per replicated volume ascending "+
			"(controller; raise to avoid host-network tenants like Ceph mgr dashboard on 7000)")
	flag.BoolVar(&o.leaderElect, "leader-elect", false,
		"elect a leader via a coordination.k8s.io Lease so extra replicas stand by warm (controller)")
	flag.StringVar(&o.leaderElectionID, "leader-election-id", "miroir-controller",
		"leader-election Lease name; keep it stable across upgrades (controller)")
	flag.StringVar(&o.leaderElectionNS, "leader-election-namespace", "",
		"leader-election Lease namespace; empty auto-detects the pod's namespace in-cluster (controller)")
	flag.StringVar(&o.podNamespace, "namespace", os.Getenv("POD_NAMESPACE"),
		"the controller's own namespace, where per-RWX-volume gateway workloads are created (controller)")
	flag.StringVar(&o.gatewayImage, "gateway-image", "",
		"container image for per-RWX-volume NFS gateway pods; empty disables RWX (controller)")
	flag.StringVar(&o.gatewaySA, "gateway-service-account", "",
		"ServiceAccount for gateway pods, with the RBAC the gateway needs (controller)")
	flag.StringVar(&o.gatewayResources, "gateway-resources", "",
		"JSON ResourceRequirements for the gateway container; empty → 10m/64Mi requests, no limits (controller)")
	flag.IntVar(&o.volumeWorkers, "volume-workers", 4,
		"concurrent volume reconciles per agent (agent)")
	flag.DurationVar(&o.poolStatsInterval, "pool-stats-interval", 0,
		"how often the agent republishes pool capacity (agent; 0 → default 60s)")
	flag.StringVar(&o.verifySchedule, "verify-schedule", "",
		"cron spec (5-field, agent-local time) for scheduled online verify of the volumes this "+
			"node coordinates (agent; empty disables; requires verify-alg in the DRBD common config)")
	flag.StringVar(&o.drbdStateDir, "drbd-state-dir", "/etc/drbd.d",
		"rendered DRBD config dir (agent; hostPath-backed)")
	flag.StringVar(&o.volumeName, "volume", "", "MiroirVolume to export over NFS (gateway)")
	flag.StringVar(&o.exportDir, "export-dir", "/export",
		"parent directory for the per-volume mount point (gateway)")
	flag.StringVar(&o.ganeshaConf, "ganesha-conf", "/etc/ganesha/ganesha.conf",
		"path the rendered NFS-Ganesha config is written to (gateway)")
	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return o, zapOpts
}

func main() {
	o, zapOpts := parseFlags()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	setupLog.Info("starting miroir", "mode", o.mode, "version", version, "commit", commit)
	if err := run(o); err != nil {
		setupLog.Error(err, "miroir exited")
		os.Exit(1)
	}
}

// agentHooks is what agent mode hands back to run: its readiness check and
// the sweeps around manager shutdown (nil without the DRBD kernel side).
type agentHooks struct {
	ready        healthz.Checker
	sweep        func()
	barrierSweep func()
	early        func()
}

func run(o options) error {
	if err := validateMode(o.mode); err != nil {
		return err
	}
	if err := validateDRBDPortBase(o.drbdPortBase); err != nil {
		return err
	}

	// Gateway mode drives the host directly and needs no controller-runtime
	// manager, so it builds none. It serves the same operational endpoint
	// itself (/healthz liveness + /metrics) on metricsAddr.
	if o.mode == modeGateway {
		return runGateway(o)
	}

	mgr, err := newManager(ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: o.metricsAddr},
		Cache:   cacheOptions(o.mode, o.podNamespace, o.nodeName),
		// The dedicated health-probe server is disabled; the probes are
		// co-hosted on the (plain HTTP) metrics listener so each workload
		// exposes a single operational port — the agent runs hostNetwork,
		// so every listener occupies a real node port.
		HealthProbeBindAddress: "0",
		// Leader election is the opt-in controller HA mode: extra
		// replicas stand by warm and only the reconcilers wait on the
		// Lease — the cache, metrics server, and CSI socket run on every
		// replica because each pod's CSI sidecars elect independently and
		// reach the driver over the pod-local socket. Gated on controller
		// mode: agents are per-node singletons, and a shared Lease would
		// serialize the whole DaemonSet down to one working node.
		LeaderElection:          o.mode == modeController && o.leaderElect,
		LeaderElectionID:        o.leaderElectionID,
		LeaderElectionNamespace: o.leaderElectionNS,
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
		return err
	}
	// healthz.CheckHandler returns 200 when the checker passes and 500
	// otherwise — the contract a kubelet HTTP probe expects.
	if err := mgr.AddMetricsServerExtraHandler("/healthz", healthz.CheckHandler{Checker: healthz.Ping}); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}

	identity := &csi.Identity{Version: version, WithController: o.mode == modeController}

	// Keep OS-signal cancellation separate from manager cancellation: the
	// topology watcher can restart the manager without looking like a node
	// shutdown.
	signalCtx := ctrl.SetupSignalHandler()
	managerCtx, stopManager := context.WithCancel(signalCtx)
	defer stopManager()

	// The hooks run around manager shutdown in agent mode only: release DRBD
	// backings when the node is going down and lift any leftover write
	// barrier — both are kernel state that outlives the process.
	hooks := agentHooks{ready: healthz.Ping}
	if o.mode == modeController {
		err = setupController(mgr, o, identity)
	} else {
		hooks, err = setupAgent(mgr, o, identity, stopManager)
	}
	if err != nil {
		return err
	}

	if err := mgr.AddMetricsServerExtraHandler("/readyz", healthz.CheckHandler{Checker: hooks.ready}); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("starting manager")
	var earlyCleanup *signalShutdown
	if hooks.sweep != nil {
		earlyCleanup = armSignalShutdown(signalCtx, hooks.early)
	}
	err = mgr.Start(managerCtx)
	err = finishManagerShutdown(err, signalCtx, managerCtx, hooks.sweep, hooks.barrierSweep, earlyCleanup)
	if err != nil {
		return fmt.Errorf("manager exited: %w", err)
	}
	return nil
}

func setupController(mgr ctrl.Manager, o options, identity *csi.Identity) error {
	// The topology is watch-driven: placement and membership fold the
	// MiroirNode CRs from the cache on every RPC/reconcile, so a
	// chart-applied topology edit takes effect without a restart.
	nodes := &nodemap.CRSource{Reader: mgr.GetClient()}
	controller := &csi.Controller{
		Client:           mgr.GetClient(),
		APIReader:        mgr.GetAPIReader(),
		Nodes:            nodes,
		ProvisionTimeout: o.provisionTimeout,
		OvercommitRatio:  o.overcommitRatio,
		FreeSpaceRatio:   o.freeSpaceRatio,
		AutoTieBreaker:   o.autoTieBreaker,
		RWXEnabled:       o.gatewayImage != "",
		DRBDPortBase:     int32(o.drbdPortBase),
		Recorder:         mgr.GetEventRecorder("miroir-controller"),
	}
	if err := (&agent.PhaseReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to set up volume phase reconciler: %w", err)
	}
	if err := setupMembership(mgr, nodes, o.autoTieBreaker, o.autoDiskfulAfter, o.autoEvictAfter); err != nil {
		return fmt.Errorf("unable to set up membership reconcilers: %w", err)
	}
	if err := setupTopology(mgr); err != nil {
		return fmt.Errorf("unable to set up topology reconcilers: %w", err)
	}
	gwResources, err := parseGatewayResources(o.gatewayResources)
	if err != nil {
		return fmt.Errorf("invalid gateway resources: %w", err)
	}
	if err := setupExport(mgr, o.podNamespace, o.gatewayImage, o.gatewaySA, gwResources); err != nil {
		return fmt.Errorf("unable to set up export reconciler: %w", err)
	}
	return serveCSI(mgr, o.csiSocket, identity, controller, nil)
}

func setupAgent(mgr ctrl.Manager, o options, identity *csi.Identity, stop context.CancelFunc) (agentHooks, error) {
	hooks := agentHooks{ready: healthz.Ping}
	if o.nodeName == "" {
		return hooks, errors.New("--node-name (or NODE_NAME) is required in agent mode")
	}
	// One breaker for the whole agent: the DRBD driver, the pool
	// backends and the node service all spawn into the same kernel
	// storage path, so the jam is counted once across all of them.
	storageWedge := backend.NewWedge(backend.DefaultWedgeLimit)
	runHost := (&backend.Runner{Wedge: storageWedge}).Run
	// The DaemonSet's chart-side scope is every schedulable node, but
	// only storage nodes run agent-backed backends. A node with no
	// MiroirNode holds no volumes and runs a client-only node service so
	// pods there can still mount RWX (NFS) volumes. A MiroirNode whose
	// pools flatten to nothing (none declared, or only block-less
	// entries from a pre-0.11 stored object that survives revalidation)
	// holds no storage either and gets the same treatment —
	// setupAgentPools would read zero pools as "every pool failed" and
	// crash-loop.
	miroirNode, found, err := agentTopology(mgr, o.nodeName, stop)
	if err != nil {
		return hooks, err
	}
	var flat nodemap.Node
	if found {
		flat = nodemap.FromSpec(miroirNode.Spec)
	}
	if msg := clientOnlyReason(found, miroirNode, flat); msg != "" {
		setupLog.Info(msg, "node", o.nodeName)
		// No volume reconciler runs here, so a client leg could never
		// be realized: the node service refuses RWO remote-access
		// staging outright (ClientOnly) — RWX/NFS staging needs no
		// DRBD leg and still works. No kernel-floor probe either: with
		// client legs refused nothing touches DRBD on this node, and
		// the probe's fatal below-floor exit would crash-loop a
		// consumer-only node over a check that protects nothing here.
		clientDRBD := &drbd.Driver{StateDir: o.drbdStateDir, Exec: runHost, Wedge: storageWedge}
		node := csi.NewNode(mgr.GetClient(), mgr.GetAPIReader(), o.nodeName, clientDRBD)
		node.ClientOnly = true
		node.Wedge = storageWedge
		return hooks, serveCSI(mgr, o.csiSocket, identity, nil, node)
	}
	pools, err := poolBackendsFor(o.nodeName, flat, runHost)
	if err != nil {
		return hooks, fmt.Errorf("unable to build the node's backends: %w", err)
	}
	if err := setupAgentPools(pools); err != nil {
		return hooks, err
	}
	drbdDriver := &drbd.Driver{StateDir: o.drbdStateDir, Exec: runHost, Wedge: storageWedge}
	// The binary is always in the image; what a local-only node lacks
	// is the kernel module. Probe once (the modprobe inside also loads
	// it proactively on nodes that ship it) and run without the DRBD
	// machinery when the kernel side is absent — otherwise the events
	// watcher hot-loops "exit status 20" every 5s forever.
	drbdKernel, drbdUtils, drbdReady, err := probeDRBD(drbdDriver)
	if err != nil {
		return hooks, err
	}
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
		// serving the node's healthy volumes.
		if err := sweepOrphans(context.Background(), mgr.GetAPIReader(), o.nodeName, drbdDriver); err != nil {
			return hooks, fmt.Errorf("orphan sweep failed: %w", err)
		}
		// Lift any IO barrier left by a previous agent crash; same
		// fatal-only-on-API-failure contract.
		if err := resumeStaleBarriers(context.Background(), mgr.GetAPIReader(), drbdDriver,
			agent.NewFreezer(), o.nodeName, apiStartupWait); err != nil {
			return hooks, fmt.Errorf("barrier resume sweep failed: %w", err)
		}
	}
	// Tracks this node's cordon state so shutdownSweep can tell a node
	// reboot/upgrade (drained, so cordoned) from a routine pod restart.
	// The sentinel file gates the DaemonSet preStop hook the same way.
	cordon := &agent.CordonWatcher{
		Client:       mgr.GetClient(),
		NodeName:     o.nodeName,
		SentinelPath: agent.CordonSentinelPath,
	}
	if err := cordon.SetupWithManager(mgr); err != nil {
		return hooks, fmt.Errorf("unable to set up cordon watcher: %w", err)
	}
	var drbdEvents chan event.GenericEvent
	if drbdReady {
		hooks.sweep = func() { agentShutdownSweep(mgr.GetAPIReader(), cordon, drbdDriver, o.nodeName) }
		hooks.barrierSweep = func() { agentShutdownBarrierSweep(mgr.GetAPIReader(), drbdDriver, o.nodeName) }
		hooks.early = func() { agentShutdownDownSecondaries(cordon, drbdDriver) }
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
		for _, r := range []struct {
			runnable manager.Runnable
			name     string
		}{
			{watcher, "DRBD event watcher"},
			{&agent.AssertionWatcher{Wedge: storageWedge}, "DRBD assertion watcher"},
			{&agent.WritebackWatcher{
				Client:   mgr.GetClient(),
				Reader:   mgr.GetAPIReader(),
				NodeName: o.nodeName,
				Recorder: mgr.GetEventRecorder("miroir-agent"),
			}, "writeback error watcher"},
		} {
			if err := mgr.Add(r.runnable); err != nil {
				return hooks, fmt.Errorf("unable to add %s: %w", r.name, err)
			}
		}
	}
	reconciler := &agent.VolumeReconciler{
		Client:     mgr.GetClient(),
		NodeName:   o.nodeName,
		Pools:      pools,
		DRBD:       drbdDriver,
		DRBDEvents: drbdEvents,
		Workers:    o.volumeWorkers,
		Recorder:   mgr.GetEventRecorder("miroir-agent"),
		Startup: &agent.StartupGate{
			Reader:   mgr.GetClient(),
			NodeName: o.nodeName,
			Deadline: time.Now().Add(agent.StartupSettleTimeout),
		},
	}
	hooks.ready = reconciler.Startup.Check
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return hooks, fmt.Errorf("unable to set up agent reconciler: %w", err)
	}
	freezer := agent.NewFreezer()
	if err := addBarrierSweep(mgr, drbdReady, drbdDriver, freezer, o.nodeName); err != nil {
		return hooks, err
	}
	snapReconciler := &agent.SnapshotReconciler{
		Client:   mgr.GetClient(),
		NodeName: o.nodeName,
		Pools:    pools,
		DRBD:     drbdDriver,
		Reader:   mgr.GetAPIReader(),
		Recorder: mgr.GetEventRecorder("miroir-agent"),
		Freezer:  freezer,
	}
	if err := snapReconciler.SetupWithManager(mgr); err != nil {
		return hooks, fmt.Errorf("unable to set up snapshot reconciler: %w", err)
	}
	groupReconciler := &agent.GroupSnapshotReconciler{
		Client:   mgr.GetClient(),
		NodeName: o.nodeName,
		Pools:    pools,
		DRBD:     drbdDriver,
		Reader:   mgr.GetAPIReader(),
		Recorder: mgr.GetEventRecorder("miroir-agent"),
		Freezer:  freezer,
	}
	if err := groupReconciler.SetupWithManager(mgr); err != nil {
		return hooks, fmt.Errorf("unable to set up group snapshot reconciler: %w", err)
	}
	// Publishes this node's pool capacities for capacity-aware placement.
	if err := mgr.Add(&agent.PoolStatsPublisher{
		Client:           mgr.GetClient(),
		NodeName:         o.nodeName,
		Pools:            pools,
		Interval:         o.poolStatsInterval,
		Recorder:         mgr.GetEventRecorder("miroir-agent"),
		DRBDVersion:      drbdKernel,
		DRBDUtilsVersion: drbdUtils,
	}); err != nil {
		return hooks, fmt.Errorf("unable to add pool stats publisher: %w", err)
	}
	// Scheduled online verify — the only cross-leg integrity check. Needs
	// the DRBD kernel side, so it is gated on drbdReady like the sweeps.
	if err := addVerifyScheduler(mgr, o.nodeName, drbdReady, o.verifySchedule, drbdDriver); err != nil {
		return hooks, err
	}
	if err := addWedgeSampler(mgr, storageWedge); err != nil {
		return hooks, err
	}
	node := csi.NewNode(mgr.GetClient(), mgr.GetAPIReader(), o.nodeName, drbdDriver)
	node.Wedge = storageWedge
	return hooks, serveCSI(mgr, o.csiSocket, identity, nil, node)
}

// finishManagerShutdown completes the shutdown coordination after the
// manager has stopped. Signal shutdown repeats the early teardown; an
// internal restart only recovers barriers, while an unexpected manager error
// keeps the defensive full sweep.
func finishManagerShutdown(err error, signalCtx, managerCtx context.Context,
	shutdownSweep, shutdownBarrierSweep func(), earlyCleanup *signalShutdown,
) error {
	if shutdownSweep == nil {
		return err
	}
	if signalCtx.Err() != nil {
		earlyCleanup.finish()
		shutdownSweep()
		return err
	}
	if err != nil && managerCtx.Err() == nil {
		earlyCleanup.finish()
		shutdownSweep()
		return err
	}
	shutdownBarrierSweep()
	if earlyCleanup.finish() {
		shutdownSweep()
	}
	return err
}

// apiStartupWait bounds how long the startup sweeps wait for the API server,
// so a reboot that races control-plane recovery does not exit on the first
// dial error and churn through CrashLoopBackOff. Kept under the liveness
// kill window: the probe endpoints are not up until the manager starts.
const apiStartupWait = 45 * time.Second

// apiDialTimeout replaces client-go's 30s default dial timeout. A dial to
// a not-yet-routable service VIP hangs for the whole timeout, which would
// leave a single effective retry inside apiStartupWait; an API server that
// takes longer than 10s to accept a connection is unreachable for every
// practical purpose.
const apiDialTimeout = 10 * time.Second

// drbdShutdownTimeout bounds the Secondary-teardown sweep at shutdown.
const drbdShutdownTimeout = 15 * time.Second

// barrierSweepInterval paces the periodic stranded-barrier sweep (addBarrierSweep).
const barrierSweepInterval = 5 * time.Minute

// apiShutdownWait bounds the shutdown barrier sweep's API access: the
// termination grace budget is 60s and the manager stop plus
// DownSecondaries can already spend 45s of it. apiStartupWait would
// guarantee a SIGKILL mid-sweep. The periodic mid-runtime sweep reuses it:
// a short per-tick budget whose failure just logs and retries next tick.
const apiShutdownWait = 5 * time.Second

// signalShutdown starts cleanup as soon as the OS signal arrives, rather
// than waiting for controller-runtime to finish stopping its runnables. Its
// finish method commits an internal restart only after the barrier sweep; its
// result reports whether signal cleanup won that race.
type signalShutdown struct {
	signalCtx context.Context
	cleanup   func()

	mu        sync.Mutex
	committed bool
	done      chan struct{}
	stop      chan struct{}
}

func armSignalShutdown(signalCtx context.Context, cleanup func()) *signalShutdown {
	s := &signalShutdown{
		signalCtx: signalCtx,
		cleanup:   cleanup,
		done:      make(chan struct{}),
		stop:      make(chan struct{}),
	}
	go func() {
		select {
		case <-signalCtx.Done():
			s.mu.Lock()
			committed := s.committed
			s.mu.Unlock()
			if !committed {
				s.cleanup()
			}
		case <-s.stop:
		}
		close(s.done)
	}()
	return s
}

// finish joins the helper. It linearizes an internal restart against signal
// cancellation: a signal observed before the commit wins and returns true.
func (s *signalShutdown) finish() bool {
	s.mu.Lock()
	if s.signalCtx.Err() != nil {
		s.mu.Unlock()
		<-s.done
		return true
	}
	s.committed = true
	close(s.stop)
	s.mu.Unlock()
	<-s.done
	return false
}

// apiWithRetry retries one API call until it succeeds, hits a terminal
// (non-transient) error, or the budget elapses — so a control plane still
// coming back up does not crash the process on startup. It is the one
// retry policy every pre-manager API access shares.
func apiWithRetry(ctx context.Context, budget time.Duration, op func(ctx context.Context) error) error {
	var lastErr error
	waitErr := wait.PollUntilContextTimeout(ctx, 2*time.Second, budget, true,
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

// newManager builds the controller-runtime manager. NewManager resolves every cached (ByObject) type through the
// RESTMapper at construction time, an API access that precedes every other
// apiWithRetry call site and would otherwise exit on the first dial error:
// a hostNetwork agent can only reach the service VIP once the CNI has
// programmed it, which lags the pod start on a node reboot. The mapper
// provider warms those lookups under the startup retry policy first, and
// the lazy mapper caches per API group, so NewManager's own lookups are
// then served locally and cannot race a second outage.
func newManager(opts ctrl.Options) (manager.Manager, error) {
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load the kube config: %w", err)
	}
	cfg.Dial = (&net.Dialer{Timeout: apiDialTimeout, KeepAlive: 30 * time.Second}).DialContext
	opts.MapperProvider = func(c *rest.Config, hc *http.Client) (meta.RESTMapper, error) {
		mapper, err := apiutil.NewDynamicRESTMapper(c, hc)
		if err != nil {
			return nil, err
		}
		if err := warmRESTMapper(mapper, opts.Scheme, opts.Cache.ByObject, apiStartupWait); err != nil {
			return nil, err
		}
		return mapper, nil
	}
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to create manager: %w", err)
	}
	return mgr, nil
}

// warmRESTMapper resolves the REST mapping of every cached type under the
// shared startup retry policy. Discovery needs no RBAC beyond
// system:authenticated, so the probe is the same in every mode.
func warmRESTMapper(mapper meta.RESTMapper, scheme *runtime.Scheme,
	objs map[client.Object]cache.ByObject, budget time.Duration) error {
	gvks := make([]schema.GroupVersionKind, 0, len(objs))
	for obj := range objs {
		gvk, err := apiutil.GVKForObject(obj, scheme)
		if err != nil {
			return err
		}
		gvks = append(gvks, gvk)
	}
	return apiWithRetry(context.Background(), budget, func(context.Context) error {
		for _, gvk := range gvks {
			if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
				return fmt.Errorf("warm RESTMapper: %w", err)
			}
		}
		return nil
	})
}

// listWithRetry retries an API list with the shared startup retry policy.
func listWithRetry(ctx context.Context, c client.Reader, list client.ObjectList, budget time.Duration) error {
	return apiWithRetry(ctx, budget, func(ctx context.Context) error { return c.List(ctx, list) })
}

// transientAPIError reports whether an API error is worth retrying. Dial
// failures during control-plane recovery (connection refused, no route to
// host) arrive as non-APIStatus errors; only explicit terminal statuses
// (auth, not-found, invalid) and a kind the server does not serve (the
// CRDs are not installed) are treated as permanent.
func transientAPIError(err error) bool {
	switch {
	case err == nil:
		return false
	case apierrors.IsUnauthorized(err), apierrors.IsForbidden(err),
		apierrors.IsNotFound(err), apierrors.IsInvalid(err),
		meta.IsNoMatchError(err):
		return false
	default:
		return true
	}
}

// agentShutdownDownSecondaries demotes all Primaries then brings down every
// Secondary on a cordoned node, releasing backing devices so the OS can tear
// down storage pools without EIO. The early signal path deliberately does not
// perform API access, so it can start before manager shutdown has completed.
func agentShutdownDownSecondaries(cordon *agent.CordonWatcher, driver *drbd.Driver) {
	if cordon.Cordoned() {
		ctx, cancel := context.WithTimeout(context.Background(), drbdShutdownTimeout)
		defer cancel()
		setupLog.Info("node cordoned; demoting Primaries and releasing DRBD backings for shutdown")
		// Demote first: --force terminates pending I/O so the subsequent
		// down and the OS teardown cannot wedge on a metadata flush.
		if err := driver.DemoteAll(ctx); err != nil {
			setupLog.Error(err, "DRBD shutdown demote failed; attempting down sweep anyway")
		}
		if err := driver.DownSecondaries(ctx); err != nil {
			setupLog.Error(err, "DRBD shutdown teardown failed; node reboot may stall")
		}
	}
}

func agentShutdownBarrierSweep(c client.Reader, driver *drbd.Driver, nodeName string) {
	// Short API budget: the chart grants 60s of termination grace and the
	// manager stop + DownSecondaries already spend up to 45s of it. A
	// stranded barrier missed here is lifted by the startup sweep on the
	// next boot.
	if err := resumeStaleBarriers(context.Background(), c, driver,
		agent.NewFreezer(), nodeName, apiShutdownWait); err != nil {
		setupLog.Error(err, "shutdown barrier sweep failed")
	}
}

// agentShutdownSweep is the final idempotent shutdown pass.
func agentShutdownSweep(c client.Reader, cordon *agent.CordonWatcher, driver *drbd.Driver, nodeName string) {
	agentShutdownDownSecondaries(cordon, driver)
	agentShutdownBarrierSweep(c, driver, nodeName)
}

// addBarrierSweep runs the startup/shutdown stranded-barrier sweep
// mid-runtime too: a wedged suspend-io (LINBIT/drbd#137) can set the kernel
// flag after drbdadm reported failure, leaving a frozen volume no recorded
// round owns until the agent restarts. resumeStaleBarriers skips live rounds
// (fresh ioSuspended timestamp); a round past agent.SuspendDeadline is void
// by protocol anyway. A round is briefly invisible between its suspend-io
// and its status patch, so a tick can in theory lift a live coordinator's
// barrier — bounded: the cut phase re-asserts suspend-io per leg (though
// not the thawed freeze, so that round's cut degrades from fs-consistent
// to crash-consistent). API-list failures are logged, not fatal: the next
// tick retries.
func addBarrierSweep(mgr manager.Manager, drbdReady bool, driver *drbd.Driver, freezer *agent.Freezer,
	nodeName string) error {
	if !drbdReady {
		return nil
	}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(barrierSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := resumeStaleBarriers(ctx, mgr.GetAPIReader(), driver, freezer, nodeName, apiShutdownWait); err != nil {
					setupLog.Error(err, "periodic barrier sweep incomplete")
				}
			}
		}
	})); err != nil {
		return fmt.Errorf("unable to add periodic barrier sweep: %w", err)
	}
	return nil
}

// wedgeSampleInterval paces the node-scoped breaker's gauge: short enough to
// show the jam building rather than only its aftermath, and cheap at one
// /proc/<pid>/stat read per outstanding child.
const wedgeSampleInterval = 30 * time.Second

// addWedgeSampler publishes the node-scoped breaker's state on a timer and
// logs its transitions. Sampled rather than event-driven because the count
// self-heals: a gauge written only when a child strands would page forever
// after the node recovered.
func addWedgeSampler(mgr manager.Manager, w *backend.Wedge) error {
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(wedgeSampleInterval)
		defer ticker.Stop()
		open := false
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				agent.RecordNodeWedge(w)
				// Edges only: a jammed node stays jammed until it reboots,
				// and repeating the line every 30s buries it.
				if tripped := w.Tripped(); tripped != open {
					open = tripped
					if tripped {
						setupLog.Error(w.Err(), "node storage stack wedged; refusing further storage commands until this node reboots",
							"stranded", w.Stranded())
					} else {
						setupLog.Info("node storage stack recovered; resuming storage commands")
					}
				}
			}
		}
	})); err != nil {
		return fmt.Errorf("unable to add the wedge sampler: %w", err)
	}
	return nil
}

// sweepOrphans removes DRBD state with no owning volume on this node. c
// must be an uncached reader: the manager has not started yet.
// Returns an error only when the volume list cannot be fetched; the sweep
// itself is best-effort and its failures are logged here.
func sweepOrphans(ctx context.Context, c client.Reader, nodeName string, driver *drbd.Driver) error {
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := listWithRetry(ctx, c, vols, apiStartupWait); err != nil {
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
	if err := driver.SweepOrphans(ctx,
		func(name string) bool { return owned[name] }); err != nil {
		setupLog.Error(err, "orphan sweep incomplete")
	}
	return nil
}

// resumeStaleBarriers lifts suspend-io left behind by a previous crash.
// The kernel's view drives the sweep: a crash between suspend-io and the
// status patch leaves a frozen device no snapshot records. It is read
// before the rounds are listed, so a barrier is never judged against a
// listing older than itself. Barriers whose round is still within the
// deadline are the reconciler's to drive. c must be an uncached reader.
// Returns an error only when a round list cannot be fetched; resume
// failures are per-resource and logged here.
func resumeStaleBarriers(ctx context.Context, c client.Reader, driver *drbd.Driver, freezer *agent.Freezer,
	nodeName string, apiBudget time.Duration) error {
	suspended, err := driver.UserSuspended(ctx)
	if err != nil {
		// No kernel view (e.g. module not loaded yet) also means nothing
		// can be suspended — don't block agent startup on it.
		setupLog.Error(err, "cannot list suspended resources; skipping barrier sweep")
		return nil
	}
	if len(suspended) == 0 {
		return nil
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := listWithRetry(ctx, c, snaps, apiBudget); err != nil {
		return err
	}
	fresh := map[string]bool{}
	for _, s := range snaps.Items {
		if s.Status.IOSuspended && s.Status.SuspendedAt != nil &&
			time.Since(s.Status.SuspendedAt.Time) < agent.SuspendDeadline {
			fresh[s.Spec.VolumeName] = true
		}
	}
	// A group round's ioSuspended lives on the group object, not on its
	// member snapshots — without this, every volume of a live group round
	// reads as stale and the sweep resumes (and thaws) mid-cut. Member
	// volumes come from the member snapshots; perLeg slot keys
	// ("<volume>/<node>") cover a member already torn out of the list.
	groups := &miroirv1alpha1.MiroirSnapshotGroupList{}
	if err := listWithRetry(ctx, c, groups, apiBudget); err != nil {
		return err
	}
	snapVol := map[string]string{}
	for _, s := range snaps.Items {
		snapVol[s.Name] = s.Spec.VolumeName
	}
	for _, g := range groups.Items {
		if !g.Status.IOSuspended || g.Status.SuspendedAt == nil ||
			time.Since(g.Status.SuspendedAt.Time) >= agent.SuspendDeadline {
			continue
		}
		for _, name := range g.Spec.SnapshotNames {
			if vol := snapVol[name]; vol != "" {
				fresh[vol] = true
			}
		}
		for key := range g.Status.PerLeg {
			if vol, _, ok := strings.Cut(key, "/"); ok {
				fresh[vol] = true
			}
		}
	}
	var errs []error
	var stale []string
	for _, vol := range suspended {
		if fresh[vol] {
			continue
		}
		stale = append(stale, vol)
		setupLog.Info("lifting stale IO barrier", "volume", vol)
		if err := driver.ResumeIO(ctx, vol); err != nil {
			errs = append(errs, fmt.Errorf("resume stale barrier on %s: %w", vol, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		setupLog.Error(err, "barrier resume sweep incomplete")
	}
	// A crash mid-round can leave the filesystem freeze behind exactly
	// like the barrier; the reconcilers only thaw volumes whose snapshot
	// objects still exist, so the sweep covers the rest. Fresh rounds
	// keep their freeze — their own close lifts it.
	if len(stale) > 0 && freezer != nil {
		vols := &miroirv1alpha1.MiroirVolumeList{}
		if err := listWithRetry(ctx, c, vols, apiBudget); err != nil {
			setupLog.Error(err, "cannot list volumes; skipping freeze thaw sweep")
			return nil
		}
		paths := map[string]string{}
		for i := range vols.Items {
			paths[vols.Items[i].Name] = vols.Items[i].Status.PerNode[nodeName].DevicePath
		}
		for _, name := range stale {
			if device := paths[name]; device != "" {
				mp, err := freezer.Thaw(device)
				switch {
				case err != nil:
					setupLog.Error(err, "cannot thaw stale filesystem freeze", "volume", name)
				case mp == "":
					// Usually a leg that never froze (only mounted legs do),
					// but a freeze leaked before its mount went away is
					// unreachable now — mount refuses the frozen device and
					// FITHAW needs a mountpoint. Not silent, so
					// the leak never looks like success; the stage-time
					// recovery is what clears it.
					setupLog.Info("stale barrier volume is not mounted; nothing to thaw here",
						"volume", name, "device", device)
				}
			}
		}
	}
	return nil
}

// probeDRBD reports whether the DRBD kernel side is usable and, when it
// is, the module and drbd-utils versions — an error below drbd.KernelFloor:
// a 9.3.1-era option rendered against an older module errors drbdadm for
// every resource on the node, so failing fast here beats poisoning them
// all later. Talos ≥ 1.13.0 ships a module at the floor.
func probeDRBD(driver *drbd.Driver) (version, utilsVersion string, ready bool, err error) {
	if !driver.KernelAvailable(context.Background()) {
		return "", "", false, nil
	}
	v, err := driver.KernelVersion(context.Background())
	if err != nil {
		// The module answered but the version read flaked; running
		// unchecked beats refusing a working node.
		setupLog.Error(err, "cannot read DRBD kernel module version; skipping floor check")
		return "", "", true, nil
	}
	if drbd.BelowKernelFloor(v) {
		return "", "", false, fmt.Errorf(
			"DRBD kernel module %s is below the supported floor %s; upgrade the node (Talos >= 1.13.0)",
			v, drbd.KernelFloor)
	}
	utils, err := driver.UtilsVersion(context.Background())
	if err != nil {
		// Informational only: an empty value beats refusing a working node.
		setupLog.Error(err, "cannot read drbd-utils version")
	}
	agent.RecordDRBDVersions(v, utils)
	return v, utils, true, nil
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
func serveCSI(mgr ctrl.Manager, socket string, identity *csi.Identity, controller *csi.Controller, node *csi.Node,
) error {
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
		return fmt.Errorf("unable to add CSI server to manager: %w", err)
	}
	return nil
}
