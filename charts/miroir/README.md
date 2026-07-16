# miroir

![Version: 0.0.1](https://img.shields.io/badge/Version-0.0.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.1](https://img.shields.io/badge/AppVersion-0.0.1-informational?style=flat-square)

Replicated block storage for small Kubernetes clusters — CSI on LVM thin, ZFS, or loopfile backends with synchronous DRBD9 replication

**Homepage:** <https://github.com/home-operations/miroir>

## Usage

Replicated block storage for small Kubernetes clusters. Control plane in
Go; data path delegated to in-kernel primitives (dm-thin, ZFS, or loop
devices, with synchronous DRBD9 replication). Full documentation at
<https://miroir.home-operations.com/>.

```sh
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  --namespace miroir-system --create-namespace -f values.yaml
```

## ZFS pool settings

ZFS pools accept `zfsVolBlockSize` (`4K` through `128K`, default `4K`)
and `zfsCompression` (default `lz4`; use `inherit` for the parent dataset
policy):

```yaml
nodes:
  node-a:
    pools:
      default:
        backend: zfs
        zfsDataset: tank/miroir
        zfsVolBlockSize: 16K
        zfsCompression: inherit
```

Both settings apply only to newly created zvols. Existing volumes are not
mutated, and snapshot clones retain their source properties.

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| home-operations | <contact@home-operations.com> |  |

## Source Code

* <https://github.com/home-operations/miroir>

## Requirements

Kubernetes: `>=1.31.0`

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| agent.extraArgs | list | `[]` | Extra arguments for the agent container. |
| agent.extraEnv | list | `[]` | Extra environment variables for the agent container. |
| agent.image.digest | string | `""` |  |
| agent.image.pullPolicy | string | `"IfNotPresent"` |  |
| agent.image.repository | string | `"ghcr.io/home-operations/miroir-agent"` |  |
| agent.image.tag | string | `""` |  |
| agent.kubeletDir | string | `"/var/lib/kubelet"` | Kubelet root on the nodes; CSI sockets and mounts hang off it. |
| agent.podAnnotations | object | `{}` | Extra annotations on the agent pods. |
| agent.podLabels | object | `{}` | Extra labels on the agent pods. |
| agent.poolStatsInterval | string | `"60s"` |  |
| agent.registrar.image | string | `"registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.17.0"` |  |
| agent.resources.limits.memory | string | `"128Mi"` |  |
| agent.resources.requests.cpu | string | `"10m"` |  |
| agent.resources.requests.memory | string | `"32Mi"` |  |
| agent.volumeWorkers | int | `4` | Concurrent volume reconciles per agent. Per-volume work is serialized by controller-runtime regardless; this bounds how many distinct volumes one agent works at once. |
| autoDiskfulAfter | string | `""` | Convert a diskless leg (client or tie-breaker) that has stayed DRBD Primary past this duration into a local diskful replica on its node, so a settled consumer stops paying network I/O (LINSTOR's auto-diskful; Go duration, e.g. "10m"). Conversion needs the leg's node in `nodes` with fresh pool stats and room for the volume's full size. Empty disables it. See the root README, "Auto-diskful". |
| autoEvictAfter | string | `""` | Re-place a dead storage node's legs once its heartbeat (MiroirNode status, refreshed ~60s) has been stale this long (LINSTOR's auto-evict; Go duration, e.g. "60m" — keep it well above any reboot or upgrade window). Each affected volume gets one atomic swap: the dead entry out, a fresh replica in (full sync follows). The dead node keeps its teardown finalizer as the record of its never-cleaned leg: deleting an evicted volume still waits for that node, and when the node returns its agent tears the leftover leg down through the normal removal flow. It never acts when more than one node looks dead, when a survivor still sees the node's DRBD links up, when the remaining legs are not clean, or when snapshots pin the volume. Needs a spare storage node carrying the volume's pool; per-node opt-out via `nodes.<name>.autoEvict: false`. Empty disables it (the default: eviction discards the dead node's data). |
| autoTieBreaker | bool | `true` | Add a diskless tie-breaker replica to 2-replica freeze volumes when a spare storage node exists, so majority quorum survives a single node loss. Also retrofits existing freeze volumes at controller startup. |
| drbd.alExtents | string | `""` | al-extents, the DRBD activity-log size (number of 4 MiB extents kept "hot"). DRBD's default (1237) forces frequent metadata updates under a scattered random-write workload; raising it (e.g. 6007) cuts that write amplification at the cost of a longer resync of the active region after a crash. Empty leaves DRBD's default. Must be a prime below 65534. |
| drbd.net.maxBuffers | string | `""` | max-buffers, the DRBD receive-buffer count (e.g. "36864"); raises resync throughput on fast links. |
| drbd.onIoError | string | `"detach"` |  |
| drbd.portBase | int | `7000` | Lowest TCP port for DRBD replication links, one per replicated volume ascending (7000, 7001, …). The agent runs hostNetwork so these bind on the node's kernel. Ceph mgr dashboard's non-SSL default is also 7000; co-locating with Rook host-network Ceph requires moving one of them (see issue #148). Existing volumes keep their assigned ports. |
| drbd.resync.discardGranularity | string | `""` | rs-discard-granularity cluster-wide fallback: during a full resync, runs of zeroes are sent as discards of this size instead of written out (e.g. "65536"), keeping a re-added thin leg thin. Normally leave empty — the agent probes each lvmthin/zfs backing device and renders an exact per-leg value that overrides this (loopfile is never probed: loop devices mishandle it, so also leave this empty on clusters with loopfile-backed replicated volumes). |
| drbd.resync.fillTarget | string | `""` | c-fill-target, the resync controller's target fill level (e.g. "1M"). |
| drbd.resync.maxRate | string | `""` | c-max-rate, the resync bandwidth ceiling used when the link is idle (e.g. "720M"). |
| drbd.resync.minRate | string | `"10M"` | c-min-rate, the resync floor guaranteed even under application I/O. Defaulted to 10M: DRBD's kernel default (250 KiB/s) leaves a degraded volume resyncing for days under load; 10 MiB/s heals a 100Gi leg in hours while still yielding most of a 1GbE link to applications. Lower on a slow shared link. |
| drbd.resync.planAhead | string | `""` | c-plan-ahead in 0.1s units; a value > 0 enables DRBD's variable-rate resync controller. |
| drbd.resync.rate | string | `""` | resync-rate, the fixed rate used only when the controller is off (planAhead empty or 0). |
| drbd.verify.algorithm | string | `"crc32c"` | verify-alg, arming `drbdadm verify <res>` — the only cross-leg integrity check (a zfs scrub only validates one leg against itself). Defaulted to crc32c: drbd.ko depends on libcrc32c so it is present on every node, and it costs nothing until a verify runs. Empty disables verification, including the schedule below. |
| drbd.verify.schedule | string | `""` | Cron spec (5-field, agent-local/UTC time) for a scheduled online verify of every replicated volume. The agent initiates it once per volume from the coordinator (first diskful replica), serialized per node, skipping volumes that are resyncing or already verifying. Findings land in the volume's status (`lastVerifyOutOfSyncBytes`), the `miroir_volume_verify_*` metrics, and a `VerifyOutOfSync` event. Empty = no scheduled verify (run it by hand). Requires `algorithm` set. |
| drbd.verify.suspend | bool | `false` | Pause scheduled verify without dropping the schedule above. |
| extraArgs | list | `[]` | Extra arguments for the controller container. |
| extraEnv | list | `[]` | Extra environment variables for the controller container. |
| freeSpaceRatio | int | `20` | Physical-space guardrail: CreateVolume is refused when the request would exceed the pool's *free* bytes × this ratio. overcommitRatio alone bounds virtual bytes, so a pool whose thin volumes have actually filled it can still admit more; running a pool out of space surfaces as I/O errors under live volumes rather than a clean refusal. 20× matches LINSTOR and BlockStor and only bites once a pool is ~90% full; lower it toward 1 to keep more physical headroom in reserve. |
| fullnameOverride | string | `""` | Override the fully qualified name prefix of every rendered object. |
| gateway.enabled | bool | `false` | Serve ReadWriteMany (and ReadOnlyMany) PVCs via per-volume NFS gateways. Opt-in: gateway pods run privileged in the release namespace, and any user who can create a PVC can cause one to be spawned, so enabling RWX is an explicit operator decision. While disabled the controller rejects RWX at provision time with a clear message, and the gateway ServiceAccount, RBAC, PodMonitor, and export alerts are not installed. |
| gateway.image.digest | string | `""` |  |
| gateway.image.pullPolicy | string | `"IfNotPresent"` |  |
| gateway.image.repository | string | `"ghcr.io/home-operations/miroir-gateway"` |  |
| gateway.image.tag | string | `""` |  |
| global.affinity | object | `{}` |  |
| global.commonLabels | object | `{}` | Labels stamped on every rendered object (fleet-wide labelling). |
| global.imagePullSecrets | list | `[]` | Pull secrets added to every pod (controller, agent, setup, uninstall). |
| global.nodeSelector | object | `{}` | Controller scheduling defaults. |
| global.tolerations | list | `[]` |  |
| image | object | `{"digest":"","pullPolicy":"IfNotPresent","repository":"ghcr.io/home-operations/miroir-controller","tag":""}` | Controller image (distroless, no storage userland — the controller never execs a storage CLI). |
| leaderElection.enabled | bool | `false` | Elect even with a single replica (replicaCount > 1 elects regardless; this can never switch election off above one replica). |
| leaderElection.id | string | `""` | Lease name; empty derives the release-scoped controller name so two releases in one namespace never share a Lease. Keep it stable across upgrades. |
| logging.format | string | `"json"` | Encoder: json (structured, default) or console (human-readable). |
| logging.level | string | `"info"` | Log level: debug | info | error (or any zapcore level). |
| monitoring.dashboards.annotations | object | `{}` | Annotations added to the dashboard ConfigMap. |
| monitoring.dashboards.enabled | bool | `false` | Render the Grafana dashboard ConfigMap (for grafana-operator or the kube-prometheus-stack sidecar). |
| monitoring.dashboards.grafanaOperator.allowCrossNamespaceImport | bool | `true` | If true allows for a Grafana in any namespace to access this GrafanaDashboard. |
| monitoring.dashboards.grafanaOperator.enabled | bool | `false` | Render a GrafanaDashboard CR (grafana-operator) instead of a sidecar ConfigMap. |
| monitoring.dashboards.grafanaOperator.folder | string | `""` | Folder to create the dashboard in. |
| monitoring.dashboards.grafanaOperator.matchLabels | object | `{}` | Selected labels for Grafana instance. |
| monitoring.dashboards.grafanaOperator.resyncPeriod | string | `"10m"` | Resync period for the Grafana operator to check for updates to the dashboard. |
| monitoring.dashboards.labels | object | `{}` | Labels added to the dashboard ConfigMap. |
| monitoring.dashboards.namespace | string | `""` | Namespace for the dashboard objects; defaults to the release namespace. |
| monitoring.podMonitor.annotations | object | `{}` | PodMonitor annotations. |
| monitoring.podMonitor.enabled | bool | `false` | Create a Prometheus Operator PodMonitor (requires its CRDs) scraping the controller and every agent pod on their metrics ports. The per-volume miroir_volume_* gauges are exported by the agents. |
| monitoring.podMonitor.interval | string | `"30s"` | Scrape interval. |
| monitoring.podMonitor.labels | object | `{}` | PodMonitor labels. |
| monitoring.podMonitor.metricRelabelings | list | `[]` | Prometheus metric relabelings. |
| monitoring.podMonitor.path | string | `"/metrics"` | Metrics path. |
| monitoring.podMonitor.podTargetLabels | list | `[]` | Pod target labels to copy from pods. |
| monitoring.podMonitor.relabelings | list | `[]` | Extra Prometheus relabelings (applied before scraping); a node label from the pod's node name is always added. |
| monitoring.podMonitor.scrapeTimeout | string | `"10s"` | Scrape timeout. |
| monitoring.prometheusRule.additionalRuleAnnotations | object | `{}` | Extra annotations added to every alert rule. |
| monitoring.prometheusRule.additionalRuleLabels | object | `{}` | Extra labels added to every alert rule. |
| monitoring.prometheusRule.annotations | object | `{}` | PrometheusRule annotations. |
| monitoring.prometheusRule.enabled | bool | `false` | Create a PrometheusRule with alerting rules (requires the Prometheus Operator CRDs). |
| monitoring.prometheusRule.labels | object | `{}` | PrometheusRule labels. |
| monitoring.prometheusRule.verifyStaleDays | int | `8` | Days since the last completed scheduled verify before MiroirVolumeVerifyStale fires. Size it to just over the schedule period (a weekly `drbd.verify.schedule` → 8). The rule is only rendered when `drbd.verify.schedule` is set. |
| nameOverride | string | `""` | Override the chart name used in labels and default object names. |
| nodes | object | `{}` |  |
| overcommitRatio | int | `2` | Thin-provisioning overcommit guardrail: CreateVolume is refused when a node's provisioned total would exceed capacity × this ratio. 2× is the classic CoW headroom; raise it only if you trust your usage to stay sparse, lower it toward 1 to provision conservatively. |
| podAnnotations | object | `{}` | Extra annotations on the controller pod. |
| podLabels | object | `{}` | Extra labels on the controller pod. |
| priorityClassName | string | `"system-cluster-critical"` | system-cluster-critical protects the single controller from eviction under node pressure — while it is down, no volume can be provisioned, expanded, or snapshotted. |
| provisionTimeout | string | `"120s"` | Wait for agents to realise a new volume. Keep sidecars.*.timeout at or above this, or the sidecar RPC deadline fires before this one and the knob has no effect. |
| replicaCount | int | `1` | Controller replicas. Anything above 1 automatically enables leader election: the extras are warm standbys (failover is lease expiry, ~15s, instead of a full pod reschedule), the rollout strategy switches to RollingUpdate, and a PodDisruptionBudget keeps one replica through voluntary disruptions. Pointless on a single-node cluster (the node is the failure domain); pair with global.affinity (pod anti-affinity) so replicas land on different nodes. |
| resources | object | `{"limits":{"memory":"128Mi"},"requests":{"cpu":"10m","memory":"32Mi"}}` | Controller resources. |
| sidecars.healthMonitor.enabled | bool | `false` |  |
| sidecars.healthMonitor.image | string | `"registry.k8s.io/sig-storage/csi-external-health-monitor-controller:v0.18.0"` |  |
| sidecars.healthMonitor.interval | string | `"1m"` |  |
| sidecars.provisioner.image | string | `"registry.k8s.io/sig-storage/csi-provisioner:v6.3.0"` |  |
| sidecars.provisioner.timeout | string | `"120s"` |  |
| sidecars.resizer.image | string | `"registry.k8s.io/sig-storage/csi-resizer:v2.2.1"` |  |
| sidecars.resizer.timeout | string | `"120s"` |  |
| sidecars.snapshotter.image | string | `"registry.k8s.io/sig-storage/csi-snapshotter:v8.6.0"` |  |
| sidecars.snapshotter.timeout | string | `"120s"` |  |
| storageCapacity.enabled | bool | `false` |  |
| storageClasses | list | `[]` | StorageClasses to create. Empty by default: declare the classes you want. One local + one replicated is the common pair (see the example below). Per entry:   name          (required) the StorageClass name   replicas      replica count, default 1; >1 makes it DRBD-replicated   quorum        freeze | last-man-standing (replicated only, default                 freeze). freeze never diverges but halts writes without a                 peer majority; last-man-standing keeps the survivor                 writable at the risk of split-brain. See the root README,                 "Replication and quorum".   fsType        ext4 | xfs, default ext4   pool          named storage pool the class provisions from, default                 "default". Every replica of a volume lands in this pool                 on its node, so the pool must exist (under nodes.<name>                 .pools) on at least `replicas` nodes.   allowRemoteVolumeAccess                 true | false (replicated only; the controller defaults                 absent to true, matching LINSTOR): pods on nodes without                 a replica consume the volume through an ephemeral                 diskless DRBD leg at replication-network speed. Set                 false to pin pods to replica nodes for local reads. See                 the root README, "Remote consumers".   bitmapGranularity                 DRBD bitmap block size in bytes (replicated only): a                 power of two, 4096–1048576, default absent (DRBD's 4096).                 Each dirty bit tracks this many bytes — coarser cuts                 bitmap RAM proportionally (65536 ≈ 1/16th) but resyncs                 more per dirty bit; worth considering for classes holding                 large volumes. Fixed when a replica's metadata is                 created: changing the class affects new volumes only.   reclaimPolicy Delete | Retain, default Delete   isDefault     set the cluster default-class annotation, default false Example (coexisting with OpenEBS, which stays the cluster default):   storageClasses:     - name: miroir-local       replicas: 1     - name: miroir-replicated       replicas: 2       quorum: freeze |
| uninstall.image | string | `"registry.k8s.io/kubectl:v1.36.2"` |  |
| volumeSnapshotClasses | list | `[]` | VolumeSnapshotClasses to create (requires the snapshot-controller + CRDs, deployed separately). Empty by default. Per entry:   name           (required) the VolumeSnapshotClass name   deletionPolicy Delete | Retain, default Delete   isDefault      set the cluster default-snapshot-class annotation,                  default false Example:   volumeSnapshotClasses:     - name: miroir-snap       deletionPolicy: Delete |

---

_This README is generated by [helm-docs](https://github.com/norwoodj/helm-docs) from `Chart.yaml` and `values.yaml`. Edit those (or `README.md.gotmpl`) and run `mise run helm-docs`._
