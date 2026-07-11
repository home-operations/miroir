# miroir

![Version: 0.0.1](https://img.shields.io/badge/Version-0.0.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.1](https://img.shields.io/badge/AppVersion-0.0.1-informational?style=flat-square)

Low-resource replicated block storage CSI driver for small Talos clusters

**Homepage:** <https://github.com/home-operations/miroir>

## Usage

Low-resource replicated block storage CSI driver for small Talos Linux
clusters. Control plane in Go; data path delegated to in-kernel primitives
(dm-thin / ZFS today, DRBD9 replication planned).

```sh
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  --namespace miroir --create-namespace
```

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
| autoTieBreaker | bool | `true` | Add a diskless tie-breaker replica to 2-replica freeze volumes when a spare storage node exists, so majority quorum survives a single node loss. Also retrofits existing freeze volumes at controller startup. |
| drbd.net.maxBuffers | string | `""` | max-buffers, the DRBD receive-buffer count (e.g. "36864"); raises resync throughput on fast links. |
| drbd.onIoError | string | `"detach"` |  |
| drbd.resync.discardGranularity | string | `""` | rs-discard-granularity cluster-wide fallback: during a full resync, runs of zeroes are sent as discards of this size instead of written out (e.g. "65536"), keeping a re-added thin leg thin. Normally leave empty — the agent probes each lvmthin/zfs backing device and renders an exact per-leg value that overrides this (loopfile is never probed: loop devices mishandle it, so also leave this empty on clusters with loopfile-backed replicated volumes). |
| drbd.resync.fillTarget | string | `""` | c-fill-target, the resync controller's target fill level (e.g. "1M"). |
| drbd.resync.maxRate | string | `""` | c-max-rate, the resync bandwidth ceiling used when the link is idle (e.g. "720M"). |
| drbd.resync.minRate | string | `"10M"` | c-min-rate, the resync floor guaranteed even under application I/O. Defaulted to 10M: DRBD's kernel default (250 KiB/s) leaves a degraded volume resyncing for days under load; 10 MiB/s heals a 100Gi leg in hours while still yielding most of a 1GbE link to applications. Lower on a slow shared link. |
| drbd.resync.planAhead | string | `""` | c-plan-ahead in 0.1s units; a value > 0 enables DRBD's variable-rate resync controller. |
| drbd.resync.rate | string | `""` | resync-rate, the fixed rate used only when the controller is off (planAhead empty or 0). |
| drbd.verifyAlg | string | `"crc32c"` | verify-alg arms `drbdadm verify <res>` — the only cross-leg integrity check (a zfs scrub only validates one leg against itself). Defaulted to crc32c: drbd.ko depends on libcrc32c so it is present on every node, and it costs nothing until a verify runs. Schedule the verify pass yourself (cron, quiet hours); out-of-sync blocks surface in the kernel log and `drbdsetup status`. Empty disables verification. |
| extraArgs | list | `[]` | Extra arguments for the controller container. |
| extraEnv | list | `[]` | Extra environment variables for the controller container. |
| fullnameOverride | string | `""` | Override the fully qualified name prefix of every rendered object. |
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
| nameOverride | string | `""` | Override the chart name used in labels and default object names. |
| nodes | object | `{}` |  |
| overcommitRatio | int | `2` | Thin-provisioning overcommit guardrail: CreateVolume is refused when a node's provisioned total would exceed capacity × this ratio. 2× is the classic CoW headroom; raise it only if you trust your usage to stay sparse, lower it toward 1 to provision conservatively. |
| podAnnotations | object | `{}` | Extra annotations on the controller pod. |
| podLabels | object | `{}` | Extra labels on the controller pod. |
| priorityClassName | string | `"system-cluster-critical"` | system-cluster-critical protects the single controller from eviction under node pressure — while it is down, no volume can be provisioned, expanded, or snapshotted. |
| provisionTimeout | string | `"120s"` | Wait for agents to realise a new volume. Keep sidecars.*.timeout at or above this, or the sidecar RPC deadline fires before this one and the knob has no effect. |
| replicaCount | int | `1` | Controller replicas. Anything above 1 automatically enables leader election: the extras are warm standbys (failover is lease expiry, ~15s, instead of a full pod reschedule), the rollout strategy switches to RollingUpdate, and a PodDisruptionBudget keeps one replica through voluntary disruptions. Pointless on a single-node cluster (the node is the failure domain); pair with global.affinity (pod anti-affinity) so replicas land on different nodes. |
| replicatedStorageClass.create | bool | `true` |  |
| replicatedStorageClass.fsType | string | `"ext4"` |  |
| replicatedStorageClass.isDefault | bool | `false` |  |
| replicatedStorageClass.name | string | `"miroir-replicated"` |  |
| replicatedStorageClass.quorum | string | `"freeze"` |  |
| replicatedStorageClass.reclaimPolicy | string | `"Delete"` |  |
| resources | object | `{"limits":{"memory":"128Mi"},"requests":{"cpu":"10m","memory":"32Mi"}}` | Controller resources. |
| sidecars.provisioner.image | string | `"registry.k8s.io/sig-storage/csi-provisioner:v6.3.0"` |  |
| sidecars.provisioner.timeout | string | `"120s"` |  |
| sidecars.resizer.image | string | `"registry.k8s.io/sig-storage/csi-resizer:v2.2.1"` |  |
| sidecars.resizer.timeout | string | `"120s"` |  |
| sidecars.snapshotter.image | string | `"registry.k8s.io/sig-storage/csi-snapshotter:v8.6.0"` |  |
| sidecars.snapshotter.timeout | string | `"120s"` |  |
| storageClass.create | bool | `true` |  |
| storageClass.fsType | string | `"ext4"` |  |
| storageClass.isDefault | bool | `false` |  |
| storageClass.name | string | `"miroir-local"` |  |
| storageClass.reclaimPolicy | string | `"Delete"` |  |
| storageClass.replicas | int | `1` |  |
| uninstall.image | string | `"registry.k8s.io/kubectl:v1.36.2"` |  |
| volumeSnapshotClass.create | bool | `true` |  |
| volumeSnapshotClass.deletionPolicy | string | `"Delete"` |  |
| volumeSnapshotClass.name | string | `"miroir-snap"` |  |

---

_This README is generated by [helm-docs](https://github.com/norwoodj/helm-docs) from `Chart.yaml` and `values.yaml`. Edit those (or `README.md.gotmpl`) and run `mise run helm-docs`._
