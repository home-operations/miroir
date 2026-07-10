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
| agent.poolStatsInterval | string | `"60s"` |  |
| agent.resources.limits.memory | string | `"128Mi"` |  |
| agent.resources.requests.cpu | string | `"10m"` |  |
| agent.resources.requests.memory | string | `"32Mi"` |  |
| controller.autoTieBreaker | bool | `true` |  |
| controller.overcommitRatio | int | `2` |  |
| controller.priorityClassName | string | `"system-cluster-critical"` |  |
| controller.provisionTimeout | string | `"120s"` |  |
| controller.resources.limits.memory | string | `"128Mi"` |  |
| controller.resources.requests.cpu | string | `"10m"` |  |
| controller.resources.requests.memory | string | `"32Mi"` |  |
| drbd.onIoError | string | `"detach"` |  |
| drbd.resync.discardGranularity | string | `""` | rs-discard-granularity: during a full resync, runs of zeroes are sent as discards of this size instead of written out (e.g. "65536"), keeping a re-added thin leg thin. lvmthin/zfs only — leave empty on clusters with loopfile-backed replicated volumes (loop devices mishandle it) or entirely to keep DRBD's default (off). |
| drbd.resync.fillTarget | string | `""` | c-fill-target, the resync controller's target fill level (e.g. "1M"). |
| drbd.resync.maxBuffers | string | `""` | max-buffers, the DRBD receive-buffer count in the net{} section (e.g. "36864"). |
| drbd.resync.maxRate | string | `""` | c-max-rate, the resync bandwidth ceiling used when the link is idle (e.g. "720M"). |
| drbd.resync.minRate | string | `"10M"` | c-min-rate, the resync floor guaranteed even under application I/O. Defaulted to 10M: DRBD's kernel default (250 KiB/s) leaves a degraded volume resyncing for days under load; 10 MiB/s heals a 100Gi leg in hours while still yielding most of a 1GbE link to applications. Lower on a slow shared link. |
| drbd.resync.planAhead | string | `""` | c-plan-ahead in 0.1s units; a value > 0 enables DRBD's variable-rate resync controller. |
| drbd.resync.rate | string | `""` | resync-rate, the fixed rate used only when the controller is off (planAhead empty or 0). |
| drbd.verifyAlg | string | `"crc32c"` | verify-alg arms `drbdadm verify <res>` — the only cross-leg integrity check (a zfs scrub only validates one leg against itself). Defaulted to crc32c: drbd.ko depends on libcrc32c so it is present on every node, and it costs nothing until a verify runs. Schedule the verify pass yourself (cron, quiet hours); out-of-sync blocks surface in the kernel log and `drbdsetup status`. Empty disables verification. |
| image.digest | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"ghcr.io/home-operations/miroir"` |  |
| image.tag | string | `""` |  |
| kubeletDir | string | `"/var/lib/kubelet"` |  |
| monitoring.podMonitor.annotations | object | `{}` | PodMonitor annotations. |
| monitoring.podMonitor.enabled | bool | `false` | Create a Prometheus Operator PodMonitor (requires its CRDs) scraping the controller and every agent pod on their metrics ports. The per-volume miroir_volume_* gauges are exported by the agents. |
| monitoring.podMonitor.interval | string | `"30s"` | Scrape interval. |
| monitoring.podMonitor.labels | object | `{}` | PodMonitor labels. |
| monitoring.podMonitor.metricRelabelings | list | `[]` | Prometheus metric relabelings. |
| monitoring.podMonitor.path | string | `"/metrics"` | Metrics path. |
| monitoring.podMonitor.podTargetLabels | list | `[]` | Pod target labels to copy from pods. |
| monitoring.podMonitor.relabelings | list | `[]` | Extra Prometheus relabelings (applied before scraping); a node label from the pod's node name is always added. |
| monitoring.podMonitor.scrapeTimeout | string | `"10s"` | Scrape timeout. |
| nodes | object | `{}` |  |
| replicatedStorageClass.create | bool | `true` |  |
| replicatedStorageClass.fsType | string | `"ext4"` |  |
| replicatedStorageClass.isDefault | bool | `false` |  |
| replicatedStorageClass.name | string | `"miroir-replicated"` |  |
| replicatedStorageClass.quorum | string | `"freeze"` |  |
| replicatedStorageClass.reclaimPolicy | string | `"Delete"` |  |
| sidecars.provisioner.image | string | `"registry.k8s.io/sig-storage/csi-provisioner:v6.3.0"` |  |
| sidecars.provisioner.timeout | string | `"120s"` |  |
| sidecars.registrar.image | string | `"registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.17.0"` |  |
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
