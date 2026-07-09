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
| controller.provisionTimeout | string | `"120s"` |  |
| controller.resources.limits.memory | string | `"128Mi"` |  |
| controller.resources.requests.cpu | string | `"10m"` |  |
| controller.resources.requests.memory | string | `"32Mi"` |  |
| drbd.resync.fillTarget | string | `""` | c-fill-target, the resync controller's target fill level (e.g. "1M"). |
| drbd.resync.maxBuffers | string | `""` | max-buffers, the DRBD receive-buffer count in the net{} section (e.g. "36864"). |
| drbd.resync.maxRate | string | `""` | c-max-rate, the resync bandwidth ceiling used when the link is idle (e.g. "720M"). |
| drbd.resync.minRate | string | `""` | c-min-rate, the resync floor guaranteed even under application I/O (e.g. "20M"); keep low on shared links. |
| drbd.resync.planAhead | string | `""` | c-plan-ahead in 0.1s units; a value > 0 enables DRBD's variable-rate resync controller. |
| drbd.resync.rate | string | `""` | resync-rate, the fixed rate used only when the controller is off (planAhead empty or 0). |
| image.digest | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"ghcr.io/home-operations/miroir"` |  |
| image.tag | string | `""` |  |
| kubeletDir | string | `"/var/lib/kubelet"` |  |
| monitoring.serviceMonitor.annotations | object | `{}` | ServiceMonitor annotations. |
| monitoring.serviceMonitor.enabled | bool | `false` | Create a Prometheus Operator ServiceMonitor (requires its CRDs). Enabling this also creates the metrics Service the ServiceMonitor scrapes. |
| monitoring.serviceMonitor.interval | string | `"30s"` | Scrape interval. |
| monitoring.serviceMonitor.labels | object | `{}` | ServiceMonitor labels. |
| monitoring.serviceMonitor.metricRelabelings | list | `[]` | Prometheus metric relabelings. |
| monitoring.serviceMonitor.path | string | `"/metrics"` | Metrics path. |
| monitoring.serviceMonitor.podTargetLabels | list | `[]` | Pod target labels to copy from pods. |
| monitoring.serviceMonitor.relabelings | list | `[]` | Prometheus relabelings (applied before scraping). |
| monitoring.serviceMonitor.scrapeTimeout | string | `"10s"` | Scrape timeout. |
| monitoring.serviceMonitor.targetLabels | list | `[]` | Target labels to copy from the Service. |
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
| sidecars.snapshotter.image | string | `"registry.k8s.io/sig-storage/csi-snapshotter:v8.6.0"` |  |
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
