# Helm chart values

Every chart value is documented value-by-value in the generated
[chart README](https://github.com/home-operations/miroir/blob/main/charts/miroir/README.md)
(kept in sync with `values.yaml` by helm-docs; CI fails if it goes
stale). This page is the orientation layer: which groups of values
exist and where their behavior is explained.

- **`nodes`** — the per-node storage topology: backend (`lvmthin` /
  `zfs` / `loopfile`), device or dataset, optional `zone` and
  replication `address`, `thinPoolSize` for shared VGs. See the
  [Quickstart](quickstart.md) layouts.
- **`storageClasses`** — the classes to create: `replicas`, `quorum`
  policy ([Replication and quorum](replication.md)), `fsType`,
  `reclaimPolicy`, `allowRemoteVolumeAccess`
  ([Remote consumers](remote-consumers.md)), `isDefault`.
- **`volumeSnapshotClasses`** — snapshot classes
  ([Quickstart](quickstart.md#4-snapshot-and-restore)).
- **`drbd`** — replication tuning: `portBase`
  ([Coexistence](coexistence.md)), `onIoError`, resync knobs,
  `verify.algorithm` / `verify.schedule`
  ([verification](resilience.md)), `autoTieBreaker`,
  `autoDiskfulAfter` ([auto-diskful](remote-consumers.md#auto-diskful)).
- **`monitoring`** — PodMonitor, PrometheusRule, dashboards
  ([Monitoring](monitoring.md)).
- **`agent` / `controller` / `sidecars`** — workload knobs: images,
  resources, `agent.kubeletDir`, `sidecars.healthMonitor`.
- **`logging`** — level and encoder for both components.

## The complete values.yaml

The block below is the chart's real `charts/miroir/values.yaml`,
pulled in at build time (MkDocs snippets), so the documented defaults
can never drift from the file Helm actually renders.

```yaml
--8<-- "charts/miroir/values.yaml"
```
