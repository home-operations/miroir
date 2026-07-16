# Helm chart values

Every chart value is documented value-by-value in the generated
[chart README](https://github.com/home-operations/miroir/blob/main/charts/miroir/README.md)
(kept in sync with `values.yaml` by helm-docs; CI fails if it goes
stale). This page is the orientation layer: which groups of values
exist and where their behavior is explained.

The per-node storage topology is **not** a chart value: it lives in
MiroirNode custom resources applied separately (`kubectl explain
miroirnode.spec`; [Quickstart](quickstart.md) shows the layouts). The
chart carries only the driver and the values below.

- **`storageClasses`**: the classes to create. `replicas`, `quorum`
  policy ([Replication and quorum](replication.md)), `pool` (which
  named pool the class provisions from), `fsType`, `reclaimPolicy`,
  `allowRemoteVolumeAccess` ([Remote consumers](remote-consumers.md)),
  `isDefault`.
- **`volumeSnapshotClasses`**: snapshot classes
  ([Quickstart](quickstart.md#4-snapshot-and-restore)).
- **`drbd`**: replication tuning. `portBase`
  ([Coexistence](coexistence.md)), `onIoError`, resync knobs,
  `verify.algorithm` / `verify.schedule`
  ([verification](resilience.md)).
- **Root-level behavior knobs** (the chart root is the controller):
  `autoTieBreaker` ([Replication and quorum](replication.md)),
  `autoDiskfulAfter` ([auto-diskful](remote-consumers.md#auto-diskful)),
  `autoEvictAfter` ([resilience](resilience.md)), `overcommitRatio` /
  `freeSpaceRatio`, `provisionTimeout`, `storageCapacity`.
- **`gateway`**: the per-RWX-volume NFS gateway. `enabled` (RWX is
  opt-in, off by default) and the gateway image
  ([ReadWriteMany](rwx.md)).
- **`monitoring`**: PodMonitor, PrometheusRule, dashboards
  ([Monitoring](monitoring.md)).
- **`agent` / `sidecars`** (and the root's image/resources): workload
  knobs for images, resources, `agent.kubeletDir`,
  `sidecars.healthMonitor`.
- **`logging`**: level and encoder for both components.

## ZFS zvol settings

Each ZFS pool can tune properties for newly created zvols:

- `zfs.volBlockSize` accepts `4K`, `8K`, `16K`, `32K`, `64K`, or
  `128K` (canonical spelling, uppercase `K`). It defaults to `4K`.
  OpenZFS requires `volsize` alignment, so miroir rounds new volume
  sizes up to this boundary. Expansion follows the existing zvol's
  actual block size, including for snapshot clones.
- `zfs.compression` defaults to `lz4`. Set it to `inherit` to omit a
  per-zvol property and use the parent dataset policy. It also accepts
  OpenZFS `on`, `off`, `lzjb`, `zle`, `gzip` levels, `zstd` levels, and
  `zstd-fast` levels (lowercase, as zfs(8) spells them).

```yaml
nodes:
    paris:
        spec:
            pools:
                - name: default
                  backend: zfs
                  zfs:
                      dataset: data-pool/miroir
                      volBlockSize: 16K
                      compression: inherit
```

These settings apply only when miroir creates a zvol. Reconciliation does
not mutate existing volumes, and restored snapshot clones retain their
source properties.

## The complete values.yaml

The block below is the chart's real `charts/miroir/values.yaml`,
pulled in at build time (MkDocs snippets), so the documented defaults
can never drift from the file Helm actually renders.

```yaml
--8<-- "charts/miroir/values.yaml"
```
