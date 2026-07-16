# Helm chart values

Every chart value is documented value-by-value in the generated
[chart README](https://github.com/home-operations/miroir/blob/main/charts/miroir/README.md)
(kept in sync with `values.yaml` by helm-docs; CI fails if it goes
stale). This page is the orientation layer: which groups of values
exist and where their behavior is explained.

- **`nodes`** — the per-node storage topology, rendered as one
  MiroirNode custom resource per entry (the `spec` passes through
  verbatim; the CRD validates it — `kubectl explain miroirnode.spec`):
  named `pools`, each with a backend (`lvmthin` / `zfs` / `loopfile`)
  and that backend's block; node-level `zone` and replication
  `address`. See the [Quickstart](quickstart.md) layouts.
- **`storageClasses`** — the classes to create: `replicas`, `quorum`
  policy ([Replication and quorum](replication.md)), `pool` (which
  named pool the class provisions from), `fsType`, `reclaimPolicy`,
  `allowRemoteVolumeAccess` ([Remote consumers](remote-consumers.md)),
  `isDefault`.
- **`volumeSnapshotClasses`** — snapshot classes
  ([Quickstart](quickstart.md#4-snapshot-and-restore)).
- **`drbd`** — replication tuning: `portBase`
  ([Coexistence](coexistence.md)), `onIoError`, resync knobs,
  `verify.algorithm` / `verify.schedule`
  ([verification](resilience.md)), `autoTieBreaker`,
  `autoDiskfulAfter` ([auto-diskful](remote-consumers.md#auto-diskful)).
- **`gateway`** — the per-RWX-volume NFS gateway: `enabled` (RWX is
  opt-in, off by default) and the gateway image
  ([ReadWriteMany](rwx.md)).
- **`monitoring`** — PodMonitor, PrometheusRule, dashboards
  ([Monitoring](monitoring.md)).
- **`agent` / `controller` / `sidecars`** — workload knobs: images,
  resources, `agent.kubeletDir`, `sidecars.healthMonitor`.
- **`logging`** — level and encoder for both components.

## ZFS zvol settings

Each ZFS pool can tune properties for newly created zvols:

- `zfs.volBlockSize` accepts `4K`, `8K`, `16K`, `32K`, `64K`, or
  `128K` (canonical spelling, uppercase `K`). It defaults to `4K`.
  Miroir rounds new volume sizes up to this boundary because OpenZFS
  requires `volsize` alignment. Expansion follows the existing zvol's
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

These settings apply only when Miroir creates a zvol. Reconciliation does
not mutate existing volumes, and restored snapshot clones retain their
source properties.

## The complete values.yaml

The block below is the chart's real `charts/miroir/values.yaml`,
pulled in at build time (MkDocs snippets), so the documented defaults
can never drift from the file Helm actually renders.

```yaml
--8<-- "charts/miroir/values.yaml"
```
