# miroir

Replicated block storage for small Kubernetes clusters. CSI driver
on top of LVM thin, ZFS, or loopfile backends, with optional
synchronous replication (2-3 replicas) via DRBD9.

📖 **Docs site: <https://miroir.home-operations.com/>** —
requirements, quickstart, replication and quorum concepts,
ReadWriteMany, node maintenance, monitoring, chart values, and
troubleshooting.

## When to use it

- You want replicated block storage without running Ceph.
- You're on 2-3 nodes and either have a spare disk per node (LVM), a
  ZFS pool (ZFS), or a few GB on the root filesystem (loopfile).
- You want snapshots that actually work for replicated volumes
  (both legs cut in lockstep, not whichever finishes first).

## When _not_ to use it

- You need >3 replicas. DRBD9 itself supports more, but the
  controller validates `1..3`, metadata reserves `--max-peers 7`, and
  the quorum policies assume 2 data replicas plus a tie-breaker.
- You need iSCSI targets or a standalone NFS/file server. miroir
  serves block devices, plus a per-volume NFS export for
  `ReadWriteMany` (see [RWX](https://miroir.home-operations.com/rwx/));
  it is not a general-purpose exporter.
- You're at fleet scale. Resource groups, automatic eviction and
  rebalancing, and multi-site replication are
  [LINSTOR](https://github.com/LINBIT/linstor-server)'s territory;
  miroir runs the same DRBD9 data plane but deliberately stops at
  what 2-3 nodes need, with the Kubernetes API as its only control
  plane.

## Quickstart

Kubernetes ≥ 1.31; storage nodes provide kernel modules only
(`dm_thin_pool`, ZFS, or `loop`, plus DRBD9 ≥ 9.3.1 for replication —
on Talos, shipped by ≥ 1.13.0). All storage userland ships in the
agent image. Details, including the Talos and Debian/Ubuntu node
setup and the graceful-node-shutdown requirement:
[Requirements](https://miroir.home-operations.com/requirements/).

MiroirNode custom resources — applied separately from the chart, one
per storage node — declare which nodes hold storage and how (named
storage pools, validated by the CRD). A StorageClass picks a pool
with `pool` (default: the pool named `default`); the chart's
`storageClasses` value declares the classes to create (`replicas: 1`
is node-local, `replicas: 2` is DRBD-replicated). The common
two-node pair:

```yaml
# topology.yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: node-a
spec:
    pools:
        - name: default
          backend: lvmthin
          lvmthin:
              device: /dev/disk/by-partlabel/r-miroir
---
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: node-b
spec:
    pools:
        - name: default
          backend: lvmthin
          lvmthin:
              device: /dev/disk/by-partlabel/r-miroir
```

```yaml
# values.yaml
storageClasses:
    - name: miroir-local
      replicas: 1
    - name: miroir-replicated
      replicas: 2
volumeSnapshotClasses:
    - name: miroir-snap
```

```bash
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --create-namespace -f values.yaml
kubectl apply -f topology.yaml
```

Then claim volumes through the created StorageClasses as usual.
Backend layouts (ZFS, loopfile, mixed, zones), snapshots and
restores, and online expansion:
[Quickstart](https://miroir.home-operations.com/quickstart/). What
the `quorum` policies do and how the automatic diskless tie-breaker
fits in:
[Replication and quorum](https://miroir.home-operations.com/replication/).

## Development

Planned work lives in the
[issue tracker](https://github.com/home-operations/miroir/issues).
Tooling pinned with [mise](https://mise.jdx.dev); `mise run test`,
`mise run lint`, `mise run build`, `mise run manifests`. The docs
site builds with `mise run docs` (strict link checking) and serves
locally with `mise run docs-serve`.
