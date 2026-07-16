# Quickstart

Check [Requirements](requirements.md) first (kernel modules, graceful
node shutdown), then:

## 1. Pick a storage layout

`nodes` declares which nodes hold storage and how. Each entry is
rendered as a MiroirNode custom resource (its `spec` passed through
verbatim and validated by the CRD), listing named storage `pools`
(one is enough; call it `default`).
`storageClasses` declares the classes to create (`replicas: 1` is
node-local, `replicas: 2` is DRBD-replicated). Pods can mount miroir
volumes from any schedulable node; only nodes in the map hold data.

**Two nodes, a spare partition each.** The common pair: one local and
one replicated class. Add a third storage node later and existing
replicated volumes pick it up as a quorum tie-breaker automatically.

```yaml
# values.yaml
nodes:
    node-a:
        spec:
            pools:
                - name: default
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-partlabel/r-miroir
    node-b:
        spec:
            pools:
                - name: default
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-partlabel/r-miroir
storageClasses:
    - name: miroir-local
      replicas: 1
    - name: miroir-replicated
      replicas: 2
volumeSnapshotClasses:
    - name: miroir-snap
```

**Three nodes, mixed backends.** DRBD replicates whatever device each
backend provides, so one volume can pair a ZFS zvol with an LVM thin
LV. `zone` (optional) spreads replicas and the tie-breaker across
failure domains; `address` (optional) pins replication to a dedicated
storage NIC/VLAN, IPv4 or IPv6 (default: the node's `InternalIP`;
applies to volumes created afterwards).

```yaml
nodes:
    kharkiv:
        spec:
            zone: rack-a
            address: 10.0.100.11
            pools:
                - name: default
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-partlabel/r-miroir
    paris:
        spec:
            zone: rack-b
            pools:
                - name: default
                  backend: zfs
                  zfs:
                      dataset: data-pool/miroir
    le-havre:
        spec:
            zone: rack-c
            pools:
                - name: default
                  backend: loopfile
                  loopfile:
                      baseDir: /var/lib/miroir
storageClasses:
    - name: miroir-replicated
      replicas: 2
      quorum: freeze
```

**Two tiers per node.** A pool name identifies the same tier across
nodes, and a StorageClass selects one with `pool` (classes that name
none use `default`). Volumes never span pools: every replica of a
volume lands in the class's pool on its node.

```yaml
nodes:
    node-a:
        spec:
            pools:
                - name: default # bulk tier
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-partlabel/r-miroir
                - name: fast # NVMe tier for latency-sensitive workloads
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-id/nvme-Micron_7450_XXXX
    node-b: # identical
        spec:
            pools:
                - name: default
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-partlabel/r-miroir
                - name: fast
                  backend: lvmthin
                  lvmthin:
                      device: /dev/disk/by-id/nvme-Micron_7450_YYYY
storageClasses:
    - name: miroir-replicated
      replicas: 2
    - name: miroir-replicated-fast
      replicas: 2
      pool: fast
```

**One node, no dedicated disk.** Dev clusters: loopfile backs volumes
with sparse files on an existing filesystem.

```yaml
nodes:
    solo:
        spec:
            pools:
                - name: default
                  backend: loopfile
                  loopfile:
                      baseDir: /var/lib/miroir
storageClasses:
    - name: miroir-local
      replicas: 1
      isDefault: true
```

| Backend    | You provide                            | Notes                                                    |
| ---------- | -------------------------------------- | -------------------------------------------------------- |
| `lvmthin`  | A partition or disk for the thin pool  | `dm_thin_pool` kernel module                             |
| `zfs`      | A ZFS pool, you specify the dataset    | ZFS module on the node ([Requirements](requirements.md)) |
| `loopfile` | A path on a reflink-capable filesystem | `loop` module; XFS `reflink=1` / btrfs                   |

[Helm chart values](configuration.md) documents every value:
per-class `fsType`, `reclaimPolicy`, and `allowRemoteVolumeAccess`,
`lvmthin.poolSize` for VGs shared with other tenants, DRBD tuning,
and more.

## 2. Install

```bash
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --create-namespace -f values.yaml
```

The chart deploys one `miroir-controller` Deployment, a
`miroir-agent` DaemonSet on every schedulable node, and one
MiroirNode object per `nodes` entry (`kubectl get miroirnodes`).
Each agent provisions its pools at startup with idempotent setup
(existing pools are reused), and restarts itself to re-run it when
its MiroirNode's pool spec changes.

## 3. Claim a volume

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: my-data
spec:
    storageClassName: miroir-replicated # or miroir-local
    accessModes: [ReadWriteOnce]
    resources:
        requests:
            storage: 10Gi
```

See [Replication and quorum](replication.md) for what the
per-class `quorum` policies do and how the automatic diskless
tie-breaker fits in.

## 4. Snapshot and restore

Requires the cluster-wide `snapshot-controller` and `volumesnapshot`
CRDs (see the [CSI snapshot docs](https://kubernetes-csi.github.io/docs/snapshots.html)),
plus a class under `volumeSnapshotClasses` (the `miroir-snap` example
above).

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
    name: my-data-snap
spec:
    volumeSnapshotClassName: miroir-snap
    source:
        persistentVolumeClaimName: my-data
```

Restore by pointing a new PVC at the snapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: my-data-restored
spec:
    storageClassName: miroir-local
    dataSource:
        name: my-data-snap
        kind: VolumeSnapshot
        apiGroup: snapshot.storage.k8s.io
    accessModes: [ReadWriteOnce]
    resources:
        requests:
            storage: 10Gi
```

For replicated volumes both legs get a copy-on-write snapshot while
DRBD briefly holds writes (a "write barrier"), so the two snapshots
are taken at the same instant and are consistent with each other,
not whichever leg happened to finish first.

## 5. Expand online

Edit the PVC's `spec.resources.requests.storage`. The agent grows
the backing device (`lvextend` / `zfs set volsize` / `truncate`) and
the filesystem in place if the volume is mounted.
