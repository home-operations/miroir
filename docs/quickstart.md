# Quickstart

Check [Requirements](requirements.md) first (kernel modules, graceful
node shutdown), then:

## 1. Pick a storage layout

`nodes` declares which nodes hold storage and how; `storageClasses`
declares the classes to create (`replicas: 1` is node-local,
`replicas: 2` is DRBD-replicated). Pods can mount miroir volumes from
any schedulable node; only nodes in the map hold data.

**Two nodes, a spare partition each.** The common pair: one local and
one replicated class. Add a third storage node later and existing
replicated volumes pick it up as a quorum tie-breaker automatically.

```yaml
# values.yaml
nodes:
    node-a:
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
    node-b:
        backend: lvmthin
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
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
        zone: rack-a
        address: 10.0.100.11
    paris:
        backend: zfs
        zfsDataset: data-pool/miroir
        zone: rack-b
    le-havre:
        backend: loopfile
        baseDir: /var/lib/miroir
        zone: rack-c
storageClasses:
    - name: miroir-replicated
      replicas: 2
      quorum: freeze
```

**One node, no dedicated disk.** Dev clusters: loopfile backs volumes
with sparse files on an existing filesystem.

```yaml
nodes:
    solo:
        backend: loopfile
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
`thinPoolSize` for VGs shared with other tenants, DRBD tuning, and
more.

## 2. Install

```bash
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --create-namespace -f values.yaml
```

The chart deploys one `miroir-controller` Deployment and a
`miroir-agent` DaemonSet on every schedulable node. Per-node setup
jobs provision each pool on install and upgrade, the agent re-runs
the same idempotent setup at startup, and existing pools are reused.

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
are taken at the same instant and are consistent with each other —
not whichever leg happened to finish first.

## 5. Expand online

Edit the PVC's `spec.resources.requests.storage`. The agent grows
the backing device (`lvextend` / `zfs set volsize` / `truncate`) and
the filesystem in place if the volume is mounted.
