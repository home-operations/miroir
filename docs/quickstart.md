# Quickstart

Check [Requirements](requirements.md) first (kernel modules, graceful
node shutdown), then:

## 1. Pick a storage layout

Storage configuration is plain manifests, applied and versioned like
any other Kubernetes object: the node topology (MiroirNode custom
resources — or a MiroirNodeGroup that materializes them from a label
selector), the StorageClasses (`replicas: "1"` is node-local,
`replicas: "2"` is DRBD-replicated), and the VolumeSnapshotClasses.
The chart installs only the driver. The CRDs validate the topology
(`kubectl explain miroirnode.spec` is the reference), `kubectl apply`
rejects unknown fields outright, and pool names and the classes that
select them sit side by side in whatever file you keep them in. Pods
can mount miroir volumes from any node with a MiroirNode — data lives
only on nodes whose pools hold replicas; the rest consume remotely
([Remote consumers](remote-consumers.md)). Keep every schedulable node
in the map, or set the `allowRemoteVolumeAccess: "false"` parameter on
the class so pods stay on replica nodes.

**A fleet with a path convention.** The common case: every storage node
carries the same partition label, so the whole fleet is ONE node group —
a MiroirNode is materialized per label-matched node, and **adding a
storage node is labeling it** (`kubectl label node node-c
storage.miroir.home-operations.com/class=std`). Existing replicated
volumes pick a third node up as a quorum tie-breaker automatically.

```yaml
# topology.yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNodeGroup
metadata:
    name: std
spec:
    nodeSelector:
        matchLabels:
            storage.miroir.home-operations.com/class: std
    template:
        pools:
            - name: default
              lvmthin:
                  device: /dev/disk/by-partlabel/r-miroir
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
    name: miroir-local
provisioner: miroir.home-operations.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
    miroir.home-operations.com/replicas: "1"
    csi.storage.k8s.io/fstype: ext4
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
    name: miroir-replicated
provisioner: miroir.home-operations.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
    miroir.home-operations.com/replicas: "2"
    csi.storage.k8s.io/fstype: ext4
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
    name: miroir-snap
driver: miroir.home-operations.com
deletionPolicy: Delete
```

Write `volumeBindingMode` and `allowVolumeExpansion` explicitly as
above — the Kubernetes defaults (`Immediate`, no expansion) are rarely
what you want. Every class parameter is documented in
[Helm chart values → StorageClass parameters](configuration.md#storageclass-parameters).

Group members inherit per-node facts from the Node object: an empty
template `zone` takes the node's `topology.kubernetes.io/zone` label,
and a dedicated replication NIC comes from a
`miroir.home-operations.com/address` annotation on the Node. A node
leaving the selector orphans its MiroirNode in place (never deleted
under live volumes); hand-authored MiroirNodes always win over groups —
use them for odd-one-out boxes.

**Mixed backends and failure domains.** Heterogeneous nodes are
per-node MiroirNodes (or label-partitioned groups per disk class).
DRBD replicates whatever device each backend provides, so one volume
can pair a ZFS zvol with an LVM thin LV. `zone` (optional) spreads replicas and the
tie-breaker across failure domains; `address` (optional) pins
replication to a dedicated storage NIC/VLAN, IPv4 or IPv6 (default:
the node's `InternalIP`; applies to volumes created afterwards).

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: kharkiv
spec:
    zone: rack-a
    address: 10.0.100.11
    pools:
        - name: default
          lvmthin:
              device: /dev/disk/by-partlabel/r-miroir
---
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: paris
spec:
    zone: rack-b
    pools:
        - name: default
          zfs:
              dataset: data-pool/miroir
---
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: le-havre
spec:
    zone: rack-c
    pools:
        - name: default
          loopfile:
              baseDir: /var/lib/miroir
```

**Two tiers per node.** A pool name identifies the same tier across
nodes, and a StorageClass selects one with the `pool` parameter
(classes that name none use `default`). Volumes never span pools:
every replica of a volume lands in the class's pool on its node.

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: node-a # node-b is identical, with its own NVMe device id
spec:
    pools:
        - name: default # bulk tier
          lvmthin:
              device: /dev/disk/by-partlabel/r-miroir
        - name: fast # NVMe tier for latency-sensitive workloads
          lvmthin:
              device: /dev/disk/by-id/nvme-Micron_7450_XXXX
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
    name: miroir-replicated-fast
provisioner: miroir.home-operations.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
    miroir.home-operations.com/replicas: "2"
    miroir.home-operations.com/pool: fast
    csi.storage.k8s.io/fstype: ext4
```

**One node, no dedicated disk.** Dev clusters: loopfile backs volumes
with sparse files on an existing filesystem. Loopfile base
directories must also be listed in the **driver** chart's
`agent.loopfileBaseDirs` so the agent pod mounts them.

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: solo
spec:
    pools:
        - name: default
          loopfile:
              baseDir: /var/lib/miroir
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
    name: miroir-local
    annotations:
        storageclass.kubernetes.io/is-default-class: "true"
provisioner: miroir.home-operations.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
    miroir.home-operations.com/replicas: "1"
    csi.storage.k8s.io/fstype: ext4
```

A pool's backend is the configuration block it carries — exactly one
of `lvmthin`, `zfs`, or `loopfile` (write `lvmthin: {}` when the VG
already exists).

| Backend    | You provide                            | Notes                                                    |
| ---------- | -------------------------------------- | -------------------------------------------------------- |
| `lvmthin`  | A partition or disk for the thin pool  | `dm_thin_pool` kernel module                             |
| `zfs`      | A ZFS pool, you specify the dataset    | ZFS module on the node ([Requirements](requirements.md)) |
| `loopfile` | A path on a reflink-capable filesystem | `loop` module; XFS `reflink=1` / btrfs                   |

[Configuration](configuration.md) documents the driver chart's values
(DRBD tuning, behavior knobs, workloads) and every
[StorageClass parameter](configuration.md#storageclass-parameters):
`quorum`, `allowRemoteVolumeAccess`, `bitmapGranularity`, fstype, and
more. For the MiroirNode spec itself, `kubectl explain miroirnode.spec`
is always current.

## 2. Install

```bash
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --create-namespace
kubectl apply -f topology.yaml
```

The driver chart deploys one `miroir-controller` Deployment and a
`miroir-agent` DaemonSet on every schedulable node, plus the CRDs — so
the topology manifests apply after it. Each agent provisions its node's
pools with idempotent setup the moment its MiroirNode exists (existing
pools are reused) — agents on nodes without one serve client-only and
switch over by themselves — and restarts itself to re-run setup when
the pool spec changes. Your manifests are the source of truth: nothing
deletes a MiroirNode but you, and decommissioning a node stays an
explicit `kubectl delete miroirnode <name>` — for a group member, first
take the node out of the selector (remove its class label), or the
group recreates the MiroirNode within seconds. Inspect the topology
with `kubectl get miroirnodes`.

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
plus a VolumeSnapshotClass (the `miroir-snap` manifest above).

Snapshots of a mounted filesystem are filesystem-consistent: on the
node where the volume is mounted, the agent freezes the filesystem
(flushing every cached write) just before the cut and thaws it right
after, so even data an application wrote without `fsync` is in the
snapshot. Raw block volumes are crash-consistent — miroir cannot know
what the application considers a consistent instant there; quiesce
writes yourself if you need more.

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

### Clone a PVC

Point a new PVC directly at an existing one to copy it without an
intermediate VolumeSnapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: my-data-copy
spec:
    storageClassName: miroir-local
    dataSource:
        name: my-data
        kind: PersistentVolumeClaim
    accessModes: [ReadWriteOnce]
    resources:
        requests:
            storage: 10Gi
```

Under the hood miroir cuts a hidden snapshot of the source volume
(same write barrier as above) and restores from it, so the clone is
a cheap copy-on-write copy that lands on the same nodes and pool as
the source. That brings the same rules as a snapshot restore: the
clone's StorageClass must ask for the source's replica count and
pool, and its size must be at least the source's. The hidden
snapshot is cleaned up when the clone is deleted.

### Group snapshots

Snapshot several PVCs as one crash-consistent set (a database and
its WAL volume, an app and its uploads): every member volume is
frozen before any snapshot is cut, and none resumes before every cut
lands, so the members are consistent with **each other**, not just
individually.

Three switches beyond the regular snapshot stack, all off by
default: the `groupsnapshot.storage.k8s.io` CRDs installed
cluster-wide, the cluster's snapshot-controller running with
`--feature-gates=CSIVolumeGroupSnapshot=true`, and
`groupSnapshots.enabled: true` in the chart (which passes the same
feature gate to the csi-snapshotter sidecar). Only replicated
volumes (`replicas` ≥ 2) can join a group: the DRBD write barrier is
what makes the cut atomic across volumes.

With external-snapshotter v8.6 and later the group API is served at
`v1` (older releases serve `v1beta2`), and upstream ships the group
CRDs with `conversion.strategy: None`; the schemas are identical
across versions, so no conversion webhook is involved. If another
chart has stamped a webhook conversion onto these CRDs (some
snapshot-controller charts do), group snapshots fail with confusing
`service not found` or `unexpected conversion version` errors while
regular snapshots keep working; patch the CRDs back to
`strategy: None`.

```yaml
apiVersion: groupsnapshot.storage.k8s.io/v1
kind: VolumeGroupSnapshotClass
metadata:
    name: miroir-group-snap
driver: miroir.home-operations.com
deletionPolicy: Delete
---
apiVersion: groupsnapshot.storage.k8s.io/v1
kind: VolumeGroupSnapshot
metadata:
    name: db-nightly
spec:
    volumeGroupSnapshotClassName: miroir-group-snap
    source:
        selector:
            matchLabels:
                app: db # every PVC carrying this label joins the set
```

Each member is an ordinary VolumeSnapshot and restores individually
through `dataSource` exactly like the single-volume restore above.
Members delete as a set: deleting the VolumeGroupSnapshot removes
them all, and a member cannot be deleted on its own.

## 5. Expand online

Edit the PVC's `spec.resources.requests.storage`. The agent grows
the backing device (`lvextend` / `zfs set volsize` / `truncate`) and
the filesystem in place if the volume is mounted.
