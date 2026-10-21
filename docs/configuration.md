# Configuration

The **miroir** chart installs only the driver; its values are documented
value-by-value in the generated
[chart README](https://github.com/home-operations/miroir/blob/main/charts/miroir/README.md)
(kept in sync with `values.yaml` by helm-docs; CI fails if it goes
stale). The storage configuration (MiroirNode/MiroirNodeGroup custom
resources, StorageClasses, and VolumeSnapshotClasses) is plain
manifests ([Quickstart](quickstart.md) shows the layouts;
`kubectl explain miroirnode.spec` is the topology reference). This page
is the orientation layer: which groups of chart values exist, where
their behavior is explained, and every StorageClass parameter.

- **`drbd`**: replication tuning. `portBase`
  ([Coexistence](coexistence.md)), `onIoError`, resync knobs,
  `verify.algorithm` / `verify.schedule`
  ([verification](resilience.md)).
- **Root-level behavior knobs** (the chart root is the controller):
  `autoTieBreaker` ([Replication and quorum](replication.md)),
  `autoDiskfulAfter` ([auto-diskful](remote-consumers.md#auto-diskful)),
  `autoEvictAfter` ([resilience](resilience.md)), `overcommitRatio` /
  `freeSpaceRatio`, `provisionTimeout`, `storageCapacity`,
  `groupSnapshots`
  ([Quickstart → Group snapshots](quickstart.md#group-snapshots)).
- **`gateway`**: the per-RWX-volume NFS gateway. `enabled` (RWX is
  opt-in, off by default) and the gateway image
  ([ReadWriteMany](rwx.md)).
- **`monitoring`**: PodMonitor, PrometheusRule, dashboards
  ([Monitoring](monitoring.md)).
- **`agent` / `sidecars`** (and the root's image/resources): workload
  knobs for images, resources, `agent.kubeletDir`,
  `agent.loopfileBaseDirs` (hostPath mounts for loopfile pools: pod
  spec the chart cannot derive from your CRs).
- **`logging`**: level and encoder for both components.

## StorageClass parameters

A miroir class is a standard StorageClass with
`provisioner: miroir.home-operations.com` and these `parameters` (all
values are strings; quote the numbers and booleans):

| Parameter                                            | Default            | Meaning                                                                                                                                                                                                                                                                                             |
| ---------------------------------------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `miroir.home-operations.com/replicas`                | `"1"`              | Replica count, 1–3. Above 1 the volume is DRBD-replicated.                                                                                                                                                                                                                                          |
| `miroir.home-operations.com/pool`                    | `default`          | The named storage pool the class provisions from. Every replica of a volume lands in this pool on its node, so the pool must exist (in the MiroirNode specs) on at least `replicas` nodes.                                                                                                          |
| `miroir.home-operations.com/quorum`                  | `freeze`           | Replicated only: `freeze` never diverges but halts writes without a peer majority; `last-man-standing` keeps the survivor writable at the risk of split-brain. See [Replication and quorum](replication.md).                                                                                        |
| `miroir.home-operations.com/allowRemoteVolumeAccess` | `"true"`           | Replicated only: pods on nodes without a replica consume the volume through an ephemeral diskless DRBD leg at replication-network speed. `"false"` pins pods to replica nodes for local reads. See [Remote consumers](remote-consumers.md).                                                         |
| `miroir.home-operations.com/bitmapGranularity`       | absent (DRBD 4096) | Replicated only: DRBD bitmap block size in bytes, a power of two 4096–1048576. Coarser cuts bitmap RAM proportionally but resyncs more per dirty bit; worth considering for classes holding large volumes. Fixed when a replica's metadata is created: changing the class affects new volumes only. |
| `csi.storage.k8s.io/fstype`                          | `ext4`             | `ext4`, `xfs`, or `btrfs` ([not for RWX](#btrfs-volumes)).                                                                                                                                                                                                                                          |

The standard StorageClass fields behave as usual, with two worth
writing explicitly, because the Kubernetes defaults are rarely what you
want: `volumeBindingMode: WaitForFirstConsumer` (delays provisioning
until a pod schedules, so placement can prefer that pod's node; the
default `Immediate` provisions on PVC creation, reasonable only for a
replicated class consumed remotely) and `allowVolumeExpansion: true`
(the default forbids [online expansion](quickstart.md#5-expand-online)).
`reclaimPolicy`, `mountOptions`, and the
`storageclass.kubernetes.io/is-default-class` annotation work exactly as
documented upstream.

VolumeSnapshotClasses need only `driver: miroir.home-operations.com`
and a `deletionPolicy`
([Quickstart](quickstart.md#4-snapshot-and-restore) has the manifest;
the snapshot-controller and its CRDs deploy separately). PVC clones
(`dataSource: {kind: PersistentVolumeClaim}`) need no class of their
own; the clone's StorageClass just has to match the source volume's
`replicas` and `pool` ([Quickstart → Clone a
PVC](quickstart.md#clone-a-pvc)).
VolumeGroupSnapshotClasses take exactly the same two fields as a
VolumeSnapshotClass, but the feature is off by default: it needs
`groupSnapshots.enabled: true` in the chart plus the cluster-side
group snapshot CRDs and feature gate;
[Quickstart → Group snapshots](quickstart.md#group-snapshots) lists
all three switches.

## btrfs volumes

`csi.storage.k8s.io/fstype: btrfs` formats volumes with btrfs, which
gives any backend transparent compression without needing ZFS.
Compression is a btrfs mount option, so set it in the class's
`mountOptions`:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: miroir-btrfs
provisioner: miroir.home-operations.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
mountOptions:
  - compress=zstd
parameters:
  csi.storage.k8s.io/fstype: btrfs
```

- Nodes need the `btrfs` kernel module. On Talos it comes from the
  `siderolabs/btrfs` system extension; see
  [Requirements](requirements.md#node-setup-by-os).
- Request at least `128Mi`: `mkfs.btrfs` refuses much smaller devices.
- A snapshot restore or PVC clone is a block-level copy that carries
  its source's filesystem UUID. Mounting it on a node where the source
  is mounted needs kernel 6.7 or newer, which gives the copy a
  temporary UUID; older kernels refuse to mount the second filesystem.
- btrfs works for ReadWriteOnce volumes, replicated or not.
  ReadWriteMany volumes are served by the [NFS gateway](rwx.md), which
  formats `ext4` or `xfs` only and rejects a btrfs class at
  provisioning.

## ZFS zvol settings

Each ZFS pool can tune properties for newly created zvols:

- `zfs.volBlockSize` accepts `4K`, `8K`, `16K`, `32K`, `64K`, or
  `128K` (canonical spelling, uppercase `K`). It defaults to `4K`.
  OpenZFS requires `volsize` alignment, so miroir rounds new volume
  sizes up to this boundary. Expansion follows the existing zvol's
  actual block size, including for snapshot restores and PVC clones.
- `zfs.compression` defaults to `lz4`. Set it to `inherit` to omit a
  per-zvol property and use the parent dataset policy. It also accepts
  OpenZFS `on`, `off`, `lzjb`, `zle`, `gzip` levels, `zstd` levels, and
  `zstd-fast` levels (lowercase, as zfs(8) spells them).

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
  name: paris
spec:
  pools:
    - name: default
      zfs:
        dataset: data-pool/miroir
        volBlockSize: 16K
        compression: inherit
```

These settings apply only when miroir creates a zvol. Reconciliation does
not mutate existing volumes, and snapshot restores and PVC clones retain
their source volume's properties.

## The complete values.yaml

The block below is the chart's real `charts/miroir/values.yaml`,
pulled in at build time (MkDocs snippets), so the documented defaults
can never drift from the file Helm actually renders.

```yaml
--8<-- "charts/miroir/values.yaml"
```
