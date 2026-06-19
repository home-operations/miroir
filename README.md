# miroir

Replicated block storage for small Kubernetes clusters. CSI driver
on top of LVM thin, ZFS, or loopfile backends, with optional
synchronous 2-node replication via DRBD9.

## When to use it

- You want replicated block storage without running Ceph.
- You're on 2–3 nodes and either have a spare disk per node (LVM), a
  ZFS pool (ZFS), or a few GB on the root filesystem (loopfile).
- You want snapshots that actually work for replicated volumes
  (both legs cut in lockstep, not whichever finishes first).

## When _not_ to use it

- You need >3 replicas. DRBD9 itself supports more, but the
  controller validates `1..3` and reserves DRBD metadata slots for 7
  (`--max-peers 7`). Going higher means lifting the cap, raising
  `--max-peers`, and picking a quorum policy beyond
  `last-man-standing` — not built.
- You need `RWX`. Volumes are `ReadWriteOnce`.
- You need iSCSI/NFS exports. Block devices only.

## Requirements

- Kubernetes ≥ 1.31.
- A `kubelet` directory the agent can `hostPath`-mount (default
  `/var/lib/kubelet`; override `kubeletDir` in Helm values).
- Storage kernel module(s) on each storage node:
    - `lvmthin` — `dm_thin_pool`.
    - `zfs` — userland + kernel module on the same minor. On Talos
      that's the `siderolabs/zfs` extension; otherwise install ZFS
      however you normally do.
    - `loopfile` — the `loop` module and a reflink-capable filesystem
      for `baseDir` (XFS `reflink=1`, e.g. Talos `/var`, or btrfs).
- For DRBD9 replication: `drbd` and `drbd_transport_tcp` kernel
  modules, plus `drbd-utils` (`drbdadm`, `drbdsetup`, `drbdmeta`).

Talos Linux is the primary target because it ships these modules
ready to go, but nothing in the controller or agent is
Talos-specific.

## Quickstart

### 1. Declare your storage topology

```yaml
# values.yaml
nodes:
    kharkiv:
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
    paris:
        backend: zfs
        zfsDataset: data-pool/miroir
```

| Backend    | You provide                            | Notes                                  |
| ---------- | -------------------------------------- | -------------------------------------- |
| `lvmthin`  | A partition or disk for the thin pool  | `dm_thin_pool` kernel module           |
| `zfs`      | A ZFS pool, you specify the dataset    | Userland and kmod on the same minor    |
| `loopfile` | A path on a reflink-capable filesystem | `loop` module; XFS `reflink=1` / btrfs |

### 2. Install

```bash
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --create-namespace -f values.yaml
```

The chart deploys a single `miroir-controller` Deployment, a
`miroir-agent` DaemonSet on every storage node, and two
StorageClasses: `miroir-local` (1 replica) and `miroir-replicated`
(2 replicas, DRBD9). Each agent bootstraps its pool on first start;
existing pools are reused.

### 3. Claim a volume

Single-node:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: my-data
spec:
    storageClassName: miroir-local
    accessModes: [ReadWriteOnce]
    resources:
        requests:
            storage: 10Gi
```

Replicated:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: my-data
spec:
    storageClassName: miroir-replicated
    accessModes: [ReadWriteOnce]
    resources:
        requests:
            storage: 10Gi
```

| Parameter          | Values                        | Default             |
| ------------------ | ----------------------------- | ------------------- |
| `miroir.io/quorum` | `last-man-standing`, `freeze` | `last-man-standing` |

### 4. Snapshot and restore

Requires the cluster-wide `snapshot-controller` and `volumesnapshot`
CRDs (see the [CSI snapshot docs](https://kubernetes-csi.github.io/docs/snapshots.html)).

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

For replicated volumes both legs get a CoW snapshot while DRBD holds
the write barrier, so the two snapshots are consistent with each
other.

### 5. Expand online

Edit the PVC's `spec.resources.requests.storage`. The agent grows
the backing device (`lvextend` / `zfs set volsize` / `truncate`) and
the filesystem in place if the volume is mounted.

## Coexistence with other provisioners

- **OpenEBS LocalPV-ZFS**: keep your pool and let `openebs-zfs` stay
  the default StorageClass. miroir scopes itself to the dataset you
  configure in Helm values.
- **Other LVM tenants**: bound the thin pool with
  `nodes.<node>.thinPoolSize` (e.g. `400g`) and let the co-tenant
  allocate from the VG's remainder.

## Troubleshooting

- **Agent pod `CrashLoopBackOff` on lvmthin**: partition or disk
  missing, or `dm_thin_pool` not loaded. Check
  `kubectl logs -n miroir-system -l app=miroir-agent` and
  `lsmod | grep dm_thin` on the node.
- **Agent pod `CrashLoopBackOff` on loopfile**: `baseDir` isn't
  reflink-capable. The agent refuses to start so the failure shows
  up immediately.
- **PVC stays `Pending`**: every node in your `nodes` map is missing
  or full. `kubectl describe pvc` shows the controller's reason.
- **Replicated volume stuck in `Degraded`**: one leg isn't
  `UpToDate`. `kubectl describe miroirvolume <name>` shows per-node
  status; usually a transient DRBD sync.

## Uninstall

```bash
helm uninstall miroir -n miroir-system
```

A pre-delete job tears down DRBD resources on each node before the
chart releases the CRDs. If a tear-down fails (a node is down), the
job blocks until you clean up manually — see `kubectl logs -n
miroir-system -l app.kubernetes.io/job-name=miroir-uninstall` for
the exact `drbdsetup` / `lvremove` / `zfs destroy` calls needed.

## Roadmap

**Should land soon**

- [x] Capacity-aware placement
- [ ] CSI `CSIStorageCapacity` reporting per pool
- [ ] Per-volume Prometheus metrics (IOPS, latency, DRBD resync
      progress, split-brain alert)

**Natural extensions**

- [ ] 3-node topology with majority quorum
- [ ] Online volume migration (node-to-node, backend-to-backend)
- [ ] Thick LVM volumes
- [ ] Read-only clones
- [ ] Pool hot-resize

**Waiting on upstream**

- [ ] `VolumeGroupSnapshot` (Kubernetes 1.36 GA)
- [ ] CBT-based incremental backups (`thin_delta` / `zfs diff`)

**Nice-to-have**

- [ ] `mctl` CLI wrapping the CRDs

## Development

Architecture: [notes/DESIGN.md](notes/DESIGN.md). Tooling pinned with
[mise](https://mise.jdx.dev); `mise run test`, `mise run lint`,
`mise run build`, `mise run manifests`.
