# miroir

Low-resource replicated block storage CSI driver for small Talos Linux
clusters. Control plane in Go; data path delegated to in-kernel primitives
(dm-thin / ZFS today, DRBD9 replication planned).

**Status: alpha.** Dynamic provisioning (filesystem + raw block),
synchronous 2-node replication over DRBD9, crash-consistent snapshots
with restore, and online expansion. Untested on real hardware beyond
single-replica volumes — treat replicated volumes as experimental.

## How it works

```
PVC → csi-provisioner → miroir-controller ──(MiroirVolume CR)──▶ miroir-agent
                                                                    │
pod ← kubelet mount ← CSI node service ← /dev/<vg>/<vol> ←─ lvm/zfs ┘
```

The controller places volumes on storage nodes and records desired state in
cluster-scoped CRs; per-node agents realize them with `lvm`/`zfs` and report
status back. The data path never depends on miroir processes.

## Requirements

- Kubernetes ≥ 1.31 on Talos Linux
- `dm_thin_pool` kernel module (lvmthin nodes) and/or the `siderolabs/zfs`
  extension (zfs nodes), loaded via machine config
- One of, per storage node:
    - an unformatted partition/disk for LVM (e.g. a Talos `RawVolumeConfig`
      partition labeled `r-miroir`), or
    - an existing ZFS pool, or
    - nothing extra — the `loopfile` backend stores loop-backed sparse files
      on the node's existing filesystem (`baseDir`, e.g. `/var/lib/miroir`).
      The filesystem must support reflink for CoW snapshots (XFS `reflink=1`,
      the Talos `/var` default, or btrfs); the agent refuses to start
      otherwise. Needs the `loop` kernel module.

## Install

The storage topology is declared in Helm values — no node labels or
annotations to manage:

```yaml
# values.yaml
nodes:
    kharkiv:
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
    paris:
        backend: zfs
        zfsDataset: data-pool/miroir
    le-havre:
        backend: loopfile
        baseDir: /var/lib/miroir
```

```bash
helm install miroir charts/miroir -n miroir-system --create-namespace -f values.yaml
```

Each agent bootstraps its pool on first start (PV → VG → thin pool, the
parent ZFS dataset, or the loopfile `baseDir` layout). Existing pools and
datasets are reused, never wiped.

Sharing storage with other provisioners works on both backends: on ZFS,
miroir confines itself to its parent dataset (e.g. in a pool OpenEBS
LocalPV-ZFS also uses); on LVM, bound the thin pool with `thinPoolSize`
(default claims all free space) and let the co-tenant allocate from the
VG's remainder.

3. Provision:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: test
spec:
    storageClassName: miroir-local
    accessModes: [ReadWriteOnce]
    resources:
        requests:
            storage: 1Gi
```

## Alpha pre-flight checklist

- [ ] `talosctl read /proc/modules | grep dm_thin` on lvmthin nodes
- [ ] ZFS userland (image: 2.4.x from Alpine) vs node's zfs module version —
      `talosctl read /sys/module/zfs/version`; same minor required
- [ ] on loopfile nodes, the `baseDir` filesystem is reflink-capable
      (`xfs_info <baseDir> | grep reflink` shows `reflink=1`) and the `loop`
      module is present (`talosctl read /proc/modules | grep loop`)
- [ ] every storage node present in the Helm `nodes` values with correct backend/device
- [ ] `openebs-zfs` remains the default StorageClass (miroir-local is opt-in)

## Development

Tooling via [mise](https://mise.jdx.dev): `mise run test`, `mise run lint`,
`mise run build`, `mise run manifests`. Layout follows
[tuppr](https://github.com/home-operations/tuppr).
