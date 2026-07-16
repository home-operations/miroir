# Quickstart

Check [Requirements](requirements.md) first (kernel modules, graceful
node shutdown), then:

## 1. Pick a storage layout

The storage topology is **MiroirNode custom resources**, one per
storage node (named after it), applied and managed separately from the
Helm chart — kubectl, GitOps, whatever applies your manifests. Each
lists named storage `pools` (one is enough; call it `default`), and
the CRD validates the spec (`kubectl explain miroirnode.spec`).
The chart's `storageClasses` value declares the classes to create
(`replicas: 1` is node-local, `replicas: 2` is DRBD-replicated). Pods
can mount miroir volumes from any schedulable node; only nodes with a
MiroirNode hold data.

**Two nodes, a spare partition each.** The common pair: one local and
one replicated class. Add a third storage node later and existing
replicated volumes pick it up as a quorum tie-breaker automatically.

```yaml
# topology.yaml — one MiroirNode per storage node
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
# values.yaml — the chart carries only the driver and its classes
storageClasses:
    - name: miroir-local
      replicas: 1
    - name: miroir-replicated
      replicas: 2
volumeSnapshotClasses:
    - name: miroir-snap
```

**Mixed backends and failure domains.** DRBD replicates whatever
device each backend provides, so one volume can pair a ZFS zvol with
an LVM thin LV. `zone` (optional) spreads replicas and the
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
          backend: lvmthin
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
          backend: zfs
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
          backend: loopfile
          loopfile:
              baseDir: /var/lib/miroir
```

**Two tiers per node.** A pool name identifies the same tier across
nodes, and a StorageClass selects one with `pool` (classes that name
none use `default`). Volumes never span pools: every replica of a
volume lands in the class's pool on its node.

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: node-a
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
```

```yaml
storageClasses:
    - name: miroir-replicated
      replicas: 2
    - name: miroir-replicated-fast
      replicas: 2
      pool: fast
```

**One node, no dedicated disk.** Dev clusters: loopfile backs volumes
with sparse files on an existing filesystem. Loopfile base
directories must also be listed in the chart's
`agent.loopfileBaseDirs` so the agent pod mounts them.

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
    name: solo
spec:
    pools:
        - name: default
          backend: loopfile
          loopfile:
              baseDir: /var/lib/miroir
```

```yaml
agent:
    loopfileBaseDirs: [/var/lib/miroir]
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
kubectl apply -f topology.yaml
```

The chart deploys one `miroir-controller` Deployment and a
`miroir-agent` DaemonSet on every schedulable node (plus the CRDs, so
the topology applies after the first install). Each agent provisions
its node's pools with idempotent setup the moment its MiroirNode
exists (existing pools are reused) — agents on nodes without one
serve client-only and switch over by themselves — and restarts itself
to re-run setup when the pool spec changes. Inspect the topology with
`kubectl get miroirnodes`.

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
