# miroir

Replicated block storage for small Kubernetes clusters. CSI driver
on top of LVM thin, ZFS, or loopfile backends, with optional
synchronous replication (2–3 replicas) via DRBD9.

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
  `--max-peers`, and revisiting the quorum policies, which are built
  around 2 data replicas plus a tie-breaker — not done.
- You need `RWX`. Volumes are `ReadWriteOnce`.
- You need iSCSI/NFS exports. Block devices only.

## How it compares to LINSTOR and blockstor

miroir runs the same data plane as [LINSTOR][linstor] and
[blockstor][blockstor]: DRBD 9 replicating thin LVM or ZFS volumes,
synchronous protocol C, quorum with diskless tie-breakers. The
difference is the control plane.

- **LINSTOR** is the reference DRBD orchestrator and the right choice
  at fleet scale: resource groups with placement counts, automatic
  eviction and rebalancing, many storage backends, WAN replication via
  DRBD Proxy. That power rides on a Java controller with its own
  database and a satellite RPC protocol; on Kubernetes the Piraeus
  stack adds an operator, controller, per-node satellites, CSI
  controller and node drivers, and an HA controller — each its own
  workload to run and upgrade.
- **blockstor** is a clean-room Go reimplementation of the LINSTOR
  model for Cozystack: it keeps the resource-definition /
  resource-group abstractions (plus auto-evict and rebalancing) while
  replacing the JVM and database with CRDs. Architecturally it is
  miroir's closest relative.
- **miroir** cuts the scope to what 2–3 nodes actually need. The
  Kubernetes API is the _only_ control plane: the controller writes
  MiroirVolume objects, node agents watch and realize them — no
  controller database, no inter-node RPC protocol, no operator
  managing an operator. One small static image serves as both the
  controller Deployment and the agent DaemonSet, and the Helm chart is
  the entire configuration surface. Placement, quorum tie-breakers,
  and barrier-consistent snapshots are automated; resource groups,
  auto-evict, and multi-site replication deliberately are not — if
  you need those, run LINSTOR.

Where the bigger projects encode operational wisdom, miroir adopts it
instead of relearning it: `on-io-error detach`, the resync and
discard tuning knobs, and the majority-quorum-plus-tie-breaker
default all follow what LINSTOR and blockstor ship.

[linstor]: https://github.com/LINBIT/linstor-server
[blockstor]: https://github.com/cozystack/blockstor

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
- Kubelet [graceful node shutdown][gns] so the agent (a
  `system-node-critical` pod) is stopped _after_ workloads on reboot
  and can release DRBD backings before the backend pool exports —
  otherwise a node reboot can wedge unmounting the pool. On Talos it
  is on by default; give critical pods enough of the window via
  `machine.kubelet.extraConfig.shutdownGracePeriodCriticalPods` (≥ the
  agent's `terminationGracePeriodSeconds`, default 60s).

[gns]: https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/#graceful-node-shutdown

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
        zone: rack-a # optional failure domain
        address: 10.0.100.11 # optional: replicate over a dedicated NIC
    paris:
        backend: zfs
        zfsDataset: data-pool/miroir
        zone: rack-b
    le-havre:
        backend: loopfile
        baseDir: /var/lib/miroir # reflink-capable fs (XFS reflink=1, btrfs)
        zone: rack-c
```

`zone` is optional: when set, replicas — and the quorum tie-breaker —
prefer distinct zones (rack, room, AZ). Nodes without a zone are
unconstrained.

`address` is optional: it pins DRBD replication to a dedicated storage
NIC/VLAN (IPv4 or IPv6); without it, the node's `InternalIP` is used. It
applies to volumes created afterwards — existing volumes keep the address
resolved at creation.

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
`miroir-agent` DaemonSet on every schedulable node (pods can mount
miroir volumes from any node; only nodes in the `nodes` map hold
storage), and two StorageClasses: `miroir-local` (1 replica) and
`miroir-replicated` (2 replicas, DRBD9). Per-node setup jobs
provision each pool on install and upgrade, and the agent re-runs
the same idempotent setup at startup; existing pools are reused.

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

| Parameter                           | Values                        | Default  |
| ----------------------------------- | ----------------------------- | -------- |
| `miroir.home-operations.com/quorum` | `freeze`, `last-man-standing` | `freeze` |

See [Replication and quorum](#replication-and-quorum) for what the two
policies do and how the automatic diskless tie-breaker fits in.

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

## Replication and quorum

Replicated volumes are 2-way synchronous (DRBD protocol C): a write
completes only once both legs have it. The quorum policy decides what
happens when the nodes can no longer see each other, and is set per
StorageClass via the `miroir.home-operations.com/quorum` parameter
(the chart's `miroir-replicated` class uses
`replicatedStorageClass.quorum`).

### `freeze` (default)

DRBD majority quorum with `on-no-quorum io-error`: a replica that
cannot reach a majority of the volume's peers refuses writes — the
workload sees I/O errors — instead of carrying on alone. Two isolated
sides can never both write, so there is nothing to un-diverge later;
when connectivity returns, the stale side simply resyncs. Expect the
filesystem on top to have gone read-only in the meantime, so the pod
usually needs a restart once the volume recovers.

Two data replicas on their own are only 2 votes, and majority of 2 is
2 — _any_ disconnect would halt writes on both sides. That is what
the tie-breaker fixes.

### The diskless tie-breaker

When a 2-replica `freeze` volume is created and a third storage node
in the `nodes` map holds neither leg, the controller adds that node
as a diskless tie-breaker: a DRBD peer rendered with `disk none` that
joins quorum voting but stores nothing — no backing device, no
capacity used, no part in snapshots or CSI topology. With 3 votes,
losing any single node (a data leg _or_ the tie-breaker) leaves a
majority of 2 and the volume keeps serving.

Behavior and knobs:

- **Placement is zone-aware.** A spare node in a zone neither data
  leg occupies is preferred (`nodes.<node>.zone`); ties break by node
  name.
- **Existing volumes are retrofitted.** Adding a third node to
  `nodes` (a Helm upgrade, which restarts the controller) appends a
  tie-breaker to every 2-replica `freeze` volume that lacks one.
  Editing a volume's `spec.quorumPolicy` from `last-man-standing` to
  `freeze` triggers the same reconciler.
- **No spare node, no tie-breaker.** On a 2-node cluster the volume
  is created with majority quorum on 2 votes: never diverges, but a
  single node loss stops I/O until the node returns. If availability
  matters more, use `last-man-standing` — or add a third node and let
  the retrofit pick it up.
- **Opt out** with `controller.autoTieBreaker=false` in Helm values.
  This disables both placement at create time and the retrofit;
  tie-breakers can still be added manually by appending
  `{node: <name>, diskless: true}` to a volume's `spec.replicas`.
- **Remove or move one** by deleting its entry from
  `spec.replicas`; the node's agent detaches and cleans up (removal
  waits until both data legs are connected and `UpToDate`). The
  `diskless` flag itself is immutable — remove and re-add the entry
  instead of editing it.

### `last-man-standing`

`quorum off`: the surviving replica keeps accepting writes even with
no peers in sight. Maximum availability — but if both sides run while
partitioned they diverge. DRBD detects the split-brain on reconnect
and deliberately stays disconnected (`after-sb-* disconnect`, never
auto-resolve); an operator inspects both legs and picks the loser.
Volumes with this policy never get a tie-breaker.

### Remote consumers

By default a pod can only run on a node holding a replica: the PV
carries node affinity to the diskful legs, and reads are local. Set
`replicatedStorageClass.allowRemoteVolumeAccess: true` (the
`miroir.home-operations.com/allowRemoteVolumeAccess` StorageClass
parameter) to drop that affinity: a pod scheduled on any node consumes
the volume through an ephemeral **diskless client leg** — a DRBD peer
with `disk none` that the CSI node service adds to `spec.clients` at
stage time and removes at unstage. The membership reconciler completes
it (node id, address) exactly like an operator-added replica, and a pod
landing on the tie-breaker's node stages through the tie-breaker leg
directly.

Trade-offs to understand before enabling it:

- **Every read and write crosses the replication network.** A remote
  consumer runs at network speed; the affinity default exists because
  local reads are the point of keeping a replica under the pod.
- **Replica nodes are only preferred at first use.** The first
  consumer's node is pinned as a replica when it is a storage node
  (falling back to capacity-ranked placement when it is not). After
  that there is no soft preference: PV node affinity is all-or-nothing
  in Kubernetes, so the scheduler is blind to replica locations. Keep
  locality-sensitive workloads on the default class, or pin them with
  their own node/pod affinity.
- **An attached client shifts quorum math.** DRBD counts every peer's
  vote: a 2+1 volume with a client attached has 4 votes, so majority
  becomes 3. While the client is connected this is neutral-to-helpful
  (the client's vote replaces a lost node's); the regression window is
  two simultaneous failures, which majority-of-4 freezes where
  majority-of-3 tolerated one loss. Client legs come and go with pods,
  so the math shifts at stage/unstage, not permanently.
- **A lost node can strand its client leg.** If a node dies without
  unstaging, its entry in `spec.clients` (and teardown finalizer)
  lingers, holding a quorum vote and blocking volume deletion until the
  node returns or the entry is removed by hand — the same semantics as
  a dead replica node. Remove it by deleting the `spec.clients` entry.

### At a glance

| Failure                      | `freeze` + tie-breaker                                     | `freeze`, 2 nodes           | `last-man-standing`                                |
| ---------------------------- | ---------------------------------------------------------- | --------------------------- | -------------------------------------------------- |
| One node down                | keeps serving (2/3 votes)                                  | I/O errors until it returns | survivor keeps writing                             |
| Replication link partitioned | the majority side serves; the minority side refuses writes | both sides refuse writes    | both sides may write → split-brain, manual resolve |
| Two nodes down               | remaining node refuses writes                              | I/O errors                  | survivor keeps writing                             |

The default was `last-man-standing` before v0.3. Volumes keep
whatever `quorumPolicy` is stored in their spec — nothing is
rewritten on upgrade; the new default only applies to volumes created
after it.

### Disk failures, node rebuilds, and verification

A failing **disk** is not a failing node. Since v0.3 the global DRBD
config defaults to `on-io-error detach` (`drbd.onIoError`): a leg
whose backing device errors drops to Diskless and the volume keeps
serving through the peer instead of surfacing EIO into the pod. The
detached leg shows as `DiskState: Diskless` in
`kubectl describe miroirvolume` and `miroir_volume_up_to_date` goes 0
for that node — replace the disk, then remove and re-add the replica.

**Rebuilding a node is safe.** A reinstall (e.g. Talos wipe) destroys
the backing devices and miroir's node-local state together; when the
node rejoins, the agent detects the wipe and makes each recreated leg
a full sync target rather than trusting its empty disk. Full
syncs stay thin automatically: the agent probes each lvmthin/zfs
backing device's discard granularity and renders it per leg, so zero
runs are sent as discards and a re-synced leg consumes what the data
needs, not the volume's virtual size (loopfile legs are skipped — loop
devices mishandle it; `drbd.resync.discardGranularity` remains as a
manual cluster-wide fallback).

**Verification** is the only cross-leg integrity check (a ZFS scrub
validates one leg against itself). Set `drbd.verifyAlg` (e.g.
`crc32c`) and run `drbdadm verify <resource>` on a storage node
during quiet hours — cron is the DRBD-documented pattern.
Out-of-sync blocks are reported in the kernel log and
`drbdsetup status`.

## Monitoring

`monitoring.podMonitor.enabled: true` creates a Prometheus Operator
PodMonitor scraping the controller **and every agent** on their
`metrics` ports (the per-volume gauges are exported by the agent on
each storage node; a `node` label is added to every series). The
agent exports, per volume on that node:

| Metric                            | Meaning                                                                                                                                     |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `miroir_volume_up_to_date`        | 1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created)                                                     |
| `miroir_volume_connected`         | 1 when all replication links to diskful peers are established (tie-breaker links excluded)                                                  |
| `miroir_volume_split_brain`       | 1 when DRBD refused to reconnect after divergence — manual resolution required                                                              |
| `miroir_volume_suspended`         | 1 while the snapshot write barrier freezes IO; sustained means a stranded barrier                                                           |
| `miroir_volume_resync_ratio`      | fraction (0-1) in sync of the least-synced diskful peer; 1 when fully in sync                                                               |
| `miroir_volume_quorum`            | 0 while a `freeze` volume has lost quorum and its IO is suspended — the "workloads are hanging" signal (always 1 under `last-man-standing`) |
| `miroir_volume_disk_failed`       | 1 when this leg's disk was detached after an I/O error and latched failed — replace the disk, then remove and re-add the replica            |
| `miroir_volume_out_of_sync_bytes` | worst per-peer out-of-sync bytes: the exposure if the healthiest peer is lost; also counts online-verify findings                           |

Each agent additionally exports its pool capacity
(`miroir_pool_capacity_bytes` / `miroir_pool_allocated_bytes` /
`miroir_pool_meta_used_ratio`) — the same sample that feeds
capacity-aware placement and the `PoolUsageHigh` condition, so pool
exhaustion is alertable, not just an Event.

`monitoring.prometheusRule.enabled: true` ships starter alerts for all
of the above (split-brain, quorum lost, stranded barrier, disk failed,
degraded replication, sustained out-of-sync, pool and thin-metadata
usage), and `monitoring.dashboards.enabled: true` installs a Grafana
dashboard — as a sidecar-labelled ConfigMap, or a grafana-operator
`GrafanaDashboard` CR via `monitoring.dashboards.grafanaOperator`.

Both processes also expose controller-runtime metrics
(`controller_runtime_reconcile_errors_total` is the wedged-reconcile
signal), and mounted volumes get `kubelet_volume_stats_*` for free
via CSI.

## Coexistence with other provisioners

- **OpenEBS LocalPV-ZFS**: keep your pool and let `openebs-zfs` stay
  the default StorageClass. miroir scopes itself to the dataset you
  configure in Helm values.
- **Other LVM tenants**: bound the thin pool with
  `nodes.<node>.thinPoolSize` (e.g. `400g`) and let the co-tenant
  allocate from the VG's remainder.
- **Rook/Ceph**: miroir's default DRBD port base (7000) collides with
  the Ceph mgr dashboard's non-SSL default on host-network clusters.
  Set `drbd.portBase` in Helm values to move miroir's range, or move
  the dashboard (`cephClusterSpec.dashboard.port`). See
  [Troubleshooting](#troubleshooting).

## Troubleshooting

- **Agent pod `CrashLoopBackOff` on lvmthin**: partition or disk
  missing, or `dm_thin_pool` not loaded. Check
  `kubectl logs -n miroir-system -l app.kubernetes.io/component=agent` and
  `lsmod | grep dm_thin` on the node.
- **Agent pod `CrashLoopBackOff` on loopfile**: `baseDir` isn't
  reflink-capable. The agent refuses to start so the failure shows
  up immediately.
- **PVC stays `Pending`**: every node in your `nodes` map is missing
  or full. `kubectl describe pvc` shows the controller's reason.
- **Replicated volume stuck in `Degraded`**: one leg isn't
  `UpToDate`. `kubectl describe miroirvolume <name>` shows per-node
  status; usually a transient DRBD sync.
- **Replicated volume stuck `Connecting`, no split-brain, PVC hangs in
  `ContainerCreating`**: the DRBD replication port (default 7000,
  allocated per volume ascending) may be occupied by a host-network
  tenant — most commonly the Ceph mgr dashboard, whose non-SSL default
  is also 7000. The agent runs `hostNetwork: true`, so DRBD binds on the
  node's kernel and the collision is silent (no split-brain, so the
  recovery path never engages). Check `dmesg` on the node for
  `Failed to initiate connection, err=-98` (EADDRINUSE); peers dialing
  in reach the dashboard instead and log
  `Wrong magic value 0x48545450` (`"HTTP"` in ASCII). Identify the
  squatter with `curl -sI http://<node>:7000/` — a `Ceph-Dashboard`
  server header confirms it. Fix by setting `drbd.portBase` (e.g.
  `7100`) in Helm values, or moving the Ceph dashboard
  (`cephClusterSpec.dashboard.port: 8081`). Existing volumes keep their
  assigned ports — only new allocations use the new base
  ([#148](https://github.com/home-operations/miroir/issues/148)).

## Uninstall

```bash
helm uninstall miroir -n miroir-system
```

A pre-delete job deletes every MiroirVolume and MiroirSnapshot and
waits while each node's agent tears down its DRBD resources and
backing devices through the finalizers. If a teardown cannot finish
(a node is down), the job blocks: `kubectl get miroirvolumes` shows
what is stuck, and the agent log on the affected node
(`kubectl logs -n miroir-system -l app.kubernetes.io/component=agent`)
shows the failing call to clean up manually.

## Roadmap

**Should land soon**

- [x] Capacity-aware placement
- [ ] CSI `CSIStorageCapacity` reporting per pool
- [x] Per-volume DRBD state metrics (`miroir_volume_up_to_date` /
      `connected` / `split_brain` / `suspended` / `resync_ratio`;
      opt-in PodMonitor via `monitoring.podMonitor.enabled`)
- [x] Per-volume quorum / failed-disk / out-of-sync gauges and pool
      capacity metrics

**Natural extensions**

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
