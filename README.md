# miroir

Replicated block storage for small Kubernetes clusters. CSI driver
on top of LVM thin, ZFS, or loopfile backends, with optional
synchronous replication (2-3 replicas) via DRBD9.

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
  `ReadWriteMany` (see [ReadWriteMany (RWX)](#readwritemany-rwx)); it
  is not a general-purpose exporter.

## How it compares to LINSTOR and blockstor

miroir runs the same data plane as [LINSTOR][linstor] and
[blockstor][blockstor]: DRBD 9 replicating thin LVM or ZFS volumes,
synchronous protocol C, quorum with diskless tie-breakers. The
difference is the control plane.

- **LINSTOR** is the reference DRBD orchestrator and the right choice
  at fleet scale: resource groups with placement counts, automatic
  eviction and rebalancing, many storage backends, WAN replication
  via DRBD Proxy. That power rides on a Java controller with its own
  database and a satellite RPC protocol; on Kubernetes the Piraeus
  stack adds an operator, controller, per-node satellites, CSI
  drivers, and an HA controller, each its own workload to run and
  upgrade.
- **blockstor** is a clean-room Go reimplementation of the LINSTOR
  model for Cozystack: the resource-definition / resource-group
  abstractions (plus auto-evict and rebalancing) with CRDs in place
  of the JVM and database. Architecturally it is miroir's closest
  relative.
- **miroir** cuts the scope to what 2-3 nodes actually need. The
  Kubernetes API is the _only_ control plane: the controller writes
  MiroirVolume objects and node agents realize them; no controller
  database, no inter-node RPC protocol, no operator managing an
  operator. One small static image serves as both the controller
  Deployment and the agent DaemonSet, and the Helm chart is the
  entire configuration surface. Placement, quorum tie-breakers, and
  barrier-consistent snapshots are automated; resource groups,
  auto-evict, and multi-site replication deliberately are not. If you
  need those, run LINSTOR.

Where the bigger projects encode operational wisdom, miroir adopts it
instead of relearning it: `on-io-error detach`, the resync,
activity-log, and discard tuning knobs, and the
majority-quorum-plus-tie-breaker default all follow what LINSTOR and
blockstor ship.

[linstor]: https://github.com/LINBIT/linstor-server
[blockstor]: https://github.com/cozystack/blockstor

## Requirements

- Kubernetes ≥ 1.31.
- A `kubelet` directory the agent can `hostPath`-mount (default
  `/var/lib/kubelet`; override `agent.kubeletDir` in Helm values).
- Kernel modules on each storage node (per-OS notes below):
  `dm_thin_pool` (lvmthin), ZFS (zfs), `loop` plus a reflink-capable
  filesystem for `baseDir` (loopfile), and, for replication, the
  DRBD9 `drbd` and `drbd_transport_tcp` modules.
- Kubelet [graceful node shutdown][gns], so the agent (a
  `system-node-critical` pod) is stopped _after_ workloads on reboot
  and can release DRBD backings before the backend pool exports;
  otherwise a node reboot can wedge unmounting the pool.

All storage userland (`drbdadm`, `lvm`, `zfs`, `mkfs`, `mount.nfs`)
ships inside the agent image; nodes only provide kernel modules.

[gns]: https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/#graceful-node-shutdown

### Talos

Talos is the primary target. The stock kernel ships `dm_thin_pool`
and `loop` (the agent loads them on demand), and `/var` is XFS with
`reflink=1`, so `lvmthin` and `loopfile` work out of the box. DRBD
and ZFS modules come from [Image Factory][factory] system extensions;
build the install image with:

- `siderolabs/drbd` for replication
- `siderolabs/zfs` for the zfs backend

Load DRBD with its usermode helper disabled: the kernel side
otherwise calls out to `/sbin/drbdadm` on the host, which does not
exist on Talos. Graceful node shutdown is on by default; size the
critical-pod window to cover the agent's
`terminationGracePeriodSeconds` (default 60s):

```yaml
machine:
    kernel:
        modules:
            - name: drbd
              parameters:
                  - usermode_helper=disabled
            - name: drbd_transport_tcp
    kubelet:
        extraConfig:
            shutdownGracePeriod: 120s
            shutdownGracePeriodCriticalPods: 60s
```

[factory]: https://factory.talos.dev

### Debian/Ubuntu

Stock kernels ship `dm_thin_pool` and `loop`, so `lvmthin` and
`loopfile` need nothing extra. For the rest:

- **DRBD9**: the in-tree `drbd.ko` is the 8.4 API and does **not**
  work with miroir's DRBD9 configuration. Install `drbd-dkms` and
  kernel headers from the [LINBIT PPA][ppa] (Ubuntu) or LINBIT's
  Debian repositories. Unless the host has a matching `drbd-utils`
  installed, disable the usermode helper for the same reason as on
  Talos: `options drbd usermode_helper=disabled` in
  `/etc/modprobe.d/drbd.conf`.
- **zfs backend**: Ubuntu kernels ship the ZFS module
  (`linux-modules-extra`); on Debian install `zfs-dkms` (contrib).
  The agent's ZFS userland is 2.3, and userland older than the
  node's module is the supported direction.
- Enable kubelet graceful node shutdown (`shutdownGracePeriod` /
  `shutdownGracePeriodCriticalPods` in the kubelet config) with a
  critical-pod window ≥ the agent's grace period (default 60s).

[ppa]: https://launchpad.net/~linbit/+archive/ubuntu/linbit-drbd9-stack

Nothing in the controller or agent is otherwise OS-specific.

## Quickstart

### 1. Pick a storage layout

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

| Backend    | You provide                            | Notes                                  |
| ---------- | -------------------------------------- | -------------------------------------- |
| `lvmthin`  | A partition or disk for the thin pool  | `dm_thin_pool` kernel module           |
| `zfs`      | A ZFS pool, you specify the dataset    | ZFS module on the node (Requirements)  |
| `loopfile` | A path on a reflink-capable filesystem | `loop` module; XFS `reflink=1` / btrfs |

The [chart README](charts/miroir/README.md) documents every value:
per-class `fsType`, `reclaimPolicy`, and `allowRemoteVolumeAccess`,
`thinPoolSize` for VGs shared with other tenants, DRBD tuning, and
more.

### 2. Install

```bash
helm install miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --create-namespace -f values.yaml
```

The chart deploys one `miroir-controller` Deployment and a
`miroir-agent` DaemonSet on every schedulable node. Per-node setup
jobs provision each pool on install and upgrade, the agent re-runs
the same idempotent setup at startup, and existing pools are reused.

### 3. Claim a volume

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

See [Replication and quorum](#replication-and-quorum) for what the
per-class `quorum` policies do and how the automatic diskless
tie-breaker fits in.

### 4. Snapshot and restore

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
StorageClass via the `quorum` field on a `storageClasses` entry with
`replicas > 1` (the `miroir.home-operations.com/quorum` parameter).

### `freeze` (default)

DRBD majority quorum with `on-no-quorum io-error`: a replica that
cannot reach a majority of the volume's peers refuses writes (the
workload sees I/O errors) instead of carrying on alone. Two isolated
sides can never both write, so there is nothing to un-diverge later;
when connectivity returns, the stale side simply resyncs. Expect the
filesystem on top to have gone read-only in the meantime, so the pod
usually needs a restart once the volume recovers.

Two data replicas on their own are only 2 votes, and majority of 2 is
2: _any_ disconnect would halt writes on both sides. That is what the
tie-breaker fixes.

### The diskless tie-breaker

When a 2-replica `freeze` volume is created and a third storage node
in the `nodes` map holds neither leg, the controller adds that node
as a diskless tie-breaker: a DRBD peer rendered with `disk none` that
joins quorum voting but stores nothing; no backing device, no
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
  matters more, use `last-man-standing`, or add a third node and let
  the retrofit pick it up.
- **Opt out** with `autoTieBreaker: false` in Helm values. This
  disables both placement at create time and the retrofit;
  tie-breakers can still be added manually by appending
  `{node: <name>, diskless: true}` to a volume's `spec.replicas`.
- **Remove or move one** by deleting its entry from
  `spec.replicas`; the node's agent detaches and cleans up (removal
  waits until both data legs are connected and `UpToDate`). A
  diskful replica can never become diskless in place; remove and
  re-add the entry instead. The other direction is allowed: flipping
  `diskless: false` on a tie-breaker makes its agent attach a fresh
  backing device to the live leg and full-sync it (this is how
  auto-diskful converts a tie-breaker).

### `last-man-standing`

`quorum off`: the surviving replica keeps accepting writes even with
no peers in sight. Maximum availability, but if both sides run while
partitioned they diverge. DRBD detects the split-brain on reconnect
and deliberately stays disconnected (`after-sb-* disconnect`, never
auto-resolve); an operator inspects both legs and picks the loser.
Volumes with this policy never get a tie-breaker.

### Remote consumers

Replicated volumes are consumable from any node by default (matching
LINSTOR): the PV carries no node affinity, and a pod scheduled on a
node without a replica consumes the volume through an ephemeral
**diskless client leg**, a DRBD peer with `disk none` that the CSI
node service adds to `spec.clients` at stage time and removes at
unstage. The membership reconciler completes it (node id, address)
exactly like an operator-added replica, and a pod landing on the
tie-breaker's node stages through the tie-breaker leg directly.

Set `allowRemoteVolumeAccess: false` on a `storageClasses` entry to
opt that class out: its PVs then pin pods to the diskful replica
nodes, guaranteeing local reads.

Trade-offs to understand:

- **Every remote read and write crosses the replication network.**
  Pin latency-sensitive workloads with
  `allowRemoteVolumeAccess: "false"` so a replica is always under the
  pod.
- **Replica nodes are only preferred at first use.** The first
  consumer's node is pinned as a replica when it is a storage node
  (capacity-ranked placement otherwise). After that there is no soft
  preference: PV node affinity is all-or-nothing, so the scheduler is
  blind to replica locations. Keep locality-sensitive workloads on a
  pinned class, or steer them with their own node/pod affinity.
- **An attached client shifts quorum math.** Its DRBD vote raises the
  majority threshold while attached (a 2+1 volume becomes 4 votes,
  majority 3); neutral-to-helpful for a single failure, stricter for
  two simultaneous ones.
- **Consumers must run on nodes listed in `nodes`.** Agents only
  start on mapped nodes, so a pod scheduled onto an unmapped node has
  no CSI driver and wedges in `ContainerCreating`. Keep every
  schedulable node in the map (a `loopfile` entry with a few spare GB
  is enough) or set `allowRemoteVolumeAccess: "false"`.
- **A lost node can strand its client leg.** Like a dead replica
  node, its `spec.clients` entry holds a quorum vote and blocks
  volume deletion until the node returns or the entry is removed by
  hand.

#### Auto-diskful

Set `autoDiskfulAfter` (e.g. `"10m"`) to convert a client leg that
has stayed attached past the threshold into a diskful replica on its
node (LINSTOR's auto-diskful). The consumer evidently lives there, so
it gets a local replica and stops paying network I/O: the entry moves
from `spec.clients` to `spec.replicas`, membership completes it as a
FullSync joiner, and the agent attaches a backing device to the live
resource while the pod keeps running. Conversion requires the
client's node in the `nodes` map with fresh pool stats and room for
the volume's full size, and a Ready volume; a 2+1 volume's
tie-breaker is replaced by the third data copy (three diskful votes
need no tie-breaker). Volumes already at 3 diskful replicas are left
alone; evicting a replica is an operator decision. Empty (the
default) disables it.

On a fully-mapped cluster (every node in `nodes`) the volume's
non-replica node is its tie-breaker, so a settled consumer stages
through that leg and no client leg ever exists. Auto-diskful covers
this too: a tie-breaker leg whose device has been held Primary past
the threshold (the agent stamps `primarySince` from the kernel role)
is flipped diskful **in place**: node id and address kept, a fresh
backing device attached to the live resource, full-synced under the
running pod.

### At a glance

| Failure                      | `freeze` + tie-breaker                                     | `freeze`, 2 nodes           | `last-man-standing`                                |
| ---------------------------- | ---------------------------------------------------------- | --------------------------- | -------------------------------------------------- |
| One node down                | keeps serving (2/3 votes)                                  | I/O errors until it returns | survivor keeps writing                             |
| Replication link partitioned | the majority side serves; the minority side refuses writes | both sides refuse writes    | both sides may write → split-brain, manual resolve |
| Two nodes down               | remaining node refuses writes                              | I/O errors                  | survivor keeps writing                             |

The default was `last-man-standing` before v0.3. Volumes keep
whatever `quorumPolicy` is stored in their spec; nothing is rewritten
on upgrade, and the new default only applies to volumes created after
it.

### Disk failures, node rebuilds, and verification

A failing **disk** is not a failing node. Since v0.3 the global DRBD
config defaults to `on-io-error detach` (`drbd.onIoError`): a leg
whose backing device errors drops to Diskless and the volume keeps
serving through the peer instead of surfacing EIO into the pod. The
detached leg shows as `DiskState: Diskless` in
`kubectl describe miroirvolume` and `miroir_volume_disk_failed` goes
1 for that node: replace the disk, then remove and re-add the
replica.

**Rebuilding a node is safe.** A reinstall (e.g. Talos wipe) destroys
the backing devices and miroir's node-local state together; when the
node rejoins, the agent detects the wipe and makes each recreated leg
a full sync target rather than trusting its empty disk. Full syncs
stay thin automatically: the agent probes each lvmthin/zfs backing
device's discard granularity and renders it per leg, so zero runs are
sent as discards and a re-synced leg consumes what the data needs,
not the volume's virtual size (loopfile legs are skipped, loop
devices mishandle it; `drbd.resync.discardGranularity` remains as a
manual cluster-wide fallback).

**Verification** is the only cross-leg integrity check (a ZFS scrub
validates one leg against itself). `drbd.verifyAlg` (default
`crc32c`) arms it, and `drbd.verify.schedule` (5-field cron, e.g.
`"0 4 * * 0"`) runs an online verify of every replicated volume on
that cadence, serialized per node and skipping volumes that are
resyncing. Findings land in the volume's status
(`lastVerifyOutOfSyncBytes`), the `miroir_volume_verify_*` metrics,
and a `VerifyOutOfSync` event; the starter alerts flag both findings
and a schedule that stopped firing. `drbdadm verify <resource>` on a
storage node does the same by hand.

## ReadWriteMany (RWX)

A PVC with `accessModes: [ReadWriteMany]` (or `ReadOnlyMany`) on a
replicated class is served as a **shared filesystem over NFS**: many
pods on many nodes read and write it at once, like CephFS.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
    name: shared-data
spec:
    storageClassName: miroir-replicated
    accessModes: [ReadWriteMany]
    resources:
        requests:
            storage: 10Gi
```

Under the hood the volume is a normal miroir DRBD volume. The
controller runs one **gateway** pod for it on a replica node: the pod
mounts the device (DRBD `auto-promote` makes it the single writer),
formats it `ext4`/`xfs`, and exports it over NFSv4 with NFS-Ganesha.
A per-volume `ClusterIP` Service fronts the gateway, and the CSI node
plugin on any node NFS-mounts that Service for pods. Because the
gateway is the only writer, single-primary DRBD fencing is unchanged:
miroir never enables dual-primary, and there is no cluster
filesystem.

Things worth knowing:

- **RWX requires a replicated class** (`replicas ≥ 2`). The gateway
  fails over by rescheduling onto another replica node, so it needs a
  second one to move to; the controller rejects RWX on a
  single-replica volume.
- **Consistency is NFS close-to-open**, not shared-memory: a writer's
  changes are visible on other nodes once it closes the file (or
  `fsync`s).
- **Failover.** If the gateway's node dies, the Deployment
  reschedules the gateway onto a surviving replica node and NFS
  clients (hard mounts) reconnect through the same Service IP. Expect
  **tens of seconds**: eviction from the dead node, DRBD promotion
  once quorum releases the old Primary, and the NFSv4 grace period.
  Client I/O stalls (never errors) across the window. Fine for the
  homelab RWX cases (media libraries, shared config); not a
  low-latency-failover HA-NAS.
- **`freeze` quorum is required** (the default). Under
  `last-man-standing` a partition could leave the old and rescheduled
  gateways both writable; the controller rejects that combination.
- **Snapshots** work exactly as for RWO volumes (crash-consistent,
  device-level; the gateway is the sole writer during the barrier),
  and the volume gets the same split-brain protections: the gateway
  stages through the same pipeline that latches "this volume holds
  data", so auto-recovery never discards a diverged leg out from
  under it.
- The gateway keeps NFSv4 lock-recovery state in a
  `.ganesha-recovery` directory at the root of the exported
  filesystem so locks survive failover; it is visible to consumers.
  Leave it alone.

## Monitoring

`monitoring.podMonitor.enabled: true` creates a Prometheus Operator
PodMonitor scraping the controller **and every agent** on their
`metrics` ports (the per-volume gauges are exported by the agent on
each storage node; a `node` label is added to every series). The
agent exports, per volume on that node:

| Metric                                        | Meaning                                                                                                                                    |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `miroir_volume_up_to_date`                    | 1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created)                                                    |
| `miroir_volume_connected`                     | 1 when all replication links to diskful peers are established (tie-breaker links excluded)                                                 |
| `miroir_volume_split_brain`                   | 1 when DRBD refused to reconnect after divergence; manual resolution required                                                              |
| `miroir_volume_suspended`                     | 1 while the snapshot write barrier freezes IO; sustained means a stranded barrier                                                          |
| `miroir_volume_resync_ratio`                  | fraction (0-1) in sync of the least-synced diskful peer; 1 when fully in sync                                                              |
| `miroir_volume_quorum`                        | 0 while a `freeze` volume has lost quorum and its IO is suspended, the "workloads are hanging" signal (always 1 under `last-man-standing`) |
| `miroir_volume_disk_failed`                   | 1 when this leg's disk was detached after an I/O error and latched failed; replace the disk, then remove and re-add the replica            |
| `miroir_volume_out_of_sync_bytes`             | worst per-peer out-of-sync bytes: the exposure if the healthiest peer is lost; also counts online-verify findings                          |
| `miroir_volume_diskless_primary`              | 1 while a diskless leg (client or tie-breaker) is Primary here: the consumer pays network I/O; see auto-diskful                            |
| `miroir_volume_verify_last_timestamp_seconds` | unix time of the last completed scheduled verify; alert on staleness to catch a schedule that stopped firing                               |
| `miroir_volume_verify_out_of_sync_bytes`      | out-of-sync bytes the last scheduled verify found (0 = clean)                                                                              |

Each agent additionally exports its pool capacity
(`miroir_pool_capacity_bytes` / `miroir_pool_allocated_bytes` /
`miroir_pool_meta_used_ratio`), the same sample that feeds
capacity-aware placement and the `PoolUsageHigh` condition, so pool
exhaustion is alertable, not just an Event.

For RWX volumes the **controller** exports `miroir_export_ready`: 1
while the volume's NFS gateway is serving (gateway pod available,
export address published). This is the signal the per-volume gauges
cannot give you: DRBD replicas stay healthy while a dead gateway
leaves every NFS client hanging.

Prometheus is not the only surface. Volume health also flows through
the CSI `VolumeCondition`: enable `sidecars.healthMonitor.enabled`
and split-brain, failed-disk, and degraded volumes surface as events
on their PVCs (`kubectl describe pvc`).

`monitoring.prometheusRule.enabled: true` ships starter alerts for
all of the above (split-brain, quorum lost, stranded barrier, disk
failed, degraded replication, sustained out-of-sync, an unavailable
RWX export, a stale verify schedule, pool and thin-metadata usage),
and `monitoring.dashboards.enabled: true` installs a Grafana
dashboard, either a sidecar-labelled ConfigMap or a grafana-operator
`GrafanaDashboard` CR via `monitoring.dashboards.grafanaOperator`.

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
  `kubectl logs -n miroir-system -l app.kubernetes.io/component=agent`
  and `lsmod | grep dm_thin` on the node.
- **Agent pod `CrashLoopBackOff` on loopfile**: `baseDir` isn't
  reflink-capable. The agent refuses to start so the failure shows
  up immediately.
- **PVC stays `Pending`**: every node in your `nodes` map is missing
  or full. `kubectl describe pvc` shows the controller's reason.
- **Replicated volume stuck in `Degraded`**: one leg isn't
  `UpToDate`. `kubectl describe miroirvolume <name>` shows per-node
  status; usually a transient DRBD sync.
- **Replicated volume stuck `Connecting`, no split-brain**: a
  host-network tenant (commonly the Ceph mgr dashboard) occupies the
  DRBD replication port; `dmesg` shows
  `Failed to initiate connection, err=-98`. Set `drbd.portBase` (e.g.
  `7100`) to move miroir's range; existing volumes keep their ports.
  Full forensics in
  [#148](https://github.com/home-operations/miroir/issues/148).

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

## Development

Architecture: [notes/DESIGN.md](notes/DESIGN.md). Planned work lives
in the [issue tracker](https://github.com/home-operations/miroir/issues).
Tooling pinned with [mise](https://mise.jdx.dev); `mise run test`,
`mise run lint`, `mise run build`, `mise run manifests`.
