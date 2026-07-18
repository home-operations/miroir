# miroir

Replicated block storage for small Kubernetes clusters. CSI driver
on top of LVM thin, ZFS, or loopfile backends, with optional
synchronous replication (2-3 replicas) via DRBD9.

In practice, miroir turns disks already in your nodes into
PersistentVolumes. Each volume is a real block device on the node
(an LVM thin volume, a ZFS zvol, or a sparse file behind a loop
device), and the CSI driver hands it to pods. When a StorageClass
asks for 2 replicas, [DRBD](https://linbit.com/drbd/) (a
replication layer built into the Linux kernel) keeps a
byte-identical copy on a second node by completing every write on
both. If a node dies, pods restart on the surviving node with
current data.

## When to use it

- You want replicated block storage without running Ceph.
- You're on 2-3 nodes and either have a spare disk per node (LVM), a
  ZFS pool (ZFS), or a few GB on the root filesystem (loopfile).
- You want snapshots that actually work for replicated volumes
  (both legs cut in lockstep, not whichever finishes first), up to
  crash-consistent group snapshots across several PVCs at once.

## When _not_ to use it

- You need >3 replicas. DRBD9 itself supports more, but the
  controller validates `1..3`, metadata reserves `--max-peers 7`, and
  the quorum policies assume 2 data replicas plus a tie-breaker.
- You need iSCSI targets or a standalone NFS/file server. miroir
  serves block devices, plus a per-volume NFS export for
  `ReadWriteMany` (see [ReadWriteMany (RWX)](rwx.md)); it
  is not a general-purpose exporter.
- You're at fleet scale. Resource groups, automatic eviction and
  rebalancing, and multi-site replication are
  [LINSTOR](https://github.com/LINBIT/linstor-server)'s territory;
  miroir runs the same DRBD9 data plane but deliberately stops at
  what 2-3 nodes need, with the Kubernetes API as its only control
  plane.

## Terminology

A handful of words recur in these docs. Most come from DRBD; none
require DRBD experience:

- **Leg** (or **replica**): one copy of a volume on one node. A
  2-replica volume has two legs, each a local block device kept
  identical by DRBD.
- **Diskful / diskless**: whether a leg has a backing device on its
  node. A diskless leg participates in the volume's replication
  network without storing any data.
- **Quorum**: majority voting among a volume's legs about which
  side may keep writing when they lose sight of each other. The
  point is to make it impossible for two disconnected sides to both
  accept writes.
- **Tie-breaker**: a diskless leg added as a third vote so that a
  2-replica volume can survive one node loss without risking the
  two-writers case. It stores nothing and uses no capacity.
- **Client leg**: a temporary diskless leg through which a pod on a
  node _without_ a replica reads and writes the volume over the
  network.
- **Primary / Secondary**: DRBD's roles. The Primary leg is the one
  serving I/O to a consumer (at most one per volume in miroir); every
  other leg is Secondary. "Promotion" means becoming Primary.
- **Split-brain**: what quorum exists to prevent. Two legs both
  accepted writes while disconnected and now hold different data.
  DRBD detects it on reconnect and refuses to merge; an operator
  picks which side's writes to keep.
- **Resync**: DRBD copying blocks from a current leg to a stale one
  (after a reboot, a replaced disk, a rejoined node) until they are
  identical again.
- **UpToDate / Degraded**: a leg is `UpToDate` when it holds
  current data; a volume reads `Degraded` while any of its legs
  doesn't (typically: a resync is running).

## Where to next

- **[Requirements](requirements.md)**: kernel modules, Talos and
  Debian/Ubuntu node setup, graceful node shutdown.
- **[Quickstart](quickstart.md)**: pick a storage layout, install
  the chart, claim / snapshot / expand a volume.
- **[Replication and quorum](replication.md)**: what the `freeze`
  and `last-man-standing` policies do and how the automatic diskless
  tie-breaker fits in.
- **[Remote consumers and auto-diskful](remote-consumers.md)**:
  mounting volumes from nodes without a replica, and converting
  settled consumers to local replicas.
- **[ReadWriteMany (RWX)](rwx.md)**: shared filesystems over NFS on
  top of replicated volumes.
- **[Node maintenance and upgrades](maintenance.md)**: the safe
  per-node loop for reboots and upgrades.
- **[Monitoring](monitoring.md)**: metrics, starter alerts, and the
  Grafana dashboard.
- **[Helm chart values](configuration.md)**: every chart knob.
- **[Troubleshooting](troubleshooting.md)**: when something doesn't
  go green.
