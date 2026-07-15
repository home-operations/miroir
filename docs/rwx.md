# ReadWriteMany (RWX)

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
gateway is the only writer, DRBD stays in its normal single-writer
(single-Primary) mode with all its safety properties intact: miroir
never enables dual-primary, and there is no cluster filesystem.

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
- **Snapshots** work exactly as for RWO volumes: device-level and
  crash-consistent (the snapshot captures the filesystem as if the
  node had lost power at that instant; journaling filesystems
  recover this cleanly), with the gateway as the sole writer during
  the barrier,
  and the volume gets the same split-brain protections: the gateway
  stages through the same pipeline that latches "this volume holds
  data", so auto-recovery never discards a diverged leg out from
  under it.
- The gateway keeps NFSv4 lock-recovery state in a
  `.ganesha-recovery` directory at the root of the exported
  filesystem so locks survive failover; it is visible to consumers.
  Leave it alone.
