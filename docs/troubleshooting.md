# Troubleshooting

- **Agent pod `CrashLoopBackOff` on lvmthin**: partition or disk
  missing, or `dm_thin_pool` not loaded. Check
  `kubectl logs -n miroir-system -l app.kubernetes.io/component=agent`
  and `lsmod | grep dm_thin` on the node. On a multi-pool node the
  agent only exits when every pool fails setup; a single bad pool is
  logged and quarantined (its volumes error, the other pools keep
  serving) and shows up in the MiroirNode status as a per-pool
  `message`.
- **Agent pod `CrashLoopBackOff` on loopfile**: `baseDir` isn't
  reflink-capable. The agent refuses to start (single-pool node) so
  the failure shows up immediately.
- **Agent pod `CrashLoopBackOff` after a node change**: the DRBD
  kernel module may be below the agent's floor
  (see [Requirements](requirements.md)); the agent refuses to start
  rather than render options the module rejects. The agent log names
  the probed version and the floor.
- **PVC stays `Pending`**: every node with a MiroirNode is missing
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
- **`MiroirVolumeOutOfSync` firing while everything reads healthy**:
  `out-of-sync` bits toward a peer with no resync draining them. With the
  connection `Connected` and both disks `UpToDate`, this is one of two
  things. A _stale bitmap_ — bits stranded by a refused clear during peer
  teardown, or a resync DRBD armed and abandoned after a rapid
  promote/demote — is detected and self-healed: the agent cycles the
  affected peer connection within a couple of poll cycles and emits a
  `StuckResyncRecovered` event; the re-run handshake discards the bitmap
  (identical data moves nothing) or starts the resync it called for. A
  _`drbdadm verify` finding_ (`lastVerifyOutOfSyncBytes` non-zero in the
  coordinator's status slot, `VerifyOutOfSync` event) is a
  genuine data difference and is deliberately left manual — auto-resyncing
  would destroy the evidence of which leg was wrong. Inspect first, then
  find the affected peer with
  `drbdsetup status <res> --verbose --statistics` on the alerting node
  (the connection whose `out-of-sync` is non-zero) and cycle it:
  `drbdsetup disconnect <res> <peer-node-id>` followed by
  `drbdsetup connect <res> <peer-node-id>` resyncs the flagged blocks
  from the UpToDate side.
- **Resync activity on every kopiur backup**: kopiur's staged PVC
  inherits the source PVC's StorageClass, so a replicated volume gets
  a replicated (and therefore syncing) staging volume once per backup
  cycle. Point `spec.staging.storageClassName` at a `replicas: "1"`
  class naming the same pool, per
  [Stage kopiur backups unreplicated](quickstart.md#stage-kopiur-backups-unreplicated).
- **RWX `MiroirVolume` stuck deleting, `Device is held open`**: a
  still-running gateway is the live opener. The controller scales the
  share Deployment to zero itself when the volume is deleted, so check
  `kubectl get deploy -n miroir-system miroir-share-<volume>` first. If
  it is still at 1 replica (controller not running, or a release from
  before it did this), scale it to zero or delete it so the agent's
  teardown finalizer can finish. If it is already at 0 but the gateway
  pod stays `Terminating`, the pod is wedged on the device itself and
  the Deployment is no longer the lever: inspect the DRBD resource on
  that node as for a stuck RWO volume. See
  [ReadWriteMany (RWX)](rwx.md).
