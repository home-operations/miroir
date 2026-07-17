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
