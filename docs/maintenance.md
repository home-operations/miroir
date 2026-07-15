# Node maintenance and upgrades

Upgrading or rebooting nodes one at a time is safe for the data:
`freeze` volumes never diverge, and with only one side down at a time
`last-man-standing` cannot split-brain either. Two rules keep it
non-disruptive as well.

**Wait for Ready between nodes.** After a node rejoins, its legs
resync from the surviving replicas and the affected volumes read
`Degraded` until that finishes. Draining the next node inside that
window takes the only UpToDate copy out of service: pods using those
volumes cannot start anywhere, the RWX gateway has no healthy replica node to fail over
to, and `freeze` volumes stop serving writes. No data is ever lost
(DRBD refuses to serve stale legs), but every replicated volume goes
unavailable until the drained node returns. Gate the loop on every
volume being Ready again:

```bash
kubectl wait miroirvolumes --all \
  --for=jsonpath='{.status.phase}'=Ready --timeout=1h
```

`miroir_volume_resync_ratio` shows progress, and the
[starter alerts](monitoring.md) flag sustained degradation.

**Cordon before rebooting.** The agent releases its Secondary DRBD
backings at shutdown only when the node is cordoned (the gate exists
so routine pod restarts do not churn replication). `kubectl drain`
and `talosctl upgrade` cordon for you; a bare `reboot` on a
Debian/Ubuntu node does not, and risks the pool-export wedge the
[Requirements](requirements.md) page warns about.

The per-node loop is therefore: cordon and drain, upgrade or reboot,
wait for the node to rejoin, wait for Ready, uncordon, next node.

While a single node is down, replicated volumes keep serving per the
quorum table (see [At a glance](replication.md#at-a-glance)), and the
RWX gateway reschedules onto the surviving replica node (client I/O
stalls for tens of seconds, never errors). Operations that need the
missing node pause and converge when it returns rather than failing:

- Creating a replicated volume whose placement needs the node (on a
  2-node cluster, any replicated PVC) stays Pending: the initial sync
  needs every leg connected.
- Expansion completes only once every diskful replica realizes the
  new size.
- Snapshots of volumes with a leg on the node retry until the write
  barrier can span all connected legs.
- Deleting such volumes blocks on the node's teardown finalizer.
