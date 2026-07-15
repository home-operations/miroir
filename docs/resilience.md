# Disk failures, node rebuilds, and verification

A failing **disk** is not a failing node. Since v0.3 the global DRBD
config defaults to `on-io-error detach` (`drbd.onIoError`): a leg
whose backing device errors drops to Diskless and the volume keeps
serving through the peer instead of surfacing I/O errors into the pod. The
detached leg shows as `DiskState: Diskless` in
`kubectl describe miroirvolume` and `miroir_volume_disk_failed` goes
1 for that node: replace the disk, then remove and re-add the
replica.

**Rebuilding a node is safe.** A reinstall (e.g. Talos wipe) destroys
the backing devices and miroir's node-local state together; when the
node rejoins, the agent detects the wipe and makes each recreated leg
a full sync target rather than trusting its empty disk. Full syncs
stay thin automatically: instead of literally writing every zero
byte of unused space (which would balloon a thin-provisioned pool to
the volume's full virtual size), the agent probes each lvmthin/zfs
backing device's discard granularity and configures DRBD to send
runs of zeros as discards — the same "these blocks are free" signal
`fstrim` uses — so a re-synced leg consumes only what the data needs
(loopfile legs are skipped, loop devices mishandle it;
`drbd.resync.discardGranularity` remains as a manual cluster-wide
fallback).

**Dead nodes: auto-evict.** A node that dies permanently leaves every
volume it carried degraded until someone re-places its replicas.
Setting `autoEvictAfter` (Helm value; e.g. `"60m"`, off by default)
automates that: once the node's heartbeat — the `MiroirNode` status its
agent refreshes about every minute — has been stale that long, the
controller swaps the dead entry out of each affected volume in one
atomic edit and adds a fresh replica, which full-syncs from the
survivors. The dead node's teardown finalizer is force-released (its
agent cannot run), and the volume's status records the eviction; if the
node ever returns, its agent uses that record to clean up the abandoned
backing device and DRBD metadata instead of leaking them.

Auto-evict is deliberately timid. It stands down when more than one
node's heartbeat is stale (that pattern points at the network or API
server, not at two simultaneous dead nodes), when any surviving replica
still sees the "dead" node's DRBD connections up (then the node is
alive and only its Kubernetes connection is broken), when the surviving
replicas are not all UpToDate, or when snapshots pin the volume (a
replacement replica would not carry them). It needs a spare storage
node with the volume's pool and room for the volume's full size — on a
cluster with no spare node it does nothing. A node with known long
outages can opt out with `nodes.<name>.autoEvict: false`. Keep the
threshold well above your longest planned reboot or upgrade window:
eviction discards the dead node's copy of the data.

**Verification** is the only cross-leg integrity check (a ZFS scrub
validates one leg against itself). `drbd.verify.algorithm` (default
`crc32c`) arms it, and `drbd.verify.schedule` (5-field cron, e.g.
`"0 4 * * 0"`) runs an online verify of every replicated volume on
that cadence, serialized per node and skipping volumes that are
resyncing. Findings land in the volume's status
(`lastVerifyOutOfSyncBytes`), the `miroir_volume_verify_*` metrics,
and a `VerifyOutOfSync` event; the
[starter alerts](monitoring.md) flag both findings
and a schedule that stopped firing. `drbdadm verify <resource>` on a
storage node does the same by hand.
