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
