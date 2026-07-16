# Disk failures, node rebuilds, and verification

A failing **disk** is not a failing node. Since v0.3 the global DRBD
config defaults to `on-io-error detach` (`drbd.onIoError`). A leg
whose backing device errors drops to Diskless, and the volume keeps
serving through the peer rather than surfacing I/O errors into the pod.
The detached leg shows as `DiskState: Diskless` in
`kubectl describe miroirvolume`, and `miroir_volume_disk_failed` goes
1 for that node. To recover: replace the disk, then remove and re-add
the replica.

**Rebuilding a node is safe.** A reinstall (e.g. a Talos wipe) destroys
the backing devices and miroir's node-local state together. When the
node rejoins, the agent detects the wipe and makes each recreated leg
a full sync target rather than trusting its empty disk.

Full syncs stay thin automatically. Writing every zero byte of unused
space would balloon a thin-provisioned pool to the volume's full
virtual size. Instead, the agent probes each lvmthin/zfs backing
device's discard granularity and configures DRBD to send runs of zeros
as discards, the same "these blocks are free" signal `fstrim` uses. A
re-synced leg then consumes only what the data needs. (Loopfile legs
are skipped, because loop devices mishandle discards;
`drbd.resync.discardGranularity` remains as a manual cluster-wide
fallback.)

**Dead nodes: auto-evict.** A node that dies permanently leaves every
volume it carried degraded until someone re-places its replicas.
Setting `autoEvictAfter` (a Helm value, e.g. `"60m"`, off by default)
automates that. Each node's heartbeat is the `MiroirNode` status its
agent refreshes about every minute. Once a node's heartbeat has been
stale that long, the controller swaps the dead entry out of each
affected volume in one atomic edit and adds a fresh replica, which
full-syncs from the survivors.

The dead node keeps its teardown finalizer on the volume. That
finalizer is the durable record that the node still holds a leg it
never got to clean up: if the node ever returns, its agent tears the
leftover leg down through the normal removal flow (safety-gated,
metadata wiped, backing device reclaimed) and releases the finalizer
itself. Until then, deleting an evicted volume waits for that node,
the same behavior as deleting any volume whose replica node is down.
If the node is gone for good, decommission it: remove it from the
`nodes` map and strip its `miroir.home-operations.com/teardown-<node>`
finalizers by hand, accepting that any leftover state on that hardware
is yours to erase.

Auto-evict is deliberately timid. It stands down in any of these cases:

- More than one node's heartbeat is stale. That pattern points at the
  network or API server, not at two simultaneous dead nodes.
- A surviving replica still sees the "dead" node's DRBD connections up.
  The node is then alive, and only its Kubernetes connection is broken.
- The surviving replicas are not all UpToDate.
- Snapshots pin the volume; a replacement replica would not carry them.

It also needs a spare storage node with the volume's pool and room for
the volume's full size; on a cluster with no spare node it does nothing.
A node with known long outages can opt out with
`nodes.<name>.autoEvict: false`. Keep the threshold well above your
longest planned reboot or upgrade window, since eviction discards the
dead node's copy of the data.

One scheduling limitation to know about: a PersistentVolume's node
affinity is fixed by Kubernetes at creation and cannot be updated, so
on volumes without `allowRemoteVolumeAccess` the pod can only ever
schedule onto the volume's _original_ replica nodes. After an eviction
the workload keeps running on the survivors, and the replacement
replica protects the data, but the scheduler cannot place the pod on
the replacement node. Remote-access volumes (the default for
replicated classes) carry no such pin and are unaffected.

**Verification** is the only cross-leg integrity check (a ZFS scrub
validates one leg against itself). `drbd.verify.algorithm` (default
`crc32c`) arms it. `drbd.verify.schedule` (a 5-field cron, e.g.
`"0 4 * * 0"`) then runs an online verify of every replicated volume on
that cadence, serialized per node, and skipping volumes that are
resyncing. Findings land in three places: the volume's status
(`lastVerifyOutOfSyncBytes`), the `miroir_volume_verify_*` metrics,
and a `VerifyOutOfSync` event. The
[starter alerts](monitoring.md) flag both findings and a schedule that
stopped firing. `drbdadm verify <resource>` on a storage node does the
same by hand.
