# Replication and quorum

Replicated volumes are 2-way synchronous (DRBD "protocol C"): a
write completes only once both legs have it on disk, so the copies
are identical at every instant, not eventually.

The hard problem in any replicated system is not a dead node; it is
a **partition**: both nodes are alive but can't see each other. If
both keep accepting writes, the copies diverge
([split-brain](index.md#terminology)) and one side's writes must
eventually be thrown away. The **quorum policy** is the per-class
answer to that situation, a trade between never diverging and
staying writable. It is set per StorageClass via the `quorum` field
on a `storageClasses` entry with `replicas > 1` (the
`miroir.home-operations.com/quorum` parameter).

## `freeze` (default)

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

## The diskless tie-breaker

When a 2-replica `freeze` volume is created and a third storage node
in the `nodes` map holds neither leg, the controller adds that node
as a diskless tie-breaker: a DRBD peer configured with `disk none` that
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
  [auto-diskful](remote-consumers.md#auto-diskful) converts a
  tie-breaker).

## `last-man-standing`

`quorum off`: the surviving replica keeps accepting writes even with
no peers in sight. Maximum availability, but if both sides run while
partitioned they diverge. DRBD detects the split-brain on reconnect
and deliberately stays disconnected (`after-sb-* disconnect`, never
auto-resolve); an operator inspects both legs and picks the loser.
Volumes with this policy never get a tie-breaker.

## At a glance

| Failure                      | `freeze` + tie-breaker                                     | `freeze`, 2 nodes           | `last-man-standing`                                |
| ---------------------------- | ---------------------------------------------------------- | --------------------------- | -------------------------------------------------- |
| One node down                | keeps serving (2/3 votes)                                  | I/O errors until it returns | survivor keeps writing                             |
| Replication link partitioned | the majority side serves; the minority side refuses writes | both sides refuse writes    | both sides may write → split-brain, manual resolve |
| Two nodes down               | remaining node refuses writes                              | I/O errors                  | survivor keeps writing                             |

The default was `last-man-standing` before v0.3. Volumes keep
whatever `quorumPolicy` is stored in their spec; nothing is rewritten
on upgrade, and the new default only applies to volumes created after
it.

Volumes are also consumable from nodes that hold no replica at all;
see [Remote consumers and auto-diskful](remote-consumers.md). For
what happens when a disk (rather than a node) fails, and how legs are
verified against each other, see
[Disk failures, rebuilds, and verification](resilience.md).
