# Remote consumers and auto-diskful

A pod does not have to run on a node that holds a copy of its
volume. Replicated volumes are consumable from any node with a
MiroirNode by default (matching LINSTOR; see the trade-offs below for
the unmapped-node case): the PV carries no node affinity, so the
scheduler is free to place the pod anywhere. When the pod lands on a
node without a replica, it reads and writes the volume over the
replication network through an ephemeral **diskless client leg**, a
DRBD peer with no local storage. miroir adds that leg to the volume's
`spec.clients` when the pod's volume is mounted and removes it on
unmount, filling in the connection details (node id, address) exactly
as for an operator-added replica. A pod landing on the tie-breaker's
node needs no client leg; it uses the tie-breaker leg directly.

Set `allowRemoteVolumeAccess: false` on a `storageClasses` entry to
opt that class out: its PVs then pin pods to the diskful replica
nodes, guaranteeing local reads.

Trade-offs to understand:

- **Every remote read and write crosses the replication network.**
  Pin latency-sensitive workloads with
  `allowRemoteVolumeAccess: false` so a replica is always under the
  pod.
- **Replica nodes are only preferred at first use.** The first
  consumer's node is pinned as a replica when it is a storage node
  (capacity-ranked placement otherwise). After that there is no soft
  preference: PV node affinity is all-or-nothing, so the scheduler is
  blind to replica locations. Keep locality-sensitive workloads on a
  pinned class, or steer them with their own node/pod affinity.
- **An attached client does not vote in quorum.** Client legs are
  configured with DRBD's `tiebreaker no` (one of the reasons for the
  9.3.1 module floor), so attaching and detaching consumers never moves the
  majority threshold, and a dead consumer node leaves no phantom vote
  behind.
- **Trims from consumers reach the real backings.** A client leg's
  device advertises the diskful legs' probed discard granularity
  (DRBD's diskless default is a 512-byte fiction dm-thin would
  silently drop), so in-pod `fstrim` and `-o discard` free thin-pool
  space as if the pod ran on a replica node.
- **Consumers must run on nodes with a MiroirNode.** On an unmapped
  node the agent runs only a client-only CSI service (for RWX/NFS
  mounts) with no reconciler to realize a DRBD client leg, so staging
  refuses with a clear `FailedPrecondition` and the pod stays in
  `ContainerCreating` until it is rescheduled. Keep every schedulable
  node in the map (a `loopfile` entry with a few spare GB is enough)
  or set `allowRemoteVolumeAccess: "false"` so the PV's node affinity
  keeps pods on replica nodes in the first place.
- **A lost node can strand its client leg.** Its `spec.clients` entry
  blocks volume deletion until the node returns or the entry is
  removed by hand (it holds no quorum vote, so the volume itself
  stays healthy).

## Auto-diskful

Set `autoDiskfulAfter` (e.g. `"10m"`) to convert a client leg that
has stayed attached past the threshold into a diskful replica on its
node (LINSTOR's auto-diskful). The consumer evidently lives there, so
it gets a local replica and stops paying network I/O: the entry moves
from `spec.clients` to `spec.replicas`, and the node's agent attaches
a fresh backing device to the live volume and full-syncs it while the
pod keeps running. Conversion requires the client's node to be in the
topology (a MiroirNode) with recent capacity data and room for the volume's full
size, and the volume to be Ready; a 2+1 volume's
tie-breaker is replaced by the third data copy (three diskful votes
need no tie-breaker). Volumes already at 3 diskful replicas are left
alone; evicting a replica is an operator decision. Empty (the
default) disables it.

On a fully-mapped cluster (every node has a MiroirNode) the volume's
non-replica node is its tie-breaker, so a settled consumer stages
through that leg and no client leg ever exists. Auto-diskful covers
this too: a tie-breaker leg whose device has been held Primary past
the threshold (the agent stamps `primarySince` from the kernel role)
is flipped diskful **in place**: node id and address kept, a fresh
backing device attached to the live resource, full-synced under the
running pod.
