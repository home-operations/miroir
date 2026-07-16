# Requirements

- Kubernetes ≥ 1.31.
- A `kubelet` directory the agent can `hostPath`-mount (default
  `/var/lib/kubelet`; override `agent.kubeletDir` in Helm values).
- Kernel modules on each storage node (per-OS notes below):
  `dm_thin_pool` (lvmthin), ZFS (zfs), `loop` plus a reflink-capable
  filesystem for `baseDir` (loopfile; reflinks are copy-on-write
  file clones: XFS with `reflink=1` or btrfs), and, for replication, the
  DRBD9 `drbd` and `drbd_transport_tcp` modules, **version ≥ 9.3.1**
  (on Talos: shipped by ≥ 1.13.0). The agent refuses to start on a
  node whose module is older: the drbd-utils in the agent image
  render options an older module rejects. Nodes without the module at
  all are fine (local-only).
- Kubelet [graceful node shutdown][gns], so the agent (a
  `system-node-critical` pod) is stopped _after_ workloads on reboot
  and can release DRBD backings before the backend pool exports.

/// warning | Graceful node shutdown is not optional for replication

Skip it and a reboot can stop the agent before or alongside the
workloads instead of after them. The agent then cannot release its DRBD
backings before the backend pool exports, and unmounting the pool can
wedge. The per-OS sections below show how to enable it.

///

All storage userland (`drbdadm`, `lvm`, `zfs`, `mkfs`, `mount.nfs`)
ships inside the agent image; nodes only provide kernel modules.

[gns]: https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/#graceful-node-shutdown

## Talos

Talos is the primary target. For replication use **≥ 1.13.0** (its
`siderolabs/drbd` extension ships the DRBD 9.3.1 module the agent
requires). The stock kernel ships `dm_thin_pool`
and `loop` (the agent loads them on demand), and `/var` is XFS with
`reflink=1`, so `lvmthin` and `loopfile` work out of the box. DRBD
and ZFS modules come from [Image Factory][factory] system extensions;
build the install image with:

- `siderolabs/drbd` for replication
- `siderolabs/zfs` for the zfs backend

Load DRBD with its usermode helper disabled: the kernel side
otherwise calls out to `/sbin/drbdadm` on the host, which does not
exist on Talos. Graceful node shutdown is on by default; size the
critical-pod window to cover the agent's
`terminationGracePeriodSeconds` (default 60s):

```yaml
machine:
    kernel:
        modules:
            - name: drbd
              parameters:
                  - usermode_helper=disabled
            - name: drbd_transport_tcp
    kubelet:
        extraConfig:
            shutdownGracePeriod: 120s
            shutdownGracePeriodCriticalPods: 60s
```

[factory]: https://factory.talos.dev

## Debian/Ubuntu

Stock kernels ship `dm_thin_pool` and `loop`, so `lvmthin` and
`loopfile` need nothing extra. For the rest:

- **DRBD9**: the in-tree `drbd.ko` is the 8.4 API and does **not**
  work with miroir's DRBD9 configuration. Install `drbd-dkms` and
  kernel headers from the [LINBIT PPA][ppa] (Ubuntu) or LINBIT's
  Debian repositories. Unless the host has a matching `drbd-utils`
  installed, disable the usermode helper for the same reason as on
  Talos: `options drbd usermode_helper=disabled` in
  `/etc/modprobe.d/drbd.conf`.
- **zfs backend**: Ubuntu kernels ship the ZFS module
  (`linux-modules-extra`); on Debian install `zfs-dkms` (contrib).
  The agent's ZFS userland is 2.3, and userland older than the
  node's module is the supported direction.
- Enable kubelet graceful node shutdown (`shutdownGracePeriod` /
  `shutdownGracePeriodCriticalPods` in the kubelet config) with a
  critical-pod window ≥ the agent's grace period (default 60s).

[ppa]: https://launchpad.net/~linbit/+archive/ubuntu/linbit-drbd9-stack

Nothing in the controller or agent is otherwise OS-specific.
