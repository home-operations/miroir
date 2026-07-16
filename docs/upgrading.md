# Upgrading

Version-to-version migration steps, newest first. Only releases that need you
to do something appear here; any version not listed upgrades by bumping the
chart.

miroir is pre-1.0, so breaking changes land in minor versions (0.9.0, 0.10.0,
…). Read the section for every version you are crossing, oldest first —
upgrading 0.8.x → 0.10.x means doing 0.9.0 and then 0.10.0.

/// tip | Check before you upgrade

`helm template` (or a Flux dry-run) against your new values catches the
chart-side failures below without touching the cluster. Both migrations here
are Helm values edits — neither moves data nor needs volume downtime.

///

## 0.9.x → 0.10.0 — named storage pools

Each node's storage config moves under a named pool. Your existing single pool
**must be adopted as the pool named `default`**: MiroirVolume replicas and
StorageClasses written before this release carry no pool reference, and they
all resolve to `default`.

### 1. Nest each node's storage under `pools.default`

`zone` and `address` stay node-level. Everything else — `backend`, `device`,
`zfsDataset`, `zfsVolBlockSize`, `zfsCompression`, `baseDir`, `thinPoolSize` —
moves:

Before:

```yaml
nodes:
  k8s-0:
    zone: rack-1
    backend: lvmthin
    device: /dev/disk/by-partlabel/r-miroir
```

After:

```yaml
nodes:
  k8s-0:
    zone: rack-1
    pools:
      default:
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
```

The chart fails fast on the flat shape, so a missed node is a template error
rather than a broken cluster. Under Flux the HelmRelease reports failed and
nothing is applied.

### 2. Upgrade

```bash
flux reconcile helmrelease miroir -n miroir-system --with-source
```

Existing volumes, snapshots, VGs (`vg-miroir`), datasets, and loopfile
directories are reused as they are — **no data migration and no volume
downtime**. During the rollout an old agent and a new controller can briefly
disagree on the `MiroirNode` status shape; placement treats those stats as
unknown (the same as a cold cluster) until the DaemonSet finishes rolling.

### 3. Optional: add a second pool

```yaml
nodes:
  k8s-0:
    pools:
      default:
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
      fast:
        backend: lvmthin
        device: /dev/disk/by-id/nvme-Micron_7450_MTFDKBA800TFS_XXXX
  # k8s-1, k8s-2 identical
storageClasses:
  - name: miroir-replicated
    replicas: 2
  - name: miroir-replicated-fast
    replicas: 3
    pool: fast
```

Each agent creates the new pool's VG and thin-pool at startup. New pools get
`vg-miroir-<pool>`; the default pool keeps `vg-miroir`, which is why step 2
needs no data migration.

### Also breaking, but unlikely to affect you

- The agent/setup `--lvm-vg` and `--lvm-thinpool` flags are gone — VG naming
  derives from the pool name. The chart never set them; only custom
  `agent.extraArgs` would notice.
- `MiroirNode.spec.backend` and the flat `status.capacityBytes`,
  `status.allocatedBytes`, `status.metaUsedPercent` fields are replaced by
  per-pool lists. Anything scraping those CR fields directly needs the new
  paths.
- Pool metrics gain a `pool` label. The chart's own alerts and dashboard are
  updated; your own recording rules or dashboards matching an exact label set
  are not.
- Do not remove a pool from `nodes` while volumes still reference it. Their
  reconciles and deletions fail loudly (`storage pool "x" is not configured on
  this node`) until the pool returns or the volumes are gone.

## 0.8.x → 0.9.0 — RWX is opt-in

Serving ReadWriteMany is now an explicit operator decision. Gateway pods run
privileged in the release namespace, and anyone who can create a PVC could
previously cause one to be spawned just by requesting RWX.

/// warning | Set this before upgrading, not after

**If you serve any RWX volumes, set `gateway.enabled: true` in your Helm
values before you upgrade.**

```yaml
gateway:
  enabled: true
```

Without it, running gateway pods keep serving until their next restart, but
the controller stops reconciling them, the gateway RBAC is removed (so a
restarted gateway cannot read its volume), and new RWX PVCs are rejected.

///

If you do not use RWX, no action is needed — the default is `false`, and the
gateway ServiceAccount, RBAC, PodMonitor, and export alert group are simply
not installed.

Should you upgrade without the flag and only then notice, setting
`gateway.enabled: true` and reconciling is enough: RWX rejection is
`FailedPrecondition`, so a pending PVC provisions on the external
provisioner's next retry. **No PVC recreation is needed.**

See [ReadWriteMany (RWX)](rwx.md) for what enabling it entails.
