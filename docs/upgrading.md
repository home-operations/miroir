# Upgrading

Version-to-version migration steps, newest first. Only releases that need you
to do something appear here; any version not listed upgrades by bumping the
chart.

miroir is pre-1.0, so breaking changes land in minor versions (0.9.0, 0.10.0,
…). Read the section for every version you are crossing, oldest
first. Upgrading 0.8.x → 0.10.x means doing 0.9.0 and then 0.10.0.

/// tip | Check before you upgrade

`helm template` (or a Flux dry-run) against your new values catches the
chart-side failures below without touching the cluster. None of these
migrations move data or need volume downtime.

///

## Every upgrade: keep the CRDs in step

The chart ships its CRDs in `crds/`, and Helm applies that directory **only on
install, never on upgrade**. An upgraded chart running against last release's
CRDs fails in a quiet way: the API server prunes spec fields the old schema
does not know, the apply succeeds, and the controller or agent then complains
about configuration you can plainly see in your values. So every upgrade
starts with the CRDs.

**Flux** can do it from the chart automatically, but not by default;
`upgrade.crds` must be set (`install.crds: Create` is already the default):

```yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
spec:
  install:
    crds: Create
  upgrade:
    crds: CreateReplace # default is Skip; required
```

**Plain Helm** has no automatic path; apply the new chart's CRDs before
upgrading:

```bash
helm show crds oci://ghcr.io/home-operations/charts/miroir \
  --version <new-version> | kubectl apply --server-side -f -
```

## 0.10.x → 0.11.0: node topology becomes MiroirNode CRs

The per-node storage topology moves out of the Helm chart entirely and
into `MiroirNode` custom resources, applied and managed separately
(kubectl, GitOps, any manifest tooling) — the chart installs only the
driver. The CRD schema is what validates the topology. Alongside the
move:

- Pool options are grouped under a per-backend block (`lvmthin`, `zfs`, or
  `loopfile`) instead of prefixed flat keys.
- The per-node setup Job is gone. The agent has always run the same pool
  provisioning at startup; a misconfigured pool now surfaces in the agent log
  and the MiroirNode status instead of a failed install hook.
- Canonical spellings are required: `volBlockSize` uppercase (`4K`, not
  `4k`), `compression` lowercase (`lz4`, not `LZ4`). Earlier releases folded
  case; the CRD validates instead.

**Existing volumes, their replicas, and their data are untouched.** This is a
migration of the configuration surface only; do not recreate any PVC,
MiroirVolume, or pool.

### 1. Update the CRDs

As above: `upgrade.crds: CreateReplace` for Flux, or `helm show crds ... |
kubectl apply --server-side -f -` for plain Helm.

/// warning | The CRD update must come first

Doing this first is what makes the rest of the upgrade safe: the new schema
refuses the partial topology writes 0.10 agents make while the rollout is in
flight (see step 3). Skip it and the old schema instead prunes the new pool
fields from your manifests. The prune is quiet, and the agents then fail
on configuration you can plainly see in your files.

///

### 2. Author and apply the MiroirNode manifests

Turn each entry of your old `nodes` values into a MiroirNode manifest —
one per storage node, named after it. `pools` becomes a list of named
entries and each pool's options move under its backend's block:

| 0.10.x values                   | MiroirNode manifest           |
| ------------------------------- | ----------------------------- |
| `nodes.<n>.zone`                | `spec.zone`                   |
| `nodes.<n>.address`             | `spec.address`                |
| `nodes.<n>.autoEvict`           | `spec.autoEvict`              |
| `nodes.<n>.pools.<p>` (map key) | `spec.pools[].name`           |
| `...pools.<p>.backend`          | `spec.pools[].backend`        |
| `...pools.<p>.device`           | `...pools[].lvmthin.device`   |
| `...pools.<p>.thinPoolSize`     | `...pools[].lvmthin.poolSize` |
| `...pools.<p>.zfsDataset`       | `...pools[].zfs.dataset`      |
| `...pools.<p>.zfsCompression`   | `...pools[].zfs.compression`  |
| `...pools.<p>.zfsVolBlockSize`  | `...pools[].zfs.volBlockSize` |
| `...pools.<p>.baseDir`          | `...pools[].loopfile.baseDir` |

The selected backend's block is required, even when it has nothing to say: an
lvmthin pool whose VG already exists still writes `lvmthin: {}`.

Before (values):

```yaml
nodes:
  k8s-0:
    zone: rack-1
    pools:
      default:
        backend: lvmthin
        device: /dev/disk/by-partlabel/r-miroir
        thinPoolSize: 400g
```

After (a manifest, applied like any other):

```yaml
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
  name: k8s-0
spec:
  zone: rack-1
  pools:
    - name: default
      backend: lvmthin
      lvmthin:
        device: /dev/disk/by-partlabel/r-miroir
        poolSize: 400g
```

Your cluster already has one `MiroirNode` per storage node (the 0.10
agents created them to publish pool capacity); applying your manifests
over them is expected and fine — the spec fields are yours, the status
stays the agents'. Apply them **before** the chart upgrade so the rolled
agents boot straight into storage mode.

Loopfile users: also set the chart's `agent.loopfileBaseDirs` to every
`loopfile.baseDir` in use — the agent pod's hostPath mounts are pod
spec, which the chart cannot derive from CRs it does not read.

### 3. Upgrade the chart and let the agents roll

Remove `nodes` from your values (the chart fails fast with a pointer at
this page if it is still set) and run the upgrade as usual. While the
agent DaemonSet rolls node by node, the not-yet-rolled 0.10 agents keep
trying to write their old, partial pool topology into the MiroirNode
spec; the new CRD schema rejects those writes. That is by design (it
stops an old agent from wiping the pool configuration you just
applied), and the visible cost is small: a node's pool-capacity
heartbeat pauses until its agent rolls, so `status.observedAt` on some
MiroirNodes goes stale for the duration of the rollout, and 0.10 agents
log rejected MiroirNode updates. Both clear on their own as the rollout
completes.

### 4. Verify

```bash
kubectl get miroirnodes
kubectl get miroirnode <node> -o yaml
```

`spec.pools` on every storage node should show the per-backend blocks from
your manifests (`lvmthin.device`, `zfs.dataset`, ...). If a block is missing,
the CRD update in step 1 was skipped and the old schema pruned it: apply the
CRDs and apply the manifests again.

### Also breaking, but unlikely to affect you

- The `--nodes-config` flag and `--mode=setup` are gone, along with the
  per-node setup Jobs and their ServiceAccount. Only custom `agent.extraArgs`
  or tooling that watched the setup Jobs would notice.
- Topology edits no longer roll the pods (the ConfigMap checksum annotation
  is gone). The controller follows MiroirNode changes live, and each agent
  restarts itself when its own pool spec changes. This is the new intended
  behavior, not a regression.
- Two MiroirNodes sharing a replication `address` no longer keep every
  component from starting. The conflict is reported as an `AddressConflict`
  condition (plus a Warning event) on the offending nodes, which are excluded
  from new placement until it is resolved.
- `helm uninstall` no longer destroys volume data by default. The pre-delete
  hook that deletes every MiroirVolume/MiroirSnapshot is now rendered only
  when `uninstall.confirmation` is set to `yes-really-destroy-data` — see
  [Uninstall](uninstall.md). If your teardown automation relied on the old
  behavior, set the confirmation.
- The controller pod now tolerates `node.kubernetes.io/unreachable` for 5
  seconds instead of Kubernetes' 300 default, so a dead node stops
  provisioning for seconds rather than five minutes
  (`unreachableNodeTolerationSeconds` restores the old value if you want it).
- The deprecated flat `MiroirNode.status.capacityBytes` /
  `status.allocatedBytes` / `status.metaUsedPercent` fields are gone from
  the schema, along with the controller's fold of them into the default
  pool. They existed for the 0.9→0.10 rollout skew; 0.10 agents already
  publish (and read) only `status.pools`. Anything scraping the flat paths
  directly has been broken since 0.10 — use `status.pools[*]`.

## 0.9.x → 0.10.0: named storage pools

Each node's storage config moves under a named pool. Your existing single pool
**must be adopted as the pool named `default`**: MiroirVolume replicas and
StorageClasses written before this release carry no pool reference, and they
all resolve to `default`.

### 1. Nest each node's storage under `pools.default`

`zone` and `address` stay node-level. Everything else moves: `backend`,
`device`, `zfsDataset`, `zfsVolBlockSize`, `zfsCompression`, `baseDir`,
and `thinPoolSize`.

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
directories are reused as they are, with **no data migration and no volume
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

- The agent/setup `--lvm-vg` and `--lvm-thinpool` flags are gone; VG naming
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

## 0.8.x → 0.9.0: RWX is opt-in

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

If you do not use RWX, no action is needed: the default is `false`, and the
gateway ServiceAccount, RBAC, PodMonitor, and export alert group are simply
not installed.

Should you upgrade without the flag and only then notice, setting
`gateway.enabled: true` and reconciling is enough: RWX rejection is
`FailedPrecondition`, so a pending PVC provisions on the external
provisioner's next retry. **No PVC recreation is needed.**

See [ReadWriteMany (RWX)](rwx.md) for what enabling it entails.
