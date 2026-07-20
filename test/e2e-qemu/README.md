# QEMU E2E Tests

End-to-end tests for the miroir CSI driver against a local Talos cluster, booted by
[talosctl-cluster-action][action] on top of `talosctl cluster create qemu`.

## Overview

Both CI legs boot the same `cluster.yaml` -- a controller node and two storage
workers, with the DRBD kernel module baked in by an Image Factory schematic -- and
drive the tests against it. The action boots the shape the document describes, exports
a kubeconfig and talosconfig, and destroys the cluster in its post step; `test.sh`
then installs the driver from a packaged Helm chart and runs the tests.

The two legs run the same document and differ in what `test.sh` drives against it, so
a failure names the path that broke:

| Leg           | Runs                                       | Class               |
| ------------- | ------------------------------------------ | ------------------- |
| `conformance` | miroir's Go specs, then the upstream suite | `miroir-local`      |
| `replicated`  | the upstream suite                         | `miroir-replicated` |

miroir's Go specs (`test/e2e`) assert the local lifecycle, snapshot/restore, block and
placement behaviour the upstream suite does not; the conformance leg runs them
(`RUN_SPECS=1`) against the miroir-local (lvmthin, replicas: 1) class before the long
external-storage run. The replicated leg drives that upstream suite against the DRBD
(replicas: 2) class, group snapshots included. Real per-node kernels and real block
devices are the point: the DRBD path needs the DRBD 9 module, which only a real Talos
node has.

```text
cluster.yaml              the one cluster shape both legs boot
schematic.yaml            the drbd system extension (referenced by cluster.yaml)
patches/modules.yaml      shared: the DRBD / dm-thin kernel modules
patches/registry.yaml     shared: where the nodes pull the miroir images
classes.yaml              the StorageClasses / snapshot classes under test
common.sh                 shared configuration for the scripts
image.sh                  builds and pushes the controller + agent images
test.sh                   installs the driver and runs the suite
diagnostics.sh            dumps cluster state on failure
```

## Requirements

- KVM (`/dev/kvm`) and `qemu-system-x86_64`
- Passwordless `sudo`, for the bridge and NAT the provisioner sets up
- `talosctl` v1.13 or newer, which mise pins
- Docker, to build and push the images (skipped if you set `CONTROLLER_IMAGE` and
  `AGENT_IMAGE`)

## Running it locally

`test.sh` runs against a cluster that already exists; the action is only how CI gets
one. Boot the shape `cluster.yaml` describes with `talosctl` directly, then point the
script at it:

```fish
# The schematic id is a content hash, so re-POSTing it is idempotent.
set -gx SCHEMATIC_ID (curl -sfX POST \
    --data-binary @schematic.yaml https://factory.talos.dev/schematics | jq -r .id)

sudo -E (mise which talosctl) cluster create qemu \
    --name miroir-e2e \
    --cidr 10.5.0.0/24 \
    --schematic-id "$SCHEMATIC_ID" \
    --controlplanes 1 --workers 2 --memory-workers 5GiB \
    --disks virtio:8GiB,virtio:20GiB \
    --config-patch @patches/modules.yaml \
    --config-patch @patches/registry.yaml \
    --talosconfig-destination /tmp/miroir-e2e/talosconfig
sudo chown -R (id -u):(id -g) /tmp/miroir-e2e

talosctl kubeconfig /tmp/miroir-e2e/kubeconfig --nodes 10.5.0.2 --force

set -gx CLUSTER_NAME miroir-e2e
set -gx KUBECONFIG /tmp/miroir-e2e/kubeconfig
set -gx TALOSCONFIG /tmp/miroir-e2e/talosconfig
set -gx TESTDRIVER testdriver.yaml # replicated leg; or testdriver-local.yaml
set -gx RUN_SPECS 1                 # also run the Go specs (the conformance leg does)
./test.sh

sudo -E (mise which talosctl) cluster destroy --name miroir-e2e -f
```

That is `cluster.yaml` spelled out as flags, minus the ephemeral profile the action
applies for you (see below); the document stays the source of truth for the shape.

`test.sh` refuses to run unless `CLUSTER_NAME` looks like an e2e cluster and both the
kubectl context and the node names agree with it, so it cannot be pointed at a real
cluster by accident.

When `CONTROLLER_IMAGE` and `AGENT_IMAGE` are unset, `test.sh` builds and pushes both
itself via `image.sh`, which uses ttl.sh so a local run needs no registry of its own:

```fish
set -gx CONTROLLER_IMAGE ghcr.io/you/miroir-controller:dev
set -gx AGENT_IMAGE ghcr.io/you/miroir-agent:dev
```

CI instead fixes the tags up front and builds them concurrently with the cluster,
since the build and the VMs share no inputs; `test.sh` then finds the images already
built and skips straight to installing them. CI builds through
`docker/build-push-action` rather than `image.sh` so the layers land in the GitHub
Actions cache, which `image.sh` has no way to reach.

CI also runs its own registry on the runner and pushes there, so the images never
cross the internet. The nodes reach them as `registry.e2e`, which the mirror in
`patches/registry.yaml` points at port 5000 on the QEMU bridge gateway. That mirror
entry is inert for a local run, where nothing references `registry.e2e`.

## How the chart is installed

`test.sh` always packages `charts/miroir` with `helm package`, so the run installs a
real chart artifact rather than the working tree. Where it installs it from depends on
`CHART_REGISTRY`:

- **CI** sets it to the runner-local registry (`oci://localhost:5000/charts`). The
  chart is pushed there and installed straight back over OCI, exercising the same
  push/pull path a release uses. It is the same registry the images go to; helm talks
  to it as plain HTTP because it is on loopback.
- **A local run** leaves it unset and installs the packaged `.tgz` directly, which
  still tests the artifact without needing a registry of your own.

The image overrides (`image.*`, `agent.image.*`) and `groupSnapshots.enabled` are
passed as `--set` either way, so they win over the chart's defaults.

The install is CR-first: the CRDs, the StorageClasses, and a labelled MiroirNodeGroup
pointing the lvmthin pool at the workers' `/dev/vdb` all land before the driver, so
the agent boots straight into storage mode. The namespace is labelled
`pod-security.kubernetes.io/enforce=privileged` first, because Talos enforces the
baseline standard and would otherwise reject the privileged agent DaemonSet.

## What the action's profile supplies

The document is small because the action's default `ephemeral` profile already applies
what a throwaway cluster wants, ahead of the patches here: the kernel args that turn
off the dashboard and auditd and side-channel mitigations, kubelet's image GC and
eviction thresholds (so a full disk reads as itself rather than as a flaky test), etcd
`unsafe-no-fsync`, and an apiserver audit policy of `None`. miroir used to patch every
one of these in by hand; they are the profile's job now. Every run logs what it
applied.

Because a schematic is in play, the profile also pins `.machine.install.image` to the
matching Factory installer, so the drbd extension would survive a `talosctl upgrade`.
The e2e never upgrades, so this is inert here, but it is the correct default and costs
nothing.

What is left in `patches/` is only what the profile has no opinion about: the DRBD and
dm-thin kernel modules the agent needs, and the `registry.e2e` mirror that feeds the
nodes their images.

## Configuration

The Talos and Kubernetes versions the cluster boots on are talosctl's own defaults,
which mise pins through the `talos` tool. Everything else about the shape -- node
counts, the worker memory ceiling that bounds how many VMs fit on a runner, the disks
-- lives in `cluster.yaml`.

Keep `metadata.name` short: it appears twice in the QEMU monitor socket path, and QEMU
refuses to start when a UNIX socket path exceeds 108 bytes. The action checks this
before it provisions anything.

[action]: https://github.com/home-operations/talosctl-cluster-action
