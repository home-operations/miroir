# miroir-config

![Version: 0.0.1](https://img.shields.io/badge/Version-0.0.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.1](https://img.shields.io/badge/AppVersion-0.0.1-informational?style=flat-square)

Storage configuration for the miroir CSI driver — node topology (MiroirNodes), StorageClasses, and VolumeSnapshotClasses as one reviewed document

**Homepage:** <https://github.com/home-operations/miroir>

## Usage

The storage configuration for the [miroir](https://miroir.home-operations.com/)
CSI driver as one reviewed document: the node topology (rendered as
MiroirNode custom resources), the StorageClasses, and the
VolumeSnapshotClasses. Install the miroir driver chart first (it carries
the CRDs); prefer plain manifests? Apply MiroirNode/StorageClass YAML
directly instead — this chart is convenience, not a requirement.

```sh
helm install miroir-config oci://ghcr.io/home-operations/charts/miroir-config \
  --namespace miroir-system -f config-values.yaml
```

Rendered MiroirNodes carry `helm.sh/resource-policy: keep`: uninstalling
this chart (or dropping an entry) never deletes the topology under live
volumes — decommissioning a node stays an explicit
`kubectl delete miroirnode <name>`.

## Storage topology

Each entry under `nodeGroups` is rendered as a MiroirNodeGroup custom
resource: one MiroirNode is materialized per label-matched node, so a
fleet sharing a storage layout is one entry and joining it is labeling
the node (per-node facts resolve from the Node object — zone from
`topology.kubernetes.io/zone`, a dedicated replication address from a
`miroir.home-operations.com/address` annotation). Members leaving the
selector are orphaned in place, never deleted.

Each entry under `nodes` is rendered as a MiroirNode custom resource;
the `spec` is passed through verbatim and validated by the CRD (see
`kubectl explain miroirnode.spec`). The backend is the block a pool
carries — exactly one of `lvmthin`/`zfs`/`loopfile`. For example, a ZFS
pool with its optional zvol settings (`volBlockSize` `4K` through
`128K`, default `4K`; `compression` default `lz4`, `inherit` for the
parent dataset policy):

```yaml
nodes:
  node-a:
    spec:
      pools:
        - name: default
          zfs:
            dataset: tank/miroir
            volBlockSize: 16K
            compression: inherit
```

Both settings apply only to newly created zvols. Existing volumes are not
mutated, and snapshot clones retain their source properties.

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| home-operations | <contact@home-operations.com> |  |

## Source Code

* <https://github.com/home-operations/miroir>

## Requirements

Kubernetes: `>=1.31.0`

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| commonLabels | object | `{}` | Labels stamped on every rendered object. |
| nodeGroups | object | `{}` |  |
| nodes | object | `{}` |  |
| storageClasses | list | `[]` | StorageClasses to create. Empty by default: declare the classes you want. One local + one replicated is the common pair (see the example below). Per entry:   name          (required) the StorageClass name   replicas      replica count, default 1; >1 makes it DRBD-replicated   quorum        freeze or last-man-standing (replicated only, default                 freeze). freeze never diverges but halts writes without a                 peer majority; last-man-standing keeps the survivor                 writable at the risk of split-brain. See the root README,                 "Replication and quorum".   fsType        ext4 or xfs, default ext4   pool          named storage pool the class provisions from, default                 "default". Every replica of a volume lands in this pool                 on its node, so the pool must exist (in the MiroirNode                 specs) on at least `replicas` nodes.   allowRemoteVolumeAccess                 true or false (replicated only; the controller defaults                 absent to true, matching LINSTOR): pods on nodes without                 a replica consume the volume through an ephemeral                 diskless DRBD leg at replication-network speed. Set                 false to pin pods to replica nodes for local reads. See                 the root README, "Remote consumers".   bitmapGranularity                 DRBD bitmap block size in bytes (replicated only): a                 power of two, 4096–1048576, default absent (DRBD's 4096).                 Each dirty bit tracks this many bytes — coarser cuts                 bitmap RAM proportionally (65536 ≈ 1/16th) but resyncs                 more per dirty bit; worth considering for classes holding                 large volumes. Fixed when a replica's metadata is                 created: changing the class affects new volumes only.   reclaimPolicy Delete or Retain, default Delete   isDefault     set the cluster default-class annotation, default false   volumeBindingMode                 WaitForFirstConsumer (default) delays provisioning until a                 pod schedules, so placement can prefer that pod's node;                 Immediate provisions on PVC creation — reasonable for a                 replicated class consumed remotely.   mountOptions  mount options for the class's PVs (list of strings)   annotations   extra annotations on the StorageClass   labels        extra labels on the StorageClass Example (coexisting with OpenEBS, which stays the cluster default):   storageClasses:     - name: miroir-local       replicas: 1     - name: miroir-replicated       replicas: 2       quorum: freeze |
| volumeSnapshotClasses | list | `[]` | VolumeSnapshotClasses to create (requires the snapshot-controller + CRDs, deployed separately). Empty by default. Per entry:   name           (required) the VolumeSnapshotClass name   deletionPolicy Delete or Retain, default Delete   isDefault      set the cluster default-snapshot-class annotation,                  default false   annotations    extra annotations on the VolumeSnapshotClass   labels         extra labels on the VolumeSnapshotClass Example:   volumeSnapshotClasses:     - name: miroir-snap       deletionPolicy: Delete |

---

_This README is generated by [helm-docs](https://github.com/norwoodj/helm-docs) from `Chart.yaml` and `values.yaml`. Edit those (or `README.md.gotmpl`) and run `mise run helm-docs`._
