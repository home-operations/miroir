#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

# Refuse to touch anything that is not our throwaway cluster. Talos derives the
# kubectl context and every node name from the cluster name, so all three have to
# agree with it before we provision or install anything.
guard_cluster() {
    [[ "$CLUSTER_NAME" == miroir-e2e* ]] ||
        die "cluster name '$CLUSTER_NAME' is not a miroir-e2e cluster; refusing to proceed"

    local ctx
    ctx=$(kubectl config current-context)
    [[ "$ctx" == "admin@${CLUSTER_NAME}" ]] ||
        die "kubectl context '$ctx' is not 'admin@${CLUSTER_NAME}'; refusing to proceed"

    local node
    node=$(kubectl get nodes -o json | jq -r '.items[0].metadata.name')
    [[ "$node" == "$CLUSTER_NAME"* ]] ||
        die "node '$node' does not start with '$CLUSTER_NAME'; refusing to proceed"
}

# CI sets CONTROLLER_IMAGE and AGENT_IMAGE and builds them concurrently with the
# cluster, so by the time we get here they are already in the registry. A local run
# leaves them unset; build both to ttl.sh so it needs no registry of its own.
build_images_if_needed() {
    if [[ -n "${CONTROLLER_IMAGE:-}" && -n "${AGENT_IMAGE:-}" ]]; then
        log "Using pre-built images: $CONTROLLER_IMAGE, $AGENT_IMAGE"
        return
    fi
    local stamp
    stamp=$(date +%s)
    CONTROLLER_IMAGE="ttl.sh/miroir-controller-e2e-${stamp}:2h"
    AGENT_IMAGE="ttl.sh/miroir-agent-e2e-${stamp}:2h"
    export CONTROLLER_IMAGE AGENT_IMAGE
    "${SCRIPT_DIR}/image.sh"
}

install_snapshot_stack() {
    log "Installing the CSI snapshot stack ($SNAPSHOTTER_VERSION, group snapshots on)..."
    kubectl apply -k "https://github.com/kubernetes-csi/external-snapshotter//client/config/crd?ref=$SNAPSHOTTER_VERSION"
    kubectl wait --for=condition=established --timeout=60s \
        crd/volumesnapshotclasses.snapshot.storage.k8s.io \
        crd/volumesnapshotcontents.snapshot.storage.k8s.io \
        crd/volumesnapshots.snapshot.storage.k8s.io \
        crd/volumegroupsnapshotclasses.groupsnapshot.storage.k8s.io \
        crd/volumegroupsnapshotcontents.groupsnapshot.storage.k8s.io \
        crd/volumegroupsnapshots.groupsnapshot.storage.k8s.io
    kubectl apply -k "https://github.com/kubernetes-csi/external-snapshotter//deploy/kubernetes/snapshot-controller?ref=$SNAPSHOTTER_VERSION"
    kubectl -n kube-system patch deploy snapshot-controller --type=json \
        -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--feature-gates=CSIVolumeGroupSnapshot=true"}]'
    kubectl -n kube-system rollout status deploy/snapshot-controller --timeout=150s
}

# Talos enforces the baseline Pod Security Standard, which rejects privileged pods;
# the namespace must carry the privileged label before the zpool pods or helm create
# any.
prepare_namespace() {
    log "Preparing the $NAMESPACE namespace..."
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    kubectl label namespace "$NAMESPACE" pod-security.kubernetes.io/enforce=privileged --overwrite
}

# The agent creates the parent dataset itself but a zpool is deliberately the
# operator's (docs/quickstart.md), so the harness plays operator: one privileged pod
# per worker runs zpool create on /dev/vdc, the third virtio disk cluster.yaml
# attaches to workers. The agent image ships the zfs userland and the host carries
# the module, exactly like the agent's own execution environment, and the mounts
# mirror the agent's: /run/udev because zpool create partitions a whole disk and
# then waits on udev for the partition nodes.
#
# The same pod pins the ARC to 512MiB, which would otherwise grow toward most of a
# worker's 5GiB the parallel suite needs for pods. Through sysfs at runtime because
# nothing earlier can deliver the parameter: a zfs.zfs_arc_max kernel arg needs
# modprobe (Talos loads modules through kmod directly), and a machine-config module
# parameter races the zfs extension's own service for who loads the module.
create_zpools() {
    log "Creating the zfs pool on each worker..."
    for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}'); do
        kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: zpool-create-${node}
  namespace: ${NAMESPACE}
  labels:
    app: zpool-create
spec:
  nodeName: ${node}
  restartPolicy: Never
  containers:
    - name: zpool-create
      image: ${AGENT_IMAGE}
      command:
        - /bin/sh
        - -ec
        - |
          echo 536870912 > /sys/module/zfs/parameters/zfs_arc_max
          zpool list tank >/dev/null 2>&1 || zpool create -f tank /dev/vdc
      securityContext:
        privileged: true
      volumeMounts:
        - name: dev
          mountPath: /dev
        - name: run-udev
          mountPath: /run/udev
  volumes:
    - name: dev
      hostPath:
        path: /dev
    - name: run-udev
      hostPath:
        path: /run/udev
EOF
    done
    # Poll rather than kubectl wait: wait --for can watch one phase only, and a
    # Failed pod should fail the run now (with its logs), not after the full
    # timeout.
    local end=$((SECONDS + 300)) phases
    while :; do
        phases=$(kubectl -n "$NAMESPACE" get pods -l app=zpool-create \
            -o jsonpath='{.items[*].status.phase}')
        if [[ "$phases" == *Failed* ]]; then
            kubectl -n "$NAMESPACE" logs -l app=zpool-create --prefix --tail=5 || true
            die "zpool creation failed"
        fi
        [[ -n "$phases" && "$phases" != *Pending* && "$phases" != *Running* ]] && break
        ((SECONDS < end)) || die "timed out waiting for the zpool pods"
        sleep 2
    done
    kubectl -n "$NAMESPACE" delete pod -l app=zpool-create --wait=false
}

# Config is CR-first, the same order the upgrade guide mandates: CRDs, then the
# classes and the topology, then the driver -- so the agent boots straight into
# storage mode instead of client-only plus a self-restart. Both workers carry the
# storage class label; /dev/vdb is the second virtio disk cluster.yaml attaches to
# workers, tank the zpool create_zpools made on the third, and the labelled
# MiroirNodeGroup is the group controller's coverage: label -> group ->
# materialized MiroirNode -> agent.
apply_storage_config() {
    log "Applying CRDs, classes, and the node topology..."
    kubectl apply -f "${REPO_ROOT}/charts/miroir/crds/"
    kubectl wait --for=condition=established --timeout=60s \
        crd/miroirnodes.miroir.home-operations.com \
        crd/miroirnodegroups.miroir.home-operations.com

    for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o name); do
        kubectl label "$node" "${STORAGE_CLASS_LABEL}=e2e" --overwrite
    done

    kubectl apply -f "${SCRIPT_DIR}/classes.yaml"
    kubectl apply -f - <<EOF
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNodeGroup
metadata:
  name: e2e
spec:
  nodeSelector:
    matchLabels:
      ${STORAGE_CLASS_LABEL}: e2e
  template:
    pools:
      - name: default
        lvmthin:
          device: /dev/vdb
      - name: zfs
        zfs:
          dataset: tank/miroir
EOF
}

# Install the chart the way it is actually distributed: packaged, and -- in CI --
# pushed to an OCI registry and pulled back, rather than from the working tree.
install_chart() {
    local controller_repo controller_tag agent_repo agent_tag
    IFS=':' read -r controller_repo controller_tag <<<"$CONTROLLER_IMAGE"
    IFS=':' read -r agent_repo agent_tag <<<"$AGENT_IMAGE"

    log "Packaging the chart..."
    local chart_dir chart_ref
    chart_dir=$(mktemp -d)
    helm package "${REPO_ROOT}/charts/miroir" \
        --version "$CHART_VERSION" --app-version "$CHART_VERSION" --destination "$chart_dir"
    local chart_tgz="${chart_dir}/miroir-${CHART_VERSION}.tgz"

    # CI points CHART_REGISTRY at the runner-local registry, so the run exercises the
    # same push/pull an OCI release uses; a local run has none and installs the
    # packaged tgz directly, which still tests the artifact rather than the tree.
    local helm_oci_args=()
    if [[ -n "${CHART_REGISTRY:-}" ]]; then
        # --plain-http only for the loopback CI registry, which serves HTTP.
        case "$CHART_REGISTRY" in
            *localhost* | *127.0.0.1*) helm_oci_args=(--plain-http) ;;
        esac
        log "Pushing the chart to ${CHART_REGISTRY} and installing from there..."
        helm push "$chart_tgz" "$CHART_REGISTRY" "${helm_oci_args[@]}"
        chart_ref="${CHART_REGISTRY}/miroir"
        helm_oci_args+=(--version "$CHART_VERSION")
    else
        log "No CHART_REGISTRY set; installing the packaged chart directly."
        chart_ref="$chart_tgz"
    fi

    log "Installing miroir via Helm..."
    helm upgrade --install miroir "$chart_ref" "${helm_oci_args[@]}" \
        --namespace "$NAMESPACE" \
        --set image.repository="$controller_repo" \
        --set image.tag="$controller_tag" \
        --set image.pullPolicy=Always \
        --set agent.image.repository="$agent_repo" \
        --set agent.image.tag="$agent_tag" \
        --set agent.image.pullPolicy=Always \
        --set groupSnapshots.enabled=true \
        --wait --timeout 10m
}

main() {
    # talosctl-cluster-action exports both configs and reports the cluster name; a
    # local run points them at whatever cluster it booted itself.
    : "${KUBECONFIG:?must point at the e2e cluster kubeconfig}"
    : "${TALOSCONFIG:?must point at the e2e cluster talosconfig}"
    : "${CLUSTER_NAME:?must be metadata.name from cluster.yaml}"

    # Which storage class the upstream suite drives. testdriver.yaml is the replicated
    # (DRBD over lvmthin) class; testdriver-local.yaml the single-node lvmthin one;
    # testdriver-zfs.yaml the replicated zfs one. run.sh reads SKIP / PROCS / VERBOSE
    # / FOCUS from the environment, so the workflow sets those.
    export TESTDRIVER="${TESTDRIVER:-testdriver.yaml}"

    # Where the Talos API calls go. CI passes the action's endpoint output; otherwise
    # the talosconfig names it.
    if [[ -z "${ENDPOINT:-}" ]]; then
        ENDPOINT=$(talosctl config info -o json | jq -r '.endpoints[0] // empty')
    fi
    : "${ENDPOINT:?no control plane address in $TALOSCONFIG}"

    guard_cluster
    log "Connected to cluster: $CLUSTER_NAME (endpoint $ENDPOINT, testdriver $TESTDRIVER)"

    build_images_if_needed

    log "Waiting for the cluster to come up..."
    talosctl --nodes "$ENDPOINT" health --wait-timeout=10m
    kubectl wait --for=condition=Ready node --all --timeout=5m

    install_snapshot_stack
    prepare_namespace
    create_zpools
    apply_storage_config
    install_chart

    # miroir's own Go specs assert the local (lvmthin, replicas:1) lifecycle,
    # snapshot/restore, block and placement behaviour the upstream suite does not.
    # The conformance leg sets RUN_SPECS to run them against miroir-local before the
    # long external-storage run, so a miroir-specific break fails fast.
    if [[ "${RUN_SPECS:-}" == "1" ]]; then
        log "Running miroir's Go e2e specs (miroir-local)..."
        (cd "$REPO_ROOT" && go test -tags e2e ./test/e2e/ -v -ginkgo.v -timeout 20m)
    fi

    log "Running the external-storage conformance suite ($TESTDRIVER)..."
    "${REPO_ROOT}/test/conformance/run.sh"

    log "=========================================="
    log "E2E QEMU SUITE PASSED ($TESTDRIVER)"
    log "=========================================="
}

main "$@"
