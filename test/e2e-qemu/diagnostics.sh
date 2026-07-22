#!/usr/bin/env bash
# Dump cluster state for debugging a failed QEMU e2e run. Invoked by the workflow's
# failure step and safe to run by hand; it never fails the job itself. Needs the
# KUBECONFIG and TALOSCONFIG the cluster action exports.
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

workers=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
    -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{" "}{end}')

echo "---- miroir CRs ----"
kubectl get miroirvolumes,miroirsnapshots,miroirsnapshotgroups -o wide
echo "---- workloads + storage ----"
kubectl get pods,pvc,pv,volumesnapshot,volumegroupsnapshot -A -o wide
echo "---- Warning events ----"
kubectl get events -A --field-selector type=Warning --sort-by=.lastTimestamp
echo "---- controller logs ----"
kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/component=controller --tail=200 --all-containers
echo "---- agent logs ----"
kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/component=agent --tail=200 --all-containers
echo "---- gateway logs ----"
kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/component=gateway --tail=200 --all-containers --prefix
kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/component=gateway --tail=200 --all-containers --prefix --previous
echo "---- drbd state ----"
for n in $workers; do
    echo "== $n =="
    talosctl -n "$n" read /proc/drbd
done
echo "---- zfs state ----"
for pod in $(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=agent \
    -o jsonpath='{.items[*].metadata.name}'); do
    echo "== $pod =="
    kubectl exec -n "$NAMESPACE" "$pod" -c agent -- zpool status 2>&1
    kubectl exec -n "$NAMESPACE" "$pod" -c agent -- zfs list -t all 2>&1
done
echo "---- worker dmesg (tail) ----"
talosctl -n "${workers// /,}" dmesg | tail -100
true
