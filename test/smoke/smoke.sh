#!/usr/bin/env bash
# Live smoke test: full volume lifecycle against a running cluster.
# Creates everything in its own namespace and cleans up after itself.
#
#   ./test/smoke/smoke.sh
#
# Requires: kubectl pointed at the cluster, the miroir-replicated
# StorageClass and miroir-snap VolumeSnapshotClass installed, and
# gateway.enabled=true in Helm values (the RWX failover scenario needs it).
set -euo pipefail

NS=miroir-smoke
SC=${SC:-miroir-replicated}
SNAPCLASS=${SNAPCLASS:-miroir-snap}
TIMEOUT=${TIMEOUT:-300s}

pass=0
step() { printf '\n==> %s\n' "$1"; }
ok() { pass=$((pass + 1)); printf 'OK  %s\n' "$1"; }
die() {
    printf 'FAIL %s\n' "$1" >&2
    kubectl get pods,pvc,volumesnapshot -n "$NS" 2>/dev/null || true
    exit 1
}

cleanup() {
    step "cleanup"
    kubectl delete namespace "$NS" --ignore-not-found --wait=true --timeout=120s || true
}
trap cleanup EXIT

agent_pod() { # agent_pod <node>
    kubectl get pods -n miroir-system -l app.kubernetes.io/component=agent \
        --field-selector spec.nodeName="$1" -o jsonpath='{.items[0].metadata.name}'
}

pod_manifest() { # pod_manifest <name> <pvc> [node]
    local affinity=""
    if [ -n "${3:-}" ]; then
        affinity="
  nodeSelector:
    kubernetes.io/hostname: $3"
    fi
    cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NS
spec:$affinity
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
    - name: app
      image: alpine:3.23
      command: [sleep, infinity]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: $2
EOF
}

step "namespace + PVC + writer pod"
kubectl create namespace "$NS"
kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: smoke-data
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $SC
  resources:
    requests:
      storage: 1Gi
EOF
pod_manifest writer smoke-data | kubectl apply -f -
kubectl wait -n "$NS" pod/writer --for=condition=Ready --timeout="$TIMEOUT" || die "writer pod not ready"
ok "PVC bound and pod running"

step "replication healthy on both legs"
pv=$(kubectl get pvc -n "$NS" smoke-data -o jsonpath='{.spec.volumeName}')
phase=$(kubectl get miroirvolume "$pv" -o jsonpath='{.status.phase}')
[ "$phase" = Ready ] || die "volume $pv phase=$phase, want Ready"
ok "miroirvolume $pv Ready (both replicas UpToDate)"

step "write checksummed data"
kubectl exec -n "$NS" writer -- sh -c \
    'dd if=/dev/urandom of=/data/seed bs=1M count=64 2>/dev/null && sha256sum /data/seed > /data/seed.sha && sync'
sum=$(kubectl exec -n "$NS" writer -- cut -d' ' -f1 /data/seed.sha)
ok "wrote 64MiB, sha256 $sum"

step "failover: same data readable from the other node"
node=$(kubectl get pod -n "$NS" writer -o jsonpath='{.spec.nodeName}')
other=$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v "^$node$" | head -1)
kubectl delete pod -n "$NS" writer --wait
pod_manifest reader smoke-data "$other" | kubectl apply -f -
kubectl wait -n "$NS" pod/reader --for=condition=Ready --timeout="$TIMEOUT" || die "reader pod not ready on $other"
kubectl exec -n "$NS" reader -- sha256sum -c /data/seed.sha >/dev/null || die "checksum mismatch on $other"
ok "data intact after move $node -> $other"

step "snapshot under write load"
# Detach the churn loop's stdio or kubectl exec waits on it forever.
kubectl exec -n "$NS" reader -- sh -c \
    'nohup sh -c "while true; do dd if=/dev/urandom of=/data/churn bs=1M count=8 2>/dev/null; done" >/dev/null 2>&1 & echo started'
kubectl apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: smoke-snap
  namespace: $NS
spec:
  volumeSnapshotClassName: $SNAPCLASS
  source:
    persistentVolumeClaimName: smoke-data
EOF
kubectl wait -n "$NS" volumesnapshot/smoke-snap --for=jsonpath='{.status.readyToUse}'=true --timeout=120s \
    || die "snapshot not ready within 120s"
ok "snapshot ready under write load"

step "no barrier left behind"
for n in $node $other; do
    suspended=$(kubectl exec -n miroir-system "$(agent_pod "$n")" -c agent -- \
        sh -c 'drbdsetup status | grep -c suspended' || true)
    [ "${suspended:-0}" = 0 ] || die "device still suspended on $n"
done
ok "no suspended devices on either node"

step "restore from snapshot"
kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: smoke-restore
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $SC
  dataSource:
    apiGroup: snapshot.storage.k8s.io
    kind: VolumeSnapshot
    name: smoke-snap
  resources:
    requests:
      storage: 1Gi
EOF
pod_manifest restored smoke-restore | kubectl apply -f -
kubectl wait -n "$NS" pod/restored --for=condition=Ready --timeout="$TIMEOUT" || die "restored pod not ready"
kubectl exec -n "$NS" restored -- sha256sum -c /data/seed.sha >/dev/null || die "restored data mismatch"
ok "restore matches snapshot content"

step "online expand"
kubectl patch pvc -n "$NS" smoke-data -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}'
deadline=$((SECONDS + 180))
while :; do
    kb=$(kubectl exec -n "$NS" reader -- df -k /data | awk 'NR==2{print $2}')
    [ "$kb" -gt 1572864 ] && break
    [ "$SECONDS" -lt "$deadline" ] || die "filesystem did not grow (still ${kb}KB)"
    sleep 5
done
ok "filesystem grew online to ${kb}KB"

step "delete -> recreate under same claim comes up healthy (issue #139)"
# A fresh PVC under a reused claim name must provision clean — i.e. the prior
# volume's DRBD state was fully released on teardown and the new legs do not
# come up split-brain. This is the deterministic #139 reproduction.
recycle_pvc() { # prints the bound PV name
    kubectl apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: recycle
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $SC
  resources:
    requests:
      storage: 1Gi
EOF
    pod_manifest recycle-consumer recycle | kubectl apply -f - >/dev/null
    kubectl wait -n "$NS" pod/recycle-consumer --for=condition=Ready --timeout="$TIMEOUT" >/dev/null \
        || die "recycle consumer not ready"
    kubectl get pvc -n "$NS" recycle -o jsonpath='{.spec.volumeName}'
}
recycle_destroy() {
    kubectl delete pod -n "$NS" recycle-consumer --wait >/dev/null
    kubectl delete pvc -n "$NS" recycle --wait >/dev/null
    deadline=$((SECONDS + 120))
    while kubectl get miroirvolume "$1" >/dev/null 2>&1; do
        [ "$SECONDS" -lt "$deadline" ] || die "recycle volume $1 not torn down"
        sleep 3
    done
}
pv_a=$(recycle_pvc)
[ "$(kubectl get miroirvolume "$pv_a" -o jsonpath='{.status.phase}')" = Ready ] \
    || die "first recycle volume $pv_a not Ready (both legs UpToDate)"
recycle_destroy "$pv_a"
pv_b=$(recycle_pvc)
[ "$pv_b" != "$pv_a" ] || die "recreate must be a distinct PV"
[ "$(kubectl get miroirvolume "$pv_b" -o jsonpath='{.status.phase}')" = Ready ] \
    || die "recreated volume $pv_b not Ready — split-brain on recreate?"
recycle_destroy "$pv_b"
ok "recreate under reused claim came up healthy ($pv_a -> $pv_b)"

step "RWX: shared filesystem across two nodes"
kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rwx-data
spec:
  accessModes: [ReadWriteMany]
  storageClassName: $SC
  resources:
    requests:
      storage: 1Gi
EOF
pod_manifest rwx-a rwx-data "$node" | kubectl apply -f -
pod_manifest rwx-b rwx-data "$other" | kubectl apply -f -
kubectl wait -n "$NS" pod/rwx-a pod/rwx-b --for=condition=Ready --timeout="$TIMEOUT" \
    || die "RWX pods not ready on both nodes"
rwx_pv=$(kubectl get pvc -n "$NS" rwx-data -o jsonpath='{.spec.volumeName}')
ok "RWX PVC bound ($rwx_pv), pods running on $node and $other"

step "RWX: write on one node visible on the other"
kubectl exec -n "$NS" rwx-a -- sh -c 'echo hello-from-a > /data/shared && sync'
# NFS close-to-open: rwx-a closed the file, so rwx-b sees the write.
got=$(kubectl exec -n "$NS" rwx-b -- cat /data/shared)
[ "$got" = hello-from-a ] || die "RWX cross-node read got '$got', want hello-from-a"
ok "write on $node read back on $other over NFS"

step "RWX: gateway pod failover"
gw=$(kubectl get pod -n miroir-system -l miroir.home-operations.com/volume="$rwx_pv" \
    -o jsonpath='{.items[0].metadata.name}')
[ -n "$gw" ] || die "no gateway pod found for $rwx_pv"
kubectl delete pod -n miroir-system "$gw" --wait
kubectl rollout status -n miroir-system "deploy/miroir-share-$rwx_pv" --timeout="$TIMEOUT" \
    || die "gateway did not reschedule"
# Hard mounts stall through the reschedule and NFS grace, then resume. Retry
# a bounded write until the replacement gateway is serving again.
deadline=$((SECONDS + 180))
until kubectl exec -n "$NS" --request-timeout=20s rwx-b -- sh -c 'echo after-failover >> /data/shared && sync' 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || die "RWX did not recover after gateway failover"
    sleep 5
done
kubectl exec -n "$NS" rwx-a -- grep -q after-failover /data/shared \
    || die "post-failover write not visible on $node"
ok "RWX survived gateway reschedule and stayed writable"

# Clean up the RWX resources so the teardown check below stays about the
# RWO PVs it already tracks.
kubectl delete pod -n "$NS" rwx-a rwx-b --wait
kubectl delete pvc -n "$NS" rwx-data --wait

step "teardown leaves nothing behind"
pv2=$(kubectl get pvc -n "$NS" smoke-restore -o jsonpath='{.spec.volumeName}')
kubectl delete namespace "$NS" --wait=true --timeout=120s
trap - EXIT
deadline=$((SECONDS + 120))
while kubectl get miroirvolume "$pv" "$pv2" 2>/dev/null | grep -q pvc-; do
    [ "$SECONDS" -lt "$deadline" ] || die "miroirvolumes not cleaned up"
    sleep 5
done
for n in $node $other; do
    pod=$(agent_pod "$n")
    leftovers=$(kubectl exec -n miroir-system "$pod" -c agent -- sh -c \
        "drbdsetup status 2>/dev/null | grep -cE '^($pv|$pv2)'" || true)
    [ "${leftovers:-0}" = 0 ] || die "DRBD resource leftover on $n"
done
ok "volumes, DRBD resources and backing devices cleaned up"

printf '\nPASS: %d steps, 0 failures\n' "$pass"
