#!/usr/bin/env bash
# Run the upstream Kubernetes external-storage e2e suite against the
# cluster kubectl currently points at, using testdriver.yaml.
#
#   ./test/conformance/run.sh                 # parallel-safe set
#   SKIP='\[Disruptive\]' PROCS=1 ./run.sh    # include [Serial] specs
#   FOCUS='.*snapshot.*' ./run.sh             # narrow down
#   TESTDRIVER=testdriver-local.yaml ./run.sh # miroir-local (lvmthin)
#   TESTDRIVER=testdriver-zfs.yaml ./run.sh   # miroir-zfs (replicated zfs)
#   TESTDRIVER=testdriver-rwx.yaml ./run.sh   # filesystem-only RWX/ROX
#   SHARD=2 ./run.sh                          # one third of the set (1, 2 or 3)
#   VERBOSE=1 ./run.sh                        # per-spec live output
#
# The e2e.test/ginkgo binaries are fetched to match the server version
# and cached under .bin/.
set -euo pipefail
cd "$(dirname "$0")"

ver=$(kubectl version -o json | jq -r .serverVersion.gitVersion)
bin=.bin/$ver
if [ ! -x "$bin/e2e.test" ]; then
    mkdir -p "$bin"
    echo "fetching kubernetes-test $ver"
    curl -fsSL "https://dl.k8s.io/$ver/kubernetes-test-linux-amd64.tar.gz" |
        tar -xz -C "$bin" --strip-components=3 \
            kubernetes/test/bin/e2e.test kubernetes/test/bin/ginkgo
fi

KUBECONFIG=${KUBECONFIG:-$HOME/.kube/config}
TESTDRIVER=${TESTDRIVER:-testdriver.yaml}
# A shard appends its pattern to the default focus, which names the driver
# ahead of the [Testpattern: ...] tag. A custom focus can match anywhere in
# the spec text, so the two do not combine.
if [ -n "${SHARD:-}" ] && [ -n "${FOCUS:-}" ]; then
    echo "SHARD=$SHARD cannot combine with FOCUS=$FOCUS" >&2
    exit 1
fi
FOCUS=${FOCUS:-'miroir.home-operations.com'}
# Disruptive specs restart kubelet; Serial ones assume exclusive use of
# the cluster; the volumeMode host check nsenters the node and runs sh,
# which Talos does not ship.
SKIP=${SKIP:-'\[Disruptive\]|\[Serial\]|should not mount / map unused volumes'}
PROCS=${PROCS:-4}

# Shards split the set by upstream test suite name, the word after the
# [Testpattern: ...] tag and any further [...] tags, grouped from CI timings
# to keep the slowest shard short. Shard 3 skips whatever the other two
# focus, so a suite that upstream adds lands there instead of in no shard.
shard_re() { printf '\\[Testpattern: [^\\]]*\\] (\\[[^\\]]*\\] )*(%s) ' "$1"; }
shard1='multiVolume|volumes'
shard2='subPath|volumegroupsnapshottable|fsgroupchangepolicy'
case "${SHARD:-}" in
    '') ;;
    1) FOCUS="$FOCUS.*$(shard_re "$shard1")" ;;
    2) FOCUS="$FOCUS.*$(shard_re "$shard2")" ;;
    3) SKIP="$SKIP|$(shard_re "$shard1|$shard2")" ;;
    *) echo "SHARD=$SHARD: want 1, 2 or 3" >&2; exit 1 ;;
esac
mkdir -p report

exec "$bin/ginkgo" -procs="$PROCS" ${VERBOSE:+-v} \
    -focus="$FOCUS" -skip="$SKIP" -fail-on-empty -timeout=3h \
    "$bin/e2e.test" -- \
    -storage.testdriver="$PWD/$TESTDRIVER" \
    -kubeconfig="$KUBECONFIG" \
    -report-dir="$PWD/report"
