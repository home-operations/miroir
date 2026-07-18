#!/usr/bin/env bash
# Run the upstream Kubernetes external-storage e2e suite against the
# cluster kubectl currently points at, using testdriver.yaml.
#
#   ./test/conformance/run.sh                 # parallel-safe set
#   SKIP='\[Disruptive\]' ./run.sh PROCS=1    # include [Serial] specs
#   FOCUS='.*snapshot.*' ./run.sh             # narrow down
#   TESTDRIVER=testdriver-local.yaml ./run.sh # kind / miroir-local
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
FOCUS=${FOCUS:-'miroir.home-operations.com'}
# Disruptive specs restart kubelet; Serial ones assume exclusive use of
# the cluster; the volumeMode host check nsenters the node and runs sh,
# which Talos does not ship.
SKIP=${SKIP:-'\[Disruptive\]|\[Serial\]|should not mount / map unused volumes'}
PROCS=${PROCS:-4}
mkdir -p report

exec "$bin/ginkgo" -procs="$PROCS" ${VERBOSE:+-v} \
    -focus="$FOCUS" -skip="$SKIP" -timeout=3h \
    "$bin/e2e.test" -- \
    -storage.testdriver="$PWD/$TESTDRIVER" \
    -kubeconfig="$KUBECONFIG" \
    -report-dir="$PWD/report"
