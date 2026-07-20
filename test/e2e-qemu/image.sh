#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

: "${CONTROLLER_IMAGE:?must be set to the controller tag to build and push}"
: "${AGENT_IMAGE:?must be set to the agent tag to build and push}"

cd "$REPO_ROOT"

GO_VERSION="${GO_VERSION:-$(mise config get tools.go)}"

# Both images are stages of the one Dockerfile; --target selects which.
build_push() {
    local target=$1 image=$2
    log "Building $target image $image..."
    docker build --build-arg "GO_VERSION=${GO_VERSION}" --target "$target" -t "$image" .
    log "Pushing $image..."
    docker push "$image"
}

build_push controller "$CONTROLLER_IMAGE"
build_push agent "$AGENT_IMAGE"

log "Images ready: $CONTROLLER_IMAGE, $AGENT_IMAGE"
