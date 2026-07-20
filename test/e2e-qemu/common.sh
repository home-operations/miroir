#!/usr/bin/env bash
# Shared configuration for the QEMU e2e scripts. Meant to be sourced.
# shellcheck disable=SC2034  # consumers of this file use these

# The external-snapshotter release the suite installs. The v1beta2 group-snapshot
# CRDs ship here, and the snapshot-controller is gated to match.
SNAPSHOTTER_VERSION="${SNAPSHOTTER_VERSION:-v8.6.0}"

# Where the driver is installed, and how a worker opts into storage.
NAMESPACE="${NAMESPACE:-miroir-system}"
STORAGE_CLASS_LABEL="storage.miroir.home-operations.com/class"

# A throwaway semver just for the packaged chart's OCI tag; the real version is
# stamped at release time. The e2e only cares that the packaged artifact installs.
CHART_VERSION="${CHART_VERSION:-0.0.0-e2e}"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

die() {
    log "ERROR: $*" >&2
    exit 1
}
