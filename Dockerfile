ARG GO_VERSION=1.26
FROM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG REVISION=unknown
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-X main.version=${VERSION} -X main.commit=${REVISION}" \
    -o /miroir cmd/main.go

# Controller: talks to the Kubernetes API only — it never execs a storage
# CLI, so it ships without one (and without a shell). The chart already
# runs it as 65532, this base's nonroot user.
FROM gcr.io/distroless/static:nonroot AS controller
COPY --from=build /miroir /usr/local/bin/miroir
ENTRYPOINT ["/usr/local/bin/miroir"]

# Agent: drives lvm/zfs/drbd/mkfs on the host through this container's
# userland; the kernel modules come from the Talos kernel + extensions.
# Debian (glibc) because the DRBD/ZFS ecosystem — LINBIT, Piraeus,
# blockstor — builds and tests the storage stack against glibc only.
# zfsutils-linux lives in contrib. Version notes (trixie, no backports):
#   - drbd-utils 9.22: every CLI/JSON surface miroir uses predates it
#     (adjust --skip-disk 8.9.7, status --json 8.9.8, quorum 8.9.11;
#     peer_devices/percent-in-sync/out-of-sync/peer-disk-state verified
#     in the v9.22.0 source). The birth generation depends on drbdadm
#     new-current-uuid --clear-bitmap behavior — re-validate with
#     smoke.sh + conformance on real DRBD (the kind e2e exercises the
#     local backend only) before shipping a base bump.
#   - zfs userland 2.3 against the siderolabs/zfs 2.4 module: userland
#     older than the module is the supported direction, and miroir only
#     uses ancient ops (create -V/snapshot/clone/promote/volsize).
FROM debian:trixie-slim AS agent
RUN sed -i 's|^Components: main$|Components: main contrib|' /etc/apt/sources.list.d/debian.sources && \
    apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    drbd-utils \
    lvm2 \
    zfsutils-linux \
    e2fsprogs \
    xfsprogs \
    # modprobe: lvm2 and drbdsetup load missing dm/drbd kernel targets on
    # demand through the pod's read-only /lib/modules hostPath (Alpine got
    # this implicitly from busybox; trixie-slim ships no kmod).
    kmod \
    # explicit though present in the base: losetup/blkid/lsblk (util-linux),
    # mount, and GNU cp for reflink clones (cp --reflink → FICLONE).
    util-linux \
    mount \
    coreutils && \
    rm -rf /var/lib/apt/lists/*
# No udevd is reachable from the container: stop libdevmapper from waiting
# on udev cookies and lvm from querying udev for the device list.
# global_filter rejects DRBD devices: lvm's device scan would block in D
# state on a suspended one, wedging the reconciler and then pod shutdown.
RUN printf 'activation { udev_sync = 0\nudev_rules = 0 }\ndevices { obtain_device_list_from_udev = 0\nglobal_filter = [ "r|^/dev/drbd|" ] }\n' \
    > /etc/lvm/lvmlocal.conf
COPY --from=build /miroir /usr/local/bin/miroir
ENTRYPOINT ["/usr/local/bin/miroir"]
