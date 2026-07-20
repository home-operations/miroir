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
# zfsutils-linux lives in contrib. Version notes:
#   - drbd-utils comes from LINBIT's public apt repo (native trixie
#     dist), not Debian trixie, which is frozen at 9.22 with no
#     backports: the 9.34.x line is what LINBIT builds and tests
#     against the DRBD 9.3.x kernel module the siderolabs extension
#     ships. The birth generation depends on drbdadm
#     new-current-uuid --clear-bitmap behavior — re-validate with
#     smoke.sh + conformance on real DRBD (the Go e2e specs exercise
#     the local backend only) before shipping a utils or base bump.
#   - zfs userland 2.3 against the siderolabs/zfs 2.4 module: userland
#     older than the module is the supported direction, and miroir only
#     uses ancient ops (create -V/snapshot/clone/promote/volsize).
FROM debian:trixie-slim AS agent
RUN export DEBIAN_FRONTEND=noninteractive && \
    sed -i 's|^Components: main$|Components: main contrib|' /etc/apt/sources.list.d/debian.sources && \
    apt-get update -qq && \
    # ca-certificates: apt needs TLS trust to reach the LINBIT repo. Kept
    # installed — the gateway stage's apt-get update fetches from it too.
    apt-get install -y --no-install-recommends ca-certificates curl && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.linbit.com/package-signing-pubkey.asc \
      -o /etc/apt/keyrings/linbit.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/linbit.asc] https://packages.linbit.com/public trixie misc" \
      > /etc/apt/sources.list.d/linbit.list && \
    apt-get update -qq && \
    apt-get install -y --no-install-recommends \
    drbd-utils \
    lvm2 \
    zfsutils-linux \
    e2fsprogs \
    xfsprogs \
    # nfs-common: mount.nfs for the CSI node service to mount an RWX
    # volume's NFS export on any consumer node (the server side is the
    # userspace gateway image below, so no nfs-kernel-server here).
    nfs-common \
    # modprobe: lvm2 and drbdsetup load missing dm/drbd kernel targets on
    # demand through the pod's read-only /lib/modules hostPath (Alpine got
    # this implicitly from busybox; trixie-slim ships no kmod).
    kmod \
    # explicit though present in the base: losetup/blkid/lsblk (util-linux),
    # mount, and GNU cp for reflink clones (cp --reflink → FICLONE).
    util-linux \
    mount \
    coreutils && \
    # curl was only needed to fetch the signing key above.
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*
# No udevd is reachable from the container: stop libdevmapper from waiting
# on udev cookies and lvm from querying udev for the device list.
# global_filter rejects DRBD and NBD devices: lvm's device scan would block
# in D state on a suspended DRBD leg or a dead NBD device, wedging the
# reconciler and then pod shutdown.
RUN printf 'activation { udev_sync = 0\nudev_rules = 0 }\ndevices { obtain_device_list_from_udev = 0\nglobal_filter = [ "r|^/dev/drbd|", "r|^/dev/nbd|" ] }\n' \
    > /etc/lvm/lvmlocal.conf
COPY --from=build /miroir /usr/local/bin/miroir
ENTRYPOINT ["/usr/local/bin/miroir"]

# Gateway: the agent userland (it stages the DRBD device with the same
# lvm/drbd/mkfs tooling) plus NFS-Ganesha, which exports the mounted
# filesystem over NFSv4 for RWX volumes. Userspace NFS server (no
# nfs-kernel-server / nfsd module) so it works on Talos, where the export
# node needs no in-kernel NFS server. The VFS FSAL serves a plain mounted
# directory. Binary entrypoint unchanged; the pod runs --mode=gateway.
FROM agent AS gateway
RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    nfs-ganesha \
    nfs-ganesha-vfs && \
    rm -rf /var/lib/apt/lists/*
