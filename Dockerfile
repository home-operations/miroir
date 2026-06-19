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

# The agent drives lvm/zfs/mkfs on the host through this container's
# userland; the kernel modules come from the Talos kernel + extensions.
# Alpine's zfs userland (2.4.x) must share a minor version with the
# siderolabs/zfs extension's module — verify on upgrades.
# drbd-utils is pinned to the Alpine 3.24 series (bumped 9.33 → 9.34 with
# the repo): GI seeding depends on drbdmeta CLI behavior — re-validate
# against smoke.sh + conformance (the kind e2e does not exercise DRBD)
# before bumping further.
FROM alpine:3.24
RUN apk add --no-cache \
    lvm2 \
    zfs \
    'drbd-utils=~9.34' \
    e2fsprogs \
    e2fsprogs-extra \
    xfsprogs \
    xfsprogs-extra \
    blkid \
    util-linux-misc \
    # loopfile backend: full losetup (-j/-O/-c, beyond busybox's) and GNU cp
    # for reflink (cp --reflink → FICLONE); both shadow busybox via PATH.
    losetup \
    coreutils
# No udevd is reachable from the container: stop libdevmapper from waiting
# on udev cookies and lvm from querying udev for the device list.
# global_filter rejects DRBD devices: lvm's device scan would block in D
# state on a suspended one, wedging the reconciler and then pod shutdown.
RUN printf 'activation { udev_sync = 0\nudev_rules = 0 }\ndevices { obtain_device_list_from_udev = 0\nglobal_filter = [ "r|^/dev/drbd|" ] }\n' \
    > /etc/lvm/lvmlocal.conf
COPY --from=build /miroir /usr/local/bin/miroir
ENTRYPOINT ["/usr/local/bin/miroir"]
