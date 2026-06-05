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
    -o /homefs cmd/main.go

# The agent drives lvm/zfs/mkfs on the host through this container's
# userland; the kernel modules come from the Talos kernel + extensions.
# Alpine's zfs userland (2.4.x) must share a minor version with the
# siderolabs/zfs extension's module — verify on upgrades.
# drbd-utils is pinned: GI seeding depends on drbdmeta CLI behavior that
# was validated against this version — re-validate before bumping.
FROM alpine:3.23
RUN apk add --no-cache \
    lvm2 \
    zfs \
    'drbd-utils=~9.33' \
    e2fsprogs \
    e2fsprogs-extra \
    xfsprogs \
    xfsprogs-extra \
    blkid \
    util-linux-misc
# No udevd is reachable from the container: stop libdevmapper from waiting
# on udev cookies and lvm from querying udev for the device list.
# global_filter rejects DRBD devices: lvm's scan opens every block device,
# and a read against a suspended DRBD device blocks in D state — an
# unkillable lvs wedges the reconciler and then pod shutdown (LINSTOR
# requires the same filter).
RUN printf 'activation { udev_sync = 0\nudev_rules = 0 }\ndevices { obtain_device_list_from_udev = 0\nglobal_filter = [ "r|^/dev/drbd|" ] }\n' \
    > /etc/lvm/lvmlocal.conf
COPY --from=build /homefs /usr/local/bin/homefs
ENTRYPOINT ["/usr/local/bin/homefs"]
