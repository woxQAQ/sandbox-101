# Build static guest binaries with `make guest` before building this OCI image.
# Put the matching Linux bootable kernel and .config in build/kernel{,.config}.
ARG BASE_IMAGE=debian:bookworm-slim
FROM ${BASE_IMAGE}
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    systemd systemd-sysv ca-certificates python3 curl iproute2 procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 sandbox \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/sh sandbox \
    && mkdir -p /boot /usr/local/libexec /etc/systemd/system/sysinit.target.wants /workspace \
    && chown 1000:1000 /workspace \
    && rm -f /etc/machine-id /var/lib/dbus/machine-id /etc/resolv.conf \
    && touch /etc/machine-id /etc/resolv.conf \
    && ln -s /etc/machine-id /var/lib/dbus/machine-id
COPY build/kernel /boot/vmlinux
COPY build/guest/initramfs /boot/initramfs
COPY build/guest/sandbox-agent /usr/local/libexec/sandbox-agent
COPY guest/systemd/sandbox-agent.service /etc/systemd/system/sandbox-agent.service
RUN ln -s ../sandbox-agent.service /etc/systemd/system/sysinit.target.wants/sandbox-agent.service
WORKDIR /workspace
# engine boots systemd through sandbox-init; OCI CMD and ENTRYPOINT are ignored.
