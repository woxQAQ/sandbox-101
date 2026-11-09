# Userspace for the standalone KVM test, not the production guest image.
# The test overlays its own boot artifacts, accounts and systemd units.
ARG BASE_IMAGE=debian:bookworm-slim
FROM ${BASE_IMAGE} AS userspace
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    systemd bash coreutils attr acl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /boot /etc /usr/local/libexec

FROM scratch
COPY --from=userspace / /
