#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
materials_dir=${1:?usage: run-guest-image.sh MATERIALS_DIR [GUEST_DIR] [ARTIFACTS_DIR]}
guest_dir=${2:-"$project_root/build/guest"}
artifacts_dir=${3:-"$project_root/build/guest-image"}
: "${GUEST_ROOTFS_DIR:?set GUEST_ROOTFS_DIR to a complete guest userspace rootfs; see scripts/README.md}"
materials_dir=$(realpath -- "$materials_dir")
guest_dir=$(realpath -- "$guest_dir")
export SANDBOX_GUEST_ROOTFS
SANDBOX_GUEST_ROOTFS=$(realpath -e -- "$GUEST_ROOTFS_DIR")
[[ -d $SANDBOX_GUEST_ROOTFS && $SANDBOX_GUEST_ROOTFS != / ]] || {
  printf 'GUEST_ROOTFS_DIR must be a dedicated rootfs directory, not the host root.\n' >&2; exit 1;
}
mkdir -p -- "$artifacts_dir"
artifacts_dir=$(realpath -- "$artifacts_dir")

# The rootfs contains genuine systemd, tools and all guest runtime dependencies.
# It is copied into the OCI fixture; no host root is mounted into the guest.
export SANDBOX_GUEST_HYPERVISOR="$materials_dir/cloud-hypervisor"
export SANDBOX_GUEST_KERNEL="$materials_dir/bzImage"
export SANDBOX_GUEST_DIR="$guest_dir"
export SANDBOX_GUEST_IMAGE_ARTIFACTS="$artifacts_dir"
export SANDBOX_REQUIRE_EROFS_TESTS=1
bash "$project_root/scripts/check-kernel.sh" "$materials_dir/kernel.config"
GOWORK=off go -C "$project_root/haven" test -tags=guest_image ./tests/native \
  -run '^TestGuestOCIFileViewAndCOW$' -count=1 -v -timeout=25m
