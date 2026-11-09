#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
materials_dir=${1:?usage: run-kvm-acceptance.sh MATERIALS_DIR [GUEST_DIR] [ARTIFACTS_DIR]}
guest_dir=${2:-"$project_root/build/guest"}
artifacts_dir=${3:-"$project_root/build/kvm-acceptance"}
: "${KVM_ROOTFS_DIR:?set KVM_ROOTFS_DIR to a complete guest userspace rootfs; see scripts/README.md}"
materials_dir=$(realpath -- "$materials_dir")
guest_dir=$(realpath -- "$guest_dir")
export SANDBOX_KVM_ROOTFS
SANDBOX_KVM_ROOTFS=$(realpath -e -- "$KVM_ROOTFS_DIR")
[[ -d $SANDBOX_KVM_ROOTFS && $SANDBOX_KVM_ROOTFS != / ]] || {
  printf 'KVM_ROOTFS_DIR must be a dedicated rootfs directory, not the host root.\n' >&2; exit 1;
}
mkdir -p -- "$artifacts_dir"
artifacts_dir=$(realpath -- "$artifacts_dir")

# The rootfs contains genuine systemd, tools and all guest runtime dependencies.
# It is copied into the OCI fixture; no host root is mounted into the guest.
export SANDBOX_KVM_HYPERVISOR="$materials_dir/cloud-hypervisor"
export SANDBOX_KVM_KERNEL="$materials_dir/bzImage"
export SANDBOX_KVM_GUEST_DIR="$guest_dir"
export SANDBOX_KVM_ARTIFACTS="$artifacts_dir"
export SANDBOX_REQUIRE_EROFS_TESTS=1
bash "$project_root/scripts/check-kernel.sh" "$materials_dir/kernel.config"
GOWORK=off go -C "$project_root/engine" test -tags=kvm ./... \
  -run '^TestKVMOCIFileViewAndCOW$' -count=1 -v -timeout=25m
