#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
materials_dir=${1:?usage: run-kvm-acceptance.sh MATERIALS_DIR [GUEST_DIR] [ARTIFACTS_DIR]}
guest_dir=${2:-"$project_root/build/guest"}
artifacts_dir=${3:-"$project_root/build/kvm-acceptance"}
materials_dir=$(realpath -- "$materials_dir")
guest_dir=$(realpath -- "$guest_dir")
mkdir -p -- "$artifacts_dir"
artifacts_dir=$(realpath -- "$artifacts_dir")

# Use the same pinned package set as the workspace, with genuine systemd and
# its complete runtime closure. Nothing from the host root is mounted in guest.
userspace=$(nix build --impure --no-link --print-out-paths --expr '
  let
    n = builtins.getFlake "github:NixOS/nixpkgs/319e4cebef10cbb636c6ea392137b52ad105336a";
    p = n.legacyPackages.x86_64-linux;
  in p.linkFarm "sandbox-kvm-acceptance-userspace" [
    { name = "systemd"; path = p.systemd; }
    { name = "shell"; path = p.bashInteractive; }
    { name = "coreutils"; path = p.coreutils; }
    { name = "attr"; path = p.attr.bin; }
    { name = "acl"; path = p.acl.bin; }
    { name = "e2fsprogs"; path = p.e2fsprogs; }
    { name = "erofs"; path = p.erofs-utils; }
    { name = "umoci"; path = p.umoci; }
    { name = "bubblewrap"; path = p.bubblewrap; }
    { name = "util-linux"; path = p.util-linux; }
  ]')
export SANDBOX_KVM_HYPERVISOR="$materials_dir/cloud-hypervisor"
export SANDBOX_KVM_KERNEL="$materials_dir/bzImage"
export SANDBOX_KVM_GUEST_DIR="$guest_dir"
export SANDBOX_KVM_SYSTEMD
SANDBOX_KVM_SYSTEMD=$(realpath -- "$userspace/systemd")
export SANDBOX_KVM_SHELL
SANDBOX_KVM_SHELL=$(realpath -- "$userspace/shell")
export SANDBOX_KVM_COREUTILS
SANDBOX_KVM_COREUTILS=$(realpath -- "$userspace/coreutils")
export SANDBOX_KVM_ATTR
SANDBOX_KVM_ATTR=$(realpath -- "$userspace/attr")
export SANDBOX_KVM_ACL
SANDBOX_KVM_ACL=$(realpath -- "$userspace/acl")
export SANDBOX_KVM_ARTIFACTS="$artifacts_dir"
export PATH="$userspace/e2fsprogs/bin:$userspace/erofs/bin:$userspace/umoci/bin:$userspace/bubblewrap/bin:$userspace/util-linux/bin:$PATH"
export SANDBOX_REQUIRE_EROFS_TESTS=1
bash "$project_root/scripts/check-kernel.sh" "$materials_dir/kernel.config"
GOWORK=off go -C "$project_root/engine" test -tags=kvm ./... \
  -run '^TestKVMOCIFileViewAndCOW$' -count=1 -v -timeout=25m
