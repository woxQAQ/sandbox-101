#!/usr/bin/env bash
set -euo pipefail
# Build in a caller-provided empty checkout; never run downloaded code from the
# engine state directory. The build receipt is trusted deployment metadata.
: "${1:?usage: build-cloud-hypervisor.sh EMPTY_BUILD_DIR INSTALL_DIR}"
: "${2:?usage: build-cloud-hypervisor.sh EMPTY_BUILD_DIR INSTALL_DIR}"
build_dir=$1
install_dir=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
revision=$(<"$script_dir/cloud-hypervisor-revision")
if [[ -e "$build_dir" ]]; then
    printf '%s\n' 'Build directory must not exist.' >&2
    exit 1
fi
git clone --no-checkout https://github.com/cloud-hypervisor/cloud-hypervisor.git "$build_dir"
git -C "$build_dir" checkout --detach "$revision"
# seccomp is always compiled in this revision; the selectable features are kvm
# and io_uring. The engine additionally forces --seccomp true at execution.
(
    cd "$build_dir"
    rustup target add x86_64-unknown-linux-musl
    cargo build --locked --release --target x86_64-unknown-linux-musl \
        --package cloud-hypervisor --no-default-features --features kvm,io_uring
)
install -d -m 0755 "$install_dir"
install -m 0755 "$build_dir/target/x86_64-unknown-linux-musl/release/cloud-hypervisor" "$install_dir/cloud-hypervisor"
read -r binary_digest _ < <(sha256sum "$install_dir/cloud-hypervisor")
printf '{"commit":"%s","sha256":"sha256:%s","features":"kvm,io_uring,seccomp"}\n' \
    "$revision" "$binary_digest" > "$install_dir/cloud-hypervisor.json"
chmod 0644 "$install_dir/cloud-hypervisor.json"
