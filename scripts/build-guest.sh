#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
guest_target=${1:-x86_64-unknown-linux-musl}
case "$guest_target" in x86_64-unknown-linux-musl|aarch64-unknown-linux-musl) ;; *) printf 'unsupported static guest target: %s\n' "$guest_target" >&2; exit 1;; esac
cargo build --manifest-path "$project_root/Cargo.toml" --locked --release --target "$guest_target" -p sandbox-agent -p sandbox-init
bash "$project_root/scripts/package-guest.sh" "$project_root/target/$guest_target/release"
