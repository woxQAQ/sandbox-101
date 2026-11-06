#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_dir=${1:?usage: package-guest.sh ARTIFACT_DIR}
for executable in sandbox-agent sandbox-init; do
  artifact="$artifact_dir/$executable"
  program_headers=$(readelf -l "$artifact")
  dynamic_headers=$(readelf -d "$artifact")
  if printf '%s\n' "$program_headers" | rg -q INTERP || printf '%s\n' "$dynamic_headers" | rg -q NEEDED; then
    printf 'guest executable is not static: %s\n' "$artifact" >&2
    exit 1
  fi
done
mkdir -p "$project_root/build/guest"
for executable in sandbox-agent sandbox-init; do
  install -m 0755 "$artifact_dir/$executable" "$project_root/build/guest/$executable"
done
initramfs_dir=$(mktemp -d)
archive=$(mktemp "$project_root/build/guest/.initramfs.XXXXXXXX")
trap 'rm -rf -- "$initramfs_dir"; rm -f -- "$archive"' EXIT
install -m 0755 "$project_root/build/guest/sandbox-init" "$initramfs_dir/init"
touch -d @0 "$initramfs_dir/init"
printf 'init\0' | cpio --quiet -D "$initramfs_dir" --null -o -H newc --owner=0:0 --reproducible > "$archive"
chmod 0644 "$archive"
mv -- "$archive" "$project_root/build/guest/initramfs"
printf 'Static binaries and initramfs: %s/build/guest\n' "$project_root"
