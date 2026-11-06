#!/usr/bin/env bash
set -euo pipefail
kernel_config=${1:?usage: check-kernel.sh PATH_TO_KERNEL_CONFIG}
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
while IFS= read -r requirement; do
  [[ -z "$requirement" || "$requirement" == \#* ]] && continue
  if ! rg --fixed-strings --line-regexp --quiet "$requirement" "$kernel_config"; then
    printf 'required built-in kernel option missing: %s\n' "$requirement" >&2; exit 1
  fi
done < "$project_root/image/kernel.config"
printf 'Kernel built-in feature requirements checked.\n'
