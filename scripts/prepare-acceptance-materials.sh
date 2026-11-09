#!/usr/bin/env bash
set -euo pipefail
# Verify caller-built artifacts and record the common acceptance handoff.
# This does not build, download or install a toolchain.
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
materials_dir=${1:?usage: prepare-acceptance-materials.sh MATERIALS_DIR}
materials_dir=$(realpath -e -- "$materials_dir")
revision=$(<"$project_root/deploy/host/cloud-hypervisor-revision")
for tool in python3 file readelf nm rg; do
    command -v "$tool" >/dev/null || { printf 'Missing tool: %s\n' "$tool" >&2; exit 1; }
done
for artifact in cloud-hypervisor bzImage vmlinux kernel.config kernel-source.json; do
    [[ -f "$materials_dir/$artifact" ]] || {
        printf 'Missing acceptance artifact: %s\n' "$artifact" >&2; exit 1;
    }
done
bash "$project_root/scripts/check-kernel.sh" "$materials_dir/kernel.config" > "$materials_dir/kernel-check.log"
file -L "$materials_dir/cloud-hypervisor" "$materials_dir/bzImage" "$materials_dir/vmlinux" > "$materials_dir/file-check.log"
readelf -h -l -d "$materials_dir/cloud-hypervisor" > "$materials_dir/cloud-hypervisor.elf.txt"
readelf -h -l -d "$materials_dir/vmlinux" > "$materials_dir/kernel.elf.txt"
if rg -q 'INTERP|NEEDED' "$materials_dir/cloud-hypervisor.elf.txt" "$materials_dir/kernel.elf.txt"; then
    printf 'Cloud Hypervisor and kernel vmlinux must be static.\n' >&2; exit 1
fi
nm --defined-only "$materials_dir/vmlinux" | rg ' [Tt] (erofs_map_dev|vmgenid_add)$' > "$materials_dir/kernel-symbols.log"
rg -q ' erofs_map_dev$' "$materials_dir/kernel-symbols.log"
rg -q ' vmgenid_add$' "$materials_dir/kernel-symbols.log"
for artifact in cloud-hypervisor kernel; do
    rg -q 'Class: +ELF64' "$materials_dir/$artifact.elf.txt"
    rg -q 'Machine: +Advanced Micro Devices X86-64' "$materials_dir/$artifact.elf.txt"
done
"$materials_dir/cloud-hypervisor" --version > "$materials_dir/cloud-hypervisor.version"
rg -Fq -- "$revision" "$materials_dir/cloud-hypervisor.version"
python3 -I - "$materials_dir" "$revision" "$project_root/image/kernel.config" <<'PY'
import hashlib
import json
from pathlib import Path
import struct
import sys
from datetime import datetime, timezone

root = Path(sys.argv[1])
revision = sys.argv[2]

def digest(path):
    with path.open("rb") as source:
        return "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()

with (root / "bzImage").open("rb") as image:
    header = image.read(0x240)
if len(header) < 0x240 or header[0x202:0x206] != b"HdrS" or struct.unpack_from("<H", header, 0x1FE)[0] != 0xAA55:
    raise SystemExit("Invalid Linux bzImage setup header")
kernel_source = json.loads((root / "kernel-source.json").read_text())
if not isinstance(kernel_source, dict) or not all(
    isinstance(kernel_source.get(key), str) and kernel_source[key]
    for key in ("version", "source_hash")
):
    raise SystemExit("kernel-source.json requires nonempty version and source_hash strings")
ch_receipt = {
    "commit": revision,
    "sha256": digest(root / "cloud-hypervisor"),
    "features": "kvm,io_uring,seccomp",
}
receipt = {
    "verified_at": datetime.now(timezone.utc).isoformat(),
    "architecture": "x86_64",
    "cloud_hypervisor": {
        **ch_receipt,
        "path": str(root / "cloud-hypervisor"),
        "version": (root / "cloud-hypervisor.version").read_text().strip(),
        "static_elf_checked": True,
    },
    "kernel": {
        "path": str(root / "bzImage"),
        "sha256": digest(root / "bzImage"),
        "vmlinux": str((root / "vmlinux").resolve()),
        "vmlinux_sha256": digest(root / "vmlinux"),
        "config": str(root / "kernel.config"),
        "config_sha256": digest(root / "kernel.config"),
        "requirements_sha256": digest(Path(sys.argv[3])),
        "source": kernel_source,
        "requirements_checked": True,
        "boot_protocol": "bzImage",
        "static_elf_checked": True,
        "erofs_external_device_mapper_linked": True,
        "vmgenid_linked": True,
    },
    "acceptance": "build inputs verified; guest boot acceptance runs separately",
}
(root / "cloud-hypervisor.json").write_text(json.dumps(ch_receipt, indent=2) + "\n")
(root / "materials.json").write_text(json.dumps(receipt, indent=2) + "\n")
PY
printf 'Verified Cloud Hypervisor: %s/cloud-hypervisor\nVerified guest kernel: %s/bzImage\nReceipt: %s/materials.json\n' \
    "$materials_dir" "$materials_dir" "$materials_dir"
