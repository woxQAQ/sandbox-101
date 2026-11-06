#!/usr/bin/env bash
set -euo pipefail
# Build standalone KVM acceptance inputs without changing the host. Nix supplies
# pinned compilers/dependencies; each run keeps its source, logs, config and receipt.
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
revision=a02b114bd42c65b73033231a27bd23f3c487fc4c
output_root=${1:-"$project_root/build/runtime/acceptance-materials"}
build_timeout=${MATERIALS_TIMEOUT_SECONDS:-1500}
build_cores=${MATERIALS_BUILD_CORES:-8}
[[ $(uname -sm) == 'Linux x86_64' ]] || { printf 'Requires x86_64 Linux.\n' >&2; exit 1; }
[[ $build_timeout =~ ^[1-9][0-9]*$ && $build_cores =~ ^[1-9][0-9]*$ ]] || {
    printf 'Build timeout and core count must be positive integers.\n' >&2; exit 1;
}
for tool in nix nix-build nix-instantiate nix-store git tar python3 timeout file readelf nm rg realpath install; do
    command -v "$tool" >/dev/null || { printf 'Missing tool: %s\n' "$tool" >&2; exit 1; }
done
rg -qx "revision=$revision" "$project_root/deploy/host/build-cloud-hypervisor.sh" || {
    printf 'Cloud Hypervisor deployment pin changed; review this builder.\n' >&2; exit 1;
}
mkdir -p -- "$output_root"
output_root=$(realpath -- "$output_root")
run_dir=$(mktemp -d "$output_root/run.XXXXXXXX")
printf 'Build workspace: %s\n' "$run_dir"
trap 'printf "Preserved build workspace: %s\n" "$run_dir"' EXIT

nixpkgs_revision=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["nodes"]["nixpkgs"]["locked"]["rev"])' "$project_root/flake.lock")
timeout --kill-after=30s 120 nix eval --raw --expr \
    "(builtins.getFlake \"github:NixOS/nixpkgs/$nixpkgs_revision\").outPath" > "$run_dir/nixpkgs-path"
nixpkgs=$(<"$run_dir/nixpkgs-path")
reference="$project_root/references/cocoonstack/cloud-hypervisor"
# Export Git objects, ignoring working-tree edits. The archive enters its own new
# empty directory; generated build scripts remain outside the source tree.
mkdir -- "$run_dir/ch-source"
timeout --kill-after=30s 60 git -C "$reference" archive "$revision" \
    | tar -x -C "$run_dir/ch-source"
cp -- "$project_root/image/kernel.config" "$run_dir/kernel.requirements.config"
cat > "$run_dir/materials.nix" <<'NIX'
{ nixpkgs }:
let
  pkgs = import nixpkgs { system = "x86_64-linux"; };
  lib = pkgs.lib;
  required = builtins.filter (line: builtins.match "CONFIG_[A-Z0-9_]+=y" line != null)
    (lib.splitString "\n" (builtins.readFile ./kernel.requirements.config));
  requiredConfig = builtins.listToAttrs (map (line: {
    name = lib.removeSuffix "=y" (lib.removePrefix "CONFIG_" line);
    value = lib.kernel.yes;
  }) required);
  kernel = pkgs.linux_6_18.override {
    enableCommonConfig = false;
    autoModules = false;
    structuredExtraConfig = requiredConfig // (with lib.kernel; {
      VIRTUALIZATION = yes;
      VIRT_DRIVERS = yes; # VMGENID is in this menu, independent of ACPI.
      HYPERVISOR_GUEST = yes;
      PARAVIRT = yes;
      KVM_GUEST = yes;
      MULTIUSER = yes;
      POSIX_TIMERS = yes;
      BPF = yes;
      BPF_SYSCALL = yes;
      CGROUP_BPF = yes;
      CGROUP_SCHED = yes;
      CGROUP_CPUACCT = yes;
      BLK_CGROUP = yes;
      CGROUP_FREEZER = yes;
      CPUSETS = yes;
      PID_NS = yes;
      NET_NS = yes;
      USER_NS = yes;
      IPC_NS = yes;
      UTS_NS = yes;
      IKCONFIG = yes;
      IKCONFIG_PROC = yes;
      EROFS_FS_ZIP = yes;
      EROFS_FS_ZIP_LZMA = yes;
      EROFS_FS_ZIP_DEFLATE = yes;
      EROFS_FS_ZIP_ZSTD = yes;
      DEBUG_INFO_NONE = yes;
    });
  };
  hypervisor = pkgs.pkgsCross.musl64.pkgsStatic.rustPlatform.buildRustPackage {
    pname = "sandbox-cloud-hypervisor";
    version = "55.0.0-a02b114bd42c65b73033231a27bd23f3c487fc4c";
    src = ./ch-source;
    cargoLock = {
      lockFile = ./ch-source/Cargo.lock;
      allowBuiltinFetchGit = true;
    };
    cargoBuildFlags = [ "--package" "cloud-hypervisor" ];
    buildNoDefaultFeatures = true;
    buildFeatures = [ "kvm" "io_uring" ];
    env.CH_EXTRA_VERSION = "a02b114bd42c65b73033231a27bd23f3c487fc4c";
    nativeBuildInputs = [ pkgs.binutils ];
    doCheck = false;
    postInstall = ''
      for kind in -l -d; do
        if readelf "$kind" "$out/bin/cloud-hypervisor" | grep -E 'INTERP|NEEDED'; then
          echo 'cloud-hypervisor must be static' >&2
          exit 1
        fi
      done
    '';
  };
in {
  inherit kernel hypervisor;
  kernelConfig = kernel.configfile;
  kernelDev = kernel.dev;
  kernelMetadata = {
    version = kernel.version;
    source_path = toString kernel.src;
    source_hash = kernel.src.outputHash;
  };
}
NIX
build_failed=0
for component in hypervisor kernel; do
    printf 'Building %s (log: %s/%s-build.log)\n' "$component" "$run_dir" "$component"
    if timeout --kill-after=30s "$build_timeout" nix-build "$run_dir/materials.nix" \
        --arg nixpkgs "$nixpkgs" -A "$component" -o "$run_dir/$component-package" \
        --cores "$build_cores" --max-jobs 4 > "$run_dir/$component-build.log" 2>&1; then
        printf '0\n' > "$run_dir/$component-build.status"
    else
        status=$?
        printf '%s\n' "$status" > "$run_dir/$component-build.status"
        printf '%s build failed (exit %s). Log retained.\n' "$component" "$status" >&2
        python3 -I -c 'import sys; print("\n".join(open(sys.argv[1],errors="replace").read().splitlines()[-30:]))' \
            "$run_dir/$component-build.log" >&2
        build_failed=1
    fi
done
(( build_failed == 0 )) || exit 1

install -m 0755 "$run_dir/hypervisor-package/bin/cloud-hypervisor" "$run_dir/cloud-hypervisor"
install -m 0644 "$run_dir/kernel-package/bzImage" "$run_dir/bzImage"
config_path=$(timeout --kill-after=30s 60 nix-instantiate --eval --strict --json \
    "$run_dir/materials.nix" --arg nixpkgs "$nixpkgs" -A kernelConfig.outPath \
    | python3 -I -c 'import json,sys; print(json.load(sys.stdin))')
install -m 0644 "$config_path" "$run_dir/kernel.config"
kernel_dev=$(timeout --kill-after=30s 60 nix-instantiate --eval --strict --json \
    "$run_dir/materials.nix" --arg nixpkgs "$nixpkgs" -A kernelDev.outPath \
    | python3 -I -c 'import json,sys; print(json.load(sys.stdin))')
timeout --kill-after=30s 60 nix-store --add-root "$run_dir/kernel-dev-package" --indirect \
    --realise "$kernel_dev" > "$run_dir/kernel-dev-root.log" 2>&1
ln -s -- "$kernel_dev/vmlinux" "$run_dir/vmlinux"
timeout --kill-after=30s 60 nix-instantiate --eval --strict --json \
    "$run_dir/materials.nix" --arg nixpkgs "$nixpkgs" -A kernelMetadata > "$run_dir/kernel-source.json"
bash "$project_root/scripts/check-kernel.sh" "$run_dir/kernel.config" > "$run_dir/kernel-check.log"
file -L "$run_dir/cloud-hypervisor" "$run_dir/bzImage" "$run_dir/vmlinux" > "$run_dir/file-check.log"
readelf -h -l -d "$run_dir/cloud-hypervisor" > "$run_dir/cloud-hypervisor.elf.txt"
readelf -h -l -d "$run_dir/vmlinux" > "$run_dir/kernel.elf.txt"
if rg -q 'INTERP|NEEDED' "$run_dir/cloud-hypervisor.elf.txt" "$run_dir/kernel.elf.txt"; then
    printf 'Cloud Hypervisor and kernel vmlinux must be static.\n' >&2; exit 1
fi
nm --defined-only "$run_dir/vmlinux" | rg ' [Tt] (erofs_map_dev|vmgenid_add)$' > "$run_dir/kernel-symbols.log"
rg -q ' erofs_map_dev$' "$run_dir/kernel-symbols.log"
rg -q 'Class: +ELF64' "$run_dir/cloud-hypervisor.elf.txt"
rg -q 'Machine: +Advanced Micro Devices X86-64' "$run_dir/cloud-hypervisor.elf.txt"
rg -q 'Machine: +Advanced Micro Devices X86-64' "$run_dir/kernel.elf.txt"
"$run_dir/cloud-hypervisor" --version > "$run_dir/cloud-hypervisor.version"
rg -q -- "$revision" "$run_dir/cloud-hypervisor.version"
python3 -I - "$run_dir" "$revision" "$nixpkgs_revision" "$nixpkgs" <<'PY'
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
from datetime import datetime, timezone

root = Path(sys.argv[1])
revision, nixpkgs_revision, nixpkgs = sys.argv[2:]
def digest(name):
    with (root / name).open("rb") as source:
        return "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()

# bzImage is a Linux boot protocol image, not an ELF executable. Check its
# real setup header in addition to the matching, uncompressed vmlinux ELF.
with (root / "bzImage").open("rb") as image:
    header = image.read(0x240)
if header[0x202:0x206] != b"HdrS" or struct.unpack_from("<H", header, 0x1FE)[0] != 0xAA55:
    raise SystemExit("Invalid Linux bzImage setup header")
ch_receipt = {"commit": revision, "sha256": digest("cloud-hypervisor"),
              "features": "kvm,io_uring,seccomp"}
(root / "cloud-hypervisor.json").write_text(json.dumps(ch_receipt, indent=2) + "\n")
receipt = {
    "built_at": datetime.now(timezone.utc).isoformat(),
    "architecture": "x86_64",
    "nixpkgs_revision": nixpkgs_revision,
    "nixpkgs_path": nixpkgs,
    "cloud_hypervisor": {**ch_receipt, "path": str(root / "cloud-hypervisor"),
        "package": str((root / "hypervisor-package").resolve()),
        "version": (root / "cloud-hypervisor.version").read_text().strip(),
        "static_elf_checked": True, "cargo_lock_sha256": digest("ch-source/Cargo.lock")},
    "kernel": {"path": str(root / "bzImage"), "sha256": digest("bzImage"),
        "package": str((root / "kernel-package").resolve()),
        "vmlinux": str((root / "vmlinux").resolve()), "vmlinux_sha256": digest("vmlinux"),
        "config": str(root / "kernel.config"), "config_sha256": digest("kernel.config"),
        "requirements_sha256": digest("kernel.requirements.config"),
        "requirements_checked": True, "boot_protocol": "bzImage", "static_elf_checked": True,
        "erofs_external_device_mapper_linked": True,
        **json.loads((root / "kernel-source.json").read_text())},
    "acceptance": "build inputs verified; guest boot acceptance runs separately",
}
for component in ["hypervisor", "kernel"]:
    package = root / (component + "-package")
    drv = subprocess.check_output(["nix-store", "-q", "--deriver", str(package)], text=True).strip()
    receipt["cloud_hypervisor" if component == "hypervisor" else "kernel"]["derivation"] = drv
(root / "materials.json").write_text(json.dumps(receipt, indent=2) + "\n")
PY
printf 'Verified Cloud Hypervisor: %s/cloud-hypervisor\nVerified guest kernel: %s/bzImage\nReceipt: %s/materials.json\n' \
    "$run_dir" "$run_dir" "$run_dir"
