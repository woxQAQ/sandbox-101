#!/usr/bin/env python3
"""Exercise public acceptance entrypoints with owned fixtures, without host privileges."""

import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parent.parent
REVISION = (PROJECT / "deploy/host/cloud-hypervisor-revision").read_text().strip()


class AcceptanceEntrypoints(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="acceptance-environment-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "workspace with spaces"
        self.root.mkdir()
        self.tools = self.root / "tools"
        self.tools.mkdir()
        # A deliberately closed PATH: no real package manager is available.
        for name in (
            "bash", "dirname", "realpath", "mkdir", "mktemp", "date", "tee",
            "timeout", "make", "python3", "rg", "file", "readelf", "nm",
        ):
            executable = shutil.which(name)
            if executable is None:
                self.fail(f"test prerequisite missing: {name}")
            (self.tools / name).symlink_to(executable)
        self.env = {
            **os.environ,
            "PATH": str(self.tools),
            "HOME": str(self.root),
            "RECORD_DIR": str(self.root),
            "SANDBOX_IMAGE_TOOL_ROOTS": str(self.tools),
        }
        self.env.pop("KVM_ROOTFS_DIR", None)
        self.env.pop("SANDBOX_KVM_ROOTFS", None)
        self.materials = self.root / "materials"
        self.materials.mkdir()
        shutil.copyfile(PROJECT / "image/kernel.config", self.materials / "kernel.config")

    def executable(self, name, body):
        script = self.tools / name
        script.write_text(f"#!{sys.executable}\n{body}")
        script.chmod(0o755)
        return script

    def run_command(self, *args, expected=0, **environment):
        result = subprocess.run(
            args, cwd=PROJECT, env={**self.env, **environment},
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def test_kvm_make_target_uses_only_supplied_environment(self):
        guest = self.root / "guest"
        rootfs = self.root / "rootfs"
        guest.mkdir()
        rootfs.mkdir()
        self.executable("go", """
import json, os, sys
from pathlib import Path
keys = ["SANDBOX_KVM_ROOTFS", "SANDBOX_KVM_HYPERVISOR", "SANDBOX_KVM_KERNEL",
        "SANDBOX_KVM_GUEST_DIR", "SANDBOX_KVM_ARTIFACTS",
        "SANDBOX_IMAGE_TOOL_ROOTS", "SANDBOX_REQUIRE_EROFS_TESTS"]
Path(os.environ["RECORD_DIR"], "go.json").write_text(json.dumps({
    "args": sys.argv[1:], "environment": {key: os.environ[key] for key in keys},
}))
""")
        self.run_command(
            "make", "-s", "kvm-e2e", f"KVM_MATERIALS_DIR={self.materials}",
            f"KVM_GUEST_DIR={guest}", f"KVM_ROOTFS_DIR={rootfs}",
            f"KVM_ARTIFACTS_DIR={self.root / 'kvm artifacts'}",
        )
        record = json.loads((self.root / "go.json").read_text())
        self.assertIn("-tags=kvm", record["args"])
        self.assertIn("^TestKVMOCIFileViewAndCOW$", record["args"])
        self.assertEqual(record["environment"]["SANDBOX_KVM_ROOTFS"], str(rootfs))
        self.assertEqual(record["environment"]["SANDBOX_KVM_GUEST_DIR"], str(guest))
        self.assertEqual(record["environment"]["SANDBOX_REQUIRE_EROFS_TESTS"], "1")
        self.assertEqual(record["environment"]["SANDBOX_IMAGE_TOOL_ROOTS"], str(self.tools))

    def test_kvm_rejects_missing_or_host_rootfs_before_launch(self):
        script = str(PROJECT / "scripts/run-kvm-acceptance.sh")
        result = self.run_command("bash", script, str(self.materials), expected=1)
        self.assertIn("KVM_ROOTFS_DIR", result.stderr)
        result = self.run_command(
            "bash", script, str(self.materials), str(self.root), expected=1,
            KVM_ROOTFS_DIR="/",
        )
        self.assertIn("not the host root", result.stderr)

    def test_production_make_target_forwards_explicit_dependencies(self):
        oci = self.root / "oci"
        oci.mkdir()
        self.executable("go", "import sys\nsys.exit(0)\n")
        self.executable("systemd-run", """
import json, os, sys
from pathlib import Path
Path(os.environ["RECORD_DIR"], "service.json").write_text(json.dumps(sys.argv[1:]))
""")
        self.executable("systemctl", "import sys\nsys.exit(0)\n")
        self.run_command(
            "make", "-s", "production-e2e", f"KVM_MATERIALS_DIR={self.materials}",
            f"PRODUCTION_OCI_LAYOUT={oci}",
            f"PRODUCTION_ARTIFACTS_DIR={self.root / 'production artifacts'}",
            SANDBOX_PRODUCTION_IO_MAX="8:0 wbps=1048576",
        )
        args = json.loads((self.root / "service.json").read_text())
        self.assertIn(f"--setenv=SANDBOX_IMAGE_TOOL_ROOTS={self.tools}", args)
        self.assertIn(f"--setenv=PATH={self.tools.resolve()}", args)
        self.assertIn(f"--setenv=SANDBOX_PRODUCTION_MATERIALS={self.materials}", args)
        self.assertIn(f"--setenv=SANDBOX_PRODUCTION_OCI_LAYOUT={oci}", args)

    def prepare_static_materials(self, revision=REVISION, symbols=True):
        # Tiny synthetic ELFs exercise real binutils/header checks, not VM boot.
        if platform.machine() != "x86_64":
            self.skipTest("VM materials acceptance currently requires x86_64")
        assembly = self.root / "fixture.s"
        message = f"cloud-hypervisor {revision}\n"
        assembly.write_text(
            ".global _start\n.text\n_start:\n"
            "mov $1, %rax\nmov $1, %rdi\nlea message(%rip), %rsi\n"
            f"mov ${len(message)}, %rdx\nsyscall\n"
            "mov $60, %rax\nxor %rdi, %rdi\nsyscall\n"
            + (".global erofs_map_dev\nerofs_map_dev:\nret\n"
               ".global vmgenid_add\nvmgenid_add:\nret\n" if symbols else "")
            + f".section .rodata\nmessage:\n.ascii {json.dumps(message)}\n"
        )
        for tool in ("as", "ld"):
            if shutil.which(tool) is None:
                self.fail(f"test prerequisite missing: {tool}")
        subprocess.run(
            [shutil.which("as"), "-o", str(self.root / "fixture.o"), str(assembly)],
            check=True, capture_output=True,
        )
        subprocess.run(
            [shutil.which("ld"), "-o", str(self.materials / "cloud-hypervisor"),
             str(self.root / "fixture.o")],
            check=True, capture_output=True,
        )
        shutil.copyfile(self.materials / "cloud-hypervisor", self.materials / "vmlinux")
        image = bytearray(0x240)
        image[0x1FE:0x200] = b"\x55\xaa"
        image[0x202:0x206] = b"HdrS"
        (self.materials / "bzImage").write_bytes(image)
        (self.materials / "kernel-source.json").write_text(json.dumps({
            "version": "synthetic-test-kernel", "source_hash": "synthetic-test-source",
        }))

    def test_materials_handoff_checks_real_elf_and_records_hashes(self):
        self.prepare_static_materials()
        self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
        )
        receipt = json.loads((self.materials / "materials.json").read_text())
        hypervisor = self.materials / "cloud-hypervisor"
        expected = "sha256:" + hashlib.sha256(hypervisor.read_bytes()).hexdigest()
        self.assertEqual(receipt["cloud_hypervisor"]["sha256"], expected)
        self.assertTrue(receipt["kernel"]["vmgenid_linked"])
        self.assertTrue(receipt["kernel"]["requirements_checked"])
        self.assertEqual(
            json.loads((self.materials / "cloud-hypervisor.json").read_text())["commit"],
            REVISION,
        )

    def test_materials_reject_wrong_revision(self):
        self.prepare_static_materials(revision="wrong-revision")
        self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
            expected=1,
        )
        self.assertFalse((self.materials / "materials.json").exists())

    def test_materials_reject_dynamic_elf(self):
        self.prepare_static_materials()
        shutil.copyfile(Path(sys.executable).resolve(), self.materials / "cloud-hypervisor")
        result = self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
            expected=1,
        )
        self.assertIn("must be static", result.stderr)
        self.assertFalse((self.materials / "materials.json").exists())

    def test_materials_reject_missing_kernel_symbols(self):
        self.prepare_static_materials(symbols=False)
        self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
            expected=1,
        )
        self.assertFalse((self.materials / "materials.json").exists())

    def test_materials_reject_invalid_boot_header_or_source_identity(self):
        self.prepare_static_materials()
        (self.materials / "bzImage").write_bytes(b"not a boot image")
        result = self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
            expected=1,
        )
        self.assertIn("Invalid Linux bzImage", result.stderr)
        self.prepare_static_materials()
        (self.materials / "kernel-source.json").write_text("{}")
        result = self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
            expected=1,
        )
        self.assertIn("source_hash", result.stderr)
        self.assertFalse((self.materials / "materials.json").exists())

    def test_materials_reject_missing_config_requirement(self):
        self.prepare_static_materials()
        (self.materials / "kernel.config").write_text("")
        result = self.run_command(
            "bash", "scripts/prepare-acceptance-materials.sh", str(self.materials),
            expected=1,
        )
        self.assertIn("required built-in kernel option missing", result.stderr)
        self.assertFalse((self.materials / "materials.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
