#!/usr/bin/env python3
"""Exercise the native gate CLI with private command fixtures, without privileges."""

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parent.parent
PACKAGE = "github.com/woxQAQ/sandbox-engine"
TESTS = (
    "TestActualLayeredEROFSImportAndExtraction",
    "TestActualSharedOCIHardlinksAndMetadata",
    "TestActualMergedOCIHardlinksAndMetadata",
    "TestGCActualLayerCacheAndCompletedImport",
)
ROOT_SKIP = "TestActualMergedOCIInvalidInputsDoNotPublish/unmapped_owner"


def event(action, test=None, output=None, package=PACKAGE):
    row = {"Action": action, "Package": package}
    if test is not None:
        row["Test"] = test
    if output is not None:
        row["Output"] = output
    return row


class NativeGateEntrypoints(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="native-gate-cli-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "workspace with spaces"
        for directory in ("scripts", ".github/workflows", "tools", "home"):
            (self.root / directory).mkdir(parents=True, exist_ok=True)
        self.gate = self.root / "scripts/check-native-gate.py"
        shutil.copyfile(PROJECT / "scripts/check-native-gate.py", self.gate)
        (self.root / "Makefile").write_text("# command fixture\n")
        (self.root / ".github/workflows/check.yml").write_text("# command fixture\n")
        self.result_file = self.root / "command-result.json"
        self.env = {
            "PATH": str(self.root / "tools"), "HOME": str(self.root / "home"),
            "NATIVE_TEST_RESULT": str(self.result_file),
        }
        # There is deliberately no nu, package manager or real Go executable.
        self.tool("git", """
import sys
if sys.argv[-2:] == ["rev-parse", "HEAD"]:
    print("synthetic-native-gate-fixture")
""")
        command = """
import json, os, sys
from pathlib import Path
if Path(sys.argv[0]).name == "make":
    assert sys.argv[1:] == ["--no-print-directory", "test-native"]
else:
    assert sys.argv[1:] == ["-C", "engine", "test", "-json", "-run",
        "^TestActualLayeredEROFSImportAndExtraction$", "-count=1", "-timeout=60s", "."]
    assert os.environ["GOWORK"] == "off"
    assert os.environ["SANDBOX_REQUIRE_EROFS_TESTS"] == "1"
    assert not (Path(os.environ["PATH"]) / "bwrap").exists()
result = json.loads(Path(os.environ["NATIVE_TEST_RESULT"]).read_text())
sys.stdout.write(result["stdout"])
sys.stderr.write(result["stderr"])
sys.exit(result["exit_code"])
"""
        for name in ("make", "go"):
            self.tool(name, command)
        self.passes = [event("pass", name) for name in TESTS] + [event("pass")]

    def tool(self, name, body):
        path = self.root / "tools" / name
        path.write_text(f"#!{sys.executable}\n{body}")
        path.chmod(0o755)

    def run_gate(self, stream, *, code=0, expected=0, args=(), home=True):
        stdout = stream if isinstance(stream, str) else "".join(
            json.dumps(row) + "\n" for row in stream
        )
        self.result_file.write_text(json.dumps({
            "stdout": stdout, "stderr": "owned command diagnostic\n", "exit_code": code,
        }))
        env = dict(self.env)
        if not home:
            env.pop("HOME")
        result = subprocess.run(
            [sys.executable, "-I", str(self.gate), *args],
            cwd=self.temporary.name, env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        log = Path(result.stdout.splitlines()[0].removeprefix("native gate logs: "))
        self.assertTrue(log.is_relative_to(self.root / "build/native-gate"))
        self.assertFalse((log / "tool-view").exists())
        invocation = json.loads((log / "invocation.json").read_text())
        self.assertEqual(invocation["euid"], os.geteuid())
        for item in json.loads((log / "entrypoints.json").read_text()):
            self.assertEqual(len(item["sha256"]), 64)
        return log

    def test_required_coverage_and_raw_logs(self):
        log = self.run_gate(self.passes)
        self.assertEqual((log / "test-native.exit-code").read_text(), "0\n")
        self.assertEqual((log / "test-native.stderr").read_text(), "owned command diagnostic\n")
        self.assertEqual((log / "result.txt").read_text(), "PASS\n")

    def test_incomplete_or_invalid_coverage_is_rejected(self):
        cases = {
            "package-only": [event("pass")],
            "missing-one": self.passes[1:],
            "invalid-record": self.passes + [{}],
            "non-object": self.passes + [[]],
            "fail-event": self.passes + [event("fail", "TestOther")],
            "extra-native-skip": self.passes + [event("skip", "TestActualFutureNative")],
            "other-package-native-skip": self.passes + [
                event("skip", "TestActualFutureNative", package=PACKAGE + "/child"),
            ],
            "mixed-output": "compiler noise\n" + "".join(
                json.dumps(row) + "\n" for row in self.passes
            ),
        }
        for name, rows in cases.items():
            with self.subTest(name=name):
                self.run_gate(rows, expected=1)
        log = self.run_gate(self.passes, code=7, expected=1)
        self.assertEqual((log / "test-native.exit-code").read_text(), "7\n")

    def test_unrelated_skips_are_recorded(self):
        log = self.run_gate(self.passes + [event("skip", "TestCaptureOnly")])
        self.assertEqual(json.loads((log / "skips.json").read_text())[0]["test"],
                         "TestCaptureOnly")

    def test_root_skip_uses_actual_identity_and_exact_reason(self):
        self.tool("id", "print(0)\n")
        reason = "    image_compat_test.go:885: root can identity-map all owners\n"
        rows = self.passes + [event("output", ROOT_SKIP, reason), event("skip", ROOT_SKIP)]
        self.run_gate(rows, expected=0 if os.geteuid() == 0 else 1)
        for message in (
            "root can identity-map all owners\n",
            reason.rstrip() + " extra\n",
            reason + "    image_compat_test.go:886: unexpected\n",
            "    image_compat_test.go:885: namespace unavailable\n",
        ):
            with self.subTest(message=message):
                self.run_gate(self.passes + [
                    event("output", ROOT_SKIP, message), event("skip", ROOT_SKIP),
                ], expected=1)

    def test_negative_requires_exact_failed_test_and_cleans_path(self):
        # A real bwrap entry in the fixture must be absent only in the child view.
        self.tool("bwrap", 'raise RuntimeError("must not run")\n')
        reason = "    fsmeta_test.go:312: incompatible: required OCI tool missing: bwrap\n"
        rows = [event("output", TESTS[0], reason), event("fail", TESTS[0]), event("fail")]
        log = self.run_gate(rows, code=1, args=("--negative",))
        self.assertEqual((log / "negative.exit-code").read_text(), "1\n")
        self.assertTrue((self.root / "tools/bwrap").exists())
        for name, output in (
            ("compile-error", "compile failed\n"),
            ("wrong-tool", reason.replace("bwrap", "mkfs.erofs")),
            ("reason-suffix", reason.rstrip() + " extra\n"),
        ):
            with self.subTest(name=name):
                self.run_gate([event("output", TESTS[0], output), event("fail", TESTS[0])],
                              code=1, expected=1, args=("--negative",))
        self.run_gate(rows, code=0, expected=1, args=("--negative",))
        self.run_gate(rows[:1], code=1, expected=1, args=("--negative",))

    @unittest.skipIf(os.geteuid() == 0, "nonroot preflight refusal needs a nonroot caller")
    def test_preflight_never_elevates(self):
        self.tool("id", "print(0)\n")
        log = self.run_gate([], expected=1, args=("--preflight",))
        self.assertIn("requires effective UID 0", (log / "result.txt").read_text())

    def test_clean_environment_has_private_runtime_home(self):
        log = self.run_gate(self.passes, home=False)
        self.assertEqual(stat.S_IMODE((log / "runtime-home").stat().st_mode), 0o700)


if __name__ == "__main__":
    unittest.main(verbosity=2)
