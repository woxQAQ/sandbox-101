#!/usr/bin/env python3
"""Require actual native image coverage using a caller-prepared host toolchain."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


PROJECT = Path(__file__).resolve().parent.parent
NATIVE_PACKAGE = "github.com/woxQAQ/haven/tests/native"
REQUIRED_TESTS = (
    "TestActualLayeredEROFSImportAndExtraction",
    "TestActualSharedOCIHardlinksAndMetadata",
    "TestActualMergedOCIHardlinksAndMetadata",
    "TestGCActualLayerCacheAndCompletedImport",
)
NATIVE_TESTS = re.compile(
    r"^Test(HavenActual|Actual|GCActual|LayerValidation|LegacyNativeLayered|SharedCached|MergedOCI)"
)
MISSING_MKFS = re.compile(
    r"[ \t]+fsmeta_test\.go:[0-9]+: required OCI tool missing: mkfs\.erofs\n"
)
REQUIRED_ENV = {"SANDBOX_REQUIRE_EROFS_TESTS": "1", "GOWORK": "off"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def capture(logs, name, command, env=None):
    result = subprocess.run(command, env=env, capture_output=True, check=False)
    (logs / f"{name}.stdout").write_bytes(result.stdout)
    (logs / f"{name}.stderr").write_bytes(result.stderr)
    (logs / f"{name}.exit-code").write_text(str(result.returncode) + "\n")
    return result


def events(result):
    stream = []
    actions = {
        "start", "run", "pause", "cont", "pass", "bench", "fail",
        "output", "skip", "build-output", "build-fail",
    }
    for line in result.stdout.decode("utf-8").splitlines():
        event = json.loads(line)
        require(isinstance(event, dict), "Go JSON event is not an object")
        require(event.get("Action") in actions, "invalid Go JSON action")
        require(isinstance(event.get("Package"), str) and event["Package"],
                "Go JSON event has no package")
        require(isinstance(event.get("Test", ""), str), "invalid Go JSON test name")
        require(isinstance(event.get("Output", ""), str), "invalid Go JSON output")
        stream.append(event)
    return stream


def check_positive(logs, result, required):
    stream = events(result)
    skipped = []
    for event in stream:
        if event["Action"] != "skip":
            continue
        output = "".join(row.get("Output", "") for row in stream
                         if row["Action"] == "output"
                         and row["Package"] == event["Package"]
                         and row.get("Test") == event.get("Test"))
        skipped.append({
            "package": event["Package"], "test": event.get("Test", ""), "output": output,
        })
    save_json(logs / "skips.json", skipped)
    for skip in skipped:
        require(not NATIVE_TESTS.match(skip["test"]),
                f"required native test skipped: {skip['test']}")
    require(result.returncode == 0, f"test command failed with exit {result.returncode}")
    require(not any(e["Action"] in ("fail", "build-fail") for e in stream),
            "Go JSON contains a failure")
    for test in required:
        require(any(e["Action"] == "pass" and e["Package"] == NATIVE_PACKAGE
                    and e.get("Test") == test for e in stream),
                f"missing native pass event: {test}")


def executable(name):
    path = shutil.which(name)
    require(path is not None, f"required gate tool missing: {name}")
    return str(Path(path).resolve())


def tool_versions(logs):
    versions = {}
    for tool in ("go", "just", "python3", "golangci-lint", "mkfs.erofs", "fsck.erofs", "dump.erofs", "umoci"):
        path = executable(tool)
        result = capture(logs, f"version-{tool}",
                         [path, "version" if tool == "go" else "--version"])
        require(result.returncode == 0, f"version check failed: {tool}")
        versions[tool] = {
            "path": path, "version": (result.stdout + result.stderr).decode().strip(),
        }
        save_json(logs / "tools.json", versions)
    erofs = []
    for tool in ("mkfs.erofs", "fsck.erofs"):
        match = re.search(r"\b(\d+)\.(\d+)\.(\d+)\b", versions[tool]["version"])
        require(match is not None, f"unrecognized erofs-utils version: {tool}")
        version = tuple(map(int, match.groups()))
        require((1, 9, 4) <= version < (2, 0, 0), "erofs-utils must be >=1.9.4 and <2")
        erofs.append(version)
    require(erofs[0] == erofs[1], "mkfs.erofs and fsck.erofs must match")
    require(versions["umoci"]["version"].splitlines()[0] == "umoci version 0.6.0",
            "umoci must be 0.6.0")


def preflight(logs):
    require(os.geteuid() == 0,
            "native CI preflight requires effective UID 0 for native ownership; no automatic sudo")
    tool_versions(logs)
    save_json(logs / "host-identity.json", {
        "uid": os.getuid(), "euid": os.geteuid(), "gid": os.getgid(), "egid": os.getegid(),
    })
    test = "TestActualMergedOCIExplicitSelectionAndVerification"
    result = capture(logs, "merged-import", [
        "go", "-C", "haven", "test", "-json", "-run", f"^{test}$",
        "-count=1", "-timeout=60s", "./tests/native",
    ], env={**os.environ, **REQUIRED_ENV})
    check_positive(logs, result, (test,))


def negative(logs):
    go = executable("go")
    view = logs / "tool-view"
    view.mkdir()
    try:
        # Only this command receives the projected PATH; installed tools remain untouched.
        for directory in os.environ.get("PATH", "").split(os.pathsep):
            if not Path(directory).is_dir():
                continue
            for entry in Path(directory).iterdir():
                link = view / entry.name
                if (entry.name != "mkfs.erofs" and entry.is_file()
                        and os.access(entry, os.X_OK)
                        and not link.is_symlink() and not link.exists()):
                    link.symlink_to(entry.absolute())
        require(shutil.which("mkfs.erofs", path=str(view)) is None,
                "negative PATH still contains mkfs.erofs")
        test = REQUIRED_TESTS[0]
        result = capture(logs, "negative", [
            go, "-C", "haven", "test", "-json", "-run", f"^{test}$",
            "-count=1", "-timeout=60s", "./tests/native",
        ], env={**os.environ, **REQUIRED_ENV, "PATH": str(view)})
        stream = events(result)
        require(result.returncode != 0, "missing mkfs.erofs unexpectedly succeeded")
        require(any(e["Package"] == NATIVE_PACKAGE and e.get("Test") == test
                    and e["Action"] == "output"
                    and MISSING_MKFS.fullmatch(e.get("Output", "")) is not None
                    for e in stream),
                "negative command did not report the exact missing-mkfs test diagnostic")
        require(any(e["Package"] == NATIVE_PACKAGE and e.get("Test") == test
                    and e["Action"] == "fail" for e in stream),
                "negative command did not execute and fail the selected test")
    finally:
        shutil.rmtree(view)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--preflight", action="store_true",
                       help="check host tools and ownership privileges, then run the merged importer")
    modes.add_argument("--negative", action="store_true",
                       help="require the selected test to fail for missing mkfs.erofs")
    options = parser.parse_args()
    os.chdir(PROJECT)
    for directory in (Path("build"), Path("build/native-gate")):
        require(not directory.is_symlink(), f"refusing symlink log directory: {directory}")
        directory.mkdir(exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run.", dir=PROJECT / "build/native-gate"))
    print(f"native gate logs: {logs}", flush=True)
    try:
        save_json(logs / "invocation.json", {
            "euid": os.geteuid(), "preflight": options.preflight, "negative": options.negative,
        })
        git = ["git", "-c", f"safe.directory={PROJECT}"]
        for name, args in (("source", ["rev-parse", "HEAD"]),
                           ("changed-paths", ["diff", "--name-only", "HEAD"])):
            require(capture(logs, name, git + args).returncode == 0, f"cannot record {name}")
        save_json(logs / "entrypoints.json", [
            {"file": file, "sha256": hashlib.sha256(Path(file).read_bytes()).hexdigest()}
            for file in ("justfile", "scripts/check-native-gate.py", ".github/workflows/check.yml")
        ])
        if not os.environ.get("HOME"):
            home = logs / "runtime-home"
            home.mkdir(mode=0o700)
            os.environ["HOME"] = str(home)
        if options.preflight:
            preflight(logs)
        elif options.negative:
            negative(logs)
        else:
            result = capture(logs, "test-native", ["just", "test-native"])
            check_positive(logs, result, REQUIRED_TESTS)
        (logs / "result.txt").write_text("PASS\n")
        print("native gate PASS")
        return 0
    except Exception as error:
        (logs / "result.txt").write_text(str(error) + "\n")
        print(f"native gate FAIL: {error}; logs: {logs}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
