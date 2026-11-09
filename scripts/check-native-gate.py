#!/usr/bin/env python3
"""Require actual native image coverage using a caller-prepared toolchain."""

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
ENGINE_PACKAGE = "github.com/woxQAQ/sandbox-engine"
REQUIRED_TESTS = (
    "TestActualLayeredEROFSImportAndExtraction",
    "TestActualSharedOCIHardlinksAndMetadata",
    "TestActualMergedOCIHardlinksAndMetadata",
    "TestGCActualLayerCacheAndCompletedImport",
)
ROOT_SKIP = "TestActualMergedOCIInvalidInputsDoNotPublish/unmapped_owner"
NATIVE_TESTS = re.compile(
    r"^Test(Actual|GCActual|LayerValidation|LegacyNativeLayered|SharedCached|MergedOCI)"
)
ROOT_REASON = re.compile(
    r"[ \t]+image_compat_test\.go:[0-9]+: root can identity-map all owners"
)
MISSING_BWRAP = re.compile(
    r"[ \t]+fsmeta_test\.go:[0-9]+: incompatible: required OCI tool missing: bwrap\n"
)
REQUIRED_ENV = {"SANDBOX_REQUIRE_EROFS_TESTS": "1", "GOWORK": "off"}
NAMESPACES = ("user", "mnt", "pid", "net")

# The probe needs only the supplied shell, id and readlink, not another
# interpreter or image executable inside the readonly namespace.
IDENTITY_PROBE = r"""
printf 'uid=%s\neuid=%s\ngid=%s\negid=%s\n' \
    "$("$1" -ru)" "$("$1" -u)" "$("$1" -rg)" "$("$1" -g)"
for ns in user mnt pid net; do
    printf 'ns_%s=%s\n' "$ns" "$("$2" "/proc/self/ns/$ns")"
done
for kind in uid gid; do
    printf '%s_map=' "$kind"
    while read -r inside outside count; do
        printf '%s %s %s;' "$inside" "$outside" "$count"
    done < "/proc/self/${kind}_map"
    printf '\n'
done
"""


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def capture(logs, name, command, env=None):
    result = subprocess.run(command, env=env, capture_output=True, check=False)
    # Preserve raw output and the original child status before interpretation.
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
            "package": event["Package"], "test": event.get("Test", ""),
            "output": output,
        })
    save_json(logs / "skips.json", skipped)
    for skip in skipped:
        if not NATIVE_TESTS.match(skip["test"]):
            continue
        reasons = [line for line in skip["output"].splitlines()
                   if not re.match(r"^(=== RUN|[ \t]*--- SKIP)", line)]
        exception = (
            os.geteuid() == 0 and skip["package"] == ENGINE_PACKAGE
            and skip["test"] == ROOT_SKIP and len(reasons) == 1
            and ROOT_REASON.fullmatch(reasons[0]) is not None
        )
        require(exception, f"required native test skipped: {skip['test']}")
    require(result.returncode == 0, f"test command failed with exit {result.returncode}")
    require(not any(e["Action"] in ("fail", "build-fail") for e in stream),
            "Go JSON contains a failure")
    for test in required:
        require(any(e["Action"] == "pass" and e["Package"] == ENGINE_PACKAGE
                    and e.get("Test") == test for e in stream),
                f"missing native pass event: {test}")


def executable(name):
    path = shutil.which(name)
    require(path is not None, f"required gate tool missing: {name}")
    return str(Path(path).resolve())


def tool_versions(logs):
    versions = {}
    tools = ("go", "make", "python3", "golangci-lint", "unshare", "id",
             "readlink", "bwrap", "mkfs.erofs", "fsck.erofs", "dump.erofs", "umoci")
    for tool in tools:
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


def readonly_roots():
    configured = os.environ.get("SANDBOX_IMAGE_TOOL_ROOTS", "")
    extra = configured.split(os.pathsep) if configured else []
    for root in extra:
        require(os.path.isabs(root) and os.path.normpath(root) == root and root != "/",
                f"invalid tool root: {root}")
        require(Path(root).is_dir(), f"missing tool root: {root}")
        for reserved in ("/proc", "/dev", "/tmp", "/work", "/tools", "/input", "/merged"):
            require(root != reserved and not root.startswith(reserved + "/")
                    and not reserved.startswith(root + "/"),
                    f"tool root overlaps private path: {root}")
    return [root for root in ("/usr", "/bin", "/lib", "/lib64")
            if Path(root).exists()] + extra


def identity(logs, name, command):
    result = capture(logs, name, command)
    require(result.returncode == 0, f"{name} probe failed")
    fields = {}
    for line in result.stdout.decode().splitlines():
        key, value = line.split("=", 1)
        require(key not in fields, f"duplicate identity field: {key}")
        fields[key] = value
    state = {key: int(fields[key]) for key in ("uid", "euid", "gid", "egid")}
    state["namespaces"] = {ns: fields[f"ns_{ns}"] for ns in NAMESPACES}
    for namespace, value in state["namespaces"].items():
        require(re.fullmatch(rf"{namespace}:\[\d+\]", value) is not None,
                f"invalid namespace identity: {namespace}")
    for kind in ("uid_map", "gid_map"):
        state[kind] = [list(map(int, row.split()))
                       for row in fields[kind].split(";") if row]
    save_json(logs / f"{name}.json", state)
    return state


def preflight(logs):
    require(os.geteuid() == 0,
            "native CI preflight requires effective UID 0 on an isolated runner; no automatic sudo")
    tool_versions(logs)
    shell, user_id, readlink = map(executable, ("sh", "id", "readlink"))
    probe = [shell, "-ec", IDENTITY_PROBE, "native-gate-probe", user_id, readlink]
    before = identity(logs, "outer", probe)
    args = [
        executable("bwrap"), "--die-with-parent", "--new-session", "--unshare-all",
        "--uid", "0", "--gid", "0", "--clearenv", "--setenv", "LC_ALL", "C",
        "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--chdir", "/",
    ]
    for root in readonly_roots():
        args.extend(("--ro-bind", root, root))
    inside = identity(logs, "shared-namespace", args + ["--"] + probe)
    for key in ("uid", "euid", "gid", "egid"):
        require(inside[key] == 0, f"shared namespace identity is not root: {key}")
    for ns in NAMESPACES:
        require(before["namespaces"][ns] != inside["namespaces"][ns],
                f"shared namespace unchanged: {ns}")
    mapped = identity(logs, "merged-mapping", [
        executable("unshare"), "--user", "--map-users=0:0:4294967295",
        "--map-groups=0:0:4294967295", "--keep-caps",
    ] + probe)
    for key in ("uid", "euid", "gid", "egid"):
        require(mapped[key] == 0, f"mapped identity is not root: {key}")
    require(mapped["namespaces"]["user"] != before["namespaces"]["user"],
            "merged user namespace unchanged")
    for kind in ("uid_map", "gid_map"):
        require(mapped[kind] == [[0, 0, 4294967295]],
                f"merged mapping must cover all valid Linux IDs: {kind}")
    test = "TestActualMergedOCIExplicitSelectionAndVerification"
    result = capture(logs, "merged-production", [
        "go", "-C", "engine", "test", "-json", "-run", f"^{test}$",
        "-count=1", "-timeout=60s", ".",
    ], env={**os.environ, **REQUIRED_ENV})
    check_positive(logs, result, (test,))


def negative(logs):
    go = executable("go")
    view = logs / "tool-view"
    view.mkdir()
    try:
        # Only this command receives the projected PATH. Installed tools and
        # compiler wrappers stay untouched, including alternate dependency roots.
        for directory in os.environ.get("PATH", "").split(os.pathsep):
            if not Path(directory).is_dir():
                continue
            for entry in Path(directory).iterdir():
                link = view / entry.name
                if (entry.name != "bwrap" and entry.is_file()
                        and os.access(entry, os.X_OK)
                        and not link.is_symlink() and not link.exists()):
                    link.symlink_to(entry.absolute())
        require(shutil.which("bwrap", path=str(view)) is None,
                "negative PATH still contains bwrap")
        test = REQUIRED_TESTS[0]
        result = capture(logs, "negative", [
            go, "-C", "engine", "test", "-json", "-run", f"^{test}$",
            "-count=1", "-timeout=60s", ".",
        ], env={**os.environ, **REQUIRED_ENV, "PATH": str(view)})
        stream = events(result)
        require(result.returncode != 0, "missing bwrap unexpectedly succeeded")
        require(any(e["Package"] == ENGINE_PACKAGE and e.get("Test") == test
                    and e["Action"] == "output"
                    and MISSING_BWRAP.fullmatch(e.get("Output", "")) is not None
                    for e in stream),
                "negative command did not report the exact missing-bwrap test diagnostic")
        require(any(e["Package"] == ENGINE_PACKAGE and e.get("Test") == test
                    and e["Action"] == "fail" for e in stream),
                "negative command did not execute and fail the selected test")
    finally:
        shutil.rmtree(view)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--preflight", action="store_true",
                       help="prove root namespaces/mapping and run the merged importer")
    modes.add_argument("--negative", action="store_true",
                       help="require the selected test to fail for missing bwrap")
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
            require(capture(logs, name, git + args).returncode == 0,
                    f"cannot record {name}")
        save_json(logs / "entrypoints.json", [
            {"file": file, "sha256": hashlib.sha256(Path(file).read_bytes()).hexdigest()}
            for file in ("Makefile", "scripts/check-native-gate.py", ".github/workflows/check.yml")
        ])
        # env -i forwards no HOME or credentials. Go still needs private cache
        # state, which is never included in the diagnostic artifact.
        if not os.environ.get("HOME"):
            home = logs / "runtime-home"
            home.mkdir(mode=0o700)
            os.environ["HOME"] = str(home)
        if options.preflight:
            preflight(logs)
        elif options.negative:
            negative(logs)
        else:
            result = capture(logs, "test-native", ["make", "--no-print-directory", "test-native"])
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
