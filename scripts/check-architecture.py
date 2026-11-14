#!/usr/bin/env python3
"""Check the compiled host dependency graph, including transitive imports."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MODULE = "github.com/woxQAQ/haven"


def packages(patterns):
    result = subprocess.run(
        ["go", "list", "-deps", "-json", *patterns],
        cwd=ROOT,
        env={**os.environ, "GOWORK": str(ROOT / "go.work")},
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "go list failed")
    decoder = json.JSONDecoder()
    remaining = result.stdout.lstrip()
    graph = {}
    while remaining:
        package, end = decoder.raw_decode(remaining)
        if package.get("Error") or package.get("DepsErrors"):
            raise RuntimeError(f"unresolved package: {package.get('ImportPath')}")
        graph[package["ImportPath"]] = package
        remaining = remaining[end:].lstrip()
    if not graph:
        raise RuntimeError("go list returned an empty graph")
    return graph


def within(package, prefix):
    return package == prefix or package.startswith(prefix + "/")


def forbidden_loom(package):
    prefixes = (
        "database/sql",
        "modernc.org/sqlite",
        "github.com/mattn/go-sqlite3",
        "github.com/gin-gonic/gin",
        "github.com/spf13/viper",
        "github.com/woxQAQ/sandbox-engine",
        "github.com/woxQAQ/sandboxd",
        MODULE + "/types",
        MODULE + "/egress",
        MODULE + "/httpapi",
        MODULE + "/httpwire",
        MODULE + "/management",
        MODULE + "/sdk",
        MODULE + "/internal",
        MODULE + "/cmd",
        MODULE + "/integration",
    )
    return package == MODULE or any(within(package, prefix) for prefix in prefixes)


def violation_paths(graph, roots, forbidden):
    violations = []
    for root in roots:
        queue = [(root, [root])]
        visited = set()
        while queue:
            package, path = queue.pop()
            if package in visited:
                continue
            visited.add(package)
            if forbidden(package):
                violations.append(" -> ".join(path))
                continue
            for dependency in graph[package].get("Imports", []):
                # cgo's pseudo-import has no separately listed package.
                if dependency == "C":
                    continue
                if dependency not in graph:
                    raise RuntimeError(f"dependency absent from graph: {dependency}")
                queue.append((dependency, [*path, dependency]))
    return violations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=("loom", "final"),
        default="final",
        help="loom checks extraction only; final also checks Haven and both protocol clients",
    )
    args = parser.parse_args()
    graph = packages(["./loom/..."])
    roots = [name for name in graph if within(name, MODULE + "/loom")
             and not within(name, MODULE + "/loom/cmd")
             and not within(name, MODULE + "/loom/config")]
    if not roots:
        raise RuntimeError("Loom packages are missing")
    violations = violation_paths(graph, roots, forbidden_loom)
    if args.phase == "final":
        for retired in ("engine", "sandboxd"):
            directory = ROOT / retired
            if (directory / "go.mod").exists() or any(directory.rglob("*.go")):
                raise RuntimeError(f"retired control module/source remains: {retired}")
        final_graph = packages([
            "./haven/...", "./sdk/...", "./types/...", "./loom/cmd/loomctl",
        ])
        for required in ("", "/cmd/haven", "/sdk/cmd/havenctl", "/loom/cmd/loomctl", "/types", "/sdk"):
            if MODULE + required not in final_graph:
                raise RuntimeError(f"required package missing: {required}")
        client_roots = [MODULE + "/" + name for name in ("sdk/cmd/havenctl", "loom/cmd/loomctl", "types", "sdk")]
        violations.extend(
            violation_paths(
                final_graph,
                client_roots,
                lambda name: name == MODULE + "/loom"
                or within(name, MODULE + "/loom/host")
                or within(name, MODULE + "/loom/imageimport")
                or name == MODULE
                or within(name, MODULE + "/egress")
                or within(name, "database/sql")
                or within(name, "modernc.org/sqlite")
                or within(name, "github.com/woxQAQ/sandbox-engine")
                or within(name, "github.com/woxQAQ/sandboxd"),
            )
        )
        violations.extend(violation_paths(
            final_graph,
            [MODULE + "/cmd/haven", MODULE],
            lambda name: within(name, "github.com/woxQAQ/sandbox-engine")
            or within(name, "github.com/woxQAQ/sandboxd"),
        ))
        if MODULE + "/loom/host" not in final_graph[MODULE + "/cmd/haven"].get("Deps", []):
            raise RuntimeError("Haven command lacks the real host runtime")
    if violations:
        print("Architecture violations:\n" + "\n".join(sorted(set(violations))), file=sys.stderr)
        return 1
    if args.phase == "loom":
        print("PASS: Loom dependency boundary (extraction only; final gate not run)")
    else:
        print("PASS: Loom, Haven runtime and business/management client dependency boundaries")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, OSError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
