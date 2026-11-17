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
CONTROL_PACKAGES = (
    "store", "execution", "access", "authorization", "operation", "ownership",
    "sandbox", "image", "quota", "guest",
    "machine", "supply", "data", "collection", "node",
)
COMPOSITION_PACKAGES = ("internal/application", "internal/publication", "internal/recovery", "internal/maintenance")
DOMAIN_PACKAGES = CONTROL_PACKAGES
RETIRED_PACKAGES = ("services", "template", "pool", "vm", "snapshot")


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


def source_violations(graph):
    result = subprocess.run(
        ["go", "-C", str(ROOT / "tools"), "run", str(ROOT / "scripts/check-haven-shape.go"), str(ROOT)],
        cwd=ROOT,
        env={**os.environ, "GOWORK": "off"},
        input=json.dumps(list(graph.values())), text=True, capture_output=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "Go source boundary check failed")
    return json.loads(result.stdout)


def within(package, prefix):
    return package == prefix or package.startswith(prefix + "/")


def application_package(package):
    return package == MODULE or within(package, MODULE + "/internal/application")


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
    return package == MODULE or any(within(package, prefix) for prefix in prefixes) or any(
        within(package, MODULE + "/" + name) for name in CONTROL_PACKAGES
    )


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
        for required in ("/internal/application", "/cmd/haven", "/sdk/cmd/havenctl", "/loom/cmd/loomctl", "/types", "/sdk"):
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
                or application_package(name)
                or any(within(name, MODULE + "/" + domain) for domain in CONTROL_PACKAGES)
                or within(name, MODULE + "/egress")
                or within(name, "database/sql")
                or within(name, "modernc.org/sqlite")
                or within(name, "github.com/woxQAQ/sandbox-engine")
                or within(name, "github.com/woxQAQ/sandboxd"),
            )
        )
        violations.extend(violation_paths(
            final_graph,
            [MODULE + "/cmd/haven", MODULE + "/internal/application"],
            lambda name: within(name, "github.com/woxQAQ/sandbox-engine")
            or within(name, "github.com/woxQAQ/sandboxd"),
        ))
        domain_roots = [name for name in final_graph if any(
            within(name, MODULE + "/" + domain) for domain in DOMAIN_PACKAGES
        )]
        violations.extend(violation_paths(
            final_graph, domain_roots,
            lambda name: application_package(name)
            or any(within(name, MODULE + "/" + layer)
                   for layer in ("services", "httpapi", "management", "cmd", *COMPOSITION_PACKAGES)),
        ))
        higher_capabilities = ("image", "machine", "supply", "sandbox", "data", "collection", "node")
        for capability, forbidden in (
            ("image", higher_capabilities[1:]),
            ("machine", ("supply", "sandbox", "data", "collection", "node")),
            ("supply", ("sandbox", "data", "collection", "node")),
            ("sandbox", ("data", "collection", "node")),
            ("authorization", higher_capabilities),
            ("operation", higher_capabilities),
            ("quota", higher_capabilities),
        ):
            roots = [name for name in final_graph if within(name, MODULE + "/" + capability)]
            violations.extend(violation_paths(
                final_graph, roots,
                lambda name, forbidden=forbidden: any(
                    within(name, MODULE + "/" + owner) for owner in forbidden
                ),
            ))
        for name in final_graph:
            if any(within(name, MODULE + "/" + retired) for retired in RETIRED_PACKAGES):
                violations.append(f"{name}: retired capability package restored")
        controller_roots = [name for name in final_graph if any(
            within(name, MODULE + "/" + layer) for layer in ("httpapi", "httpwire", "management")
        )]
        violations.extend(violation_paths(
            final_graph, controller_roots,
            lambda name: application_package(name) or within(name, MODULE + "/cmd"),
        ))
        for name in controller_roots:
            for dependency in final_graph[name].get("Imports", []):
                if any(within(dependency, MODULE + "/" + domain)
                       for domain in DOMAIN_PACKAGES if domain not in ("authorization", "operation", "sandbox", "supply", "data", "node")) or any(
                    within(dependency, MODULE + "/loom/" + effect) for effect in ("host", "imageimport")
                ):
                    violations.append(f"{name} -> {dependency} (controller bypasses business capabilities)")
        base_fixtures = [name for name in final_graph if within(name, MODULE + "/internal/testfixture")]
        violations.extend(violation_paths(
            final_graph, base_fixtures,
            lambda name: application_package(name) or within(name, MODULE + "/internal/testapp"),
        ))
        violations.extend(source_violations(final_graph))
        for name, package in final_graph.items():
            for dependency in package.get("Imports", []):
                if any(within(dependency, MODULE + "/internal/" + fixture) for fixture in ("testfixture", "testapp")) and not (
                    within(name, MODULE + "/internal/testfixture") or within(name, MODULE + "/internal/testapp") or within(name, MODULE + "/tests")
                ):
                    violations.append(f"{name} -> {dependency} (production imports test fixture)")
                if dependency.startswith(MODULE + "/") and "/internal/dbgen" in dependency:
                    owner = dependency.split("/internal/dbgen", 1)[0]
                    if not within(name, owner):
                        violations.append(f"{name} -> {dependency} (foreign generated queries)")
        if MODULE + "/loom/host" not in final_graph[MODULE + "/cmd/haven"].get("Deps", []):
            raise RuntimeError("Haven command lacks the real host runtime")
    if violations:
        print("Architecture violations:\n" + "\n".join(sorted(set(violations))), file=sys.stderr)
        return 1
    if args.phase == "loom":
        print("PASS: Loom dependency boundary (extraction only; final gate not run)")
    else:
        print("PASS: compiled import graph, App composition and SQL ownership boundaries")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, OSError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
