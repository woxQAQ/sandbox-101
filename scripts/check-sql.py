#!/usr/bin/env python3
"""Verify sqlc output in a private tree without rewriting reviewed source."""

from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
HAVEN = ROOT / "haven"
SQL_QUOTED = re.compile(
    r"'(?:(?:'')|[^'])*'|\"(?:(?:\"\")|[^\"])*\"|"
    r"`(?:(?:``)|[^`])*`|\[[^\]]*\]|--[^\n]*|/\*.*?\*/", re.S
)


def generated_files(root):
    return {
        path.relative_to(root): path.read_bytes()
        for directory in root.glob("**/internal/dbgen")
        for path in directory.rglob("*")
        if path.is_file()
    }


def check_query_source(root):
    # sqlc v1.30.0 can misnumber SQLite placeholders when a query mixes
    # sqlc.arg/narg macros with positional parameters. Check each query, ignoring
    # quoted SQL values, identifiers and comments that may contain question marks.
    for source in root.rglob("*.sql"):
        for query in re.split(r"(?m)^-- name:", source.read_text())[1:]:
            name, _, body = query.partition("\n")
            sql = SQL_QUOTED.sub("", body)
            if re.search(r"\b(?:DELETE\s+FROM|REPLACE\s+INTO)\b", sql, re.I):
                raise SystemExit(
                    f"FAIL: physical deletion in {source.relative_to(root)}:{name.strip()}"
                )
            if re.search(r"\bsqlc\.(?:arg|narg)\s*\(", sql) and "?" in sql:
                raise SystemExit(
                    f"FAIL: mixed sqlc named and positional parameters in {source.relative_to(root)}:{name.strip()}"
                )


def check_generated_queries(files):
    # A pinned generator can leave a macro below an unsupported SQL AST node.
    # Such output can reproduce byte-for-byte while still being invalid SQL.
    for path, contents in files.items():
        if not str(path).endswith(".sql.go"):
            continue
        for query in re.findall(r"(?m)^const \w+ = `([^`]*)`", contents.decode()):
            sql = SQL_QUOTED.sub("", query)
            if re.search(r"\bsqlc\.(?:arg|narg)\s*\(", sql):
                raise SystemExit(f"FAIL: unexpanded sqlc parameter in generated {path}")


def main():
    check_query_source(HAVEN)
    with tempfile.TemporaryDirectory(prefix="haven-sqlc-") as temporary:
        target = Path(temporary)
        shutil.copyfile(HAVEN / "sqlc.yaml", target / "sqlc.yaml")
        for source in HAVEN.rglob("*.sql"):
            destination = target / source.relative_to(HAVEN)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        subprocess.run(
            ["go", "-C", str(ROOT / "tools"), "tool", "sqlc", "generate",
             "-f", str(target / "sqlc.yaml")],
            env={**os.environ, "GOWORK": "off"},
            check=True,
        )
        expected = generated_files(target)
        actual = generated_files(HAVEN)
        if not expected:
            raise SystemExit("FAIL: sqlc generated no files")
        check_generated_queries(expected)
        differences = sorted(
            path for path in expected.keys() | actual.keys()
            if expected.get(path) != actual.get(path)
        )
        if differences:
            raise SystemExit(
                "FAIL: stale, missing or unexpected sqlc output; run just generate-sql:\n"
                + "\n".join(str(path) for path in differences)
            )
        print(f"PASS: {len(expected)} sqlc files match the pinned generator")


if __name__ == "__main__":
    main()
