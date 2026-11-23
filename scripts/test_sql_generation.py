#!/usr/bin/env python3
"""Exercise the sqlc drift gate against an owned copy of its inputs and outputs."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    with tempfile.TemporaryDirectory(prefix="haven-sqlc-gate-") as temporary:
        root = Path(temporary)
        (root / "scripts").mkdir()
        shutil.copyfile(ROOT / "scripts/check-sql.py", root / "scripts/check-sql.py")
        (root / "tools").symlink_to(ROOT / "tools", target_is_directory=True)
        haven = root / "haven"
        haven.mkdir()
        shutil.copyfile(ROOT / "haven/sqlc.yaml", haven / "sqlc.yaml")
        for source in (ROOT / "haven").rglob("*"):
            if source.is_file() and (
                source.suffix == ".sql" or "/internal/dbgen/" in source.as_posix()
            ):
                destination = haven / source.relative_to(ROOT / "haven")
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)

        def check(expected, reason="stale, missing or unexpected"):
            result = subprocess.run(
                [sys.executable, "-I", str(root / "scripts/check-sql.py")],
                capture_output=True, text=True, check=False,
            )
            if (result.returncode == 0) != expected:
                raise RuntimeError(result.stdout + result.stderr)
            if not expected and reason not in result.stderr:
                raise RuntimeError("negative case failed for another reason: " + result.stderr)

        check(True)
        generated = haven / "operation/internal/dbgen/queries.sql.go"
        original = generated.read_bytes()
        generated.unlink()
        check(False)
        generated.write_bytes(original + b"\n// unexpected edit\n")
        check(False)
        generated.write_bytes(original)
        unexpected = generated.parent / "unexpected.go"
        unexpected.write_text("package dbgen\n")
        check(False)
        unexpected.unlink()
        check(True)
        queries = haven / "supply/queries.sql"
        original_queries = queries.read_bytes()
        queries.write_bytes(original_queries + b"\n-- name: MixedParameters :execrows\nUPDATE template_owners SET candidate_operation_id=sqlc.arg('candidate_operation_id') WHERE template_id=?;\n")
        check(False, "mixed sqlc named and positional parameters")
        queries.write_bytes(original_queries)
        check(True)
        queries.write_bytes(original_queries + b"\n-- name: UnexpandedParameter :one\nSELECT COUNT(*) FROM template_owners WHERE NOT (template_id=sqlc.arg(template_id));\n")
        check(False, "unexpanded sqlc parameter")
        queries.write_bytes(original_queries)
        check(True)
        print("PASS: sqlc gate accepts clean output and rejects missing, edited, extra files, mixed parameters and unexpanded macros")


if __name__ == "__main__":
    main()
