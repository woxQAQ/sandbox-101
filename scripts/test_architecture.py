#!/usr/bin/env python3
"""Check architectural rejection using real Go imports in a private workspace."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = "github.com/woxQAQ/haven"


def main():
    with tempfile.TemporaryDirectory(prefix="haven-architecture-") as temporary:
        root = Path(temporary)
        (root / "scripts").mkdir()
        shutil.copyfile(ROOT / "scripts/check-architecture.py", root / "scripts/check-architecture.py")
        shutil.copyfile(ROOT / "scripts/check-haven-shape.go", root / "scripts/check-haven-shape.go")
        (root / "go.work").write_text("go 1.26.5\nuse (\n ./haven\n ./loom\n ./sdk\n ./types\n)\n")
        for module in ("haven", "loom", "sdk", "types"):
            directory = root / module
            directory.mkdir()
            import_path = MODULE if module == "haven" else MODULE + "/" + module
            (directory / "go.mod").write_text(f"module {import_path}\n\ngo 1.26.5\n")
        (root / "tools").mkdir()
        for name in ("go.mod", "go.sum"):
            shutil.copyfile(ROOT / "tools" / name, root / "tools" / name)
        sources = {
            "haven/internal/application/app.go": "package application\ntype App struct{}\nfunc Open() *App { return &App{} }\nfunc (app *App) Close() {}\n",
            "haven/internal/testfixture/fixture.go": "package testfixture\n",
            "haven/internal/testapp/fixture.go": "package testapp\n",
            "haven/cmd/haven/main.go": f'package main\nimport _ "{MODULE}/loom/host"\nfunc main() {{}}\n',
            "haven/data/data.go": "package data\n",
            "haven/image/image.go": "package image\n",
            "haven/image/internal/dbgen/queries.go": "package dbgen\ntype GetStepParams struct { StepKey string }\ntype GetDeletionReceiptParams struct { StepKey string }\n",
            "haven/internal/publication/projector.go": "package publication\n",
            "haven/internal/recovery/validation.go": "package recovery\n",
            "haven/authorization/domain.go": "package authorization\n",
            "haven/store/metadata.go": "package store\n",
            "haven/httpapi/http.go": f'package httpapi\nimport "{MODULE}/supply"\ntype Controller struct {{ Templates *supply.TemplateCatalog }}\n',
            "haven/supply/supply.go": "package supply\ntype TemplateCatalog struct{}\n",
            "haven/machine/machine.go": "package machine\n",
            "haven/sandbox/sandbox.go": "package sandbox\n",
            "haven/node/node.go": "package node\n",
            "loom/effects.go": "package loom\n",
            "loom/host/host.go": "package host\n",
            "loom/cmd/loomctl/main.go": "package main\nfunc main() {}\n",
            "sdk/client.go": "package sdk\n",
            "sdk/cmd/havenctl/main.go": "package main\nfunc main() {}\n",
            "types/types.go": "package types\n",
        }
        for owner in ("machine", "sandbox", "supply", "collection"):
            sources[f"haven/{owner}/internal/dbgen/queries.go"] = (
                "package dbgen\ntype GetStepParams struct { StepKey string }\n"
                "type InsertStepParams struct { StepKey string }\n"
                "type InsertCleanupIntentParams struct { ID string; ResourceKind string }\n"
            )
        for name, source in sources.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)

        def check(expected, dependency=""):
            result = subprocess.run(
                [sys.executable, "-I", str(root / "scripts/check-architecture.py")],
                capture_output=True, text=True, check=False,
            )
            if (result.returncode == 0) != expected:
                raise RuntimeError(result.stdout + result.stderr)
            if not expected and ("Architecture violations:" not in result.stderr or dependency not in result.stderr):
                raise RuntimeError("negative case failed for another reason: " + result.stderr)

        check(True)
        for source, package, dependency in (
            ("haven/authorization/domain.go", "authorization", "sandbox"),
            ("loom/effects.go", "loom", "authorization"),
            ("sdk/client.go", "sdk", "data"),
            ("haven/data/data.go", "data", "httpapi"),
            ("haven/image/image.go", "image", "node"),
            ("haven/image/image.go", "image", "internal/publication"),
            ("haven/image/image.go", "image", "internal/recovery"),
            ("haven/machine/machine.go", "machine", "supply"),
            ("haven/machine/machine.go", "machine", "sandbox"),
            ("haven/supply/supply.go", "supply", "sandbox"),
        ):
            path = root / source
            path.write_text(f'package {package}\nimport _ "{MODULE}/{dependency}"\n' + ("type TemplateCatalog struct{}\n" if package == "supply" else ""))
            check(False, MODULE + "/" + dependency)
            path.write_text(sources[source])
        for source, replacement, reason in (
            ("haven/httpapi/http.go", f'package httpapi\nimport _ "{MODULE}/internal/application"\n', MODULE + "/internal/application"),
            ("haven/httpapi/http.go", f'package httpapi\nimport _ "{MODULE}/store"\n', "controller bypasses business capabilities"),
            ("haven/data/data.go", f'package data\nimport _ "{MODULE}/internal/application"\n', MODULE + "/internal/application"),
            ("haven/data/data.go", f'package data\nimport _ "{MODULE}/internal/testfixture"\n', "production imports test fixture"),
            ("haven/data/data.go", f'package data\nimport _ "{MODULE}/internal/testapp"\n', "production imports test fixture"),
            ("haven/internal/testfixture/fixture.go", f'package testfixture\nimport _ "{MODULE}/internal/application"\n', MODULE + "/internal/application"),
            ("haven/internal/testfixture/fixture.go", f'package testfixture\nimport _ "{MODULE}/internal/testapp"\n', MODULE + "/internal/testapp"),
            ("haven/data/data.go", 'package data\nconst query = "SELECT id FROM operations"\n', "handwritten SQL"),
            ("haven/authorization/domain.go", 'package authorization\nimport "database/sql"\nfunc Read(db *sql.DB, query string) error { _, err := db.Exec(query); return err }\n', "raw SQL method"),
            ("haven/internal/application/app.go", sources["haven/internal/application/app.go"] + 'type Old = App\n', "root compatibility alias"),
            ("haven/internal/application/app.go", sources["haven/internal/application/app.go"] + 'func (app *App) Execute() {}\n', "root owns a business method"),
            ("haven/internal/application/app.go", 'package application\ntype component struct{}\ntype App struct{ *component }\n', "embeds a compatibility method set"),
        ):
            path = root / source
            path.write_text(replacement)
            check(False, reason)
            path.write_text(sources[source])
        check(True)
        for retired in ("services", "template", "pool", "vm", "snapshot"):
            directory = root / "haven" / retired
            directory.mkdir()
            legacy = directory / "legacy.go"
            legacy.write_text(f"package {retired}\n")
            check(False, "retired capability package restored")
            legacy.unlink()
            directory.rmdir()
        supply_path = root / "haven/supply/build.go"
        supply_source = (
            'package supply\nimport ("context"; "database/sql")\n'
            'type guest interface { Exec(context.Context, string, string, string) error }\n'
            'type Supply struct { opts struct { Guest guest }; db *sql.DB }\n'
            'func (service *Supply) runBuild() { '
            'service.opts.Guest.Exec(context.Background(), "resource", "operation", "command") }\n'
        )
        supply_path.write_text(supply_source)
        check(True)
        supply_path.write_text(supply_source.replace(
            'service.opts.Guest.Exec(context.Background(), "resource", "operation", "command")',
            'service.db.Exec("query")',
        ))
        check(False, "raw SQL method")
        supply_path.unlink()
        command_path = root / "haven/sandbox/sandbox.go"
        command_path.write_text(
            'package sandbox\nimport "database/sql"\n'
            'type commands struct{}\nfunc (*commands) Prepare(string) {}\n'
            'type service struct { commands *commands; db *sql.DB }\n'
        )
        for filename, function, argument in (
            ("configure.go", "admitConfiguration", "desired"),
            ("fork_admission.go", "AdmitFork", "command"),
            ("claim.go", "AdmitClaim", "command"),
        ):
            path = root / "haven/sandbox" / filename
            source = (
                f"package sandbox\nfunc {function}(service *service, {argument} string) {{ "
                f"service.commands.Prepare({argument}) }}\n"
            )
            path.write_text(source)
            check(True)
            path.write_text(source.replace("service.commands.Prepare", "service.db.Prepare"))
            check(False, "raw SQL method")
            path.unlink()
        command_path.write_text(sources["haven/sandbox/sandbox.go"])
        statement_path = root / "haven/store/statements.go"
        statements = (ROOT / "haven/store/statements.go").read_text()
        statement_path.write_text(statements)
        check(True)
        statement_path.write_text(statements.replace(
            "database.PrepareContext(ctx, query)", "database.Prepare(query)"
        ))
        check(False, "raw SQL method")
        statement_path.unlink()
        check(True)
        image_path = root / "haven/image/image.go"
        for namespace in ("image_import", "image_cleanup"):
            image_path.write_text(
                f'package image\nimport queries "{MODULE}/image/internal/dbgen"\n'
                f'var step = queries.GetStepParams{{StepKey: "{namespace}"}}\n'
            )
            check(True)
        for expression, reason in (
            ('queries.GetStepParams{StepKey: "resource"}', "unowned or dynamic step namespace"),
            ('queries.GetStepParams{StepKey: key}', "unowned or dynamic step namespace"),
            ('queries.GetStepParams{}', "omits its owned step namespace"),
            ('queries.GetDeletionReceiptParams{StepKey: "image_import"}', "unowned or dynamic step namespace"),
        ):
            image_path.write_text(
                f'package image\nimport queries "{MODULE}/image/internal/dbgen"\n'
                f'var key = "image_import"\nvar step = {expression}\n'
            )
            check(False, reason)
        image_path.write_text(sources["haven/image/image.go"])
        for owner, keys in {
            "machine": ("resource", "capture", "stop", "discard", "capture_discard", "capture_cleanup", "snapshot_delete", "direct_retirement"),
            "sandbox": ("lifecycle", "warm_claim", "fork_group"),
            "supply": ("supply", "supply_cleanup"),
            "collection": ("gc",),
        }.items():
            path = root / f"haven/{owner}/steps.go"
            prefix = f'package {owner}\nimport queries "{MODULE}/{owner}/internal/dbgen"\n'
            for key in keys:
                path.write_text(prefix + f'var step = queries.GetStepParams{{StepKey: "{key}"}}\n')
                check(True)
            for expression, reason in (
                ('queries.GetStepParams{StepKey: "image_import"}', "unowned or dynamic step namespace"),
                ('queries.GetStepParams{StepKey: key}', "unowned or dynamic step namespace"),
                ('queries.GetStepParams{}', "omits its owned step namespace"),
            ):
                path.write_text(prefix + f'var key = "{keys[0]}"\nvar step = {expression}\n')
                check(False, reason)
            path.write_text(prefix + 'type Alias = queries.InsertStepParams\nvar step = Alias{StepKey: "image_import"}\n')
            check(False, "unowned or dynamic step namespace")
            if owner != "collection":
                cleanup_id, cleanup_kind = {
                    "machine": ("capture:", "snapshot"),
                    "sandbox": ("checkpoint:", "snapshot"),
                    "supply": ("supply:", "supply"),
                }[owner]
                path.write_text(prefix + f'var step = queries.InsertCleanupIntentParams{{ID: "{cleanup_id}" + "op", ResourceKind: "{cleanup_kind}"}}\n')
                check(True)
                path.write_text(prefix + f'var step = queries.InsertCleanupIntentParams{{ID: "image_cleanup:" + "op", ResourceKind: "{cleanup_kind}"}}\n')
                check(False, "unowned or dynamic ID namespace")
                path.write_text(prefix + f'var step = queries.InsertCleanupIntentParams{{ID: "{cleanup_id}" + "op", ResourceKind: "image_import"}}\n')
                check(False, "unowned or dynamic resource kind")
            path.unlink()
        image_path.write_text("package image\ntype Repository struct{}\n")
        check(False, "image repository facade was restored")
        image_path.write_text(sources["haven/image/image.go"])
        for name in ("TemplateState", "PoolState"):
            facade = root / "haven/supply/restored.go"
            facade.write_text(f"package supply\ntype {name} struct{{}}\n")
            check(False, "supply row facade was restored")
            facade.unlink()
        for owner, name in (("machine", "VMState"), ("machine", "SnapshotState"), ("sandbox", "Repository")):
            facade = root / f"haven/{owner}/restored.go"
            facade.write_text(f"package {owner}\ntype {name} struct{{}}\n")
            check(False, "retired persistence facade was restored")
            facade.unlink()
        reader = root / "haven/sandbox/readonly.go"
        generated = root / "haven/sandbox/internal/dbgen/readonly.go"
        projector = root / "haven/internal/publication/projector.go"
        generated.write_text(
            'package dbgen\nimport "database/sql"\n'
            'func Read(db *sql.DB) error { return db.Ping() }\n'
            'func Write(db *sql.DB) error { _, err := db.Exec("fixture"); return err }\n'
        )
        reader_source = (
            f'package sandbox\nimport ("database/sql"; "{MODULE}/sandbox/internal/dbgen")\n'
            'type ActivationHistory struct{}\n'
            'func (ActivationHistory) ReadActivationProof(db *sql.DB) error { return ReadForProjection(db) }\n'
            'func ReadForProjection(db *sql.DB) error { return helper(db) }\n'
            'func helper(db *sql.DB) error { return dbgen.Read(db) }\n'
        )
        reader.write_text(reader_source)
        check(True)
        reader.write_text(reader_source.replace("dbgen.Read(db)", "dbgen.Write(db)"))
        check(False, "read-only boundary reaches")
        reader.write_text(reader_source.replace(
            'func (ActivationHistory) ReadActivationProof(db *sql.DB) error { return ReadForProjection(db) }',
            'func (ActivationHistory) ReadActivationProof(db *sql.DB) error { go helper(db); return nil }',
        ))
        check(False, "read-only boundary starts a goroutine")
        reader.write_text(reader_source.replace(
            'func (ActivationHistory) ReadActivationProof(db *sql.DB) error { return ReadForProjection(db) }', "",
        ))
        projector.write_text(
            f'package publication\nimport ("database/sql"; "{MODULE}/sandbox")\n'
            'func Project(db *sql.DB) error { return sandbox.ReadForProjection(db) }\n'
        )
        check(True)
        reader.write_text(reader.read_text().replace("dbgen.Read(db)", "dbgen.Write(db)"))
        check(False, "read-only boundary reaches")
        reader.unlink()
        generated.unlink()
        projector.write_text(sources["haven/internal/publication/projector.go"])
        check(True)
        print("PASS: architecture gate rejects import, controller, SQL namespace, read-only and facade violations")


if __name__ == "__main__":
    main()
