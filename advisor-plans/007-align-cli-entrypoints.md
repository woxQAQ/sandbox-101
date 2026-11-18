# Plan 007: Align havenctl/loomctl entrypoint shape

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- sdk/cmd loom/cmd`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P3
- **Effort**: M
- **Risk**: LOW (CLI dispatch restructure; behavior and messages preserved)
- **Depends on**: none (sequenced before 010 — same files)
- **Category**: tech-debt / dx
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

`havenctl` and `loomctl` solve the same problem — parse settings, dispatch one
subcommand, print JSON — with two different shapes: havenctl uses one big
`switch args[0]` whose arms assign `value, err` before a shared
`return cli.JSON(os.Stdout, value)` tail; loomctl mixes early-return `if`
blocks (`status`, `operation`) with a trailing `switch` plus a separate
`client.Manage` tail. Same problem, visibly different structure — a reader of
one cannot predict the other. Import style also differs: `haven/cmd/haven` and
`loom/cmd/loomctl` both alias the shared loader as `loader`, while havenctl
imports it unaliased as `config`.

## Current state

- `sdk/cmd/havenctl/main.go` — target shape exemplar: `switch args[0]` with
  grouped cases (`case "status", "list", ...`), inner switches for
  sub-dispatch, every arm assigns `value, err`, one shared tail:

  ```go
  if err != nil {
      return err
  }
  return cli.JSON(os.Stdout, value)
  ```

- `sdk/cmd/havenctl/config.go:7` — `import "github.com/woxQAQ/haven/common/config"`
  used as `config.Load(...)` (:30).
- `loom/cmd/loomctl/main.go` — `status`/`operation` handled by two
  early-return `if` blocks (≈ lines 45–62) before the management `switch`
  (≈ lines 69–105); two separate `cli.JSON` call sites.
- `loom/cmd/loomctl/config.go:10` and `haven/cmd/haven/config.go:9` — both
  already use `loader "github.com/woxQAQ/haven/common/config"`.
- `loom/cmd/loomctl/config.go` keeps its settings struct in `loom/config`
  (package documented in README); havenctl keeps `settings` locally. This
  placement asymmetry is documented/deliberate — do NOT move either struct.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Tests | `GOWORK=off go -C sdk test ./... -count=1 && GOWORK=off go -C loom test ./... -count=1` | all pass |
| Vet/fmt | `just check` | exit 0 |
| Smoke | `just smoke` | all pass (drives both CLIs against a real daemon) |

## Scope

**In scope**:
- `loom/cmd/loomctl/main.go`
- `sdk/cmd/havenctl/config.go` (import alias only)

**Out of scope**:
- `loom/config/cli.go`, `sdk/cmd/havenctl/config.go` struct definitions
- Error message texts (keep byte-identical), flag names, defaults, help text
- `haven/cmd/haven/config.go`

## Git workflow

- Branch: `advisor/007-cli-shape`
- Commits: `refactor: align loomctl dispatch with havenctl switch shape` and
  `refactor: alias common/config as loader in havenctl`

## Steps

### Step 1: Unify the loader import alias in havenctl

In `sdk/cmd/havenctl/config.go`, change the import to
`loader "github.com/woxQAQ/haven/common/config"` and the call to
`loader.Load(flags, "HAVENCTL", &result)`.

**Verify**: `GOWORK=off go -C sdk build ./cmd/havenctl` → exit 0;
`rg -n "loader.Load" sdk/cmd loom/cmd haven/cmd` → three matches, one per CLI.

### Step 2: Restructure loomctl dispatch to the havenctl shape

Rewrite `run` in `loom/cmd/loomctl/main.go` so that:

1. The preamble (usage error, `--request-file` gate, client, signal context)
   is unchanged.
2. A single `var value any` + one `switch args[0]` drives everything:
   - `case "status":` — keep the `len(args) != 1` check and message, assign
     `value, err = client.Status(ctx)`.
   - `case "operation":` — keep the `len(args) != 2` check, assign
     `value, err = client.Operation(ctx, args[1])`.
   - `case "import", "gc", "supply-cleanup", "image-cleanup", "request":` —
     keep the `len(args) != 1` "management commands take options before the
     command" check; build `types.ManagementRequest` in an inner switch exactly
     as today (same action strings, same request-file handling, same key
     conflict check); keep the trailing `--key is required` validation; assign
     `value, err = client.Manage(ctx, request)`.
   - `default:` — `return fmt.Errorf("unknown command %q", args[0])`.
3. One shared tail identical to havenctl's:

   ```go
   if err != nil {
       return err
   }
   return cli.JSON(os.Stdout, value)
   ```

All error messages, action strings, and validations must be preserved
byte-for-byte — this is structure-only.

**Verify**: `just build` → exit 0;
`diff <(bin/loomctl --help 2>&1) <(git stash && just build >/dev/null && bin/loomctl --help 2>&1; git stash pop)` —
help output unchanged (or verify manually that usage text is untouched);
`GOWORK=off go -C loom test ./... -count=1` → all pass.

### Step 3: Gates

**Verify**: `just check` → exit 0; `just smoke` → all pass.

## Test plan

No new tests. `just smoke` exercises both CLIs against a real daemon
(status/operation/GC paths) — that is the behavior gate for this restructure.

## Done criteria

- [ ] loomctl `run` has exactly one `switch args[0]`, one `value, err`
      assignment per arm, and one shared `cli.JSON` tail
- [ ] havenctl uses the `loader` import alias like the other two CLIs
- [ ] All CLI error messages and action strings byte-identical (`git diff`
      shows no string literal changes)
- [ ] `just build`, `just check`, `just smoke` pass
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- Preserving an error message forces the structure to diverge (e.g. a check
  that only makes sense mid-switch) — report the conflict instead of
  rewording the message.
- `just smoke` fails for reasons unrelated to the restructure (environment
  issues) — report as BLOCKED, do not weaken the gate.

## Maintenance notes

- New subcommands: add a case to the single switch, assign `value, err`, done.
- The two files should stay visually diffable side by side; keep them so.
