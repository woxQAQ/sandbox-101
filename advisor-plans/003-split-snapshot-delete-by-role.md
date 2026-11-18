# Plan 003: Split `snapshot_delete.go` by workflow role

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- haven/machine`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW (code motion only, no renames, no behavior change)
- **Depends on**: 002 (avoid concurrent edits in `haven/machine`)
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

Every machine workflow follows the same file layout: the service struct, its
request and step types live in the base file (`captures.go`, `stops.go`,
`discard.go`), admission lives in `X_admission.go` (`capture_admission.go`,
`capture_discard_admission.go`), and workflow stepping lives in
`X_workflow.go` (`capture_workflow.go`, `stop_workflow.go`, `discard_workflow.go`).
Snapshot deletion is the only exception: `snapshot_delete.go` is a grab-bag of
request + step + admission + workflow, while its service sits alone in
`snapshot_deletion.go`. Two near-identical filenames differing only by
`delete`/`deletion` is exactly the "similar things must look similar" failure.

## Current state

`haven/machine/snapshot_delete.go` (at 129c156) contains, in order:

- `:21` `type SnapshotDeleteRequest struct` and a `:26 type ( ... )` block
  (check the block's members when moving — they belong with the step types)
- `:40 func LoadSnapshotDelete(...) (SnapshotDeleteStep, error)`
- `:72 func (sd *SnapshotDeletion) snapshotDeleteProof(...)`
- `:252 func snapshotRawPath(source SnapshotSource) string`
- `:256 func (sd *SnapshotDeletion) AdmitSnapshotDelete(...)`
- `:402 checkSnapshotDelete`, `:423 snapshotDeleteReceipt`,
  `:505 runSnapshotDelete`, `:557 markSnapshotRemoved`,
  `:589 completeSnapshotDelete`

`haven/machine/snapshot_deletion.go` contains the `SnapshotDeletion` service
struct, `SnapshotDeletionGuards`, and `NewSnapshotDeletion` — this file is
already correctly placed and must not move.

Exemplar to mirror: the capture triad —
`haven/machine/captures.go` (service + `CaptureRequest` + `CaptureStep` +
`LoadCapture`), `haven/machine/capture_admission.go` (`AdmitCapture`,
`AdmitForParent`), `haven/machine/capture_workflow.go` (run/complete helpers).

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build haven | `GOWORK=off go -C haven build ./...` | exit 0 |
| Machine tests | `GOWORK=off go -C haven test ./machine/... -count=1` | all pass |
| Component tests | `GOWORK=off go -C haven test ./tests/... -p=1 -count=1 -timeout=120s` | all pass |
| Vet | `just check` | exit 0 |

## Scope

**In scope**:
- `haven/machine/snapshot_delete.go` (emptied and deleted)
- `haven/machine/snapshot_deletion.go` (gains request/step types + loader)
- `haven/machine/snapshot_delete_admission.go` (new)
- `haven/machine/snapshot_delete_workflow.go` (new)

**Out of scope**:
- Any type or function rename (`SnapshotDeleteRequest`, `SnapshotDeleteStep`,
  `AdmitSnapshotDelete`, etc. all keep their names — `delete` is this
  workflow's verb family and stays)
- Any caller outside `haven/machine` (none should need changes — no identifier
  moves between packages)
- Behavior changes of any kind

## Git workflow

- Branch: `advisor/003-snapshot-delete-split`
- Commit: `refactor: split machine snapshot deletion files by workflow role`

## Steps

### Step 1: Move request/step types and loader into the service file

Move from `snapshot_delete.go` into `snapshot_deletion.go`:
`SnapshotDeleteRequest`, the `type (...)` block at line 26,
`SnapshotDeleteStep` (defined in the same file — locate it), and
`LoadSnapshotDelete`. Placement mirrors `captures.go`: types near the service,
`LoadSnapshotDelete` right after the step type.

**Verify**: `GOWORK=off go -C haven build ./machine/` → exit 0.

### Step 2: Create `snapshot_delete_admission.go`

Move `AdmitSnapshotDelete` (and any helper used ONLY by it — verify with
`rg -n "<helper>" haven/machine/` before moving; if a helper is shared, leave
it behind). File starts with `package machine` and the imports it needs.

**Verify**: `GOWORK=off go -C haven build ./machine/` → exit 0.

### Step 3: Create `snapshot_delete_workflow.go`

Move the remaining workflow functions: `snapshotDeleteProof`,
`checkSnapshotDelete`, `snapshotDeleteReceipt`, `runSnapshotDelete`,
`markSnapshotRemoved`, `completeSnapshotDelete`, `snapshotRawPath`. Delete the
now-empty `snapshot_delete.go`.

**Verify**: `GOWORK=off go -C haven build ./machine/` → exit 0;
`ls haven/machine/snapshot_delete.go` → "No such file".

### Step 4: Gates

**Verify**: `just check` → exit 0;
`GOWORK=off go -C haven test ./machine/... ./tests/... -p=1 -count=1 -timeout=120s`
→ all pass.

## Test plan

No new tests. The existing component tests
`haven/tests/component/snapshot_delete_test.go` and
`snapshot_delete_boundary_test.go` exercise this workflow end-to-end; they
must pass unmodified.

## Done criteria

- [ ] `haven/machine/` contains `snapshot_deletion.go`,
      `snapshot_delete_admission.go`, `snapshot_delete_workflow.go`, and no
      `snapshot_delete.go`
- [ ] `git diff` shows only code motion (use `git diff --color-moved` to
      confirm no line was edited in content)
- [ ] `just check` exits 0; machine + component tests pass
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- A function you are about to move is referenced from a test or another
  package in a way that requires editing that caller (pure motion should not).
- The `type (...)` block at line 26 contains something that is neither a
  request nor a step type — report what it is instead of guessing its home.

## Maintenance notes

- Future snapshot-deletion code: admission logic goes to
  `snapshot_delete_admission.go`, stepping to `snapshot_delete_workflow.go`,
  service/state types to `snapshot_deletion.go`.
- If plan 002 landed, note `SnapshotDeletion` keeps its name — it was already
  prefix-free.
