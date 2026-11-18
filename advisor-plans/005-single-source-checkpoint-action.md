# Plan 005: Single-source the checkpoint action spellings

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- haven/operation haven/httpapi haven/sandbox haven/internal`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: MED (touches the boundary between stored and public action names;
  the public wire spelling must NOT change)
- **Depends on**: none (sequenced before 009 — same file `httpapi/http.go`)
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

> **Review outcome (2026-11-18)**: the constant-based approach below was
> implemented and then reverted after review. Using constants for a single
> action while every sibling action stays a literal created a worse mixed
> style than the duplication it removed. The final state keeps literals
> everywhere and documents the stored/public spelling pair with
> cross-referencing comments at the two translation points
> (`haven/operation/identity.go` decode and `haven/httpapi/http.go` mutate).

One logical action — deleting a checkpoint — exists under two spellings:
`checkpoint_delete` (stored in the operations table, matched in recovery) and
`delete_checkpoint` (public operation view). The mapping is re-implemented by
hand at two distant places, and `haven/internal/publication/projector.go`
accepts both spellings. Anyone adding a third boundary must discover the
convention by archaeology. The mapping should live in one named place with
both spellings defined once.

## Current state

At 129c156:

- **Stored spelling** `checkpoint_delete`: persisted by
  `haven/sandbox/lifecycle_checkpoint.go:195-196` (`StepKey`/`Action`),
  matched in `haven/internal/recovery/workflows.go:71` and admitted in
  `haven/sandbox/requests.go:51`.
- **Public spelling** `delete_checkpoint`: emitted for public views;
  `haven/operation/identity.go:167-168` rewrites stored → public when decoding:

  ```go
  if command.Action == "checkpoint_delete" {
      command.Action = "delete_checkpoint"
  }
  ```

- **HTTP boundary** `haven/httpapi/http.go`: route `DELETE /v1/checkpoints/:id`
  calls `adapter.mutate(c, "delete_checkpoint", "", ...)` (:158); `mutate`
  rejects the stored spelling from clients (:305) and translates public →
  stored before submit (:347-348):

  ```go
  if action == "delete_checkpoint" {
      intent.Action = "checkpoint_delete"
  }
  ```

- `haven/internal/publication/projector.go:45` rejects projections whose
  command action is neither `checkpoint` nor `delete_checkpoint` (public
  spelling, since projection reads decoded public views).

First read `haven/sandbox/api.md` and `docs/haven-control.md` for the
documented public spelling — the wire-facing name (`delete_checkpoint`) is a
published public contract and MUST NOT change. The stored spelling is internal
but persisted in existing databases; do NOT rewrite stored rows — old rows
keep `checkpoint_delete`, so the stored spelling stays too. This plan only
centralizes the mapping; it changes no spelling.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Haven tests | `GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` | all pass |
| Integration | `just integration` | all pass |
| Vet/fmt | `just check` | exit 0 |

## Scope

**In scope**:
- `haven/operation/identity.go` (or a sibling file in `haven/operation` — the
  constants' home)
- `haven/httpapi/http.go`
- `haven/sandbox/lifecycle_checkpoint.go`, `haven/sandbox/requests.go`
- `haven/internal/recovery/workflows.go`,
  `haven/internal/publication/projector.go`

**Out of scope**:
- Changing either spelling, migrating stored rows, or editing `docs/`/api.md
- The `checkpoint` / `template_*` / other action names

## Git workflow

- Branch: `advisor/005-checkpoint-action-names`
- Commit: `refactor: single-source checkpoint action spellings in operation`

## Steps

### Step 1: Define the constants

In `haven/operation` (use `identity.go` unless the package already has a
constants file — check with `rg -n "Action" haven/operation/*.go` first), add:

```go
// ActionCheckpointDelete is the stored action; ActionCheckpointDeletePublic is
// the public operation-view spelling. The two boundary translations (HTTP
// admission and public decode) are the only places that may convert.
const (
	ActionCheckpointDelete       = "checkpoint_delete"
	ActionCheckpointDeletePublic = "delete_checkpoint"
)
```

Adjust names to match any existing action-constant convention you find in the
package; if one exists, extend it instead of inventing a second style.

**Verify**: `GOWORK=off go -C haven build ./operation/` → exit 0.

### Step 2: Replace every literal

Replace both spellings' string literals with the constants at all sites listed
in "Current state" (re-derive with `rg -n "checkpoint_delete|delete_checkpoint"
haven/ -g '!*_test.go'`). At the two translation points, add a one-line comment
pointing at the constant pair so the pairing is visible at each end.

**Verify**: `rg -n '"checkpoint_delete"|"delete_checkpoint"' haven/ -g '!*_test.go'`
→ matches only inside the const declaration; `just build` → exit 0.

### Step 3: Gates

**Verify**: `just check` → exit 0; haven tests pass; `just integration` →
all pass (the guest fixture path exercises operation encoding).

## Test plan

No new tests. Existing tests in `haven/operation`, `haven/httpapi` (via
`haven/tests/component`) and recovery tests already cover both spellings;
they must pass unmodified, which proves no spelling changed.

## Done criteria

- [ ] Both spellings defined exactly once in `haven/operation`
- [ ] No remaining string literals for either spelling outside the const block
      and tests
- [ ] `just build`, `just check` exit 0; haven tests and `just integration` pass
- [ ] No spelling change (public API and stored data untouched)
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- `haven/sandbox/api.md` or `docs/haven-control.md` documents a DIFFERENT
  mapping than described here (e.g. the public spelling is the stored one).
- Tests assert on the literal strings in a way that requires editing test
  expectations — report instead of editing (tests may encode the contract).
- A third translation site is found beyond those listed.

## Maintenance notes

- Any new action with distinct stored/public spellings must define both as
  constants here and translate only at the two boundary layers.
- Reviewer: diff must show zero spelling changes — constants only.
