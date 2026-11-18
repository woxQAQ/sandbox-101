# Plan 004: Export discard step input/state

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- haven/machine`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW (export rename of two structs; JSON tags already present, so
  persisted bytes do not change)
- **Depends on**: 002 (avoid concurrent edits in `haven/machine`)
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

Every machine workflow exports its step triple — `CaptureInput`/`CaptureState`/
`CaptureStep`, `ResourceInput`/`ResourceState`/`ResourceStep`,
`DirectRetirementInput/State/Step` — except discard: `DiscardStep` is exported
but its field types `discardInput`/`discardState` are not. Callers outside the
package (`haven/supply/cleanup.go`, `haven/sandbox/adapters.go`,
`haven/internal/recovery/workflows.go`, `haven/tests/fixtures/discard.go`)
receive a `DiscardStep` from `LoadDiscard` yet cannot name the type of
`step.Input`. Visibility should be uniform across sibling step triples.

## Current state

`haven/machine/discard.go` (at 129c156):

```go
// discard.go:29
type discardInput struct {
	Request         DiscardRequest `json:"request"`
	Source          ResourceStep   `json:"source"`
	CurrentInstance string         `json:"current_instance"`
}

// discard.go:35
type discardState struct {
	Phase string `json:"phase"`
}

// discard.go:39
type DiscardStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       discardInput
	State       discardState
}
```

Both structs already carry complete JSON tags, so exporting them changes no
persisted encoding. `discard_workflow.go` uses `discardState{...}` at ≈ lines
34 and 402. Verified at audit time: no file outside `haven/machine` references
`discardInput`/`discardState` by name (they can't).

Secondary asymmetry in the same family: `haven/machine/resource_models.go:61`
`ResourceStep` lists `Status` before `Hash`, while `StopStep`, `CaptureStep`
and `DiscardStep` all list `Hash` before `Status`. Field order has no wire
effect here (steps are assembled from DB columns, not serialized as structs).

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Tests | `GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` | all pass |
| Vet/fmt | `just check` | exit 0 |
| Old names gone | `rg -n "discardInput|discardState" haven/` | no matches |

## Scope

**In scope**:
- `haven/machine/discard.go`
- `haven/machine/discard_workflow.go`
- `haven/machine/resource_models.go` (field order only)

**Out of scope**:
- `haven/machine/stops.go` `StopInput` JSON tags — deliberately NOT changed;
  adding tags would alter the persisted encoding and `InputHash` of future
  operations. See "Maintenance notes".
- Any other step struct.

## Git workflow

- Branch: `advisor/004-discard-export`
- Commit: `refactor: export machine discard step input and state`

## Steps

### Step 1: Export the two structs

In `discard.go`: `discardInput` → `DiscardInput`, `discardState` →
`DiscardState`; update the two `DiscardStep` field types and the doc comment
above the input struct (keep the comment text, adjust the name). In
`discard_workflow.go`: update both `discardState{...}` literals.

**Verify**: `rg -n "discardInput|discardState" haven/` → no matches;
`just build` → exit 0.

### Step 2: Align `ResourceStep` field order

In `resource_models.go`, reorder so `Hash` precedes `Status`, matching
`CaptureStep`/`DiscardStep`:

```go
type ResourceStep struct {
	OperationID      string
	Key              string
	Action           string
	Hash             string
	Status           string
	Candidate        string
	ExpectedInstance string
	// ... rest unchanged
}
```

**Verify**: `just build` → exit 0 (pure field reorder; if any literal uses
positional fields without names, the compiler will flag it — fix by switching
that literal to named fields, and note it in the commit message).

### Step 3: Gates

**Verify**: `just check` → exit 0;
`GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` → all pass.

## Test plan

No new tests. The discard workflow is covered by
`haven/tests/component/` discard tests and `haven/tests/fixtures/discard.go`;
they must pass unmodified.

## Done criteria

- [ ] `DiscardInput`/`DiscardState` exported; no `discardInput`/`discardState` remain
- [ ] `ResourceStep` field order matches the sibling step structs
- [ ] `just build`, `just check` exit 0; haven tests pass
- [ ] `git diff` shows no JSON tag changes
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- Any file outside `haven/machine` fails to compile after Step 1 for a reason
  other than a missed reference (should be impossible — report it).
- Step 2 reveals a positional (unnamed) struct literal of `ResourceStep` —
  convert it to named fields; if more than three such literals exist, STOP
  and report instead.

## Maintenance notes

- `StopInput` (stops.go) still lacks JSON tags on most fields while
  `CaptureInput` has them — a real asymmetry left in place deliberately:
  `StopInput` is frozen into operation envelopes and tagged encoding feeds
  `InputHash`. Changing it needs a hash-compatibility analysis, not a rename.
- New workflow steps: export Input/State/Step as a triple, fields tagged,
  order `OperationID, Hash, Status, Input, State`.
