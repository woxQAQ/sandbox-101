# Plan 008: Prefix gc/dataplane dependency names

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- haven/gc haven/dataplane haven/internal/application`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: LOW (pure rename of internal types)
- **Depends on**: 002 (both touch `haven/internal/application/app.go`)
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

The dominant capability convention names dependency bundles after their
service: `image.ImageGuards`/`ImageDependencies`/`NewImages`,
`supply.SupplyGuards`/`SupplyDependencies`/`NewSupply`,
`sandbox.SandboxGuards`/`SandboxDependencies`/`NewSandboxes`,
`machine.CapturesGuards`/`CapturesDependencies`/`NewCaptures`. Two packages
deviate with bare names: `gc.Guards`/`gc.Dependencies` (service `Collector`)
and `dataplane.Guards`/`dataplane.Options`/`dataplane.Dependencies` plus ports
`Backend`/`Guest` (service `Plane`). In `app.go` — where every capability is
wired side by side — these read as a different, unexplained idiom.

## Current state

- `haven/gc/service.go:18` — `type Guards interface` (Admission, NodeRequest,
  GarbageCollection, Artifacts, GCOperation); `:42` `type Dependencies struct`;
  `:58` `func NewCollector(state *store.State, dependencies Dependencies, ...)`.
- `haven/dataplane/service.go:19` — `type Backend interface`; `:23`
  `type Guest interface`; `:27` `type Guards interface`; `:31` (approx)
  `type Dependencies struct`; `:35` `type Options struct`; `:72`
  `func NewPlane(state, dependencies Dependencies, options Options)`.
- External references are concentrated in
  `haven/internal/application/app.go:244` (`gc.NewCollector(state,
  gc.Dependencies{...})`) and `:419/:428` (`dataplane.NewPlane(state,
  dataplane.Dependencies{...}, dataplane.Options{...})`). Executor must
  re-derive the full list with `rg` including tests.

Exemplar to mirror: `haven/image/service.go` (`ImageGuards`,
`ImageDependencies`, `ImageOptions`, `NewImages`).

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Tests | `GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` | all pass |
| Vet/fmt | `just check` | exit 0 |

## Scope

**In scope**:
- `haven/gc/*.go` (non-generated)
- `haven/dataplane/*.go`
- `haven/internal/application/app.go` and any other file referencing the
  renamed identifiers (re-derive with `rg`)

**Out of scope**:
- `haven/gc/internal/dbgen/**` (generated)
- Renaming `gc.Collector`, `dataplane.Plane`, or any method
- Touching `dataplane.Audits`/`NewAudits` (different constructor shape;
  deliberate — it is not dependency-injected the same way)

## Git workflow

- Branch: `advisor/008-dependency-prefixes`
- Commit: `refactor: prefix gc and dataplane dependency names with their services`

## Steps

### Step 1: gc

In `haven/gc`: `Guards` → `CollectorGuards`, `Dependencies` →
`CollectorDependencies`. Update `NewCollector`'s signature, the `Collector`
struct fields' types, doc comments, and all references.

**Verify**: `rg -n "gc\.Guards|gc\.Dependencies|\bGuards\b|\bDependencies\b" haven/gc/`
→ only `CollectorGuards`/`CollectorDependencies` remain;
`GOWORK=off go -C haven build ./gc/` → exit 0.

### Step 2: dataplane

In `haven/dataplane`: `Guards` → `PlaneGuards`, `Options` → `PlaneOptions`,
`Dependencies` → `PlaneDependencies`, `Backend` → `PlaneBackend`,
`Guest` → `PlaneGuest`. Update `NewPlane`, struct fields, doc comments,
references (including test files and `app.go`).

**Verify**: `GOWORK=off go -C haven build ./dataplane/` → exit 0;
`just build` → exit 0.

### Step 3: Gates

**Verify**: `just check` → exit 0; haven tests pass.

## Test plan

No new tests. Existing `haven/dataplane` and `haven/gc` tests plus component
tests must pass unmodified.

## Done criteria

- [ ] No bare `Guards`/`Dependencies`/`Options`/`Backend`/`Guest` type names
      remain in `haven/gc` or `haven/dataplane`
- [ ] `just build`, `just check` exit 0; haven tests pass
- [ ] Doc comments updated to the new names
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- A renamed name collides with an existing identifier in the package.
- `dataplane.Backend`/`Guest` turn out to be implemented by types outside
  `haven/` in a way that makes the rename ripple beyond `haven/` — report the
  implementers before proceeding.

## Maintenance notes

- Convention: capability packages name dependency bundles
  `<Service>Guards` / `<Service>Dependencies` / `<Service>Options`; ports are
  `<Service>Backend` / `<Service>Guest` when the package could grow a second
  port of the same kind.
