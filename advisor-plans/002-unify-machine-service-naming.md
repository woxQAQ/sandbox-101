# Plan 002: Unify machine/sandbox service naming (drop stray VM prefixes)

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- haven/machine haven/sandbox haven/supply haven/internal haven/tests`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: LOW (pure identifier rename; all types are internal, unpublished)
- **Depends on**: none (sequenced before 003/004/008 only to avoid conflicts)
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

Everything in `haven/machine` is about VMs, yet only two services carry a `VM`
prefix (`VMStops`, `VMDeletion`) while their siblings do not (`Captures`,
`CaptureDiscard`, `SnapshotDeletion`, `Provisioning`). Singular/plural also
swings (`VMStops` vs `VMDeletion`). Meanwhile `sandbox.VMProvisioning` pairs
with `machine.Provisioning` — the distinguishing prefix sits on the business
side only. Same-layer things should be named the same way; the package name
already carries the scope. Per repo policy, internal unpublished APIs need no
backward compatibility, so a clean rename is preferred over aliases.

## Current state

Declarations and constructors (at 129c156):

- `haven/machine/stops.go:27` — `type VMStops struct`, `:40 VMStopsDependencies`,
  `:49 func NewVMStops(...) *VMStops`
- `haven/machine/deletion.go:26` — `type VMDeletion struct`,
  `:42 VMDeletionDependencies`, `:54 func NewVMDeletion(...) *VMDeletion`
- `haven/sandbox/provisioning.go:68` — `func NewVMProvisioning(...) *VMProvisioning`
  (type `VMProvisioning`; its deps are already `ProvisionDependencies` — note
  the existing Provision/Provisioning asymmetry is deliberate: the deps struct
  keeps the file's `provision_*` naming family)
- Counter-examples that must NOT change: `machine.Provisioning`
  (`provisioning.go:15`), `machine.Captures`, `machine.CaptureDiscard`,
  `machine.SnapshotDeletion`.

Known external references (executor must re-derive with `rg`; this list is not
guaranteed complete):

- `haven/internal/application/app.go` — App fields `VMStops`, `VMDeletion`,
  `VMProvisioning` (≈ lines 64–70) and constructions at ≈ lines 209, 223, 296.
- `haven/supply/service.go:62,82` — `Deletion *machine.VMDeletion` field.
- `haven/internal/maintenance/service.go:28,43` — `*sandbox.VMProvisioning`.
- `haven/tests/fixtures/{vm.go,restore.go,restore_target.go,...}` — use
  `service.VMProvisioning.AdmitCold/AdmitClone/AdmitRestore`.
- `haven/sandbox/lifecycle.go` and others referencing the field, not the type.

Repo convention exemplar: sibling services — read
`haven/machine/captures.go:36-62` (`type Captures`, `CapturesDependencies`,
`NewCaptures`) and mirror it exactly.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Haven tests | `GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` | all pass |
| Vet/fmt | `just check` | exit 0 |
| Lint | `just lint` | exit 0 |
| Old names gone | `rg -n "VMStops|VMDeletion|VMProvisioning" haven/` | no matches |

## Scope

**In scope**:
- `haven/machine/stops.go`, `haven/machine/deletion.go` (type/deps/constructor
  renames + doc comments mentioning the old names)
- `haven/sandbox/provisioning.go` (type/constructor rename + doc comments)
- All Go files referencing the renamed identifiers (including
  `haven/internal/application/app.go`, `haven/supply/service.go`,
  `haven/internal/maintenance/service.go`, `haven/tests/**`)

**Out of scope**:
- `machine.VMGuards` (`ports.go`) — shared guard interface, deliberately kept
- `machine.VMRecord`, `VMRoot`, `VMUsers` etc. — not service types; untouched
- File renames (`stops.go`, `deletion.go` keep their names)
- Any behavior change

## Git workflow

- Branch: `advisor/002-machine-naming`
- One commit is fine: `refactor: drop stray VM prefixes from machine and sandbox services`

## Steps

### Step 1: Rename machine services

In `haven/machine/stops.go`: `VMStops` → `Stops`, `VMStopsDependencies` →
`StopsDependencies`, `NewVMStops` → `NewStops`. Keep the existing receiver
names and method set unchanged. In `haven/machine/deletion.go`:
`VMDeletion` → `Deletion`, `VMDeletionDependencies` → `DeletionDependencies`,
`NewVMDeletion` → `NewDeletion`. Update each type's doc comment if it mentions
the old name.

**Verify**: `rg -n "VMStops|VMDeletion" haven/machine/` → no matches.

### Step 2: Rename sandbox.VMProvisioning

In `haven/sandbox/provisioning.go`: `VMProvisioning` → `Provisioning`,
`NewVMProvisioning` → `NewProvisioning`. Do NOT rename
`ProvisionDependencies`/`ProvisionGuards`/`ProvisionOptions` (they belong to
the `provision_*` file family and stay).

**Verify**: `rg -n "VMProvisioning" haven/sandbox/` → no matches.

### Step 3: Update all references

`rg -ln "VMStops|VMDeletion|VMProvisioning" haven/ loom/ sdk/ operator/` and
update every hit: App fields `VMStops`→`Stops`, `VMDeletion`→`Deletion`,
`VMProvisioning`→`Provisioning`; `machine.VMDeletion`→`machine.Deletion`, etc.
In `haven/supply/service.go` the field is already named `Deletion`, only its
type changes.

**Verify**: `rg -n "VMStops|VMDeletion|VMProvisioning" haven/ loom/ sdk/ operator/` → no matches; `just build` → exit 0.

### Step 4: Gates

**Verify**: `just check` → exit 0; `just lint` → exit 0;
`GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` → all pass.

## Test plan

No new tests (pure rename). Existing component tests under
`haven/tests/component/` and fixtures under `haven/tests/fixtures/` exercise
every renamed service; they must compile and pass unchanged in behavior.

## Done criteria

- [ ] `rg -n "VMStops|VMDeletion|VMProvisioning" haven/ loom/ sdk/ operator/` → no matches
- [ ] `just build`, `just check`, `just lint` all exit 0
- [ ] Haven module tests pass
- [ ] No new identifiers introduced; no file renamed
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- A renamed identifier collides with an existing declaration in the same
  package (e.g. something already named `Provisioning` in `sandbox`).
- Any reference is found outside `haven/` (loom/sdk/operator/tests) that the
  plan did not anticipate in kind (type, field, or constructor use).
- A test failure appears that is not a compile error from a missed reference.

## Maintenance notes

- Reviewers: this is a mechanical rename; scrutinize only that no behavior
  crept in and that doc comments still read correctly.
- Future services in `machine`/`sandbox`: follow the no-prefix convention
  (`Stops`, `Deletion`, `Provisioning`).
