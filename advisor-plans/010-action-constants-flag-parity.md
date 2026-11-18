# Plan 010: Management action constants + CLI flag/struct parity tests

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- types/ loom/cmd haven/node haven/cmd sdk/cmd`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P3
- **Effort**: M
- **Risk**: LOW
- **Depends on**: 007 (both edit `loom/cmd/loomctl/main.go`)
- **Category**: tech-debt / tests
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

Two paired-but-separated duplications:

1. Management action strings (`"import"`, `"gc"`, `"supply_cleanup"`,
   `"image_cleanup"`) are wire-contract values, yet they are spelled as raw
   literals on both ends: produced in `loom/cmd/loomctl/main.go` (each twice —
   `case` label and `request.Action = ...`) and consumed in
   `haven/node/management.go:107-126`. Wire vocabulary belongs in `types`,
   next to `ManagementRequest` itself, defined once.
2. Each CLI keeps two manually synchronized lists: the `flags.String(...)`
   registrations and the settings struct's `mapstructure` tags. A struct field
   without a matching flag is silently never set (config file key rejected as
   unknown, env never bound) — an asymmetric failure only a user notices.

## Current state

- `types/management.go:68` — `type ManagementRequest struct { Action string ... }`;
  the package already defines `types.ErrNotFound`-style constants in
  `types/errors.go`, so constants are an established idiom here.
- `loom/cmd/loomctl/main.go:70-95` — literal action strings; :107 repeats
  `"gc"` in the key exemption.
- `haven/node/management.go:107-126` — `switch` over the same literals.
- Flag/struct pairs: `sdk/cmd/havenctl/config.go` (6 flags ↔ `settings`),
  `loom/cmd/loomctl/config.go` (15 flags ↔ `config.CLI` in
  `loom/config/cli.go`), `haven/cmd/haven/config.go` (30 flags ↔
  `config.Daemon` in `haven/config/daemon.go`). Each `parseConfig` builds its
  `pflag.FlagSet` inline.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Tests | `just test` | all pass |
| Vet/fmt | `just check` | exit 0 |
| Smoke | `just smoke` | all pass |

## Scope

**In scope**:
- `types/management.go`
- `loom/cmd/loomctl/main.go`, `haven/node/management.go`
- `sdk/cmd/havenctl/config.go` + new `sdk/cmd/havenctl/config_test.go`
- `loom/cmd/loomctl/config.go` + new `loom/cmd/loomctl/config_test.go`
- `haven/cmd/haven/config.go` + new `haven/cmd/haven/config_test.go`

**Out of scope**:
- `common/config/load.go` (the loader already validates what it can)
- Changing any action string value or flag name/default
- The `checkpoint_delete` pair (plan 005)

## Git workflow

- Branch: `advisor/010-action-constants-flag-parity`
- Commits: `refactor: define management action constants in types` and
  `test: pin CLI flag and settings parity`

## Steps

### Step 1: Action constants in types

In `types/management.go`, directly above `ManagementRequest`:

```go
// Management actions accepted by the private administration socket. Both the
// daemon and loomctl must use these constants; the strings are wire contract.
const (
	ManagementActionImport        = "import"
	ManagementActionGC            = "gc"
	ManagementActionSupplyCleanup = "supply_cleanup"
	ManagementActionImageCleanup  = "image_cleanup"
)
```

**Verify**: `GOWORK=off go -C types build ./...` → exit 0.

### Step 2: Use the constants at both ends

In `loom/cmd/loomctl/main.go`, replace every literal in the management switch
(`case types.ManagementActionImport:` etc. — note the CLI names use hyphens
while actions use underscores; keep `case "supply-cleanup":` CLI spellings as
labels and assign `request.Action = types.ManagementActionSupplyCleanup`) and
the `"gc"` in the key-exemption check. In `haven/node/management.go`, replace
the case labels with the constants.

**Verify**: `rg -n '"import"|"gc"|"supply_cleanup"|"image_cleanup"' loom/cmd haven/node types/`
→ matches only inside the const block (and legitimate non-action uses, if any
— inspect each); `just build` → exit 0.

### Step 3: Extract flag registration helpers

In each of the three CLI `config.go` files, extract the flag registration into
a package-level helper used by `parseConfig`:

```go
func newFlagSet() *pflag.FlagSet {
	flags := pflag.NewFlagSet("havenctl", pflag.ContinueOnError)
	flags.SetInterspersed(false)
	flags.String("config", "", "explicit configuration file")
	// ... unchanged registrations ...
	return flags
}
```

`parseConfig` then starts with `flags := newFlagSet()`. No registration text
changes.

**Verify**: `just build` → exit 0; `bin/havenctl --help`,
`bin/loomctl --help`, `bin/haven --help` outputs unchanged.

### Step 4: Parity tests

Create `config_test.go` in each of the three cmd packages with the same shape:

```go
package main

import (
	"reflect"
	"testing"
)

func TestFlagsMatchSettings(t *testing.T) {
	flags := newFlagSet()
	remaining := map[string]bool{}
	flags.VisitAll(func(f *pflag.Flag) { remaining[f.Name] = true })
	typ := reflect.TypeOf(settings{}) // loom: config.CLI{}; haven: config.Daemon{}
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("mapstructure")
		if !remaining[tag] {
			t.Errorf("settings field %q has no registered flag", tag)
		}
		delete(remaining, tag)
	}
	for name := range remaining {
		t.Errorf("flag %q has no settings field", name)
	}
}
```

(Add the `pflag` import; loomctl's test uses `config.CLI`, haven's uses
`config.Daemon`.) The flag names and mapstructure tags are hyphenated on both
sides, so a direct string comparison is correct.

**Verify**: each new test passes; deliberately remove one flag registration,
confirm the test fails, restore.

### Step 5: Gates

**Verify**: `just test` → all pass; `just check` → exit 0; `just smoke` → all
pass.

## Test plan

The parity tests ARE the test deliverable (Step 4). Structural pattern: there
is no existing cmd-level test to copy; keep the three test files line-for-line
parallel except for the settings type. Behavior gate for the constants:
`just smoke` drives `loomctl gc --dry-run` against a real daemon.

## Done criteria

- [ ] Four action constants defined once in `types/management.go` and used at
      both ends
- [ ] Three `config_test.go` parity tests exist and pass (including the
      remove-a-flag failure check, then reverted)
- [ ] `just build`, `just test`, `just check`, `just smoke` pass
- [ ] No string value, flag name, default, or help text changed
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- An action literal appears somewhere not listed (e.g. docs generation,
  another switch) — report the additional site before editing it.
- A CLI registers a flag intentionally absent from the struct (or vice versa)
  — report it; do not delete the flag or the field.

## Maintenance notes

- New management actions: add the constant in `types`, never a literal.
- New CLI flags: the parity tests fail until both lists are updated — that is
  the intended guard.
