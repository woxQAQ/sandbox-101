# Plan 006: Unify sdk receiver names

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- sdk/`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

Repo convention (established by commit `f08d9f7` "replace 'service' receivers
with type-derived short names") is a single type-derived letter per receiver.
In `sdk`, `client.go` follows it (`c *Client` on all 11 methods) but
`management.go` does not: it uses `client` for `*ManagementClient` and — worse
— defines one `*Client` method (`Status`) with receiver `client`, so the same
type has two receiver names within one package. Identical things should be
identical.

## Current state

- `sdk/client.go` — all `*Client` methods use receiver `c` (e.g. :95
  `func (c *Client) Claim(...)`).
- `sdk/management.go` — :33/:40/:48 use `func (client *ManagementClient)`;
  :55 defines `func (client *Client) Status(...)`, a `Client` method living in
  the wrong file with the wrong receiver name.
- Convention exemplar (from `f08d9f7`): `func (a *Authentication) ...`,
  `func (t *Tenants) ...`, `func (p *Plane) ...`.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build sdk | `GOWORK=off go -C sdk build ./...` | exit 0 |
| Test sdk | `GOWORK=off go -C sdk test ./... -count=1` | all pass |
| Vet/fmt | `just check` | exit 0 |

## Scope

**In scope**:
- `sdk/management.go`
- `sdk/client.go` (only to receive the moved `Status` method)

**Out of scope**:
- Any other package; any method body change; any rename of exported API.

## Git workflow

- Branch: `advisor/006-sdk-receivers`
- Commit: `refactor: unify sdk receiver names with type-derived short names`

## Steps

### Step 1: Move `Status` for `*Client` home

Move `func (client *Client) Status` from `sdk/management.go:55` into
`sdk/client.go`, placed next to the other read methods (`Get`, `Operation`),
with receiver renamed to `c`. Body unchanged.

**Verify**: `GOWORK=off go -C sdk build ./...` → exit 0.

### Step 2: Rename ManagementClient receivers

In `sdk/management.go`, rename receiver `client` → `m` on
`Status` (:33), `Manage` (:40), `Operation` (:48). Inside those bodies,
`client.client` becomes `m.client`. In `NewManagement`, the local variable
`client` may stay (it is a local, not a receiver) — but if the struct literal
`&ManagementClient{client: client}` reads awkwardly, `&ManagementClient{client: New(...)}`
is acceptable; keep changes minimal.

**Verify**: `rg -n "func \((c|client|m) \*(Client|ManagementClient)\)" sdk/` →
every `*Client` method uses `c`, every `*ManagementClient` method uses `m`;
`GOWORK=off go -C sdk test ./... -count=1` → all pass.

### Step 3: Gates

**Verify**: `just check` → exit 0.

## Test plan

No new tests (mechanical rename). `sdk/client_test.go` must pass unmodified.

## Done criteria

- [ ] All `*Client` methods live in `client.go` with receiver `c`
- [ ] All `*ManagementClient` methods live in `management.go` with receiver `m`
- [ ] `just check` exits 0; sdk tests pass
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- Another file in `sdk` defines methods on either type with yet another
  receiver name — report the full inventory before proceeding.

## Maintenance notes

- New sdk methods: receiver is the type's lowercase initial (`c`, `m`), and the
  method lives in the file named after the type's client role.
