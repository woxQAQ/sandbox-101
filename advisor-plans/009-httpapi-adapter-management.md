# Plan 009: Give httpapi Adapter its Management dependency

> **Executor instructions**: Follow step by step, run every verification. On
> any STOP condition, stop and report. When done, update the status row in
> `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- haven/httpapi`
> On drift, compare excerpts below against live code; mismatch → STOP.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: 005 (both edit `haven/httpapi/http.go`)
- **Category**: tech-debt
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

`httpapi.Services` advertises nine capabilities, but `Adapter` stores only
eight: `Management` (and its `status`/`drain` routes) bypass the adapter and
are closed over from `dependencies` inside `New`. Same kind of dependency, two
different access paths — and the route table mixes adapter methods with
closures that secretly capture the constructor argument. The Adapter should
own every capability it serves, and the route table should be a flat,
eyeball-diffable list of `adapter.X` handlers.

## Current state

`haven/httpapi/http.go` (at 129c156):

- `Services` (:26) field order: Authentication, Catalog, Templates, Operations,
  Delivery, Tenants, Requests, Dataplane, Management.
- `Adapter` (:38) fields in a different order: requests, plane,
  authentication, catalog, templates, operations, delivery, tenants — no
  management.
- `/v1/status` (≈ :97) and `/v1/drain` (≈ :102) are closures using
  `dependencies.Management` directly; every other route uses `adapter.*`.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build | `just build` | exit 0 |
| Tests | `GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` | all pass |
| Smoke | `just smoke` | all pass |
| Vet/fmt | `just check` | exit 0 |

## Scope

**In scope**:
- `haven/httpapi/http.go`

**Out of scope**:
- `haven/management/http.go` (the UDS management adapter — separate surface)
- Route paths, methods, or any response body
- `haven/httpapi/data.go`, `operation.go`

## Git workflow

- Branch: `advisor/009-adapter-management`
- Commit: `refactor: route httpapi status and drain through the adapter`

## Steps

### Step 1: Add the field and align order

Add `management *node.Management` to `Adapter` and populate it in `New`.
Reorder `Adapter`'s field declarations and the literal in `New` to match
`Services` field order exactly (authentication, catalog, templates,
operations, delivery, tenants, requests, plane/dataplane, management) so the
two structs diff cleanly side by side.

**Verify**: `GOWORK=off go -C haven build ./httpapi/` → exit 0.

### Step 2: Convert the two closures to methods

Add to `http.go` (next to `putTenant`/`metrics`):

```go
func (adapter *Adapter) status(c *gin.Context) {
	value, err := adapter.management.Status(c.Request.Context(), principal(c))
	respond(c.Writer, value, err)
}
```

and a `drain` method moving the root check and `Drain()` call from the
closure. Replace the route registrations with
`v1.GET("/status", adapter.status)` and `v1.POST("/drain", adapter.drain)`.

**Verify**: `rg -n "dependencies\." haven/httpapi/http.go` → no matches outside
the `New` function body; `just build` → exit 0.

### Step 3: Gates

**Verify**: `just check` → exit 0;
`GOWORK=off go -C haven test ./... -p=1 -count=1 -timeout=120s` → all pass;
`just smoke` → all pass (smoke drives `havenctl status`, which hits
`/v1/status`).

## Test plan

No new tests. `just smoke` covers `/v1/status` via `havenctl status`;
component tests cover the drain flag behavior. Both must pass unmodified.

## Done criteria

- [ ] `Adapter` holds all nine capabilities; field order matches `Services`
- [ ] Route table contains only `adapter.*` handler references for `/v1/*`
- [ ] `just build`, `just check`, `just smoke` pass
- [ ] No response or status-code change
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- Something else also captures `dependencies` after `New` returns (e.g. the
  `NoRoute`/`NoMethod` handlers capturing more than `adapter`) — report before
  restructuring those.

## Maintenance notes

- Future capabilities: add to `Services`, add to `Adapter` in the same
  position, expose via an `adapter.*` method. The route table should stay a
  flat list with no closures.
