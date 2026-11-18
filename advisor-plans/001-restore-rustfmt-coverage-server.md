# Plan 001: Restore rustfmt coverage on guest `server.rs`

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving on. If
> anything in "STOP conditions" occurs, stop and report — do not improvise.
> When done, update the status row for this plan in `advisor-plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 129c156..HEAD -- guest/agent/src/server.rs`
> If the file changed, compare the "Current state" excerpts against the live
> code; on mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW (pure reformatting; no logic change permitted)
- **Depends on**: none
- **Category**: tech-debt / dx
- **Planned at**: commit `129c156`, 2026-11-18

## Why this matters

`just check` runs `cargo fmt --all -- --check`, which is the repo's formatting
gate. That gate currently passes on `guest/agent/src/server.rs` **even though
the file contains hand-compressed code rustfmt would normally rewrite** — e.g.
`let slots=if portal==Portal::Workload{agent.requests.clone()}else{...};`.
Verified mechanism: `rustfmt --edition 2024 --emit stdout` reproduces the file
byte-for-byte, while the same constructs in isolation are rewritten. rustfmt
fails to format `serve_connection` (and likely `dispatch`/`Output::send`),
silently keeps the original text, and `--check` then sees "no diff". The
formatting gate has a hole exactly over the connection-multiplexing hot path.
Fixing it means re-flowing the poisonous constructs so rustfmt can engage again.

## Current state

- `guest/agent/src/server.rs` — guest RPC connection server. Poisoned spots
  (line numbers at 129c156):
  - `Output::send` (≈ line 27): `timeout(Duration::from_secs(30),self.tx.send(frame))` — missing space after comma.
  - writer loop (≈ line 78–86): `if !matches!(tokio::time::timeout(...).await,Ok(Ok(()))) { writer_cancel.cancel(); break; }` one-liner inside `tokio::select!`.
  - main loop (≈ line 103): `let next = if let Some(frame) = first.take() { Some(frame) } else { tokio::select! { ... => result? } };` single line.
  - main loop (≈ line 108–112): `tokio::select!` statement whose handler contains `ensure!(matches!(result,Ok(Ok(()))), ...)`; missing spaces: `matches!(result,Ok(Ok(())))`.
  - ≈ line 118: `requests.insert(frame.id.clone(),input_tx); drop(requests);`
  - `dispatch` admin arm (≈ line 141–151): the whole `"quiesce"` arm is one ~500-char line.
  - `dispatch` workload arm (≈ line 162): the whole `"info"` arm is one ~500-char line.
  - `next_input` (≈ line 232): `timeout(Duration::from_secs(60),input.recv())`.
- Every other guest `.rs` file is already rustfmt-canonical (verified by
  diffing `rustfmt --emit stdout` output against each file; only `server.rs`
  deviates from normal style — `lib.rs` only looks different because rustfmt
  inlines submodules there).
- Bisect evidence: removing the main loop's two `tokio::select!` blocks lets
  rustfmt reformat the function; each one independently re-poisons it. The
  trigger is the over-long macro invocation lines rustfmt cannot split.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Format gate | `cargo fmt --all -- --check` (repo root) | exit 0 |
| Clippy | `cargo clippy --locked --workspace --all-targets -- -D warnings` | exit 0 |
| Rust tests | `cargo test --locked --workspace --all-targets` | all pass |
| Full check | `just check` | exit 0 |

## Scope

**In scope**:
- `guest/agent/src/server.rs` (whitespace/line-breaking only)

**Out of scope**:
- Any other file (all other guest files are already canonical)
- Any logic, visibility, or signature change in `server.rs`
- CI/justfile changes to the format gate

## Git workflow

- Branch: `advisor/001-rustfmt-server`
- Commit style: conventional, e.g. `refactor: reflow guest server.rs so rustfmt covers it` (see `git log --oneline`)

## Steps

### Step 1: Demonstrate the hole (canary)

Temporarily insert a deliberately misformatted line inside `serve_connection`,
e.g. after the `let cancel = CancellationToken::new();` line add
`let canary=1;let _=canary;`. Run `cargo fmt --all -- --check`.

**Verify**: gate passes (exit 0) despite the misformatting → hole confirmed.
Remove the canary line. If instead the gate FAILS at this point, the codebase
has drifted — STOP.

### Step 2: Reflow the poisonous constructs

Edit only whitespace/line breaks in `guest/agent/src/server.rs`:

1. Add the missing spaces: `(Duration::from_secs(30),self.tx.send(frame))` →
   `(Duration::from_secs(30), self.tx.send(frame))`; same for
   `(Duration::from_secs(60),input.recv())`, `matches!(result,Ok(Ok(())))`,
   `requests.insert(frame.id.clone(),input_tx)`.
2. Expand the writer-loop `if !matches!(...) { ... }` one-liner to a normal
   multi-line `if`.
3. Break the `let next = if ... else { tokio::select! { ... } };` one-liner
   into a multi-line `if/else` with the `select!` block expanded.
4. Expand the `"quiesce"` and `"info"` match arms into multi-line statements
   (chain calls on separate lines, as rustfmt would).
5. After each edit, run the canary from Step 1 inside the function you just
   touched: insert `let canary=1;let _=canary;`, run
   `cargo fmt --all -- --check`. **Expected once the function is healable:
   exit 1 with a diff reported for `server.rs`.** Remove the canary, confirm
   exit 0 again.

**Verify**: `cargo fmt --all -- --check` → exit 0, AND the canary test fails
the gate for each of the four functions (`Output::send`, `serve_connection`,
`dispatch`, `next_input`).

### Step 3: Normalize

Run `cargo fmt --all` and inspect `git diff -- guest/agent/src/server.rs`:
only whitespace/line-break changes may appear. If rustfmt reorders or rewrites
anything semantic-looking, STOP.

**Verify**: `cargo fmt --all -- --check` → exit 0; `git diff` shows no
non-whitespace change.

### Step 4: Full gates

**Verify**: `cargo clippy --locked --workspace --all-targets -- -D warnings` →
exit 0; `cargo test --locked --workspace --all-targets` → all pass.

## Test plan

No new tests (no behavior change). Existing coverage: `cargo test --locked
--workspace --all-targets` (guest agent tests live in `guest/agent/tests/`).
The canary procedure in Steps 1–2 is the regression check for the format gate
itself; do not commit the canary.

## Done criteria

- [ ] `cargo fmt --all -- --check` exits 0
- [ ] Canary misformatting inside each of `Output::send`, `serve_connection`,
      `dispatch`, `next_input` makes `cargo fmt --all -- --check` exit 1
      (verified, then reverted)
- [ ] `git diff 129c156..HEAD -- guest/agent/src/server.rs` contains only
      whitespace/line-break changes
- [ ] `cargo clippy --locked --workspace --all-targets -- -D warnings` exits 0
- [ ] `cargo test --locked --workspace --all-targets` passes
- [ ] No file other than `guest/agent/src/server.rs` modified
- [ ] `advisor-plans/README.md` status row updated

## STOP conditions

- The canary in Step 1 already fails the gate (drift).
- After reflowing every listed construct, the canary still passes silently
  inside some function → the poison is a construct not listed here; STOP and
  report which function, do not restructure the function (e.g. extracting
  closures into named async fns is NOT allowed under this plan).
- `cargo fmt --all` wants to make a non-whitespace change.

## Maintenance notes

- Future contributors writing `tokio::select!`/macro-heavy code in this file
  can re-poison rustfmt the same way; if a reviewer sees compressed one-liners
  here again, re-run the canary check.
- If the repo later pins a newer rustfmt that formats these constructs,
  `cargo fmt --all -- --check` may start failing on this file — that is the
  gate finally working; just run `cargo fmt --all`.
