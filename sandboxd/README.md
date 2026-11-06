# sandboxd API and lifecycle

The Go module `github.com/woxQAQ/sandboxd` provides the sandbox service, HTTP control API, end-to-end guest SDK, host egress manager and CLI. Build with `make build`; deployment and metadata recovery are described in [deploy/sandboxd](../deploy/sandboxd/README.md).

## Configuration

The HTTP API uses Gin, including authenticated route groups, raw Upgrade connections and Preview forwarding. Daemon and CLI configuration use Viper with `--config FILE`, retaining the existing flags. Explicit flags override environment variables, then file values, then defaults. YAML, JSON and TOML files use flat flag names such as `engine-socket` and `root-token-file`; environment names use `SANDBOXD_` or `SANDBOXCTL_` and replace hyphens with underscores. Files can also be selected with `SANDBOXD_CONFIG` or `SANDBOXCTL_CONFIG`. Unknown file keys and invalid typed values are errors; configuration is loaded once at startup.

See the [daemon example](../deploy/sandboxd/sandboxd.example.yaml) and [client example](../deploy/client/sandboxctl.example.yaml). Token contents remain in private files. CLI options precede the command, so guest command arguments keep their original meaning.

## Tokens and commands

Use `Authorization: Bearer TOKEN`. Root tokens administer tenants and metrics; tenant tokens manage their templates/sandboxes/checkpoints; sandbox tokens are restricted to their own sandbox. Root may select an existing tenant with `X-Tenant-ID`. Foreign and missing objects return the same 404 body.

Mutations require a stable `Idempotency-Key`. The same key and parameters return the original operation/result, including terminal failure. Changed parameters conflict. A canceled HTTP request stops waiting while durable work continues. `Prefer: respond-async` returns 202 with an operation and `Location`; poll that location with the same authority. Result responses can contain sandbox tokens, so store them privately.

Create a tenant using a root token and `POST /v1/tenants`:

```json
{"id":"team","max_sandboxes":20,"max_storage_mib":65536}
```

The response returns the tenant token when generated. With that tenant authority, register a template using `POST /v1/templates` and a unique idempotency key:

```json
{
  "action":"template_put",
  "template":{
    "id":"python",
    "image":"sha256:REPLACE_WITH_IMPORTED_MANIFEST_HEX",
    "shape":{"vcpus":1,"memory_mib":512,"disk_mib":512},
    "target_warm":0
  }
}
```

Replace the digest with an already imported engine image. `template_put` registers cold provisioning or an existing published golden snapshot. `template_build` creates a build VM, runs optional `template.warmup`, prepares the guest for capture, publishes a golden snapshot and retires the build VM. Build warmup uses a deterministic keyed guest execution intent. Build network currently remains blocked, so bake dependencies into the image or supply them without external fetches. Increase `target_warm` on a published golden template to maintain a pool.

Claim using `POST /v1/claim`:

```json
{
  "template_id":"python",
  "ttl_seconds":300,
  "on_expire":"pause",
  "auto_renew":true,
  "policy":{"mode":"none"},
  "env":{"PROJECT":{"value":"demo"}}
}
```

Provisioning atomically transfers a matching warm instance, otherwise clones the frozen golden snapshot or cold boots the image. A rejected warm activation is durably marked and cleaned before using golden/cold supply, with distinct activation keys. Replay resumes the same replacement. Every delivery checks engine readiness, disables the NIC, probes the workload portal, binds egress and activates the guest before enabling a verified direct NIC.

## Routes

| Route | Behavior |
| --- | --- |
| `GET /healthz` | Public process health |
| `GET /metrics` | Root-only sandbox state counts |
| `POST /v1/tenants` | Root tenant/quota/token administration |
| `GET /v1/templates`, `POST /v1/templates` | List/register/build tenant templates |
| `POST /v1/claim` | Claim a sandbox |
| `GET /v1/sandboxes`, `GET /v1/sandboxes/{id}` | Tenant/sandbox inventory |
| `POST /v1/sandboxes/{id}/{action}` | `renew`, `configure`, `pause`, `resume`, `checkpoint`, `fork`, `rollback`, `release` |
| `DELETE /v1/sandboxes/{id}` | Release without waking a paused sandbox |
| `GET /v1/checkpoints`, `DELETE /v1/checkpoints/{id}` | List/delete checkpoints |
| `POST /v1/checkpoints/{id}/fork` | Fork a published checkpoint |
| `GET /v1/operations/{id}` | Durable operation status |
| `GET /v1/sandboxes/{id}/connect` | HTTP/1.1 Upgrade to raw guest workload portal |
| `POST /v1/sandboxes/{id}/preview?port=8080&ttl_seconds=60` | Signed Preview URL; send `{}` body |
| `POST /v1/drain` | Root-only stop of new claims/refills |

Bodies are strict JSON with a 1 MiB limit. Fork takes `count`; rollback takes `checkpoint_id`. Checkpoint creation returns a sandbox checkpoint referring to a published engine snapshot. A paused source can checkpoint or fork without waking. Release closes active data streams and deletes dedicated resources. Repeated operation requests converge on the same result.

`paused` means an immutable sleep snapshot and no VMM. Concurrent data requests converge on one resume operation and new clone. Resume preserves machine identity, shell/process state and the guest execution ledger; fork resets identity/environment/history. Failed resume retains the sleep snapshot and permits a later distinct attempt. Reaper actions record their expected deadline and recheck it under the sandbox lock, so stale expiry work cannot delete a renewed sandbox.

`auto_renew` refreshes TTL when a data connection is established or a Preview HTTP request is admitted. Expired leases cannot be revived by activity. Existing raw streams are not decoded by the host, so internal RPC frames do not continuously renew a long-lived connection; call `renew` explicitly for that case. Rebind work that outlives the lease cannot renew it afterward.

## Raw protocol and SDK

Connect with `Connection: Upgrade` and `Upgrade: sandbox-v1`. After 101, the host copies bytes with bounded buffers/backpressure, preserving buffered upgrade bytes and half-close. Only the Rust guest and client parse user frames. Guest host administration is on port 2046, system control on 2047 and unprivileged workload RPC on 2048. Workload RPC cannot invoke administrative activation or reseeding.

Frames are generated Protobuf preceded by a four-byte big-endian length. Maximum frame size is 8 MiB; data chunks are 64 KiB. See [protocol](../protocol/README.md) and [guest RPC](../guest/README.md). `port_forward` must be first on a fresh connection: one ready frame, then entirely raw duplex bytes with no final Protobuf frame.

```go
client := sdk.New("https://sandbox.example", tenantToken)
result, err := client.Claim(ctx, "persisted-claim-key", sandboxd.Command{
    TemplateID: "python", TTLSeconds: 300,
    Policy: egress.Policy{Mode: "none"},
})
if err != nil { return err }
conn, err := client.Connect(ctx, result.Sandbox.ID)
if err != nil { return err }
defer conn.Close()
_, err = conn.Call(ctx, "exec", map[string]any{
    "exec_key": "persisted-work-step", "command": "python3 --version",
}, consumeOutputFrame)
```

Use imports `github.com/woxQAQ/sandboxd`, `/sdk`, `/egress`, and the generated frame type for the output callback. A keyed exec records intent before spawn, deduplicates matching inputs and conflicts on changed inputs. An unresolved intent after agent restart returns `result_unknown` instead of rerunning side effects. Keyed stdin/session/PTY restrictions and bounded saved output are documented in the guest guide. Frame IDs alone are correlation IDs. `Call` returns terminal guest parameters even on error; `errors.As(err, &rpcError)` with `var rpcError *sdk.RPCError` exposes `Code` and `ResultState`, including `result_unknown`. Unkeyed exec is not deduplicated; the SDK never blindly retries uncertain side effects. The ledger is part of VM snapshot history: rollback to before an intent also rolls back that record, so cross-rollback deduplication requires an external host ledger/fence.

CLI examples use tokens from private files:

```sh
sandboxctl --token-file /private/tenant-token list
sandboxctl --token-file /private/tenant-token --key claim-001 claim < claim.json
sandboxctl --token-file /private/sandbox-token exec SANDBOX_ID 'python3 --version'
sandboxctl --token-file /private/tenant-token --key pause-001 change SANDBOX_ID < pause.json
```

`pause.json` contains `{"action":"pause"}`. `sandboxctl exec` uses unkeyed execution; use the SDK's explicit `exec_key` for durable deduplication.

## Egress, Preview and boundaries

Relay mode applies host/DNS/address checks before pinned numeric dialing and verifies upstream TLS. Host-only env values use `guest:false` and an `inject` rule; only the harmless placeholder reaches guest state. Secret values never enter SQLite or snapshots. They must be resupplied after restart. Injection requires a durable sanitized audit record before header replacement. See [egress](egress/README.md) for matching, trusted CA, reflection handling and SOCKS restrictions.

Direct mode accepts canonical IPv4 `allowed_cidrs` and `max_connections` (default 256), installs them through the engine policy API, and denies private/reserved/metadata targets. Nonempty `allowed_hosts` and host-only credentials are rejected. Empty direct CIDRs deny all. Use explicit `0.0.0.0/0` for permitted public destinations. Host bandwidth and conntrack allocation quotas remain deployment responsibilities.

Preview capabilities bind tenant, sandbox, creation epoch, port and expiry capped by lease. Authorization/Cookie headers are stripped before guest forwarding; responses enforce `Referrer-Policy: no-referrer`. Preview uses a signed path on the service origin. It does not provide per-sandbox browser origins or rewrite absolute asset URLs; use a separate origin gateway when that isolation is needed.

Current bounds include managed-only process inspection/attach, output-only attach and nonrecursive watch. [Tree transfer](../guest/README.md#directory-tree-transfers) preserves supported file/link/mode/mtime/user-xattr semantics and normalizes imported ownership to the workload UID/GID. The engine collects proven unreachable caches, completed import work and retired resource versions/backings; published images and operation/idempotency history stay retained, while uncertain ownership and recovery evidence fence GC. Audit is locally chained without external rollback anchoring. Optional gossip placement and remote archives are outside this single-host implementation. Real production acceptance has passed the privileged launcher and live daemon-failure adoption, complete 512 MiB lifecycle, golden/warm provisioning and guest/TAP policy+SNAT TCP after fork/resume. Standalone KVM/guest and kernel isolation acceptance complement unit and Rust fixture tests. Reproduction commands, exact limits and deployment-specific qualification are in [the acceptance record](../deploy/ACCEPTANCE.md).
