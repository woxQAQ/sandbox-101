# sandboxd API and lifecycle

The Go module `github.com/woxQAQ/sandboxd` provides the sandbox service, HTTP control API, end-to-end guest SDK, host egress manager and CLI. Build with `make build`; deployment and metadata recovery are described in [deploy/sandboxd](../deploy/sandboxd/README.md).

## Configuration

The HTTP API uses Gin, including authenticated route groups, raw Upgrade connections and Preview forwarding. Daemon and CLI configuration use Viper with `--config FILE`, retaining the existing flags. Explicit flags override environment variables, then file values, then defaults. YAML, JSON and TOML files use flat flag names such as `engine-socket` and `root-token-file`; environment names use `SANDBOXD_` or `SANDBOXCTL_` and replace hyphens with underscores. Files can also be selected with `SANDBOXD_CONFIG` or `SANDBOXCTL_CONFIG`. Unknown file keys and invalid typed values are errors; configuration is loaded once at startup.

See the [daemon example](../deploy/sandboxd/sandboxd.example.yaml) and [client example](../deploy/client/sandboxctl.example.yaml). Token contents remain in private files. CLI options precede the command, so guest command arguments keep their original meaning.

## Tokens and commands

Use `Authorization: Bearer TOKEN`. Root tokens administer tenants and metrics; tenant tokens manage their templates/sandboxes/checkpoints; sandbox tokens are restricted to their own sandbox. Root may select an existing tenant with `X-Tenant-ID`. Foreign and missing objects return the same 404 body.

Mutations require a stable `Idempotency-Key`. The same key and parameters return the original operation/result, including terminal failure. Changed parameters conflict. A canceled HTTP request stops waiting while durable work continues. `Prefer: respond-async` returns 202 with an operation and `Location`; poll that location with the same authority. Sandbox tokens may poll only their own renew/release operations, including the completed release after termination; terminated credentials remain rejected on other routes. An operation ID is not an authorization credential. Result responses can contain sandbox tokens, so store them privately.

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

Replace the digest with an already imported engine image. `template_put` registers cold provisioning or an authorized published golden snapshot. Snapshot provenance is private to one tenant: tenants can register their own sandboxd captures; external golden snapshots require a root `template_put` with an explicit target tenant (`X-Tenant-ID` over HTTP). Published engine state alone grants no tenant access. Root import cannot transfer an existing snapshot owner. Root may pause or checkpoint an existing sandbox with its default or selected request tenant; every resulting capture remains owned by the source sandbox's tenant, including receipt recovery. Captures and root grants are immutable SQLite authorization records, rechecked before publication, clone and warm refill; these records do not retain physical snapshots for GC. `template_build` creates a build VM, runs optional `template.warmup`, prepares the guest for capture, publishes a golden snapshot and retires the build VM. Build warmup uses a deterministic keyed guest execution intent. Build network currently remains blocked, so bake dependencies into the image or supply them without external fetches. Increase `target_warm` on a published golden template to maintain a pool.

The first admission of a template name reserves it permanently to that tenant (`template_intents`): another tenant's submission gets the same 404 as a foreign published name, a second in-flight publication from the owning tenant conflicts, and a failed build keeps the reservation so only the owner retries. Root updates require the explicit owning target tenant and never transfer the reservation. A rolling rebuild keeps serving the previously published snapshot until the new one publishes.

A published template with a missing ownership placeholder or a conflicting tenant requires manual recovery; admission preserves the existing rows and never reconstructs an owner from the published catalog. A publication pointer is reusable only when it references a `succeeded` or `failed` template operation for the same tenant and template. Missing operations, unknown statuses or inconsistent provenance also require manual recovery; foreign tenants still receive the uniform 404. Publication verifies ownership and writes the catalog in one transaction under the resource ownership guard, after guest and engine calls finish.

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

`paused` means an immutable sleep snapshot and no VMM. Concurrent data requests converge on one resume operation and new clone. Resume preserves machine identity, shell/process state and the guest execution ledger; fork resets identity/environment/history. Publishing a running sandbox atomically transfers its old sleep snapshot holder to a durable cleanup intent while retaining its full resource charge. Cleanup retries independently, preserves live shared references, and retains the tenant charge until physical reclamation. A later pause always captures the current running state. Failed resume retains the sleep snapshot and permits a later distinct attempt. Reaper actions record their expected deadline and recheck it under the sandbox lock, so stale expiry work cannot delete a renewed sandbox.

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

Metadata schema 5 preserves snapshot authorization, configuration fences, template ownership placeholders and a durable resource quota ledger through backup/restore. Schema 1–4 and future versions are rejected before schema DDL. No labels, sandbox JSON or existing template references are used to migrate a ledger or infer owners. Stop the service and preserve a consistent backup before planning an explicit migration; do not overwrite old state to bypass the version boundary.

## Durable quota responsibility

`quota_resources` is the only source of quota totals. `quota_refs` records holders of those resources; it grants no engine deletion authority. A resource without references still counts until a trusted engine receipt or NotFound proves reclamation. Query failures and unknown dispatch outcomes retain the charge. `snapshot_owners` remains authorization provenance and is never a billing table or GC root.

D denotes the shape's writable disk MiB, and M its captured memory MiB. Running guest RAM is not disk usage. These are conservative logical MiB; neither reflinks nor hardlinks promise physical savings to admission.

| Resource kind | Stable resource key | Slots | Storage MiB | Holders and retirement proof |
| --- | --- | ---: | ---: | --- |
| `sandbox_slot` | sandbox ID | 1 | 0 | Admission through unknown allocation, live VM or exclusive sleep cleanup; retired after proven VM/unknown-allocation reclamation and either snapshot reclamation or transfer to an independent live holder |
| `vm_disk` | VM ID; independent restore versions also include the engine restore receipt ID | 0 | D per independent disk | Sandbox and exact allocation receipt; VM deletion/NotFound retires its disks and backing holders together |
| `snapshot` | snapshot ID, deduplicated within its trusted tenant | 0 | D+M | Sleep, checkpoint, template, operation dependency, VM/warm memory backing or cleanup; physical absence plus no live/dependent holders retires the charge |
| `pending_storage` | deterministic operation/step key | 0 | Cost of the intended allocation | Persisted before dispatch; receipt transfers the charge to its actual identity in one transaction. A terminal receipt without an identity, or a durable proof that dispatch never began, can cancel it |

`active_use` means an actual consumer, `engine_dependency` means allocation/dispatch has not resolved, and `cleanup` means retained deletion responsibility. Cleanup is not a permanent GC blocker. The existing ownership checks choose one collector per identity and exclude only its own/transferred holders; other active catalog users and unresolved dependencies block deletion. Engine deletion of a snapshot catalog entry does not reclaim a clone's hardlinked memory backing. The ledger retains that snapshot's full conservative D+M charge through its `backing:VM_ID` holder until the VM is deleted or proven absent. A stopped VM still retains its backing. Already billed snapshots also track existing template/warm consumers without introducing template-build or warm admission policy; those policies remain plan013.

| Lifecycle phase | Durable responsibility before engine work | Receipt/publication transaction | Failure and cleanup |
| --- | --- | --- | --- |
| claim / fork branch | slot + pending disk; shared source snapshot dependency; running fork additionally reserves D+M capture | Pending disk/capture becomes its receipt identity; clone adds backing holder | Unknown dispatch keeps pending cost and slot; failed branches retain physical allocations through compensation |
| pause | Existing VM disk + new D+M capture reservation | Capture gains sleep holder; confirmed VM deletion retires only that VM's disks/backings | Failed VM deletion retains both disk and snapshot; exclusive sleeping deletion failure retains slot + D+M |
| resume | Sleep snapshot + pending disk D | Clone gains independent disk and backing; running publication transfers sleep holder to cleanup atomically | Clearing `Sandbox.SnapshotID` never clears backing cost; failed clone compensation retains the sleep capture |
| checkpoint | Running source reserves D+M; sleeping source shares its SID without another reservation | Checkpoint publication and active holder commit together | A failed or unknown capture retains its actual/reserved cost; checkpoint deletion transfers/removes only its own holder |
| rollback | Target snapshot dependency + D for a new clone or independent restore disk version | Receipt records the exact clone/version and backing | Older versions/backings remain conservatively charged until VM deletion because the public engine API supplies no per-version GC proof |
| release | Terminal access marker and cleanup-holder transfer commit together | VM/snapshot proof retires only physically reclaimed responsibility | `terminated` revokes access immediately. Delete failure, lost response, restart and unknown instance do not free quota |
| backup / restart | SQLite snapshot copies both ledger tables and ownership/fence tables | Restored metadata remains behind the existing recovery acknowledgement fence | Startup rejects negative/noninteger amounts, unsupported kinds, dangling references and live sandboxes without slots |

Every admitted operation persists `ChargeTenantID` separately from request/idempotency `TenantID`. Existing sandbox/checkpoint actions charge their trusted owner even when root uses its default identity or selects another tenant. Root bypasses admission limits only, while recording the same full responsibility. Maintenance and recovery allocate against the owner's ordinary budget. Public authorization, idempotency namespaces and failed client results remain unchanged.

Boundary examples for D=64 and M=128: cold claim = 64; running checkpoint or resume peak = 64+192=256; running one-branch fork peak = source disk 64 + branch disk 64 + capture/backing 192 = 320; shared checkpoint/backing plus a new pause capture peak = VM disk 64 + shared SID 192 + new SID 192 = 448. The corresponding 63/255/319/447 limits reject before allocation. A running rollback retaining its original disk uses original D + restored D + checkpoint D+M = 320. Multiple holders of one SID never add another 192.

## Egress, Preview and boundaries

Relay mode applies host/DNS/address checks before pinned numeric dialing and verifies upstream TLS. Host-only env values use `guest:false` and an `inject` rule; only the harmless placeholder reaches guest state. Secret values never enter SQLite or snapshots. They must be resupplied after restart. Injection requires a durable sanitized audit record before header replacement. See [egress](egress/README.md) for matching, trusted CA, reflection handling and SOCKS restrictions.

Direct mode accepts canonical IPv4 `allowed_cidrs` and `max_connections` (default 256), installs them through the engine policy API, and denies private/reserved/metadata targets. Nonempty `allowed_hosts` and host-only credentials are rejected. Empty direct CIDRs deny all. Use explicit `0.0.0.0/0` for permitted public destinations. Host bandwidth and conntrack allocation quotas remain deployment responsibilities.

`configure` on a running sandbox revokes the existing proxy binding and closes live streams before the new environment or policy is published. Admission records a durable configuration fence with the operation: if revocation, engine networking, the agent probe or activation fails, the sandbox stays fenced with the old data path closed, `last_error` records the diagnostic, and automatic resume/rebind and maintenance never reopen the proxy. Only the matching explicit `configure` that finishes activation and delivery clears its fence; an older operation cannot clear a newer fence, and `release` retires it with teardown. Final sandbox delivery, the successful operation result and retirement of its fence commit together; a final persistence failure revokes the new binding and keeps the fence. A superseding admission also closes any binding published by a blocked older worker, which must recheck ownership before publishing or completing. Maintenance revokes locally owned proxies even when VM verification is unknown, and changes the NIC only for a verified matching instance. Retry a failed change with a new intent (new idempotency key).

Preview capabilities bind tenant, sandbox, creation epoch, port and expiry capped by lease. Authorization/Cookie headers are stripped before guest forwarding; responses enforce `Referrer-Policy: no-referrer`. Preview uses a signed path on the service origin. It does not provide per-sandbox browser origins or rewrite absolute asset URLs; use a separate origin gateway when that isolation is needed.

Current bounds include managed-only process inspection/attach, output-only attach and nonrecursive watch. [Tree transfer](../guest/README.md#directory-tree-transfers) preserves supported file/link/mode/mtime/user-xattr semantics and normalizes imported ownership to the workload UID/GID. The engine collects proven unreachable caches, completed import work and retired resource versions/backings; published images and operation/idempotency history stay retained, while uncertain ownership and recovery evidence fence GC. Audit is locally chained without external rollback anchoring. Optional gossip placement and remote archives are outside this single-host implementation. Real production acceptance has passed the privileged launcher and live daemon-failure adoption, complete 512 MiB lifecycle, golden/warm provisioning and guest/TAP policy+SNAT TCP after fork/resume. Standalone KVM/guest and kernel isolation acceptance complement unit and Rust fixture tests. Reproduction commands, exact limits and deployment-specific qualification are in [the acceptance record](../deploy/ACCEPTANCE.md).
