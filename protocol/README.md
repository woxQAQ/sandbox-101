# Guest wire protocol v1

`sandbox.proto` is the source of truth. Rust builds types with `prost-build` in `guest/agent/build.rs`. Go generated types and framing helpers are in `protocol/gen`, whose module/import path is `github.com/woxQAQ/sandbox-protocol/gen`. Regenerate Go using `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10`, then `sh protocol/generate.sh` with that executable on PATH. Generated protobuf code is checked in; no custom protobuf serializer is used.

## Transport and authorization

Management listeners use AF_VSOCK exclusively, reject peers other than host CID 2, and do not require a guest NIC. Host Cloud Hypervisor UDS clients issue `CONNECT <port>\n` and consume its `OK <port>\n` before speaking this protocol.

| Guest port | Caller | Allowed methods |
|---|---|---|
| 2046 | sandboxd administration, never exposed through tenant relay | `activate`, `quiesce`, `prepare_template` |
| 2047 | engine | `ping`, `reseed`, `clock_sync` |
| 2048 | authenticated sandboxd tenant byte relay | Workload methods below; rejects all system/admin methods |

CID 2 identifies the host, not individual host applications. Host permissions on the per-VM UDS are authoritative. sandboxd must relay tenant traffic only to 2048 and must not permit a user-selected vsock port. Workload commands run with UID/GID 1000 by default, cleared supplementary groups and `PR_SET_NO_NEW_PRIVS`; the production binary refuses workload UID or GID zero. Workload filesystem path resolution and mutations run with the workload's filesystem credentials. These restrictions protect privileged agent configuration inside the guest; the VM and host relay remain the tenant isolation boundary.

Every message is:

```
[u32 big-endian protobuf payload length][serialized sandbox.v1.Frame]
```

Length must be 1..8,388,608 inclusive, counting the protobuf payload only. A partial prefix/payload, invalid protobuf, invalid identity or continuation, duplicate active request ID, or stalled input terminates the connection. EOF between messages closes the connection and cancels its foreground RPCs. Request/response bytes use the same Frame schema:

| Field | Meaning |
|---|---|
| `id` | Required nonempty request identifier, at most 128 UTF-8 bytes; echoed on every response. Unique among active RPCs; use a fresh identifier per call. |
| `method` | Initial request method, at most 128 bytes. Replies echo it. Continuation requests use an empty method. |
| `params` | `google.protobuf.Struct`, an object containing arguments or result/event fields. |
| `data` | Raw file/process/terminal/entropy bytes, never base64. |
| `eof` | Request: no more input for this RPC. Response: terminal completion, except raw forwarding described below. |
| `error` | Empty on requests. A terminal response with a nonempty string is failure; no successful result is implied. |

JSON-shaped examples below describe Struct contents, not a JSON wire format. Integers supplied in Struct must be nonnegative exact integers <=2^53-1. Large byte counts/offsets returned by filesystem operations are decimal **strings**. Exit codes are signed numbers, with signal exits reported as 128+signal.

Ordinary calls set request `eof=true`. Streaming input starts with `eof=false`, then sends `{id, method:"", data, eof}` chunks of at most 65,536 bytes until `eof=true`. Continuations cannot carry params. Initial data may occupy the full permitted frame. Request EOF does not close the shared transport. Responses interleave by ID, allowing 16 concurrent RPCs per connection and unlimited sequential calls. Except forwarding, the final response has `eof=true`, no data, and completion params; prior frames have `eof=false`. A failed mutation can have committed before response loss; do not automatically retry exec or non-idempotent mutations. IDs provide active-call correlation, not durable execution deduplication.

Limits: 64 workload connections, 8 system and 8 admin connections with separate admission budgets; 128 workload RPCs and 32 control RPCs globally; 8 queued response frames per connection and 8 input chunks per RPC; 30-second output-stall timeout; 60-second streaming-input idle timeout. Slow readers backpressure foreground output; a stalled or disconnected reader cancels its connection and kills foreground process groups. System capacity is separate from tenant capacity. No arbitrary command output is logged by the agent itself.

### Structured failures

Every failed RPC terminal frame retains a nonempty `error` and carries
`params.error_code`, on all three portals and on raw-forward setup failures.
Malformed transport/continuation frames can instead close the connection without
a reply. The protobuf schema and field numbers are unchanged.

| `error_code` | Caller action |
|---|---|
| `invalid` | Correct the argument type, range, path, encoding or unsupported combination; do not retry unchanged |
| `not_found` | The target file, process or session is absent; check its identity |
| `conflict` | Resolve incompatible current state, such as an exited process or cross-filesystem rename |
| `resource_exhausted` | Reduce usage or transfer size before retrying admission |
| `quiesced` | Drain existing work or await host-authorized activation before retrying |
| `unauthorized` | Use the correct portal/authority; private agent state is never exposed by filesystem RPC |
| `internal` | Unclassified I/O, syscall or internal failure; inspect diagnostics rather than assuming no side effects |
| `exec_in_progress`, `exec_key_conflict`, `exec_ledger_capacity_exhausted`, `result_unknown` | Preserve the keyed-exec rules in the [guest contract](../guest/README.md#durable-keyed-exec) |

`error` is a human diagnostic containing operation/target context and
`file:line:column` locations, not a machine-parsing contract. Diagnostic context
does not dump command text, environment values or exec keys. SDK `RPCError.Code`
exposes the category for every method; `ResultState` retains the optional
`params.result_state`. Terminal result params are returned even on failure:
in particular, `tree_push` postcommit failures retain all commit/durability and
cleanup fields alongside `error_code`. Neither classification nor an internal
error authorizes automatic replay of an uncertain mutation.

The Go host control client translates failures to sandboxd's own error contract,
not the Engine API. Guest `conflict`/`quiesced` become `conflict`, and
`resource_exhausted` remains an admission-capacity failure. A missing management
resource requires operator recovery; invalid host-constructed management
arguments, authorization failures and unknown management codes are internal
failures, not tenant validation errors or tenant-resource 404s. Private guest
diagnostics remain in the host error's non-serialized cause.
Missing, unknown or internal keyed-exec failures remain `result_unknown` at
the host, never a reason to choose a new key.

## System methods (2047)

| Method | Request | Terminal params |
|---|---|---|
| `ping` | `{}` | `{protocol:1, service:"sandbox-agent", portal:"system"}` |
| `reseed` | `{}`, `data`: 32..4096 bytes of fresh host entropy | `{entropy_bytes:number, reseed_ioctl:bool}` |
| `clock_sync` | `{unix_seconds:number, nanoseconds:number}`; nanos <1e9 | `{}` |

Reseed uses `RNDADDENTROPY` on `/dev/urandom` and `RNDRESEEDCRNG`. Only ENOTTY from the latter is treated as an older kernel without that ioctl and explicitly returns `reseed_ioctl:false`; other errors fail. It never changes machine-id or user-space RNG state. Clock sync calls `clock_settime(CLOCK_REALTIME)`. Missing device access/capabilities fail; these methods never shell out and never synthesize success.

## Administration methods (2046)

`activate` params: `{sandbox_id:string, hostname?:string, reset_identity?:bool, env?:object<string,string>, network:"none"|"relay"|"direct", ca_pem?:string}`. Returns `{sandbox_id, network, identity_reset}`.

Each activation atomically replaces `/run/sandbox.env` with a JSON string-to-string environment object, mode 0600, synced before same-directory rename and directory sync. Omitted/empty env clears previous claim variables. The file is JSON rather than shell syntax; quotes/newlines are represented without shell evaluation. Only guest-visible variables or harmless placeholders belong here; host-only credentials must never be sent. Child processes load it at creation, then apply request env overrides.

`reset_identity=true` is for forks/new identities: requires a nonempty hostname <=63 characters containing only ASCII letters/digits/dots/hyphens, calls `sethostname`, and atomically writes random `/etc/machine-id` and `/etc/hostname`. `reset_identity=false` preserves both, even if a hostname argument is supplied, for restore/auto-resume. Existing process identity caches are not rewritten. Activation side effects across separate files/syscalls are not transactional: on failure egress stays `none`, readiness is not granted, and the host must retry or dispose of the VM.

A nonempty `ca_pem` (<=128 KiB, certificate PEM envelope) replaces `/run/sandbox-ca.pem`, mode 0644. Omission/empty string removes the previous certificate. In relay mode children receive `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, and `NODE_EXTRA_CA_CERTS` pointing there. This does not edit a distribution's global trust store; applications must honor these settings. PEM content is supplied by trusted host administration; only its envelope is checked here.

`network=relay` enables the guest's localhost byte bridges `127.0.0.1:3128 -> host CID2:2049` and `127.0.0.1:1080 -> host CID2:2050`. HTTP/CONNECT/SOCKS are parsed and authorized **on the host**. The guest bridge is transparent, capped at 64 tunnels, and closes old tunnels on any activation/policy generation change. Child env receives upper/lowercase HTTP(S) proxy variables, ALL_PROXY/all_proxy `socks5h://127.0.0.1:1080`, and NO_PROXY for localhost. None/direct modes reject these bridge connections. Direct mode only records intent; host engine controls the NIC/firewall. Existing application environments and network sessions require application cooperation after policy changes; host egress authorization is always authoritative.

`quiesce` params `{}` gates new workload requests and returns `{quiescent:bool, in_flight:number, running_processes:number, sessions:number}`. It reports true only with no other workload RPCs/raw forwards, no running managed processes, and no persistent sessions. Existing work is not killed. Call again after draining; `activate` opens admission again.

`prepare_template` params `{}` gates admission, requires no workload RPCs/raw forwards or persistent sessions, disables relay, clears sandbox ID, environment, CA, machine-id, exited records and retained agent logs, then `syncfs`s root. Returns `{quiescent:true, prepared:true}`. Running detached application processes are deliberately retained for warm templates. Their memory/env, application logs, shell histories and external registration state cannot be generically scrubbed by this method; the template builder must start from a clean build instance and explicitly clean application-specific material. Failure keeps admission gated. Activated template clones reopen admission.

## Workload methods (2048)

`info {}` returns `{protocol:1, portal:"workload", sandbox_id, network, active:bool, processes:number, in_flight:number, quiesced:bool}`. `in_flight` excludes the info call itself and includes raw forwards. This handshake is permitted while quiesced; it is not an application health check.

### Processes and terminals

`exec {command:string, argv?:array<string>, cwd?:string, env?:object, detach?:bool, session_id?:string}`. Without argv, command executes through `/bin/sh -c`; with argv (including `[]`) command is the executable and argv contains its arguments, excluding argv[0]. Empty command is invalid. Default PATH is standard image paths; HOME is `/home/sandbox`; cwd defaults to the agent's working directory. Set cwd explicitly for application workspaces. Request data/continuations feed stdin; EOF closes stdin.

Foreground response starts with `{started:true, process_id:string, pid:number}`, then raw output chunks with params `{stream:"stdout"|"stderr", sequence:number, process_id}`, then final `{exit_code:number, process_id}`. Each output stream preserves its own byte order; stdout/stderr interleaving follows scheduling. Linux pidfd readiness observes exit before reaping, allowing safe process-group cleanup while the leader PID remains reserved. Shell descendants in the same process group are killed when the command finishes or disconnects. A process that deliberately escapes its process group is outside this cleanup guarantee; VM/systemd lifecycle remains the final cleanup boundary.

`detach=true` closes stdin and immediately returns `{process_id, pid, detached:true}`. Detached jobs survive client disconnects. Each managed process has a 1 MiB ring of stdout/stderr chunks, at most 256 records total; exited records expire after five minutes (30-second sweep). There is no durable process/result journal or restart recovery beyond VM snapshots.

| Method | Params | Result |
|---|---|---|
| `proc_ps` | `{}` | `{processes:[{process_id,pid,exit_code:number|null,pty:bool}]}` for managed jobs, not every OS process |
| `proc_kill` | `{process_id, signal?:number}`; default SIGTERM, 1..64 | `{}`; signals managed process group, rejects exited jobs |
| `cancel` | `{process_id}` | `{}`; SIGKILL to managed process group |
| `proc_logs` | `{process_id}` | Initial `{first_sequence,last_sequence,truncated}`, output chunks `{stream,sequence}`, terminal `{exit_code:number|null,process_id}` |
| `proc_attach` | `{process_id}` | Same replay followed by live output and final exit code; a slow subscriber exceeding 32 queued log chunks receives an explicit overflow error, then can replay proc_logs |
| `pty_open` | Same as exec, plus `rows?:number, cols?:number`; cannot detach | A real controlling PTY with merged stdout/stderr in `stream:"stdout"`, registered process ID and final exit code |
| `pty_resize` | `{process_id,rows?:number,cols?:number}` | `{}`, native TIOCSWINSZ; defaults 24x80, each dimension 1..4096 |

PTY input uses the same continuation frames. Input EOF writes terminal VEOF (Ctrl-D), which follows terminal line-discipline semantics; it is not a socket shutdown. Signals use proc_kill. PTY/process attachment is output observation; stdin remains on the original RPC.

### Persistent shell sessions

`session_create {cwd?,env?}` starts one real `/bin/sh`, returns `{session_id,pid}`. `session_list {}` returns `{sessions:[string]}`. `session_rm {session_id}` kills and reaps the shell; it waits for a current command, which has a five-minute bound. Sessions have a 64-entry cap and expire after 30 idle minutes (30-second sweep).

`exec {session_id,command,cwd?,env?}` serializes commands into that existing shell, preserving shell variables, functions and cwd. It streams stdout/stderr and returns `{exit_code,process_id:"session:<session_id>",session_id}`. It does not accept argv, detach, or stdin data. Completion travels over reserved shell fd 3, independently of command stdout/stderr. Commands that close/repurpose fd 3, replace/exit the shell, or corrupt its parser can terminate the session; disconnect/error/timeout kills the shell group and removes the session. Claim env is refreshed on each command; session_create env supplies persistent defaults, and per-exec env overrides apply for that command. Managed exports omitted by a later command are unset. Shell-assigned variables outside those managed keys persist. Output from background jobs may appear in later commands; use detached exec for independent tracked jobs. Continuous output after completion is capped by the five-minute command timeout.

### Files and directory trees

Paths must be absolute. Workload filesystem permissions apply, including symlink traversal permissions. `fs_stat` reports symlink metadata without following the final symlink. `fs_write`/replace overwrite the final directory entry by rename rather than following a final symlink. No operation grants root filesystem authority to a tenant. Completion after rename may still fail on directory fsync; a lost/error response is not evidence that nothing changed.

| Method | Request params/data | Events / terminal params |
|---|---|---|
| `fs_read` | `{path,offset?:number,length?:number}` | 64 KiB data chunks `{offset:decimal_string}`, final `{bytes:decimal_string}` |
| `fs_write` | `{path,mode?:number}`, input data stream, <=1 GiB total; default mode 0600 | Same-directory temp file, full EOF then fsync+rename+directory fsync; final `{bytes:decimal_string}`; interrupted uploads discard staging |
| `fs_stat` | `{path}` | `{path,size:decimal_string,mode,uid,gid,mtime_seconds,directory,symlink}` |
| `fs_list` | `{path}` | `{entries:[stat_result]}`; max 10,000 entries /4 MiB JSON-shaped metadata, otherwise explicit error/use find |
| `fs_mkdir` | `{path}` | Recursive mkdir, `{}` |
| `fs_rm` | `{path}` | Recursive directory removal or unlink for other entries, `{}` |
| `fs_rename` | `{path,destination:absolute_string}` | Native rename and both parent directory syncs, `{}`; cross-filesystem failure propagates |
| `fs_find` | `{path,name?:string}` | Incremental stat events whose basename contains literal name; does not follow directory symlinks, max 100,000 visited entries; final `{}` |
| `fs_replace` | `{path,find:nonempty_string,replace:string}` | Literal all-occurrence replacement, regular UTF-8 files/input/output <=8 MiB; atomic replacement preserves permission bits excluding special bits; final `{replacements:number}` |
| `fs_watch` | `{path}` | Installs native nonrecursive inotify watch before `{ready:true}`, then `{mask:number,cookie:number,name:string}` events. Queue overflow returns terminal error requiring rescan. Watch removal emits final `{removed:true}`. Close connection to cancel. |
| `tree_pull` | `{path}` directory | Uncompressed tar data chunks then `{}`; spools to disk, bounded at 1 GiB archive /100,000 entries |
| `tree_push` | `{path}` directory destination, tar input stream | Copies existing tree to same-filesystem staging, extracts archive there, syncs staged files/directories, then atomically swaps destination via renameat2(RENAME_EXCHANGE), or renames a new tree; terminal params independently report commit, publication durability and cleanup as below |

After rename, `tree_push` terminal params contain `committed:true`, `publication_durable:bool`, `cleanup_complete:bool`, and `cleanup_required:bool` (the inverse of `cleanup_complete`). Ordinary success has all of the first three fields true and `cleanup_required:false`. If publication-directory fsync succeeds but cleanup fails, the frame still succeeds with `publication_durable:true`, `cleanup_complete:false`, `cleanup_required:true`, `cleanup_name:string`, and `cleanup_reason:"old_tree_cleanup_failed"`. `cleanup_name` is only a basename within this destination's pinned parent; it identifies remaining old-tree data, which may already be partly removed.

If publication-directory fsync fails after rename, the terminal frame has a nonempty `error` **and** these result params: `committed:true`, `publication_durable:false`. The error remains a failure for existing clients; the params describe the mutation that already happened, not a successful durable publication. An exchanged old tree is retained without attempting cleanup, with `cleanup_complete:false`, `cleanup_required:true`, `cleanup_name`, and `cleanup_reason:"publication_sync_failed_cleanup_not_attempted"`. A newly created destination has no old tree, so its cleanup is complete even if publication sync fails. No automatic rollback/exchange or archive retry occurs. Before rename, failures retain the ordinary error frame and leave the destination unchanged. Pending cleanup requires caller-authorized inspection/removal of only the reported old-tree basename in the verified parent, with writers isolated; the agent does not escalate to root or run background garbage collection. `cleanup_complete` reports observed removal, not crash durability of that removal.

Tree push merges archive entries into a staged copy of the old tree; unmentioned paths are retained. Existing/new tree support regular files and directories, not symlinks, hardlinks or special entries. Absolute/parent-traversing archive names and privileged mode bits are rejected. Archive bytes and staged expanded bytes are each bounded at 1 GiB, entries at 100,000 (including existing-tree entries). Extraction uses the maintained tar crate's containment checks. Failure/disconnect before the commit leaves the destination unchanged; a post-commit fsync failure is reported with the new tree possibly visible. Concurrent external filesystem writers are not transactionally merged; callers must serialize modifications to a destination tree. Filesystems lacking atomic rename exchange explicitly fail an existing-tree update. File/tree spools consume guest disk, and disk-full errors propagate. No protocol implementation claims quota enforcement beyond these transfer limits.

### Raw port forwarding

`port_forward {port:number}` requires a **fresh exclusive workload connection**, valid loopback port 1..65535, and empty request data. The agent connects to `127.0.0.1:<port>`, replies with one Frame `{id,method:"port_forward",params:{ready:true},eof:false}`, then switches the whole connection to **raw bytes** in both directions. No additional protobuf frames, including final frames, occur after ready. Client and server TCP/vsock FINs independently half-close each direction; the reverse direction remains open until its own EOF. Setup failure is a normal terminal error Frame followed by connection close. Request `eof` has no raw-stream half-close meaning; use the transport's write shutdown after ready. Regular multiplexed connections reject a subsequent port_forward request. This works in none/relay/direct network modes and does not depend on NIC access.

## Verification and remaining integration work

`cargo test --manifest-path guest/agent/Cargo.toml` exercises protobuf bounds/golden bytes, UnixStream multiplexing/portal rejection, env replacement, output streaming, disconnection cleanup, bounded detached logs, real shell state, actual PTYs/resize, file rollback/watch and raw half-close forwarding. `cargo test --manifest-path guest/init/Cargo.toml` checks serial-role discovery and unprivileged PID-1 refusal. `GOWORK=off go -C protocol/gen test ./...` checks generated types/framing, short writers, invalid/truncated sizes and the same Rust/Go golden bytes.

Real AF_VSOCK peer-CID enforcement, privileged entropy/clock/hostname changes, complete initramfs mounting/switch_root, actual EROFS multi-device mapping, systemd boot and snapshot/resume require a Linux VM test environment. Unit tests do not fake these successes. See `guest/README.md` for boot/image/build requirements and concrete limitations.
