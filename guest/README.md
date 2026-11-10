# Guest image contract

The workspace crates are `guest/agent` (`sandbox-agent`) and `guest/init` (`sandbox-init`). Both are native Linux Rust binaries, with no shell/tool subprocesses used for initialization, entropy, clock, mounts, hostname, PTYs or proxy bridging. The agent generates its protobuf Rust types from `protocol/sandbox.proto` using prost and requires protoc at build time. Its only intentional child programs are tenant commands and persistent `/bin/sh` sessions.

## Build and installation

For x86_64 static images:

```
cargo build --release --target x86_64-unknown-linux-musl -p sandbox-agent -p sandbox-init
```

Use the matching aarch64 musl target for aarch64. Install the target's Rust stdlib and a suitable musl linker/toolchain first. The agent uses rusqlite with bundled SQLite and BLAKE3; their native build steps need a target-compatible C compiler and archiver (set Cargo's target linker and `CC_<target>`/`AR_<target>` when cross-compiling). SQLite is compiled into the agent; no guest SQLite shared library or CLI is required. There is no OpenSSL/libsystemd dependency. The normal host-target test build does not itself prove a static musl artifact was built. `make guest` rejects both dynamic interpreters and shared-library dependencies before installing either binary. Package prebuilt binaries from any environment provider using `make guest GUEST_ARTIFACT_DIR=/path/to/package/bin`; the checks are the same as for a local Cargo build. Packaging creates a reproducible root-owned `newc` initramfs containing only `/init`. See the [project README](../README.md#build-and-local-verification) for general build tools.

Install `sandbox-init` as executable `/init` in the initramfs. It must run as PID 1. The final OCI root must contain:

- `/usr/lib/systemd/systemd`, `/bin/sh`, libc/runtime files required by tenant programs.
- Executable `/usr/local/libexec/sandbox-agent`.
- `guest/systemd/sandbox-agent.service` installed to `/etc/systemd/system/`, enabled by `/etc/systemd/system/sysinit.target.wants/sandbox-agent.service`.
- A workload user/group with UID/GID 1000 and writable `/home/sandbox`, `/tmp`, and application working directories. Other IDs can be passed with agent `--uid/--gid`; zero is rejected.
- Real directories `/dev`, `/proc`, `/sys`, `/run`, `/etc`; a regular `/etc/resolv.conf` when DNS boot configuration is supplied. `/etc/machine-id` must be a replaceable regular directory entry, not a bind mount. Prefer `/var/lib/dbus/machine-id` symlinked to `/etc/machine-id`.

The unit starts before sysinit without network-online dependencies. It runs the agent as root for system cooperation, while commands drop identity/groups and cannot gain new privileges. Do not add private network namespaces to this service: forwarded application ports and localhost proxy listeners must share the guest's loopback namespace. Workload process groups remain in the service cgroup so VM/service shutdown kills them. The unit's process/memory/file-descriptor caps are deployment defaults; size them for the image's workload.

## Kernel and block roles

Build the early-boot drivers into the kernel, or otherwise make them available without external helpers: devtmpfs, procfs, sysfs, tmpfs, virtio transport/PCI, virtio block/net/vsock, EROFS (including the fsmeta device-table features used by the image builder), ext4, OverlayFS, kernel IP autoconfiguration, UNIX sockets, PTYs/devpts, inotify, pidfd, entropy and clock syscalls. The full image/systemd boot must provide devpts, including `/dev/ptmx`; PTY creation explicitly fails if unavailable. aarch64 CH boot must enable ACPI/VMGenID as required by the engine contract.

Stable virtio block serial roles:

| Role | Default serial | Access/filesystem |
|---|---|---|
| Combined lower | `sandbox-root` | Read-only EROFS fsmeta |
| Writable upper/work backing | `sandbox-write` | Read-write ext4 |
| Optional EROFS table backends | Explicit ordered serials | Read-only EROFS data devices |

Init polls `/sys/block/*/serial`, checks expected identities, duplicate/unexpected serials, and each role's `/sys/block/*/ro` flag. It never infers roles from vdX order. Missing devices time out; an identity mismatch fails immediately. Loop/RAM devices without serial attributes are ignored. Serial-bearing devices with empty identities are rejected. All serials must be unique, ASCII alphanumeric/hyphen/underscore, <=20 bytes. At most 16 EROFS data backends are accepted.

Kernel parameters:

```
sandbox.lower_serial=sandbox-root
sandbox.upper_serial=sandbox-write
sandbox.data_serials=sandbox-data-0000,sandbox-data-0001
sandbox.device_timeout_ms=10000
sandbox.dns=1.1.1.1,8.8.8.8
```

Data serials are optional and map to repeated EROFS `device=/dev/...` mount options **in exactly that order**. The engine/image builder must ensure the fsmeta device-table order and mapped data bytes agree; init does not construct a linear mapping or rewrite EROFS metadata. With a single flattened lower, omit data_serials. The kernel's `ip=` parameter supplied by engine configures the fixed guest IP/prefix/default route; init does not use DHCP or alter captured guest network state during restore. DNS accepts IPv4 literals only. NIC MTU/queue compatibility remains an engine/image contract.

## Root handoff

Init mounts devtmpfs, proc, sysfs and tmpfs `/run`, discovers block identities, mounts lower at `/run/sandbox-boot/lower` and ext4 at `/run/sandbox-boot/writable`, and creates upper/work on that same ext4. Overlay mounts at `/newroot`. It enables loopback through native socket ioctls, writes the configured resolver file, then moves `/dev`, `/proc`, `/sys` and `/run` into the new root, carrying the nested lower/writable mounts with `/run`.

Before handoff it verifies PID 1 and that the old root is initramfs tmpfs/ramfs, checks the new root is a separate filesystem, removes old initramfs contents without crossing mount-device boundaries, moves the new root onto `/`, chroots into that mount, and execs `/usr/lib/systemd/systemd --system` as PID 1. This releases the old initramfs contents and preserves OverlayFS backing mounts, rather than leaving a chroot-only boot. Any failed syscall or missing prerequisite prints a serial diagnostic and exits PID 1, causing a kernel failure; there is no success fallback.

Memory restore resumes the captured kernel, mounts, systemd and agent directly; it does not execute init again. Engine reconnects on System 2047 and performs ping/reseed/clock_sync. sandboxd separately activates via Admin 2046 before exposing Workload 2048. See `protocol/README.md` for exact framing, methods and tenant/admin boundary.

## Error diagnostics

Init errors retain operation/target context and compile-time caller locations
(`file:line:column`) through `InitError`. Mount failures identify source, target,
filesystem, flags and data; discovery identifies serials and sysfs paths; root
handoff and loopback failures identify the failed action. `sandbox-init` no
longer depends on anyhow.

The agent library uses `Error` with a flat `ErrorCode`, typed diagnostic frames
and additional result params. `#[track_caller]` captures static locations without
collecting a backtrace. `ResultContext::or_context` preserves classification;
`map_code` explicitly assigns a boundary's category. Context closures format only
on error. Argument validation defaults to `invalid`; runtime and admission checks
name their category, and standard I/O failures retain their actionable kind or
fall back to `internal`. SQLite failures remain internal until keyed execution's
post-admission fence conservatively promotes an unconfirmed result to
`result_unknown`. No new crate or SQLite/protobuf schema is required.

All RPC failures pass through the same failure-frame encoder. The exact codes,
optional result state and postcommit fields are defined in the
[structured-failure protocol](../protocol/README.md#structured-failures).
Messages carry method and permitted target identifiers, not command text,
environment values or raw exec keys. Locations and wording may change with source
edits; callers branch on codes and inspect result params, not message text.

## Durable keyed exec

Workload `exec` accepts an optional explicit `params.exec_key`. `Frame.id` remains a connection-local multiplexing identifier and never enables deduplication. Unkeyed exec retains its existing streaming, disconnect cleanup, detach and session behavior and has no durable deduplication guarantee.

Keyed requests support foreground non-session, non-PTY execution only. Supply all stdin in the initial frame with `eof=true`, at most 64 KiB; keyed streaming stdin, `detach=true`, session exec and PTYs are rejected before spawning. Keys contain 1–256 visible ASCII bytes. The input fingerprint covers all request parameters except the key, the method, complete stdin, effective environment (including activation defaults and request overrides), effective cwd, workload UID/GID and sandbox identity. Changing any input under the same key conflicts. Changing frame IDs or reconnecting does not change request identity.

The agent exclusively owns `/var/lib/sandbox-agent/exec.sqlite3` on the writable guest root. Its directory is root-owned `0700`, database and lock file are `0600`, and the filesystem RPCs reject or omit this private state, including symlink aliases. The agent fails startup on unsafe ownership/permissions or another live ledger owner. Tests and the UNIX fixture inject a private temporary directory and their current unprivileged UID/GID via `Config`; production runs as root and drops privileges for workload execution. Do not place this directory on `/run` or another ephemeral filesystem, or permit workloads to write its parent directory.

SQLite uses a synchronous FULL transaction in DELETE journal mode to persist the intent before spawn. Concurrent connections serialize admission. The replies distinguish:

- `result_state=completed`: a durable exit code plus output counts and truncation flags. Repeating the same key and inputs returns the saved result with `replayed=true`.
- `result_state=in_progress`, `error_code=exec_in_progress`: an admitted request is still running; no second process starts. Retain the original key when checking again.
- `result_state=result_unknown`, `error_code=result_unknown`: a previous intent has no confirmed durable result. Startup converts all unfinished intents to this terminal state. The key never executes again, including after further restarts.
- `error_code=exec_key_conflict`: the key already belongs to different inputs.
- `error_code=exec_ledger_capacity_exhausted`: the identity's admission bound has been reached; the request was not admitted.

Both the first keyed response and saved replay retain only the first 64 KiB of stdout and first 64 KiB of stderr; the process is drained beyond these limits and byte counts/truncation flags remain explicit. Ordinary process logs retain their existing separate 1 MiB memory ring. A result becomes completed only after the exit code and bounded output are committed. A spawn, I/O, cancellation, task or persistence failure without a confirmed result becomes unknown; this is deliberately conservative even when execution may not have started. Keyed foreground disconnects still cancel the workload process group.

Admission is bounded to 1,024 keys per sandbox identity (`Config.ledger_max_entries` can lower this for fixtures). There is no TTL or automatic key eviction: completed and unknown keys consume a slot for that identity's lifetime, so an old key cannot silently replay. The SQLite main file is capped at 40,960 pages (160 MiB with its default 4 KiB pages); rollback journals and identity-reset vacuum need additional temporary disk space. Audit columns contain salted BLAKE3 key/input fingerprints, state, timestamps, exit status and output counts. Raw keys, commands, argv and environment are not persisted as audit metadata; bounded output may itself contain application secrets and stays private.

The durable sandbox identity is restored when the agent restarts. Same-identity activation/resume preserves the ledger. Host-authorized activation with a different sandbox identity clears the previous scope only when no keyed execution is in progress. `prepare_template` requires the existing RPC/session drain, clears the ledger and process history, and returns to a private pre-activation scope; a new sandbox cannot replay the template's results. Identity clearing uses secure deletion and vacuum. Pre-activation template warmup is supported within that VM's private scope. The production host supplies `sbx-op:<operation_id>:step:template-warmup`; it never generates a replacement key after an unknown result. The host's conservative `warmup_inflight` recovery gate still stops an uncertain build rather than dispatching it again.

The ledger is guest cooperation, not an external authority: restoring an older whole-VM snapshot also restores its older ledger. Preventing replay across arbitrary rollback to a point before admission requires a host-owned durable ledger outside snapshot history and host operation/instance fencing. A guest disk loss, hostile root or externally replaced database cannot be covered by the guest ledger.

## Directory-tree transfers

`tree_pull` spools a complete, validated tar before streaming bounded frames. The spool is created under the workload filesystem identity and immediately unlinked; archive generation uses a clone of that descriptor, then streaming rewinds and reads the retained descriptor after the writer finishes. No spool pathname is reopened. `tree_push` receives the complete stream, copies the current destination into a private staging directory on the destination filesystem, merges the archive there, syncs it, and atomically publishes with `renameat2` (`RENAME_EXCHANGE` for an existing tree, `RENAME_NOREPLACE` for a new tree). A disconnect before protocol EOF, malformed archive, metadata failure or cancellation before commit leaves the destination unchanged. Existing names absent from the archive remain. Replacing a regular file or symlink unlinks that staged name first, so changing one hardlink name does not truncate the other names. Replacing a directory with a different entry type, or writing through a symlink parent, is rejected. Writers must remain isolated for the duration of pull/push; these operations do not take a filesystem snapshot or coordinate concurrent workload writers.

Before copying an existing destination, push checks its directories through pinned descriptors with Linux `faccessat2(AT_EMPTY_PATH | AT_EACCESS)`, using the current filesystem credentials and honoring directory ACL access. An unavailable access check fails before publication. Any directory that needs chmod to make cleanup possible must belong to the workload filesystem UID; a readable but non-writable foreign-owned directory is rejected before publication. Already writable/searchable/readable foreign-owned directories are accepted, and foreign ownership of a readable regular file alone does not block unlinking it from a writable parent. This admission does not reserve permissions against concurrent changes; caller writer isolation remains required.

A terminal push result independently reports publication and cleanup: normal success carries `committed:true`, `publication_durable:true`, `cleanup_complete:true`, `cleanup_required:false`. If publication-directory fsync succeeded but old-tree cleanup failed, success instead carries `cleanup_complete:false`, `cleanup_required:true`, a `cleanup_name` basename in the destination's pinned parent, and the fixed reason `old_tree_cleanup_failed`. Cleanup may have made partial progress; the remaining name is evidence to inspect, not an intact backup guarantee. If publication-directory fsync fails after rename, the terminal frame retains a nonempty `error` alongside `committed:true`, `publication_durable:false`. For an exchanged tree, cleanup is not attempted: the old tree remains under `cleanup_name`, with `cleanup_complete:false`, `cleanup_required:true`, and `cleanup_reason:publication_sync_failed_cleanup_not_attempted`. For a new destination there is no old tree, so cleanup is complete even when publication sync fails. These postcommit outcomes never exchange back or automatically resend the archive. Before commit, errors retain the ordinary error response and leave the destination unchanged.

Cleanup runs under the workload filesystem identity and explicitly checks permission restoration and removal results. There is no root cleanup service or automatic permission escalation. A caller handling pending cleanup must isolate writers, verify the destination parent and reported basename, and arrange authorized manual inspection/removal of the remaining old data without targeting the newly published tree. Cleanup completion describes removal observed by this call; it does not promise that the removal itself survives a crash.

Transfers preserve regular-file contents, directory entries, relative symlink payloads (including safe dangling links), hardlink identity within the transferred tree, file/directory permission bits (`07777`, including workload-owned setuid/setgid and sticky bits), and nanosecond mtimes for files, directories and symlinks. The optional tar root directory entry (`.`) carries root metadata; directory metadata is applied after its children. Symlink permissions remain Linux `0777`. Pull records the source UID/GID; push deliberately creates both copied and imported entries as the configured workload UID/GID and ignores archive ownership requests. It does not perform arbitrary root chown, preserve another owner's identity, or reconnect hardlinks to inodes outside the transferred tree. Ordinary permission checks, metadata syscalls, staging and commit run under the workload filesystem identity.

Regular files and directories preserve supported `user.*` xattrs with UTF-8 names up to 255 bytes and binary values up to 64 KiB, subject to the destination filesystem's smaller limits. Pull emits interoperable PAX `LIBARCHIVE.xattr` records with percent-encoded names and unpadded base64 values, including NUL/LF bytes. Push accepts these records (padded base64 is also accepted) and `SCHILY.xattr` records that the tar crate can parse. The two formats name attributes differently: a `SCHILY.xattr` suffix is the literal UTF-8 attribute name and is never percent-decoded, while a `LIBARCHIVE.xattr` suffix is percent-decoded after escape validation; SCHILY values are the raw bytes and LIBARCHIVE values are base64. Raw LF-containing SCHILY values are unsupported and fail parsing. Identical SCHILY/LIBARCHIVE representations of the same attribute are accepted, conflicting values and duplicate PAX keys are rejected. Directory merges keep existing user attributes unless replaced by an incoming value. Symlink xattrs, non-UTF-8 attribute names, ACLs, capabilities, privileged xattr namespaces, access/birth/change times and sparse-file layout are not preserved; explicit unsupported xattr/ACL imports fail rather than claiming success. A filesystem that cannot set an imported user attribute causes rollback.

Extraction uses pinned directory descriptors and `openat`/`O_NOFOLLOW`; tar paths cannot be absolute, contain `..` or empty components, or exceed 64 components/4095 bytes. A hardlink target must name a regular file inside staging; forward references are supported, while missing targets and cycles fail. Symlinks are stored without following them for extraction. Absolute or escaping targets are rejected, and Linux `openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)` checks the final link graph, rejecting escapes and loops while allowing dangling relative targets. Linux with `openat2` and mounted `/proc` is required; unavailable confinement fails explicitly. Pull omits the private exec ledger and symlink aliases to it; destructive pushes cannot replace its ancestors.

Each transfer/archive is capped at 1 GiB, with a combined 1 GiB expanded-data budget for the copied destination plus incoming entries, 100,000 entries (including copied entries and created implicit parents), 16 MiB of aggregate metadata, 128 KiB per PAX/GNU extension and 400,000 raw tar headers. Hardlink reference chains are capped at 64. Special files, sparse tar entries and global PAX headers are rejected. Memory stays bounded; push may additionally spool up to 1 GiB for the old destination before extraction, and disk budgeting must cover received/archive spools, staging and the old tree until exchange cleanup. Network output retains the existing bounded-frame/backpressure behavior. Cancellation/errors before commit attempt explicit staging removal, including directories whose final mode removes owner access; postcommit cleanup failures are reported as described above. Abrupt process/VM failure can leave temporary disk artifacts, and a disconnect racing an already completed atomic commit cannot undo it.

## Filesystem RPC constraints

`fs_read` and `fs_replace` operate on regular files only. Both open the target with `O_NONBLOCK` (`fs_replace` also keeps its `O_NOFOLLOW`) and verify the type on the acquired descriptor itself, so a writer-less FIFO is rejected promptly instead of stalling a workload RPC slot inside an uninterruptible `open`. Devices, sockets and directories are likewise rejected, on the same acquired descriptor, once their open returns. `fs_read` still follows symlinks that resolve to a regular file; `fs_replace` rejects symlink paths. Failed requests never rewrite the target path. This guarantee covers nonblocking FIFO admission plus same-FD rejection of other acquired non-regular descriptors only: a device driver's `open` may ignore `O_NONBLOCK`, prompt rejection does not make arbitrary filesystem I/O hard-cancellable, and a stuck storage device can still hold a slot until the kernel returns.

## Validation limits

`make guest-fs-isolation-e2e` builds the opt-in `filesystem-fixtures` integration tests before entering fresh user/mount/PID namespaces. It requires subordinate UID/GID mappings, `newuidmap`/`newgidmap`, mounted `/proc` and namespace mount permissions, and fails if any required scenario is missing or skipped. A namespace-root daemon serves actual Workload RPCs with filesystem UID/GID 1; a separately executed workload process proves nonzero real/effective/saved IDs, empty supplementary groups, zero capabilities and denial of a harmless root-private fixture. The tests check retained archive descriptors despite name replacement, offset rewind, empty/large trees, private-ledger exclusion, disconnects and cleanup; they also exercise mixed-owner push admission, successful cleanup, and distinct postcommit cleanup/fsync failures over real RPC. The runner enumerates this build's test binary, requires all seven named scenarios as a minimum, and checks every listed scenario actually passed exactly once. Ordinary Cargo runs do not execute the namespace-only scenarios. The feature defaults off; its private scratch registration and one-operation scheduling gate are absent from normal builds and have no RPC or environment switch.

Unit and Unix-fixture tests exercise framing, dispatch, subprocesses, actual shell/PTY state, filesystem transactions/inotify, TCP half-close and sysfs fixtures. The separate `make kvm-e2e` test has booted the static musl guest with the exact pinned CH and matching kernel through real mount migration, switch_root/systemd, EROFS external-device mappings and AF_VSOCK. Both materializations pass file-view and binary tree-transfer checks, followed by COW clone/resnapshot recovery with real reseed/time RPCs. A root precheck before agent startup confirms raw reads succeed and writes to every readonly root/data block device are rejected. These checks use the standalone VMM and complement production launcher/adoption and complete sandbox lifecycle acceptance on a provisioned host. No privileged operation reports success after an authority failure.

Additional current limits: unkeyed execution and session/PTY execution have no durable deduplication guarantee; ordinary process logs are bounded memory only; a job that creates a new session/process group can escape per-RPC process-group cleanup until systemd/VM shutdown; session commands must preserve reserved completion fd 3 and may disrupt their own shell; tree push normalizes ownership to the workload UID/GID and supports only the metadata and archive subset described above; filesystem writer isolation during atomic tree replacement is a caller responsibility; watch is nonrecursive; the certificate bridge depends on application proxy/trust-env support; machine-id changes do not refresh application caches or user-space RNGs. The host must treat guest replies as cooperation rather than proof of guest trust.
