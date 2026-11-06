# Engine integration

The Go module is `github.com/woxQAQ/sandbox-engine`. `api/types.go` is the shared contract, including durable per-instance network policy. The runtime always constructs the real Linux Cloud Hypervisor backend. The injected backend in `*_test.go` is confined to tests.

## Build and public client

From the repository root:

```sh
GOWORK=off go -C engine build -o /tmp/engined ./cmd/engined
GOWORK=off go -C engine test ./...
GOWORK=off go -C engine test -race ./...
GOWORK=off go -C engine vet ./...
```

`engine/go.mod` references `../protocol/gen`, the generated shared protocol module, and the pinned `third_party/go-erofs` source with its device-table correction. Keep both source locations when building outside the workspace. A consumer that imports the engine runtime must carry the same go-erofs replacement, as `sandboxd/go.mod` does: Go does not inherit dependency modules' replace directives. Runtime dependencies include Gin for HTTP routing, Viper/pflag for configuration, `modernc.org/sqlite`, `golang.org/x/sys`, the OCI image-spec/digest packages, go-erofs and the shared generated protobuf code. See [dependency provenance](third_party/go-erofs/UPSTREAM.md).

The public UDS client satisfies `api.Client`:

```go
c := client.New("/run/sandbox-engine/engine.sock")
defer c.Close()
op, err := c.Submit(ctx, api.Request{
    Key: "tenant-request-123", Action: "create",
    Config: &api.Config{
        Image: "sha256:<64 hexadecimal characters>",
        Shape: api.Shape{VCPUs: 1, MemoryMiB: 512, DiskMiB: 1024},
    },
})
// Persist op.ID/op.VMID. Poll GetOperation, or use api.Wait with the same request.
```

`Submit` returns durable acceptance, not lifecycle completion. Client cancellation after acceptance does not cancel the daemon's operation. Keys contain 1–256 bytes. An identical request with the same key returns the original operation, including its fixed terminal result; different parameters under that key return `conflict`. If a response is lost, use `FindOperation(ctx, key)` before issuing a new key. Operation records remain after VM/snapshot deletion. IDs for new VMs and snapshots are allocated by the engine in the admission transaction.

VM phases are `creating`, `created`, `running`, and `stopped`. Read `Verified`, `BlockedReason`, `OperationID`, `Instance`, `AgentReady`, `NetworkEnabled`, `NetworkActual`, and `Deleting` as well as `Phase`. An identity/API inspection failure leaves the last confirmed phase and marks the VM unverified. An instance token changes on cold start or restore; use `ExpectedInstance` for actions whose target must be that particular running instance.

## Actions

| Action | Request fields | Completion behavior |
| --- | --- | --- |
| `create` | `Config` | Validates published image and host prerequisites, reserves resources, prepares an independent ext4 upper disk and blocked network. Leaves phase `created`. |
| `start` | `VMID`, optional `ExpectedInstance` | Records launch intent, launches/adopts one VMM, boots, completes system-agent ping, then records running. A failed cold agent handshake attempts to stop the VMM. |
| `stop` | `VMID`, optional `ExpectedInstance` | Verifies ownership, blocks network, terminates through pidfd, confirms exit before recording stopped. |
| `snapshot` | `VMID`, optional `ExpectedInstance` | Requires verified running/agent-ready source; returns `SnapshotID` once the durable operation is accepted. Publication is a later step. |
| `restore` | `VMID`, `SnapshotID`, optional `ExpectedInstance` | Requires a stopped target with matching image and shape. Creates fresh target resources and a fresh instance; network defaults off. |
| `clone` | `SnapshotID`, optional matching `Config` | Allocates a new VM and restores from a published snapshot with default-off network. |
| `delete` | `VMID`, optional `ExpectedInstance` | Persists a tombstone, stops, removes owned resources, then removes the VM record and its reservations/backing records. |
| `delete_snapshot` | `SnapshotID` | Tombstones before deletion. Existing reader leases cause a failed `conflict`; retry with a new operation key after readers finish. New readers cannot enter a tombstoned snapshot. |
| `network_policy` | `VMID`, required `ExpectedInstance`, `Policy` | Requires actual and desired network off; atomically installs and verifies a versioned IPv4 CIDR/connection policy. A nil policy revokes authorization and installs deny-all. |
| `network` | `VMID`, `NetworkEnabled`, required `ExpectedInstance` | Saves desired state before effects. Enabling requires verified running/agent-ready state, a verified policy for this exact instance, and host admission prerequisites. |

Sandbox `none`/`relay` modes keep engine network disabled. Direct mode uses `network(false)` → `network_policy` → guest activation → `network(true)`. Policy replacement and activation share the per-VM lifecycle lock. Policies are never inherited by a new cold/restore/clone instance. Nonempty hostname policies are rejected; reserved/private/metadata destinations remain denied. Receipt and live nft rules must agree before enabling or adopting enabled interfaces. See [network policy](api/NETWORK_POLICY.md) for the exact budget and destination semantics.

The transport exposes these JSON routes:

```text
POST /v1/operations
GET  /v1/operations?key=...
GET  /v1/operations/{id}
GET  /v1/vms
GET  /v1/vms/{id}
GET  /v1/snapshots
GET  /v1/snapshots/{id}
```

Lists return all current records, including unpublished/deleting snapshots. Only `Published && !Deleting` snapshots can acquire new restore leases. HTTP errors preserve `api.Error` codes. Request bodies are limited to 1 MiB and reject unknown fields; client response decoding is bounded at 64 MiB. There is no pagination, so exceptionally large inventories need a later transport extension. The UDS is mode 0600; access to it is privileged control over all engine VMs.

## Command line

`engine/cmd/engined` uses Viper with pflag. Explicit flags override `ENGINE_*` environment variables, then a file selected with `--config` or `ENGINE_CONFIG`, then defaults. File keys match flag names; environment names replace hyphens with underscores (`ENGINE_GC_INTERVAL=30s`, for example). YAML, JSON and TOML are supported, with typed decoding and rejection of unknown keys. Files are loaded once, with no implicit discovery or runtime reload. See [engined.example.yaml](../deploy/host/engined.example.yaml).

| Flag | Default / meaning |
| --- | --- |
| `--config` | Optional explicit configuration file |
| `--state-dir` | `/var/lib/sandbox-engine`, private durable directory, mode 0700 |
| `--socket` | `/run/sandbox-engine/engine.sock` |
| `--init` | Explicit creation of a fresh empty store; never infers recovery from missing metadata |
| `--mode` | `serve`, or `import`, `gc`, `backup`, `restore-backup`, `recover`, `doctor` |
| `--gc-dry-run` | Report eligible resources without unlinking (`gc` mode) |
| `--gc-batch` | Maximum artifacts per administrative GC pass, default 16, range 1–1024 |
| `--gc-interval` | Background collection interval, default 1 minute |
| `--backup` | Backup DB path; matching receipt is `<path>.json` |
| `--recovery-note` | Required operator review note for `recover` |
| `--oci-layout`, `--digest` | Local OCI layout and exact manifest digest for `import` |
| `--materialization` | `layered-erofs-v1` by default; explicit `merged-oci-erofs-v1` uses isolated umoci materialization |
| `--cloud-hypervisor` | `/usr/local/libexec/sandbox/cloud-hypervisor` |
| `--build-receipt` | Defaults to `<cloud-hypervisor>.json` |
| `--cgroup-root` | `/sys/fs/cgroup/sandbox-engine` (the sample service overrides this to `sandbox-vms`) |
| `--host-policy` | `/etc/sandbox-engine/host-policy.json` |
| `--io-max` | Required real-backend single-line cgroup v2 device budget |
| `--vmm-uid`, `--vmm-gid` | 65534/65534; both must be nonzero and have device access |
| `--max-memory-mib` | Default reservation ceiling: 80% of physical RAM |
| `--max-vcpus` | Default reservation ceiling: host logical CPU count |
| `--storage-margin-bytes` | Default 256 MiB |

Shape bounds are 1–255 vCPUs, 128–2^24 MiB RAM, and 64–2^30 MiB disk; admission also applies actual host budgets. Per-VM memory reservations include 128 MiB VMM overhead. Reservations are durable and conservative, and admission can reject a workload even when current physical allocation is smaller. Snapshot reservations precede pause. Clone/restore reserve private disk plus COW backing before copying.

All modes that open an Engine acquire the same exclusive flock. CLI `import` and `backup` are therefore offline administrative commands: stop the daemon first. Existing VMMs survive daemon shutdown. Opening the store for those modes still reconciles resources and can resume unfinished operations; do not use them as a way to bypass resource review. `doctor` is read-only and does not create missing metadata.

## Snapshot and restore persistence

SQLite uses WAL, synchronous FULL, and one writer connection. Operation and object intents, tombstones, leases, and reservations are committed before external effects. Host side effects remain non-transactional; uncertain identities require explicit diagnosis instead of destructive guesses.

Capture pauses the source, invokes CH snapshot, then reflinks or sparse-copies the writable disk into the operation's capture directory while the source remains paused. There are no hardlinks for writable disks. It records capture completion, resumes the source, then hashes, seals, fsyncs, renames, fsyncs parent directories, and finally publishes snapshot metadata. Source changes after resume cannot alter the snapshot disk. A crash before confirmed capture never publishes the partial capture and attempts to resume the source; a crash after capture continues publication after resuming as needed.

Restore requires matching engine format, CH build digest/revision/features, architecture, CPU identity (including microcode and flags), image materialization, and shape. Compatibility includes `layout-v2`, whose saved disk IDs/order/paths/serials/read-only roles must match the verified image device table. It requests CH `CopyOnWrite` with prefault disabled and resume disabled. Admission validates static private RAM and complete, aligned, nonoverlapping saved ranges against each pinned memory-manager region; shared/file-backed/hugepage/mergeable/hotplug/virtio-mem layouts are rejected. Before resuming, it reads the verified VMM's `/proc/PID/maps` and requires complete writable-private mappings of the retained backing's device/inode and exact file offsets. Upstream eager-copy fallback therefore prevents readiness. The mapping proof is repeated when adopting restored instances. Filesystems whose stat and proc mapping device identities differ are rejected; the development Btrfs subvolume exhibited this limitation. Standalone real CH COW/clone/resnapshot acceptance has passed on tmpfs with matching identities. Production launcher, daemon adoption and full lifecycle outcomes are tracked in [the acceptance record](../deploy/ACCEPTANCE.md).

Config/state are target-private copies, disk is a reflink or sparse copy, and immutable `memory-ranges` is a per-VM hardlink on the same filesystem. The durable backing registration, lease removal, and operation journal transfer happen in one transaction after filesystem persistence. This lets the source snapshot be deleted without breaking a clone's recovery or running backing inode. Images have no delete API and remain retained.

## Reference-aware garbage collection

The engine runs a GC pass every minute, with a two-second context budget and at most 16 artifact deletions per pass. `CollectGarbage(ctx, GCOptions)` exposes the same collector to embeddings; `LastGCReport()` returns its latest result, including concrete deferral reasons. For an explicit administrative report, stop the daemon and run `engined --state-dir=... --mode=gc --gc-dry-run`; omit `--gc-dry-run` to collect. Dry-run does not unlink artifacts or retire their backing/reservation metadata, including when a historical artifact is already missing. As with other Engine-opening administrative modes, opening the store reconciles resources and resumes pending operations. A batch limit or context expiry leaves further work for a later pass. Scanning and checksums consume the context budget, so large inventories can require a longer explicit administrative context.

GC holds the same import guard for cache lookup/build/publication and collection, takes VM/snapshot lifecycle locks without waiting for active work, then freezes admission and rechecks the inventory. Roots include current VM resources, all snapshot records including tombstones, catalog image manifests and their fsmeta/layer dependencies, unpublished image destinations, pending/unresolved operation old/candidate resources, snapshot leases, and current backing registrations. Historical journals prove version ownership without permanently rooting retired versions. A retired writable/backing version and its restore reservation are collected only when unreachable and the owning VM plus every recorded instance has been inspected and confirmed dead. Current stopped VM resources remain roots. Deleting a source snapshot never authorizes deletion of a clone's retained backing link. Open descriptors or mappings in the engine process, unknown instance identity, mounts, and unexpected artifact names prevent collection.

Completed import work is eligible only with matching catalog publication, durable import intent and reservation ownership; blob identities and exact expected contents are checked. Shared layer cache entries require a verified immutable receipt and artifact checksum and must be absent from every rooted image/fsmeta dependency. Interrupted/unknown import stages and cache stages without ownership receipts are retained, regardless of age. Images have no delete API and remain roots, so collection currently reclaims orphan cache entries rather than evicting published images.

Before the first unlink, SQLite `gc_deletions` records the exact file/directory inode identities and a durable HEAD checkpoint. Files and parent directory entries are synced before backing/reservation retirement and deletion-intent removal. An interrupted pass rebuilds reachability and checks the recorded inode identities before continuing; replacement files and unknown additions are preserved. GC never recursively removes arbitrary directories and never prunes operation/idempotency records. Missing/corrupt roots, unsupported metadata, inconsistent leases/backing, or recovery evidence fence collection and are reported. A reviewed database rollback remains fenced: the current recovery acknowledgement does not establish a complete ownership inventory or authorize GC of history it may have lost. The authority boundary is engine-owned resources and recorded runtime instances; unrelated privileged external readers are outside that boundary and are not enumerated through a global `/proc` scan.

A failed create/clone keeps its current ownership records and reservations until explicit deletion. Failed/unpublished snapshots remain visible and can be explicitly deleted once capture recovery is resolved; completed failed capture work can be collected after its source's readers have exited. Unknown temporary files, including anonymous atomic-write stages without durable ownership, remain diagnostic evidence.

## OCI materialization boundary

`ImportOCI` accepts a local OCI layout and an exact sha256 manifest digest for native Linux architecture. Registry fetching/authentication and OCI indexes/platform selection are outside this baseline. It validates manifest/config checksums, compressed blob checksums, uncompressed diff IDs, expanded-byte/file-count bounds, tar paths, hardlink targets, entry types, and empty regular whiteouts. Only uncompressed and gzip layers are accepted; zstd and special device/FIFO entries are rejected.

Default imports use native erofs-utils >=1.9.4 and <2 inside bubblewrap to create a shared immutable EROFS payload cache per `diff_id`. Each cache retains all regular entry versions and a bounded, authenticated header index encoded with standard sorted gob records. The pinned go-erofs library composes `fsmeta.erofs` through its public metadata-only API, referencing those payload extents through ordered external devices. The dependency owns EROFS serialization; a documented one-line device-table offset correction is part of the immutable builder identity. Existing native-rebuild catalogs retain their original profile verification. Explicit `ImportOptions.Materialization = MergedOCIEROFSLayout` (CLI `--materialization=merged-oci-erofs-v1`) selects umoci 0.6.0 native extraction followed by native mkfs from the merged root. This uses a mapped UID 0 user namespace, a bubblewrap-isolated tmpfs root, native ownership/capabilities/ACLs and readonly tool/input bindings; it never uses synthetic rootless ownership xattrs. Unprivileged callers need configured subordinate UID/GID mappings and working mapping helpers; unmapped IDs and unsupported metadata fail. Native fsck checks/extracts either final view before boot materials are copied. See [image materialization](IMAGE-MATERIALIZATION.md) for tool/mapping prerequisites, supported semantics, publication and runtime bindings. The imported root must contain:

```text
boot/vmlinux
boot/initramfs
usr/lib/systemd/systemd
usr/local/libexec/sandbox-agent
etc/systemd/system/sysinit.target.wants/sandbox-agent.service
```

Boot files must be regular nonsymlink files. The merged-root checks use `os.OpenRoot`; extraction and boot-file copying are confined. Filesystem output is synced before SQLite publication. Files left on disk by interrupted import are unavailable until explicitly verified/published by import; create/start/restore require the catalog record and matching image metadata/checksums.

Layered publications use `images/<manifest-hex>/{kernel,initramfs,fsmeta.erofs,image.json}`, with shared data files under `layer-cache/<builder-hex>/<diff_id-hex>/`. Their manifests use `format=2`, `layout=layered-erofs-v1`, exact builder/tool identity and ordered layer devices/receipts. Compatibility publications use `{kernel,initramfs,root.erofs,image.json}`, `format=3`, `layout=merged-oci-erofs-v1`, ordered original layer/diff IDs and a distinct builder pinned to exact umoci/unshare/bubblewrap/EROFS versions and policy. They expose one `/images/root.erofs` readonly disk with serial `sandbox-root`, no data serials and no shared layer cache. Snapshot manifests embed the selected image manifest. Explicit legacy `format=1`, `layout=merged-erofs-v1` catalogs remain readable. Each digest's first published layout is immutable; a different requested materialization returns `conflict`. No error triggers automatic fallback.

The shared builder has actual-tool regressions for root ownership/mode/xattrs/nanosecond mtime, inherited and implicit directories, same-layer and cross-layer regular/symlink hardlinks, true forward chains, duplicate inode replacement, root/nonroot opaque markers and whiteout/replacement combinations. Native capability and named-user ACL bytes are preserved. Cache-only rebuild works without original layer blobs; independent imports produce deterministic artifacts. Existing restrictive native-rebuild catalogs remain readable under their frozen identity. Explicit umoci compatibility retains its own tested semantics and fixed umask 077; it refuses true forward links to nonexistent targets. Both builders reject directory/cyclic/unresolved links, unsupported PAX/xattrs, special files, zstd, invalid metadata and Linux symlink modes other than 0777.

The default shared path now provides the architecture's per-layer data reuse with broader OCI namespace semantics. Explicit merged compatibility remains separately selectable, duplicates file data per image and requires original layer blobs. Inputs retain the 1..16 layer bound; layered images have at most 16 data backends plus fsmeta and upper, compatibility has one root backend plus upper. Fresh shared-profile KVM acceptance has verified serial-based EROFS mounts, root/file metadata, ACL/capability values, cross-layer and forward hardlink inode identity, implicit parents and duplicate replacement in a booted guest and after COW resnapshot/grandchild recovery. Earlier merged acceptance likewise proved cross-layer links and COW recovery. Boot-time guest-root raw reads succeed and writes to every readonly root/data block backend are rejected. Production launcher and adoption evidence is tracked separately in the acceptance record. Reference-aware GC reclaims proven orphan caches, completed owned work and retired resource versions; unknown/interrupted stages remain retained. FHS and pure Nix tool closures are supported by readonly tool-root bindings. Published-image worst-case reservations remain conservative. Defaults remain 8 GiB uncompressed tar bytes and one million entries (Go `ImportOptions` can override within bounds); aggregate compressed descriptors are bounded to twice the byte budget and metadata blobs to 16 MiB each. Compatibility tmpfs extraction is bounded to `MaxBytes + MaxFiles*4096` and consumes memory/swap under host limits.

## Guest contract

Each VM has a virtio-net device/TAP even when links are blocked. Stable VMM paths include `/boot/kernel`, `/boot/initramfs`, `/disks/upper.raw`, `/run/vsock`, and `/restore/*`. Layered images bind `/sandbox/ro/fsmeta.erofs` and ordered `/sandbox/ro/data-NNNN.erofs` read-only; merged layouts bind `/images/root.erofs` read-only. Root and upper serials are `sandbox-root` and `sandbox-write`. Layer data serials are `sandbox-data-0000` through `sandbox-data-0015`; `sandbox.data_serials` preserves device-table order independently of kernel device enumeration. The guest discovers roles by serial and mounts EROFS with those external data devices before ext4/OverlayFS. Other cmdline keys are `sandbox.lower_serial`, `sandbox.upper_serial`, and `sandbox.dns`. Network identity is fixed at guest `192.168.127.2/30`, gateway `192.168.127.1`, guest MAC `02:00:00:00:00:02`, MTU 1500. Host mapping/namespace NAT gives each VM a distinct external identity.

The VMM vsock CID remains 3 across restore; its private mount namespace maps the same socket path onto target-owned host resources. The system agent connects through CH's UDS-vsock transport (`CONNECT 2047\n`) using shared generated length-prefixed protobuf frames. Requests have unique nonempty IDs and `eof=true`; replies must match ID and terminal EOF. Cold start requires `ping` protocol 1/portal system. Restore additionally sends 64 fresh random bytes through `reseed`, then `clock_sync` with `unix_seconds` and `nanoseconds`. Failure prevents running readiness. sandboxd uses host-only Admin port 2046 for `activate`, `prepare_template` and `quiesce`; Workload port 2048 carries tenant exec/filesystem/session/port RPC through its authenticated byte relay. Neither channel belongs to engine readiness, and tenant relay cannot access System or Admin methods.

## Backup and explicit recovery

1. Stop the daemon; retain running VMMs and kernel ownership anchors. Create a consistent metadata backup:

   ```sh
   engined --state-dir=/var/lib/sandbox-engine --mode=backup --backup=/safe/engine.db
   ```

   Save both DB and `.json` receipt. `VACUUM INTO` captures a consistent DB; copying a live WAL DB is unsupported. This metadata backup does not copy writable disks, snapshots, or images. Coordinate a separate resource backup with VM/snapshot semantics if disaster recovery requires those bytes.

2. Run `--mode=doctor` to inspect missing/corrupt metadata. Preserve the entire damaged state and runtime evidence. With the daemon stopped, move the original DB, `metadata.db-wal`, and `metadata.db-shm` together to evidence storage. Keep resource directories at their recorded absolute paths; moving the engine to a different state path is unsupported.

3. Restore explicitly:

   ```sh
   engined --state-dir=/var/lib/sandbox-engine --mode=restore-backup --backup=/safe/engine.db
   ```

   Existing DB/WAL/SHM paths cause refusal. Checksum and receipt must match. A durable `RECOVERY_REQUIRED` marker blocks normal startup.

4. Compare the backup point against operation history, retained process receipts/PID starttimes, boot identity, cgroups, namespace receipts/inodes, disks, captures, and snapshot dependencies. An old DB may omit still-live instances or externally applied effects. Do not delete unknown resources or restart old operations based solely on the recovered phase.

5. Record the completed review with `--mode=recover --recovery-note='...'`. Recovered pending/running operations become fixed `manual_recovery_required` failures; they are not replayed from rolled-back history. VM verification is cleared; conservative leases/reservations remain. Start normally to reconcile retained resources, then submit new explicit keys only after resolving uncertainty.

`HEAD.json` store identity/epoch detects a DB rolled back behind a retained HEAD. A coordinated rollback of both DB and HEAD cannot be detected locally. Recovery review does not reconstruct lost history. Unknown ownership, partial namespace creation without a receipt, and live processes with unreachable CH APIs can require manual recovery. The engine preserves evidence rather than guessing.

## Validation and remaining operational work

The test suite covers idempotency and immutable terminal responses, restart adoption without relaunch, snapshot crashes at six boundaries, source resume and disk independence, clone dependency handoff followed by source snapshot deletion, resource rejection before pause, exclusive writer/missing metadata refusal, explicit backup review, identity uncertainty, failed cold agent handshake, restore's fresh instance/default-off network, image publication, OCI traversal/type/diffID/size validation, independent writable copies, and typed UDS transport errors.

Lifecycle unit tests use an injected backend. Required-native image tests exercise real mkfs.erofs/fsck.erofs/umoci in isolated namespaces, and COW mapping tests use actual private/shared native memory mappings. `make kvm-e2e KVM_MATERIALS_DIR=... KVM_ARTIFACTS_DIR=...` additionally boots the exact pinned CH, matching built-in-driver kernel and static guest through systemd. Both image layouts pass guest file/metadata and binary tree-transfer checks, independent writable disks, source-capture deletion, retained private-memory mapping proof and COW resnapshot recovery. Merged cross-layer regular hardlinks are checked before and after restore.

`make kernel-isolation-e2e` verifies actual nft/NAT/TCP policy behavior, including a shared two-flow budget and rejection of a third concurrent connection, in fresh private namespaces and real CLONE_INTO_CGROUP placement/PID/CPU/memory enforcement and actual block-I/O throttling in a new delegated transient user service. Its I/O probe discovers the accounted backing device through io.stat, then compares bounded 4 MiB O_DIRECT writes with fdatasync before and after production setupScope applies a 1 MiB/s budget. It exercises production network preparation with a veth substituted for guest packet injection. Missing prerequisites fail instead of skipping. `make production-e2e` separately passed the actual root launcher/pivot/UID/GID/fd handoff, live VMM adoption after daemon failure, complete 512 MiB sandbox lifecycle, golden/warm provisioning and actual guest/TAP policy+SNAT TCP for source/fork/resume. It uses private namespaces and bounded bindfs/tmpfs storage while preserving all production VMM limits. Actual block-I/O throttling is proven by the separate kernel suite; each deployment must validate its configured backing-device budgets and external firewall integration. See [host setup](../deploy/host/README.md), [the acceptance record](../deploy/ACCEPTANCE.md) and `make privileged-e2e` for external-host lifecycle checks. No runtime simulation fallback is provided.
