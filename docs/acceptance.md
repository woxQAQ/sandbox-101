# Acceptance record

The single-host implementation has historical passes for production-daemon lifecycle, launcher/adoption, golden/warm provisioning, guest/TAP TCP, standalone VMM/guest and kernel-isolation acceptance, alongside unit/race tests, Rust transport integration and actual daemon/CLI smoke. Each result below belongs to its recorded source, materials and runtime limits; it is not a claim that the current tree or later harness changes were revalidated.

Current entrypoints are under [Reproduce](#reproduce). Source-controlled plan receipts are in the [archive](../.agents/archive/README.md); links under `build/` and machine-local paths are retained historical artifacts and may be unavailable in another checkout. No latency/density SLO is established by this record.

## Historical HEAD revalidation — 2026-11-08

Commit `b4f2b2f750d63a5c36e49f13d58ad91d926b9a24` passed fresh standalone KVM and production acceptance in the root checkout. Both production daemons and the tagged acceptance binaries were compiled from this commit. Guest sources, acceptance scripts and Nix definitions are unchanged from `47f5ce71`; the existing matching guest, pinned kernel/VMM and bootable OCI were reused after SHA256 verification, including all six reachable OCI descriptors. Source HEAD/status and kernel/VMM/guest hashes remained unchanged through execution.

| Fresh gate | Result |
|---|---|
| Full `make kvm-e2e` | Exit 0, command 13.370 s; layered 5.53 s and merged 4.66 s, both PASS, no FAIL/SKIP |
| Full `make production-e2e` | Exit 0, command 144.060 s; top-level test and all four subtests PASS, no FAIL/SKIP |
| Production subtests | Live daemon adoption 3.99 s; complete lifecycle 40.36 s; golden/warm provisioning 22.19 s; real guest TCP 43.38 s |
| Limits and identity | Guest RAM/disk 512 MiB; VMM memory limit 640 MiB; 13 retained VMM memory records with oom=0/oom_kill=0; before/after/warm records show UID 65534, empty groups, all five capability sets zero, NNP=1 and seccomp=2 |
| Cleanup | Transient service not-found/inactive/dead, MainPID=0; its service cgroup absent; no pinned VMM processes or owned host mounts remain |

Fresh evidence: [KVM log](../build/head-vm-pu14c8xn/kvm.log), [production log](../build/head-vm-pu14c8xn/production/run.xqFAZHtS/test.log), and [run report](../build/head-vm-pu14c8xn/REPORT.md). Standalone KVM used a new private 24 GiB tmpfs and namespace-local subordinate-ID files; the host mappings were not changed. Synthetic standalone RAM/disk payloads were released after copying diagnostics. The retention filter also omitted the newly generated OCI, so production used the separately verified existing OCI for the unchanged guest; no KVM retry was needed. Production retained its normal state archive. This run does not repeat kernel-isolation enforcement, throughput measurements or cross-host qualification.

## Reproduce

On an x86_64 Linux host with working user namespaces and user-authorized KVM/TUN, supply the tools and artifacts described in the [integration test inputs](../scripts/README.md#vm-test-inputs). The commands below describe the current entrypoints; the recorded results remain historical and do not validate subsequent harness changes.

```sh
bash scripts/prepare-acceptance-materials.sh /absolute/path/to/materials
make guest
make kvm-e2e KVM_MATERIALS_DIR=/absolute/path/to/materials \
  KVM_ROOTFS_DIR=/absolute/path/to/guest-userspace-rootfs \
  KVM_ARTIFACTS_DIR=/dev/shm/sandbox-kvm-acceptance
make kernel-isolation-e2e
SANDBOX_PRODUCTION_IO_MAX="$ENGINE_IO_MAX" make production-e2e \
  KVM_MATERIALS_DIR=/absolute/path/to/materials \
  PRODUCTION_OCI_LAYOUT=/absolute/path/to/a/bootable-oci-layout
```

Set `ENGINE_IO_MAX` to the verified device budget described in [host deployment](../deploy/host/README.md#durable-storage-and-resource-limits). The production runner needs subordinate UID/GID mappings and usable `/dev/fuse`. It builds and starts both real daemons in fresh namespaces under its own delegated transient user service. Its private bindfs view serves byte/inode-bounded tmpfs state while charging underlying writes to the owned storage service; VMM limits stay RAM plus 128 MiB. Direct tmpfs capture is unsuitable here: a 512 MiB diagnostic guest reached its exact 640 MiB limit, recorded `oom=1`, `oom_kill=1` and retained about 499 MiB shmem after death. The bindfs primitive proves matching stat/maps identity, private COW, hardlinks and real dropped-UID permissions. Production block-backed deployments must validate their selected filesystem, shape and storage budget separately.

The materials preparation step verifies caller-supplied artifacts without invoking an environment provider. Kernel-isolation acceptance also needs native ip/nft/unshare tools and a systemd user manager that delegates cpu, memory, io and pids. Missing prerequisites fail explicitly. The kernel runner creates a new transient user service and private namespaces, modifies only the resources it owns, then cleans them. Keep `KERNEL_ARTIFACTS_DIR` on block-backed storage: its I/O probe deliberately fails when no real backing device accounts for the writes.

Use artifact storage with consistent `stat` and `/proc/PID/maps` device identity. Tmpfs passed; the development Btrfs subvolume is rejected by the retained-memory proof. The fixture reserves about 9 GiB per offline image import, plus space for OCI input and retained captures. Repeated runs retain their evidence: archive stopped runs before tmpfs free space becomes insufficient. This reservation is admission accounting rather than measured physical consumption.

## Verified materials

- CH commit: `a02b114bd42c65b73033231a27bd23f3c487fc4c`, static musl, `kvm,io_uring,seccomp`.
- CH SHA256: `0179f0fc96fb34c801ef689856c522556d41c905bfe5187f082f31c248003ed7`.
- Linux: 6.18.56, with the required early-boot/EROFS/vsock/VMGenID options built in.
- bzImage SHA256: `97281dae575cfabd6381554b7f3cb8940acc95f2f8516898a4ab46eed7c6ec9a`.
- nixpkgs revision: `319e4cebef10cbb636c6ea392137b52ad105336a`.
- Guest package: `/nix/store/n7ikq6x3vb6ccz5c0pciklpxdf6maq73-sandbox-guest-static-x86_64-unknown-linux-musl-0.1.0`.

The materials receipt includes derivations, source/config identities and ELF checks. Guest packaging rejects `INTERP` and `NEEDED` and constructs a reproducible initramfs. Local build outputs and logs are ignored artifacts, not checked-in binaries or an independent build attestation.

## Final composed verification of plans 001–005

All five implementations and their review revisions were applied together to the root checkout, based on `da513a6`, without creating a parent-branch commit. Independent second-round code reviews approved both sandboxd and engine changes. The final combined run rebuilt and tested the actual root-checkout code; its full command/exit/time records are in [REPORT.txt](../build/implementation-final/run.Jd1dCRzq/REPORT.txt), with the implementation summary in [.agents/archive/IMPLEMENTATION.md](../.agents/archive/IMPLEMENTATION.md).

| Final combined gate | Result |
|---|---|
| Workspace lint / capture-tag engine lint | Both passed with 0 issues |
| Go vet: engine, sandboxd, protocol | All passed |
| Rust fmt, clippy, workspace all-target tests | Passed; 29 tests, 0 ignored |
| Required-native full engine race | Passed with `SANDBOX_REQUIRE_EROFS_TESTS=1`; command 86.214 s |
| Full sandboxd / protocol race | Passed; commands 25.149 / 1.323 s |
| Actual two-UID capture isolation | Four scenarios executed and passed, no skips |
| Actual Rust fixture integration / race integration | Passed; commands 1.236 / 2.789 s |
| Rebuilt actual daemon/CLI smoke | All four checks passed |
| Combined production microVM acceptance | Passed; test 99.56 s, command 103.966 s |
| Working/index whitespace checks | Both passed |

The combined production evidence is [production/run.eVLB3y9x](../build/implementation-final/run.Jd1dCRzq/production/run.eVLB3y9x/). It exercised live daemon adoption, complete sandbox lifecycle, golden/warm provisioning and real guest TCP with 512 MiB guest RAM and the unchanged 640 MiB VMM limit. Before/after/warm observations retained UID/GID 65534, empty supplementary groups, all five capability sets zero, NoNewPrivs=1 and Seccomp=2. All thirteen scope records had no OOM kill. The owned transient service exited 0/SUCCESS and was subsequently inactive/not-found; its cgroup was absent. See [production-audit.json](../build/implementation-final/run.Jd1dCRzq/production-audit.json).

The first combined harness used an overly deep TMPDIR, causing Unix socket path-length failures and preventing dropped-UID writer traversal through home ancestors. Original failure logs are retained. The affected commands were rerun sequentially with a newly owned short, traversable temporary directory; every required gate then passed without source/index changes. Ordinary non-verbose Go output does not enumerate optional internal skips; required native tooling, capture scenarios and production execution were separately enforced. Staged source-index and working-source fingerprints matched before/after verification; only prose/plan completion records were updated afterwards.

Deployment boundaries remain explicit: sandboxd schema 2 rejects schema 1 before DDL, and engine restore rejects old snapshots lacking independent-publication proof. Preserve older state and use the matching previous binary for consistent backup before a separately reviewed migration; do not initialize over existing state. Deterministic disk/RAM truncation, equal-length zeroing and removal tests model loss/corruption; no physical host power-off was performed. Uncertain capture stages are resumed and fenced rather than rebuilt from the live source.

## Privileged capture publication boundary (plan005)

The implementation checkout is `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-43b862e6`, based on `da513a6`. All paths below belong to this new checkout or its newly created KVM scratch; the retained parent materials were read only. No old daemon/state, parent evidence, capabilities, namespace restrictions or COW identity checks were modified.

### Durability revision after P1 review

Review found that the initial `materialized` journal flag preceded artifact fsync, and that replay of `captured` / `source_resumed` could hash a damaged unsynced disk into a new manifest. The revision makes the private-material completion proof durable only after source resume, fixed checksums, and successful `syncTree` of all four files and every child/staging directory. The journal atomically records `materialized` with `capture_files`, the fixed checksum map. Replays compare existing material to that proof and never replace it with hashes of changed bytes. `captured`, `source_resumed` and the new `capture_copied` progress step do not authorize replay publication: recovery resumes the source if necessary, fails Incompatible and preserves the uncertain materials. The live disk is not used to reconstruct the earlier generation. A legacy materialized flag without its checksum proof also fails closed. The complete step table is in [snapshot persistence](engine-integration.md#snapshot-and-restore-persistence).

Current revision evidence is under `build/plan005-revision/` in the same isolated checkout:

| Check | Revision evidence |
|---|---|
| Focused RED | `durability-red.log` / `.exit=1`: old code published truncated or equal-length zeroed disk/RAM at durable journal checkpoints, including materialized/synced/renamed states |
| Focused GREEN | `focused-green.log` / `.exit=0`, 52.903 s under race; all 30 truncate/zero/remove cases pass; intact synced material converges with the original disk/RAM generation; uncertain earlier phases fail safely; GC/Clone/Reservation behavior regressions pass |
| Required-native full engine race | `engine-native-race.log` / `.exit=0`, root 87.303 s; `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test -race ./... -count=1 -timeout=180s`, using the native tool closure recorded below; detached execution preserved the full result beyond a 60 s caller wait budget |
| Two-UID isolation | `capture-isolation.log`, all four actual filesystem scenarios pass without skips; retained source FD mutation cannot affect final publication; writer cannot open the published file for writing |
| Workspace lint | `make lint`: `lint.log` / `.exit=0`, **0 issues** across engine/operator/protocol/sandboxd in this checkout |
| Tagged engine lint | `golangci-lint run --build-tags=capture_isolation ./engine/...`: `lint-capture-tag.log`, **0 issues** |
| Vet and whitespace | `engine-vet.log`, engine vet exit 0; `git diff --check` exit 0 |
| Final production | `production.log` / `.exit=0`, **104.21 s**, artifacts `production/run.RoWMLVgm/`; real launcher, daemon adoption, lifecycle, golden/warm provisioning and guest TCP pass |

The damage regression explicitly truncates, replaces bytes with equal-length zeros, or unlinks disk/RAM files after durable journal checkpoints before reopening the engine. It verifies non-publication, no re-capture, no extra VMM launch, preserved evidence and source recovery. These are deterministic filesystem-loss/corruption models; this acceptance did not physically power off the host. A process close/open that retains cached pages alone is not considered power-loss evidence. Focused tests without the tool PATH may skip the native-only GC fixture; the separate required-native full suite above supplies the tools and enforces their presence.

Revision isolation evidence records source device 34/inode 517867 and independent publication inode 517873. A retained source write FD changes the source after publication seal/fsync/rename, while published hashes remain unchanged and the writer receives EACCES for the final path. Production reused the existing current guest and owned OCI input described below; no guest/kernel/VMM material rebuild or parallel VM suite was started. The final VMM identity/capability/COW/resource constraints remain unchanged. The transient production service exited successfully; the VM slot was explicitly released to the parent. Only plan005 source/test/document paths were modified, including fixes for its existing errcheck/staticcheck/formatter findings; no sandboxd or parent-plan edits were made.

### Initial boundary verification (retained evidence)

These runs predate the durability revision above and establish the original namespace/UID/inode boundary and material identities:

| Check | Initial result / evidence |
|---|---|
| Capture-tag compilation | `GOWORK=off go -C engine test -tags=capture_isolation -run '^$' -count=1`, exit 0 |
| Real two-UID filesystem isolation | `make capture-isolation-e2e`, all four named scenarios passed with no skips; `build/plan005/capture-isolation.log` |
| Lifecycle and crash recovery | Lifecycle/SnapshotCrash focused suite passed; new prepared/materialized boundaries, legacy journal rejection, replaced staging inode, failed artifact resume and reservation coverage also passed under race |
| GC | Engine service regressions passed for private staging, unknown entries, replaced parent/staging identities, dry-run and partial deletion/restart including substituted file inodes |
| Required-native engine suite | `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test -race ./... -count=1 -timeout=180s`, exit 0; root package 54.998 s; `build/plan005/engine-native-boundary.log` and `.exit` |
| Vet / whitespace | `GOWORK=off go -C engine vet ./...` and `git diff --check`, exit 0; `build/plan005/engine-vet.log` |
| Real KVM, both layouts | `make kvm-e2e`, exit 0, 10.418 s; `build/plan005/kvm.log`; artifacts `/dev/shm/plan005-kvm-43b862e6/kvm-acceptance-2325353818` |
| Real production launcher | Final `make production-e2e`, exit 0, 120.46 s; `build/plan005/production-boundary.log`; artifacts `build/plan005/production-boundary/run.F5xey5OS/` |

The isolation runner creates fresh user/mount/PID namespaces with subordinate mappings: namespace UID 0 maps host UID 1000, and UIDs 1–65536 map host 100000–165535; namespace GID 0 maps host GID 100 with the same subordinate range. The writer runs at UID/GID 1 with zero effective/permitted capabilities. The root-owned capture/operation parents are 0711 and publication is 0700. One retained-FD run recorded source device 34/inode 489888 and publication device 34/inode 489893. After private copy, seal, fsync and final rename, the writer changed the source through its preexisting write FD, received EACCES opening the final publication file, and all publication SHA256s remained unchanged. Separate dropped-UID processes created FIFO, symlink, directory, unknown and oversized artifacts; the production handoff rejected every case without blocking. Metadata/content sentinels remained unchanged. The engine operation regression also verifies that rejected material never publishes and the source resumes.

Current guest package `/nix/store/05dhwqkv3l90d72q2kk6yp1bnbypfpa3-sandbox-guest-static-x86_64-unknown-linux-musl-0.1.0` was built from this checkout and packaged into `build/guest`, refreshing the Workload protocol. SHA256s: agent `2463ee2fbcba231fc2ef48d78b6674fdf02f29da6f844b0bb7a0a0af6f557ddb`, init `6ef7d992c9e95688ac3455f539226aee1efb7c3534f63d1eccd181c7f5dfc015`, initramfs `fa30e046b478fa0f772d644ca10ad09139c7046acf1db5b5fe37c9bacf4a1699`. Guest build receipt/log: `build/plan005/guest-package.txt` and `guest-build.log`. Native tools came from the pinned Nix closure `/nix/store/ppifd9prd8f7a4kkbrq5m50ss153r4jr-sandbox-kvm-acceptance-userspace`; its erofs, umoci, bubblewrap and e2fsprogs bin directories were explicitly added to PATH for required-native race tests.

KVM reused the parent's exact kernel/VMM receipt at `/home/woxQAQ/sandbox-101/build/runtime/acceptance-materials/run.rVAvN3Ca` and the fresh `build/guest`. Production consumed the newly generated OCI layout under the new KVM run, using the previously verified explicit budget `259:0 rbps=268435456 wbps=134217728 riops=20000 wiops=10000`. No duplicate kernel/VMM materials were built. The final production run preserved UID/GID 65534, empty supplementary groups, zero capability sets, private namespaces, 512 MiB guest RAM and the unchanged 640 MiB VMM limit. It passed live daemon adoption, complete sandbox lifecycle, golden/warm provisioning and guest TCP allow/deny; its transient service exited successfully, with peak memory 8 GiB and no swap. Prior successful development runs remain separately at `build/plan005/production/run.kyNm30G5/`, `build/plan005/production-final/run.C8VOUuF1/` and `build/plan005/production-prepared/run.bfj62zPY/`; the final evidence is the boundary run above.

The staging layout is operation-local `capture/<op>/publication`, under a root-controlled parent, with its exact path and inode identity journaled. It moves atomically onto the final snapshot path on the same state filesystem. This avoids a new top-level GC namespace and lets the existing recursive inode deletion journal cover staging descendants; GC ownership/inventory and behavior tests were updated accordingly. Admission reserves `(2 * MemoryMiB + DiskMiB + 224) MiB`, covering copy fallback and the retained CH source. Old capture journals without identities fail explicitly and preserve evidence, including existing GC deletion intents without a matching boundary. New manifests carry `publication_mode: IndependentInodes`; restore rejects older published or already-transferred manifests that cannot prove this boundary. A service regression verifies rejection without another VMM launch or evidence mutation. The plan index remains parent-maintained.

## Passed verification

For the terminology cleanup and Workload portal rename, verification passed for Rust workspace tests with all targets, clippy and formatting; all sandboxd package race tests and vet; privileged integration compilation; the actual Rust fixture through the Go client under the race detector; and `make smoke`. The portal identity is now `workload`, so rebuild and deploy the guest agent and host daemons together, including refreshing guest image/template artifacts that contain the agent. Existing guest artifacts using the previous portal identity must be rebuilt before use with the updated host. The retained production/KVM results and material hashes below describe earlier runs; production/KVM acceptance was not rerun for this cleanup.

After the final runtime changes, required-native whole-engine race tests passed in 41.422 s and engine vet passed. sandboxd/protocol race tests and vet passed again (sandboxd root 8.399 s); privileged integration vet/compilation also passed. Rust workspace tests (29), formatting and clippy with warnings denied passed. Final `make integration smoke` rebuilt all three Go CLIs and the Rust workspace, exercised the actual Rust agent fixture (0.521 s), engine UDS, sandboxd health/API and CLI, and verified restart idempotency and offline metadata backup. The required-native GC race suite covers the dry-run and shared-cache fixes.

After the Gin/Viper migration, required-native whole-engine race tests and vet passed again (engine root 44.863 s), as did sandboxd/protocol race tests and vet (sandboxd root 9.555 s). Configuration tests cover flags/environment/file/default precedence, explicit false/zero/empty overrides, malformed types, unknown keys, missing files and CLI argument passthrough. The actual Rust transport fixture passed through Gin under the race detector in 1.892 s, including Upgrade, keyed-exec replay, raw half-close forwarding and Preview. Rebuilt-daemon smoke passed configuration-file startup, environment and flag overrides across all three binaries, restart idempotency and offline backup. The sample service units validated with executable paths mapped to workspace binaries. These checks cover the HTTP/configuration migration; the production/KVM measurements below belong to the retained earlier runs.

`TestProductionMicroVMAcceptance` passed through both real daemons in 104.68 s with a 1-vCPU, 512 MiB RAM, 512 MiB disk guest and the unchanged 640 MiB per-VMM cgroup limit:

- The production launcher performed pivot_root and created separate mount/PID/IPC/UTS/network namespaces. The VMM was namespace PID 1, UID/GID 65534, with empty supplementary groups, all five capability sets zero, no_new_privs=1 and seccomp=2. Kernel mountinfo records readonly boot/image/restore bindings. The actual KVM/TAP device check used the dropped identity; cold TAP fd handoff used preboot vm.add-net.
- SIGKILL of both daemons followed by restart adopted the exact live VMM: PID 134, starttime 13201959, instance `run-99fc0edeb2ba928a53425700de3c4626`. Guest machine-id and live shell state survived. This subtest passed in 3.85 s.
- The complete original `TestMicroVMLifecycle` assertions passed in 23.74 s: cold claim/env activation, checkpoint, two independent forks, hibernation, eight concurrent transparent resumes, identity/session preservation, rollback, paused zero-wake deletion and branch survival after source deletion.
- Real template_build warmup and prepare_template, golden clone and a target_warm=1 ready-pool claim passed in 13.41 s. The warm claim reused PID 1163. Its immutable 512 MiB backing was device `0:ab`, inode 8756, mode 0444; `/proc/PID/maps` showed the matching complete writable-private `/restore/memory-ranges` mapping.
- Actual guest TCP crossed virtio/TAP, namespace policy and SNAT for source, fork and resume in 34.88 s. The local server observed `10.200.0.1`, `10.200.0.2`, then `10.200.0.1`; unlisted CIDR and none-policy probes were blocked. Public-address test endpoints existed only inside the disconnected acceptance namespace.

The production fixture uses a new 24 GiB/300,000-inode tmpfs behind packaged bindfs and a transient service capped at 8 GiB RAM, no swap and 2,048 tasks. Its image import is bounded to 512 MiB tar bytes and 100,000 entries; ordinary import defaults are documented separately. The service peaked at 7.9 GiB, swap remained zero, and all 13 retained VMM scope records reported oom=0/oom_kill=0. The collected service was inactive/not-found and its owned cgroup was absent after completion. Logical storage sizing accommodates active and retiring reservations; no production admission condition or VMM limit was loosened.

`TestKVMOCIFileViewAndCOW` passed for both `layered-erofs-v1` and `merged-oci-erofs-v1`, using actual offline OCI import, native erofs-utils/umoci, pinned go-erofs metadata composition for the new shared profile, the pinned VMM, packaged init/agent and genuine systemd/runtime closure. The latest complete run passed in 10.825 s (shared 4.94 s, explicit merged 5.22 s). Its offline-import Engine uses an explicit test backend; VMM execution, snapshot/restore and guest RPC use the real implementation.

The guest checks establish:

- Serial-based EROFS external-device mounting and the ext4/OverlayFS root, initramfs handoff to systemd and real AF_VSOCK readiness.
- Contents, numeric owners, modes, mtimes, hardlink identity, symlink payload, override/whiteout/opaque behavior, binary user xattrs, named-user ACLs and capability bytes.
- Workload UID/GID 1000, protected system directories, writable workspace and rejection of writes through the readonly lower filesystem. Before agent startup, a root precheck proves block-node write permissions and successful raw reads, then confirms raw writes to every readonly root/data device are rejected. The actual kernel returns EPERM on those writes.
- Binary tree push/pull/push with forward hardlinks, relative/dangling symlinks, permission bits, nanosecond mtimes and binary user xattrs; imported owners normalize to the workload identity.
- Actual private retained-memory mappings before resume, independent writable source/clone disks, deletion of the first source capture while its clone runs, and restore from the clone's next snapshot.
- Cross-layer regular-hardlink identity, root metadata/xattrs and duplicate replacement in both layouts after boot and after the second restore. The new shared profile also proves true forward hardlink chains and inherited implicit-parent attributes. The explicit merged fixture supplies readable directory headers because that profile's fixed umask 077 makes otherwise implicit directories private.

`TestKernelIsolationRootlessNetwork` passed real local TCP probes through production network preparation and policy methods, substituting a veth for guest packet injection after TAP creation. All target routes are first proven reachable. Checks cover a narrow public allowlist, private/metadata denial under `/0`, off-only replacement, nil revocation, stale receipt/instance rejection, ruleset-tamper detection and shutdown. With `max_connections=2`, concurrent TCP connections to two distinct public targets succeed and a third handshake is rejected. Kernel `ct count` can retire completed TCP flows before their conntrack entries expire; the saturation test holds both connections open explicitly.

`TestKernelIsolationRootlessCgroup` passed production limit readbacks and CLONE_INTO_CGROUP placement, then tightened only its newly owned test scope to prove enforcement. PID exhaustion returned EAGAIN and incremented `pids.events`; a 25 ms CPU-time worker increased `nr_throttled` under `cpu.max=5000 100000`; a bounded 48 MiB page-touch worker under a 32 MiB limit was SIGKILLed and incremented `oom_kill`. Two 4 MiB O_DIRECT writes with fdatasync established actual backing-device accounting: device `259:0` recorded 4,231,168 written bytes per probe; baseline took 12.82 ms and the production-applied `wbps=1048576` budget took 4.014 s. The device is discovered through io.stat rather than guessed from the filesystem device number. The transient unit and owned groups were cleaned.

GC tests cover current/pending references, leases, catalog/cache dependencies, source-snapshot deletion with retained clone backing, mapped retired files, concurrent import, bounded batches, durable deletion-intent replay, inode replacement, corrupt/missing roots and idempotency retention. The dry-run regression proves missing historical paths do not cause backing/reservation retirement or a metadata-epoch change.

## Local evidence

The following paths identify this development run; they are not portable deployment configuration:

- `build/gin-viper-acceptance.4snJul4a/`: Gin/Viper migration checks, including final required-native engine race/vet, sandboxd/protocol race/vet with the actual Rust fixture, and rebuilt daemon/CLI configuration smoke logs.
- `build/runtime/acceptance-materials/run.rVAvN3Ca/materials.json`: VMM/kernel materials and identities.
- `build/acceptance-report.KRA6gvNM/`: preserved engine/sandboxd/protocol/GC race logs, integration/smoke log, kernel-flow log and static guest SHA256s. `kvm-final.log` records the final root raw-write/tree/cross-layer/COW pass; `kvm/` retains 43 guest/VMM serial logs, memory-map proofs and JSON metadata files.
- `build/kvm-acceptance-archive.lsike5Xi/`: archived complete stopped KVM runs, including `kvm-acceptance-866084354` with the latest shared/merged broader-OCI guest/COW pass, `kvm-acceptance-1606659795` with the complete image/disk/memory bytes and OCI input used by production acceptance, and older `kvm-acceptance-1172430281` with merged cross-layer checks. The production OCI input was moved from tmpfs only after both delegated tasks stopped and no VMM remained; its 3.2 GiB evidence is preserved on the project filesystem.
- `build/acceptance-report.KRA6gvNM/kvm-shared-full.log` and `kvm-shared-pa5jal6_/`: latest complete KVM result and 45 guest/VMM diagnostic and metadata files, retained separately from the full archived payloads.
- `build/production-acceptance/run.TJ5ZWowV/`: complete successful production result, exact compiled daemons/test, compressed engine/sandboxd state, guest TCP observations, launch/adoption and restored-memory proofs, per-scope memory events and cleanup proof.
- `build/acceptance-report.KRA6gvNM/production-pk423r9m/`: 71 copied production diagnostic files, including the full test log, quota/shape records and private FUSE primitive evidence. The selected shape is 512 MiB in both the shape record and test log; an older explanatory line in that run's record still mentions the provisional 128 MiB default. The final runner defaults to 512 MiB.
- `build/acceptance-report.KRA6gvNM/engine-shared-final-race.log`, `engine-shared-final-vet.log`, `product-protocol-final-race-vet.log` and `integration-production-final-smoke.log`: final checks after production runtime edits.
- `build/production-acceptance/run.XldWAgGK/`: diagnostic raw-tmpfs snapshot OOM evidence explaining the bindfs fixture; this failed preliminary run is separate from the successful final result.
- `build/kernel-isolation-acceptance-flow/`: nft rules, stateless ruleset JSON, flow-budget result, cgroup configuration/enforcement and user-service test log.
- `build/kernel-isolation-acceptance-io/`: repeated complete kernel suite with `cgroup-io-enforcement.txt` recording actual direct-I/O bytes, observed device, applied budget and timings.

Successful suite durations are evidence of completion, not cold-start or production latency measurements.

## Remaining acceptance and implementation boundaries

The standalone VMM runs as the invoking device-authorized nonroot user; the separate production runner proves the actual root launcher, restricted root/mounts, UID/GID reduction, KVM/TAP fd handoff, live adoption, complete sandbox lifecycle and guest/TAP policy path. Its endpoints and quotas belong to fresh isolated resources. Each deployment still needs qualification of its real block-device budgets, external firewall/iptables priorities, bandwidth/packet quotas, MTU/offload/fragment handling, concurrent capacity and teardown configuration; use [sandboxd deployment](../deploy/sandboxd/README.md#real-microvm-acceptance). Actual block-I/O throttling is proven by the separate kernel suite, since the production fixture stores data in tmpfs.

Boot-time guest-root raw block-device write rejection is proven by the standalone fixture; production mountinfo additionally records readonly boot/image/restore bindings. The accepted runtime materials are Linux/x86_64 on the recorded CPU and filesystem configuration. No performance/density targets or cross-host compatibility are claimed.

The default shared materializer now uses native per-diff-ID payload caches and pinned go-erofs metadata-only composition, covering broader OCI namespace semantics without per-image regular-data duplication. Fresh real guest/COW acceptance proves cross-layer and true forward hardlinks, root metadata, implicit parents and duplicate replacement alongside ACL/capability values. Existing native-rebuild catalog identities remain readable. Explicit umoci compatibility remains separate and still refuses true forward links to nonexistent targets. Both paths retain the documented limits for special files, zstd, unsupported metadata and more than 16 layers; see [the image contract](image-materialization.md#oci-namespace-semantics-and-limits).

Tree transfer supports its documented metadata subset and normalizes ownership; arbitrary owner restoration, privileged xattrs/ACLs, sparse layout and recursive watch are outside that contract. Published images and operation/idempotency history remain retained. Unknown ownership, incomplete metadata and recovery evidence conservatively fence GC; reviewed rollback does not automatically authorize history collection. See [GC](engine-integration.md#reference-aware-garbage-collection), [guest limits](../guest/README.md#validation-limits) and [host acceptance](../deploy/host/README.md#recovery-and-acceptance).
