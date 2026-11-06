# Acceptance record

The single-host implementation has passed real production-daemon lifecycle, launcher/adoption, golden/warm provisioning, guest/TAP TCP, standalone VMM/guest and kernel-isolation acceptance, alongside unit/race tests, Rust transport integration and actual daemon/CLI smoke. Production acceptance used fresh isolated resources and the unchanged runtime limits. This record identifies the materials, evidence and deployment-specific boundaries; the design documents' latency figures remain targets.

## Reproduce

On an x86_64 Linux host with Nix, working user namespaces and user-authorized KVM/TUN:

```sh
bash scripts/build-acceptance-materials.sh
guest_package=$(nix build .#guest-static --no-link --print-out-paths)
make guest GUEST_ARTIFACT_DIR="$guest_package/bin"
make kvm-e2e KVM_MATERIALS_DIR=/absolute/path/to/the/printed/materials-directory \
  KVM_ARTIFACTS_DIR=/dev/shm/sandbox-kvm-acceptance
make kernel-isolation-e2e
SANDBOX_PRODUCTION_IO_MAX="$ENGINE_IO_MAX" make production-e2e \
  KVM_MATERIALS_DIR=/absolute/path/to/the/printed/materials-directory \
  PRODUCTION_OCI_LAYOUT=/absolute/path/to/a/bootable-oci-layout
```

Set `ENGINE_IO_MAX` to the verified device budget described in [host deployment](host/README.md#durable-storage-and-resource-limits). The production runner needs subordinate UID/GID mappings and usable `/dev/fuse`. It builds and starts both real daemons in fresh namespaces under its own delegated transient user service. Its private bindfs view serves byte/inode-bounded tmpfs state while charging underlying writes to the owned storage service; VMM limits stay RAM plus 128 MiB. Direct tmpfs capture is unsuitable here: a 512 MiB diagnostic guest reached its exact 640 MiB limit, recorded `oom=1`, `oom_kill=1` and retained about 499 MiB shmem after death. The bindfs primitive proves matching stat/maps identity, private COW, hardlinks and real dropped-UID permissions. Production block-backed deployments must validate their selected filesystem, shape and storage budget separately.

The materials builder needs the pinned CH Git objects under `references/cocoonstack/cloud-hypervisor`. Kernel-isolation acceptance also needs native ip/nft/unshare tools and a systemd user manager that delegates cpu, memory, io and pids. Missing prerequisites fail explicitly. The kernel runner creates a new transient user service and private namespaces, modifies only the resources it owns, then cleans them. Keep `KERNEL_ARTIFACTS_DIR` on block-backed storage: its I/O probe deliberately fails when no real backing device accounts for the writes.

Use artifact storage with consistent `stat` and `/proc/PID/maps` device identity. Tmpfs passed; the development Btrfs subvolume is rejected by the retained-memory proof. The fixture reserves about 9 GiB per offline image import, plus space for OCI input and retained captures. Repeated runs retain their evidence: archive stopped runs before tmpfs free space becomes insufficient. This reservation is admission accounting rather than measured physical consumption.

## Verified materials

- CH commit: `a02b114bd42c65b73033231a27bd23f3c487fc4c`, static musl, `kvm,io_uring,seccomp`.
- CH SHA256: `0179f0fc96fb34c801ef689856c522556d41c905bfe5187f082f31c248003ed7`.
- Linux: 6.18.56, with the required early-boot/EROFS/vsock/VMGenID options built in.
- bzImage SHA256: `97281dae575cfabd6381554b7f3cb8940acc95f2f8516898a4ab46eed7c6ec9a`.
- nixpkgs revision: `319e4cebef10cbb636c6ea392137b52ad105336a`.
- Guest package: `/nix/store/n7ikq6x3vb6ccz5c0pciklpxdf6maq73-sandbox-guest-static-x86_64-unknown-linux-musl-0.1.0`.

The materials receipt includes derivations, source/config identities and ELF checks. Guest packaging rejects `INTERP` and `NEEDED` and constructs a reproducible initramfs. Local build outputs and logs are ignored artifacts, not checked-in binaries or an independent build attestation.

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

The standalone VMM runs as the invoking device-authorized nonroot user; the separate production runner proves the actual root launcher, restricted root/mounts, UID/GID reduction, KVM/TAP fd handoff, live adoption, complete sandbox lifecycle and guest/TAP policy path. Its endpoints and quotas belong to fresh isolated resources. Each deployment still needs qualification of its real block-device budgets, external firewall/iptables priorities, bandwidth/packet quotas, MTU/offload/fragment handling, concurrent capacity and teardown configuration; use [sandboxd deployment](sandboxd/README.md#real-microvm-acceptance). Actual block-I/O throttling is proven by the separate kernel suite, since the production fixture stores data in tmpfs.

Boot-time guest-root raw block-device write rejection is proven by the standalone fixture; production mountinfo additionally records readonly boot/image/restore bindings. The accepted runtime materials are Linux/x86_64 on the recorded CPU and filesystem configuration. No performance/density targets or cross-host compatibility are claimed.

The default shared materializer now uses native per-diff-ID payload caches and pinned go-erofs metadata-only composition, covering broader OCI namespace semantics without per-image regular-data duplication. Fresh real guest/COW acceptance proves cross-layer and true forward hardlinks, root metadata, implicit parents and duplicate replacement alongside ACL/capability values. Existing native-rebuild catalog identities remain readable. Explicit umoci compatibility remains separate and still refuses true forward links to nonexistent targets. Both paths retain the documented limits for special files, zstd, unsupported metadata and more than 16 layers; see [the image contract](../engine/IMAGE-MATERIALIZATION.md#oci-namespace-semantics-and-limits).

Tree transfer supports its documented metadata subset and normalizes ownership; arbitrary owner restoration, privileged xattrs/ACLs, sparse layout and recursive watch are outside that contract. Published images and operation/idempotency history remain retained. Unknown ownership, incomplete metadata and recovery evidence conservatively fence GC; reviewed rollback does not automatically authorize history collection. See [GC](../engine/INTEGRATION.md#reference-aware-garbage-collection), [guest limits](../guest/README.md#validation-limits) and [host acceptance](host/README.md#recovery-and-acceptance).
