# sandbox-101

A single-host microVM engine and sandbox service implementing [the engine architecture](architecture.md) and [the product architecture](product-architecture.md).

`engined` owns VMs, Cloud Hypervisor processes, namespaces, cgroups, images, disks and snapshots. `sandboxd` uses its Unix-socket API to provide templates, warm pools, tenant leases, hibernation, checkpoints, fork and raw guest connections. Each daemon owns an independent SQLite store and writer lock. A static Rust guest agent handles host administration, system readiness and unprivileged workload RPC on separate portals; a small Rust PID 1 mounts the EROFS/ext4/OverlayFS root and execs systemd.

The production entrypoints use the real runtime. Unit tests inject explicit backends; transport integration launches the actual Rust Unix fixture. Standalone KVM acceptance boots the exact pinned Cloud Hypervisor with the static guest and genuine systemd, verifies both EROFS materializations inside the guest, exercises tree transfer, and restores COW clones and their snapshots. A separate production acceptance runner exercises the privileged launcher, live adoption after daemon failure and the product microVM lifecycle through both actual daemons; its measured outcomes are recorded in [the acceptance record](deploy/ACCEPTANCE.md). The latency figures in the design documents are targets, not measured results.

## Build and local verification

Use Linux with Go compatible with the module go directives, Rust, protoc and GNU make. `nix develop` provides the development tools from the pinned flake. Submodules under `references/` are design references and are not needed to build the service.

```sh
make build
make test check test-race
make lint
make integration
make smoke
```

The repository root is a Go workspace, not a module. `make lint` checks all four modules in `go.work` using explicit package patterns; `make lint-fix` also applies automatic fixes. Running `golangci-lint run ./...` or `go fix ./...` at the root fails because `./...` does not expand to the workspace modules.

Binaries are written to `bin/{engined,sandboxd,sandboxctl}`. `make integration` launches the Rust agent fixture and exercises exec, file transfer, HTTP Upgrade, raw forwarding and Preview through the Go service and SDK. `make smoke` launches the actual daemons and CLI in temporary private state, verifies control-plane traffic and restart idempotency, and takes an offline metadata backup. It does not boot a VM.

Image tests use native erofs-utils and umoci when available. To require the layered EROFS tests instead of skipping missing tools:

```sh
SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test ./... -count=1
```

See [image materialization](engine/IMAGE-MATERIALIZATION.md) for compatibility test requirements and the supported OCI contracts.

## HTTP and configuration

The engine UDS API and product HTTP API use Gin routes and authentication middleware. The product retains HTTP/1.1 Upgrade, raw duplex streaming and Preview forwarding; the egress proxy uses goproxy for HTTP/CONNECT protocol handling.

`engined`, `sandboxd` and `sandboxctl` use Viper with optional `--config FILE`. Files use the same flat, hyphenated keys as the existing flags; YAML, JSON and TOML are supported. Configuration precedence is explicit flags, environment, file, then defaults. Environment variables use `ENGINE_`, `SANDBOXD_` and `SANDBOXCTL_` respectively, with hyphens replaced by underscores: `ENGINE_IO_MAX`, `SANDBOXD_LISTEN` and `SANDBOXCTL_ENDPOINT`, for example. A file can also be selected with the corresponding `_CONFIG` variable. Configuration is loaded at startup, without implicit file discovery or hot reload. Unknown file keys and invalid typed values fail startup.

```sh
engined --config /etc/sandbox-engine/config.yaml
SANDBOXD_LISTEN=127.0.0.1:8888 sandboxd --config /etc/sandboxd/config.yaml
sandboxctl --config /private/sandboxctl.yaml --key claim-001 claim < claim.json
```

Example files are in [host configuration](deploy/host/engined.example.yaml), [product configuration](deploy/product/sandboxd.example.yaml) and [client configuration](deploy/client/sandboxctl.example.yaml). Token values stay in private token files referenced by configuration. Keep `--init` as an explicit first-start option rather than enabling it in a persistent configuration file.

## Static guest and image

```sh
make guest GUEST_TARGET=x86_64-unknown-linux-musl
```

This requires the Rust musl target and native tools used by bundled SQLite, plus `readelf` and GNU `cpio`. Packaging rejects dynamic interpreters and shared-library dependencies and produces `build/guest/{sandbox-agent,sandbox-init,initramfs}`. The flake's `guest-static` package cross-builds both binaries with the static musl toolchain and checks the ELF outputs during installation. To package those binaries instead of using the host Cargo toolchain:

```sh
guest_package=$(nix build .#guest-static --no-link --print-out-paths)
make guest GUEST_ARTIFACT_DIR="$guest_package/bin"
```

The same `GUEST_ARTIFACT_DIR` option works with `make image`. Guest runtime and RPC details are in [guest/README.md](guest/README.md).

Supply `build/kernel` and its `build/kernel.config` using the built-in drivers in [image/kernel.config](image/kernel.config), then run `make image`. The Dockerfile installs systemd, the agent service and the boot materials. Export the resulting image into a local OCI layout and import its exact manifest digest using the offline engined import command.

Default `layered-erofs-v1` imports share immutable per-diff-ID EROFS payloads and compose ordered fsmeta mappings through the pinned go-erofs library. The shared path preserves root metadata, cross-layer and forward hardlinks, implicit parents, duplicate replacements, whiteouts, xattrs, ACLs and capabilities. Explicit `--materialization=merged-oci-erofs-v1` retains a separate isolated umoci compatibility profile with self-contained image data. A published digest has a fixed materialization; requesting a different layout conflicts. Neither path silently treats corrupt data as a fallback opportunity.

## Runtime acceptance

On an x86_64 Linux host with user-accessible KVM, user namespaces and Nix, build the matching static VMM and kernel, package the static guest as above, then run:

```sh
bash scripts/build-acceptance-materials.sh
make kvm-e2e KVM_MATERIALS_DIR=/absolute/path/to/the/printed/run-directory \
  KVM_ARTIFACTS_DIR=/dev/shm/sandbox-kvm-acceptance
```

The materials builder needs the pinned Cloud Hypervisor Git objects under `references/cocoonstack/cloud-hypervisor`. It preserves sources, build logs, kernel configuration and artifact hashes in a fresh run directory. The KVM test uses actual OCI import, initramfs, systemd, vsock, EROFS/OverlayFS and native VMM snapshot APIs. It checks file contents, ownership, modes, hardlinks, symlinks, whiteouts, opaque directories, xattrs, ACLs and capabilities in the guest. A boot-time root precheck also proves raw reads succeed and raw writes are rejected for every readonly root/data block device. It also checks binary tree-transfer roundtrips, private retained RAM mappings, independent source/clone disks, deletion of a source capture while a clone runs, and restoration of a COW clone's next snapshot. Missing prerequisites fail explicitly.

Use artifact storage with matching `stat` and `/proc/PID/maps` device identity. The development Btrfs subvolume fails that strict proof; tmpfs passed. The fixture reserves about 9 GiB for each offline import before starting VMs; keep additional free space for the retained runtime/image/capture artifacts. Completed runs remain on disk for diagnosis, so archive stopped runs before repeating on a bounded tmpfs. This standalone test runs the actual VMM as the invoking nonroot user. The separate production runner below exercises the privileged launcher, daemon adoption and product lifecycle through both daemons.

With usable user namespaces, TUN, native networking tools and a systemd user manager that delegates cpu/memory/io/pids, run the separate kernel checks:

```sh
make kernel-isolation-e2e
```

This creates fresh private namespaces and a transient user service, verifies actual nft/NAT/TCP allow/deny/revocation/tamper behavior, tests a shared flow budget with concurrent TCP connections, and proves PID exhaustion, CPU throttling, memory OOM enforcement and actual block-I/O throttling. The I/O probe writes two bounded 4 MiB files with O_DIRECT and fdatasync, selects the backing device from io.stat, then verifies that a production-applied 1 MiB/s budget slows the second write. Only the newly owned resources are modified and cleaned. `KERNEL_ARTIFACTS_DIR` selects the evidence directory and must use block-backed storage for the I/O probe; a tmpfs path fails the accounting check. The [acceptance record](deploy/ACCEPTANCE.md) distinguishes these results from remaining production-host checks.

For the production launcher and daemon lifecycle checks in fresh isolated resources, supply a bootable OCI layout and the verified block-device budget from [host deployment](deploy/host/README.md#durable-storage-and-resource-limits):

```sh
SANDBOX_PRODUCTION_IO_MAX="$ENGINE_IO_MAX" make production-e2e \
  KVM_MATERIALS_DIR=/absolute/path/to/the/printed/run-directory \
  PRODUCTION_OCI_LAYOUT=/absolute/path/to/a/bootable-oci-layout
```

This runner needs subordinate UID/GID mappings, usable `/dev/fuse` and a systemd user manager with delegated controllers. It builds both actual daemons and uses the production launcher in fresh user/network/mount/PID namespaces. A private bindfs view serves bounded tmpfs state, so storage writes are charged to the owned FUSE service while VMMs retain their production limits. The exact guest shape and resource budgets are preserved with the artifacts. See the [acceptance record](deploy/ACCEPTANCE.md) for measured outcomes and retained evidence.

## Deploy and use

Provision the engine according to [host deployment](deploy/host/README.md), then follow [product deployment](deploy/product/README.md). Both initialization and recovery acknowledgment are explicit. The private engine UDS is a privileged host interface. Product HTTP should listen on loopback behind a TLS gateway or use its certificate/key flags.

The [product API guide](sandboxd/README.md) documents tokens, commands, raw connections, Preview and the Go SDK. Network `none` and `relay` leave the NIC blocked. Direct mode installs and verifies an instance-bound IPv4 CIDR/flow policy before enabling the NIC; hostname filtering and credential injection require relay mode. Host-only credentials remain in host memory and must be reconfigured after a daemon restart.

`make privileged-e2e` runs the real lifecycle acceptance suite against an isolated, configured host. Missing `SANDBOX_E2E_*` configuration fails explicitly. It covers cold claim, checkpoint, independent branches, hibernation, concurrent transparent resume, identity/session preservation, rollback and paused deletion. Additional host acceptance requirements are listed in the deployment guide.

## Persistence and current boundaries

Accepted operations are durable and use deterministic step keys. Reusing a key with changed parameters conflicts; a terminal failure remains the original result. Cancellation stops waiting, not accepted work. Unknown runtime identity or uncertain side effects remain visible for recovery instead of being guessed away. Snapshot disk bytes are independent from the live source, while immutable memory backings remain retained independently for clones. Keyed guest exec deduplicates within its retained ledger, which is itself restored with VM snapshots; rollback to before admission requires an external host ledger/fence for stronger guarantees.

Engine and product backups cover metadata and receipts/keys; they do not copy VM disks, image data or memory captures. Restoring a known older point fences startup until an operator reviews it against actual resources. Local audit chains and engine HEAD cannot detect coordinated rollback of all local history.

Reference-aware engine GC reclaims proven orphan layer caches, completed import work and retired resource versions/backings, with durable inode deletion intents and bounded background passes. It retains published images, operation/idempotency records and uncertain resources; recovery evidence fences collection. See [the collection contract](engine/INTEGRATION.md#reference-aware-garbage-collection).

Tree transfer preserves in-tree links, modes, nanosecond mtimes and supported binary user xattrs; imported ownership is normalized to the workload identity. Arbitrary owners, privileged xattrs/ACLs and sparse layout are outside that transfer contract. Recursive filesystem watch remains unsupported. Escaped workload process groups can require VM shutdown for final cleanup. Preview uses signed paths and does not provide per-sandbox browser origins or rewrite absolute asset URLs. Optional gossip placement and remote snapshot archives are not implemented. Production host acceptance remains separate from the standalone VMM/guest and kernel isolation checks.
