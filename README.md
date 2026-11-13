# Haven / Loom

A single-host microVM engine and sandbox service implementing [the engine architecture](docs/architecture.md) and [the sandbox architecture](docs/sandbox-architecture.md).

Start with the [documentation index](docs/README.md) for component contracts, deployment and acceptance. [Unfinished plans](plans/README.md) and [completed plans/evidence](.agents/archive/README.md) are maintained separately from current architecture.

`haven` owns tenant and resource workflows in one SQLite store: templates, warm pools, leases, hibernation, checkpoints, fork, images, VM generations, cleanup and accounting. Loom performs explicit Cloud Hypervisor, Linux and filesystem effects without a control database. `havenctl` uses the business HTTP API; `loomctl` uses the same daemon's private management socket. A static Rust guest agent handles host administration, system readiness and unprivileged workload RPC on separate portals; a small Rust PID 1 mounts the EROFS/ext4/OverlayFS root and execs systemd.

The production command constructs the real host runtime. Component tests inject explicit effects; transport integration runs the actual Rust Unix fixture through Haven and the root SDK. KVM, kernel-isolation and production runners now use Haven/Loom paths; their current results and prerequisites are tracked separately from [historical acceptance](docs/acceptance.md). A compiled runner or historical pass does not establish current Haven production acceptance.

Plan 024 has moved control protocols into `internal/haven/` and resource effects into `loom/` and `loom/host/`. The old Go modules, daemons and control UDS facade have been removed. Build, workspace, CI, runners and deployment examples use the single Haven entrypoint. No existing deployment or old state has been switched or migrated. See [Haven's control contract](docs/haven-control.md) and [Plan 024](plans/024-haven-loom-single-control-design.md) for current verification and blocked acceptance.

## Build and local verification

Use Linux with Go compatible with the module go directives, Rust/Cargo, protoc and GNU make. Scripts and smoke tests also need Bash, Python >=3.11, GNU coreutils and ripgrep; binary checks need file and binutils. `make lint` needs golangci-lint. Prepare these tools on `PATH` before running the commands below; they do not invoke a package manager. The [optional Nix entrypoint](nix/README.md) supplies these tools and runs the same commands. Submodules under `references/` are design references and are not needed to build the service.

```sh
make build
make test check test-race
make lint
make integration
make smoke
```

The repository root is the `github.com/woxQAQ/haven` host module; `operator` and `protocol/gen` remain separate workspace modules. `make test`, `make check` and `make test-race` cover the root and protocol modules; workspace lint explicitly includes operator. The root excludes readonly `references/` and `third_party/` from recursive package discovery. Ordinary builds do not initialize reference submodules.

`make test-race` gives each complete module the standard Go 10-minute test
budget; SQLite-heavy service suites can exceed three minutes under the race
detector. The separate native gate retains its explicit 300s root-module budget.

Binaries are written to `bin/{haven,havenctl,loomctl}` plus the original Rust workspace artifacts. `make integration` explicitly enables the `integration` tag, builds the Rust Unix fixture and checks exec, file transfer, HTTP Upgrade, raw forwarding, Preview, HTTP authorization/idempotency and long runtime paths through Haven and the root SDK. `make smoke` starts one real Haven in private state, checks both clients and writer exclusion, verifies restart idempotency and read-only GC, and backs up the same business/management metadata. Neither target boots a VM.

The quick tests above may skip native image tests when tools or namespaces are
unavailable. `make test-native` instead forces `SANDBOX_REQUIRE_EROFS_TESTS=1`
and runs the complete root Haven/Loom race suite with a 300s timeout. Its stdout is
only Go test JSON. To also require actual pass events for shared/merged
materialization and native GC, and reject unexpected native skips:

```sh
python3 scripts/check-native-gate.py --negative
python3 scripts/check-native-gate.py
```

Prepare matching mkfs/fsck.erofs >=1.9.4 and <2, dump.erofs, umoci 0.6.0,
bubblewrap, util-linux unshare, coreutils and Python >=3.11 on a controlled PATH;
Go/race also needs a C compiler. The gate does not install/download tools or
invoke an environment provider. `--negative` creates a private PATH without
bwrap and succeeds only after observing the selected required test fail for
that exact missing tool; it never removes installed tools. Logs, separate
stdout/stderr and original child exit codes are retained under fresh
gitignored `build/native-gate/run.*` directories.

The `native-image` CI job prepares these tools on a disposable Ubuntu 24.04
runner, runs `--preflight`, `--negative` and the full gate as root with a minimal
environment, then runs lint as the ordinary user. `--preflight` requires
effective UID 0, proves fresh namespaces and full identity mappings, and runs
the actual mapped merged importer. It is not a request to run sudo on a
developer host. Nonroot local imports need usable user/mount namespaces and
subordinate mappings for merged images. See
[image materialization](docs/image-materialization.md#validation) for the
exact capabilities, skip policy and separation from KVM/production acceptance.

## HTTP and configuration

Haven serves the business HTTP API and a private local management UDS from the same service. The business adapter preserves Gin routing, HTTP/1.1 Upgrade, raw duplex streaming and Preview; the egress proxy uses goproxy for HTTP/CONNECT handling.

`haven`, `havenctl` and `loomctl` use Viper with optional `--config FILE`. Files use flat, hyphenated flag keys; YAML, JSON and TOML are supported. Precedence is explicit flags, prefixed environment, file, then defaults. Prefixes are `HAVEN_`, `HAVENCTL_` and `LOOMCTL_`; hyphens become underscores, such as `HAVEN_IO_MAX`, `HAVEN_LISTEN` and `LOOMCTL_SOCKET`. The matching `_CONFIG` variable also selects a file. Startup rejects unknown/null keys, invalid typed values and old flags. It does not consult `ENGINE_`, `SANDBOXD_` or `SANDBOXCTL_`.

```nu
haven --config /private/haven.yaml
with-env {HAVEN_LISTEN: "127.0.0.1:8888"} { haven --config /private/haven.yaml }
open --raw claim.json | havenctl --config /private/havenctl.yaml --key claim-001 claim
loomctl --config /private/loomctl.yaml --dry-run gc
```

Keep token values in private files and `--init` as an explicit first-start flag. Full startup, management, HTTP and backup instructions are in [Haven control](docs/haven-control.md). Deployment examples are [haven configuration](deploy/host/haven.example.yaml), [single service](deploy/host/haven.service), [havenctl](deploy/client/havenctl.example.yaml) and [loomctl](deploy/client/loomctl.example.yaml).

## Static guest and image

```sh
make guest GUEST_TARGET=x86_64-unknown-linux-musl
```

This requires the Rust musl target and native tools used by bundled SQLite, plus `readelf` and GNU `cpio`. Packaging rejects dynamic interpreters and shared-library dependencies and produces `build/guest/{sandbox-agent,sandbox-init,initramfs}`. To package prebuilt binaries from any provider instead of using the host Cargo toolchain:

```sh
make guest GUEST_ARTIFACT_DIR=/absolute/path/to/static-binaries
```

The same `GUEST_ARTIFACT_DIR` option works with `make image`. Guest runtime and RPC details are in [guest/README.md](guest/README.md).

Supply `build/kernel` and its `build/kernel.config` using the built-in drivers in [image/kernel.config](image/kernel.config), then run `make image`. The Dockerfile installs systemd, the agent service and the boot materials. Export the resulting image into a local OCI layout, then use `loomctl --key KEY --oci-layout PATH --digest DIGEST import` through a running Haven's local management socket.

Default `layered-erofs-v1` imports share immutable per-diff-ID EROFS payloads and compose ordered fsmeta mappings through the pinned go-erofs library. The shared path preserves root metadata, cross-layer and forward hardlinks, implicit parents, duplicate replacements, whiteouts, xattrs, ACLs and capabilities. Explicit `--materialization=merged-oci-erofs-v1` retains a separate isolated umoci compatibility profile with self-contained image data. A published digest has a fixed materialization; requesting a different layout conflicts. Neither path silently treats corrupt data as a fallback opportunity.

## Runtime integration checks

On an x86_64 Linux host with user-accessible KVM and user namespaces, supply the matching static VMM/kernel and complete guest userspace described in the [integration test inputs](scripts/README.md#vm-test-inputs), package the static guest as above, then run:

```sh
bash scripts/prepare-vm-materials.sh /absolute/path/to/materials
make kvm-e2e KVM_MATERIALS_DIR=/absolute/path/to/materials \
  KVM_ROOTFS_DIR=/absolute/path/to/guest-userspace-rootfs \
  KVM_ARTIFACTS_DIR=/dev/shm/haven-kvm
```

The preparation command checks the supplied static VMM/kernel, kernel configuration and source metadata, and records artifact hashes; it does not build dependencies. The rootfs must include genuine systemd and its complete runtime dependencies. The KVM test uses actual OCI import, initramfs, systemd, vsock, EROFS/OverlayFS and native VMM snapshot APIs. It checks file contents, ownership, modes, hardlinks, symlinks, whiteouts, opaque directories, xattrs, ACLs and capabilities in the guest. A boot-time root precheck also proves raw reads succeed and raw writes are rejected for every readonly root/data block device. It also checks binary tree-transfer roundtrips, private retained RAM mappings, independent source/clone disks, deletion of a source capture while a clone runs, and restoration of a COW clone's next snapshot. Missing prerequisites fail explicitly.

Use artifact storage with matching `stat` and `/proc/PID/maps` device identity. The development Btrfs subvolume fails that strict proof; tmpfs passed. The fixture reserves about 9 GiB for each offline import before starting VMs; keep additional free space for the retained runtime/image/capture artifacts. Completed runs remain on disk for diagnosis, so archive stopped runs before repeating on a bounded tmpfs. This standalone test runs the actual VMM as the invoking nonroot user. The separate production runner below exercises the privileged launcher, daemon adoption and sandbox lifecycle through one Haven.

With usable user namespaces, TUN, native networking tools and a systemd user manager that delegates cpu/memory/io/pids, run the separate kernel checks:

```sh
make kernel-isolation-e2e
```

This creates fresh private namespaces and a transient user service, verifies actual nft/NAT/TCP allow/deny/revocation/tamper behavior, tests a shared flow budget with concurrent TCP connections, and proves PID exhaustion, CPU throttling, memory OOM enforcement and actual block-I/O throttling. The I/O probe writes two bounded 4 MiB files with O_DIRECT and fdatasync, selects the backing device from io.stat, then verifies that a production-applied 1 MiB/s budget slows the second write. Only the newly owned resources are modified and cleaned. `KERNEL_ARTIFACTS_DIR` selects the evidence directory and must use block-backed storage for the I/O probe; a tmpfs path fails the accounting check. The [acceptance record](docs/acceptance.md) distinguishes these results from remaining production-host checks.

For the production launcher and daemon lifecycle checks in fresh isolated resources, supply a bootable OCI layout and the verified block-device budget from [host deployment](deploy/host/README.md#存储与资源预算):

```nu
with-env {SANDBOX_RUNTIME_IO_MAX: $env.HAVEN_IO_MAX} {
    make runtime-e2e KVM_MATERIALS_DIR=/absolute/path/to/materials RUNTIME_OCI_LAYOUT=/absolute/path/to/bootable-oci-layout RUNTIME_ARTIFACTS_DIR=/private/new-runtime
}
```

This runner needs subordinate UID/GID mappings, usable `/dev/fuse` and a systemd user manager with delegated controllers. It builds the actual Haven daemon and uses the production launcher in fresh user/network/mount/PID namespaces. A private bindfs view serves bounded tmpfs state, so storage writes are charged to the owned FUSE service while VMMs retain their production limits. The exact guest shape and resource budgets are preserved with the artifacts. See the [acceptance record](docs/acceptance.md) for measured outcomes and retained evidence.

## Deploy and use

The new local entrypoint and explicit initialization/recovery commands are described in [Haven control](docs/haven-control.md). Current [host deployment](deploy/host/README.md) describes the single Haven service; existing deployments have not been switched automatically. The management UDS is a privileged host interface; business HTTP should listen on loopback behind a TLS gateway or use its certificate/key flags.

The public [DTOs](api/types.go), [root Go SDK](sdk/client.go) and [control contract](docs/haven-control.md) define the new entrypoint. Network `none` and `relay` leave the NIC blocked. Direct mode installs and verifies an instance-bound IPv4 CIDR/flow policy before enabling the NIC; hostname filtering and credential injection require relay mode. Host-only credentials remain in host memory and must be reconfigured after a daemon restart.

`make privileged-e2e` runs the real lifecycle acceptance suite against an isolated, configured host. Missing `SANDBOX_E2E_*` configuration fails explicitly. It covers cold claim, checkpoint, independent branches, hibernation, concurrent transparent resume, identity/session preservation, rollback and paused deletion. Additional host acceptance requirements are listed in the deployment guide.

## Persistence and current boundaries

Accepted operations are durable and use deterministic step keys. Reusing a key with changed parameters conflicts; a terminal failure remains the original result. Cancellation stops waiting, not accepted work. Unknown runtime identity or uncertain side effects remain visible for recovery instead of being guessed away. Snapshot disk bytes are independent from the live source, while immutable memory backings remain retained independently for clones. Keyed guest exec deduplicates within its retained ledger, which is itself restored with VM snapshots; rollback to before admission requires an external host ledger/fence for stronger guarantees.

Haven takes one offline metadata backup containing the complete SQLite state, node key and matching HEAD; it does not copy VM disks, image data or memory captures. Restoring a known older point fences startup until operator review, and reviewed metadata grants no replay, data or GC authority. Local audit chains and HEAD cannot detect coordinated rollback of all local history.

Haven's reference-aware GC reclaims proven orphan layer caches, completed import work and retired resource versions/backings through explicit Loom effects, durable inode deletion intents and bounded passes. It retains published images, operation/idempotency records and uncertain resources; recovery evidence fences collection. See [the current management boundary](docs/haven-control.md#本地管理协议).

Tree transfer preserves in-tree links, modes, nanosecond mtimes and supported binary user xattrs; imported ownership is normalized to the workload identity. Arbitrary owners, privileged xattrs/ACLs and sparse layout are outside that transfer contract. Recursive filesystem watch remains unsupported. Escaped workload process groups can require VM shutdown for final cleanup. Preview uses signed paths and does not provide per-sandbox browser origins or rewrite absolute asset URLs. Optional gossip placement and remote snapshot archives are not implemented. Production host acceptance remains separate from the standalone VMM/guest and kernel isolation checks.
