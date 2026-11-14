# Haven / Loom

[中文](README.zh-CN.md) · English

A single-host microVM engine and sandbox service implementing the [resource architecture](docs/architecture.md) and [sandbox architecture](docs/sandbox-architecture.md). Start with the [documentation index](docs/README.md) for contracts and deployment. [Unfinished plans](plans/README.md) and [historical evidence](.agents/archive/README.md) are maintained separately.

Haven owns tenant and resource workflows in one SQLite store: templates, warm pools, leases, hibernation, checkpoints, fork, images, VM generations, cleanup and accounting. Loom performs explicit Cloud Hypervisor, Linux and filesystem effects without a control database. `havenctl` uses the business HTTP API; `loomctl` uses the same daemon's private management socket. The Rust guest separates System, Admin and unprivileged Workload RPC; a small Rust PID 1 mounts the EROFS/ext4/OverlayFS root and execs systemd.

## Workspace

The repository root contains `go.work`; each component owns its module. No `internal/` package layer remains.

| Directory | Responsibility |
|---|---|
| `haven/` | Durable control service, HTTP adapters, daemon settings and `cmd/haven` |
| `loom/` | Explicit host effects, management-client settings and `cmd/loomctl` |
| `sdk/` | Business/management clients, guest RPC and `cmd/havenctl` |
| `types/` | Shared wire data, including sanitized error payloads |
| `common/` | Shared CLI file I/O, configuration loading and logging initialization |
| `tools/` | Development tools such as structfmt |
| `protocol/gen/` | Generated Go guest-protocol bindings |
| `operator/` | Kubernetes integration |

Modules use local replacements for standalone `GOWORK=off` checks. The workspace connects them for navigation and workspace lint. CLI flag parsing lives beside each command; `haven/config` and `loom/config` contain their respective settings. Logging initialization in `common/logging` selects a standard `slog` handler.

## Build and local verification

Use Linux with Go matching the module directives, Rust/Cargo, protoc and just. Scripts also need Bash, Python >=3.11, GNU coreutils and ripgrep; binary checks need file and binutils. Lint and formatting need golangci-lint. Prepare these tools on `PATH`, or use the [optional Nix shell](nix/README.md). Ordinary builds do not initialize or modify `references/` submodules.

```nu
just build
just test check test-race
just lint
just integration
just smoke
```

Binaries go to `bin/{haven,havenctl,loomctl}`. Test, vet and race recipes cover Haven, Loom, SDK, types, common, tools and protocol modules; workspace lint also includes operator. `just test-race` retains the 10-minute per-package timeout. `just integration` builds the actual Rust Unix fixture and tests Haven/SQLite, HTTP and guest RPC. `just smoke` starts one real daemon in private state and checks both CLIs, writer exclusion, configuration, restart, GC and offline backup. Neither boots a VM.

Quick tests may skip native checks when tools or namespaces are unavailable. `just test-native` requires EROFS coverage and emits Go test JSON with the existing race/count=1/300s settings. The native gate additionally requires actual shared/merged materialization and GC pass events:

```nu
python3 -I scripts/check-native-gate.py --negative
python3 -I scripts/check-native-gate.py
```

Required tools include matching mkfs/fsck.erofs >=1.9.4 and <2, dump.erofs, umoci 0.6.0, bubblewrap, util-linux unshare and a C compiler. The gate never installs tools or elevates privileges. The negative run uses a private PATH without bwrap. Logs and original exit codes go to fresh `build/native-gate/run.*` directories. See [native verification](docs/image-materialization.md#validation) for namespace and mapping requirements.

## HTTP and configuration

Haven serves business HTTP and a private management UDS. Files selected with `--config FILE` accept flat, hyphenated YAML/JSON/TOML keys. Precedence is explicit flags → prefixed environment → selected file → defaults. Prefixes are `HAVEN_`, `HAVENCTL_` and `LOOMCTL_`; `_CONFIG` selects a file. Unknown/null keys, invalid numeric bounds, incomplete TLS and retired flags are rejected.

```nu
haven --config /private/haven.yaml
with-env {HAVEN_LISTEN: "127.0.0.1:8888", HAVEN_LOG_FORMAT: "json"} {
    haven --config /private/haven.yaml
}
havenctl --config /private/havenctl.yaml --key claim-001 --request-file /private/claim.json claim
loomctl --config /private/loomctl.yaml --dry-run gc
```

Business intents (`tenant`, `claim`, `template`, `change ID`) and generic management `request` use an explicit `--request-file FILE`. Each file must contain one JSON value, at most 1 MiB, with no unknown fields. Stdin is never read. Simple management commands use named flags. Keep tokens and secret-bearing request files private. Daemon logs go to stderr; `--log-level` accepts debug/info/warn/error and `--log-format` accepts text/json.

Use `--init` only for the first start in a new empty private state directory. Full startup, HTTP, management and recovery contracts are in [Haven control](docs/haven-control.md). Deployment examples: [daemon](deploy/host/haven.example.yaml), [service](deploy/host/haven.service), [havenctl](deploy/client/havenctl.example.yaml), [loomctl](deploy/client/loomctl.example.yaml).

## Static guest and image

```nu
just GUEST_TARGET=x86_64-unknown-linux-musl guest
# Package prepared static binaries instead of compiling them.
just GUEST_ARTIFACT_DIR=/absolute/path/to/static-binaries guest
```

Guest builds need Rust musl support, SQLite build dependencies, readelf and GNU cpio. Packaging rejects dynamic executables and produces `build/guest/{sandbox-agent,sandbox-init,initramfs}`. See the [guest contract](guest/README.md). With a kernel and matching `build/kernel.config` satisfying [built-in driver requirements](image/kernel.config), `just image` builds the systemd guest image. Import its exported OCI layout using `loomctl --key KEY --oci-layout PATH --digest DIGEST import`.

Default `layered-erofs-v1` shares immutable layer payloads. Explicit `merged-oci-erofs-v1` uses an isolated umoci materializer. Published digest/materialization identity is fixed; invalid data never silently falls back to another profile.

## Runtime integration checks

Read [runner inputs and boundaries](scripts/README.md) before native, cross-UID or VM runs. KVM needs the pinned static VMM, matching kernel, complete guest systemd userspace, packaged guest, usable `/dev/kvm` and namespaces/mappings. Artifact storage must expose matching stat and `/proc/PID/maps` device identities.

```nu
bash scripts/prepare-vm-materials.sh /absolute/path/to/materials
just KVM_MATERIALS_DIR=/absolute/path/to/materials KVM_ROOTFS_DIR=/absolute/path/to/rootfs KVM_ARTIFACTS_DIR=/dev/shm/haven-kvm kvm-e2e
just kernel-isolation-e2e
with-env {SANDBOX_RUNTIME_IO_MAX: $env.HAVEN_IO_MAX} {
    just KVM_MATERIALS_DIR=/absolute/path/to/materials RUNTIME_OCI_LAYOUT=/absolute/path/to/oci RUNTIME_ARTIFACTS_DIR=/private/new-runtime runtime-e2e
}
```

Kernel checks need TUN, networking tools and a delegated user systemd manager. Production lifecycle checks also need bindfs, `/dev/fuse`, subordinate UID/GID mappings and an explicit block-device I/O budget. `just privileged-e2e` targets an explicitly configured private deployment through `SANDBOX_E2E_*` inputs. Missing required materials fail; component/fixture success does not prove KVM, kernel isolation or production acceptance. Current gaps are tracked in [Plan 024](plans/024-haven-loom-single-control-design.md); [historical acceptance](docs/acceptance.md) applies only to its recorded sources. Existing deployments and old databases are never switched automatically.

## Persistence and current boundaries

Accepted operations retain fixed requests, idempotency identities and terminal results. Canceling a wait does not cancel accepted work. Unknown effects retain resources and charges until trusted inspection proves retirement. Captures use independent disks and immutable RAM backings retained while clones reference them. Guest keyed-exec deduplication rolls back with its snapshot; stronger cross-rollback guarantees require an external fence.

Offline metadata backup includes one SQLite snapshot, node key and matching HEAD; disks and RAM are separate. Restore requires explicit review, which grants no replay, data or GC authority. GC retires proven resources through durable inode deletion intents and atomic responsibility transfer.

Network none/relay leaves the NIC off. Direct mode verifies instance-bound IPv4 CIDR/flow rules before enabling it; hostname filtering and credential injection require relay. Host-only credentials need explicit reconfiguration after restart. Preview uses signed paths without per-sandbox browser origins. Recursive file watch, cross-node recovery, remote snapshot archives and gossip placement remain outside the implemented scope. See the [control contract](docs/haven-control.md) and [Sandbox contract](sandboxd/README.md).
