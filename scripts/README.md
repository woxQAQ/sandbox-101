# Integration tests

Choose a runner by the behavior it exercises. KVM is the Linux virtualization interface used by Cloud Hypervisor. A Haven deployment may use the same interface as a standalone guest image test.

| Command | What runs | What the test observes |
|---|---|---|
| `just integration` | Haven/SQLite and the Rust Unix fixture | Guest RPC through Haven HTTP and the sandbox relay |
| `just smoke` | One daemon, `havenctl` and `loomctl` in private state | Writer exclusion, daemon restart, GC and offline backup |
| `just guest-image-e2e` | A standalone Cloud Hypervisor VM booted with each OCI layout | Guest file metadata, guest RPC and independent disk and RAM state across clone and restore |
| `just host-policy-e2e` | TCP peers and cgroup workers in private Linux namespaces, without a VM | Network allow/deny and flow limits, CPU throttling, PID exhaustion, memory limits and block I/O throttling |
| `just runtime-e2e` | A new private Haven daemon launches VMs | Provisioning, adoption of a live VM after daemon restart, sandbox lifecycle and guest TCP over TAP |
| `just privileged-e2e` | A caller-provisioned private Haven deployment | Sandbox lifecycle through the business API, with read-only VM observations |

Deployment qualification applies to a particular host, including its storage budget, firewall and delegated controllers. Historical run records identify the source, inputs and outcome for each execution. See [host deployment](../deploy/host/README.md) and [historical test results](../docs/verification-history.md).

## Go package tests and integration scenarios

`just integration` builds the Rust fixture and selects `haven/integration/` with the `integration` build tag. The fixture supplies owned host, System and Admin effects. Haven persists control intents, witnesses and accounting in SQLite. Missing `SANDBOX_AGENT_FIXTURE` fails the tagged run.

Package tests live beside the implementation they exercise. `haven/image` tests OCI import and materialization, and `haven/gc` tests reclamation against real artifacts. `haven/integration/workflows` covers in-process scenarios spanning multiple services and runs in the ordinary test suite. Tests under `haven/integration` select external runtime dependencies with build tags. See [Haven test ownership](../haven/README.md#测试归属).

The native gate captures the Go JSON stream from `just test-native` and requires explicit pass events for its native test inventory. `just --quiet` suppresses that stream. Missing tools or runner capabilities fail a required run. The negative gate checks that deliberately removing a required tool produces the expected diagnostic. See [build and local verification](../README.md#build-and-local-verification).

## VM test inputs

The VM runners consume prepared tools and artifacts. On x86_64 Linux, supply a dedicated writable `VM_MATERIALS_DIR` with the following files.

| File | Required input |
|---|---|
| `cloud-hypervisor` | Static x86_64 ELF matching the [deployment revision](../deploy/host/cloud-hypervisor-revision), built with `kvm,io_uring` and compiled-in seccomp |
| `bzImage` | The x86_64 Linux boot image from the chosen guest kernel build |
| `vmlinux` | That kernel's uncompressed static ELF, retaining symbols |
| `kernel.config` | That build's config, satisfying [the built-in driver requirements](../image/kernel.config) |
| `kernel-source.json` | Source identity with nonempty `version` and `source_hash` strings |

Keep each build's files and provenance together. The following command verifies architecture, static linkage, the CH version pin, kernel config, symbols and boot header. It records hashes in `materials.json` and writes `cloud-hypervisor.json`. Building the artifacts, verifying their origin and booting the guest are separate steps.

```nu
bash scripts/prepare-vm-materials.sh /absolute/path/to/materials
```

### Guest image and snapshot restore

`GUEST_ROOTFS_DIR` must contain a complete guest userspace, including genuine systemd at `/usr/lib/systemd/systemd`, vendor units, `/bin/sh`, `/bin/{cat,stat,id,readlink,sync,dd,getfattr,getfacl}` and all required interpreters, libraries and symlink targets. Absolute symlinks resolve inside this tree. Supply a dedicated guest root. The fixture copies it with root ownership and normalized timestamps, then overlays its kernel, static agent/initramfs, accounts and systemd units. Keep `/boot`, `/etc` and `/usr/local/libexec` as real directories when present. Exclude devices, sockets, credentials and live runtime state.

The optional [fixture Dockerfile](fixtures/guest-rootfs.Dockerfile) prepares this userspace. The runner reads packaged guest binaries from `GUEST_DIR` (default `build/guest`) and writes artifacts to `GUEST_IMAGE_ARTIFACTS_DIR` (default `build/guest-image`).

```nu
docker buildx build --output type=local,dest=build/guest-rootfs -f scripts/fixtures/guest-rootfs.Dockerfile .
just guest
just VM_MATERIALS_DIR=/absolute/path/to/materials GUEST_ROOTFS_DIR=build/guest-rootfs GUEST_IMAGE_ARTIFACTS_DIR=/dev/shm/haven-guest-image guest-image-e2e
```

The invoking user needs access to `/dev/kvm`, namespaces/mappings, e2fsprogs and the [native image tools](../docs/image-materialization.md#validation) on an administrator-controlled PATH. Artifact storage must report the same device identity through `stat` and `/proc/PID/maps`. The test uses the Haven importer and boots the VMM directly. The combined run currently uses one identity for both steps. VM launch requires a nonroot caller, while the merged OCI importer requires effective UID 0 to preserve ownership. This blocks the merged boot scenario for an ordinary caller. The rootfs regression below exercises import and extraction independently.

### Host network policy and cgroup limits

`just host-policy-e2e` needs TUN, iproute2, nftables, util-linux and a user systemd manager delegating cpu/memory/io/pids. Set `HOST_POLICY_ARTIFACTS_DIR` (default `build/host-policy`) to block-backed storage so the I/O probe can observe device accounting. The runner creates and cleans up its own transient service and namespaces.

### Haven daemon and VM lifecycle

`just runtime-e2e` takes `VM_MATERIALS_DIR`, a bootable `RUNTIME_OCI_LAYOUT` and `RUNTIME_ARTIFACTS_DIR` (default `build/runtime`). Supply `SANDBOX_RUNTIME_IO_MAX` with the verified block-device budget. This runner also needs usable KVM/TUN and network tools, bindfs, `/dev/fuse`, subordinate UID/GID mappings, delegated cgroup controllers, procps, tar and gzip. It resolves PATH before masking `/run`, starts one Haven, imports through the management UDS, compares management and business store IDs, kills and restarts the daemon, and checks the live VMM and guest state.

### Caller-provisioned private deployment

`just privileged-e2e` takes `SANDBOX_E2E_ENDPOINT`, `SANDBOX_E2E_TOKEN_FILE`, `SANDBOX_E2E_IMAGE_DIGEST` and `SANDBOX_E2E_STATE_DIR`. Supply the exact `SANDBOX_E2E_HYPERVISOR`, `SANDBOX_E2E_BUILD_RECEIPT`, `SANDBOX_E2E_CGROUP_ROOT`, `SANDBOX_E2E_IO_MAX`, `SANDBOX_E2E_VMM_UID` and `SANDBOX_E2E_VMM_GID` for read-only VM inspection. `SANDBOX_E2E_TENANT` selects the owner for root HTTP requests. Lifecycle operations use the running daemon.

## Runner regressions without VMs

These entrypoint tests use a closed PATH without a package manager, synthetic ELFs checked by real binutils and recorded Go/service invocations. The native gate regression also checks that the installed `just` forwards Go JSON through a private recipe.

```nu
python3 -I scripts/test_runner_environment.py
python3 -I scripts/test_native_gate.py
```

The rootfs regression imports and extracts the generated OCI layer through both materializers, including absolute guest symlinks and boot overlays. It needs native image tools and ownership support.

```nu
with-env {GOWORK: "off", SANDBOX_REQUIRE_EROFS_TESTS: "1"} {
    go -C haven test -tags=guest_image ./integration -run '^TestGuestRootfsOCI$' -count=1
}
```
