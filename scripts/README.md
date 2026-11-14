# Integration tests

The normal build, transport integration and daemon smoke commands are described
in the [project README](../README.md#build-and-local-verification). The additional
tools and inputs below are needed only by the relevant integration tests, not by
every build or deployment. Runners use prepared tools and artifacts; they do not
install a toolchain or build Cloud Hypervisor or a kernel.

`just integration` builds the real Rust Unix fixture and runs `haven/integration/`
with the explicit `integration` build tag. The fixture supplies only owned host,
System and Admin effects; all control intents, witnesses and accounting use Haven's
real SQLite store. Missing `SANDBOX_AGENT_FIXTURE` is a failure in that tagged
entrypoint. `just smoke` starts one real Haven and both protocol CLIs, including
writer exclusion, dry-run, restart and unified offline backup. Neither boots a VM.
The VM/production runners below use the Haven workspace module and explicit Loom
packages; historical outcomes do not establish current Haven production acceptance.

## VM test inputs

`haven/kvm_integration_test.go` covers the actual VMM/guest and COW behavior,
not the privileged launcher or daemon lifecycle. The separate `kernel_isolation`
tests under `loom/host/` cover real host networking and cgroup enforcement without a VM; production
checks exercise the launcher and single Haven daemon. These are distinct evidence
boundaries, not interchangeable levels of a test. Runner names describe their
checked behavior; artifact roots default to `build/kvm`,
`build/kernel-isolation` and `build/runtime`.

The standalone KVM test currently requires x86_64 Linux. Supply a dedicated,
writable `KVM_MATERIALS_DIR` with these files:

| File | Required input |
|---|---|
| `cloud-hypervisor` | Static x86_64 ELF matching the [deployment revision](../deploy/host/cloud-hypervisor-revision), built with `kvm,io_uring` and compiled-in seccomp |
| `bzImage` | The x86_64 Linux boot image from the chosen guest kernel build |
| `vmlinux` | That same kernel's uncompressed static ELF, retaining symbols |
| `kernel.config` | That build's config, satisfying [the built-in driver requirements](../image/kernel.config) |
| `kernel-source.json` | Source identity with nonempty `version` and `source_hash` strings |

Keep each kernel build's files and source provenance together. The existing
input checker verifies architecture/static linkage, the CH version pin, kernel
config and symbols, and the Linux boot header. It records artifact hashes in
`materials.json` and writes the `cloud-hypervisor.json` receipt:

```nu
bash scripts/prepare-vm-materials.sh /absolute/path/to/materials
```

This checks the supplied artifacts, not their source-to-binary provenance or
ability to boot. It neither downloads nor builds them. Production provisioning
is documented separately in [host deployment](../deploy/host/README.md).

### Guest userspace fixture

`KVM_ROOTFS_DIR` is a complete guest userspace directory, not the host root or a
list of package prefixes. It must contain genuine systemd at
`/usr/lib/systemd/systemd`, vendor units, `/bin/sh`, and
`/bin/{cat,stat,id,readlink,sync,dd,getfattr,getfacl}`, with all interpreters,
libraries, helper files and symlink targets needed at their guest-visible paths.
Absolute symlinks refer to this guest root, never to the host.

The test copies the tree with root ownership and normalized timestamps, then
overlays its kernel, static agent/initramfs, accounts and systemd units. Leave
`/boot`, `/etc` and `/usr/local/libexec` as real directories when present. Do not
include devices, sockets, credentials or live runtime state.

The optional [userspace fixture Dockerfile](fixtures/kvm-rootfs.Dockerfile) can
prepare this input in a container build. It is not the production guest image:

```nu
docker buildx build --output type=local,dest=build/kvm-rootfs -f scripts/fixtures/kvm-rootfs.Dockerfile .
just guest
just KVM_MATERIALS_DIR=/absolute/path/to/materials KVM_ROOTFS_DIR=/absolute/path/to/rootfs KVM_ARTIFACTS_DIR=/dev/shm/haven-kvm kvm-e2e
```

## Host capabilities

Native image tests need the tools and namespaces described in
[image materialization](../docs/image-materialization.md#validation), plus
e2fsprogs for KVM tests. Use an administrator-controlled PATH. Alternate tool
installations declare their readonly dependency roots using
[`SANDBOX_IMAGE_TOOL_ROOTS`](../docs/image-materialization.md#host-tool-installation).

KVM tests need user-authorized `/dev/kvm`, usable namespaces/mappings and storage
with matching `stat` and `/proc/PID/maps` device identity. Kernel/production tests
additionally need TUN, systemd/systemctl/systemd-run, iproute2, nftables,
util-linux (including sysctl), procps, tar and gzip. Production tests also need
bindfs, subordinate UID/GID mappings, `/dev/fuse`, a user systemd manager
delegating cpu/memory/io/pids, and the explicit block-device I/O budget described
in host deployment. Use `just runtime-e2e` with `RUNTIME_OCI_LAYOUT`,
`RUNTIME_ARTIFACTS_DIR` and `SANDBOX_RUNTIME_IO_MAX` for the full daemon lifecycle.
The runner resolves PATH before masking `/run`
and forwards the configured image-tool roots into its transient service.

Missing prerequisites fail in required runs. Logs and hashes identify the source
and inputs used for each result; [historical results](../docs/acceptance.md) do
not validate later changes.

## Isolated provisioned-host lifecycle

`just privileged-e2e` runs the same real lifecycle against an explicitly prepared
private Haven deployment. Supply `SANDBOX_E2E_ENDPOINT`, `SANDBOX_E2E_TOKEN_FILE`,
`SANDBOX_E2E_IMAGE_DIGEST` and `SANDBOX_E2E_STATE_DIR`. Read-only VM observations
use that store plus the real inspector; also supply the exact
`SANDBOX_E2E_HYPERVISOR`, `SANDBOX_E2E_BUILD_RECEIPT`,
`SANDBOX_E2E_CGROUP_ROOT`, `SANDBOX_E2E_IO_MAX`, `SANDBOX_E2E_VMM_UID` and
`SANDBOX_E2E_VMM_GID`. `SANDBOX_E2E_TENANT` selects the owner for root HTTP
requests. No second control socket or control store is opened.

The KVM test remains in `haven/` to reuse the native OCI fixtures and
exercise the real durable importer without exporting test-only control hooks.
Its `kvm` tag and all guest/COW assertions remain intact. Privileged production
scenarios live in `haven/integration/`; their inventory helper is read-only and
has no lifecycle or admission methods. The production runner starts one Haven,
imports through management UDS, compares management/business store IDs, kills
and restarts that daemon, and verifies the exact live VMM plus guest state.

## Runner regressions without VMs

These entrypoint tests use a closed PATH without a package manager, synthetic
ELFs checked by real binutils, and recorded Go/service invocations. They do not
start systemd services or VMs:

```nu
python3 -I scripts/test_runner_environment.py
```

The rootfs regression imports and extracts the generated OCI layer through both
actual materializers, including absolute guest symlinks and boot overlays. It
needs native image tools and namespaces/mappings, but no VM:

```nu
with-env {GOWORK: "off", SANDBOX_REQUIRE_EROFS_TESTS: "1"} {
    go -C haven test -tags=kvm . -run '^TestKVMRootfsOCI$' -count=1
}
```
