# Host deployment

Use a dedicated Linux KVM host. These files are examples for reviewed host provisioning, not an unattended installer. The daemon uses the real Cloud Hypervisor backend and fails lifecycle operations when prerequisites are unavailable. The API and recovery contract are in [engine integration](../../engine/INTEGRATION.md); sandboxd deployment is in [sandboxd](../sandboxd/README.md).

## Hypervisor, tools and device identity

The supported Cloud Hypervisor revision is exactly `a02b114bd42c65b73033231a27bd23f3c487fc4c`. The launcher requires seccomp and a static binary. Provision a matching artifact and its JSON receipt from a trusted supplier. Building CH here is not a deployment or acceptance prerequisite. If no matching artifact is available and a source build is explicitly desired, the optional builder uses a fresh nonexistent directory separate from engine state:

```sh
bash deploy/host/build-cloud-hypervisor.sh /build/ch-engine-pinned /usr/local/libexec/sandbox
GOWORK=off go -C engine build -o /tmp/engined ./cmd/engined
install -m 0755 /tmp/engined /usr/local/bin/engined
```

The script builds x86_64 musl with `kvm,io_uring`; seccomp is built in at this revision. Other architectures require their own static toolchain and privileged validation. Keep the binary and JSON receipt writable only by a trusted administrator. The engine verifies source revision, feature identity, binary SHA256 and absence of dynamic dependencies. The receipt is deployment trust metadata, not an independent build attestation.

Runtime requires root, KVM API version 12, `/dev/net/tun`, pidfd, CLONE_INTO_CGROUP, mount/PID/IPC/UTS/network namespaces, pivot_root, seccomp, cgroup v2, iproute2, nftables, e2fsprogs and sysctl. Image import uses erofs-utils >=1.9.4 and <2 plus bubblewrap; explicit merged OCI compatibility also uses umoci. See [image materialization](../../engine/IMAGE-MATERIALIZATION.md) for exact tool and namespace requirements. System tool directories and explicitly configured `SANDBOX_IMAGE_TOOL_ROOTS` are bound read-only; see [host tool installation](../../engine/IMAGE-MATERIALIZATION.md#host-tool-installation). Configure an administrator-controlled PATH for host helpers and provide any extra readonly dependency roots in the engined service environment.

The daemon runs as root for setup. Each VMM runs with a nonzero configured UID/GID, no supplementary groups or capabilities, no_new_privs and seccomp, inside a restricted private root. The launcher execs CH in place; no privileged supervisor remains. Cold boot creates the VM without a NIC, then passes the owned TAP descriptor through preboot `vm.add-net` with SCM_RIGHTS: this pinned CH ignores network descriptors on `vm.create`. The engine sets MTU 1500 before dropping privileges, so CH does not need a privileged MTU ioctl. Replay inspects the existing net0 before attaching another device. Do not add a parent-death signal or put VMMs in the daemon service cgroup.

Set `--vmm-gid` to the KVM group, or provision narrowly scoped persistent device ACLs for the VMM UID. A root:kvm 0660 device usually cannot be opened by the default GID 65534. Preflight now launches a short-lived helper with the exact configured UID/GID and an empty supplementary group set; it opens KVM/TUN and checks the KVM API before VM allocation. Device access failure rejects preparation. Do not solve device access by running the VMM as root.

## Durable storage and resource limits

Initialize private root-owned state explicitly. Never use `--init` to replace damaged metadata over retained resources.

The state filesystem must provide reliable file/directory fsync, sparse files and hardlinks. XFS or btrfs reflinks reduce writable-disk copy and resnapshot work; the fallback is an independent sparse copy. Keep snapshot and per-VM immutable memory links on one filesystem. Never truncate or edit published memory. Writable disks are always independent inodes.

Capture roots and operation parents remain root-owned 0711; only the `ch` leaf is writable by the VMM UID/GID. Do not recursively chown capture trees to the VMM. The engine copies/reflinks CH files into new root-private publication inodes before sealing and atomic publication, so a retained VMM source descriptor cannot alter a published snapshot. Admission includes two full RAM captures, one disk, 160 MiB for bounded CH JSON in both domains and the existing 64 MiB metadata margin. Raw CH output may remain until its source readers exit; provision capacity for this conservative reservation even when reflink succeeds. Legacy in-progress capture journals without directory identities require manual recovery and retain their evidence. The layout, identity checks and durable steps are specified in [capture publication](../../engine/INTEGRATION.md#snapshot-and-restore-persistence).

COW restore validates saved static memory topology and then proves complete writable-private mappings of the backing device/inode in the verified VMM's `/proc/PID/maps` before resume. Use a filesystem with consistent stat/mapping identity. The development Btrfs subvolume returned different device IDs between stat and proc mappings and was rejected. Standalone acceptance passes on tmpfs, where those identities agree. Btrfs COW restores remain unsupported under this strict identity check; its reflink support alone does not establish restore compatibility. Compatibility includes the ordered fsmeta/data/upper block layout (`layout-v2`).

Provision an empty cgroup v2 parent such as `/sys/fs/cgroup/sandbox-vms` through the host cgroup manager. It must survive service restarts, with `cpu memory io pids` enabled in `cgroup.subtree_control` and delegated by ancestors. Do not remove populated VM children when engined stops. The engine creates per-instance children and spawns atomically into them.

Required files include `cpu.max`, `cpu.max.burst`, `cpu.weight`, `memory.max`, `memory.swap.max`, `pids.max` and `io.max`. Per instance the engine applies RAM plus 128 MiB overhead, no swap, 256 tasks, one CPU quota per requested vCPU, a 25 ms burst and weight 100. Restore loading temporarily has unlimited CPU quota; runtime quota is armed before resume. Configure aggregate host limits separately.

Create `/etc/sandbox-engine/engine.env` with the actual device budget and nonzero VMM identity:

```text
ENGINE_IO_MAX="259:0 rbps=268435456 wbps=134217728 riops=20000 wiops=10000"
ENGINE_VMM_UID=65534
ENGINE_VMM_GID=REPLACE_WITH_KVM_GROUP_ID
```

Replace the device major:minor and group placeholder. Device-mapper and multi-device filesystems need storage-specific accounting. Accepted io.max syntax does not prove that the intended backing I/O is limited.

SQLite reservations are conservative admission accounting. Provision hard storage/project and inode quotas plus a safety reserve over engine state. Host tc/packet/allocation limits remain deployment responsibilities. The engine direct-policy connection budget counts tracked flows; it does not replace host bandwidth and conntrack allocation controls. After applying those controls, create root-owned `/etc/sandbox-engine/host-policy.json`:

```json
{"version":1,"storage_quota":true,"network_quotas":true}
```

This receipt acknowledges external provisioning; it does not fully inspect or attest those controls.

## Network ownership and direct policy

Reserve nonoverlapping engine ranges:

- `192.168.127.0/30`: guest `.2`, gateway `.1`, reused in separate namespaces.
- `10.200.0.0/17`, slots 1–32767: host-visible guest mappings.
- `10.201.0.0/16`: host veth /31 links.

Each VM owns namespace `sbe-<durable-token>`, host veth `sb<five-digit-slot>h`, namespace peer `uplink`, and TAP `tap0`. Receipts retain namespace token/inode; link aliases prove ownership. Namespace NAT maps the shared guest address to an independent host-visible identity. VMM-local IP input/output is denied except loopback. IPv6 forwarding is denied and namespace IPv6 is disabled.

Review and load `sandbox-engine.nft`, enable host IPv4 forwarding, and integrate chain priorities with the host firewall. The example owns only `inet sandbox_engine`; it never flushes other rules. Preserve its chains, static sets, `active_ifaces` and `active_ips` while VMMs remain alive.

Links default down. To enable direct egress, submit `network(false)`, then `network_policy` with exact `ExpectedInstance`, then `network(true)`. Version 1 accepts canonical IPv4 CIDRs and an explicit flow budget, rejects hostname policies, and always denies private/reserved/metadata addresses. An empty allowlist denies all; explicit `0.0.0.0/0` permits public destinations under that reserved deny. Policy replacement requires actual and desired network off. Nil policy installs deny-all and revokes authorization.

The engine atomically replaces its namespace policy table while retaining NAT, fsyncs a receipt binding policy/instance/namespace and stateless nft rules, and verifies actual rules before enabling/adopting an enabled VM. New cold/restore/clone instances have no inherited authorization. Reconcile attempts to shut down unverified interfaces and never reinstalls policy or re-enables links automatically. See [the exact policy contract](../../engine/api/NETWORK_POLICY.md), including tracked-flow lifetime and replacement semantics. Sandbox `none` and `relay` leave the NIC blocked; relay traffic uses vsock.

Network inspection checks namespace inode/token, ownership aliases, veth pairing, UP state, and installed policy rules. Complete validation of every address/route/MAC/MTU remains pending. Partial creation without a receipt preserves unknown resources for diagnosis. Teardown records intent and removes the namespace last, rejecting replaced namespaces/reused links before deletion.

## First start and restart

The daemon loads configuration with Viper. Install and review [engined.example.yaml](engined.example.yaml) as `/etc/sandbox-engine/config.yaml`; the sample service passes this path with `--config`. Keys match the existing flag names. Explicit flags override `ENGINE_*` environment variables, which override the file and then defaults. Hyphens become underscores in environment names, so the service's existing `ENGINE_IO_MAX`, `ENGINE_VMM_UID` and `ENGINE_VMM_GID` settings apply directly. Unknown keys and invalid typed values reject startup. Configuration is read once; initialization and recovery remain explicit operations.

The sample `sandbox-engine.service` requires initialized state, installed binaries, host policy, cgroup parent, firewall and environment. `KillMode=process` and `SendSIGKILL=no` keep separately placed VMMs alive across daemon exit; review parent slice cleanup as well.

Initialize manually with the production flags, then stop cleanly before enabling the service:

```sh
engined --init --state-dir=/var/lib/sandbox-engine \
  --socket=/run/sandbox-engine/engine.sock \
  --cgroup-root=/sys/fs/cgroup/sandbox-vms \
  --vmm-gid=REPLACE_WITH_KVM_GROUP_ID \
  --io-max='259:0 rbps=268435456 wbps=134217728 riops=20000 wiops=10000'
```

Replace placeholders first and use the same identity/budgets in the service. CLI import and backup are offline commands sharing the writer flock. The private 0600 UDS grants control over every engine VM; arrange sandboxd service access explicitly.

On restart retain state, mounts, cgroups, namespaces, binary and nft sets. The engine compares PID/starttime/boot ID/executable/cgroup, reattaches pidfds and inspects CH before replay. A host reboot removes processes and kernel anchors; reconstruction of every lost anchor is not automatic. Preserve evidence before replacements.

## Recovery and acceptance

Follow [engine recovery](../../engine/INTEGRATION.md#backup-and-explicit-recovery). Keep DB/WAL/SHM together as evidence, retain absolute resource paths, and restore only through the explicit recovery fence. Backups contain metadata; resource bytes require separate coordinated backup. Review an old backup against real processes, instances and resources before acknowledgment. Local HEAD detects some DB-only rollbacks, not rollback of the entire store and its history.

Standalone acceptance has built the exact pinned static CH and matching kernel, booted both EROFS materializations through the actual static init and systemd/agent, and verified guest file/tree semantics, boot-time root raw write rejection for every readonly root/data block device, private COW mappings, independent writable disks, source-capture deletion and COW resnapshot recovery. `make kvm-e2e` runs these checks as a device-authorized nonroot user; see [runtime acceptance](../../README.md#runtime-acceptance). Use storage with matching stat/mapping identity and sufficient free space for conservative import reservations and retained captures.

`make capture-isolation-e2e` runs four real filesystem isolation scenarios in fresh user/mount/PID namespaces with separate mapped root and writer identities. It verifies protected parents, symlink/destination rejection, independent publication despite retained source write FDs, and bounded rejection of special/unknown/oversized artifacts. Missing subordinate UID/GID mappings fail the suite; no KVM, cgroups or host-root elevation is needed.

`make kernel-isolation-e2e` runs real nft/NAT/TCP checks in fresh user/network/mount/PID namespaces and cgroup checks in a new transient systemd user service with delegated controllers. The test prepares the production TAP topology, then substitutes a veth for guest packet injection. It verifies allow/deny, shared-flow budget saturation, revocation, stale-instance rejection, tampered-policy shutdown, CLONE_INTO_CGROUP placement, PID exhaustion, CPU throttling, memory OOM enforcement and actual block-I/O throttling. The bounded I/O probe discovers the accounted backing device through io.stat and compares two O_DIRECT writes with fdatasync before and after applying a 1 MiB/s budget. It changes only the newly owned namespace/service subtree and cleans those resources. It needs usable user namespaces, TUN and systemd user delegation; missing prerequisites fail explicitly. It does not exercise the production privileged launcher. Its actual block-I/O result applies to the observed test device; validate the configured budget on each deployment's backing storage.

`make production-e2e` has additionally passed the actual production launcher, pivot_root, UID/GID/capability reduction, KVM/TAP fd handoff and live daemon-failure adoption. Both daemons passed the complete 512 MiB sandbox lifecycle, template_build/golden/warm provisioning and real guest/TAP policy+SNAT TCP for source/fork/resume. This runner uses fresh namespaces, a private bounded bindfs/tmpfs filesystem and its own transient user delegation; see [reproduction and evidence](../ACCEPTANCE.md). On each provisioned host also run `make privileged-e2e` with the `SANDBOX_E2E_*` variables from [sandboxd deployment](../sandboxd/README.md) and qualify the actual storage budget, external firewall/MTU integration, concurrent allocation and partial teardown. [Reference-aware GC](../../engine/INTEGRATION.md#reference-aware-garbage-collection) collects proven unreachable caches, completed owned work and retired resource versions; published images and operation/idempotency history remain retained.
