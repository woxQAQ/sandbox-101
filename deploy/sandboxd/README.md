# sandboxd deployment

Provision and validate [the engine host](../host/README.md) first. `sandboxd` owns sandboxd metadata and orchestrates the engine through its private UDS. It must reach the engine control socket and create host proxy sockets beside each VM's vsock socket. The sample runs as root for this access; it does not isolate the sandboxd process from the engine's control authority.

## First start

Build `make build`, install `bin/sandboxd` and `bin/sandboxctl` under `/usr/local/bin`, and create private configuration/state:

```sh
install -d -m 0700 /etc/sandboxd /var/lib/sandboxd
umask 077
openssl rand -hex 32 > /etc/sandboxd/root-token
install -m 0600 deploy/sandboxd/sandboxd.example.yaml /etc/sandboxd/config.yaml
sandboxd --config=/etc/sandboxd/config.yaml --init
```

Run `--init` only against fresh sandboxd state. Stop the initial process cleanly before installing/enabling `sandboxd.service`; the service deliberately omits initialization. Its engine state and runtime paths must match your actual deployment. The example allows writes to engine state because CH's guest-to-host proxy listeners live beside VM vsock sockets. Review these paths when changing the engine layout.

Review [sandboxd.example.yaml](sandboxd.example.yaml) before starting. Both the daemon and CLI use Viper: explicit flags override environment variables, then configuration file values, then defaults. Daemon environment names use `SANDBOXD_` and CLI names use `SANDBOXCTL_`, with hyphens replaced by underscores. For example, `SANDBOXD_LISTEN` overrides `listen` in the daemon file. `--config FILE` selects a file explicitly; the existing individual flags remain supported. Unknown keys and invalid typed values fail startup. Keep token contents in private files and configure only their paths.

Use a TLS gateway preserving HTTP/1.1 `Connection: Upgrade` / `Upgrade: sandbox-v1`, streaming and long-lived connections. Alternatively pass `--tls-cert` and `--tls-key` together. The daemon defaults to loopback. Root tokens require at least 32 characters. Keep root, tenant and sandbox tokens private; use separate tenant tokens for ordinary work. `GET /healthz` is public; `/metrics` requires root authentication. The [API guide](../../sandboxd/README.md) includes tenant/template/claim commands.

## Image and network configuration

Import a native Linux OCI manifest into the engine before registering its digest in a template. `engined --mode=import` is offline and shares the engine writer lock. See [materialization](../../engine/IMAGE-MATERIALIZATION.md) for layered versus explicit merged compatibility. Sandbox templates freeze the selected image, resource shape and golden snapshot dependency at admission. Tenant snapshot imports require trusted provenance for that tenant. Import external golden snapshots with a root `template_put` and explicit `X-Tenant-ID`; an existing owner cannot be transferred. Metadata schema 2 persists capture owners and root import grants, including in backups. Schema 1 is rejected before DDL: stop the service, preserve a consistent backup and review resource ownership before designing an explicit migration. Do not overwrite or delete old state to bypass this check.

Network modes:

- `none`: no host egress proxy, NIC blocked.
- `relay`: guarded HTTP/HTTPS and SOCKS5 over vsock, NIC blocked. Hostname and address guards apply before numeric dialing. With host-only credentials, use HTTP/HTTPS; opaque SOCKS is rejected.
- `direct`: instance-bound IPv4 CIDR policy and tracked-flow budget installed through engine operations, then guest activation and NIC enable. AllowedHosts and host-only credentials are rejected before VM allocation. IPv6 and private/reserved/metadata destinations are denied.

Host-only credentials are accepted on claim/configure and retained only in process memory. Guest receives public CA material and configured placeholders. Restart loses secrets; affected sandboxes remain blocked and need explicit `configure` with fresh credentials. Existing non-secret relay bindings are recreated with the new proxy CA before admission, preserving guest identity and the current lease. Local audit persistence is required before injection; persistence failure rejects the request without injecting or dialing upstream.

## Restart, drain and backup

`POST /v1/drain` with the root token stops new claims/refills while retaining existing sandboxes. Graceful shutdown drains admission, closes byte streams and proxy bindings, and releases the sandboxd writer lock. It does not delete VM/snapshot resources. Engine and VMM lifetime remain independent. Restart with the same sandboxd database and node key; changing the node key invalidates derived sandbox tokens and Preview signatures.

The node key, SQLite store, audit rows and sanitized operation records are under the private state directory. Guest-visible environment values are persisted so they can be restored; host-only secret values are not. Treat backups as confidential metadata.

Take a consistent sandboxd backup with the daemon stopped:

```sh
sandboxd --mode=backup --state-dir=/var/lib/sandboxd --backup=/safe/sandboxd-point-001
```

The destination must be new. It contains `sandboxd.db`, `node.key` and a checksummed `backup.json` receipt. This does not copy engine disks, snapshots, images or memory. Coordinate engine metadata/resource backups separately.

For recovery, preserve the entire damaged sandboxd state as evidence. Restore into a new destination directory:

```sh
sandboxd --mode=restore-backup --state-dir=/var/lib/sandboxd-restored \
  --backup=/safe/sandboxd-point-001
```

A durable `RECOVERY_REQUIRED` fence blocks startup. Compare the backup point with the current engine inventory, operation keys, instances and live resource ownership. Resolve unknown effects without replaying old guest commands. Then acknowledge the actual review:

```sh
sandboxd --mode=recover --state-dir=/var/lib/sandboxd-restored \
  --recovery-note='describe reviewed resources and resolved discrepancies'
```

Point the service at the reviewed directory. Acknowledgment fails old pending work rather than replaying rolled-back history. Compensating resource cleanup has separate durable intents; original client failures stay terminal. Unknown identities/query failures preserve resources for diagnosis. Local HMAC audit chains cannot detect rollback of the complete state and its history; external anchoring is not implemented.

## Real microVM acceptance

Use an isolated provisioned host and a tenant token with quota for the suite. Import the exact image, run both daemons, then set:

```sh
export SANDBOX_E2E_ENDPOINT=https://YOUR_SANDBOX_ENDPOINT
export SANDBOX_E2E_TOKEN_FILE=/private/test-tenant-token
export SANDBOX_E2E_IMAGE_DIGEST=sha256:YOUR_IMPORTED_MANIFEST_HEX
export SANDBOX_E2E_ENGINE_SOCKET=/run/sandbox-engine/engine.sock
make privileged-e2e
```

Replace the endpoint and digest placeholders. The endpoint certificate must be trusted. The test creates a private template and sandboxes, exercises cold boot/checkpoint/fork/hibernate/concurrent auto-resume/rollback/paused deletion and attempts bounded cleanup. Missing configuration fails; compiling this suite does not establish acceptance.

A separate `make production-e2e` runner builds both daemons, imports a supplied bootable OCI layout, then uses the production launcher inside fresh private namespaces and a delegated transient user service. It requires `KVM_MATERIALS_DIR`, `PRODUCTION_OCI_LAYOUT` and `SANDBOX_PRODUCTION_IO_MAX`; see the [reproduction commands](../ACCEPTANCE.md#reproduce). It retains namespace/privilege, cgroup and console evidence and exercises live daemon-failure adoption before the lifecycle suite. It serves bounded tmpfs through a private bindfs view to separate storage-page charges from VMM memory, without changing production per-VM limits. Exact guest shape and quota evidence are retained; the external-host lifecycle suite defaults to 512 MiB.

Standalone `make kvm-e2e` has passed real pinned-CH boot, guest EROFS/OverlayFS views and tree transfer, boot-time guest-root raw write rejection for readonly block devices, private COW mapping proof, independent source/clone disks, source-capture deletion and COW resnapshot recovery for both materializations. `make kernel-isolation-e2e` has passed actual nft allow/deny/shared-flow-budget/revocation/stale-instance/tamper checks and delegated cgroup PID/CPU/memory enforcement and actual block-I/O throttling. These suites complement the Rust transport integration and actual daemon/CLI control-plane smoke.

The isolated production runner has passed the complete original 512 MiB lifecycle assertions through both daemons, actual privileged launcher/adoption, template_build, golden/warm provisioning and real guest/TAP policy+SNAT TCP after fork/resume. The service and owned cgroups were cleaned with no VMM OOM. See [the acceptance record](../ACCEPTANCE.md) for exact materials, limits and evidence. Qualify each deployment's storage budgets, host firewall/MTU integration, concurrent capacity and teardown behavior with its actual configuration; the isolated runner has no external uplink.
