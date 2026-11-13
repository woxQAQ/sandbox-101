# Direct network policy contract

Haven owns desired policy, activation generation and retirement responsibility. Loom performs explicit `Network`, `InstallNetworkPolicy` and `VerifyNetworkPolicy` effects on the frozen `loom.Resource`. Network, start/stop, capture, restore and deletion share the VM lifecycle guard; long host effects run outside global admission/ownership locks. There is no public engine network operation API.

## Policy and packet limits

The resource policy shape remains:

```json
{"version":1,"allowed_cidrs":["1.1.1.1/32","8.8.8.0/24"],"max_connections":256}
```

Version is 1. The resource policy requires an explicit connection count 1..65535 and at most 256 canonical IPv4 CIDRs. Haven normalizes the business default to 256. Empty/null CIDRs deny all. Explicit `0.0.0.0/0` permits public destinations subject to mandatory reserved denial. Duplicate, noncanonical, IPv6 and wholly private/reserved/metadata prefixes fail. Broader prefixes cannot override denied ranges; this implementation has no private-network exemption.

Mandatory denial covers unspecified/this-network, RFC1918, CGNAT, loopback, link-local/cloud metadata, IETF protocol assignments, documentation, 6to4 relay, benchmarking, multicast, reserved/broadcast space and Azure `168.63.129.16`. Only IPv4 TCP/UDP/ICMP outbound flows and tracked replies are admitted. Reply rules check the original destination under the current policy, so old conntrack does not bypass narrower replacement. IPv6 forwarding and VMM-originated IP sockets are blocked; namespace loopback/vsock are independent.

`allowed_hosts` must be empty in direct mode. Haven rejects hostname constraints and host-only secret/injection before installation; it never silently drops names or resolves them once into CIDRs. Direct does not enforce TLS/HTTP identity or inject L7 credentials. Use relay for those requirements.

The namespace-local nft `ct count` budget is shared across outbound/reply traffic and all destinations. It counts TCP/UDP/ICMP flows under kernel lifetime rules, not requests, FDs or guest sockets. Completed TCP can leave the count before conntrack expiration; close does not promise an immediate free slot. Both directions enforce the same count after replacement. Host deployment separately provides bandwidth, packet-rate and conntrack allocation quotas; forward filtering does not bound allocations before the hook.

## Haven activation and exact retirement

The sequence in `internal/haven/activation.go` is:

1. Verify the original resource/instance and ensure every effective interface is off.
2. Install and verify the frozen policy through Loom while off, recording its exact instance and durable activation/retirement relationship.
3. Configure through Admin 2046, confirm Workload 2048, and establish the binding for that exact settings generation.
4. Verify policy, receipt, current ownership and live/System-ready instance before enabling links.
5. Under the final configuration guard, recheck lease/fence/instance/settings and atomically publish delivery.

`InstallDirect` verifies the policy already installed by that activation and returns teardown bound to its original step, policy and instance. Even partial/unknown installation retains explicit retirement responsibility. The callback never chooses a current successor by VM ID alone.

New cold/restore/clone instances start off with no inherited policy. Installing an identical policy is a verified no-op only while all effective links remain off. Replacement/revocation refuses effective active interfaces; it does not silently shut them down to satisfy the precondition. Nil policy installs deny-all.

Retirement freezes its own immutable input before effects, turns the original instance off, then verifies/revokes its policy. An uncertain query/effect keeps the pending step and fence. Restart retries the same responsibility. A matching prior retirement/replacement or verified original-instance teardown can complete it; stale callback cannot revoke a successor. SQL completion failure retains the old responsibility. Cached terminal results remain history and do not reapply effects.

Configure admission commits its fence and revokes old data/proxy before saving new settings. Any networking/guest/bind/SQL failure retains blocking; automatic rebind does not clear an explicit fence. Final data admission verifies actual policy and current generation, not only last committed metadata.

## Kernel identity and restart

Loom replaces only the owned `inet engine_policy` table in one nft transaction, preserving NAT and other tables. That retained kernel table name is a resource identity, not an independent engine control plane. Its fsynced receipt binds policy, instance, namespace token/inode and a hash of the full stateless ruleset. Hashing excludes allocation handles/tool metainfo and includes rules/hooks/verdicts/address sets/NAT/budgets. Interrupted installation cannot authorize delivery before the matching Haven step commits.

Inspection verifies namespace inode/token, link ownership aliases, veth pairing, effective UP state and actual rules. Missing/mismatched receipt, unverified instance or rules tampering triggers a shutdown attempt and blocks admission. Query error is never trusted absence. Reconciliation can preserve a still-valid live binding; it does not manufacture historical witnesses or authorize stale policies. Full address/route/MAC/MTU inspection remains outside the current proof, as documented in [host deployment](../deploy/host/README.md).

Host root and tools are trusted. There is no continuous kernel watcher that instantly disables links when another administrator edits rules; activation, data admission and reconciliation detect mismatches. Policy never attests external host firewall or allocation quotas.

## Validation boundary

`make kernel-isolation-e2e` exercises real nft/NAT/TCP in fresh user/network/mount/PID namespaces: public allow/deny, mandatory private/metadata denial under `/0`, revocation, stale instance and tampered-rule shutdown. A two-flow budget holds connections to two public targets and rejects a third handshake. The fixture proves target reachability and substitutes a veth for packet injection after real TAP preparation; it does not prove production privileged VMM launch.

Production separately retains real guest virtio/TAP plus policy/SNAT probes for source, fork and resume. Current command outcomes and missing prerequisites are recorded in [Plan 024](../plans/024-haven-loom-single-control-design.md); historical [acceptance](acceptance.md) does not validate the new Haven tree. Host bandwidth/conntrack/storage qualification remains per deployment.
