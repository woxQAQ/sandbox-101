# Direct network policy contract

`network` alone cannot activate direct networking. The engine serializes
`network`, `network_policy`, start/stop, restore, snapshot and deletion using the
same per-VM lifecycle lock. No global lock covers policy installation or launch.

The stable JSON policy is:

```json
{
  "version": 1,
  "allowed_cidrs": ["1.1.1.1/32", "8.8.8.0/24"],
  "max_connections": 256
}
```

`version` must be 1. `max_connections` must be explicitly supplied in 1..65535;
zero is invalid. At most 256 canonical IPv4 CIDRs are supported. Empty/null
`allowed_cidrs` denies every destination. `0.0.0.0/0` permits public destinations
subject to the mandatory deny rules. Duplicate, noncanonical, IPv6 and wholly
private/reserved/metadata prefixes are rejected. Broader prefixes that include
reserved ranges never override the deny rules. There is no host-policy mechanism
for private-network exemptions in this implementation.

The mandatory deny ranges cover unspecified/this-network, RFC1918, CGNAT,
loopback, link-local/cloud metadata, IETF protocol assignments, documentation,
6to4 relay, benchmarking, multicast and reserved/broadcast space, plus Azure's
168.63.129.16 service endpoint. Only IPv4 TCP, UDP and ICMP outbound flows and
tracked replies are allowed. Reply packets are checked against the current
policy's original destination, including after replacement; old conntrack state
cannot bypass a narrower destination policy. IPv6 and VMM-originated IP sockets
are blocked; namespace loopback and vsock are independent of this mechanism.

`allowed_hosts`, if supplied, must be empty. Hostnames cannot be converted to a
CIDR policy by dropping that constraint or resolving them once. Reject hostname
policies (including a combination of hosts and CIDRs), or require the caller to
explicitly choose an address policy. Direct mode does not enforce TLS/HTTP
identity and performs no L7 credential injection. The bridge must reject
host-only secrets/injection before installing direct policy.

The connection budget is a namespace-local nft `ct count` shared across outbound
and reply traffic and all allowed destinations. It counts TCP/UDP/ICMP flows
according to the kernel's `ct count` lifetime rules, rather than application
requests, file descriptors or guest sockets. Completed TCP flows can leave this
count before their conntrack entries expire; closing a socket does not guarantee
an immediate reusable slot. Both directions pass the same budget rule; already
tracked flows are counted as they send or receive packets after policy replacement. Host deployment remains responsible
for bandwidth, packet rate and kernel conntrack allocation quotas: this filter
limits admitted forwarding, not allocations made before the forward hook.

## Sandboxd `Egress.InstallDirect` bridge

Use `Binding.VMID` and the exact `Binding.Instance` captured for the activation.
Use the durable public client and await each operation's terminal result; Submit
alone means acceptance. Suggested requests, using unique persisted keys scoped
to this binding/activation generation:

```go
// 1. Confirm shutdown before installation; this is safe to repeat.
api.Wait(ctx, engine, api.Request{
    Key: bindingKey + ":off", Action: "network", VMID: vmID,
    ExpectedInstance: instance, NetworkEnabled: false,
})

// 2. Install the policy while every effective interface remains down.
api.Wait(ctx, engine, api.Request{
    Key: bindingKey + ":policy", Action: "network_policy", VMID: vmID,
    ExpectedInstance: instance,
    Policy: &api.NetworkPolicy{
        Version: 1, AllowedCIDRs: policy.AllowedCIDRs,
        MaxConnections: maxConnections, // normalize the sandbox default to 256
    },
})
```

`InstallDirect` returns success only after step 2 succeeds. It returns a cleanup
closure even after an uncertain/partial installation. It never enables links.
After guest configuration/activation through Admin port 2046, sandboxd separately awaits:

```go
api.Wait(ctx, engine, api.Request{
    Key: bindingKey + ":on", Action: "network", VMID: vmID,
    ExpectedInstance: instance, NetworkEnabled: true,
})
```

The engine requires a live, verified, agent-ready VM, a committed policy bound to
this instance, a matching namespace ownership receipt, an intact installation
receipt and a matching full stateless namespace nft ruleset before enabling.
Policy installation by a new key with identical parameters is a verified no-op
when the rules still match and every interface is off. Replacing or revoking
while desired network state or any effective interface is on fails `conflict`.
A policy operation never disables an active interface merely to satisfy that
precondition.

## Revocation and retries

The cleanup closure awaits an instance-conditioned `network(false)` using its
own stable cleanup key, then awaits `network_policy` with `Policy:nil` using a
second stable cleanup key. Nil atomically installs deny-all and clears the
committed authorization. Repeating either operation with the same key returns
its original terminal result. A new key reasserts shutdown or deny-all. Neither
operation flushes conntrack or promises that all old NAT state has disappeared.

All network and network_policy requests require a nonempty ExpectedInstance.
Both admission and execution check it while holding the lifecycle lock. Stale
cleanup must not target a fresh instance. A stale-instance `conflict`, confirmed
VM `not_found`, or successful deletion can be treated by sandboxd as cleanup of
that old binding being complete; unrelated conflicts and unknown failures must
remain retryable/diagnosable. A successful cached operation is a historical
result, not a fresh inspection, and never reapplies effects to a newer instance.
Do not reuse binding keys across activation generations, even on the same VM.
If an operation has a terminal failure, retries needing new effects require a
new key after inspecting its outcome. In-flight operations survive caller
cancellation; use FindOperation with the original key after a lost response.

`VM.NetworkPolicy` and `NetworkPolicyInstance` expose the last committed policy;
they are not a claim that kernel rules are currently verified. Cold start,
restore and clone create a fresh instance with networking off and no committed
policy. Policy is not inherited from snapshots. A daemon reopen keeps a live
network only if its persisted intent, instance, receipt and actual rules match.
Unknown inspection, missing/mismatched policy or stale rules triggers a shutdown
attempt and marks the VM unverified. Reconciliation repeats these checks for
active links; it never automatically reinstalls policy or enables links.

## Runtime verification limits

Installation replaces only the engine-owned inet engine_policy table, in one
nft transaction. NAT and other tables are not flushed. Its fsynced receipt binds
the policy, instance, namespace token/inode and a hash of the full stateless
namespace ruleset. Allocation handles/tool metainfo are excluded from that hash;
rules, hooks, verdicts, address sets, NAT and budgets are included. An interrupted
install cannot authorize activation before the engine commits the matching
policy. Reinstalling while off is the explicit repair path.

Host root and its tools are trusted. There is no continuous kernel-side watcher
that shuts links at the instant another privileged administrator edits rules;
engine activation and reconciliation detect mismatches. The existing host
firewall/quota prerequisite still applies, and host policy can further restrict
access. `make kernel-isolation-e2e` has verified actual nft/NAT/TCP behavior in
fresh user/network/mount/PID namespaces: public allow/deny, mandatory private and
metadata denial under `/0`, revocation, stale-instance rejection and shutdown
after ruleset tampering. A two-flow policy admits concurrent connections to two
public destinations and rejects a third handshake while both remain open. The
fixture first proves all targets reachable and substitutes a veth for guest
packet injection after production TAP preparation. It does not exercise the
production privileged VMM launcher or attest host bandwidth/conntrack allocation
quotas; those remain provisioned-host checks.
