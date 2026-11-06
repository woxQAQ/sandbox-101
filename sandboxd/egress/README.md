# Host egress portals

`github.com/woxQAQ/sandboxd/egress` implements product architecture section 3 and the host portion of P4/P5. It uses Go `net/http`, `github.com/elazarl/goproxy v1.9.2` for TLS interception, and `github.com/things-go/go-socks5 v0.1.4` for SOCKS5. The parent module owns these dependencies. No engine interfaces or host network devices are changed by this package.

```go
manager, err := egress.New(egress.Options{
    AuditRecord: func(event egress.AuditEvent) error {
        // Return nil only after appending to the host's durable audit stream.
        // The callback must be concurrency-safe and must not reenter Manager.
        return appendAuditEvent(event)
    },
})
if err != nil {
    return err
}

err = manager.Bind(ctx, sandboxID, tenantID, engineVsockPath,
    egress.Policy{
        Mode: "relay",
        AllowedHosts: []string{"api.example.com"},
        MaxConnections: 256,
    },
    map[string]egress.Secret{
        "API_KEY": {
            Value: "Bearer real-host-secret",
            Hosts: []string{"api.example.com"},
            Header: "Authorization",
            Placeholder: "Bearer harmless-placeholder",
        },
    },
)
if err != nil {
    return err
}

// Only the public CA and harmless placeholders go to the guest.
caPEM := manager.CACertPEM()
binding, _ := manager.Binding(sandboxID)
_ = caPEM
_ = binding
// Install CA trust, placeholders and relay configuration through activation.
// Keep the engine NIC disabled in relay mode; then publish the sandbox handle.
```

`Bind` returns only after both Unix listeners exist and have mode `0600`. `Options.PrepareSocket` can grant an independently verified VMM UID/GID ownership before listeners serve requests. The production service verifies the tenant/VM/instance/vsock path, anchors the new socket inode without following symlinks, and changes ownership to the nonroot owner of the verified VMM's own vsock socket. Permissions remain 0600; a failed handoff closes the binding. Standalone managers with no callback retain their creating process's identity. For a CH vsock path `/run/engine/vm/vsock`, guest connections to host vsock port 2049 arrive at `/run/engine/vm/vsock_2049`; port 2050 arrives at `/run/engine/vm/vsock_2050`. There is no additional `CONNECT 2049` handshake on these guest-to-host streams: they carry the HTTP proxy or SOCKS5 protocol directly. The caller must provide an absolute, clean path in a control-plane-owned runtime directory, inaccessible to guests and untrusted host processes. Provision socket access for the CH process under the same trusted host identity.

The `ctx` argument covers installation. Canceling it after successful `Bind` does not revoke the sandbox. Call `Revoke(sandboxID)` when pausing, releasing or replacing a binding; it removes both listeners, cancels outstanding DNS/dials and HTTP requests, and closes all accepted, hijacked and upstream sockets. `Revoke` is idempotent. Call `Close` at daemon shutdown. A second `Bind` for the same sandbox is rejected: explicitly revoke before changing policy or credentials. Sandbox identity comes exclusively from the dedicated socket binding, never guest-supplied proxy authentication.

Modes are explicit and fail closed:

- `none` creates no egress listeners and accepts no secrets.
- `relay` creates HTTP/CONNECT and SOCKS5 listeners and keeps the engine NIC disabled.
- `direct` requires `Options.InstallDirect(ctx, Binding, Policy) (teardown func() error, err error)`. This callback must synchronously install all host/netns policies while the engine NIC is disabled, return teardown even after partial installation, and return success only when policies are effective. `Bind` never enables a NIC. The parent enables it only after successful installation and guest activation. The parent must disable the NIC before revoking direct rules. Secrets in direct mode are rejected because an unconstrained raw NIC path cannot support host-only credential substitution. The callback must implement and validate the actual network policy itself; proxy guards do not inspect direct packets.

By default, existing files and sockets cause binding to fail. Restart reconciliation can set `Options.RecoverStaleSocket func(Binding, string, os.FileInfo) bool`, but must first prove exclusive ownership of that VM runtime directory and that the previous binding is dead. Even with that proof the package refuses regular files, reachable sockets, permission failures and changed inodes; only an unchanged socket with `ECONNREFUSED`/`ENOENT` can be removed. Normal teardown also checks file identity before unlinking, so it preserves replacement sockets. The exclusive-directory requirement is essential to preventing pathname replacement races.

Host and address policy:

- `AllowedHosts` contains exact ASCII DNS names or numeric IPs. Matching is case-insensitive, normalizes a terminal DNS dot, and does not support wildcards, userinfo, IP zones, ambiguous numeric spellings or Unicode names. Supply IDNs as ASCII A-labels.
- Empty `AllowedHosts` permits any destination that passes address checks. Nonempty `AllowedCIDRs` restricts every resolved address to those prefixes; when both lists are present, both constraints must pass.
- The default native resolver checks **every** A/AAAA answer before dialing any address. A mixed public/private answer is rejected entirely. Numeric dialing pins each connection to a checked address and never delegates DNS to the dialer. Upstream keepalive pooling is disabled, so each new request repeats validation.
- Private IPv4/ULA, CGNAT, loopback, link-local, cloud metadata, multicast, unspecified, documentation, benchmark, reserved and known IPv6 transition/NAT64 ranges are blocked. IPv4-mapped addresses are unmapped before checking. Public IPv6 is restricted to global unicast allocation `2000::/3`, with special-purpose ranges excluded.
- Administrators may set `Options.AllowPrivateCIDRs` to permit private unicast/CGNAT addresses explicitly included in `AllowedCIDRs`. Metadata, loopback, link-local, non-unicast and transition prefixes remain blocked. This option is a host administration decision; tenant-supplied CIDRs alone cannot enable private access.
- `Resolver`, `DialContext` and `RootCAs` are trusted host integration/test hooks. The dialer receives only checked numeric addresses and must honor context cancellation. Set `RootCAs` for a private upstream PKI; upstream certificate verification and hostname checking are always enabled.

Credential behavior:

- Secret `Value` is the complete header replacement, including an authentication scheme when required. `Placeholder == ""` overwrites that header on authorized HTTPS requests. A nonempty placeholder replaces matching occurrences in its configured header. Exact host authorization, a checked destination, TLS SNI, CONNECT authority and inner request authority must agree before substitution. An `AuditRecord` or legacy `Audit` callback is required when binding secrets.
- All CONNECT requests use TLS interception and require TLS; a plaintext request inside CONNECT is rejected. Missing DNS-name SNI, mismatched SNI, changed inner Host/port and nested CONNECT are rejected. HTTP/1.1 is negotiated for interception. The CA is generated per manager and kept in host memory; install the new public CA before delivering or restoring a guest. Certificate-pinned clients need compatible trust configuration.
- Placeholders in other headers or on unauthorized/non-HTTPS hosts are rejected. URL/path/query placeholders, including URL-encoded query values, are rejected instead of supporting query credential injection. Headers are inspected before hop-by-hop headers are stripped. Body contents are passed through and never used for credential substitution.
- `Options.AuditRecord func(AuditEvent) error` is the preferred audit sink. Before changing any credential header, the proxy synchronously records the request's `inject` event. A persistence error returns a fixed HTTP 503 response and leaves all credential headers unchanged, with no upstream dial. A later request may proceed after the sink recovers. When configured, `AuditRecord` takes precedence and never falls back to `Audit` on error. The existing `Audit func(AuditEvent)` remains compatible when `AuditRecord` is nil; a normal return must guarantee persistence because it cannot report failures.
- Secrets live only in manager memory. `Secret.Value` is excluded from JSON; ordinary and Go-syntax string formatting redact `Secret` entirely. Errors and library logs do not reveal request data. Audit records contain time, tenant, sandbox, target host, method and a fixed action/reason, with no path, query, headers, secret names or values. The audit callback owns durability and access control.
- Literal secret reflections are redacted in response headers and streamed bodies, including across read boundaries; response trailers are discarded for secret-bearing bindings. Encoded responses are rejected for those bindings so compressed reflections cannot bypass inspection. Ordinary streaming output is released without waiting for an entire credential-sized lookahead. Authorized upstream endpoints remain a trust boundary: an endpoint can intentionally disclose an encoded or transformed credential, which generic proxy filtering cannot prevent.
- SOCKS5 permits TCP CONNECT only and applies the same host/DNS/address guards. UDP ASSOCIATE and BIND are denied. SOCKS5 is denied for bindings containing secrets, because opaque TCP cannot enforce credential/placeholder inspection. Use the HTTP/HTTPS proxy for those sandboxes. HTTP Upgrade and request trailers are also rejected.

`MaxConnections` defaults to 256 and bounds accepted conversations across both portals, including idle handshakes and hijacked connections. Excess sockets are immediately closed. Each conversation can hold one upstream socket; with upstream pooling disabled, the proxy's steady-state socket budget is at most twice the conversation limit plus its listeners. Idle I/O defaults to 90 seconds; DNS/dial/TLS setup defaults to 10 seconds. Normal HTTP header limits and response-header timeouts also apply. SOCKS TCP forwarding preserves half-close.

The tests use actual Unix proxy listeners and local HTTP/TLS/TCP upstreams behind controlled resolvers and numeric dialers, so they need no Internet access at runtime:

```sh
go test -race ./egress
go vet ./egress
```

Coverage includes audit persistence before header mutation, denial without dialing on audit failure, recovery on the next request, legacy audit compatibility, HTTPS substitution and reflection redaction, cross-host and encoded placeholders, verified upstream TLS, SNI/Host mismatches, plaintext CONNECT rejection, special/mapped/NAT64 addresses, mixed DNS answers and rebinding, SOCKS5 guards and half-close, shared connection budgets, revoke during active connections/dials, policy ownership, direct activation gates, stale-socket recovery and replacement-inode protection.
