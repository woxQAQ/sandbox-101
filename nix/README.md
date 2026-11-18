# Optional Nix entrypoint

The default development shell supplies the tools listed in the
[project README](../README.md#build-and-local-verification). Run the same just
recipes inside or outside the shell; common scripts do not invoke Nix or require
NixOS. Prepare VM materials separately for the tests that boot a guest.

## Development and static guest

```nu
nix develop --command just test check test-race
nix develop --command just lint
let guest_package = (nix build .#guest-static --no-link --print-out-paths | str trim)
nix develop --command just $"GUEST_ARTIFACT_DIR=($guest_package)/bin" guest
```

The default shell supplies the host toolchain through PATH. Image tools run
directly against private work directories; no readonly tool-root projection is needed.
`guest-static` cross-builds with `pkgsStatic` and rejects dynamic ELF dependencies.
It does not replace the host-side Rust tests.

Integration tests use the same explicit inputs and commands documented in
[scripts/README.md](../scripts/README.md). The shell supplies host tools, not
KVM/TUN authorization, namespace mappings, delegated controllers or storage.
Cloud Hypervisor, the guest kernel and test rootfs are supplied separately;
this flake does not build or select them.
