# Optional Nix entrypoint

The default development shell supplies the tools listed in the
[project README](../README.md#build-and-local-verification). Run the same Make
targets inside or outside the shell; common scripts do not invoke Nix or require
NixOS. There is no separate acceptance shell or automatically prepared VM fixture.

## Development and static guest

```nu
nix develop --command make test check test-race
nix develop --command make lint
let guest_package = (nix build .#guest-static --no-link --print-out-paths | str trim)
nix develop --command make guest $"GUEST_ARTIFACT_DIR=($guest_package)/bin"
```

The default shell supplies the host toolchain and sets
`SANDBOX_IMAGE_TOOL_ROOTS=/nix/store`, declaring the readonly dependencies of its
native tools through the same explicit setting available to other installations.
`guest-static` cross-builds with `pkgsStatic` and rejects dynamic ELF dependencies.
It does not replace the host-side Rust tests.

Integration tests use the same explicit inputs and commands documented in
[scripts/README.md](../scripts/README.md). The shell supplies host tools, not
KVM/TUN authorization, namespace mappings, delegated controllers or storage.
Cloud Hypervisor, the guest kernel and test rootfs are supplied separately;
this flake does not build or select them.
