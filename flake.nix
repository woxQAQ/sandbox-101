{
  description = "Single-node microVM engine, sandbox service, and static Rust guest";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/319e4cebef10cbb636c6ea392137b52ad105336a";
  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      eachSystem = nixpkgs.lib.genAttrs systems;
    in {
      devShells = eachSystem (system:
        let pkgs = import nixpkgs { inherit system; };
        in { default = pkgs.mkShell {
          packages = with pkgs; [ go rustc cargo rustfmt clippy protobuf gnumake ripgrep
            erofs-utils e2fsprogs umoci skopeo bubblewrap iproute2 nftables cpio binutils ];
        }; });
      packages = eachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
          cross = if system == "x86_64-linux" then pkgs.pkgsCross.musl64 else pkgs.pkgsCross.aarch64-multiplatform-musl;
        in { guest-static = cross.pkgsStatic.rustPlatform.buildRustPackage {
          pname = "sandbox-guest";
          version = "0.1.0";
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = path: type:
              let relative = pkgs.lib.removePrefix (toString ./. + "/") (toString path);
              in toString path == toString ./.
                || builtins.elem relative [ "Cargo.toml" "Cargo.lock" "guest" "protocol" "protocol/sandbox.proto" ]
                || pkgs.lib.hasPrefix "guest/" relative;
          };
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [ pkgs.protobuf pkgs.binutils ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          postInstall = ''
            for executable in sandbox-agent sandbox-init; do
              if readelf -l "$out/bin/$executable" | grep -q INTERP; then
                echo "guest executable has a dynamic interpreter: $executable" >&2
                exit 1
              fi
              if readelf -d "$out/bin/$executable" | grep -q NEEDED; then
                echo "guest executable has dynamic dependencies: $executable" >&2
                exit 1
              fi
            done
          '';
          cargoBuildFlags = [ "-p" "sandbox-agent" "-p" "sandbox-init" ];
          doCheck = false; # Host-side tests run separately; this package is cross compiled.
        }; });
    };
}
