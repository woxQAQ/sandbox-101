set shell := ["bash", "-euo", "pipefail", "-c"]

root := justfile_directory()
modules := "haven loom sdk types common tools protocol/gen"
packages := "./haven/... ./loom/... ./sdk/... ./types/... ./common/... ./tools/... ./protocol/gen/..."
GUEST_TARGET := env_var_or_default("GUEST_TARGET", "x86_64-unknown-linux-musl")
GUEST_ARTIFACT_DIR := env_var_or_default("GUEST_ARTIFACT_DIR", "")
IMAGE_TAG := env_var_or_default("IMAGE_TAG", "sandbox-guest:local")
KVM_MATERIALS_DIR := env_var_or_default("KVM_MATERIALS_DIR", "")
KVM_GUEST_DIR := env_var_or_default("KVM_GUEST_DIR", root / "build/guest")
KVM_ARTIFACTS_DIR := env_var_or_default("KVM_ARTIFACTS_DIR", root / "build/kvm")
KVM_ROOTFS_DIR := env_var_or_default("KVM_ROOTFS_DIR", "")
KERNEL_ARTIFACTS_DIR := env_var_or_default("KERNEL_ARTIFACTS_DIR", root / "build/kernel-isolation")
RUNTIME_OCI_LAYOUT := env_var_or_default("RUNTIME_OCI_LAYOUT", "")
RUNTIME_ARTIFACTS_DIR := env_var_or_default("RUNTIME_ARTIFACTS_DIR", root / "build/runtime")

all: build

build:
    mkdir -p bin
    GOWORK=off go -C haven build -o '{{root}}/bin/haven' ./cmd/haven
    GOWORK=off go -C sdk build -o '{{root}}/bin/havenctl' ./cmd/havenctl
    GOWORK=off go -C loom build -o '{{root}}/bin/loomctl' ./cmd/loomctl
    cargo build --locked --workspace

test:
    for module in {{modules}}; do GOWORK=off go -C "$module" test ./... -count=1 -timeout=120s; done
    cargo test --locked --workspace --all-targets

test-native:
    @for module in {{modules}}; do SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C "$module" test -json -race ./... -count=1 -timeout=300s; done

test-race:
    for module in {{modules}}; do GOWORK=off go -C "$module" test -race ./... -count=1 -timeout=10m; done

check:
    for module in {{modules}}; do GOWORK=off go -C "$module" vet ./...; done
    cargo fmt --all -- --check
    cargo clippy --locked --workspace --all-targets -- -D warnings

lint:
    go fix {{packages}} ./operator/...
    golangci-lint run {{packages}} ./operator/...

lint-fix:
    golangci-lint run --fix {{packages}} ./operator/...

fmt:
    rg --files "$PWD/haven" "$PWD/loom" "$PWD/sdk" "$PWD/types" "$PWD/common" "$PWD/tools" "$PWD/protocol/gen" -g '*.go' -g '!sandbox.pb.go' -0 | xargs -0 go -C tools tool structfmt -w
    golangci-lint fmt {{packages}}
    cargo fmt --all

proto:
    protoc --go_out=protocol/gen --go_opt=paths=source_relative -I protocol protocol/sandbox.proto
    cargo build --locked -p sandbox-agent

guest:
    if [ -n '{{GUEST_ARTIFACT_DIR}}' ]; then bash scripts/package-guest.sh '{{GUEST_ARTIFACT_DIR}}'; else bash scripts/build-guest.sh '{{GUEST_TARGET}}'; fi

# Supply a kernel with the built-in drivers in image/kernel.config, and its .config.
image: guest
    bash scripts/check-kernel.sh build/kernel.config
    docker build -f image/Dockerfile -t '{{IMAGE_TAG}}' .

integration:
    cargo build --locked -p sandbox-agent --example unix_fixture
    SANDBOX_AGENT_FIXTURE='{{root}}/target/debug/examples/unix_fixture' GOWORK=off go -C haven test -tags=integration ./integration -count=1 -timeout=90s

smoke: build
    python3 -I scripts/smoke.py '{{root}}'

privileged-e2e:
    GOWORK=off go -C haven test -tags=privileged ./integration -run TestMicroVM -count=1 -timeout=10m

kvm-e2e:
    KVM_ROOTFS_DIR='{{KVM_ROOTFS_DIR}}' bash scripts/run-kvm.sh '{{KVM_MATERIALS_DIR}}' '{{KVM_GUEST_DIR}}' '{{KVM_ARTIFACTS_DIR}}'

kernel-isolation-e2e:
    bash scripts/run-kernel-isolation.sh '{{KERNEL_ARTIFACTS_DIR}}'

guest-fs-isolation-e2e:
    bash scripts/run-guest-fs-isolation.sh

capture-isolation-e2e:
    bash scripts/run-capture-isolation.sh

runtime-e2e:
    bash scripts/run-runtime.sh '{{KVM_MATERIALS_DIR}}' '{{RUNTIME_OCI_LAYOUT}}' '{{RUNTIME_ARTIFACTS_DIR}}'
