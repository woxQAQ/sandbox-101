SHELL := /bin/sh
ROOT := $(abspath .)
GO_MODULES := engine protocol/gen sandboxd
GO_WORK_PACKAGES := ./engine/... ./operator/... ./protocol/gen/... ./sandboxd/...
GUEST_TARGET ?= x86_64-unknown-linux-musl
GUEST_ARTIFACT_DIR ?=
IMAGE_TAG ?= sandbox-guest:local
KVM_MATERIALS_DIR ?=
KVM_GUEST_DIR ?= $(ROOT)/build/guest
KVM_ARTIFACTS_DIR ?= $(ROOT)/build/kvm-acceptance
KVM_ROOTFS_DIR ?=
KERNEL_ARTIFACTS_DIR ?= $(ROOT)/build/kernel-isolation-acceptance
PRODUCTION_OCI_LAYOUT ?=
PRODUCTION_ARTIFACTS_DIR ?= $(ROOT)/build/production-acceptance

.PHONY: all build test test-race check lint lint-fix fmt proto guest image integration smoke privileged-e2e kvm-e2e kernel-isolation-e2e production-e2e capture-isolation-e2e guest-fs-isolation-e2e
all: build

build:
	mkdir -p bin
	GOWORK=off go -C engine build -o $(ROOT)/bin/engined ./cmd/engined
	GOWORK=off go -C sandboxd build -o $(ROOT)/bin/sandboxd ./cmd/sandboxd
	GOWORK=off go -C sandboxd build -o $(ROOT)/bin/sandboxctl ./cmd/sandboxctl
	cargo build --locked --workspace

test:
	@set -e; for module in $(GO_MODULES); do GOWORK=off go -C $$module test ./... -count=1 -timeout=120s; done
	cargo test --locked --workspace --all-targets

test-race:
	@set -e; for module in $(GO_MODULES); do GOWORK=off go -C $$module test -race ./... -count=1 -timeout=180s; done

check:
	@set -e; for module in $(GO_MODULES); do GOWORK=off go -C $$module vet ./...; done
	cargo fmt --all -- --check
	cargo clippy --locked --workspace --all-targets -- -D warnings

lint:
	go fix $(GO_WORK_PACKAGES)
	golangci-lint run $(GO_WORK_PACKAGES)

lint-fix:
	golangci-lint run --fix $(GO_WORK_PACKAGES)

fmt:
	gofmt -w $$(rg --files engine sandboxd protocol/gen -g '*.go' -g '!sandbox.pb.go' -g '!**/third_party/**')
	cargo fmt --all

proto:
	protoc --go_out=protocol/gen --go_opt=paths=source_relative -I protocol protocol/sandbox.proto
	cargo build --locked -p sandbox-agent

guest:
	@if [ -n "$(GUEST_ARTIFACT_DIR)" ]; then \
		bash scripts/package-guest.sh "$(GUEST_ARTIFACT_DIR)"; \
	else \
		bash scripts/build-guest.sh $(GUEST_TARGET); \
	fi

# Supply a kernel with the built-in drivers in image/kernel.config, and its .config.
image: guest
	bash scripts/check-kernel.sh build/kernel.config
	docker build -f image/Dockerfile -t $(IMAGE_TAG) .

integration:
	cargo build --locked -p sandbox-agent --example unix_fixture
	SANDBOX_AGENT_FIXTURE=$(ROOT)/target/debug/examples/unix_fixture GOWORK=off go -C sandboxd test ./integration -count=1 -timeout=90s

smoke: build
	python3 -I scripts/smoke.py $(ROOT)

privileged-e2e:
	GOWORK=off go -C sandboxd test -tags=privileged ./integration -run TestMicroVM -count=1 -timeout=10m

kvm-e2e:
	KVM_ROOTFS_DIR="$(KVM_ROOTFS_DIR)" bash scripts/run-kvm-acceptance.sh "$(KVM_MATERIALS_DIR)" "$(KVM_GUEST_DIR)" "$(KVM_ARTIFACTS_DIR)"

kernel-isolation-e2e:
	bash scripts/run-kernel-isolation-acceptance.sh "$(KERNEL_ARTIFACTS_DIR)"

guest-fs-isolation-e2e:
	bash scripts/run-guest-fs-isolation.sh

capture-isolation-e2e:
	bash scripts/run-capture-isolation-acceptance.sh

production-e2e:
	bash scripts/run-production-acceptance.sh "$(KVM_MATERIALS_DIR)" "$(PRODUCTION_OCI_LAYOUT)" "$(PRODUCTION_ARTIFACTS_DIR)"
