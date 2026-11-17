# Haven / Loom

中文 · [English](README.md)

单宿主 microVM 引擎与 Sandbox 服务。整体职责见[资源架构](docs/architecture.md)和 [Sandbox 架构](docs/sandbox-architecture.md)，部署与组件契约从[文档索引](docs/README.md)进入。[未完成计划](plans/README.md)与[历史证据](.agents/archive/README.md)分别维护。

Haven 在唯一 SQLite store 中负责租户与资源工作流，包括模板、warm pool、租约、休眠、checkpoint、fork、镜像、VM generation、清理和费用。Loom 接收明确对象，执行 Cloud Hypervisor、Linux 与文件系统效果，没有控制数据库。`havenctl` 使用业务 HTTP；`loomctl` 使用同一 daemon 的私有管理 socket。Rust guest 将 System、Admin 和非特权 Workload RPC 分开；Rust PID 1 挂载 EROFS/ext4/OverlayFS 根文件系统后启动 systemd。

## Workspace

根目录保留 `go.work`，组件各自拥有 Go module。Haven 的 `App` 组装 image、machine、supply、sandbox、data、collection、node 能力；各能力拥有用例与私有 SQL，HTTP/管理 adapter 直接接收所需用例。

| 目录 | 职责 |
|---|---|
| `haven/` | 持久控制服务、HTTP adapters、daemon 配置与 `cmd/haven` |
| `loom/` | 明确宿主效果、管理客户端配置与 `cmd/loomctl` |
| `sdk/` | 业务/管理客户端、guest RPC 与 `cmd/havenctl` |
| `types/` | 共享 wire 数据，以及经过清理的错误 payload |
| `common/` | 共享的有界 CLI 文件读写、配置加载与日志初始化 |
| `tools/` | 固定版本的 structfmt、sqlc 等开发工具 |
| `protocol/gen/` | 生成的 Go guest 协议绑定 |
| `operator/` | Kubernetes 集成 |

模块用本地 `replace` 支持 `GOWORK=off` 独立检查，workspace 用于跨模块开发与 lint。命令行解析放在各自 command 旁；`haven/config` 与 `loom/config` 分别保存配置类型。`common/logging` 只初始化标准 `slog` handler，业务代码直接使用 slog。

## 构建与本地验证

需要 Linux、匹配 module 声明的 Go、Rust/Cargo、protoc 和 just。脚本还需要 Bash、Python >=3.11、GNU coreutils 与 ripgrep；二进制检查需要 file 和 binutils，lint 与格式化需要 golangci-lint。预先准备 PATH，或使用[可选 Nix shell](nix/README.md)。普通构建不初始化、不修改 `references/` 子模块。

```nu
just build
just test check test-race
just lint
just integration
just smoke
```

二进制输出为 `bin/{haven,havenctl,loomctl}`。test、vet 与 race 覆盖 Haven、Loom、SDK、types、common、tools 和 protocol 模块；workspace lint 另包含 operator。`just test-race` 保留每包 10 分钟超时。`just integration` 构建真实 Rust Unix fixture，验证 Haven/SQLite、HTTP 与 guest RPC。`just smoke` 在自建私有状态中启动一个真实 daemon，检查两个 CLI、writer 排他、配置、重启、GC 和离线备份。这两个入口均不启动 VM。

Haven 的版本化 migration SQL 同时供运行时初始化和 sqlc 使用。修改领域 SQL 后运行 `just generate-sql`、`just check-sql`；生成绑定纳入版本控制，普通构建不需要 sqlc。检查在私有临时目录生成，能识别缺失、修改和多余文件，不改写工作树。版本与事务边界见 [store migrations](haven/store/README.md)。架构门禁同时检查实际 import graph 和 Go AST，拒绝 controller 绕过业务服务、根兼容 facade 与手写业务 SQL；负例验证拒绝能力，SQLite 维护例外在 store 契约中逐项列明。

普通测试缺少工具或 ownership 权限时可能跳过 native 项。`just test-native` 强制 EROFS 覆盖，保留 race、count=1、300s 设置，stdout 仅输出 Go test JSON。native gate 还要求 shared/merged 物化与 GC 的实际 pass 事件：

```nu
python3 -I scripts/check-native-gate.py --negative
python3 -I scripts/check-native-gate.py
```

需要版本相同的 mkfs/fsck.erofs >=1.9.4 且 <2、dump.erofs、umoci 0.6.0 与 C 编译器。镜像工具直接执行；完整 merged/元数据验证需要 effective UID 0 和原生 ownership/xattr 支持。gate 不安装工具、不自动提权；negative 通过私有 PATH 隐藏 mkfs.erofs。日志和原始退出码保存在新建的 `build/native-gate/run.*`。详见[镜像验证](docs/image-materialization.md#validation)。

## HTTP 与配置

Haven 提供业务 HTTP 和私有管理 UDS。`--config FILE` 显式选择 YAML/JSON/TOML 文件，key 为平面的连字符名称。优先级为：显式 flag → 对应前缀环境变量 → 文件 → 默认值。前缀为 `HAVEN_`、`HAVENCTL_`、`LOOMCTL_`，对应的 `_CONFIG` 也可选择文件。未知/null key、非法数字边界、不完整 TLS 和已删除 flag 均拒绝。

```nu
haven --config /private/haven.yaml
with-env {HAVEN_LISTEN: "127.0.0.1:8888", HAVEN_LOG_FORMAT: "json"} {
    haven --config /private/haven.yaml
}
havenctl --config /private/havenctl.yaml --key claim-001 --request-file /private/claim.json claim
loomctl --config /private/loomctl.yaml --dry-run gc
```

业务 intent（`tenant`、`claim`、`template`、`change ID`）和通用管理 `request` 必须使用 `--request-file FILE`。文件只接受一个 JSON 值，上限 1 MiB，拒绝未知字段；CLI 不读取 stdin。简单管理操作使用命名 flag。token 和包含 secret 的请求文件应放在私有目录中。daemon 日志写 stderr；`--log-level` 接受 debug/info/warn/error，`--log-format` 接受 text/json。

只有首次启动新的空私有 state 目录时使用 `--init`。完整启动、HTTP、管理与恢复契约见 [Haven control](docs/haven-control.md)。部署示例：[daemon](deploy/host/haven.example.yaml)、[service](deploy/host/haven.service)、[havenctl](deploy/client/havenctl.example.yaml)、[loomctl](deploy/client/loomctl.example.yaml)。

## 静态 Guest 与镜像

```nu
just GUEST_TARGET=x86_64-unknown-linux-musl guest
# 也可以直接封装已准备的静态二进制。
just GUEST_ARTIFACT_DIR=/absolute/path/to/static-binaries guest
```

guest 构建需要 Rust musl target、SQLite 构建依赖、readelf 与 GNU cpio。封装拒绝动态可执行文件，输出 `build/guest/{sandbox-agent,sandbox-init,initramfs}`，详见 [Guest 契约](guest/README.md)。提供 kernel 及满足[内置驱动要求](image/kernel.config)的 `build/kernel.config` 后，`just image` 构建 systemd guest 镜像。导出 OCI layout，再经 `loomctl --key KEY --oci-layout PATH --digest DIGEST import` 导入。

默认 `layered-erofs-v1` 共享不可变 layer payload；显式 `merged-oci-erofs-v1` 走隔离的 umoci 物化。已发布 digest/materialization 身份固定，非法数据不自动回退到另一 profile。

## Runtime 验证

native、跨 UID 或 VM 验证前，先阅读[测试输入与 runner 边界](scripts/README.md)。KVM 需要固定版本的静态 VMM、配套 kernel、完整 systemd guest userspace、封装后的 guest、可用 `/dev/kvm` 与 namespace/mapping。artifact 存储必须让 stat 和 `/proc/PID/maps` 报告相同 device identity。

```nu
bash scripts/prepare-vm-materials.sh /absolute/path/to/materials
just KVM_MATERIALS_DIR=/absolute/path/to/materials KVM_ROOTFS_DIR=/absolute/path/to/rootfs KVM_ARTIFACTS_DIR=/dev/shm/haven-kvm kvm-e2e
just kernel-isolation-e2e
with-env {SANDBOX_RUNTIME_IO_MAX: $env.HAVEN_IO_MAX} {
    just KVM_MATERIALS_DIR=/absolute/path/to/materials RUNTIME_OCI_LAYOUT=/absolute/path/to/oci RUNTIME_ARTIFACTS_DIR=/private/new-runtime runtime-e2e
}
```

kernel 验证还需要 TUN、网络工具和委派了 controller 的 user systemd manager。production 生命周期另需 bindfs、`/dev/fuse`、subordinate UID/GID mapping 及显式块设备 I/O 预算。`just privileged-e2e` 用 `SANDBOX_E2E_*` 指向显式配置的私有部署。缺少必需材料会失败；组件或 fixture 通过不能证明 KVM、kernel isolation 或 production 通过。当前缺口见[计划 024](plans/024-haven-loom-single-control-design.md)，[历史验收](docs/acceptance.md)仅对应其记录的源码。既有部署和旧数据库不会自动切换。

## 持久与当前边界

已接纳 operation 的请求、幂等身份与终态固定；取消等待不会取消工作。未知效果保留资源与费用，直到可信核验确认退休。capture 使用独立磁盘，clone 引用的不可变 RAM backing 会继续保留。guest keyed exec 的去重记录随快照回滚，跨 rollback 的更强保证需要外部 fence。

离线 metadata backup 包含同一点的 SQLite、node key 和 HEAD，不含磁盘与 RAM。restore 必须显式 review，review 不授予重放、数据面或 GC 权限。GC 通过持久 inode 删除意图与原子责任交接退休已经证明可回收的资源。

none/relay 网络保持 NIC off；direct 在开启前核验绑定实例的 IPv4 CIDR/flow 策略。hostname 过滤和凭据注入需 relay，host-only secret 在重启后须显式重供。Preview 使用签名 path，没有每 sandbox 独立浏览器 origin。递归文件 watch、跨节点恢复、远端快照归档与 gossip placement 尚未实现。详细边界见[控制契约](docs/haven-control.md)与 [Sandbox 契约](sandboxd/README.md)。
