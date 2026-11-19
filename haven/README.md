# Haven 组装与测试

Haven 按 image、machine、supply、sandbox、dataplane、gc、node 组织业务用例与私有 sqlc。
`internal/application` 是 daemon 和测试共用的组装入口；internal/publication、internal/recovery
和 internal/maintenance 组合跨能力投影、只读验证与固定顺序的恢复。所有组件共用 store、
execution/ephemeral 及关闭边界，HTTP 与管理 adapter 直接接收所需用例。

template/pool 查询归 supply，VM/snapshot 查询归 machine；旧 services 与表仓库包已删除。
各 owner 直接消费私有 dbgen，跨能力入口表达历史证明、授权、资源接纳或事务参与。
实施与验收证据见 [027 证据](../plans/evidence/027-capability-map.md)。

## 测试归属

按断言选择目录。`foo.go` 的测试集中在同包目录的 `foo_test.go`，通过公开接口验证时使用 `包名_test`。文件先写 `Test...` 用例，再写本包私有的测试类型、函数和夹具。测试需要 SQLite、完整 App 或真实镜像工具时，仍按被验证的行为归属。

| 位置 | 示例 |
|---|---|
| `machine/`、`sandbox/`、`supply/` | 捕获与删除的原子性、租约与 fork、模板与 warm pool 回收 |
| `image/`、`gc/` | 用真实 OCI/EROFS 材料验证导入、发布、元数据和缓存回收 |
| `internal/application/` | 启动时拒绝不完整的工作流、装配回调、关闭时保护 writer |
| `integration/workflows/` | 跨服务行为，如从 checkpoint 分支、删除原 checkpoint 后验证分支，或备份后恢复整个 App。随普通 `go test ./...` 执行 |
| `integration/` | `integration` 标签运行 Rust RPC fixture，`guest_image` 运行 guest 镜像场景，`privileged` 运行带 VM 的 Haven 生命周期测试 |

基础 store/runtime 材料在 `internal/testfixture/`，完整 App 组装在 `internal/testapp/`。`tests/fixtures/` 提供多个测试包直接或间接共用的类型与操作。仅在一个包使用的夹具就近放在该包的用例之后。只有测试导入这些夹具。

`fixtures.ParallelLifecycle` 保留最多四个活跃生命周期场景，普通/race/native 入口统一以 `-p=1` 串行调度 Go 包。
包调度或测试归属变化不构成旧 120 秒超时的性能修复证据。
测试使用自建状态，不接管现有 daemon、未知资源或真实旧库。

## 入口

在仓库根目录使用 `just test`、`just test-race`、`just integration` 和 `just smoke`。
`just test-native` 强制真实镜像覆盖，缺工具或权限会失败。
超时保持普通组件每包 120 秒、race 每包 10 分钟、required-native 每包 300 秒。

详细材料与 runner 边界见 [scripts](../scripts/README.md)，镜像直接工具调用与
ownership 条件见 [镜像物化契约](../docs/image-materialization.md)。
