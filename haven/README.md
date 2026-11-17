# Haven 组装与测试

Haven 按 image、machine、supply、sandbox、dataplane、gc、node 组织业务用例与私有 sqlc。
`internal/application` 是 daemon 和测试共用的组装入口；internal/publication、internal/recovery
和 internal/maintenance 组合跨能力投影、只读验证与固定顺序的恢复。所有组件共用 store、
execution/access 及关闭边界，HTTP 与管理 adapter 直接接收所需用例。

template/pool 查询归 supply，VM/snapshot 查询归 machine；旧 services 与表仓库包已删除。
各 owner 直接消费私有 dbgen，跨能力入口表达历史证明、授权、资源接纳或事务参与。
实施与验收证据见 [027 证据](../plans/evidence/027-capability-map.md)。

## 测试归属

| 目录 | 验证边界 |
|---|---|
| `tests/component/` | 真实 App、SQLite、私有文件系统与注入的宿主/guest 效果；验证责任交接、恢复、幂等、关闭与 HTTP 行为 |
| `tests/native/` | 实际 OCI/EROFS 工具、缓存、元数据、发布与 GC；KVM 用例另受 `kvm` build tag 控制 |
| `integration/` | `integration` tag 的真实 Rust Unix fixture；`privileged` tag 的显式私有部署验收 |
| `store/`、`guest/`、`egress/` | 各自的 SQLite/跨 UID、RPC 或代理行为边界 |
| `authorization/`、`image/`、`machine/` | 单能力 Service/SQLite 与缺少能力端口时的留责边界，不构造完整 App |
| `internal/testfixture/` | 私有 store、效果替身和故障注入；不得反向依赖 application/testapp |
| `internal/testapp/` | 完整 App 组装与重启 fixture；仅跨能力/运行场景使用，生产包禁止导入 |

`component` 表示应用组件集成测试，不是独立函数单测，也不证明 VM 或 kernel 路径。
组件套件的 `parallelLifecycle` 保留最多四个活跃生命周期场景，普通/race/native 入口统一以 `-p=1` 串行调度 Go 包。
包调度或测试归属变化不构成旧 120 秒超时的性能修复证据。
测试使用自建状态，不接管现有 daemon、未知资源或真实旧库。

## 入口

在仓库根目录使用 `just test`、`just test-race`、`just integration` 和 `just smoke`。
`just test-native` 强制真实镜像覆盖，缺工具或权限会失败。
超时保持普通组件每包 120 秒、race 每包 10 分钟、required-native 每包 300 秒。

详细材料与 runner 边界见 [scripts](../scripts/README.md)，镜像直接工具调用与
ownership 条件见 [镜像物化契约](../docs/image-materialization.md)。
