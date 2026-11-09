# 工作约束

## 按任务读取

- 跨 engine/sandboxd 职责、生命周期或隔离边界变更，先读 [Engine 架构](docs/architecture.md) 和 [Sandbox 架构](docs/sandbox-architecture.md) 中相关协议与不变量。
- 修改 engine API、receipt、捕获/恢复、GC 或备份时，读 [Engine 集成契约](docs/engine-integration.md)；修改 OCI/EROFS 时再读 [镜像物化](docs/image-materialization.md)，修改 direct 网络时再读 [网络策略](docs/network-policy.md)。
- 修改 sandbox 授权、模板、配额、恢复或数据面时，读 [sandboxd 契约](sandboxd/README.md)；修改代理或 secret handling 时再读 [Egress 契约](sandboxd/egress/README.md)。
- 修改 guest 或 RPC 时，读 [Guest 契约](guest/README.md) 与 [Protocol](protocol/README.md)。修改 operator 时遵循其目录内的 `AGENTS.md`。
- 构建与验证从 [README](README.md#build-and-local-verification) 和 `Makefile` 选当前入口；原生/跨 UID/VM 验收先读 [测试输入与 runner 边界](scripts/README.md)，部署条件见 [文档索引](docs/README.md#组件与运行)。
- 继续计划时先核对 [未完成计划索引](plans/README.md)。仅在追查历史设计或验收来源时读取 [.agents/archive](.agents/archive/README.md)，其中执行指令与授权均为历史。

## 持久与安全边界

- 变更保持“持久意图 → 外部效果 → 核验 → 原子责任交接”的顺序。查询失败保留未知状态、资源和费用；只有可信回收证明才能退责任。
- operation 的请求、幂等身份与已提交终态保持固定。实例相关副作用使用冻结的历史证明和当前归属共同核验；当前查询不补造缺失 witness。
- 生产入口使用真实 runtime，替身经测试显式注入。旧 schema/不完整证明走明确拒绝与人工恢复；未发布内部细节直接重整，不增加推断式兼容路径。
- 测试使用自建私有状态与有归属的资源。保留现有 daemon、`.local-run/`、未知实例及真实旧数据库；secret 只经私有输入提供，诊断和文档使用无实际用途的值。
- `references/` 仅作只读机制参考，`engine/third_party/` 的变更按其 provenance 独立评估；普通构建不初始化或修改参考子模块。

## 验证与交付

- 测试围绕真实行为单元（Service/HTTP/SQLite、跨 UID 文件系统、guest RPC、Engine 生命周期）设计，避免镜像 helper 实现的独立函数单测。
- 跨 Go/Rust 的协议变更同时检查 schema、生成绑定与真实 Rust fixture；宿主替身通过不证明 guest、KVM 或 kernel 路径。
- 原生镜像验证使用 `SANDBOX_REQUIRE_EROFS_TESTS=1`；缺工具、namespace、mapping 或验收材料时报告 BLOCKED/未运行，保持断言与生产资源限制。
- 选择能证明本次行为的最窄检查；明确区分组件测试、真实 fixture、KVM、kernel-isolation 和 production。报告实际命令、FAIL/SKIP 与未运行项，历史通过不挪用到新树。
- 文档按 [维护边界](docs/README.md#维护边界) 更新；当前契约、未完成计划与历史记录分别维护。完成计划归档不自动授权继续暂停工作、运行旧验收或发布代码。
