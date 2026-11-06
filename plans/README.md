# 实施计划索引

由 improve 审计生成于 2026-11-06，代码基准 commit `da513a6`。用户已授权实施审计建议的五项。sandboxd 001–004 在独立工作区顺序执行，engine 005 在另一独立工作区执行；父线程负责复核、整合及维护本索引。状态与验证结果以完成后的实际证据更新，不将已派发视为已完成。

每份计划独立包含问题、源码摘录、设计约束、修改范围、分步验证、行为回归、完成标准及 STOP 条件。验证命令使用 Nushell；原仓库命令与本计划新增的验收目标已作区分。S 约数小时，M 约一天，L 为多日，均包含测试；Risk 表示修复本身的风险。

## 顺序与状态

| 计划 | 目标 | 优先级 | 工作量 | 风险 | 硬依赖 | 状态 |
|---|---|---|---|---|---|---|
| [001](001-operation-result-authorization.md) | 按调用方权限读取 operation，并限制结果令牌签发 | P1 | S | LOW | 无 | DONE |
| [002](002-fork-compensation-keys.md) | 严格校验 fake 幂等契约，独立回滚每个 fork 分支 | P1 | S | LOW | 无 | DONE |
| [003](003-resume-snapshot-handoff.md) | 将运行状态与旧快照清理、存储计费责任原子分离 | P1 | M | MED | 无 | DONE |
| [004](004-snapshot-tenant-authorization.md) | 建立快照来源归属与模板引用授权 | P1 | L | MED | 无 | DONE |
| [005](005-privileged-capture-boundary.md) | 隔离 VMM 捕获写入与 root 快照发布 | P1 | L | MED | 无 | DONE |

状态值：`TODO`、`IN PROGRESS`、`DONE`、`BLOCKED（原因）`、`REJECTED（原因）`。执行者完成后记录实际验证命令与结果；必要验收无法执行时标 BLOCKED，不能因普通测试通过而标 DONE。若 reviewer 管理索引，由执行者交付结果、reviewer 更新。

## 依赖与并行安排

推荐顺序为 **001 → 002 → 003 → 004**。这些计划没有必须整份依赖前一份的逻辑关系，但共享 service/lifecycle/recovery 与测试 fixture，串行能减少冲突并让后续计划复用更严格的后端替身。

- 002 内部存在硬顺序：先让 fake 拒绝同键不同参数，再复现和修复分支回滚；不得跳过。
- 003 可独立实施；若 002 已完成，必须保留其资源级 release 步骤和严格 fake。旧快照移入 cleanup 时必须同步承接现有存储计费，不能因清空 SnapshotID 提前释放租户额度。
- 004 可独立实施，但和 003 都涉及元数据与引用发布，建议待 003 合入后再动工。它将内部 schema 提升为 2，默认拒绝无可信来源的旧状态；真实旧状态迁移需求须先报告，不自动推断或删除。
- 005 主要位于 engine，可与 sandboxd 计划在独立 checkout 并行。若同时运行真实 VM 验收，要协调 CPU/RAM/磁盘与临时存储容量，不能多个大测试争用材料后把容量失败误报为代码失败。
- 独立 checkout 必须包含计划依赖的最新本地提交及这些文档。T3 child worktree 不复制未提交文档；派发前应明确传递计划内容或使用包含文档的基准，不能假定子任务能看见本目录。

## 计划复核记录

首轮独立冷读指出两个阻断项，均已落实到文档：

1. **003 的计费交接遗漏**：已补入 `Service.quota` 及其文件范围，明确在原子交接时持久化 tenant/内存计费量，按 tenant/snapshot 去重，实际回收前保留额度。新增多轮 pause/resume 删除失败与重启计费行为回归；不扩展模板/warm pool 配额。
2. **004 的现有测试范围遗漏**：已将 `maintenance_test.go`、`warm_fallback_test.go` 加入 Scope 与 drift check；明确将未来版本保护改为高于当前 schema，golden fixture 经 root 为目标 tenant 显式导入，并单独列出原有测试门禁，保留恢复断言。

001、002、005 首轮未发现同等级阻断项。第二轮独立冷读已确认 003、004 的两个阻断项在计划层面解决，未发现剩余同等级阻断项。上述说明仅是计划评审结果，不代表实现或测试已完成。

## 实现复核进展

两组实现均已在隔离工作区完成，首轮实现复核发现的下列边界已修订并经第二轮独立复核 APPROVE：

- 004：root 捕获已有沙箱时，快照租户归属必须来自可信源资源，不能直接使用调用方的默认/所选 tenant。
- 002：完整 maintenance/reaper 路径不得绕过 unknown-instance 证明；删除已成功但响应丢失后须能安全收敛残留引用。
- 005：独立材料在完成标志持久化前必须完成落盘；恢复时不能对未同步且可能损坏的磁盘材料重新计算合法 manifest。

上述修订已全部整合至根工作区暂存区，未创建父分支提交。最终组合验证全部通过：强制原生工具 engine race、sandboxd/protocol race、Go/Rust 静态检查与测试、实际跨 UID 隔离、Rust 中继及 daemon smoke、production microVM 全流程。Production 测试 99.56 秒，临时服务/cgroup 已清理，验证前后源码指纹一致。提交来源、RED→GREEN、复核与完整日志见 [实施与验收记录](IMPLEMENTATION.md)。

## 两项实现边界

**004 的快照授权**：`published` 仅表示可恢复，不授予租户读取权。最小模型是不可变 owner 与管理员明确外部导入，不引入隐式全局共享。计划估算从审计粗估 M 调整为 L，因为持久来源、重放、备份与元数据边界必须一并完成。

**005 的安全验收**：目录 fd 防止路径替换，新发布 inode 防止源写 FD 继续修改；二者缺一不可。它需要多 UID namespace 测试及当前真实 KVM/生产 launcher 验收。环境不满足时保留 BLOCKED 和具体证据，不扩大 VMM 权限或借用历史通过记录。

## 审计中考虑后排除的事项

以下是已记录设计边界，不作为这五项的缺陷或附带扩展：

- metadata backup 不复制 VM 磁盘/RAM；本目录不扩展全量备份产品。
- keyed exec 账本随 VM 快照回滚，跨回滚去重需要外部 fence；本目录不承诺更强语义。
- Preview 共享 origin、非递归 watch、managed-only process attach 是已记录限制。
- Btrfs stat 与 proc maps 身份不一致时严格拒绝 COW；不通过放宽身份检查改善兼容性。
- 单机范围、未实测的性能/密度目标不当作已完成能力。

## 保留但未纳入本次实施的发现

这些事项没有被否定，不要因未生成计划视为已修复：

- 文件 RPC 打开 FIFO 可阻塞 worker/permit。
- start/restore 的 ExpectedInstance 与自身新实例持久化之间的恢复冲突。
- 首次模板提交与异步发布之间的跨租户归属竞争。
- template build、golden snapshot 和 warm pool 尚未进入完整租户配额。
- CI 缺少强制原生 OCI/EROFS 测试；lint 与 Nix devShell/CI 的工具范围未对齐。
- guest tree pull 的特权重开临时路径、cwd 与降权顺序仍需独立双 UID 验证；不将未完成复现当作已确认漏洞。
- 供应延迟、池状态指标与只读恢复诊断属于方向选项，另行选择。

## 执行与审查纪律

- 不输出任何真实令牌、秘密、`.env` 值；测试使用生成的无实际用途数据，不复用运行状态。
- 不操作 `.local-run/`、现有 daemon、未知归属文件或真实旧数据库。
- 不安装依赖、不修改宿主 NixOS 配置，不因为本地缺少前提而降低验证标准。
- 不为未发布内部 helper/步骤名保留多余兼容层；已存在状态无法安全重放时要明确拒绝并保留证据。
- 测试应覆盖实际 Service/HTTP/SQLite、跨 UID 文件系统或 Engine 生命周期行为，不增加独立函数的实现镜像式单测。
- 不因计划存在就提交、推送、开 PR 或启动执行代理；执行授权与计划交付分开。
