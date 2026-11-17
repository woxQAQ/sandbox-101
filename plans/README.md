# 未完成计划

本目录仅保留未完成工作。025、026 的实施已完成，按用户归档指令将计划与证据移入 [归档目录](../.agents/archive/README.md)；验收缺项保留在下方跟踪。017、020、021 继续暂停。归档不授权提交、推送、部署、旧库迁移或真实 VM 验收。

| 计划 | 当前状态 | 剩余边界 |
|---|---|---|
| [027：按业务能力重组 Haven](027-haven-capability-boundaries.md) | IMPLEMENTED / VALIDATION_BLOCKED | 能力、私有 SQL、fixture/测试、门禁与文档实施完成；剩余普通 120s 超时及真实 native ownership 验收 |
| [017：有界恢复](017-bounded-resource-recovery.md) | PAUSED / 未实现 | 持久待核对集合与有界恢复扫描；原 013 依赖已交付，但不自动启动后续工作 |
| [020：供应观测设计](020-provisioning-observability-design.md) | PAUSED / 来源映射未收尾 | 历史设计稿曾交付；当前目录无最终设计交付物，不能视为指标已上线 |
| [021：只读恢复诊断设计](021-readonly-recovery-diagnostics-design.md) | PAUSED / 来源映射未收尾 | 历史设计稿曾交付；当前目录无最终设计交付物，不能视为诊断 API 已上线 |
| [024：Haven / Loom 单控制面重设计](024-haven-loom-single-control-design.md) | STAGE_F_PARTIAL / VM_ACCEPTANCE_BLOCKED | 旧边界清除和测试/runner/部署迁移完成；当前 KVM/production 缺显式私有材料和预算，整体尚未完成 |

027 已完成声明/SQL/测试逐项归属、publication/恢复边界与全部业务能力迁移。最终 Haven race **673 pass、0 fail、35 skip**，生成/架构、真实 Rust fixture、八组 smoke 与静态检查通过；原普通/native 缺项继续保留。最终源身份与完整结果见 [027 证据](evidence/027-capability-map.md)。017/020/021 不因此恢复。

024 的历史 F 树组件、完整 race、六项 Rust integration、六组 smoke、静态检查及 required-native 均通过：`run.9exdrqy5` 为 **677 pass、0 fail/skip**，Haven **278.938s**。capture/kernel 实际检查通过；这些结果不验证后续 025/026 源码。命名整理及定向验证见 [阶段 F](evidence/024-stage-f-retirement.md)，历史来源见 [E](evidence/024-stage-e-entrypoints.md)、[D](evidence/024-stage-d-control-matrix.md) 与 [迁移记录](evidence/024-migration-map.md)。既有部署未切换；恢复暂停工作前须映射当前 Haven store/step/观测与诊断边界。

## 待完成验收

以下缺项源自已归档的 [025](../.agents/archive/025-haven-services-sqlc.md) 与 [026](../.agents/archive/026-direct-image-tools-and-tests.md)，在 027 最终树继续核对。实施完成不将失败、未运行或缺权限标为通过。

| 缺项 | 最近源码绑定的结果 | 完成边界 |
|---|---|---|
| 普通组件 120 秒入口 | 027 最终 `just test`：component **120.014s timeout**；同树 Haven race **673 pass、0 fail、35 skip**，component **276.088s** | 在原预算内完成普通入口；保持断言、timeout、生命周期并发槽和生产资源限制 |
| 完整原生镜像验收 | 027 preflight `run.sa0aku_r` 明确拒绝当前非 root；全套未运行。026 的 **73 pass、15 fail、0 skip** 与 `security.capability` 缺失仅作历史证据 | 在具备真实 ownership/xattr 权限的 runner 上通过现行 required-native 门禁；不自动提权、不将 required 失败改成 skip |

026 已删除镜像 bwrap/unshare 包装；025 的缺 bwrap/namespace 条件仅是历史记录，不是当前待办。详细命令、源码身份与原始失败见 [026 证据](../.agents/archive/evidence/026-direct-image-tools-and-tests.md)；025 分批交付与原树验证见 [025 证据](../.agents/archive/evidence/025-services-sqlc.md)。KVM/kernel-isolation/production 不由组件或原生镜像通过替代，024 的 VM 缺项继续由原计划跟踪。

## 当前源码入口

根目录仅保留 `go.work`，控制面位于 `haven/`，效果库位于 `loom/`，wire DTO 位于 `types/`，SDK 位于 `sdk/`，共享代码位于 `common/`，开发工具位于 `tools/`。Haven 的 `internal/application` 负责 App 组装和最终关闭；单能力 Service/SQLite 测试在 owner 包，跨能力/完整 App 场景在 `tests/component`，实际镜像在 `tests/native`。`internal/testfixture` 提供基础材料，`internal/testapp` 单独组装完整 App，生产包不得导入。业务用例和私有 SQL 已归 image/machine/supply/sandbox/data/collection/node，跨能力组合位于 internal；旧 services/表仓库目录已删除。构建入口为 `justfile`；CLI 显式使用 `--request-file FILE`，不读取 stdin。历史通过只绑定原树，当前验证以实际命令为准。

## 恢复工作前

1. 核对任务范围、计划基准与当前源码/未提交改动。历史源码摘录、文件名和 schema 仅作定位线索，先重建实际映射再修改。
2. 对照计划检查目标、依赖、Scope 与必需材料；交付物或原始证据不在本 checkout 时，明确缺口，不重造历史通过记录。
3. 完成后记录真实命令与结果；必要验收缺失保留 BLOCKED。计划、设计审查、实现、运行验收与合入是不同状态。
4. 完成计划及证据移入 [.agents/archive](../.agents/archive/README.md)，更新入链与本索引。按用户指令先归档已完成实施时，尚未通过的验收继续在本索引保留。

已完成的 001–016、018、019、022、023、025、026，以及历史审计/执行/组合验收均在 [归档索引](../.agents/archive/README.md)。原完整计划索引保留为 [历史快照](../.agents/archive/PLAN-HISTORY.md)，不作为当前执行指令。
