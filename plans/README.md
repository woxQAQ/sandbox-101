# 未完成计划

本目录仅保留未完成工作。025、026 的实施已完成，按用户归档指令将计划与证据移入 [归档目录](../.agents/archive/README.md)；验收缺项保留在下方跟踪。017、020、021 继续暂停。归档不授权提交、推送、部署、旧库迁移或真实 VM 验收。

| 计划 | 当前状态 | 剩余边界 |
|---|---|---|
| [028：Haven review 实施提示词](028-haven-review-prompt.md) | IMPLEMENTED | 第 2、10、14、15、22、23 条复查缺口已修复，条件 release、checkpoint fork 和冻结结果完整性已有行为回归。第 19 条补做完成，业务 HTTP 与管理 UDS 统一选择公开错误响应。普通测试中因 `mkfs.erofs` 跳过的 35 条 Go 测试事件，在 dev shell 内用私有 UID namespace、tmpfs 及 required 模式复跑，35/35 逐项通过（image、gc 共 75 条通过事件，0 fail/skip）。普通套件另有 700 条 Go 通过事件，Rust 34 项通过。`just integration`、`just smoke`、`just check` 及变更相关 Haven 包 race 通过。VM 与完整原生验收仍按对应计划跟踪 |
| [027：按业务能力重组 Haven](027-haven-capability-boundaries.md) | IMPLEMENTED / VALIDATION_BLOCKED | 能力、私有 SQL、测试与文档实施完成，剩余完整 required-native 门禁 |
| [017：有界恢复](017-bounded-resource-recovery.md) | PAUSED / 未实现 | 持久待核对集合与有界恢复扫描；原 013 依赖已交付，但不自动启动后续工作 |
| [020：供应观测设计](020-provisioning-observability-design.md) | PAUSED / 来源映射未收尾 | 历史设计稿曾交付；当前目录无最终设计交付物，不能视为指标已上线 |
| [021：只读恢复诊断设计](021-readonly-recovery-diagnostics-design.md) | PAUSED / 来源映射未收尾 | 历史设计稿曾交付；当前目录无最终设计交付物，不能视为诊断 API 已上线 |
| [024：Haven / Loom 单控制面重设计](024-haven-loom-single-control-design.md) | STAGE_F_PARTIAL / VM_TESTS_BLOCKED | 旧边界清除和测试/runner/部署迁移完成；当前 guest 镜像和 Haven 生命周期测试缺显式私有材料和预算，整体尚未完成 |

027 已完成声明/SQL/测试逐项归属、publication/恢复边界与全部业务能力迁移。最终 Haven race **673 pass、0 fail、35 skip**，生成/架构、真实 Rust fixture、八组 smoke 与静态检查通过。旧树的普通入口超时和原生工具权限缺项见 [027 证据](evidence/027-capability-map.md)。017/020/021 不因此恢复。

024 的历史 F 树组件、完整 race、六项 Rust integration、六组 smoke、静态检查及 required-native 均通过：`run.9exdrqy5` 为 **677 pass、0 fail/skip**，Haven **278.938s**。跨 UID capture 与宿主策略检查通过；这些结果不验证后续 025/026 源码。命名整理及定向验证见 [阶段 F](evidence/024-stage-f-retirement.md)，历史来源见 [E](evidence/024-stage-e-entrypoints.md)、[D](evidence/024-stage-d-control-matrix.md) 与 [迁移记录](evidence/024-migration-map.md)。既有部署未切换；恢复暂停工作前须映射当前 Haven store/step/观测与诊断边界。

## 待完成验收

以下缺项源自已归档的 [025](../.agents/archive/025-haven-services-sqlc.md) 与 [026](../.agents/archive/026-direct-image-tools-and-tests.md)，在 027 最终树继续核对。实施完成不将失败、未运行或缺权限标为通过。

| 缺项 | 最近源码绑定的结果 | 完成边界 |
|---|---|---|
| 完整原生镜像验收 | 027 preflight `run.sa0aku_r` 明确拒绝当前非 root；全套未运行。026 的 **73 pass、15 fail、0 skip** 与 `security.capability` 缺失仅作历史证据 | 在具备真实 ownership/xattr 权限的 runner 上通过现行 required-native 门禁；不自动提权、不将 required 失败改成 skip |

按源码文件归位后的工作区执行 `GOFLAGS=-json just test`，普通入口通过，跨服务工作流包用时 74.209s。各 Go 模块共 690 条测试通过事件、35 条跳过事件，Rust 34 项通过。单独在私有 UID namespace 中以 `-race` 和 `SANDBOX_REQUIRE_EROFS_TESTS=1` 执行镜像、GC 和 guest rootfs 子集，29 个顶层场景通过。完整 required-native 门禁仍由上表跟踪。见[普通测试日志](../build/go-test-layout.AB3Q4Ja0/test.log)。

026 已删除镜像 bwrap/unshare 包装；025 的缺 bwrap/namespace 条件仅是历史记录，不是当前待办。详细命令、源码身份与原始失败见 [026 证据](../.agents/archive/evidence/026-direct-image-tools-and-tests.md)；025 分批交付与原树验证见 [025 证据](../.agents/archive/evidence/025-services-sqlc.md)。024 继续跟踪 guest 镜像启动与 Haven 生命周期所需的 VM 材料和宿主预算。

## 当前源码入口

根目录仅保留 `go.work`，控制面位于 `haven/`，效果库位于 `loom/`，wire DTO 位于 `types/`，SDK 位于 `sdk/`，共享代码位于 `common/`，开发工具位于 `tools/`。Haven 的 `internal/application` 负责 App 组装和最终关闭；各包的测试放在实现旁，跨服务场景在 `integration/workflows`，带标签的 RPC 与 VM 场景在 `integration`。夹具与入口见 [Haven 测试归属](../haven/README.md#测试归属)。架构检查脚本及其回归入口已删除；旧计划和证据中的相关命令只记录历史验证。业务用例和私有 SQL 已归 image/machine/supply/sandbox/data/collection/node，跨能力组合位于 internal；旧 services/表仓库目录已删除。构建入口为 `justfile`；CLI 显式使用 `--request-file FILE`，不读取 stdin。历史通过只绑定原树，当前验证以实际命令为准。

## 恢复工作前

1. 核对任务范围、计划基准与当前源码/未提交改动。历史源码摘录、文件名和 schema 仅作定位线索，先重建实际映射再修改。
2. 对照计划检查目标、依赖、Scope 与必需材料；交付物或原始证据不在本 checkout 时，明确缺口，不重造历史通过记录。
3. 完成后记录真实命令与结果；必要验收缺失保留 BLOCKED。计划、设计审查、实现、运行验收与合入是不同状态。
4. 完成计划及证据移入 [.agents/archive](../.agents/archive/README.md)，更新入链与本索引。按用户指令先归档已完成实施时，尚未通过的验收继续在本索引保留。

已完成的 001–016、018、019、022、023、025、026，以及历史审计/执行/组合验收均在 [归档索引](../.agents/archive/README.md)。原完整计划索引保留为 [历史快照](../.agents/archive/PLAN-HISTORY.md)，不作为当前执行指令。
