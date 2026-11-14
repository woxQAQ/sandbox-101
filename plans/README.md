# 未完成计划

本目录仅保留未完成工作。016 和本轮完成的 [023](../.agents/archive/023-structured-error-design.md) 已归档；017、020、021 继续暂停。本轮 023 实施授权不包含提交、推送、共享分支合入或 VM 验收。

[024：Haven / Loom 单控制面重设计](024-haven-loom-single-control-design.md) 当前为 STAGE_F_PARTIAL / VM_ACCEPTANCE_BLOCKED。旧 Go 控制模块、双 daemon 和 engine UDS facade 已删除；workspace、CI、native gate、runner 和部署示例迁到单 Haven。最终组件、完整 race、六项 Rust integration、六组 smoke、静态检查及 required-native 均通过：`run.9exdrqy5` 为 **677 pass、0 fail/skip**，Haven **278.938s**，JSON/race/count=1/300s 不变。上述全量数据绑定命名整理前的 F 树；当前入口为 `runtime-e2e` / `SANDBOX_RUNTIME_*`，后续命名整理及定向验证见 F 证据。capture/kernel 实际检查通过；KVM boot/production 缺显式私有材料、rootfs、OCI 和 I/O 预算，整体尚未全部完成。当前树证据见 [阶段 F](evidence/024-stage-f-retirement.md)，历史来源分别见 [E](evidence/024-stage-e-entrypoints.md)、[D](evidence/024-stage-d-control-matrix.md) 与 [迁移记录](evidence/024-migration-map.md)。既有部署未切换；017/020/021 继续暂停，恢复前必须映射当前 Haven store/step/观测与诊断边界。未提交、推送或合入。

| 计划 | 当前状态 | 剩余边界 |
|---|---|---|
| [017：有界恢复](017-bounded-resource-recovery.md) | PAUSED / 未实现 | 持久待核对集合与有界恢复扫描；原 013 依赖已交付，但不自动启动后续工作 |
| [020：供应观测设计](020-provisioning-observability-design.md) | PAUSED / 来源映射未收尾 | 历史设计稿曾交付；当前目录无最终设计交付物，不能视为指标已上线 |
| [021：只读恢复诊断设计](021-readonly-recovery-diagnostics-design.md) | PAUSED / 来源映射未收尾 | 历史设计稿曾交付；当前目录无最终设计交付物，不能视为诊断 API 已上线 |
| [024：Haven / Loom 单控制面重设计](024-haven-loom-single-control-design.md) | STAGE_F_PARTIAL / VM_ACCEPTANCE_BLOCKED | 旧边界清除和测试/runner/部署迁移完成；组件/Rust/smoke/静态、required-native 677 pass 与实际 capture/kernel 通过，当前 KVM/production 缺显式私有材料和预算 |

## 当前源码入口

review 整理后根目录仅保留 `go.work`，控制面位于 `haven/`，效果库位于 `loom/`，wire DTO 位于 `types/`，SDK 位于 `sdk/`，共享代码位于 `common/`，开发工具位于 `tools/`。构建入口为 `justfile`；CLI 显式使用 `--request-file FILE`，不读取 stdin。上述历史 F 阶段通过记录仍只绑定原树；本次验证以当前会话实际命令为准。

## 恢复工作前

1. 获得本次任务的明确范围，核对计划基准与当前源码/未提交改动。历史源码摘录、文件名和 schema 仅作定位线索，先重建实际映射再修改。
2. 对照计划检查目标、依赖、Scope 与必需材料；交付物或原始证据不在本 checkout 时，明确缺口，不重造历史通过记录。
3. 完成后记录真实命令与结果；必要验收缺失保留 BLOCKED。计划、设计审查、实现、运行验收与合入是不同状态。
4. 实现及所需复核完成后，将计划和对应证据移入 [.agents/archive](../.agents/archive/README.md)，更新入链与本索引。

已完成的 001–016、018、019、022、023，以及历史审计/执行/组合验收均在 [归档索引](../.agents/archive/README.md)。原完整计划索引保留为 [历史快照](../.agents/archive/PLAN-HISTORY.md)，不作为当前执行指令。
