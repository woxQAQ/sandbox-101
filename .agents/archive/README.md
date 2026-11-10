# 已完成计划与历史证据

此目录保存已完成工作及其原始设计、复核和验收来源。正文中的 Scope、STOP、分支、机器路径与运行授权均属于当时上下文，不作为当前执行许可。未完成工作只在 [plans](../../plans/README.md) 维护；当前系统契约从 [docs](../../docs/README.md) 阅读。

归档表示计划已有完成/接受记录，不表示本次重新运行了其中的检查，也不证明当前 HEAD 通过旧验收。JSON/脚本证据原样移动，内部 source/path/hash 保留历史含义。

## 完成计划

| 计划 | 已交付内容 |
|---|---|
| [001](001-operation-result-authorization.md) | operation 查询授权及结果令牌边界 |
| [002](002-fork-compensation-keys.md) | fork 分支独立补偿与严格幂等键 |
| [003](003-resume-snapshot-handoff.md) | resume 快照与清理/费用责任交接 |
| [004](004-snapshot-tenant-authorization.md) | 快照可信租户来源与显式外部导入 |
| [005](005-privileged-capture-boundary.md) | VMM 捕获叶子与 root 私有发布 inode 隔离 |
| [006](006-mitm-header-bounds.md) | CONNECT 内层头解析资源上限 |
| [007](007-configure-egress-revocation.md) | configure 撤权、持久 fence 与最终数据接纳 |
| [008](008-filesystem-fifo-bounds.md) | 文件 RPC 非阻塞 FIFO 拒绝 |
| [009](009-template-admission-ownership.md) | 模板首次接纳即固定 owner |
| [010](010-restore-candidate-durability.md) | restore 候选材料持久完成证明 |
| [011](011-instance-condition-recovery.md) | 原操作自身候选的实例条件恢复 |
| [012](012-resource-quota-ledger.md) | 统一资源账本与回收证明 |
| [013](013-template-build-quota.md) | build/template/warm 配额及冻结分配责任 |
| [014](014-sdk-call-cancellation.md) | SDK 排队调用取消 |
| [015](015-schily-xattr-names.md) | SCHILY xattr 字面属性名 |
| [016](016-native-image-ci.md) | Python 标准库原生镜像门禁、官方 lint action 与真实 root/分支 CI；见[接受收据](evidence/016-native-ci-acceptance.json) |
| [018](018-tree-push-cleanup-outcomes.md) | tree push 发布与后置清理的独立结果 |
| [019](019-tree-pull-identity-investigation.md) | 双 UID 调查、保留 FD 与身份约束；未确认泄露 |
| [022](022-responsibility-and-api-boundaries.md) | 职责/测试单元/API 重组，见 [结果](022-RESULT.md) |
| [023](023-structured-error-design.md) | Guest RPC 分类与调用位置；sandboxd 自有错误契约、Engine/Agent 边界翻译及私有/公开诊断，见 [验证证据](evidence/023-structured-errors.md) |

## 历史批次与证据

| 问题 | 入口 |
|---|---|
| 001–005 的组合实施与验收 | [IMPLEMENTATION](IMPLEMENTATION.md) |
| b93 审计与后续计划来源 | [AUDIT](AUDIT-b93fc24.md) |
| 006–021 的历轮执行、失败及修订 | [EXECUTION](EXECUTION-006-021.md) |
| 完成计划的来源提交分布 | [DELIVERY-MAP](DELIVERY-MAP-006-021.md) |
| 十二项组合的范围、review 与最终结果 | [组合合同](FINAL-COMBINATION-12.md)、[审查入口](FINAL-REVIEW-12.md)、[结果](FINAL-COMBINATION-12-RESULT.md) |
| 022 的结构重组与独立验证 | [结果](022-RESULT.md)、[最终接受收据](evidence/022-final-acceptance.json) |
| 原计划索引与阶段状态 | [PLAN-HISTORY](PLAN-HISTORY.md) |

各计划的 Markdown 合同与机器可读收据共置于 [evidence/](evidence/)。旧 47f5 的 VM/production/Rust 成绩、33a3 的重组验证及后续宿主验收分别绑定各自源码，不互相挪用。面向运行的验收记录维护在 [docs/acceptance.md](../../docs/acceptance.md)。
