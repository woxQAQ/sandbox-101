# Plan 017：以持久待核对集合替代每轮全历史恢复扫描

> 当前 PAUSED，尚未实施。以下步骤仅在明确恢复本计划后执行；013 已完成不自动解除暂停。历史文件名与摘录须先映射到当前职责重组后的源码，完成状态见 [计划索引](README.md)。

> 按阶段验证；不得用删除幂等历史或忽略未知资源换取性能。完成更新索引 017；reviewer 维护时交付证据。先执行下列 drift check 并核查未提交差异，009/012/013 的 schema、owner、账本变动为已知依赖。

```nu
git diff --stat b93fc24..HEAD -- sandboxd/recovery.go sandboxd/reconcile.go sandboxd/service.go sandboxd/store.go sandboxd/types.go sandboxd/lifecycle.go sandboxd/checkpoints.go sandboxd/fork.go sandboxd/warm_fallback.go sandboxd/maintenance_test.go sandboxd/recovery_test.go sandboxd/backup_test.go sandboxd/recovery_budget_test.go sandboxd/README.md deploy/sandboxd/README.md
```

## 状态

- Priority：P2；Effort：L；Risk：HIGH。
- Depends on：013（包含 007/009/012；在 schema 6 的完整资源责任模型上优化）。
- Category：perf；Planned at：`b93fc24`，2026-11-06；Status：PAUSED（未实施，继续前须重核源码与范围）。
- 审计映射：发现 12。原粗估 M/MED 调整为 L/HIGH，因为不能破坏终态操作仍有在途 engine 副作用的保护。

## 问题与目标

每轮维护扫描所有历史 operation，再为所有分配步骤逐条跨 daemon FindOperation；已删除资源的历史 claim 仍重复查询。reap/pools 在其后执行，成本随总历史增长。目标保留完整幂等历史，持久化有限的“仍需核对什么”，使稳定历史不再参与每次跨 daemon 查询。

## 当前状态与不变量

`recovery.go`：

```go
"SELECT body,spec_hash FROM operations ORDER BY id"
// 对每个终态 operation：
for _, step := range recoveryAllocationSteps(o) {
    engineOp, e := s.findResourceStep(o, step.name, step.action)
```

`findResourceStep` 内实际调用 `s.opts.Engine.FindOperation(s.ctx, key)`。resourceReferences 又扫描所有 operation 核对依赖。`reconcile.go`：

```go
s.reconcile()
s.reap()
s.pools()
```

设计允许 operation 终态而 engine 步骤仍在执行；`TestRecoverResourcesProtectsInFlightEngineDependencyOfTerminalSandboxOperation` 明确保护这种情况。query 失败不能解释为不存在，未知 ownership 保留；确定 key 重提不能换随机 key。012/013 的 quota_resources/quota_refs 提供持久资源责任，不能因优化队列而删账。

范例：`TestRecoverResourcesCollectsOnlyProvenUnownedVMs`、`TestRecoverResourcesOwnershipTransferIsSerializedWithCollection`、`TestRefillBackoffSurvivesRestartAndSuccessResets`。测试经过真实 Service/SQLite 和严格 engine receipts，不单测循环 helper。

## Scope

仅上述 drift 路径，新增 recovery_budget_test.go；lifecycle/checkpoints/fork 仅持久引用发布的接续点。禁止 engine API、幂等记录清理、降低引用核验、真实数据迁移、改 TTL 或池目标、引入 Redis/后台常驻外部队列。

## 目标模型

新增 `resource_work(operation_id, step, action, engine_operation_id, state, retry_at, sequence)`，确定 `(operation_id,step)` 唯一，索引覆盖未完成 state/retry_at/sequence。另有 `resource_work_dependencies(work_id, direction, resource_kind, resource_id, expected_instance)`，逐条区分请求输入与已观察输出：VM、snapshot、当前 instance fence 可以同时存在，不能只留一个 resource_id。再新增 `resource_deletions(resource_kind, resource_id, collector_id, engine_key, expected_instance, state)` 的唯一持久删除 fence。以上都不含凭据。schema 6→7，旧版本 DDL 前拒绝，不通过运行时全库猜测回填。

- `Service.engine` 调用 Submit/Find/Wait 之前持久化 watch 和所有已知请求输入依赖；收到接纳/查询 receipt 后记录原 key 的 engine operation ID 与全部已知输出，即使后续等待失败也不遗漏。clone 的输入 snapshot、snapshot 的输入 VM 与输出 snapshot、restore 的旧/候选 VM 均明确表示。
- “分配资源责任已交接”与“engine 依赖观察已结束”是两个条件：settled 必须既取得原 key 对应 engine operation 的可信终态、释放输入读取/捕获依赖，又已将所有输出交给持久 holder/cleanup 或证明不存在。不能仅因 VM 有 holder 就 settled，父 operation 终态也不够。
- 未知接纳结果、暂时 FindOperation NotFound、查询错误继续保留 work 和依赖；不因多次重试/年龄超时丢弃。
- 每轮最多 64 个历史步骤查询，使用两秒 recovery pass context。所有长期 sandbox/checkpoint/pool/resource-ownership 锁获取使用 TryLock 或 context 可取消准入；忙项持久延后，不能仍调用不可取消 s.lock 后声称有预算。保持既有锁顺序，s.mu 只做短 map 操作且不包含 I/O。公平选择 retry_at/sequence，坏项退避但不饥饿其他项。
- GC 语义分三类：active_use、未结束 engine_dependency 是阻挡删除的使用根；cleanup/recovery 是清理责任，不构成 collector 自身的永久阻挡。按物理 identity 选唯一 collector，仅排除自己的/明确转交的重复 cleanup ref，以及当前完全相同 delete watch 的自身删除输入；其他读取/捕获/恢复依赖绝不能排除。费用保留至物理删除确认。
- 删除不得持 resource-ownership 锁等待 engine：在锁/事务内重读使用根并登记唯一 resource_deletions fence 后释放锁，再以 pass context 等待原 engine key；超时保留 fence/work/charge，绝不视为删除完成。所有引用发布、claim/template/checkpoint/warm 转移在同一 ownership 协议下检查该删除 fence，禁止等待期间重新发布同一资源。回收者重新取得锁核对 absent/receipt 后才清账/移除 fence。终态删除失败的新 attempt 也须持久化，不随机换 key。
- 每轮当前资源 inventory 仍是正确性依据；不得每秒加载 settled 历史。当前 holder/未结束 work 的使用根决定可回收性，cleanup fence 决定唯一删除序列。总历史不可影响 idle 稳态 FindOperation 查询量；当前活跃量和未解决 work 可以影响成本，不承诺完全 O(1) 或未测量 SLO。

## 命令

根目录 Nushell：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -run 'TestRecoverResources|TestRetiredSnapshot|TestForkRollback' -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
```

新增 TestRecoveryBudget 名称和 BenchmarkRecoveryHistory 必须用 go test -list 核对；benchmark 结果只能叫本机测量，不等于生产性能。

## 步骤

### 1. 测量真实维护入口的查询复杂度

新增 `TestRecoveryBudgetDoesNotRescanSettledHistory` 和 `BenchmarkRecoveryHistory`。用已证明完成且资源已回收的 receipt/operation fixture，分别 100/1000/10000 个历史记录，调用实际维护 pass，记录 FindOperation 次数、SQL/资源数量、耗时。fixture 可批量装载真实格式状态，必须保留签名／账本一致性，不能把 FindOperation 固定返回值当恢复正确性。

**Verify**：focused TestRecoveryBudgetDoesNotRescanSettledHistory 在原实现对“稳定历史零重复 Find”断言 RED；benchmark 生成基线记录但不设置易抖动毫秒门槛。

### 2. 先持久登记，再切换恢复读集合

实现 resource_work、完整输入/输出依赖、删除 fence、schema/备份检查、Service.engine 的先登记后副作用；逐个迁移 reference 发布、孤儿分配和补偿接续。只有可信 engine 终态和输出责任交接均完成才能 settled。resourceReferences 使用当前 active_use+未解决 engine_dependency，不再查询已settled历史步骤；cleanup 责任只能由唯一 collector 按上述规则排除。

**Verify**：新增 `TestRecoveryBudgetCrashPreservesUnresolvedWork`，覆盖 engine 已接纳而上层终态、登记后未接纳、receipt 后未发布、清理失败。另增 `TestRecoveryBudgetActiveHolderDoesNotEndEngineDependency`：已有 holder→父失败→holder release→engine snapshot 仍在运行，输入 VM/snapshot 在重启后继续被保护，直到 engine 可信终态。旧 InFlightDependency/OwnershipTransfer/UnknownInstance 回归必须通过。

### 3. 加预算、公平性和持久退避

让 context 显式传递到查询、可取消锁获取和 engine 等待，不在深层 helper 继续用无限 s.ctx。新增 `TestRecoveryBudgetFairnessAndReaperProgress`、`TestRecoveryBudgetBackupRestart`；并增 `TestRecoveryBudgetBusyLocksKeepIndependentReaperProgress`，另一生命周期持锁超过 pass deadline，恢复仍在有界测试裕量内返回、不丢 work，独立过期项继续推进。维护入口还必须先执行轻量 reap 或将 recovery 作为独立有预算阶段，不能让不受预算的完整 reconcile 长等待再次阻塞 reap。

新增 `TestRecoveryBudgetDeletionFenceSurvivesWaitTimeout`，证明已接受 delete 超时后发布者被持久 fence 拒绝，重启仍接续同 key，确认 absent 后才退费。新增 `TestRecoveryBudgetCleanupCollectorReferences` 覆盖只有 cleanup、重复 cleanup、cleanup+active_use 三类，分别可回收、只执行一条删除序列、禁止回收。不得以提前放掉 ownership 锁且没有持久 fence 来达到时间门槛。

现有 `TestRecoverResourcesHoldsOwnershipUntilEngineDeletionCompletes` 直接断言 mutex 在等待期间不可取得，与新机制有意冲突。允许将其改名为 `TestRecoverResourcesDeletionFenceBlocksPublication`：证明锁可及时取得但同一资源发布被持久 fence 拒绝、其他资源发布可推进、删除完成/失败/超时后的 fence 处理正确。保留其“删除与重新发布不能交错”的原行为保证，不保留“必须长期持特定 mutex”的内部实现约束，也不能删掉该保护测试。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -run '^$' -bench '^BenchmarkRecoveryHistory$' -benchtime=1x -count=1 }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
git diff --check
```

预期全部通过。稳定历史新增 100→10000 后 FindOperation 不随其增长；未解决 work 每轮历史步骤查询 <=64，超预算 pass 不丢工作。保留原终态 operation 查询结果。

## 完成标准

- [ ] 至少八个 TestRecoveryBudget 场景和 benchmark 实际执行，有查询量、忙锁、engine 依赖与删除 fence 证据。
- [ ] work 先于 engine 副作用，settled 有责任接收方；未知/暂时 NotFound 不被遗忘。
- [ ] 稳定历史不反复查询、预算与公平性可断言，reaper/池仍推进。
- [ ] schema 7/备份恢复/原安全回归及 race/vet/lint/integration/diff 全通过。
- [ ] 文档、Scope 与索引记录实际结果；未声称生产 p95 或删除历史。

## STOP 与维护

如果无法证明某步的 settled 条件，先保留 watch，报告资源责任缺口；不能按终态/年龄自动忽略。发现必须支持旧 schema 迁移、增加外部服务或扫描真实未知状态时停止。以后新增 engine 步骤必须登记 work 并定义终结证明。分支 `advisor/017-bounded-recovery`；另获提交授权才使用 `sandboxd: bound recovery work independently of operation history`。
