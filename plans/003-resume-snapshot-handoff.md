# Plan 003：原子分离已运行沙箱与待清理的旧休眠快照

> 本文供零会话上下文的执行者使用。逐步运行验证，发生 STOP 条件时报告证据。仅修改 Scope 文件；完成后更新索引 003，除非 reviewer 负责索引。
>
> Drift check：`git diff --stat da513a6..HEAD -- sandboxd/lifecycle.go sandboxd/checkpoints.go sandboxd/recovery.go sandboxd/store.go sandboxd/service.go sandboxd/types.go sandboxd/service_test.go sandboxd/recovery_test.go sandboxd/lifecycle_extra_test.go sandboxd/README.md`。同时检查未提交差异。计划 002 可能已修改 release/recovery/test fixture；识别其预期变化，不回退它们。

## 状态

- Priority：P1
- Effort：M
- Risk：MED；涉及状态提交、引用保活、现有存储计费责任及清理重试
- Depends on：无硬依赖；推荐在 002 后执行以复用严格幂等 fake，并减少同文件合并冲突
- Category：bug / data consistency
- Planned at：`da513a6`，2026-11-06
- Status：DONE；原子清理/计费交接及重启回归通过，独立复核和最终组合验收通过，见 [实施与验收记录](IMPLEMENTATION.md)。

## 问题与目标

恢复先通过 `activate` 提交 running，再通过 `finishResume` 删除旧休眠快照。若删除失败，running 记录仍保留 `SnapshotID`。之后的 pause 看到非空 SnapshotID，会跳过新捕获并删除当前 VM，导致本次运行中的修改丢失。

需要区分两个概念：`Sandbox.SnapshotID` 是当前休眠/恢复所依赖的捕获；旧快照的删除是独立的持久清理责任。不能用同一个字段同时表达这两件事。

## 当前实现

`sandboxd/lifecycle.go:95` 的激活尾部：

```go
b.State = "running"
b.OperationID = ""
b.LastError = ""
return saveSandbox(s.ctx, s.store.db, *b)
```

`lifecycle.go:542`：

```go
if b.SnapshotID == "" { return nil }
if err := s.retireSnapshot(*o, b.SnapshotID, b.ID, ""); err != nil {
    return err
}
b.SnapshotID = ""
b.PausedUntil = time.Time{}
return saveSandbox(s.ctx, s.store.db, *b)
```

`lifecycle.go:428` 的 pause 只在 `b.SnapshotID == ""` 时调用 `hibernate-snapshot`，之后删除 VM。这个条件对“pausing 中捕获完成后的重放”有必要，但对 running 遗留旧快照不安全。

已有结构及约束：

- `recovery.go` 的 `resourceRecovery` 包含 Kind、Source、Snapshot、Attempt、Done，可持久化清理；`finishRecoveryAttempt` 只在已知 engine 终态失败后增加 attempt，未知或在途不能改键。
- `recoverResources` 当前发现失败 sandbox 仅覆盖 provisioning/pausing/resuming/terminated。普通 running 已清空 OperationID，不能靠该扫描补救。
- `checkpoints.go` 的 `retireSnapshotOwned` 在 resource-ownership 锁下检查 sandbox/checkpoint/template/warm/pending operation 引用，再删除。保留这一锁顺序与“读引用出错则不删”的规则。
- `store.go` 使用 SQLite FULL/WAL、单连接，`saveSandbox` 和 `saveOp` 能接收事务 executor；不要在持有事务时通过同一个 db 打开嵌套查询。
- `checkpoints.go` 的 rollback 最后也调用 `finishResume`，须一起覆盖。
- `service.go:736` 的现有 quota 通过 `Sandbox.SnapshotID != ""` 计入 `Shape.MemoryMiB`，不统计 `resource_recovery`。只清空字段会提前释放仍占空间的旧快照额度，因此交接必须同时保留现有计费责任。

```go
used += sb.Shape.DiskMiB
if sb.SnapshotID != "" {
    used += sb.Shape.MemoryMiB
}
```

设计文档 `sandbox-architecture.md` P8 要求捕获失败不丢原状态；本计划进一步保证成功恢复后的新状态不会被旧清理任务覆盖。客户端已终态失败的操作不能被后台改写成功。参考服务级测试：`TestRecoverResourcesFailedResumeOnlyDeletesItsClone`、`TestRecoverResourcesRetainsPendingOperationSourceSnapshot`、`TestRecoverResourcesOwnershipTransferIsSerializedWithCollection`。

## 目标设计

### 运行态与旧快照的原子交接

引入内部持久清理种类 `retired_snapshot`，复用 `resource_recovery` 表。名称表示“已退出当前沙箱状态、等待按引用规则回收的快照”，不表示可无条件删除。

1. 成功激活提交 running 的同一 SQLite 事务内：若当前 sandbox 的 SnapshotID 非空，插入稳定 ID 的清理记录，保存 sandbox ID、tenant ID、旧 snapshot ID、来源 operation ID，以及原有内存快照计费量 `Shape.MemoryMiB`；同时清空 Sandbox.SnapshotID / PausedUntil，提交 running/新 instance。
2. 交接在 sandbox 锁之后取得 resource-ownership 锁。不得在事务提交前把旧引用从内存中永久移除；事务失败时状态和责任均未交接。
3. 事务提交后才可以尝试清理。清理错误记录在独立 intent 中，不把已成功运行的 sandbox 改回 paused/resuming，也不阻止下一次 pause 捕获当前状态。
4. 所有 `activate` 成功路径都遵守该不变量，包括普通 resume、rollback、失败恢复后的激活，以及任何遇到已存在 SnapshotID 的 running 提交。不要只给单一调用点加特殊条件。
5. 重放同一交接不得重复创建清理责任。清理 ID 绑定 sandbox + snapshot，engine 删除 key 还须包含持久 attempt。

### 存储计费责任的原子转移

清空 Sandbox.SnapshotID 不代表物理快照已回收。将原字段承载的内存快照计费责任一同转移到 cleanup intent；这是维持既有配额约束，不是增加新的计费产品。

- 在运行状态与清理 intent 的同一事务内持久化 tenant、snapshot 和原内存计费量。提交前由 sandbox 引用计费，提交后由业务引用或 cleanup 责任承接，不得存在两边都不计费的窗口。
- 修改 `Service.quota`，把尚未实际回收的 cleanup 存量计入对应租户；新增 cleanup 责任按 `(tenant_id, snapshot_id)` 去重，重放、多份 cleanup 及多个 attempt 不得重复计费。若现有 sandbox/checkpoint 等业务计费已经覆盖同一快照的内存份额，不再叠加 cleanup 的相同份额；保留现有磁盘计费及其他预留规则，不借此重构全部历史计费语义。
- 新捕获 admission 必须看见旧 cleanup 仍占用的额度。明确删除失败、未知或在途时不得释放；只有已确认实际回收/NotFound，或同一事务中明确转移给仍覆盖该份额的业务计费责任，才能释放 cleanup 的对应计费。
- 数据库重启、备份及清理 attempt 更新均不能丢失或重置占用；owner/计费量不从可变沙箱当前状态重新推算。读取失败时拒绝新资源接纳，不能当作零占用。
- 有 checkpoint 共享引用时仍保持快照存活；其删除或引用变更与 cleanup 计费查询采用一致事务/锁顺序，避免业务计费刚移除、cleanup 尚未承接时少计。

### 清理行为

- 显式分派 `retired_snapshot` handler，不能将其错误送入要求源 allocation receipt 的 `recoverOrphanResource`。该记录的可信来源是原子交接，不依赖原客户端操作必须 failed/succeeded。
- `resource_recovery` 表必须在首次交接前已存在；将创建放入 openStore 的幂等 DDL，不能依赖后台 maintenance 恰好先运行。
- 重新读取实时引用后才删除；checkpoint、template、warm、pending operation 或其他 sandbox 仍引用时保留清理记录，稍后再试，不能当场删除或丢失责任。
- 引用计数的查询须能排除当前清理记录自身；若其他清理记录提供同一快照责任，不能互相永久保活。不要把 Done=false 清理意图直接等同于业务使用引用。
- engine NotFound 视为已回收；已知终态删除失败用新的持久 attempt；未知、连接中断或在途继续跟踪原删除键。源 VM/新实例不得被此类快照清理删除。
- 清理器不得写回其发现时缓存的整份 Sandbox，从而覆盖后来 pause/renew/configure 的状态。

这是内部元数据扩展，不增加 HTTP 字段。已有异常 `running + SnapshotID` 记录在暴露数据面/执行下一次 pause 前要以同样的原子交接修复；无法证明归属时阻断危险 pause 并报告恢复状态，不能静默清空或删除。

## Scope

允许修改：

- `sandboxd/lifecycle.go`
- `sandboxd/checkpoints.go`
- `sandboxd/recovery.go`
- `sandboxd/store.go`
- `sandboxd/service.go`（仅 quota 对旧快照 cleanup 计费的承接）
- `sandboxd/types.go`（仅必要的内部状态表达）
- `sandboxd/service_test.go`、`sandboxd/recovery_test.go`、`sandboxd/lifecycle_extra_test.go`
- `sandboxd/README.md`
- `plans/README.md`（状态）

不修改 engine GC/COW、guest、网络策略、token、租户配额上限、模板/warm pool 计费、全局审计设计、API schema。现有快照计费责任随 cleanup 交接及 `Service.quota` 的对应读取属于本计划必要范围，不得延期。不要通过强制删除共享快照、禁用 checkpoint 或阻止正常后续 pause 回避问题。

## 执行步骤与验证

命令使用 Nushell，从仓库根目录执行。测试必须驱动真实 Service/SQLite 与可检查的后端故障，不单测新内部 helper。

### 1. 复现二次休眠丢状态路径

新增 `TestResumeCleanupFailureDoesNotReuseOldCapture`：完成 pause → resume，对旧快照删除注入明确失败；resume 后继续运行并改变可观测 generation，再 pause/resume。fake 后端需记录 snapshot/clone 对应的 generation，不能只断言调用次数或 ID 非空。验证恢复的是最新 generation，旧快照删除失败不会改变已运行 sandbox。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test . -run '^TestResumeCleanupFailureDoesNotReuseOldCapture$' -count=1 -timeout=30s }
```

预期修复前失败，且失败原因确实是第二次 pause 重用了旧捕获。

### 2. 实现原子交接

创建清理表初始化和携带 tenant/内存计费量的 `retired_snapshot` 记录，同步让 quota 读取未回收 cleanup 的去重占用，统一激活尾部的持久发布；调整 `finishResume`、rollback 和恢复调用，移除“删除成功才能清空运行态 SnapshotID”的顺序。仍需保留 pausing/resuming 重放时的快照引用。

新增 `TestResumeHandoffIsAtomicAcrossRestart`：在交接提交前和提交后中断并重开同一状态目录；前者保留原依赖，后者 running 的 SnapshotID 为空且清理责任存在。使用事务失败或确定性 gate/故障点，禁止 sleep 触发窗口。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(ResumeCleanupFailure|ResumeHandoff|ConcurrentAutoResume|AutoResumeRetries)' -count=1 -timeout=90s }
```

预期全通过；检查 SQLite 中不存在“running 已发布，但旧快照责任或存储计费无处记录”的窗口。

### 3. 接入持久清理与共享引用保活

新增 `TestRetiredSnapshotCleanupHonorsLiveReferences`：旧捕获仍被 checkpoint 引用时不删除，移除最后业务引用后最终回收。新增 `TestRetiredSnapshotCleanupRetriesAfterRestart`：删除终态失败后重启继续，新 attempt 键不同；删除未知/在途时不能另起键。新增 `TestRetiredSnapshotCleanupDoesNotOverwriteNewPause`：旧清理与下一轮 pause 交错，后者的新 SnapshotID、VM 与状态不能被覆盖。

测试中覆盖异常历史 running 记录的保守修复；没有可信快照归属时保持资源并报告错误。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(RetiredSnapshot|RecoverResources|Rollback|Resume)' -count=1 -timeout=120s }
```

预期所有新增及原有恢复/引用测试通过。

### 4. 验证清理失败期间不提前释放额度

新增 `TestRetiredSnapshotCleanupKeepsQuotaUntilReclaimed`：为实际 tenant 设置小额度，旧捕获删除持续失败，执行多轮 pause/resume。验证新捕获在“活动资源 + 旧 cleanup + 本次预留”超出额度时于接纳阶段被拒绝，拒绝之前不调用新的 engine snapshot，现有运行 generation 保留；恢复清理成功后可重新接纳。

新增 `TestRetiredSnapshotQuotaSurvivesRestartAndDeduplicates`：旧捕获计费交接后重启，校验 quota 拒绝/允许的边界不变；同 snapshot 的重放、多个 cleanup attempt，以及与 checkpoint 的引用共享不能重复收取同一 cleanup 内存份额，也不能出现无计费窗口。通过实际服务 admission 与 SQLite 恢复验证，不直接单测求和 helper。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'TestRetiredSnapshot(CleanupKeepsQuotaUntilReclaimed|QuotaSurvivesRestartAndDeduplicates)' -count=1 -timeout=90s }
```

预期两项测试通过，持续删除失败不能通过反复恢复规避既有租户存储配额。

### 5. 全量回归并记录行为

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env { GOWORK: "off" } { go -C sandboxd vet ./... }
make integration
make smoke
git diff --check
```

预期 exit 0；文档说明激活成功与旧快照后台清理的独立语义，不把清理失败伪装成状态恢复失败。integration/smoke 不替代真实 VM 验收；未跑 privileged 套件须如实记录。

## 完成标准

- [x] 上述七个命名行为测试存在且通过。
- [x] 第二轮休眠产生并恢复新 generation，旧清理失败不丢新运行状态。
- [x] 成功运行记录的 SnapshotID 已清空，旧捕获有原子持久责任。
- [x] 崩溃前后均无引用或存储计费丢失，cleanup 不覆盖后续状态。
- [x] 删除失败/未知/在途期间旧快照仍计入租户额度；按 tenant/snapshot 去重，实际回收后才释放；多轮 pause/resume 不能绕过 admission。
- [x] 共享引用存在时不删，引用消失后责任仍可执行。
- [x] 删除失败/未知/在途按不同规则处理，客户端终态不变。
- [x] 全包 race/vet、实际 integration、smoke 与 diff check 通过。
- [x] 修改位于获授权计划的 Scope 并集内，索引已更新。

## STOP 条件

- 无法在现有单连接 SQLite 模型下证明交接事务不嵌套；先提交锁/事务顺序设计，不用增加连接数规避。
- 需要 engine 层放宽资源身份或修改 GC 契约。
- 发现历史 running 记录的快照归属不明；不得推断所有权并自动删除。
- 修复要求修改 HTTP schema、令牌或快照物理格式。
- 两次合理修正后验证仍失败或需要 Scope 外文件。

## Git 与维护

使用已绑定 checkout；否则建议 `advisor/003-resume-snapshot-handoff`。若要求提交，使用 `sandboxd: persist retired snapshot cleanup`；未授权不提交、推送或开 PR。

后续增加新的“恢复执行并交付”路径时，都必须保持运行状态和清理责任的原子交接。SnapshotID 不应重新承载垃圾回收状态；清理记录要有重启恢复、错误可见性和幂等生命周期。原有快照计费随 cleanup 转移属于本计划必须完成的安全约束。自动观测指标、模板与 warm pool 的完整配额治理属于后续工作，不在本计划中扩展。
