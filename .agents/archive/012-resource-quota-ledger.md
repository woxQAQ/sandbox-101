# Plan 012：以持久资源计费记录保护 release 到物理回收的交接

> 完整执行验证，不能因 terminated 而推断资源已回收。**r3及后续实施必须同时完整读取 [资源交接合同](evidence/012-r3-approved-contract.md) 与 [production有限owner预算合同](evidence/012-production-owner-budget-contract.md)，其精确Scope/基线/状态与门禁优先于下方历史交接描述。** reviewer 管理索引，执行者只交付结果，不改父计划/index。009 的 template_intents/schema 4 与已批准010/011为声明依赖；007r3仅在父源码批准后接其自身补丁，不带整棵stage2/guest018。其余未经说明变化先 STOP。

```nu
git diff --stat b93fc24..HEAD -- sandboxd/store.go sandboxd/types.go sandboxd/service.go sandboxd/lifecycle.go sandboxd/fork.go sandboxd/checkpoints.go sandboxd/recovery.go sandboxd/reconcile.go sandboxd/warm_fallback.go sandboxd/quota.go sandboxd/quota_recovery_test.go sandboxd/service_test.go sandboxd/recovery_test.go sandboxd/backup_test.go sandboxd/maintenance_test.go sandboxd/warm_fallback_test.go sandboxd/direct_test.go sandboxd/proxy_socket_linux_test.go sandboxd/http_test.go sandboxd/README.md deploy/sandboxd/README.md sandbox-architecture.md
```

## 状态

- Priority：P1；Effort：L；Risk：HIGH。
- Depends on：009、010、011（按已知 schema 4 接续，保留 007 配置 fence 与模板归属；最终组合另需批准007r3自有补丁）。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支；产品与fixture独立SOURCE APPROVE，精确78bc04f完整production通过并清理释放）。
- 审计映射：发现 7。为避免和 013 重复发明计费分支，范围包含统一账本基础，故从审计粗估 M/MED 调整为 L/HIGH。

## 问题与目标

release 先将 sandbox 标为 terminated 以立即撤销访问，再调用 engine 删除。quota 排除 terminated，删除延迟、失败或崩溃期间实际 VM／快照仍占空间，却已经允许新的 claim 使用其额度。

目标是分离访问授权与资源责任，所有仍由该租户承担的可写盘、快照和未结束 sandbox 名额都有持久记录。暂停、恢复、checkpoint/fork、失败补偿和备份恢复不能留下无计费窗口；013 将在同一模型上接入模板与构建。本计划不改变宿主物理容量预算。

## 当前实现与契约

`sandboxd/lifecycle.go:644`：

```go
b.State = "terminated"
b.OperationID = o.ID
if err := saveSandbox(s.ctx, s.store.db, *b); err != nil { return err }
// 随后 s.engine(... releaseVMStep(b.ID), delete ...)
```

`sandboxd/service.go:769`：

```go
"SELECT body FROM sandboxes WHERE tenant=? AND state!='terminated'"
```

quota 另行累加 checkpoints、retired_snapshot 和 operation ReservedMiB；多个逻辑集合拼接使责任转移容易遗漏。`recovery.go` 已有 resourceRecovery 的持久补偿、unknown instance 证明及 lockResourceOwnership。`backup.go` 的 VACUUM INTO 复制全部表，不能在恢复时丢弃计费表。

设计 P7/P8 要求零唤醒删除和失败保留状态；I18 查询失败不得当不存在。匹配 `TestForkRollbackRecoveryAfterDeleteFailure`、`TestRetiredSnapshotQuotaSurvivesRestartAndDeduplicates`、`TestConcurrentQuotaAdmission`，使用真实 Service/SQLite、严格幂等 engine 替身；不单测 quota 加法 helper。

## Scope

只允许 drift check 路径；新增 quota.go、quota_recovery_test.go。现有测试文件仅调整明确依赖真实状态构造的 fixture／配额断言，保留原生命周期及权限断言。README／架构仅计费和 schema 说明。

r3 唯一 engine 例外：`engine/api/types.go`、`engine/operations.go`、新增 `engine/result_instance_test.go`、必要 `engine/engine_test.go`、`engine/INTEGRATION.md`，仅按已批准实施合同的 ResultInstance/finish 事务与行为测试范围。其余 engine（包括 admission/backend/launcher/GC）、guest/egress、公开 quota 字段、新 public request/endpoint、任意调大预算、自动真实数据迁移仍禁止。r3使用既有JSON body/PK，不加DDL或升schema。013 的模板/build/warm 政策本计划不实现；已有 warm fallback 接通同一责任模型。超Scope必须具体证据回父，不能自行扩大。

## 数据模型与不变量

在 quota.go 集中实现，以两张内部表取代 quota 对多个 JSON 集合的重复计费：

- `quota_resources(tenant_id, kind, resource_key, sandbox_slots, storage_mib, source_operation_id)`，主键为前三列，整数非负；
- `quota_refs(tenant_id, kind, resource_key, holder_kind, holder_id)`，同一 holder/reference 唯一，关联前表。refs 用于责任转移，不自动授权删 engine 资源。

kind 和计量：sandbox_slot 每个 sandbox 计 1；vm_disk 每个可证明 VM 的独立可写盘计 DiskMiB；snapshot 每个 snapshot ID 计 DiskMiB+MemoryMiB；pending_storage 使用确定性 operation/step key 预留尚未取得 ID 的额外存储。禁止把 guest RAM 当正在运行时的磁盘占用。金额统一按配置的保守逻辑 MiB，不宣称计算实际 reflink 物理节省。

同一租户同一个 snapshot 只计一次，多个 checkpoint/sleep/cleanup 引用不重复。VM disk 与独立 snapshot disk 为不同资源，应分别计费；这比旧代码更保守。允许按明确资源等式调整原有精确边界 fixture，但必须同时增加低于边界的拒绝/零 engine 副作用断言，禁止任意放大预算。例如 `TestRetiredSnapshotQuotaSurvivesRestartAndDeduplicates` 中 VM disk 64 +共享 checkpoint snapshot 192 +新 pause snapshot 192 =448 MiB，原 384 应改为精确 448，并验证 447 时在分配前拒绝；其余受影响边界逐一列出身份/金额/预期，不移除恢复和去重断言。

计费 tenant 与请求身份分离：对已有 sandbox/checkpoint 的 pause/resume/rollback/release/checkpoint/fork 等动作，用可信源资源 owner；build/import/refill 用明确模板目标 tenant。admission 在内部 Operation 持久化 ChargeTenantID，receipt、pending→resource、补偿和重启均沿用它。原 Operation.TenantID、principal、幂等 key namespace 不修改；root 代 Alice 操作即使选择 Bob/default root 也不能把费用记给 Bob/root。root 可跳过额度拒绝，但所有资源责任仍完整记给真实 owner；自动维护以 owner 的普通预算执行，不继承 root 绕过。

准入事务内检查账本总量并创建 slot／预留／逻辑引用。引擎副作用前先有额度；receipt 确定 ID 后原子将 pending 转为确定资源，不双计、不提前释放。任何终态失败保留实际／未知资源计费，直到可信 NotFound、已完成回收证明，或同事务转交另一个仍承担责任的引用。query error 不是 NotFound。

release 的 terminal marker 与 cleanup 引用转移同事务：立即失效 token，但 vm_disk/snapshot 不因状态失效消失。cleanup/recovery holder 必须继续承接原 sandbox_slot，单纯转为清理责任不能退还名额。原 VM 或未知分配结果尚未可信回收时，slot 保持 1；独占睡眠快照删除失败时 slot=1 且 storage=D+M；只有已无自身 VM/未知分配责任、剩余共享快照已由 checkpoint/template 等独立活跃使用者承担时，才允许 slot=0 而共享 snapshot 继续收费。VM 删除失败时，即使存储余量很大，MaxSandboxes=1 仍拒绝新 claim。

holder 的 GC 含义分开：active_use 是实际使用引用；engine_dependency 是在途引擎依赖；cleanup 是待删除责任，不是 collector 自己的永久阻挡根。按物理 resource identity 选择唯一 collector，它可排除自身及明确转交的重复 cleanup refs，但仍尊重所有其他 active_use 和 engine_dependency；删除确认之前资源行/费用保持，不能先清账再删。retired_snapshot 改为同一 snapshot 资源和 cleanup ref，保留 003 的无窗口保证。

升 schema 4→5，旧库在 DDL 前拒绝，不自动从旧 JSON 猜测账本。新状态构造必须走 Service admission，直接持久化的故障 fixture 要同时创建其真实责任行，不能关闭校验。启动检查负数、缺失资源引用或不支持 kind，发现不一致 fail closed。若新模型无法覆盖已有行为，STOP 出具状态/责任表再修改。

## 命令与基线

根目录 Nushell：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
```

现有基线通过；新增 TestQuotaLedger 名称先用 `go -C sandboxd test . -list '^TestQuotaLedger'` 确认。公开命令、权限和返回码保持现状。

## 步骤

### 1. 建立 release 回归和责任矩阵

新增 `TestQuotaLedgerReleaseKeepsCharge`：一名额租户创建 sandbox，阻塞 delete 时新 claim 应 Exhausted；delete 失败、paused snapshot 删除失败、daemon 重启时仍拒绝；可信资源删除完成后允许。新增 `TestQuotaLedgerLostDeleteResponse` 区分“已删响应丢失”和“查询错误”，不增加第二次物理操作。

**Verify**：focused `go -C sandboxd test . -run '^TestQuotaLedger(ReleaseKeepsCharge|LostDeleteResponse)$' -count=1 -timeout=60s`（GOWORK=off），原实现在保留额度处 RED。gate cleanup 必须放行。

### 2. 建账并逐条切换生命周期

先实现两表/schema/内部操作，再把 claim/fork/checkpoint/pause/resume/rollback/release 的准入、ID 取得、引用发布与补偿全部接入。按上面的资源表逐条记录“意图前、receipt 后、交付后、清理中、确认不存在”的持久状态；必须在代码/文档能逐项对应。替换 Service.quota 的旧 JSON 聚合，不能新旧同时累加。暂不为模板/build 创建额外责任，013 再接入；已有 checkpoint 被模板引用的释放漏洞保持在 013 的范围，不能据此跳过本计划的其他引用。

**Verify**：步骤 1 GREEN；新增 `TestQuotaLedgerPauseResumeCheckpointSharing`，检查多轮 pause/resume、共享 checkpoint、retired cleanup 去重和不低估独立磁盘。

### 3. 验证恢复、备份和并发原子性

新增 `TestQuotaLedgerAdmissionIsAtomic`、`TestQuotaLedgerBackupAndRestart`：并发 claim/fork 不超配，失败操作不抹掉计费；备份恢复保留账本和恢复 fence；不可信 instance 不清账；schema 4/未来版本拒绝且 DDL 不写旧库。

另增 `TestQuotaLedgerRootUsesResourceOwner`，沿现有 root 默认 tenant/选 Bob 代 Alice 捕获回归，检查预留、resource、cleanup、重启费用仅归 Alice且请求身份不变；`TestQuotaLedgerSlotRetirementBoundaries` 分别覆盖 VM 删除失败、独占 sleep 删除失败、共享 checkpoint 转交；`TestQuotaLedgerSnapshotChargeExactBoundaries` 验证上面的 448/447 等式。只有 cleanup、重复 cleanup、cleanup+active_use 三类回收也必须分别证明可回收/唯一删除/禁止回收。旧 RetiredSnapshot/ForkRollback/RecoverResources/TemplateSnapshot 权限回归保留。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
git diff --check
```

预期全部通过。由于跨生命周期改动，最终还需真实 production 生命周期验收。仅使用明确交付的隔离材料，执行以下 Nushell 命令：

```nu
with-env {SANDBOX_PRODUCTION_IO_MAX: $env.PLAN_PRODUCTION_IO_MAX} {
    make production-e2e $"KVM_MATERIALS_DIR=($env.PLAN_KVM_MATERIALS)" $"PRODUCTION_OCI_LAYOUT=($env.PLAN_PRODUCTION_OCI_LAYOUT)" $"PRODUCTION_ARTIFACTS_DIR=($env.PLAN_PRODUCTION_ARTIFACTS)"
}
```

这些 PLAN_* 环境变量分别是已经验证的 block-I/O 预算、pinned 材料、该次隔离 OCI 输入及新建专属 artifact 目录；缺失立即 STOP/BLOCKED，不读取已有 daemon/.local-run 或猜测路径。production runner 负责自建 namespace/daemon 并清理，不能用旧成功记录替代。

## 完成标准

- [x] 至少八个 TestQuotaLedger 行为测试存在并执行，覆盖运行/休眠删除、丢响应、重启、共享、并发、备份、root owner、slot 和精确金额边界。
- [x] 新 quota 仅汇总持久资源账本；每个移交有事务边界，无 terminated 免费资源。
- [x] 不确定资源继续计费，可信不存在才释放；共享快照去重，独立磁盘分别计费。
- [x] schema 5 的安全拒绝、旧授权/恢复回归及所有静态/race/integration/真实生命周期门禁通过。
- [x] 文档、Scope 检查、索引实际结果完成。必要验收未执行时不标 DONE。

## 历史 r1 候选与必要验收

执行者交付STOPPED，未APPROVE。候选worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f5c61c7`，branch `t3/implement-plan012-durable-resource-ledger`，009继承tip `b6813cf7f28881531b2839a13d47b9912a9d5cf0`，自身提交 `6360636758addc6bca61f99ce762a5eac2456a7a`。schema4→5，仅16个Scope文件；完整交接 `/tmp/plan012-handoff.md`，责任矩阵在候选sandboxd/README.md。

自报release/lost-delete RED→GREEN、16项TestQuotaLedger、fulltest29.892s、已提交HEAD全race64.484s、vet/lint0issues、actualfixtureintegration0.774s/diff通过。D64/M128等式与低1MiB零副作用边界明确；旧quota fixture448/447调整有资源身份依据。父已读取全部生产diff与quota.go842行；独立review-plan012-r1已确认SOURCE REVISE：补偿pause捕获丢响应或SQLite入账失败后，recovery identity不在operations扫描中，sandbox回running清OperationID使recovery提前Done，release/反复恢复/重启后仍留snapshot/pending/slot各1。两个真实Service/SQLite overlay均RED；父已读report/源码，已新派012r2独立持久分配接管与清理修订，含restore同类窗口/原公开失败不变。报告 `/tmp/plan012-review-r1-report.md`；独立其余16Ledger/fullrace62.669s/vet/lint/真实fixture均过但不解除此P1。

原make production-e2e实际exit2，test28.34s；device检查和OCI import通过，首个live-daemon-adoption claim报 `internal: incompatible workload agent`，完整lifecycle/golden/warm/TCP未执行。证据 `/tmp/p012-production.ElQ9Ictd/run.vlMobV2h/test.log`。父已读原log及cleanup-review：owned unit `sandbox-production-acceptance-1793993579-3715441.service` notfound/inactive/dead/MainPID0，原cgroup/vm-scopes不存在、driver退出；原privateNS daemons/mount结束。012明确释放VM槽，旧handoff已slot_granted=false/expired。当前槽位转交010r6，禁止生产重试并行。

兼容检查在未修改的sandboxd/agent/client.go Info：Workload2048响应portal必须workload/protocol必须numeric1；暂不能归因为账本或某具体字段。静态材料诊断 `/tmp/p012-info-compat-diagnosis.md` 已证明旧guest为product→workload改名前构建，与b93源码不匹配；Info检查正确。父找到同源码已缓存Nix package05dh...，agent新SHA2463ee2f...ddb、init未变；另派全新隔离guest/OCI打包及实际Info验证，不覆盖旧build/guest、无需新依赖构建。当前VM槽位归该材料准备任务，011与012r2均不得并行启动VM。执行偏差已披露：首次integration编译了缓存Cargo源码依赖17.65s（无fetch/install，但违反禁止uncacheddependencybuild）；Nix不认识offline setting，实际substitute/builders禁用且无build/fetch，不能把无效setting当保证。013仍等本项完整复核及production通过。

## 历史第二轮候选（独立REVISE已返回）

r2候选worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f0c098e`，branch `t3/fix-recovery-quota-reconciliation`，inheritedr1tip1689dedfa7b1b592b500cc1e03afe9ab70994472，own8892e5df8f759a43cea9a1c18f3c1c8a3454061a，treea56cfd1fbf72cd3158f2ffb38e0eeed197f3e3ac。五Scope文件内，现有resource_recovery.Allocations冻结完整分配意图并与pending同事务；对账不依赖当前sandbox状态/OperationID，snapshotcleanup持久移交后才Done，restore版本仍到真实VM删除才退费。

父已读全部r2生产delta与344行新增测试；新独立review-plan012-r2进行中，不自行认定P1已关闭。执行者原overlay2例与新capture4/restore2例已RED→GREEN，18Ledger/fullrace73.851s/vet/lint/四actualRustfixture等自报过；无Cargo、无VM。生产仍等新匹配材料与授权后完整验收，不能标DONE；材料准备task当前持有VMslot。完整交接 `/tmp/plan012-r2-handoff.md`。最终组合已有007/009时只取本项6360636→8892e5d，不重复祖先副本。

## 第二轮独立REVISE与第三轮方案核对

独立r2报告 `/tmp/plan012-review-r2-report.md` 确认capture原P1已关闭，但restore release-first存在新P1：成功恢复A→B后丢响应/SQLite入账失败，release先持久terminated与新opID、仍以A请求delete；late receipt入账后parentDone/Settled，而清理身份仍A。transport/SQLite两独立RED在反复恢复/重启/新release后VM1/slot1/storage320/pending0，restore只1，原failedrollback/release body均不变。18Ledger/fullrace73.227s、8额外durable边界等通过仍不足批准。

父已读完整report/overlay及实际API：api.Operation尚无输出instance字段。已派只读第三轮设计核对可信B证据和最小Scope；未擅自授权engine范围扩展，禁止仅取GetVM当前instance可能误认foreign C。新production原Info与adoption通过，但fork branch exec EOF，后续golden/warm/TCP未到达；报告 `/tmp/p012-r2-production.B158QEtk/report.md`。原预算未改，max触顶且oom=0不证明根因，另派只读诊断。该productionownedunit/cgroups/进程均清理且slot释放，新grant已expired；重跑等具体修订/诊断与新交付。013继续等待完整APPROVE。

## 第三轮设计APPROVE与正式Scope调和

完整规格 `/tmp/plan012-r3-design-proposal-r2.md` 经新的独立冷审 `/tmp/plan012-r3-cold-design-review-r2-818ffa57.md` DESIGN_APPROVE，D1–D4设计异议为0。父据此正式采用 [012 r3实施合同](evidence/012-r3-approved-contract.md)：窄engine字段/finish事务、唯一tenant/S/V/Bcollector、原Aintent冻结、Resolution/Progress分工、T-active同事务、公开旧receipt不可变和完整故障矩阵。普通A终态失败补偿语义保持；一次完整required-native engine race覆盖全部，避免机械重复。该合同是本轮授权实现范围，不再把engine禁令解释成整包可任意改动。

上述历史槽位描述全部过期；当前VM槽FREE、r2生产grant已失效，本实现无VM权限。007r3候选8d08ba0…尚在独立源码复核，未批准不采用；其生产EOF问题独立于本P1。本项维持BLOCKED/P1未闭，013等待最终源码与production。缺失成功witness的公开历史receipt不自动修复、不猜B；未发布内部r2 Settled格式不保兼容。

## 第三轮源码候选（等待独立SOURCE复核）

候选 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-f3e87a98`，branch`t3/resource-quota-ledger-r3`。postdepsBASEdbdbecdf2994df64f8e79c2db3291d48ccd9405c已含r2与批准010/011，own8729475fa9a6b057ae5b58c8f60474933dac12a2→2c15f095ec02e867f5592a2593a3b73c68d25efc，treeaf3de051610bcb6644004eaec3b0c7174e47ecce，11Scope文件+3475/-243。完整报告 `/tmp/plan012-r3-handoff.md`，审计 `/tmp/plan012-r3-audit-f3e87a98/final.json`。执行者交付witness/责任模型及原两个release-first实际RED→GREEN，又发现并修复terminalfailed snapshotdependency与release-before-destination窗口；原18/新28Ledger、engine5proof/010011/native20实际非空。engine全native race194.606s/34required，最终sandboxdrace118.315s/4actual b93Rustfixture，skip/race0，vet/lint/diff过；额外sandbox全race由新RED/修改导致。

父已读全部生产diff并启动新的独立全源码SOURCE复核，以及finish忽略UPDATE原子性专项（尚未判finding）。这些本地门禁不构成APPROVE/P1最终闭合。源码未接未批准007r3-r5链，b93guest来源保留；7获批后只接自身增量，最终组合/production仍独立BLOCKED，无VM权限。

## 最新前置：007r3–r5已SOURCE APPROVE

父完整读最终独立报告 `/tmp/plan007-r3-r4-r5-review-t3-e680e7d7/report.md` 并批准findings=[]，007恢复DONE。最终012必须只接8d08ba0…→1c1e735…→2a8e9c0…纯增量，不能接stage2/guest018。原012r3主审/专项继续看2c原快照；另建隔离provisional组合仅作来源准备，12未批准不宣称最终通过，不重写审查对象。当前VM槽FREE、旧grant失效。

## r3 finish专项P1与窄修订

独立专项 `/tmp/plan012-r3-engine-finish-atomicity-review-92d9f7ee.md` 确认finish三类必需SQL写入只查error、不验实际效果。实际Engine/test-ownedSQLite RAISE(IGNORE)五例：start/restore×VM UPDATE忽略导致succeeded/witnessB但owner残留、重启后exactB delete仍Conflict；×terminal UPDATE忽略导致owner已清但op仍running无witness，重启固化owner-conflict failure；pre-candidate restore failure忽略lease DELETE导致journalLease=false而真实reader1，重启snapshotdelete仍被挡。无默认生产schema已含trigger的证据，不描述成已发生事故/攻击。

修订仍限已批准`engine/operations.go`与`engine/result_instance_test.go`：校验每个必需写入的实际效果，零行/异常行数/无法证明效果时rollback整个completion，保留staged发布、durable重读/terminal不改、010失败fence/lease与011候选边界。保留实际IGNORE故障（不能换ABORT），断言故障时非terminal/no witness、owner/candidate/lease一致，解除后同key/reopen正常完成不额外launch；失败restore保持backend失败时正常固定failed/no witness且真实lease释放、snapshot不再假阻挡。原6个ABORT/commit场景及5proof/010011/native全保留。无需新API/DDL/backend/ledger范围。

新隔离修订只基于2c原candidate并提交engine两文件；另一个012主审继续原2c快照，provisional7组合也不改。先五原反例RED→正式GREEN，再因engine代码变更一次完整required-native engine race/vet/workspace lint/diff，sandboxd未改不机械重跑118s。待主审完整发现后再新任务合并所有修订和批准7纯链，最终组合另验证。源码P1未闭，无VMgrant。

## r3主审P2：成功B已不存在时交付回收责任

主审 `/tmp/plan012-r3-review-f3d6f35c/report.md` SOURCE REVISE（唯一P2），父完整读report与review_service_test.go。quota.go的可信NotFound分支只接failed receipt；new成功witnessed B在destination提交前被合法ExpectedInstance=B Engine delete回收后，release已清V metadata，late allocation仍result_known，snapshot dependency永不退。transport/真实SQLite两例recover4/reopen/recover4/freshrelease后，删CP仍storage192/unresolved1，restore/delete各1，旧publicfailed bytes固定，新claim在引擎前被拒。

修订只在既定sandboxd quota/recovery/正式行为测试Scope：验证完整成功receipt/witness/可信S/tenant/history以及真实Engine NotFound，不把queryerror、foreignW/C、other owner或缺失旧witness当不存在；原子提交reclaimed destination、仍剩V账本/backing/slot与terminal snapshotdependency退休。即使原release已把V清空，也按冻结intent/history核验，不重建/猜当前实例。保留真实终态不变、不额外restore/physicaldelete、其他active/dependency/CP持久引用；不能只吞NotFound或置Done/清收费。

新隔离分支从已准备provisional组合c97543e…开始（2c候选+批准7纯链），只交付本P2自身commit。engine finish P1在另一task改operations/test，稍后单独集成，不并行编辑其文件。先原两故障RED→正确收敛GREEN：release后slot0/storage192（独立CP存在）→CP真正删除/恢复后0与allocation交付、新claim成功；将原调查最终ledgerRejectClaim改为恢复后成功规格，不减少故障/顺序/public bytes/effect断言。对照queryerror/currentC/missingwitness/真实dependency仍保守。新增真SQLreclaimed destination/dependency写失败重启，fault时留费/责任，解除后同proof收敛，无额外restore/delete。

因sandboxd新代码及7组合，一次最终sandboxd fullrace包含7全部47sub、原新28Ledger及新case与4actual b93fixture，vet/正常workspace lint/diff/source保全；engine未改不重复194s。主审已独立145Engine focused/111Ledger PASS、完整解析广测；其明确原engine广测未保留pre-format源码/hash，无法证明语义等价，必须保留限制。finish修订将绑定精确新source重新跑必需engine全race，不能声称补回不存在旧证据。两P1/P2修订与最终组合仍需新的独立SOURCE review与production，013不放行。

## finish F1修订候选（待独立SOURCE复核）

own d5654123c00ed016211115e4433f3959e47ff9e0 parent2c15f095…/tree46a46419382d3b6d28235287c5b91923c506cb84，worktree`t3-37fa8d75`。两fileops/resulttests+517/-9，必需写result/RowsAffected==1，精确lease删除operation+SID，保留010前置fence两阶段。五IGNORE故障RED→GREEN、同key去故障重启正常收敛/旧终态固定；新完整requirednative race195.661s/448PASS/36required/skip0绑定最终source全部614文件哈希。报告 `/tmp/plan012-r4-engine-finish-37fa8d75/handoff.md`，新的独立F1review已派，不自行标APPROVE。原r3engine194s的preformat证据缺口保留历史，新run是新的source证明。另一次提前全race主动中止用于lint修订已披露未当PASS。sandboxd P2另task继续，最终组合未SOURCEAPPROVE/未production，无VMgrant。

## r4最新：F1批准，P2候选与最终组合待审

F1独立 `/tmp/plan012-r4-review-7f7d1ee4/report.md` SOURCE APPROVE/findings=[]，d565412…两文件可采用；30focusedPASS及prefence两阶段额外overlay/精确448全native新源码绑定已核实。旧r3preformat证据限制不改写。

P2候选adbd61a2d416328387c183e0c96c989bffeddd8d parentc975…/treef19cb6c0217c56bbdc4d3b5bc32b64bc6edb819d，三个sandboxd授权文件+609/-28，46实际行为sub（4顺序/24SQL ABORTIGNORE/18拒绝）与原28Ledger/007全47sub/四Rust在一次全race398PASS/skip0，精确源码副本前后保留。交接 `/tmp/plan012-p2-8ce1b630/handoff.md`。父已读完整prod/test增量，未SourceApprove；先新隔离组合adbd+已批d565，后新独立SOURCE复核P2及最终精确组合。任何本地成功不代替production，当前无VM授权，013 hold。

## 最终精确组合待独立SOURCE复核

候选90f68b616faf7a1a6c82423674c0d27a2dda9bd6/tree64a4fc8940e625587a5db4417858aa7e3e04e43b，t3-ac14c9c0/branch`t3/combine-approved-engine-source`，parentadbd61a…，仅将获批F1d565复制为90。完整engine与d565一致、sandboxd与adbd一致、33b93guest/protocol/Cargo保留；父读 `/tmp/source-combo-ac14c9c0/report.md`并独立git验证，未测试runtime。已派新独立P2及最终90整体source复核；批准7/F1不代表P2获批，仍BLOCKED。生产材料只读hash/preflight仍匹配，不是生产运行/slotgrant；实际全production等source批准后新授权与专属artifact，013 hold。

## 最终90 SOURCE APPROVE与新production验收

最终独立 `/tmp/plan012-final-review-65a243e3/report.md` SOURCE APPROVE/findings=[]，精确90/tree64a4全部source可采用。独立P2三组49PASS/critical44/额外partialIGNORE与SID真实独立引用overlay10，vet/正常lint/diff与398SB/448Engine广测source绑定证明核验；F1/7精确subtree与patch链已映射。父已全文读取，源码P1/P2在此组合关闭；不等于production或DONE。

新唯一VMgrant `evidence/production-verification-012-final90.json` owner/task verify-production-plan012-final90，专属artifact`/tmp/p012-r4-production-90f68b6`，精确SOURCE90/tree64a4与原pinnedCH/kernel/new2463guest/baseOCI/IO259:0 1MiB预算，原VMM640MiB/outer8G等限制不改。一次原完整production任务已派发，必须liveadoption/fullLifecycle/golden/warm/realTCP全部实际PASS，结束独立核所有own资源清理并显式释放slot，父再expiregrant。当前本任务持唯一slot，禁止其他VM；012BLOCKED/013hold直到该完整门禁复核通过。

## final90真实production失败与新的诊断

报告 `/tmp/p012-r4-production-90f68b6/report.md`，测试精确90/tree64a4，一次makeexit2/wall53.210s/top48.58s；adoption4.11sPASS，lifecycle20.18s越过两个fork分支身份/env与pause后，8路Connect+info的wg.Wait结束收errors时报microvm_test.go227 NotFound。原日志无法区分哪个goroutine/调用/对象，根因未证，不改变资源预算；后续resume身份/shell/rollback/zeroWake/branch存活及golden/warm/TCP未执行，不标DONE/不放013。

输入531/source616/index/refs保持、四真实managedVMM原权限隔离与cap/cgroup断言通过。原test/runner清理所有owned资源，unit1794007083-137900不在/cgroup-scopes不在/60PIDstarttime结束/无hostmount，slot-release.json显式释放；父将final90grant置false/expired/none。当前无VM授权，禁止重试。

另新只读诊断task对fresh允许日志/源码NotFound传播和pause→8autoResume时序，可用/tmpoverlay test-ownedSQLite真实组件probe但不可改源码、VM、旧/生产DB或archive。源码90原独立批准保留，先证根因再定具体修订，012继续BLOCKED，10DONE。详见执行台账。

## auto-resume缺失tenant预算前置：组件证实，有限setup方案待定

诊断 `/tmp/plan012-final90-autoresume-diagnosis.md`：actualService/HTTP SDK/新SQLite在root无tenant row时八路Connect全typedNotFound/无resumeop/clone0/sleepSID存在，相同root显式resume可clone1；预建root预算与ordinaryAlice对照都八路→一次clone。组件VERIFIED，原生产DB未读、历史缺行对象归因LIKELY。父亲读Authenticate/PutTenant/quota与原production setup确认root默认owner认证不建tenant，012正确新增resume D预留后自动维护必须普通owner预算，触发缺行。保持该产品合同，不能赋root旁路给自动维护。

父选择先拟定验收setup显式ROOT tenant API有限预算，保留原root token/owner/生命周期断言/guest shape/资源cap。新production测试setup当前仍在原Scope之外，已派只读设计推导完整adoption/lifecycle/goldenwarm/TCP的有限资源峰值与精确auto-resume额度低1MiB拒绝，输出窄Scope建议；无source修复或VM授权。合成probe中1<<20只是控制变量不可复制。方案/Scope具体化、实施及独立source复核后才新grant真实验收，012继续BLOCKED/013hold。

## 有限production owner预算提案（独立冷审中）

父完整读 `/tmp/plan012-production-owner-budget-design-a55f6dce/proposal.md`：9slots/12D+7(D+M)累计场景身份上界，D512/M512时13312MiB。它不声称同时峰值或root公共请求硬比较，source90未计build/未领取warm/物理refill次数仍属013，原全部物理cap/guestshape保持。只拟setup调用既有POST/v1/tenants给原root owner有限row，保持rootcredential/defaultnamespace；新external无特权HTTP SDK/SQLite行为测试5组，exact auto4608/4607与scaled768/767、Alice完整lifecycle1024/1023等原子拒绝，root/Bob/scoped身份全部owner普通额度。Scope拟仅production_acceptance_test.go setup与新production_owner_budget_test.go，暂未授权实现。新独立冷审已派核算、保护token与原断言，设计/Scope确认后才执行，当前仍BLOCKED/无VMgrant。

## production有限预算setup设计APPROVE与正式Scope例外

父完整读 `/tmp/plan012-production-owner-budget-cold-review-dce02974/report.md`，DESIGN_APPROVE/blocking[]，9slots/12D+7S累计包络及精确auto4608/4607、scaled768/767、ordinaryAlice1024/1023成立。C1组件额度更新保留普通token、C2两shape全24leaf、C3多版本/backing及当前instance network_policy、C4首轮maintenance显式控制，均正式纳入 [setup实施合同](evidence/012-production-owner-budget-contract.md)，无需新产品政策决定。

**原012 Scope新增唯一测试例外**：`sandboxd/integration/production_acceptance_test.go` 仅首次ready后finite ROOT tenant API setup、局部公式/注释/有界HTTPhelper/imports；新增`sandboxd/production_owner_budget_test.go`无特权external-package实际HTTP SDK/SQLite组件5组。所有产品源/SDK/Engine/SQL/DDL/依赖/guest不改，原microvm/network tests/runner/shape/caps/全部assertions字节不动。生产credential与owner原样，有限row不提权，原产品source90批准保持。

新隔离执行者FF90按此合同实施，真实成功oracle无setup先RED→有限API setupGREEN，原缺row负控制与精确/低额度前置拒绝保留；一次240s必要SB全race+4既有b93fixture、privileged compile-only明确非production、vet/正常workspace lint/diff/精确source/cleanup。后续新独立source review与新grant完整production仍必需；当前仅设计批准/无VM授权，012BLOCKED/013hold/10DONE。

## 有限owner预算setup交付，独立源码复核中

交付 `78bc04fc8715d18319ed4cba69aa92a30975f9e5`，parent90，tree `d8a15809daa2ad2f8f6271edc9f400ba955888ac`，t3-f20b872f / t3/owner-budget-test-setup。恰好production setup+65与新external组件+1936两测试路径；父已全文读两文件并核实diff/文件SHA/fixtureSHA，产品与原assertions/caps保持。完整交接 `/tmp/plan012-owner-budget-f20b872f/handoff.md`；同八流成功oracle省setup实际RED→有限API setupGREEN，五组含24主leaf/192首轮+96恢复、完整12D/7SID/9身份重放、1024/1023物理峰值与保守backing/reopen/immutable。唯一final SB fullrace217.111s，430带Test字段的(package,test)PASS节点，原398保留/31Ledger/007全47sub/四actualRust，testSkip/race0；vet/正常workspace lint/diff与privileged compile-only通过。后者零实际测试，不能作production。

新独立 `review-plan012-production-owner-budget` 已派，须核实C1–C4、原始JSON/源码绑定与严格fixture边界，不能用执行者自报替代批准。源码门禁通过后仍须fresh grant执行完整lifecycle/golden/warm/TCP。当前无VM授权，012BLOCKED/013hold/10DONE。

## fixture SOURCE APPROVE，新grant完整production验收中

独立 `/tmp/plan012-owner-budget-review-b7162bd3/report.md` 与decision.json：SOURCE APPROVE/findings[]，精确78bc04f/tree d8a15809；C1–C4、实际32focused/24leaf/192+96连接、额外HTTP凭据/receipt/policy探针、原430广测/398保留节点/617源码副本绑定均复核，615旧source/33guest输入不变，清理无残留。父已全文读并采用该结论；产品source90批准不变。该source批准仍不是production PASS或012DONE。

父新签 [owner-budget78 production grant](evidence/production-verification-012-owner-budget78.json)，唯一owner/task verify-production-plan012-owner-budget78。已重新验证旧60个进程身份/unit/cgroup结束、材料五SHA/OCI六reachable内容吻合、工具缓存与真实KVM/设备存在，新artifact根 `/tmp/p012-owner-budget-production-78bc04f` 不存在。新隔离执行者FF78，只原始完整production一次，原512MiB guest/VMM640MiB/outer8G/IO259:0 1MiB预算及原assertions/caps不改。lifecycle后半、golden/warm和真实TCP都必须实际PASS；owned清理与显式slot-release后父expiregrant。无额外VM探针、自动重试、源码修改或实际DB/状态archive正文取证授权。当前新任务独占VMslot；012BLOCKED/013hold/10DONE。

## 最终 APPROVE / DONE：精确78完整production通过

父已全文读 `/tmp/p012-owner-budget-production-78bc04f/report.md`、handoff/verification/slot-release，并直接解析原始 `run.MsR695xv/test.log`：一次原始调用make exit0/wall137.401s，顶层132.82s、adoption3.82s、完整lifecycle35.92s、golden/warm21.51s、真实TCP42.97s；实际5RUN/5PASS、fail/skip0。lifecycle越过八流auto-resume后继续完成live-shell/identity、rollback、second pause、zero-wake与branches，warm复用真实PID/instance、SNAT/TCP allow/deny均按原断言执行。未改原requirements、caps、shape、credential、产品源，无额外VM/guest probes或重试。

源码保全617路径/12gitlinks/10预存reference状态均一致，精确HEAD78bc04fc8715d18319ed4cba69aa92a30975f9e5/tree d8a15809daa2ad2f8f6271edc9f400ba955888ac，worktree t3-3c237f22 / branch t3/verify-production-owner-budget78。父独立解析stable-managed-vmm原始样本，13台均UID/GID65534、Groups空、CapInh/Prm/Eff/Bnd/Amb全0、NNP1/seccomp2及原armed CPU/memory/swap/pids/IO限制。自有parser初用错误kind标签导致空集合assert，按原事件stable-managed-vmm修正分类后通过，不涉及Go/VM重跑或弱化门禁。

父重新核验全部88PID/starttime结束、fresh unit sandbox-production-acceptance-1794012534-338056.service不存在/inactive/dead/MainPID0、owned cgroup不存在；原receipt挂载/PGID无残留，状态归档仅stat、未打开/列内容/hash正文。EXPLICIT SLOT RELEASE已接受，`production-verification-012-owner-budget78.json`已slot_granted=false/owner none/handoff_expired=true，槽FREE，无重试授权。

最终结论 **APPROVE / DONE（隔离分支）**，第11项完成；013依赖解除，开始基于78的漂移与设计调和。012其余历史失败/证据等级保留，013仍需为build/template/unclaimed-warm接入完整计费；当前成功不宣称这些政策已实现。父dev仍b93、源码无改/无merge/push。

## STOP 与维护

发现既有测试依赖人为构造无归属资源、需要迁移用户真实数据库、或没有可证明的 receipt 就要释放配额时停止报告。严禁把授权 snapshot_owners 当账本/GC 保活表。后续引入新资源须先定义 kind、保守成本、稳定 identity、holder 和释放证明。分支 `advisor/012-resource-quota-ledger`；另获提交授权才使用 `sandboxd: retain quota responsibility through resource cleanup`。
