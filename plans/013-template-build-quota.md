# Plan 013：将构建、模板快照和预热存储接入租户资源账本

**本轮已完成：用户要求“做完013就结束”。013的独立源码审查、完整生产验收和清理证据现均通过，DONE后已STOP，不启动017或其它计划/整批集成/fixture维护。历史后续依赖安排不再作为执行指令。**

> 逐步执行并验证；完成更新索引 013，reviewer 管理时交付证据。**本轮唯一有效实施基线/Scope/门禁以 [013 R2正式实施合同](evidence/013-r2-approved-contract.md) 及其绑定的完整R2提案为准；必须全文读取。** 先运行下列 drift check，再核对未提交差异。009/012 是已知前置实现，必须保留其 owner 与账本模型；下方历史设计/审查阶段保留为记录，不覆盖已批准R2。

```nu
git diff --stat b93fc24..HEAD -- sandboxd/service.go sandboxd/lifecycle.go sandboxd/reconcile.go sandboxd/recovery.go sandboxd/warm_fallback.go sandboxd/checkpoints.go sandboxd/store.go sandboxd/types.go sandboxd/quota.go sandboxd/template_quota_test.go sandboxd/quota_recovery_test.go sandboxd/production_owner_budget_test.go sandboxd/template_admission_test.go sandboxd/service_test.go sandboxd/recovery_test.go sandboxd/maintenance_test.go sandboxd/warm_fallback_test.go sandboxd/backup_test.go sandboxd/integration/production_acceptance_test.go sandboxd/integration/microvm_test.go sandboxd/README.md sandbox-architecture.md deploy/sandboxd/README.md
```

## 状态

- Priority：P1；Effort：L；Risk：HIGH。
- Depends on：012（包含 007/009；要求 schema 5 的 quota_resources/quota_refs）。
- Category：security；Planned at：`b93fc24`，2026-11-06；Status：DONE（精确1414双独立SOURCE_APPROVE/七项关闭；唯一完整production5RUN5PASS/13VMM guard及89PID清理释放父核通过，grant已过期，本轮STOP）。
- 审计映射：发现 8，以及旧索引中的 warm pool 配额治理。涉及完整持久责任链，风险较粗审计 MED 上调。

## 问题与目标

tenant 的 template_build 不经过 quota，构建 VM 和黄金快照占用未计费资源。checkpoint 可登记为模板，删除 checkpoint 后物理快照由模板保留，却可能丢失计费责任。每模板 warm 上限也不是租户或宿主总资源预算。

目标：所有租户发起的构建先预留，发布后的模板快照按真实 snapshot identity 去重计费；预热池仍是未认领 VM，不将它们冒充租户 sandbox，但其独立磁盘和共享 backing 的保存责任明确，池目标由 root 管理。

## 当前状态与约束

`sandboxd/service.go` 的 template_put/template_build 分支只校验 shape、TargetWarm 和来源，没有 quota 调用；末尾 saveOp 并启动异步执行。`lifecycle.go`：

```go
v, err := s.engine(*o, "build-create", api.Request{
    Action: "create", Config: &api.Config{Image: t.Image, Shape: t.Shape, /* labels */},
})
```

`checkpoints.go` retireSnapshotOwned 计算模板/warm 等引用，有 users 就保留快照；deleteCheckpoint 随后删除 checkpoint 行。`reconcile.go` pools 直接构造 refill Operation/saveOp，再执行 warm-clone。文档要求 pool 中不含租户秘密且网络关闭。

012 的依赖契约：quota_resources 按 `(tenant,kind,resource_key)` 唯一保存 slots/storage/source_operation，quota_refs 区分 active_use/engine_dependency/cleanup；snapshot 计 D+M、vm_disk 计 D、sandbox_slot 计 1、pending_storage 按确定步骤预留；无 refs 不代表已物理删除，可信不存在才销账。ChargeTenantID 来自可信资源 owner，与原 operation 的请求 tenant/key namespace 分离；root 绕过准入也完整记账。009 的 template_intents 保持不可变 owner 与条件发布，007 的 configuration_fences 不能被模板/配额操作绕过。不要重新引入 JSON 多集合加总。

范例：`TestConcurrentQuotaAdmission`、`TestSnapshotOwnershipSurvivesReplayAndRestart`、`TestRefillBackoffSurvivesRestartAndSuccessResets`；新回归经过 Service/SQLite 和严格 receipt 替身，不写纯计算函数单测。

## Scope

只允许 drift check 的路径，新增 template_quota_test.go。types.go 仅必要内部字段；production_acceptance_test.go/microvm_test.go 仅测试设置中的明确 root/tenant 选择和预算断言，保留所有真实生命周期、安全及资源限制断言。新增 `production_owner_budget_test.go` 例外仅按013真实计费调整build/模板/unclaimed-warm/claim的身份与费用断言、有限预算推导及必要的确定维护gate；五组HTTP行为、24主leaf/192+96连接、原missing-row负对照、ordinary精确/低1MiB拒绝、owner授权/物理责任/receipt/policy/零wake边界不得删除或放松。`quota_recovery_test.go` 仅现有warm/template fixture的root actor与有资源等式支持的额度/保留责任断言，以及必要schema边界；31个Ledger及007全部47子场景保留。`template_admission_test.go` 仅允许为009r2的publication ownership gate调整修改TargetWarm的调用身份：使用明确原owner tenant的root，以适配本项root-only池目标政策；原engine-query/ownership持锁门禁、0→1目录发布、同snapshot及终态断言全部保留，不扩大生产范围或放宽普通tenant权限。部署/架构只配额与 pool 权限。保留 public Tenant/Command/Template JSON 字段。

执行前漂移核对（009r2已实装）：`TestTemplateAdmissionPublicationOwnershipGate` 的 `update.TargetWarm=1` 当前由普通tenant提交，原计划编写时该新增测试尚不存在。本项将TargetWarm变更设为root-only，若不狭义调整测试actor，该测试会在进入原engine/ownership gate之前被正确拒绝。上述Scope增补仅解决这一已确认的前置测试漂移，不能删除测试、调大deadline掩盖锁问题或把目标权限规则改回tenant可写。

禁止：engine 资源预算、guest、egress、改变 snapshot owner、跨租户共享、物理缓存配额算法、真实数据库自动迁移、自动扩大租户上限。

## 目标语义

1. Build admission 在保存 operation 同事务预留 1 个 build_slot 和 D+(D+M) MiB：构建盘 D、将产生的独立快照 D+M。取得 build VM/snapshot receipt 后原子替换预留为确定资源；删除 VM 后释放其 D/build_slot，模板 snapshot 继续计 D+M。不得因 build 终态失败释放实际或未知资源。新013自动VM删除必须有已冻结成功start/clone的非空immutable ResultInstance及完整V/B/owner/source资格；created/空instance、自己的start从未dispatch或failed-start看似仍created都不授权空ExpectedInstance delete。缺合法witness保留已知D/build_slot并ManualRecovery，禁止generic orphan/新attempt/current-B猜测绕过；可信实际GetVM NotFound可按完整group及未知分配责任条件做账本回收，不发送无条件delete。
2. 新增 kind=build_slot，计 slots=1；它消耗现有 MaxSandboxes 对租户并发计算的预算，不创建可访问 sandbox token。升级 schema 5→6，旧版本 DDL 前拒绝，部署注明；不提供自动迁移。
3. template_put 有 snapshot 时取得同租户授权后创建 template holder：已计 snapshot 不重复收费，外部 root 显式导入也建立目标租户的责任记录。替换模板时新旧快照责任原子交接，旧产物仍实际存在时进入 cleanup ref，不能只把目录指针挪走。
4. 删除 checkpoint 时若模板/warm/sleep 仍用同一 snapshot，保留唯一资源计费并转给持久 holder。多个模板引用同一 snapshot 只计一次；不同 snapshot 即使字节相同仍分别计费。
5. Warm VM 保持未认领，不消耗 sandbox_slot/build_slot；每份独立 vm_disk 计模板 owner 的 D，共享 golden snapshot 已在模板 holder 计 D+M。refill 在 saveOp 前按 ChargeTenantID 检查并登记 pending_storage；无额度不创建 engine 资源，保留有界退避/诊断。

   warm→claim 必须覆盖 admission，而非只在异步 claimWarm 末尾退费：Submit 对 claim 先按 request→pool:<template>→resource-ownership 顺序取锁，再在同一事务读取 ready 模板、选择/占有一个匹配 snapshot 的 warm 行、创建新 sandbox slot，并把已有 vm_disk holder 从 warm 原子转到新 sandbox/operation。只有没选到 warm 时才额外预留 D。事务内不调用 engine/agent；实际健康检查留在取得该 VM 归属后的激活阶段。两个 claim 不得取得同一行。

   坏 warm 的 fallback 通过 warm_fallback.go 原有实例证明和确定 delete key 清理；删除未确认时保留旧 D/slot，不能分配新 VM。确认不存在后在同一账本事务把原 D 转为 fallback 的 pending_storage，避免先退费让其他操作占走额度。重启接续原已选 warm/删除 receipt/预留，不另选另一台。evict/delete 未确认也继续计费。
6. Tenant 首次模板 TargetWarm 必须为 0；已有模板的池目标仅 root 可修改，tenant 更新必须保持现有 TargetWarm。root 可以为明确 tenant 配置目标，但自动 refill 仍遵守该 tenant 存储预算及 engine 全局准入。对拒绝使用现有 Invalid/Conflict/Exhausted/NotFound 类别并写明规则，不新增权限框架。

## 命令

Nushell，从根目录；现有基线和 fixture 在 012 完成后重新确认：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
```

新增 TestTemplateQuota 名称先 `go -C sandboxd test . -list '^TestTemplateQuota'` 检查，以下 focused 门禁不得零匹配。

## 步骤

### 1. 证明构建和模板引用不能绕过配额

新增 `TestTemplateQuotaBuildAdmission`：用 gate 保持第一个构建，第二次不同模板构建超过 slot/storage 时拒绝且 engine create 次数不变；失败/重启不免费，可信删除后恢复额度。新增 `TestTemplateQuotaCheckpointReferenceHandoff`：同租户 checkpoint→一个/多个 template→删除 checkpoint；后续新 checkpoint 不能使用已被模板占用的额度，共享 ID 不双计。

**Verify**：focused `go -C sandboxd test . -run '^TestTemplateQuota(BuildAdmission|CheckpointReferenceHandoff)$' -count=1 -timeout=60s`（GOWORK=off）。至少 build 场景在修复前 RED；若 012 已保守保留 checkpoint 费用，该子场景可先 GREEN，记录原因，不制造失败。

### 2. 接入 build/template/refill 的资源责任

按目标语义修改 service admission、template 发布、pools/refill、claimWarm、evict、recoverResources；保持统一 ownership 锁和事务顺序，不在事务中调用 engine。新资源 ID 只能来自原确定 key 的 receipt；warmup result_unknown 不可换 key 重放。quota_refs 生命周期必须覆盖临时构建与失败的 warm-clone；recovery 删除成功才退费。

**Verify**：步骤 1 GREEN；原 TemplateSnapshot/ForkRollback/RetiredSnapshot/RefillBackoff 回归继续通过。

### 3. 覆盖预热、滚动更新和恢复

新增 `TestTemplateQuotaWarmPoolOwnership`、`TestTemplateQuotaReplacementAndRestart`、`TestTemplateQuotaBackupAndAuthorization`：root/tenant target 权限、refill 无额度退避、warm→claim 原子交接、坏 warm 拒绝/删除失败、替换 golden 后旧资源仍收费、账本备份恢复、外租户 snapshot 拒绝、schema 5/未来版本 DDL 前拒绝。必须增加预算恰为 `(D+M)+D` 的 warm 领取子场景，证明不再预扣第二份 D；两个 claim 竞争同一 warm 时最多一个可复用，另一个按剩余额度拒绝或合法 fallback。对坏 warm 测试删除不确定期间不产生新 VM，可信删除后同一预算可 fallback，重启不重新选择资源。测试采用明确的资源等式更新必要 fixture，不任意放大配额。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./... -count=1 -timeout=360s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
# 通过已核验的既有 SANDBOX_AGENT_FIXTURE 运行四项实际 Go integration；不调用 Cargo。
with-env {GOWORK: 'off'} { ^go -C sandboxd test ./integration -run '^TestRust' -count=1 -v -timeout=90s }
git diff --check
```

实际 production-e2e 的 golden/warm/lifecycle 子流程必须通过。所需 PLAN_* 环境变量只来自明确交付的隔离资源：验证过的 block-I/O 预算、pinned VMM/kernel 材料、该次 OCI 输入和新建 artifact 目录。缺失即 BLOCKED，不查找旧状态。执行精确 Nushell 命令：

```nu
with-env {SANDBOX_PRODUCTION_IO_MAX: $env.PLAN_PRODUCTION_IO_MAX} {
    make production-e2e $"KVM_MATERIALS_DIR=($env.PLAN_KVM_MATERIALS)" $"PRODUCTION_OCI_LAYOUT=($env.PLAN_PRODUCTION_OCI_LAYOUT)" $"PRODUCTION_ARTIFACTS_DIR=($env.PLAN_PRODUCTION_ARTIFACTS)"
}
```

## 完成标准

- [x] 五组 TestTemplateQuota 实际执行，构建、template 引用、warm、替换、重启和备份均覆盖（最终644及独立85/53，来源见下方精确1414批准）。
- [x] 所有资源在 engine 分配前持有额度，ID/holder 交接无双计、无空档，失败仍保留责任（七项源码合同已独立关闭）。
- [x] 租户不能任意提高池目标，warm 不是 sandbox，但其存储有 owner；引擎宿主限制未降低（精确source/材料与fresh grant物理上限保持）。
- [x] schema 6、原授权/未知结果/退避回归、race/vet/lint/integration/真实生产流程通过（精确1414最终644与原始production5RUN5PASS，未改物理限制）。
- [x] 文档与索引记录实际结果和部署边界，无未授权迁移（最终验收/清理与证据限制已归档，013完成后STOP）。

## 012最终源码前置与门禁漂移核对（013设计调和中）

012已在精确 `78bc04fc8715d18319ed4cba69aa92a30975f9e5` / tree `d8a15809daa2ad2f8f6271edc9f400ba955888ac` 完成产品/fixture独立SOURCE APPROVE及一次完整production，父已核验并标DONE。production报告 `/tmp/p012-owner-budget-production-78bc04f/report.md`，make0/137.401s、5RUN/5PASS、13managedVMM/88进程清理完毕；grant已expire、槽FREE。013唯一实现基线为该78，不能只接旧90而丢有限root owner fixture；不带stage2/guest018或重复祖先。当前只做漂移与完整设计调和，不提前运行产品实施或VM。

实际依赖新增：engine Operation.ResultInstance为成功start/restore/clone的不可变结果实例，旧receipt不补写；任何引用此字段的build/warm lifecycle都必须匹配完整frozen request/VMID及真实instance证明，不能由GetVM current值填结果。012 recoveryAllocation只有单一Resolution，无Settled兼容；Progress区分可继续lifecycle与可parentDone，tenant/S/V/B唯一instance_cleanup/原Aintent/unknownkey历史/active与reclaimed同事务责任交接必须保持。writeRecovery及reclaim/backing/slot/terminal dependency在必需写入时核实际效果，SQLite IGNORE不视为成功；013应复用同一模型而非为build/refill新拼费用集合或跳过已知SQL/未知结果边界。

007最终DataReady与relay/preview在外部清理/Agent之后，以configuration guard最终fence核验与连接登记；长engine/Agent/Manager等待不得新增到该短guard中。013新增pool/request/ownership锁仍按目标顺序，不能为了模板/warm额度产生config→sandbox逆序或把engine调用移入SQL。保留全部已批准Configure47sub及原31Ledger（含successBabsence46sub、capture/release-first、acceptedfailed D与rootowner），不重写失败终态或把schema变更当数据修复。

当前b93匹配guest/protocol/Cargo33文件与实际fixture保持，后续使用既有 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f5c61c7/target/debug/examples/unix_fixture` SHA d7fc857eb67e7f651a2857401bf677cc03117eff8a4f03baf272bf14802d41e9，先核source/binary再Go集成，禁止make integration隐含Cargo或借stage2guest018 fixture。fullsandboxdrace的最新精确78 baseline为217.111s/430带Test字段的(package,test)PASS（旧398全部保留），取代旧167.562s基线。新增本项模板/build/warm故障覆盖后将最终fullrace timeout从240调整为300s，保留全部用例/原断言；这仅是Go测试deadline，不改production RuntimeMax/guest/VMM/outer物理cap。接受既有已绑定源码的基线证据，不在改源前机械再跑217s。最终真实production另由父新有效grant/新artifact安排，012所有grant均已失效。

父已实际执行b93→78全部Scope drift stat并读新增fixture关键实现：build完成后原FiniteHTTP断言`build-unclaimed-013`为零账、unclaimed warm只收golden S，正是本项要修的差异，不能要求这些旧缺口断言原样保留。TestQuotaLedgerWarmBackingKeepsSharedCharge使用普通actor创建TargetWarm1、原source D+CP S=256、未计warm D；013须明确root代同owner及按实际D+S+D=320等式更正，不能任意增额度。009 publication gate的0→1 actor改法仍受上述窄Scope约束。schema边界现用storeSchemaVersion+1动态未来值，保留拒绝原4，并由新测试明确5/未来版本DDL前拒绝。

实施前必须完成source-grounded设计方案及新独立冷审：将build/template/warm责任接入012的Resolution/frozen receipts/实际SQL写效果，描述每个原子交接、锁顺序、并发/丢响应/重启/失败cleanup与精确warm预算；特别重算012生产有限预算的适用解释、build_slot/新D/S及maintenance refill，不能继续声称13实现后12D+7S覆盖所有新收费意图，也不能用无限额度/Root传递掩盖。允许保留充分的原有限额度但须给出真实身份峰值/准入证明；若须改额，先列可审核的有限等式。strict fixture多版本/backing及InstallDirect/真实HTTP语义保持。设计只读源，报告在/tmp；无新source/VM权限。

## 013完整设计已交付，新的独立冷审中

父全文读取 `/tmp/plan013-design-t3-252d1250/proposal.md`（含50KiB截断后续）及audit.json。设计基线精确78，617源码/index/12gitlinks/既存reference状态保持，无source编辑/测试/build/VM。提案选择私有typed allocationContext与allocation_group承接build/warm，schema6 build_slot，Submit同事务选warm/新slot/已有D holder转交，旧D→fallback pending同事务，显式专属cleanup与012原collector分派；新状态不加入公开Operation JSON。五组TestTemplateQuota、ABORT/IGNORE/实际COMMIT故障与真实资源身份规格已具体化。

预算拟保留9/13312，移除“覆盖全部收费历史的累计12D”解释，按实际在账11D+7S=12800包络给出512余量，包含永久golden S与一份后继refill D，build D/build_slot须在成功发布前可信退休。该证明依赖目标1的ready+pending+unknown cleanup持久占位及完整成功退休边界，仍须独立冷审；不可直接视为预算批准。新external fixture拟逐轮permit维护gate，单独观察原12D与新增build D/后继refill D，保留24leaf/192+96与所有owner/低额/物理责任边界。

已新派 `review-plan013-design-after-approved012`，独立全文冷审原brief/013计划/012合同、proposal与实际78调用路径、Scope/全局锁序/009 query gate/007 exact-instance guard、canonical collector/unknown结果/SQL实际效果和有限预算。父另直接读Engine提交与执行条件：ExpectedInstance为空时都跳过instance比较；“created且本group从未start dispatch”是否足以安全清理、外部start在GetVM→delete之间的原子身份边界，列为必须给出源据结论的未决挑战，不自行扩Engine/API或猜B。冷审结论前不授权产品实现，当前无VMgrant/slot FREE；012DONE/11项DONE、017等待013。

## 013冷审REVISE：D1已采纳，设计修订中

父已全文读 `/tmp/plan013-cold-design-review-t3-f66831c7/report.md` 与decision.json。唯一阻塞D1/P1：原提案74/135允许created空instance自动清理，Engine admission及execute都仅非空ExpectedInstance才比较，GetVM看到created后external start C可完成，空条件delete会删除C。自己的group从未dispatch start、Config匹配、Service ownership锁均不能封闭该跨组件窗口。B挑战确认不成立；不得扩大Engine/API或以current B补witness。

本计划已正式采用最小修正：取消新013两类空实例自动删除例外；缺成功非空immutable generation witness就保留D/build_slot、group/dependency与ManualRecovery，不能落回generic orphan。可信实际GetVM NotFound可在完整frozen group/已知资源与未知步骤责任核验后仅回收合格账本；不把inventory遗漏/queryerror当absence，也不因此全清unknown SID pending。ManualRecovery是禁止危险动作的诊断/门禁，不是新Resolution/Done/免费状态，后续可信absence观察仍须有合法通路。

G预算获条件性设计认可：固定正常流程中9slots/13312，保守11D+7S=12800，必须build D/build_slot退休、G/W释放真实成功、ready+pending+unknown cleanup持续占目标1池位；不保证无限失败历史活性，不是新runtime证据。C1逐轮inventory park与异步refill完成必须分别观察，C2真实SQLite tx.Commit失败/完整回滚/同连接可用/reopen收敛为必需规格；Engine旧finish_before_commit ErrInterrupted不能冒称SQLite COMMIT故障。C3 identity清单仅组织建议。

已新派 `revise-plan013-design-D1` 在新隔离只读源码工作树输出完整self-contained R2方案、D1/C1/C2/G响应与audit，不改原proposal/产品/父计划、不运行测试或VM。所有collector资格、T-delete/回收进度、reopen/backup/SQL故障/五组测试与closing focus应一致去掉空B例外，新增created-known/failed-start缺witness及external-start-C gated恢复规格：无delete尝试、不采current B、不退款；真实外部删除后按条件一次退休。R2交付后新的独立冷审，批准后才给最终实施合同。当前012DONE/11项DONE，013设计修订，产品实施未开始/无VM授权。

## 013完整R2交付，新独立冷审中

父已全文读 `/tmp/plan013-design-r2-t3-45a594bc/proposal.md`（431逻辑行/62785 bytes、首50KiB后346→EOF续读）、完整response/audit与712行proposal.diff。新提案SHA `1930d294642bf5c8df89e6115dc0fe6b33d967aaddd9b11d34a0c28317e4204f`，完整替代首稿，不能再按首稿74/135的空B例外实施。工作树t3-45a594bc只授权FF78，617源码/index/12gitlinks/10既存reference dirt保持，无产品/测试修改、Go/build/VM或新commit。

R2在全部collector/T-delete/build/failed-start/warm evict/selection-fallback/recovery/backup/fault/测试章节一致要求非空immutable成功B，canonical delete collector无空B身份；私有BuildIdentity冻结已知V/create/source，RecoveryGate仅动作诊断，Resolution仍单一六态，缺B保费/slot/依赖、不可lifecycle/Done但显式扫描可继续观察原key晚receipt或实际absence。可信NotFound只对完整资格known V做eligible D/所有版本/backing/必要slot的原子账务退休，未知capture SID/pending/依赖保持；不猜currentB、不走generic orphan或旧failProvision空delete。external-start-C未来规格检查delete调用尝试为0、C不受影响，外部合法删除后无本Service物理delete而账务一次收敛。

C1明确next inventory park与原receipt/ready row/group destination异步提交分别有界等待，维护gate不持engine mutex观察DB；C2要求实际deferred-FK tx.Commit错误、完整durable与可见内存回滚、同连接可用、移除trigger后原receipt/重开收敛，非ABORT或precommit ErrInterrupted。G保留条件性9/13312与11D7S12800正常在账包络，实际identity原12+X+Q=14D/7SID/9用户S，未移除build退休/GW释放/持久占池位前提。

新派 `review-plan013-design-r2-D1`，携带完整原brief、首稿、D1原冷审、R2全稿/差异/响应及未决关闭要求，独立审D1是否真全局消除且不新增恢复漏扫/提前退休/权限/锁序/Scope问题，确认C1/C2/G及其它原A/C/D/E/F/H合同。R2作者说未决选择0不等于批准；冷审结论前仍无013产品实施或VM权限。012DONE/11项DONE，017等待013。

## R2 DESIGN_APPROVE，正式合同与隔离实施

父全文读 `/tmp/plan013-cold-design-review-r2-t3-19be6bf9/report.md` 与decision.json：DESIGN_APPROVE/blocking[]，D1设计层关闭，绑定R2 SHA1930d294642bf5c8df89e6115dc0fe6b33d967aaddd9b11d34a0c28317e4204f。C1异步交付独立等待、C2实际COMMIT故障/回滚/连接复用、C4 durable op/group/marker判合法lifecycle、C5身份覆盖三主体/slot区分、C6显式扫描与逐资源absence退休，全部纳入 [正式实施合同](evidence/013-r2-approved-contract.md)。G的9/13312及11D7S正常包络保留全部退休/持久占池位前提，非runtime PASS。

父再次执行b93→78全部Scope drift stat，核R2/hash/批准及无active grant，授权单个新隔离executor在绑定工作树FF78后仅Scope实施并自有scoped commit；父dev源码不动/不合并/不推送。先actual build RED，再五组与D1/C1/C2/C4–C6真实Service/SQLite矩阵GREEN，保留原回归，最终source绑定一次300s SB fullrace、vet/正常workspace lint/privileged compile-only、fixture与清理。旧430节点唯一已知版本名称映射为动态future `.../schema-6`→`.../schema-7`，场景及原4/新增5/future的DDL前拒绝必须执行，不造schema6拒绝或静默少测。此为版本推进的验证说明，不改产品合同。

实现交付后须新的独立SOURCE REVIEW，之后再fresh production grant；当前无VM权限，013非DONE，012DONE/11项DONE。

## 首轮准备差异已调和，继续隔离实施

首任务 `implement-plan013-r1` / t3-90582e1d STOPPED，未完成产品、无own commit。父全文读 `/tmp/plan013-implementation-t3-90582e1d/report.md`、audit.json及reference-baseline-comparison：当前进入工作树即9个initialized，cubesandbox未初始化，而历史R2为10；12gitlink OID全等、原9个checkout保持。这不是产品Scope/API障碍。原正式合同误将历史10个当通用数量要求，现已明确改为保全每个新工作树实际初始化集合/HEAD/status，不初始化或恢复其他工作树reference，不因数量9/10差异停止。raw index hash因stat-cache/编辑撤回变化不等staged代码变更，以stage/path/mode/OID语义及Scope外保全为准、raw诊断如实记，不恢复原raw来造相等。

父已独立读取全部617个tracked source并与HEAD78 blob/symlink逐byte验证相等，cached diff空、所有12gitlinks从before/bound/stopped保持。唯一自有增量为未提交starter `sandboxd/template_quota_test.go` SHA2b6b29bd62b409486ac866f956b719f3f9a500f7a805f071fd3cd05fee03499d；schema/私有类型试改已撤回，原工作树/证据只读保留。父解析初始rawJSON确认build slots/storage/low-one三sub及父FAIL、CP handoffPASS，exit1/wall1.047s，真实不同模板超slot/storage和255构建绕过均成立。五组list不是五组完成；后三组只是starter，完整D1/C1/C2/C4–C6矩阵、GREEN/race/full/vet/lint/Rust/privcompile都未完成。

父在 [正式合同](evidence/013-r2-approved-contract.md) 增补准备与index语义、明确导入starter权限，新派 `implement-plan013-r1-resume` 在**新的绑定隔离工作树**FF78，用read/write只导入该starter并核SHA，不写旧工作树、无旧commit可pick；补完全部R2实现与实际门禁，正常自有scoped commit授权不变。若短focused RED为新source/context需重跑可做，不机械重跑217s基线/195sEngine。产品合同/Scope/D1/预算/全部安全及SOURCE与后续fresh production门禁未降低，无需新设计或用户许可。012DONE/11项DONE，013继续实施，当前无VMgrant。

## 首个完整实现d3be730已交付，独立SOURCE复核中

候选 `d3be730e257ad223e3103f1d284e59e15424f7a7`，direct parent精确78bc04f，tree `f5bd17c33db4a753da8244dcf10c8a157120ff69`，worktree t3-64481713 / branch t3/template-build-snapshot-quota-1，唯一owncommit `sandboxd: account template builds and retained snapshot resources`。19路径均在23许可内，+6766/-459；产品9文件、唯一新template_quota_test2859行、既有fixture/docs窄改。父已全文读 `/tmp/plan013-implementation-t3-64481713/report.md`、handoff及最终verification门禁，读service/store/checkpoint/types/lifecycle/reconcile/warmfallback等实际diff；完整源码由新主审独立全量复核。

执行者最终SB fullrace exit0/wall295.815s、509个带Test的(package,test)PASS，focused五组79PASS34.932s，正常workspace lint0/vet/privileged compile-only通过；后者零实际privileged测试。父直接解析最终raw JSON确认509/原430经唯一schema-6→7映射无缺、新79、testFail/testSkip/race0；逐byte核全618源码在当前worktree、门禁前/后/commit后三份副本全部等commit，Engine subtree原样、指定既有Rustfixture SHA正确。四Rust及33inputs绑定/owner24leaf192+96证据交独立审再核；不重复未改295s或195s套件。

历史偏差如实保留：两次主动终止fullrace（约64/142s，exit-15）非PASS，一次真实full009query gate panic（约208s，390PASS）及修复前618源码保留；早期120s timeout/lint/typecheck和少数缺recorder wall/exit前台命令不能补造成功。最终全部必需门禁有receipt/精确source绑定。R1自身C6真实RED unknown-start V absent过早退slot0/storage0→GREEN slot1/storage0直到原key终态receipt；这不自动证明所有恢复边界正确。

已新派两个独立任务：`review-plan013-r1-source` 审完整19路径/R2/全部2859新测试、原始509/430及source绑定，实际focused五组及旧关键回归；`review-plan013-r1-adversarial` 独立用实际Service/SQLite与公开严格Engine flow的/tmpoverlay挑战warm→S后旧refill/group/selection清理、fallbackD→pending与并发release/claim、unknownstart/capture/absence/metadata故障/固定B等，避免重复全部广测。两者均read-only candidate、不修源/不改父计划、无VM权限，只有新test-owned状态。SOURCE APPROVE/DONE尚未给出；13 production待独立批准后fresh grant，012DONE/11项DONE、017等待013。

## A1 P1：warm selection持久删除资格缺校验，REVISE修订中

父全文读 `/tmp/plan013-adversarial-review-t3-a07822b7/report.md`、decision及实际warm_fallback/validateAllocationGroups/selection结构；并独立解析最终probe raw为6PASS/1FAIL、testSkip/race0与overlay SHA吻合。A1具体反例：正常ready warm claim激活失败，首次合法固定B delete transport Unknown留下rejected/dispatched selection；仅在fresh test-owned SQLite清空内嵌Deletion.Request.ExpectedInstance，Close→实际Service.Open仍成功，recoverResources向strict Client发出一次空predicate delete尝试。strict Client先记录再拒绝，故本轮V/B/slot1/storage256/public failed bytes保持，**没有物理误删证明或普通tenant写DB主张**。产品本应startup或replay前ManualRecovery、空条件调用0。这是原R2持久损坏拒绝/固定B合同失败，不是新威胁模型或Scope扩展。

缺口为quota.go validateAllocationGroups只核Steps/Group.Identity，不核Selection内嵌Refill/Identity/Deletion/Stage/Attempt/History；warm_fallback.go已dispatched请求在Find NotFound后直接api.Wait，当前B检查不等于请求资格验证。父采用最小修订：同一完整selection来源/原claim/ownerShape/V/B/immutable成功receipt/完整delete request-key/阶段-marker/terminal-failed历史校验，startup与每次持久marker/receipt replay/dispatch都执行；不一致留责ManualRecovery/Engine delete尝试0，不按currentB或原identity重建坏请求。

新派 `fix-plan013-warm-selection-delete-validation`，新隔离FFd3，只warm_fallback.go、quota.go、必要recovery.go与现有template_quota_test.go；原candidate/worktree/overlay证据只读。需原A1 actual Open/recovery RED→GREEN、startup及Open后runtime corruption多维度负对照、合法无delete即absence/unknown同key/terminal-failed合法history与active/fallback历史正对照，全部原509场景及四独立交错保持。原主SOURCE review仍以d3运行，状态已查working；不能假称其它异议结束，后续发现另调和。

原fullrace295.815s距300s仅4.185s，新startup/replay/history矩阵新增覆盖，正式合同将本修订最终一次SB race deadline调至360s；仅Go测试deadline，不变production时限/guest/VMM/outer/IO/安全断言。修订后source副本绑定、原509+新case、vet/正常workspace lint/privileged compile-only/输入与清理仍必需，Engine未改不重跑195s。交付后新的独立SOURCE round要含原brief/A1/响应/主审未决，再fresh grant生产；当前013非DONE、012DONE/11项DONE、slot FREE无VM授权。

## 主审SOURCE REVISE：R1/R2/R3已并入A1在途修订

父全文读 `/tmp/plan013-source-review-t3-67912afb/report.md`、decision.json及实际independent_test.go、commitSelectionActiveTx；直接解析raw确认五个RED节点（reader1与ready三sub及父）、同一个baseline_test.go父78 PASS/候选d3 FAIL，skip/race0。主审全文19路径/9产品/2859新测试/全部增量，独立79focused和22旧关键回归真实PASS，原509/430/618源码三copy/33fixture证据均复核，但新增三个P2不能被这些GREEN抵消。

- R1/P2正常成功warm claim reader泄漏：active交付只写Stage/Done不退原claim source reader；group finish和active resolver都提前return。target0、释放V、同名新build、反复恢复/重开后旧T reader1/旧SID实际存在，storage384而只应新T192，447额度下新D64claim错误Exhausted。原public claim bytes固定。同一个probe在父78无reader且旧SID NotFound，确认新提交回归。须在完整active目的地/S/holder同TX精确退原reader，并支持合法已交付遗留责任幂等收敛，不退其他真实reader/backing/unknown source责任。
- R2/P2ready warm startup：实际D64→1、缺warm active holder、缺精确SID backing holder三种test-owned DB损坏，真实Service.Open都接受。validator对delivered跳过实际D/refs、结尾只查template。须校真实ready行与原refill冻结owner/完整Shape/SID/V/B/receipt/provenance及D费用/精确holders，不调Engine或自动补账；rolling旧version合法待evict、已claim转S无warm行均不可误拒绝。
- R3/P2强制SQL矩阵未齐：现有deferred-FK COMMIT确实真实；缺new known-S/owner/reader/Resolution、qualified collector/marker及D/backing/slot交接、terminal dependency、新partial多行IGNORE。除absence外尚缺同一物理连接query/普通TX复用，不可只用pool还能用代替。补各新业务族ABORT/IGNORE/真实COMMIT完整回滚/解除后原key私有责任及重开收敛；新key publication-retry是新intent，不能说原failed public变成功。

父已正式在合同纳入三项，并通过 `t3_thread_send(mode=steer)` 把完整main report/证据/响应/未决与原brief约束送给**已有运行中的** fix-plan013-warm-selection-delete-validation；工具返回delivery=steered、原childRun ordinal1不变。这是同一未完成实施轮的用户新信息，不另开并行源码分支/新review轮，Scope仍4文件、parent仍d3。A1和R2共用startup validator、R1/R3共用同事务reader退休，合并统一收敛并避免重复full。四项聚合后新source绑定一次final360s/原509+新增/vet/lint/compile-only/清理与自有commit，后续新的独立SOURCE round携A1/R1–R3全部findings/responses再fresh prod。所有四项当前均未宣告关闭，013非DONE、012DONE/11项DONE、017hold，slot FREE无VM权限。

## 四项合并修订da4b7d6交付，新双独立SOURCE复核中

当前候选`da4b7d6d3efb3eda2dec267e7f1ff7e8570dd8c6`，directparent精确d3、tree`e64bdf34f60fccc2cf42163d6c7c8a4c61a10ae4`，t3-a261ca35/branch t3/validate-warm-selection-cleanup。仅4许可路径+2018/-91；没有Engine/API/schema/DDL/deps变化。父已全文读交接报告和三个产品文件全部diff；selection完整校验、active同事务精确reader退休、ready冻结版本启动核验及SQL矩阵是实施响应，关闭结论仍由新独立审查确认。

父直接解析原始full-race.jsonl：611带Test节点PASS，旧509全留、012旧430全留（既定schema6→7名称映射）、新增102，testFail/testSkip/race0，四actual Rust逐名PASS；原未改adversarial7节点和main两probe文件6节点均PASS。最终full332.951s，618当前源码和full前/后/commit后三份实际副本逐byte等于git提交，canonicalSHA495b19e2969c5d67db9d73ffdf256f8afcdc559d2c2abb4424bcbf80f1399f7f；33fixture inputs逐byte等于git b93/指定fixture checkout/候选，既存binary hash相同。focused181实际74.167s，与最终只差两处格式换行，未冒充相同bytes；静态门禁与privileged零测试编译收据保留，未重复333s/Engine195s/Cargo/Rust。

新派`review-plan013-r2-closure`（全4diff/1441新测试/R1–R3与完整SQL矩阵/aggregate SOURCE）及`review-plan013-r2-validator-adversarial`（原A1、validator/replay及生命周期边界），均独立worktree/新childRun，不复用旧review线程。完整携原R2/D1/四发现/响应/未决；等待实际独立探针和判决，未SOURCE_APPROVE。原d3与证据只读。入口[父核验](evidence/013-da4-parent-verification.json)，完整响应`/tmp/plan013-a1-fix-t3-a261ca35/report.md`及handoff/verification/audit/source-proof。

父HEAD仍b93。另观察到root engine、sandboxd、protocol/gen、operator各go.mod/go.sum共8个未提交变动，来源未归因，原样保留，不纳入候选或恢复。33fixture只用git b93/候选/指定checkout，不能用root当前模块文件代替；该澄清已steer两个在途review。父raw初次路径误写.stdout，改为真实.jsonl后核验通过；末尾root无产品差异断言因8个模块变动失败，没有把整条脚本冒充exit0。当前012 DONE/总11 DONE、017 hold，三份旧VM grant仍false/expired；013尚未DONE，无production权限。

## 新focused SOURCE REVISE A2：Stage缺真实目的地；第二次有界修订已派

父全文读`/tmp/plan013-r2-validator-review-t3-f2a33310/report.md`、decision与finding-stage-proof.txt、完整own_test.go及对应warm_fallback/recovery实际代码，采纳A2/P2。仅将合法rejected selection的Stage改成fallback_reserved，actualOpen接受；恢复分派绕过selection resolver，把无replacement/Identity nil/Allocations空误判为可Done，退休原claim reader1→0；原S仍provisioning/warm-rejected、旧V/B实在、slot1/storage256/public failed bytes不变。重开后即使恢复receipt可见仍不查原delete key，fresh release错误等待不存在的replacement。cleanup_done同一字段startup也错误接受，但其runtime已有目的地guard，正确ManualRecovery/state不变；没有物理误删、提前退款、tenant写DB或生产事故归因主张。

父独立解析own-final.jsonl为17PASS/4FAIL（3leaf+父），原adversarial7+main5合并12PASS，testSkip/race0；probe SHA d3cee324f5e4a761603139470a5bf40f9da1d1d6d2928772d24e867302d5d500核验匹配。合法graph拒绝/Find中篡改拒绝/fallback distinct old-newB→Root0→同名新SID→Backup/Restore/Ack→pause/release/reopen均GREEN，必须保留。原A1空predicate反例在focus关闭，R1/R2原probe支持关闭、R3source/raw矩阵支持补齐；完整A1持久资格仍被A2阻塞，不推定整项SOURCE_APPROVE。

正式合同已加入A2完整条款：中央DB-only stage与原claim唯一provision reservation/完整ownerShape/request/key/Resolution/真实目的地匹配，合格旧warm D各版本/backing/S V-B退休和cleanup Sterminated/clearVM证明，unknown原步骤/reader/Done不能凭标签或空集合丢失。同一SQL TX中目的地写入次序须与集中验证调和，不增永久校验豁免/第二完成标志或新窗口，不重建请求/猜currentB/新key重分配/改旧public terminal/startup调Engine；合法pending/known/active/回收后历史均须保留。

新派`fix-plan013-selection-stage-destinations`，唯一base da4、directparent da4、原4许可路径，无新文件/Engine/API/schema/DDL/deps/VM。先原probe精确RED→GREEN/meaningfulfocused与SQL写序故障，再聚合所有在途主审发现统一format/source副本绑定一次full360s/原611+新增、vet/normal go.work lint/零test privcompile/33inputs/cleanup及自有scoped commit；不重跑Engine195/Cargo/Rust。主SOURCE `review-plan013-r2-closure`实际task_status仍working，已用steer递送A2 report/probe/原响应/未决，工具delivery=steered同childRun1；未宣称主审结束，新的返回可并入同一未完成实施轮。当前012 DONE/11DONE、013 REVISE A2、017 hold、slot FREE，后续新独立SOURCE round再fresh生产grant。

## 主SOURCE已完成：聚合A2/R4，原闭环NO/YES/NO/YES

父全文读`/tmp/plan013-r2-source-review-t3-10b951c8/report.md`、decision、完整missing_context_test.go与对应validator/claimWarmTx/generic resolver代码，直接解析missing-context.jsonl为1FAIL/skip0/race0，核probe SHA297a87c7214c4bd495b2f563057d2fc05e03c694dec42e7cf2b3de355a808ec6。主审现已完成SOURCE_REVISE精确da4，采用peer A2并新增R4/P2；原四项最小反例均GREEN，但完整合同A1 NO、R1 YES、R2 NO、R3 YES。R4交叉影响A1/R2，不重复计两个缺陷；其余完整19路径/四文件及1441新测试已审范围无新阻塞。

R4只删除合法rejected warm claim的整个Group.Selection；其他group/S/refill/operation/fullShape/owner/V-B/ledger/refs/receipt不变，actualOpen接受，recovernil→Done1/reader0，V/B实在、slot1/storage256、公开failedbytes固定，Submit2→2/Find1→1。claimed warm已离warm表，readyRows校验不能看到；nilSelection直接成功，随后空Identity/Allocations被普通收尾当作从未分配。无物理误删/提前退款/tenant写DB或生产归因。主审独立181PASS71.574s、原main6+adv7=13PASS12.087s、新5正对照PASS7.995s、旧关键22PASS11.104s，611/509/mapped430/618三副本/33inputs全部核验，不用这些GREEN抵消R4。

R4已写入正式合同并用`steer-plan013-a2-fix-include-r4-missing-selection`送入**同一正在运行的**fix-plan013-selection-stage-destinations，工具delivery=steered/childRun1。主审不再pending，当前唯一源码轮合并A2+R4，base/parent仍da4、4许可路径不扩。须从原claim/S/冻结refill→sandbox交接及完整durable provenance识别必需Selection，nil fastpath/startup/load/write/replay/terminal前ManualRecovery、完整留责/零Engine；不能只凭当前ProvisionSource/VM、非持久Origin、suffix/新authorityflag或全禁nilSelection。合法build/warm、普通cold/golden非selection、fallback replacement及active后更换SID/Shape/pause-release历史保持，原probe不改GREEN并补runtime去context和正对照。

聚合全部已知发现后最终一次full360s/原611+新增/source618三副本/33inputs/vet/正常workspace lint/零测试compile/cleanup/一个directparentda4 scoped commit，再新独立SOURCE round含原完整brief/四原发现/响应/A2/R4/所有未决。原source/evidence只读，root模块漂移保留；当前012 DONE/11DONE、013 REVISE A2+R4、017 hold、slot FREE，无VM授权。

## A2/R4修订5c交付；原probe矛盾已澄清，新独立复核已派

候选`5c530395dd0452c98f11ef173e788b1d509650f3`，directparent da4，tree66d950b749a02e7c757670a9f9b6ce287f13d735，t3-2760705d/branch t3/fix-warm-fallback-validation；只4许可路径+750/-33。完整交接`/tmp/plan013-a2-fix-t3-2760705d/report.md`及handoff/verification/audit/source-proof。实施和提交完成，执行者保留验收STOPPED：原A2 probe要求runtime拒绝/损坏状态保持后又无条件ledgerRestart要求Open成功，与startup拒绝合同矛盾。父已全文核原probe/helper和失败raw；此前要求该原文件原字节全部GREEN不成立，现采用[oracle澄清](evidence/013-a2-oracle-clarification.md)，不修改产品或原probe/helper/raw、不弱化拒绝规则。

新独立review自有/tmp副本仅将错误restart/新release诊断段改为Close→实际Open期望ManualRecovery→完整只读DB等于before，保留所有runtime/state/reader/费用/publicbytes/V-B/attempts/合法历史断言，另核Find不增。必须独立SHA/diff/实际运行，证明旧stage缺陷仍能检出；不能把原19PASS/2FAIL（1leaf+父）冒充21PASS。原main6+adversarial7+新main5+R4共19PASS，和原A2合并38PASS/2FAIL收据保持。

父直接解析最终635PASS343.254s/旧611、509、映射430全留/新24/testFail-testSkip-race0/四actualRust；618当前条目及full前/后/commit后三实际副本的bytes、执行位、symlink均与git相同，canonicalSHA0f03c0a1e583d82811d52deb8b073aa1e7234954dcd91cb94e4574e41a6beeed，33fixture输入/binary吻合。父已读所有产品3diff；完整4diff/370测试变化及SQL/责任模型由新主审独立全量复核。[父核验收据](evidence/013-5c-parent-verification.json)。

首个长TMPDIR全套exit-15/123.292s/235PASS/11FAIL/3race输出保留；父读栈均定位integration/transport_test203启动失败时bytes.Buffer.String与exec stderr writer。该文件不在13delta，主审独立判定是否预存fixture失败路径及验收影响，不抹去race或宣称所有错误路径安全；短TMPDIR11定点PASS后才执行同源最终635，未改物理限制。

新派`review-plan013-r3-source-5c530395`与`review-plan013-r3-oracle-and-destinations`，均新独立worktree/childRun，provider pi/model gpt-6.1-sol/thinkinghigh已核工具返回。完整携原brief/R2/D1/全部六发现/响应及oracle澄清，不续旧review。前者全修订/SQL及635来源/race定性，后者修正版实际对照与A2/R4边界；不重复未改343s/Engine195/Cargo/Rust。当前实现已交付、oracle标准已明确、源码审查尚未批准，012 DONE/11DONE、017 hold、slotFREE/无VM，parent dev无merge/push。

## 5c主SOURCE REVISE R5：typed holder修订已派

父全文读`/tmp/plan013-r3-source-review-t3-5b541bf0/report.md`、decision、holder_kind_test.go和holder_positive_test.go及charged/active分支实际查询，采纳唯一R5/P2。正常fallback replacement running自然有active_use/sandbox:S与cleanup/sandbox:S两refs，仅删前者一行，actualOpen与recover仍nil，cleanup被只核holder_id的查询当成active证明；全state/publicbytes/slot1/storage256/V-B保持，Submit/Find0，无物理误删或提前退款/tenant写DB/生产归因。父直接解析holder-kind.jsonl为两leaf+父3FAIL，holder-positive.jsonl为1PASS；核probe SHA715c3387b25a9fbd49bd8993ba868c195ed8820a98bec9e1c38daf501bf7e265、positive SHA0f32862bd308013b7ed1f48e90555fdb5d85fc79f07961f7c3a9feab0726fcad。完整闭环A1 NO/R1 YES/R2 NO/R3 YES/A2 NO/R4 YES；所有NO引用同一个R5，旧最小反例全GREEN。

新派`fix-plan013-typed-destination-holders`，新隔离FF精确5c、owncommitdirectparent5c，原4路径不扩。目的地按真实Resolution/producer/生命周期核完整typed quotaRef.Kind+ID、owner/Shape/cost/source/proofs；当前running同V-B必需active_use，collector需cleanup，pending/known/后继terminated或资源已退历史各自有正确责任类型，不能一刀active_use或IN两kind通吃，保自然双引用/单资源只计一次。原R5probe原字节actualRED→GREEN，必要业务负对照/各阶段正对照入现有test，中央留责零Engine/noRepair/immutablepublic规则不变。635+新增/final360s/source618模式副本/33inputs及静态和cleanup门禁仍需；不重跑未改Engine195/Cargo/Rust。

主审已完成REVISE，原对抗审实际task_status仍working，父已steerR5/主结论，delivery=steered/childRun1。新executor先meaningfulfocused，若对抗审另有返回并入同一未完成轮后统一final，避免同文件并行源fix/重复full。主审34focused+19prior+1positivePASS和全部635来源核验保留，其他已审无新阻塞。

父另直接核corrected-probe SHA22bfacf06f9145a3a755fa4f756d93d5452ae7788cd1f1d3fe3187de29060f79/完整diff及raw：corrected40PASS含A2完整21，da4stage1PASS/4FAIL仍检旧缺陷，skip/race0；原19/2不改报。旧长TMP三race被独立主审据四版本同blob和Service.Open前调用链分类既存fixture失败诊断缺陷，非新13阻塞；留作独立测试维护跟进，不扩四路径修它。当前总11DONE、013 REVISE R5、017hold、slotFREE，后续修订还需新独立SOURCE round再fresh生产grant。

## R5独立对抗复核也已完成：无其它阻塞，单一实施轮收齐输入

父全文读`/tmp/plan013-r3-oracle-review-t3-c6984cce/report.md`、decision、完整active-holder-probe.go与ordinary-positive-final.go，直接解析原raw。对抗审独立R5两leaf+父3FAIL，source SHA4e1b9df32ad2dd08eae13a24128b0ba6dd2546176f639e959c499b591b6b3814，结论FOCUS_SOURCE_REVISE精确5c，与主审同一个R5，无其它新阻塞；临时A3别名统一R5，不重复计。双方闭环均A1NO/R1YES/R2NO/R3YES/A2NO/R4YES。原oracle更正40PASS/A2全21及da4 stage1PASS4FAIL与原19/2保留，既存fixture race分类一致。

新ordinary正对照3PASS，SHAc13d3bf21d40faacca2276c5d39dfd6dbe5f776721e6cbdcfb75eed424011c03，cold/golden普通claim无Selection→Open→pause-release→更换S.OperationID→Open/recover保持publicbytes、slot0/storage0或192。未知失败pending引用合法为cleanup，早期假设engine_dependency的未命中属于probe setup错误；普通claim也可能合法没有allocation_group，不能将早期setup断言变成产品要求。typed-holder修订保这些真实producer/生命周期事实，不一律强加active_use/engine_dependency或新group。

父核唯一executor仍working，以`steer-plan013-r5-fix-focused-review-complete`递送peer完整报告/两probe/raw/闭环/合法producer事实及原限制，delivery=steered、原childRun1。两旧review都已完成，该轮无需再等待它们，可将全部已知要求聚合后focused→一次必要final360s/635+新增→source/输入/静态/cleanup/单commitdirect5c，再新独立SOURCE。未另开源码分支、未重复广测；当前仍11DONE/013未批准/017hold/无VM。

## R5修订1414b4e交付，新独立SOURCE复核中

精确commit1414b4e376a70fec9ed9f16b30db588cd38c4b88/directparent5c530395/tree96602c8b6eb902d81687dfab3865f11ec59438b0，t3-0c3b63bb/branch t3/fix-warm-fallback-holder-validation；实际只quota/warm_fallback/现有template_quota_test三路径+367/-50，无recovery或其它禁区变化。完整交接`/tmp/plan013-r5-fix-t3-0c3b63bb/report.md`及handoff/verification/source-proof，工作区干净。中央requireQuotaResource使用完整typed quotaRef.Kind+ID，精确资源slots/cost/source并用EXISTS避免双引用重复计数；pending、known、active、collector、terminatedcleanup、slot/buildslot各按实际producer/生命周期，保自然active+cleanup/unknown pending cleanup/无group普通claim/已退休资源历史。无SQL写序或公开协议变化。

父全文读报告及三文件全部diff/268新测试行，独立解析原raw50外部、9新业务、84producer均PASS；final-full-race644PASS342.713s/原635-611-509-mapped430全留/新9/testFail-testSkip-race0/四actualRust逐名。618当前条目和full前/后/commit后三实际副本bytes/执行位/symlink完全等git，canonicalSHAe94e454d8de9fb60667e98c8a20c6e76886d1d23f679b0f7d993812508ec7780，33fixture输入/binary吻合。原baseR5 actual3FAIL→两套原probe新GREEN；早期50probe无完整618快照的限制如实保留，final格式source绑定producer84/full644。详[父核验](evidence/013-1414-parent-verification.json)。

新派`review-plan013-r4-source-1414b4e`和`review-plan013-r4-typed-holder-adversarial`，独立新worktree/childRun1/providerpi/modelgpt-6.1-sol/thinkinghigh返回已核，完整携原brief/R2/D1/七发现/全部响应/正确oracle澄清/旧失败限制，不续旧review。主审完整三diff/新测试及全闭环/producer-644来源；对抗审独立原两R5与typed后继交接/正确oracle和原正对照。均working，无源码批准结论；未重复342s/Engine195/Cargo/Rust。原A2旧19/2及预存fixture失败race保持原等级，source修订没修其Scope外路径。当前总11DONE，013待SOURCE再fresh实际production，017hold，所有旧grantfalse/expired/slotFREE，parentHEADb93/root8modulepatch保持，无merge/push。

## 1414独立focused已批准，整体主审仍在途

父全文读`/tmp/plan013-r4-typed-review-t3-ac4bfeee/report.md`、decision和新增typed_boundary_test.go，独立解析original50.jsonl为50PASS、boundary.jsonl为3PASS，fail/skip/race0；新边界SHA81363916c1897bb515d5931fb5e0c8aae05ff5d2f8356e8d88eae882f29c306e。FOCUS_SOURCE_APPROVE精确1414/tree/parent吻合，指定focus内七项全YES，无新finding。新增实际pause使S.OperationID变化但旧replacement V-B仍在，先合法Open，再删唯一active_ref，startup/runtime在任何Engine前拒绝、完整state/publicsuccess/slot1与storage448/V-B保持。原50保留两R5/正确A2全21/ordinary及backup历史；未重复644/84/Engine/Rust。R3完整矩阵沿用原已审来源，无新SQL写序；aggregate主审仍拥有全644/copies/fixture来源及整体判定。

父确认主SOURCE仍working，已steer focus完整报告/新增边界和用户最新“做完013就结束”到同一未完run，delivery=steered/childRun1。整体源码批准尚未成立，无VM权限，当前只推进013到SOURCE+fresh生产验收/清理交接完成，然后结束；其它计划/批次集成均暂停，历史017hold不触发后续执行。

## 精确1414双SOURCE已批准，fresh完整生产任务已派

父全文读`/tmp/plan013-r4-source-review-t3-46728132/report.md`与decision，SOURCE_APPROVE/all七项YES/findings[]；直接解析独立typed9+完整build/SQL72+非selection producer4=85PASS、fail/skip/race0。独立focus53已接受，source644/618模式三副本/33fixture全核验。只批准精确1414/tree96602，旧REVISE与oracle/fixture失败历史不反改。

父当前[生产preflight](evidence/013-production-preflight-1414.json)确认13材料hashsize、26guest source及NAR、OCI6reachable与12blob、11工具/83闭包；原88PID-starttime结束/旧unitnotfound-inactive-dead-MainPID0/cgroupScopes/mount无残留，无active生产unit。Btrfs subvolume st_dev错误初断言保留，后按findmnt/sysfs实际父盘259:0核对，不改IObudget；旧4MiB测速是历史，run内fresh精确io.max读回仍必需。

新[唯一fresh grant](evidence/production-verification-013-1414.json)已签给`verify-production-plan013-1414`（task前缀10a8b55d…，providerpi/modelgpt-6.1-sol/thinkinghigh返回已核），仅一次原始make production-e2e/精确1414/全新0700 `/tmp/p013-production-1414b4e`/pinned材料。guest512-VMM640-outer8G-IO2590wbps1048576及owner9slots/13312不改。原adoption/fullLifecycle/golden-warm/realTCP四child与top全部实际PASS、每个managedVMM host+namespace guard证明、所有ownedPID/unit/cgroup/mount清理和显式release必需；archive只stat不open/list/hash，actualDB/config/env/secret/argv/FD bodies禁读，不能extraVM/retry/sourcefix/hostroot/旧资源操作。完成/失败/释放即结束授权，父expireJSON。

当前SOURCE已批准，实际生产结果仍pending，013尚非DONE、总11DONE。生产/清理/证据全部返回且父复核通过才标DONE；按用户最新要求随后STOP，不启动任何其它计划/批次集成/fixture维护，parentdev不merge/push。

## 最终验收通过：013 DONE，已STOP

精确最终提交`1414b4e376a70fec9ed9f16b30db588cd38c4b88`（tree96602c8b6eb902d81687dfab3865f11ec59438b0），生产分支`t3/plan013-production-acceptance`，worktree`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-68d5daed`。feature chain在78bc04f之后为d3be730→da4b7d6→5c530395→1414b4e，最终head包含这些完整自身修订，不只最后R5谓词patch。双独立SOURCE_APPROVE/all七项YES、644全race及85/53独立审查在前文留证。

一次原始make production-e2e exit0/wall142.601168s，raw top+四children5RUN/5PASS/fail0/skip0：adoption3.96s、完整lifecycle42.61s、golden/warm26.97s、realguestTCP45.15s，top137.93s。父直接解析原test.log/exit-status，并从原namespace+host proof配对核13台actualVMM的exe/PID-starttime、65534↔165533 UID-GID、groups空/五caps0/NNP1/seccomp2/root和namespace隔离、每台armedCPU/mem671088640/swap0/pids256/fresh2590wbps1048576、outer8G守卫。没有extraVM/重试/源码测试预算物理限制变更。

父fresh复查89PID-starttime结束/26PGID空、unitnotfound-inactive-dead-MainPID0/cgroupScopes absent/hostmount0；源码618当前及实际pre/post副本bytes/执行位/symlink精确等git，13material重新hash和26/33输入/12OCI-6descriptors/NAR前后保持。archive仅STAT157182239bytes/0644，artifactroot0700，未读任何actualDB/config/env/argv/FDbody/secret或archive内容。显式release已接受、grantfalse-expired/ownernone、禁止重跑。

[父最终接受收据](evidence/013-production-acceptance-1414.json)，完整[原生产报告](/tmp/p013-production-1414b4e/report.md)、[cleanup](/tmp/p013-production-1414b4e/cleanup.json)、[release](/tmp/p013-production-1414b4e/slot-release.json)。限制保留：FF前完整index/refs/reference快照缺口（实际完整快照始于FF后pre-run）；历史IO测速未冒充fresh吞吐，observer PIDns退出-9已wait；父metadata parser纠正数据形状/实际VMMexe+PID-starttime选择，不改变测试或重跑。schema6只新state/旧4-5-futureDDL前拒绝，无自动迁移，独立分支未合并父dev。

当前批次12项DONE，已遵用户“做完013就结束”停止。016/017/020/021、整批集成和fixture维护全部暂停，parentHEADb93/root既有modulepatch和archive改动保留，未merge/push；不提出或启动后续工作。

## STOP 与维护

如果业务必须允许 tenant 自主购买未计计算的 warm 池、root 绕过目标 tenant 配额、或不同 tenant 共用 snapshot，先报告产品合同冲突，不自行扩展字段。后续模板删除/归档必须显式转移最后一个 holder 的计费责任。分支采用T3新绑定工作树分支（建议`t3/account-template-build-warm-quota`，不手动迁移binding）；正式合同已授权仅本项自有scoped commit `sandboxd: account template builds and retained snapshot resources`，不授权父dev提交/合并/推送。
