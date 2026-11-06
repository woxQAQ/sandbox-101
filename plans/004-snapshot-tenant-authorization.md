# Plan 004：为模板引用的 engine 快照建立可持久核验的租户授权

> 执行者须按本文完成设计、行为测试和验证，不依赖会话上下文。只修改 Scope；发生 STOP 条件时报告证据。完成后更新索引 004，除非 reviewer 维护索引。
>
> Drift check：`git diff --stat da513a6..HEAD -- sandboxd/types.go sandboxd/store.go sandboxd/service.go sandboxd/lifecycle.go sandboxd/fork.go sandboxd/checkpoints.go sandboxd/reconcile.go sandboxd/recovery.go sandboxd/backup.go sandboxd/service_test.go sandboxd/lifecycle_extra_test.go sandboxd/recovery_test.go sandboxd/http_test.go sandboxd/backup_test.go sandboxd/maintenance_test.go sandboxd/warm_fallback_test.go sandboxd/README.md sandbox-architecture.md deploy/sandboxd/README.md`。同时检查未提交差异。若 001–003 已执行，逐一核对其预期变化，不能恢复旧实现。

## 状态

- Priority：P1
- Effort：L；比单一入口校验更大，包含来源记录、重放及状态格式处理
- Risk：MED；主要影响旧元数据与合法外部黄金快照导入
- Depends on：无逻辑硬依赖；推荐 001–003 后执行以降低共享文件冲突
- Category：security
- Planned at：`da513a6`，2026-11-06
- Status：DONE；含 root 捕获保留源租户及 receipt 恢复回归，独立复核和最终组合验收通过，见 [实施与验收记录](IMPLEMENTATION.md)。

## 问题与目标

普通 tenant 可提交带任意 `Template.SnapshotID` 的 `template_put`。当前发布只验证 engine 快照存在、published、未删除、shape 匹配，没有验证调用方是否被授权引用它。持有效 tenant token、已知另一租户仍可用快照 ID 且 shape 匹配时，可以将其登记为自己的模板并克隆。该结论不假定 ID 可枚举，也不涉及仅保存在宿主内存的秘密。

目标：快照可恢复性与租户读取授权分别验证。`sandboxd` 是授权来源，engine UDS 仍为可信宿主管理接口。默认快照私有；仅允许本租户来源或管理员明确引入到本租户的外部黄金快照。

## 当前代码与设计契约

`service.go:618` 只检查现有模板 ID：

```go
existing, e := getTemplateTx(ctx, tx, t.ID)
if e == nil && !p.Root && existing.TenantID != p.TenantID {
    return o, api.Fail(api.NotFound, "not found")
}
t.TenantID = p.TenantID
```

`lifecycle.go:706` 的来源校验仅为：

```go
snap, err := s.opts.Engine.GetSnapshot(s.ctx, t.SnapshotID)
if err != nil { return err }
if !snap.Published || snap.Deleting || snap.Config.Shape != t.Shape {
    return api.Fail(api.Incompatible, "template snapshot shape or publication does not match")
}
```

`lifecycle.go:156` 将 `t.SnapshotID` 直接交给 engine clone。`reconcile.go` refill 也从冻结模板 clone。即使激活覆盖环境及身份，原工作负载磁盘和恢复状态仍保留，不能当作清洗后的公共镜像。

持久化当前为 schema 1，`store.go` 的 `version` 表与 `openStore` 检查版本。备份复制整个 SQLite，因此新授权表应自然包含在备份内；`backup.go` 的恢复标记目前写 `sandboxd schema 1`。`recovery.go` 已明确说明 clone 的 labels 会继承，不能证明当前 sandbox 归属。

两个必须同步保留行为的现有测试：

- `maintenance_test.go:230` 的 `TestFutureStoreSchemaIsRejectedBeforeDDL` 当前将版本写为 2，并在打开失败后检查版本仍为 2、checkpoints 表没有被重建。升级当前 schema 后必须改用高于新当前版本的值，继续保证任何 DDL 前拒绝未来版本。
- `warm_fallback_test.go:29` 的 `warmFallbackFixture(..., true)` 直接在 fake engine 构造外部 golden snapshot，再以普通 tenant 登记模板。新规则下须改走 root 为目标 tenant 的显式导入，以保持其 warm fallback/重放测试前提；不能绕过授权直接写 owners 表。

相关角色：

- `Principal.Root` 仅由认证得到，根用户可通过现有 `X-Tenant-ID` 选择 tenant。
- `Submit` 事务接纳并冻结模板；`Operation` 终态不改写。
- 生成快照的路径包括 template_build、pause、checkpoint、fork 临时捕获；paused checkpoint 可引用已有捕获。
- `resource-ownership` 锁保护引用发布与删除；表内授权记录不应成为永久存储保活引用。

设计文档 4.1 要求租户仅访问自己的资源，foreign 与不存在同样返回 404。README 允许登记已有 golden snapshot，但没有将所有 published 快照声明为公共资源。

## 选定的最小授权模型

新增 sandboxd 内部 `snapshot_owners` 表，至少保存：`snapshot_id` 主键、`tenant_id`、`source_operation_id`、`source_kind`。`source_kind` 区分 sandboxd 捕获与管理员外部导入，具体名称在实现中统一解释。它是不可变的授权来源记录，不是引用计数，也不提供列表发现接口。

规则：

1. 一个 snapshot ID 只归属一个 tenant；既有记录不可被另一租户的 template_put/upsert 改写。重复同来源/同租户写入幂等，矛盾来源阻断并报告。
2. sandboxd 捕获结果在发布到 sandbox/checkpoint/template/operation 可用引用时同步记录归属。丢失响应后的重放必须根据原 operation 和 engine receipt 恢复同一归属；不能给一个仅由请求提供的 ID 自动认领。
3. 普通 tenant 的 raw SnapshotID 模板登记必须命中同 tenant 的可信归属记录。未知或 foreign 都按相同 NotFound 拒绝，不因 engine 返回形状/存在性差异泄漏信息。
4. 外部黄金快照只能由 root 使用现有 template_put、为明确的目标 tenant 引入。将这次已授权接纳的 grant 与 operation 一起持久化，重启重放不能丢失 root 授权，也不能信任请求体里的伪造 Root/TenantID。
5. 已有其他 tenant 的快照不能通过 root 导入隐式转移所有权。本计划不提供跨租户共享；需要共享时另行设计明确授权，而不是放宽 published 判断。
6. 接纳时校验来源；异步发布/clone 前重新核验冻结快照的授权，覆盖 claim、warm/refill、模板更新和重启恢复。默认拒绝已经存在但来源不可信的模板，不能只保护新写入。
7. 保持引用保活、shape/compatibility/published 检查；通过授权不代表快照可恢复。
8. 授权记录本身不能阻止快照 GC，也不能绕过引用锁。删除物理快照不允许自动将同 ID 的未知对象重新分配给其他 tenant。

### 元数据格式决策

本项目尚未发布，不维护旧内部 schema 的隐式兼容。将 sandboxd schema 提升为 2，并同步初始化/恢复标记和测试；schema 1 状态以明确 Incompatible/ManualRecovery 拒绝正常启动，不自动根据已有模板或 engine labels 批量推断归属。

开发数据重建或旧状态迁移必须在服务停止、完成备份并核对资源后另行处理。本计划不删除旧数据库，不运行 `--init` 覆盖旧状态，不提供自动迁移工具。若维护者需要保留 schema 1 的真实部署状态，按 STOP 条件提交只读迁移设计，不能扩大本计划偷偷迁移。此边界是避免把审计发现之前可能错误登记的快照关系当作可信授权来源。

## Scope

允许修改：

- `sandboxd/types.go`、`sandboxd/store.go`、`sandboxd/service.go`
- `sandboxd/lifecycle.go`、`sandboxd/fork.go`、`sandboxd/checkpoints.go`
- `sandboxd/reconcile.go`、`sandboxd/recovery.go`
- `sandboxd/backup.go`
- `sandboxd/service_test.go`、`sandboxd/lifecycle_extra_test.go`、`sandboxd/recovery_test.go`、`sandboxd/http_test.go`、`sandboxd/backup_test.go`
- `sandboxd/maintenance_test.go`（未来版本保护）
- `sandboxd/warm_fallback_test.go`（golden fixture 的显式授权导入）
- `sandboxd/README.md`、`sandbox-architecture.md`、`deploy/sandboxd/README.md`
- `plans/README.md`（状态）

不修改 engine、guest、token 签名、协议帧、HTTP 字段、egress、operator、配额和新共享/发现 API。不顺手修复模板 ID 并发归属问题；只允许为本计划新增授权记录所需的条件更新。不要安装依赖或操作 `.local-run/`。

## 执行步骤与验证

命令使用 Nushell，从仓库根目录执行。只写服务/HTTP/恢复/备份行为测试，不写单独 SQL helper 单测。

### 1. 建立两租户授权回归

在实际 Service + SQLite fixture 中由 tenant A 生成真实 checkpoint/休眠捕获或 build snapshot，将其 ID 作为测试数据交给 tenant B 的模板登记请求，shape 保持匹配。新增 `TestTemplateSnapshotTenantAuthorization`，覆盖 foreign、unknown、同 tenant 的可信快照、sandbox token、root 为目标 tenant 导入外部快照。

HTTP foreign 与 unknown 都应返回相同 404 body；拒绝时没有 clone/refill 发生，不能只检查最后 claim 失败。当前 fake snapshot Config.Shape 必须与生成 VM 一致，避免因为测试形状错误而提前失败，掩盖授权缺陷。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test . -run '^TestTemplateSnapshotTenantAuthorization$' -count=1 -timeout=45s }
```

修复前 foreign snapshot 场景应失败；其他用例可随模型实现逐步补齐。

### 2. 持久化可信来源与版本边界

新增 schema 2 和 owners 表；同步 `TestFutureStoreSchemaIsRejectedBeforeDDL`，使用高于当前 schema 的版本，并更新其保持原版本和不执行 DDL 的断言。优先引用当前 schema 常量再加一；若尚无常量，至少固定为 3 并明确与当前版本的关系，不能删掉未来版本保护。使用事务 conditional insert，不能无条件 ON CONFLICT 更新租户。为捕获引用发布统一接入归属写入；`snapshot_owners` 不能代替现有资源引用表。

新增 `TestSnapshotOwnershipSurvivesReplayAndRestart`，在 engine 捕获完成、sandboxd 引用发布前中断后重开，验证可从原 operation receipt 完成来源记录，同 ID 无重复 ownership、不因重试换租户。覆盖 template build、pause、checkpoint、fork 临时捕获，以及 paused checkpoint 对已有快照的复用。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(SnapshotOwnership|TemplateSnapshotTenantAuthorization|RecoverResources)' -count=1 -timeout=120s }
```

预期 provenance 与引用始终一致；来源未知不被自动接管。另单独运行：

```nu
with-env { GOWORK: "off" } { go -C sandboxd test . -run '^TestFutureStoreSchemaIsRejectedBeforeDDL$' -count=1 -timeout=30s }
```

预期 exit 0，未知未来版本被拒绝且数据库内容不变。

### 3. 保护 admission、发布与 clone 所有消费点

先调整 `warmFallbackFixture(..., true)`：通过真实 `Service.Execute` 的 template_put，以 root principal 明确选择 fixture 的目标 tenant，完成已有外部 golden snapshot 导入；其余普通 tenant 的请求仍使用原 principal。保留无 golden 的冷回退场景，不能将整个 fixture 改成 root 权限，也不能放宽生产授权或直接播种 owners 表。保留原有分支计数、确定性重放、删除与实例身份断言。

在 admission 事务内验证 tenant 来源或记录 root 外部授权；重放从持久 grant 读取，不能依赖进程内 map 或把 Root 布尔值暴露为请求可控字段。异步模板发布再次验证。

claim/refill/恢复只使用已经授权的冻结 snapshot。新增 `TestUntrustedExistingTemplateCannotCloneSnapshot`，将缺少 ownership 的模板作为测试状态写入，再通过 claim/refill 验证拒绝且 engine clone 次数不增加。新增 `TestRootSnapshotGrantSurvivesRestart`：root 已接纳的外部导入在完成前重启仍能完成；普通 tenant 的同形状请求不能产生 grant。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(TemplateSnapshot|SnapshotOwnership|UntrustedExistingTemplate|RootSnapshotGrant)' -count=1 -timeout=120s }
rg -n 'SnapshotID|Action: "clone"' sandboxd/lifecycle.go sandboxd/reconcile.go sandboxd/fork.go sandboxd/checkpoints.go sandboxd/recovery.go
```

预期测试通过；在交付中列明每个 clone 路径的授权来源，不以 grep 无匹配代替行为验证。必须运行现有 warm fallback 门禁：

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run '^Test(RejectedWarmFallsBackAndKeepsDeterministicReplay|WarmFallbackRecoversDeletionBeforeReferenceCommit|WarmFallbackPreservesChangedInstance)$' -count=1 -timeout=90s }
```

预期三项现有测试通过，golden 与 cold 子场景均保持原有恢复断言。

### 4. 验证备份与旧状态拒绝

新增 `TestSnapshotOwnershipBackupRoundTrip`，执行现有备份/恢复/acknowledge 流程，确认 owner/grant 仍有效，foreign 仍被拒绝。新增 `TestLegacySnapshotOwnershipStateRequiresReview`，打开 schema 1 的受控测试状态时不启动、不创建新来源、不删除任何 engine 资源。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(SnapshotOwnershipBackup|LegacySnapshotOwnership|SandboxdBackup)' -count=1 -timeout=90s }
with-env { GOWORK: "off" } { go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env { GOWORK: "off" } { go -C sandboxd vet ./... }
make integration
make smoke
git diff --check
```

预期 exit 0。文档说明 raw snapshot import 的 root/tenant 规则及 schema 2 边界，禁止建议删除旧状态绕过校验。

## 完成标准

- [x] 六个新增命名行为测试存在且通过；现有未来版本保护和三项 warm fallback 测试均显式执行通过。
- [x] Foreign/unknown snapshot HTTP 404 一致，拒绝发生在任何 clone 前。
- [x] Tenant 自有捕获可用；root 外部导入有明确目标 tenant 且重启后保留授权。
- [x] 归属不可通过模板 upsert、重放、恢复或 clone labels 改写；root 捕获仍归源租户。
- [x] 未授权旧模板不能经 claim/warm/refill 绕过。
- [x] schema 1 明确阻断，不自动删除或推断归属；备份恢复包含新授权状态。
- [x] 高于当前 schema 的版本在 DDL 前拒绝且内容不变；golden fixture 经 root 为目标 tenant 导入，其他请求仍保留 tenant 权限。
- [x] 引用/GC、shape 与恢复兼容性检查仍保留。
- [x] 全包 race/vet、实际 integration/smoke 与 diff check 通过；修改位于获授权计划的 Scope 并集内。

## STOP 条件

- 发现已发布公共合同要求 tenant 导入任意外部 raw snapshot，或实际要求跨租户共享。报告需明确的共享主体和撤销语义，不以“ID 随机”作为授权。
- 维护者要求保留真实 schema 1 状态；先交付只读迁移方案，不执行升级/重建。
- 无法证明某个旧捕获的来源；不能以已有标签、目录名或请求字段为证据认领。
- 需要修改 engine 或新建外部授权服务。
- 两次合理修正后验证仍失败，或需要 Scope 外实现。

## Git 与维护

使用已绑定 checkout；否则建议分支 `advisor/004-snapshot-tenant-authorization`。若要求提交，使用 `sandboxd: authorize snapshot provenance`；未授权不提交、推送或开 PR。

后续增加捕获/导入路径必须同时定义 provenance 写入与重放。共享快照应另设显式授权，不能把 owner 表变成多义的可见性列表。owners 表不是 GC root；不要为保留审计证据而永久保留磁盘数据。
