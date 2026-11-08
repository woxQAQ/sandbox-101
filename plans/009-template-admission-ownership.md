# Plan 009：在模板首次 admission 时持久化不可变租户归属

> 按步骤和门禁执行，失败或越界时 STOP。完成更新索引 009；reviewer 管理索引时交付证据。先执行下列 drift check，再检查未提交差异；仅接受已声明前置计划的已知改动，不覆盖其他工作。

```nu
git diff --stat b93fc24..HEAD -- sandboxd/service.go sandboxd/lifecycle.go sandboxd/store.go sandboxd/types.go sandboxd/service_test.go sandboxd/recovery_test.go sandboxd/maintenance_test.go sandboxd/warm_fallback_test.go sandboxd/backup_test.go sandboxd/template_admission_test.go sandboxd/README.md sandbox-architecture.md deploy/sandboxd/README.md
```

## 状态

- Priority：P1；Effort：M；Risk：MED；Depends on：007（保留配置 fence 和 schema 3）。
- Category：security；Planned at：`b93fc24`，2026-11-06；Status：DONE（第二轮隔离分支复核通过，未合入用户分支）。
- 审计映射：发现 4。012 按本计划 schema 4 顺序接续。

## 问题与目标

两个租户可在同名模板首次发布前分别提交构建，admission 均看到模板不存在，异步执行锁不能阻止后来已接受的 operation 覆盖前者归属。影响是模板目录身份，不是绕过独立的 snapshot_owners。

目标是模板名在首次接纳时就归属一个租户，后续发布只能由该归属下当前操作完成。失败／重启不使名称转给另一租户；既有 ready 模板滚动构建期间继续提供原版本，避免把“正在发布的意图”混成“已发布模板”。

## 当前实现与约束

`sandboxd/service.go` 的模板 admission：

```go
existing, e := getTemplateTx(ctx, tx, t.ID)
if e == nil && !p.Root && existing.TenantID != p.TenantID {
    return o, api.Fail(api.NotFound, "not found")
}
// 最后仅 saveOp(ctx, tx, o) 和 tx.Commit()
```

`sandboxd/lifecycle.go:813`：

```go
"INSERT INTO templates(id,tenant,body) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET tenant=excluded.tenant,body=excluded.body"
```

`store.go` 当前 `storeSchemaVersion=2`，打开旧 schema 在 DDL 前拒绝；`backup.go` VACUUM INTO 完整数据库。`service.go` 的 admission 已持 resource-ownership 锁并开启单写 SQLite transaction；不能在其中调用 engine 或重新开启 db 查询。`lifecycle.go` 模板执行持 template:<id> 锁。设计 P12 外租户和不存在资源返回相同 NotFound；snapshot owner 为独立不可变授权。

测试匹配 `TestTemplateSnapshotTenantAuthorization`、`TestSnapshotOwnershipSurvivesReplayAndRestart`、`TestFutureStoreSchemaIsRejectedBeforeDDL`；使用实际 Service/SQLite，以 agent/engine gate 阻塞构建，不用调度 sleep 模拟竞争。

## Scope

允许：上述 drift check 中全部路径，新增 `template_admission_test.go`；文档仅模板 admission／schema 说明，索引仅状态。`types.go` 仅内部 Operation 字段。

禁止：engine/guest/egress、模板 public API 字段变更、跨租户共享或转移、自动删除旧状态／模板、修改 001–005 历史计划。不要以公开化模板或随机改名绕过竞争。

## 目标数据与协议

新增 `template_intents(template_id PRIMARY KEY, tenant_id NOT NULL, operation_id NOT NULL)`：行同时是永久 owner 占位和当前发布意图，operation_id 可为空表示没有在途发布。名称归属不随失败释放。

- Admission 同事务核对 owner、已发布模板 tenant 及当前 operation；首个 owner 占位和 saveOp 一起提交。其他租户仍统一 NotFound，root 仅能为明确同一目标 tenant 更新，不隐式转移归属。
- 同租户存在 pending/running 发布时返回 Conflict。当前指针指向终态操作时允许新操作占用，终态结果不改写；指向丢失 operation 则 ManualRecovery。
- 发布在 ownership 锁内，以 template_id/tenant/operation_id 三者条件核验，并在同一事务更新已发布 templates。零匹配不能 UPSERT 覆盖，须明确 Conflict/ManualRecovery。
- 成功／失败可清除当前指针，但只能条件清除自己的 ID。崩溃后同一 operation 可接续，不能由旧结果清空新 intent。
- 首次失败保留归属行，Templates 列表不必把 intent 伪装成 ready。已有 ready 版本在新构建中继续可 claim。

007 的依赖契约是 configuration_fences 持久保护失败配置，schema 3；本计划保留该表和自动激活前的 fence 检查，将内部 schema 升为 4，旧 schema 3 明确拒绝且 DDL 前不修改。这里只提供拒绝说明，不提供自动迁移；不得对真实状态执行 --init。若已有其他 schema 变动，STOP 协调顺序。

## 命令

根目录 Nushell，现有基线：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
```

下述 TestTemplateAdmission 名称为新增，先 `go -C sandboxd test . -list '^TestTemplateAdmission'` 确认。现有 schema fixture 断言要按当前常量构造 future version，不能移除保护。

## 步骤

### 1. 用双租户 gate 复现归属竞争

新增 `TestTemplateAdmissionOwnerReservedBeforePublication`：阻塞 A 首次构建，B 同名提交必须 NotFound，与确实外租户已占名响应一致；A 放行后发布仍属 A。另测两个同租户不同 key 的并发首次构建，第二个 Conflict，同 key 返回原 operation。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -run '^TestTemplateAdmissionOwnerReservedBeforePublication$' -count=1 -timeout=45s }
```

修复前拒绝断言 RED；锁/gate 均在 cleanup 放行，避免测试结束死锁。

### 2. 实现持久占位与条件发布

按目标协议在 store.go 定义表、service.Submit 核对和占位；不在事务中执行 engine。lifecycle.template 发布用事务核对当前 intent，ready 表更新不再写入任意 tenant。沿用 api.Fail、getTemplateTx 和 encode，不另建权限框架。run 的终态清理若无法在 lifecycle 内安全安排，可在下一次 admission 根据终态条件回收指针，不扩大 service 状态机。

**Verify**：新增 admission 测试变绿，原 `TestTemplateSnapshotTenantAuthorization` 通过。

### 3. 覆盖失败、重启、备份和旧版本

新增 `TestTemplateAdmissionRestartAndFailedBuild`、`TestTemplateAdmissionPublicationFence`、`TestTemplateAdmissionBackupPreservesOwner`：失败后 B 仍不可占名，A 新 key 可重试；旧 operation 不能覆盖/清空新 intent；重启保留同一 owner；备份恢复仍拒绝 B，且保持 recovery fence。覆盖 ready 模板构建中继续 claim 原快照。schema 3 fixture 启动拒绝且 DB 原字节/表集合不变，schema >4 同样拒绝。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
git diff --check
```

预期全部通过；同步更新两份模板文档及部署 schema 边界，不加入迁移命令。

## 完成标准

- [x] 四个新增 TestTemplateAdmission 测试存在并实际执行。
- [x] owner/intent/operation 同事务接纳，发布和清指针均有归属条件。
- [x] 首次失败、崩溃、备份恢复和同租户滚动版本均满足上述行为。
- [x] snapshot 授权、future schema、unknown instance、旧回归保持有效。
- [x] race/vet/lint/integration/diff check 通过，Scope 与索引验证结果完成。

## 实施与复核结果

第二轮 APPROVE。最终 worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-eac74176`，分支 `t3/fix-template-admission-invariants`；基准 `b93fc24`，007继承 `72f111b`→`d637bb6`，本项 `86046a2`→`c3c21abe136062c72cbcaf520f2bc1f987ff0ef8`。最终组合已有007时，只接009自身原提交 `6521ef6e2eb437c4efc1da8e92289d6dd428e310` 与 `c3c21ab`，不重复007副本。

首轮真实双租户构建 gate 复现原实现越权接纳 RED→GREEN；父完整读取生产增量与557行行为测试并重跑全部门禁。复核要求完善缺失/冲突owner、pointer来源/未知状态；对发布锁最初“缺失”的判断经完整函数核对已更正：锁原已存在，实际问题是跨越Engine.GetSnapshot。第二轮将外部查询移出ownership guard，snapshot owner核验与发布留在guard+SQLite事务；没有内部owner兼容重建。

父读取全部第二轮生产增量与376行新增测试：八组 TestTemplateAdmission 包含11类坏pointer、owner/foreign/root目标身份、持锁与engine gate、原终态字节不变及原四组生命周期。父重跑sandboxd全race（38.504s）、vet、lint（0 issues）、真实Rust fixture integration（0.719s）与diff check，全exit0。schema4/007配置fence/固定configure终态/旧库DDL前拒绝保留，012可接续；未合入用户分支。

## STOP 与维护

若发现已发布合同承诺 root 可直接跨租户转移模板、需要对真实 schema 2 自动迁移，或需改变 engine 快照授权，先报告，不自行保留内部兼容旁路。新增模板删除功能未来必须明确永久 owner 占位和名称复用规则；本计划不引入该功能。

使用绑定的 checkout；可选分支 `advisor/009-template-admission`。另获授权提交时使用 `sandboxd: reserve template ownership at admission`。
