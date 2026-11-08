# Plan 002：让 fork 的每个分支独立、可恢复地删除 VM

> 执行者须完整阅读本文，运行每步验证，遇到 STOP 条件报告证据。完成后更新索引 002；若 reviewer 维护索引，则只交付结果。本文不依赖会话或其他计划提供上下文。
>
> Drift check：`git diff --stat da513a6..HEAD -- sandboxd/fork.go sandboxd/lifecycle.go sandboxd/recovery.go sandboxd/service_test.go sandboxd/recovery_test.go sandboxd/lifecycle_extra_test.go`。另检查未提交差异；对照下文摘录，不能覆盖其他计划的修改。

## 状态

- Priority：P1
- Effort：S
- Risk：LOW；步骤名必须与恢复依赖枚举同步
- Depends on：无；必须先完成本计划第 1 步的严格 fake，再修复补偿
- Category：bug / tests
- Planned at：`da513a6`，2026-11-06
- Status：DONE；含 maintenance 实例证明和删除响应丢失补充回归，独立复核及最终组合验收通过，见 [实施与验收记录](IMPLEMENTATION.md)。

## 问题与目标

fork 为多个分支调用同一个 operation 的 `release`。`release` 使用固定 `release-vm` 步骤，因此不同 VM 删除请求产生相同幂等键。真实 engine 拒绝同键不同参数，回滚可能只删除第一台 VM，其余分支保留。现有 fake engine 直接返回旧结果，不能模拟这一约束。

目标是让一个 fork operation 的每个分支都有确定且唯一的删除步骤，并在后台补偿、重启和依赖追踪中使用相同规则。一次分支删除失败不能阻止其他分支获得持久的清理责任。

## 当前实现与仓库契约

`sandboxd/fork.go:131`：

```go
for _, id := range o.IDs {
    unlock := s.lock(id)
    b, err := getSandbox(s.ctx, s.store.db, id)
    if err == nil {
        err = s.release(o, &b)
    }
    unlock()
    if err != nil {
        return errors.Join(cause, err)
    }
}
```

`sandboxd/lifecycle.go:568`：

```go
if _, err := s.engine(
    *o,
    "release-vm",
    api.Request{
        Action: "delete", VMID: b.VMID, ExpectedInstance: b.Instance,
    },
); err != nil { return err }
```

`sandboxd/types.go` 的 `stepKey` 组合 `sbx-op:<operation ID>:step:<step>`。因此步骤必须在同一 operation 内标识具体资源。

`engine/engine.go:562` 的真实规则：

```go
if cachedHash != hash {
    return cached.Operation, api.Fail(api.Conflict, "key already belongs to different parameters")
}
```

`sandboxd/service_test.go:47` 当前 fake：

```go
if o, ok := f.ops[r.Key]; ok {
    f.mu.Unlock()
    return o, nil
}
```

fake 在等待 `cloneGate` 后还有一次重复查询，两个位置都必须遵守真实契约。

相关文件职责：

- `fork.go`：并发创建分支、失败后 `rollbackFork`。
- `lifecycle.go`：`release` 标记 terminated、关闭连接、删除 VM 与快照。
- `recovery.go`：`recoveryDependencySteps`、`recoverSandboxResource`、`finishRecoveryAttempt` 跟踪步骤与失败重试。
- `service_test.go`：共享 fake、Service/SQLite fixture。
- `recovery_test.go`：`recoveryTestEngine` 和 `TestRecoverResourcesFailedProvisionCleanupRetriesDurably` 提供故障补偿范例。

设计约束：失败的客户端 operation 终态不可变；后台补偿使用自己的持久 intent。保留 `ExpectedInstance`，未知 VM 身份不得删除。不要通过全局关闭 engine 幂等参数校验“修复”问题。

## Scope

允许修改：

- `sandboxd/service_test.go`
- `sandboxd/lifecycle_extra_test.go`
- `sandboxd/recovery_test.go`
- `sandboxd/fork.go`
- `sandboxd/lifecycle.go`（仅 release 步骤与补偿所需衔接）
- `sandboxd/recovery.go`
- `plans/README.md`（状态）

不修改 engine、guest、egress、正常 fork 身份/环境语义、配额、协议、外部响应格式。只写服务级故障场景测试，不写 `stepKey` 等函数的单独单测。

## 步骤与验证

命令使用 Nushell，从仓库根目录执行。

### 1. 让共享 fake 拒绝同键不同参数

使用与真实 engine 等价的请求编码/参数比较，在 fake 的两次缓存命中处均验证完整请求。参数相同返回原 operation；参数不同返回 `api.Conflict`，不执行新副作用。复用仓库的 JSON 编码惯例，不能只比较 `Action` 或 `VMID`。不引入新的 mock 框架。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test . -count=1 -timeout=120s }
```

预期原有合法幂等测试通过；若发现其他同键参数冲突，保留并报告具体场景，不削弱 fake。需要超出本计划范围的修复时 STOP。

### 2. 添加多分支失败回归

新增 `TestForkRollbackDeletesEveryBranch`，通过真实 Service 创建源、发起至少三个分支，在至少两个分支已经存在 VM 后对指定分支注入激活失败。用 gate/channel 确定时序，禁止依靠 sleep 碰概率。

验证：fork 客户端 operation 失败且终态保持不变；所有分支被终止；每台分支 VM 均收到有效删除；源 VM 存活；回滚快照按引用规则处理。记录每个删除 request 的 key、VMID、ExpectedInstance 并校验不同分支 key 不同，同一分支重放 key 不变。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test . -run '^TestForkRollbackDeletesEveryBranch$' -count=1 -timeout=30s }
```

修复前应失败。不要把 expected conflict 断言改为允许遗留 VM。

### 3. 统一资源级步骤并完成全部分支补偿

统一使用 `release-vm:<sandbox ID>`；可用内部 helper 形成步骤名，所有调用点和依赖枚举使用同一规则。

- `release` 使用资源级步骤，保留 VMID 和 ExpectedInstance。
- `recoveryDependencySteps` 不仅覆盖 action=release，也覆盖 fork 各分支可能发生的删除步骤，避免在途删除被当作无依赖。
- `recoverSandboxResource` 调用 `finishRecoveryAttempt` 时传入相同步骤名；不能继续查询旧字面量。
- `rollbackFork` 在非服务取消错误下继续处理其余分支并聚合错误；每个失败分支保留可被 `recoverResources` 发现的持久责任。成功激活过、清空 `OperationID` 的分支也不能逃过失败 fork 的终止/清理。
- 服务取消时保留可重放意图，不把未清理资源当作成功。

当前内部步骤未发布，不新增旧步骤 fallback。若运行环境有旧未完成 operation，依 STOP 条件处理，不猜测旧副作用。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(Fork|RecoverResources)' -count=1 -timeout=90s }
rg -n 'release-vm' sandboxd
```

预期测试通过，所有产生或读取删除步骤的位置均使用资源级规则；不得遗留固定步骤生成路径。

### 4. 验证删除失败和重启恢复

新增 `TestForkRollbackRecoveryAfterDeleteFailure`：让一个分支删除明确失败，其余分支仍被清理；重启同一 SQLite 状态后恢复清理失败分支，源保持可用。失败的 engine operation 只有在确认终态失败后才使用新补偿 attempt；未知/在途 operation 继续跟踪原键。

新增或扩展 `TestForkRollbackPreservesUnknownInstance`：实例归属不能证明时保留资源并报告恢复错误，不删除其他实例。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'TestForkRollback' -count=10 -timeout=120s }
with-env { GOWORK: "off" } { go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env { GOWORK: "off" } { go -C sandboxd vet ./... }
git diff --check
```

预期全部通过。重复运行用于验证本计划新增的确定性并发场景，不增加无关压力基准。

## 完成标准

- [x] fake 的每次缓存命中都拒绝完整参数不匹配。
- [x] `TestForkRollbackDeletesEveryBranch`、`TestForkRollbackRecoveryAfterDeleteFailure`、`TestForkRollbackPreservesUnknownInstance` 存在并通过。
- [x] 至少三个分支的失败 fork 最终全部终止，源不受影响。
- [x] 所有 release 删除及恢复跟踪使用 `release-vm:<sandbox ID>`。
- [x] 删除单分支失败不阻止其他分支清理；重启后剩余责任继续执行。
- [x] unknown instance 在完整 maintenance 中也不被删除，客户端失败终态不被改写为成功。
- [x] 全包 race/vet 和 diff check 通过，修改位于获授权计划的 Scope 并集内。

## STOP 条件

- 严格 fake 暴露本范围之外的真实契约冲突，需要改动其他业务路径。
- 发现已有部署仍持有使用旧步骤键的未完成操作。报告其数量/类型及迁移方案，不读取或输出凭据，不执行删除。
- 拟议实现需要放宽 ExpectedInstance、清除 engine 历史或修改原客户端 operation 终态。
- 两次合理修正后验证仍失败，或必须修改 Scope 外文件。

## Git 与维护

使用派发时绑定的 checkout；没有绑定时建议分支 `advisor/002-fork-compensation`。若被要求提交，使用 `sandboxd: isolate fork compensation keys`；未获指令不提交、推送或开 PR。

后续批量 VM 操作都必须将资源身份纳入步骤名；修改步骤时同时审查 `recoveryDependencySteps` 和 `finishRecoveryAttempt`。共享 fake 的严格契约不能为了使单个测试通过而关闭。
