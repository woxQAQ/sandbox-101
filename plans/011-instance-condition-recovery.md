# Plan 011：让实例前置条件允许接续同一操作的候选实例

> 逐步执行并验证。完成更新索引 011，reviewer 管理索引时交付结果。Drift check：`git diff --stat b93fc24..HEAD -- engine/operations.go engine/engine.go engine/engine_test.go engine/instance_recovery_test.go engine/INTEGRATION.md`，并核对未提交差异。010 对 restore journal 的持久完成证明是已知前置变化，必须保留；其他实例语义变化先 STOP。

## 状态

- Priority：P1；Effort：M；Risk：MED。
- Depends on：010（执行顺序依赖：先确立候选材料交付边界，再允许接续其新实例）。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（独立SOURCE APPROVE、前置匹配guest门禁已关闭，未合入用户分支）。
- 审计映射：发现 6。

## 问题与目标

start/restore 允许对已停止实例 A 携带 ExpectedInstance=A。操作自身持久化新实例 B 后、完成前崩溃，execute 再比较原 A 与当前 B，误判为过期请求而永久 Conflict。同键只能读原失败，不能走本来可接管的候选路径。

目标是区分“初次 admission 的目标条件”和“已接受操作自身候选的执行归属”。不能跳过真正外部实例变化的保护，也不能把任意最新实例当作自己的候选。

## 当前实现与约束

`engine/operations.go:45`：

```go
if v.VM.OperationID != id {
    err = api.Fail(api.Conflict, "operation no longer owns VM")
} else if want := o.Operation.Request.ExpectedInstance; want != "" && want != v.VM.Instance {
    err = api.Fail(api.Conflict, "running instance changed")
}
```

start 先记录再保存：

```go
o.Journal.Resource = &v.Resource
if err = e.saveOp(o, "launch_intent"); err != nil { return err }
if err = e.saveVM(*v); err != nil { return err }
```

restore 在候选集交接后同样把 `v.Resource=r`、`v.VM.Instance=r.Instance` 写入 VM，再调用 start。`engine.go` admission 已原子检查 ExpectedInstance，并要求 VM 无其他操作；它必须保持原样。设计 I9/I16 要求进程身份、当前 operation 与运行实例同时成立；PID/starttime 与 backend Inspect 是实际归属证明，journal 不能替代。

测试范例：`engine/engine_test.go` 的 `openTest`/`awaitStep`/`ErrInterrupted`、`TestStartupAdoptsLiveInstanceWithoutRelaunch`、`TestRestoreUsesFreshInstanceAndDefaultNetworkBlock`。010 后有候选持久完成证明；本计划不得重新复制已交付的 disk，也不放宽 COW 映射核验。

## Scope

允许：`engine/operations.go`、`engine/engine.go`（仅必要的实例判定组织，不改变 admission 契约）、`engine/engine_test.go`，新增 `instance_recovery_test.go`，`engine/INTEGRATION.md` 和索引状态。

禁止：network policy 放行、Backend 权限/launcher、存储完成证明、捕获协议、snapshot 授权、公共 Request 字段、自动修改客户端 key 或终态结果。

## 命令

Nushell，从根目录执行：

```nu
with-env {GOWORK: 'off'} { ^go -C engine test . -run 'Test(StartupAdopts|RestoreUses|NetworkPolicyInstance|TerminalOperation)' -count=1 -timeout=90s }
with-env {GOWORK: 'off'} { ^go -C engine vet ./... }
make lint
```

新增 TestInstanceCondition 名称先 `go -C engine test . -list '^TestInstanceCondition'` 确认。

## 步骤

### 1. 复现带条件的同操作重启

新增 `TestInstanceConditionStartRecovery` 与 `TestInstanceConditionRestoreRecovery`：先 create/start/stop 取得非空旧实例 A；submit 条件为 A 的 start 或 restore；在新实例已落库、launched、agent_ready 等 checkpoint 中断并重开同一个测试 Engine。assert 原 operation succeeded、VM 身份 B、launch 次数没有重复、默认网络阻断。旧 implementation 应在至少 launched 边界 RED。

**Verify**：`go -C engine test . -run '^TestInstanceCondition(StartRecovery|RestoreRecovery)$' -count=1 -timeout=90s`，放在 `with-env {GOWORK:'off'}` 中。测试循环只使用新 fixture 和测试 backend，不操作真实 VMM。

### 2. 仅允许自身候选接续

execute 保留 operation 所有权检查。只有 action=start/restore、Journal.Resource 存在、候选 VMID 与目标相同、当前 VM.Instance 和当前 Resource.Instance 都等于 journal 候选且资源标识一致时，才允许原 ExpectedInstance 与自身新实例不同。初次 current=A 的路径继续按原条件执行；journal 已写而 VM 尚未切换时，不把旧合法 A 误判为外部变化。

network/network_policy/stop/delete/snapshot 不适用此例外。journal 指向 B、当前变成 C，或 operation 指针变化，仍返回 Conflict/ManualRecovery，并且零额外 Launch/Stop/Delete/Network-on。候选 actual Inspect 不确定时不能标成功，不依据 phase 字符串猜归属。无需保存对未发布 helper 的兼容签名。

**Verify**：步骤 1 变绿；原 `TestUnknownIdentityNeverStopsOrDeletes` 和 `TestNetworkPolicyInstancePreconditions` 保持通过。

### 3. 添加反例与幂等验证

新增 `TestInstanceConditionRejectsForeignCandidate`、`TestInstanceConditionKeepsTerminalResult`：通过测试持久状态夹具设置 operation pointer 改变、journal/VM/resource 三者不一致、真正 stale A、新请求条件不符；必须拒绝且不影响外部实例。原 key 重查参数和结果不变；没有条件的原接管测试保持成功。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C engine test -race ./... -count=1 -timeout=180s }
with-env {GOWORK: 'off'} { ^go -C engine vet ./... }
make lint
make integration
git diff --check
```

预期全通过；文档写清条件针对 admission 原实例、接续必须证明自身候选，不改公开 exactly-once 保证。

## 完成标准

- [x] 四个新增 TestInstanceCondition 测试存在并实际执行。
- [x] 带 A 条件的合法 start/restore 重启完成且没有第二次 launch。
- [x] 外部实例、未知身份、不同 operation、网络动作仍严格拒绝。
- [x] 010 的候选证明、Clone/GC/Snapshot 原回归通过，race/vet/lint/integration/diff check 通过。
- [x] Scope 与索引记录准确，无终态历史重写。

## 实施与复核结果

APPROVE。实现worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-3430e919`/branch`t3/fix-instance-condition-recovery`，010继承tip0bed9ba0614a2bd039d3759db9ff55d69283302d，own5115a9f00c1003f94c6114457551790aca34fd14，treeb0ffa3663cafd111bba450ba0dea7603400f99bb。四Scope文件+521/-3，生产仅18行严格predicate增量；admission/010 proof/GC/launcher不改，openTest仅内部Backend接口注入。

父已完整读全部4文件含498行行为测试。独立review`t3-2d5d68d9`/branch`t3/review-instance-condition-recovery`，自身copy532a82831a6eaee11f265a8909bf90924741d9ca，tree一致，findings=[]。独立20组focused Engine race71.722s、生产CloudHypervisor.Inspect实证的3组overlay25场景19.269s、vet/lint0issues/四actualRustfixture/diff均过；旧源码overlay独立复现11个RED边界，最终GREEN。before-VM/current-B的未知PID/starttime、010 proof损坏/被替换backing inode和不同operation/target都严格拒绝，无额外Launch/Stop/Delete/Network-on，原终态编码字节不变。审查首次误要求仍合法stopped A也必须unverified的过强断言已纠正并保留记录，不改生产源码。

完整required-native race由执行者在相同最终tree实际175.704s/engine173.374s/180s timeout通过；独立review解析完整JSON确认100顶层及318子场景PASS、零testskip/无DATA RACE（api包无test另列），没有冒称自行重跑全套。010新匹配guest双格式+6actualInfo及完整清理证据已复核，前置hold解除。本项未运行VM、无生产isolation新声称。完整 `/tmp/p011-review011r1-2d5d68d9/report.md` 和 `/tmp/p011-3430e919/report.md`。最终组合已有010时只接本项5115a9f，保留用户dev不动。

## STOP 与维护

不能证明 journal 候选与当前资源是同一实例时停止；不能用“已有 journal 就跳过 ExpectedInstance”修复。若 010 的阶段定义无法区分准备与已 launch，先修订计划，不新增第二套存储阶段。后续引入重启/resize 动作时分别定义是否允许自身后继实例。可用分支 `advisor/011-instance-recovery`；另获提交授权才使用 `engine: preserve instance fencing during operation recovery`。
