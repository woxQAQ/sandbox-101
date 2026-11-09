# Plan 021：设计双 daemon 的只读恢复诊断合同

> 当前 PAUSED，最终来源映射未收尾；本 checkout 无 `plans/designs/021-*` 最终交付物。以下设计不代表新的诊断 API/CLI 已上线。恢复工作前按 [Engine 集成](../docs/engine-integration.md#backup-and-explicit-recovery) 和 [Sandbox 恢复模型](../docs/sandbox-architecture.md#4-恢复清理与数据面接纳) 重核实际读取边界。

> 这是 direction 设计/验证计划，不实现自动恢复或生产 CLI。完成交付物验证后更新索引 021；reviewer 管理时交付设计结论。Drift check：`git diff --stat b93fc24..HEAD -- engine/admin.go engine/api/types.go engine/cmd/engined/main.go sandboxd/recovery.go sandboxd/types.go sandboxd/store.go sandboxd/backup.go sandboxd/cmd/sandboxctl/main.go plans/designs/021-recovery-diagnostics.md plans/designs/021-diagnostic-contract.json plans/designs/021-scenarios.json plans/designs/021-validate.nu`，再核对未提交差异。源码路径只读。

## 状态

- Priority：P3；Effort：M（设计/spike）；Risk：LOW；Depends on：无。
- Category：direction；Planned at：`b93fc24`，2026-11-06；Status：PAUSED（历史设计已交付，第三轮读取能力/provenance修订及最终映射复核未收尾）。
- 审计映射：方向选项 2。012/013/017 已落地时按实际账本/工作队列更新证据映射，设计本身不要求先实施这些计划。

## 意图与边界

engine 提供 Doctor、VM.BlockedReason 和 operation，sandboxd 的 LastError、资源补偿、来源授权与计费责任则分散。运维需要区分确认事实、过期观察、未知归属以及备份回退后需要人工复核的部分；单个 phase 不能回答“是否还在运行、谁在负责清理”。

此计划形成在线/离线读取路径、有限诊断 reason code、输出 schema 和证据场景，为后续 CLI/API 实施准备。不会运行 Engine.Open、Service.Open、reconcile、acknowledge、Submit、GC 或任何会接管/清理现场的入口。

## 当前状态与契约

`engine/admin.go`：

```go
func Doctor(ctx context.Context, stateDir string) (Diagnostic, error) {
    // 检查 RECOVERY_REQUIRED 和 metadata.db
    db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbpath)+"?mode=ro")
```

`engine/cmd/engined/main.go` 在创建 Backend/Engine 前处理 doctor mode；serve/import/gc/backup 则会打开 Engine，可能恢复未完成操作，因此不能借它们实现只读诊断。

`sandboxd/recovery.go`：

```go
type resourceRecovery struct {
    // ID、Source、Kind、VMID、Snapshot、Instance、Attempt、Done、LastError
}
```

`sandboxd/types.go` 的 Sandbox 有 State/VMID/SnapshotID/Instance/OperationID/LastError；`engine/api/types.go` 另有 Verified/BlockedReason/Deleting。副作用未知时数据库 phase 只是最后确认状态。snapshot_owners 是授权证据，不等于保活引用；备份不包含 VM 磁盘/RAM，recovery fence 不代表对象都可删除。操作/审计/环境可能含敏感内容，禁止通用 SQL dump 或原样打印所有 JSON。

## Scope

执行者只可新增/修改以下交付物及索引状态：

- `plans/designs/021-recovery-diagnostics.md`；
- `plans/designs/021-diagnostic-contract.json`；
- `plans/designs/021-scenarios.json`；
- `plans/designs/021-validate.nu`，只读校验合成合同与场景。

源码、数据库、现有 daemon、`.local-run/`、已有验收目录全部只读且不作为数据输入。本计划只使用文档中合成的无敏感 fixture，不构建实际部署客户端或修改 CLI/API。

## 必需设计决策

1. 在线通过既有授权 read API 获取库存/已知 operation；结果带观察时刻、来源、是否 verified 和完整性状态，超时/失败为 unknown，不能把失败当空库存。不同 daemon 的视图不是同一事务快照，明确交错风险。
2. 离线只面对用户明确提供的一致 metadata backup 和 receipt，或已停止且可安全只读打开的库。不要对活跃 WAL 库直接使用 immutable=1；mode=ro 也不自动承诺零文件系统副作用，文档必须说明 SQLite WAL/shm 读协调约束。无法保证只读条件时返回 blocked，不调用初始化来补齐文件。
3. 输出按 operation/resource/sandbox 组织 facts、uncertainties、references、quota responsibility、recommended_review。对依赖版本尚不存在的 quota/work 表记录 unavailable，而非推断无计费/无操作。
4. reason code 来自有限枚举，例如 inventory_unavailable、identity_unverified、cleanup_pending、quota_retained、snapshot_owner_unverified、backup_recovery_required、schema_unsupported、integrity_failed。不要透传任意底层错误、命令、env、token/node key、完整请求或内存文件。
5. 不输出 safe_to_delete=true，不提供自动 repair/force/ack 选项。建议只能说明需要核对的证据和对应管理员手册；任何动作仍由原生命周期接口/人工恢复流程承担。
6. schema/version 变动必须显式识别。未知版本仍可报告“无法解释”，不能在诊断中自动 DDL、迁移、创建 marker 或获取写者锁。

## 命令与核验入口

现有只读结构核验（根目录 Nushell）：

```nu
rg -n 'func Doctor|mode=ro|case "doctor"|RecoveryRequired|BlockedReason' engine/admin.go engine/cmd/engined/main.go engine/api/types.go
rg -n 'resourceRecovery|LastError|snapshot_owners|quota_|resource_work' sandboxd
```

下面的 validate.nu 是本计划新增文档校验器，不是现有诊断功能。不运行测试服务、doctor 或真实数据库探测来假装合同已上线。

## 步骤

### 1. 映射权威来源和只读执行边界

设计文档逐个字段注明当前 source symbol/table/API、访问权限、完整性限制、缺失语义、脱敏方式。画清在线 read API 与离线 backup 两种路径；可以用表，不需要构建可视化产品。显式列出禁止调用的副作用入口和 SQLite WAL 条件。解释现有 Doctor 哪些信息可复用、哪些不足，不能声称当前支持跨 daemon 原子诊断。

**Verify**：`rg -n 'online|offline|WAL|unknown|Open|Submit|snapshot_owners|unavailable' plans/designs/021-recovery-diagnostics.md` 必须覆盖这些主题；来源表至少覆盖 engine identity、sandbox binding、operation、cleanup、quota、backup fence 六类。

### 2. 写机器合同与合成场景

contract.json 包含 format=1、kind=design-spike、baseline_commit、finite_reason_codes、field_allowlist、redaction_rules、read_adapters、unsupported_version_behavior。scenarios.json 至少九类：正常一致、engine 不可达、实例变更、代理/策略不符、cleanup 待完成但 quota 保留、备份回退 fence、损坏数据库、未来 schema、活跃 WAL 离线读取受阻。

每类输入为合成、有限事实，不含真实路径/凭据；输出有 expected facts/unknowns/reason codes/actions（动作仅 review 提示）。同一字段不能在某场景里 query error 被转成 confirmed absence。

**Verify**：

```nu
open plans/designs/021-diagnostic-contract.json | get format kind baseline_commit
open plans/designs/021-scenarios.json | get scenarios | length
```

预期 1/design-spike/b93fc24，场景数 >=9。

### 3. 校验合同并写实施交接

validate.nu 检查字段白名单、reason code 枚举、场景 source/verified/unknown 完整性、无 destructive action、无 secret-bearing 字段、未知版本不被当已支持。提供 --self-check，在内存中注入不允许字段/破坏性动作/失败等于不存在的场景，必须被拒绝。校验的是设计合同，不测试生产 helper。

设计最后列出后续实现的具体文件范围、行为测试、线上/离线权限与授权问题、输出格式的潜在公开合同影响。若有 unresolved 的读协调问题，单列为实施前 STOP 决策，不通过扩大只读承诺抹平。

**Verify**：

```nu
nu plans/designs/021-validate.nu
nu plans/designs/021-validate.nu --self-check
git diff --check
```

预期 exit 0，正向验证至少九场景，self-check 证明拒绝非法输入；全程不打开运行状态或写库。

## 完成标准

- [ ] 四份设计交付物完整，六类权威来源及九类情景覆盖，两个校验入口通过。
- [ ] 在线非原子、离线 WAL/backup、未知版本、fence、归属和计费事实均有明确限制。
- [ ] 无秘密字段、无副作用建议、无自动修复；未知状态保持 unknown。
- [ ] 后续实施范围和 STOP 决策可独立交接，Scope/diff 检查及索引“设计完成”记录准确。
- [ ] 不将本计划的设计交付说成生产 CLI/API 已实现。

## STOP 与维护

需要读取真实 token/node key、打开真实 `.local-run`、执行写者 Open/DDL/恢复确认或承诺安全删除时停止。未来改变 schema、账本或 work 队列须同步字段来源和 unsupported 规则；诊断不能成为新的资源权威。分支 `advisor/021-diagnostics-design`；另获提交授权才使用 `plans: specify read-only cross-daemon recovery diagnostics`。
