# 执行状态与共享 guard

`execution` 只管理进程内执行和互斥，不读取数据库、不选择恢复策略，也不调用 Loom。一个应用实例组装一份 `Lifetime`、`Workers` 和 `Guards`，各业务服务接收所需的能力；不能为每个服务另建同名锁域。

## Lifetime 与 worker 完成

`Lifetime.Begin` 与 Close 的关闭标记受同一 mutex 保护：关闭后不再增加在途计数。每次成功 Begin 必须匹配 End。Close 先关闭接纳并取消运行 context，在 mutex 外撤销连接，等待在途工作，然后调用唯一的最终关闭逻辑。调用方等待超时不会放弃后台收尾或释放 writer lock。

`Workers.Start` 对同一个 operation ID 拒绝并行重复执行，但允许已结束的 operation 再次派发以接续恢复。它在启动 callback 前登记 Lifetime custody；callback 返回后一次性发布 `WorkerState`，再退出 custody。工作流必须在返回之前释放自己的 guard 并完成 HEAD 更新。

```mermaid
sequenceDiagram
    participant S as 业务服务
    participant W as Workers
    participant L as Lifetime
    participant F as 工作流
    S->>W: Start(operation ID, callback)
    W->>L: Begin
    L-->>W: 接纳或拒绝
    W->>W: 登记 Active
    W->>F: 执行 callback（注册表锁外）
    F->>F: 持久交接、HEAD、释放工作流 guard
    F-->>W: 返回诊断
    W->>W: 原子发布完成快照
    W->>L: End
```

`WorkerState.Active` 表示当前 runner 在途；`Completed` 区分“从未执行”与“已执行且 Err 为 nil”；`Err` 是最后一次返回的诊断。在新一次 runner 活跃期间，先前的 Completed/Err 仍可见，调用方必须同时检查 Active。这些字段不替代持久 operation 终态、授权或资源证明。公开终态与 worker 完成是两道条件，等待服务不能因为数据库已经提交成功就提前越过仍在收尾的 runner。

`Workers.Context` 仅返回同一 Lifetime 的运行 context；业务服务不另建取消源。`AdmissionGate` 借用原登记 mutex、Closing 读取能力和不可变 reviewed 状态，在原短互斥下检查能否接纳。它不登记工作、不另持关闭状态，也不替代调用方保护持久状态变更的 `ForStoreMutation` 锁。Images、VMProvisioning、Lifecycle、Supply、Forks 等接纳使用同一屏障。

## Guards 的锁域

`Guards` 用独立的种类、owner、ID 字段定位同一实例内的共享 guard，不使用业务代码拼接字符串前缀。条目在进程生命周期内不驱逐，防止已有持有者与新调用者得到不同 guard。返回的 `Guard` 仅提供 Lock/Unlock/TryLock，实际 mutex 与 map 保持私有。

| 能力 | 保护范围 |
|---|---|
| ForTenantIdempotencyKey / ForNodeIdempotencyKey | 分别按 `(tenantID, key)` 和 `(nodeID, key)` 串行化幂等查询与接纳 |
| ForVMLifecycle | 同一逻辑 VM 的启动、停止、捕获、恢复、网络变更、删除及完成核验 |
| ForSandboxConfiguration | 同一 sandbox 的 settings、binding、撤权与最终数据交付 |
| ForSandboxLifecycle | 同一 sandbox 跨 VM generation 的生命周期推进与就绪检查 |
| ForSandboxWakeup | 同一 paused sandbox 上并发数据请求触发的透明唤醒 |
| ForTemplateSupply | 同一模板的构建、补池与清理 |
| ForImageImport / ForImageCleanup / ForGCOperation / ForForkBatch | 按 operation ID 串行推进对应工作流 |
| ForSupplyCleanup / ForSnapshotDeletion | 分别按候选 VM ID 和 snapshot ID 串行回收 |
| ForStoreMutation | 节点内的持久接纳、责任交接、tenant 写入及最终 store 关闭 |
| ForServiceAdmission | 连接登记、drain 与关闭接纳之间的短临界区 |
| ForImageArtifacts | 镜像发布、清理与 GC inventory 之间的互斥 |
| ForGCAdmission | 节点内的 GC 接纳与 inventory |
| AcquireCaptureSlot | 等待并占用唯一的 capture 名额，等待可取消，成功后由调用者释放 |

`For…` 方法返回共享锁，由调用者决定加锁顺序、持有范围以及使用 `Lock` 或 `TryLock`。长 syscall、RPC 和 hash/copy 在 `ForStoreMutation` 的临界区外执行。`AcquireCaptureSlot` 成功时已经取得名额，调用者在捕获保护范围结束时调用返回的释放函数。

Dataplane 与 App.Close 借用同一 `ForServiceAdmission` 锁，保留连接登记和关闭快照的原短互斥；mutex 本身由 `Guards` 唯一持有。draining 决策属于 `execution.Availability`，查询和 Drain 使用同一 guard。关闭标记不能单独替代最后一次接纳/登记与连接快照的原子协议。`ephemeral.Registry` 生成并持有唯一 daemon epoch，activation 和 data 服务使用同一不可变值。
