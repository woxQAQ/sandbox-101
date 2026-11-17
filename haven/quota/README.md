# 资源与费用引用

Repository 拥有 resource_refs、quota_refs、quota_resources、reservations 的持久读写、精确 holder 交接、费用与使用量查询。所有业务 SQL 均由领域仓库执行；各业务能力在调用者原事务中协调跨领域责任，不自行重建费用证明。

`Reservation` 保存宿主 bytes、memory 与 CPU 容量；`CostIdentity` 固定 tenant、费用类别、resource key 和最初承担费用的 operation，`Cost` 再带 slot/storage 数量。创建和退休均要求恰好一行。跨 tenant 的 CountCost/CountCostHolders 用于检查全部责任，不能改成只看当前租户而隐藏异常重复归属。

`CapacityUsage` 是节点现有 reservation 的 bytes、memory MiB、CPU 汇总，不表示新的 owner 或释放许可。`ReservedCapacity` 与 `Usage` 在 v9 的整数类型约束下汇总；SUM 溢出仍返回查询错误。fork 预测交接先删除精确匹配的 reservation、再移除预测 holder、最后按原 slots/storage/source operation 调用 `RetireMatchingCost`；实际分支责任仍在同一调用者事务中接替。

`ChargeWarmSlot` 只把指定出生 operation 的零单位 warm slot 改为一个单位，要求原 storage 为零。`MoveCostReference` 只更新 holder ID，`TransferCostReference` 才更新 kind 与 ID；不扩大 UPDATE OF trigger 的触发列。`RetireMatchingCost`、`RetireCost`、`RetireCostAtKey` 分别保留原调用点的金额加来源、来源、tenant/kind/key 条件；调用者仍先完成各自证明，不把较窄删除的条件推断为其他路径的授权。

`SlotBudget` 定义逻辑 slot 的配额类别和单位值，与物理网络 slot 分开；未交付的 warm 实例可以消耗零单位。持久 JSON 顺序和 tags 保持。

`ResourceReference` 是阻止资源不可达的 holder；`CostReference` 是租户成本记录的 holder。二者不是同一责任，删除引用不会删除费用，也不能证明外部资源已经回收。必须写入/移除的引用均使用 store.RequireOne，SQL IGNORE 或竞争导致零行时拒绝交接。

跨领域接纳和责任退休由应用核验冻结来源、当前归属及可信效果证据，在同一事务调用领域 repository。这里不开额外事务，不持有 runtime、总 Service、guard 或推断式回收策略。
