# Operation 身份与冻结协议

operation 保存 operation 的公共身份、幂等、终态、通用链接头及 publication/recovery 协议。业务资格与 step/cleanup/deletion 行族查询属于 image、machine、supply、sandbox、gc 各自的私有 sqlc；相同物理表不代表共享业务写权限。

HTTP 严格解码 `types.Command`。`NewIntent` 复制公开字段，宿主恢复与配置工作流可另外设置 `ObservedGeneration`。`Intent` 保持原 JSON 字段顺序，以便同键重试能核对已存 MAC。环境值与注入描述使用 `types.EnvVar` 和 `types.Injection`，进入持久请求前由业务入口净化 secret。

`AcceptedCommand` 与 `LoadPublication` 保留原冻结结果读取。`Monitor` 提供通用状态与等待；`sandbox.OperationViews.Get` 按调用者权限返回 operation 视图。

`ValidateDelivery` 按成功动作检查必需对象。claim 和生命周期命令交付对应 sandbox，fork 交付与冻结 ID 顺序一致的所有分支，checkpoint 创建交付 checkpoint，模板发布与 refill 交付模板。checkpoint 删除允许对象已解除绑定。提交投影、读取原 publication 和启动核验共用此检查。失败操作保留已获得的部分结果。

`Committer` 接收显式 Projector。工作流在自己的 SQLite 事务中核验业务结果后，用 `Complete(tx, ...Outcome)` 显式指定本次完成的 operation ID 和结果：Committer 统一写终态与完成时间，并为携带 credential 或 activate step 的 operation 在同一事务快照内投影并冻结 publication。一个事务可完成多个 ID；父结果依赖子 publication 时，子结果已由各自事务先行冻结。事务所有者在全部步骤成功后一次提交，任一步失败回滚整个事务；不再在正常提交时扫描全库寻找待发布终态。启动恢复仍核验历史缺失或损坏的 publication 并明确拒绝。实际业务投影位于 internal/publication；没有提交后补发或新的数据库/runner。

Step 只承载 recovery 枚举所需的原始字节和通用头；业务 owner 解码自己的 payload。测试的私有意图植入放在 testfixture，不恢复通用业务写 API。迁移与源码核对见 [能力清单](../../plans/evidence/027-capability-map.md)。
