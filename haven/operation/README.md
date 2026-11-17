# Operation 身份与冻结协议

operation 保存 operation 的公共身份、幂等、终态、通用链接头及 publication/recovery 协议。业务资格与 step/cleanup/deletion 行族查询属于 image、machine、supply、sandbox、collection 各自的私有 sqlc；相同物理表不代表共享业务写权限。

`Command`、`EnvVar` 和 `TemplateSpec` 是跨用例使用的冻结命令结构。`TemplateSpec` 表示命令中的模板规格；它不读取或授权模板 catalog。类型移动保留字段顺序、JSON tags、nil 与摘要，不替换 wire DTO。`EnvVar.Inject` 使用 authorization 的原注入描述值；进入持久请求前仍由业务入口净化 secret。

`AcceptedCommand` 与 `LoadPublication` 保留原冻结结果读取。`Operations` 提供通用状态与等待；sandbox 的凭据核验和 public operation 投递由 `sandbox.OperationDelivery` 承担。

`Committer` 接收显式 Projector，在调用方事务中枚举全部 UnpublishedTerminals，依次投影、Publish，最后 Commit。实际业务投影位于 internal/publication；没有提交后补发或新的数据库/runner。

Step 只承载 recovery 枚举所需的原始字节和通用头；业务 owner 解码自己的 payload。测试的私有意图植入放在 testfixture，不恢复通用业务写 API。迁移与源码核对见 [能力清单](../../plans/evidence/027-capability-map.md)。
