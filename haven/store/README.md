# Store lifecycle、migrations 与 sqlc

`State` 是 writer lock、共享数据库、node key、HEAD 和关闭顺序的唯一所有者。普通打开、备份及恢复都使用显式 `Validation`；publication 与 workflow 两个回调缺一时，在访问 state 目录之前拒绝。业务规则由 `internal/recovery` 组合，只接收数据库和不可变的 `Origin`（目录与 review 状态），不持有运行中的总 Service 或 runtime。

已有 state 继续按 immutable probe、私有 WAL probe、HEAD/业务证明核验、可写连接的顺序打开。只读阶段的回调拒绝不会触发 dispatch、补写 witness 或创建原 state 的 SQLite sidecar。publication 结构校验仍会在打开可写连接后重复执行，保留原覆盖范围；验证回调必须保持纯读取。备份读取使用已核验的 node key 检查 journal HMAC 链；review 只允许元数据诊断，不授权新效果。`haven/internal/application/store.go` 仅为普通与离线入口组装同一验证器；App 直接持有 State，没有第二层 store adapter、key、锁或数据库。

`Workflow` 只借出 Database（query 与 BeginTx）、不可变 Directory/Reviewed 和 Checkpoint 函数。它没有 Close、key 或 writer-lock 能力；各用例仍使用同一个 State 和共享数据库，不能把它当作第二个生命周期所有者。

发布前 schema 版本固定为 **0**。表、约束和索引直接维护在 `migrations/000000_baseline.up.sql`，运行时通过 embed 读取，sqlc 读取同一份 SQL。`operations` 每行保存创建时间，提交终态时在同一条更新中写入完成时间，epoch 由 `Checkpoint` 在写 HEAD 前推进。

`Migrate` 在 writer lock 下检查 SQLite `application_id`、`user_version` 和库内对象。新 SQLite 文件的 `user_version` 默认为 0，只有 application ID 为 0 且没有用户对象的库才进入初始化。schema、Haven application ID、store identity 以及 Goose 的零号记录在同一事务提交；初始化失败时一起回滚。成功后 State 执行 checkpoint/HEAD 协议。

已有 state 在只读 probe 中核对 Goose 版本记录、store.json、DB identity/version、HEAD、publication 和 workflow proof。普通启动拒绝旧原型及不完整格式，保留现场供人工恢复。

`HasForeignKeyViolations` 为启动结构验证和 GC 的只读事务提供 normalized reference 检查；PRAGMA 由 store 持有，query/row/close 失败保留未知，不解释为不存在引用。

store 自有 journal、metadata 初始化、身份读取与 epoch 查询也通过本包 `internal/dbgen` 生成。metadata 初始化使用原事务；版本按完整整数比较，不窄化后接受错误版本。`AppendAudit` 在调用者业务接纳保护内执行事务，核验追加恰好一行，提交后 checkpoint；HEAD 错误不会被解释为事务回滚。

手写维护 SQL 限于以下操作；新增操作需要审阅其事务、归属和恢复语义：

| 位置 | 允许的维护操作 |
|---|---|
| `state_linux.go:initialize` | store identity 元数据初始化、`wal_checkpoint(FULL)` |
| `state_linux.go:validate` | quick_check、application_id/user_version、`sqlite_master` 的 trigger 检查 |
| `migrations.go:Migrate` | SQLite header、空库检查、嵌入的 v0 baseline 和 Goose 零号记录 |
| `validation.go:HasForeignKeyViolations` | foreign_key_check |
| `backup_linux.go:backup` | VACUUM INTO |
| `statements.go` | 原 bounded statement cache 的 PrepareContext/QueryContext/QueryRowContext 委托 |

其余静态 SQL 必须经所属包的 queries.sql/dbgen；用例、controllers 和能力非生成代码不能通过 raw Query/Exec/Scan 绕过私有 sqlc。

领域查询位于各领域的 `queries.sql`，由 `haven/sqlc.yaml` 配置生成到该领域的 `internal/dbgen/`。生成结构不越过领域 API；事务内操作绑定原 `*sql.Tx`，必需单行写仍核验影响行数恰好为 1。

`templates` 指向当前 `template_versions`，标签按版本存入 `template_version_labels`。`pool_slots` 的 ready 索引服务于选择，`warm_claims` 固定原 refill 和资源操作。重启时从这些历史记录、接纳请求和 `activation:warm` 的设置重建 claim，再核对接纳时的摘要。模板替换、slot 退休和当前 VM 归属转移都遵循这条历史链。

`operation_steps` 保存步骤身份、候选资源、期望实例、状态和阶段。`resource_links` 固定父步骤与子操作。`operation_steps.input` 保留需要按字节核验的步骤输入。关联型步骤按保留的关系重建输入，读写跨表时共享 SQLite 事务。资源、capture、fork、镜像导入和 GC 各自保存进度；GC 按序追加回收结果，删除流程分别保留原始文件树与当前路径的回收权。

`operations` 保存类型化结果，`operation_result_sandboxes` 按位置保存交付顺序。终态事务在 `operation_publications` 固定响应摘要和模板版本，同时写入当时可见的 sandbox、网络规则及 checkpoint 字段。重放在同一个读快照中结合原请求与终态结果重建响应，并核验该摘要。资源的 launch observation、恢复文件身份、capture 边界及镜像导入证明均由列和子表保存。

`operations` 固定作用域、幂等键、动作与请求种类。资源目标、镜像导入选项、GC 参数及公开命令分别写入请求明细表。命令的模板、环境、策略规则和注入 host 随同 operation 原子写入，列表按位置恢复。同键重试比较接纳时的字段。`journal.signed_payload` 保存 HMAC 覆盖的原字节。

`tenants.max_sandboxes` 和 `tenants.max_storage_mib` 是非负整数列。`authorization.Repository` 在同一条 UPSERT 中写入 token hash 和两项预算。认证 sandbox token 时只查询 `sandboxes.tenant` 和 `sandboxes.id`，普通认证按 `state` 排除已终止记录。

`operation.Repository.LiveTenantRequest` 在同一快照读取资源参数和结果，核对租户作用域及运行状态。provisioning 从原 cold、clone 或 restore 请求取得模板、快照、预期实例及 stop operation，与资源步骤及历史归属共同核验。

```nu
just generate-sql
just check-sql
```

工具版本固定在 `tools/go.mod`，生成绑定提交到版本控制，普通构建不需要 sqlc。`check-sql` 在私有临时目录重新生成并逐文件比较，能检测缺失、多余和修改的生成文件，拒绝命名/位置参数混用及生成后残留的 sqlc 参数宏，不改写工作树。`just fmt` 的 structfmt 阶段排除 sqlc 输出。
