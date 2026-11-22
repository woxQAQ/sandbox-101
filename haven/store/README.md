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
| `state_linux.go:validate` | quick_check、application_id/user_version、sqlite_master 的必需 table 计数 |
| `migrations.go:Migrate` | SQLite header、空库检查、嵌入的 v0 baseline 和 Goose 零号记录 |
| `validation.go:HasForeignKeyViolations` | foreign_key_check |
| `backup_linux.go:backup` | VACUUM INTO |
| `statements.go` | 原 bounded statement cache 的 PrepareContext/QueryContext/QueryRowContext 委托 |

其余静态 SQL 必须经所属包的 queries.sql/dbgen；用例、controllers 和能力非生成代码不能通过 raw Query/Exec/Scan 绕过私有 sqlc。

领域查询位于各领域的 `queries.sql`，由 `haven/sqlc.yaml` 配置生成到该领域的 `internal/dbgen/`。生成结构不越过领域 API；事务内操作绑定原 `*sql.Tx`，必需单行写仍核验影响行数恰好为 1。

sqlc 生成数据库表结构、查询参数和查询投影，不能从 BLOB 推导其中的 JSON 协议。当前开启 `omit_unused_structs: true`：例如 `templates(id, tenant, body BLOB)` 的查询只读 `body`，生成结果是 `[]byte`，因此其 `models.go` 可以为空；`GetPublisherRow` 等多列投影则生成在 `queries.sql.go`。手写 `template.Record` 负责 JSON 内的 `loom.Config`、字段顺序/tags 与公开视图转换，属于领域/持久协议模型。纯行投影在能力内直接消费生成类型；不为了隐藏 sqlc 而复制整行。BLOB 中的冻结 JSON 仍由手写协议类型表达，不能为填满 `models.go` 生成未使用类型。

`tenants.max_sandboxes` 和 `tenants.max_storage_mib` 是非负整数列。`authorization.Repository` 在同一条 UPSERT 中写入 token hash 和两项预算。认证 sandbox token 时只查询 `sandboxes.tenant` 和 `sandboxes.id`，普通认证按 `state` 排除已终止记录。

provisioning 需要按原始字节验证冻结请求。`operation.Repository.LiveTenantRequest` 在同一行核对摘要、租户范围和运行状态，返回已核验的请求字节及类型化结果。`machine` 定义 cold、clone 和 restore 的请求格式，工作流取得对应的请求类型后核对资源归属与历史证明。

```nu
just generate-sql
just check-sql
```

工具版本固定在 `tools/go.mod`，生成绑定提交到版本控制，普通构建不需要 sqlc。`check-sql` 在私有临时目录重新生成并逐文件比较，能检测缺失、多余和修改的生成文件，拒绝命名/位置参数混用及生成后残留的 sqlc 参数宏，不改写工作树。`just fmt` 的 structfmt 阶段排除 sqlc 输出。
