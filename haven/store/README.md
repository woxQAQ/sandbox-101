# Store lifecycle、migrations 与 sqlc

`State` 是 writer lock、共享数据库、node key、HEAD 和关闭顺序的唯一所有者。普通打开、备份及恢复都使用显式 `Validation`；publication 与 workflow 两个回调缺一时，在访问 state 目录之前拒绝。业务规则由 `internal/recovery` 组合，只接收数据库和不可变的 `Origin`（目录与 review 状态），不持有运行中的总 Service 或 runtime。

已有 state 继续按 immutable probe、私有 WAL probe、HEAD/业务证明核验、可写连接的顺序打开。只读阶段的回调拒绝不会触发 dispatch、补写 witness 或创建原 state 的 SQLite sidecar。publication 结构校验仍会在打开可写连接后重复执行，保留原覆盖范围；验证回调必须保持纯读取。备份读取使用已核验的 node key 检查 journal HMAC 链；review 只允许元数据诊断，不授权新效果。`haven/internal/application/store.go` 仅为普通与离线入口组装同一验证器；App 直接持有 State，没有第二层 store adapter、key、锁或数据库。

`Workflow` 只借出 Database（query 与 BeginTx）、不可变 Directory/Reviewed 和 Checkpoint 函数。它没有 Close、key 或 writer-lock 能力；各用例仍使用同一个 State 和共享数据库，不能把它当作第二个生命周期所有者。

`migrations/*.up.sql` 是表与索引的版本化来源。当前 `000009_baseline.up.sql` 对应单控制面 v9；运行时通过 embed 读取，sqlc 也直接读取同一目录。没有独立 `schema.go` 或生成后需要手工同步的 schema 副本。schema 不含 trigger：不可变/保留不变量由写入路径的 RequireOne 与原谓词保证，operation 时钟由 `operation` 包在 Insert/Complete/Fail 的原事务中显式写入 `operation_metadata`，epoch 由 `Checkpoint` 在写 HEAD 前显式推进。

`Migrate` 使用固定版本的 **goose v3.30.1 Provider**，接收原共享 `*sql.DB` 和初始化回调。goose 的 Go migration 在同一个事务中执行嵌入的 v9 baseline、SQLite `application_id` / `user_version`、store identity，并记录 `goose_db_version`；由 goose 提交或回滚。采用 Go migration 是为了让动态节点身份与 schema 原子提交，SQL 仍只有上述一份来源，没有自写 migration 排序或版本 runner，也不使用进程全局注册表。

writer lock 仍由 State 持有，迁移完成后才执行原 checkpoint/HEAD 协议。初始化回调失败时，业务 schema、identity、PRAGMA 版本和 goose 的 v9 记录一起回滚；goose 自身的 version-zero bookkeeping 表可以保留，但它不表示 baseline 已应用。

调用者负责完整 state 的验证和责任边界。当前正常 reopen 在只读 probe 中同时核对 goose 版本历史、store.json、DB identity/version、HEAD、publication 与 workflow proof，并拒绝早期、foreign、future 或不完整格式。缺失 goose 历史的旧 v9 库也明确拒绝，不根据 PRAGMA 推断并补造历史；已有 state 的拒绝检查不创建版本表或原库 sidecar。当前仅有 v9 baseline，没有 v8→v9 的升级脚本，也没有对真实旧 state 执行迁移。

后续 schema 变更须新增版本化 SQL、注册对应 goose migration，并更新 `Version`。已应用的 SQL 文件不能重写。实际旧版本升级还必须设计对应的只读前置验证、store_metadata/store.json 版本交接和 crash recovery；仅添加 SQL 文件并不能授权普通 startup 自动升级。不可变 operation、授权历史与 witness 不提供自动 down migration。

`HasForeignKeyViolations` 为启动结构验证和 GC 的只读事务提供 normalized reference 检查；PRAGMA 由 store 持有，query/row/close 失败保留未知，不解释为不存在引用。

store 自有 journal、metadata 初始化、身份读取与 epoch 查询也通过本包 `internal/dbgen` 生成。metadata 初始化使用原事务；版本按完整整数比较，不窄化后接受错误版本。`AppendAudit` 在调用者业务接纳保护内执行事务，核验追加恰好一行，提交后 checkpoint；HEAD 错误不会被解释为事务回滚。

手写维护 SQL 限于以下操作；新增操作需要审阅其事务、归属和恢复语义：

| 位置 | 允许的维护操作 |
|---|---|
| `state_linux.go:initialize` | store identity 元数据初始化、`wal_checkpoint(FULL)` |
| `state_linux.go:validate` | quick_check、application_id/user_version、sqlite_master 的必需 table 计数 |
| `migrations.go:Migrate` | application_id/user_version、空库与 goose journal 存在性检查、Go migration 中嵌入的 baseline 与受控版本 PRAGMA |
| `validation.go:HasForeignKeyViolations` | foreign_key_check |
| `backup_linux.go:backup` | VACUUM INTO |
| `statements.go` | 原 bounded statement cache 的 PrepareContext/QueryContext/QueryRowContext 委托 |

其余静态 SQL 必须经所属包的 queries.sql/dbgen；用例、controllers 和能力非生成代码不能通过 raw Query/Exec/Scan 绕过私有 sqlc。

领域查询位于各领域的 `queries.sql`，由 `haven/sqlc.yaml` 配置生成到该领域的 `internal/dbgen/`。生成结构不越过领域 API；事务内操作绑定原 `*sql.Tx`，必需单行写仍核验影响行数恰好为 1。

sqlc 生成数据库表结构、查询参数和查询投影，不能从 BLOB 推导其中的 JSON 协议。当前开启 `omit_unused_structs: true`：例如 `templates(id, tenant, body BLOB)` 的查询只读 `body`，生成结果是 `[]byte`，因此其 `models.go` 可以为空；`GetPublisherRow` 等多列投影则生成在 `queries.sql.go`。手写 `template.Record` 负责 JSON 内的 `loom.Config`、字段顺序/tags 与公开视图转换，属于领域/持久协议模型。纯行投影在能力内直接消费生成类型；不为了隐藏 sqlc 而复制整行。BLOB 中的冻结 JSON 仍由手写协议类型表达，不能为填满 `models.go` 生成未使用类型。

以 `tenants` 为例，`id` 和 `token_hash` 是 SQL 可查询的列，`body` 保存两个预算上限的 JSON。`authorization.Repository.Limits` 负责解码，配额调用方直接读取 `Limits` 的字段。冻结的 operation 请求有不同的读法。`operation.Repository.RequestState` 返回原始请求字节，sandbox provision 工作流先核对已保存的摘要和 scope，再按 action 解码对应的请求结构。这里的消费者需要知道该 action 的持久格式。

```nu
just generate-sql
just check-sql
```

工具版本固定在 `tools/go.mod`，生成绑定提交到版本控制，普通构建不需要 sqlc。`check-sql` 在私有临时目录重新生成并逐文件比较，能检测缺失、多余和修改的生成文件，拒绝命名/位置参数混用及生成后残留的 sqlc 参数宏，不改写工作树。`just fmt` 的 structfmt 阶段排除 sqlc 输出。
