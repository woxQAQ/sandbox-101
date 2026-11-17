# 025 分批实施证据：migration、sqlc 与服务/状态分层

归档说明：实施已完成，按用户后续归档指令与计划一同归档。正文保留收尾时的验证状态及“暂不归档”决定作为历史记录；归档不改变 FAIL/SKIP/BLOCKED。剩余验收见 [未完成索引](../../../plans/README.md#待完成验收)。

日期：2026-11-14 至 2026-11-16。基于 `6162397891e0e454fd2f6b3fc511c11d004fde5b` 的未提交工作树。未提交、推送、部署或修改真实旧 state。当前为 **IMPLEMENTED / VALIDATION_BLOCKED**：services/领域 SQL/App/controller 迁移已完成；普通 120 秒组件入口和 required-native 仍未取得必要通过，因此不归档。下方前五十批保留各自当时的状态，最终结果以第五十一、五十二批为准。Go 数量按 Test 级 pass/fail/skip 事件统计，包含子用例；无测试文件的 package 不计入这些数量。

## 第一批源码

- 移除根 `haven/schema.go` 的 SQL 字符串；原 v9 SQL 进入 `haven/store/migrations/000009_baseline.up.sql`。与 HEAD 原 SQL 比较，除首尾空白外相同。
- `store.Migrate` 在调用者事务中执行嵌入的连续编号 migration，并原子更新 SQLite application/version；baseline 只接纳空库，拒绝 foreign、早于 v9 与 future origin；无 downgrade。现有 reopen 的 proof/HEAD 检查没有被跳过。
- 当前只有 v9 baseline；没有 v8→v9 升级脚本，没有普通 startup 自动升级旧 state。逐表 epoch trigger 仍按原受控列表初始化。未来 migration 必须同时设计 store_metadata/store.json 交接和恢复，不把添加 SQL 文件当作授权。
- sqlc v1.30.0 固定于 tools module。17 条首批查询分属 operation、authorization、sandbox，生成代码分别位于各自 `internal/dbgen`。operation 结果读取仍核验原 request hash；publication 的插入保持恰好一行及原事务提交顺序。
- `services.Authentication` 和 `services.Catalog` 已由 HTTP 认证、sandbox 查询/列表路径使用。领域模型保持原 JSON 字段顺序与 tags，生命周期 catalog 通过窄读取 port 使用原完整 step 校验，原事务投影仍绑定同一 `*sql.Tx`。
- 根 Service 暂时组装这两个服务，未迁移的资源 workflow、template/pool/snapshot/image/quota、关闭协议与状态仍在原位置。临时 alias/组装应在后续迁移移除，不能视为最终 facade 设计。
- 增加 `just generate-sql`、`just check-sql`；生成检查在临时目录进行，识别缺失/修改/额外输出。structfmt 排除 dbgen；CI 增加生成及架构 gate 回归。
- 架构检查新增 control domain 范围、反向 services/controller import、客户端/Loom 越界和 foreign dbgen 约束。负例使用实际临时 Go workspace，不替换 Go import resolution。

## 第一批验证

详细本地日志：`build/025-services-sqlc/`（ignored 构建产物）。所有测试使用自建 state/fixtures。

| 命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run 'TestHaven(Store\|Public\|Downstream\|StepShutdown\|RestoreHandoff\|Backup\|SecretRestart\|StartupRejects)' -count=1 -timeout=120s`（编辑前） | PASS，根包 25.519s；建立本轮定向基线 |
| `GOWORK=off go -C haven test ./... -run 'Test(SQLiteMigration\|Haven(Store\|Public\|RestoreHandoff\|Backup))' -count=1 -timeout=120s` | PASS；migration SQLite rollback/重复执行/拒绝来源与 store/publication/handoff/backup 行为 |
| `GOWORK=off go -C haven test ./... -run 'TestHaven(Authorization\|Public\|Downstream\|Lifecycle\|SecretRestart)' -count=1 -timeout=120s` | PASS，根包 4.393s；首批 services 接通后运行 |
| `GOWORK=off go -C haven test -json ./... -count=1 -timeout=120s` | **FAIL：根包在 120.027s 超时**；306 pass、19 skip 测试事件，未先出现单项断言失败；不能算全量组件通过 |
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` | **PASS**；578 pass、35 skip 测试事件；根包 397.037s、egress 26.846s、store 1.284s。采用现有 race 入口的 10m 限制，未修改任何 timeout |
| `GOWORK=off go -C haven vet ./...` | PASS |
| `golangci-lint run ./haven/...` | PASS，0 issues；首次报告 migration 测试 import 分组问题，修正后通过 |
| `just generate-sql check-sql` | PASS，最终 9 个生成 Go 文件一致 |
| `python3 -I scripts/test_sql_generation.py` | PASS，clean 正例与 missing/edited/extra 三项负例 |
| `python3 -I scripts/check-architecture.py` | PASS；一次下载依赖出现 Go proxy EOF，补齐后重跑通过 |
| `python3 -I scripts/test_architecture.py` | PASS；domain→services、Loom→domain、SDK→services、services→controller 均拒绝 |
| `just integration` | PASS；真实 Rust Unix fixture 构建成功，Haven integration 0.351s |
| `just smoke` | PASS，8 组检查；真实 daemon、两 CLI、writer 排他、GC dry-run、restart、logging、统一 backup |
| `python3 -I scripts/check-native-gate.py --preflight` | **BLOCKED / exit 1**：需要隔离 runner 的有效 UID 0；当前非 root，未自动 sudo。日志 `build/native-gate/run.89fs1vdj` |
| `git diff --check` | PASS |

上述 race 测试包含 native SKIP，不证明 required-native。完整 required-native、KVM、kernel isolation、production 未运行；不引用 024 的历史通过替代本树证据。race 后的源码变化仅格式整理与 migration 测试 import 分组；后续脚本/文档修改有各自检查，不改变被测运行逻辑。

## 第二批：store 生命周期、access 与关闭屏障

本批继续在同一未提交工作树实施，没有操作真实旧 state。第一批全量结果仅绑定第一批树；以下为后续变更及重新执行的验证。

- `store.State` 接管原 `store_linux.go`、probe、statement cache、epoch 与 backup 实现，唯一持有 DB/key/HEAD/writer lock。根 `store.go` 暂时适配未迁出的 workflow，只共享数据库 handle 和不可变 origin。
- `store.Validation` 要求 publication/workflow 两个回调，配置缺失时在访问 state 目录前拒绝。`services.Recovery` 组合冻结交付与 workflow 证明；根 `workflow_validation.go` 通过纯 loader 验证，移除构造半初始化 Service 的路径。原 immutable/WAL probe、可写后重复结构验证、HEAD 与关闭顺序保留。
- 新增真实 SQLite/文件系统场景：回调缺失不创建目录，只读阶段证明拒绝后 state 文件 hash/mode/sidecar 不变、writer 可再次取得。retained-descriptor handoff 测试迁入 store，验证显式解锁与下一 writer 排他性。
- journal 查询迁至 store 私有 sqlc，签名与追加由 State 执行；Audit 仍在业务接纳/在途保护内调用。备份读取装入已核验的 node key，以核验含审计记录的 HMAC 链；备份恢复测试新增两条审计并核对恢复数量。
- `access.Registry` 独占 operation/sandbox secrets、binding 与 socket 登记；凭据复制包含 Hosts slice，调用者不能通过共享 map 改写登记内容。`access.Connection` 固定 sandbox/instance/generation 与 registry 归属，去掉对整个 Service 的回指；关闭 socket 时不持 registry 锁。
- `execution.Lifetime` 独占关闭标记、cancel、在途计数和单次收尾结果。Begin/Close 共用接纳互斥；先关闭接纳和撤销连接，再等待在途工作，最后按 Egress/backend/store 顺序关闭。data socket 登记仍与最终接纳检查共用根 guard，调用方超时不释放 worker 的 writer lock。
- 14 个 workflow loader 切换到 `operation.Repository.Step`，保留原 digest、action、candidate、expected instance 与状态证明验证。恢复查询增加到 operation，journal 增加到 store：当前共 23 条 sqlc 查询、12 个生成 Go 文件。
- 按用户要求移除不必要的 `operations` / `sandboxes` import alias；必要处将局部参数改名 `operationID`。`persistence` 仍用于根包已有 `store` 适配类型造成的真实标识符冲突。

| 本批实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run 'Test(Store\|SQLiteMigration\|Haven(Store\|WriterHandoff\|Public\|Backup\|StartupRejects))' -count=1 -timeout=120s` | PASS；根包 6.731s、store 0.784s，store 移动后运行 |
| `GOWORK=off go -C haven test ./... -run 'TestHaven(Downstream\|Secret\|Configure\|Data\|RestartRebind\|Backup\|Public\|Lifecycle\|Writer\|Close\|Shutdown)' -count=1 -timeout=120s` | PASS；根包 19.186s，access/lifetime 接通后运行 |
| `GOWORK=off go -C haven test -race ./... -run 'TestHaven(StepShutdown\|Snapshot.*Shutdown\|DataAdmission\|Downstream\|Configure\|Secret\|DirectData\|RestartRebind)' -count=1 -timeout=10m` | PASS；根包 30.284s，覆盖在途 data setup、迟到 handoff、阻塞 worker/backend 保留 writer |
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` | **PASS / exit 0**；585 pass、35 skip 测试事件；根包 394.701s、egress 26.864s、store 3.354s。包含本批 loader 迁移与 import alias 清理后的运行源码 |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues；修正了迁移中的 import 与类型分组格式问题 |
| `python3 -I scripts/check-architecture.py`、`python3 -I scripts/test_architecture.py` | PASS；实际 import graph 及违规负例 |
| `just check-sql`、`python3 -I scripts/test_sql_generation.py` | PASS；12 个生成文件一致，missing/edited/extra 负例拒绝 |
| `just integration` | PASS；真实 Rust Unix fixture，Haven integration 0.388s |
| `just smoke` | PASS；8 组真实 daemon/CLI/store/backup 检查 |
| `git diff --check` | PASS |

完整 race 的独立日志位于 `build/025-services-sqlc/lifecycle/`，`race.exit` 为 0；该次测试期间及之后仅更新文档，没有改动被测运行源码。第一批的 120s 全量组件超时仍保留为 FAIL；required-native 的隔离 root runner 前提仍未满足，本批未重跑 native/KVM/kernel-isolation/production。

## 第三、四批：worker 状态、Operations 与 Tenants

- `execution.Workers` 独占 runner 去重和结果跟踪。一个 `WorkerState` 同时记录 Active、Completed 与 Err，快照不暴露 map；Completed 区分未执行与成功 nil 结果。登记仍加入同一个 Lifetime，workflow 返回、释放 guard/完成 HEAD 后才发布完成结果。已完成 runner 可再次接续，新的完成结果替换旧诊断。
- 根 Service 的 active/lastErrors map 已移除，原 Service/SQLite 测试通过原子快照观测 runner。binding-maintenance 的诊断进入 access，保持成功重试不抹掉最后失败的原语义，不再混入 operation runner 状态。
- `services.Operations` 接管 tenant/sandbox/root/node scope 读取与同步等待；HTTP adapter 直接持有该服务。root-only MAC、请求 hash 校验及 sandbox capability 限制保持。Fork 使用明确的 `WaitAccepted` 等待已有 child，原冻结 parent/child 核验与责任交接仍在 workflow 内执行。
- `services.Tenants` 接管 root tenant 更新，共享原 admission 锁与 Lifetime 关闭屏障，checkpoint 仍位于 SQL 提交之后。没有新建锁域、数据库或自动重试。HTTP adapter 直接使用该服务和既有 `types.Tenant`，删除根 Tenant DTO 副本。
- authorization 拥有 token hash、tenant upsert 与 `Limits` 解码；provision/fork/restore/capture/warm 的预算读取全部绑定原 `*sql.Tx`。根 tenantRecord 副本已删除，没有另建兼容 alias。当前四个生成目标共 27 条查询、12 个生成 Go 文件；生产代码中的 tenants 静态 SQL 已全部收束到 authorization queries。
- 新增真实 Service/SQLite 场景：公开终态已提交但 activation worker 尚未退出时等待超时；runner 诊断不改写已提交成功，完成重放清理旧诊断且不重启 VM；binding 维护失败不改 operation；tenant UPDATE 的 IGNORE/ABORT 保留旧 token/预算，正常替换退休旧 token，关闭接纳拒绝写入，checkpoint 失败保留已经提交的凭据。

开发中暴露并修正的失败：迁出 wait 后遗漏 Fork 的内部等待调用，编译拒绝后接入 WaitAccepted；新等待测试最初错误地把 resource-ready 当成业务终态，断言失败后改为阻塞真实 activation_completed；lint 指出多余嵌入选择，HTTP 改为显式用例依赖。这些失败没有通过删除断言、改变 timeout 或弱化授权来绕过。

| 实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run 'TestHaven(StepConcurrent\|StepShutdown\|DataAdmission\|Downstream\|NoneMaintenance\|RestartRebind\|Public)' -count=1 -timeout=120s` | PASS，根包 4.917s；worker 状态迁移后 |
| `GOWORK=off go -C haven test -race ./... -run 'TestHaven(OperationWait\|BindingMaintenance\|StepConcurrent\|StepShutdown\|Authorization\|Public\|Fork\|NoneMaintenance\|RestartRebind)' -count=1 -timeout=10m` | PASS，根包 43.519s；新增等待/binding 场景修正后 |
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`（第三批中间树） | PASS；587 pass、35 skip，根包 399.769s，egress 27.074s，store 4.505s；日志 `build/025-services-sqlc/workers/`。绑定 worker/Operations 实现，早于 HTTP 多余选择清理和 Tenants 迁移 |
| `GOWORK=off go -C haven test ./... -run 'TestHaven(TemplateRoot\|StepMissing\|ForkBatch\|WarmClaim\|OperationWait\|BindingMaintenance)' -count=1 -timeout=120s` | PASS，根包 23.485s；tenant 预算读取和服务迁移后 |
| `GOWORK=off go -C haven test -race ./... -run 'TestHaven(Tenant\|TemplateRoot\|OperationWait\|BindingMaintenance\|StepShutdown)' -count=1 -timeout=10m` | PASS，根包 6.616s；新增 tenant 拒写/关闭/checkpoint 场景 |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues |
| `just generate-sql`、`just check-sql`、`python3 -I scripts/check-architecture.py` | PASS；12 个生成文件一致，依赖方向有效 |
| `just integration` | PASS；真实 Rust Unix fixture，Haven integration 0.350s |
| `just smoke` | PASS；8 组真实 daemon/CLI/store/backup 检查 |

第四批完整 race **PASS / exit 0**：591 pass、35 skip，根包 401.740s、egress 26.855s、store 3.274s；日志位于 `build/025-services-sqlc/tenants/`。本次运行源码摘要为 `4ffe747196b5b7e1b57797d107ec4d401d5ad7e3906b739b6aa4fa2240e1b902`：对 `rg --files haven -g '*.go' -g '*.sql' -g '*.yaml'` 排序，依次向 SHA256 写入路径、NUL、文件内容、NUL。该次运行期间及记录结果前仅更新文档；第三批的通过不替代第四批验证。native/KVM/kernel-isolation/production 本批未运行，历史 120s 组件 FAIL 和 required-native BLOCKED 保留。

## 第五批：具名共享 guard

`execution.Guards` 接管唯一的 keyed guard registry、admission/artifact/GC 互斥域及单 capture 名额。根 vmLock、裸锁 map、artifact/GC mutex 与 capture channel 已删除。调用方明确使用 VM、Configuration、Sandbox、AutoResume、TemplateWork、各 operation workflow 和 TenantRequest/NodeRequest 等能力；请求 scope/key 用独立字段表示，不再拼接前缀或 JSON 元组。业务冻结请求、hash 与持久 identity 不变。

所有原调用点继续保持原 Lock/Unlock 顺序、持有范围和 TryLock 失败分支。tenant 更新与资源事务/最终关闭接收同一个 Admission guard；Capture 仍是容量 1、可取消等待、原范围 defer 释放。执行状态无 Service 回指、数据库或外部效果。契约与锁域表见 [execution](../../../haven/execution/README.md)。

- `GOWORK=off go -C haven test ./... -run 'TestHaven(StepConcurrent|StepShutdown|Downstream|DataAdmission|Direct|SnapshotAdmissionAndShutdown|SnapshotShutdownResumesSource|Tenant|ForkBatchSources)' -count=1 -timeout=120s`：PASS，根包 18.821s。
- 同一选择器 `go test -race ./... -count=1 -timeout=10m`：PASS，根包 38.140s。覆盖真实 Service/SQLite 上的并发重放、关闭、direct 退役、迟到 handoff、snapshot shutdown 和 fork；不把这些 fixture 当作 kernel/native 证明。
- `golangci-lint run ./haven/...`：PASS，0 issues；整理了新 guard 方法的 gofumpt 分组格式。vet、实际 import architecture、diff check 通过。
- 第五批完整 race **PASS / exit 0**：591 pass、35 skip；根包 387.459s、egress 26.855s、store 3.295s。日志 `build/025-services-sqlc/guards/`；同一摘要算法的源码 SHA256 为 `0ca49990b1ec998c9f121731484effce26d7438640325d04a0bc0fe034bad94a`。第四批全量通过不替代此树验证。

随后检查 `provisionTargetValidator`：resume、warm fallback 和 fork branch 三个实现均不使用总 Service 参数，已从接口、实现和调用点移除；原冻结输入/同事务 proof 检查未变。`resourceParent` 的跨工作流接纳仍待按真实依赖拆分，没有用 callback 回填总 Service。

参数收窄后的 `golangci-lint run ./haven/...`（0 issues）、vet、check-sql、architecture、diff check 均通过；`just integration` 0.372s，`just smoke` 8 组通过。`GOWORK=off go -C haven test -race ./... -run 'TestHaven(Fork|WarmClaim|RestoreHandoff|LifecycleAdmission)' -count=1 -timeout=10m` 首次被工具的 60 秒等待上限中断，未得到测试结果，不记为通过。确认没有遗留测试进程后，以相同 Go 参数加 `-json` 后台重跑，日志 `build/025-services-sqlc/handoff/`；重跑 **PASS / exit 0，29 pass、0 fail/skip**，根包 61.769s。第五批全量数据绑定参数收窄前的树，此后新增变更使用上述定向验证。

## 第六批：VM 当前归属与历史事实读取

新增 `vm.Repository` 和领域私有 sqlc 绑定，五个查询保留原 SELECT 字段、身份约束、NULL current owner 和错误传播。Current 与历史 instance/version 采用分开的读取接口；release 取得历史行后仍比较原 operation/step/body，不能用当前 release owner 代替出生归属。全树现有 32 个生成查询、15 个生成文件。

根 `loadVM` 已去掉 Service receiver；各调用点仍传原 DB/事务和冻结 resource step。generation、capture、discard、provision completion 与 release 的原 instance/version 读取迁入 repository；JSON 解码、完整资源/观测/disk identity 相等性及 quota/holder 检查保持。没有引入包别名或持有总 Service 的新组件。VM JSON 模型、写入和跨领域 workflow 迁移仍未完成，详见 [VM 契约](../../../haven/vm/README.md)。

新增 `TestHavenReleaseRequiresOriginalGenerationRecords`：真实 Service/SQLite 上先完成 claim、接纳 release，再分别改写原 instance/version owner 或删除历史行。当前 VM 仍属于本次 release；各场景都拒绝 stop/remove、保留 64 MiB 费用和当前 custody，且原成功 step 完整不变。

- `GOWORK=off go -C haven test ./... -run 'TestHaven(Generation|VMResponsibility|RestoreTarget|Discard|CaptureDiscard)' -count=1 -timeout=120s`：PASS，根包 15.663s，覆盖首批三类读取。按实际命中的测试计，选择器不意味着每个前缀都有测试。
- 扩展后 `go test -race ./... -run 'TestHaven(ReleaseRequiresOriginalGenerationRecords|StepRelease|RestoreStoppedTarget|RestoreHandoff|Discard|CaptureDiscard)' -count=1 -timeout=10m` 被工具 60 秒等待窗口中断，未取得结果，不记为通过。
- `GOWORK=off go -C haven test -race ./... -run '^TestHavenReleaseRequiresOriginalGenerationRecords$' -count=1 -timeout=10m`：PASS，根包 3.895s，包含四个破坏历史的子场景。
- `golangci-lint run ./haven/...`：PASS，0 issues。vet、diff check、check-sql（15 文件）、架构及两组 negative gate 均通过。
- `just integration`：真实 Rust Unix fixture PASS，0.331s。本批未重复 smoke；不把上一批 smoke 作为新树结果。
- 完整 `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，596 pass、35 skip**；根包 393.129s、egress 26.888s、store 3.951s。日志 `build/025-services-sqlc/vm/`。源码 SHA256（沿用前述算法）`9816f06e5e31fd9ec5dc061d3326326d4aa3c07cae176c79951b0d62c6c8ebb8`。启动后只更新文档；当前模型的目标归属及避免 vm/snapshot 循环的引用方向已补入计划，不把目标表当成已完成迁移。

native/KVM/kernel/production 本批未运行；此前 required-native 环境阻断与历史 120s FAIL 继续保留。

## 第七批：公开交付、checkpoint、模板、预热池和宿主 Guest

Operations 接管公开 operation 读取与最终 token 交付，终态仍读取原冻结 publication；Catalog 接管 checkpoint 权限读取。sandbox.Checkpoint、template.Record、pool.Reservation 移入所属领域，字段顺序、tags 和 nil 语义保持。TemplateCatalog 核验 publisher 的冻结 supply step 与完整模板记录；永久 owner、catalog 前值和 publication 条件写入由 template.Repository 执行。

checkpoint_bindings、templates/template_intents、pool_slots/warm 的生产 SQL 已进入领域私有 sqlc；quota 接管首批 resource_refs/quota_refs 操作，operation 接管首批条件完成及幂等查询。所有 repository 仍接收原事务；refill、模板 publication 和 warm selection 保持写入顺序、旧 body 条件和 RequireOne。跨领域编排、其余费用/资源 SQL 和大部分 Service receiver 尚未迁完。

宿主 `guest.Port`、`guest.Activation`、`guest.Client` 与 socket 端点形成已独立；daemon 和 integration fixture 直接使用。协议边界测试随客户端迁移，错误 sentinel、128-bit correlation ID 格式、固定 portal、显式 exec key 与未知结果拒绝重放保持。Rust/protobuf 无修改。

新增 Service/SQLite 行为验证：

- `TestHavenCheckpointResponsibilityRejectsSQL`：running/paused/delete × IGNORE/ABORT；拒写不提交部分 binding/reference，不提前退休存储责任，恢复重试不重复 capture。
- `TestHavenWarmSelectionRejectsSQL`：selected 写入 IGNORE/ABORT；原 ready body、warm 可用性、operation 数量、资源/费用引用和 slot/storage 责任保持，恢复同 key 后使用原预热 VM 完成 activation，不增加 launch。独立 race 通过（9.823s，发生在后续 warm 查询迁移之前）。

开发过程保留的失败及修复：ReplaceTemplate 混用具名/匿名参数导致实际替换失败，改为统一具名并为生成 gate 加拒绝混用负例；SnapshotUsers 参数从 JSON BLOB 误推断，改为带字符串类型且无比较 affinity 的表达式，使用实际 modernc SQLite 比较 96 组 JSON/text/NULL 输入保持结果；pool INSERT 隐式列形式被 sqlc 拒绝，显式列名后生成通过，未修改 schema。Guest 移动遗漏 socket 调用点造成编译失败，调用方改用 guest.Socket；resume import 分组 lint 失败已修正。一次误用 go tool golangci-lint 未运行 lint，随后使用仓库实际 golangci-lint 入口。

中间树定向模板/refill/snapshot-delete 测试通过（27.386s；后续类型表达式调整后相关集 20.623s），warm/refill/supply-cleanup 定向集通过（20.520s）。这些结果不冒充最后全树验证。

第七批完整 `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` **PASS / exit 0，606 pass、35 skip**：根包 412.609s、egress 26.828s、store 3.265s、guest 1.020s。日志 `build/025-services-sqlc/catalog-pool/`，源码 SHA256 为 `37cc0a9f15c1c7607fc7d61da035b85aa311ae981146ae456ab9d61d1deb04bf`；运行期间仅更新文档。该树 check-sql（24 个生成文件）、生成 gate 负例、架构正负例、vet、lint（0 issues）通过；`just integration` 0.284s，`just smoke` 8 组通过。native/KVM/kernel/production 本批未运行，历史 120s FAIL 与 required-native BLOCKED 保留。

## 第八批：父子 admission 的实际依赖

resourceParent 的全部实现已删除总 Service 参数，接口只接收原事务和子操作事实。lifecycleAdmission 持有 snapshotProofs；forkAdmission 持有 snapshotProofs、原接纳屏障和只读节点预算。snapshotProofs 只借用状态目录及 Backend.Compatibility 能力，查询继续使用调用者原 DBTX。admissionGate 借用原 registration mutex 和同一 Lifetime，不创建第二份关闭状态。其余父流程只使用原事务，未添加总 Service 的接口包装或回调闭包。

恢复来源与 publication 的验证由 snapshotProofs 执行；capture cleanup、rollback/release capture 责任核验与部分父意图函数移除无用 receiver。字段顺序、冻结输入、父子 link 条件与持久写入顺序保持。初始化点全部显式提供历史验证与预算依赖，代码仍在根应用包，尚未冒称完整 services 迁移。

`GOWORK=off go -C haven test ./... -run 'TestHaven(WarmSelectionRejectsSQL|ForkBatch|Rollback)' -count=1 -timeout=120s` 通过（25.071s）。更广的 `-json -race ./... -run 'TestHaven(Fork|LifecycleAdmission|Rollback|WarmClaim|Template|Refill|SupplyCleanup|CaptureCleanup|CaptureDiscard|RestoreHandoff)' -count=1 -timeout=10m` **PASS / exit 0，95 pass、0 fail/skip**，根包 114.661s；日志 `build/025-services-sqlc/parent-admission/`，源码 SHA256 `2ef7277ebab7c0fe4f3d5f6e71fcace1616ad08863d8c68341cc5e9845f75e61`。移出验证方法时误删 AdmitClone 仍使用的 imports，编译拒绝后已恢复。

该树 lint（0 issues）、vet、架构 gate、check-sql、diff check 和真实 Rust integration 通过。第七批完整 race/smoke 绑定其原摘要，本批未重跑完整 race/smoke/native；历史 FAIL/BLOCKED 与 KVM/kernel/production 未运行保持。

## 第九批：持久模型归属与 snapshot 事实

vm.Record/Version、snapshot.Record/Manifest/Source、quota.SlotBudget 的结构定义移入领域包，JSON 字段顺序、tag、nil/omitempty 保持。跨领域 workflow 仍在根包，通过私有别名暂时引用；别名须随后续 services 迁移删除，不作为兼容接口。snapshot.Source 不导入 resource/capture workflow，避免 vm/snapshot 循环依赖。

snapshot.Repository 接管八类事实查询：租户归属 body、独立 owner/publication、原 source VM 的 capture 行、GC inventory、带删除阶段条件的 published body，以及 snapshot/owner/lease 计数。发布事实不替代原 capture witness，恢复与删除继续使用各自原来的兼容性规则。仅供内部测试观察的根 GetSnapshot 已删除，行为 fixture 通过领域 repository 查询真实 SQLite。

完整 `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` **PASS / exit 0，606 pass、35 skip**：根包 409.344s、egress 26.868s、store 3.476s、guest 1.017s。日志 `build/025-services-sqlc/models-snapshot/`，源码 SHA256 `df0f78cdd2b3e0996d7eeb027e507cb79aa43319d54784c6729f9f719d08c060`；运行期间仅更新文档。该树 lint/vet、check-sql（27 个生成文件）、架构和生成正负例、Rust integration、8 组 smoke、diff check 通过。开发中 lint 发现 embedded field 分隔和 import 分组问题，修正后复验。

snapshot/checkpoint/lease/backing 写入、其余 VM/quota SQL、跨领域工作流与剩余服务拆分仍未完成。本批未运行 native/KVM/kernel/production，历史 FAIL/BLOCKED 保留。

## 第十批：snapshot 写入与责任交接

snapshots、snapshot_owners、资源 checkpoints 的当前业务 SQL 已全部进入 snapshot 私有 sqlc。capture、snapshot delete 和 capture discard 的准入/完成，继续在原事务和原顺序中调用 operation、snapshot、quota、vm 仓库；保留原 body/source VM/instance/status 条件及 RequireOne。永久 owner 不随 catalog 删除。

quota 增加容量 reservation、费用原始归属与 holder 交接；VM 接管与释放保留 running/stopped 的不同谓词。operation 的 CompleteRunningStep 明确要求原 running 状态，未复用允许 pending 的一般完成路径。retireDeletedSnapshot 不再是总 Service 方法，只依赖原事务、冻结来源与历史证明；原 capture/parent/hash、删除 inode receipt、物理缺席、读者、全部引用和费用数量核验保持，不用当前 backend 兼容性取代历史删除证明。

新增 `TestHavenSnapshotCompletionRequiresRunningStep`：私有 SQLite trigger 在发布事务中把 step 改成 pending，完成必须失败并整体回滚 checkpoint、费用和 publication；移除 trigger 后恢复成功，不重复 capture/launch。开发中的一次缺失函数闭括号导致编译失败，修复后执行以下验证。

| 实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run 'TestHavenSnapshotAdmission' -count=1 -timeout=120s` | PASS，根包 0.373s；准入仓库接通后 |
| `GOWORK=off go -C haven test ./... -run 'TestHavenSnapshot(AdmissionAndShutdown\|AtomicPublication)' -count=1 -timeout=120s` | PASS，根包 10.081s；含发布各写点 ABORT/IGNORE |
| `GOWORK=off go -C haven test ./... -run 'TestHaven(Snapshot\|CaptureDiscard)' -count=1 -timeout=180s` | PASS，根包 43.649s；包含新增 running fence、删除/backing 最后退责与失败 capture 丢弃 |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues |
| `just check-sql`、生成/架构正负例、`git diff --check` | PASS，27 个生成文件一致 |
| `just integration` | PASS，真实 Rust Unix fixture，0.410s |
| `just smoke` | PASS，8 组 daemon/CLI/store/backup 检查 |

完整 `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` **PASS / exit 0，607 pass、35 skip**：根包 406.551s、egress 27.305s、store 4.848s、guest 1.075s。日志 `build/025-services-sqlc/snapshot-writes/`，源码 SHA256 `3909fa7c8265561e4a02ba27bb8b42d1b973bbc91a7086aca6aaff3b2696243c`；运行期间仅更新文档。source_leases、backing、其余 VM/quota/image SQL 与 services 拆分仍未完成；native/KVM/kernel/production 本批未运行，历史 FAIL/BLOCKED 保留。

## 第十一批：source lease、backing 与 generation 退役

source_leases 与 backing 的全部当前业务 SQL 已进入 snapshot 私有 sqlc；GC 保留原 ORDER BY 与未知关系阻断。fork、rollback、warm claim、cold clone、原 VM restore 的 source pin/release 协调原 lease、资源引用和费用引用三次写入；没有新事务或新的授权捷径。restoreRelations 只持有 snapshotProofs，区分尚有 source lease 与已交接 backing 的事实要求；restore handoff 保留材料/盘 identity、旧 step body/hash 和 running-only 条件。saveRestore 的 pending/running 条件与严格 handoff 分开实现。

VM 仓库补齐 instance/version 数量、排序清单、原身份条件删除和首次 disk identity 的旧 body 条件写入。retireGeneration、retireRollbackLease、retireWarmClaimLease 不再依赖总 Service；generation 依原顺序退 backing/lease、reservation 和 snapshot 最后费用，再移除历史 version/instance。完整应用仍有 232 个 Service receiver，领域持久层进展不代表完成业务服务拆分。

新增 `TestHavenRestoreHandoffRequiresRunningStep`：在 backing INSERT 的私有 trigger 中把 step 改为 pending，确认事务整体回滚、费用和 lease 留存、目标不 launch；恢复后只进行一次目标 launch。

| 实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run 'TestHavenRestoreHandoff' -count=1 -timeout=120s` | PASS，20.570s；接通关系核验后 |
| `GOWORK=off go -C haven test ./... -run 'TestHaven(RestoreHandoff\|ReleaseRequiresOriginalGenerationRecords\|SnapshotDeleteBackingRetirement)' -count=1 -timeout=120s` | PASS，20.168s；接通历史删除与最后 backing 退责后 |
| `GOWORK=off go -C haven test ./... -run 'TestHavenRestoreHandoffRequiresRunningStep' -count=1 -timeout=120s` | PASS，2.285s |
| `GOWORK=off go -C haven test -json -race ./... -run 'TestHaven(Restore\|Snapshot\|Fork\|Rollback\|WarmClaim\|GC\|Discard\|Release\|Resume\|Lifecycle)' -count=1 -timeout=10m` | **PASS / exit 0，246 pass，无 fail/skip**；根包 212.550s。其余包仅编译，没有匹配测试 |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues |
| `just check-sql`、生成/架构正负例、`git diff --check` | PASS，27 个生成文件一致 |

日志 `build/025-services-sqlc/source-dependencies/`，源码 SHA256 `3d7dd7ebee17ea4dc3c8671a138d273d5d8771a59c56f98869aad16aa1aba424`。race 期间仅更新文档/读取源码，未修改被测 Go/SQL/YAML。这是定向组件证据，不替代最终全树验收。native/KVM/kernel/production 未运行，历史 FAIL/BLOCKED 保留。

## 第十二批：Images 用例与镜像领域 SQL

`services.Images` 实际拥有导入接纳、运行、目录发布、失败留责、清理、恢复选择、GC work/cache inventory 和冻结候选核验。管理入口、全局恢复与 GC 直接接入；根镜像 receiver 与私有 workflow 实现已移除，没有总 Service callback。共享 ImageGuards、Workers 和 AdmissionGate 保留原锁域与关闭屏障；配置仅注入 safety bytes 与 Fault。image Repository 拥有 images/image_intents/image_caches 的全部静态业务 SQL，独立 cache witness 与费用保留原来源、body、值和精确一行条件；operation/quota 仓库补齐对应交接。

幂等 hash 冲突不再派发旧 operation；匹配请求仍根据已有冻结 step 恢复。新行为测试验证这两个分支。另一组测试使用私有 SQLite 恢复初态和真实目录：源在 staging 前缺失后，真实 runner 留下失败终态与 prepared-work 证明；清理完成写 IGNORE 或费用记录不匹配时，物理删除后整笔元数据责任仍保留，修复后恢复退责且原失败终态不变。此 fixture 不运行 EROFS 物化。原 OCI/cache/recovery 测试改为构造时注入 fault，动态阶段使用注入端原子开关；没有测试专用生产观察口。

| 实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run 'TestHaven(ImageCatalog\|ActualOCI\|GC\|Management)' -count=1 -timeout=120s` | PASS，根包 10.803s；其中真实 OCI 缺 bwrap，不能算物化通过 |
| `GOWORK=off go -C haven test -run '^TestHavenImage(ReplayDoesNotRunConflictingRequest\|CleanupRetainsResponsibilityUntilAtomicCompletion)$' -count=1 -timeout=60s -v` | PASS，0.995s；四个子场景实际执行 |
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` | **PASS / exit 0，614 pass、35 skip**；根包 417.044s、egress 26.830s、store 3.351s、guest 1.020s |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues |
| `just check-sql`、生成/架构正负例、`git diff --check` | PASS，30 个生成文件一致 |
| `just integration`、`just smoke` | PASS；真实 Rust fixture 0.382s，8 组进程/CLI smoke |

日志 `build/025-services-sqlc/images/`，源码 SHA256 `f36fe3b097c213cbfacf84a334c2bd020c6693eec2b11ec38eda5b17215ca749`；完整 race 期间仅更新文档/读取源码，结束后摘要复核一致。迁移时的编译错误与新 fixture 的状态、表名和输入字段顺序错误均已定位修正，未放宽生产条件；精确 intent body 比较仍拒绝不同字节序的初态。根 Service 当前仍有 220 个 receiver，不能将本批视为 025 完成。required-native 继续受原隔离 runner/EUID 前提阻断；KVM/kernel-isolation/production 未运行，历史 FAIL/BLOCKED 保留。

## 第十三批：VM 全部持久读写

vms、instances、resource_versions 的非测试生产 SQL 已全部进入 VM 私有 sqlc。接通候选/出生历史插入、slot 与 GC inventory、当前及历史计数、启动/停止记录、完成时释放 owner、restore/release/discard 的 owner 交接和停止实例删除。RecordStopped 与 CompleteStop 保持不同责任语义；CompleteStart 继续要求 created，停止删除继续要求原 instance、owner 和 stopped。SQL NULL owner 的存在性通过 Current.Owned 显式传递，不能用空字符串替代 GC 的 NULL 判定。

跨域事务保留原写入顺序；涉及的 capture cleanup、discard、stop 完成同时使用现有 operation/quota 仓库，running-only 与原 body/hash 条件未合并。旧 errorsJoinRows helper 已随最后调用迁移删除。VM 写入、计数与 inventory 完成分包不表示资源用例已经拆出：根 Service 仍有 220 个 receiver，quota/sandbox/operation 其余 SQL 和业务拆分继续进行。

| 实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test ./... -run '^$'` | PASS，全部 Haven 包编译 |
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` | **PASS / exit 0，614 pass、35 skip**；根包 422.033s、egress 26.822s、store 3.265s、guest 1.019s |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues；开发中一处 gofumpt 空行已修正 |
| `just check-sql`、架构与生成正负例、`git diff --check` | PASS，30 个生成文件一致 |
| `just integration`、`just smoke` | PASS；真实 Rust fixture 0.315s，8 组进程/CLI smoke |

日志 `build/025-services-sqlc/vm-persistence/`，源码 SHA256 `d0ca19d941db9aae8be8e40571daa982de44d4f690f5aa11a08a96175cd6eb10`；race 期间仅更新文档/读取源码，结束后摘要一致。复用既有出生历史、恢复 handoff、IGNORE/ABORT、停止/删除、GC 与关闭行为测试，没有新增 repository/helper 单测。native/KVM/kernel-isolation/production 本批未运行；35 skip、历史 120s FAIL 与 required-native BLOCKED 保留。

## 第十四批：reservation 与 fork 预测责任

reservations 的全部生产 SQL 已进入 quota 私有 sqlc。`CapacityUsage` 汇总 bytes/memory/CPU；cold 与 fork 接纳复用仓库使用量/容量查询，restore 复用 ReservedBytes。capture/discard/delete/release 的 reservation 核验保留精确金额和零 memory/CPU 条件，仍在原事务、任何效果或责任写入之前完成。部分纯计数核验前移至原 checks 循环之前，写入、commit、checkpoint 与 fault 顺序不变。

fork 的预测接纳、reservation 消耗及 holder/cost 删除使用领域值；`RetireMatchingCost` 保留原 slot/storage/source operation 的全部条件。cold 完成按原 reservation → cost/holder → physical refs 顺序核验。其余费用和引用 SQL、完整业务服务仍待继续迁移。

- `GOWORK=off go -C haven test ./... -run 'TestHaven(Cold|Fork|RestoreHandoff)' -count=1 -timeout=120s`：PASS，根包 26.060s。
- `GOWORK=off go -C haven test -json -race ./... -run 'TestHaven(Cold|Fork|Restore|Snapshot|Release|Discard|Capture|VM)' -count=1 -timeout=10m`：**PASS / exit 0，280 pass、0 fail、0 skip**，根包 191.046s；其他组件未选中行为测试。
- lint 0 issues、vet、架构与生成正负例、30 文件 sqlc 一致性、diff check：PASS。

日志 `build/025-services-sqlc/reservations/`，源码 SHA256 `414d0312e4f3cfb39adbb502c8103ed0a5a48e83ad10e9774af19bb1dd860127`，race 前后摘要一致。开发中曾构造小数责任记录以核对 SUM 类型；五个 fixture 均因 v9 的 typeof(integer) CHECK 拒绝注入而 FAIL。确认该初态不可达后删除这项新测试并撤回额外转换逻辑，保留 schema 约束与既有整数聚合；没有削弱生产检查。迁移中的未使用变量和 ctx 编译错误也已修复，以上 PASS 为修复后的树。

本批未重跑 integration/smoke、native、KVM、kernel-isolation 或 production；第十三批对应结果不作为本批通过证据。025 保持 IN_PROGRESS。

## 第十五批：费用与引用的全部生产写入

quota_resources、quota_refs、resource_refs 的非测试生产写入 SQL 已全部进入 quota 私有 sqlc。迁移覆盖 cold 完成、VM/discard 接纳与完成、fork 接纳与未分配预测退休、warm slot 计费、pause/resume/rollback/paused release、template/supply 与 supply cleanup。读取侧的剩余 SQL 仍待迁移。

新增 ChargeWarmSlot 保留 slot 0→1、storage=0、出生 operation 条件；MoveCostReference 只更新 holder ID，不扩大 UPDATE OF 触发列。RetireCostAtKey 保留 cold 交接原 tenant/kind/key 删除谓词，不把来源/金额核验推断成新的 SQL 条件。精确来源/金额退休继续使用 RetireMatchingCost。拆开原混合 SQL 列表后，写入次序、事务、影响一行、commit/HEAD/fault 顺序保持；并未让仓库独立提交。

| 实际命令 | 结果与范围 |
|---|---|
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` | **PASS / exit 0，614 pass、35 skip**；根包 407.203s、egress 26.914s、store 3.669s、guest 1.021s |
| `golangci-lint run ./haven/...`、`GOWORK=off go -C haven vet ./...` | PASS，lint 0 issues |
| 架构/生成正负例、`just check-sql`、`git diff --check` | PASS，30 个 sqlc 文件一致 |
| `just integration`、`just smoke` | PASS；真实 Rust fixture 0.356s，8 组进程/CLI smoke |

日志 `build/025-services-sqlc/quota-writes/`，源码 SHA256 `b5f199d6e53e32001701ce2c239836c48cbb5f897c03fda4f242f1956fe4f997`，race 前后摘要一致。中途 Cold/Discard/Release/Resume/Fork 的非 race 检查也曾通过，但最终树以本节完整 race 为准。复用既有 SQLite、故障边界与生命周期行为测试，无新增 helper 单测。native/KVM/kernel-isolation/production 未运行，历史 FAIL/BLOCKED 与本次 35 skip 保留。

再次按 AST 核对，根 Service 仍为 220 个 receiver。后续在迁移其余读取的同时，先收束 resource/capture 的共享冻结类型与纯历史证明，再拆实际 snapshot/resource 服务；不通过总 Service 回指替代依赖拆分。025 仍未完成。

## 第十六批：跨领域冻结模型与 snapshot 只读证明

services 拥有 ResourceInput/State/Step、QuotaDestination 与 CaptureRequest/Input/State/Step；LoadResourceStep、LoadCapture 与 ValidateResourceStep 已迁入应用层。按基准 HEAD 对八个结构逐字段比对，在归一化原私有别名后，顺序、类型与 JSON tags 全部一致；没有复制成 domain 模型或改变 nil 表达。

实际 `services.SnapshotProofs` 承接当前来源读取、历史 publication/compatibility 核验以及 restore lease/backing 关系核验。它只持有 directory 和 Compatibility 读取接口，调用者提供原 DBTX/事务；没有 Service 回指、缓存、独立提交或 runtime 效果。业务调用点改用该类型方法。原 admitRollbackLease 改为应用自由函数，继续在原事务写入 lease，不把写入留在只读证明组件。根包的模型别名、loader/validation 转发及大部分业务用例仍待删除/迁移，Service receiver 仍为 220。

- 完整 `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，614 pass、35 skip**；根包 416.994s、egress 26.875s、store 3.703s、guest 1.022s。
- lint 0 issues、vet、架构/生成正负例、`just check-sql`（30 文件）、diff check：PASS。
- `just integration`：PASS，真实 Rust fixture 0.402s；`just smoke`：PASS，8 组进程/CLI 检查。

日志 `build/025-services-sqlc/workflow-models/`，源码 SHA256 `e43d20188afac52f2e885c949d71ef34c05fbcbe1defa6b9d5bbf8f3afff7fb7`，race 前后摘要一致。迁移后一次编译因残留 context import 失败，已删除并通过以上检查。未增加 helper 单测；native/KVM/kernel-isolation/production 未运行，35 skip 和历史 FAIL/BLOCKED 保留。模型与证明组件迁移不是完整资源服务拆分，025 保持 IN_PROGRESS。

## 第十七批：generation 历史核验组件

实际 `services.ResourceProofs` 接管 GenerationHistory/GenerationHistoryAt、VerifyGeneration/VerifyGenerationAt、CleanupGenerations；所有原调用点已接通，根实现已删除，只保留窄构造入口。组件仅组合 SnapshotProofs，事务由调用者提供，不持有 Service、锁或新的生命周期状态。磁盘费用与 holder 的原联查进入 quota sqlc，保留原 tenant/kind/key/storage/source operation/holder 条件，不增加 slot 谓词。根 Service receiver 经 AST 复核从 220 降至 216。

新增 `TestHavenStopRequiresOriginalDiskFunding`：在私有 Service/SQLite 中分别损坏费用来源、金额、holder，核验没有 stop、当前归属/费用和原成功 witness 保持；恢复原证明后同一请求只 stop 一次。单独运行三个子场景通过，根包 0.999s。

`GOWORK=off go -C haven test -json -race ./... -run 'TestHaven(Cold|Fork|Restore|Snapshot|Release|Discard|Capture|VM|Stop|Warm|Template|Refill|Direct|GC)' -count=1 -timeout=10m`：**PASS / exit 0，307 pass、0 fail、0 skip**，根包 248.134s。其他组件未选中行为测试。lint 0 issues、vet、架构/生成正负例、30 文件 sqlc 一致性、diff check 均通过。

日志 `build/025-services-sqlc/resource-proofs/`，源码 SHA256 `681d27053f0a83c6550bbebda2e1f67633403ab8823fd1adf82d9e6ab5a6e130`，race 前后摘要一致。本批未重跑 integration/smoke、native/KVM/kernel-isolation/production；不挪用第十六批通过，历史 FAIL/BLOCKED 保留。025 仍未完成。

## 第十八批：snapshot backing 枚举与删除 SQL

`SnapshotProofs.Backings` 接管显式 holder 枚举、冻结 clone/restore 关联查找及完整来源/materials/lease 证明，根 snapshotBackings 方法已删除。quota ResourceHolderIDs 保留排序；operation RestoreBackingOperation 保留原 JSON 比较的无 affinity 语义。sqlc 对直接 JSON 参数推断为 []byte，最终使用 CAST(... AS TEXT)||'' 生成 string 参数；用实际 modernc SQLite 重跑 96 组 JSON/text/NULL 比较，结果与原查询相同。

snapshot_delete 路径生产代码不再直接 Query/Exec。费用/引用计数、cleanup 原 body 与完整匹配、parent projection、删除 receipt、pending→running 和 running body 推进均接入仓库。BeginPendingStep 仍只接受 pending；ReplaceRunningStepBody 只写 body，不额外更新 status。原 parent 核验、计数、写入与 commit/checkpoint/fault 顺序保持。根 Service receiver 为 215；实际删除用例仍待从根服务迁移，不能把 SQL/证明分包认定为完成。

- `GOWORK=off go -C haven test -run '^TestHavenSnapshotDelete$' -count=1 -timeout=90s`：PASS，最终迁移后 5.358s。
- `GOWORK=off go -C haven test -json -race ./... -run 'TestHaven(SnapshotDelete|SnapshotRelease|Restore|Release|GC)' -count=1 -timeout=10m`：**PASS / exit 0，142 pass、0 fail、0 skip**，根包 134.541s；其他组件未选中行为测试。
- lint 0 issues、vet、架构/生成正负例、30 文件 sqlc 一致性、diff check：PASS。

日志 `build/025-services-sqlc/snapshot-delete-queries/`，源码 SHA256 `591b5118876929d33b3750466e3495843bdcfbe2bd05ef43d36f029e360d5ead`。复用真实 Service/SQLite 的删除 ABORT/IGNORE、backing 留责、reader 与故障恢复场景。本批未重跑 integration/smoke、native/KVM/kernel-isolation/production；历史 FAIL/BLOCKED 保留，025 仍为 IN_PROGRESS。

## 第十九批：实际 SnapshotDeletion 服务

接纳、当前/历史责任核验、持久删除 receipt、reader/物理回收、原子完成交接及恢复派发迁入 `services.SnapshotDeletion`；GC 直接调用组件。组件借用原 admission/snapshot guards、Workers/Lifetime 和 DB/checkpoint 能力，无总 Service/Options 回指，无新锁域或独立生命周期。`RetireDeletedSnapshot` 由删除完成和 generation 退役共享，仍由原调用者事务负责 commit。根删除模型/loader 观察 helper 仅保留于 `_test.go`，根 receiver 从 215 降至 208。

Fault hook 构造时注入，删除 fixture 通过原子开关切换私有故障。新增 `TestHavenSnapshotDeleteConflictDoesNotDispatch`：先持久接纳并阻断派发，冲突请求不得启动原 worker 或移除文件；匹配请求恢复同一 operation，成功后 input/hash 不变并精确退责。原七个删除 crash 边界、fence、unlinked-reader 场景继续通过。

- 开发期 lint 曾因显式选择 promoted 字段触发 QF1008；改为先选择实际组件再调用，未抑制规则。最终 lint：0 issues。
- `GOWORK=off go -C haven test -run '^TestHavenSnapshotDelete(ConflictDoesNotDispatch|Recovery|Fence|LeaseFence|UnlinkedReader)$' -count=1 -timeout=120s -v`：PASS，8.146s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，619 pass、0 fail、35 skip**，根包 416.568s。
- `GOWORK=off go -C haven vet ./...`、架构门禁及正负例、生成正负例、`just check-sql`（30 文件）、`git diff --check`：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.469s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/snapshot-deletion-service/`，源码 SHA256 `a9a179b544fbc5a644804bead21ef1f2cb0426c679cb663cf33e8a016453370f`。35 skip 仍含缺 bwrap 的真实 OCI/EROFS 路径；本批没有 native/KVM/kernel-isolation/production 验收。历史 FAIL/BLOCKED 保留，025 仍为 IN_PROGRESS。

## 第二十批：生命周期冻结模型与父子依赖协议

`Command`/`EnvVar`、lifecycle/rollback 冻结结构和 loader 迁入 services；环境 secret 判定语义不变。`ResourceLink` 的读取/hash/原始子操作校验、插入与完成迁入 services；operation 私有 sqlc 接管 link action 筛选、child request hash/status 与 pending-only status/body 更新。调用者仍持原事务；完成返回是否发生写入，终态重放保留不 commit/checkpoint 的原行为。根 Service receiver 仍为 208，父接纳与其余生命周期编排继续迁移。

`TestHavenLifecycleLinkRequiresWrite` 在真实私有 Service/SQLite 上以 ABORT/IGNORE 阻断 link 完成：已成功 capture 和 256 MiB/1 slot 责任保留，checkpoint binding 不得发布；修复后重放原父操作，不再次 capture，并绑定原 snapshot。开发期测试误期望 ErrNotFound，实际目录契约返回 sql.ErrNoRows；修正断言，未改生产错误行为。临时冻结结构检查先后补上基线合并字段声明展开和 Checkpoint 类型别名归一化，最终十个结构的字段顺序/类型/JSON tag 对照 PASS。

- `GOWORK=off go -C haven test -run '^TestHavenLifecycleLinkRequiresWrite$' -count=1 -timeout=90s -v`：PASS，1.213s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，622 pass、0 fail、35 skip**，根包 408.195s；验证后源码摘要一致。
- lint 0 issues、vet、架构/生成正负例、`just check-sql`（30 文件）、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.357s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/lifecycle-protocol/`，源码 SHA256 `acdcbf4520eab237be7cd494be5ccbc7d1766ddf25e1205a530870d4740fdd6c`；结构对照为 `frozen-models.log`。本批未执行 native/KVM/kernel-isolation/production；35 skip 与历史 FAIL/BLOCKED 保留。025 仍 IN_PROGRESS。

## 第二十一批：实际父接纳协议与 capture 留责核验

`LifecycleAdmission` 及窄 `ResourceParent`/`ProvisionTargetValidator` 端口迁入 services；仍由父类型的冻结证明决定接纳，没有总 Service 参数或回指。原 parent → credential → step → configuration fence → rollback lease → 上层责任 → resource link 顺序不变；原 SQL 读写归 operation/sandbox，事务仍由子资源接纳者提交。resume sleep 目标校验也由实际父对象执行。

raw capture 发布与转移证明、release/rollback holder 枚举移入 services，使用 operation/quota 仓库，保留排序、nil 集合和查询失败留责。source lease 与 resource/cost holder 的 pin/release、rollback 最终退 lease 均迁入 services，继续借用原事务。根两个纯核验 receiver 删除，数量 208 → 206；重复 rowsQueryer 移除，剩余读取使用同一个 store.DBTX。

开发期 `sqlc generate` 对无列名 UPSERT 报 INSERT expressions 数量错误；为 configuration_fences 显式列出原两个列，SET 仍仅 operation_id，schema/触发列未改。编译初检发现残余 rowsQueryer 和 lifecycleAdmission 的 provision 方法；已迁移窄目标端口并更新所有实现/调用，没有保留不可达适配。

- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，622 pass、0 fail、35 skip**，根包 418.691s；验证后源码摘要一致。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`（30 文件）、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.441s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/lifecycle-admission/`，源码 SHA256 `0a4e6435975a374ef921e160540ef6837de5f71127c410b79e7d44a105a197b0`。复用失败 capture、raw cleanup、rollback/paused release、fork/warm/supply 的真实 Service/SQLite 场景；没有新增 helper 单测。本批未执行 native/KVM/kernel-isolation/production，35 skip 和历史 FAIL/BLOCKED 保留。025 仍 IN_PROGRESS。

## 第二十二批：实际 CaptureCleanup 服务

raw VMM 输出的接纳、stop/generation 核验、durable receipt、reader 检查、物理删除、完成交接及恢复迁入 `services.CaptureCleanup`，GC 与 rollback 直接接入。它借用同一 admission/VM guard、AdmissionGate、Workers 与 DB/checkpoint，构造时注入窄 `InstanceInspector` 和 fault；没有总 Service/Options 回指。根 raw cleanup loader/模型观察 helper 只留在测试文件。根 receiver 数 206 → 199。

stop、supply、supply-cleanup 冻结模型/loader 与实际 SupplyCleanupAdmission、allocation fence 进入 services。collector 仍必须是匹配原父意图的具体类型，没有将原授权替换成 owner 字符串。raw 用例全部静态 SQL 已归 operation/quota/vm/sandbox；新增 pending capture transferred 与 pending collector owner 查询保留原条件。冻结结构对照累计 22 项 PASS（字段顺序、类型、tags）；nil 集合未变。

新增 Service/SQLite 回归覆盖 transfer 的 ABORT/IGNORE：parent/step/cleanup 接纳整体回滚，原 pending 责任、空 VM owner 和 256 MiB 费用保持，修复后可重试；实例 observation 失败保留 raw 文件及两个 capture holder，恢复后同一 operation 成功。开发期后一测试的重放观察 helper 读取了仍运行的新 worker 所保留的上次错误；改为等待当前 worker 结束，再断言持久终态，未改生产 Workers 语义。

- `GOWORK=off go -C haven test -run '^TestHaven(PublishedRawCleanup.*|RollbackKeepsLogicalSandbox|SupplyCleanup.*)$' -count=1 -timeout=120s -v`：PASS，18.157s。
- 新增边界测试 `^TestHavenPublishedRawCleanup(RequiresTransfer|RetainsUnknownInstance)$`：PASS，2.336s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，626 pass、0 fail、35 skip**，根包 414.375s；验证后源码摘要一致。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`（30 文件）、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.514s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/capture-cleanup-service/`，源码 SHA256 `b4be816d3c69e56523b50eb261ecdda44d12da0729f42acd5279f70c0fb81ff7`，结构对照 `frozen-models.log`。native/KVM/kernel-isolation/production 未运行，35 skip 和历史 FAIL/BLOCKED 保留。025 仍 IN_PROGRESS。

## 第二十三批：实际 CaptureDiscard 服务

失败 capture 的接纳、原 stop/generation/费用证明、双 receipt、物理回收与最后退责由 `services.CaptureDiscard` 承接；release/supply cleanup 和恢复直接调用。使用共享 admission/VM guards、Lifetime/Workers、ResourceProofs 和 InstanceInspector，无总 Service 回指。根观察 helper 仅留在测试，receiver 数 199 → 190。

全部 capture-discard 静态 SQL 归领域仓库；新增 quota tenant-scoped holder 计数保留原 tenant 谓词，没有替换成全局计数。父 operation 查询先于延后的责任计数，保留原核验顺序。阶段开始仍 pending-only，files_removed 仍只 SET body；完成、费用与 reservation 的条件不变。GenerationCounts 纯核验迁入 services，borrowed persistence 类型独立成文件。私有 fixture 在 reopen 构造时注入 fault，不再依赖修改根 Options。

- `GOWORK=off go -C haven test -run '^TestHaven(CaptureDiscard.*|ReleaseFailedCapture|SupplyCleanup.*)$' -count=1 -timeout=120s -v`：PASS；覆盖双 receipt 原子性、接纳/完成 ABORT/IGNORE、reader、目录替换、未知实例及七个 crash 点。
- 冻结结构对照累计 25 项 PASS（字段顺序、类型、JSON tag）。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，626 pass、0 fail、35 skip**，根包 409.173s；验证后源码摘要一致。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`（30 文件）、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.535s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/capture-discard-service/`，源码 SHA256 `c4447c33361f93b56e913d3107e0d74b1c825dbf52fb827cc8e3daa689c8e2e4`。本批未新增 helper 单测；native/KVM/kernel-isolation/production 未运行，35 skip 和历史 FAIL/BLOCKED 保留。025 仍 IN_PROGRESS。

## 第二十四批：实际 Captures 服务

捕获的接纳、原 source/归属/费用证明、pause/capture/resume、artifact 固化、发布和失败留责迁入 `services.Captures`。lifecycle、fork、supply build 与恢复直接调用；共享 admission/VM guards、capture 名额和 Lifetime/Workers，backend/fault 构造时注入。独立 15 秒安全 resume 上下文、原 COMMIT/HEAD/fault 顺序保持。静态 SQL 全部迁入领域仓库，失败写保留原 status/body/hash 条件，磁盘费用不新增 slot 条件。借用的 persistence 改为无 Close 能力的窄接口。根 receiver 数 190 → 181。

新增 `TestHavenCaptureConflictDoesNotDispatch`：冲突请求不派发、不改变 256 MiB 原责任；匹配重放复用原 input/hash，capture 只执行一次。

开发期定向检查首次 FAIL：rollback fixture 在 Open 后替换 backend，新服务仍持有原 cold runtime，capture 缺少可恢复 RAM topology。改为构造时注入后，下列检查 PASS：

- `GOWORK=off go -C haven test -run '^TestHaven(CaptureConflictDoesNotDispatch|SnapshotShutdownResumesSource|SnapshotCompletionRequiresRunningStep|RollbackKeepsLogicalSandbox|LifecycleAdmissionAndHandoff)$' -count=1 -timeout=120s -v`：7.486s。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`、diff check：PASS；冻结结构对照 25 项 PASS。
- `just integration`：真实 Rust unix fixture PASS，0.310s；`just smoke`：8 组 PASS。

完整 `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**FAIL / exit 1，626 pass、1 fail、35 skip**，根包 413.008s。唯一失败 `TestHavenDirectOldBindingCannotRevokeResumedInstance` 同样来自 direct fixture 在 Open 后替换 runtime，缺少 snapshot memory manager state。该轮源码 SHA256 `665c23c78a2e5652666a72ecaf0d0a69e83b6b22190e1d87a18b07665d423c6b`。

修正 direct fixture 的构造注入后，`GOWORK=off go -C haven test -race -run '^TestHavenDirectOldBindingCannotRevokeResumedInstance$' -count=1 -timeout=60s -v` PASS，5.073s；lint/diff check 再次 PASS。修正后源码 SHA256 `1ed3bdfddb2b02d891bff49950ca45eddff6bfe761f44b867d560fa324c9c8ce`。此树未重跑完整 race，后续批次须覆盖，不能将失败轮改记为通过。

日志 `build/025-services-sqlc/capture-service/`。native/KVM/kernel-isolation/production 未运行，35 skip 与历史 FAIL/BLOCKED 保留；025 仍 IN_PROGRESS。

## 第二十五批：实际 VMStops 服务

`services.VMStops` 接管 stop 接纳、原 generation/费用与当前归属核验、持久 intent、进程退出和网络移除确认，以及原事务完成。lifecycle、rollback、supply cleanup 与恢复直接调用；使用共享 admission/VM 锁、Lifetime/Workers、ResourceProofs 和构造注入的窄 StopBackend。原具体 LifecycleAdmission、父 link/fence、pending/running/body 条件及 COMMIT/HEAD/fault 顺序保持。stop 的全部静态 SQL 使用现有 operation/sandbox/vm 仓库，无 schema 变化。共用锁端口改名 `VMGuards`，不再以 capture 命名其他 VM 用例的依赖。根 receiver 数 181 → 177。

新增 `TestHavenStopCompletionAtomicity`，覆盖终态写 ABORT/IGNORE：实际 fixture stop/network-off 后，拒绝步骤完成会回滚 VM 状态/owner 写入，保留 stop intent、64 MiB 磁盘费用及 1 slot；修复后同一请求完成原 hash/input。

- `GOWORK=off go -C haven test -run '^TestHaven(Stop.*|RollbackKeepsLogicalSandbox|LifecycleAdmissionAndHandoff|SupplyCleanup.*|DirectOldBindingCannotRevokeResumedInstance)$' -count=1 -timeout=120s -v`：PASS，19.161s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，630 pass、0 fail、35 skip**，根包 439.010s；源码摘要核对一致。此轮同时完整覆盖第二十四批 direct fixture 构造注入修正，之前的失败轮仍保留。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`、diff check：PASS；冻结结构对照 25 项 PASS。
- `just integration`：真实 Rust unix fixture PASS，0.334s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/vm-stop-service/`，源码 SHA256 `6a5096310f1c612ac92230c14130201a6d59c247dfa7e5e1c10af95d48333f5e`。native/KVM/kernel-isolation/production 未运行，35 skip 与历史 FAIL/BLOCKED 保留；025 仍 IN_PROGRESS。

## 第二十六批：实际 DirectPolicies 与 network policy 持久事实

`services.DirectPolicies` 接管 direct binding 核验、固定原 activation/instance 的 teardown 及 pending retirement 恢复。原 callback 10 秒独立上下文、恢复 configuration guard、COMMIT/checkpoint/fault 顺序保持。Arm/Replacement/已回收 generation 的退役借用原事务，无总 Service 回指。activation/direct 冻结结构与 loader 迁入 services；全部 network_policies SQL 归 vm，step/cleanup 使用 operation sqlc。retirement 的 owner-only 查询仍不读取 policy body，pending/running/armed 与 body/hash 条件分别保留。根 receiver 数 177 → 175。

`TestHavenDirectRetirementCompletionAtomicity` 覆盖 ABORT/IGNORE：宿主 policy 已移除后，完成写失败回滚原 policy 行与 cleanup；配置 fence 继续阻断交付，同一 intent 恢复后原 claim publication 不变。既有恢复 fixture 改为构造时注入 fault。

- `GOWORK=off go -C haven test -run '^TestHavenDirect.*$' -count=1 -timeout=120s -v`：PASS，6.147s；新增原子性测试后同组 `-race` PASS，14.224s。
- `GOWORK=off go -C haven test -json -race -run '^TestHaven(Direct.*|Release.*|Discard.*|RestoreStoppedTarget.*|WarmClaim.*)$' -count=1 -timeout=10m`：**PASS / exit 0，89 pass、0 fail、0 skip**，89.237s，源码摘要一致。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`、diff check：PASS；冻结结构对照累计 30 项 PASS。
- `just integration`：真实 Rust unix fixture PASS，0.369s；`just smoke`：8 组 PASS。

开发期 activation loader 移出后留有 unused encoding/json import，编译/lint FAIL；删除后上述检查通过。网络契约按当前实现明确 VM 生命周期锁与 configuration 串行化的不同调用边界，未新增锁或修改运行顺序。

日志 `build/025-services-sqlc/direct-policy-service/`，源码 SHA256 `b3600cf1c8ef1f22ef46a8949174578b4b7eaa7b86056d33a9ad14bd8645f35d`。本批未重跑完整 race；第二十五批完整通过不挪用到此树。native/KVM/kernel-isolation/production 未运行，历史 35 skip 与 FAIL/BLOCKED 保留；025 仍 IN_PROGRESS。

## 第二十七批：warm 与 pause/resume slot 交接

warm claim 冻结模型/loader、实际 WarmClaimAdmission 的 resource-parent/provision-target 协议、CurrentSlotBudget、release slot destination、resume 责任补偿和 selected pool 退役迁入 services。调用者继续提供原事务与 context，不新增提交、锁或运行时回指。原 warm activation/retirement receipt/link/fence、sleep snapshot 及费用 holder 联合核验保持；paused sandbox 与 settings 写入分别保留 terminated/body 和原 settings body 条件，使用 sandbox 私有 sqlc。根 receiver 数 175 → 168。

- `GOWORK=off go -C haven test -run '^TestHaven(WarmClaimAndFallback|ResumeFailurePreservesSleep|ResumeRetainsSandboxSlot|DataResumeConverges)$' -count=1 -timeout=120s -v`：PASS，16.622s。
- `GOWORK=off go -C haven test -json -race -run '^TestHaven(Warm.*|Resume.*|DataResumeConverges|RestoreStoppedTarget.*|ReleasePausedAndFailedCandidates|CheckpointResponsibilityRejectsSQL)$' -count=1 -timeout=10m`：**PASS / exit 0，56 pass、0 fail、0 skip**，95.434s，源码摘要一致。
- lint 0 issues、vet、架构门禁及正负例、生成正负例、`just check-sql`、diff check：PASS；冻结结构对照累计 34 项 PASS。

开发期 WarmClaimAdmission alias 跨包后仍有根 ValidateProvisionTarget 方法，编译/lint FAIL；将其实际原目标证明迁入 services 后通过，没有删除核验。使用现有真实 Service/SQLite 的 warm selection SQL 拒绝、fallback、resume 失败补偿、checkpoint 责任与 restore 退役场景，不新增 helper 单测。

日志 `build/025-services-sqlc/slot-handoffs/`，源码 SHA256 `2df9a7d489361431fea4d26b627ed859381a99db33e94301e3ffdd9333e74f47`。本批未运行完整 race、integration、smoke、native/KVM/kernel-isolation/production；历史通过不挪用，历史 35 skip 与 FAIL/BLOCKED 保留；025 仍 IN_PROGRESS。

## 第二十八批：VM release/discard 与终态投影

`services.VMDeletion` 已接管成功 generation 的 release 和失败 intent 的 discard：scoped 接纳、原 generation/请求/费用证明、stop/network/remove、持久删除 receipt、物理回收、原事务内最后退责及恢复。lifecycle、fork 补偿、supply cleanup、warm fallback 和恢复直接调用实际组件；共享原 admission/VM guards、Lifetime/Workers，backend/fault 构造时注入，没有总 Service/Options 回指。两种冻结证明保持独立；generation retirement 继续先解除 backing/lease、退出原 reservation 和 snapshot 最后责任，再删除 version/instance。

该用例的静态 SQL 全部经领域仓库；新增 operation 的状态/body、pending cleanup、未完成 cleanup 计数、pending/blocked 转移和仅按 running/input 身份完成 step 的查询，sandbox 增加原 terminated/body 更新。保留 release 与 discard 不同的完成 body 条件、UPDATE 列、恰好一行检查，以及 commit/checkpoint/fault 次序。原 warm/resume slot 补偿仍在同一完成事务执行。

`CommitPublications` 和跨领域 operation 投影已归 services，借用原事务冻结结果并提交。Catalog 直接从同一 DBTX 读取已验证的 lifecycle，不再回调根应用。fork 冻结模型/loader、cold/clone/restore/release 请求及 discard 冻结记录迁入 services；比较新增 8 个结构，共 45 个字段顺序、类型与 JSON tags 检查通过。根 Service receiver 从 168 降到 151；剩余编排和 SQL 尚未迁完。

新增 `TestHavenDeletionConflictDoesNotDispatch` 覆盖 release/discard 接纳后未派发的边界：冲突重放不调用 stop/remove，不改变 cleanup owner、64 MiB/1 slot；匹配请求完成原 input/hash，源 generation 历史不变。复用现有删除完成 ABORT/IGNORE、物理 receipt、替换目录、未知 reader、generation 原出生证明、fork/publication 和 slot 交接测试。相关旧 fixture 的 Open 后 Fault 替换改为构造注入或显式重开；没有放宽生产证明或测试断言。

- `go test ./haven/... -run '^$'`：编译 PASS。
- `golangci-lint run ./haven/...`、`go vet ./haven/...`、`just check-sql`（30 个生成文件）、架构与生成正负例、45 个冻结结构比较、`git diff --check`：PASS。
- `go test -race -count=1 -timeout=15m -json ./haven/...`：**PASS / exit 0，636 pass、0 fail、35 skip**；根包 416.410s、egress 26.864s、store 3.484s、guest 1.019s。
- `just integration`：真实 Rust unix fixture PASS，0.327s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/vm-deletion-service/`，源码 SHA256 `591ee55f333a8a17e7bc63cfc90cbd9cb5a1389545e47e91859ea79a27b627bc`；完整 race 期间仅更新文档和读取源码，结束后摘要一致。开发时 formatter 错用不支持的目录省略参数而未执行，随后按 `justfile` 的显式文件列表入口重跑并通过 lint。native/KVM/kernel-isolation/production 未运行，真实 OCI 仍缺 bwrap；历史 FAIL/BLOCKED 保留。025 继续 IN_PROGRESS。

## 第二十九批：Supply 独立回收与 ready warm 证明

`services.SupplyCleanup` 已接管 node-scoped collector 的接纳、执行、同事务完成和恢复。复用原 node request/template work/admission/supply cleanup guards、AdmissionGate 与 Workers；backend 效果由实际 VMStops、CaptureDiscard 和 VMDeletion 执行，子链接不再经根 Service 回调派发。保留具体 SupplyCleanupAdmission、原 source/pool/capture failure/retired receipt，以及 ResourceLink 的原身份与终态核验；子链接无新写入时不多做 commit/checkpoint。

collector 的全部静态 SQL 归领域仓库；sandbox 增加只按 sandbox ID 计数 settings 的查询，其余复用 operation/pool/quota/vm 的精确条件。pool/image holder/allocation fence 仍在最后一个原事务中退出，cleanup kind 和 step 的 phase/body/hash/status 条件不变。`ResourceProofs.ReadyWarm` 接管原 refill/link、generation、当前 VM、配置编码、零 slot 费用与 holder 核验；refill 完成、warm selection、collector 共用，根 `pool_types.go` 已删除。根 Service receiver 从 151 降至 147。

新增两类 Service/SQLite 行为验证：`TestHavenSupplyCleanupRetainsPoolAfterChildRetirement` 在子 VM 已实际退役后拒绝 collector 完成（ABORT/IGNORE），证明 pool、image holder 和 allocation fence 留存；匹配重放使用原有子操作 receipt 和 collector input/hash，原 refill 结果不变。`TestHavenSupplyCleanupRetiresFailedBuildCapture` 验证失败 build capture 的 stop → capture_discard → release 全链路，不重捕获、不重新创建 VM，最终费用/cleanup 责任退出且原失败结果不变。相关 fixture 改为构造时注入 Fault。

开发检查曾 **FAIL**：新增 capture 链测试最后的调用次数断言错误，根包 1.546s；原 runtime 的 before hook 返回失败会先于内部 count 增加。修正为 hook 入口的原子尝试计数，仍要求恰好一次；此前的子 receipt、VM/费用/cleanup 和历史结果断言已通过。记录在 `capture-chain-initial.txt` 与对应源码摘要中，未改生产逻辑或放宽断言。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(Supply.*|GC.*|Warm.*|Refill.*|FailedRefill.*|Pool.*|Template.*)$' -count=1 -timeout=10m`：**PASS / exit 0，30 pass、0 fail/skip**，根包 80.751s。
- lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、45 个冻结结构比较、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.520s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/supply-cleanup-service/`，源码 SHA256 `5928ef580640c551e795e50f2feb74c7cbab6fc347acd8b3874cd0b626f1cc2e`，定向 race 前后摘要一致。补充 capture 链测试前的 24 pass/74.485s 保存在 `initial/`，不代替新树结果。本批未运行完整 race、native/KVM/kernel-isolation/production；最近完整 race 仍绑定第二十八批。历史 FAIL/SKIP/BLOCKED 保留，025 仍 IN_PROGRESS。

## 第三十批：实际 GarbageCollection 与生成参数负例

`services.GarbageCollection` 已接管 node-scoped 接纳、只读 root 清点、候选核验、冻结 pass 进度与恢复。直接协调 Images、SupplyCleanup、SnapshotDeletion、CaptureCleanup，并由 Operations 等待原 scope 的子操作；不回指根 Service。共享原 node request/GC inventory/artifact/GC operation/admission guards、AdmissionGate 与 Workers。默认 batch=16、budget=2 秒、batch 上限 1024、原 deadline、候选排序、批次截断、dry-run 与 unknown/deferred 规则保持。根 Service receiver 从 147 降至 137。

GC 静态业务 SQL 已归 operation/quota 仓库，保留 step 的 rowid 顺序、stop 的 rowid 倒序、capture link 的 JSON 类型比较与原 cleanup 条件。`store.HasForeignKeyViolations` 统一承接原 PRAGMA 结构检查，startup 和 GC 继续在原位置读取；query/row/close 错误保留未知。TemplateCatalog 直接读取同一 DBTX 的原 supply step，删除根 publisher 回调与副本结构。

sqlc v1.30.0 首次生成暴露两个问题：JSON child ID 被推断为 BLOB；NOT 节点下面的第二个绑定未被遍历，命名参数也会原样残留。开发编译/lint因此失败；另有已删除 GCItem/snapshotRecord 别名的调用/unused 问题，均已修正。JSON 参数使用既有 CAST+空串连接策略；snapshot 引用排除条件用 De Morgan 等价式，保持原谓词与 NULL 逻辑。176 个真实 modernc SQLite 场景覆盖 TEXT/BLOB/numeric/NULL 和 JSON 比较；`check-sql` 新增未展开参数宏拒绝，外部命令负例重现 NOT 缺陷并确认 gate 拒绝。

GC 冻结模型/loader 归 services，48 个结构的字段顺序/类型/JSON tag 比较通过。新增 `TestHavenGCCompletionRetainsChildProgress` 在子 collector 已成功后对 GC 最终 step 写入注入 ABORT/IGNORE，证明已提交 progress 和子结果保留，匹配重放使用原 inventory/hash/budget/deadline，不重复 stop/remove，不改写原 supply 失败。native GC fixture 的 importer Fault 也改为构造注入；该原生路径本批仍受工具缺项阻断，编译通过不算原生验收。

- 新 GC 完成边界定向 race：PASS，根包 9.234s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，643 pass、0 fail、35 skip**；根包 434.740s、egress 26.815s、store 3.579s、guest 1.018s。
- lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、48 个冻结结构比较、176 个 SQL 谓词兼容场景、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.352s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/garbage-collection-service/`，源码 SHA256 `eb6940a5029f64409c482eb1430213abc8c76e73e9a26253db26fa4df1c161ae`；Go/SQL/YAML 在完整 race 前后摘要一致，运行期间补充的生成 gate 脚本另经命令负例验证。native/KVM/kernel-isolation/production 未运行；真实 OCI 缺 bwrap、required-native runner 条件和历史 FAIL/BLOCKED 保留。provisioning、activation、供给/生命周期/数据面编排及剩余应用状态尚未迁完，025 继续 IN_PROGRESS。

## 第三十一批：activation 历史证明与父级事务交接

`ResourceProofs.Activation` 接管冻结 resource/hash/witness、pending direct retirement、generation、当前 sandbox/VM、configuration fence、settings、原 request scope/hash 和 step target 的核验，读取顺序不变。operation 仓库新增 RequestIdentity、StepTarget 和 live body 读写，sqlc 保留原投影与 predicates。

`CompleteWarmClaim` / `CompleteLifecycleActivation` / `RetireWarmClaimLease` 归 services。selected pool/source lease、fallback 的原 release receipt、resume 的 sleep 引用/费用、rollback lease 和 parent step/result 均借用原 activation 事务交接；无额外事务、提交、checkpoint 或重试。原 body/hash/status 条件、SET 列与 publication 顺序保持。根 Service receiver 从 137 降至 134。

新增 `TestHavenResumeActivationHandoffRejectsSQL`：activation 最终 step 写入 ABORT/IGNORE 时，先前父级交接整体回滚，补偿保留同一 sleep snapshot 的引用和费用；显式再次 resume 能交付 running 并释放旧 sleep 责任，原 pause/失败结果不改写。初次测试使用错误的 quota_refs 列名，FAIL/3.207s；修正后普通 sandbox fixture 缺 memory manager state，在 admission 前 FAIL/3.371s。已改用现有 restorable fixture，未放宽生产校验；两个开发失败日志分别保留在 `boundary-initial.log`、`boundary-restore-fixture.log`。最终边界 race PASS/20.014s。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Activation.*|Configure.*|Warm.*|Resume.*|Rollback.*|Fork.*|Direct.*|Sandbox.*|Lifecycle.*|Restore.*|Data.*|Pause.*|Checkpoint.*|Claim.*)$' -count=1 -timeout=10m`：**PASS / exit 0，149 pass、0 fail、0 skip**，根包 200.323s。
- lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、48 个冻结结构比较、diff check：PASS。

日志 `build/025-services-sqlc/activation-handoff/`，源码 SHA256 `df61b6c75db3f4286eecfa8d87a2e786aa3f65720916a74860aa13a1cbd8f2c0`，定向 race 前后摘要一致。本批未运行完整 race、integration/smoke、native/KVM/kernel-isolation/production；最近完整 race 与 Rust fixture 仍绑定第三十批。activation 执行服务与剩余 provisioning/供给/生命周期/数据面尚未完成，025 仍 IN_PROGRESS。

## 第三十二批：实际 Activations 执行服务

`services.Activations` 已承接实际 guest/network/egress activation、分阶段 configure settings 保存、network policy 交接、running publication 与失败保留。由原 provisioning、warm、resume/rollback、configure 与 recovery 路径直接调用；没有根 Service 回调或可变 Options 依赖。构造时固定 backend、guest、egress、CA、daemon epoch 和 fault，借用原 store、Lifetime context、admission/configuration guards 与 access registry；父 runner 保持 VM guard 和 worker custody。根 Service receiver 从 134 降至 122。

Admin/Workload readiness、NIC off → policy → guest activation → NIC on 顺序、RPC 后 fence 重验、原 instance/history proof 和 binding signer 保持。失败清理继续使用独立 10 秒 context，仅原 fence owner 能 revoke；superseded worker 不撤销继任者。operation secrets 仍在 CommitPublications 成功后移除，再 checkpoint、执行原 after-commit fault。新的组件 Continue 入口明确拒绝 reviewed metadata 效果。静态读写走 operation/sandbox/vm 仓库，settings、running/body、fence、step、operation 的条件与 SET 列和顺序保持。

cold/sandbox/direct fixture 改为构造时注入实际 guest/egress，direct fixture 的既有模拟 callback 仍只属于测试替身。worker 提交后等待和 malformed warm startup 的 activation fault 同样在构造时注入；不再通过修改根 Options 影响已构造组件。

- claim、direct、worker custody、malformed warm startup 与 resume ABORT/IGNORE 定向 race：PASS，根包 28.182s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，646 pass、0 fail、35 skip**；根包 446.014s、egress 26.900s、store 3.587s、guest 1.025s。
- lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、48 个冻结结构比较、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.480s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/activation-service/`，源码 SHA256 `74838fd08cebf5b222326c1a2ad21a6daed56d98745882e349a0a8f11ac51ed0`，完整 race 前后摘要一致。native/KVM/kernel-isolation/production 未运行，历史 FAIL/SKIP/BLOCKED 保留。剩余 provisioning、供给/生命周期/数据面、静态 SQL 与应用状态迁移仍未完成，025 继续 IN_PROGRESS。

## 第三十三批：provisioning 证明与 activation 接纳

`ClaimAdmission` / `ProvisionTarget` 归 services；50 个结构的字段顺序、类型和 JSON tag 比较通过。`Activations.AdmitIn` 在原资源事务内依次写 credential MAC、settings、configuration fence 和 activation step，仍使用原 Lifetime context；registry 登记与 dispatch 仍在外层 commit 后执行。INSERT 保留拒绝重复的语义，不改成 UPSERT。

`ResourceProofs.ProvisionIntent` 接管原请求/result、tenant scope、clone/restore source、stop history、当前目标、完整 generation、template owner/restore relations、image/instance/version、reservation 和 quota/reference 证明，按原顺序核验。operation.RequestState 单次读取原 request/status/body 投影。cold 完成剩余 SQL 归 sandbox/operation 仓库；保留 provisioning 条件、原 SET 列、pending storage→disk/引用责任交接、step/result/fault/commit/checkpoint 顺序。根 Service receiver 从 122 降至 119。

首次 sqlc 生成对无列名 INSERT 报 `INSERT has more expressions than target columns`，已显式指定原两列及顺序，原失败保存在 `generate-initial.log`；embedded struct 字段空行 lint 失败已修正并记录。未改 schema、trigger 或业务断言。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*(Cold|Clone|Restore|Provision|Instance|Admission|Responsibility|Completion|Activation).*|Warm.*|Fork.*|Template.*|Supply.*|Refill.*|FailedRefill.*|Resume.*|Rollback.*|Claim.*)$' -count=1 -timeout=10m`：**PASS / exit 0，213 pass、0 fail、0 skip**，根包 225.096s。
- 补充 `GOWORK=off go -C haven test -json -race -run '^TestHavenStep(CrashRecovery|AbruptSQLiteRecovery|ConcurrentReplayAndCancellation|MissingSourceAndQuotaRetainState)$' -count=1 -timeout=5m`：**PASS / exit 0，21 pass、0 fail、0 skip**，12.785s，覆盖主选择未匹配的原 step 崩溃、取消与责任保留场景。
- lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、50 个结构比较、diff check：PASS。

日志 `build/025-services-sqlc/provision-protocol/`，源码 SHA256 `c436edaa14eb9b60301c3b2a8bde1f21b8f29a9a7b84207d09c302f8b87c5d67`，两次定向 race 前后摘要一致。本批未运行完整 race、integration/smoke、native/KVM/kernel-isolation/production；最近完整 race/Rust fixture 仍绑定第三十二批。025 继续 IN_PROGRESS，下一步迁移实际 cold/clone/restore 执行器与剩余 admission。

## 第三十四批：实际 VMProvisioning 执行器

`services.VMProvisioning` 已承接 cold/clone/restore 的实际执行，原恢复路由最后调用该组件。原 VM guard 内的 step 重读、路径/当前归属核验、Prepare/Launch/Boot/System readiness、磁盘 identity、restore copy/publish/sync、backing/lease 交接、资源完成及失败保留均由服务直接执行；成功后调用 Activations。现有 admission/recovery dispatcher 保持原 worker custody 和路由顺序，组件没有根 Service 回调或独立执行池。backend/fault 构造注入，共享原 Lifetime、admission/VM guards 和 store。根 Service receiver 从 119 降至 110。

所有迁入的静态 SQL 经 operation/quota/snapshot/vm/sandbox 仓库；原 predicates、SET 列、写入与 fault/commit/checkpoint 顺序保持。SQL/inspection 失败保留 unfinished step；明确 runtime 失败固定原失败结果，保留 candidate、quota、reservation 与 dependency。已有退出候选不重新 launch，未完整 restore materials 不补造证明；新的组件 Continue 入口拒绝 reviewed metadata 效果。

restore/discard/target 故障 fixture 改为构造注入或共享 atomic fault switch，崩溃 fixture 的持久 backend 和 shutdown fixture 的 blocking closer 同样经构造注入，继续命中原阶段而非修改根 Options。迁移后无调用者的 expectedCurrentInstance 根别名因 unused lint 失败而删除，原日志保留。

- restore 运行中写入、未证明目录保留、真实子进程 SQLite abrupt recovery、shutdown backend deadline 定向 race：PASS，7.698s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，646 pass、0 fail、35 skip**；根包 426.703s、egress 26.889s、store 3.288s、guest 1.018s。
- lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、50 个结构比较、diff check：PASS。
- `just integration`：真实 Rust unix fixture PASS，0.433s；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/vm-provisioning-service/`，源码 SHA256 `feb3f4a460cead12d91c3c39aa22ae063b54bf4e2f90b9e5a36ba6765418fedb`，完整 race 前后摘要一致。native/KVM/kernel-isolation/production 未运行，历史 FAIL/SKIP/BLOCKED 保留。resource admission、供给/生命周期/数据面、剩余静态 SQL 与应用状态仍待迁移，025 继续 IN_PROGRESS。

## 第三十五批：资源事务接纳归 VMProvisioning

`VMProvisioning.AdmitIn` 接管模板/镜像/source、tenant 配额、节点 reservation、slot 和 parent 证明核验，以及 operation/resource step、sandbox、VM/instance/version、reservation、费用/引用、source lease、activation 的原事务写入。普通接纳和 fork batch 直接调用；外层 admission lock、fault、commit、checkpoint、secret 登记和 dispatch 位置保持，未新增事务或 runner。`ProvisionCapacity` 固定构造时的内存/vCPU/安全余量预算，数值和边界表达式不变。

sandbox 新增与复用写入归其 sqlc；复用仍要求原 state/body，slot cost 仍要求原 source、1 slot、0 storage。provisional template 来自冻结 parent 的 ProvisioningTemplate；cold template 判定归 TemplateCatalog，sandbox identity 创建归 services。字段、SET 列、写入顺序和父级接纳时点保持。根 Service receiver 从 110 降至 109。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Admission.*|.*Quota.*|Step.*|Fork.*|Warm.*|Template.*|Supply.*|Refill.*|FailedRefill.*|Claim.*|Resume.*)$' -count=1 -timeout=10m`：**PASS / exit 0，150 pass、0 fail、0 skip**，根包 147.657s。
- 编译、lint（0 issues）、vet、check-sql（30 个生成文件）、架构与生成正负例、50 个结构比较、diff check：PASS。

日志 `build/025-services-sqlc/provision-admission/`，源码 SHA256 `c13ca7f12df186ee8da70fbc53c8c3a2a6d391be07a46f19d281f7800cb38715`，定向 race 前后摘要一致。本批未运行完整 race、integration/smoke、native/KVM/kernel-isolation/production；最近完整 race/Rust fixture 仍绑定第三十四批。025 继续 IN_PROGRESS。

## 第三十六批：SandboxCommands 命令与凭据策略

`SandboxCommands` 接管完整命令的 role/scope MAC、环境变量拆分、secret 注入限制、网络模式、TTL/on_expire 规则和已接纳 operation 的凭据重放比较。构造注入原 lifetime context、operation 仓库、固定 MaxTTL 和 store signer；不持有根 Service/Options。claim、configure、fork、模板与生命周期接纳直接调用；sanitize 保持 map 非 nil、字段顺序、secret 值清空与 Hosts 拷贝语义，原输入仍参与 MAC。store 不再承载 command MAC 业务规则；sandbox token 直接复用既有 Authentication。根 receiver 从 109 降至 106。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(SandboxOperationAuthorization|SecretRestart|Configure.*|Claim.*|Direct.*|TemplateRootOwnerAndPublicationProvenance|TemplatePutOwnerAndReplay|.*Admission.*|Fork.*|Lifecycle.*|Resume.*|Rollback.*|Checkpoint.*|.*Lease.*|.*Renew.*)$' -count=1 -timeout=10m`：**PASS / exit 0，124 pass、0 fail、0 skip**，根包 144.909s。
- 编译、lint、vet、check-sql、架构与生成正负例、50 个结构比较、diff check：PASS。

日志 `build/025-services-sqlc/sandbox-commands/`，源码 SHA256 `bdb61da51b992f78a3d72a9d8b80bdc511a56acf5a2c004cc074b21fc58734f7`，定向 race 前后摘要一致。未运行完整 race、integration/smoke、native/KVM/kernel-isolation/production；025 继续 IN_PROGRESS。

## 第三十七批：cold/clone/restore 接纳与共享调度归 VMProvisioning

`VMProvisioning` 接管 cold/clone preflight、资源接纳外层事务、credential replay、commit 后 secret 登记、checkpoint/fault 与 shared worker dispatch，以及原 VM restore 的全部接纳证明。构造注入同一 AdmissionGate/Workers/access/Lifetime、DirectPolicies、固定 ProvisionOptions；普通资源 runner 先续接 direct retirement，再进入原资源执行。应用恢复路由顺序不变，没有根 Service 回调、第二执行池或关闭所有者。

restore 独立 closing/reviewed 判断、原 stop/generation/当前 stopped owner、祖先与 instance/version 数量、峰值费用和 restore reservation 仍按原顺序核验。operation/step、sandbox 原 state/body、VM claim、instance/version、费用/source lease/引用仍使用原事务及精确条件。tenant/key credential SQL 归 operation；严格 SUM 归 quota.FundedUsage。sqlc 对普通 SUM 推导 sql.NullFloat64，故使用 INTEGER 投影避免整数舍入；不加 COALESCE，NULL 仍扫描失败。新增真实 Service/SQLite 的 `TestHavenRestorePeakQuotaPreservesIntegerBoundary`，在 2^53+1 的费用边界证明差 1 MiB 必拒绝且不新增 operation/generation，补足后成功。backup 和 caller-cancellation fault 改为构造注入的 atomic 开关。

开发中 embedded field 顺序/冗余 selector、已移动 helper/alias 的 unused lint 与遗漏 vm.GenerationRef 包限定的编译失败保留；修正后无生产语义放宽。根 Service receiver 从 106 降至 101。

- 定向 race 边界 PASS；restore/并发重放/backup/warm 边界 15.588s，大整数配额行为测试 PASS。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，647 pass、0 fail、35 skip**，根包 435.777s；egress 26.922s、store 3.331s、guest 1.018s。
- `just integration`：PASS，真实 Rust Unix fixture，0.383s；`just smoke`：8 组 PASS。
- lint、vet、check-sql（30 个生成文件）、架构/生成正负例、50 个冻结结构比较、diff check：PASS。

日志 `build/025-services-sqlc/provision-admission-service/`，源码 SHA256 `b7d745074cda7a5f666117ea6c9b642d82240f5fa7264c0512c219b655d52c0e`，完整 race 前后摘要一致。native/KVM/kernel-isolation/production 未运行；required-native 与 OCI 环境阻断仍有效。供给/生命周期/数据面编排、剩余静态 SQL 和应用状态未迁完，025 继续 IN_PROGRESS。

## 第三十八批：Availability 与登记 guard 的唯一所有权

`execution.Guards.Registration` 唯一持有原数据登记/closing/admission 的短锁；根组装仅借用其 Guard，不再拥有 mutex。`services.Availability` 独占进程内 draining 状态并使用同一 Registration guard，claim/refill/fork/maintenance/status 读取和 Drain 的原临界区长度及顺序保持。Close、data setup 和 HTTP handoff 继续借用相同 guard；没有新增关闭标记或 worker 状态。

新增 `TestHavenDrainPreservesReplayAndRunningAdmission`，通过真实 Service/SQLite 验证 drain 可由管理状态读取、完成请求重放不变、新 claim 拒绝且不留 intent、已有 running sandbox 仍通过数据接纳。原 shutdown/writer 保留、最终数据 guard 和 downstream handoff 边界继续通过。根 receiver 从 101 降至 100。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(Drain.*|.*Shutdown.*|Data.*|Direct.*|Downstream.*|Claim.*|.*Admission.*|Lease.*|Fork.*|Refill.*|Template.*|Warm.*)$' -count=1 -timeout=10m`：**PASS / exit 0，95 pass、0 fail、0 skip**，根包 112.430s。
- lint、vet、check-sql、架构/生成正负例、50 个冻结结构比较、diff check：PASS。

日志 `build/025-services-sqlc/availability-registration/`，源码 SHA256 `503c8e72441908a008f0a0ff29fd00ba22c5714f398777b3b9d63882d05a6976`，定向 race 前后摘要一致。该批未运行完整 race、integration/smoke、native/KVM/kernel-isolation/production；最近完整验证仍绑定第三十七批。025 继续 IN_PROGRESS。

## 第三十九批：实际 Supply 服务与供给 SQL

`Supply` 接管 template put/build、refill、资源 parent 接纳、warmup/capture/release 协调、模板与 warm publication、失败留责和恢复；应用与 controller 调用实际服务。固定 SupplyOptions 只含窄 Backend/Guest、MaxWarm 和 fault；SupplyDependencies 指向实际命令、可用性和资源服务，共享原 tenant/template/VM/admission guards、AdmissionGate 与 Workers。没有根 Service 回调。Root recovery 的 supply 路由位置、template guard 与重读步骤保持。

`SupplyAdmission` 在原子资源事务中固定 parent/credential/supply step/image holder/template intent 或 pool reservation/link。`ResourceLinks` 保留子执行、ErrInterrupted、完成回执事务、checkpoint 和终态重读的原顺序；Supply 提供实际子服务执行端口，根生命周期/fork 暂时直接借用 Complete。`SandboxCommands.Existing` 保留 scoped publication → credential MAC → 原内存 secret 登记，调用业务服务派发原 worker。

原 supply INSERT/状态更新/结果/cleanup、待完成 link 计数和 refill backoff SQL 均归 operation 私有 sqlc；template/pool/quota 交接继续原仓库和原事务。warmup_inflight 未知结果保留原资源，失败 supply 等待 pending links，费用、pool 和 image holder 不因失败提前退责。backoff 保留 rowid DESC/LIMIT 10、遇到成功停止 JSON 解码与时间算法。warmup crash fixture 改为构造 fault 注入；三个不再使用的根 supply 类型别名 lint 失败已保留并清除。根 receiver 从 100 降至 88。

- warmup crash、golden claim、refill 占用/失败、root owner 与 replay 定向 race：PASS，14.460s。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，648 pass、0 fail、35 skip**，根包 441.996s；egress 26.850s、store 3.342s、guest 1.031s。
- `just integration`：PASS，真实 Rust Unix fixture，0.472s；`just smoke`：8 组 PASS。
- lint、vet、check-sql（30 个生成文件）、架构/生成正负例、50 个冻结结构比较、diff check：PASS。

日志 `build/025-services-sqlc/supply-service/`，源码 SHA256 `81f8b1a0d02a8246543fca81490d258359849194fdc755592a649a6665709db1`，完整 race 前后摘要一致。native/KVM/kernel-isolation/production 未运行。Lifecycle、fork/warm claim、数据面与剩余 SQL/恢复组装尚未迁完，025 继续 IN_PROGRESS。

## 第四十批：实际 WarmClaims 与 warm 事务 SQL

`WarmClaims` 接管 ready warm 选择、warm activation、原 release/golden fallback、失败留责和 release supersession；应用恢复保留原 warm 路由位置、Sandbox guard 与重读。构造注入共享 AdmissionGate/Workers/access、Sandbox/VM/Configuration/admission guards 及实际 Activations/VMProvisioning/VMDeletion/DirectPolicies/ResourceLinks；没有根回调或额外关闭状态。tenant request guard 仍由 claim 调用者持有。

接纳按原顺序保留 template/source/ready pool/tenant budget 证明、operation/credential/warm step、settings/fence、activation、sandbox 原 body、slot 费用、pool/ref/source lease，以及 commit → secret → checkpoint → fault → dispatch。sandbox 选择 body 更新使用精确 provisioning/旧 body 条件；warm 进度与失败写、activation 和 fence/result 读取归原领域仓库。`RetireSupersededIn` 借 release 原事务核验并退出原 pool/lease，最后 cleanup intent 更新保留 0 行合法的原 :exec 语义。两个未再使用的根 warm 类型别名 lint 失败保留并清除。根 receiver 从 88 降至 81。

- warm/pool/golden claim/lifecycle handoff 定向 race：PASS。
- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Warm.*|.*Pool.*|Claim.*|Direct.*|Lifecycle.*|Resume.*|Rollback.*|Sandbox.*|StepShutdown.*|SupplyCleanup.*|Release.*|.*Activation.*)$' -count=1 -timeout=10m`：**PASS / exit 0，62 pass、0 fail、0 skip**，根包 127.733s。
- lint、vet、check-sql、架构/生成正负例、50 个冻结结构比较、diff check：PASS。

日志 `build/025-services-sqlc/warm-claim-service/`，源码 SHA256 `d3ce1dc5a5520b36b515fef3449f10b3e0997b525e4a157b963b0cb2fb0c72fc`，定向 race 前后摘要一致。此批未重跑完整 race、integration/smoke、native/KVM/kernel-isolation/production；最近完整验证仍绑定第三十九批。025 继续 IN_PROGRESS。














## 第四十一批：实际 Lifecycle 与生命周期 SQL

`Lifecycle` 拥有 pause/resume、rollback、release、checkpoint 创建/删除的接纳、执行、失败保留与恢复。应用恢复保持原路由位置及 Sandbox guard 内重读；子操作直接协调实际 Captures/VMStops/VMDeletion/CaptureDiscard/CaptureCleanup/VMProvisioning/Activations，使用原 ResourceLinks 回执协议。共享 request/configuration/sandbox/VM/admission guards、Workers、AdmissionGate、access registry 和 store，没有根 Service 回调或第二份执行状态；Backend.Check、PausedTTL 和 fault 构造注入。

operation/sqlc 新增原 rowid 倒序的成功 pause 枚举和失败 lifecycle 子步骤计数；sandbox/sqlc 区分 terminated 的仅 body 更新、paused 的仅 body 更新及 paused→terminated 更新。其余 SQL 使用原仓库的精确投影、SET 列、body/hash/status 和恰好一行条件。paused rollback 仍直接 Commit，其他原冻结终态路径保留 CommitPublications；事务、secret/connection 撤权、checkpoint 与 fault 顺序不变。Catalog.OwnedRecord 接管同库 owner/visibility 核验，SandboxCommands 接管原凭据缺失判定，access 先快照连接集合再在锁外关闭。根 receiver **81→55**。

- 初始编译的端口/类型名和 reflect import 错误、随后 unused 过渡别名 lint 失败均已修正；保留 `compile-initial.log`、`compile.log`、`lint.log`，最终编译/lint 退出 0。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，648 pass、0 fail、35 skip**，根包 **441.688s**。覆盖 checkpoint ABORT/IGNORE、resume activation 责任交接拒绝、失败 resume sleep 保留、rollback、paused release、重启与关闭边界。
- `just integration`：PASS，真实 Rust Unix fixture **0.303s**；`just smoke`：8 组 PASS。
- `go vet ./haven/...`、`just check-sql`、architecture/SQL generation 正负例、50 个冻结结构比较、`git diff --check`：PASS。

日志 `build/025-services-sqlc/lifecycle-service/`；源码 SHA256 **`7224c51c4f74553fda5ab724fa5dc95161dbdf8ef34aacc3cb28c7c4bc3837e7`**，全量 race 前后相同。35 个 native skip 及既有 required-native/OCI BLOCKED 保留；未运行 KVM、kernel-isolation 或 production。025 继续 IN_PROGRESS，租约、fork、configure、数据面与最后组装/过渡层仍待迁移。

## 第四十二批：Lifecycle 租约与 reaper

renew 接纳及到期回收归 `Lifecycle`，MaxTTL/PausedTTL 在构造时固定；原 tenant request/configuration/admission guard 顺序、同事务 owner/lease/fence 核验、operation/credential/step/settings 写入、publication→checkpoint 和 fault 位置保留。运行中与 paused 分别使用原期限和上限，过期租约不复活；重放不再次延长期限。reaper 的有序 running/paused + settings 枚举归 sandbox 私有 sqlc，完整读取/解码失败不提交任何到期请求；提交继续携带原 observed deadline，后续 renewal 赢得 configuration guard 时拒绝过期的回收请求。根 receiver **55→53**。

新增 `TestHavenRenewRejectedWriteKeepsLeaseAndRequest`，以私有 Service/SQLite 覆盖 running/paused × ABORT/IGNORE：拒绝 settings 写入时不提交请求、不修改 lease/configuration；移除故障后原 key 成功，重放保持同一 operation 和期限。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Lease.*|.*Renew.*|.*Reap.*|Lifecycle.*|Resume.*|ReleasePaused.*|Paused.*|Data.*|.*Configure.*|.*Configuration.*)$' -count=1 -timeout=10m`：**PASS / exit 0，58 pass、0 fail、0 skip**，根包 **85.678s**。
- generation、编译、lint、vet、check-sql、architecture/SQL generation 正负例、50 个冻结模型比较、diff check：PASS。

日志 `build/025-services-sqlc/lease-service/`；源码 SHA256 **`7804a509105598f5207f7dbe5f897088e2dc8c5de0b02dc79d8619b8935cfe6c`**，race 前后一致。该批未重跑全量 race、integration/smoke、native/KVM/kernel-isolation/production；最近完整验证仍绑定第四十一批。025 保持 IN_PROGRESS。

## 第四十三批：实际 Sandboxes claim/configure

`Sandboxes` 接管逻辑 claim/configure 接纳与匹配请求重放派发；实际 WarmClaims/VMProvisioning/Activations/DirectPolicies 继续拥有资源和 activation 执行，使用同一 Workers、guards、access 和 closing。backend Check/Network、guest capability 与 fault 构造注入，没有根回调。claim 保留命令/MAC、draining、模板/image/source 验证及 warm 选择顺序；configure 保留原配置期限、observed generation、fence、source witness 和角色凭据核验。

配置事务 SQL 归域仓库；新增 CompletePendingOperation 保留 paused configure 原 pending-only 状态谓词，不复用更宽的 pending/running 完成写。running 配置仍先提交持久撤权，随后在独立 10 秒上下文关闭网络和撤销 binding；失败调用原 BlockGuarded，成功登记 secret、fault 后派发。COMMIT/checkpoint 失败返回与原已接纳身份保持。清除三个 unused 根 helper/别名的 lint 失败保留在日志中。根 receiver **53→50**。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Claim.*|.*Configure.*|.*Configuration.*|.*Sandbox.*|.*Warm.*|.*Activation.*|.*Secret.*|.*Credential.*|.*Binding.*|.*Data.*|.*Direct.*|.*Drain.*|.*Renew.*|.*Lease.*|.*Admission.*|.*Shutdown.*)$' -count=1 -timeout=10m`：**PASS / exit 0，123 pass、0 fail、0 skip**，根包 **141.657s**。
- `just integration`：PASS，真实 Rust Unix fixture **0.301s**；`just smoke`：8 组 PASS。
- generation、编译、最终 lint、vet、check-sql、architecture/SQL generation 正负例、50 个冻结模型比较、diff check：PASS。

日志 `build/025-services-sqlc/sandbox-service/`；源码 SHA256 **`889fd685071c8e9bb53abcdf9ea19d903b5dc1aacac31477e5d5887f62a51c00`**，定向 race 前后一致。该批未重跑全量 race、native/KVM/kernel-isolation/production；最近全量 race 仍绑定第四十一批。025 保持 IN_PROGRESS，下一步为 fork、数据面和最后应用组装/过渡层。

## 第四十四批：fork 静态 SQL 与完成写拒绝

fork 接纳、目标预留、分支 settings 交接、阶段推进、未分配目标退役、discard tombstone 退役和 group 完成的全部静态 SQL 已归 operation/sandbox 私有 sqlc。新增 fork branch link 计数保留原 step_key LIKE 条件；新终态写保留 status/body SET 列和 pending/running/hash 条件；settings 删除保持原 body 比较，未分配目标退役保持 provisioning/body 条件。所有仓库均借原事务，不自行提交，group 仍先验证原分支 publication，再交接 parent source/image 责任和终态。

新增 `TestHavenForkCompletionRejectsSQL`：成功分支已交付后拒绝 group 完成写（ABORT/IGNORE），父 source lease 保留、父 publication 不出现；修复后匹配重放消费原分支交付，不重复 launch/clone/capture。初始测试误用不存在的 snapshot_leases 表失败 **9.181s**，改为实际 source_leases 后通过 **10.170s**；初始失败保留 `completion.log`。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Fork.*|Checkpoint.*|.*Clone.*|.*Provision.*|.*Quota.*|.*Lifecycle.*|.*Deletion.*|.*Discard.*)$' -count=1 -timeout=10m`：**PASS / exit 0，133 pass、0 fail、0 skip**，根包 **93.769s**。
- generation、编译、lint、vet、check-sql、architecture/SQL generation 正负例、50 个冻结模型比较、diff check：PASS。
- 生产 fork 文件已无 Query/Exec 或手写 SQL；根 receiver 仍为 **50**，完整 fork 服务迁移尚未完成。

日志 `build/025-services-sqlc/fork-sql/`；源码 SHA256 **`62efd7842ff18ae130183f20b68823541c2a7922200e2d5eb13a5c0f07332372`**，定向 race 前后一致。该批未重跑全量 race、integration/smoke、native/KVM/kernel-isolation/production；最近全量 race 仍绑定第四十一批，integration/smoke 绑定第四十三批。025 保持 IN_PROGRESS。

## 第四十五批：fork/data 共享持久证明

`Catalog.Unfenced` 与 `Catalog.ResourceForData` 接管原配置 fence 查询和原 source/instance 的持久就绪证明。fork 接纳直接调用 Catalog，不再依赖根数据接纳 helper；数据面与 proxy 的原 helper 仅转发同一证明。保留原 record→source→VM 读取顺序、成功 witness/current instance、owner 空值、Verified/AgentReady/Deleting 条件；没有据此省略调用者的授权、lease、实时 backend、network 核验或新增效果。初始 lint 对显式嵌入字段选择器的 QF1008 已修正。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Data.*|.*Fork.*|.*Direct.*|.*Proxy.*|.*Binding.*|.*Admission.*|.*Ownership.*)$' -count=1 -timeout=10m`：**PASS / exit 0，85 pass、0 fail、0 skip**，根包 **80.443s**。
- 编译、最终 lint、vet、check-sql、architecture/SQL generation 正负例、50 个冻结模型比较、diff check：PASS。

日志 `build/025-services-sqlc/catalog-resource/`；源码 SHA256 **`072e61f8b9807d113b489cdab29f9cc4158f659208c5da7d8bc0282e2a3f1107`**，定向 race 前后一致。根 receiver 仍为 50；未重跑全量 race、integration/smoke、native/KVM/kernel-isolation/production。下一步仍是实际 Forks、数据面及应用组装，025 保持 IN_PROGRESS。

## 第四十六批：实际 Forks 服务

`Forks` 拥有 fork 接纳、原 source/checkpoint 核验、批量 forecast/slot 预留、capture handoff、同事务 branch 接纳、分支执行与等待、原 branch publication 汇总及失败补偿。应用恢复保留原 fork 路由位置、ForkGroup guard 内重读和终态短路；分支继续使用共享 Workers、原 direct retirement 前置和实际 VMProvisioning。capture/discard/lifecycle 子回收直接调用实际服务并消费 ResourceLinks，没有根 Service 调度回指。

父 capture、branch target 和 cleanup admission 的具体历史证明与原事务一起迁入 services；容量使用构造时固定的 ProvisionCapacity，预算计算、slot holder、原 fields/hash/nil 与各 fault/commit/checkpoint 位置保持。全部目标和真实 branch 责任先在一个事务提交，再启动分支效果。完成只消费原 branch publication；失败等待所有回收 receipt。删除根 unused publication/link/identity/restore helper 与类型别名，移除根 fault/admission/getOperationIn 业务转发；根 receiver **50→35**。初始 unused lint 失败保留。

fork 重启 fixture 改为构造注入 snapshotFaults 原子开关，重开同一私有 store 时重新注入；不修改运行中的 root Options。五个原中断边界通过 **26.108s**，包括已完成分支续期后 group 仍消费旧冻结交付。

- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，656 pass、0 fail、35 skip**，根包 **451.747s**，覆盖第四十二至四十五批新增用例与当前服务接线。
- `just integration`：PASS，真实 Rust Unix fixture **0.379s**；`just smoke`：8 组 PASS。
- 编译、最终 lint、vet、check-sql、architecture/SQL generation 正负例、50 个冻结模型比较、diff check：PASS。

日志 `build/025-services-sqlc/fork-service/`；源码 SHA256 **`5884bd427c7035bdf8fddcff98db750cf8c5e82f7861d4f57bc549ca0adbb3d9`**，全量 race 前后一致。35 个 native skip、既有 required-native/OCI BLOCKED 保留；未运行 KVM、kernel-isolation 或 production。025 保持 IN_PROGRESS，剩余数据面、代理宿主回调、应用调度与最后过渡层清理。

## 第四十七批：实际 Data 服务

`Data` 接管 live readiness、自动 resume、network witness/repair、restart rebind、最终数据接纳、Connect/Forward、HTTP AttachData、连接 lease 监控及 preview capability。它借用原 Sandbox/AutoResume/Configuration/Registration guards、Lifetime、access registry、signer 和 daemon epoch，直接协调 Catalog、SandboxCommands、Sandboxes、Lifecycle、Operations、Activations；窄 Inspect 与 Connect/Forward effects 构造注入，无根回指或独立计数/锁。

保留 setup 的嵌套 Begin/End、最后配置 guard 下重检、registration guard 内 Begin/Track、HTTP accept 后的相同登记，以及每连接 100ms lease 检查。迟到连接仍关闭，关闭 snapshot 与登记共用一把短锁。auto-renew 使用原 settings body CAS 和 checkpoint；restart rebind 不续租，secret 缺失仍需显式 resupply。direct network repair 保留 observed generation 与独立 10 秒上下文。running subject 查询由 sandbox 私有 sqlc 保留原 settings JOIN 和 id 顺序，完整读取失败不执行 rebind；reviewed metadata 不执行 ReconcileBindings。preview-v1 MAC、base64、字段/tag/顺序、出生 epoch 和到期检查保持，冻结模型比较扩展至 **51** 项。根 receiver **35→18**。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Data.*|.*Preview.*|.*Forward.*|.*Connect.*|.*Binding.*|.*Direct.*|.*Secret.*|.*Credential.*|.*Configure.*|.*Configuration.*|.*Resume.*|.*Shutdown.*|.*Lease.*|.*Renew.*|.*Availability.*|.*Drain.*)$' -count=1 -timeout=10m`：**PASS / exit 0，75 pass、0 fail、0 skip**，根包 **91.568s**。
- `just integration`：PASS，真实 Rust Unix fixture **0.374s**；`just smoke`：8 组 PASS。
- generation、编译、lint、vet、check-sql、architecture/SQL generation 正负例、51 个冻结模型比较、diff check：PASS。

日志 `build/025-services-sqlc/data-service/`；源码 SHA256 **`83850f3b270a8fdfc98c338dc3cea59d287fffaab9fa1c20d80b2a84d682820f`**，定向 race 前后一致。本批未重跑全量 race、native/KVM/kernel-isolation/production；最近全量 race 仍绑定第四十六批。025 保持 IN_PROGRESS，剩余 egress host callbacks、应用调度与最后过渡层。

## 第四十八批：Egress 宿主能力

`Audits` 接管 sanitized event 包装和持久 journal，使用原 admission guard、Lifetime、reviewed 标志与窄 AppendAudit 能力；原 writer/key/HEAD 仍仅在 store。`ProxySockets` 接管 binding/VM/instance/vsock 路径及 live runtime 核验、inode anchoring、非 root VMM UID/GID 的 0600 socket 交接和 stale socket 授权。原路径限制、交接前后检查、独立时限和失败行为保持。

`Options.EgressFactory` 改为只接收 `EgressCallbacks`：Audit、PrepareProxySocket、RecoverStaleSocket、InstallDirect 四个方法值均直接绑定到实际 Audits/ProxySockets/DirectPolicies；不接收或捕获根 `*Service`，也不持有通用 SQL executor。daemon 使用这些窄能力建立 portal。新增 `TestHavenEgressFactoryUsesOwnedCallbacks` 通过真实 Service/SQLite 验证 factory 阶段可以写入审计、保留的 callback 在关闭后被同一 Lifetime 拒绝，以及离线 store 校验仍通过。根 receiver **18→14**。

- factory 边界独立 race：PASS，**1.708s**。
- `GOWORK=off go -C haven test -json -race ./... -run '^Test(.*Audit.*|.*Proxy.*|.*Socket.*|Haven(.*Egress.*|.*Direct.*|.*Binding.*|.*Secret.*|.*Credential.*|.*Backup.*|.*Shutdown.*|.*Data.*))$' -count=1 -timeout=10m`：**PASS / exit 0，41 pass、0 fail、0 skip**；根包 **30.175s**，egress **1.047s**。
- 编译、lint、vet、check-sql、architecture/SQL generation 正负例、51 冻结模型、diff check：PASS。
- `just integration`：PASS，真实 Rust Unix fixture **0.404s**；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/egress-host/`；源码 SHA256 **`5ba6bbc7ff7f76c16392ade9ddf6d5f57eb5d89f17c8308f134c5dbf0472ce13`**，定向 race 前后一致。本批是组件与实际 Unix proxy/fixture 验证，不代表跨 UID、KVM、kernel-isolation 或 production 验收；未运行项与此前 BLOCKED 保留。025 继续 IN_PROGRESS，剩余恢复验证派发、应用调度/管理与最终过渡层清理。

## 第四十九批：只读恢复验证与管理服务

冻结 workflow 的校验派发已归 `services/workflow_validation.go`，`Recovery` 不再接收根函数或任何 effect/runtime/worker；启动、私有 WAL probe、可写连接验证、offline inspect/backup/restore 继续注入同一纯读取验证器。所有 step key/action、原 loader、input hash/JSON 校验和 reviewed resource validation 分支保持。删除失去生产/测试使用的根 activation/supply/warm/资源校验转发与版本别名；首次 lint 的 9 项 unused 失败及清理后通过分别保留。

`Management` 拥有 root/scope 授权、Status、管理请求校验、正常 import/GC/supply cleanup/image cleanup 接纳与等待，直接调用实际业务服务，不访问 runtime 或通用 SQL executor。`NodeMetadata` 仅暴露 immutable identity/reviewed 与当前 Epoch；offline InspectState 仍由根持有独占 store 的开关生命周期，再调用相同的 ReadNodeStatus。原 status 部分结果、draining、错误优先顺序与 management DTO 语义保持。

唯一 `GetEpoch` 静态查询已进入 store 私有 sqlc；普通 status、HEAD 检查/checkpoint、backup copy 与恢复校验都使用原 DB/只读 probe，不新开事务或连接。查询仍为 `SELECT epoch FROM store_metadata WHERE singleton=1`，缺行/NULL/转换错误不补零。根 receiver **14→11**。

- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**PASS / exit 0，657 pass、0 fail、35 skip**；根包 **441.753s**，egress **26.840s**，store **3.330s**。
- 编译、清理后 lint、vet、check-sql、architecture/SQL generation 正负例、51 冻结模型、diff check：PASS。
- `just integration`：PASS，真实 Rust Unix fixture **0.303s**；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/recovery-management/`；源码 SHA256 **`d7f85e62767d99e4f18062b286ca46126881c2b1a9b5e25b16b1bbd259f7c649`**，全量 race 前后一致。35 个 native SKIP、required-native BLOCKED 和 KVM/kernel-isolation/production 未运行边界保留，不计为完整原生验收。025 保持 IN_PROGRESS，下一步为命令路由、维护调度、controller 直接接入与根过渡层清理。

## 第五十批：命令路由与维护调度

`SandboxRequests` 根据原 action 路由到实际 Sandboxes/Lifecycle/Supply/Forks，并通过 Operations 等待；不拥有数据库、效果或执行状态。`Maintenance` 负责 unfinished 枚举、共享 Workers 去重、rebind、reaper 和 refill 调度；reviewed、Lifetime Begin/End、关闭拒绝与错误顺序保持。根 dispatch/run/Reconcile/Maintain/Submit/Execute 和 beginDataCall 已删除。根 receiver **11→4**，仅余 Close 与三个构造辅助函数。

恢复使用构造时固定的 16 个窄 method values，严格保留 direct→images→GC→supply cleanup→fork→warm→supply→capture cleanup→lifecycle→snapshot delete→capture discard→stop→discard→capture→standalone activation→release 顺序，最后进入原 provisioning Continue。选择与冻结证明仍由实际服务核验，不按当前对象推测 step 或增加持久插件协议。`Activations.Resume` 在原位置读取 source，再持有相同 VM guard 执行 Continue；resource-owned activation 仍由资源 runner 接管。`Supply.RefillReady` 保留完整模板校验、pool count、逐模板 backoff 与原 refill admission，满池和 backoff 不创建 intent。

初次 lint 的两项 QF1008 与两个未用 activation 转发已修正；首轮与修正结果均保留。worker 重派发测试直接调用实际 Maintenance 的 Dispatch，保留原 committed success/last error/仅一次 launch 断言。

- `GOWORK=off go -C haven test -json -race ./... -run '^TestHaven(.*Reopen.*|.*Recovery.*|.*Refill.*|.*Maintenance.*|.*Maint.*|.*Drain.*|.*Worker.*|.*Shutdown.*|.*Activation.*|.*Resume.*|.*Warm.*|.*Supply.*|.*Wait.*|.*Fork.*|.*Startup.*|.*Restart.*)$' -count=1 -timeout=10m`：**PASS / exit 0，142 pass、0 fail、6 skip**，根包 **197.430s**；6 个原生场景缺环境而 SKIP，未放宽断言。
- 编译、修正后 lint、vet、check-sql、architecture/SQL generation 正负例、51 冻结模型、diff check：PASS。
- `just integration`：PASS，真实 Rust Unix fixture **0.362s**；`just smoke`：8 组 PASS。

日志 `build/025-services-sqlc/maintenance-service/`；源码 SHA256 **`de86da83d0ebbea7121762d9a5a9c61cb935f62dfa867a2fe58862d49f3b6a23`**，定向 race 前后一致。最近完整 race 仍绑定第四十九批；本批不新增 native/KVM/kernel/production 通过声明。025 继续 IN_PROGRESS，最后处理 App 组装、controller 直接依赖、模型/loader/store 过渡层与最终验收。

## 第五十一批：App、controller 与根过渡层收尾

`haven/app.go` 替换旧 `service.go`，`Open` 返回 `*App`；组件使用显式具名字段，根 receiver 仅 `Close`。三个构造 helper 收入构造过程，删除根 store 适配层、模型/error alias 和 loader 转发；fixture 直接使用 `store.State`、所属领域值与 services loader。仅测试所需的 ID/编码/digest/单行写/终态观察辅助留在 `fixture_values_test.go`。类型感知改写保留原测试名与断言，不通过嵌入字段维持 facade。

`access.Registry` 生成并持有不可变 daemon epoch；Data/activation 使用同一值。`httpapi.New(Services{...})` 和 `management.New(*services.Management)` 直接接收实际用例，controller 不 import 根组装包。daemon/integration fixtures 使用同一组装，生产仍为真实 runtime。

metadata 初始化和读取使用私有 sqlc；初始化仍在原 tx，版本比较保留完整 int64。唯一 v9 migration 与基准 `haven/schema.go` 的 `storeSchema` trim 后完全相同，10343 字符；51 个冻结模型的字段顺序、类型和 JSON tag 比对通过。最初验证脚本误找 `schemaSQL` 的失败已修正为实际常量名，没有更改 schema。

新增 AST shape gate，拒绝旧 Service/根 alias、嵌入式 App、业务 receiver、未经允许的原始 SQL/Query/Exec/Scan；维护例外精确到文件/函数/语句或调用。import gate 拒绝 controller 根/cmd 回指及绕过服务访问领域/效果（认证类型例外明确）；正负例覆盖这些边界。

- 初次 metadata generation 的 `INSERT has more expressions than target columns` 通过显式目标列列表修正；初次 shape gate 的 unused fmt 也已修正，初始与最终日志分别保留。
- `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m`：**657 pass、0 fail、35 skip**；根包 **437.006s**，egress **26.823s**，store **3.336s**，guest **1.022s**。
- 原普通入口 `-timeout=120s`：**FAIL / exit 1**，根包 **120.014s** 超时，完成 **296 pass、24 skip**；不能以 race 通过覆盖。
- compile/lint/vet、生成正负例、51 冻结模型、integration/smoke、runner 回归通过。架构 fixture 当时通过，但实际树 gate 仍有 4 项 Prepare/PrepareContext 误报；最终核对才发现，精确修正及新增回归列入下一批。首次 workspace lint 报 `tools/structfmt/main.go` 的 ST1005，修正及最终验证也列入下一批。
- preflight **BLOCKED / exit 1**：`native CI preflight requires effective UID 0 on an isolated runner; no automatic sudo`。foreground 实际 gate 被工具 60 秒限制中断，`run.udgtzhec` 仅为不完整尝试；后台重试 `run.j__934is` 为 exit 1 且 Go JSON 为空，原因在下一批定位。

日志：`build/025-services-sqlc/app-wiring/`。本批 Haven Go/SQL/YAML SHA256 **`cef2bf1baa235cd5f25a8f2209769b122e0fb082bc8d90ad47ba8f58a4a65f06`**，不包含 scripts/docs/其他 module。

## 第五十二批：有界 fixture 调度与最终验收

普通入口的总超时未出现对应行为断言失败。核对私有状态、环境修改和嵌套 parallel 后，将 **66 个**控制面场景纳入现有 `parallelLifecycle` 调度，仍为 **4 个**并发 case；未增加资源名额、修改测试名/断言/timeout。六个原生串行场景、修改全局环境的场景和 abrupt-exit 子进程入口保持原调度。完整 race 从 437.006s 降至 282.350s，但普通 120 秒入口仍超时；没有把调度改善写成该入口已修复。

实际 native gate 没有 JSON 的原因是当前 `just --quiet` 会抑制 recipe 输出。入口改为 `just test-native`，保留原 Go JSON/race/count=1/300s、required-native 集合和通过判定。新增真实 just、私有 recipe 与闭合 PATH 的 CLI 回归，验证原始事件确实到达 gate；其余拒绝与 negative 测试保持。`tools/structfmt` 的 usage 文本改为 `FILE [FILE ...]`，修正 ST1005；这是开发工具错误提示的最小改动。

最终逐日志复核发现实际架构 gate 的 4 项失败此前没有正确归入通过判断：三个 `SandboxCommands.Prepare` 被误判成 SQL，statement cache 的精确调用清单仍使用已经不存在的 `database.DB.PrepareContext`。修正仅接纳三个确定文件/函数中的 command preparation 调用，以及当前受控的 `database.PrepareContext(ctx, query)`；没有按目录或统一方法名豁免。新增真实 Go workspace 正例，并将同一位置替换为 SQL Prepare 验证必须拒绝。`architecture-final.exit` / `architecture-tests-final.exit` 均为 **0**；初始四项失败保留在 `architecture.log`，不覆盖历史结果。

最终 Haven Go/SQL/YAML SHA256 **`ff794cbbd57557cf2bc895d721aaaddcea1cac8094a5e21f506bc90e9aa05969`**，`source.sha256` / `source-after.sha256` / `source-final.sha256` 一致。扩展源码/构建/验证输入指纹 **`43e69559213e94e09e16f54e0e92603d406e444894362a817e3bcc39dbcbd3b0`**，覆盖 564 个路径，最终清单在 `source-inputs-final.json`；其按排序路径、NUL、内容、NUL 计算 SHA256，包含 tests、生成绑定、scripts、tools、CI 和 justfile，不含文档、build artifacts、references/third_party 或运行 state。下述本批日志位于 `build/025-services-sqlc/bounded-fixtures/`。

### 当前树通过项

| 命令 / 行为 | 结果 |
|---|---|
| `GOWORK=off go -C haven test -json -race ./... -count=1 -timeout=10m` | **PASS / exit 0：657 pass、0 fail、35 skip**；Haven **282.350s**，egress **26.842s**，store **3.262s**，guest **1.020s** |
| `just integration` | PASS；真实 Rust Unix fixture **0.370s** |
| `just smoke` | 8 组 PASS；包含同库身份、writer 排他、restart 和离线 backup |
| `just check` | PASS；最终复核 exit 0 |
| `golangci-lint run ./haven/... ./loom/... ./sdk/... ./types/... ./common/... ./tools/... ./protocol/gen/... ./operator/...` | PASS，`0 issues`；初次 ST1005 保留在前批日志 |
| `go vet ./haven/...`、`just check-sql`、架构/生成正负例、51 frozen models | PASS |
| `python3 -I scripts/test_runner_environment.py` | 10 项 PASS，1.237s |
| `python3 -I scripts/test_native_gate.py` | 8 项 PASS，2.513s，包含真实 just 输出保留 |
| `python3 -I scripts/check-native-gate.py --negative` | PASS，`build/native-gate/run.pad5twzx`；只证明缺工具明确拒绝 |
| `GOWORK=off go -C haven test -tags=kvm ./... -run '^$'` | PASS；仅编译，未运行 VM |
| `GOWORK=off go -C tools test -race ./... -count=1 -timeout=120s` | PASS；覆盖 usage 修正后的当前工具源码 |

workspace 其余 Go module 使用 `GOWORK=off go -C <module> test -json -race ./... -count=1 -timeout=10m`：Loom **27 pass**、SDK **1 pass**、tools **2 pass**、protocol/gen **3 pass**；types/common 编译通过、无测试。`cargo test --locked --workspace --all-targets` 为 **34 pass、0 fail/ignored**。这些未变更的 module/Rust 日志在 `app-wiring/`；tools usage 修正后另有本批 race。工作树中 5 个 Loom 文件的字段格式展开差异已核对，保留现有改动，没有行为修改。

### 普通组件入口仍 FAIL

命令均为 `GOWORK=off go -C haven test -json ./... -count=1 -timeout=120s`；tmpfs 变体只将 TMPDIR 指向本次 mktemp 创建的私有 `/dev/shm/haven025-component.*`，GOTMPDIR 使用独立 `/tmp/haven025-go.*`。这些私有目录已由 wrapper 在退出时删除，没有清理其他状态。

| 本批运行 | 结果 |
|---|---|
| 默认磁盘 `component.json` | **FAIL / exit 1**，Haven **120.286s**；完成 **522 pass、35 skip** |
| tmpfs，与 native gate 并行 `component-tmpfs.json` | **FAIL / exit 1**，Haven **120.013s**；完成 **555 pass、35 skip** |
| tmpfs，等待其他本任务测试结束后单独运行 `component-tmpfs-isolated.json` | **FAIL / exit 1**，Haven **120.012s**；完成 **518 pass、31 skip** |

上述均为 package 总 timeout，未获得完整普通入口通过。不能据这些尝试断言单一磁盘原因，也不把未执行完的用例计为通过；此前 120.027s 的历史失败仍保留。未提高 timeout 或降低测试范围来转绿。

### required-native 仍 BLOCKED

- 本批 preflight：**FAIL / exit 1**，effective UID 0 / isolated runner 要求不满足，`run.xibu236t`；没有 sudo 或系统配置修改。
- `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C haven test -json -race . -run '^Test(ActualLayeredEROFSImportAndExtraction|ActualSharedOCIHardlinksAndMetadata|ActualMergedOCIHardlinksAndMetadata|GCActualLayerCacheAndCompletedImport)$' -count=1 -timeout=300s`：**4 fail**，均为 `required OCI tool missing: bwrap`，根包 **0.011s**。
- 修正输出前的本批 gate `run.4onv4e1u` 保留 exit 1/空 JSON，不能当成行为覆盖。
- 修正输出后的 `python3 -I scripts/check-native-gate.py`：**FAIL / exit 1**，`build/native-gate/run.9mda9zuq` 保存 **3587 条 Go JSON**；**652 pass、40 fail、0 skip**，Haven **294.717s**，没有 timeout。失败在原生工具前提检测及其父用例，required 四项均未通过；just 在 Haven 失败后没有继续其他 module。
- 普通 race 的 35 skip、negative gate、真实 Rust fixture 和 KVM 编译均不能替代 native 通过。KVM、kernel-isolation、production 本任务未运行；不挪用 024 的历史验收。

## 最终状态与剩余边界

业务服务、领域持久化、应用状态归属、App/controller、模型与 loader 清理已完成；没有剩余 workflow/SQL 迁移项。当前架构/构建/领域契约和计划索引已更新，根 production receiver 仅 `App.Close`。最终 `git diff --check` 为 exit 0；变更 Markdown 的本地链接均存在，本任务后台检查已结束，私有 tmpfs fixture 目录已清理。

**IMPLEMENTED / VALIDATION_BLOCKED**：完整 race 与上述组件外验证通过，普通 120 秒组件入口和 required-native 未满足完成条件，所以 025 暂不归档，也不声称整体验收完成。后续须保留当前断言、timeout、资源限制和原生要求，在合适 runner 上完成验证；不恢复 017/020/021、不自动执行 024 VM 验收、不提交或部署。

