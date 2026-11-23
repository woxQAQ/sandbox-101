# 027：能力边界实施与验证证据

## 状态

**IMPLEMENTED / VALIDATION_BLOCKED**。Haven 的能力归属、私有 SQL、测试/fixture 边界和当前契约已收尾。普通 120 秒入口仍超时；当前 native runner 缺真实 root ownership 条件。二者保留为验收缺项，不能由分包、race 通过或普通入口的 SKIP 替代。

实现没有提交、推送、部署或迁移真实旧库；017/020/021 保持暂停，024 的 VM 验收独立跟踪。

## 基准与最终映射

实施基准是包含 025/026 和 goose 接入的未提交树，HEAD `6162397891e0e454fd2f6b3fc511c11d004fde5b`。424 个实施输入的 SHA256 为 `0863dcdfd12e740067dbc4f32a7cd854ea99410252e4072fc88596472c5f6a77`；它与设计阶段的 `8a8c8d8af2d359fb3bb89329065fca9a19ccce2165383566b43a43df14d5438a` 不同。原始源码保存在 `build/027-capabilities/baseline-source.tar.gz`，不能用 HEAD 代替这一比较基准。

[逐项映射](027-migration-map.json) 保留 **1,297 个生产声明、282 个原 SQL 查询、224 个原测试名**。保留声明记录最终文件、符号和 owner；移除的包装记录实际私有查询和当前消费者；查询记录迁入或复制后的 owner 输入及原 SQL 摘要。两个新增行为测试另列，不冒充原测试。完整类型引用清单位于 `build/027-capabilities/{baseline,current}-symbols.json`。

最终 SQL 共 **315 个定义**，逐项核对原 SQL 文本通过；sqlc 生成 **30 个文件**，与固定版本生成器一致。**76 个冻结 JSON 根及递归模型**的字段顺序、类型和 tag 与实施前相同，名称按显式迁移清单归一化。v9 baseline、`store/migrations.go`、`store/journal.go` 与实施基准逐字节相同。

## 最终所有权

| owner | 用例、持久化与边界 |
|---|---|
| image | import/cache/publication/cleanup、冻结材料证明及本能力 step/cleanup/deletion 查询；直接消费私有 dbgen |
| machine | VM/generation、capture/canonical snapshot、stop/discard/release/restore、source lease/backing、物理 policy 和相应历史证明；VM/snapshot SQL 合并 |
| supply | template/build/refill、warm publication/selection/retirement、supply cleanup/backoff；template/pool SQL 合并 |
| sandbox | claim/settings、activation、生命周期/checkpoint、warm/fork、业务投递及连续持锁的 provisioning coordinator；自身 step/cleanup 查询 |
| data | data admission/handoff、preview/auto-resume、binding reconcile、proxy socket 和 sanitized audit 适配；durable audit 仍由 store journal 负责 |
| collection | 同一只读事务中的全局 roots/candidates 与 GC 协调；自身 GC step 查询，物理删除委托 owner |
| node | status/drain/管理业务；没有 transport 或独立控制面 |
| operation | 不可变请求/幂等/MAC、通用链接头、终态、publication/recovery 头；不再提供业务 step/cleanup 的通用写仓库 |
| internal | application 只组装与关闭；publication、recovery、maintenance 分别组合投影、只读校验和固定顺序恢复 |

旧 `services`、`template`、`pool`、`vm`、`snapshot` 目录已删除。image Repository、VMState、SnapshotState、TemplateState、PoolState、sandbox Repository 及纯行复制类型已移除，不留兼容 alias。authorization/operation/quota 的编码、身份、费用和公共协议入口保留其实际语义。

`ownership` 只承载原 Birth/Sandbox/GenerationRef 冻结值，没有 loader 或业务 authority。Sandbox 的 CapabilityLookup 仅提供授权查找；CommandIdentity 属于 operation，保留原角色 MAC、凭据比较和幂等 secret 恢复。

## 事务与证明核对

- `operation.Committer` 保留 Background context 与原事务中的完整 UnpublishedTerminals → Project → Publish → Commit 次序；实际投影在 internal/publication，不在提交后补发。
- sandbox.VMProvisioning 连续持有原 VM guard，调用 machine.Provisioning.AdvanceLocked 后继续 activation；没有释放/重取锁。machine.AdmitProvisionIntent、AdmitCandidate、RetainGenerationIntent 等参与者只消费调用方事务。
- restore 的 claim stopped body → sandbox ownership update → instance/version intent 次序保持。SelectWarm 保留 ready row 消费 → warm 引用退出 → selected 转换；最终 retirement 继续比较原 body 和 selected 状态。
- OwnershipHistory、StopHistory、CaptureDiscardHistory、DeletionHistory、SupplyCleanupHistory 和 ActivationHistory 使用调用方 DBTX，保留原 intent/hash/action/link/instance/fence 核验。当前行不补造历史，缺失必需端口返回 ManualRecovery，保留 pending intent、费用与 epoch，不启动 worker。
- DeletionHandoff/ProvisionHandoff 等写参与者不 Commit、Checkpoint、派发效果或取得 workflow lock。data 的 RenewActivityLease 仍在连续 configuration guard 下比较原 settings bytes，checkpoint 留在原调用者。
- VMRoots/SnapshotRoots、模板与 pool 根查询借用 collection 的同一 read transaction。owner 内解码 catalog 并保留独立 catalog key、原 body hash、排序及错误前的部分根集合；错误仍使全局状态未知。
- 所有迁入 SQL 保持原谓词、投影、SET 列、UPDATE OF、NULL/整数和 RequireOne 语义。GetStep scan 失败仍返回零行值；缺失与损坏不合并为“无引用”。

恢复顺序保持：direct → images → GC → supply cleanup → fork → warm → supply → capture cleanup → lifecycle → snapshot delete → capture discard → stop → discard → capture → standalone activation → release → provisioning Continue。startup/offline 使用同一套显式只读 validators，未知 kind 或缺失证明拒绝。

## 门禁与测试组织

架构检查禁止反向业务依赖、owner 导入组合层/transport/外国 dbgen、恢复旧 facade 和生产导入 fixture。Go 类型信息检查 StepKey，包括 alias、常量和明确的 owner step.Key；cleanup ID 前缀/resource kind 限定共享容器的写入责任。SQL 没有新增整目录 raw bypass。

只读门禁从 sandbox 的具名历史 reader 与 internal/publication 出发，沿已解析调用进入私有 helper 和生成查询，拒绝 Exec、Begin/Commit、Checkpoint、worker Start 与 goroutine。对应正反例验证直接/间接写路径；它与 SQLite `query_only` 行为测试互补，不宣称静态分析能证明任意动态回调纯度。

基础私有 store/runtime 材料在 internal/testfixture，完整 App 组装/重启在 internal/testapp；基础 fixture 不依赖 application/testapp。两个租户 Service/SQLite 测试迁到 authorization，一个镜像重放测试迁到 image 外部测试包。需要全局 Reconcile 的镜像清理、跨能力事务、重启/关闭等场景留在 component；native/KVM/transport 入口保持。

新增 `TestMissingCapabilityPortsRetainIntentAndResponsibility` 验证缺端口时原 intent、4096 bytes reservation、epoch 和 worker 状态；`TestHavenCapabilityReadersBorrowReadOnlySnapshot` 在实际 query_only 事务中验证 owner reader 与缺失 activation 证明。原 atomicity、历史拒绝、UNKNOWN、publication、configure、warm/fork/恢复测试名与断言保留。

普通/race/native 入口统一 `-p=1`，保留四个活跃生命周期槽及 120s/10m/300s 预算。测试归属变更没有被用来声明旧超时已解决。

## 最终运行

原始输出均在 `build/027-capabilities/`，每个后台步骤有独立 exit 文件。镜像测试迁移前的一轮结果保留在 `pre-image-owner/`；不能代替下表的最终树结果。

| 实际命令/核对 | 结果 |
|---|---|
| `GOWORK=off go -C haven test ./... -tags=integration,kvm,privileged -run '^$'` | PASS；仅编译 |
| `GOWORK=off go -C haven test -json -race ./... -p=1 -count=1 -timeout=10m` | PASS：673 pass、0 fail、35 skip；component 276.088s；`final-race.jsonl`/`final-race-summary.log` |
| `just test` | FAIL：component 120.014s timeout；原 120s 预算未改，不能称全部验收完成；`final-ordinary.log` |
| `python3 -I scripts/check-architecture.py` | PASS |
| `python3 -I scripts/test_architecture.py` | PASS，包含私有查询间接写、projector 写入和 goroutine 的拒绝正反例 |
| `just check-sql`；SQL 文本/冻结布局核对 | PASS，30 个生成文件、315 个 SQL 定义、76 个布局 |
| `python3 -I scripts/test_sql_generation.py` | PASS |
| `python3 -I scripts/test_native_gate.py`；`test_runner_environment.py` | PASS；runner 回归不代表真实 native/VM 验收 |
| `just check` | PASS：全部 workspace vet、Rust fmt/clippy |
| `golangci-lint run ./haven/... ./loom/... ./sdk/... ./types/... ./common/... ./tools/... ./protocol/gen/... ./operator/...` | PASS，0 issues |
| `just integration`；`just smoke` | PASS：真实 Rust unix_fixture 0.355s；八组 daemon/CLI smoke |
| `python3 -I scripts/check-native-gate.py --preflight` | BLOCKED / exit 1：要求实际 euid 0，禁止自动 sudo；`build/native-gate/run.sa0aku_r` |
| required-native 全套、KVM/kernel-isolation/production | 未运行；不自动提权，不启动旧验收 |

最终输入清单覆盖 **588 个** Go/Rust/SQL、module、工具与 runner 输入，不包含文档。最终 SHA256 为 `23b74c1a499ad7567e11f5e613b08f23e7072bf12c51d25bcdf872d87dfb9448`，见 `final-inputs-current.json` 与 `final-source-identity.json`。

`final-inputs-before.json` 与最终清单之间只有 `scripts/check-haven-shape.go`、`scripts/test_architecture.py` 两项变化：只读门禁收口发生在 runtime 验证期间，随后实际树及完整门禁正反例均通过。其余 **586 个 runtime/工具/runner 输入逐项字节一致**，按路径与 bytes 聚合的 SHA256 为 `696f4fda21b1f4a6e3e4745eff4cbcec8fd8333f7c9ebc926d0a974267bdfaef`。不主张全时段全部 588 个输入稳定。镜像测试迁移前曾运行含 go fix 的 just lint，所选输入前后未变；最终树单独执行上表的只读 lint。

`final-skip-inventory.json` 逐项保留 35 个 SKIP 的测试名与原输出；tagged KVM/privileged 只编译，未运行项不算 SKIP 或 PASS。`git diff --check` 通过。

026 的 xattr 丢失仍是历史 native 失败证据；本次 UID preflight 拒绝不能解释成该 xattr 问题已经解决。普通/native 缺项继续在 [未完成计划索引](../README.md) 跟踪。
