# 022：职责、测试单元与 public API 重整结果

**DONE。独立 SOURCE_APPROVE、完整新门禁及父最终核验通过，目标分支已更新且工作树干净。**

- 分支：`t3/final-twelve-source-combination`
- HEAD：`33a3f829e70969a0f4269b999fed437398e0c54a`
- tree：`56c1e1e319ae401b7b84b4a9cb5309284db2b98f`
- 工作树：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-fe782bd5`
- 本轮 review 范围：`47f5ce71..33a3f829`，2 个提交。
- 含此前12项的完整范围：`b93fc24..t3/final-twelve-source-combination`，30 个提交。
- [最终接受收据](evidence/022-final-acceptance.json)、[执行计划](evidence/022-sole-executor-approved-plan.md)、[过程与历史](022-responsibility-and-api-boundaries.md)。

## 改动

保留现有 package 边界，按真实职责搬移完整声明。没有新增 package、interface、生产函数或测试入口，没有引入 models/services/interfaces 层、getter、wrapper 或兼容 alias。

Engine 将准入、元数据、查询、操作执行/完成、实例生命周期、snapshot/restore/cleanup、GC 图与清理、CH 进程与执行、镜像校验分别归位。例如完成事务与 RowsAffected 校验共置于 `engine/operation_completion.go`，对应 `operation_completion_test.go`；恢复候选的材料交付与显式清理各有真实生产单元。原 599 个完整声明均有唯一批准目标。

sandboxd 在同包内将完整 Submit 放入 `admission.go`，配置 intent/fence 放入 `configure.go`，最终数据路径准入与连接登记放入 `data_admission.go`；领域 DTO、存储读写和对应业务代码共置。activation、warm selection、allocation、恢复与 typed cleanup 分文件维护，原事务与锁序保持。主包最终31个生产文件，原889个完整声明均有唯一批准目标。egress仅重组测试，生产源码保持；SDK/agent/cmd/integration及Rust/protocol/operator/deploy未改。

普通逻辑单元测试与真实同名生产文件配对；完整场景、故障矩阵、断言、gate、Cleanup和测试入口名称保持。共享fixture保持private并有明确owner；14个Engine跨单元共享声明归 `engine_test.go`。owner budget整组归 `tenant_test.go`，仍为external `sandboxd_test`组件测试。原privileged/kernel/KVM/capture及跨语言integration例外按既有性质保留，不造空source文件配名。

保留六个现有interface及完整方法集。Backend虽只有一个production实现，仍是实际host-effect与故障恢复测试的内聚边界；NetworkPolicyBackend是可选能力；api.Client有Engine与UDS两个生产实现；Agent维持无环依赖；Egress/Resolver已是小消费方接口。未把testfake算作第二production实现，也未拆出更多单实现接口。

## 七个 Go API 名称收回

| 原对象 | 新名称 |
|---|---|
| `sandboxd.(*Service).VM` | `verifiedVM` |
| `sandboxd.(*Service).CredentialsMissing` | `credentialsMissing` |
| `sandboxd.EnvVar.IsSecret` | `isSecret` |
| `sandboxd.Operation.Origin` | `origin` |
| `sandboxd.Operation.Hash` | `hash` |
| `engine.CloudHypervisorCommit` | `cloudHypervisorCommit` |
| `engine.CloudBuildFeatures` | `cloudBuildFeatures` |

两Operation字段保持原string类型、位置及 `json:"-"`；SQL列、HMAC输入和其他类型同名字段不变。所有持久DTO类型名、其余字段顺序与tags、错误文字、wire字段集保持。

这七个旧Go名称不再公开，外部selector、interface要求、struct literal、反射公开性或字段格式化可能受影响；这是本轮明确选择的可见性调整，不声明完整Go源码兼容或仓外消费者为零。Fault/ErrInterrupted、LastGCReport、镜像binding/serial方法等已有文档承诺保留，Wait签名不改。23处public comments与一处package doc迁移经过实际实现核对和独立审查。

## 无逻辑漂移依据

- 逐完整声明比较AST、token、原literal拼写、字段/group/iota、body和comments；仅对批准的7个resolved对象、43个identifier位置逆向归一。
- 独立before/after共8次source-only类型分析均diagnostics0；绑定、method sets、interface满足关系、初始化依赖及有效顺序、构建选择/imports/依赖一致。
- 方法Selection.Index的末位变化被独立解析为实际声明方法对象；字段索引及嵌入路径严格比较，未忽略promotion或field布局。
- 314个静态Test/Benchmark/Example/Fuzz入口保留；SQL/DDL、请求/JSON、权限/预算/超时、锁/事务/副作用顺序及所有旧安全body保持。未修旧Unix路径诊断race或推进016/017/020/021。
- 父最后在目标worktree直接核对678个ordinary paths的bytes/modes/symlinks及完整semantic index，与已审候选和完整测试快照一致；12gitlinks和原initialized references集合0保全。

## 实际新门禁

| 门禁 | 本轮结果 |
|---|---|
| Engine full race，300s timeout | exit0，199.431s，448 RUN/PASS；精确原集合、每节点各一次 |
| 包含的required native/proof | 36个顶层PASS，含20个TestActual；原cached工具及test-owned rootless路径 |
| sandboxd full race，480s timeout | exit0，360.476s，678 RUN/PASS；精确原集合、每节点各一次 |
| 包含的真实Rust relay | 4个入口全部RUN/PASS；fixture SHA及full34输入前后匹配 |
| protocol race | 3 RUN/PASS |
| 三module vet、正常go.work lint | 全通过，lint 0 issues |
| gofmt只读、diff-check | 通过 |

两套full race各只跑一次，测试级FAIL/SKIP/RACE均0；Engine api无测试文件的package skip单列。独立review没有重复产品suite，审计了原raw events、运行收据和全部gate窗口源码绑定。9个已知正PGID及自有短TMP已清理，父fresh只读确认。

## 提交和证据

1. `418486a49dbad9fed67ab643ce04f8eab004917e` — `engine: group lifecycle and image responsibilities`
2. `33a3f829e70969a0f4269b999fed437398e0c54a` — `sandboxd: group orchestration responsibilities and internal APIs`

- 实施：`/tmp/plan022-implementation-t3-8a03fb80/handoff.md`、`handoff.json`、`execution-notes.md`。
- 独立源码：`/tmp/plan022-source-review-t3-4fb3cdc3/report.md`、`decision.json`、`equivalence-summary.json`、`source-binding-audit.json`。
- 原目标owner最终FF：`/tmp/plan022-target-ff-t3-fe782bd5/report.md`、`handoff.json`、`before.json`、`after.json`、`ff-result.json`。
- 父本地核验：[local intake](evidence/022-33a3-parent-local-intake.json)、[SOURCE接受](evidence/022-33a3-parent-source-acceptance.json)、[最终接受](evidence/022-final-acceptance.json)。

保留全部工具失败与修订历史。最终FF只执行一次；其后置报告初次因两个symlink记录多出target字段而断言失败，原诊断保留。后续只读按共有字段并与git实际target bytes核对，父再独立核678个实际source/index，确认无漂移，没有重做FF或改源码。

本轮没有重跑VM/production、Cargo/Nix或guest构建；旧47f5的production/KVM/Rust49是其精确旧树历史，不转为33a3的运行成绩。父dev仍b93、既有deploy/ACCEPTANCE及go.work.sum保持，未merge/push父dev，也未部署。所有本轮任务已结束，016/017/020/021及无关维护继续暂停。
