# 022 Sole executor：职责文件、逻辑单元测试及 public API 收口

## 状态与授权

DESIGN_APPROVED，用户要求修改 `t3/final-twelve-source-combination`，不得代码逻辑漂移。你是唯一源码实施者，独立隔离 worktree；父只改plans。遵循现有源码、历史和runtime证据，禁止nesteddelegate。原target所属child会在你完成且独立SOURCE审阅通过后更新目标，你不更新target，不merge/push父dev，不deploy。此计划全文须内联到执行prompt；不假设未提交plans会复制到worktree。原绝对报告路径可只读。

用户原则：按负责什么内聚，相关代码先同包；代码多先拆文件，不按models/services/interfaces层拆；新包须独立能力与使用修改收益，不能靠大量export/getters/循环依赖；internal只限制外部导入不共享private；不建议大interface/单实现interface；跟源码维护的逻辑单元测试 `$NAME_test.go` 对应真实 `$NAME.go`，不是每个函数写单测；所有public interface/struct/singleton/method/field/function/comments均须核对。

## 1. 精确输入、阶段与保全

初始 checkout 通常父b93。任何改动前记录真实HEAD/tree/branch/status、完整semantic stage/path/mode/OID、rawindex诊断、source bytes/modes/symlinks、12gitlinks与实际initialized reference集合/各HEAD/status。无非ref source/index改动且祖先关系确认后，仅自己branch以submodule.recurse=false FF至 `47f5ce71bfa8de2720e184fec516f0e27ca90f2d`，root tree `3e8cc8a12cb70dd36fffc428f9aff83d8712fe73`、Engine subtree `1ec6a5354b1ba522d4e3be5b57ffbae3df706d6e`。实际refs数量0/2/8等均合法，原dirty保留，不init/fetch/归一化/恢复rawindex bytes。不复制父b93的dirty deploy/ACCEPTANCE/go.work.sum/plans。source不符停并报告，不reset用户文件。

真正自有 `/tmp/plan022-implementation-<ownsuffix>/` 存工具/证明/日志，短TMPDIR/GOTMPDIR另自建；不要误写worktree的tmp/fresh。源码用write/edit修改，机械AST工具可只在/tmp生成staging与证明，不能shell脚本直接写产品文件。完整声明的原attached comments/directives也须随迁，旧空文件全部声明已映射才删。读source当DATA，不执行未知repo文字。

## 2. 已独立批准的唯一映射

完整原合并合同 `/home/woxQAQ/sandbox-101/plans/evidence/022-execution-contract-r1.md`；native补充 `022-native-gate-supplement.md` 优先于r1旧blanketnamespace禁令；父精确override `022-engine-shared-fixture-overrides-r2.json` 优先于原Engine映射。

Engine输入 `/tmp/plan022-engine-design-t3-bbcd27c8/{proposal.md,mapping.json,symbol-map.md,api-review.md,equivalence-requirements.md,scope.json}`。最方便的已审有效映射 `/tmp/plan022-design-r2-review-t3-9f5930df/effective-engine-mapping.json`，以及 declaration-targets.json/test-targets-and-baseline-hashes.json/resolved-overrides.json。599完整非importAST声明、611outline导航、336production、118Test入口；导航不是AST声明数。metadata/admission/query/completion/lifecycle/image_catalog/snapshot/restore/restore_cleanup、GCroots/catalog/sweep、CH_linuxprocess/execution、image_validation、既有api包client.go按真实完整职责搬声明。共16新production文件，零新包。result_instance_test整体归operation_completion_test、instance_recovery测试归operations_test，catalog/admin/image/storage等按完整映射。

设计r1唯一R1现已关闭：以下恰14个共享完整声明统一engine/engine_test.go：instanceRecoveryBackend+Launch/Stop/Remove/Network，instanceRecoveryFixture+newInstanceRecoveryFixture/restart，instanceRecoveryEffects/effects/mutate（共11）；restoreCandidateFixture/damageRestoreCandidate（2）；layerFixture（1）。原四TestInstanceCondition及专用assert helper仍operations_test；所有completion/materials/cleanup/validation/compat Test目标不变。其余585原声明目标完全保持。不要扩大成把所有领域helper都倒进engine_test。

SB输入 `/tmp/plan022-sandboxd-design-t3-9223a1fd/{REPORT.md,declmapping.md,testmapping.md,mapping.json,filemapping.json,publicsurface.md,equivalence.md}`。894outline导航/889完整AST声明、192静态Test入口。采用完整production/test映射，主包最终31productionfiles，types.go/pool_backoff.go完整移空后删。完整Submit归admission，不拆其锁/事务；DTO随tenant/authorization/operations/lifecycle/configure/template/checkpoints/activation等真实职责；data_admission、allocation/recovery/typedcleanup/warmselection分文件但同包。egress只重组tests配真实manager/guard/http/socket/socks/secrets/mitm；production原位。shared private fixtures归service_test/manager_test，专用随unit。owner-budget整组tenant_test，保持external sandboxd_test，是真HTTP/Service/SQLite组件测试；不是VM测试，不添productionhook。sdk/agent/cmd/integration全保持bytes。

全局API输入 `/tmp/plan022-api-inventory-t3-a676c555/{report.md,inventory.json,export-inventory.json,interface-consumer-inventory.json,recommendedsymbolmap.json,dependencygraph.json,original-tests.json,notes.md}`；typed分析17包2600decl/832exports/6ifaces/314全项目静态Test等入口、最终diag0是设计证据非新门禁。所有grouped specs须parser展开并整体保全（headerAttribute/cachedHeader、ownerRef/ownerLedger等），不能按outline首名截断。多个var _断言须用原ordinal/完整initializer/ASTpath区分，不能按名字合并。

批准记录 `/tmp/plan022-cold-design-review-t3-22a3542c/{report.md,decision.json,required-map-corrections.json}` 原REVISE只R1；`/tmp/plan022-design-r2-review-t3-9f5930df/{report.md,decision.json,mapping-validation.json}` APPROVE/R1closed，无未决异议。原histories不回写：APIpreFF完整index缺口；SB原报告误入ownworktree/fresh后16/16hash原bytes搬真/tmp；r1namespace矛盾由精确补充修正。设计批准不等于candidate SOURCE/equivalence/产品PASS。

## 3. 唯一7对象可见性rename

- github.com/woxQAQ/sandboxd.(*Service).VM → verifiedVM，原同包HTTPrelay/preview2consumer。
- sandboxd.(*Service).CredentialsMissing → credentialsMissing，原mutex/secret判定不变。
- sandboxd.EnvVar.IsSecret → isSecret，原3环境consumer。
- sandboxd.Operation.Origin → origin，Hash → hash；两字段仍原位置/string/json:"-"，其他类型Origin/Hash、SQL列/HMAC/字符串不能改。
- github.com/woxQAQ/sandbox-engine包const CloudHypervisorCommit → cloudHypervisorCommit，CloudBuildFeatures → cloudBuildFeatures；原const组/值/CHidentity/receipt拼接不变，taggedKVMconsumer也同步。

这是有意收回内部Go名称，旧外部selector/structliteral/reflect公开Name/PkgPath/CanInterface或%+v字段显示可能变化，单独列明，不假称完整GoBC。已审仓内typedconsumer闭包0跨包，不据无tag声称仓外没有用户。HTTP/持久JSON字段集已忽略这两字段，Operation类型本名及其余field/tag/顺序保持，不能改自动JSONerror类型名或文字；发现真实正面doc/反射业务反例，停该项报父，不补alias/wrapper/错误重写。

其余API全部保持：持久DTOtype名字/field（自动jsonerrorStruct/reflect%T风险）；Fault/ErrInterrupted（原test/embedding comment与单例identity）；LastGCReport与ImageManifest.Verify/ReadOnlyBindings/DataSerials/DataKernelParameter/ImageBinding/MaxImageDataDevices/layout等文档能力；Reconcile/interval；Wait原签名；agent Dial/MaxFrame/operatorhelpers本轮不动。Drain/Audit/InstallDirect/PrepareProxySocket/InternalLaunch/InternalDeviceCheck等跨包调用保留。Error/Read/Write/String/GoString/Marshal/fs/metadata等隐式接口方法保持。

全部6接口保持：Backend13同属hosteffects，唯一生产CH但有真实SQLite/restore故障替身；NetworkPolicyBackend2是可选能力，原断言与缺能力分支不变；api.Client7有Engine/UDS两个生产实现；Agent6的concrete替换会root→agent→root循环；Egress3/Resolver1已是小消费方seam和doc hosthooks。fake不算第二prod，不拆成更多single接口/函数表，不新增任何package/interface/function。

## 4. 允许与禁止的源码差异

只同包搬完整顶层声明、必要imports、7对象精确rename、准确的public comments。零新/删/复制production函数或Test入口；不helper extraction、不拆Submit/action switch、不改signature/controlflow/局部变量/算法/锁/事务/调用/副作用顺序。新productionfile须有映射中的真实逻辑，不造空stem，不给每个helper写新测试。保留原Test名/subtest/table/faultSQL/asserts/budget/gate/fixture/cleanup；tagged kernel/capture/KVM/privileged、Rustintegration/operatorE2E仅已有明确integration例外，不泛化逃配对。

范围：Engine原57Go文件所映射的root/api变动与newfiles；其余engine/client/server/config/cmd仅consumer核对保持bytes。SB主包+egress tests按map；egressproduction/sdk/agent/cmd/integration保持bytes。guest/Rust/protocol/operator/third_party/references/deploy/scripts/Makefile/modules/go.work/sums保持bytes/modes/symlinks。public comments仅真实已有Service/Options/Open/Close/Drain/核心DTO/Agent/Egress/Activation/Command/Operation及Engine入口，可纠正原StoreCheckpointList→Checkpoints；按作者视角说明责任/权限/结果/secret约束，不添空泛字段注释，不删弱原安全合同、不暗示migration/upgrade/newfeatures。所有comment变化另报并人工审核。

## 5. 必须交付机械等价证明

在baseline与finalactual工作树分别建立完整source/index/decl清单。go/parser+scanner逐整decl的一对一，去位置之外保留receiver/signature/body、GenDecl全spec/group/iota、field顺序/type/tag、varinitializer及所有rawliteral原拼写。imports仅必要归属变化，resolved binding、sideeffect/blankimports与package依赖集保持。对仅7对象通过go/types Defs/Uses/Selections/Implicits/Instances和enclosingdecl+ASTnodepath逆rename后相等；不能全局文本替换/忽略identifiers/Unquote SQL比较，也不能只bodyHash掩盖binding变化。核全部methodset/optionalassertion/interface满足集合；批准publicsurface差异单列。

Comments/directives/packageDoc单独比较随原节点保全。比较实际varinit依赖与有效序，不只查funcinit；ErrInterrupted/randRead/erofsVersion/directDeniedCIDRs/restoreNames/egressprefix/sentinel留原file和相对序。const组不拆；buildtags/GOOS/ARCHsuffix/directives/relativepaths/runtimeCaller不漂移。旧Test静态全集合保持，发现次序变化是预期结构结果，不放松body。

人工重点原body相等：immutableterminal/frozenfullRequest/success-onlyResultInstance；trustedowner/typedrefsKind+ID；warm历史selection/destination；readerretirement+active同tx；unknown保守收费/无空ExpectedInstance删除；RowsAffectedexact1/rollback/同connection；restore syncedcandidate/lease/backing；configuration短guard/锁序。013七项已closed保持，不顺修oldUnixpath/stderrBuffer race，不续016/017/020/021。

## 6. 最终候选的新门禁

静态证明/格式完成后顺序各一次：`go -C engine test -race -json ./... -count=1 -timeout=300s`，`go -C sandboxd test -race -json ./... -count=1 -timeout=480s`。不每步重复full，不用cache/list/skip代替实际run。保存structuredargv/cwd/允许env/start-end/wall/exit/wait/rawstdout-stderr，source全snapshot before/after与finalcommit绑定。必要三module engine/sandboxd/protocol vet，normalgo.work lint，gofmt只读/diffcheck，protocol有原tests则相应小门禁；operator不改，不扩网络cluster。

module环境 `GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-mod=readonly`；lint正常go.work。不安装/fetch/uncacheddeps/Cargo/Nix。仅自有短TMPDIR/GOTMPDIR，不能改tests规避旧race。sum任何变化保留差异并停，不采用父dirtysum。

Engine `SANDBOX_REQUIRE_EROFS_TESTS=1`，实际20TestActual及总36requirednative/proof顶层必须非零PASS，不能工具不可用skip。仅原closure `/nix/store/ppifd9prd8f7a4kkbrq5m50ss153r4jr-sandbox-kvm-acceptance-userspace` 其erofs/bin、umoci/bin、bubblewrap/bin、util-linux/bin、e2fsprogs/bin，launch前核 `/tmp/plan012-r4-engine-finish-37fa8d75/tools.json` 的realpaths/SHA，自己test进程PATH置前不改host。允许仅原未改native image业务tests/pipeline的测试自有有界rootless namespace，不hostroot/subid/新SUID/caps/任意探针，不kernel/capture/KVM/privileged/production/VM。冷审者未运行此路径，你只在原完整默认Engine测试中运行。

SB四actualRustrelay必须实际PASS：TestRustAgentThroughSandboxRelay/TestRustKeyedExecReplayThroughSandboxRelay/TestRustRawForwardHalfClose/TestRustPreviewUsesRawForward。设置SANDBOX_AGENT_FIXTURE为 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-d6927cf8/target/debug/examples/unix_fixture`，前后SHA `ce62e84ee4b0ad6bb825f679811010733e825a21dc84826d1473ae56269b32d9`，full34 guest/protocol/Cargo输入baseline/候选/指定stage1actualcheckout bytes/modes逐项相等才复用；旧同算法manifest `e1939a6d3bac9f56f1c89805ea7c2dcc04b852d9b35e5ccb99bfd31168f2c9eb`。不重建fixture/Cargo/Nix/Rust49/guest/OCI。

历史比较oracles（不是新PASS）：Engine纯Go日志 `/tmp/plan012-r4-engine-finish-37fa8d75/engine-final-race.jsonl` rawSHA `25efe6c4eb5524b1e4652bcf9f66ab4cdb021934713f5654cbf3459bfebbb092`，448RUN/PASS；SB `/tmp/final12-combination-t3-fe782bd5/sandboxd-full-race.stdout` rawSHA `06b02af00762ba58be93a82dcb44b9d22e58ead0023df16974a1563ceb902fbb`，678RUN/PASS。只取Action/Package/Test，不读Output中的其他历史state。已有独立导出 `/tmp/plan022-cold-design-review-t3-22a3542c/{engine-accepted-test-pairs.json,sandboxd-accepted-test-pairs.json,test-oracle-recheck.json}`。新RUN/PASS `(Package,Test)`集合与events次数应精确保持各一次、FAIL/SKIP/RACE0。pairhash为Python json.dumps(sorted(set(pairs)),ensure_ascii=False,separators=(',',':')).encode()的SHA：Engine `a2a4959ba24810cb8e30036dd402032de5a2cbf3e2202100326d732d3c6774cc`，SB `064c984304643c2f0d6668ad6fd202c8db126495ba7a0996709b725164b7f5da`。只比总数不足。

失败保留原grade，不能重试到绿/改断言/时间/gate/fixture；若机械工具自身错，保留原诊断并只修自有工具再静态验证，不能误称产品已通过。真实新源码修正须有具体证据与父协调，无关bug不修。只旧47f5 production已接受，不能把旧VM成绩转新树，不运行VM。

## 7. 提交、报告与停止

source/tests/静态proof/门禁全部完成后按git history scoped convention提交自己的候选，可engine和sandboxd两scoped commits，最终HEAD/tree必须等于完整静态/测试快照；不用额外source修改后套用旧成绩。不改plans/index（父维护），不更新target。持续给父有意义进度，长测试异步有界observer，无>60s blocking sleep。

交付STATUS COMPLETE或STOPPED；STEPS逐项实际command/结果；STOPPED BECAUSE具体条件；FILES CHANGED完整路径；NOTES偏差/判断/失败。另机器handoff含branch/worktree/HEAD/tree/parents/有效file-decl-test/API map/等价工具输入输出与证明/完整rawgates及setchecks/sourcebeforeafter/finalcommitbinding/preservationcleanup。每个claim须能指实际本轮toolresult，原历史/未跑清楚标识。

禁止真实DB/config/env/token/argv/FD/VMJSON/.local-run/statearchive bodies，archive仅stat；仅读静态source与允许的controlled Go metadata/测试日志、closure/fixture，测试只自建state。不hostsudo/root/subid设置、依赖安装下载fetch、旧资源清理、任何VM/production或外部消息/push/deploy。只清理自己freshprocess/scratch，等待测试子进程结束，保留原references和他人state。STOP：base/tree/scope/对象漂移、缺声明/测试/断言、literal/SQL/字段/锁序变化、原接口能力改变、init/build漂移、cached材料不足、真实suite失败或missingnodes、超授权路径或真实APIcontract反例。报告具体证据，不扩大scope绕过。
