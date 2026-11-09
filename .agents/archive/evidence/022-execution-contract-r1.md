# 022 执行合同 r1（待独立设计审查）

状态：PROPOSED_FOR_COLD_REVIEW，尚未授权源码实施者按未审方案修改。用户已授权修改 `t3/final-twelve-source-combination`，不得逻辑漂移；这不是向用户新增确认门，父在独立设计审查解决具体异议后直接派执行者。父保持 improve advisor，源码只由一个新隔离 worktree 执行者修改。

## 本轮补充的优先级

本r1须与 `022-native-gate-supplement.md` / `022-accepted-test-oracles.json` 一起评审：补充明确纠正末尾一概禁namespace与required-native测试的冲突，仅未来soleexecutor原测试自有rootless native image隔离可运行；冷审不运行tests/namespace、全部VM禁令不变。原矛盾保留，不改写为起初没有矛盾。另源码编辑必须通过write/edit工具；机械工具可只在真正/tmp生成staging与证明，不能shell脚本直接改产品文件。

## 精确输入与交付

- baseline commit `47f5ce71bfa8de2720e184fec516f0e27ca90f2d`，root tree `3e8cc8a12cb70dd36fffc428f9aff83d8712fe73`，Engine subtree `1ec6a5354b1ba522d4e3be5b57ffbae3df706d6e`。
- 唯一最终目标分支 `t3/final-twelve-source-combination`，所属 worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-fe782bd5`；只有原所属 child 在新候选独立 SOURCE 审阅通过后重新确认 clean/HEAD47f5，才 FF 更新。新执行者只提交自己的候选分支，不更新 target。
- 父 repo `/home/woxQAQ/sandbox-101`，dev/b93fc24，既有 deploy/ACCEPTANCE/go.work.sum/plans 等 dirty 全保留，不复制进实现，不 merge/push。
- 总原则和执行上下文：`plans/022-responsibility-and-api-boundaries.md`。历史12项组合成绩在 `plans/FINAL-COMBINATION-12-RESULT.md`，只为47f5历史，不是新树成绩。

## 三份设计输入与唯一合并决议

1. Engine：`/tmp/plan022-engine-design-t3-bbcd27c8/{proposal.md,mapping.json,symbol-map.md,api-review.md,equivalence-requirements.md,scope.json}`。57个Go文件、611条outline导航记录、263条测试/fixture导航。采用 proposal 的完整 production/test 目标映射与责任解释。16个新增production file（含既有api包内client.go）；不新package。result_instance_test→operation_completion_test，instance_recovery_test→operations_test，catalog/admin/image/storage等按真实完整unit迁移，特权tagged/E2E例外明确保留。
2. sandboxd：原报告 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-9223a1fd/fresh/tmp/plan022-sandboxd-design-t3-9223a1fd/`，其原worktree内路径偏离/tmp约束已明确记录，正由原作者只迁移自己报告到真正 `/tmp/plan022-sandboxd-design-t3-9223a1fd/`；使用存在且逐文件hash一致的新路径；若尚未迁完可只读原产物，不能改历史证据。输入 `REPORT.md/declmapping.md/testmapping.md/mapping.json/filemapping.json/publicsurface.md/equivalence.md`。采用完整production/test目标映射，894条outline导航、192个Test入口，保持同包、主包最终31 production files，types.go/pool_backoff.go搬空才删除。egress仅测试配对重组，production原位；sdk/agent/cmd/integration不重组。tenant_test仍external sandboxd_test，原production_owner_budget整组为真实HTTP/Service/SQLite组件测试，不因名称带production而变VM测试。
3. 全局API：`/tmp/plan022-api-inventory-t3-a676c555/{report.md,inventory.json,export-inventory.json,interface-consumer-inventory.json,recommendedsymbolmap.json,dependencygraph.json,original-tests.json,notes.md}`。typed分析17包/2600声明、832个非生成导出符号、6个导出interface、314个静态顶层Test/Benchmark/Example/Fuzz记录；最终diagnostics0。typed分析不是产品测试。原FF前完整semantic/rawindex未持久化的缺口保留，不重建。

mapping是规划输入，不是可直接源码slice的脚本。outline记录不是完整AST声明数，必须parser展开全部GenDecl.Specs（如engine headerAttribute/cachedHeader、外包ownerRef/ownerLedger）并保留整组。不允许为迁移便利丢掉第二个spec、attached comments或directive。三份文件映射互补，API可见性只采用下一节；其余候选全部排除。必要的import整理须保持resolved binding和原package依赖集。

## Public API唯一变更白名单（7个对象）

| package / owner / kind / old | new | 精确理由与保持项 |
|---|---|---|
| github.com/woxQAQ/sandboxd / *Service / method / VM | verifiedVM | 仅http relay/preview的同包readiness helper；新名字说明现有matching instance + running + agent-ready + verified检查，完整body/error不变。 |
| github.com/woxQAQ/sandboxd / *Service / method / CredentialsMissing | credentialsMissing | 7个同包production调用；mutex与secret缺失判定body不变。 |
| github.com/woxQAQ/sandboxd / EnvVar / method / IsSecret | isSecret | 同包环境处理的3处消费；EnvVar/Injection类型、JSON字段、secrets语义不变。 |
| github.com/woxQAQ/sandboxd / Operation / field / Origin | origin | 分支新增的allocation origin运行时标记，json:"-"；字段位置/type/tag不变，不改任何持久resource字段Origin或字符串。 |
| github.com/woxQAQ/sandboxd / Operation / field / Hash | hash | 仅准入/store/recovery和同包tests内部使用的幂等校验，json:"-"；SQL hash列/HMAC输入/字段位置/type/tag不变，其他类型Hash不变。 |
| github.com/woxQAQ/sandbox-engine / package / const / CloudHypervisorCommit | cloudHypervisorCommit | 仅同包implementation及tagged KVM test；值/pinned CH身份/compatibility receipts不变；原同组const整体保留。 |
| github.com/woxQAQ/sandbox-engine / package / const / CloudBuildFeatures | cloudBuildFeatures | 同上；不改CloudOptions/BuildReceipt字段名或JSON。 |

这些是有意收回内部Go名称，外部源码若使用旧名字将不兼容，反射公开可见性/field名也会变化；不能把这一明确API范围的变更称为完整Go API BC。仓内typed对象闭包无跨包消费者；没有以无tag断言仓外不存在调用者。除上述有意可见性差异外，不允许运行逻辑/HTTP/SQL/持久JSON/错误/自动JSON error结构名变化，不保留内部BC alias或wrapper。现有运行代码未见按这些名字反射注册；实现者必须重新以types精确核查uses/selections与reflect/fmt等消费。发现真实文档承诺/隐式interface/反射业务依赖而不能证明保持行为时，暂停该具体rename，向父提供证据以收窄白名单，不私自改错误文字或补兼容层。

明确不采用：持久DTO类型改名/字段普遍小写（自动json.UnmarshalTypeError.Struct、reflect/%T可变化）；Fault/ErrInterrupted（原comment正面embedding合同，不抹掉再声称unused）；LastGCReport（INTEGRATION明确承诺）；ImageManifest.Verify/ReadOnlyBindings/DataSerials/DataKernelParameter、ImageBinding/MaxImageDataDevices/layouts（文档能力）；Reconcile/ReconcileInterval（维持本轮简单边界）；agent Dial/MaxFrame与operator/test/utils候选（本轮不扩散）；Wait函数签名缩窄（函数值Go签名兼容，不存在受阻caller）。

全部6个interface保持声明与方法集：Backend13同属host-effect lifecycle，唯一生产CloudHypervisor与真实故障替身；NetworkPolicyBackend2是可选能力，不强制并入Backend；api.Client7确有Engine/UDS两个生产实现并被router/Service完整消费；Agent6是消费方seam，concrete化会root→agent→root成环；Egress3、Resolver1已足够小并有documented host hooks。testfake不冒充第二production implementation，不新增interface、callback函数表、models/interfaces/internal包。internal不赋予兄弟包private共享。

## 精确实现边界

- 默认只同包搬完整顶层声明，遵循两份mapping。零新package、零新interface、零新production函数、零新测试入口；零删除/复制原逻辑。不能helper extraction、拆Submit switch、重构SQL/事务、改变参数/返回值、局部变量、算法或特殊分支。
- Engine原30production+27tests的声明及其目标files；api/types.go仅Client/Wait整声明搬api/client.go。其余engine/client/server/config/cmd保持bytes，只核对consumers。新增*_linux.go保留隐式Linux选择。
- sandboxd主包production/test按mapping，egress测试按mapping。egress production、sdk/agent/cmd/integration全部保持bytes。protocol/guest/Rust/operator/upstream/third_party/references/deploy/scripts/Makefile/Go manifests/workspace全保持bytes/modes/symlinks。
- 可同步准确public comments：Service/Options/Open/Close/Drain、核心领域DTO/Agent/Egress/Activation/Command/Operation及Engine核心入口；原StoreCheckpointList错误comment改Checkpoints，说明现有tenant/root范围。注释变化逐对象另列，准确讲已有业务约束和职责，不声称迁移、升级、新能力；安全合同原义不删弱。不为每个字段机械补空泛评论；隐式接口的Error/Read/Write/Marshal/String/GoString/fs等方法名保留。
- 新production文件必须容纳映射中的真实原实现，不造空壳配测试；不是每个source都新增测试。原逻辑单元Test完整移动，保持入口名/t.Run/故障table/SQL trigger/断言/timeout/gates/fixtures/Cleanup。shared private test helpers留对应真实service_test/engine_test/manager_test，专用helpers随单元。真实integration/tagged/E2E明确例外，不泛化逃配对。

## 无逻辑漂移证明（先静态，后业务门禁）

1. baseline和最终实际工作树完整source bytes/modes/symlinks与git对象绑定、stage/path/mode/OID index语义、12gitlinks、实际初始reference集合/HEAD/status保全。实际数量0/2/8等均合法，不init/fetch/归一化。记录初始化前后真实时刻，不伪造缺失快照，不恢复raw index bytes。
2. 用go/parser/scanner逐声明一对一：package identity/kind/receiver/name/type parameters、完整GenDecl groups/specs、函数signature/body、struct顺序/type/tag、const/iota/varinitializer、原literal拼写。移位置/必要imports之外零差，仅上述7对象resolved-symbol反向归一。不能全局字符串替换、ignore所有identifiers或Unquote SQL后比较。原错误、SQL/DDL、JSON tags、faultsteps、timers/budgets原token保持。
3. go/types比较Defs/Uses/Selections/Implicits/Instances，含普通/同包tests/外包tests、原tagged variants，旧对象reference closure与新归一对象一致；所有未批准对象绑定、method sets/optional capability assertions/interface satisfaction不变。批准exported surface差异单列。Go importer/metadata只用cache，缺依赖不fetch、不换模块。一次性分析器和机械搬移工具可放自有真正/tmp，不进产品source。
4. comments/documentation与body证据分离，attached Doc、bodycomments、directives无遗漏；原package doc正确归属。side-effect/blank imports保留。compare package var init依赖与有效顺序，不只查func init；ErrInterrupted/randRead/erofsVersion/directDeniedCIDRs/restoreNames/egress prefix tables/sentinel保持原file与相对顺序；const group不拆。buildtags/GOOS/GOARCH suffix/directives/相对paths/runtime.Caller行为保全；tests原入口不改，文件发现次序变化属预期结构变化且须用最终race检查隔离。
5. 人工重点复核原安全body严格相等：immutable terminal/complete frozen request/ResultInstance；trusted owner/typed refs Kind+ID；warm historical selection与destination；reader retirement+active delivery同tx；unknown conservative charge/无empty ExpectedInstance删除；RowsAffected exact1、rollback/同connection；restore synced candidate/lease/backing；短configuration admission guard/原锁序。旧013七项保持closed，不借机修旧fixture race。

## 新候选门禁（由sole executor运行，历史不替代）

- 所有修改完成、机械等价/格式/source绑定通过后，各一次Engine full race `go -C engine test -race -json ./... -count=1 -timeout=300s`、sandboxd full race `go -C sandboxd test -race -json ./... -count=1 -timeout=480s`。顺序运行避免host争用；实际长任务不让parent >60s无更新。保留原始stdout/stderr、exact structured argv/cwd/exit/wall，不能list/cache/skip代替执行。失败保留，不为凑绿重跑或改fixture/timeout。
- Engine全原test集合与原36 required native/source-names保留；SB动态RUN/PASS集合应与历史678精确一致，四actualRust relay必须实际PASS、全部FAIL/SKIP/RACE0。各按Package/Test集合比对，不仅总数。原static314跨模块入口也保持（含默认未选privileged/E2E）。Engine历史448可能选集含platform/native setup，须读取已接受sanitized名字manifest/原Go test日志定向导出或明确声明集合缺口，不把历史数冒充新通过。
- 用现有cache工具执行三module engine/sandboxd/protocol vet、正常go.work下lint、gofmt只读/diff-check；protocol有原Go测试则运行适当小门禁。operator不改，不扩大到其网络/cluster门禁。
- Go module命令使用 `GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-mod=readonly`；lint正常go.work。TMPDIR/GOTMPDIR用自建短路径，保持测试代码不变。任何module/workspace sum变化停止并保留差异，不采用父dirty sum。
- reusable fixture `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-d6927cf8/target/debug/examples/unix_fixture`，SHA256 `ce62e84ee4b0ad6bb825f679811010733e825a21dc84826d1473ae56269b32d9`。实际full34 guest/protocol/Cargo输入bytes/modes与baseline/指定stage1 checkout逐项匹配且前后fixture hash相同，才设置SANDBOX_AGENT_FIXTURE运行四个真实Rust relay。accepted full34 manifest `e1939a6d3bac9f56f1c89805ea7c2dcc04b852d9b35e5ccb99bfd31168f2c9eb` 仅同算法适用，不能以另算法hash不同比较。无Rust修改，不Cargo/Nix/new guest/VM。旧Rust49/production5保持历史等级。

## 范围/阶段/收尾

先独立设计冷审本合同+三原brief/报告/映射与全部已知异议；批准后一个新worktree sole executor保全自己的真实状态、FF47f5、实施/证明/门禁、按现有commit history scoped convention提交自有候选，给出commit/tree/change inventory/API map/full test/source proof/cleanup。不要更新目标，不merge/push。再派新的独立SOURCE/equivalence审查任务；每轮必须新delegate，完整携带原brief、先前findings/回复/未决异议，不用send childThreadId续独立reviewround。

独立通过后父核验实际证据，原target child执行唯一FF（用户已授权，无需另问）：fresh branch正确/clean、HEAD仍47f5、candidate含baseline且等于审过tree、12gitlinks/自身reference保全。若后来有用户改动，不覆盖/reset，报告并调和。父dev及原dirty始终不动；只更新plans证据和交付地图，新HEAD不是旧VM接受对象。

禁止nested delegate、依赖install/download/fetch/submodule init、Cargo/Nix/VM/namespace/hostroot/subid配置、外部消息/push/deploy、旧runtime清理；真实DB/config/env/token/argv/FD/VMJSON/.local-run/statearchive bodies不可读，archive仅stat。测试仅自建state。016/017/020/021及旧UNIX路径fixture race继续暂停。设计或机械工具自身问题与原失败必须保留，不篡改旧grade。只有owned fresh报告/进程/scratch可清理。
