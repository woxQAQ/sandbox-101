# Plan 022：按职责重整文件、测试单元和公有边界

## 授权与状态

用户明确要求按其目录/接口/测试/public API原则修改`t3/final-twelve-source-combination`，且不得代码逻辑漂移。状态：DONE（候选33a3新门禁及独立SOURCE_APPROVE已父接受；原target owner唯一FF已完成，父fresh678source/完整index/分支与原改动核验通过，目标干净33a3，执行结束）。这是12项最终组合验收之后的新结构整理任务，不启动016/017/020/021，不维护无关旧fixture问题，不merge/push父dev。用户明确命名的目标分支在独立审查通过后由其原T3所属执行线程进行FF更新；父advisor仍只维护plans。

精确基线：47f5ce71bfa8de2720e184fec516f0e27ca90f2d，tree3e8cc8a12cb70dd36fffc428f9aff83d8712fe73。入场时原目标工作树`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-fe782bd5`已父只读核实branch正确、HEAD47f5、status clean。最终由原owner再次保全/freshclean检查后唯一FF至33a3；父独立核最终tree56c1、678ordinarysource bytes/modes/symlinks、完整semanticindex与已审/已测试候选一致，12gitlinks/原initialized0保全，工作树干净。见[最终报告](022-RESULT.md)和[最终接受](evidence/022-final-acceptance.json)。

原47f5有完整12项组合SOURCE_APPROVE、sandboxd race678/350.166s、独立focused65、fresh Rust49、新guest/OCI双格式KVM及真实production5PASS/141.101s、13VMM guards/109PID26PGID清理；这些是精确旧树历史事实，不直接冒充本轮新source通过。所有VMgrants已false/expired，本轮没有VM/生产权限。

## 用户原则（本轮权威）

1. 按负责什么内聚，相关代码先同包；不先按models/services/interfaces层分散。文件多于包不是问题。
2. 代码多先拆文件；新包必须说明独立提供什么能力、谁更容易使用修改，不能只为目录整齐，不能靠大量export/getters/相互依赖维持拆包。
3. internal仅限制仓库外部导入，不给内部包共享private访问权。
4. 不建议大interface或单实现interface；看真实生产实现与消费者，testfake不伪称第二生产实现，也不能把大interface拆成一堆无独立能力的小单实现抽象来应付。
5. 跟源码一起维护的单元/逻辑单元测试，一个$NAME_test.go对应一个$NAME.go，文件边界遵循逻辑单元而非函数计数。真实组件行为tests继续测Service/SQLite/Engine/UDS等；不新增helper实现镜像单测、不削弱旧断言。integration/privileged/E2E须明确其语义，不生造空production文件凑名字、不把所有难配对测试统称integration逃避要求。共享testfixture保持private *_test.go，优先放已对应的service_test.go/engine_test.go。
6. Public API需逐项有真实理由：interface/struct、exported单例、interface与struct方法/字段、普通函数及注释。可减少内部导出，不给内部/未发布分支细节保留多余BCalias。真实已发布公共合同和实际跨包消费者必须识别；HTTP/JSON/持久字段并非凭无直接调用可变private。
7. 全部调整不得逻辑漂移：不能顺手修bug、换算法、改错误/SQL/schema/JSON/权限/预算/锁序/操作序或去掉测试。

## 初始只读发现（47f5基线历史）

Go source分布：engine57文件、sandboxd56、protocol/gen3（含generated）、operator4。Engine大文件operations1321、gc_files1271、cloudhypervisor_linux1174、engine1078；sandboxd recovery3320、quota3219、service1464、lifecycle1397。大测试template_quota4914、quota_recovery3276、configure_egress2842、production_owner_budget2109。

`ast-grep outline`已核实：quota.go同时含quota资源/引用存储、预留/收据、allocation group、build/template/warm准入与交接/验证；recovery.go同时含持久恢复模型、扫描与snapshot/clone/rollback/group/instance collector。优先同包按真实职责拆完整声明，不能按固定行数切。

缺同名source的示例：sandboxd configure_egress、quota_recovery、template_admission、template_quota、maintenance、lifecycle_extra、production_owner_budget；Engine result_instance、instance_recovery、catalog、image_compat_links/validation；egress audit/egress/lifecycle。平台与integration测试另有无同名source场景，按性质判断，不造壳。

Backend13methods、optional NetworkPolicyBackend2methods；api.Client7methods且api.Wait仅用Submit/GetOperation；sandboxd.Agent6methods、Egress3methods。是否收窄/保留以真实consumer和实现图决定，不能仅按方法数。typedDTO/API Request/Operation/VM/Snapshot等有跨模块/HTTP合同；Error()/MarshalJSON等可能通过隐式接口/反射使用，不能只凭rg无引用私有化。

## 零逻辑漂移验收设计

首先锁定基线47f5的生产与测试Go声明、public surface、imports/actualcompiled files、buildtags/directives、const/var/init顺序、全部Test入口/subtest名称和原关键业务集合。旧历史只作参考，当前实现与runtime证据是source of truth。

允许的默认变化是同包搬完整声明、按职责命名文件/测试文件、经逐symbol审查的private rename和对应调用方/注释可见性同步。无必要不新package、不改package import paths；不把test-onlyfixtures移进production。

必须建立可复核的声明映射和机械证明：正常搬移在去位置的AST/token上相等；私有化只经明确resolved symbol map归一，不能文本替换string/SQL/JSON/tag或局部同名变量；struct字段顺序与tags、接口动态能力、error文字、map/切片别名、控制流和调用/锁/事务/副作用顺序保持。默认不提取/合并函数body、不修改Submit switch逻辑；若确需helper extraction，先具体证明dataflow/eval-order/defer生命周期等价，不凭全绿自行扩展。

init/有副作用全局var的有效初始化序不得因文件字典序改变；const iota组不拆散改变值；Go文件GOOS/GOARCH suffix、build constraints、go:embed/linkname/generate/line/cgo、test relative paths和runtime.Caller等要特别记录并保持。Public comments中的安全合同不可删除弱化，新的注释按作者视角。

大interface/singleimplementation的改变必须解释why，不通过函数表/回调字段扩大内部公共面替代同样问题。可见性变化须核隐式接口/反射/JSON错误类型名等可观察影响；必要的wire/persisted类型及字段保持，无法证明无漂移的候选不改。

完整原testfunc及subtest字面名称、断言/故障/strictreceipts/fixture gates/cleanup不变。只拆tests文件归属，不删测试、改预算、缩case、为凑配对把同一函数的断言拆开。

后续源实现需一次必要的新Engine300s fullrace、sandboxd480s fullrace（exactold678与Engine448关键集合保留）、protocol必要门禁、三module vet/正常go.work lint/gofmt/diffcheck与source原副本绑定；原Makefile180s不能冒称通过。纯Go组织调整若guest/protocol/Cargo输入34未变，可复用stage1既存fixture SHAce62e84ee4b0ad6bb825f679811010733e825a21dc84826d1473ae56269b32d9，四actualRustrelay必须实际PASS，不Cargo/Nix/安装/VM。所有offlineGo GOWORKoff/GOPROXYoff/GOSUMDBoff/GOTOOLCHAINlocal/GOFLAGS-modreadonly；lint正常go.work；短TMPDIR避免既有启动诊断race。不得重复未改Rust49/旧VM；本轮不冒称新production通过。

## 执行分工

当前3个只读设计任务，均provider pi/model magpie/remote-magpie/codex/gpt-6.1-sol/thinkinghigh，新T3 isolation worktree、实际初始references数量保全/12gitlinks不变，仅授权FF47f5，报告只/tmp。Engine设计已完成并父全文读proposal/API/equivalence，文件映射仍待与另两份结果合并：

- design022-sandboxd-responsibilities-and-test-units：COMPLETE，`/tmp/plan022-sandboxd-design-t3-9223a1fd/`。894条导航/192静态Test入口完整mapping，同包主包31productionfiles，真实unit配对、保原package身份，egress仅test重组；原报告误写ownworktree/fresh/tmp，后只迁移自己16文件至真正/tmp，16/16hash相等，旧历史bytes保留，新relocation记录不回写；ownuntrackedfresh已清，仅原8reference dirty保全。
- design022-engine-responsibilities-and-test-units：COMPLETE，`/tmp/plan022-engine-design-t3-bbcd27c8/`，零新包/整声明搬移建议，16个newproductionfile、11个test目标名、原6个test文件搬空后删除；611条是outline导航记录而非AST声明总数，实施必须用parser展开GenDecl.Specs，不能丢grouped type字段。result_instance_test整体rename为operation_completion_test以配真实finish事务unit，instance_recovery_test→operations_test配execute候选fence；特权/E2E例外明确。API renameMap默认空、候选另列，须全局API审计并入后选择。实际initialized2/12且两个原dirty reference保留，非0/4/7/10也合法。
- design022-public-api-and-interface-consumers：COMPLETE，`/tmp/plan022-api-inventory-t3-a676c555/`，17包typed分析最终diag0/2600decl/832exports/6interfaces/314静态Test等入口；6接口保留理由明确，Fault/ErrInterrupted/LastGCReport/ImageManifest等有正面doc合同，持久DTO类型改名因自动JSONerror/reflect风险暂缓。原FF前完整semantic/rawindex未持久化缺口原样保留。

父合并合同 `plans/evidence/022-execution-contract-r1.md` 已写，选择7对象可见性变更（3个sandboxd内部method、Operation两个json忽略field、两个CH实现const），其余默认保持；有意Go名称/reflect公开可见性变化单列，不假称完整GoBC。已派新独立设计冷审 `review022-combined-design-contract-r1`，结果REVISE，唯一R1为三组跨单元Engine共享fixture仍留专用test文件，与共享owner规则冲突。父写新 `plans/evidence/022-engine-shared-fixture-overrides-r2.json`，恰14个完整声明destination改existing engine_test.go；已机械核对独立required清单14/14一致、无重复，原Test入口/body/所有其他映射不变，原报告bytes不回写。新独立复审 `review022-combined-design-r2-exact-fixture-override` APPROVE、R1关闭、无新增阻塞；父已读完整报告/decision/validation，确认14override、其余585decl/全部336production与118Test目标保持。依据[完整执行计划](evidence/022-sole-executor-approved-plan.md)已派唯一隔离 `execute022-approved-structural-reorganization-r1`，provider/model/options与要求一致（pi/codexgpt6.1sol/thinkinghigh），prompt内联计划全文。独立源码审阅及最终目标更新仍待执行，不把设计批准冒称SOURCE或产品PASS。

## 实施候选与本地核验

唯一实施者COMPLETE，branch `t3/reorganize-responsibilities-public-api`，worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-8a03fb80`。两scopedcommits `418486a49dbad9fed67ab643ce04f8eab004917e` → `33a3f829e70969a0f4269b999fed437398e0c54a`，tree `56c1e1e319ae401b7b84b4a9cb5309284db2b98f`，119paths仅批准scope。完整handoff `/tmp/plan022-implementation-t3-8a03fb80/handoff.json`，原工具失败/修订见同目录execution-notes.md，不掩盖成成功。

新Engine fullrace exit0/199.431s/448RUNPASS、36required含20Actual；新SBfullrace exit0/360.476s/678RUNPASS/四actualRustrelay；protocolrace3，vet三module/normalworkspace lint0issues/gofmt/diffcheck通过，fullsuite各仅一次/testFAIL-SKIP-RACE0。父独立解析raw及8门禁exit/wait/rawhash，Engine/SB旧pair集合和各一次精确保持；678actualsource gitbytes/modes/symlinks、八before/after/finalsource/index/refs/linkssnapshots相等，fresh34fixtureinputs/fixtureSHA与9knownpositivePGID/shortTMP清理核实，见 `plans/evidence/022-33a3-parent-local-intake.json`。父全文读23publiccomments+packageDoc搬移与工具历史。

新独立SOURCE任务 `review022-source-equivalence-candidate33a3-r1`（实际task前缀05d5e0a3-8b67-4db9-ac44-3b0b4cb6e514）审声明/token/绑定/init/build/方法ordinal归一工具有效性、API/comments/test配对和rawgates/sourcebinding。SOURCE_APPROVE，findings/requiredrevisions/unresolved均空；父已全文读decision/report/equivalence/cleanup并接受。独立fresh8次module类型分析diag0、实际599/889目标/shared14、43引用/314入口、方法ordinal沿真实receiver/embedding路径解析至同一个Obj，fieldindex/embeddingprefix保持；全量comments原义/23变更与packagedoc搬移、678source全部gatewindows对finaltree、554artifacthash和100write收据等已独立核。父SOURCE收据 `plans/evidence/022-33a3-parent-source-acceptance.json`。已通过新followup联系原target所属执行线程 `apply022-reviewed-candidate33a3-target-ff-once`，仅freshclean/HEAD47f5正确branch可FF33a3，不新commit/测试/build/VM；原owner唯一FF exit0已完成，report `/tmp/plan022-target-ff-t3-fe782bd5/report.md`。其后置报告初次因两个symlink记录额外target字段作整dict比较而assert；原诊断保留，之后只读按共有字段及git实际target bytes核实，父fresh再核678actualsource/index，确认无漂移/无重复FF/无新源码改动。父接受 `plans/evidence/022-final-acceptance.json`，状态DONE/STOPPED。旧47f5VM/production/Rust49不冒称新candidate成绩，父dev/b93及既存deploy/ACCEPTANCE/go.work.sum保留、未merge/push/deploy；016/017/020/021继续暂停。

本轮task前缀`node:delegated-task:command%3Amcp%3A2d01b9ec-2ec4-48a9-bb49-882313884af4%3Adelegate-task%3A`。原final12任务用291e旧前缀，不混用。

父读取/vet实际设计引用后，补齐精确file/declaration/test/API mapping，另新冷审批准，然后只派一个sourceexecutor（所有Scope文件同一checkout串行，不并行编辑），提交自有候选；再新独立SOURCE/equivalence复核，完整brief/映射/所有异议及回复每轮携带。最终通过后联系原目标所属child`thread:delegated-task:command%3Amcp%3A291e8287-90a6-4d67-884a-c5779f6f8a5e%3Adelegate-task%3Aexecute-final12-combination-r1`，作为新的test/apply followup，仅在其ownbound fe782bd5 freshclean且HEAD仍47f5条件下FF到已审候选。若用户有新改动保全并调和，不覆盖/重置。用户已明确要求改此目标分支，此范围内最终FF不另问确认；父dev永不merge/push。

## 安全与范围

不读取真实DB/config/env/token/argv/FD bodies/VMJSON/.local-run或statearchive内容，archive仅STAT；不安装/fetch/uncached依赖/Nix/Cargo/hostsudo或subid配置、旧资源cleanup、无新VM授权。测试只自建state。12项原安全机制、immutableterminal/owner/resultgeneration/typedrefs/unknown收费/无emptyExpectedInstance删除/SQLwriteproof/短configurationadmissionguard及既有锁序全部保持。022仅结构整理，不借原任务授权推进016/017/020/021或维护旧fixture。

历史检索已经按obelisk成功refresh，首轮CLI输出过宽后用plans/evidence/022-history-query.mjs的6条/300字投影做有界检索；命中为tool_result/source artifact，明确不当作新用户决定或独立当前事实。当前47f5与已接受正式合同仍为权威。
