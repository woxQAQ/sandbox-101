# 013 R2 正式实施合同

本合同由父在完整阅读013 R2及新独立冷审后采用，授权单个新隔离executor实施、验证并创建自身scoped commit。它不授权parent dev提交/合并/推送、VM、真实生产状态取证、依赖安装或宿主修改。父只维护plans，executor不修改父计划/index。用户原批次落实授权已存在；最新明确要求“做完013就结束”，当前只继续013到独立SOURCE与实际生产门禁/清理/证据全部完成，随后停止，不再启动017、推进016/020/021或整批集成。无需再询问013内已授权的例行许可；完成范围收口不降低013验收标准，也不授权parent dev合并/推送。

## 最终状态：013 DONE，本轮已按用户要求STOP

精确`1414b4e376a70fec9ed9f16b30db588cd38c4b88`/tree`96602c8b6eb902d81687dfab3865f11ec59438b0`已完成双独立SOURCE_APPROVE（七项全YES）及一次原始完整生产验收。父已接受[最终生产与清理收据](013-production-acceptance-1414.json)，原report/handoff/verification/cleanup/release为`/tmp/p013-production-1414b4e/`各同名文件；生产分支t3/plan013-production-acceptance，worktree`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-68d5daed`。

实际make一次exit0/wall142.601168s，父直接解析top+四子test5RUN/5PASS/fail0/skip0：adoption3.96s、完整lifecycle42.61s、golden/warm26.97s、realguestTCP45.15s，top137.93s。13managed VMM全部host+namespace exe/PID-starttime身份、UID/GID65534↔165533、emptygroups/5capsets0/NNP1/seccomp2/root-namespace隔离、每台armedCPU及memory671088640/swap0/pids256/freshIO2590wbps1048576读回父核通过，outer8G/swap0/tasks2048保持。无extraVM/重试/源码测试预算物理限制更改。

89记录PID-starttime已由父fresh确认结束，26owned PGID empty，unit sandbox-production-acceptance-1794045606-1183581.service notfound/inactive/dead/MainPID0、cgroup/VMscopes absent、hostmount0。全部618当前source/pre-run/post-run实际副本bytes/执行位/symlink与git精确匹配、13材料重新hash及26/33 source/OCI12blob-6descriptor/NAR前后保持。archive只STAT157182239bytes/0644且root0700，未open/list/hash正文。显式release已接受，fresh grant现slotfalse/owner none/handoff_expiredtrue，所有授权已结束，禁止复用或重跑。

限制据实保留：FF前记录HEAD/tree/branch/clean status/12gitlinks，但raw/full semantic index、refs及reference初始化完整快照缺失；完整实际保全从FF后pre-run开始，不重建冒充原观测。IO吞吐历史证据未重测，本次声明fresh per-VMM io.max；namespaceobserver因原PIDns teardown exit-9已wait，不冒称graceful。父只读审计parser最初误把unit样本计数当数组、把starttime单独当唯一身份、把exec前engined阶段混入VMM及未处理null exe，已按真实数据形状、exe+PID/starttime校验纠正，无产品/测试变更或生产重跑。所有失败历史与原oracle分级继续保留。

当前总12项DONE，本轮**STOPPED_AFTER_013**。016/017/020/021、批次集成和fixture维护保持暂停，不因依赖解除自动启动。父HEAD仍b93/root8module既有patch保留，未merge/push。部署边界仍schema6 fresh-state、旧4/5/futureDDL前拒绝，无自动迁移/预算bootstrap。

## 本次fresh生产授权与执行过程（已结束，grant已过期）

精确`1414b4e376a70fec9ed9f16b30db588cd38c4b88`/tree`96602c8b6eb902d81687dfab3865f11ec59438b0`已获**SOURCE_APPROVE + FOCUS_SOURCE_APPROVE**，A1/R1/R2/R3/A2/R4/R5完整关闭均YES，blocking/newP1P2为空。主判决`/tmp/plan013-r4-source-review-t3-46728132/{report.md,decision.json}`、focus`/tmp/plan013-r4-typed-review-t3-ac4bfeee/{report.md,decision.json}`父全文核验；父直接解析主审typed9+完整build/SQL72+非selection4=85PASS，focus50+3=53PASS，fail/skip/race0。

父当前只读preflight通过：[013-production-preflight-1414.json](013-production-preflight-1414.json)。13材料hashsize、26guest source及packageNAR、OCI6可达/全部12blob、11工具/83缓存闭包匹配；原012最后88PID-starttime均结束、unitnotfound/inactive/dead/MainPID0、cgroup/Scopes/mount无残留、无其它active生产unit。Btrfs subvolume st_dev非blockID的首个断言诊断如实保留，findmnt分区→sysfs父nvme0n1确认259:0，未改预算；旧4MiB I/O测速是历史，不冒称fresh，当前run每VMM io.max需fresh精确读回。

仅新[fresh production grant](production-verification-013-1414.json)授权`verify-production-plan013-1414`：当时独占slottrue/owner本task；当前显式release已接受、slotfalse/owner none/grant expired，FF精确1414后以pinned材料与全新0700 `/tmp/p013-production-1414b4e`运行**一次原始make production-e2e**。不改源码/测试/安全断言/9-13312有限预算/guest512-VMM640-outer8G-IO2590wbps1048576/生产时限，无extraVM/独立guestprobe/retry/hostroot/旧资源操作/安装fetch构建/nesteddelegate。原runner可按既定机制创建/清理自己的unit、user/net/mnt/pid namespaces/private24G/daemons/VMMscopes；只读boundedobserver仅采ownedPID/starttime/status/exe-link/ns/cgroup/root-link/UID-GIDmaps与完整Caps/NNP/seccomp/CPU-memory-swap-pids-IOguard，不能取实际DB/config/env/token/argv/FD bodies或state archive内容。原grant完整允许动作与禁止边界优先。

原始top+四subtest必须全RUN/PASS（adoption/full lifecycle/golden-warm/real-guest-tcp）；材料/source618/index/gitlinks/reference实际set不变。完成/失败/cleanup或显式release立即结束授权，原archive只stat不open/list/hash，清理全部ownedPID-starttime/PGID、unit/cgroup/scopes/mount后交cleanup与slot-release，父expireJSON。本次实际5RUN5PASS及清理已被父核验接受，不能复用此历史grant重跑。用户限定**013生产、清理与证据完成后STOP**，不启动其他计划/批次集成/fixture维护，parentdev不merge/push。

## 采用依据与基线

- 主计划：`/home/woxQAQ/sandbox-101/plans/013-template-build-quota.md`。本合同及下列已批准R2优先于主计划中保留的历史阶段描述。
- 完整R2：`/tmp/plan013-design-r2-t3-45a594bc/proposal.md`，SHA256 `1930d294642bf5c8df89e6115dc0fe6b33d967aaddd9b11d34a0c28317e4204f`，62785 bytes／431逻辑行；必须全文读到EOF，工具首次50KiB后从346继续。
- 响应、差异及审计：同目录`response.md`、`proposal.diff`、`audit.json`。原首稿不是可执行规格，尤其不得沿用其74/135行的空B删除例外。
- 新独立批准：`/tmp/plan013-cold-design-review-r2-t3-19be6bf9/report.md`与`decision.json`，**DESIGN_APPROVE / blocking[] / D1 closed（设计层）**；必需C1/C2/C4/C5/C6全部采用，C3仅可选组织建议。
- 原D1与交错：`/tmp/plan013-cold-design-review-t3-f66831c7/report.md`。新executor必须阅读原异议与R2回应，不只读批准摘要。
- 012已经DONE；产品90批准`/tmp/plan012-final-review-65a243e3/report.md`，fixture78批准`/tmp/plan012-owner-budget-review-b7162bd3/report.md`，精确78实际production报告`/tmp/p012-owner-budget-production-78bc04f/report.md`。只读这些收据，不访问实际DB/config/archive正文。
- 唯一实施BASE：`78bc04fc8715d18319ed4cba69aa92a30975f9e5`，tree `d8a15809daa2ad2f8f6271edc9f400ba955888ac`；直接parent90f68b616faf7a1a6c82423674c0d27a2dda9bd6/tree64a4fc8940e625587a5db4417858aa7e3e04e43b。Engine subtree `1ec6a5354b1ba522d4e3be5b57ffbae3df706d6e`不改。
- 新绑定工作树原branch仅授权`merge --ff-only`到BASE；不checkout迁移binding、不接stage2/guest018、不重复cherry-pick依赖。保存FF前/后及最终source/index/refs/gitlinks/reference实际证据，预存reference dirt保持。
- **工作树准备状态澄清（本轮权威，修正历史数量假设）**：617个BASE源码blob/symlink与12个tracked gitlink OID是固定源码基线；reference checkout的初始化数量、实际HEAD/status应以**该新工作树进入任务时的观测**为准。历史R2的10个initialized不要求每个工作树复制。9个、10个或其他初始数量本身均不是STOP；未初始化的cubesandbox等保持未初始化，不fetch/submodule update或恢复到另一个工作树的HEAD。逐项记录path/initialized/gitlink/实际HEAD/status，保全该工作树原有状态。只有实际产品构建确依赖缺失材料、或发现本轮意外改写，才报告具体阻碍，不能以数量不同代替产品障碍。
- **index保全澄清**：保存raw index SHA并如实记录变化；编辑/撤回/commit或git status刷新stat cache可以改变raw bytes，不等于staged代码变化。应比较stage/path/mode/OID语义、Scope外entries及实际源码；不恢复raw index去伪造相等。static最终门禁窗口保存前后semantic entries及raw诊断，任何raw变化解释来源；自身Scope的正常add/commit单列，Scope外不能改写。

- **首轮源码审查A1修订（R2设计合同不变）**：d3be730的warm selection内嵌Deletion缺完整校验，实际Service.Open接受test-owned DB里清空ExpectedInstance后的rejected selection，并在恢复发出一次空predicate delete；strict Client拒绝后实物/费用仍在，不代表产品安全。按 `/tmp/plan013-adversarial-review-t3-a07822b7/report.md`、decision及最终probe源码/raw修订：selection的Refill/identity成功generation、原claim/group/tenant/Shape/V/B、完整delete request/key、Bound/Dispatched/Proof、Stage/Attempt/terminal-failed History必须startup和每次持久marker/receipt replay/dispatch前一致核验，不只检查ExpectedInstance非空。任何损坏ManualRecovery/调用尝试0/费用与public bytes保留，禁止从current B或重新生成请求修补。合法未发delete即已实际absence、unknown原key、terminal-failed历史前进和active/fallback历史都需正对照。
- A1有界修订基线为d3be730e257ad223e3103f1d284e59e15424f7a7，tree f5bd17c33db4a753da8244dcf10c8a157120ff69；修复仅允许warm_fallback.go、quota.go、必要recovery.go及现有template_quota_test.go。父仍不改源，原d3工作树/证据只读。原主审已返回R1/R2/R3并并入同一修订（下述）；该轮已交付da4b7d6，新双独立SOURCE复核中，不能宣称这些或A1已经关闭。
- A1的新startup/replay/历史与corruption矩阵新增实际覆盖，原最终fullrace已有295.815s、距300s仅4.185s。修订后唯一最终SB fullrace允许`-timeout=360s`，只增加测试进程deadline以容纳新覆盖，保留全部509场景（旧430/schema映射包含在内）/所有原安全断言；不变更guest/VMM/outer/IO/production时限等物理或生产限制。focused先验证具体RED→GREEN，最终source副本绑定后只跑一次必要full；未改Engine195s不重跑。若主审新增发现，父另行调和，不能为省时混过未决问题。

- **原主SOURCE审查已返回，A1同一在途修订加入R1/R2/R3**：`/tmp/plan013-source-review-t3-67912afb/report.md`、decision.json及independent_test.go、baseline_test.go/raw为新增权威发现。R1/P2成功warm selection的claim reader未在active交付事务退休，释放VM/换模板/反复恢复重开后旧SID仍有reader，费用384而非新T192，447额度下新D64 claim错误拒绝；同一probe父78 PASS/d3 FAIL。R2/P2实际ready warm的D64被改1、warm holder缺失、backing holder缺失三例Open均接受。R3/P2现有真实deferred-FK COMMIT证据有效但known-S/owner/reader/Resolution、qualified cleanup/marker及slot/backing交接、terminal dependency与新partial多行IGNORE覆盖缺失；除absence外缺同一物理连接query/普通TX复用证据。
- 这不是新的设计选择或产品Scope扩展：R2原合同已要求这些责任/校验/窗口。A1在途executor在**同一实施修订轮**继续全部四项，不另开并行源码分支；保持现有4文件Scope与parent d3。R1须在selection active目的地与S running/holder交付的同一事务，完整证明后退休精确原claim source reader，并让合法已交付且遗留reader的私有责任幂等收敛，公开终态bytes不可改；不得全删真实其他reader/backing/未交付source依赖。R2 startup以每条实际ready row与原refill冻结owner/完整Shape/SID/V/B/receipt/provenance匹配D费用、warm active holder及精确backing holder，不能以delivered跳过；旧SID滚动更新warm允许按冻结旧version合法保费等待evict，已claim转S且无warm行的旧group不能误报损坏。startup只读Service DB、不调用Engine/补账。原三损坏及有效旧version/claimed/reopen要实际正反对照。
- R3按原第11节补完缺失新013真实Service/SQLite矩阵，每个相应handoff有ABORT/IGNORE/实际COMMIT及解除fault后原key/私有责任/重开收敛，补新交接partial多行IGNORE；验证完整ledger/refs/owner/group Resolution/destination/gate/metadata及staged内存回滚。每个COMMIT业务族以connection-local TEMP marker等证明实际同一物理连接在错误后query与普通TX可用，不能只看pool后续可用；不改driver/DDL/Engine或加public hook。新key发布是新intent，不能冒充原public failed被改成功；原key保持failed，原私有资源只一次正确交付/回收。已认可旧COMMIT是真实的，无需把所有已跑病例重建成无意义helper镜像。
- main review不再pending；它是SOURCE_REVISE，现四项A1(P1)、R1/R2/R3(P2)均须关闭。可复用原主审实际5个RED节点、父候选同file对照及adversarial7节点，将对应实际组件回归纳入同一现有测试文件。聚合四项后统一格式/最终source绑定，再跑一次必要full360s，避免只修A1先跑full又立即为R1–R3再跑；若在收到steer前已实际开始，保存真实执行状态，不伪造取消或完成。owncommit仍只代表本修订交付，后续新独立SOURCE review与fresh生产门禁不省。

## 候选1414b4e源码已批准（独立审查历史与实现证据）

独立focus`review-plan013-r4-typed-holder-adversarial`已完成**FOCUS_SOURCE_APPROVE**，精确1414/tree96602c8/parent5c，七项A1/R1/R2/R3/A2/R4/R5在指定focus全部YES，findings[]。权威`/tmp/plan013-r4-typed-review-t3-ac4bfeee/{report.md,decision.json,audit.json,preservationcleanup.json}`。父已全文读report/decision、新ownprobes/typed_boundary_test.go，并独立解析original50.jsonl50PASS和boundary.jsonl3PASS，fail/testSkip/race0；独立wall32.676336s+6.978975s，boundary SHA81363916c1897bb515d5931fb5e0c8aae05ff5d2f8356e8d88eae882f29c306e。

新增真实pause后继边界：S.OperationID已为新的pause，replacement V/B仍在、实际capture新增sleep SID，费用slot1/D64+golden192+sleep192=448；先actualOpen接受合法pausing，再精确删除唯一active_use保cleanup sibling，startup/runtime均ManualRecovery、全state/public原successbytes/fee/V-B保持、Submit/Find0。它验证OperationID改变不使当前V-B的必需active holder成为可省略历史。R3完整矩阵在此focus沿用已审证据，source无新SQL写序；完整644/source-copies/fixture mapping仍归aggregate独立主审核验，不能把focus的53PASS当作其全量执行。

父此前将focus与用户收口steer到主审的记录保留；现主审已完成SOURCE_APPROVE，完整核验其report/decision及实际85PASS（9 typed+72 build/完整SQL+4非selection producer），无剩余P1/P2。focus53及source644/618copies/33fixture来源共同接受，七项聚合已关闭。仅按最上方新的fresh grant授权一次013生产，不把历史等待阶段或旧VMgrant转为新权限；完成013后停止。

唯一新commit`1414b4e376a70fec9ed9f16b30db588cd38c4b88`，directparent精确5c530395，tree`96602c8b6eb902d81687dfab3865f11ec59438b0`；t3-0c3b63bb/branch t3/fix-warm-fallback-holder-validation。仅quota.go、warm_fallback.go及现有template_quota_test.go三许可路径+367/-50，无recovery.go增量或新增文件；Engine/API/publicJSON/schema/DDL/deps/其它禁区不变。完整响应`/tmp/plan013-r5-fix-t3-0c3b63bb/{report.md,handoff.json,verification.json,source-proof.json,audit.json,preservation.json,cleanup.json}`。

中央requireQuotaResource精确资源tenant/kind/key/slots/cost/source及quotaRef{Kind,ID} EXISTS单资源计一次；groupAllocationRef依实际pending/known producer和sandbox终止交接选类型。active当前同V-B未terminated需active_use，terminated同代历史需cleanup；known D和collector分别精确cleanup holder，source reader/backing类型保留。无新权威flag/第二Done/SQL写序变化；保natural active+cleanup并存、unknown failed pending合法cleanup、ordinary cold/golden可无group，以及已退休资源历史。原R5反probebase实际3FAIL后同文件newGREEN、两独立probe均原字节不改。这是实现响应，是否完整闭环由新独立审查确认，不直接将旧R5判为关闭。

父已全文读报告与三个文件完整diff/268新增测试行，独立解析final-full-race.jsonl：644带Test节点PASS，旧635/611/509/映射430全保留，新9，testFail/testSkip/race0，四actualRust逐名PASS；typed-first-probes50、typed-new-business9、typed-producer-regressions84的raw也逐节点核通过。全部618当前bytes及final-before-full/source、final-after-full/source、final-after-commit/source三份实际副本的bytes、执行位、symlink等git blobs，canonicalSHA`e94e454d8de9fb60667e98c8a20c6e76886d1d23f679b0f7d993812508ec7780`。33fixture inputs/binary与git b93既定收据、指定checkout吻合；parentHEAD仍b93，root8module并发patch未采用。

最终formatted source绑定producer84与full644342.713s/360deadline、vet1.523s/normalworkspace lint4.097s0issues/privcompile0.717s零tests；早期external50有4路径hash但无单独618实物snapshot、format前后来源差异已披露，不冒充它与commit全字节绑定，新的review需精确源实际probe。原oracle19/2与已明确corrected副本、旧长TMP失败fixture race等级均按前文保留，非新的SOURCE豁免。

新派`review-plan013-r4-source-1414b4e`（aggregate全三diff/268测试及所有七项闭环、644来源/SQL/producer）和`review-plan013-r4-typed-holder-adversarial`（独立两R5 probe与pending-known-active-cleanup/后继历史边界），均新的delegate_task/隔离worktree/childRun1，不续旧review。实际task前缀`node:delegated-task:command%3Amcp%3A10a8b55d-fdde-47d5-b2e5-27d66d942a13%3Adelegate-task%3A`，分别接上述requestId；providerpi/modelmagpie/remote-magpie/codex/gpt-6.1-sol/thinkinghigh返回已核。完整携原brief/R2/D1/A1/R1R2R3/A2/R4/R5/全部响应/oracle澄清/限制。不重复未改342s/Engine195/Cargo/Rust；meaningful精确源实际组件探针与read-only全证据复核继续。

当前精确源码双审批准/七项聚合关闭、原始完整production及cleanup/evidence也已返回并被父接受，013DONE后本轮STOP，所有grant过期无VM权限。012DONE/总11DONE、017hold、旧VMgrantfalse-expired/slotFREE保持；只有新SOURCE实际批准后另发fresh独占生产grant，parentdev不merge/push。

## 5c阶段R5双审收敛与原修订授权（历史产品要求继续有效）

对抗`review-plan013-r3-oracle-and-destinations`已完成**FOCUS_SOURCE_REVISE**，全文`/tmp/plan013-r3-oracle-review-t3-c6984cce/{report.md,decision.json,audit.json,active-holder-probe.go,active-holder.jsonl,ordinary-positive-final.go,ordinary-final.jsonl,preservationcleanup.json}`。父全文读report/decision/正反probe并直接解析raw：其独立R5 startup/runtime两leaf+父共3FAIL/9.309649s、skip/race0；probeSHA`4e1b9df32ad2dd08eae13a24128b0ba6dd2546176f639e959c499b591b6b3814`。它与主审R5独立同源缺口收敛，临时名A3统一为R5，只计一项；**无其它新阻塞**。完整闭环仍A1 NO/R1 YES/R2 NO/R3 YES/A2 NO/R4 YES，所有NO只引用R5。两旧review现均已完成，当前仅一个source executor在途。

新增实际正对照`ordinary-positive-final.go` SHA`c13d3bf21d40faacca2276c5d39dfd6dbe5f776721e6cbdcfb75eed424011c03`，真实cold/golden普通claim不含Selection，actualOpen→pause/release→S.OperationID变化→Open/recover均通过，3PASS6.168098s，终值slot0/storage0或192，原publicbytes固定。其早期“普通claim必须有allocation_group”断言是probe setup错误，不能变成产品要求。另一早期pending探针错误假定unknown失败后的subject ref仍engine_dependency，实际合法责任已经cleanup，UPDATE命中0也是setup错误；**typed holder修订须保这种合法cleanup pending，不可全部改要求active_use/engine_dependency**。原R5自然running active+cleanup正对照仍需保留；普通无group路径不自动新增group或适配层。

正确oracle已完成40PASS含A2全21、da4 stage实际1PASS4FAIL，原19/2FAIL保留；既存fixture race分类双方一致，不是R5以外的新阻塞。父向`fix-plan013-typed-destination-holders`同一正在working的原childRun1发request `steer-plan013-r5-fix-focused-review-complete`，携完整peer报告/独立R5probe/正对照/producer事实/原brief约束/全部已知finding状态；工具delivery=steered。它仍base5c/directparent5c/4路径，**不再等待任何旧审查返回**；focused聚合完成后一次必要final360s/635+新增及原source/输入/静态/cleanup门禁，再自有commit/新独立SOURCE round。不是新实施轮或并行source任务，未SourceAPPROVE/013DONE/VM授权。

## R5采用条款（继续有效）

新主SOURCE `review-plan013-r3-source-5c530395`已完成，精确5c530395 **SOURCE_REVISE / 唯一新增R5 P2**。权威`/tmp/plan013-r3-source-review-t3-5b541bf0/{report.md,decision.json,holder_kind_test.go,holder_positive_test.go,holder-kind.jsonl,holder-positive.jsonl,audit.json}`；父全文读report/decision/完整正反probe及warm_fallback425–483与producer引用，采纳发现。完整合同A1 NO、R1 YES、R2 NO、R3 YES、A2 NO、R4 YES；A1/R2/A2共同引用同一个R5，不重复计数，原所有最小反例已GREEN。

正常warm拒绝→原exact-B删除→replacement正常clone/激活running，D自然有active_use/sandbox:S和cleanup/sandbox:S两引用。只在fresh test-owned DB精确删除前者一行，保留后者与所有金额/source/owner/fullShape/refill/请求receipt/backing/Srunning/V-B/publicbytes。actualOpen与runtime recover均nil，完整state/publicbytes/slot1/storage256/V-B保持，Submit/Find增量0。charged EXISTS只认holder_id，cleanup冒充必需active holder，delivered分配一般校验不能补漏。两leaf+父3真实FAIL5.271537s；自然双引用/重开恢复正对照1PASS5.511577s，无物理误删/提前退款/tenantDB能力或生产归因主张。

新有界修订唯一base **5c530395dd0452c98f11ef173e788b1d509650f3**，owncommit directparent5c，仍只warm_fallback.go、quota.go、必要recovery.go、现有template_quota_test.go四路径。以完整typed quotaRef（holder_kind和holder_id）表达目的地要求，保留资源tenant/kind/key/fullShape精确cost/source及真实来源/Resolution证明。不能只认名字/holder ID，不能用IN(active_use,cleanup)对所有阶段含混放行；也不能一刀把pending/known/cleanup/后继terminated/历史active都要求active_use。每类要求从当前实际producer和合法生命周期推导，并核当前公开/私有责任及真实持有关系。运行中同V/B active replacement必须精确active_use/sandbox:S；cleanup collector需cleanup/recovery:<collector>，保留自然active+cleanup并存，资源只计一次，不能回退为重复JOIN计费或禁止cleanup共存。对本次新目的地helper/必要同类查询审计类型检查，保留独立engine_dependency reader/backing/slot与unknown职责、合法资源已退休历史；不扩大到旧012非本次范围重构。

startup/load/write/replay/全group preflight在任何Find/Submit/退休/Done前fail closed；R5缺失active只ManualRecovery，所有state/费用/publicbytes及实际V-B保留，不补账/从currentB修数据/新key重分配/改公开终态/添加权威flag。原R5反probe和自然双引用positive原字节保留，实际base RED→新GREEN；在现有测试文件补有意义的typed-kind负对照/完整状态及合法各Resolution、后继pause-release/原keylate receipt/reopen正对照，不新增helper函数单测/机械镜像。中央数据模型优先，不做分散的单个漏洞补丁。若有实际SQL写序变化，沿用必要ABORT/IGNORE/真实deferred-FK COMMIT、同物理连接query/普通TX和原私有key重开收敛；只读谓词修订本身不机械复制故障矩阵。

原A2 oracle澄清仍有效，主审已独立核peer已完成corrected40PASS27.278783s（A2完整21）以及旧da4 stage组1PASS/4FAIL5.400376s，newSHA22bfacf06f9145a3a755fa4f756d93d5452ae7788cd1f1d3fe3187de29060f79；原probe19PASS/2FAIL保留。对抗review-plan013-r3-oracle-and-destinations已完成，最终独立收敛同一R5无新阻塞（上节），两个旧SOURCE round均不再pending，单一实施轮已steer合并全部已知信息。主审自己34focusedPASS/原19PASS/正1PASS，635及旧集合/618副本/33inputs独立核验。旧长TMP三race已基线四版本同blob证实为Service.Open前预存fixture启动失败诊断路径缺陷，非本次13新回归/独立阻塞；失败等级仍留作独立测试维护跟进，不扩本轮四路径修它。

meaningfulfocused/全部在途新发现聚合后一次final SB fullrace360s保留635+新增，短fresh TMPDIR避免Unix SUN_LEN（不改限额），source618三窗口实际副本含执行位/symlink及gitblob、33fixture输入/binary/31Ledger/00767/owner24leaf192+96/四actualRust/vet/正常go.work lint/零test privcompile/ownedcleanup/一个directparent5c scoped commit。不得重复343s未改广测、Engine195s/Cargo/Rust；若实际新覆盖超360保存真实结果回父调和，不偷偷扩deadline/物理限制或删旧场景。后续新的独立SOURCE round必须携原brief/R2/D1/A1/R1R2R3/A2/R4/R5/全部响应及oracle澄清，不续旧review child；SourceAPPROVE和fresh生产仍未授权。012 DONE/总11DONE、017hold、所有VMgrantfalse/expired/slotFREE，parentdev不merge/push，root8module并发漂移原样保留。

## 候选5c交付及A2探针判定澄清（历史收据继续有效）

A2+R4实施轮已交付单提交`5c530395dd0452c98f11ef173e788b1d509650f3`，directparent精确da4，tree`66d950b749a02e7c757670a9f9b6ce287f13d735`；t3-2760705d/branch t3/fix-warm-fallback-validation，4许可路径+750/-33，无Engine/API/schema/DDL/deps/public JSON增量。完整交接`/tmp/plan013-a2-fix-t3-2760705d/{report.md,handoff.json,verification.json,audit.json,source-proof.json,preservation.json}`。实施者实现/提交完成，验收自报STOPPED是未获准改变原探针矛盾oracle，不是源码已批准或需要用户例行许可。

父全文读报告/原A2probe/ledgerRestart/helper及失败raw，确认原probe52–66要求runtime ManualRecovery/损坏state不变后，68却无条件调用只接受Open成功的ledgerRestart；同一未修复损坏DB按合同startup必须ManualRecovery，二者不能同时成立。父先前要求整原文件原字节全部GREEN不成立，明确采用[oracle澄清](013-a2-oracle-clarification.md)。允许新独立review在自有/tmp建立仅改该错误restart/新release诊断段的副本：Close→真实Open仍须ManualRecovery→只读完整DB与before相同，保留其余runtime/state/reader/fee/publicbytes/VM-B/attempts/graph/Find/history及合法backup对照，加Find不变。原文件/helper/raw不改，原19PASS/2FAIL（1leaf+父）保持失败等级；新副本必须独立SHA/diff/实际GREEN并证明旧stage缺陷仍可检出。不得据此放宽产品启动规则、自动修补、skip或新增source exemption。

父已直接解析final-full-race.jsonl：635带Test节点PASS，原611/509/映射430全保留、新24，testFail/testSkip/race0，四actualRust逐名PASS；既存fixture33inputs/binary hash吻合。当前候选及retry-before-full/source、final-after-full/source、final-after-commit/source三份实际618条目逐byte、可执行位、symlink target全等committed blobs，canonicalSHA`0f03c0a1e583d82811d52deb8b073aa1e7234954dcd91cb94e4574e41a6beeed`。parent HEAD仍b93，root8module漂移原样保留。未重复343s/Engine195s/Cargo/Rust。

首个长TMPDIR全套exit-15/123.292s/235PASS/11FAIL/三条race输出仍保留，短TMPDIR11定点GREEN后才有同源635PASS343.254s最终full。父已读三race栈，均为integration/transport_test.go203失败启动分支bytes.Buffer.String与os/exec stderr writer；文件不在13修订delta。新的主审负责独立对照基线/上下文评估是否预存fixture失败路径及其验收影响，不能抹去race、简单凭最终全绿宣称全部错误路径安全，也不自行扩4文件Scope修复旧fixture。

新派`review-plan013-r3-source-5c530395`（完整4diff/新370测试增量/责任模型、SQL及aggregate SOURCE、635/源码来源、race定性）和`review-plan013-r3-oracle-and-destinations`（正确oracle新副本、原/修正版对照和A2/R4实际边界）。均新delegate_task/独立worktree/新childRun，不续旧review child；实际task前缀`node:delegated-task:command%3Amcp%3A10a8b55d-fdde-47d5-b2e5-27d66d942a13%3Adelegate-task%3A`，各接上述requestId。完整携原brief/R2/D1/A1/R1R2R3/A2/R4/所有响应/澄清/未决，两review返回前不得宣告全部发现关闭。

本节与oracle澄清优先于下方历史“原probe全字节GREEN”要求；保留历史FAIL和源码合同，不反改旧收据。当前整项SOURCE未批准、13非DONE、12DONE/总11DONE、17hold、所有VMgrant false/expired/slotFREE；只有新SOURCE实际关闭后父才可另发fresh独占生产grant。

## da4阶段R4追加采用（历史证据与产品要求继续有效）

主SOURCE `review-plan013-r2-closure`现已**完成SOURCE_REVISE，精确da4**。完整权威`/tmp/plan013-r2-source-review-t3-10b951c8/{report.md,decision.json,missing_context_test.go,missing-context.jsonl,audit.json,preservation.json,cleanup.json}`；父全文读report/decision/完整R4probe及对应warm_fallback22、quota1854/claimWarmTx、recovery2548/2611，核probe SHA`297a87c7214c4bd495b2f563057d2fc05e03c694dec42e7cf2b3de355a808ec6`并直接解析raw真实1FAIL、skip/race0。原四项最小反例都GREEN；完整合同判定**A1 NO、R1 YES、R2 NO、R3 YES**，A1/R2共同引用R4不重复计数。聚合仅两项P2：已采用peer A2和新R4，其余已审范围没有新阻塞。

R4实际只把正常rejected warm claim的`Group.Selection`置nil。首次原固定B delete transportUnknown beforeaccept、其他group/S/refill/operation/owner/fullShape/V/B/ledger/refs/receipts不改，Close→实际OpenInitializefalse接受；recoverResources返回nil、Done1/reader0，原V/B实在、slot1/storage256/publicfailedbytes固定，Submit2→2/Find1→1。不是删除整条recovery或伪造金额/身份，没有物理误删、提前退款、普通tenant写DB或实际生产归因。claimed warm已离warm表，readyRows校验不能补这个缺口；validator仅验证存在的Selection，nil fastpath让缺失的必需context被当作从未分配，generic无Identity/空allocation分支错误完成责任。

在**同一未完成A2实施轮**加入R4，不增加executor或Scope/base。须中央DB-only从原claim/S/冻结refill→sandbox交接和完整durable provenance识别Selection的必需存在性与一致性，在nil fastpath、startup、持久load/write/replay、任何收尾/reader退休/Done之前ManualRecovery。不得从current V/B/ProvisionSource单一标签、字符串suffix、非持久Operation.Origin或新增权威flag猜测/重建；也不能一刀拒绝所有nilSelection。合法build/warm主体、普通cold/golden非warm sandbox group必须接受；原warmclaim的fallback replacement、Root0/同名新SID或Shape、active后pause/release/restore、S.OperationID更换历史仍需原Selection和真实冻结来源，不能仅按当前S看是否warm。错误完整state/reader/费用/publicbytes保留、Engine Submit/Find0；普通空identity分支不能制造错误Done。

原R4 probe逐byte不改overlay必须实际RED→GREEN，补同进程runtime去Selection、合法非selection主体及claimed各种历史正对照到原template_quota_test.go，保留A2原own21节点全部GREEN和原A1/main探针。主审新的5个正对照（真实不同完整Shape/SID rolling ready、legacyactive extraReader跨pause/release、active SQL三fault仅原network-off一步合法进展、其他完整group不变）不能丢失或弱化。主审独立181PASS71.574s、原main6+adv7=13PASS12.087s、新5PASS7.995s、旧关键22PASS11.104s；611/509/mapped430/618三副本/33输入全核验，这些是已执行门禁，不为R4自动豁免。

父已向正在working的`fix-plan013-selection-stage-destinations`发送完整main原约束/原闭环/新R4证据与具体响应，request `steer-plan013-a2-fix-include-r4-missing-selection`工具delivery=steered、childRunordinal1不变；该轮现在**A2+R4合并修订**，main不再pending。仍唯一base da4/directparent da4/原4路径/一自有commit；全已知发现聚合后一次必要final360s保留原611+新增、source618三副本与gitblob/33inputs/vet/normalworkspace lint/compile-only/cleanup，无Engine195/Cargo/Rust/VM或物理限制变化。后续新独立SOURCE round须携原brief/R2/D1/A1/R1R2R3/A2/R4/全部响应与未决；总11DONE/017hold/slot FREE不变。

## A2采用条款（与R4统一修订，继续有效）

精确da4b7d6的新focused review返回**FOCUS_SOURCE_REVISE / A2 P2**。权威证据`/tmp/plan013-r2-validator-review-t3-f2a33310/{report.md,decision.json,finding-stage-proof.txt,own_test.go,own-final.jsonl,audit.json}`；完整probe SHA`d3cee324f5e4a761603139470a5bf40f9da1d1d6d2928772d24e867302d5d500`。父已全文读report/decision/proof、完整A2探针及对应validator/resolver/transfer代码，并采纳发现。

合法rejected selection（首次固定B delete transportUnknown）仅在test-owned SQLite改Stage为fallback_reserved，actualOpen接受；runtime绕过selection resolver，因Identity nil/Allocations空而退休原reader并写Done=true；原provisioning S/旧V/B/slot1/storage256/public failed bytes仍在，重开后恢复不再查询原delete key，fresh release错误等待不存在的replacement。cleanup_done同字段变体startup也接受，但runtime现有目的地guard正确ManualRecovery并保持完整state/reader，不夸大为同类运行时退休。无物理误删或提前退款、普通tenant改DB能力或生产事故归因主张。

采用最小修订：在中央DB-only validator核**每个stage与真实持久责任目的地**。fallback_reserved不能只看标签/空分配“全部完成”；必须绑定原claim的唯一provision request/key/owner/fullShape/source及实际replacement reservation或已采用/交付/合格回收历史，旧warm D的全部版本/backing与旧S V/B已按合法原交接退休。pending、known/result_known、active、cleanup、reclaimed/cancelled等合法历史分别按唯一Resolution/destination及当时/后继真实资源约束核验，不能要求所有历史永久持有原pendingD/旧warmD/当前template目录版本。cleanup_done要合格真实terminated S、清空旧V/B及旧责任已退，Done/reader退休仍需所有原step/未知责任满足必要条件。不得用公有operation failed/succeeded或非空B、Stage/Done标签本身代替目的地证明。selected/active/rejected及fallback/cleanup均保留原A1完整资格要求。

startup、load/write marker TX、receipt replay与runtime分派在任何Engine/reader退休/Done/退款前一致fail closed；损坏ManualRecovery，Engine attempts/Find增量0、原reader/所有费用/public bytes/完整state保留。普通resolver不能将不合格selection作为空allocation完成。先完成同一事务内合法目的地写入再做集中验证，调和transfer/写入顺序及staged内存，不得以合法事务中间状态为由加永久校验豁免、第二完成状态/权威标志或额外不安全窗口；也不能从currentB重建请求、分配新key、真实查询Engine修补启动元数据、改变旧公开终态。

原own完整21节点17PASS/4FAIL（3leaf+父），其中cleanup runtime正对照已GREEN；原adversarial7+main5合并12PASS。修订必须让**原字节不变probe全部GREEN**，保留graph自环/异源拒绝、Find期间request/history损坏拒绝，尤其合法fallback oldWarmB≠replacementB→Root target0→同名template换SID→Backup/Restore/Ack→pause/release/recover/reopen、selection原bytes不变/old+replacement实物NotFound/newT192正对照。增补fallback pending/known/active及qualified cleanup/late receipt/release-first、原步骤未终态时不提前Done/退休的真实业务对照；如新增SQL写次序，沿用ABORT/IGNORE/真实deferred-FK COMMIT及完整rollback/同物理连接query+普通TX证明，不造helper单测镜像。

新有界实施base唯一为**da4b7d6d3efb3eda2dec267e7f1ff7e8570dd8c6**，只允许原4文件warm_fallback.go、quota.go、必要recovery.go及现有template_quota_test.go；own commit directparent da4，不修改原候选/任何review worktree/父source。无新设计/Scope/Engine/API/schema/DDL/依赖/公开hook/物理限制变化，无VM权限。先意味明确focused/原反例/所有新主审异议聚合，最终格式/source副本绑定后一次必要SB fullrace360s保留全部611及新增；不开第二个改相同文件的source executor，不重复Engine195s/Cargo/Rust套件。原主SOURCE review-plan013-r2-closure已完成SOURCE_REVISE A2/R4（上节），新增R4已steer并入同一未完成实施轮；全部已知发现聚合再统一门禁，不能把主审说成仍pending。

原A1空predicate反例在该focus关闭，R1/R2原probe支持关闭，R3来源与raw矩阵支持补齐；**完整A1持久校验仍被A2阻塞，整项SOURCE_APPROVE未成立**。该轮修订后须新独立SOURCE round携原brief/R2/D1/A1/R1–R3/A2/所有响应与未决，不续旧review child；之后另发fresh生产grant。012 DONE/11项DONE、017 hold、slot FREE不变。root8模块漂移保留，源码/fixture只对git b93/候选与指定checkout，不用root当前modulebytes。

## 修订da4的交付与独立复核窗口（A2发现前后证据保留）

四项合并修订候选`da4b7d6d3efb3eda2dec267e7f1ff7e8570dd8c6`已交付，directparent精确d3be730，tree`e64bdf34f60fccc2cf42163d6c7c8a4c61a10ae4`；t3-a261ca35/brancht3/validate-warm-selection-cleanup。仅原4许可文件，+2018/-91，无Engine/API/schema/DDL/deps变化。完整响应与历史证据见`/tmp/plan013-a1-fix-t3-a261ca35/report.md`及handoff/verification/audit/source-proof。父已读三个产品文件全部diff，完整4文件与1441新增测试由新主审全量复核。

父独立解析最终full-race.jsonl：611带Test的(package,Test)PASS，原509/509、012原430/430（仅既定schema6→7映射）、新增102，testFail/testSkip/race0，四actual Rust逐名PASS；原main两个未改probe文件6PASS、原adversarial7PASS。实际618条目当前bytes、full前/后/commit后三份源码副本全部逐byte等于git提交，canonicalSHA495b19e2969c5d67db9d73ffdf256f8afcdc559d2c2abb4424bcbf80f1399f7f；完整full332.951s绑定最终bytes。focused181的功能源码与最终仅有两处换行差异如实保留，不能把它冒充相同bytes。静态门禁/零test privileged编译与开发失败收据保持，不重复未改333s/Engine195s/Cargo/Rust。

新主SOURCE `review-plan013-r2-closure` 与独立validator/replay对抗 `review-plan013-r2-validator-adversarial` 均为新的delegate_task、独立worktree/childRun；本轮返回前四项仍未关闭，旧SOURCE_REVISE不被广测覆盖。两个task实际前缀为`node:delegated-task:command%3Amcp%3Aca89d2da-49f7-4adb-a008-e45e62f1351e%3Adelegate-task%3A`，分别连接上述requestId。两者完整携原R2/D1/A1/R1R2R3/响应/未决，未复用旧review child thread。

父root HEAD仍b93fc24。2026-11-07T07:24:59Z另观察到8个未提交模块文件变动（engine、sandboxd、protocol/gen、operator各go.mod/go.sum）；来源未归因，父不修改/撤回/纳入本候选。候选全部618条目及module inputs仍精确git da4。审查源码/33fixture inputs应对照git b93/da4及指定fixture checkout，不能使用或复制父root当前模块bytes，也不能声称当前root所有product路径无差异。父plans仍可按明确绝对路径读取；该澄清已steer两位在途reviewer。父初次raw路径写为不存在的.stdout，改用真实.jsonl后核验通过；末尾root无产品差异断言因这8个变动失败，两个诊断均保留，不冒充整条脚本exit0或重跑Go。

当前012 DONE/总11 DONE、017 hold；旧3份VM grant仍slot_granted=false/handoff_expired=true，slot FREE。只有新独立SOURCE关闭四项后父才可另发fresh独占生产授权；本候选未SOURCE_APPROVE、未013_DONE、无VM权限。

## 精确Scope

产品仅9文件：`sandboxd/service.go`、`lifecycle.go`、`reconcile.go`、`recovery.go`、`warm_fallback.go`、`checkpoints.go`、`store.go`、`types.go`、`quota.go`。

唯一新增文件：`sandboxd/template_quota_test.go`。允许窄调旧测试：`sandboxd/service_test.go`、`recovery_test.go`、`maintenance_test.go`、`warm_fallback_test.go`、`backup_test.go`、`template_admission_test.go`、`quota_recovery_test.go`、`production_owner_budget_test.go`、`sandboxd/integration/production_acceptance_test.go`、`microvm_test.go`。这些仅真实actor/身份/费用/准入fixture、strictness及维护gate；不能删场景、弱化业务、安全、权限或物理限制断言。009 publication gate仅修改TargetWarm0→1那次actor为原owner Root，保留query/ownership/旧ready/同SID/终态及deadline断言。production/microvm只setup/明确tenant-root选择/有限预算解释，不改真实流程断言；production_network_acceptance_test.go只读。

文档仅`sandboxd/README.md`、`sandbox-architecture.md`、`deploy/sandboxd/README.md`中的计费/schema/pool权限。

禁止修改Engine、fork.go、backup.go、pool_backoff.go、bindings/direct/HTTP/SDK/egress/guest/protocol、依赖、runner、Makefile或新增其他文件/共享测试包/公开hook。types.go仅必要内部状态；不得扩公开Tenant/Command/Template/Operation JSON。无内部BC wrapper、dummy用户sandbox/token、自动旧库迁移、跨tenant共享、自动增quota、默认tenant/bootstrap、无限预算或Root维护代理。若真实Scope阻碍，保留具体源码/状态/最小需要后STOP回父，不自行扩大。

## 必须实现的单一责任模型

1. schema5→6：fresh version=6，quota kind加入`build_slot`（1slot/0storage，消耗MaxSandboxes）；旧4/5/future在任何schema DDL前拒绝。warm表无需新列。validateQuota同时验证新kind、subject、refs、费用、冻结来源及合法gate；只读本Service状态，不启动Engine或补旧账。
2. quota只汇总持久quota_resources/quota_refs。D=可信完整Shape.DiskMiB，M=MemoryMiB，每个独立SID计Sfee=D+M；独立磁盘版本分别D，同tenant同SID只一次。ChargeTenantID由可信资源owner确定，与原Operation.TenantID和key namespace分离。Root manual可跳比较，但完整收费；自动resume/refill/recovery按普通owner额度，缺tenant为NotFound，不提权/建默认row。
3. 内部typed allocationContext描述sandbox/build/warm主体及owner/完整Shape/resource/holder/来源。新私有resource_recovery Kind=allocation_group，ID=allocation:<原opID>，冻结原request/key/marker/receipts/主体/模板来源，不能用TemplateID跨generation重建身份，不在公开Operation JSON存新协议。按op/step共用dispatch/adoption/lateFind，去掉recovery:前缀追踪假设，不tracked的new013请求不可dispatch。
4. Resolution仍唯一六态unresolved/result_known/active/cleanup/reclaimed/cancelled；result_known不等交付。active/cleanup/reclaimed/cancelled有精确可验证destination；Progress区分继续lifecycle和允许parentDone，无Settled或第二完成状态。durable重读与deep staged内存，commit后发布。
5. 保留012冻结A意图、成功start/restore/clone immutable ResultInstance、tenant/S/V/B唯一instance_cleanup及全部来源/资格/history、active/reclaimed同事务责任交接、旧public失败/成功bytes不变、未知同key不换请求/猜B。新主体显式resolver，不能直接套需要真实S/restore的旧分支。

## Build与template

- Build admission同saveOp事务：永久owner/intent pointer、1build_slot、pending create D、pending capture Sfee、私有group及完整create Config/labels/key；SQL外才Engine create。D64/M128=1slot/256MiB，255前置拒绝，slot与storage的第二个**不同模板名**构建分别测，不能009同名Conflict冒充quota拒绝。
- create receipt固定V，只证明已知D，不证明B。start独立冻结request/marker/receipt，只有本原key成功start非空ResultInstance才绑定B；capture RequestBound在V/B已确定后一次绑定完整请求，同key不可变化。known D/S与pending、owner及proof同事务替换；failed带ID仍收费，unknown保留pending。warmup_inflight重启result_unknown，不重跑guest Exec。
- 正常build exact-B删除/可信回收与D/build_slot退休成功后才publish。新模板S继续收费。只剩capture未dispatch可取消该步S pending，不因此释放knownV或slot。任何未知capture可能有SID，保留S pending/source refs/必要slot，直到原receipt或独立destination结清。
- template_put用可信CP/sleep/capture/import完整Shape报价，不只比D+M总数；已有可信SID不在Submit增加阻塞GetSnapshot而破坏009 query gate，无来源Root外部导入在SQL/ownership锁外查metadata，再TX重核不可变owner/Shape/provenance，ordinary未知/外租户优先NotFound。sameSID新holder不改最初captured/import source。
- put admission原子增量S/intent/op/source dependency；publication已持template锁，顺pool→ownership→TX，新目录/新holder/active destination同TX。换SID先建立或采用旧SID唯一collector与cleanup ref，再精确tenant/kind/SID/holder删除旧active ref；不再holder_id全表清除。old实际存在或有backing/其他active/reader继续费，CP删除只去自己的holder。不能因catalog NotFound或无refs就退款。

## D1：无witness VM只有观察与合格账务，不自动删除

新013所有自动VM delete（build/refill/evict/selection fallback及cold replacement）必须有**已冻结成功start/clone的非空immutable B**和完整request/key/V/owner/Shape/config/current verified V/B/使用者/依赖资格。新allocation_cleanup canonical ID为固定域plan013-allocation-cleanup-v1与tenant/subject tag/subject ID/V/B逐UTF-8字段8字节big-endian长度前缀SHA256；保存tuple，冲突逐项核对。无空B collector、created tuple替代、current B补写或generic fallback；marker与固定ExpectedInstance=B delete请求先持久化。

Engine在ExpectedInstance空时不比较，Service GetVM看到created后external start C可完成。因此“自己的start从未dispatch”“failed start看似仍created”“完整Config/labels/Service ownership锁”均不授权delete。

- 私有已知身份记录（R2名BuildIdentity，但语义覆盖三主体）保留actual V、原create/clone receipt/request/source owner/完整Shape，即使没有B也不丢来源。
- RecoveryGate reason=generation_witness_missing、mode=observe_absence_only是动作诊断，不是Resolution/Done。保留knownD、相应build_slot或sandbox_slot、source依赖及pool占位，group.Done=false、两个Progress=false，但显式resolver持续扫描原key晚receipt和actual V。不能从generic discovery先建旧orphan，不能新attempt/Root代理/补start制造B；旧failProvision空delete也不允许接管新013。
- C4：以subject锁下最新durable public op+group+step marker/归属判断合法nonterminal原冻结步骤与已失败/失去lifecycle的cleanup gate，不能依据内存o.Status、临时error或VM.Phase。真正pending/running合法build可继续原start；receipt采用/SQL故障后也不得把public failed变可执行。晚到原key真实成功B可按完整资格交cleanup，public失败bytes不改。
- actual GetVM typedNotFound只允许进入账务TX。SQL/ownership外观察，subject→ownership→TX durable重核全context、V/所有D版本/backing/slot/其他使用者及unknown steps。只退eligible责任并同时记录精确absence destination/Resolution；queryerror/ListVMs遗漏/零VM不是absence。未知capture SID pending/原key/source refs和必要slot保持，不能全清group。确未dispatch只取消该步且不得已有actual结果。全部必要退休已结束或有合格独立destination后，D/slot/gate同TX一次退休，不发送物理delete。
- C5/C6：warm clone无B同样保留pool责任；fallback cold replacement无B保留真实sandbox_slot与selection，不能强造成build/dummyS。group/gate剩余unknown责任可继续扫描，不因V账务reclaimed或Done漏掉；两个generic退款入口和release入口都要覆盖。

## Warm池、claim与fallback

- ordinary首Template TargetWarm=0；后续ordinary保持已发布当前target，任何方向变target都Root-only，永久owner不变。Root选Bob不能更新Alice模板；已有Alice sandbox/CP由Root代操作仍Alice付费，op namespace维持原请求。refill永远普通owner预算。
- pool→ownership→TX重读ready模板/owner/Shape/SID、target/drain/backoff、ready+pending+known未交付+unknown/rejected cleanup占位；每补位先普通quota+D，再同TX pending/sourceS dependency/group/frozen clone/saveOp。无warm用户slot。target1下未确认Q回收前不再新key补第二台；missingB gate仍占位。失败准入允许明确未dispatch、无allocation的terminal诊断op，沿原1…256s退避并break，不每10ms刷op。
- clone receipt V/B/sourceSID与pending→known D/backing同TX，successful immutable非空B/current完整验证才publish ready。Info/Activate/none网络/空env无秘密在SQL/ownership外；已持或按template→pool→ownership→TX重核发布资格，ready row/warmD holder/backing/active destination同TX。目标0/旧SID/满池转合格cleanup；缺B只观察absence。
- claim Submit request→pool→ownership→TX，byKey先幂等，授权优先，稳定选真实ready row与完整owner/T/SID/Shape/V/B/clone proof，精确DELETE要求1，新slot/S/op/private selection与warm→S已有D holder同TX；不预留第二D、不造fake provision receipt。无warm才预留D。worker固定selection不再选pool；两claim只能一V。
- typed activation target带origin op、destination、固定V/B，warm派生step opID与origin独立传，不以字符串后缀猜。避免activateWarm后普通activate第二GetVM采current C；每次VM/Agent/network及最终running publication固定B。保留012原target校验、007最终fence/state/lease/credentials/LastError及Relay/Preview登记；无SnapshotID快速commitRunning也必须交selection active TX。
- rejected selection保留oldD/slot/V/B，exact delete key+marker先持久，未知不换key/分配新VM。拦住s.engine delete后generic reclaim和reconcileQuota NotFound退款：trusted absence后同TX oldD/精确backing→原claim provision key的同值pendingD、b清VM/Instance、golden/cold selection fallback_reserved/destination；总费不下降、不二次quota争抢。physical delete成功SQL失败仍留oldD，重开同proof只收账不再delete。
- release先行则交真实S cleanup、不fallback或激活；pending已dispatch unknown留责，未dispatch仅取消该步。fallback replacement新knownV无B同D1 gate，不借旧failProvision逃逸。固定delete terminal failed才保留history后同TX进入同V/B的下一attempt，missingB不进入此规则。

## 锁与持久原子性

保持request→pool→ownership→SQL、template→pool→ownership→SQL、S→configuration/ownership及resource-recovery协调顺序。无pool→template锁升级；需template时先退出pool重拿并重读。template worker已持自身锁，helper不能重入。ownership不得反拿S/template/pool。Engine/Agent/Manager长等待不进SQL或新增短config guard；不普遍改旧generic GC持ownership跨delete的原测试合同，新warm用持久fence转移后锁外IO。

所有新013必需SQL写核RowsAffected或完整postcondition，IGNORE/partial多行效果不能盲当成功，幂等INSERT=0要完整tuple/成本/source/refs/body相等。durable重读最新记录，deep staged mutable状态，commit后发布，失败不晚写覆盖。admission、known adoption、warm selection/publish、template replace、fallback、qualified cleanup、witnessless absence、terminal dependency必须覆盖ABORT/IGNORE及实际COMMIT窗口。

C2真实COMMIT：test-owned SQLite deferred FK+AFTER trigger，使目标业务语句执行、在tx.Commit才实际报约束错误；foreign_keys ON、单连接、原cached modernc driver不改。保存实际error/stage，比较完整rows/refs/group Resolution/destination/gate、模板/warm/S metadata及可见内存回滚；故障后同连接query/普通TX可用，drop trigger后原receipt/key+Close/reopen一次收敛。不能pin唯一连接等待Service，不加公开hook，不用ABORT/ctxcancel或Engine finish_before_commit ErrInterrupted冒充。若无法实证，STOP具体证据缺口。

## 有限预算与旧fixture

保留生产9slots/13312MiB=12D+7Sfee，D512/M512，按**实际在账责任**证明，删除旧全收费历史累计12D解释。正常trace保守life+adoption6D4S=7168；build7D5S=8704；claimedW+唯一Q8D5S=9216；W真实退休后TCP并保留Q cleanup为11D7S=12800。永久T费用一直存在。build D/build_slot成功发布前退休，G release在warm enable前，Root target0后W真实释放再使用TCP行；Q ready/pending/known未交付/unknown cleanup持续占target1。任意退休失败则production失败，未知失败可Exhausted，不承诺无限失败活性，不提额度/Root代理/物理cap。

- FiniteHTTP build结束slot0/D0/T收费；unclaimedW D+T；claim原D转交；tiers/TCP末尾永久T。原12D/7SID/9用户S集合另加真实build X和唯一后继Q，共14 distinctD；W/T不双加。逐identity receipt、费用或reclaimed destination证明，不只加法helper。
- C1逐轮maintenance permit，next inventory park只证明上一轮返回；单独有界等clone receipt、ready row和group destination。claim时park、明确一轮唯一Q、Root target0后等Q absence/Resolution/占位退休。gate不持Engine mutex等DB，全部失败/取消/Close放行并等待WG；原24leaf quiet协议保持。
- qrecWarmBacking同owner Root首target1：D64/S192/warmD64为320，CP/source release后256，oldwarm+oldS+newS448，evict后永久newT192；原实物/catalog断言保留。旧handmade warm测试补真实来源/账本/receipt，保留cold/golden、changed-instance与旧generic持锁控制合同。
- owner五HTTP组/24主leaf/192首轮+96恢复、missing-row负对照、ordinary root Root=false/Bob真实Upgrade/scoped visibility/精确与低1MiB零effect/1024vs1023/zeroWake/全部disk版本与backing/deep receipt/policy/实际InstallDirect全保持。

## 验证、证据与交付

1. 先新增五组TestTemplateQuota：BuildAdmission、CheckpointReferenceHandoff、WarmPoolOwnership、ReplacementAndRestart、BackupAndAuthorization。先list非零，至少build在78产品真实RED，CP若已保守GREEN如实说明；保存source/原始JSON/具体资源和attempts，不制造RED。
2. 实现后focused race实际五组、D1/C1/C2/C4–C6全部窗口、必要旧TemplateSnapshot/ForkRollback/RetiredSnapshot/RefillBackoff/Ledger/007行为非零通过；测试是实际Service/SQLite/公开Engine receipts边界，不写helper函数镜像单测。
3. D1具体覆盖create-known/start未dispatch与failed-start无成功witness，重复recover/reopen留费；受控GetVM实际created观察后external actor公开Client start C，Service delete尝试0、不采B/不退款/不新分配；external exact-C delete后actualNotFound合格账务一次退休。另测unknown capture lateSID不全清，以及absence ABORT/IGNORE/实际COMMIT。记录外部和Service attempts分别计数，strict fake拒绝危险delete不能伪装为产品正确。
4. backup/Restore/Ack含新kind/group/known/active/cleanup/selection各阶段、缺B gate/unknownSID、proof/keys/history费用；源Service先Close不双writer；旧schema4/5/future DDL前拒绝且test-owned DB bytes/table-set/version不变。
5. 最新78已有430带Test的(package,test)PASS／217.111s基线，不在改源前机械重跑。格式完成后存实际source副本/hash，再做一次最终 `GOWORK=off go -C sandboxd test -race ./... -json -count=1 -timeout=360s`（原d3基线300s/295.815s；A1新增矩阵的deadline修订见前述），保留全部旧业务场景并增加新案例，testFail/testSkip/dataRace=0。SDK no-test-files包级事件单列，不能混成test skip。
6. **唯一已知合法测试名称映射**：当前旧节点`TestQuotaLedgerSchemaBoundaryAndCorruption/schema-6`取自storeSchemaVersion+1；schema升级后同一future-rejection场景名为`.../schema-7`。比较旧430集合时显式记这一映射，不伪称430原名字逐字节不变，不造schema6拒绝来维持旧名字。原schema4保留，新组显式schema5/未来拒绝；若发现其他真实版本派生名字变化，逐项用源码与实际执行证据交代，不能静默删测试。
7. 所有module Go test/vet离线cached：GOWORK=off、GOPROXY=off、GOSUMDB=off、GOTOOLCHAIN=local、GOFLAGS=-mod=readonly。vet ./...、normal go.work `make lint`（不能GOWORK=off）零issue，diff/scope/source/index/ref保全。privileged `go -C sandboxd test -tags=privileged ./integration -run '^$' -count=1 -timeout=30s`仅编译，实际测试0，不算production或VM。
8. 原既有Rust fixture唯一 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f5c61c7/target/debug/examples/unix_fixture`，SHA d7fc857eb67e7f651a2857401bf677cc03117eff8a4f03baf272bf14802d41e9；33guest/protocol/Cargo输入与b93/fixture checkout逐字节匹配，binary前后hash。四actual TestRustAgentThroughSandboxRelay、TestRustKeyedExecReplayThroughSandboxRelay、TestRustRawForwardHalfClose、TestRustPreviewUsesRawForward在fullrace实际PASS可不重复单跑。缺材料BLOCKED，不Cargo/make integration/安装/fetch/Nix。Engine无改，不重复旧195s native广测。
9. 31Ledger、007全部20顶层/47sub、owner5组/24leaf全部保留；SQL fault/release-first/successBabsence/unknown/rootowner不丢。old430映射与new names逐(package,test)核，不只看总数/顶层。source全tracked blob/symlink+新未tracked文件在最终full前/后/owncommit逐byte绑定；gitlinks12及该工作树实际初始reference状态保持（不要求历史10个initialized），parallelT3新增refs单列，不声称全局冻结。
10. 清理全部自有Service/Engine fake、SQLite Rows/连接、HTTP body/stream/server、Agent WG/gates/进程组，bounded waits，保存cleanup证据。只允许新测试自有DB；真实production DB/config/archive、`.local-run`、旧daemon或未知资源不打开/修改。

## Preparation差异后继续实施（R2产品合同不变）

首个executor在t3-90582e1d已FF78，新增未提交starter `sandboxd/template_quota_test.go`；产品/schema/私有类型试改已精确撤回，全部617个tracked source等于78。报告 `/tmp/plan013-implementation-t3-90582e1d/report.md` 与audit.json，reference-baseline-comparison.json证实只有cubesandbox初始未初始化、12gitlink一致且原9个checkout保持。上述数量差异现已由本合同调和，不构成产品Scope/API阻碍，不需要再设计/用户许可或初始化reference。

已保存实际初始RED：build slots/storage/255低1三sub fail，CP handoff PASS；exit1/wall1.047s，首次create gate账本0slot/0MiB。五名称list不代表五组完整覆盖，后三组starter还未完成。初始未格式化source副本与formatted handoff字节不同已明确记录，不能混称同source。RED日志/副本保留，不清旧证据。

新的独立实施任务可以从自己FF78的新工作树，以read/write显式导入该starter文件（formatted SHA256 `2b6b29bd62b409486ac866f956b719f3f9a500f7a805f071fd3cd05fee03499d`），仅此文件、不导入其他旧工作树增量。验证copy hash并记录来源；它只是初始测试基础，须补完整R2五组/D1/C1/C2/C4–C6矩阵和strictness。若重跑短focused RED以绑定新source/context可执行，不重复未改217s全套或195sEngine。不得修改原t3-90582e1d或原/tmp收据；没有本项own commit可cherry-pick，自己的新commit parent仍精确78。

交付STATUS COMPLETE|STOPPED、每步骤与实际命令argv/env允许项/exit/wall/case/skip及source hash、RED/GREEN、D1/C1/C2/C4–C6证据、原430映射/新五组/31Ledger/007/24leaf/四Rust完整性、Scope文件diff、owncommit/parent/tree/worktree/branch、source/index/ref/fixture保全、cleanup和如实偏差。commit沿项目风格`sandboxd: account template builds and retained snapshot resources`，只提交本项Scope增量，不父dev commit/merge/push，不改plans/index，不嵌套委派。

SOURCE APPROVE必须后续新独立review给出；本轮无任何VM grant。实现交付后父审完整diff并发新SOURCE review，之后才可fresh grant/新artifact一次原完整production（adoption/lifecycle/golden/warm/真实TCP全实际PASS、原512guest/VMM640/outer8G/IO/caps/credentials保持、owned清理显式slot release）。012旧grant和旧PASS不转移为013通过。013仍IN PROGRESS，017等待它；012DONE／11项DONE保持。
