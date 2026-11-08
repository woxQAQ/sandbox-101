# 012 production owner 有限预算：已批准 setup-only 实施合同

本合同是 Plan012 的必读窄增补。设计提案 `/tmp/plan012-production-owner-budget-design-a55f6dce/proposal.md` 已经独立 `/tmp/plan012-production-owner-budget-cold-review-dce02974/report.md` **DESIGN_APPROVE，blocking findings=[]**。父完整读取两份材料并正式采用；实施者也须全文读至EOF，C1–C4以本合同的明确要求执行。不需再次询问已授权批次的常规许可。

这不是源码批准、production PASS、012 DONE或VM grant。source90的产品实现批准保持；只修改下面两个测试路径。主计划此前禁止修改生产验收测试的规则，在本合同指定的setup范围内作唯一例外。

## 原因与产品合同

source90为 `90f68b616faf7a1a6c82423674c0d27a2dda9bd6`，tree `64a4fc8940e625587a5db4417858aa7e3e04e43b`。原production fresh root credential默认owner root，却没有建立普通tenant预算行。root公共操作可绕过拒绝并完整收费；DataReady按owner普通Principal做自动resume，新D预留在tenant查询处NotFound。实际Service/HTTP SDK/新SQLite八路无row失败/零resumeop/零clone，同snapshot显式root resume及有row对照证明组件因果；历史production DB未读取，具体历史对象归因仍LIKELY。

保持“root只跳过比较、自动维护走owner普通有限预算”的政策，不添加默认无限quota或产品root bootstrap，不传Root:p.Root，不按TenantID字符串提升权限。生产使用原root credential、原默认owner，不切tenant/token，不改变任何生命周期/安全断言、guest shape或物理限制。

## 精确允许范围

1. 修改 `sandboxd/integration/production_acceptance_test.go`：首次sandboxd List-ready之后、首个live-daemon-adoption之前，一次显式有限tenant HTTP setup；本文件局部公式/注释、同文件有界HTTP helper和必要imports。
2. 新增 `sandboxd/production_owner_budget_test.go`：不带privileged tag，external `package sandboxd_test` 的实际Service/HTTP SDK/新SQLite组件行为与测试内strict Engine/Agent/Egress；测试自有SQLite只读断言。

禁止修改任何产品源、SDK、Auth/admission/quota/recovery/engine/guard/SQL/DDL、依赖、guest/protocol；原 `microvm_test.go`、`production_network_acceptance_test.go`、runner/Makefile/limits及全部旧assertions保持字节不变。不加共享budget产品包、公开helper或product test hook。父管理plans/index，执行者不改；只在新隔离worktree实施/自有scoped commit，无父dev合并/推送。

## 有限额度及证明

D=原DiskMiB512，M=原microVMMemoryMiB(t)512，S=D+M=1024是每个不同SID的保守逻辑费用，不是运行RAM额外收费。

| 固定场景 | slots身份 | 独立D allocation | SID | 累计费用 |
|---|---:|---:|---:|---|
| adoption | 1 | 1 | 0 | D |
| lifecycle | 3 | 5 | 4 | 5D+4S |
| golden/warm | 2 | 2 | 1 | 2D+S |
| TCP | 3 | 4 | 2 | 4D+2S |
| 总计 | 9 | 12 | 7 | 12D+7S=13312MiB |

setup payload id=root、MaxSandboxes=9、MaxStorageMiB=12D+7(D+M)。每项系数按上述场景局部表达与注释保留，不用1<<20或百分比余量。这是固定allocation intents的无退款累计包络，包含pending或确定identity、延迟cleanup和backing；**不是同时峰值、root公共请求硬上限或所有物理资源已计费的证明**。原物理VMM640MiB/outer8GiB等完全不变。

source90未完整收费build/unclaimedwarm属013；warm物理refill attempt数无固定场景整数上界，不拿本预算声称该缺口已解决。固定场景新增capture/claim/restore时重新列身份再调整公式；任意故障无限retry不保证预算足够，不加预算掩盖真实失败。

lifecycle distinct资源：V0、forkB1/B2、autoResumeV1及V1的rollback额外D；SID C(checkpoint)/F(fork新capture)/H1/H2(sleep)不同。paused=2D+3S，autoReserve后E=3D+3S，rollback=4D+3S，第二pause同步delete前峰值4D+4S。

- D512/M512：paused4096，auto exact4608/low4607；lifecycle同时峰值6144，lifetime累计6656。
- D64/M128：paused704，auto768/767；普通Alice整个lifecycle第二pause峰值1024，1023拒绝。

Root公共请求合法绕过比较，不能用root公共second-pause在low预算下必须拒绝作oracle；精确拒绝用owner普通auto或普通Alice。

## 生产setup HTTP与保密

SDK没有PutTenant，不新增SDK方法。用现有 `POST /v1/tenants`、原root bearer、原默认namespace；不设X-Tenant-ID。首次只发一次请求，子context如10s受父ctx约束，HTTP Body关闭，响应限64KiB，status201才通过。成功只decode/检查id及两limits；返回的普通tenant token被忽略，不打印/落盘body、header、Tenant完整struct或client对象。请求省略Token，不把管理root credential复制为tenant.Token，不新增token文件、不切换SDKcredential。失败只报固定stage/status/允许的typed code，不回显raw响应。

C1：组件后续调E/E-1时把已有test-owned**普通tenant token**原样带回，避免PutTenant省Token会轮换hash导致普通caller认证NotFound；生产首次setup仍omitToken。管理root credential绝不能当普通token传入。组件helper两种语义无需强行共享。

## 必需实际行为矩阵

五组明确顶层，所有子场景实际执行，不写budget/helper算术单测：

1. `TestProductionOwnerBudgetMissingRowHTTP`：原无row/root完整claim→CP→fork2→pause→8Connect+info，八typedNotFound、clone/op/pending增量0，sleepSID存在；同credential/snapshot显式rootresume成功且仍无row。负控制保留，不把诊断oracle PASS冒充成功RED。
2. `TestProductionOwnerBudgetFiniteHTTP`：同一成功oracle省略tenant API先实际RED，再显式finite setup GREEN。至少原D512/M512的8流一次clone/op；Service reopen预算保留；完整production allocation序列的actualHTTP行为重放（adoption Service close/reopen同VM、life、golden/warm实际maintain/refill/claim、TCP资源顺序），按每个resource identity/refs检查费用，而非自己算公式当证明。fake网络不冒充真实guestTCP/COW。
3. `TestProductionOwnerBudgetAutoResumeBoundaryHTTP`：**6 callers×exact/low×2 shapes=24主leaf必须全部执行**；每leaf8次Connect，12个low leaf八typedExhausted/op0/clone0/pending0/resource-ref不变，同state经API升到E再8流成功一次clone。callers为原管理root/rootowner、普通root tenant token/rootowner、Alice普通、root默认代Alice、root真实X-Tenant-ID=Bob代Alice、Alice sandbox-scoped token。费用与auto opnamespace始终owner；Bob再高也不能代理Alice低额度。scoped不能读其他S。Root公共低预算bypass另外fresh state对照，不能污染auto低场景。
4. `TestProductionOwnerBudgetLifecyclePeakHTTP`：ordinary Alice D64/M128/3slots，1024完整life成功；1023在secondpause准入Engine network/capture/delete/Agent前Exhausted，无op/pending；API回1024原key/合法流程收敛。外部hibernate-delete gate见证4D+4S实际rows，不直接SQL写ledger。
5. `TestProductionOwnerBudgetRetainedIdentityHTTP`：实际catalogNotFound+live backing/query error/reopen/samekey immutable proof，费用不早退，真实V删除后才退，CP独立S、无额外clone。007pausedfence/secret缺失、running到期无admission、zeroWakeRelease不读guest/clone等保持；不能新发明“expired paused必clone前拒绝”oracle（source90先auto-resume再判running）。

C2：主矩阵24leaf是硬要求，不是建议；192次首轮Connect及96次low→exact recovery按case/effect核对，不能只list顶层或省shape。

SDK.Connect不走Client.HTTP，selectedBob真实header需测试内bounded rawHTTP Upgrade，经真实Handler/Auth，返回用 `sdk.Connection`/`agent.BufferedConn`继续info framing；不能RoundTripper伪称header生效/在server直接注入Principal。

C3 strict fake：同key同参数固定receipt/异参数Conflict；mutable Request/Config/Policy返回深隔离，Get/Find不按currentVM补witness；clone ReqVMID空/SID存在Published，capture/network/delete ExpectedInstance真实匹配；首次start/clone/restore成功固定ResultInstance；VM→多个disk versions及多个SID backing集合，restore不覆盖旧集合；catalog delete与backing独立、VM delete才清其全部versions/backings。direct Egress Bind通过公开 `Service.InstallDirect`或同等严格当前V/instance Engine network_policy边界，在NICoff安装后才network-on；新instance无旧policy。unsupported action fail，不能默认成功或只数Bind/写NetworkActual=true。

C4 maintenance：Service启动在ticker前立即跑第一轮，Interval=time.Hour不能当“后台已停”。用测试fake明确bounded握手确认/控制首轮quiet；fullpool重放用真实maintain及有界condition wait。peak gate放fake外部，不持fake全局mutex等待DB观测以免交叉死锁；所有context/cleanup放行gate。test-owned SQLite用mode=ro只读断言、Rows/Body/streams全关闭、Service.Close及Agent goroutines全Wait。

## 验证与交付

实际RED成功oracle原无setup→HTTPsetupGREEN，缺row负控制保持；exact/low矩阵本来正确无需伪造其RED。保留原31Ledger、007全部47sub与F1/P2产品source，身份/权限/公开失败bytes不放宽。

offline cached Go：tests/vet GOWORK=off/GOPROXY=off/GOSUMDB=off/GOTOOLCHAIN=local/GOFLAGS=-mod=readonly；lint正常go.work无GOWORKoff。新5组focused race实际非空，之后**一次**final sandboxd全race（timeout240，新增矩阵必要预算，不删旧case），包含四actual b93 Rust fixture/零testSkip/无DataRace；vet、正常workspace lint0、diff/scope/source-preservation。新source前后hash/实文件副本保存，避免post-gate formatting来源缺口。

fixture只用已存在 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f5c61c7/target/debug/examples/unix_fixture` SHA d7fc857eb67e7f651a2857401bf677cc03117eff8a4f03baf272bf14802d41e9；核33b93guest/protocol/Cargo逐字节与binary前后一致，缺材料BLOCKED，不Cargo/make integration/Nixbuild补依赖。Go本项目测试编译使用已缓存modules允许。原精确engine448native已获批且产品未改，不重跑195s。

另外 `go -C sandboxd test -tags=privileged ./integration -run '^$' -count=1` 只编译类型检查新privileged setup，明确**不计作production/真实测试执行/VM授权**。保留精确argv/exit/wall/所有requiredtop与24leaf子case、实际资源身份费用/effects/opkey/ChargeTenantID、不输出token。自有fixture程序/handler/worker/SQLite全部Close/Wait、tempdir清理，日志保留；预存refs/输入/父source不改。

交付owncommit/headtree/parent/source90对应/两路径diff+实际REDGREEN+fullrace/privileged compile-only/vet/lint/diff/sourcehash及资源cleanup。完成后必须新的独立source review；本合同不授权VM/production重试或真实DB/archive读取。现有所有旧grant失效/slotFREE，012BLOCKED/013hold；source fixture批准后父另签新grant、新artifact执行原完整production全门禁。
