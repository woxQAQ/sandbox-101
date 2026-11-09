# Plan 007：使运行中配置切换在前置失败时关闭旧出网授权

> 逐步执行并核对验证结果，发生 STOP 条件时报告。只修改 Scope；完成更新索引 007，reviewer 管理索引时交付证据即可。先运行 `git diff --stat b93fc24..HEAD -- sandboxd/lifecycle.go sandboxd/http.go sandboxd/bindings.go sandboxd/reconcile.go sandboxd/service.go sandboxd/store.go sandboxd/service_test.go sandboxd/maintenance_test.go sandboxd/backup_test.go sandboxd/configure_egress_test.go sandboxd/README.md deploy/sandboxd/README.md`，并核查未提交差异。006 可改变代理内部，但不改变本计划的 Service 撤权契约。

## 状态

- Priority：P1；Effort：M；Risk：MED；Depends on：无。
- Category：security；Planned at：`b93fc24`，2026-11-06；Status：DONE（r3–r5已独立SOURCE APPROVE；此前两P2关闭，隔离分支，未合入用户分支）。
- 审计映射：发现 2。

## 问题与目标

configure 已保存 none／更窄 allowlist／新秘密后，activate 的 VM 查询、network-off 或 Agent.Info 失败会直接返回，旧 relay binding 尚未撤销。Guest 原有 vsock 出网不经过数据面接入检查，LastError 不能阻止其继续使用旧权限。

目标：配置切换必须先撤销旧 binding 和已有连接，才发布新的配置并进行激活。前置失败留下明确关闭的代理状态，后续维护不能仅因“binding 存在”就认为配置正确。访问吊销与配置失败不能延长租约、重置身份或重放 workload。

## 当前状态与惯例

`sandboxd/lifecycle.go`：

```go
// configure 分支：保存 b 后执行
s.closeConnections(b.ID)
if err = s.activate(*o, &b, false); err != nil {
    b.LastError = err.Error()
    _ = saveSandbox(s.ctx, s.store.db, b)
    return err
}
```

activate 在调用 bind 前：

```go
if err = s.opts.Agent.Info(s.ctx, v); err != nil {
    return err
}
```

`sandboxd/bindings.go` 只有进入 bind 且指纹不同时才调用 revoke：

```go
if current != "" {
    if err := s.revoke(b.ID); err != nil { return err }
}
```

`sandboxd/reconcile.go` 当前仅在 `b.Policy.Mode != "none"` 且 binding 不存在时重建。binding 指纹含 vsockPath、策略及内存秘密，但秘密只能驻留内存；禁止把指纹原材料、真实凭据或请求内容写入日志。设计要求先装策略再交付，none/relay 均保持 engine NIC 关闭。

测试结构匹配 `sandboxd/maintenance_test.go:16` 的 `TestRestartReactivatesProxyBeforeAdmissionWithoutRenewing`，Service+真实 SQLite+明确 engine/agent 替身；`sandboxd/egress/egress_test.go` 展示真实 Manager/UDS/本地 upstream。新增联合 fixture 应直接构造实际 egress.Manager，不把 Revoke 是否生效完全替换成计数 mock。

## Scope

允许：上述 drift 路径，新增 `configure_egress_test.go`；store.go/service.go 仅 configure fence 与 schema 接入；backup_test.go 验证 fence 保留；两份 README 仅失败关闭/schema 部署契约，索引仅状态。r5正式窄增补：允许`http.go`的relay/preview最终fence核验与连接登记，以及必要service.go内部共享登记helper；不新增endpoint/capability格式/代理算法。该例外只解决实际DataReady消费者与configure admission的guard关系。

禁止：engine/guest、egress 的 TLS/解析算法、token/public Command 格式、自动重新运行失败 operation、真实 `.local-run/` 状态。内部 helper 可改签名，不保留无用途兼容层。

## 持久失败关闭约束

新增内部 `configuration_fences(sandbox_id PRIMARY KEY, operation_id NOT NULL)`。configure admission 在保存 operation 的同一事务登记/替换 fence，旧参数不写入该表，schema 2→3，旧库 DDL 前拒绝，不自动迁移。009 必须在本计划之后接续 schema 3。

DataReady 在自动 resume/rebind 之前及 sandbox 锁内二次读取时均检查 fence；activate 只允许 fence 指向的那个显式 configure 操作继续，其他调用（包括 rebind/resume/rollback）不能绕过。reconcile 对 fenced sandbox 只做按已证实例关 NIC／撤销代理和诊断，不自动激活。缺失/失败的 fence 查询不能当作无 fence。

只有匹配 operation 的 configure 确认激活、持久配置提交和 running 交付完成后，才能在同一最终事务条件删除该 fence。失败、保存新配置失败、daemon 重启或旧操作晚到均保留 fence；新 key 的显式 configure 可替换 fence，旧操作不得删除新 fence。release 可以继续撤权/销毁，完成后删除其 fence。LastError 只用于诊断，不承担安全判定；不能复用会被其他生命周期改写的 Sandbox.OperationID 作为永久 fence。

## 命令

以下为 Nushell，从根目录执行；无需安装。

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
```

审计时基线通过。新测试名称以下文为准，focused 执行前 `go -C sandboxd test . -list '^TestConfigureEgress'` 必须列出实际测试，不能以零匹配通过。

## 步骤

### 1. 复现新配置已保存而旧代理仍可用

在 `configure_egress_test.go` 添加 `TestConfigureEgressInfoFailureRevokesOldBinding`：创建可实际出网的 relay 沙箱，保持一条旧隧道，令 agent Info 在配置切换时失败，提交 none。通过新／旧 UDS 连接均不能再到达本地 upstream来断言撤权，而非只检查 LastError。增加窄 allowlist、凭据轮换两个子场景；只用随机生成的无实际用途凭据，断言中不输出值。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test . -run '^TestConfigureEgressInfoFailureRevokesOldBinding$' -count=1 -timeout=45s }
```

预期修复前失败；必须给全部网络连接加测试 deadline。不要连接公网或构造滥用负载。

### 2. 先撤权，再发布／激活新配置

在 admission 已持久登记 fence、且持有 sandbox 锁时，运行态 configure 首先关闭数据面，撤销实际 egress binding，确认 Revoke 成功后再切换内存秘密、保存新 Env/Policy 并 activate。当前代码内存秘密替换在前，需移动，避免旧 binding 仍可工作而 Service 认为已换凭据。

Revoke 失败时返回失败并保留 fence/LastError，不能将新 none 策略保存为成功。已经接受的操作及 key 保持原终态；恢复不得重放未知 guest exec。Info/VM/network/Activate/commit 失败都不得删除 fence；对新绑定之后发生的失败也必须 revoke。configure 目标 paused 时不创建代理，保存配置成功后才能条件解除自己的 fence。

按持久失败关闭约束修改 DataReady、activate、reconcile，先检查 fence 再考虑任何自动 rebind。none 显式撤销残留 binding；非 none 只有在无 fence 时才经当前指纹比较的 bind 路径核验，不能只测试 map key 存在。保留凭据缺失的关闭分支，不能因重启而把旧秘密恢复为新配置。复用现有 helper，不新增第二套策略判断。

**Verify**：上一步测试变绿；`TestRestartReactivatesProxyBeforeAdmissionWithoutRenewing`、`TestHostSecretNeverPersistedAndRestartFailsClosed` 必须继续通过。

### 3. 验证失败矩阵与恢复

新增 `TestConfigureEgressFailureMatrix`（GetVM、network-off、Info、Activate、绑定失败）和 `TestConfigureEgressRecoveryHonorsSavedPolicy`（重启、维护、none 残留、指纹不符）。必须单独覆盖“撤权成功→新配置保存失败→DataReady／维护／重启”，数据库仍有旧宽策略也不得重开代理。再覆盖备份恢复保留 fence、新 key 显式 configure 成功解除、旧操作不能解除新 fence、schema 2/未来版本在 DDL 前拒绝。断言 LastError、实际 UDS 可达性、旧连接关闭；身份和 deadline 不变。测试暂停配置不唤醒、不会提前创建 listener。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
git diff --check
```

预期全通过；原 secret 零落地测试必须通过。README 明确失败时的关闭状态和新意图重试方式。

## 完成标准

- [x] 三组新增行为测试实际执行；none、窄策略、轮换凭据的旧 listener/流均失效。
- [x] 前置失败不会保存声称成功的新出网状态，后置失败不泄漏新 binding。
- [x] 持久 fence 与 operation 原子接纳；DataReady/自动恢复在任何激活前检查，只有匹配的新显式 configure 成功或完成 release 才解除。
- [x] schema 3/备份保留 fence，保存新配置失败且旧策略仍在库中也不自动重新放行。
- [x] reconcile 清理 none 残留且比较有效配置；健康 none workload 跨维护不中断，重启不续租、不重置身份、不复原旧秘密。
- [x] r3–r5 race/vet/lint/真实既有 fixture integration/diff check 通过；新两P2及最终fence读取故障独立通过，原矩阵保留。

## 实施与复核结果

第二轮 APPROVE。最终 worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-967b8b47`，分支 `t3/fix-configure-fence-lifecycle`；基准 `b93fc24`，采用 `b45f1b0573d6b0d8c724e86e82c25f24ab9ab49e`（r1 的 cherry-pick）及 `75c13079f3d3515612b890520b25c9b20108d076`。不要同时重复采用原 `e3f1d0e`。

首轮虽通过全部既有门禁，父 reviewer 源码复核仍发现最终事务错误漏撤权、维护只看 binding 非空、未知 verification 跳过本地撤权及 supersede/持久终态交错四项，均要求修订。第二轮通过统一失败 teardown、持久配置与公开成功结果/fence retirement 同事务、configuration publication/admission 短 guard、按来源权限分开本地撤权与 NIC 操作、实际 reconcile→bind 指纹核对收敛。未修改 recovery.go 或扩大权限。

新七组 TestConfigureEgress 包含真实 SQLite 新配置保存/running保存/最终保存/DELETE错误及零行/operation结果/延迟外键 COMMIT 故障，以及 Info/Activate gate、迟到旧 intent、重启固定终态、实际策略维护与未知库存。执行者用 Go overlay 将新回归作用到 r1 源码取得 RED，当前全部 GREEN。父 reviewer 阅读生产完整增量及第二轮全部测试增量，重跑 sandboxd 全 race（31.867s）、vet、lint（0 issues）、真实 Rust fixture integration（0.704s）与 diff check，均 exit 0。schema 3 与备份/旧库 DDL 前拒绝回归保持，009 可接续；未合入用户分支。

## 第三轮：none binding 与健康 workload 中继

父核对已集成源码 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-ea88fcd4`，HEAD `df769bbfefb2de647c6da2815fccfc0a6a393c58` / tree `58b5a2cbcafd145464397ea12709f1b1a587be65`：activate 在 lifecycle.go:69 有 manager 就 bind；bindings.go 对 none 也记录指纹。reconcile.go:226 的新 none 分支却将所有非空 binding 认作残留并执行 closeConnections+revoke，会关闭健康 Service workload 中继。父已读三处完整相关函数；本缺陷属于 007 r2 增量（012 副本 `c2994c9`，标准原提交 `75c13079…`），不属于 012 计费 P1。

生产 EOF 只读诊断 `/tmp/p012-r2-eof-diagnosis.md` 将该机制评为 LIKELY，留存日志没有确切 close/tick 记录。报告把部分其他假设写成“证伪”和给出 10–20% 概率，父不采纳超出观测的绝对排除或统计估计；也不把匹配 guest standalone KVM 当成 baseline 全 production PASS。无需先耗 VM 槽位：先通过实际 Service/真实 SQLite/实际 Manager/存活中继与明确维护 gate 作确定性 RED，再修复。实际组件 RED→GREEN 证明源码回归；具体历史 EOF 仍保留 LIKELY，直至真实验收补证据。

r3 使用上述十项中间基线建立**独立 worktree**，保留所有既有提交，仅提交本项窄修订。最小模型原则：本地 binding 表应表达真正需要维护的代理授权，不能把健康 none activation 自己制造的 bookkeeping 当旧授权并断开 workload。审查所有 bind 生产者与维护消费者，统一 none 的含义；none 没有持久 proxy binding。内部 bookkeeping 撤销不应无条件关闭健康 workload。configure/admission fence、策略收窄、秘密轮换、失败/未知 VM 的实际撤权及连接关闭契约必须保持，不能仅删安全检查换绿色。

新增实际行为组（建议 `TestConfigureEgressNonePolicyKeepsWorkloadConnections`）：真实 Service+SQLite+真实 egress.Manager，明确隔离 Egress proxy 与 workload relay；在 healthy none claim/fork 或等价 activation 之后先打开可传输中继，再显式跨至少两次 reconcile，原连接继续双向传输。证明无第二 activation/no identity reset/deadline renewal/no network-on/no持久none binding；模拟残留代理必须被真实 revoke，同时健康 workload 继续；wide→none、窄策略/secret/fence 前置/后置失败的旧 proxy+旧连接确实关闭继续通过。用 gate/显式 reconcile 和 bounded IO，不能靠短睡眠或概率。优先现有实际 Service/http/agent transport fixture，可用实际 net.Pipe/UDS，不写 helper 单测。若不能无 VM 忠实复现，STOP 附具体缺口，不擅自生产重跑。

Scope 仍是原 007 所列文件；优先 bindings.go、reconcile.go、configure_egress_test.go 的最小更改，必要少量 lifecycle/service 的原范围辅助及文档。无 engine/guest/egress parser/schema/依赖/公共 API 扩域。父管理计划/index，执行者不可改。先 RED 录实际非空测试名，再 GREEN + sandboxd full race/vet/正常 workspace lint/四项既有 source 匹配 Rust fixture Go integration/diff，不运行 Cargo 或全 Engine 未变套件。本次 stage1 guest18 fixture 固定 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-d6927cf8/target/debug/examples/unix_fixture`，SHA256 `ce62e84ee4b0ad6bb825f679811010733e825a21dc84826d1473ae56269b32d9`，核对 guest/protocol/Cargo 与 stage1 相同后用 Go 阶段。无 VM 授权。

## 第四轮：admission直接确认none撤销，不能依赖诊断保存

r3候选8d08ba0a1333df64f99d4c7cc74dc15103dcf7d2（parentdf769…/treed773dfeb5c8c24bc1db090570bf3932179cc6ab8）经新独立审查 SOURCE REVISE，唯一P2见 `/tmp/plan007-r3-review-t3-56da2195/report.md`、`admission_counterexample_test.go`。父已读完整报告/overlay与DataReady完整相关路径。Revoke失败后维护仅写LastError且忽略保存错误；none DataReady跳过bind/revoke，SQLite UPDATE ABORT及RAISE(IGNORE)时仍空LastError，实际新HTTP101 workload可双向传输。真实Manager与生产Service.InstallDirect两层均复现；none NIC一直关闭，未证明旧出网权限可用，不扩大成已证绕过。

在新的隔离worktree FF上述r3候选作为修订起点，保留健康none无持久proxy binding、跨维护不断relay、实际残留proxy撤销及全部configure/fence矩阵。最小增量在现有DataReady的sandbox锁及二次configuration fence检查之后，对none经共享bind/revoke实际确认，错误直接拒绝；安全判定独立于LastError UPDATE成功。实际cleanup成功后可以接入，不得因此关闭健康workload；已持久的其他诊断/显式configure fence按原规则处理，不借本次检查清空fence/LastError。无第二份状态、无新schema/公共API/engine权限。

新增组件回归保留独立overlay原故障：save_ok对照、save_abort、save_ignored，以及生产Service.InstallDirect的network-off成功/network_policy撤销失败+诊断ABORT；断言oldworkload已关，不能在Manager仍有未完成teardown时新HTTP101传输，允许在接入前成功重试真实撤销后接入。增加故障解除/清理后健康旧流不受无故切断，source身份/Deadline/nonce不变、NIC不打开，不靠sleep。原overlay转正式行为测试放configure_egress_test.go，其他范围仍限原007，必要lifecycle.go已在Scope。

先记录精确overlay在r3上的RED→正式测试GREEN，再一次sandboxd fullrace（包含对应guest18既有fixture四actualRust，禁Cargo）、vet/正常workspace lint/diff/来源与连接handler清理。原3新组/9case和七组fence必须实际非空保留；无engine广测或VM。需下一次新的独立source review后才APPROVE。012继续独立实施，不采用未批准007r3；父批准后只交付8d08…与本r4自有增量链，不带整棵stage2/guest018。

## 第五轮：接入与configure intent的线性化边界

r4候选1c1e735e2227fb415a6a3d5db5884de58d3ca7a3独立复核 SOURCE REVISE，新P2见 `/tmp/plan007-r3-r4-review-t3-cb306882/report.md` 与 `concurrent_fence_test.go`。原ABORT/IGNORE+实际InstallDirect P2独立GREEN，健康none修复保留。新实际Service/SQLite/Manager/HTTP gate：DataReady过两次fence后Revoke等待；并发生产Submit(configure)提交pending operation/fence并关闭旧连接；真实cleanup完成后旧admission建立HTTP101并双向传输，fence仍在、NIC关、身份deadline不变。仅证接入时序缺口，不证明旧出网可用。

父已读全部报告/反例及DataReady/Submit/relay/preview相关实际源码。configure Submit持`configuration:S` guard，但DataReady只持S锁；relay在DataReady返回后重新锁S、调用VM/Agent.Connect再track，也不核对fence；preview Forward也存在同一登记边界。因此正式在原Scope上**仅增http.go的这两个最终连接消费者**，并允许已有service.go增加必要短guard/fence+登记共用helper。不要扩endpoint、权限/请求/capability格式或非相关HTTP处理。

修复须为完整顺序模型：DataReady的可能阻塞撤销完成后重查持久fence，返回前最终判定不能依赖旧读取；relay和preview在实际外部Agent.Connect/Forward完成后，使用与configure admission相同的configuration guard做最后fence判定及所有连接登记，或等价严格线性化。fence先提交：拒绝并关闭未交付upstream，不写101、不转发缓冲workload；连接先被合法登记：随后configure的closeConnections必须覆盖它。只在DataReady尾部再读一次而保留relay/preview登记间隙不充分。

保持已有锁顺序S→configuration→ownership/SQL；configure Submit不新加S锁，不能制造倒序。Agent.Info/Activate/Connect/Forward、实际Manager.Revoke与engine等待不得新放进configuration短guard，以便更新的configure仍可及时持久fence。最终guard限本地fence/合法状态复核与登记；HTTP101写/Flush和数据传输在guard外，未交付连接失败都close。需要的post-revoke早期fence检查也使原pendingconfigure gate场景能直接拒绝，不被长持guard调度假死。

先精确原并发overlay RED→GREEN，正式测试至少覆盖：configure在none cleanup gate期间提交；configure在Agent.Connect与Forward gate期间提交但尚未登记；先合法登记再configure确实关闭旧流；healthy none的relay/preview跨维护与重复admission继续，不重置租约/身份。两种顺序均用真实Submit、持久fence、实际连接和bounded channel gate，不以sleep、计数mock或只看DataReady返回值代替。原r3/r4所有故障/恢复/旧七组fence与四Rustfixture保持。

在新隔离worktree FF1c候选实施，自有r5 commit、独立新review，未批准前不交007链给012。全sandboxd race一次（新代码变更合理要求），vet/正常workspace lint/diff/source/cleanup。无VM或engine广测，无DDL/依赖/guest变更。若现有guard关系需要超出精确路径，STOP给具体锁图与source证据，不继续追加无关联特例。

## 第五轮最终独立APPROVE

SOURCE APPROVE/findings=[]：`/tmp/plan007-r3-r4-r5-review-t3-e680e7d7/report.md`，audit同目录。reviewt3-e680e7d7，HEAD2a8e9c084113272b55c9990adf167f5f61807a15/treebfcd2a0515f7ae6074ad3dd40e501057953a39f7，与执行者源一致。父已读全部r3/r4/r5生产增量与482+371+569新增测试、最终完整报告及关键audit，并重核engine/guest/protocol/scripts/依赖无漂移。

独立focusedrace20顶层47sub18.964s、原并发overlay3.696s实际HTTP409、原诊断故障/实际InstallDirect overlay4.414s、最终SQLite fence查询故障组件3case4.560s（拒接/不续租/upstream关闭/不泄漏登记），全部exit0/testskip0/无race；vet/lint0/diff过。执行者唯一fullrace41.746s/138top134sub/四actualRust与原264PASS名保留、620file source/hash/cleanup独立解析核验，未机械重跑全量。新顺序仅短guard内fence+登记，101/Flush/IO在外；实际Go/Gin Hijack仅本地所有权转移已核实，不新阻塞配置intent。

007恢复DONE。已有r1/r2的目标仅接获批纯链8d08ba0a1333df64f99d4c7cc74dc15103dcf7d2→1c1e735e2227fb415a6a3d5db5884de58d3ca7a3→2a8e9c084113272b55c9990adf167f5f61807a15，不接整个stage2/guest018。当前该最终分支含十项批准source基线，engine/guest未改，以前组合门禁与本次变更门禁可精确对应；不代表012/整批或production完成。历史EOF仍LIKELY，当前无VMgrant。

## STOP 与维护

若 Egress.Revoke 无法可靠关闭其拥有的 listener/连接，或错误被当作已关闭证明，先报告接口限制；不通过禁用失败注入、放宽 policy 或清空宿主网络解决。后续增加配置字段时纳入 binding 指纹，并检查所有激活前置返回。单独分支可用 `advisor/007-egress-revocation`；若另获提交授权，使用 `sandboxd: revoke egress before configuration activation`。
