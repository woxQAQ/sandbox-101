# 012 r3 已批准实施合同

本合同是 Plan 012 的必读增补。原目标与账本不变量保持；本合同替换未发布 r2 的内部 Settled 模型，并精确放开下列 engine 路径。用户已授权整个 006–021 批次；设计已独立 APPROVE，不需要再次询问许可。**设计批准不等于源码批准、P1 已关闭或 production PASS。**

父已完整读取设计初稿、首轮 REVISE、完整修订规格及第二轮 APPROVE：`/tmp/plan012-r3-design-proposal.md`、`/tmp/plan012-r3-cold-design-review-dd484b1b.md`、`/tmp/plan012-r3-design-proposal-r2.md`、`/tmp/plan012-r3-cold-design-review-r2-818ffa57.md`。这些报告保留设计论证；本文件与主计划共同给出实施要求。执行者还必须读完整修订规格和批准报告，不能只读摘要。

## 基线与明确依赖

012r2 候选为 `8892e5df8f759a43cea9a1c18f3c1c8a3454061a`，tree `a56cfd1fbf72cd3158f2ffb38e0eeed197f3e3ac`，位于 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f0c098e`。其自身 r1/r2 提交为 `6360636758addc6bca61f99ce762a5eac2456a7a` → `8892e5d…`，前面已含 007/009，禁止重复采用这些副本。

明确接入已批准 010/011 的三个 engine 变化：`270eb47e63bfa28b185b6fc2f88ff2e8b182bcfc` → `7df482995c75a62795eacadde870d82d9e80948d` → `5115a9f00c1003f94c6114457551790aca34fd14`。010 在 011 checkout 的等价副本为 `3741b96793c44b611a68bf68390ac6e991bb6d21` → `0bed9ba0614a2bd039d3759db9ff55d69283302d`，两条链不能重复采用。接入后整个 engine 应与批准 011 的 snapshot 相同，再实施 r3。

007r3–r5已获父确认SOURCE APPROVE/findings=[]，报告 `/tmp/plan007-r3-r4-r5-review-t3-e680e7d7/report.md`。最终012组合必须顺序接获批纯链 `8d08ba0a1333df64f99d4c7cc74dc15103dcf7d2` → `1c1e735e2227fb415a6a3d5db5884de58d3ca7a3` → `2a8e9c084113272b55c9990adf167f5f61807a15`。只取这三个自有增量，不能接其整棵stage2/guest018祖先；007r1/r2已在012r2，不重复。原012r3实现/主审快照2c不被改写，父另建provisional组合，待012源码review后再新任务确认最终组合门禁。

保留 b93 guest/protocol/Cargo 来源及匹配 agent `2463ee2f…7ddb`；本项不改 guest。所有实现和测试在新的隔离 worktree，父只维护 plans。自有 scoped commit 允许，父 dev commit/merge/push、真实数据库/旧运行资源、宿主配置、嵌套委派均禁止。当前无 VM 授权；旧 production grant 已失效。

## 当前必须关闭的反例

r1 capture 的独立 durable allocation/receipt adoption/snapshot cleanup 已修复并须保留。r2 的新 P1 是 compensation restore A→B 成功后回复丢失或真实 SQLite quota INSERT 失败，release 先持久 terminated/新 OperationID 后持旧 A 删除，被正确拒绝；晚 receipt 入账后 parent Done/Settled 却无人承担 B 删除。独立两个反例经过反复 recover/reopen/fresh release 仍 VM1/slot1/storage320/pending0、restore1，原 failed rollback/release bytes 不变。

原始证据 `/tmp/plan012-review-r1-report.md`、`/tmp/plan012-review-r2-report.md`、`/tmp/plan012-review-r2-restore-release_test.go`。保留故障及先后顺序，将 overlay 最终取证的 stranded 断言变为真实收敛规格；不能删故障、跳过恢复或放松公开 bytes/零额外 restore 断言。

## engine 的唯一新合同

`api.Operation` 增加 `ResultInstance string`，JSON 为 `result_instance,omitempty`。它是该成功 operation 自己产生、验证并交付的 candidate instance，restart/adoption 沿用原 candidate；不是查询时 current instance，也不是单独删除授权。

- 新完成的 start/restore/clone 成功必须有精确 witness；pending/running/failed 和其他 action 为空。失败曾形成 candidate 也不能给成功 witness。
- 旧 terminal receipt 的 Get/Find/cache hit/启动扫描不补写字段，body/可观察 bytes 不变。旧成功 receipt 无 witness 的 late path 保留费用与 slot、明确 fail closed，不从 GetVM 猜 B，不迁移历史。
- 新 key start 对已运行 VM 原本是 Conflict；没有需要兼容的无 candidate 成功 no-op。合法 adoption 继续实际 observe/Boot/COW/Agent/observe。
- finish 的成功 generation 事务加载 durable nonterminal op 和 current VM，核对完整 request/journal candidate/IDs、VM.OperationID、非空 Instance、完整 Resource 相等、既有 running/Verified/AgentReady 交付。start/restore Request.VMID 必须匹配；clone Request.VMID 必须空，Operation.VMID 是 engine 分配目标。
- 实际 backend/Agent/材料证明仍在 SQL 外照常执行，不能用元数据替代。事务内不调用 backend。
- 使用隔离可变成员的 staged nextOp/nextVM/lease；同 TX 写最终 identity 确认、status/step/time/witness、owner release 和原 lease 更新，commit 后才发布内存。SQL/commit 失败无半状态，旧指针不能后续 save 成 terminal；重入重读 durable record。已 terminal 不重新 finalize/enrich。
- generation success 的 VM 查询/解码错误必须失败；普通 delete 成功时 VM 已不存在是正常情况，不套 generation 检查。
- 010 RestoreBlocked/BlockedRestoreID/ResumeRequired/RestoreCleaned、lease/owner 保留、候选材料校验和明确 cleanup 全保持。011 execute 例外仍只 start/restore 的同 operation full candidate，不扩大 clone/delete/network 白名单。

## 一份持久责任状态，两种临时推进判断

删除内部 `recoveryAllocation.Settled` 及全部读写/过滤/双写/fallback/r2格式转换。未发布内部字段无需 BC；不写 r2 真实 DB 升级测试。公开旧 terminal receipt 的不可变是另一回事，必须保留。

allocation 继续冻结完整 keyed Request、OperationID/Step、Shape、TenantID/ChargeTenantID、S。新增唯一 Resolution：State 为 unresolved/result_known/active/cleanup/reclaimed/cancelled，保存 exact receipt proof/结果 V 或 SID/B，以及具体 destination 或回收/确定未 dispatch 的证明。费用已采用或写 r.Instance=B 不等于责任交付。

resolver 返回非持久 Progress，明确区分 AllowedLifecycleWork 与 AllowedParentDone：

| durable 状态 | 允许工作 | parent Done |
|---|---|---|
| 未知 receipt/query error/identity 不明 | 等待，保留 pending/slot/依赖 | 否 |
| result_known，原 lifecycle 仍合法拥有 S/V/B | 携 exact B activation，不能重 restore | active TX 前否 |
| result_known，S terminated | 只交接唯一 B cleanup，不 activation | handoff 前否 |
| 已提交 active/cleanup destination | 幂等收尾 | 满足 parent 自身步骤后可 |
| reclaimed/确定未 dispatch cancelled | 不重建资源 | 可 |
| foreign W/C、unverified、other owner、proof mismatch | fail closed，不猜实例或新 key | 无其他已交付证明则否 |

`recoverSandboxResource` 入口删除通用 `!settled → return`，让 result_known+AllowedLifecycleWork 到达 exact-B recoverRollback。结束 `finishRecoveryAttempt` 仅消费 AllowedParentDone；不得由旧内存 r 覆盖刚写入 Resolution/Attempt。扫描按未交付责任/instance_cleanup 调度，不依赖当前 sandbox.OperationID 或 parent.Done；没有旧 r2 backfill。

snapshot 沿用 r2 durable owner/receipt/独立 cleanup destination，同样先 TX 交接后 parent Done。accepted failed restore 只有 VMID 无成功 witness 时，费用交给已验证的存量 V 责任；这不授权猜未知 candidate。没有验证 destination 就保守等待，不退款。

## 原 A 意图与唯一 B collector

首次 release 在任何 A delete dispatch 前，同 TX 保存 terminated/原 cleanup/slot 转交及 ReleaseIntent：可信 tenant/S/V/A、原 Source/request identity/charge owner、完整原 A delete request/key/step。dispatch marker 也先持久；失败不调用 engine。**不可在 b.Instance 后来变 B 后重建原 A 请求。**

B collector 用现有 resource_recovery PK/body，新内部 Kind `instance_cleanup`。ID 为 `instance-cleanup:` 加 SHA256：对固定域 `plan012-instance-cleanup-v1`、可信 tenant、S、V、B 各 UTF-8 字段作固定宽度 length-prefix 后哈希。body 保存完整 tuple，PK 碰撞必须逐字段核验，不盲 upsert、不生成替代随机 ID。采用标准库，无 DDL/新表。

collector 固定原 failed release Source、A intent、exact successful A→B receipt proof/shape/charge owner、独立 B delete request、当前 attempt marker/receipt/不可变历史与费用/ref identities。B key 使用现有 `recovery:<collectorID>:<Attempt>` synthetic operation 和 release step 的 stepKey。fresh public release 不改变 collector ID/Source/key/Attempt；从不创建新 restore key。

创建/消费资格：trusted owner 一致，S terminated、仍 V、metadata 仅 A 或已接管 B，原 failed release 确属 S/V，完整 request-matching 成功 receipt 固定 B，当前 verified V/B 无其他在途 owner（或已有精确完成证明），无实际 active/dependency 使用者。active S、不同 W、已发布 C、未知/错误查询不接管。

release/background/discovery 先查 frozen intent/collector，早于普通 !owned→Done、普通 reference 阻挡、generic orphan。独立 Kind 明确分派，不落入原默认 orphan。fresh release 改 b.OperationID 不撤销已验证 terminated V/B continuation。

T-handoff：锁顺序 sandbox/checkpoint/pool → resource ownership guard → SQL；engine proof 查询在 SQL/guard 外。TX 内重读校验，原子费用采用或核对 result_known、deterministic collector、cleanup refs/slot 责任、Resolution.cleanup+destination。失败全部回滚。若费用先采用，必须同 TX 留 result_known/descriptor refs，不能先 Done。可把内部 Instance A 更新 B，但仍 terminated，public body 不改。

仅排除该 collector 自身、该 terminated S 的 cleanup 责任及明确转交的重复 cleanup refs；其他 active_use/engine_dependency/warm/pending 使用者继续阻挡。fresh pending release 的派生 root 只有核验是同 binding 的纯 cleanup continuation 后才归为重复 cleanup；不能略过该 op 的真实 engine dependency。restore 版本 key `V:restore:<receiptID>` 经可信 vm:V holder 映射物理 V，不当 VMID。VM→SID backing 不应自阻挡 V 删除，也不提前退款。

### key 与普通 A 失败规则

- A unknown/nonterminal：查询/重放原 request/key。Find NotFound after dispatch 不证明未调用；query error 等待。
- 普通同实例 A 清理无 A→B proof：保留原 sandbox recovery。exact A terminal failed 且原 A 仍有合法证明，可按既有 synthetic compensation/attempt 规则继续；不因冻结而永远重放已失败 public key，fresh release 不开启第二序列。
- A stale pre-admission Conflict 无 receipt：只有 exact frozen A + immutable successful restore→B + 当前 verified B + 无正面 nonterminal/矛盾 receipt，并依 engine admission/execute 的 instance fence，才证明 A 请求不能删除 B，从而交接独立 B。不是伪造 failed receipt，也不是拿 NotFound 当未 dispatch。
- B unknown/nonterminal/response lost/query error/dispatch 后 Find NotFound：同 key 查询/replay，不换 Attempt。仅 exact terminal failed receipt 才一个 TX 保存旧请求/失败历史、Attempt+1、冻结新 B 请求；TX 失败保留旧 attempt。pre-admission 不明错误不能无界换 key。
- 先对账当前 attempt，再检查下一次删除引用，避免自己在途请求使查询永远卡住。

## exact-B active 交付与回收

同步成功 response 和 late Find receipt 经同一 resolver。替换 recoverRollback 的 sameVMID heuristic，已知 B 不重复 restore。只对受影响 compensation 引入内部 exact-target activation helper，普通调用保持原业务语义；重读 VM、network-off/on、Agent 对象及最后 publication 都固定 V/B，看到 C 停止。保留 007 configuration/publication fence；不得先操作 C 再拿新 network receipt 补 R→B。

T-active 无论 SnapshotID 是否空，都在一个 Service TX 保存 S running/V/B、active holder、必要 retired snapshot handoff、Resolution.active+精确历史 destination。commit 后更新内存。内部 Resolution 写失败整个 publication rollback；外部 Agent 可能已执行但 durable 仍 result_known。提交后 parent Done/收尾失败则认历史 active destination，不再 activate/restore；若正常后继已到 C，仅收尾不覆盖 C。跨 engine/Service 不声称分布式事务，不新增锁 endpoint。

T-reclaim 只在可信 delete/当前不存在证明后，原子清 V 全部独立 D、准确 backing refs、可退休 slot、collector 完成证明、相关 Resolution 与仍匹配的 terminated metadata。独立 CP/template active SID 保留；catalog NotFound 而 backing 在不退款。query 到 C 不借旧 B receipt 清 C 费用/metadata。物理删除成功但 ledger 写失败，重启查同 key 后只收账本，物理 B delete 一次；旧 receipt 重扫不重建已回收 D。

## 精确 r3 Scope 例外

只允许这些 engine 文件：`engine/api/types.go`（唯一字段），`engine/operations.go`（finish 验证/事务/staged/必要窄 fault hook），新增 `engine/result_instance_test.go`（actual Engine 行为），`engine/engine_test.go`（必要现有断言/fixture），`engine/INTEGRATION.md`（字段/旧 receipt/mixed-version）。

原 sandboxd Scope 保持；r3 主体限定 `recovery.go`、`quota.go`、`lifecycle.go`、必要 `service.go`、`quota_recovery_test.go`、`service_test.go`、`recovery_test.go`、`README.md`，内部类型确需可在 `types.go`。其他原列文件只作明确受影响原 fixture/计费文档，不一般重构。

禁止 engine.go admission、backend/runtime/launcher、guest/protocol/egress/GC、SQL DDL/新表/schema 升级、依赖/锁文件、新 public request/endpoint、真实迁移、无关授权重写。原 schema4→5 拒绝保持，JSON body 新内部状态不再升 schema。超出路径须带具体源码证据 STOP，不能自行套“必要其他”。

## 实施顺序与必须证据

先写 actual Engine witness 缺失 RED 和原 release-first 两故障 RED，再实现生产 witness、严格 fixtures、单一 Resolution/collector/active/reclaim。行为通过公开 Engine Submit/Wait/Get/Find/reopen、实际 Service/SQLite/真实故障与 physical effect 断言，不单测 helper，不只给 fake 填字段。

1. Engine 至少五组：Lifecycle（start/restore/clone 精确 witness、其他 action 空）、AtomicCompletion（VM/terminal/lease/commit 故障重启不半状态/不重复 effect）、RejectsForeignCandidate（owner/VMID/instance/full Resource）、SurvivesRestartAndNewerInstance（B→C→delete 后原 receipt bytes 固定）、FailureAndLegacy（actual Inspect/Agent/material failure 无 witness，010 fence 保留，旧 public receipt 不 enrich）。011 原 strict fence/010 材料全保留。
2. Service 两 fault（transport/真实 SQLite charge INSERT）× release-first/recover-first；原 recover4/reopen/recover4/fresh release 顺序保留。result_known 真到 active，release-first 只 cleanup；restore effect1、B physical delete1，原所有 failed public bodies bytes 固定。
3. B before-dispatch、lost-reply、post-delete-ledger-failure 三边界各两 fault：fresh release R2→background→reopen→R3→background，始终一个 target collector/unknown key，Source/Attempt 不被 fresh ID 改写。原 A key/参数不改，新 claim 在清理未确认时引擎副作用前 Exhausted。
4. adoption/collector INSERT/refs/Resolution/parent Done/T-reclaim 的真实 SQLite fault；activation 发布前/内部 Resolution UPDATE/发布后收尾故障及后继 C；foreign W/C 在采用、activation 重读、Agent 前、delete admission 前；query error/reopen、missing witness、unverified/other owner、key mismatch 全保守。
5. 普通 A terminal-failed/reopen 补偿保持；unknown A/B、B terminal failure、A pre-admission 无 receipt 分别检验 key/Attempt。cleanup-only 可推进，真正 active/dependency 仍阻挡。root Bob 请求操作 Alice 仅 Alice 计费，请求 principal/key namespace 不改。
6. 原 18 Ledger/capture 两反例及所有权限/模板/retired/shared/backup/schema/并发回归保留。fixture witness 在真实生成成功时与 receipt 固定，Find/Get/cache hit 不看 current VM 回填，同 key 不同参数 Conflict。accepted failed restore 保留实际 VMID/no witness。
7. D64/M128 旧注入反例原盘64+成功恢复盘64+SID192=320；忠实 accepted-failure 另有失败保守 D64，共384（383 拒绝，零副作用），逐 receipt/resource/ref/item 列明，不为固定320删 VMID。保留64/63、256/255、320/319、448/447。B 删除后 CP 独立存在只剩192/slot0，CP可信删除后0，新 claim 可成功；未知和 backing 存在不退款，不涨预算。

focused 新 RED→GREEN 后，**一次**完整 required-native engine race，逐项 JSON 核对新增5组、010五组、011四组、20 TestActual 及原全 suite PASS/测试级 skip0；不机械重复 focused01011race/fullnonrace。新失败/修改/未解疑点才补测。timeout240s 是新增覆盖预算，原批准 Engine 已约176s。sandboxd 新矩阵 focused 后一次全 race，所有四个真实 Rust fixture 必须执行，vet 两 module、正常 go.work lint、diff/scope/source preservation 必须过。

未来隔离 executor 的 Nushell gate（逐个检查 LAST_EXIT_CODE，保存完整 JSON/exit/wall/实际子场景，不能只检查末项）：

```nu
with-env {GOWORK: 'off', GOPROXY: 'off', GOSUMDB: 'off', GOTOOLCHAIN: 'local', GOFLAGS: '-mod=readonly', SANDBOX_REQUIRE_EROFS_TESTS: '1'} {
    ^go -C engine test -race ./... -json -count=1 -timeout=240s
    if $env.LAST_EXIT_CODE != 0 { error make {msg: 'engine native race failed'} }
    ^go -C engine vet ./...
    if $env.LAST_EXIT_CODE != 0 { error make {msg: 'engine vet failed'} }
}
# sandboxd 全 race 前必须设置已核对 source/binary 的 SANDBOX_AGENT_FIXTURE。
with-env {GOWORK: 'off', GOPROXY: 'off', GOSUMDB: 'off', GOTOOLCHAIN: 'local', GOFLAGS: '-mod=readonly'} {
    ^go -C sandboxd test -race ./... -json -count=1 -timeout=180s
    if $env.LAST_EXIT_CODE != 0 { error make {msg: 'sandboxd race failed'} }
    ^go -C sandboxd vet ./...
    if $env.LAST_EXIT_CODE != 0 { error make {msg: 'sandboxd vet failed'} }
}
with-env {GOPROXY: 'off', GOSUMDB: 'off', GOTOOLCHAIN: 'local'} {
    hide-env -i GOWORK
    ^make lint
    if $env.LAST_EXIT_CODE != 0 { error make {msg: 'workspace lint failed'} }
}
^git diff --check
```

native cached closure 只读 `/nix/store/ppifd9prd8f7a4kkbrq5m50ss153r4jr-sandbox-kvm-acceptance-userspace`，从已存在 bin 链取 erofs/umoci/bubblewrap/util-linux/e2fsprogs 等，设置 PATH 并记录版本；缺工具 BLOCKED，不 build/fetch。Go 本项目编译允许，模块与工具必须已缓存；禁 Nix develop/install/fetch/依赖构建。

b93 guest 对应既有 fixture `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-5f5c61c7/target/debug/examples/unix_fixture`，历史 SHA `d7fc857eb67e7f651a2857401bf677cc03117eff8a4f03baf272bf14802d41e9`，未来先重核 guest/protocol/Cargo 逐文件与 binary 前后哈希；不得用 guest018 fixture 替代。只跑四项实际 Go integration，不 Cargo、不 make integration 隐含重建。四项 TestRust 必须 PASS/零 skip。

## 交付与最终验收

完整报告提供 parent/head/tree/准确所有依赖映射、自有 scoped commits、逐状态事务/资源金额/真实 RED→GREEN/全部非空子场景/每门禁 exit-wall/源码与 index/预存 refs 保留/自有进程连接挂载清理。原输入、source hashes、公有终态不改，证据保留。

随后必须新的独立 source review。当前 VM slot FREE、旧 grant expired；本合同不授权生产运行。最终精确组合含批准007r3后，父另交新 grant/新 owned artifacts，原完整 production lifecycle/golden/warm/真实 TCP 必须全部执行。历史 r1 incompatible-agent 已由匹配材料关闭；r2 fork EOF 的007机制仍是独立验收事项。任何设计/本地源码门禁/旧 standalone KVM 都不能替代 production，012 仍 BLOCKED，013 不放行。
