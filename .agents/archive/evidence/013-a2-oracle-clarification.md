# 013 A2外部探针判定条件澄清

父已核实完整原probe、ledgerRestart实现和5c530395的原始失败日志，明确采用本澄清。它只纠正外部审查探针的测试判定条件（oracle），不豁免产品行为，不修改候选源码或旧证据，不需要新的用户许可。

原文件`/tmp/plan013-r2-validator-review-t3-f2a33310/own_test.go`，SHA256 `d3cee324f5e4a761603139470a5bf40f9da1d1d6d2928772d24e867302d5d500`，继续只读保留。原A2在da4为17PASS/4FAIL；新5c530395原文件为19PASS/2FAIL（唯一fallback_reserved/startup-false leaf及父）。完整原日志`/tmp/plan013-a2-fix-t3-2760705d/original-final.jsonl`、*.result.json保持失败等级，不改报21PASS。

## 矛盾及采用判断

原probe52–66行要求runtime返回ManualRecovery且损坏的完整DB、reader、费用、公开bytes保持。68行却无条件调用`ledgerRestart(t,s)`；既有helper在`Open(Initialize=false)`返回任何错误时Fatal。批准R2/A2合同同时要求startup拒绝这份未修复的损坏DB，两项预期不能同时成立。新源实际先返回正确runtime ManualRecovery、state SHA前后相同，随后正确startup ManualRecovery触发这个helper Fatal。父先前将该文件原样全部GREEN作为硬门槛不成立，现明确修正这个验收条件。原helper/候选测试源码/产品拒绝规则都不改。

## 新独立审查探针

新的独立reviewer在自己的`/tmp`证据目录复制原文件并保留原SHA；只替换`TestFocused013StageRequiresDestination`中`if stage == "fallback_reserved"`的错误重开/新release诊断分支。新分支须：

1. 保留分支前runtime错误、完整state/reader/费用/publicbytes全部断言及其原真实数据；不提前return/skip、不弱化预期。
2. 保存`opts := s.opts`，设置Initialize=false，关闭原Service。
3. 调用真实`Open(opts)`，要求typed ManualRecovery；若意外返回非nil Service仍关闭，并判失败。不能用要求Open成功的ledgerRestart。
4. 以`templateQuotaReadOnlyState(t,opts)`读取这份fresh test-owned DB，要求与原before完整bytes相同。读操作不修复、无Engine、无旧生产状态。
5. 不再对一个本应拒绝Open的Service调用recoverResources/Submit release。原来的receipt可见后fresh-release段是为已发生错误接受的旧代码追踪后果，不能成为修复后必须重开成功的产品要求。
6. 保留原分支后的runtime ManualRecovery、实际旧V/B存在、Submit尝试数不变检查；补Find数不变断言及完整state核验。其余graph/Find-time mutation/合法BackupRestoreAck与replacement历史测试不改。

新副本记录独立路径、SHA和准确unified diff，报告明确区分“原字节probe19PASS/2FAIL”和“澄清后probe实际结果”。允许在精确da4 baseline的只读源overlay做仅stage相关actual RED对照，证明修正版仍拒绝旧startup/runtime缺陷；不得机械重跑旧广测。新源修正版完整21节点、原R4及所有原GREEN正对照应实际通过，才能关闭此测试门禁。不能仅凭现有635全绿或执行者声称来认定探针已通过。

## 本澄清后的独立实际结果（原失败不改报）

正确副本`/tmp/plan013-r3-oracle-review-t3-c6984cce/corrected-probe.go` SHA`22bfacf06f9145a3a755fa4f756d93d5452ae7788cd1f1d3fe3187de29060f79`，实际corrected.jsonl40PASS/exit0/27.278783s，其中A2完整21，fail/skip/race0。原da4 stage baseline用完整四路径原blob overlay，base-stage.jsonl1PASS/4FAIL（3leaf+父）、exit1/5.400376s，仍检出旧缺陷。主SOURCE及父分别读完整oracle.diff、核新SHA和直接解析两份raw；它只改错误restart诊断段为startup拒绝与readonly state不变，并补Find0/actualV-B检查，其余测试未改。原文件/helper/原19PASS2FAIL保持。该实际测试矛盾已调和，不是当前停止理由；focused reviewer已完成FOCUS_SOURCE_REVISE并独立收敛同一R5，无其它新阻塞；这些已完成GREEN不构成SOURCE_APPROVE。

主源码审查另发现R5/P2 typed holder缺口，详正式013合同当前R5章节；产品完整资格仍REVISE。旧长TMPDIR三race由新主审对照b93/78/da4/5c同一transport_test.go blob acb8982487ef9c1bf8a4fa6b308d3ddd2408aec0，归类Service.Open前既存fixture启动失败stderr诊断并发读取路径，建议独立测试维护Scope join后读诊断；非本次13新回归/独立阻塞。失败raw仍留存，不宣称所有错误路径安全，不扩四文件修复旧fixture。

## 源码与验证边界

当前候选`5c530395dd0452c98f11ef173e788b1d509650f3`，directparent da4b7d6，tree66d950b749a02e7c757670a9f9b6ce287f13d735，四路径+750/-33，source618 canonicalSHA0f03c0a1e583d82811d52deb8b073aa1e7234954dcd91cb94e4574e41a6beeed。完整实施交接`/tmp/plan013-a2-fix-t3-2760705d/report.md`及handoff/verification/audit/source-proof。

实施者STOPPED是其未获准修改旧oracle时对矛盾的如实报告；本澄清允许新独立审查继续，不等SOURCE_APPROVE或013DONE。父只写plans，review只在新worktree只读+自有/tmp probes，不修改原probe/旧helper/任何候选源码。原A1/R1R2R3/A2/R4所有产品约束、合法历史、完整SQL故障/同物理连接、source/refs/index/fixture/cleanup规则继续有效。

保留首个长TMPDIR全套尝试的exit-15/235PASS/11FAIL/3条DATA RACE输出与123.292s原始收据。父读取race栈为原`integration/transport_test.go:203`在启动错误时bytes.Buffer.String与os/exec stderr writer并发；source未在本次四路径delta改动。新review须评估是否已存fixture失败路径、是否影响当前源码关闭及必要后续交接，不能把race抹去或凭短TMPDIR最终635PASS推断所有失败路径安全。只读比较基线与raw即可先评估，不自行扩大源码Scope或复制失败广测。

最终短TMPDIR full635PASS/343.254267s、原611/509/mapped430/31Ledger/00767/owner矩阵/四Rust和静态门禁仍需独立来源核验；不因本澄清重跑未变343s/Engine195s/Cargo/Rust。后续必须新的独立SOURCE round，携原brief/R2/D1/全部六项发现及响应、本澄清/未决，源码全部关闭后才另发fresh生产grant。当前12DONE/总11DONE、17hold、所有VMgrant false/expired/slotFREE，parent dev不merge/push，root8module并发漂移保留。
