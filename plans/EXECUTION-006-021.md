# 006–021 执行与复核记录

**后续[022职责/API与测试单元重整](022-responsibility-and-api-boundaries.md)已DONE，统一目标`t3/final-twelve-source-combination`已由原owner唯一FF至33a3/tree56c1，独立SOURCE_APPROVE、新Engine448/SB678/protocol3完整race及父678source/index最终核验通过。见[交付](022-RESULT.md)/[最终接受](evidence/022-final-acceptance.json)，当前工作已完成并停止。下方FINAL12_COMBINEDPASS绑定旧47f5历史基线，旧VM/production/Rust49不转为022新成绩；016/017/020/021不续做。**

**最终12项组合已由父接受：FINAL12_COMBINEDPASS。统一review分支`t3/final-twelve-source-combination`，HEAD47f5ce71bfa8de2720e184fec516f0e27ca90f2d/tree3e8cc8a12cb70dd36fffc428f9aff83d8712fe73；SOURCE_APPROVE/65focused、完整sandboxd race678PASS/旧644保留、fresh Rust49、匹配新guest/OCI的双格式KVM和唯一完整production5RUN5PASS/141.101s均通过。13actualmanagedVMM的全部liveguard、109PID/26PGID及unit/cgroups/mount清理已父独立核验；所有grant已失效，slot FREE。见[最终报告](FINAL-COMBINATION-12-RESULT.md)、[最终接受收据](evidence/final12-47f5-final-acceptance.json)。原KVM材料汇总NameError STOPPED与后续独立只读post接受分开保留，无VM重跑。**

**用户“做最终组合吧”的范围现已完成并停止。016/017/020/021及无关fixture维护仍暂停；parent dev b93未合并/推送，原用户改动hash保全。历史013或8/10项PASS仍各自绑定旧树，不替代本次新实际验收。**

基准：`b93fc24`。用户已授权落实全部计划。父代理遵循 improve execute：源码由独立 worktree 中的执行代理修改并提交，父代理只读审查、在隔离工作区重跑门禁、维护 `plans/`。派发或执行者自报不等于通过，只有完成 Scope/diff/行为测试与必要验收复核才标 DONE。

根 checkout `/home/woxQAQ/sandbox-101` 的 `dev` 分支不由代理合并/提交。现有 001–005 归档、`deploy/ACCEPTANCE.md` 链接调整及其他用户改动保留。006–021 文档尚未提交，每次实施派发均内联完整计划；独立 review 可以读取明确交付的父目录绝对计划路径。依赖只在前置复核通过后启动；依赖提交只接入执行者的隔离分支。

## 已复核通过

| 计划 | 最终分支 / worktree | 采用提交 | 复核结论 |
|---|---|---|---|
| 006 | `t3/bound-connect-inner-http-parsing` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-84acdd1c` | `6d490ea850f0a51a25037265e547893a8dc41654` | APPROVE，隔离分支 DONE |
| 007 | `t3/fix-egress-admission-fence-race` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-cdc262ce` | r1/r2 `b45f1b0`→`75c13079…`；追加 `8d08ba0…`→`1c1e735…`→`2a8e9c084113272b55c9990adf167f5f61807a15` | r5新独立SOURCE APPROVE/findings=[]，隔离DONE |
| 008 | `t3/fifo-rpc-test-doc-revisions` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-2b480da4` | `0542708` → `fba80816a4a38fcb049c64bd2504106fe24462a1` | 第二轮 APPROVE，隔离分支 DONE |
| 009 | `t3/fix-template-admission-invariants` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-eac74176` | 009自身 `6521ef6` → `c3c21abe136062c72cbcaf520f2bc1f987ff0ef8`；最终worktree内r1副本为`86046a2` | 第二轮 APPROVE，隔离分支 DONE |
| 012 | `t3/verify-production-owner-budget78` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-3c237f22` | 产品最终组合`90f68b6`，fixture自身`78bc04fc8715d18319ed4cba69aa92a30975f9e5` | 产品及fixture独立SOURCE APPROVE；原完整production 5RUN/5PASS、清理释放，隔离DONE |
| 013 | `t3/plan013-production-acceptance` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-68d5daed` | 最终`1414b4e376a70fec9ed9f16b30db588cd38c4b88`，feature chain d3→da4→5c→1414 | 双SOURCE_APPROVE/七项关闭；唯一完整production5RUN5PASS、13VMM guards、89PID/26PGID清理释放，DONE后STOP |
| 014 | `t3/sdk-queued-call-cancellation` / `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-833f0eda` | `579054ccbd735fc6ae80a673471d0e8c3e081a4d` | APPROVE，隔离分支 DONE |

父 reviewer 已完整阅读三项生产增量与新增测试，独立复跑必需门禁：

- 006：egress race 27.178s、sandboxd vet、lint 0 issues、实际 Rust fixture integration 0.704s、diff check 均 exit 0；有限 header、持续慢 header、1.5 MiB/超过 10s 的 body、复用、SNI/authority、撤权与半关闭均覆盖。
- 008：首轮要求改为 130 次交替 FIFO 拒绝，并分开 rescue 与断连阶段、join server task、收窄设备文件及时拒绝承诺。第二轮完成后父 reviewer 再跑 Rust workspace 30 tests、fmt/clippy、真实 fixture integration 1.085s、diff check 全通过。原 `abb2bc0` 已 cherry-pick 为最终分支的 `0542708`，组合时不要重复应用。
- 014：五个真实 net.Pipe/帧行为测试及源码检查通过；父 reviewer 跑 sdk/agent/integration race（sdk 2.077s）、vet、lint、实际 fixture integration 0.689s、diff check 全通过。取消回调 stop=false 必须等 Close 收尾后交还准入，已关闭流不复用；不承诺强制取消调用方 consume。

完整交接见各计划新增的“实施与复核结果”。015采用自身`583a9f719047f38453d91c946862b8a80d466740`、019采用自身`314c4f7e1599220c3a0b6699bd77f9d1ab9c5533`，均已APPROVE。018采用自身`6c090e81160e59e75280c79ca9f4041d8c0c2343`，现已APPROVE；继承008/015/019副本不重复取。当前共十二项DONE（007r5、010/011批准维持；012精确78及013精确1414完整production已复核通过）；013全部源码/生产/清理及证据完成后当时按要求结束。用户现另行授权汇总这12项，组合尚待验收；其它未完成计划继续暂停。

## 已派发 round 台账

Task ID 早期前缀（后续轮若明示新前缀，以工具实际返回为准）：

`node:delegated-task:command%3Amcp%3A7adb67e5-0df2-4799-8753-2a9b509bf62c%3Adelegate-task%3A`

后接 request ID 即完整 taskId。每个修订/复核 round 独立派发，不通过 backing childThreadId 启动新复核。路径稳定，分支可能由 T3 自动重命名，以最终报告/refs inventory 为准。

| request | 模型 | worktree（`/home/woxQAQ/.t3/worktrees/sandbox-101/` 下） | 当前结果 |
|---|---|---|---|
| implement-plan006-r1 | Codex high | `t3-84acdd1c` | COMPLETE → 父 APPROVE |
| implement-plan007-r1 | K3 high | `t3-0664710c` | `e3f1d0e033fbf1eb693694d4eddad8b861171959`，父 REVISE |
| implement-plan007-r2 | Codex high | `t3-967b8b47` | COMPLETE → 父 APPROVE；最终 `b45f1b0`→`75c13079f3d3515612b890520b25c9b20108d076` |
| implement-plan008-r1 | K3 high | `t3-397d9808` | `abb2bc0`，已由 r2 取代；原分支 `advisor/008-fifo-rpc` |
| implement-plan008-r2 | K3 high | `t3-2b480da4` | COMPLETE → 父 APPROVE |
| implement-plan010-r1 | Codex high | `t3-fd97d849` | `2226e2859e2a8416069e91ec7dfdcecbbf2492fb`，完整 KVM 因容量 BLOCKED |
| review-plan010-r1 | Codex high | `t3-65f178fd` | 完成：两项生命周期overlay RED，已交r2修订；KVM存储BLOCKED |
| implement-plan014-r1 | K3 high | `t3-833f0eda` | COMPLETE → 父 APPROVE |
| implement-plan015-r1 | K3 high | `t3-d8a72b8e` | COMPLETE → 父 APPROVE；分支 `t3/preserve-schily-xattr-names`，本项 `583a9f719047f38453d91c946862b8a80d466740` |
| implement-plan016-r1 | K3 high | `t3-cc3c5c51` | 已由r2取代；源码复核需修正门禁解析/身份/日志；真实CI仍缺 |
| implement-plan019-r1 | Codex high | `t3-a9563b37` | COMPLETE → 父 APPROVE（结构性调查限定）；`314c4f7e1599220c3a0b6699bd77f9d1ab9c5533` |
| implement-plan020-r1 | K3 high | `t3-ec03cbba` | `d0db0538fbc6220e4d8049eb774faced0ed8e7c9`，父 REVISE |
| implement-plan020-r2 | K3 high | `t3-59a41a19` | COMPLETE：`3491768`→`11dab38`；父正向/selfcheck已重跑，最终来源复核待整合阶段 |
| implement-plan021-r1 | K3 high | `t3-0ef09f8a` | `5c730a515a16fe8c9eb7c16944f040242512b1cb`，父 REVISE |
| implement-plan021-r2 | K3 high | `t3-d5649764` | COMPLETE：`60f506d`→`b11ad97`；父正向/selfcheck已重跑，最终 provenance 复核待整合阶段 |

未派发：011、013、017；012已在009第二轮APPROVE后派发。硬依赖仍为 `007→009→012→013→017`、`010→011`、`019→018`。子任务完成不自动解除依赖，先复核；最新 round 与剩余发现见文末续记。

## 历轮复核发现与处理

以下保存首轮发现，当前结论以各计划正文及本记录最新续记为准；007/009已关闭，020/021仅最终来源映射待定。

### 007（首轮发现已在r2关闭）

父 reviewer 实际重跑 r1 sandboxd 全 race（28.266s）、vet/lint/真实 integration（0.689s）通过，但源码仍有：

1. configure activate 成功后另开最终保存/清 fence 事务，后者失败没有撤销新 binding。需真实 SQLite finalization 失败注入和 UDS 关闭断言。
2. reconcile 仍只用 map 非空判 bound；所谓指纹不符测试直接调用 bind，绕过维护。需真正维护入口核对有效配置、保留身份/租期。
3. `!v.Verified` 在 fence 检查前 continue，可能留下服务自有代理；未知实例不能改 NIC，但应撤销自己拥有的流/listener。
4. 新 configure admission 可在旧 worker 被 Info/Activate gate 卡住时替换 fence。旧操作必须不能晚到发布、把零行条件 DELETE 视为成功，或在新操作已清 fence 后再次执行。最终交付/操作终态/fence 解除的崩溃边界需一致，真实 channel-gated Service 并发验证，不能只测 CRUD helper。

### 020（r1发现，r2已修订；待最终来源映射）

父已读全部交付物并跑 normal/selfcheck（282 series/11 scenarios）通过，额外发现：

- reaper_backlog expected 有重复 JSON metric key，标准解析静默丢 reap 观测；严格 Python object-pairs 校验已实际拒绝。应改 observations 列表，自然容纳同 metric 多 label，补齐 phase 观测。
- warm-rejected/fallback 失败未完整映射，counter 的 attempt/completion 口径需清晰；不引入无界 operation-ID 去重记录。
- 把真实 Rust **guest agent** fixture 写成 Rust engine，测量权威错误；需据真实 transport_test/Makefile 修正，并写具体版本/shape 前提。
- 合并维度 1440 的论证有不存在的轴；正确比较为包含 total 的 phase4×source4×result2×15=480，加其余72=552>512；现有拆分282正确。
- reversed-bucket selfcheck 同时把 series 写错成15，可能因错误理由拒绝；改为正确5并断言 bucket 类错误。声明 root series_total 应与计算一致。

### 021（r1发现，r2已修订；待最终来源映射）

父读完整合同/校验器/设计并跑 normal/selfcheck 通过；额外在内存调用同一 validate-scenarios，实际观察两种 ZERO errors 假接受：

- S02 engine_reachable=false，engine VM subject 被改成 confirmed/verified true、删 unknown/reason，添加 absent=true。不能仅相信输出 status，应依有限 adapter/provenance 核对失败输入；不能因此误伤健康 sandbox 或离线 backup。
- S08 schema3 不在支持[2]，改输出 schema_supported=true 并删 schema_unsupported reason 被接受。应依据输入版本集合判定。
- 字段白名单目前把所有 section 合并，不能检测错层字段；verified 也应是 boolean。
- quota 来源表漏掉 baseline 已有 checkpoints、retired_snapshot resource_recovery 去重责任与 pending/running ReservedMiB，只写 tenants+sandboxes 不准确；需补齐，未来持久 quota/work 表仍明确不存在。

## 验收材料与资源交接

只读复用已核实材料，不改旧材料/旧验收状态：

- Receipt：`build/runtime/acceptance-materials/run.rVAvN3Ca/materials.json`；完整路径根为 `/home/woxQAQ/sandbox-101/`。
- CH commit `a02b114bd42c65b73033231a27bd23f3c487fc4c`，SHA256 `0179f0fc96fb34c801ef689856c522556d41c905bfe5187f082f31c248003ed7`；kernel 6.18.56。
- Guest：根 checkout `build/guest/`，只供010这类未改 guest 的验收；涉及 guest 代码修改必须重建匹配产物。
- 已有 native closure：`/nix/store/ppifd9prd8f7a4kkbrq5m50ss153r4jr-sandbox-kvm-acceptance-userspace`，其 erofs/umoci/bubblewrap/util-linux/e2fsprogs 的 bin 可入 PATH；systemd/shell/coreutils/attr/acl 供 KVM fixture 使用，无需安装。
- UID1000 可读写 `/dev/kvm`、`/dev/fuse`；bindfs/fusermount3 已存在。

010 r1 原生 required race 实际122.294s、其他门禁通过；全套 KVM 为 layered PASS、merged import 空间不足（free7941894144/request9442689024），完整 make exit2。只作为先前证据，不冒充全通过。报告 `/tmp/plan010-handoff.md`，原生 `/tmp/plan010-race-final.log`，KVM `/tmp/plan010-kvm-2.log`；旧 artifact `/dev/shm/plan010-kvm.zSDzbe4L/` 保留未删。

父已在010 worktree的全新 ignored build scratch 运行自有 FUSE 前提探针：`bindfs -f --no-allow-other` 映射空 Btrfs source 到自有 view，UID1000 下 stat.dev/inode 与 `/proc/self/maps` 一致、hardlink 同 inode、MAP_PRIVATE 写不改 backing、file/directory fsync 均通过，空闲297.1GiB；probe 正常卸载且只清理其自建临时文件。此为前提，不是 VM 验收。

**此段为旧 r1 资源交接，已结束**：`review-plan010-r1` 实际发现 plain-bindfs 的 readonly-FD truncate 兼容障碍，报告后已清理自有挂载并释放槽位。当前槽位与兼容存储交接见下方最新续记。

## 后续组合与收尾

- 父仅维护计划，不实现源码或合并用户分支。待各项 APPROVE 后，由隔离 executor 组合已批准提交、解决 Scope 内冲突并跑整体门禁；重复 cherry-pick 的008祖先只取一条，015只取其自身 scope commit，019同理。
- 020/021 设计完成后仍需在最终整合的007/012/013/017真实源码上重核 source/schema 映射；不能把 baseline 不存在的概念写成已实现，也不能把设计交付当生产 instrumentation/diagnostic API。
- 必需原生/双 UID/KVM/production/CI 验收缺失 ⇒ BLOCKED，明确差哪一项；没有推送/宿主变更授权时不自行操作。
- 独立 gate 已通过后只因新修改/冲突/未解决风险而重跑，避免无意义重复。最终保持 dev 用户改动不丢失，交付可审阅分支/提交、证据和确切剩余条件。

## 最新续记：已放行006/007/008/014/015/019

007 第二轮源码/测试增量已完整读取，父跑 sandboxd 全race31.867s、vet/lint、实际fixture integration0.704s全通过，四项发现关闭；009可推进。015父读全部自身三文件diff并跑Rust32tests/fmt/clippy/actualintegration0.703s通过，只接其583a9f7，不重复008祖先。019父读全部生产/feature/580行fixture/runner/证据并实际跑required3PASS0.25s、默认30tests及featuretree8/clippy、fmt、真实integration0.701s/语法/diff全通过，限定未复现泄露。三项计划正文与索引已记录APPROVE。

新增 round（完整 taskId 仍用上方前缀 + request）：

| request | 模型 | worktree 后缀 | 当前结论 |
|---|---|---|---|
| implement-plan009-r1 | K3 high | `t3-eba7dedf` | `0b73604`→`6f1d9c4`（007）→`6521ef6e2eb437c4efc1da8e92289d6dd428e310`；父REVISE |
| implement-plan009-r2 | Codex high | `t3-eac74176` | `c3c21ab`，第二轮父APPROVE，全部门禁通过 |
| implement-plan010-r2 | Codex high | `t3-2c14242b` | `270eb47e63bfa28b185b6fc2f88ff2e8b182bcfc`→`7df482995c75a62795eacadde870d82d9e80948d`；源码修正COMPLETE，KVM待复审 |
| review-plan010-r2 | Codex high | `t3-0392241e` | 源码APPROVE；required native race128.940s零skip，完整KVM copy_file_range EACCES仍BLOCK，槽位已释放 |
| implement-plan016-r2 | K3 high | `t3-166d2501` | `9439544`→`baa5808`；`t3/fix-native-gate-validation`，COMPLETE但CI/root/Nix shell门禁BLOCKED |
| review-plan016-r1 | K3 high | `t3-97e4fc7a` | 源码APPROVE，actual local正/负/拒绝门禁通过；整体CI/root/Nix shell仍BLOCKED |
| implement-plan018-r1 | Codex high | `t3-b0631c43` | `6c090e81160e59e75280c79ca9f4041d8c0c2343`，父APPROVE；actual7场景及全部Rust/integration门禁通过 |

### 009首轮发现（第二轮已关闭）

父已读r1完整生产增量与557行行为测试，独立重跑全race35.163s、vet/lint、actualintegration0.732s/diff全通过，但原计划三项未满足，已完整交r2：

- 发布锁复核已更正：r1完整函数已有`lockResourceOwnership`，父此前仅读diff上下文而误报缺失。实际问题是该锁覆盖`Engine.GetSnapshot`；r2已将外部查询移出guard，并在guard/SQLite事务内校验snapshot owner和发布。新增实际Service门禁证明engine阻塞时ownership可取得、持锁时publication不可推进。父需继续核对新源码/测试和gates。
- 已存在intent时忽略publishedTemplate.TenantID与owner冲突；missing placeholder又从ready row推断重建owner，这是未声明内部兼容/责任猜测。schema4应缺失/冲突failclosed，不改变原行/发布；外租户仍NotFound。
- 仅查current pointer的status，并把所有非pending/running都当终态；需同tenant/template/action关联，只有succeeded/failed可回收，缺失/异常/错关联ManualRecovery。补root显式target权限和真实Submit断言。

### 010独立审查与存储修正

review010-r1报告 `/tmp/p010-review-r1.HuyzzS/report.md`；scope/tree与原候选相同，native race128.197s零skip、原回归/vet/lint全通过，但两项生命周期overlay实际RED：

1. InspectAll再StopAll与生产Inspect拒同VM其他live scope冲突，已知候选仍活时delete永远卡住。
2. 原restore清理后，旧failed delete的ManualRecovery仍阻断全局GC，需关联原清理证明，保留两份公开失败结果。

r2仅六个允许文件修正：先凭自校验Stop处理全部已登记实例，再Inspect全部，全部证明后才network/remove；GC按关联restore/VM/source/receipt/lease/VM退休证明解开旧delete内部责任。测试含known多实例/unknown sibling/unknownidentity和未解决/跨VM/无关ManualRecovery；overlay对r1 RED，最终focus50.217s、required race136.459s零skip、vet/lint/diff过。等待review010-r2，不提前DONE或派011。

存储事实：plain bindfs/Btrfs有容量且stat/maps等合格，但0400创建的O_RDWR FD ftruncate失败EACCES，完整两格式KVM因此在import失败；独立report记录，所有自有挂载已正常卸载。父另试new user/mount namespace private24GiB tmpfs+外部bindfs，仍PermissionError；helper/bindfs退出0并清理，仅为失败前提探针。

父用只读offline `nix eval`查询候选包，发现**已缓存** `/nix/store/7jhqs8y79f85sy0pcvkgd4mplakdxwnr-fuse-overlayfs-1.18/bin/fuse-overlayfs`，未安装/构建。新自有空lower/upper/work/view目录上直接默认mount，UID1000下实际完整probe PASS：创建0400+O_RDWR→ftruncate4096→fsync、mode0400和UID不变、stat/maps dev+ino相同、MAP_PRIVATE写不改backing、hardlink同inode、目录fsync、空闲293.5GiB。非lazy卸载并等待daemon退出0，仅清自建probe。

最新review010-r2已收到完整材料和上述**兼容的fuse-overlayfs**交接：新短 `/tmp/p010-ov.XXXXXX/{l,u,w,v}`，只用新v作artifact，保留u及日志，完整actual make kvm-e2e两格式+原生gate、独立两finding复审；当前KVM槽位仅其使用。旧材料/旧run不改，不把probe当KVM成功。

### 016修订与仍需BLOCKED的条件

r1全部六文件已由父读取。r2针对严格event JSON（不忽略杂行/只打印错误行号）、exact source-located root skip reason+euid0、四namespace及全real/effective IDs比对、fresh per-run日志+删除自有PATHview/上传排除工具farm完成。执行者actuallocalpositive4/4pass87s，negative真missingbwrap子exit1，nonrootpreflight与mixedstdout/非rootSkip/子串reason拒绝均过。独立review016-r1正重跑，不假装rootCI。

必须保留：无GitHubpush授权/真实CI run；root preflight/fullidentitymap和合法root skip接受路径未执行；完整nixdevelop因既有GC移除bootstrap链不能运行（baseline同样），禁止下载/构建补齐。新四包eval/outPath缓存和同版本lint是partial evidence，不等同shell/CI验收。

### 018计划漂移已先reconcile

019 runner固化恰好3required names，018新增4场景会被其拒绝。父在**派发前**仅扩展018 Scope/drift到现有scripts/run-guest-fs-isolation.sh的通用枚举/结果核对，保留原3必需下限+全部新增实际PASS、fresh namespace/身份/caps/截止/清理，不能另建runner或弱化skip约束。

018依赖已顺序接008→015自身→019自身，其copySHAs068e35e/be747c9/225d457/9ea1ea2，inheritedtip`9ea1ea2be2066bd31448ada8cf49cf4ad26fd78b`，自身`6c090e81160e59e75280c79ca9f4041d8c0c2343`。仅六个允许文件，reported7required真实PASS、Rust32tests/featuregates/actualintegration都过，待父读全部增量/回归，暂不DONE。特别核对faccessat2(AT_EMPTY_PATH|AT_EACCESS)在当前filesystem identity下的实际权限、混合owner/writable/group/ACL目录、不丢非空Frame.error的持久化失败、postcommit旧stage证据、明确cleanup责任。

### 020/021在最终整合后再做来源收尾

r2两个validator和selfcheck父都重新运行通过（020=282series/12scenarios，021=9scenarios+7negative/2legal）。原始大阻断已针对性修正，**仍未最终APPROVE**，避免在012/013/017前反复改映射：

- 020：把go.mod的`go1.26.5`称作pinned compiler不正确（父actual`go version`=1.27.3，模块行是minimum）；Cargo.lock也不pin rustc。最终设计须写minimum与实际pinned Nix/runtime版本、记录完整shape/机器材料；修正`run串行dispatch`的措辞，多操作实际并发。其余observations数组、warm-rejected→unknown、fallback invocation、282/552公式和独立bucket selfcheck已读/验证。
- 021：r2 provenance把`resource_recovery table`与`durable quota/work tables`映到现有online_sandboxd_api，但现有API不提供这些私有表；baseline明确absent ledger仍可能仅因sandboxd reachable被当available。最终须按真实可用read API/一致backup区分，缺能力/未实现表保持unavailable，不能设计成直接读live WAL或虚构endpoint。结合012/013/017真实schema重核source mapping；线上非原子/离线WAL和公开结果不改的边界保持。

未派发仍为011/013/017，等待硬依赖全验收。012已接续APPROVE009，执行中；生产资源与当前独占槽位见下方最新续记。017本身无production门禁，需依赖已批准账本和真实有界恢复行为。

## 最新续记：009/018放行，012实施，010/016保留BLOCK

009父已读r2全生产增量及376行行为测试，八组场景包括11类坏pointer、owner冲突、缺placeholder、root显式target与engine/ownership真实gate。父全sandboxd race38.504s、vet/lint0issues、actualfixtureintegration0.719s/diff均exit0。发布锁此前误报已更正，实际跨engine查询问题已修复；schema4/007语义保留。009正文DONE，012可接续。

018父已完整读六文件增量。required双UID实际7/7PASS，默认workspace/alltargets32tests、fmt/clippy、featuretree10tests及alltargetsclippy、真实fixtureintegration0.702s、shellsyntax/diff均过。父曾错误传guest/Cargo.toml导致立即exit101，随后正确根workspace完整重跑；不将失败算成功。faccessat2/currentfsuid、foreign/group/ACL、typederror+params、keep/explicitcleanup及限定故障均复核通过。018正文DONE。

016独立review候选tree7a07abe254969f188db6eb92612cfea28c325a68完全一致。actualnative正向4/4PASS约84s/1440纯JSON、negative实际missingbwrap exit1、非rootpreflight及各种非法skip/输出拒绝、lint0issues/offlineeval/YAML均过。源码无剩余revisionrequest，但缺真实GitHubrun/rootfullmap和合法root例外接受/完整nixdevelop，整体BLOCKED；不得借本地证据标DONE。

新增任务（沿用前述taskId前缀）：

| request | 模型 | worktree | 当前结论 |
|---|---|---|---|
| verify-production-io-materials-r1 | K3 high | t3-dd0b4aac | PASS，仅前置IO预算；未启动VM/生产 |
| test-plan010-kvm-r3 | Codex high | t3-d50888e3 | 完整原make两格式实际error14，BLOCK；正常清理，槽位释放 |
| test-plan010-kvm-r4 | Codex high | t3-d6d66253 | 完整layered PASS，merged嵌套newuidmap EPERM；BLOCK，槽位释放 |
| implement-plan012-r1 | Codex high | t3-5f5c61c7 | `6360636`候选，16项测试/静态门禁自报过；production首claim兼容失败，STOPPED并释放VM槽 |
| review-plan012-r1 | Codex high | t3-b57c3787 | SOURCE REVISE：P1补偿分配丢接管路径，两故障overlay实际RED；其余全race62.669s等过 |
| implement-plan012-r2 | Codex high | t3-5f0c098e | `8892e5df8f759a43cea9a1c18f3c1c8a3454061a` COMPLETE_SOURCE；18Ledger/race73.851s等自报过，production仍BLOCK |
| review-plan012-r2 | Codex high | t3-b7acba05 | SOURCE REVISE：restore release-first新P1，两独立故障RED；旧captureP1已GREEN |
| design-plan012-instance-handoff-r3 | Codex high | 只读候选路径 | DESIGN_APPROVEABLE_PROPOSAL，现公开receipt无B证据；最小ResultInstance+统一resolution方案已返回 |
| review-plan012-r3-design-r1 | Codex high | t3-dd484b1b | DESIGN REVISE：canonical唯一/continuation及result_known推进两P1；Settled兼容/native env两P2 |
| revise-plan012-r3-design-r2 | Codex high | 只读候选路径 | DESIGNSPEC_REVISED：完整571行规格D1-D4回应返回，父全文读取 |
| review-plan012-r3-design-r2 | Codex high | t3-818ffa57 | DESIGN APPROVE：D1-D4设计关闭，N1普通A补偿/N2一次native全race执行说明 |
| implement-plan012-r3-result-handoff | Codex high | t3-f3e87a98 | COMPLETE own8729475→2c15f09/treeaf3d…，engine194.606/sandbox118.315真实门禁过，未接未批007 |
| review-plan012-r3-result-handoff | Codex high | t3-f3d6f35c | SOURCE REVISE1P2：成功B在destination前可信已删仍result_known/SID依赖留费，两故障RED |
| implement-plan012-r4-absent-success | Codex high | t3-8ce1b630 | COMPLETE ownadbd61a…parentc975/treef19c…；46新sub/398fullPASS/0skip，待独立复核 |
| review-plan012-r3-finish-atomicity | Codex high | t3-92d9f7ee | SOURCE FINDING/P1：实际Engine IGNORE VM/terminal/lease五例RED，r4修订已派 |
| implement-plan012-r4-finish-effects | Codex high | t3-37fa8d75 | COMPLETE own d565412…parent2c/tree46a…；5IGNORE GREEN/精确新源native全race195.661s，待复核 |
| review-plan012-r4-finish-effects | Codex high | t3-7f7d1ee4 | SOURCE APPROVE F1两文件/findings=[]；五IGNORE+额外prefence两阶段+精确448广测核验 |
| prepare-plan012-f1-p2-final-combination | Codex high | t3-ac14c9c0 | PREPARED90f68b6…/tree64a4…，engine=d565/sandboxd=adbd，父重核通过 |
| review-plan012-r4-final-combination | Codex high | t3-65a243e3 | SOURCE APPROVE/findings=[]，P2+整体90准确来源/独立49+44+10PASS与广测绑定核验 |
| verify-production-plan012-final90 | Codex high | t3-cf1d30e7 | BLOCKED：adoptionPASS/fork+pause通过，八路auto-resume NotFound；一次run/已清理slot释放 |
| diagnose-production-auto-resume-notfound-r1 | Codex high | t3-afcbb6a2 | VERIFIED组件owner tenant budget row缺失/八路0clone；历史prod对象LIKELY；无VM/原DB |
| design-plan012-production-owner-budget | Codex high | t3-a55f6dce | PROPOSAL：finite9slots/12D+7S=13GiB累计包络，warm物理attempt不涵盖属013 |
| review-plan012-production-owner-budget-design | Codex high | t3-dce02974 | DESIGN APPROVE/blocking[]；9/13GiB与边界成立，C1-C4纳入正式合同 |
| implement-plan012-production-owner-budget | Codex high | t3-f20b872f | DELIVERED 78bc04f；仅两测试文件+2001/-0，final fullrace217.111s/430测试节点，独立review中，无VM |
| review-plan012-production-owner-budget | Codex high | t3-b7162bd3 | SOURCE APPROVE/findings[]；32focused+HTTP隔离probe，24leaf/192+96、430广测与617源码绑定复核，无VM |
| verify-production-plan012-owner-budget78 | Codex high | t3-3c237f22 | ACTUAL_PRODUCTION_PASS make0/137.401s，5RUN/5PASS、13VMM、88PID清理释放；父APPROVE/012DONE，grant expired |
| design-plan013-after-approved012 | Codex high | t3-252d1250 | DESIGN_READY；完整typedcontext/事务/锁序/恢复/预算与五组行为规格已交付，冷审中，无source/tests/VM |
| review-plan013-design-after-approved012 | Codex high | t3-f66831c7 | REVISE D1/P1：created空B无原子删除条件；G有限预算成立；C1/C2执行规格必需 |
| revise-plan013-design-D1 | Codex high | t3-45a594bc | DESIGN_READY_R2；完整431行替代稿，D1/C1/C2/G响应已交付，新冷审中，无source/tests/VM |
| review-plan013-design-r2-D1 | Codex high | t3-19be6bf9 | DESIGN_APPROVE/blocking[]，D1设计层关闭，C1/C2/C4–C6必需；G9/13312条件成立，无source/tests/VM |
| implement-plan013-r1 | Codex high | t3-90582e1d | STOPPED准备差异（9实际initialized vs历史10）；真实build3subRED/CP PASS，产品撤回至78，唯有starter未提交；合同已调和 |
| implement-plan013-r1-resume | Codex high | t3-64481713 | DELIVERED d3be730(parent78)，19路径；final509PASS295.815s/79focused，独立SOURCE复核中，无VM |
| review-plan013-r1-source | Codex high | t3-67912afb | SOURCE REVISE R1/R2/R3 P2：warm成功reader泄漏、ready费用/refs启动缺校验、SQL必需矩阵缺项；独立79+22PASS与5新RED |
| review-plan013-r1-adversarial | Codex high | t3-a07822b7 | REVISE A1/P1：持久selection deletion空ExpectedInstance经实际Open后发出一次空predicate调用；独立6GREEN/1FAIL |
| fix-plan013-warm-selection-delete-validation | Codex high | t3-a261ca35 | DELIVERED da4b7d6，directparentd3/4路径；611PASS332.951s/原509+102，父核618源码三副本，未自批 |
| review-plan013-r2-closure | Codex high | t3-10b951c8 | SOURCE_REVISE A2+新R4：claimed必需Selection整体缺失时Open接受/错误Done-reader退休；181+13+5+22PASS，独立R4 FAIL；完整闭环NO/YES/NO/YES |
| review-plan013-r2-validator-adversarial | Codex high | t3-f2a33310 | FOCUS_SOURCE_REVISE A2/P2：fallback/cleanup stage缺真实目的地；own17PASS/3leaf+父FAIL，原12PASS；原A1空predicate关闭focus |
| fix-plan013-selection-stage-destinations | Codex high | t3-2760705d | COMMITTED 5c530395/directparentda4/4路径+750/-33；635PASS343.254s。验收STOPPED原A2 oracle矛盾；父已澄清并派新review，非SOURCEAPPROVE |
| review-plan013-r3-source-5c530395 | Codex high | t3-5b541bf0 | SOURCE_REVISE R5/P2：running fallback缺active_use时cleanup同名holder仍被接受；实际两RED leaf+父，34+19+1PASS；闭环NO/YES/NO/YES/NO/YES |
| review-plan013-r3-oracle-and-destinations | Codex high | t3-c6984cce | FOCUS_SOURCE_REVISE独立同一R5无其它阻塞；R5两leaf+父3FAIL，corrected40PASS/A2全21，da4 stage1PASS4FAIL/ordinary3PASS |
| fix-plan013-typed-destination-holders | Codex high | t3-0c3b63bb | DELIVERED1414b4e/directparent5c/3路径+367/-50；644PASS342.713s/50外部9新84producer，父核618源码三模式副本，未自批 |
| review-plan013-r4-source-1414b4e | Codex high | t3-46728132 | SOURCE_APPROVE/all7YES/findings[]；独立9+72+4=85PASS/完整SQL，644来源/618模式三副本/33fixture全核 |
| review-plan013-r4-typed-holder-adversarial | Codex high | t3-ac4bfeee | FOCUS_SOURCE_APPROVE，七项全YES/findings[]；原50PASS32.676s+真实后继pause边界3PASS6.979s，无重复广测 |
| verify-production-plan013-1414 | Codex high | t3-68d5daed | ACTUAL_PRODUCTION_PASS：一次make exit0/142.601s，5RUN5PASS，13VMM双身份/guard；89PID-starttime/26PGID/unit/cgroups/mount0父核清理；明确release/grant过期/013DONE后STOP |
| diagnose-production-agent-compat-r1 | K3 high | 待报告 | ROOT CAUSE VERIFIED：旧guest二进制pre-rename portal=product；不改Info检查 |
| test-plan010-kvm-r6 | Codex high | t3-89fadb10 | 原完整双格式PASS12.539s/零skip，资源已清/槽位释放；后发现guest来源需补匹配验证 |
| implement-plan011-r1 | Codex high | t3-3430e919 | `5115a9f00c1003f94c6114457551790aca34fd14`候选，requirednative Engine race175.704s零test skip等自报过，父已读四文件 |
| review-plan011-r1 | Codex high | t3-2d5d68d9 | SOURCE APPROVE/findings=[]；父读报告/审计，011 DONE |
| prepare-corrected-guest-oci-r1 | Codex high | t3-70aca3ea | MATERIAL_READY：匹配b93/actual Info6VM+双格式14.09s全PASS、正常清理/槽位释放 |
| verify-production-plan012-r2 | Codex high | t3-4663c711 | BLOCK：adoption过、fork分支exec EOF，golden/warm/TCP未执行，资源已清/槽位释放 |
| diagnose-production-fork-eof-r1 | K3 high | 只读候选路径 | LIKELY：007r2 none activation bind与reconcile误close机制；历史无确切tick记录 |
| implement-plan007-r3-none-relay | Codex high | t3-2603d552 | COMPLETE own8d08ba0…/parentdf769…；实际Service RED→GREEN9case/fullrace44.421s，待新复核 |
| review-plan007-r3-none-relay | Codex high | t3-56da2195 | SOURCE REVISE：1P2 actualManager/Service.InstallDirect+SQLite诊断保存失败时新admission通过 |
| implement-plan007-r4-admission-revoke | Codex high | t3-33bfef09 | COMPLETE own1c1e735…parent8d/正式6fault场景与nilManager/fullrace45.926s；待新独立复审 |
| review-plan007-r4-admission-revoke | Codex high | t3-cb306882 | SOURCE REVISE：旧P2关闭，新1P2 none Revoke期间configure fence被在途HTTP接入穿过 |
| implement-plan007-r5-fence-registration | Codex high | t3-cdc262ce | COMPLETE own2a8e9c0…parent1c/treebfcd…；原overlayHTTP409/六新场景/fullrace41.746s，待复核 |
| review-plan007-r5-fence-registration | Codex high | t3-e680e7d7 | SOURCE APPROVE/findings=[]，20top47sub/两旧overlay与最终fence查询故障独立GREEN，父批准 |
| prepare-plan012-approved007-combination | Codex high | t3-0a11ebfe | PREPARED无冲突c97543e…/tree4f4a…，只三个批准7patch-id一致；未测试/未批012 |
| integrate-approved-stage2 | Codex high | t3-ea88fcd4 | COMBINEDPASS：14patch一致、全Go race222.515s/required-native20零skip，父快照重核通过 |
| integrate-approved-stage1 | Codex high | t3-d6927cf8 | 八项APPROVED源组合COMBINEDPASS，父快照核验通过 |
| implement-plan020-r3 | K3 high | t3-ddcdb336 | `cfd76ec`→`39c982d`→`02a9590`，版本/并发事实已修正；父发现两处表述需r4 |
| implement-plan020-r4 | K3 high | t3-f7cdd4b9 | `29d62f8a0af74b7fdd39ec91211400eb952ecad9`，父已读增量并重跑normal/selfcheck通过；两处表述关闭，最终017映射待定 |
| implement-plan021-r3 | K3 high | t3-d2c109d2 | `a140c6e`，私有表能力修正通过；父补测发现缺失/null schema仍假接受，交r4 |
| implement-plan021-r4 | K3 high | t3-b65ae5bc | `797a0084b9a9b091fc2a6236704c053a6bb4642e`，父复核normal/11negative+4legal与missing/null/support缺失反例通过；最终来源映射待定 |
| probe-plan010-nested-mapping-r5 | Codex high | t3-834133d8 | PROBE_PASS，原nested unshare与两项actual OCI metadata PASS；无VM，正常清理 |

### 新鲜生产前置与OCI

独立IOtest使用新/tmp/kix-b93fc24及新systemduser unit sandbox-kernel-acceptance-1793990064-3572200.service，kernel-isolation原入口exit0/wall5.146s、rootlesscgroup4.54s真实PASS零skip。O_DIRECT/fdatasync4MiB用io.stat发现NVMe259:0，baseline12.688621ms/throttled4.011733338s、各wbytes4231168，io.max精确readback `259:0 rbps=max wbps=1048576 riops=max wiops=max`。因此PLAN_PRODUCTION_IO_MAX明确交付为`259:0 wbps=1048576`；父已读原始cgroup-io-enforcement.txt。该unit inactive/notfound/cgroup目录不存在，pids/cpu/memory强制力也实际证明。11个productiontools包offlineeval均缓存；仅是前置，不是production成功。

r3makeexit2/wall3.630s，在engine初始化SQLite `unable to open database file (14)`，未启动VMM。文件0400/FD/copyrange/fsync/COW/statmaps/hardlink前提通过，但same-versionmoderncSQLiteWAL也失败；不猜根因已证实。材料hash不变，全部ownedhelper/bindfs正常卸载wait0，最终tree74d93ebbd9a6fc3b52e3080f3e9dacded16015d5。完整report `/tmp/p010-r3.4mkybi8y/report.md`。

当次新OCI只读归档 `/tmp/p010-r3.4mkybi8y/artifacts/kvm-acceptance-1854451212/oci`，index sha9b44a6b30f8f18bee0ee15ae070df9a66184c2447b470f768a81207f32e0b65b、manifest85cbaa2cd1387166a4a2f6ac0b2bf6261aac829a8550f0bda36e1d49cc9ce3e1，6blobs/digest/size重核。可明确交付作为新输入，不能称boot验证通过。

r4已完整运行原makeexit2/wall8.738s、零skip。layered完整PASS4.86s，两次真实512MiB原inode rw-p映射、boot/clone/sourcecapture删除/独立写盘/COWresnapshot+restore均过；merged0.53s在原runMappedImageTool nestedunshare执行newuidmap EPERM，未启动其VMM。Go/test/3VMM的host及namespaceUID1000/GID100、五capsets零/NNP1、继承groups原样，SQLite/文件探针全部过。source/tree/输入hash不变，普通卸载helper0、所有owned进程退出，槽位释放。report `/tmp/p010-r4.joIbnoSg/report.md`，新archive121项/IPCskip0，资料完整非双格式成功。r5仅研究嵌套helper前提，不占VM槽：Go/test可保留原始非特权bounding与NNP0；真实VMM仍由原launcher强制NNP1/降权限。若需要private helper，限定已有subuid/subgid子集、未修改的缓存raw helper字节、仅新namespace tmpfs中的namespace-root权限和private/etc/subid子集，不能扩大宿主授权或伪造kernel mapping。

012获得完整内联计划、已批准007/009、IO预算及固定材料；生产只读明确 `/home/woxQAQ/sandbox-101/plans/evidence/production-materials-012.json`，要求format1/task=implement-plan012-r1/slot_granted=true。r4释放后父已写有效handoff并steer同一活动012任务；当前唯一VM槽位持有者为012。父独立重新校验r3OCI六descriptor/digest/size/index一致；交付未被格式测试改写的base85cbaa...manifest，专属新artifact `/tmp/p012-production.ElQ9Ictd`。r4主OCI已指merged扩展manifest，不自动替换。缺必要gate仍BLOCKED，最终真实production不能省略；完成后012必须报告清理并释放槽位。

### 八项中间集成已复核

`integrate-approved-stage1` 在 `t3-d6927cf8`/分支`t3/integrate-eight-approved-plans`按指定11提交无冲突组合006/007/008/009/014/015/018/019。HEAD `f1a3fb62a8c3b2aedef14f96bb2fb407e7e81ced`，tree `a8262d78cafaba0673337373b5d06cdecbaf1b3c`。11对patch-id全部相同，无额外逻辑提交；父重核最终HEAD、完整guest/runner/protocol快照与018、service/store/lifecycle/reconcile及007/009测试与009r2均diff0，diffcheck过。其他源码保留由source-preservation.json记录。

原make test-race只一次132.157s，SANDBOX_REQUIRE_EROFS_TESTS=1，三个module实际race且20native场景PASS、零skip/race；Rust32tests、check、lint0issues、featuretree10/clippy、required双UID7/7、actualintegration4场景、原make smoke全freshfake-daemon4场景、语法/diff均exit0。全部owned daemon/fixture停止wait，mount/进程核查无残留；父读完整REPORT/source-preservation/cleanup，无需对相同源码重复整套门禁。完整记录 `/tmp/sandbox-eight-integration-t3-d6927cf8/REPORT.md`。这是已批准八项的组合基线，未含010/012/016/020/021，不是全部批次完成；未来各项完成后仍新round扩展组合、复核必要交互，绝不merge/push用户dev。

020r3父已读完整两文件delta：最低Go指令/锁文件≠compilerpin、host实际版本与flake eval版本区分、并发dispatch/串行maintain、017未来来源均修正。flake eval为go1.26.8/rustc1.98.1/cargo1.98.1，host为go1.27.3/rustc1.98.1/cargo1.98.0/Nu0.116.1，不混为材料compiler。新发现：go.sum是校验和非选择依赖版本的锁文件；'282registry+active集合'不能合称常量上界，existing active无固定concurrencycap且不是新指标聚合状态。已完整交r4修订，无新exporter/限制功能。21r3继续修正私有表不能仅靠onlineAPI liveness声明available。最终012/013/017来源映射仍待实际实现。

父文档检查首次误把归档001–005五项DONE算入本批，临时断言失败；限定006–021后通过：16计划/索引链接与围栏有效、本批八DONE、productionhandoff有效，父engine/guest/sandboxd/protocol/scripts/Make/flake均diff0，diffcheck过。

020r4父完整读取相对inheritedtip `d5e072226158edd1cb7af4f0a50cdc47b8c8939a` 的三文件增量；最终候选worktree `t3-f7cdd4b9`、branch `t3/020-design-contract-corrections`、own `29d62f8a0af74b7fdd39ec91211400eb952ecad9`。go.mod/MVS选择与go.sum完整性核验、Cargo.lock/编译器回执区分准确；常量指标聚合仅282序列，既有active去重和局部monotonic时钟开销随实际并发变化，不新增IDmap或虚报worker上限。父实际nu normal=282/512/12、selfcheck四非法类拒绝+合法接受均exit0，diffcheck及所有源码路径相对b93无差异。此轮两项复核意见关闭，020最终SOURCE017映射仍待后续真实实现，不计生产指标上线或最终DONE。

021r3父完整读四文件delta及validator前300行，actualnormal9/selfcheck9negative+3legal通过、source相对b93diff0。私有resource_recovery仅offlinebackup/stopped、未来durable能力需storepath+presence、精确API/not_exposed及S05stale混合视图修正符合要求。候选`t3-d2c109d2`/`t3/fix-diagnostics-read-capabilities`，r1/r2copy `e8db2c4`→`e823e81`，own`a140c6e`。

父独立source同一validator并内存变异S05：分别reject inputs.schema_version、upsert其为null，validate-scenarios均ZERO errors，接受了未知schema的verified private-store观测。根因adapter与schema_bad均仅在sv!=null时检查，违反contract原require规则。首次Nu update闭包误按field而非wholeRow操作导致column_not_found，已纠正并实际复现上述两个零错误；不把命令错误当反例。已新派021r4完整交接：store adapter必须已知且支持的version、未知不得schema_supportedtrue；不得误伤不需DBschema的纯在线API，新增missing/null负例与纯在线合法对照。最终012/013/017实际schema/table映射仍待实现后终审。

010r5前置父已读完整report/native原日志/final-integrity，实际nestedunshare exit0/0.061s，原TestActualMergedOCIHardlinksAndMetadata与TestActualSharedOCIHardlinksAndMetadata均RUN/PASS（.05/.04s，总wall.094s），required=1/零skip；sameversionSQLiteWAL与0400FD/copy/fsync/statmaps/privateCOW/hardlink也过。候选worktree`t3-834133d8`/branch`t3/probe-nested-uidmap-prerequisites`，源tree仍74d93ebbd9a6fc3b52e3080f3e9dacded16015d5。报告 `/tmp/p010-nested._4tfbbaa/report.md`，最终fixture/supervisor在其attempt2目录。

实际可行环境：fresh24GiB tmpfs保持nosuid,nodev；parent0→host100000 setup、1000→host1000、100001→host100001 count65535，GID对应0/100/100001；仅新mountns的/etc/subuid和subgid readonly配置woxQAQ:100001:65535，完全包含于既有授权。Go/test UID1000/GID100/EffPrmInhAmb0、保留普通Bnd/NNP0；原host wrappers正常写实际nestedmaps，真实shadow4.20.2 ELF采样及hash留证，**未复制/创建SUID helper，未放开nosuid**。这是一组可行前提，不宣称已单因素确定r4 EPERM根因；原launcher VMM强制NNP1/dropBnd不变。

两个run全部mount普通卸载/setup/client0，无descendant/mount残留、host subid原文未变。首run比较器误把耗时不同当sourcechanged及多做小采样probe已公开，最终修正校验全部源码/证据一致；不当作产品失败/成功。父查询012任务仍running，无pendingquestion/approval；当前VM槽位仍归012，010r6完整双格式须等明确释放后新派，probePASS不解除010/011依赖。

021r4父已完整读取相对r3 `a140c6e73de5199d7c02dad9ce7a7af296c72654` 的增量并实际重跑normal9/selfcheck11negative+4legal，全exit0；严格两JSON重复键检查也过。独立重做S05 missing_version、null_version与新增missing_supported_set，三者均按provenance+version规则拒绝，不是无关结构错误。store/schema主张需要known-supported版本，纯在线无store主张仍可用。所有源码路径diff0、diffcheck过，此轮版本门禁缺口关闭；最终012/013/017映射仍待真实实现。

重要提交交接偏差：该执行者没有保留原要求的r1→r2→r3 cherry-pick祖先链，而是从b93取r3的四份完整设计文件后修订，形成单个aggregate提交`797a0084b9a9b091fc2a6236704c053a6bb4642e`，其parent确为b93。worktree`t3-b65ae5bc`/branch`t3/fix-store-schema-version-gate`。父已比较r3内容，仅预期的contract/md/validator增量、scenarios完全保留，未丢前三轮修正。未来整合021采用该单个aggregate，不再重复r1/r2/r3提交；本项仍仅四设计文件，无source或用户branch变更。

## 最新状态：012候选与production BLOCK，010r6独占验收

012执行者STOPPED：worktree`t3-5f5c61c7`/branch`t3/implement-plan012-durable-resource-ledger`，基准b93，007/009副本43de6b5→cf2e25a→d7e4893→b6813cf7f28881531b2839a13d47b9912a9d5cf0，own6360636758addc6bca61f99ce762a5eac2456a7a。16Scope文件，schema5；QuotaResources唯一汇总，ChargeTenantID可信owner，slot/reservation→receipt→active/backing→cleanup→可信回收。独立restore盘D因无单版本GC证明保守至整VM删除；snapshotcatalog删除不清backing费用。D64/M128边界64/63、256/255、320/319、448/447，低界零engineeffects。父已读quota.go全842行及完整生产/文档/旧fixture增量，尚未源码APPROVE；另派独立review全部源码与1188行实际Service/SQLite测试。

自报16TestQuotaLedger/RED→GREENrelease+lostdelete、fulltest29.892s、committedHEAD全race64.484s、vet/lint0issues、actualintegration.774s/diff过；不把自报当父审通过。原生产make实际exit2/test28.34s，device检查和OCI import过，但首live-daemon-adoption claim `internal: incompatible workload agent`，后续lifecycle/golden/warm/TCP未执行，整体BLOCKED。父已读原test.log、handoff与cleanup-review，sourceagent.Info未改，条件portalworkload/numericprotocol1；根因字段仍未知，仅新派静态来源诊断，不放宽契约。

owned production unit1793993579-3715441已notfound/inactive/dead/MainPID0，cgroup与vm-scopes不在、driver退出/privateNS结束。执行者明确SLOTRELEASE，父将production-materials-012.json置slot_granted=false/expired，旧输入不能授权再跑。新VM槽位只授test-plan010-kvm-r6；其使用成功r5attempt2的nosuid/nodev/private subid子集和实际原wrapper，无SUIDfallback、无Go/testBnd额外限制，仍强制原VMM NNP1/dropBnd与所有COW/provenance/budget证明。完整两格式门禁后再明确释放，012review/compatdiagnosis不启动VM。

012偏差：初次integration在缓存Cargo源码上编译依赖17.65s，无fetch/install但违反禁止uncacheddependencybuild；未来review复用源码相同的已有Rustfixture。production NIX_CONFIG含不识别offline警告，实际禁用substitute/builders且缓存tools无build/fetch；不把忽略setting当保证。后续统一用有效max-jobs0/substitutefalse/builders/substituters空配置。013仍不派发，等012完整复核与实际production通过。

### 012独立SOURCE REVISE：补偿分配失联

review`t3-b57c3787`/branch`t3/review-resource-ledger-lifecycle`，HEAD0b74e684483a6a62da1aa2681a903514bacb9cd7，tree72ce188a02f8eb8fd5461bfecda61f7d7c31dd9e与r1完全一致。完整读取16文件与1188行行为测试/引擎真实backing契约；16Ledger6.443s、fulltest28.622s/fullrace62.669s、专项14.615s、vet/lint0issues/四actualRustfixture.818s/diff全过，但独立真实Service+SQL overlay两个故障RED。

P1：recovery pause使用r.operation()身份而非operations行；snapshot完成后响应丢失，或真实SQLite trigger拒snapshot ledger入账，pause错误回running并清OperationID。下一pass !owned把recovery标Done，普通operation扫描无法找到此receipt。release/反复recover/重启后仍snapshots1,pending1,slot1、原VMdelete仅1、新claimExhausted。父已读完整report与overlay源码，证据/tmp/plan012-review-r1-report.md及counterexample.log、overlay_test.go、overlay.json。已新派implement012r2完整原brief+发现/响应/未解gate，先设计独立持久identity/step/request/owner接管与cleanup，再覆盖lostreply/SQLite写失败/restart及restore同类窗口；绝不删除未知pending/换key/改原公开failed结果。013不放行。

### 010 r6完整standalone PASS与来源补验

父已读/tmp/p010-r6.AL80kzLv/report.md/finalcleanup及integrity核心证据：唯一原makeexit0/wall12.539s，layered4.65/merged4.59均RUN/PASS零skip，全部boot/clone/sourcecapturedelete/独立disk/COWresnapshot+grandchildrestore、四512MiB原inode完整rw-p均通过。6VMM双视角UID1000/GID100/EffPrmInhAmb0，实际NNP1、Bnd普通非零；源码tree74d.../材料hash不变，155archiveitems/IPCskip0/8稀疏盘+hardlinks保留，所有ownedPIDs/3mount正常退出，slotrelease。

父更正一项自身brief误设：standalone KVM test:35/797直接exec VMM，不走production InternalLaunch；此前把生产NNP/dropBnd当其硬条件无源码依据。已同活动tasksteer纠正，原source/proof/测试未变；不把standalone PASS称managedproductionisolation证明。主test comm截断导致无其直接status样本，Go/其他test/全部6VMM都有；该局限保留，不重跑相同gate补样。

随后兼容材料静态诊断证明旧build/guest binary源不匹配（见下），因此**匹配源码guest的必要来源确认仍待补验**，暂不将010最终DONE。011已获010批准源码隔离实施授权，补充steer明确这项新来源资格，最终011approval/integration须等新匹配guest门禁；不是声称旧binary与b93一致。

### 陈旧guest根因与全新材料准备

/tmp/p012-info-compat-diagnosis.md的原始错误、OCI逐字节hash和历史/strings支持根因：旧guest SHA5802dbc9...8ad是da513a6 product→workload改名前构建，当前b93双方Info应portalworkload/protocolnumeric1。OCI embeddedagent与旧build/guest完全一致，unit却是当前源码，形成混合材料；r6 file/tree/exec未调用Info，故不能排除此漂移。协议数字/codec/端口无变化，不放宽兼容检查。

父额外readonlyofflineeval根当前b93相关guestfilter得已缓存package /nix/store/05dhwqkv3l90d72q2kk6yp1bnbypfpa3-sandbox-guest-static-x86_64-unknown-linux-musl-0.1.0。实际newagentSHA2463ee2fbcba231fc2ef48d78b6674fdf02f29da6f844b0bb7a0a0af6f557ddb，initSHA6ef7d992...015与旧相同；newbinary workloadmethod1/productmethod0、workloadportal2/productportal0。不需构建/安装依赖。已授prepare-corrected-guest-oci-r1唯一VM槽：核对b93过滤源码与package outpath一致，在其OWNworktree用原package-guest.sh新打包而不覆盖父build/guest；新OCI+原双格式gate，并用只读自有VM的UNCHANGEDagent.Client.Info验证实际portal/protocol。只交付新生产baseOCI，不误用最终merged-specific index，冻结hash/receipt，正常清理并释放槽位后才给012r2新handoff。旧materials/guest/OCI保留不改；012r2与011均无VM授权。

### 012 r2已交付，独立复核中

候选`t3-5f0c098e`/branch`t3/fix-recovery-quota-reconciliation`，inheritedr1tip1689dedfa7b1b592b500cc1e03afe9ab70994472（r1tree72ce...一致），own8892e5df8f759a43cea9a1c18f3c1c8a3454061a，finaltreea56cfd1fbf72cd3158f2ffb38e0eeed197f3e3ac。仅quota/recovery/service/行为测试/README五文件+705/-35，未增表/DDL。现有resource_recovery body的Allocations冻结op/step/FULLrequest/请求tenant/chargeowner/SBid/shape，与pending同事务；receipt对账先于sandbox身份/状态，并在Done前再次完成，独立snapshotcleanup带proof持久化，未知继续收费。父已读全部生产增量和344行新增行为测试；尚未APPROVE，已新派review-plan012-r2带全原brief、P1、响应与未解决production。

自报原2overlayr1RED→r2GREEN.675s；新增capture4/restore2在r1生产source-overlay全RED→修复GREEN，含queryerror/restart、release-first/rootBob代Alice、原失败body字节不变、一次capture/restore/deleteeffects。18Ledger8.397s/fulltest41.025s/finalrace73.851s/旧回归19.908s/vet/lint0issues/四actualexistingRustfixture1.093s/diff过，无Cargo/依赖构建。新grant未提供，未跑任何VM，整体仍BLOCKED。r2handoff误写槽位仍归旧test010r6，实际由新材料prepare-corrected-guest-oci-r1持有；父新reviewbrief已明确更正。完整/tmp/plan012-r2-handoff.md。

## 最新：010 DONE，011独立复核，012新生产验收

新材料MATERIAL_READY父已读完整report/handoff/finalcleanup，并独立计算newGuest3SHA/productionindex与6reachable descriptor/digest/size/fourlayers。与b93相同的26文件过滤source和Nix derivation source一致，清单SHA9d5709...d61b1；缓存05dh... agent2463ee...ddb，init6ef7...015/initramfsfa30...1699。唯一原make用newGuest完整双格式exit0/wall14.090s、layered5.27/merged5.25，全部COW/源删除/独立盘/四512MiB原inode rw-p通过；全部6VMM实际UNCHANGEDagent.Client.Info2048 portalworkload/protocolfloat64(1)通过，主test及VMM真实身份样本完整。旧32输入hash不变，全部children退出wait/3mount普通卸载，slotrelease。证据/tmp/p012-newguest.vcowRD9Q/report.md及handoff/provenance/OCIreceipt/cleanup。结合010独立source/native门禁，匹配来源缺口关闭，010 DONE/011前置hold解除；不是managedproduction证明。

新productionOCI `/tmp/p012-newguest.vcowRD9Q/production-oci`，indexc393ea39686f0834923cf09877051ec30174ddfaa61dcaac3eba81823cb34130，manifestad930052c8446239e8a1ea84974bfaacc162ae99d05bcd97c2707f53e7a0e1a8/source原base四层，非最终merged-specific index；rootlayerf014...fb0内agent与缓存逐字节一致。Parent已创建明确 `plans/evidence/production-verification-012-r2.json`，taskverify-production-plan012-r2/slot_grantedtrue、目标candidate8892/treea56...、原pinned材料/IO259:0wbps1048576、新OCI和新artifact `/tmp/p012-r2-production.B158QEtk`。当前唯一VM槽位授该验证任务；原implement012r1旧grant继续expired。只测试候选，不代表源码先获批准，source review012r2并行且不运行VM。原production完整lifecycle/golden/warm/TCP结果必须全部实际PASS，否则仍BLOCK。

011实现t3-3430e919/t3/fix-instance-condition-recovery，010inheritedtip0bed9ba...，own5115a9f00c1003f94c6114457551790aca34fd14，treeb0ffa3663cafd111bba450ba0dea7603400f99bb。父已读4文件全增量，包括18行生产predicate与498行真实Engine tests。候选仅start/restore严格sameop/VMID/非空Instance/fullResource==journal可接续B，backendInspect和010证明仍必需。自报11RED边界→13GREEN、46反例、终态字节不变；requirednative全race175.704s零testskip（api包无tests分列）、vet/lint/4actualRustfixture等过。独立review011r1已新派全原brief+source/candidate+前置matchingguest结果，尚不APPROVE，不复用旧pending来源误判。

### 013派发前Scope漂移狭义reconcile

父只读确认009r2新增TestTemplateAdmissionPublicationOwnershipGate在template_admission_test.go用普通p提交TargetWarm0→1。013将池目标变更root-only，原计划Scope未含新增测试文件，会在抵达engine/ownership锁门禁前被正确拒绝。因此已在派发前仅将该测试文件加入013 drift/Scope，允许改actor为明确原tenant root，保持所有enginequery/锁阻塞/0→1发布/同snapshot/终态原断言。不得删测试/放宽普通tenant权限/变生产Scope。013仍待012源码和production全通过才派发。

## 最新：011 DONE；012新release-first P1及production EOF

011独立SOURCEAPPROVE/findings=[]：reviewt3-2d5d68d9/branchreview-instance-condition-recovery，copy532a82831a6eaee11f265a8909bf90924741d9ca/treeb0ffa...一致。亲自20focusedEngineRace71.722s/3生产Inspect与010proof overlay25cases19.269s/vet/lint0issues/4actualRustfixture/diff通过；独立旧源码overlay重现11RED→最终GREEN。完整required-native175.704s是执行者同tree证据，独立解析100顶层/318子PASS、testskip0，不冒充重跑。父读完整report/finalaudit且此前已读全四文件，010matchingguest前置关闭，011 DONE；当前十项DONE。Source/indexclean/refsubmodules原样，无VM。已派stage2只FF已有stage1f1...再pick010270eb...→7df...→0115115...；不混入未批准012，无VMM权限。

012独立r2reviewt3-b7acba05/branchreview-quota-ledger-r2，HEAD76e6c9b0e0a6cd91d48873cd76c03d4abcf39c8e/treea56...与候选相同。旧capture两个overlayGREEN；18Ledger9.479/fulltest33.672/fullrace73.227/45旧回归15.748/vet/lint/4actualfixture1.032s/diff过。额外capture写入owner/cleanup/Settled/Done失败、dispatch前故障、Attempt变更/新VM保护共8caseGREEN，但**restore release-before-reconcile两case RED**：restoreA→B完成丢回复或SQLite入账失败，先release把SBterminated且opID替换，持旧A delete被engine拒；late receipt虽采纳pending0/Settledtrue/parentDone，删除责任仍A。反复recover/restart/新explicitrelease后VM1/slot1/storage320、restore1，原failedrollback和failedreleasebody逐字节不变，slot持续拒新claim。父已读完整report与overlay源码/相关实际engine API与release/recoverRollback上下文。报告/tmp/plan012-review-r2-report.md及restore-release_test.go/log。

第三轮先只读设计验证而未直接加特例：engine api.Operation目前没有输出instance，只含VMID/SID/request/status等，当前GetVM.Instance不自动证明该receipt产生的B（可能同VMID外部C）。已新派design-plan012-instance-handoff-r3核对现有证据是否足够，若不足提出最小可信、原子、不可变结果身份接口及必要Scope增补；尚未授权engine API/source扩范围实现。要求不能盲认当前实例/放宽ExpectedInstance/复活terminated/新restorekey/清未知费；late receipt须持久实例绑定cleanuphandoff再Done，完整失败body保留。等具体方案和冷审后再实现r3，013仍不放行。

新production使用correctedguest及原grant/限额真实一次makeexit2/wall50.2507s/test45.53s：device/OCI/原Info均过，live-daemon-adoption3.93sPASS；MicroVMLifecycle12.13s在forkCount2成功后的branch cat/etc/machine-id报exec EOF/code-1，后续pause/resume/rollback/zeroWake和golden/warm/TCP未执行。不得把adoptionPASS当完整production。4managedVMM身份/原640MiB上限与outer8G未改，source scope peak=max640MiB/maxevents248但oom/oom_kill均0、outerpeak7.3G，不能据此确定OOM或涨限。父读完整report、原test.log/失败代码；另派只读EOF诊断，不盲重跑。证据/tmp/p012-r2-production.B158QEtk/report.md、run.riGYCb19/test.log/memory-summary等。

生产51输入SHA/mode/size不变、source/tree不变。ownedunit1793996430-3849725notfound/inactive/dead/MainPID0、cgroup/vmscopes不在、25采样process及driver/supervisor退出、privateNSmount结束；执行者明确SLOTRELEASE，父将production-verification-012-r2.json置false/expired，当前VM槽位FREE，所有设计/诊断/中间集成均无VM授权。新重跑必须新有效grant/新专属artifact。

### 012第三轮具体提案已返回，Scope冷审中

父完整读/tmp/plan012-r3-design-proposal.md。现公开receipt不能区分R成功产生B与同VMID之后C；currentVM/PID/config/时间都不补缺失映射。提议唯一optionalengine Operation.ResultInstance `json:result_instance,omitempty`，新成功start/restore/clone在finish事务校验owned完整candidate+currentVM后，与status/最终identity确认/ownershiprelease一起持久化；failed不发布成功witness、旧terminal不读时补写，缺证据failclosed。Sandboxd将fee adoption与resource-responsibility resolution分开，先持久active或canonicalV/Bcleanup，再Settled/Done；freeze原Arequest与Bcompensationdeletekey，freshrelease/后台同一collector，同VMIDforeignC严格不动。收紧受影响same-VM heuristic与activation重读，不给新权限。

原012Scope明确禁engine，因此此时仅提案、**未授权实现扩域**。已独立冷审review-plan012-r3-design-r1完整交接原计划/P1/r2响应/提案/未解production，核对最小types/operations/actualEngine result行为tests/INTEGRATION例外及原sandboxd文件。父特别要求确认正常already-running/no-opstart、clone空Request.VMID、canonicalfailedReleaseOpID在freshrelease后唯一且不被!owned跳过、finish写失败内存与durable状态、禁止unpublishedinternalSettled/Resolution双轨兼容、actualacceptedrestore失败VMID保留与精确D费用、required-nativeenv不遗漏。011已正式APPROVE/DONE，可显式作为新前置，纠正提案撰写时其状态未知的旧限定；实现前必须正式调和Scope与依赖，不能默认用旁边候选。当前VM槽FREE，冷审/EOF静态诊断/stage2integration都无VM授权。

### 十项中间集成基线 COMBINEDPASS

父已读/tmp/sandbox-ten-integration-t3-ea88fcd4/REPORT.md与全部gate收据。t3-ea88fcd4/branch`t3/integrate-approved-stage2-plans`，FF继承stage1 f1a3fb...后无冲突接010270eb→abac05e002d537cb363b0fda0425092aaa578678、7df4829→6679e192e342bae967aec76c151e7348435dc3c3、0115115→HEADdf769bbfefb2de647c6da2815fccfc0a6a393c58，tree58b5a2cbcafd145464397ea12709f1b1a587be65。14/14patch-id相同，无额外逻辑提交。父实际git重核整个engine与approved011完全一致，sandboxd/guest/protocol/scripts/Make/flake与stage1完全一致，diffcheck过。

唯一原make test-raceexit0/wall222.515s（engine176.240s，仍每包180s），234顶层/431子测试PASS；20actualnative、5RestoreCandidate、4InstanceCondition及4actualGo→Rust全到达，testskip0/无DATA RACE；3modulevet/正常workspace lint0issues/diff均过。Rustfixture用stage1对应guest18的既有binary SHAce62...32d9，未编译Cargo；原Rust32/feature10/requiredUID7/smoke/匹配guestKVM只列reviewed旧证据，未冒充新跑。源码/index与预存references保持、自有process/mount无残留、无VM/parentmerge/push。此分支是当前后续集成基线，仅10approvedplans，不混012/16/20/21等候选。

### 新007 r3与012设计cold REVISE（当前九项DONE）

父完整读/tmp/plan012-r3-cold-design-review-dd484b1b.md。窄ResultInstance合同可行，实际start源码排除no-op成功无candidate：新key运行中startConflict，adoption复用journal并重做Inspect/Boot/Agent；clone空Request.VMID已明确。010/011批准依赖可直接写入，不重询权限。设计DESIGNREVISE两P1：failedReleaseOpID作canonicalID不足跨freshrelease唯一/旧collector被!ownedDone跳过；单一settledbool会在result_known尚未active时挡住recoverRollback activation。两P2移除branch-onlySettled兼容、requirednativeenv。已新派revise-plan012-r3-design-r2给完整原brief/所有报告/未解项，只/tmp全量规格修订；偏好trustedtenant/S/V/B确定ID+PK、固定source/原A/未知Bkey，绑定continuation在!owned与自引用之前；ONE持久责任state+临时进度区分可activation与可parentDone。未授权API实现扩域。

EOF静态报告/tmp/p012-r2-eof-diagnosis.md返回LIKELY：007r2(copyc2994c9/标准75c13079)reconcile.none+bound closeConnections，而activate有Egress时包括none都会bind并写fingerprint，导致健康none中继被下tick关。父亲自读stage2lifecycle.activate/bind/revoke/reconcile完整相关函数确认源码矛盾。报告没有生产确切close/tick事件；不采纳其10–20%统计估计/对所有替代原因的绝对证伪；matchingguestStandaloneKVM绝非baselinefullproductionPASS。先无VM实际组件RED，不复用旧slot或3次probe。

007重开INPROGRESS/r3，撤回当前DONE（先前审批/门禁真实历史保留）；现九项DONE：006/008/009/010/011/014/015/018/019。已将原007计划加第三轮窄规格并新派implement-plan007-r3-none-relay，从stage2df769...隔离FF，scope仍bindings/reconcile/configure测试等原7路径，无engine/guest/12/schema扩域。健康none无持久proxybinding，bookkeeping回收不无条件断workload；原configure收窄/none/secret/fence未知身份撤权照旧。明确双向活relay跨两次reconcile/残留真实proxy撤销/身份deadline不变/非空RED→GREEN、sandboxdfullrace/vet/workspace lint/四实际既有RustfixtureGo，禁Cargo/VM。后续单独独立源码review，stage2COMBINEDPASS是当时基线门禁结果，不能掩盖新发现回归或当完整production。

### 012第三轮设计规格r2送新冷审

父完整读 `/tmp/plan012-r3-design-proposal-r2.md`（571行）。D1确定existing resource_recovery内部instance_cleanup kind、trustedtenant/S/V/B length-prefixed哈希确定ID+PK，原Aintent/marker在首次delete前持久，独立Bkey/历史；所有入口在!owned/selfreference之前按binding调度，freshrelease不换Source/ID/key/Attempt，三个delete边界×两次freshrelease/reopen/background验收。D2单一Resolution+临时Progress分别决定可继续B工作/可parentDone；T-active无论snapshot是否空都将runningB/holder/Resolution同TX，后继C时只认历史destination收尾不再激活。D3移除Settled全部兼容，公开旧receipt缺witness仍不补写；D4required-native离线JSONskip0/actualEngine/4真实fixture。

已新派 review-plan012-r3-design-r2，完整带原计划/r1r2source报告/初稿/首轮冷审/新回应/未解production。窄engineScope仍未正式扩域；不重新质疑已源码排除的startnoop/已批准010011依赖。父另明确focused新RED/GREEN后一次fullrequired-nativeRACE覆盖20Actual及010/011即可作宽门禁；无新失败不要把另一次01011focusedrace+fullnonrace机械重复，保留所有实际非空事件/零skip/240s新覆盖预算。007r3source修复独立继续，后续只接其ownpatch到012依赖，不能带stage2guest18污染b93匹配guest来源。无VM授权。

### 007r3 COMPLETE待独立源码复核

候选t3-2603d552/branch`t3/fix-none-egress-workload-revocation`，ownHEAD8d08ba0a1333df64f99d4c7cc74dc15103dcf7d2，parentdf769bbf.../treed773dfeb5c8c24bc1db090570bf3932179cc6ab8。父完整读报告 `/tmp/plan007-r3-report-t3-2603d552.md`、全部production增量及482行真实组件测试。仅bindings/reconcile/configure_egress_test 3文件+492/-6：共享bind.none直接真实revoke不创建proxy/fingerprint；healthyNone维护无条件revoke覆盖untracked残留，成功不切workload，真实revokeerror仍close+LastError；显式configure/fences未改。

执行者RED3顶层9子7FAIL2PASS→GREEN全9；真实Service+SQLite+Manager+HTTP Upgrade/netPipe workload跨2显式维护双向通，claim/fork/tracked/untracked旧wideproxy真实关闭；none/narrow/secret显式configure旧workload和proxy仍关；actualManager retryable Direct teardown错误保持拒绝，显式retry恢复。唯一fullsandboxdrace44.421s/134top122sub0skip/4actualRustfixturePASS，vet/workspace lint0/diff过；两次lint格式/runner GOWORK误设已披露保留，未重跑race。旧14提交/其余源码/refs/fixtureSHA保留，无残留process/mount/VM。

已新派review-plan007-r3-none-relay完整原brief/原r1r2finding/新诊断/响应/未解项。需独立focusedrace+旧fence/真实proxy/healthrelay覆盖；无新疑点不机械重复44s全suite。父提出尚未判bug的额外检查：maintenanceRevokeerror只写LastError且saveSandbox忽略错误，在真实Manager部分revoke失败+SQLite写失败时DataReady.none能否仍保持拒绝；结合manager实际revoked/direct责任/NICoff判证，不凭假设报安全问题。当前007仍INPROGRESS，SOURCECOMPLETE≠APPROVE，历史生产EOF仍LIKELY。

### 012r3 DESIGN APPROVE，正式实施已派发

父完整读 `/tmp/plan012-r3-cold-design-review-r2-818ffa57.md`。D1-D4设计异议0，N1明确普通同实例A terminalfailed仍走既有synthetic compensation（不无限重放failed publickey），N2新focused GREEN后一次fullrequired-native engine race，去重复广测但保留5new/9fence/20Actual/全suite。010/011三个source变化可显式接入；报告称3741/0bed为canonical实际为已批准010原270eb/7df的副本，实施采用原270eb→7df→5115并验证整个engine等于approved011，不两套重复。

父正式写入 `plans/evidence/012-r3-approved-contract.md`（完整自包含基线/Scope/witness/单一Resolution/Progress/uniqueCollector/Aintent/Bkeys/引用/active与reclaimTX/故障矩阵/门禁/清理），主012计划首段设为必读、正式调和唯一engine5路径例外与010/011依赖，旧槽位段标历史。仍禁engine.goadmission/backend/launcher/GC/guest/protocol/egress/DDL/deps/publicrequest/真实迁移；不会把engine禁令整体撤销。012仍BLOCKED/P1未闭，源待实现/新复核/最终完整production。

已新派implement-plan012-r3-result-handoff，新的隔离分支FF8892e5d继承r2，再pick010270eb/7df/0115115，映射/engine完整snapshot先记录。先RED actualEngine缺witness和原release-first两故障，再生产witness/严格fixtures/单一责任交接；完整D1/D2/ordinaryA/foreignC/rootowner/384vs320精确矩阵及全部旧回归，组件式而非helper单测。007r3own8d08…尚待独立sourceAPPROVE，不偷接未批准补丁；本项engine/ledger可独立推进，父批准7后显式steer自身补丁，最终production组合必须含7修复且不带stage2guest18。fixture使用b93匹配的既有d7fc…仅Go阶段，不Cargo；native cachedclosure、offlinecachedGo/正常workspace lint/全suite零skip遵循合同。无VM/宿主/真实DB/父source操作权限，当前无新grant。

### 007r3独立REVISE，一项P2转r4

父完整读 `/tmp/plan007-r3-review-t3-56da2195/report.md`、完整admission_counterexample_test.go及DataReady相关完整代码。独立复核t3-56da2195精确HEAD8d/tree d773与来源保持；16focusedrace/35sub15.169s及vet/lint过，原fullrace44.421s只复核未重复。健康none修复确有效，但诊断UPDATEABORT/RAISEIGNORE时none DataReady跳过bind/revoke，只依赖未成功保存的LastError，真正新HTTP101 relay可双向传输。actualManager retryableDirectteardown两故障RED/save_ok对照PASS；进一步生产Service.InstallDirect路径network-off成功/network_policy撤销失败+诊断ABORT也RED。最终两组overlayraceexit1/3.054s，无DATARACE。noneNIC始终关、Managerproxyendpoint已closed，仅待directcleanup；**未证明旧出网权限可用**，不夸大P2为已证egress bypass。

新派implement-plan007-r4-admission-revoke从8d候选隔离FF继续修订，最小DataReady.none在sandbox锁/二次fence后经共享bind/revoke实际确认，错误直接拒绝而不依赖诊断写入，不另加状态/schema/权限；成功cleanup不切健康workload，显式configure及其他LastError/fence不自动清除。先原overlay精确RED→正式ABORT/IGNORE/actualInstallDirectGREEN，保留r3九场景/旧七组矩阵，再一次fullsandboxdrace+4actualguest18fixture/vet/workspace lint/diff/有界连接handler清理，无VM。执行者不能批准自己，后续新独立review。

已向正在实施012r3的原task steer依赖状态：8d仍未批准，r4另task修DataReady，不自行改007/不接整棵stage2；012继续自身engine/ledger，父等7源码新批准后交付8d+r4自有链。当前007仍INPROGRESS、9DONE、012BLOCKED，父source未改、无VMslotgrant。

### 007r4 COMPLETE，新独立复审中

父完整读 `/tmp/plan007-r4-report-t3-33bfef09.md`、生产10行增量及371行正式HTTP/SQLite/Manager/InstallDirect测试。候选t3-33bfef09/branch`t3/fix-egress-revocation-admission`，own1c1e735e2227fb415a6a3d5db5884de58d3ca7a3 parent8d08ba0…，treef349f51dbd6d8dc3794a8353b3752c99c0d549d8。DataReady.none在原锁/fence/生命周期/credential检查之后经共享bind(ctx,b,"")→revoke，错误直接Conflict，不清诊断/fence，无新状态；nilManager共享guard正常。仅lifecycle/configuretests+381，其他来源保留。

执行者原reviewoverlayraceRED→GREEN；正式manager/install_direct×save_ok/ABORT/IGNORE，原8d最终正式RED为两save_ok PASS/四fault FAIL/nil正常PASS，修复后均GREEN；连续两轮actualDataReady+HTTP101拒绝/cleanup后恢复并保持身份/Deadline/NIC关闭。唯一fullrace45.926s/136top128sub/零skip/4actualRustfixture，原256PASS名全保留，vet/正常workspace lint/diff过，首lint仅格式错已披露。实际旧生产EOF仍LIKELY，不冒充新VM验收。

已新派review-plan007-r4-admission-revoke携完整原brief/先前P2/原overlay/r4响应/未解事项；要求整个r3+r4来源/scope及新旧focused实际race与overlay独立GREEN，复核精确fullrace日志不机械重跑。当前没有APPROVE，不向012交补丁；新复审过后仅8d→1c两个自身增量（mbox亦只二项）接入，不能合入整棵stage2/guest018。所有task无VM，旧grant仍expired。

### 007r4新并发P2，r5正式收敛最终接入边界

父完整读 `/tmp/plan007-r3-r4-review-t3-cb306882/report.md` 与concurrent_fence_test.go，亲读DataReady/Submit配置guard及relay/preview全部相关函数。独立原ABORT/IGNORE/Service.InstallDirect overlay2top3subPASS4.225s、18focused41sub17.397s/旧新矩阵过，原P2确已关闭；新实际Manager cleanup gate期间生产Submitconfigure提交pendingfence并closeConnections，随后旧admission仍返回且在close之后trackHTTP101双向bytes。最终counterraceexit1/2.240s/1FAIL/noSkip/noRace，实际Manager已清/NIC关/身份deadline不变；仅证接入时序，不证旧egress继续可用。精确HEAD1c/tree f349/来源不变，fullrace136/128日志只复核未重复。

原因明确：DataReady最后fence查询在blockingRevoke之前；Submit只需configuration:S而不需S锁，仍可持久fence；实际HTTPrelay与previewForward在DataReady之后只验running/lease/VM、不在track处核对fence。父正式把007Scope窄加http.go这两个final连接消费者及必要service内部helper（无新endpoint/capability格式），完整规格见007第五轮。模型要求post-revoke重查，最终短configurationguard fence+登记与Submit同序，外部Agent/Manager/engine等待不放guard内；S→config→ownershipSQL，无configure新增S倒序。fence先：拒绝/close未交付upstream、不101/不转发缓冲bytes；track先：随后configureclose覆盖。健康成功cleanup不切workload、旧P2与全部fences保留。

已新派implement-plan007-r5-fence-registration带完整原brief/所有finding/响应/未解项，从1c候选隔离FF，先原并发RED→GREEN，再cleanup/Connect/Forward gate期间Submit、先登记再关闭、健康relay/preview与全部旧矩阵。必要一次sandboxdfullrace/4actualguest18fixture/vet/workspace lint/diff来源与有界清理，禁VM/Engine广测。12实施task已steer7r4未批准/7r5独立修复，继续12本体，不采8d/1c或整棵stage2，父后续只交获批纯007链。当前9DONE/007INPROGRESS/012BLOCKED/无新VMgrant。

### 007r5 COMPLETE，独立源码复核中

候选t3-cdc262ce/branch`t3/fix-egress-admission-fence-race`，own2a8e9c084113272b55c9990adf167f5f61807a15 parent1c1e735…，treebfcd2a0515f7ae6074ad3dd40e501057953a39f7。父全文读 `/tmp/plan007-r5-report-t3-cdc262ce.md`、4文件生产全部增量与569行新增真实组件测试。service内部dataFence+lockDataAdmission：先快速failclosed查fence，再configuration guard重读fence/状态/lease/credential/LastError；DataReady在teardown/activation之后finalguard判定+AutoRenew；HTTP relay/preview在外部Connect/Forward后sameguard最终判定+全部track，Hijack仅localownership，101写/Flush/数据均guard外。S→config→localSQL/track，不把真实Manager/Agent/engine等待放guard。

执行者本轮指定原overlay在r4RED2.594s实际101→r5GREEN3.236sHTTP409；正式三场景原101/缓冲字节/preview请求bytesRED→GREEN，最终新增cleanup/cleanupautorenew/relayConnect/previewForward/registeredFirstrelay与preview六case，旧诊断ABORT/IGNOREactualInstallDirectoverlay仍GREEN。唯一fullrace41.746s138top134subPASS/零skip/无race，r4原264PASS名完整保留，4actualRustfixture/34sourcefiles+binarySHA同，vet/正常workspace lint/diff过，无自有process/mount，未VM。

已新派review-plan007-r5-fence-registration带完整原brief/各轮findings与回应/精确Scope/未解项；要求独立focusedrace+两个原overlay、全source来源与长I/O/锁序/错误cleanup核对，不机械重复41s全suite。当前只是COMPLETE，未APPROVE，不交012；将来只8d→1c→2a自身增量，不带stage2/guest018，最终production仍另有效grant下完整验收，历史EOF仍LIKELY。

### 012r3 COMPLETE，新全量源码复核与finish专项中

父全文读 `/tmp/plan012-r3-handoff.md`、新门禁摘要关键事件、全部production diff（engine types/finish、sandboxd lifecycle/quota/recovery）与fixture增量。t3-f3e87a98/branch`t3/resource-quota-ledger-r3`，r2FF8892...后010270→3b0195209238e671b11287d2edbd81289d7f24f8、7df→32458d99512dfbab93f765b3376e9f744f364052、0115115→postdepsBASEdbdbecdf2994df64f8e79c2db3291d48ccd9405c/tree1bc325ad940fcdf0d9aa0a83da2815064e9f22c7；接入前engine与approved011全同。OWN8729475fa9a6b057ae5b58c8f60474933dac12a2→HEAD2c15f095ec02e867f5592a2593a3b73c68d25efc/treeaf3de051610bcb6644004eaec3b0c7174e47ecce，11Scope文件+3475/-243，无未批准007链/guest018。

执行者actualEngine缺witness/原release-first两故障RED→GREEN，实际额外terminalfailed allocation dependency泄漏与release-before-destination先RED再第二commit修复。ResultInstance原子finish/单一Resolution/Progress/冻A/唯一Bcollector/exactBactivation/active与reclaim事务均交付。新5Engine/28Ledger加原18完整保留，D1六case/D2八case/SQL十case/foreign20case/keyrefs四case/intentmarker两case/terminalfailure五case与384/383等式具具体事件。唯一engine required-native全race194.606s/442PASS/34required/skip0；最终sandboxdrace118.315s/321PASS/28Ledger/4actualRust/skip0。sandbox另外两次113.316/118.875均因新真实RED与新改动后合理补跑，非机械重复。vet/lint0diff过。

父已新派review-plan012-r3-result-handoff带完整原brief/r1r2P1/批准设计N1N2/r3响应+额外发现/未解007与production。独立focused+实际overlay挑战全部关键边界，不机械重复194/118s全suite；须核明engine broad后最终format-only的语义等价，不声称字节全同（sandbox最终race与提交SHA一致）。fixture为b93既有d7fc.../33sourcefiles，不是stage1guest18的ce62.../34，未Cargo；refs报告只明确index保留，review需不夸大workingdiff证据。

另基于父实际读finish代码的新未解问题派review-plan012-r3-finish-atomicity：tx.Exec UPDATE无RowsAffected，SQLite RAISE(IGNORE)可能nil/0行而提交owner/lease与status/witness半状态。仅假设待实际EngineSubmit/Wait/reopen/test-ownedSQLite反例，不预判finding；专项不改源码/不复跑全native/无VM。新结论将随下一源码修订完整交接，不用后续消息偷偷延续review round。

当前007r5与012主审/专项均active、无VMgrant，012仍BLOCKED/P1未闭/013不放行。源实现未采用8d/1c/2a未批准链，父source未改/无mergepush，现9DONE。

### 007r5 SOURCE APPROVE，恢复十项DONE

父完整读 `/tmp/plan007-r3-r4-r5-review-t3-e680e7d7/report.md` 及关键audit字段；已读全部r3-r5source/test增量。findings=[]，HEAD2a8e9c0.../treebfcd...；真实20top47subfocused18.964s，两原overlay3.696/4.414，新的最终fenceSQL读取故障3case4.560s（实际relay500/preview502、无payload/登记/续租，上游关闭），skip0/race0，vet/workspace lint0/diff过。全138/134及4Rust原41.746s日志独立解析，264旧PASS保留，620普通源逐项一致；refs各自初始9vs执行者10分别保留，不能跨checkout误称一样。父额外git重核engine/guest/protocol/Cargo/scripts/flake与十项基线完全一致，diffcheck过。007恢复DONE；当前十项DONE。

获批纯7链8d08ba0a1333df64f99d4c7cc74dc15103dcf7d2→1c1e735e2227fb415a6a3d5db5884de58d3ca7a3→2a8e9c084113272b55c9990adf167f5f61807a15。最终7分支已含17commit的十项批准source，engine/guest仍前次组合证明。本批准不等于历史EOF已确证或新productionPASS。

012主审与finish专项继续看原2c精确快照，父不改变其审查对象。另派prepare-plan012-approved007-combination在新隔离branch仅FF2c候选+pick批准7三ownpatch；全路径/patchid/engineguest来源核对，遇冲突STOP不随意改。此为provisional组合，12仍未SOURCEAPPROVE；不急做昂贵全suite，待两路审查结果如需12修订则在明确组合上新任务实施。无VMM/生产/新grant/父mergepush，guest018不引入012。

### finish F1/P1确认，012r4窄修订与provisional组合

父完整读 `/tmp/plan012-r3-engine-finish-atomicity-review-92d9f7ee.md`。SOURCEFINDING/P1限定要求的SQLsilent-zero-effect模型：actualEngine SQLite IGNOREstart/restore×VMwrite使terminal/witness已固定而owner残留，去故障重启exactBdelete仍Conflict；×terminalwrite使owner清而oprunning无witness，重启固化ownerConflictfailed/B还在；实际preCandidatefailedrestore忽略leaseDelete使journalLeasefalse/真实reader1，重启snapshotdelete阻挡。新5caseRED原6AtomicABORT/commitGREEN，raceexit1 wall10.986/2.920 skip0/noRace。未声称生产默认schema有trigger/已发生事故或攻击；确为批准原子性合同缺口。

新派implement-plan012-r4-finish-effects，以2c原candidate隔离FF，**仅engine/operations.go与result_instance_test.go**验证必需SQL写入RowsAffected/实际效果，异常rollback完整completion。保留stagedpublish/durablereload/oldterminalbytes/010blockedlease/011边界；正式IGNORE五case去故障同key/reopen无extraLaunch、失败lease正常释放/snapshot可删，不把IGNORE换ABORT。focusedRED→GREEN后因engine新修改一次fullrequirednativeRACE（240s/newproof+9fence+20Actual+全suite，skip0）、vet/workspace lint/diff；sandboxd未变不重复118s。012主审仍原2c，未包含该未来fix；父后续整合主审发现/finishfix/已批7链再独立复核与组合验收，不能仅本专项修复就标全APPROVE。

provisional准备任务返回PREPARED，父读完整 `/tmp/provisional-combo-0a11ebfe/report.md`并独立git核engine/guest/protocol/Cargo与2c全同/diffcheck。t3-0a11ebfe/branch`t3/prepare-provisional-source-combination` HEADc97543e68e0ef0da1fff75470aeed3be9c5fb0ba/tree4f4a9719aea9bbc2b2aef1740224845739e1f63b；7纯8d→92249e39b0527ce3079900c56cc69c3b913ee2fc、1c→fb5c0578cd6aa97d725e40959107fd8b049a1deb、2a→c97543e…，3整体/逐文件patchid与增删bytes同，无conflict/no手工修复。仅预期6路径，12release/activation去DataReady后bytes完全同，33guest/protocol/Cargo+b93fixture SHA保留；未测试/build/VM。审计初把2未初始化gitlink向上解析parent误报STOP已更正测量，10真正refs原样保留，原错误日志保留。

该组合不包含未来finishfix也不SOURCEAPPROVE，保持独立review快照不变；后续另任务pick获批fix到此或新base并验证。现10DONE/012BLOCKED/F1未闭/013等待，无VMgrant。

### 012r3主审REVISE：成功结果已删的P2并行修订

父全文读 `/tmp/plan012-r3-review-f3d6f35c/report.md` 与完整review_service_test.go。独立源码全11文件/全部新test/28Ledger确认原capture/restore-releasefirst/P1矩阵保留且GREEN，engine145PASS/90.497s、Ledger111PASS/68.960s、vet2/workspace lint0过，新正向collectorBackingCatalogAbsence与actualEngineRestoreLeaseTransferFailure保持fence也GREEN。唯一新P2：quota.go NotFounddestination只处理failed。successfulR→B的transport/真实SQLite故障窗口内先合法Engine ExpectedBdelete，publicrelease见V可信不存在清metadata；lateadoptresult_known无destination，recover4/reopen/recover4/freshrelease后CPdelete/recover4仍SID192+undelivered1，slot0/restore1/delete1/publicfailbytes固定，新claim在引擎前被拒。两subRED raceexit1/3.627s无skip/race，不等同原foreverliveB/slot1P1。

证据限制明确：原engine194.606s broad缺pre-format源码/hash，own.patch重建只得最终格式化版本，**无法独立证明原广测前后AST等价/字节相同**。保留该缺口，不接受“只format”口头等价；finishF1修订会在实际新源码绑定required-native全race与前后hash，形成新验收，不伪造旧证据。独立最终145focus已覆盖5new/9fence但不当native20全证明。

新派implement-plan012-r4-absent-success，以已准备provisionalc975（2c+批准7）新隔离FF，只sandboxdquota/recovery必要路径+正式quota测试：完整successreceipt/witness/可信S/tenant/history+真实NotFound，原子reclaimedDest/V账本backingSlot/terminalSnapshotDep退休，支持release已清VMID；queryerr/foreignC/missingwitness/其他active依赖保持closed，no吞错/Done/no新restore/no二次delete。先原transportSQLRED→正确收敛GREEN，CP在192/slot0→删后0/claim成功，旧取证ledgerRejectClaim终点改回归恢复后成功、不删故障与次序。真SQLdest/dependency失败重启仍原子收敛，原28Ledger+批准7全部47sub+新case，4actualb93fixture含在一次最终sandboxdrace，未改engine不重复194s。

engineF1修订任务仍独立只两engine文件基于原2c；P2任务基于c975并无engine改动，后续父精确pick两个获批ownfix到同一组合/新独立review+验收。无并行改同checkout，主审原2c快照不改。012仍BLOCKED/P1P2未闭/013不放行，10DONE，无VMgrant。

### finish F1窄修订COMPLETE，新独立复核中

父全文读 `/tmp/plan012-r4-engine-finish-37fa8d75/handoff.md`、全部operations生产delta与486行actualEngine新测试。t3-37fa8d75/branch`t3/engine-finish-atomicity-fix`，own d5654123c00ed016211115e4433f3959e47ff9e0 parent2c15f095…，tree46a46419382d3b6d28235287c5b91923c506cb84；两file+517/-9。requireCompletionWrite校验result非nil/RowsAffected可读且恰1，VM/terminal/010prefence写及条件lease精确operation+requestSID delete都必经，错误rollback不publishstaged；成功lease早已转backingflagfalse不误要reader。其他finish/010011/实际材料证明未改。

五实际IGNORE组件在旧2cRED→新GREEN：故障时op非terminal/no witness/owner+candidate+lease一致、trigger记账INSERT也rollback；解除故障同key/reopenB交付无extraLaunch/exactBdelete可行且termreceiptbytes固定；preCandidate失败保持backendfault、SQL解除后fixedfailed/noWitness/reallease0/snapshot可删。原6Atomic ABORT/commit及全部5proof保留。

最终一次requirednative全race195.661s/448PASS/36required（5oldproof+2IGNOREtop+9fence+20Actual）skip0/race0，原442名单全在。此新run前全部614tracked-source hashes与最终commit一致，建立新source绑定，不假称补回原r3preformat证据。vet/正常workspace lint0/diff过，无sandboxd/Rust重跑（两file仅engine）。执行偏差：先前fullrace为修lint测试格式/同步，在44.954s主动TERM本任务组exit-15，明确未当PASS；首次lintrawlabel覆盖只保留transcript/历史标识，后门禁raw全保留；最终source全race唯一过。所有记录own进程组含abort/controllers均Wait无残留，refs旧值保留/并行新refs列明。

已新派review-plan012-r4-finish-effects完整原brief/F1五RED/old6GREEN/response/overallP2及证据边界，只两file独立focusedactualrace+full195JSON/source绑定核对，不机械重复宽suite；源码仍未APPROVE，不标F1闭。sandboxd成功Babsent P2另task在c975组合继续，最终父另集成两个ownfix与7然后newreview/production。无VM/新grant，10DONE，012BLOCKED。

### F1 SOURCE APPROVE；成功Babsent修订COMPLETE，组合后复核

父全文读 `/tmp/plan012-r4-review-7f7d1ee4/report.md`。SOURCE APPROVE/findings=[]限定d565412...F1两file，独立7proof30PASS/18.482s、额外010prefence/terminal两阶段IGNOREoverlay3PASS/5.293s，精确source448广测/36required/614file manifest原始JSON逐条核，vet/workspace lint0/diff/source-index-preservation/ownedcleanup过。五原IGNORE都实际收敛无extraLaunch/terminalbytes固定；正常lease转backing、失败/blocked两阶段语义保持。父据此APPROVE F1源修复，不等同整体12DONE。

父全文读P2执行交接 `/tmp/plan012-p2-8ce1b630/handoff.md`、全部production189quota/17recoverydelta及431行组件新test。t3-8ce1b630/branch`t3/fix-quota-reclamation-proof` ownadbd61a2d416328387c183e0c96c989bffeddd8d parentc97543e…，treef19cb6c0217c56bbdc4d3b5bc32b64bc6edb819d，三file+609/-28。成功exactwitnessB可信NotFound+frozen publicfailedrollback/S/tenant/shape/history校验，reclaimedDest+精确terminalDep+V版本/backing/slot+匹配metadata同TX；writeRecovery/DeleteCount/postcondition防ABORT/IGNORE半状态，未交付allocation继续阻止genericreclaim抢先退费。无新engine/API/DDL/key/guest变化。

原两故障overlaybaseRED→正式4ordercaseGREEN，24实际SQLABORT/IGNORE边界与18foreign/query/owner/dependency拒绝合46sub；原28Ledger+批准7全47sub+4actualb93Rust含在一次fullrace398PASS/命令169.262s（recorder170.507s，主pkg167.562s）skip/race0；source-before-full实文件与finalcommit相同，vet/正常workspace lint/diff/33b93inputs+binary保留。已读披露开发测试trigger错JSONpath/lint格式及runnerGOWORK标签修正，最终sourcefull只一次。

已派prepare-plan012-f1-p2-final-combination，仅FFadbd+pick已批d565，自身branchsource-only无测试，要求wholeengine=d565/wholesandboxd=adbd/其他entries不变、patchid/source映射。当前wait预算60s超时不是取消，任务仍运行，父只读一次needed-midturn状态无重复启动；待准备结果后新独立P2+最终精确组合SOURCE review，旧审查快照保持。P2尚未批准、F1已批准；10DONE/012BLOCKED/013hold，无VMgrant/production动作。

### 最终精确90组合 PREPARED，P2与整体源码复核中

父完整读 `/tmp/source-combo-ac14c9c0/report.md`，原adbd候选FF+仅copy已批d565→90f68b616faf7a1a6c82423674c0d27a2dda9bd6，parentadbd61a…/tree64a4fc8940e625587a5db4417858aa7e3e04e43b，t3-ac14c9c0/branch`t3/combine-approved-engine-source`。无冲突/无手工改动，F1patch-id9ff95...及diffbytes同。整个engine=d565、整个sandboxd=adbd，其余entries=adbd，33b93输入/旧refs/四来源worktrees保持；未测试或runtime。父独立git核HEAD/tree及engine/sandboxd/guest/protocol/Cargo等价、diffcheck过。

已新派review-plan012-r4-final-combination在新隔离FF90精确snapshot，完整原brief/全部r1r2r3P1P2/批准合同/F1已批证据/P2响应/7来源/未解production。目标独立46newsub真SQL+oldP2顺序、unknown/foreign/realdependencies、整个最终来源联合，必要focused组件与额外overlay，不机械重复同字节195engine/170sandboxd全suite。若P2闭且组合无新问题，可SOURCEAPPROVE90整体12（不等于DONE/production）。F1/7先前批准只按exactsubtree证据转移，不伪造旧engine194s格式化前记录。

父并行做只读生产材料preflight：pinned CH0179...及kernel9728...实际SHA匹配receipt，新guestagent2463...匹配，newOCI index c393...与六descriptor逐个SHA/size匹配；/dev/kvm及/sys/dev/block/259:0仍存在。该检查只读，未VM/daemon/IOpressure/host配置，不是production或新IOenforcementPASS，未发slotgrant。当前sourceReview未批，production旧grant仍expired/槽FREE，待源码通过后另交新grant/新artifact跑原全门禁。现10DONE/012BLOCKED/013hold。

### 最终90 SOURCE APPROVE，新的唯一production grant已派发

父全文读 `/tmp/plan012-final-review-65a243e3/report.md`。SOURCE APPROVE/findings=[]针对完整90f68b616faf7a1a6c82423674c0d27a2dda9bd6/tree64a4fc8940e625587a5db4417858aa7e3e04e43b，engine=d565(F1)与sandboxd=adbd(P2)全subtree、批准7纯链patch映射/增删bytes、其余来源逐entry核验。P2实际49PASS/50.221s、critical44/29.370、原反例GREEN+partialIGNORE只漏restoreD及独立SIDactive/dependency引用overlay10/12.801，skip0/fail0/race0，vet/lint0/diff过。独立398sandbox/448engine原始全JSON、精确source/copies核明，未重复宽suite，旧r3preformat限制如实保留。源码P1/P2已在最终获批组合关闭；012仍BLOCKED仅因完整production未过。

父已创建新 `plans/evidence/production-verification-012-final90.json`，task/唯一slotOwner=verify-production-plan012-final90，slot_grantedtrue/handoff_expiredfalse，之前r2cleanup已明确释放，所有旧grant失效。源90/tree64a4固定、源码APPROVE报告引用、已核pinnedCH/kernel/new2463guest/原base4layerOCI6descriptor、IO259:0wbps1048576及真实旧O_DIRECT证据、新artifact精确`/tmp/p012-r4-production-90f68b6`（父确认不存在）均交付。原guest512/disk512/vCPU1、VMM640MiB、outer8G/swap0/tasks2048不变，原Info/namespace/capBnd0等managedproduction要求保持。

新隔离test任务FF90，仅一次原make production-e2e，必须全部liveadoption/完整MicroVMLifecycle/golden-warm/realTCP实际非空PASS，禁多次跑到绿/额外VMprobe/修改assertions/调预算；缓存11toolNixfarm4h0h...只可本地resolve，validNixmaxjobs0/substitutefalse/builderssubstitutersempty，Go原本项目编译允许，无Cargo/依赖buildfetch/Nixdevelop。输入源/许可SHA前后保留；仅freshownedunit/privateNS/cgroup/daemon资源可用，白名单过程与内存/资源/身份观测，不读rawDB/环境/secret/statearchive。结束正常清理ownunitNS并Wait所有进程、核unit/cgroup/scopes/PIDstart/mount全无、显式SLOTRELEASE后父expiregrant，绝不复用true旧文件授权重跑。

此时唯一VMslot由verify-production-plan012-final90持有，其他任务无VM授权。12未DONE/13不启动，父source未改/无mergepush，现10DONE。

父在production等待期间只更新013前置漂移说明，未派实施：精确90 baseline与ResultInstance/单一Resolution/写入效果/007最终短guard保持；31Ledger/Configure47sub与b93既有fixture不丢；实际SBbaseline167.562s加新模板覆盖，013fullrace合理预算240s、不跳旧用例；去除make integration隐式Cargo，四actualGo fixture要求保留。013仍TODO直到012完整production复核过；本次无额外VM授权。

### final90 production仍BLOCK：新auto-resume NotFound，grant已过期

父全文读 `/tmp/p012-r4-production-90f68b6/report.md`、slot-release.json及cleanup关键unit/cgroup/PID字段、原run.BoOxDMNz/test.log与microvm源码177–291阶段。测试隔离t3-cf1d30e7/branch`t3/verify-production-acceptance` FF90/tree64a4，无新commit。唯一makeexit2/wall53.210s/testexit1/top48.58s，actual3名0skip；adoptionPASS4.11s原machine-id/liveShell/instance不变，MicroVMLifecycleFAIL20.18s。此前checkpoint+fork2两个branch身份/env与pause断言已通过，新的失败在wg.Wait完成后收8goroutine Connect+info errors，line227 not_found。原log不能区分goroutine/ConnectvsInfo/内部对象，**根因UNPROVEN**；后继resume身份/shell/rollback/zeroWake/branchsurvival、golden/warm/TCP没到达，不能标PASS。

四真实managedVMM稳定status/exe/ns样本全部5capset0/NNP1/seccomp2/降权Groups空，1vCPU/VMM640/guest512/disk512/outer8G+原io.max等不变。SourceVMM640触顶max283、outer8G触顶max862（包含archive），oom各0不足以确定原因/排除内存影响，禁调大预算。原旧Info兼容与forkEOF此次未在旧边界复现，不把它们转为新错误归因。

616源码/index/refs与531输入SHA/size/mode/symlink前后相同。归档保留但正文/成员/SQLite/env/config/activation均未读。独立cleanupunit1794007083-137900notfound/inactive/dead/MainPID0、cgroup/vmscopes无、60组PID/starttime均结束、无本轮hostmount；执行者EXPLICIT SLOT RELEASE、无retry。父即将newfinal90grant置slotfalse/currentOwnernone/handoffExpiredtrue，保存cleanup/slot-release receipt，当前槽FREE。该保留true历史内容不授权重跑；任何新真实运行须另有效grant。

新派diagnose-production-auto-resume-notfound-r1：readonlyfresh白名单日志+实际90 pause/resume/8connect/guestInfo typedNotFound producers/ownership/snapshot/retirement/lock时序，原父source/rootgrant不改，禁止VM/旧资源/productionDB/archive。可在自身隔离FF90以/tmpoverlay做bounded真实Service/test-ownedSQLite组件probe，不implementation/不广测，不因当前max/oom计数猜测；返回VERIFIED|LIKELY|UNKNOWN与最小修复/后续instrumentation证据。012仍BLOCKED/013hold/10DONE；Source90原批准事实保留，等待真实失败根因分析，不预判源码哪个组件要重开。

### auto-resume组件VERIFIED：production root owner缺普通tenant预算前置

父全文读 `/tmp/plan012-final90-autoresume-diagnosis.md`，实际核对source90 Authenticate(rootToken)→Roottrue/Tenantroot、PutTenant/store、quota普通查询和production setup300–549与microvm全相关阶段。root认证/新库不自动tenant行，productiontest无PutTenant；root模板/claim/capture/fork/pause绕过额度拒绝但资源收费，DataReady按既定合同Execute OWNER普通Principal，012新增resume D预留触发tenants/root查询缺行NotFound，先于saveOp/resumeClone/owner检查。不能把此错归010COW/0011instance或7fence/缺snapshot。

独立own/tmp组件actualHTTP SDK→Authenticate→Service→新SQLite，严格idempotency/ExpectedInstance/clone空ReqVMID/immutableResultInstance fakeAPI+netPipeProto Agent，完整claim/checkpoint/fork2/pause/8Connect。root无tenant8typedNotFound/clone0/resumeop0/sleepSID存在；相同snapshot显式rootresume成功clone1；预建rootrow与alice对照八路→clone1/op1，race唯一exit0/7.616s三sub（失败机制诊断oraclePASS不是产品验收GREEN）。组件因果VERIFIED，历史productionrow仍未直接取证所以LIKELY，未读archiveDB/env/新VM。

父决定保持原批准政策：root仅跳拒绝且全收费，自动维护走owner普通有限预算；不自动传root权限/默认无限fallback/改变budget产品语义。此属于新计费前置下的**验收初始化缺失**：拟通过现有ROOTtenantAPI给原root-owned测试资源显式建有限budget，保留原root credential/owner与全部真实断言/shape/VMM640/outer8G等。生产测试文件此前不在012实施Scope，必须先精确推导与登记窄setup例外，不能擅自改测试或随意加quota。

已派design-plan012-production-owner-budget readonly提出完整adoption/lifecycle/goldenwarm/TCP独立D/SID/backing/cleanup峰值资源表+有限MaxSandboxes/MaxStorage公式、实际auto-resume预留恰好/低1MiB拒绝组件矩阵、root/选Bob代Alice/普通tenant/scopedtoken身份/owner/fence/zeroWake保持。报告中合成控制1<<20不得复制生产。Scope仅测试setup/必要组件fixture/有限预算说明草案，产品Auth/Quota/Engine/guest/SQL不改，当前仅设计未授权source或VM；新production等实现/独立review/newgrant。12BLOCKED/13hold/10DONE，所有旧grant已expired槽FREE。

### production owner预算具体方案返回，新冷审中

父全文读 `/tmp/plan012-production-owner-budget-design-a55f6dce/proposal.md`。建议原production首次ready后/首subtest前ROOT现有POST/v1/tenants建立原owner root有限budget：MaxSandboxes=1(adoption)+3(life)+2(goldenwarm)+3(TCP)=9；MaxStorage=D+(5D+4S)+(2D+S)+(4D+2S)=12D+7S，其中S=D+M、D=M512时13312MiB。此为固定场景分配身份**累计保守包络**、不依赖跨subtest即时退款，不是同时峰值/任意root请求硬限或所有物理资源已计费。lifecycle同时峰值4D+4S6144；auto paused2D+3S4096→reserveD后4608/4607；scaledD64/M128 auto768/767、Alice全lifecycle第二pause前1024/1023。Root管理身份不切换，普通root tenant token不因名字获得Root。

Source90尚无build/unclaimedwarm完整收费(013已知)，refill物理attempt次数可能随调度/故障变动无固定场景整数上界；13GiB不能当这些物理预算证明。副作用仍受原engine/VMM640/outer8G等限制，均不改。正常原root公共动作可合法绕过budget比较，精确拒绝只用owner普通auto/Alice，不设虚假rootpubliclow拒绝oracle。

方案Scope拟仅integration/production_acceptance_test.go setup/本文件boundedHTTPhelper+formula+imports，以及新无特权external sandboxd/production_owner_budget_test.go actualHTTP SDK/SQLite/strictfixture5组；原microvm/network文件原断言字节不动/无SDK产品API或Sharedhelper包。root manager+ordinaryroot/Alice/rootdefault或X-Bob代Alice/scopedtoken caller两shape exact/low，sdk.Connect不走HTTPtransport故X-Tenant-ID须测试raw真实upgrade；有限setup成功oracle省API先RED再GREEN，缺row负控制保持NotFound。生成tenantToken响应只decode白名单idlimits/不记录bodytoken/header。

已新派review-plan012-production-owner-budget-design携完整原brief/90批准/诊断/提案/原production源码，核算真实resource身份费用/延迟cleanup、有限budgetScope与令牌保密/矩阵可执行；产品Rootbudget语义已决定不另问用户。当前仅提案，productiontestsetup仍未正式扩Scope/未改源码，不以文字预算替代验收；240s新增组件fullrace时长预算不改物理或存储上限。12BLOCKED/13hold/10DONE、所有旧grant expired/无VM任务。

### 有限owner预算 DESIGN APPROVE，正式两测试路径实施中

父全文读 `/tmp/plan012-production-owner-budget-cold-review-dce02974/report.md`。blocking[]，9slots/12D+7S累计身份包络、4608/4607/768/767/普通Alice1024/1023精确边界与仅两test路径可执行；source90产品不重开。C1调budget保留已有普通Token避免轮换认证错，prod首次omitToken；C2两shape×6caller×exact/low全24leaf硬要求及低额度同state恢复；C3strictfake多versions/backing/immutabledeepreceipt、direct通过实际InstallDirect→当前instance policy/NICoff装再on；C4首轮maintain立即执行需显式quiet握手、gate不持fake全局mu等待DB。

父新建 `plans/evidence/012-production-owner-budget-contract.md` 全量selfcontained合同并在主012正式调和唯一setup例外：integration/production_acceptance_test.go初次ready后一次有限ROOTtenantAPI/formula/comments/有界HTTPhelper/imports，以及新增external unprivileged production_owner_budget_test.go。microvm/network/runner/产品API/Auth/Quota/Engine/SQL/SDK/guest/依赖全字节不动；prod原credential/owner/断言/shape/物理cap保持。设计不是权限绕过、root新默认预算或无限liveness承诺，13未计warm缺口明示。

已新派implement-plan012-production-owner-budget完整原brief/诊断负对照/提案/coldC1-C4/批准合同，隔离FF90，只两file自有commit。先actualsuccessoracle省setup RED→ROOTHTTP有限row GREEN；缺row负控制本来typedNotFound保持；五组含24主leaf+root低公共bypass分离、全资源identity重放、ordinary生命周期峰值/精确低1MiB拒绝、保守backing/queries/reopen。一次final240s sandboxdrace包含原31Ledger/7全47sub/四b93actualRust、privileged compile-only明确非真实测试、vet/正常workspace lint/diff/sourcecopies/有界清理。未改engine故不机械重跑195s。源码交付后新独立review及新grant全production，当前012BLOCKED/013hold/10DONE，槽FREE旧grant全expired。

### 有限owner预算 setup已交付，新的独立SOURCE review中

交付 `78bc04fc8715d18319ed4cba69aa92a30975f9e5`，direct parent90f68b6，tree `d8a15809daa2ad2f8f6271edc9f400ba955888ac`，worktree t3-f20b872f / branch t3/owner-budget-test-setup。父完整读 `/tmp/plan012-owner-budget-f20b872f/handoff.md`、production diff及新增1936行external测试；提交恰好两路径，production+65/-0与external+1936/-0，没有产品源变化。生产仅首次ready后既有POST有限root预算，9/13312、10s/64KiB/201、无body/token日志、原credential/owner/assertions/shape/caps保留。严格Engine保存多disk version/backing、固定deepcopy receipt/current-instance policy；direct经实际InstallDirect；maintenance第三次inventory外部quiet gate与实际pool阶段；RO新库观察非直接种账。

执行者实际同成功oracle省setupRED1.924s→finiteGREEN；五组、24主leaf、192首轮/96低→精确恢复连接、ordinaryAlice1024/1023、retained physical责任。唯一最终SB fullrace217.111s，430带Test字段的(package,test)PASS节点，原398全部保留，31Ledger/007全67节点/四b93Rust均过；六个有测试package PASS，sdk无test-files包级skip不能误报成testSkip。testSkip/fail/race0。vet/正常workspace lint/diff过，privileged compile-only零实际test；最终617blob/symlink与full前/后逐字节绑定，615旧源码路径原样。初期lint formatter与GOWORK runner误标已保留/修正，不算PASS。

父另独立git-tree只两路径变化/两个文件SHA/实际fixtureSHA正确，解析原始fullJSON确认430测试节点/六packagePASS、24leaf、testFail/testSkip/race0；最初验证脚本把无Test字段的package事件与测试节点混计导致assert失败，修正事件分类后通过，未重跑Go或改源。候选worktree干净、父产品源未动。新派review-plan012-production-owner-budget独立冷上下文、全文合同/原brief/诊断/C1–C4/回应/未解除prod门禁，重点fixture行为真实性、旧source字节及原始测试证据；无修复/VM权限，不重复未改217s全套或195sEngine。SOURCE批准仍待review；012BLOCKED/013hold/10DONE，无新VM grant。

### 有限owner预算 SOURCE APPROVE，新完整production独占验收中

父全文读 `/tmp/plan012-owner-budget-review-b7162bd3/report.md` 与decision.json：SOURCE APPROVE/findings[]，精确78bc04f/tree d8a15809；仅两测试路径，615原source/617副本/33guest输入全字节一致，90产品批准保持。独立focused45.804s/32PASS含全部24leaf与192+96连接，HTTP身份/receipt/policy隔离overlay3.939s/1PASS，privileged compile-only1.232s/零测试；原430广测及原398节点/31Ledger/00767节点/四Rust实际记录复核，testSkip/race0。自有进程/挂载清理无残留；父计划并发状态更新与T3新增checkpoint ref被明确区分，无源码改动或旧ref删除。

父签发前重新验证上次60组PID/starttime均结束，旧unit absent/inactive/dead/MainPID0、旧cgroup不存在；原CH/kernel/三guest SHA、OCI index及六reachable descriptor内容/size吻合，cached11tool entries存在、/dev/kvm与259:0存在、新artifact根尚不存在。没有把设备存在或io.max读回称为新I/O实测，沿用原O_DIRECT证据。

新唯一grant `plans/evidence/production-verification-012-owner-budget78.json`，owner/task verify-production-plan012-owner-budget78，精确78bc04f/tree d8a15809/source批准与原90产品批准。artifact根 `/tmp/p012-owner-budget-production-78bc04f`；只允许一次原始make production-e2e，无额外VM/guest probes或自动重试。1vCPU/512MiB guest/512disk、VMM640MiB/outer8G swap0tasks2048、IO259:0 wbps1048576和pinned CH/kernel/newguest/OCI原样。新隔离执行者FF78，不改源码/旧断言/物理caps/ownercredential；原runner创建的独占新unit/namespace/private mounts/daemons/VMM scopes与受限只读observer获授权，禁止实际DB/状态archive/config/token/env正文访问。全部lifecycle（含后半rollback/zero-wake/branches）、golden/warm、真实TCP必须实际执行PASS；原状态archive仅stat、不打开/列成员/hash。结束复核所有owned进程/units/cgroups/mounts并显式slot-release，父再expireJSON。当前此任务独占slot，禁止并发VM；012BLOCKED/013hold/10DONE直到完整门禁被父接受。

### 012 最终 APPROVE / DONE；013基于78的设计调和开始

父全文读 `/tmp/p012-owner-budget-production-78bc04f/report.md`、handoff.json、verification.json、cleanup/release收据，并从原test.log独立重算：一次make exit0/wall137.401s，顶层132.82s；adoption3.82s、完整lifecycle35.92s、golden/warm21.51s、真实TCP42.97s；5RUN/5PASS/skip0，所有必需阶段实际完成。原命令/requirements/物理cap/原凭据保持，617source/12gitlinks/10predirtyreferences前后一致，零source修订/额外VM探针/重试。精确78bc04f/tree d8a15809，t3-3c237f22 / t3/verify-production-owner-budget78。

父直接解析stable-managed-vmm原始namespace样本，全部13台身份/五cap全零/NNP1/seccomp2和原armed CPU/671088640 memory/swap0/pids256/IO259:0 1MiB限制成立；最初自有parser用vmm标签未匹配而assert失败，改读实际stable-managed-vmm分类后通过，非产品或原门禁失败。父重新查全部88PID/starttime结束、fresh unit absent/inactive/dead/MainPID0、owned cgroup不存在；收据PGIDs/mount残留空、状态归档仅stat未打开。接受EXPLICIT SLOT RELEASE，父已将owner-budget78 grant置false/none/expired并记录PASS/release/cleanup。槽FREE，无VM任务或重试授权。**012 APPROVE/DONE，11项DONE**；其历史所有失败及证据限制仍保留，根dev不合并/提交/推送。

父执行013完整Scope b93→78 drift stat并核对实际新增fixture：production_owner_budget_test FiniteHTTP仍断言build零费、unclaimed warm仅S；现有quota_recovery_test WarmBacking普通actor首次TargetWarm1、费用D+S=256未算warmD。它们正是13变化，不可要求缺口断言原样通过。父窄增两测试文件Scope，仅root actor/按真实身份资源等式修改账本断言/维护gate/有限预算说明，不删除5HTTP组/24leaf/192+96连接/31Ledger/00747sub/原安全行为。未来schema目前已动态+1，仍须新增schema5拒绝DDL前证明。

013唯一BASE为已验收78（含finite owner fixture），新派design-plan013-after-approved012只读源设计，输出/tmp完整数据/事务/锁序/receipt与unknown/buildslot/模板holder/warm→claim和fallback同预算原子交接及SQL ABORT/IGNORE矩阵、精确预算与root管理/ordinary自动refill决策。12D+7S不能继续冒充覆盖13新增build/refill全部收费意图；可保留充足原有限额度但需真实峰值/准入证明，或给出可审有限等式后变更。既有最新SB fullrace217.111s/430节点取代167.562s基线，13最终deadline由240到300以容纳新增真实覆盖，不删测试/不改物理cap；不机械重复未变基线或195sEngine。设计后新独立冷审，再交隔离实现；当前013 IN PROGRESS设计调和，产品实现未开始/无VM，017等待013，016外部门禁与20/21来源映射仍未解除。

### 013 DESIGN_READY交付，新独立冷审进行中

父完整读 `/tmp/plan013-design-t3-252d1250/proposal.md` 与audit.json至EOF，候选设计绑定78/tree d8a15809，617源码/索引/12gitlinks/既存reference及33fixture来源/binary SHA保持，无源码编辑、测试、构建、VM、委派或新commit。提案不构成实施批准：私有typed allocationContext/allocation_group、build_slot schema6、冻结create/start/capture与ResultInstance、非sandbox清理来源，warm Submit同事务选择/holder转交和fallback旧D→pending，专属resolver防generic提前退款，原012 canonical collector/Resolution与public历史保持。模板put真实Shape报价、旧新SID holder交接、TargetWarm root管理、ordinary owner refill及持久占位、全局template→pool→ownership/S→configuration顺序都有具体边界。

预算建议保留9slots/13312但改为实际在账责任包络：保守TCP11D7S=12800，包含未清旧stage身份、永久goldenS与一份后继refillD，buildD/slot在成功发布前已退休。不能继续用原12D历史集合声称覆盖全部013新收费意图；新fixture逐轮maintenance gate有意创建一份后继refill，原12D单独+build1/refill1，账费/退款凭据逐identity证明，5HTTP组/24leaf/192+96及全部旧安全边界保留。五新TestTemplateQuota groups、真实SQL ABORT/IGNORE/COMMIT故障规格与source绑定最终300s全SB门禁明确，未改Engine不重复195s。

已派新独立review-plan013-design-after-approved012全文冷审所有契约/原brief/回应/实际源码。父已读Engine admission/execute的ExpectedInstance非空才比较逻辑，created无B清理在外部start并发下的原子保护列为未决重点；不要把服务group从未dispatch自己的start当作跨Engine锁保证，需冷审给明确源据与最小修正。另一重点是预算11D7S是否真的涵盖unknown refill占位及各成功删除交付，不接受root旁路证明普通维护额度。Scope/SQL效果/009外部GetSnapshot gate/新激活exact B与原007 guard/所有collector资格也需批准。当前仅设计READY/冷审中，未授权013产品实施或任何VM，012DONE/11项DONE、slot FREE。

### 013冷设计REVISE D1/P1，最小修订已派

父全文读 `/tmp/plan013-cold-design-review-t3-f66831c7/report.md` 与decision.json。唯一D1/P1证实父B挑战：created/空instance查询后external start C可完成，Engine admission/execute都跳过空ExpectedInstance比较，原proposal74/135例外delete会移除C。提案内“build无B failclosed”与允许该delete自相矛盾。自己的group未dispatch/完整Config/Service ownership锁不是Engine原子guard。父正式采用取消所有新013空B自动删除例外，不扩Engine/API，不猜current B、不走generic orphan逃逸或换attempt。缺合法冻结成功非空ResultInstance时留D/build_slot、group/依赖并ManualRecovery；可信GetVM实际NotFound可按完整group及其它unknown责任只回收合格账本，queryerror/inventorymissing不可替代。

G冷审根据完整真实4prod子流程逐identity核算认可固定正常9slots/13312（11D7S12800包络），条件为buildD/slot发布前退休、G/W成功释放与target1 ready/pending/unknown cleanup持久占位；不承诺无限失败活性、不作为runtime PASS。A/C/D/E/F/H无新增阻塞。C1 gate park不等异步ready/group destination，需要独立有界等待；C2真实SQLite COMMIT error/完整回滚/连接可用/reopen原receipt收敛，不能把旧ErrInterrupted precommit label当实COMMIT；两项是必需规格。C3 identity manifest为可选组织建议。

新派revise-plan013-design-D1完整原brief/提案/冷审/回应/未决D1，在新隔离FF78只读源，/tmp交付完整R2proposal与响应/audit，去掉全部空B例外并统一collector资格/回收/slot/unknown/backup/fault/未来Service行为规格。新增external-start-C query gate后无delete尝试、不采B、不退款，真实外部删除后可信absence一次条件退休；ManualRecovery不能成Done或永久跳过absence观察。无source修复/测试/build/VM，父未改源。R2后新独立冷审再决定实施，当前013 IN PROGRESS设计修订，012DONE/11项DONE，slot FREE。

### 013 R2完整交付，新独立冷审进行中

父全文读 `/tmp/plan013-design-r2-t3-45a594bc/{proposal.md,response.md,audit.json,proposal.diff}`；proposal431逻辑行/62785bytes SHA1930d294642bf5c8df89e6115dc0fe6b33d967aaddd9b11d34a0c28317e4204f，50KiB截断后从346继续EOF；diff712行分段全文。R2完整替代首稿，作者未自批。FF78后617source/index/12gitlinks/10predirtyrefs保持，仅新增T3checkpoint ref单列；无source/test编辑、Go/build/VM、新commit或push。

R2取消所有新013空B自动VM删除/空canonical collector，含created-known、failed-start、warm clone/evict和selection fallback cold replacement的旧failProvision逃逸。BuildIdentity固定V/create/source，私有RecoveryGate observe_absence_only独立于单一Resolution，保D/slot/依赖、group不Done/不可lifecycle但仍显式扫描；原key晚成功receipt可核实B后进入合格cleanup，不能改public failed。可信实际NotFound仅完整证明的V版本/backing/必要slot账务退休，不清unknowncaptureSIDpending/源依赖；generic discovery和reclaim均先识别group，不走旧orphan/新attempt绕过。未来external-start-C检查delete尝试0/不采B/不退款；外部合法delete后同proof原子退休+ABORT/IGNORE/实际COMMIT矩阵。

C1 next-inventory park与异步cloneReceipt/readyRow/group destination分别等待，逐轮permit明确W/Q、不持engine mutex等DB，全部gate清理。C2真实deferredFK Commit error及durable/metadata/Resolution/gate/内存回滚、同conn正常TX/解除trigger/reopen原receipt收敛，不冒称precommit ErrInterrupted。G保留条件9/13312/11D7S12800，原12D+buildX+唯一Q=14 distinctD/7SID/9用户S；build退休/GW实际释放/target1持久unknown占位等前提未弱化。原五新组/31Ledger/00747sub/owner5HTTP24leaf192+96/四Rust及sourcebound300s门禁保持。

已派NEW独立review-plan013-design-r2-D1，完整原brief/首稿/原D1 findings/R2响应/未决关闭要求与source78，不能靠作者说已修或grep无空B自动批准。检查ManualRecovery跨重开/备份可继续观察、不提前退unknown责任、合法nonterminal仍继续原冻结步骤、无假新API或Scope、原authority/锁序/预算/SQL证据完整。结论前未授权产品实现或VM；012DONE/11项DONE、013设计冷审中、slot FREE。

### 013 R2 DESIGN_APPROVE，正式合同已采用并派发实施

父全文读 `/tmp/plan013-cold-design-review-r2-t3-19be6bf9/report.md` 与decision.json，DESIGN_APPROVE/blocking[]，D1设计层关闭、无新D2。绑定R2 SHA1930d294642bf5c8df89e6115dc0fe6b33d967aaddd9b11d34a0c28317e4204f：所有新013自动VM删除要成功非空immutable B/full资格，含generic discovery/evict/fallback/cold replacement；缺B gate保费/相应slot/依赖且继续原key/可信absence观察，unknown capture不能被V NotFound全清。C1异步receipt/ready/destination与inventory park分别等待；C2实际SQLite COMMIT error/完整rollback/同连接可用；C4 durable op/group/marker区分合法nonterminal和cleanup，C5三主体身份/slot不同，C6扫描不漏gate/逐资源回收，全部必需。G9/13312与11D7S正常在账包络条件保留，非runtime证明。

父再次执行b93→78全部Scope drift stat，核R2批准SHA及无active grant，新建权威 `plans/evidence/013-r2-approved-contract.md` 并在主计划设最高优先级，正式授权单个隔离executor只23Scope路径实施、自有scoped commit，父dev不动。实际旧430 raw检查发现唯一动态future名字schema-6在新schema6后应为schema-7；合同显式合法映射且要求保留同一future-rejection/原4/新增5及DDL前保护，不造schema6拒绝或默删节点。此仅验证标签解释，无设计/产品合同扩展。

新派implement-plan013-r1，完整可执行plan内联并显式绝对路径交付未提交父计划/正式合同/完整R2/原D1/新冷审与回复。只FF78不接stage2/guest018，先actual build RED、五组真实Service/SQLite/strictEngine与D1/C1/C2/C4–C6故障GREEN，窄调Rootactor/资源等式/逐轮gate，最终格式后source副本绑定一次SB300s保原430业务（唯一schema标签映射）/31Ledger/00747sub/owner5组24leaf192+96/四b93Rust。vet/正常go.work lint/privileged compile-only0tests/diff/source/index/refs/fixture/owned清理；无fetch/install/Cargo/Nix或未变Engine195s机械重跑。实施自有commit授权，不代父提交/合并/推送或改plans，不嵌套委派，不操作真实DB/archive/旧资源。交付后新独立SOURCE review，再fresh grant完整production；当前**013 IN PROGRESS实施中、尚无SOURCE/production批准**，无VM授权/slot FREE，012DONE/11项DONE、017等待013。

### 013 preparation数量误约束已调和，新的隔离续实施派发

implement-plan013-r1在t3-90582e1d/branch t3/template-build-snapshot-quota STOPPED且无own commit。父全文读report/audit/reference comparison与165行starter，并独立核全部617 tracked bytes/symlink等HEAD78、cached diff空、before/bound/stopped12gitlinks一致、新test SHA2b6b29...03499d吻合。初始9reference进入任务即如此：cubesandbox未初始化，历史R2为initialized HEADf5b949...，gitlink5741deb...同一；其余9实际状态保持。它不是Scope/API不可能。父在合同不当把历史数量10固化成通用要求，现明确纠正为每工作树实际初始集合/HEAD/status保全；不初始化/fetch/恢复Scope外reference，不以数量差异再停工。

raw index hash在试改store/recovery并撤回、status刷新后变化，但staged mode/OID无改，617actualsource完全78。合同现要求rawhash真实记录+semantic stage/path/mode/OID/Scope外entries保全，正常编辑/add/commit或stat-cache不等损坏，不恢复raw伪造相等。临时产品代码已撤回，不接半套schema6；唯一starter未提交留原处，旧证据只读。

父直接解析initial-red.stdout：BuildAdmission slots/storage/low-one-no-effects+父FAIL，CP handoffPASS/testSkip0，exit1/wall1.047s；不同模板第二build仍接纳，createGate账本0slot0MiB、255build接纳。真实RED成立、CP本已保守GREEN，非五list顶层算覆盖。其余三组仅starter，全部GREEN/D1/C1/C2/C4–C6/race/full/vet/lint/privcompile/Rust未完成，不能SourceAPPROVE/13DONE。

新派implement-plan013-r1-resume完整原brief/先前STOP原因/父响应/准备合同澄清/未完事项/R2批准及D1，新的隔离FF78，仅read/write显式导入starter并核SHA，原905/receipt不写，无旧commit可pick。补齐真实业务fixture/strictness与全部产品/测试门禁，不因reference实际9/10或rawstatcache再停；actualScope/material障碍才给具体证据。Schema6future标签映射、完整原430/31Ledger/7全47/owner24/四fixture/真实COMMIT/fullsource绑定300s/后续SOURCEreview+freshproduction要求保持，不机械重跑未改广测。当前013继续IN PROGRESS，012DONE/11项DONE、slot FREE无VM权限。

### 013 d3be730完整交付，新的主SOURCE与并发恢复独立复核

父全文读 `/tmp/plan013-implementation-t3-64481713/report.md`、handoff、最终verification gate；候选d3be730e257ad223e3103f1d284e59e15424f7a7 directparent78bc04fc8715d18319ed4cba69aa92a30975f9e5/treef5bd17c33db4a753da8244dcf10c8a157120ff69，t3-64481713/brancht3/template-build-snapshot-quota-1。唯一owncommit，19Scope路径+6766/-459（新测试2859行），Engine及禁止路径保持。父已读service/store/checkpoint/types/lifecycle/reconcile/warmfallback/生产setup/旧fixture/doc实际diff及部分新resolver，完整19路径和全部新测试由新主审全量独立复核。

最终fullSB race exit0/wall295.815s、509带Test(package,test)PASS；原430按唯一未来schema6→7映射全部保留，新五组79PASS、testFail/testSkip/DATA RACE0。父亲自解析完整raw JSON与原430对照，并逐byte验证全部618源码在当前worktree+source-final-before-full/source-final-after-full/source-final-after-commit三copy全等gitcommit，Engine subtree1ec6...不变、指定b93Rustfixture SHA正确。执行者focused79PASS34.932s、normalworkspace lint2.711s0issues、vet1.560s、privcompile-only1.588s零真实测试；4Rust实际执行/33input+binary、owner24leaf288frames细节交独立审再核。

报告保留两次主动终止full（约64/142s exit-15）非PASS、一次真实009query gate panic全race失败（约208s390PASS）与其618修复前源、早期120s timeout/lint/typecheck；少数前台recorder无exit/wall不补造。最终required gates及全source三份绑定完整，不把中止或旧FAIL算通过。另actualC6RED unknown nonalloc start Vabsent错误全退slot0/fee0→slot1/fee0直到late originalterminalreceipt的修复证据保留。不是全恢复正确性或production证明。

两个NEW独立复核已并行派发，使用新的实际task前缀 `node:delegated-task:command%3Amcp%3Ae4183005-5654-4cd0-b510-765384c1fa7f%3Adelegate-task%3A`，分别后接review-plan013-r1-source / review-plan013-r1-adversarial；不要用旧7adb前缀管理这些task。主审全文Scope/diff/R2/原D1C1C2C4–C6/2859测试并实际五组focused、关键旧回归、独立raw/source审计。对抗审独立实际Service/SQLite/strict公开Engine flow的/tmpoverlay，挑战warm转真实S后旧group/selection来源是否仍能错误清理、fallbackD→pending与并发release/claim、unknownstart/capture/SID/absence/SQL效果/固定B权限及保留费用。无源修复/父plan编辑/nesteddelegate/VM权限，不重复未变295sSB或195sEngine/Cargo。SOURCE APPROVE仍待两者证据；013 IN PROGRESS复核中，012DONE/11DONE、017hold、slot FREE。

### 013 独立adversarial REVISE A1/P1；有界修订已派，主审仍在进行

父全文读 `/tmp/plan013-adversarial-review-t3-a07822b7/report.md` 与decision、实际warm_fallback165/81、quota1849 validator、warmSelection结构，并独立解析final-probes.jsonl：7测试节点6PASS/1A1FAIL、testSkip/DATA RACE0、最终overlay SHA687255...5115f正确。正常ready warm激活失败→首次合法B delete transportUnknown持久rejected selection→仅fresh test-owned SQLite清空Deletion.Request.ExpectedInstance→Close→实际Service.Open接受→recoverResources发送1空predicate delete尝试。strict Client先记录再Invalid，实际V/B/slot1/storage256/原failed公开bytes保持来自mock拒绝，不是产品资格guard；没有声称普通tenant改DB或实际C误删。反例违反原R2持久损坏拒绝与固定B请求合同，父采A1 P1。

源码只核Selection.Identity当前B，既有validator核Group.Steps/Identity但未核Selection.Refill/完整Deletion/Stage/AttemptHistory，已dispatched请求FindNotFound后直接api.Wait可绕过。四独立GREEN交错有具体实际source/VMID/费用证明：stale pool观测后真实claim/pause/release/reopen不夺D；physicaldelete后IGNORE/实际COMMIT与并发claim/release无退款/重分配窗口；旧unknown capture同名新build后lateSID只清旧S；unknowncloneB→externalC/target0/SIDreplace/reopen后lateB拒删C，外部exactCabsent才回收。它们不抵消A1。

父task_status读取原review-plan013-r1-source仍working，未有最终结论，不取消该原d3只读主审。新派fix-plan013-warm-selection-delete-validation（同e418前缀），新隔离FFd3，Scope限warm_fallback.go/quota.go/必要recovery.go/现有template_quota_test.go，中央完整selection来源/原claim/owner/fullShape/V/B/immutablegeneration/request/key/BoundDispatchedProof/Stage/Attempt/terminalfailed历史校验覆盖startup、latest marker TX、receipt replay和实际dispatch前。不得只补非空检查、不按currentB重建冻结坏请求、不走generic orphan。正对照须包含合法预delete实际absence、unknown原key、合法failed历史前进、active/fallback旧warm身份与新replacement区分；负对照实际Service.Open与Open后runtime DB损坏均Engine尝试0/留费/immutablepublic。保留原独立probe与509业务场景。

基于已测295.815s仅余4.185s且新corruption/startup/replay/history矩阵，父将本修订最终Go fullrace deadline300→360s，合同及当前命令已调和；不改物理caps/生产时限/安全断言。新source before/after/commit绑定一次必要full+vet/正常workspace lint/privcompile-only/四Rust输入来源/cleanup，新独立SOURCE复核再新grant实际prod。主审若另发现由父新调和，不假称已过；当前d3为REVISE/A1修订中，012DONE/11DONE、017hold、slot FREE无VM权限。

### 013主SOURCE REVISE R1/R2/R3；四项合并同一在途修订

父全文读 `/tmp/plan013-source-review-t3-67912afb/report.md`、decision、实际独立测试和commitSelectionActiveTx，并直接解析三个raw：independent五RED（reader顶层、ready三sub及父）/skip-race0；完全相同baseline_test.go在父78产品+旧fixtureoverlay PASS、d3 FAIL。主审完整19路径8289diff/九产品/2859新测试/全部增量，独立五组79PASS35.142s+旧关键22PASS10.960s，原509/430/007全67/31Ledger/owner288frames/四Rust/618三copy/33输入均核实；这些真实GREEN不消除三个P2。

R1成功selection只Stageactive/Donetrue，原claim SID reader无退休路径（finishQuotaOperation group return、resolveWarmSelection active return）。真实build/refill/claim→target0/release→同名T2build→recover/reopen后slot0/fee384而只应T2=192，旧reader1/旧SID仍实际存在，447MiB额度新D64claim错误Exhausted/clone0；旧公开bytes不改，父78对照无此回归。需完整active交付TX精确退休source reader，并合法旧active私有责任幂等收敛，不影响真实backing/未交付/其他reader。

R2真实ready warm test-owned损坏D64→1、warm active holder缺失、SID backing holder缺失各例Service.Open均接受；delivered分支跳过实际资源/holder、结尾只有template检查。需按ready行及原refill冻结旧version owner/Shape/SID/V/B/immutable proof/provenance验证D/source/精确holders，DB-only failclosed；合法rolling旧warm或已claim转S的历史不可误拒绝。R3现有COMMIT确实deferredFK真实，但缺known-S/owner/reader/Resolution、qualifiedcleanup/marker/slot/backing、terminal依赖、新partial多行IGNORE，除absence缺同物理conn query/正常TXoracle；按原R2必需矩阵补，不冒称旧012resolver病例覆盖new013，不用新publickey发布假称原failedintent复活。

父最新合同纳入三项，并用stable request steer-plan013-active-fix-main-findings向正在运行的A1实施thread发完整原约束/main findings/证据/具体响应/所有未决；工具delivery=steered、原task/childRun ordinal1不变。未另启修改相同quota validator的并行源码分支，这是同一未完成修订轮在新用户信息下归并A1+R1/R2/R3，Scope4/parentd3仍不变。A1P1及三P2都须关闭再新的独立SOURCE round，不假称主审仍pending或已通过。先全部代码/meaningfulfocused/SQL证据完成，统一format/source绑定一次必要full360s，避免先A1full再立即重复；若steer前已发生门禁如实保留，不伪造取消。原生命周期/owner/schema/未知费用/原509全部保留，连接/backup/历史gate/预算与物理caps不变，无VM权限。

当前013 SOURCE_REVISE四项在途修订，012DONE/11DONE、017hold、slot FREE。后续新独立review prompt必须含原完整brief/A1/R1R2R3/实际响应/未决，不能复用旧reviewchildthread推进新轮。

### 013四项合并修订da4b7d6已交付，新主SOURCE与validator复核已派

候选`da4b7d6d3efb3eda2dec267e7f1ff7e8570dd8c6`，directparentd3be730，tree`e64bdf34f60fccc2cf42163d6c7c8a4c61a10ae4`；t3-a261ca35/brancht3/validate-warm-selection-cleanup，单owncommit。4许可路径+2018/-91，产品3路径/现有test1441新增行；Engine完整subtree1ec6a535…及API/schema/DDL/deps不变。父全文读report及全部产品3diff；新主审全量4diff/测试与聚合13合同复核。

父直接解析最终`/tmp/plan013-a1-fix-t3-a261ca35/full-race.jsonl`：611测试节点PASS、旧509/509和012映射430/430保留、新增102、testFail/testSkip/race0、四actualRust逐名PASS；原未改main6与adversarial7GREEN。full进程收据332.951429s/exit0；618当前source及full前/后/commit后三实际副本逐byte等于committed blobs，canonicalSHA495b19e2969c5d67db9d73ffdf256f8afcdc559d2c2abb4424bcbf80f1399f7f。33fixture inputs逐byte等于git b93/指定fixturecheckout/精确候选，既存binary SHA d7fc857…2d41e9吻合。focused181/74.167s功能源与最终2换行差异如实保留；全部早期开发FAIL和编译/格式修正收据保留，未重复最终full333/Engine195/Cargo/Rust。

新T3 task前缀均为`node:delegated-task:command%3Amcp%3Aca89d2da-49f7-4adb-a008-e45e62f1351e%3Adelegate-task%3A`，完整taskId分别接`review-plan013-r2-closure`、`review-plan013-r2-validator-adversarial`。两新review round均精确FFda4、新隔离worktree/childRun ordinal1，provider pi/model magpie/remote-magpie/codex/gpt-6.1-sol/options thinking=high已核工具返回。主审全部4diff/新测试/R1R2R3SQL+aggregate；对抗审A1/validator/replay/生命周期边界。完整brief/R2/D1/四发现/响应/未决均携带，没有复用旧review thread或派并行源fix。两项现在working，尚无SOURCE_APPROVE或任何四项关闭结论。

父原始核验先误用不存在.stdout，改实际.jsonl后上述源/节点检查通过；末尾“root product全部无差异”断言因新观察的8个模块文件变动失败，不把整条脚本冒充exit0。root HEAD仍b93；engine、sandboxd、protocol/gen、operator各go.mod/go.sum未提交变动来源未归因、原样保留，未纳入da4或改写。两在途review已通过steer接收澄清，fixtures/source比较只用git b93/da4及指定checkout，不用root当前modulebytes；父plans仍按明确绝对路径读取。具体hash/边界见[父核验收据](evidence/013-da4-parent-verification.json)。

当前012DONE/11DONE、013新双SOURCE复核中、017hold。父核旧3grant均slot_granted=false/handoff_expired=true，slot FREE；13无VM权限。只有新SOURCE实际关闭四项后，才能另发fresh独占grant做一次完整生产与清理释放；未merge/push parent dev。

### 013新focused REVISE A2/P2，Stage目的地有界修订已派

父全文读`/tmp/plan013-r2-validator-review-t3-f2a33310/{report.md,decision.json,finding-stage-proof.txt,own_test.go}`及validator182、resolver2472/2548/2611、transfer/release实际代码。A2：fresh test-owned SQLite仅把合法rejected selection的Stage改成fallback_reserved，Open接受、recover绕过selection resolver，以Identity nil/Allocations空误判完成，原claim reader1→0、Donefalse→true，旧V/B与Sprovisioning/warm-rejected/slot1/storage256/旧public failed bytes不变。重开后即使原delete receipt可见仍不Find，fresh release错误等待不存在replacement。cleanup_done同单字段startup也接受，runtime现有guard正确ManualRecovery/state固定，未夸大该分支；无物理误删/提前退款/tenantDBwrite或生产事故归因主张。

父独立解析最终own raw17PASS/4FAIL（3leaf+父）、原adversarial7+main5合并12PASS，skip/race0，核完整probe SHA d3cee324f5e4a761603139470a5bf40f9da1d1d6d2928772d24e867302d5d500。4initialized/12gitlinks/618candidate与semanticindex/ownedcleanup由focused preservation真实核验，未强求历史数量。新GREEN包含原Refill graph拒绝、Find期间完整Request/history损坏拒绝、合法fallback distinct old/newB跨Root0/同名新SID/BackupRestoreAck/pause-release-reopen，不能通过一刀拒绝fallback历史关闭A2。原A1空predicate在focus关闭，R1/R2原probe支持关闭，R3完整新SQL source/raw覆盖支持补齐；整项SOURCE仍REVISE A2。

合同新增中央DB-only stage→真实原claim provision reservation/采纳/交付/合格回收历史，完整owner/fullShape/request/key/Resolution/destination及旧warm全部D/backing/旧S V-B退休、cleanup真实Sterminated/clearVM、unknownsteps/reader/Done约束；startup/load/write/replay/dispatch一致、损坏ManualRecovery且完整责任保留。调和同事务写入顺序再集中验证，禁止永久豁免/第二完成权威标记/新窗口/当前B补请求/新key/改公有终态/启动Engine修账。合法pending/known/active及后续回收历史须正确接受。

新taskId为`node:delegated-task:command%3Amcp%3Aca89d2da-49f7-4adb-a008-e45e62f1351e%3Adelegate-task%3Afix-plan013-selection-stage-destinations`；新隔离worktree，provider pi/model magpie/remote-magpie/codex/gpt-6.1-sol/options thinking=high返回已核。base唯一da4、owncommitdirectparentda4，原4路径不扩展。先实际原own21节点RED→GREEN/新必要业务边界及SQL写序故障，聚合主审返回后一次final360s/原611+新增/source618三副本/33inputs/vet/normalworkspace lint/零test compile/cleanup；无重复Engine195/Cargo/Rust，无VM。

主SOURCE task_status实际stillworking；父向同一未完成review run发送A2完整发现/report/probe/原闭环限制，delivery=steered/childRunordinal1不变。它仍审精确da4全部原brief，不替换未来commit，不sourcefix；如再返回新发现可steer进同一未完成A2实施轮，避免冲突executor/先full后重复full。后续修订交付仍另发新独立SOURCE round，不能续旧review child。父更新合同/主计划/README/父核验收据，仅plans写入，原source和模块漂移保留；当前012DONE/11DONE、013REVISE A2、017hold、slot FREE。

### 013主SOURCE已完成REVISE A2/R4，R4已并入同一在途实施轮

父全文读`/tmp/plan013-r2-source-review-t3-10b951c8/report.md`与decision、完整missing_context_test.go及对应validator22/validateAllocationGroups1854/claimWarmTx/generic resolver2548/2611。父独立核probe SHA297a87c7214c4bd495b2f563057d2fc05e03c694dec42e7cf2b3de355a808ec6、直接解析missing-context.jsonl为1真实FAIL/skip0/race0，实际日志确认Open接受、recovernil、Done1/reader0/slot1/storage256/V-B实在/publicfailed固定、Submit2→2/Find1→1。唯一test-owned mutation是去掉整个Selection，非删除整个recovery或伪造金额身份；无物理误删、提前退款、tenant写DB或生产归因。

完整主审判定A1 NO（A2目的地+R4必需context）、R1 YES、R2 NO（R4 claimed全局一致性）、R3 YES；原四项最小反例全GREEN不等完整闭环，R4交叉引用不重复计数。独立实际181PASS71.574s、原main6+adv7=13PASS12.087s、新严格单network-off SQL/不同完整Shape oldready/legacyreader等5正对照PASS7.995s、旧关键22PASS11.104s；611/509/mapped430/全部618三组源码副本/33inputs独立核验。完整四文件/1441新增测试及78→da4的19聚合路径已审，其余无新阻塞。候选source、indexsemantic、4实际references/12gitlinks保持；共享旧refs/heads/t3/f2a33310并发移除如实单列，不归因/恢复/强求全局冻结。

父先查唯一A2executor task_status仍working，再通过request `steer-plan013-a2-fix-include-r4-missing-selection`把main完成结论/全原约束/新R4完整证据/最小修订与未决送入同一task；工具delivery=steered、原childRunordinal1不变，未另启相同文件source分支。现该轮A2+R4统一修订，主审已结束而非仍pending。base/parent精确da4、仅原4路径/一个owncommit，无新设计/Scope/Engine/API/schema/DDL/deps/VM。

新增正式要求中央DB-only校验Selection必需存在性，从原claim/S/冻结refill→sandbox transfer/fullprovenance判断，不能缺失即无责任；nil fastpath/startup/load/write/replay/任何Done-reader消费前ManualRecovery、全state/refs/费用/publicbytes留住/Engine0。不得用当前S ProvisionSource/VM或非持久Origin/suffix/新flag猜补，禁止一刀拒绝所有nilSelection；保留合法build/warm/普通cold-golden及replacement/active后Root0/新SID-Shape/pause-release/restore历史。原R4实际原字节probeRED→GREEN并加runtime缺context与正对照、保A2own21和所有原GREEN；新的SQL写次序适用真实fault矩阵，聚合全部A2/R4后一次final360s/611+新增/source618三副本与blob/33inputs/vet/normalworkspace lint/零test privcompile/cleanup。后续新独立SOURCE round包含原brief/四原发现/响应/A2/R4及未决，不能续旧review child。总11DONE、017hold、slot FREE保持，parent dev仍不merge/push。

### 013 A2/R4新候选5c530395交付，原A2 oracle明确澄清后新双SOURCE复核

自有commit5c530395dd0452c98f11ef173e788b1d509650f3/directparentda4/tree66d950b749a02e7c757670a9f9b6ce287f13d735，t3-2760705d/brancht3/fix-warm-fallback-validation；4许可路径+750/-33，无Engine/API/schema/DDL/deps/publicJSON增量。父全文读report/handoff/audit、全部产品3diff、原A2完整probe/ledgerRestart和原失败日志。执行者实现完成/自有commit已交付，验收STOPPED系原probe矛盾：runtime应Manual/state不变之后68行无条件helper要求Open成功，而startup应拒同一损坏DB。父先前要求原文件原样全部GREEN不成立，现明确采用[oracle澄清](evidence/013-a2-oracle-clarification.md)；不要求用户许可或产品放宽拒绝，不改原文件/helper/旧收据。新review自有/tmp副本只改该诊断重开段为Close/Open期望Manual+完整readonly state等于before，其他断言/正对照全保留，SHA/diff/实际旧RED-新GREEN分别记录。原19PASS/2FAIL（1leaf+父）仍FAIL，合并原外部38PASS/2FAIL不冒充全GREEN。

父直接解析final-full-race.jsonl：635带Test节点PASS，旧611/509/映射430全保留、新24、testFail/testSkip/race0，四actualRust逐名PASS；final result exit0/343.254266821s。618当前source及retry-before-full/source、final-after-full/source、final-after-commit/source逐byte/执行位/symlink等git，canonicalSHA0f03c0a1e583d82811d52deb8b073aa1e7234954dcd91cb94e4574e41a6beeed；33fixture输入/既存binary吻合。Source模式证据不冒充早期7plans排除的611/601副本；base-complete618实际在改源前已保存。原main6/adv7/newmain5/R4共19PASS；原A2其余19PASS，失败准确定位矛盾helper。详见[父核验](evidence/013-5c-parent-verification.json)。

首个长TMPDIR full失败主动终止保留-15/123.292s/235PASS/11FAIL/3race输出；父全文读三race栈，均为原integration/transport_test.go203失败启动读取bytes.Buffer.String与exec stderr异步writer。文件不在13delta，新主审独立对照基线及失败路径定性，不能用最终635全绿抹掉race/声称所有错误路径安全或自行扩4路径。新建短TMPDIR后11定点PASS才执行最终full，同源/相同fixture/360s deadline/物理caps不变。旧长路径失败不作PASS，未重复Engine195/Cargo/Rust或旧635广测。

新独立task前缀`node:delegated-task:command%3Amcp%3A10a8b55d-fdde-47d5-b2e5-27d66d942a13%3Adelegate-task%3A`，完整ID分别接`review-plan013-r3-source-5c530395`与`review-plan013-r3-oracle-and-destinations`。两新worktree/childRun ordinal1，providerpi/modelmagpie/remote-magpie/codex/gpt-6.1-sol/optionsthinkinghigh已核；完整携原brief/R2/D1/A1/R1R2R3/A2/R4/响应/oracle澄清/未决，没有续旧review。主审全4diff/370测试/SQL责任模型及635来源/race定性，对抗审澄清后probe实际RED-GREEN/目的地context与合法历史；当前均working，未SOURCE_APPROVE。

父仅plans写入，parentHEADb93/root8module并发patch保留不采用，0 initialized references本次实现实际入场合法/12gitlinks保持；19自有PGID结束与TMP清理由执行者收据和新审查独立复核，不冒充父重做全部runtime。总11DONE、013新双SOURCE复核中、017hold，所有旧VMgrant false/expired/slotFREE，parentdev不merge/push。只有新源码实际关闭后另发fresh独占生产grant。

### 013主SOURCE新R5/P2采用并派typed-holder修订，对抗审仍在途

父全文读`/tmp/plan013-r3-source-review-t3-5b541bf0/{report.md,decision.json,holder_kind_test.go,holder_positive_test.go}`及warm_fallback425/427/471/483实际资格查询。精确5c唯一新R5：正常running fallback replacement D自然active_use+cleanup两个同名sandbox holder，精确删除active_use一行后cleanup仍满足仅holder_id的EXISTS，actualOpen与recover均nil。完整DB/public/fee slot1-storage256/V-B不变，EngineSubmitFind0，无物理误删/提前退款/tenantDB写或生产归因。父独立解析两leaf+父3FAIL与自然双引用1PASS，核两probe SHA715c3387…e265及0f32862b…6fcad。完整闭环A1NO/R1YES/R2NO/R3YES/A2NO/R4YES，共同NO只算一个R5；原最小反例均GREEN，已审其余无新阻塞。

主审34focusedPASS22.845s、原19PASS17.167s、合法双引用positive1PASS5.512s，R5反例5.272s；635/旧611-509-mapped430/618模式与symlink三副本/33fixture等独立全核。父另直接读取peer已完成corrected-probe和oracle.diff，核SHA22bfacf06f9145a3a755fa4f756d93d5452ae7788cd1f1d3fe3187de29060f79，解析corrected40PASS/A2完整21/exit0/27.278783s、da4 stage1PASS4FAIL/exit1/5.400376s，skip/race0。原probe19/2与旧raw仍保留；oracle矛盾已实际调和，不是当前停止理由，peer最终源码结论尚未交付。

旧长TMP race由独立主审对照b93/78/da4/5c transport_test同blob acb8982487ef9c1bf8a4fa6b308d3ddd2408aec0、三完整stack及SUN_LEN启动错误，发生在Service.Open前stderr Buffer诊断未join；分类既存fixture失败路径缺陷，保留真实失败、不宣称所有错误安全、不扩Scope修它，独立fixture维护跟进。R5是本轮SOURCE_REVISE的独立原因。

父检查原focus task仍working，向其同一未完run steer主审R5/原判定/正反probe/完整限制（delivery=steered/ordinal1）；它仍审精确5c，不sourcefix或审未来commit。新单一source task`node:delegated-task:command%3Amcp%3A10a8b55d-fdde-47d5-b2e5-27d66d942a13%3Adelegate-task%3Afix-plan013-typed-destination-holders`已派，providerpi/modelmagpie/remote-magpie/codex/gpt-6.1-sol/thinkinghigh返回已核。新worktree只FF5c/base5c/owncommitdirectparent5c、仍原4许可路径。

合同采用typed quotaRef Kind+ID按各Resolution和真实S生命周期/producer核资格，当前running同V-B需active_use、collector需cleanup；pending-known/terminated-后继回收历史不能一律active_use或IN两kind含混。保自然active+cleanup共存/资源只计一次、owner-fullShape-cost-source-proofs/其他reader-backing-slot，startup/preflight/readwrite/replay留责零Engine/noRepair/public immutable。原R5同file实际RED→GREEN、必要真实业务对照入现有test，不helper单测。对抗审若另返发现并入同一未完成修订后统一format/source绑定一次final360s/635+新增/618模式副本/33inputs/vet/normalworkspace lint/零testcompile/cleanup，不重复未改343s/Engine195/Cargo/Rust。随后新独立SOURCE round携原完整brief/7项发现与所有响应/oracle澄清/未决，再fresh生产grant。父只plans变更，11DONE/017hold/slotFREE、root8module patch保留，parentdev不merge/push。

### 013独立focus最终也为同一R5，无其它新阻塞，已汇入唯一实施轮

父全文读`/tmp/plan013-r3-oracle-review-t3-c6984cce/report.md`、decision和完整active-holder-probe.go/ordinary-positive-final.go；独立解析active-holder.jsonl为0PASS/3FAIL（两leaf+父）与ordinary-final.jsonl3PASS，skip/race0，核两source SHA4e1b9df3…b3814、c13d3bf2…11c03。对抗审独立复现与主审同一R5，临时A3统一编号不重复计，最终FOCUS_SOURCE_REVISE精确5c/无其它新阻塞。双方完整闭环NO/YES/NO/YES/NO/YES；原oracle正确40PASS/A2完整21、da4 stage真实RED和旧19/2保存、既存fixture错误路径race定性均一致，不用这些GREEN遮盖typed active holder缺口。

ordinary cold/golden实际无Selection→Open→pause/release→S.OperationID更改→Open/recover正对照保持公开bytes、终值slot0/storage0或192；普通路径可合法没有allocation_group，初轮强求该内部record是probe setup错误。unknown失败pending的subject holder实际合法cleanup，早期假定engine_dependency后UPDATE0也非product finding。上述producer事实明确送入typed-holder修订，不能“一律active_use/engine_dependency”或给普通路径新建group。保自然active+cleanup并存与单资源一次成本。

父task_status确认唯一`fix-plan013-typed-destination-holders`仍working，request `steer-plan013-r5-fix-focused-review-complete`送peer最终全文/完整probe/raw/原brief与闭环限制；delivery=steered、原childRunordinal1。两个5c旧review都完成，该source轮base/parent5c/四路径不变，无需再等待旧审查，聚合全部已知要求后focused→一次final360s/635+新增/618模式副本/33inputs/静态compile-only/清理/owncommit，再新的独立SOURCE round。未另起sourceexecutor/嵌套委派或重复343s广测；11DONE、013未APPROVE、017hold、slotFREE，rootpatch/parentHEADb93保留。

### 013 R5修订1414b4e交付，新的两独立SOURCE round已派

候选1414b4e376a70fec9ed9f16b30db588cd38c4b88，directparent精确5c530395/tree96602c8b6eb902d81687dfab3865f11ec59438b0，t3-0c3b63bb/branch t3/fix-warm-fallback-holder-validation，base后一个selfcommit。仅quota.go/warm_fallback.go/现有template_quota_test.go三路径+367/-50，无recovery或其它禁区增量。父全文读report/handoff、全部三diff/268新增业务测试行；requireQuotaResource精确owner-resource-key-slots-storage-source及typedKind+ID EXISTS，groupAllocationRef按actual pending-known/terminated交接保合法cleanup，active/collector/buildslot/unknownslot相应类型核验；自然双引用不重复计成本、普通无group和历史退休合法，未新增SQL写序/public/schema/deps。

父独立解析raw：外部50PASS32.324649s、新业务9PASS11.138526s、最终格式producer84PASS40.845097s、唯一final644PASS342.713474s（360s deadline）。全部原635/611/509/mapped430保留、新9，testFail/testSkip/race0、四actualRust逐名PASS；618当前source及final-before-full/source、final-after-full/source、final-after-commit/source三个实际copy逐byte/执行位/symlink等gitblobs，canonicalSHAe94e454d8de9fb60667e98c8a20c6e76886d1d23f679b0f7d993812508ec7780。33fixtureinputs/既存binary匹配gitb93既定manifest/候选/指定checkout。静态vet1.523/lint4.0970issues/零testcompile0.717收据保留，原baseR5真实3FAIL和两独立probe新GREEN不改oracle。初期外部50有4路径hash但无独立全618snapshot，format来源不冒充相同；final644严格绑定。详[父核验收据](evidence/013-1414-parent-verification.json)。

新task实际前缀`node:delegated-task:command%3Amcp%3A10a8b55d-fdde-47d5-b2e5-27d66d942a13%3Adelegate-task%3A`，完整ID分别接`review-plan013-r4-source-1414b4e`与`review-plan013-r4-typed-holder-adversarial`，均新隔离worktree/childRun1/providerpi/modelmagpie/remote-magpie/codex/gpt-6.1-sol/optionsthinkinghigh工具返回已核。完整携原brief/R2/D1/A1/R1R2R3/A2/R4/R5/全部响应及formaloracle澄清、旧失败/源证据限制，不复用旧review child。主审全三diff/新268测试与typed producer/SQL、全644来源和七项闭环；对抗审原两R5/frozencontext及pending-known-active-cleanup/terminated历史/正确oracle独立实际探针。当前均working，未SOURCE_APPROVE，不重复未改342s/Engine195/Cargo/Rust。

root8并发modulepatch/parentHEADb93保留，不从root当前modules取fixtureauthority；实际reference0/12gitlinks与indexsemantic/自身commit范围由新审查独立核。原A2错误oracle19/2FAIL与corrected21GREEN、旧长TMP235/11FAIL3race既存fixture分类保持，不宣称Scope外失败路径已修。实现交付≠源码批准≠13生产PASS；当前11DONE、017hold、旧VMgrantsfalse-expired/slotFREE，待新SOURCE实际关闭全部七项后另发fresh独占生产grant，parentdev无merge/push。

### 013精确1414 focused SOURCE_APPROVE，aggregate主审继续

父全文读`/tmp/plan013-r4-typed-review-t3-ac4bfeee/report.md`、decision与完整新边界ownprobes/typed_boundary_test.go，直接解析original50.jsonl50PASS32.676336s和boundary.jsonl3PASS6.978975s，fail/testSkip/race0；boundary SHA81363916c1897bb515d5931fb5e0c8aae05ff5d2f8356e8d88eae882f29c306e。指定focus七项全部YES/findings[]，精确commit1414/tree96602c8/parent5c均吻合。真实后继pause切换S.OperationID、捕获sleep SID而旧replacementV-B仍在，合法pausing状态可Open；删除active_ref保cleanup后startup/runtime均在Submit/Find前Manual、完整state/public原成功bytes/slot1-storage448/V-B固定。R3完整SQL沿用已审证据，本次只读谓词无新写序；source-copy/fixture/full644聚合核验仍归独立主审，不冒称focus重做这些广测。自身0initialized/12gitlinks/source618/indexclean/两PGID清理由完整preservationcleanup据实记录。

父task_status实际查aggregate主SOURCE仍working，request steer-plan013-r4-main-focus-approve-stop-after013将peer最终报告/新边界/用户最新收口送入同一未完run，工具delivery=steered/ordinal1。主审继续精确1414原scope，无sourcefix/VM权；focused APPROVE不等整体SOURCE_APPROVE。当前仍11DONE、013未DONE、旧grantsfalseexpired/slotFREE，parentHEADb93/rootmodulepatch保留；用户明确完成013即结束，016/017/020/021及批次集成暂停，不因13依赖解除再启动其它工作。

### 013聚合SOURCE_APPROVE与当前preflight通过，唯一fresh生产任务已派

父全文读`/tmp/plan013-r4-source-review-t3-46728132/report.md`、decision：精确1414b4e/parent5c/tree96602c8，七项全YES、blocking/newP1P2[]。父直接解析typed-new9PASS、producer-sql72PASS（完整SQL43leaf+父与build28）、own-producer4PASS，共85PASS/fail-skip-race0；独立focus53此前已核。两独立SOURCE现均批准同一源码，旧候选REVISE/原oracle19-2/fixture失败保持历史等级，实际644与source618三模式副本/fixture33全部核验，不重跑未改广测。

父新[preflight收据](evidence/013-production-preflight-1414.json)于2026-11-07T09:47:23Z只读核13材料hash-size/26guest source/guest包NAR、OCI6reachable/12blob、tools11entry与83运行闭包、KVM访问；原012最后88PID-starttime均结束、原unitnotfound/inactive/dead/MainPID0/cgroup-vmscopes-mount无残留，无active生产unit。首轮错误比较Btrfs子卷st_dev与物理block而assert失败，后由findmnt /dev/nvme0n1p6→sysfs父nvme0n1 dev259:0核实，诊断保留/noVM；原io.max2590wbps1048576不变，旧4MiB测速仅历史，本run所有VMM精确IO读回必须fresh。

新[production-verification-013-1414.json](evidence/production-verification-013-1414.json)为唯一activefreshgrant，ownerverify-production-plan013-1414，全部旧3grantfalseexpired。新task完整ID`node:delegated-task:command%3Amcp%3A10a8b55d-fdde-47d5-b2e5-27d66d942a13%3Adelegate-task%3Averify-production-plan013-1414`，providerpi/modelmagpie/remote-magpie/codex/gpt-6.1-sol/thinkinghigh已核工具返回。新隔离只FF1414/源码材料不变、fresh0700 /tmp/p013-production-1414b4e、原make production-e2e一次；缓存Go编译允许、原Nix只realize已缓存83闭包且maxjobs0/no substitute-builders-substituters，无build-fetch安装。原guest512/vmm640/outer8G/swap0/tasks2048/private24G/CPU-pids-IO及owner9/13312不变；原top+adoption/fullLifecycle/golden-warm/realTCP全实际RUN/PASS无skip，所有managedVMM稳定host/namespace映射65534↔165533/groups0/caps5sets0/NNP1/seccomp2/隔离与资源guards须live证明。无standaloneVM/guestprobe、auto retry、source/testfix、hostroot/subidconfig、旧资源操作或nesteddelegate。

boundedobserver仅记录ownedPID-starttime/status/exe/ns/cgroup/rootlink/maps和允许test/console，不读actualDB/config/env/token/argv/FD bodies/VMJSON；archive只能stat不open/list/hash。原runner清理自己的freshunit/namespaces/scopes/mount，所有ownedPID-PGID结束/unitnotfound-inactive-dead-MainPID0/cgroupScopes absent/hostmount0、cleanup.json和显式slot-release必需。失败/完成/释放立即终止授权，无retry，父接收后expireJSON；不自行DONE/发grant/改父plans。

当前源批准、fresh生产已派但结果pending，总11DONE/013非DONE；只有实际生产及cleanup/evidence父复核通过才标DONE，并按用户“做完013就结束”STOP。其余计划/整批集成/fixture维护暂停，parentHEADb93/root8module并发patch保留、无merge/push。

### 013 ACTUAL_PRODUCTION_PASS / DONE，最终清理释放接受，本轮STOP

父全文读`/tmp/p013-production-1414b4e/{report.md,handoff.json,verification.json,cleanup.json,slot-release.json,source-proof.json}`和允许的provenance/raw字段，接受精确1414/tree96602/parent5c生产结果，分支t3/plan013-production-acceptance/worktree t3-68d5daed；没有sourcefix/重试/extraVM。一次原make exit0/142.601168s，父解析原run.UDuBDK4u/test.log为5RUN/5PASS/fail0/skip0，exit-status0。adoption3.96、全lifecycle42.61、golden/warm26.97、realTCP45.15秒/top137.93；不拿旧12PASS或compile替代本轮。

父独立原proof配对13台实际cloud-hypervisor exe+host/namespace PID-starttime，核UID-GID65534↔165533/groups0/5caps0/NNP1/seccomp2/受限root与net-mnt-pid-ipc-uts隔离，每台至少一armedCPU100000/100000、mem671088640/swap0/pids256和freshIO2590wbps1048576、outer8G/swap0/tasks2048/runtime1300s/stop10s。原4MiB吞吐仍历史，fresh只是精确io.max，不声称错误路径都安全。两侧实际VMM proof无漏配，不把exec前engined过程或nullable退出样本当作VMM安全状态；不同PID可同startjiffy，identity按exe+PID/starttime。

父freshcheck89PID-starttime全部结束、26ownedPGID均空，unit sandbox-production-acceptance-1794045606-1183581.service notfound/inactive/dead/MainPID0，cgroup/VMscopes absent、hostmount0。初收到显式release即将唯一grant false/ownernone/expired，核验后记录parent accepted；无retry权限。618当前source与两份实际pre/post副本bytes/执行位/symlink全等git1414/canonicale94e45…7780，13material当前重新hash、26/33source/OCI12blob-6reachable/NAR前后保全；clean生产worktree/exactHEAD已核。archive157182239bytes/0644和root0700只STAT，未open/list/hash正文。

[父最终验收](evidence/013-production-acceptance-1414.json)保存实际证据、限制和读数；原运行完整report/verification/provenance/cleanup/slot-release保留。FF前只记HEAD/tree/branch/cleanstatus/12gitlinks，完整raw/semantic index/refs/reference快照始于FF后pre-run，未补造前值。namespaceobserver被原PIDns teardown exit-9且已wait，非graceful；历史oracle19-2/长TMP fixture race等失败等级不改。父只读审计过程的unit样本形状、starttime单独唯一假设、exec阶段过滤与null exe解析错误已如实记录，纠正后完整验证通过，无Go/VM重跑或安全断言弱化。

正式标013 DONE，批次DONE数11→12。parentHEAD仍b93/root8既有modulepatch与archive/deploy改动保留，未merge/push。**遵用户“做完013就结束”：本轮已STOP，016/017/020/021、整批集成与fixture维护均不再推进；所有VMgrant已过期，slotFREE。**

