# 最终组合：12项已完成成果

## 授权与范围

用户最新明确指令：“做最终组合吧”。这覆盖此前“013后停止”中关于最终组合的暂停，仅恢复12项已完成成果006/007/008/009/010/011/012/013/014/015/018/019的汇总、必要冲突处理、独立审查和组合验收。016/017/020/021不启动。父dev不merge/push/commit，不采用或撤销用户未提交改动。Advisor仅维护plans，源码只由隔离executor改动。

状态：**FINAL12_COMBINEDPASS，已完成并停止**。独立SOURCE_APPROVE/65focused、sandboxd完整race678PASS、fresh Rust49、newsource-matched guest/OCI、原双格式KVM及唯一完整production5RUN5PASS/141.101s全部通过。父已核13台actualmanagedVMM liveguards、109PID/26PGID、unit/cgroup/mount清理；所有grants已失效，slotFREE。原KVM汇总NameError STOPPED与后续独立只读post补证接受分开保留，无VM重跑。见[最终交付报告](FINAL-COMBINATION-12-RESULT.md)与[evidence/final12-47f5-final-acceptance.json](evidence/final12-47f5-final-acceptance.json)。旧013与8/10项历史PASS仍各自绑定旧树，本次有独立新实际验收。

## 已完成的只读漂移核对

父当前HEAD仍b93fc24a631bb94c9e87484a460703f016742cea；实际tracked status与历史已不同（包括go.work.sum，不再列旧8个模块补丁）。保留当前实际状态，不按旧摘要重建。初始worktree reference数量以各自实际值为准；12gitlinks不得变动。

对下面7个待追加提交的19条路径核对b93→1414，仅sandboxd/README.md发生后续漂移；其余追加目标没有漂移。README以保留013资源合同并补SDK取消约定处理，不能替换整个文件丢掉013文档。

## 源码组装合同

隔离executor在T3绑定的新worktree保全初始HEAD/tree/branch/status、完整semantic index（stage/path/mode/OID）、raw index hash诊断、现存refs、实际references checkout set/HEAD/status、12gitlinks、工作树bytes/mode/symlink清单。不得修改其它工作树/refs或复用父dirty文件。FF到base `1414b4e376a70fec9ed9f16b30db588cd38c4b88`，它已含007/009/010/011/012/013最终链。不要再次复制其已包含计划。

仅追加以下pure提交，顺序固定：

| 计划 | 原始批准提交 |
|---|---|
| 006 | 6d490ea850f0a51a25037265e547893a8dc41654 |
| 008 r1 | 054270810044ae60b52225e8a43c9c6a97fb7a5e |
| 008 r2 | fba80816a4a38fcb049c64bd2504106fe24462a1 |
| 014 | 579054ccbd735fc6ae80a673471d0e8c3e081a4d |
| 015 | 583a9f719047f38453d91c946862b8a80d466740 |
| 019 | 314c4f7e1599220c3a0b6699bd77f9d1ab9c5533 |
| 018 | 6c090e81160e59e75280c79ca9f4041d8c0c2343 |

不合并各自完整分支的旧祖先，不重复已有改动。逐提交原SHA→本地SHA→stable patch-id映射，出现重复/冲突/空提交须具体报告。只做必要冲突解决，不趁机重构；发现真实交互缺陷另报可复现反例，由父指定单一有界修订。提交后锁定candidate HEAD/tree，不改测试断言为绿色。

Scope为这7个提交的diff-tree路径并集：Makefile；guest/README.md；guest/agent/Cargo.toml；guest/agent/src/fs.rs；guest/agent/tests/agent.rs、tree.rs、fs_identity.rs；sandboxd/README.md；sandboxd/egress/README.md、egress_test.go、http.go、mitm.go、mitm_test.go；sandboxd/sdk/client.go、client_test.go；scripts/run-guest-fs-isolation.sh；protocol/README.md；随原提交携带的plans/evidence/018-tree-cleanup.md与019-fs-identity.md。不得修改其它plans索引/用户文件；自己报告放/tmp。全量gitlinks、source mode/symlink保全。

## 材料与测试

第一阶段executor无VM/namespace/特权native/生产权限，不能复用任何旧grant。无安装/fetch/下载、uncached依赖或Nix/Cargo构建。真实最终生产需要独立新鲜preflight/唯一串行grant，旧所有grant均falseexpired/slotFREE。所有真实DB/config/env/token/argv/FD bodies/VM JSON/生产archive内容不得读取，archive只STAT；自建测试数据允许。

Guest源码变更后不能沿用013的b93 guest33input fixture/静态image。预期最终guest/protocol/Cargo输入等于stage1 `f1a3fb62a8c3b2aedef14f96bb2fb407e7e81ced`，必须按git实际SHA与文件字节/mode核验。匹配的原stage1 native unix_fixture在 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-d6927cf8/target/debug/examples/unix_fixture`，预期SHA `ce62e84ee4b0ad6bb825f679811010733e825a21dc84826d1473ae56269b32d9`，34inputs。使用前后现查完整输入+binary哈希，保持旧worktree只读。另有独立材料调查agent核对cached Rust/static guest/OCI，不重复生成或构建。

Go验证统一GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-mod=readonly；lint使用正常仓库go.work。短TMPDIR避免已知长UNIX路径触发旧fixture startup diagnostic race；该旧问题不在范围内，不维护。必要时记录原始失败与原因而不抹去。

组合新增Go集中006 egress和014SDK，完整SB必须真实运行：go -C sandboxd test -race -json ./... -count=1 -timeout=480s，指向source-matched stage1 fixture；包括真实四条Rust relay integration不可SKIP。480s是本次完整测试进程预算，不改变产品、VM1300s/9slot/13312MiB等物理预算。原Makefile180s无法容纳已有013 343s，因此不用伪称原make test-race通过，不修改Makefile超时掩盖。

Final644旧PASS集合必须保留并映射新增006/014节点，31Ledger/00720top47sub/owner24leaf192+96/fouractualRust等关键组按raw JSON核对，不只列名/零测试/compile-only/缓存。完整source清单在commit、pre-test、post-test三份实际bytes/mode/symlink一致；记录实际command/环境仅非secret允许项、start/end/exit/wall、每个RUN/PASS/FAIL/SKIP与race diagnostics。全套成功一次后不无故重复。

Go vet三个modules+normal go.work lint、gofmt检查（不自动修改既有代码）、git diff --check。Engine/protocol与1414完整输入比对，原engine195s native full证据可明确沿用为相同组件历史证据但不伪称本轮重跑；若后续组合review发现相关变化/疑点，再指定必要新跑。原Makefileintegration/smoke/check包含Cargo，不擅自运行。可先仅执行等价Go真实integration（完整SB已含）和静态检查。

Rust完整32/feature10/dualUID7旧证据只在完整source/binary输入相同的条件下明确引用，不能标记新跑。材料调查完成后决定是否可直接重跑已缓存test binaries/required双UID或需要单独材料方案，未解决前不得算最终全PASS。

## 独立审查与收口

新review task需带本brief、所有历史补丁/反例、修订回复与未决反对；reviewer独立核对完整差异、重复/遗漏、source proof、真实测试、跨计划相互作用（006隧道关闭与007撤权/014取消；guest FD/非阻塞/xattr/cleanup组合；012/013配额/public terminal/Engine witnesses）。发现P1/P2必须具体反例；不扩展审计到旧无关fixture维护。每轮独立新task，源码修订单一executor。

本地组合/source审查通过后，基于最终精确HEAD与匹配guest材料规划实际生产验收、新鲜清理/IO259:0/UID/cap/cgroup前提，另签唯一新grant；原13 PASS不能移植。验收后关闭grant、确认所有本次PID/starttime/PGID/unit/cgroup/mount归零，source/materials未变，archive不读取；再标最终组合完成并记录branch/HEAD/worktree/source mapping/检查与限制。当前阶段无VM授权。

## 委派过程记录（现已全部收口，最终结论见文首）

- 材料调查：clientRequestId inventory-final12-cached-guest-materials，provider pi，model magpie/remote-magpie/codex/gpt-6.1-sol，thinking high；只读，无构建/VM权限。
- 源码组装与本地验证：execute-final12-combination-r1，worktree `t3-fe782bd5`，branch `t3/final-twelve-source-combination`，当前组装candidate `47f5ce71bfa8de2720e184fec516f0e27ca90f2d`；7purepicks无冲突，最终本地门禁进行中。
- 新独立aggregate source review：review-final12-combination-r1-aggregate，精确47f5已SOURCE_APPROVE/findings[]；独立65RUN65PASS/0failSkipRace，原自有probe缺Env的3FAIL真实保留。父全文读报告/probes并独立解析65raw接受，见evidence/final12-47f5-parent-source-review.json。source/local阶段通过不等于最终生产PASS。
- 新guest材料/Rust任务prepare-final12-source-matched-guest-r1 COMPLETE：新project一次离线编译80.264s/only1drv/fetch0，匹配27/34；freshRust32+10+7=49PASS，parent已核原始日志、27store源bytes=git和实际package NAR。见evidence/final12-47f5-parent-guest-verification.json。
- 新KVM材料阶段prepare-final12-oci-kvm-47f5返回STOPPED：原双格式KVM唯一一次已actualPASS12.692668s/4个原inode512MiB连续rw-p/6stableVMM，新OCI12blobs/6reachable已生成；自有final verifier在postsource snapshot后NameError `prep` 未定义，因此最终source34/filter27、material与oldstatic afterhash/refs/PID汇总缺失。原raw/失败保留，不重跑VM。显式release2026-11-07T15:51:35Z已收到，父先expire材料grant（false/none/true）。新read-only独立review-final12-kvm-post-integrity-readonly补齐证据，原task STOPPED等级不改；prepare-final12-production-runner-readonly并行准备futuredriver、无VM/生产权限。新生产授权必须等只读post审计接受后另立。
- KVM预算字段澄清：材料grant中guest_disk_mib512误沿用了production字段，但同一grant/brief明确原完整KVMtest/预算/断言不改。原native gate实际256MiB sparse upper，执行者保持原实现正确并披露；父按实际standalone256评估此gate，不宣称证明production512，不为凑字段改test/重跑VM。后续生产仍必须原512与全部managed身份/cgroup guard。
- KVM其它保留偏差：最初rg--files超范围列到旧p012artifact filenames（无bodyread/hash）；unsupported offline=true初始config警告在gate前改为有效配置；6stablepinnedVMM与10配对sample不冒称所有瞬时auxiliary已采全；39runtime/config/state/DB/log只STAT。只读post审计不得抹去这些偏差。

## 新材料准备阶段（基于调查结果的明确范围）

只读调查 `/tmp/final12-material-inventory-t3-c70069e9/report.md` 实测nativefixture SourceMatch YES，但现有5个静态guest包全部SourceMatch NO，旧OCI内嵌b93 agent也为NO。最终guest full34清单SHA `e1939a6d3bac9f56f1c89805ea7c2dcc04b852d9b35e5ccb99bfd31168f2c9eb`，filtered27 SHA `e4d7dda9fdc7edc8934b3c5b93bf68e3efdb507c380cfc6c07ddd05179e64ddc`。

用户新的“做最终组合”包含产生可验收的匹配材料。历史013不改guest，因此其“不得Cargo/Nix编译”任务边界不能替代本次新guest源码的必要构建。现在仅对单独build-only executor明确允许：从最终精确47f5未改flake离线本地构建一个guest-static项目包（包括用已有锁定vendor进行Rust crate编译），不安装/下载、不补建其它缺失Nix依赖/工具链。先offline eval/dry-run并确认needed build仅本项目guest-static drv且fetch集合为空；如果出现其它Nix依赖build或缺缓存，STOP返回具体输出，不启动它们。使用本次新工作树/ignored build/guest，不覆盖任何旧产物/旧worktree。记录newsource27/NAR/drv/outpath/packageNAR/agent/init/initramfs/staticELF/defaultfeatureoff，完整源码前后不变。这个阶段明确是新构建，不冒充缓存命中。其它source executor/reviewer仍保持其原无Cargo/no-build边界。

同一材料测试executor可从已核对stage1缓存直接运行现存Rust default32、featuretree10二进制（不Cargo），记录当前hash及source34映射，零test默认fs_identity不算验收。required双UID7可在本次新scratch复制精确feature二进制，按原脚本既有unshare flags进入新user/mount/PID namespace，直接调用**未改原runner的 --inside**部分；跳过的是已明确记录的编译/选择阶段，不能伪造Cargo输出或称完整原make入口重跑。原inside脚本列全test并强制actual7PASS/noSKIP及所有UID/capability/namespace/sentinel guards；scratch权限、90s+5sdeadline、namespace退出与owned清理保留。只新自有namespace，无VMKVMVMM权限，不改hostsubids/root。已有nativebinaryhash缺历史执行时attestation的限制保留，只证明本轮当前binary与当前source归属和实际结果。

新的OCI会从新GuestDir通过既有完整KVM材料入口生成；该入口需单独fresh串行VM grant，在guest生成及源码审查就绪前不授权。仍不准生产/standalone重试/旧runtime内容读取或archive读取。旧013/012生产PASS只历史。

