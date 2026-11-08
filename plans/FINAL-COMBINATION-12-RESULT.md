# 最终12项组合：已验收通过

**后续022已完成：当前同一目标分支HEAD为33a3f829/tree56c1，保留原12项完整祖先并追加2个职责/API重组提交。见[022结果](022-RESULT.md)。本报告以下HEAD、源码位置和production/KVM/Rust49成绩均绑定旧47f5验收快照，不是新33a3运行成绩。**

**FINAL12_COMBINEDPASS，父最终核验接受。** 全部12项成果现已集中在同一条源码分支，可直接review整个范围。

- 分支：`t3/final-twelve-source-combination`
- HEAD：`47f5ce71bfa8de2720e184fec516f0e27ca90f2d`
- tree：`3e8cc8a12cb70dd36fffc428f9aff83d8712fe73`
- 基准：`b93fc24a631bb94c9e87484a460703f016742cea`
- 工作树：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-fe782bd5`
- 包含：**006/007/008/009/010/011/012/013/014/015/018/019**。

整个分支是交付对象。最后一个47f5提交本身只是018补丁；从b93到47f5的28个提交才是全部12项。总变化57文件、+29949/-889，包含大量行为与恢复测试。[review阅读顺序](FINAL-REVIEW-12.md)、[来源映射](DELIVERY-MAP-006-021.md)、[最终机器验收收据](evidence/final12-47f5-final-acceptance.json)。

## 组合方式

以已验收的1414为基础，保留007/009/010/011/012/013最终链，再按顺序加入006、008两提交、014、015、019、018的7个批准纯补丁。没有冲突、人工源码修复或额外功能提交，7对stable patch-id全部一致；没有重复各旧分支祖先。

| 原计划 | 最终新增提交 |
|---|---|
| 006 | ce615437 |
| 008 | 994421d7 → 02948492 |
| 014 | cdcb9248 |
| 015 | 5da47a94 |
| 019 | 2a883d3d |
| 018 | 47f5ce71 |

完整原始SHA、对应本地SHA和继承链见 `/tmp/final12-combination-t3-fe782bd5/mapping.json`。013旧分支仍只是其自身所需六项；本次新分支才是全部12项合集。

## 实际验收

| 阶段 | 本轮结果 |
|---|---|
| 独立源码与交互审查 | SOURCE_APPROVE、findings=[]；65 RUN/PASS，fail/skip/race0 |
| 完整sandboxd race | 678 RUN/PASS，350.166s；旧644全保留，新增00629+0145；fail/skip/race0 |
| Go静态检查 | 三module vet、正常go.work lint、gofmt只读检查、diff-check通过 |
| Rust与身份隔离 | fresh默认32+feature tree10+required双UID7，共49 PASS，无遗漏/ignored/skip |
| 新静态guest | 一次离线本项目构建80.264s；仅1drv，fetch0、其它Nix依赖构建0；匹配full34/filtered27 |
| 双格式KVM | 唯一原完整gate exit0/12.693s；layered与merged实际PASS；四个原RAM inode连续512MiB rw-p |
| 最终生产 | **唯一原完整make exit0/141.101s，5 RUN/5 PASS，fail0/skip0** |
| 最终清理 | 109个已知PID/starttuple全部结束、26个已知有效PGID全空、服务/cgroup/scopes/挂载无残留 |

完整sandboxd门禁包含31 Ledger、00720top/47sub、owner24leaves及192initial/96recovery、四条真实Go→Rust relay。Engine源树精确未变，原448PASS/36required/195.661s只作为相同组件的已审历史证据沿用，未冒充本轮重跑。

生产5节点原始时间：top135.55s、live adoption4.06s、完整lifecycle38.01s、golden/warm21.58s、real guest TCP42.78s。13台actual managed VMM均有稳定host/namespace PID-starttime-exe-inode-scope配对；UID/GID65534↔host165533、空groups、五caps0、NNP1/seccomp2、namespace/root隔离及CPU、内存、swap、pids和IO实时读回符合原要求。

预算没有提高：guest1vCPU/RAM512MiB/disk512MiB；每VMM CPU100000100000、burst25000、weight100、memory671088640、swap0、pids256，`259:0 wbps=1048576`；outer8GiB/swap0/tasks2048/runtime1300/stop10/private24GiB。owner9slots/13312MiB合同及正常ledger11D+7S12800与实际必要回收保持。

生产source625条加12gitlinks、semantic index、HEAD/tree/branch/status和新的manifest637前后相同；13份静态材料、guest34/Nix27、新OCI12blobs/6reachable及实际package NAR前后相同。新guest/OCI与最终源码匹配，使用了新产物而非沿用013旧guest。

## 材料和原始证据

- 源码组合与local raw：`/tmp/final12-combination-t3-fe782bd5/`
- 独立SOURCE审查：`/tmp/final12-source-review-t3-798e69ce/`
- 新guest与Rust49：`/tmp/final12-guest-material-t3-9b274dc7/`
- 新GuestDir：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-9b274dc7/build/guest`
- 新OCI：`/tmp/final12-kvm-material-47f5/production-oci`
- OCI index：`21e2332014ed988e70c1f2bca2318e05a776ba7b925c7109f50e37eac9dbb151`
- OCI base manifest：`sha256:8486eb0a86a0a8844ad39169c144b8286010c0440d4880c6ca0824363802f13d`
- 材料post独立审计：`/tmp/final12-kvm-post-audit-t3-cff006ed/`
- 最终生产报告：`/tmp/final12-production-47f5/report.md`
- 原始生产测试：`/tmp/final12-production-47f5/run.O5vlk1Aj/test.log`
- 最终释放：`/tmp/final12-production-47f5/final-slot-release.json`
- 生产收据摘要清单：`/tmp/final12-production-47f5/final-receipt-hashes.json`，SHA256 `3d0e4a1afe64cebefe26feee8a7bcd318cccfd59813074b2edd85059a17c3996`

生产状态归档只STAT，247468693bytes，外层目录0700；未打开、列出或hash内容。运行授权全部false/expired，slot FREE，没有重试或进一步VM权限。

## 保留的限制

原KVM材料任务在gate已通过后，自有最终汇总脚本发生NameError，原任务STOPPED与失败记录保留。后续独立只读审计补齐新时刻post证据、获APPROVE并由父接受，没有修补重跑VM。早期pathname范围/不支持的offline设置、审计自身无效PGID解析错误和source-review自有probe缺Env的3FAIL也保留。

生产准备阶段FF前raw index SHA未采，已有semantic/source/refs证明，不重建缺失值。历史Rust执行binary hash未attest，当前binary与源码/fingerprint及本轮49实跑已核，未捏造历史hash。旧fixture长UNIX路径启动诊断race保持原历史等级，未在本次维护。namespace observer按原PID namespace销毁退出-9且已wait，不宣称graceful。IO证明是实时cgroup读回，未重测吞吐。

父`dev`仍在b93，未合并、未推送，初始非plans用户改动hash保持。没有升级任何现有部署；schema6不自动迁移旧schema4/5状态。016/017/020/021和无关fixture维护继续暂停。本次最终组合已完成并停止，无自动后续执行。
