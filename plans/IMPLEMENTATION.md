# 五项计划实施与验收记录

代码基准为 `da513a6`。本记录仅跟踪这五项计划的实现和实际验证，不将已知但未纳入的审计发现标为已修复。

## 当前状态

五项实现及复核修订均已应用到 `/home/woxQAQ/sandbox-101` 的暂存区，未创建父分支提交、未推送。源码与两个获批工作区的最终内容逐路径比较一致。组合后的独立全套验证全部通过，五项计划均为 DONE。验证过程中未修改源码或索引；本次仅补充验收文档与完成状态。

## 实现内容

| 计划 | 最终行为 |
|---|---|
| 001 | Operation 按类型和 principal 授权，结果目标逐一检查，令牌 map 重新生成；terminated token 的例外仅用于自身 renew/release 的 operation GET。 |
| 002 | Fake 校验完整请求幂等性；每个分支使用独立 release-vm 步骤；统一 release 实例证明；reaper 不再覆盖既有补偿责任；未知接纳重用原键，原删除完成后可收敛遗留 VMID。 |
| 003 | Running 发布、旧休眠引用及 tenant/存储计费责任原子交接；旧快照清理独立重试并尊重实时引用；存储额度在实际回收前保留；retired collector 与通用 orphan collector 不竞争删除序列。 |
| 004 | Schema 2 保存不可变 snapshot owner 与 root 外部导入授权；所有消费路径核验来源；root 捕获已有沙箱时使用源资源租户；旧 schema 1 在 DDL 前拒绝。 |
| 005 | Root 控制 capture 父目录，VMM 只写 CH 叶子；输入使用有界 no-follow/nonblocking FD；发布到新 inode；同步材料后才持久保存固定哈希 proof，重放不能重新认领损坏材料；GC 与空间预留同步调整。 |

## 代码交付来源

Sandboxd 工作区：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-debe32d2`。

按序应用：

- `a4924ec6cef9b4f5847b26509ab44cbfbcfb77b2`
- `f7b6a83d07ff685ac8d70857bac971c806a29f27`
- `0e498503c520b91a9ba9372fa9e2dd62653bce07`
- `1a6ceb8452decba943bfe23512b2b134b4aa9d57`
- `1f1457f585cb9e151c3253b58d6bb17647b1d84a`
- `f5ec49e7ffeb2171f003a4dbcb06930f5f8d576b`
- `6a98e8d0a8c2552af65232a22b8db4fb43fddda5`
- `56d484d6e928a93d6f2add895f5d2981bec43c3b`
- `a94c0f499505358c6734572b4c7fde897b10b111`
- `7bd1239f331c95909ada3c96bfd6afda2c87c759`

Engine 工作区：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-43b862e6`。

按序应用：

- `ce99c97ba4e9b1a75876a7677c9125a72d46f36b`
- `a6fc66a2ff595f86bdf68abe826b547e5f9165ba`
- `20feaad7091b323e6612bd096bbd988cf4dd5837`

整合采用 `git cherry-pick --no-commit`，保留上述来源而不改写父分支 HEAD。

## 独立代码复核

两组实现均经历两轮独立复核，首轮问题修复后第二轮 APPROVE：

- Sandboxd：root 捕获租户归属、maintenance/reaper 实例证明旁路、删除响应丢失后的原 receipt 跟踪与不存在 VM 收敛均补充 RED→GREEN 回归。
- Engine：修正完成标志先于材料持久化的问题。captured/source_resumed/capture_copied 尚无固定 proof 时，恢复先处理源恢复，再拒绝发布并保留材料；同步全部独立文件和目录后才保存 Materialized/CaptureFiles。损坏模型包含截断、等长清零和丢失，不声称做过实际物理断电。

## 已取得的隔离工作区验证证据

以下结果属于最终工作区实现；组合验证将单独记录，不能用本节替代。

- Sandboxd：所有计划命名回归、root HTTP 捕获及 receipt replay、完整 maintenance 与 lost-delete-response 测试通过；全包 race 23.522 秒，vet 通过；fork rollback race 重复十次 23.971 秒；lint 0 issues，真实 Rust integration 0.596 秒，实际 daemon smoke 四项通过。
- Engine：30 个材料损坏/丢失场景及阶段恢复回归通过；强制原生工具全 engine race 87.303 秒，vet/lint/capture-tag lint 通过；真实跨 UID 套件无 skip。两个镜像布局 KVM 验收 10.418 秒；最终修订版 production 104.21 秒，保持原 VMM UID/GID、capability 与资源限制。
- Engine 最终修订证据：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-43b862e6/build/plan005-revision/`，其中 `production/run.RoWMLVgm/` 为最终 production owned run。

父线程已独立重跑 sandboxd 最终代码全包 race 25.656 秒、vet、workspace lint 0 issues；重跑跨 UID capture 套件及关键 fork/root 捕获 race。早期 engine 全 race 的 60 秒父 shell 等待超时没有完整结果，不计作通过或代码失败；其后工作区 detached 全套 exit 0 已单独记录。

## 组合验证

全部通过。完整报告、每条命令、日志、退出码和耗时位于 [REPORT.txt](../build/implementation-final/run.Jd1dCRzq/REPORT.txt)。下表耗时均为命令 wall time，production 另列测试本身耗时。

| 最终组合门禁 | 结果 | 耗时 |
|---|---|---|
| Workspace lint / capture-tag engine lint | 均 0 issues | 1.869 / 1.777 秒 |
| engine / sandboxd / protocol Go vet | 全部通过 | 0.287 / 0.152 / 0.093 秒 |
| Rust fmt / clippy / workspace all-targets | 通过，29 tests、0 ignored | 0.125 / 0.118 / 1.774 秒 |
| 强制 native EROFS 的完整 engine race | 通过，SANDBOX_REQUIRE_EROFS_TESTS=1 | 86.214 秒 |
| 完整 sandboxd / protocol race | 通过 | 25.149 / 1.323 秒 |
| 实际跨 UID capture isolation | 四项场景全部执行通过，无跳过 | 0.721 秒 |
| 实际 Rust fixture integration | 通过 | 1.236 秒 |
| 实际 Rust fixture race integration | 通过 | 2.789 秒 |
| 重建后的 daemon / CLI smoke | 四项 PASS | 2.746 秒 |
| 组合 production microVM | 通过；测试本身 99.56 秒 | 103.966 秒 |
| Working / index diff check | 通过 | 0.107 秒 |

Production 证据位于 [production/run.eVLB3y9x](../build/implementation-final/run.Jd1dCRzq/production/run.eVLB3y9x/)，清理和身份核验见 [production-audit.json](../build/implementation-final/run.Jd1dCRzq/production-audit.json)。覆盖 live daemon adoption、完整生命周期、golden/warm 供应与真实 guest TCP；guest 512 MiB，VMM 内存上限仍为 640 MiB。before/after/warm 均为 UID/GID 65534、空附加组、五组 capability 全零、NoNewPrivs=1、Seccomp=2。13 个 VMM scope 均无 OOM kill。临时 service 退出 0/SUCCESS，复查为 inactive/not-found，所属 cgroup 已消失，VM 槽位已释放。

初始 harness 将 TMPDIR 放在过深的证据目录，造成 Unix socket 超长；home 祖先权限还阻止降权 writer 执行。原始失败日志均保留。改用该次新建的短临时目录后，受影响门禁逐项重跑全部通过，没有修改源码。没有遗留失败或跳过的必需门禁；普通 Go 非 verbose 输出不能枚举内部可选测试的 skip。

验证前后 source index fingerprint 一致：`aa40e8f258136cbe8452c5d641ffc644c7c5c1cea22eb28823c4aa821d16f46b`；working source checksum manifest 一致：`8c68eb6743689dbf6bc11b8cd86ae5a9c035643194a4d4b85275fbbc566650ec`。这些指纹只覆盖代码与构建配置，不含父线程维护的 prose/plans。

复验时须使用短且可被映射 writer 遍历的临时路径、受支持的原生工具及新建隔离资源；完整命令和环境记录于报告旁的 `.command` 文件。不得直接对旧数据库/daemon 运行验收。

## 部署边界

- Sandboxd schema 2 与 schema 1 不隐式兼容。保留旧状态，先用匹配的旧版本完成一致备份，再评审显式迁移；不得删除旧数据库或运行 init 覆盖来绕过来源检查。
- 新 engine restore 要求 IndependentInodes 发布证明。缺少该证明的旧快照/旧捕获 journal 被明确拒绝并保留材料，不自动升级为可信数据。
- 捕获在持久 proof 完成前中断，可能以 Incompatible 保守结束；这不代表已发布快照损坏，也不会从继续运行的源重建旧状态。
- 原审计中 FIFO RPC、ExpectedInstance 的 start/restore 重放、模板 ID 并发归属、模板/warm pool 完整配额及其他未选项仍不在这五项交付范围内。
