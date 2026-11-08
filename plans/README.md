# 实施计划索引

**[022 按职责重整文件、测试单元和公有边界](022-responsibility-and-api-boundaries.md) 已DONE。统一分支`t3/final-twelve-source-combination`现为HEAD `33a3f829e70969a0f4269b999fed437398e0c54a` / tree `56c1e1e319ae401b7b84b4a9cb5309284db2b98f`，原工作树干净。完整声明/binding/init/build等价及7项内部API收口独立SOURCE_APPROVE；新Engine448/SB678/protocol3完整race、vet/lint/格式通过，原owner唯一FF及父最终source/index核验通过。见[022交付](022-RESULT.md)、[最终接受](evidence/022-final-acceptance.json)。本轮完成并停止，父dev未合并/推送，016/017/020/021与无关维护继续暂停。下方47f5是原12项组合的历史验收快照，其VM/production/Rust49成绩不转为33a3的新运行成绩。**

**最终12项组合已验收通过（FINAL12_COMBINEDPASS）：`t3/final-twelve-source-combination`，HEAD `47f5ce71bfa8de2720e184fec516f0e27ca90f2d`，涵盖006/007/008/009/010/011/012/013/014/015/018/019。独立SOURCE_APPROVE/65交互测试、sandboxd完整race678PASS、fresh Rust49、匹配新guest/OCI的双格式KVM及唯一完整production5RUN5PASS/141.101s均通过。父已核13台managedVMM liveguards、109PID/26PGID及unit/cgroup/mount清理，所有grant已失效、slot FREE。见[最终交付报告](FINAL-COMBINATION-12-RESULT.md)、[review入口](FINAL-REVIEW-12.md)和[最终接受收据](evidence/final12-47f5-final-acceptance.json)。**

**上一阶段“做最终组合吧”已完成并停止其运行授权。当前022结构重整由用户新指令单独授权；016/017/020/021及无关fixture维护继续暂停。父dev仍为b93、用户改动保留，未合并、未推送，也未升级现有部署。**

当前批次由 improve 基于 commit `b93fc24` 于 2026-11-06 编写。用户已授权落实全部 16 份 `006–021`；006/007/008/009/010/011/012/013/014/015/018/019 共12项已在隔离分支复核通过。012精确78bc04f已完成完整production及清理释放；013精确1414b4e已获双独立SOURCE_APPROVE/七项关闭，唯一原始production142.601s/5RUN5PASS、13VMM liveguards和89PID/26PGID清理已父核验接受，grant已失效、slotFREE，013 DONE后当时已按要求STOP。用户随后另行明确授权最终组合，上述12项已汇总并获最终COMBINEDPASS，完整交付见上方新分支与最终报告。016原外部门禁BLOCKED、017未启动、020/021原未决映射保留并继续暂停。执行、复核及材料交接见 [执行记录](EXECUTION-006-021.md)。`001–005` 已完成并归档至 `.agents/archive/`，保留下方历史记录。

每份新计划独立包含问题、源码摘录、设计约束、修改范围、分步验证、行为回归、完成标准及 STOP 条件。验证命令使用 Nushell，新增验收入口与现有命令明确区分。S 约数小时，M 约一天，L 为多日，均包含测试；Risk 表示修复本身的风险。计划完成不代表代码已实现或验收已通过。

## 当前批次：006–021

| 计划 | 目标 | 审计映射 | 优先级 | 工作量 | 风险 | 依赖 | 状态 |
|---|---|---|---|---|---|---|---|
| [006](006-mitm-header-bounds.md) | 限制 CONNECT 内层 HTTP 解析资源 | 发现 1 | P1 | M | MED | 无 | DONE（隔离分支） |
| [007](007-configure-egress-revocation.md) | 配置切换失败时撤销旧代理权限 | 发现 2 | P1 | M | MED | 无 | DONE（r5独立源码复核通过） |
| [008](008-filesystem-fifo-bounds.md) | 文件 RPC 及时拒绝 FIFO 并释放名额 | 发现 3 | P1 | S | LOW | 无 | DONE（隔离分支） |
| [009](009-template-admission-ownership.md) | 首次接纳模板即持久化租户归属 | 发现 4 | P1 | M | MED | 007 | DONE（隔离分支） |
| [010](010-restore-candidate-durability.md) | 候选复制完成并落盘后才交接恢复材料 | 发现 5 | P1 | M | MED | 无 | DONE（隔离分支；匹配 guest 双格式已过） |
| [011](011-instance-condition-recovery.md) | 实例条件允许接续原操作自身候选 | 发现 6 | P1 | M | MED | 010 | DONE（隔离分支） |
| [012](012-resource-quota-ledger.md) | 统一资源计费责任，删除完成前不退额度 | 发现 7／账本基础 | P1 | L | HIGH | 009、010、011 | DONE（精确78bc04f完整production通过） |
| [013](013-template-build-quota.md) | 接入构建、模板快照与预热存储计费 | 发现 8／池治理 | P1 | L | HIGH | 012 | DONE（精确1414双SOURCE及完整production通过，清理释放；该项收口完成） |
| [014](014-sdk-call-cancellation.md) | 排队中的 SDK Call 可取消 | 发现 9 | P2 | S | LOW | 无 | DONE（隔离分支） |
| [015](015-schily-xattr-names.md) | 保留 SCHILY xattr 的字面名称 | 发现 10 | P2 | S | LOW | 无 | DONE（隔离分支） |
| [016](016-native-image-ci.md) | 强制原生 OCI/EROFS CI 与统一 lint 工具链 | 发现 11 | P2 | M | LOW | 无 | PAUSED（用户收口；原CI/root/Nix gate仍BLOCKED） |
| [017](017-bounded-resource-recovery.md) | 持久待核对集合与有界恢复扫描 | 发现 12 | P2 | L | HIGH | 013 | PAUSED（最终组合仅限12项已完成成果，不续实现） |
| [018](018-tree-push-cleanup-outcomes.md) | 区分 tree push 已提交与清理未完成 | 发现 13 | P2 | M | MED | 019 | DONE（隔离分支） |
| [019](019-tree-pull-identity-investigation.md) | 双 UID 验证 tree pull，并保持授权 FD | 调查项，MED 置信度 | P1 调查 | M | LOW | 无 | DONE（调查限定） |
| [020](020-provisioning-observability-design.md) | 有界供应与维护观测合同 | 方向 1，设计/验证 | P3 | M | LOW | 无 | PAUSED（用户收口；已有设计与未决映射保留） |
| [021](021-readonly-recovery-diagnostics-design.md) | 双 daemon 只读诊断合同 | 方向 2，设计/验证 | P3 | M | LOW | 无 | PAUSED（用户收口；已有设计与未决映射保留） |

### 当前批次依赖与执行顺序

优先启动 006、007、008、010；009 接续 007 的持久配置 fence/schema。同一 checkout 内不要并发编辑共享文件。逻辑上可并行的工作也应通过独立 checkout 与明确的最新基准交接，不能让未提交计划在新 worktree 中消失。

- **007 → 009 → 012 → 013 → 017**：先建立配置失败的持久阻断，再建立模板 owner、统一资源计费责任，接入 build/template/warm，最后优化恢复集合。各自计划目标 schema 为 3、4、5、6、7；根dev仍为b93/schema 2；已验收的012隔离候选为schema 5，计划数字不表示根分支已合并或真实库已迁移。执行期间只使用新测试状态，现有真实旧库保留并明确拒绝，不自动迁移或重初始化。
- **010 → 011 → 012 r3**：先证明候选材料完成交接，再允许同一操作接续其新实例；012 r3 在已批准来源上窄加不可变结果实例 witness/finish 原子发布。共同修改 operations.go，串行落地。007r3自身补丁获批后是012最终组合/production前置，不接整棵stage2或guest018。
- **019 → 018**：先建立真实双 UID fixture/runner 和默认关闭的测试 feature，再验证混合 owner 与提交后 cleanup 结果；018 必须在 019 后执行，即使编号更小。008/015 可先完成，但与两者共享 fs.rs，应串行或明确合并。
- **016** 可独立启动，是后续 OCI/EROFS 修改的前置门禁；它不替代 010 的真实 KVM、012/013 的真实生命周期和 019/018 的双 UID 验收。缺必要材料须标 BLOCKED。
- **014、015** 独立且较小，可穿插；**020、021** 交付设计/验证文件，完成状态应明确为“设计完成”，不得写成生产指标/诊断 API 已上线。
- 012、013、017 已因持久模型与恢复范围将估算提升为 L/HIGH；不以独立 special case 拼接来维持最初较小估算。

### 当前批次文档复核

全部计划已完成两轮独立只读冷读。首轮据反馈修订：配置持久 fence 与自动重绑入口、候选失败后 lease/操作归属的保留与显式 delete、内层明文拒绝语义、SDK 取消回调交接、CI JSON/固定 root namespace 验证、tree 提交/同步/清理独立结果、root 计费 tenant、精确预算与 warm admission、恢复输入/输出依赖、collector 自身引用和持久删除 fence。

第二轮四组均为 **APPROVE**：

- 006/007/010/011：egress 与 engine 恢复计划，原三项阻断已解决。
- 009/012/013/017：模板、统一账本与有界恢复，原七项阻断已解决。
- 008/015/018/019：guest 权限与树传输，持久化错误继续保留 Frame.error，双 UID/feature 边界明确。
- 014/016/020/021：SDK、CI 与设计计划，取消回调收尾、JSON 门禁和固定 runner 身份明确。

文档结构检查已确认 16 份连续编号、基准/date/TODO、代码围栏、索引/归档链接和无环硬依赖；`git diff --check` 通过。上述结果只说明计划的交接质量，不表示源码实现、运行测试或生产验收已完成。

## 已归档计划

001–005 已在 `b93fc24` 中实现，计划与验收记录已移至 `.agents/archive`。以下保留实施过程及验证背景。

| 计划 | 目标 | 优先级 | 工作量 | 风险 | 硬依赖 | 状态 |
|---|---|---|---|---|---|---|
| [001](../.agents/archive/001-operation-result-authorization.md) | 按调用方权限读取 operation，并限制结果令牌签发 | P1 | S | LOW | 无 | DONE |
| [002](../.agents/archive/002-fork-compensation-keys.md) | 严格校验 fake 幂等契约，独立回滚每个 fork 分支 | P1 | S | LOW | 无 | DONE |
| [003](../.agents/archive/003-resume-snapshot-handoff.md) | 将运行状态与旧快照清理、存储计费责任原子分离 | P1 | M | MED | 无 | DONE |
| [004](../.agents/archive/004-snapshot-tenant-authorization.md) | 建立快照来源归属与模板引用授权 | P1 | L | MED | 无 | DONE |
| [005](../.agents/archive/005-privileged-capture-boundary.md) | 隔离 VMM 捕获写入与 root 快照发布 | P1 | L | MED | 无 | DONE |

状态值：`TODO`、`IN PROGRESS`、`DONE`、`BLOCKED（原因）`、`REJECTED（原因）`。执行者完成后记录实际验证命令与结果；必要验收无法执行时标 BLOCKED，不能因普通测试通过而标 DONE。若 reviewer 管理索引，由执行者交付结果、reviewer 更新。

## 归档批次的依赖与并行安排

推荐顺序为 **001 → 002 → 003 → 004**。这些计划没有必须整份依赖前一份的逻辑关系，但共享 service/lifecycle/recovery 与测试 fixture，串行能减少冲突并让后续计划复用更严格的后端替身。

- 002 内部存在硬顺序：先让 fake 拒绝同键不同参数，再复现和修复分支回滚；不得跳过。
- 003 可独立实施；若 002 已完成，必须保留其资源级 release 步骤和严格 fake。旧快照移入 cleanup 时必须同步承接现有存储计费，不能因清空 SnapshotID 提前释放租户额度。
- 004 可独立实施，但和 003 都涉及元数据与引用发布，建议待 003 合入后再动工。它将内部 schema 提升为 2，默认拒绝无可信来源的旧状态；真实旧状态迁移需求须先报告，不自动推断或删除。
- 005 主要位于 engine，可与 sandboxd 计划在独立 checkout 并行。若同时运行真实 VM 验收，要协调 CPU/RAM/磁盘与临时存储容量，不能多个大测试争用材料后把容量失败误报为代码失败。
- 独立 checkout 必须包含计划依赖的最新本地提交及这些文档。T3 child worktree 不复制未提交文档；派发前应明确传递计划内容或使用包含文档的基准，不能假定子任务能看见本目录。

## 计划复核记录

首轮独立冷读指出两个阻断项，均已落实到文档：

1. **003 的计费交接遗漏**：已补入 `Service.quota` 及其文件范围，明确在原子交接时持久化 tenant/内存计费量，按 tenant/snapshot 去重，实际回收前保留额度。新增多轮 pause/resume 删除失败与重启计费行为回归；不扩展模板/warm pool 配额。
2. **004 的现有测试范围遗漏**：已将 `maintenance_test.go`、`warm_fallback_test.go` 加入 Scope 与 drift check；明确将未来版本保护改为高于当前 schema，golden fixture 经 root 为目标 tenant 显式导入，并单独列出原有测试门禁，保留恢复断言。

001、002、005 首轮未发现同等级阻断项。第二轮独立冷读已确认 003、004 的两个阻断项在计划层面解决，未发现剩余同等级阻断项。上述说明仅是计划评审结果，不代表实现或测试已完成。

## 实现复核进展

两组实现均已在隔离工作区完成，首轮实现复核发现的下列边界已修订并经第二轮独立复核 APPROVE：

- 004：root 捕获已有沙箱时，快照租户归属必须来自可信源资源，不能直接使用调用方的默认/所选 tenant。
- 002：完整 maintenance/reaper 路径不得绕过 unknown-instance 证明；删除已成功但响应丢失后须能安全收敛残留引用。
- 005：独立材料在完成标志持久化前必须完成落盘；恢复时不能对未同步且可能损坏的磁盘材料重新计算合法 manifest。

上述修订已全部整合至根工作区暂存区，未创建父分支提交。最终组合验证全部通过：强制原生工具 engine race、sandboxd/protocol race、Go/Rust 静态检查与测试、实际跨 UID 隔离、Rust 中继及 daemon smoke、production microVM 全流程。Production 测试 99.56 秒，临时服务/cgroup 已清理，验证前后源码指纹一致。提交来源、RED→GREEN、复核与完整日志见 [实施与验收记录](../.agents/archive/IMPLEMENTATION.md)。

## 两项实现边界

**004 的快照授权**：`published` 仅表示可恢复，不授予租户读取权。最小模型是不可变 owner 与管理员明确外部导入，不引入隐式全局共享。计划估算从审计粗估 M 调整为 L，因为持久来源、重放、备份与元数据边界必须一并完成。

**005 的安全验收**：目录 fd 防止路径替换，新发布 inode 防止源写 FD 继续修改；二者缺一不可。它需要多 UID namespace 测试及当前真实 KVM/生产 launcher 验收。环境不满足时保留 BLOCKED 和具体证据，不扩大 VMM 权限或借用历史通过记录。

## 审计中考虑后排除的事项

以下是已记录设计边界，不作为这五项的缺陷或附带扩展：

- metadata backup 不复制 VM 磁盘/RAM；本目录不扩展全量备份产品。
- keyed exec 账本随 VM 快照回滚，跨回滚去重需要外部 fence；本目录不承诺更强语义。
- Preview 共享 origin、非递归 watch、managed-only process attach 是已记录限制。
- Btrfs stat 与 proc maps 身份不一致时严格拒绝 COW；不通过放宽身份检查改善兼容性。
- 单机范围、未实测的性能/密度目标不当作已完成能力。

## 原批次保留发现与本轮映射

以下原先保留的事项现已覆盖进 006–021；这表示已计划，不表示已修复：

- 文件 RPC 打开 FIFO 可阻塞 worker/permit。
- start/restore 的 ExpectedInstance 与自身新实例持久化之间的恢复冲突。
- 首次模板提交与异步发布之间的跨租户归属竞争。
- template build、golden snapshot 和 warm pool 尚未进入完整租户配额。
- CI 缺少强制原生 OCI/EROFS 测试；lint 与 Nix devShell/CI 的工具范围未对齐。
- guest tree pull 的特权重开临时路径、cwd 与降权顺序仍需独立双 UID 验证；不将未完成复现当作已确认漏洞。
- 供应延迟、池状态指标与只读恢复诊断属于方向选项，另行选择。

## 后续标准审计（b93fc24，全部已选为计划）

[本轮审计记录](AUDIT-b93fc24.md) 已核实 13 项后续发现，完整证据、风险、验证边界、调查项与排除理由均在记录中。用户已选择全部 13 项、1 项调查和 2 个方向；对应 006–021 及执行依赖见上表。审计发现编号与实施计划编号分开，用户已授权落实全部计划，当前状态以上表及执行记录为准。

本轮 `make test`、`make check`、`make lint`、真实 Rust fixture `make integration` 通过。原生镜像 TestActual 场景因工具缺失全部跳过；未重新运行特权／KVM 验收。`tree_pull` 的双 UID 路径重开仍是调查项，不作为已证实漏洞。新增排除事项包括已核验的 DNS／CONNECT authority 检查、direct cleanup 的实际关网顺序、普通 FD 读取授权及未证实的 cwd／PID 复用假设，详见审计记录“已考虑并排除／降级”。

## 执行与审查纪律

- 不输出任何真实令牌、秘密、`.env` 值；测试使用生成的无实际用途数据，不复用运行状态。
- 不操作 `.local-run/`、现有 daemon、未知归属文件或真实旧数据库。
- 不安装依赖、不修改宿主 NixOS 配置，不因为本地缺少前提而降低验证标准。
- 不为未发布内部 helper/步骤名保留多余兼容层；已存在状态无法安全重放时要明确拒绝并保留证据。
- 测试应覆盖实际 Service/HTTP/SQLite、跨 UID 文件系统或 Engine 生命周期行为，不增加独立函数的实现镜像式单测。
- 不因计划存在就提交、推送、开 PR 或启动执行代理；执行授权与计划交付分开。
