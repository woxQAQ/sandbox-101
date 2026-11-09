# 标准深度审计：b93fc24

审计基准：`b93fc24`，开始时工作区干净。审计等级为 standard。本文是发现与取舍记录；后续用户已选择全部事项，独立计划为 `006–021`，映射与状态见 `plans/README.md`。既有 001–005 已另行归档至 `.agents/archive/`，不重复立项。本轮规划仅修改 `plans/`，未实施源码变更。

## 结论与验证边界

现有验证基线有效，问题主要集中于权限／资源责任切换和恢复中的中间状态。四个只读子任务分别审查 engine、sandboxd 编排、Rust guest、egress/HTTP/SDK；主审重新打开了下列所有源码证据，去除重复及超出证据的结论。

本轮实际运行并通过：

- `GOPROXY=off GOSUMDB=off make test`：三个核心 Go module 及 Rust workspace all-target 测试通过。Go engine 根包约 35.2 秒，sandboxd 根包约 12.5 秒；Rust 共 29 个测试通过。
- `GOPROXY=off GOSUMDB=off make check`：Go vet、Rust fmt check、clippy warnings-denied 通过。
- `GOPROXY=off GOSUMDB=off make lint`：包含 operator 的四个 Go module，0 issues。
- `GOPROXY=off GOSUMDB=off make integration`：真实 Rust Unix fixture 与 Go transport 集成通过。
- `GOPROXY=off GOSUMDB=off GOWORK=off go -C engine test -run '^TestActual' -count=1 -v .`：命令返回成功，但原生镜像场景及相关子场景全部因缺少 `bwrap` 跳过；PATH 同样缺少 mkfs.erofs、umoci。此结果仅用于确认 skip 行为，不计作原生路径验证通过。

本轮没有新增故障注入测试或漏洞复现程序。下列 HIGH 表示明确的控制流／资源边界证据，不表示每项均已运行复现。未重新执行 race、smoke、KVM、生产 launcher、kernel/capture isolation 验收，也未将旧验收记录视为本轮复验。当前 Go 工具链为 1.27.3，CI 固定为 1.26.5，本地结果不替代固定 CI 工具链结果。

覆盖：核心 daemon 生命周期与恢复、模板和租户准入、文件 RPC、guest 进程权限边界、代理和客户端、CI／Nix／依赖配置／设计文档。仅抽查 OCI/GC 实现与已有测试，不是完整 tar/EROFS 编码器审计；未逐行审计 third_party、生成代码、构建参考仓库、operator scaffold 的全部部署模板。未访问 `.local-run/`、启动现有 daemon 或使用真实数据库。govulncheck、cargo-audit 未安装，因此未执行在线漏洞公告审计，不作“依赖无已知漏洞”的结论。

## 已核实发现

S 约数小时，M 约一天，L 为多日，均包含行为回归；Risk 指修复本身的风险。除另列调查项外，以下控制流结论置信度均为 HIGH。排序综合影响、工作量及修复风险，前五项建议作为第一批计划。

| # | 发现 | 类别 | 影响 | 工作量 | 修复风险 | 主要证据 |
|---|---|---|---|---|---|---|
| 1 | CONNECT 内层 HTTP 缺少请求头大小边界 | 安全 | 单条 relay 连接可在策略检查前消耗共享 daemon 内存 | M | MED | `sandboxd/egress/http.go:95`；goproxy `https.go:466` |
| 2 | configure 前置失败保留旧 relay 权限和凭据 | 安全／正确性 | 已保存的禁网、收紧策略或凭据轮换不能撤销旧代理 | M | MED | `sandboxd/lifecycle.go:348`、`:42`；`sandboxd/reconcile.go:171` |
| 3 | FIFO 打开可永久占用文件 RPC permit | 正确性／可用性 | 断连不释放阻塞 worker，最终耗尽 128 个 workload 名额 | S | LOW | `guest/agent/src/fs.rs:175`、`:358`；`guest/agent/src/server.rs:122` |
| 4 | 首次模板提交没有持久化归属占位 | 安全／并发正确性 | 已接纳的异租户构建可以覆盖模板目录归属 | M | MED | `sandboxd/service.go:645`、`:692`；`sandboxd/lifecycle.go:813` |
| 5 | restore 候选副本以“存在”代替完整性证明 | 数据一致性／恢复 | 中断复制或同步前损坏可被当作候选材料继续交接 | M | MED | `engine/operations.go:839`、`:876`、`:900`；`engine/storage.go:114` |
| 6 | ExpectedInstance 阻断接续自身新实例 | 正确性／恢复 | start/restore 崩溃后永久 Conflict，无法完成原操作接管 | M | MED | `engine/operations.go:45`、`:328`、`:939` |
| 7 | release 在物理回收前释放租户计费 | 配额／正确性 | 删除延迟、失败或崩溃期间仍占用资源却可再次准入 | M | MED | `sandboxd/lifecycle.go:644`、`:649`；`sandboxd/service.go:769` |
| 8 | template build 与模板保留快照绕过租户配额 | 配额／安全 | 可持续分配未计费构建资源、保留黄金或 checkpoint 快照 | L | MED | `sandboxd/service.go:628`、`:748`；`sandboxd/checkpoints.go:41`、`:71` |
| 9 | SDK 等待串行锁期间不响应取消 | SDK 正确性 | 排在长 RPC/watch 后面的短 deadline 调用可无限等待 | S | LOW | `sandboxd/sdk/client.go:260`、`:268` |
| 10 | SCHILY xattr 名称被错误 percent decode | 数据正确性 | 合法含百分号名称被拒绝或静默改名 | S | LOW | `guest/agent/src/fs.rs:717`、`:735`；`guest/agent/tests/tree.rs:594` |
| 11 | CI 没有强制执行原生镜像契约测试 | 测试／DX | tar→EROFS→实际提取的主要行为回归可全部跳过而保持绿灯 | M | LOW | `.github/workflows/check.yml:21`；`engine/fsmeta_test.go:23` |
| 12 | 每轮恢复扫描全部历史操作并逐项查询 engine | 性能／架构 | 维护成本随总历史增长，reap 与补池在扫描之后执行 | M | MED | `sandboxd/recovery.go:86`、`:185`、`:1064`；`sandboxd/reconcile.go:50` |
| 13 | tree push 忽略交换后旧树清理失败 | 正确性／资源回收 | 成功返回后仍可能遗留完整旧树并累积磁盘占用 | M | MED | `guest/agent/src/fs.rs:1250`、`:1343`、`:1363` |

### 1. CONNECT 内层请求头边界

**证据与触发边界**：`sandboxd/egress/http.go:95` 使用 `ConnectMitm`。`:228` 的 `MaxHeaderBytes` 仅覆盖外层 net/http server。实际锁定依赖是 `github.com/elazarl/goproxy v1.9.2`（`sandboxd/go.mod:6`），其本地源码 `github.com/elazarl/goproxy@v1.9.2/https.go:466` 为隧道新建 RequestReader；`internal/http1parser/request.go:54` 直接调用 `http.ReadRequest`。主审读取的 Go 1.27.3 `net/http/request.go:1095`、`:1152` 显示该路径以 MaxInt64 限制解析头，并读取没有业务长度边界的请求行。

relay guest 完成获准目标的 CONNECT/TLS 握手后，内层解析在请求过滤前发生。持续输入不能被 `sandboxd/egress/manager.go:450` 的每次读刷新 idle deadline 限制总大小；连接数限制也不限制单连接解析内存。影响为共享宿主进程可用性，不需要先成功建立 upstream 连接。

**修复方向与验证**：对每条内层请求的 request line、累计 header 字节、header 完成时间建立边界。不能简单限制整个 TLS 连接总字节数，否则会破坏合法大 body 与复用。需要真实代理 UDS CONNECT/TLS 行为回归，覆盖超限拒绝、正常流式大 body、同隧道多请求。风险 MED；依赖适配方案需要先明确，不以未核实的上游升级代替边界验证。

### 2. 配置切换失败时旧代理仍有效

`service.change` 在 `sandboxd/lifecycle.go:329` 替换内存秘密集，`:348` 保存新策略，随后调用 activate。activate 在 `:22` 查询 VM、`:30` 执行 network-off、`:42` 请求 Agent.Info；这些步骤失败都会在调用 bind 前返回。旧 binding 只有在 `sandboxd/bindings.go:36` 或后续部分失败处理才被撤销。

因此，原运行 relay 沙箱切换到 none、收紧授权目标或轮换凭据时，Info 等前置步骤失败可留下旧 listener、已有连接和旧秘密。network-off 不关闭 vsock relay。`sandboxd/reconcile.go:171` 对 none 不重绑，对其他模式仅检查 binding 是否存在，不比较其当前配置指纹，因而不会自行修正这个残留。LastError 对新数据面接入的阻止不能代替 egress 撤权。

**修复方向与验证**：明确配置发布与旧授权撤销顺序；任何接续新配置的前置失败都应留下可证明的关闭状态或明确保留的旧配置，不能让已保存策略与实际代理长期不一致。测试以实际 Service+egress 为边界，注入 Info 失败，检查旧 socket、新连接、已有连接及凭据轮换；保留已有 direct cleanup 先关网语义。风险 MED。

### 3. FIFO 阻塞不能随断连取消

`guest/agent/src/fs.rs:175` 先阻塞 File::open，`:177` 才判断普通文件；fs_replace 在 `:358` 同样先 open，再验证类型。O_NOFOLLOW 不能阻止 FIFO 等待写端。`:150` 的 spawn_blocking 不可通过取消等待者中止正在执行的 open；`guest/agent/src/server.rs:122` 的 permit 持有至 dispatch 返回，`:118` 的连接取消不构成该 syscall 的取消机制。全局名额在 `guest/agent/src/lib.rs:67` 固定为 128。

**修复方向与验证**：以非阻塞方式打开并在同一 FD 上校验类型，保持现有允许的普通文件／符号链接语义。通过真实 FIFO 的 RPC 行为测试验证及时失败、断连后名额恢复、并行 info 正常完成；无需增加独立 helper 单测。风险 LOW。

### 4. 模板首次发布归属竞争

`Submit` 在 `sandboxd/service.go:645` 仅检查 templates 中已发布的记录；`:692` 只保存 operation。`sandboxd/lifecycle.go:682` 的模板锁作用于异步执行，无法保护两个尚未发布的首次提交。`:813` 的 UPSERT 无 owner/version 条件。

A 的同名模板构建未完成时，B 可以提交自身的同名模板。两次 admission 均看到不存在；串行发布后，后一 operation 仍会覆盖前者 tenant 和内容。影响是模板目录与后续更新／claim 的归属，不据此宣称 B 能读取 A 的快照：schema 2 snapshot_owners 仍独立校验。

**修复方向与验证**：在 admission 事务原子持久化模板 ID 归属和发布意图，以条件提交阻止跨租户覆盖；明确失败构建与 root 管理语义。回归覆盖双租户并发首次提交、首个构建失败和重启接续。风险 MED。

### 5. restore 候选材料缺少复制完成证明

`engine/operations.go:776` 校验源快照；`:839` 对已存在的候选 config/state 跳过复制。`engine/storage.go:114` 在最终名称创建目标后才执行 reflink／稀疏复制，SIGKILL 不执行失败清理 defer。`:876` 对已存在候选磁盘也跳过复制；磁盘 rename 在 `:886`，syncTree 在 `:890`。随后 `:900` 开始持久化依赖转移并释放源 lease，没有验证这些候选副本与源清单一致。

在非 reflink 复制期间退出会留下最终名称但不完整的状态文件；磁盘 rename 后、同步前的掉电／数据丢失也可能留下名称存在但内容受损的候选。重放验证完整源并不能证明目标副本完整。现有 `engine/engine_test.go:839` 的恢复测试从 restore_resources_ready 之后开始，未覆盖这些边界。本轮没有实际断电或复现该数据丢失窗口。

**修复方向与验证**：仍持源租约时，完成私有候选集的复制、校验、同步和完成证明，再交接。重放只能在未交付 VMM 前安全重建候选；已经用于运行、发生写入的磁盘不得重新覆盖。回归需要复制中断、同步前损坏、源快照保留／删除及交接后继续接管。风险 MED。

### 6. 实例条件与自身候选实例冲突

`engine/operations.go:45` 每次执行都比较原请求 ExpectedInstance 与当前 VM.Instance。start 在 `:328` 记录候选 journal、`:332` 保存新实例；restore 在 `:939`／`:949` 同样先保存新实例再完成启动。

已有停止实例 A 的 VM 接受 ExpectedInstance=A 的 start/restore，换为自身候选 B 后崩溃；重启入口比较 A 与 B，直接固定为 Conflict，无法进入原 journal 接管。B 已存活时也可能尚未完成 agent 协作。准入条件本来合法，拒绝自身后继实例并非过期调用保护的预期行为。`engine/INTEGRATION.md:42` 后的 Actions 明确支持这两个操作的可选 ExpectedInstance。

**修复方向与验证**：分离准入时的原实例条件与接续时的候选归属证明；只允许 operation 自己已持久化且实际身份匹配的候选继续。覆盖 start/restore 新实例落库、launch 后、agent-ready 前后的崩溃，以及真正外部实例变化仍拒绝。风险 MED。

### 7. release 提前释放配额

`sandboxd/lifecycle.go:644` 先保存 terminated 以吊销访问，`:649` 才删除 VM；失败时仍保留 VMID。快照删除也在 terminal marker 之后。`sandboxd/service.go:769` 无条件排除 terminated 记录；`:822` 仅补计 retired_snapshot，未覆盖 release 的 sandbox recovery。`sandboxd/recovery_test.go:93` 已证明 fork 补偿可留下 terminated 且仍持 VMID 的记录，但没有验证期间的配额准入。

**修复方向与验证**：保留访问立即吊销，同时原子保留或转移未回收 VM／快照的计费责任；确认删除后再解除。覆盖阻塞删除、失败、响应丢失及重启后新 claim 的配额判定，避免与既有 resume 清理责任重复。风险 MED。

### 8. 构建和模板快照未计费

`sandboxd/service.go:628` 的模板分支没有 quota 或资源预留；构建在 `sandboxd/lifecycle.go:685` 直接分配 VM。当前 quota 统计 sandbox、checkpoint、retired_snapshot 和 operation reserved_mib，不包括模板保留快照或构建 VM。

独立的绕过路径是：合法计费的 checkpoint 被登记为同租户模板，`sandboxd/checkpoints.go:41` 因模板引用保留物理快照，`:71` 删除 checkpoint 记录并解除对应计费。可反复积累未计费数据，最终仅受 engine 全局资源预算限制，侵占其他租户余量。

**修复方向与验证**：持久化构建资源预留，以 tenant/snapshot 统一计费和去重，在 checkpoint、template、cleanup 之间原子交接，直至物理回收才解除。这里不主张将每台未绑定租户的 warm VM 直接算入 MaxSandboxes；warm pool 的预算和创建权限需要单独明确。风险 MED，工作量 L。

### 9. SDK 排队取消无效

`sandboxd/sdk/client.go:260` 在整个 Call 周期持有 sync.Mutex，context 回调到 `:268` 才注册。一个持久 watch／长 RPC 可令后续短 deadline 调用无限等待锁，超过 context 期限仍不返回。

**修复方向与验证**：保留串行协议，改用可取消准入并在发送前确认 context。两个共享 Connection 的真实 RPC 回归应证明：未发送的排队调用可取消，且不会关闭前一个正在使用的连接。风险 LOW。

### 10. SCHILY 属性名称解析失真

`guest/agent/src/fs.rs:717` 将 SCHILY 与 LIBARCHIVE 前缀合并，`:723` 对两者验证 percent escape，`:735` 对两者解码，仅 value 分支区分格式。SCHILY 名称为原始后缀；当前 engine 的独立 OCI 解析也在 `engine/fsmeta.go:508`、`engine/image_compat.go:115` 保留原始名称。`guest/agent/tests/tree.rs:594` 只覆盖不含百分号的等价名称。

**修复方向与验证**：仅对 LIBARCHIVE 名称 percent decode，再统一验证名称与重复属性。tree_push 行为回归覆盖原始百分号、形似转义序列的名称及两种格式的同属性表示，确认名称和值保持一致。风险 LOW。

### 11. CI 原生镜像测试未成为门禁

`.github/workflows/check.yml:21` 只安装 protoc，`:22` 运行的 make targets 没有开启 SANDBOX_REQUIRE_EROFS_TESTS。`engine/fsmeta_test.go:23`、`engine/image_compat_test.go:22` 在缺少工具／namespace／mapping 时默认 Skip。当前机器上的显式 TestActual 运行再次确认这一机制。Nix flake 提供相关原生工具，但没有被根 CI 使用；Nix devShell 也未包含 make lint 所需 golangci-lint。

**修复方向与验证**：建立固定版本工具及 namespace/mapping 条件的 CI job，强制 SANDBOX_REQUIRE_EROFS_TESTS=1，缺前提须失败。保持特权 KVM 验收与此类原生用户态测试分开；同时统一 lint 工具入口。验证包括故意移除工具的失败门禁，不能以某次手工完整验收代替持续覆盖。风险 LOW；不要求先重建普通测试基线。

### 12. 历史记录反复参与维护扫描

`sandboxd/recovery.go:86` 全量读取 operations；`:185` 遍历全部终态操作并枚举资源分配步骤；`:1064` 每次对每步调用 engine.FindOperation。资源是否仍存在的过滤在查询后进行，因此已经全部回收的历史 claim 也继续产生查询。已完成 resource_recovery 记录同样每轮完整读取。`sandboxd/reconcile.go:50` 的 maintain 串行执行 reconcile → reap → pools，默认 Interval 在 `sandboxd/service.go:57` 为一秒。

确定的复杂度是每轮至少随历史 allocation 步数线性增长的跨 daemon 查询；有待处理回收时，resourceReferences 还会反复扫描和核对历史依赖。本轮未测量实际 p95 或声称已发生生产超时。

**修复方向与验证**：保留完整幂等历史，另建持久化的待核对集合／进度与未完成依赖索引，给每轮维护限定工作预算；不能靠删除历史、缓存未知结果或降低归属核验解决。通过 Service+engine transport 的规模场景检查历史增长后的每轮查询量与 reaper 推进，先建立量化基线。风险 MED；若历史核对是必要恢复证明，先设计证明如何持久转移，不能直接跳过。

### 13. tree push 成功但清理旧树失败

`guest/agent/src/fs.rs:1315` 可复制 workload 可读的旧目录；`:1343` 的 RENAME_EXCHANGE 后 staging 指向旧树。`:1250` 的 cleanup_tree 先 chmod；`:1363` 忽略其错误，随后 TempDir Drop 再尝试删除并忽略错误，仍返回原成功结果。

当目标父目录允许替换，而旧树含其他 UID 所有、可读但不可写的非空目录时，复制和交换可成功，旧树 chmod／删除却失败。该控制流不依赖并发写或进程崩溃；旧完整数据可能重复积累。本轮未执行双 UID 行为验证，影响规模未经测量。

**修复方向与验证**：明确提交前的可清理性条件与提交后的清理责任及响应语义，以 workload 权限完成，不引入任意 root 删除。集成场景同时验证目标树结果、commit 状态与临时目录；仅返回普通失败也不足以表达已提交事实。风险 MED，建议在前述安全和一致性事项之后处理。

## 单列调查项，不作为已证实漏洞

**tree_pull 具名归档路径重开，MED confidence / M / LOW fix risk**：`guest/agent/src/fs.rs:249` 以 workload filesystem identity 创建临时归档，`:258` 再以路径写入，`:262` 在普通异步上下文以 daemon 身份重新按路径读取。`guest/agent/tests/tree.rs:26` 的 fixture 将 daemon 与 workload 设为同一 UID，不能证明生产双 UID 边界。需要独立双 UID 的无敏感哨兵场景核验路径替换；可考虑始终复用创建时授权的 FD。未完成该验证前不宣称 guest root 文件泄露，也不与已确认安全项合并排名。

## 方向选项

1. **供应路径与维护延迟的测量基线（粗估 M）**。`sandboxd/lifecycle.go:136`、`:162` 和 `sandboxd/types.go:59` 已区分 warm/golden/cold，`sandboxd/http.go:172` 的 metrics 目前仅按状态计数。先测 claim 各阶段、池可用／待补数及维护延迟，能把设计延迟目标变成可验证的产品决策；代价是新增有界指标与基准场景，标签不能使用 sandbox ID 或 tenant 明细形成无界基数。
2. **双 daemon 的只读恢复诊断（粗估 M）**。engine 已有 `engine/admin.go:111` 的 Doctor 和 VM.BlockedReason，sandboxd 的 LastError、resource_recovery、engine receipt 则分散在不同表与对象。一个按操作／资源组织的只读诊断可以降低未知结果和配额残留的处理成本；需明确事实、未确认项和人工建议，避免将诊断入口变成自动猜测归属或自动删除工具。

operator 目前主要是 scaffold，未发现足以优先于核心正确性与运行证据的扩展依据，本轮不建议先建设集群编排或 gossip。

## 依赖与计划取舍

- 第一批建议将 **1–5** 转成独立实施计划；后续编号从 **006** 开始。本文发现编号不是实施计划编号。
- **6** 可与 **5** 顺序落在同一 engine 工作流：先建立可复用的候选复制／崩溃行为回归，再处理实例接续；两者没有整份计划的硬依赖，但共享 operations.go，应串行执行以减少冲突。
- **7、8** 应先统一资源计费责任的模型，再拆分执行。推荐先修 release／cleanup 交接，再把 build/template 纳入该模型，避免各自发明一套计费分支；保留 003 的去重与物理回收边界。
- **11** 是后续 OCI/EROFS 更改的前置门禁，不阻塞不涉及镜像格式的 1–4 等修复。新权限边界仍需各自的真实 UID／连接场景，普通测试绿色不能替代。
- **12** 应在生命周期正确性回归稳定后开展；先测量再改持久恢复队列。保留历史是既定契约，优化对象是重复全量工作。
- **9、10** 独立且较小，可单独选择；**13** 需要先明确已提交但 cleanup 失败的结果语义，不能仅补一条错误返回。
- 调查项若被选择，应生成验证／设计计划，先证明权限边界，不按已确认漏洞直接实施。

## 已考虑并排除／降级

- 001–005 的原始问题不重复立项：operation 结果权限、fork 资源级补偿键、resume 旧快照清理计费、snapshot_owners 来源及捕获独立 inode 边界已有当前实现和回归。此处不等于重新完成 005 的特权验收。
- 模板目录竞争不能推导跨租户读取快照，来源授权是独立边界。
- DNS 重绑定：实际 dial 再次校验所有地址并使用数值 IP；CONNECT、SNI、内层 authority 一致性及 placeholder 逐请求检查未发现可确认绕过。
- direct cleanup 表面上先撤策略，但 InstallDirect 的 cleanup 内部先关网络，不作为缺陷。
- 普通文件 RPC 在 workload 身份下获得 FD，再在异步上下文读该 FD，不等于 root 按路径重新授权。cwd 降权先后本身不足以证明权限绕过，保留调查而不报已确认漏洞。
- 进程退出的正常清理路径在 wait/reap leader 之前标记退出并处理进程组，未确认 PID 复用问题。
- metadata-only backup、keyed-exec ledger 随整机快照回滚、Preview 共享 origin／不重写绝对资源 URL、非递归 watch、managed-only attach、逃逸进程组需 VM shutdown、tree 所有权归一化、单宿主机范围均为已记录边界。
- 不确定实例／人工恢复证据使 GC 保守保留，是既定行为；严格 stat/maps COW 身份核验不因文件系统兼容性被放宽。
- 本轮未以常规 AGENTS／README 开发说明作为可执行指令；没有发现需要独立报告的恶意 prompt-injection 内容。
