# Haven review 实施提示词

你在 `woxQAQ/sandbox-101` 实施基于 `c658c77` 的 review 意见。目标是让读代码的人能从路由看出业务能力，从包和方法名看出依赖与生命周期。交付代码、同步后的当前契约、行为测试结果，以及下面 16 个编号的完成情况。编号对应原 review，本文件包含执行所需的结论。

先读根 `AGENTS.md`，核对当前 HEAD 和已有工作区改动。用 `rg` 找定义、调用者和测试；基准提交的路径是定位线索。HTTP 依据 `docs/haven-control.md` 和 `haven/sandbox/api.md`，guest 依据 `guest/README.md`、`protocol/README.md`，出网语义依据 `haven/egress/README.md`。路由改动开始前列出旧 method/path、使用方、目标 method/path、对应业务命令，盘点 SDK、CLI、脚本、测试和当前文档。然后按以下顺序实施。

## 一、澄清身份、后端和 guest 接口（2、4、9、10、11）

- **2**　`--root-token-file` 是业务 HTTP 管理员 bearer 的启动输入。内部表达该凭据的配置和构造参数用 `AdminTokenFile`、`AdminToken`；在 flag help 与启动文档说明用途、首个租户的创建方式，以及本机管理 socket 使用 peer UID 鉴权。沿用现有 flag、配置 key、环境变量、token 校验和身份行为。
- **4**　`host.CloudOptions` 改为 `host.CloudHypervisorOptions`，更新生产、测试及当前文档中的构造示例。
- **9、10**　`guest.Port` 改为 `guest.Agent`，通过 vsock 的生产实现仍叫 `guest.Client`。给接口六个方法写参数名和方法注释。逐一写明 `Info` 核验哪个协议，`Activate` 可以传哪些配置，`PrepareTemplate` 需要什么确认，`Exec(ctx, resource, execKey, command)` 的键、退出状态与结果未知语义，以及 `Connect` 和 `Forward` 成功时分别交付什么连接、由谁关闭。接口级注释说明 guest 答复能证明的范围。本次抽象重命名不涉及 guest wire schema。
- **11**　把 `/connect` 的 handler `relay` 改名 `connectSandbox`。它处理客户端到 guest Workload 的 HTTP Upgrade。双向字节复制函数 `relayBytes` 和 guest 对外出网的策略值 `proxy` 各自保留。验证 Upgrade 前已读到的字节、half-close 及撤权后的连接关闭行为。

## 二、显式装配业务 API 与路由（12、13、14、15、16、17）

- **13**　在 `haven/cmd/haven/main.go` 用普通 Go 构造调用组装 `api.Services`，由 `api.New(services)` 创建私有 `handler` 并注册路由。`handler` 直接持有这一份 `Services`，消除当前 `Adapter` 逐字段复制九项依赖的代码。确有调用方只需要某个服务的部分能力时，在消费方声明窄接口。沿用手工注入，不引入 DI 框架。验收时从生产入口和测试入口都能沿构造参数追到实际依赖，handler 只获得业务能力，writer lock、egress 回调绑定、启动恢复和最终关闭仍由 `application` 显式管理。

- **12、14**　把 `haven/httpapi` 改为 `haven/api`。`main` 显式提供服务，router 注册路由与中间件，handler 负责 HTTP 解码、调用业务服务、写响应。每个受支持的动作在 Gin 中显式注册，包括 sandbox 和 checkpoint 的两种 fork；preview 单独注册。消除 `/:action` 加 `mutate` 二次分派。共享代码只提取真正相同的提交、等待和响应行为。`SandboxRequests` 可继续把已确定的业务命令交给对应服务。将 sandbox capability 的动作范围和 root fork 的 owner 校验落实在业务接纳边界，覆盖直接服务调用。
- **15**　资源集合用名词，已有对象的生命周期命令用显式动作。目标路由包括 `POST /v1/sandboxes` 领取、`DELETE /v1/sandboxes/:id` 释放、`POST /v1/sandboxes/:id/pause` 等；节点命令使用 `POST /v1/node/drain`。模板登记走 `POST /v1/templates`，构建走 `POST /v1/templates/:id/build`。让 URL 决定命令与目标，校验路径和请求体中重复出现的 ID 或动作是否一致。逐项核对步骤开头的清单，尤其是 renew、configure、rollback、fork、checkpoint、preview 和 SDK 的 `Change(..., release)`。对 refill 核对公开契约和真实调用方，避免把通用分派碰巧可达的内部命令暴露为新能力。按项目发布边界迁移：分支内协议直接替换；对有发布证据的旧路径保留该版本契约并测试，记录依据。同步 SDK、CLI、示例、smoke 和当前契约，历史归档仍描述原版本。
- **14、15 的验收**　保留 `X-Tenant-ID`、foreign 与 missing 的 404、退休 capability 仅可读取原有授权历史、幂等键与已提交请求身份、同步 Result、异步 202 与 `Location`。旧 operation 的请求内容、对外 action 拼写和冻结结果可继续读取。路径和 HTTP method 的调整不得改变同一有效业务请求的幂等身份。使用公开 DTO 的严格解码，继续拒绝未知字段、多个 JSON 值及内部专用字段。
- **16、17**　删除仅转调 `httpwire` 的五个 wrapper，让业务和本机管理 handler 直接使用共享 HTTP 编解码。合并只有 `operationResponse` 的文件，把响应格式化放到 operation 响应的调用处附近。保留一处集中处理已接纳 operation 终态失败的逻辑，供同步、异步和 GET 使用。

## 三、重做错误到 HTTP 响应的转换（19）

在复查基准 `91fffa9` 上，`587b73f` 仅更名协议常量。旧 `PublicError` 裁剪 `err.Error()`，旧 `Failure` 从 code 反查 HTTP 状态。以下验收针对这两条路径。

1. 在 HTTP 边界定义一次失败的完整表示，包含响应状态和可公开的 `types.ApiError`。业务错误通过 `errors.Is` 选中一个完整响应，writer 直接使用其中的状态与 payload。删除按 DTO 的 `Code` 反推状态的 switch。HTTP 自身的失败也使用同一写出路径：例如普通参数错误和 Upgrade 缺头都使用 wire code `invalid`，前者返回 400，后者返回 426。保留 405 与 Preview 502 的独立语义。业务包继续使用标准 Go error，SDK 继续读取现有 `code/message`。
2. 将内部诊断和公开消息分开构造。接纳错误只从固定公开消息或产生错误时显式选定的安全字段取得响应文本。梳理依赖现有 message 的 SDK、CLI、契约与用例，保留有意公开的输入纠错信息。移除从包装错误 `Error()` 截取、拼接或直接透传文本的路径；内部 cause 继续支持 `errors.Is/As`。对 `sql.ErrNoRows` 逐个核对边界：只有清楚在查询哪个公开对象的业务入口才能判定对象缺失并返回 404；缺失内部记录按对应业务语义处理。
3. 迁移业务 HTTP 与本机管理 HTTP 的错误写出处。共用的 404、管理员权限错误和固定 internal 失败各由一处定义。handler 可以选择与路由有关的具体失败。同步操作终态失败和 operation GET 使用同一份固定 internal payload，后者的外层响应仍为 200；异步接纳保留 202。持久化结果继续排除 HTTP DTO 和私有诊断。

**验收证据**

- 先增加一条在 `91fffa9` 会失败的 HTTP 行为回归。通过请求触发接纳阶段的错误，让测试依赖返回一个真正包装已识别 sentinel 的错误，并带上私有测试标记。验证约定的 HTTP status、wire code 和完整公开 message，响应中不能出现标记。现有 `TestHavenHTTPFormatsFrozenOperationFailure` 覆盖的是已接纳操作的终态失败，保留它以验证另一个时点。
- 通过业务和本机管理 handler 分别请求相同类别的失败，检查状态及 JSON。覆盖相同 wire code 对应不同 HTTP 状态的场景，并验证取消等待、已接纳且待恢复的公开语义。复用现有 HTTP/SQLite 测试入口，报告新增用例在旧实现和新实现上的实际结果。
- 交付时给出一条真实调用链，从错误产生处追到公开响应，指出安全消息的来源，并列出删除的旧分类、反查和透传路径。同步更新 `docs/haven-control.md` 的错误契约。

## 第 19 条实施记录

- `POST /v1/templates` 经 `sandbox.SandboxRequests.Submit` 进入 `supply.AdmitTemplate`。例如管理员漏填模板 owner 时，校验处用 `errs.InvalidInput` 选择固定提示，`httpwire.classify` 按 `errors.Is` 确认 invalid、按 `errors.As` 取出该提示，`Failure.Write` 写入 HTTP 400 和公开 JSON。HTTP writer 直接使用该次分类的状态，包装诊断仅留在 Go 错误链。
- 在 `91fffa9` 运行 `go -C haven test ./api -run TestTemplateAdmissionFailures -count=1`，quota 用例返回了私有测试标记，内部缺行用例返回了 404。补做后两项分别返回固定的 429/resource_exhausted 与 500/internal。最终六个子用例均通过，覆盖显式公开提示、普通 invalid、等待取消及已接纳待恢复。
- `just integration` 验证 400、405、426 使用相同 wire code，缺失 checkpoint 返回 404，以及已接纳失败的同步响应和 GET 视图。`just smoke` 在同一真实 daemon 上验证经过 bearer 和 peer UID 鉴权的两个入口均拒绝多余 JSON，返回相同的状态、code 和消息。
- 普通套件曾因 shell 缺 `mkfs.erofs` 跳过 35 条测试事件。在默认 dev shell（erofs-utils 1.9.4、umoci 0.6.0）中以映射宿主用户的私有 UID namespace 和独立 tmpfs 作为 `TMPDIR`，运行 `GOWORK=off SANDBOX_REQUIRE_EROFS_TESTS=1 go -C haven test -json -race -p=1 -count=1 -timeout=300s -run '^(TestActual|TestHavenActual|TestGCActual)' ./image ./gc`。按包名和完整测试名与原 SKIP 清单核对，35/35 通过；该次定向复跑共 75 条 pass，0 fail、0 skip。

## 四、显式完成 operation 并冻结历史结果（22）

1. 从 `haven/operation/README.md`、`committer.go`、`queries.sql` 和 `haven/internal/publication/projector.go` 核对接纳、完成、交付三个时点。盘点所有写 `succeeded` 或 `failed` 的入口，包括接纳即终态、恢复重试及一个事务完成多个 operation 的情况。按 `ListUnpublishedTerminals` 当前条件标出哪些 operation 需要冻结业务视图，包含供父 fork 读取的子操作。
2. 工作流核验业务结果后，在自己已有的 SQLite 事务中显式指定本次完成的 operation ID 和结果。由 `operation` 统一写终态、完成时间，并为符合条件的 ID 冻结 publication。`publication.Project` 负责在同一事务快照内组装业务视图。一个事务可以完成多个 ID；若父结果依赖子 publication，先冻结子结果，再构建父结果。所有步骤成功后由事务所有者一次提交，任何一步失败均回滚本事务。以本次明确完成的 ID 取代正常提交时扫描全库寻找待发布终态的做法。保留启动时对历史缺失或损坏的核验与拒绝语义。
3. 保持接纳时冻结的请求、幂等身份及已提交终态。查询时由 delivery 校验权限、读取原 publication 并按授权派生 token，HTTP 错误格式沿用第 19 条。更新 `haven/operation/README.md` 的提交说明；涉及 sqlc 查询时用 `just generate-sql` 更新绑定并运行 `just check-sql`。

验收使用真实 Haven/SQLite 行为：成功 claim 后 release 并重启，查询和同键重试仍交付原 claim 结果；fork 父结果使用各子操作冻结的结果；注入投影或 publication 写入失败后，从独立读事务观察不到本次部分提交，重试也不会覆盖已有终态。优先扩展 `haven/integration/workflows/publication_test.go` 和 `haven/sandbox/fork_workflow_test.go` 的现有场景。

## 五、明确维护职责与测试支持的依赖方向（20、21、23）

- **20、21**　`internal/maintenance` 改为 `internal/reconcile`，使用 `reconcile.Coordinator`、`reconcile.Dependencies`、`reconcile.New`。`ResumePending` 恢复未完成工作并核对数据面绑定；`RunOnce(ctx)` 执行一轮恢复、租约处理以及允许接纳时的 warm refill。注释写明各方法的适用时机。启动继续在对外服务前进行恢复，保留现有 worker 去重、reviewed/draining 限制与关闭顺序。沿用当前 GC 默认值与关闭配置。
- **23**　梳理 `haven/internal/testfixture`、`haven/internal/testapp`、`haven/tests/fixtures` 的 import 和真实调用方。以 `internal/testapp` 管理私有 App 的启动、恢复和关闭，以 `internal/testfixture` 承载跨包共享的测试数据和 runtime 替身。只在多个测试包确有需要时，把通用断言、等待或并发辅助归 `internal/testutil`；单包 helper 放回对应 `_test.go`。先消除仅转调另一包的入口，再搬调用方并移除腾空的旧包。检查 import cycle 和所有 build tag 下的引用；保留私有状态、并发上限与 fixture 清理语义。

## 后续复查修复记录

- **14、15**　条件 release 通过 SDK、DELETE 路由和业务接纳保留 `lease_deadline`。HTTP 回归覆盖续租后拒绝旧 deadline、空 body 释放、同键重放与异参冲突。checkpoint fork 由 `Client.ForkCheckpoint` 和 `havenctl fork-checkpoint ID` 发往 checkpoint 路由。fork 接纳按所选租户查询两种来源，再读取状态和恢复证明。
- **22**　SQLite 回归在 renew 事务更新 settings 后移除该行，验证投影失败会回滚租约，同键重试可以完整交付。启动时拒绝已冻结但缺少 sandbox 的成功 claim。模板构建与 refill 的临时资源 ID 仅用于工作流追踪。
- **2、10、23**　认证服务使用 `adminToken`；guest keyed exec 的重试说明与实际 conflict 和持久退出状态一致。原生镜像测试的 App 在 `testapp` 创建和关闭，OCI 测试数据由 `testfixture` 构造。测试直接使用现有 `operation.Encode`、`Digest`、`NewID`。
- **验证**　默认 Nix dev shell 中，`just integration`、`just smoke`、`just check` 及上述工作流与启动核验的定向 `-race` 通过。普通 UID 的原生镜像用例遇到 ownership 和 `security.capability` 限制；在映射 UID 的私有 namespace 和 tmpfs 中设置 `SANDBOX_REQUIRE_EROFS_TESTS=1` 后，`just test` 通过，image/GC 原生子集 `-race` 得到 75 条用例通过事件、0 fail/skip。`integration`、`privileged`、`guest_image` 标签编译通过，真实 VM 验收另行跟踪。

## 验证与交付

按根 `justfile` 先跑覆盖本批变更的 Go 包测试，再用 `just test` 检查跨包重命名及 fixture 迁移，`just integration` 检查真实 Rust fixture 的 HTTP/guest 契约，`just smoke` 检查 daemon、SDK/CLI 入口和 UDS。优先扩展现有服务/HTTP 测试来覆盖变动路由、权限和错误映射。最后用 `rg` 在当前源码、构建入口与当前文档核查旧包和旧路由的残留，并解释需要保留的引用。报告每条命令的实际 PASS、FAIL、SKIP 或 BLOCKED、缺少的具体输入，以及 16 条 review 的改动落点。运行只使用本次测试私有状态。
