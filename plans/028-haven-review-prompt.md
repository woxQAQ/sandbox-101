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

## 三、收拢错误模型（19）

从 `haven/internal/errs/errors.go`、`types/errors.go` 和 `haven/httpwire/http.go` 顺着调用方检查。内部 Go 错误继续用 sentinel 和 `errors.Is`；wire code 用明确的 `CodeNotFound` 等命名，JSON 保持现有 `code/message` 与稳定字符串。将每种公开错误的 code、HTTP 状态和公开消息规则集中在 HTTP 边界，两种 listener 共用。按上一节相同的发布依据处理 `types` 中已发布的 Go 标识符。公开消息使用已核准的文本或显式安全的字段；宿主错误可能携带私有路径、诊断或 secret。

验收时区分接纳失败、取消等待、已接纳且待恢复、已完成但失败四种情况。最后一种在同步调用返回 HTTP 错误，在 operation GET 返回带固定 `internal` 失败信息的 operation；异步应答继续遵循 202 契约。检查数据库的冻结结果和重启查询，确保响应格式化只发生在交付时。参考 `haven/integration/operation_publication_test.go` 和 `haven/integration/workflows/plain_error_publication_test.go`。

## 四、显式完成 operation 并冻结历史结果（22）

1. 从 `haven/operation/README.md`、`committer.go`、`queries.sql` 和 `haven/internal/publication/projector.go` 核对接纳、完成、交付三个时点。盘点所有写 `succeeded` 或 `failed` 的入口，包括接纳即终态、恢复重试及一个事务完成多个 operation 的情况。按 `ListUnpublishedTerminals` 当前条件标出哪些 operation 需要冻结业务视图，包含供父 fork 读取的子操作。
2. 工作流核验业务结果后，在自己已有的 SQLite 事务中显式指定本次完成的 operation ID 和结果。由 `operation` 统一写终态、完成时间，并为符合条件的 ID 冻结 publication。`publication.Project` 负责在同一事务快照内组装业务视图。一个事务可以完成多个 ID；若父结果依赖子 publication，先冻结子结果，再构建父结果。所有步骤成功后由事务所有者一次提交，任何一步失败均回滚本事务。以本次明确完成的 ID 取代正常提交时扫描全库寻找待发布终态的做法。保留启动时对历史缺失或损坏的核验与拒绝语义。
3. 保持接纳时冻结的请求、幂等身份及已提交终态。查询时由 delivery 校验权限、读取原 publication 并按授权派生 token，HTTP 错误格式沿用第 19 条。更新 `haven/operation/README.md` 的提交说明；涉及 sqlc 查询时用 `just generate-sql` 更新绑定并运行 `just check-sql`。

验收使用真实 Haven/SQLite 行为：成功 claim 后 release 并重启，查询和同键重试仍交付原 claim 结果；fork 父结果使用各子操作冻结的结果；注入投影或 publication 写入失败后，从独立读事务观察不到本次部分提交，重试也不会覆盖已有终态。优先扩展 `haven/integration/workflows/publication_test.go` 和 `haven/sandbox/fork_workflow_test.go` 的现有场景。

## 五、明确维护职责与测试支持的依赖方向（20、21、23）

- **20、21**　`internal/maintenance` 改为 `internal/reconcile`，使用 `reconcile.Coordinator`、`reconcile.Dependencies`、`reconcile.New`。`ResumePending` 恢复未完成工作并核对数据面绑定；`RunOnce(ctx)` 执行一轮恢复、租约处理以及允许接纳时的 warm refill。注释写明各方法的适用时机。启动继续在对外服务前进行恢复，保留现有 worker 去重、reviewed/draining 限制与关闭顺序。沿用当前 GC 默认值与关闭配置。
- **23**　梳理 `haven/internal/testfixture`、`haven/internal/testapp`、`haven/tests/fixtures` 的 import 和真实调用方。以 `internal/testapp` 管理私有 App 的启动、恢复和关闭，以 `internal/testfixture` 承载跨包共享的测试数据和 runtime 替身。只在多个测试包确有需要时，把通用断言、等待或并发辅助归 `internal/testutil`；单包 helper 放回对应 `_test.go`。先消除仅转调另一包的入口，再搬调用方并移除腾空的旧包。检查 import cycle 和所有 build tag 下的引用；保留私有状态、并发上限与 fixture 清理语义。

## 验证与交付

按根 `justfile` 先跑覆盖本批变更的 Go 包测试，再用 `just test` 检查跨包重命名及 fixture 迁移，`just integration` 检查真实 Rust fixture 的 HTTP/guest 契约，`just smoke` 检查 daemon、SDK/CLI 入口和 UDS。优先扩展现有服务/HTTP 测试来覆盖变动路由、权限和错误映射。最后用 `rg` 在当前源码、构建入口与当前文档核查旧包和旧路由的残留，并解释需要保留的引用。报告每条命令的实际 PASS、FAIL、SKIP 或 BLOCKED、缺少的具体输入，以及 16 条 review 的改动落点。运行只使用本次测试私有状态。
