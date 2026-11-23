# Plan 001：按调用方权限读取 operation 并返回沙箱令牌

> 执行者须完整阅读本文，逐步执行验证；发生 STOP 条件时报告具体证据。只修改 Scope 内的文件。完成后更新 `plans/README.md` 中 001 的状态；若由 reviewer 维护索引，则只交付结果。
>
> 先运行 `git diff --stat da513a6..HEAD -- sandboxd/service.go sandboxd/http.go sandboxd/service_test.go sandboxd/http_test.go sandboxd/README.md sandbox-architecture.md`，再检查这些路径的未提交差异。其他计划造成的已知变化需要重新核对本文摘录，不可覆盖。本文基于 commit `da513a6` 的代码。

## 状态

- Priority：P1
- Effort：S，含服务及 HTTP 行为测试
- Risk：LOW；重点保留 sandbox 自身 renew/release 的异步查询
- Depends on：无
- Category：security
- Planned at：`da513a6`，2026-11-06
- Status：DONE；实现已整合，独立复核和最终组合验收通过，见 [实施与验收记录](IMPLEMENTATION.md)。

## 问题与目标

`sandboxd` 有 root、tenant、sandbox 三种权限。Sandbox token 只允许自己的数据面、续租和释放。当前 operation 查询复用源沙箱的可见性检查；tenant 从某个 sandbox fork 后，持源 sandbox token 且已知该 operation ID 的调用方可以读取结果，并获取全部分支 token。这里不假定 operation ID 可枚举，也不声称调用方因此获得 tenant 权限。

目标是让操作读取权限和结果凭据权限分别成立：源资源可见，不能自动授权读取 tenant 才能执行的操作，更不能授权获取结果中的其他沙箱令牌。

## 当前实现与约束

- `sandboxd/service.go`：`Submit`、`Operation`、`Wait`、`resultTokens`。
- `sandboxd/http.go`：`GET /v1/operations/:id` 调用 service 后生成 token。
- `sandboxd/fork.go`：成功 fork 将所有分支放进 `Result.Sandboxes`，本计划不修改 fork 行为。
- `sandboxd/http_test.go` 的 `TestHTTPClaimIdempotencyAndOperationRoute` 是 HTTP 测试结构范例；`sandboxd/service_test.go` 的 `fixture`、`claimTest` 提供真实 Service + SQLite 与显式后端替身。

`sandboxd/service.go:840` 的当前授权：

```go
if !visible(p, o.TenantID, o.Command.SandboxID) {
    return Operation{}, api.Fail(api.NotFound, "not found")
}
return o, nil
```

`sandboxd/service.go:846` 当前对结果不做调用方检查：

```go
if len(r.Sandboxes) > 0 {
    r.Tokens = map[string]string{}
    for _, b := range r.Sandboxes {
        r.Tokens[b.ID] = s.issuedToken(b)
    }
}
```

`sandboxd/http.go:110`：

```go
if err == nil {
    v.Result = s.resultTokens(v.Result)
}
respond(c.Writer, v, err)
```

设计契约来自 `sandbox-architecture.md` 4.1：“Sandbox Token……仅授权操作该 sandbox_id 的数据面与自身续租/释放”。`sandboxd/README.md` 要求异步结果以相同权限轮询。外国资源和不存在资源都使用 `api.NotFound`，HTTP 错误由 `writeError` 规范化为相同 404 body。

匹配仓库惯例：使用 `api.Fail` 和已有 `Principal`，不新增平行权限框架；沿用 Gin 路由、SQLite 和长度前缀 protobuf。测试覆盖服务/HTTP 行为，不增加独立权限函数的镜像式单测。

## 目标权限表

| 调用方 | 可读取 operation | 可取得结果 token |
|---|---|---|
| Root | 全部 | 已获准读取结果中的沙箱 |
| Tenant | 本 tenant operation | 本 tenant 的结果沙箱 |
| Sandbox | 同 tenant、同 sandbox 的 `renew` / `release` operation | 仅自身；terminated 结果不发 token |

额外规则：

1. Sandbox principal 不能读取 claim、fork、checkpoint、configure、template 或其他 tenant operation，即使其 `Command.SandboxID` 等于当前 sandbox。
2. 不返回“部分授权”的 operation body；结果包含越权对象时整个读取失败，不能只删 token 后仍泄漏对象数据。
3. Token 生成前清除或重新构建 `Result.Tokens`，不能信任已有 map。逐个验证 `Sandbox` / `Sandboxes` 的租户和 ID，验证失败返回 `api.NotFound`。
4. 保持现有幂等、取消等待和终态结果契约；不改变 token 推导算法、epoch 或签名格式。

## Scope

允许修改：

- `sandboxd/service.go`
- `sandboxd/http.go`
- `sandboxd/service_test.go`
- `sandboxd/http_test.go`
- `sandboxd/README.md`
- `sandbox-architecture.md`（仅 operation 授权文字）
- `plans/README.md`（仅状态）

禁止修改 engine、guest、egress、fork 供应逻辑、Preview、token 算法、HTTP 响应字段、依赖及其他计划。无需兼容未发布的内部 helper 签名；可调整 `resultTokens` 的签名并一次更新所有调用点。

## 执行与验证

以下命令使用 Nushell；从仓库根目录执行。不要安装依赖或访问 `.local-run/`。

### 1. 建立行为回归

在已有测试文件中加入 `TestSandboxTokenCannotReadForkOperation`：tenant 创建源 sandbox 和两个 fork 分支；取得源 principal 后，通过 service 和 HTTP 查询已知 fork operation ID，均应得到 NotFound，响应不能含分支信息或令牌。测试直接使用返回的 operation ID，不加入枚举或攻击演示。

同时添加 `TestSandboxTokenCanPollOwnLeaseOperations`：源 token 可查询自己提交的 renew/release，release 结果不带有效 token；其他 sandbox 和其他 tenant 均返回相同 404。使用实际 Service 状态流转，不直接构造一个恒定返回值的权限 mock。

**Verify**：

```nu
with-env { GOWORK: "off" } { go -C sandboxd test . -run 'TestSandboxToken(CannotReadForkOperation|CanPollOwnLeaseOperations)' -count=1 -timeout=30s }
```

修复前 fork 授权断言必须失败；若未失败，先定位差异，不删除断言迎合实现。

### 2. 收紧读取与结果生成

在 `Service.Operation` 执行上述权限表。让生成 token 的路径接收 `Principal`，在生成前验证所有结果目标；`Wait` 和 HTTP operation GET 都必须使用该路径，不能留一个不接收 principal 的旁路。校验整个结果后再返回，避免部分写入 token map。

增加 `TestOperationResultTokensRespectPrincipal`，通过已有服务/HTTP fixture 验证 tenant 正常读取 fork 结果、root 正常读取、sandbox 越权失败；同时确保响应前已有 `Tokens` 不会被直接透传。后一个场景可使用测试持久化夹具，但须通过实际 HTTP/Service 入口验证，不直接测试 helper。

**Verify**：

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race . -run 'Test(SandboxToken|OperationResultTokens|HTTPClaim)' -count=1 -timeout=60s }
rg -n 'resultTokens' sandboxd
```

预期测试全通过；所有 token 结果生成调用都显式携带调用方，`rg` 结果人工逐项核对并在交付中列出。

### 3. 记录权限契约并跑回归

在 README 说明 sandbox 只能轮询自身 renew/release；随机 operation ID 不构成权限凭据。文档只描述最终行为，不记录聊天历史。

```nu
with-env { GOWORK: "off" } { go -C sandboxd test -race ./... -count=1 -timeout=180s }
with-env { GOWORK: "off" } { go -C sandboxd vet ./... }
make integration
git diff --check
```

预期均 exit 0。`make integration` 必须实际启动 Rust fixture，不能用普通 integration 包的跳过结果替代。

## 完成标准

- [x] 上述三个新增行为测试存在且通过。
- [x] 源 sandbox token 查询 fork operation 返回与不存在 operation 相同的 HTTP 404 body。
- [x] Tenant/root 正常取得授权分支 token，源 sandbox 不能取得分支 token。
- [x] Sandbox 自身 renew/release 异步轮询仍正常，release 不发 terminated token。
- [x] 所有结果 token 生成路径携带 Principal 并逐目标验证。
- [x] 全包 race、vet、实际 Rust integration、diff check 通过。
- [x] diff 位于获授权计划的 Scope 并集内；索引状态及验证结果已更新。

## STOP 条件

- 摘录对应的权限模型已改变，或发现已发布客户端明确依赖读取 tenant operation 的 sandbox 权限。
- 为达到目标需要扩大 sandbox token 权限、修改签名或 HTTP schema。
- 两次合理修正后验证仍失败，或需要修改 Scope 之外的实现。

## Git 与维护

若执行者由 orchestration 派发，使用其预先绑定的 checkout，不自行创建或切换 worktree。否则可使用 `advisor/001-operation-authorization` 分支。近期提交以简短描述为主，历史也有 scoped 提交；若被要求提交，使用 `sandboxd: restrict operation result authorization`。未获指令不提交、推送或开 PR。

后续新增 operation 动作时必须明确三种 principal 的读取权限；结果中增加资源集合时必须同时扩展逐目标授权。operation 状态、资源可见性和凭据签发是三个不同检查，不要重新合并为源 sandbox 的一次 `visible` 判断。
