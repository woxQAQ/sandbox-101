# Plan 014：让 SDK 排队中的 Call 响应 context 取消

> 逐步执行门禁；完成更新索引 014，reviewer 维护时交付结果。Drift check：`git diff --stat b93fc24..HEAD -- sandboxd/sdk/client.go sandboxd/sdk/client_test.go sandboxd/README.md`，再检查未提交差异；零测试匹配不是通过。

## 状态

- Priority：P2；Effort：S；Risk：LOW；Depends on：无。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支复核通过，未合入用户分支）。
- 审计映射：发现 9。

## 问题与目标

同一个 Connection 的 Call 持 mutex 直到 RPC 终结。排在长执行／watch 后面的短 deadline 调用在获得锁前不会注册取消回调，可能无限等待。目标保持每连接串行协议，但排队者及时取消、零发送，且不关闭前一个正在执行的 RPC。

## 当前状态与惯例

`sandboxd/sdk/client.go:260`：

```go
c.mu.Lock()
defer c.mu.Unlock()
c.next++
id := fmt.Sprint(c.next)
// 构造 request 后才执行：
stop := context.AfterFunc(ctx, func() { _ = c.Close() })
defer stop()
```

Connection 嵌入 net.Conn，另有 next 计数。Send/Receive 文档要求调用方独占连接，不能在本计划中混合实现 multiplexing。`sandboxd/agent/client_test.go` 展示 net.Pipe 上的真实 protobuf 帧读写；`sandboxd/integration/transport_test.go` 的 TestRustAgentThroughSandboxRelay 证明端到端 Call/Exec 行为。

错误继续使用 ctx.Err/原 RPCError，不吞掉取消，不新增协议字段或第二个 RPC ID 体系。

## Scope

允许：`sandboxd/sdk/client.go`，新增 `sandboxd/sdk/client_test.go`，`sandboxd/README.md` 和索引状态。

禁止：guest、framing、HTTP Upgrade、自动重试命令、改变 Call 串行契约、取消别人的执行。允许零值内部准入结构懒初始化，不能要求调用方额外初始化 Connection。

## 命令

根目录 Nushell，现有命令：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test ./sdk ./agent -count=1 -timeout=45s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make integration
```

审计时 sdk 无测试，所以下列名称为新增；先用 `go -C sandboxd test ./sdk -list '^TestConnection'` 确认。

## 步骤

### 1. 添加排队取消的连接级回归

新增 `TestConnectionWaitingCallHonorsContext`：net.Pipe 对端接收第一个真实请求后 gate 阻塞响应；第二个已取消 context 必须在第一个仍挂起时返回 context.Canceled，且对端没有收到第二帧，连接未关闭。再用短 deadline 覆盖 DeadlineExceeded。最后放行第一 RPC，验证其成功，再发一个正常 RPC 验证连接可复用。用通道同步，不用 sleep 猜锁已取得，cleanup 关闭 pipe 并解 gate。

**Verify**：focused `go -C sandboxd test ./sdk -run '^TestConnectionWaitingCallHonorsContext$' -count=1 -timeout=20s`（GOWORK=off），原实现应在有界等待断言 RED，不能挂死整个测试。

### 2. 用可取消准入保护整个 Call

将内部 sync.Mutex 替换为容量 1 的通道准入；用 sync.Once 做每个 Connection 的懒初始化，支持现有 `&Connection{Conn: ...}` 构造。select context.Done 与准入成功，取得后再次检查 ctx.Err，只有确认可发送才分配 next 并注册关闭回调。不要为已取消的排队者关闭共享连接，也不要让未获准请求消耗 ID。

准入释放必须排在取消回调收尾之后：AfterFunc 回调在关闭 transport 后 close 一个完成 channel；Call 退出时先调用 stop，若 stop=false 表示回调已开始/被安排，则等待完成 channel，再把连接标记为不可复用、最后归还 token。不能仅 `defer stop()` 后就归还 token，因为 stop 不等待已启动回调。下个获准 Call 若 transport 已被取消关闭，应在写帧前返回关闭错误。正常完成且成功停止回调时可继续复用。保留 next/关闭状态均由同一准入保护，不引入数据竞争。

**Verify**：步骤 1 GREEN，新增 `TestConnectionCanceledBeforeSend` 覆盖预先取消但 token 可用的竞争；`TestConnectionActiveCallCancellationClosesTransport` 保留已发送请求取消会中断其 transport 的现有语义。

### 3. 覆盖并发和真实 transport

新增 `TestConnectionConcurrentCallsStaySerialized`，有限数量请求并发，验证不同 ID 且无交错响应；不模拟 helper返回值。再新增 `TestConnectionCancellationCallbackCannotOutliveAdmission`，使用真实帧及只阻塞 Close 收尾的可控 net.Conn 包装，制造终结响应与 context 取消竞争：旧回调未收尾时下一调用不得发送，回调结束后被关闭的连接不得继续使用。不要以 sleep 猜交错。

README 区分排队取消和已发送 RPC 的取消。consume 回调由调用方代码控制，本计划不承诺强制中止任意阻塞回调。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./sdk ./agent ./integration -count=1 -timeout=90s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
git diff --check
```

普通 integration 包可能跳过 fixture，必须另行实际运行 make integration。

## 完成标准

- [x] 五个 TestConnection 名称存在并执行；排队取消时零发送且首个 RPC 不受影响。
- [x] AfterFunc 收尾在准入交接之前，旧关闭回调不会影响后继 RPC；已被活动取消关闭的连接不复用。
- [x] 准入后再次检查 context，零值内部结构可用，原串行/活动取消语义保持。
- [x] race/vet/lint/实际 integration/diff check 通过；Scope 与索引结果完成。

## 实施与复核结果

APPROVE。worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-833f0eda`，分支 `t3/sdk-queued-call-cancellation`；基准 `b93fc24`，提交 `579054ccbd735fc6ae80a673471d0e8c3e081a4d`。只改 client.go、新增真实 net.Pipe 帧测试与 README。

执行者观察旧实现排队取消测试有界 RED，修复后五个连接级测试通过。父 reviewer 读取完整源码增量与全部五个测试，核对容量一准入/懒初始化、取 token 后重查 context、ID 只在准入后分配，以及 stop=false 时等待已启动 Close 回调退出才交还 token。父 reviewer 重跑 sdk/agent/integration race（sdk 2.077s）、sandboxd vet、make lint（0 issues）、实际 Rust fixture `make integration`（0.689s）及 diff check，全 exit 0。consume callback 仍由调用方负责返回；未增加 multiplexing/retry 或修改用户分支。

## STOP 与维护

若发现已发布调用方依赖跨 Send/Receive/Call 并发复用、需要新增公开初始化 API 或加入自动重放，停止报告。未来做真正多路复用时必须重新设计按 request 取消，不能把当前关闭 transport 的语义直接搬过去。分支 `advisor/014-sdk-cancel`；另获提交授权才使用 `sdk: honor context cancellation while waiting for RPC admission`。
