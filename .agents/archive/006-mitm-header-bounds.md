# Plan 006：限制 CONNECT 隧道内层 HTTP 的解析资源

> 执行者完整阅读本文后逐步实施；每步验证失败两次、证据漂移或越界时 STOP。完成后更新索引 006；reviewer 维护索引时只交付结果。本文不授权提交、推送或操作现有运行状态。
>
> Drift check：`git diff --stat b93fc24..HEAD -- sandboxd/egress/http.go sandboxd/egress/manager.go sandboxd/egress/mitm.go sandboxd/egress/mitm_test.go sandboxd/egress/egress_test.go sandboxd/egress/README.md`，再检查这些路径的未提交差异。新增文件在基准不存在；其他实现变化必须先与下列摘录核对。

## 状态

- Priority：P1；Effort：M；Risk：MED。
- Depends on：无。与 007 同属 egress，整包验证串行运行。
- Category：security；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支复核通过，未合入用户分支）。
- 审计映射：发现 1。

## 问题与目标

sandboxd 是多个租户共享的 Go 进程。relay guest 通过实例专属 UDS 发起 CONNECT，代理终结 TLS 后检查域名并代填宿主秘密。当前内层 HTTP 解析不受外层 server 的请求头限制；持续输入超长请求行／头会在策略 hook 执行前分配内存，idle timeout 和连接数限制不能限制单连接总输入。

目标是在内层每条请求的解析入口限制 header 字节和完成时间，保留 body 流式转发与隧道复用。不生成可运行的滥用演示，不改变 TLS 信任、域名准入和审计先于注入的顺序。

## 当前实现与约束

`sandboxd/egress/http.go:95`：

```go
return &goproxy.ConnectAction{
    Action: goproxy.ConnectMitm,
```

同文件外层 server：

```go
b.server = &http.Server{
    Handler: proxy,
    ReadHeaderTimeout: 10 * time.Second,
    IdleTimeout: b.manager.opts.IdleTimeout,
    MaxHeaderBytes: 1 << 20,
```

锁定依赖为 `github.com/elazarl/goproxy v1.9.2`。该版本 `https.go` 在 MITM 内部使用 `http1parser.NewRequestReader`，其 ReadRequest 调用 `http.ReadRequest`，不继承上面的 server 限制。可用的公开接入点为：

```go
// goproxy v1.9.2, https.go:139
Hijack func(req *http.Request, client net.Conn, ctx *ProxyCtx)
```

`ConnectHijack` 要求调用方自己写 CONNECT 响应。`sandboxd/egress/manager.go` 的 trackedConn 负责撤权后关闭已 hijack 连接；idleConn 保留显式 deadline，不能绕过这层包装。现有 OnRequest 同时验证 CONNECT authority、SNI、URL/Host 和 tunnel，使用数值 IP 拨号；这些检查全部保留。

测试范例：`sandboxd/egress/egress_test.go` 的 `newTestManager`、`connectTunnel`、`TestCONNECTRejectsPlaintextSNIAndInnerHostMismatch` 与 `TestRevokeClosesHijackedCONNECT`，使用真实 UDS/TLS 和本机 upstream，不连公网。仓库错误使用 `ErrDenied` 和固定脱敏响应，不回显请求头或凭据。

## Scope

允许修改：`sandboxd/egress/http.go`、`manager.go`，新增 `mitm.go`、`mitm_test.go`；调整 `egress_test.go`，更新 `sandboxd/egress/README.md` 及索引状态。

禁止修改：依赖版本、go.mod/go.sum、第三方源码、engine、guest、凭据注入算法和 public Policy JSON。不要复制整个 HTTP parser、用正则解析 HTTP，或对整条隧道应用累计字节上限。

## 命令与基线

从根目录使用 Nushell。现有命令：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test ./egress -count=1 -timeout=60s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
```

预期 exit 0；审计时普通测试与 lint 已通过。以下 TestMITM 开头的名称为本计划新增门禁，不是现有测试；须先 `go test -list` 确认名称，不能接受零测试通过。缺工具时报告 BLOCKED，不安装或修改宿主。

## 步骤

### 1. 添加真实隧道回归

新增 `mitm_test.go`，复用上述 UDS fixture。添加 `TestMITMHeaderBounds` 和 `TestMITMHeaderCompletionDeadline`：在正常 CONNECT/TLS 后，以有限尺寸数据检查超长 request line、单头、累计多头均拒绝，且 upstream dial 计数为零；测试数据上限数 MiB，禁止用耗尽内存作断言。超过 MaxHeaderBytes 至少 16 KiB 的输入必须拒绝，避免依赖 net/http 的固定 reader 余量。header 持续小步输入也不得无限延长明确的 header deadline；给测试连接设置自身 deadline，防止回归挂住 suite。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test ./egress -list '^TestMITM' }
with-env {GOWORK: 'off'} { ^go -C sandboxd test ./egress -run '^TestMITM(HeaderBounds|HeaderCompletionDeadline)$' -count=1 -timeout=45s }
```

修复前必须出现预期拒绝断言失败；没有复现时 STOP，不扩大载荷。

### 2. 使用标准 server 承载内层解析

在 `mitm.go` 实现 ConnectHijack 的受控接管：保留原 authority/resolveOnly 检查，回复 CONNECT 成功。设置有限握手 deadline 后，用有界 buffered reader 只窥视首字节识别 TLS record；明显明文请求返回现有固定 HTTP 403 并关闭，零 upstream dial，保留 `TestCONNECTRejectsPlaintextSNIAndInnerHostMismatch` 的该分支语义。不能先解析整条明文请求或无限 read。reader 缓冲字节通过 net.Conn 包装传给 tls.Server，不能丢掉已读握手字节。

使用原 CA/SNI 校验创建 tls.Server，有限 deadline 完成握手后清除握手 deadline，再交给专属 `http.Server` 和单连接 listener。内层设置 MaxHeaderBytes=1 MiB、ReadHeaderTimeout=10s、IdleTimeout=原 binding 值；不设置会截断合法流式 body 的整请求短 WriteTimeout。SNI 拒绝、内层 authority 拒绝和明文 403 各自独立验证。

listener 只接受这一个已跟踪连接；其关闭必须唤醒后续 Accept，server/handler 结束必须关闭连接，所有 goroutine 有退出路径。不要再次消耗 inbound 连接配额。用 request context 的私有类型 key 传递不可变 tunnel authority，仅内层 handler 能设置；将 origin-form URL 规范为 https 后继续使用同一个 goproxy 的请求/响应 hook 和受控 Transport。调整 OnRequest 从该可信 context 取 tunnel，不能相信 guest 头中的标记，不能让内层 CONNECT 递归接管。

若现有公开 API 无法安全复用请求过滤，则 STOP 提交具体接口限制，不能临时放弃 TLS/Host 检查或 vendor 第三方实现。

**Verify**：执行上一步 focused 命令，预期两个测试通过；`TestCONNECTRejectsPlaintextSNIAndInnerHostMismatch`、`TestHTTPSSecretInjectionAndNonLeak` 通过。

### 3. 补齐复用、body 与撤权验证

新增 `TestMITMStreamingBodyAndReuse`、`TestMITMRevokeClosesParser`：正常 body 大于 header 上限仍完整流式传输；同隧道第二条合法请求通过；第二条超限拒绝；read/write 半关闭和 Revoke/Close 结束内层 server；header 失败没有凭据注入事件。覆盖持续慢 header 与合法慢 body 的区别，不依赖长 sleep 或 goroutine 数量快照。

**Verify**：

```nu
with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./egress -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C sandboxd vet ./... }
make lint
make integration
git diff --check
```

预期全部 exit 0，实际 Rust fixture 执行。README 写清内外层边界及 body 不受 header 限制。

## 完成标准

- [x] 四个 TestMITM 测试存在并执行，超限与超时在 upstream 前拒绝。
- [x] body、同隧道复用、SNI/authority、防重绑定、审计注入及撤权回归全部通过。
- [x] 没有凭据／任意请求头进入错误或日志，未替换依赖或改 public schema。
- [x] race、vet、lint、integration、diff check 成功，变更限于 Scope，索引附实际结果。

## 实施与复核结果

APPROVE。worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-84acdd1c`，分支 `t3/bound-connect-inner-http-parsing`；基准 `b93fc24`，提交 `6d490ea850f0a51a25037265e547893a8dc41654`。变更只在五个授权 egress 文件，未更换依赖或 public Policy schema。

执行者报告有限载荷的超限/超时测试 RED→GREEN；父 reviewer 读取全部生产 diff、single-connection listener 与所有新增真实 UDS/TLS 测试，确认沿用 tracked/idle 连接、私有 tunnel context、标准 http.Server 及原 guard/injection hooks。streaming body 大于 header 上限且持续超过 header deadline，复用和第二条超限、握手/半关闭/撤权/异常 Upgrade 的 teardown 均实际覆盖。父 reviewer 重跑 egress race（27.178s）、sandboxd vet、make lint（0 issues）、实际 Rust fixture `make integration`（0.704s）和 diff check，全部 exit 0。未合入用户分支。

## STOP 与维护

不接受取消 TLS 校验、改变允许目标、以全连接累计长度模拟 header 限制、引入新的特权或后台常驻 parser。若单连接 listener 生命周期无法证明结束，先报告设计问题。后续 HTTP/2、WebSocket 或 goproxy 升级必须重新验证内层解析入口；原有 Upgrade 禁止保持有效。

沿用 orchestration 绑定的 checkout；若单独执行可命名分支 `advisor/006-mitm-bounds`。只有另获提交授权才使用 `egress: bound CONNECT inner HTTP parsing`，不自行提交、推送或开 PR。
