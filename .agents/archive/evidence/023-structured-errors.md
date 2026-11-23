# 023：实施与验证证据

## 源码与授权

- 用户授权实施 023，随后批准扩大 Go 范围：sandboxd 自有错误契约、场景化边界翻译、私有诊断与公开投影；不包含 Shape 等其他 DTO 的独立化。
- 原始基准为 `6f498c8`。开始时计划及索引有用户未提交改动；实施中用户将计划提交为 `a73d1d56970828afa87a4656247d66a81302c48d`。本轮代码未提交、未推送、未合入。
- 验证对象为该 HEAD 加本轮工作区源码差异，不使用历史验收替代本轮结果。
- 80 个新增/修改源码文件（`.go`、`.rs`、`.toml` 与 `Cargo.lock`）的变更清单指纹：
  `3fa2e55351f05059afd0166bb8d7050778c5b2a97583459a4f30d4691098ec09`。
  算法：合并 `git diff --name-only HEAD` 与 Git 非忽略未跟踪文件名，按路径排序并过滤上述源码类型；每项编码为 `path + NUL + SHA256(file).hexdigest() + LF`，再对完整 UTF-8 清单做 SHA256。文档归档不改变此指纹。
- 017/020/021 未启动；未使用子代理。此证据为命令结果摘要，完整输出在本线程工具记录中；临时终端日志不冒充持久验收产物。

## 交付

- InitError 和 agent Error/ErrorCode 使用 `#[track_caller]` 静态位置与惰性上下文；不新增外部 crate。移除 init 的 anyhow 依赖并同步 Cargo.lock。
- Agent library 全模块使用本层 Result；统一失败帧编码，保留 keyed exec 的精确 code/result_state 与 tree push 已提交结果参数。程序 CLI/fixture 入口继续可使用 anyhow，不构成 RPC 分类边界。
- sandboxd 拥有 ErrorCode/Error/Fail/Wrap，保持 code/message JSON；Cause 不序列化、不实现 Unwrap。API 错误类型只在 Engine 适配分类处被识别。
- 回收路径区分本层拒绝与直接 Engine 观察。已翻译错误、隐藏 404、混合联合错误都不能证明 physical absence。移除 release 中用于表示未 dispatch 的假 NotFound。
- 未消费的 Engine NotFound/Conflict/ManualRecovery 翻译为本层恢复要求；Engine Invalid 是宿主构造请求的问题，不归为租户参数错误。Unknown、容量和不兼容类别保留动作语义。
- 新 Operation 错误持久化安全 action/step 文案；现有私有 audit journal 的事件保留原始 cause 诊断，无新增表或 schema 版本。journal 写入是 best-effort，不改变终态提交结果。
- HTTP、Operation 查询/重放、嵌套结果及 Sandbox.LastError 使用安全投影；所有已持久错误类别的旧消息均不被当作可信公开文案。历史 error JSON、幂等身份、result witness 与终态落盘字节不因读取而重写。
- SDK 的 HTTP 错误使用 sandboxd.Error，直接 import 列表无 sandbox-engine/api；公开 Shape 等 DTO 的间接 Engine 依赖未变。

## 最终通过的命令

以下命令从仓库根执行；最后的 Go、Rust、lint 和 integration 检查使用 Nushell 调用等价入口。

| 命令 | 结果与范围 |
|---|---|
| `cargo test --locked --workspace --all-targets` | PASS；含 init 非 PID 1 拒绝、sysfs fixture，agent RPC/进程/会话/树/账本行为 |
| `cargo test --locked -p sandbox-agent --features filesystem-fixtures --all-targets` | PASS；普通 feature 运行本身不证明跨 UID 场景实际执行 |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --locked --workspace --all-targets -- -D warnings` | PASS |
| `with-env {GOWORK: 'off'} { ^go -C protocol/gen test ./... }` | PASS；最终重复调用可使用缓存；本轮也有非缓存通过记录 |
| `with-env {GOWORK: 'off'} { ^go -C engine test -race ./... -count=1 -timeout=10m }` | PASS；Engine 主包约 132.051s |
| `with-env {GOWORK: 'off'} { ^go -C sandboxd test -race ./... -count=1 -timeout=10m }` | PASS；最终主包约 342.043s，agent/egress/SDK 同次通过 |
| `make integration` | PASS；实际构建并启动 Rust Unix fixture，Go service/SDK/relay/RPC 路径通过；最终约 0.712s |
| `make guest-fs-isolation-e2e` | PASS；fresh user/mount/PID namespaces，7 个双 UID 场景实际通过，无 skip；含 publication fsync failure 的完整 postcommit error_code 与 cleanup 状态 |
| `make lint` | PASS；四个 Go workspace 模块，0 issues |
| `git diff --check` | PASS |

### 新增或增强的行为证明

- 普通 RPC：invalid、not_found、conflict、unauthorized、quiesced、resource_exhausted；含目标和原始 Rust 文件位置，不包含环境值或 exec key。
- Keyed exec：首次 spawn 或结果持久化失败均为 result_unknown，同键不再启动；重启、并发 admission、容量和 replay 继续通过。
- Host client：真实 CH 握手/Protobuf 传输上的管理错误翻译与 keyed completion 未确认围栏。
- Service/SQLite/HTTP：本层 404、翻译后的 cause 和 joined Engine NotFound 不释放费用；直接可信 Engine absence 可以退休责任。
- HTTP：Engine 缺少现有 sandbox 的 VM 返回 503，不成为租户 404；root-only 三条路由统一 403。
- 历史 receipt：以旧 code/message JSON（包含非 internal 的 conflict 私有消息）构造私有测试记录，重启后查询与幂等重放均脱敏，SQL body 字节不变。
- 私有 cause：SQL fault 断言检查实际私有 journal 或内存 Cause，继续要求真实 deferred FK COMMIT 失败，不以公开安全文案替代失败证据。

## 本轮曾失败或中断的检查

- 最早两项全量 Go race 被用户中断，没有记为 PASS。
- Engine `-race ... -timeout=180s` 曾 FAIL：既有完成事务测试仍断言旧错误文案，同时该次全量运行到达 180s 总预算。更新文案断言以包含新 action/step，保持所有租约、触发器、实例和终态断言；窄 race 重跑通过，最终按 README 现行 10 分钟预算完整通过。没有放宽生产限制或删除测试。
- 扩展 sandboxd 类型迁移期间，全量普通 Go 测试曾 FAIL：测试仍用 Engine 类型检查 SDK 错误，或从已脱敏公开错误文本寻找 SQL fault。测试分别改为本层类型、明确的 Engine 观察检查及私有诊断证据；最终完整 race 运行通过。
- Rust 迁移期间的编译/Clippy 以及 Go golines 格式检查曾 FAIL；修正转换/类型标注、移除多余包装并使用项目 formatter 后，最终检查通过。

## 未运行与不作出的结论

- 未运行 `SANDBOX_REQUIRE_EROFS_TESTS=1` 原生镜像门禁；普通 Engine suite 可按现有条件跳过 native 场景。本次未修改 OCI/EROFS，不据普通测试声称原生镜像验收通过。
- 未构建/启动新的静态 musl guest、KVM、完整 initramfs/systemd、AF_VSOCK、entropy/clock/hostname 特权路径。
- 未运行完整 kernel-isolation 或 production acceptance；跨 UID 文件系统 runner 只证明其列出的 Linux namespace/权限场景。
- 不保证 audit 写入失败时还能持久保留原始 cause，也不把 Cause 当作重启后的内存链。已有的私有 audit/metadata 备份须继续按 operator-private 材料保护。
