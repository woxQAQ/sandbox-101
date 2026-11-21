# 024 阶段 E：唯一入口、协议与验证

本页绑定阶段 E 完成树，历史状态为 `STAGE_E_PASSED / STAGE_F_NOT_STARTED`。阶段 E 的四个入口门槛与当时最终 required-native 均已通过；原 300s/race/JSON/count=1 条件保持。E 最终 native 为 `run.d6zrkslx`：**940 pass、0 fail、0 skip**，Haven **273.092s**，Engine **44.249s**。当前 F 的旧边界删除、新树结果与 VM BLOCKED 项见 [阶段 F 证据](024-stage-f-retirement.md)；下文不作为 F 通过证明。

## 入口与职责

| 边界 | 当前实现 | 实际验证 |
|---|---|---|
| 唯一 daemon | `cmd/haven`、`internal/config`；真实 `loom/host.CloudHypervisor`、`LiveGuest`，内部 launch/device-check 在配置/DB 之前分派 | `make build`、实际进程 smoke；不构建/启动旧 daemon |
| 业务协议 | `api/`、`sdk/`、`internal/httpapi/`、`cmd/havenctl` | 原路由、JSON/error、Bearer/root/tenant/sandbox scope、严格 JSON、幂等/async、Upgrade、Preview；真实 Rust HTTP 集成 |
| 管理协议 | `internal/localuds/`、`internal/management/`、`internal/haven/management.go`、`cmd/loomctl` | mode 0600、受控父目录、kernel peer credentials；node scope、同 store ID/epoch、同一 GC/OCI/cleanup 工作流 |
| 启动协作 | `Options.EgressFactory`、`proxy_socket_linux.go`、`audit.go` | reconcile 前绑定真实代理回调；精确实例/vsock inode 授权、原 direct retirement、持久审计 |
| 调度与退出 | `maintenance.go`、`service.go`、`cmd/haven` | 维护进入原工作流，pool 满/退避不造新意图；数据建立与连接参与退出，writer 最后释放 |
| 冻结交付 | `publication.go`、`store_publications.go`、schema 9 | 终态/交付同事务；状态与结果同一个读 snapshot；改变配置/lease/生命周期后重开不重写旧结果；损坏在可写打开前拒绝 |
| fork 批次 | `fork*.go`、`provision_plan.go`、`checkpoint_paused.go` | running/paused/checkpoint，1–32 分支；整批费用预留与资源分配；原失败 discard、成功业务 release；SQL/进程中断后固定目标、原分支交付视图 |
| 数据最终交接 | `data_admission.go`、`internal/httpapi/data.go` | Hijack 前再次核对原 instance/configuration；迟到上下游不能跨 configure fence；Preview 不转发平台凭据 |
| 长 Unix 路径 | `loom/unixsocket/`，System/Workload/CH API 与代理 socket 调用点 | 原父目录 FD 上 bind/dial；live/stale/replacement inode 断言和真实 Rust 长路径请求 |
| 同一 metadata backup | 原 Haven offline backup/review，加新 daemon 模式 | smoke 证明业务 tenant 与管理 GC operation 在同一备份 DB，key/HEAD/checksum 同点；live backup 被 writer 拒绝 |
| 依赖图 | `scripts/check-architecture.py` | Loom 无 DB/业务依赖；两个 CLI、API/SDK 无 Loom/Haven 控制/SQLite/旧模块；Haven cmd 绑定真实 host runtime |

Haven schema 为 **9**，拒绝旧 engine/sandboxd 和旧 Haven prototype，没有自动迁移。`sandboxIdentity` 保存不随 VM generation 改写的业务出生事实。`operation_publications` 保存同库冻结交付；内部 fork 分支也冻结原激活结果，但没有公开 credential 的资源子操作不通过业务 operation API 暴露。

`statementDB` 在 writer 生命周期内最多保留 256 个固定只读 prepared statements。它复用 SQL statement，不缓存查询值或资源证明；事务仍使用原连接/事务边界。这个有界复用来自 required-native 超时后的 CPU 采样，保留完整 hash 和身份核验。

## Rust fixture 的迁移与证明范围

原 `sandboxd/integration/transport_test.go` 四个场景已迁到根 `integration/transport_test.go`，原测试名与断言保留：

- `TestRustAgentThroughSandboxRelay`：复用 stream、结构化 guest 错误、stdout/stderr、二进制文件流、拒绝 Admin、release 关闭连接。
- `TestRustKeyedExecReplayThroughSandboxRelay`：原 keyed exec 结果、输出重放、不同参数冲突、错误后连接继续可用、副作用只一次。
- `TestRustRawForwardHalfClose`：原 raw bytes 与 half-close 之后的反向输出。
- `TestRustPreviewUsesRawForward`：原路径、Authorization/Cookie 去除与 Referrer-Policy。

新增 `TestRustTransportWithLongRuntimePath` 和 `TestHavenHTTPContractAndIdempotency`。后者覆盖 async JSON、原 operation ID/终态重放、默认 300s lease、sandbox token 的操作权限、foreign/missing 404、root 操作读取、严格 JSON、405/426、lease/release 与 retired token。

fixture 从 `loom.Backend` 和 guest System/Admin 效果边界显式注入，创建私有 disk/runtime/socket。离线输入阶段只预置私有 immutable image，不插入 operation、成功结果、witness 或收费行；真实 Haven/SQLite 完成其余所有控制事实。Workload 请求经过真实 HTTP Upgrade 和真实 Rust dispatcher。没有独立 fake engine control client。

`make integration` 显式使用 `integration` tag 和刚构建的 Rust Unix fixture；缺少 `SANDBOX_AGENT_FIXTURE` 时 FAIL。普通 Go/native 组件入口不声明运行 Rust fixture，真实 fixture 证据由该独立入口提供。这不证明 guest boot、KVM、kernel isolation 或生产路径。

## 配置、smoke 与管理边界

新配置前缀为 `HAVEN_`、`HAVENCTL_`、`LOOMCTL_`，原 `ENGINE_`、`SANDBOXD_`、`SANDBOXCTL_` 不作代理。smoke 使用实际新二进制验证 flags/env/file/default 优先级，拒绝旧 flag、未知/null key、非整数预算与不完整 TLS 配置。

smoke 只启动一个 Haven，验证两个 CLI 的 store ID 一致、tenant HTTP、第二 writer 拒绝、运行时 offline backup 拒绝、dry-run 不改 epoch、GC 固定管理 operation、重启幂等、同一 DB/key/HEAD 备份。所有 state/socket/token/backup 都是新建私有资源；没有接触现有 daemon、`.local-run/`、旧真实 DB 或未知实例。

具体命令与操作说明见 [Haven 控制契约](../../docs/haven-control.md)。

## 实际验证与失败记录

统一通过 `nix develop --no-write-lock-file -c ...` 执行；需要大量私有文件的测试使用新建的短 `/dev/shm/h24e.*` TMPDIR。没有修改资源限额、既有 gate 超时或原行为断言。

| 检查 | 当前结果 |
|---|---|
| `make build` | PASS：`bin/haven`、`bin/havenctl`、`bin/loomctl` 和原 Rust workspace |
| `make integration` | PASS：根真实 Rust fixture 六项集成，原四项 transport 场景保留 |
| `make smoke` | PASS：六组实际进程/CLI/配置/备份断言 |
| `python3 -I scripts/check-architecture.py` | PASS：Loom、真实 Haven runtime、两个协议 CLI、API/SDK 依赖边界 |
| workspace `golangci-lint run ./... ./engine/... ./operator/... ./protocol/gen/... ./sandboxd/...` | PASS，0 issues；没有执行 `go fix` |
| tagged integration `golangci-lint run --build-tags integration ./integration/...` | PASS，0 issues |
| 根 `GOWORK=off go vet ./...`、`git diff --check` | PASS |
| 异步 HTTP 完成/查询修复后连续 20 次 | PASS；最终入口门槛另外重跑 |
| fork/publication/configure/backup/startup 组合 `-race` | PASS；SQLite schema fault 与新 statement 复用一并验证 |
| required-native 首次补充回归 `run.8inzmkwl` | FAIL：Haven 达到 300.030s，原 300s timeout；没有报告断言失败。原始日志保留 |
| required-native 预算复核 `run.llld0pvq`、`run.cg6fj9v1` | FAIL：仍触及原 300s；没有报告断言失败，日志保留 |
| required-native `run.h8vk51pe` | Haven 完整 race/native PASS，268.003s；随后 Engine 因 Go 要求同步 module 清单而阻断，不能视作整体验证通过 |
| required-native 最终复核 `run.d6zrkslx` | **PASS：940 pass、0 fail、0 skip**；Haven race **273.092s**、Engine **44.249s**，原模块 300s/JSON/race/count=1 未变 |

最终入口/静态/lint 命令日志为 `/tmp/haven024-e-delivery-final.log`，最终 native 日志为 `/tmp/haven024-e-native-complete.log`，原始 JSON/exit code/入口 receipt 位于 `build/native-gate/run.d6zrkslx/`。异步查询第一次最终复跑确实暴露过 pending/terminal 混读；旧状态先读、结果后读已改成同一个 SQLite 读事务，该失败日志保留在 `/tmp/haven024-e-entrypoint-gates.log`。

原完整组件预检也暴露过终态先于模板 guard/HEAD 结束的 cleanup 竞态：同步 Wait 已覆盖 worker 释放 guard 与 HEAD 更新。先前 `/tmp` 组件预检到 120s 的失败保留；之后改用规定的私有 tmpfs 输入，组件通过 92.385s。该较早结果不替代最终源码的 required-native 复核；最终当前树已由 `run.d6zrkslx` 完整覆盖。

新增测试复用其有归属的已冻结来源并执行正常 retirement，保留全部来源、三个分支、五个恢复点、预算与历史结果断言。生命周期测试的有界并发从 2 调整为最终固定的 4（中间 3 仍触及预算），仅改变私有 fixture 调度；没有减少既有断言、修改生产资源限额、拆分测试门槛或扩大 300s。对共享 paused 来源新增 checkpoint 时发现未迁入的零唤醒路径，现已用原 pause receipt/睡眠 publication 和同库绑定完成，不启动或重捕获 VM。

## F 与尚未执行项

F 未开始：旧 daemon/module、engine UDS facade、部署/runner 的最终清除与当前 Haven 的真实路径验收仍待执行。017/020/021 保持暂停。没有运行当前 Haven 的 KVM、kernel-isolation、capture-isolation 或 production 验收，没有部署切换、提交、推送或合入。`docs/README.md`、`docs/research/sandbox-comparison.md` 保持原用户内容。交付时另观察到只读 reference 子模块 `references/microsandbox` 的 `crates/agentd/Cargo.toml` 有工作区改动；本轮未修改、初始化或回退该文件，保持现场。
