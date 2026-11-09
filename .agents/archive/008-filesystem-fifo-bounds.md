# Plan 008：让文件 RPC 在打开 FIFO 时及时拒绝并释放名额

> 逐步执行，检查每个门禁的实际测试名；STOP 时报告。完成更新索引 008，reviewer 管理索引时交付日志。Drift check：`git diff --stat b93fc24..HEAD -- guest/agent/src/fs.rs guest/agent/tests/agent.rs guest/README.md`，同时检查未提交差异；015/018/019 也修改 fs.rs 时只合入各自授权改动。

## 状态

- Priority：P1；Effort：S；Risk：LOW；Depends on：无。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支复核通过，未合入用户分支）。
- 审计映射：发现 3。

## 问题与目标

文件 RPC 在读取 FD 前已经取得 workload 的全局 permit。当前 fs_read/fs_replace 先阻塞打开路径，再检查是否为普通文件；没有写端的 FIFO 可以无限等待。连接取消不能中断 spawn_blocking 内的 open，最终令 workload RPC 名额耗尽并阻止排空。

目标是非阻塞打开后在同一 FD 上确认普通文件，非普通文件立即返回错误，不改变工作负载文件权限、不添加全局 root 旁路。

## 当前实现与契约

`guest/agent/src/fs.rs:175`：

```rust
let file = access(identity, move || Ok(File::open(&target)?)).await?;
let mut file = tokio::fs::File::from_std(file);
ensure!(file.metadata().await?.is_file(), "read requires a regular file");
```

同文件 fs_replace：

```rust
let mut file = OpenOptions::new()
    .read(true)
    .custom_flags(libc::O_NOFOLLOW)
    .open(&target)?;
let m = file.metadata()?;
```

`access` 使用 spawn_blocking + with_identity；后者通过线程级 setfsuid/setfsgid 借用 workload 身份，无 async suspension。`guest/agent/src/server.rs` 将 owned permit 放在整个 dispatch 的 async task 中，`guest/agent/src/lib.rs` 的 requests 是 Semaphore(128)。不得通过单纯 timeout/drop JoinHandle 声称阻塞 syscall 被取消。

fs_read 当前允许访问符号链接指向的可读普通文件，fs_replace 使用 O_NOFOLLOW；分别保留这两个语义。设计 P10 要求有界数据面；保留私有 ledger 拒绝、流式背压、offset/length 及原子替换。

测试匹配 `guest/agent/tests/agent.rs` 的 setup/connection/rpc，经过真实 Workload serve_connection 与实际文件系统；不要新增独立 open helper 的单测。该 fixture 使用当前 UID，无需 root 即可创建自己的 FIFO。

## Scope

允许：`guest/agent/src/fs.rs`、`guest/agent/tests/agent.rs`、`guest/README.md` 和索引状态。

禁止：server.rs 请求数、portal 权限、ledger、进程执行、tree transfer 算法、依赖。可抽取内部 FD 打开 helper，但不为它单独写镜像式测试。

## 命令

现有根目录 Nushell 命令：

```nu
cargo test --locked -p sandbox-agent --test agent
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
make integration
```

审计基线通过。下文新增名称先用 `cargo test --locked -p sandbox-agent --test agent -- --list` 核对，不能接受零匹配。

## 步骤

### 1. 加入不会挂死自身的 FIFO 行为测试

在 agent.rs 新增 `filesystem_fifo_rejection_releases_rpc_slots`，对 fs_read/fs_replace 的无写端 FIFO 请求加短而宽裕的测试 timeout。先收集 timeout 结果，执行清理后再 assert：清理通过测试拥有的 FIFO 的 O_RDWR|O_NONBLOCK rescue FD 解开旧实现中阻塞的 open，等待 handler 结束／permit 回到 128，最后关闭 rescue FD。该恢复动作只用于 RED 测试收尾，不进入产品实现，也不提前给 FIFO 提供写端而掩盖回归。

再测试请求发出后立即断连、连续超过 128 次顺序请求的 permit 恢复、并发普通 info 正常完成。每次只保留有界在途请求，不故意挂起 128 个 blocking worker。不得在 Tokio runtime 的 shutdown 等待中无限阻塞；若无法保证清理，改为有父进程硬截止的独立测试子进程。

**Verify**：

```nu
cargo test --locked -p sandbox-agent --test agent filesystem_fifo_rejection_releases_rpc_slots -- --exact --nocapture
```

旧实现应在及时拒绝断言失败并正常退出，不能让整个命令挂死。

### 2. 非阻塞打开并验证取得的 FD

fs_read 用 OpenOptions 的 O_NONBLOCK 打开，在身份借用闭包内通过 FD metadata 确认 is_file 后返回 FD；fs_replace 在原有 O_NOFOLLOW 上增加 O_NONBLOCK，同 FD 校验普通文件和大小。不要先 lstat 再重开；不要将 fs_read 的全部 symlink 一并禁止。普通文件无需为读写清除 O_NONBLOCK，若确需调整仅操作已验证 FD。

**Verify**：上一步新增测试变绿；现有 `files_rollback_and_watch_ready` 通过。

### 3. 保留文件行为及运行契约

扩展该 RPC 测试，覆盖普通文件、fs_read symlink、fs_replace symlink 拒绝、目录拒绝、EOF、offset/length，确认失败不改写原路径或 FIFO。README 写清普通文件约束和及时拒绝特殊文件；不承诺所有文件系统 I/O 都可硬取消。

**Verify**：运行命令表的 Rust workspace、fmt、clippy、实际 integration，以及 `git diff --check`，预期全部 exit 0。该变更不需要 KVM；不将普通测试升级成无关特权验收。

## 完成标准

- [x] FIFO 测试存在、执行且正常退出；及时拒绝与断连后的 permit 均验证。
- [x] 普通文件与原 symlink 语义保留，特殊文件未产生写副作用。
- [x] 所有打开后的类型验证针对同一 FD，未使用检查后重开的竞争模式。
- [x] Rust 全门禁、integration、diff check 通过；Scope 及索引结果更新。

## 实施与复核结果

APPROVE。最终 worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-2b480da4`，分支 `t3/fifo-rpc-test-doc-revisions`；基准 `b93fc24`，提交 `0542708`（初始实现）和 `fba80816a4a38fcb049c64bd2504106fe24462a1`（复核修订）。原 `abb2bc0` 已被 cherry-pick 为 `0542708`，后续组合只采用最终分支这两个提交，避免重复。

执行者记录旧实现 focused FIFO 测试 RED，正常退出；首次实现后父 reviewer 重跑 Rust 全套/fmt/clippy/实际 fixture integration 均通过。复核要求把 130 次 info 改为真正交替拒绝 FIFO RPC，并将断连测试与 rescue 阶段分开、join server task；README 同时明确设备驱动可忽略 O_NONBLOCK。第二轮逐项完成，父 reviewer 再读完整增量并重跑 `cargo test --locked --workspace --all-targets`（30 tests）、fmt、clippy、`make integration`（实际 Rust fixture，1.085s）及 diff check，全部 exit 0。变更只在三个 Scope 文件；未修改用户分支或现有运行状态。

## STOP 与维护

如果需要改 portal 名额、为取消执行全局 kill、增加 root 文件权限或改变公开文件语义，停止报告。若读写异常只能在不受信任 FUSE 等环境复现，另行定义能力边界，不扩展本 FIFO 修复。后续新增文件读取入口应复用已验证的打开模式。分支可用 `advisor/008-fifo-rpc`；另获提交授权才使用 `guest: reject nonregular files without blocking open`。
