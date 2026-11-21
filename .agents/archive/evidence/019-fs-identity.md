# Plan 019：tree_pull 双 UID 与归档 FD 验证证据

状态：**COMPLETE（隔离分支实现及必需门禁完成，待父线程独立复核）**。没有 BLOCKED 门禁。结论是已验证的 FD 生命周期简化；旧源码只观察到结构性 FD 不变量 RED，未复现私有数据泄露。

## 交付与来源

- Worktree：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-a9563b37`。
- Branch：`advisor/019-guest-fs-identity`（由已绑定的 `t3/plan019-fs-identity` 在本隔离 worktree 内重命名）。
- Base：`b93fc24a631bb94c9e87484a460703f016742cea`。
- 首先执行：`git cherry-pick 0542708 fba80816a4a38fcb049c64bd2504106fe24462a1`，exit 0，无冲突。
- 008 第一项继承提交：`3dc7d12dc106d55496dd81710364878250ebf27a`，来源 `054270810044ae60b52225e8a43c9c6a97fb7a5e`。
- 008 最终继承 tip：`89edd5c45527aec92f98cacf7bcc991f98d65436`，来源 `fba80816a4a38fcb049c64bd2504106fe24462a1`。
- 未 cherry-pick `abb2bc0`，未纳入 015。`fs_read` 的 `O_NONBLOCK` 和同 FD 类型检查、`fs_replace` 的 `O_NOFOLLOW | O_NONBLOCK` 原样保留。
- 019 实现提交是包含本文的 `guest: retain authorized descriptors through tree pull`。父线程只需从上述继承 tip 读取本提交增量；最终 hash 由交付消息报告。
- 本分支不修改计划索引。建议父线程在独立复核后将 019 更新为 DONE，并保留“结构性 RED；未确认泄露”的调查限定。父线程管理的计划和验收文档没有纳入本分支。

## 步骤 1：实际身份与 namespace 前提

宿主检查 `id` 得到当前 UID 1000、GID 100。`command -v unshare newuidmap newgidmap cargo` 全部可用；`rg '^woxQAQ:' /etc/subuid /etc/subgid` 确认现有 subordinate 范围均为 `100000:65536`。没有安装或修改映射。

预检查实际执行了：

```sh
unshare --user --map-auto --map-root-user --mount --pid --fork --kill-child=KILL --mount-proc --propagation private -- setpriv --reuid=1 --regid=1 --clear-groups --inh-caps=-all --ambient-caps=-all sh -c 'id; rg "^(Uid|Gid|Groups|Cap(Inh|Prm|Eff|Amb)):" /proc/self/status'
```

exit 0，real/effective/saved/fs UID/GID 均为 1、补充组为空、上述四个 capability 集合全零。该预检查只决定继续调查；最终权限证明来自 required runner 的 Rust 子进程，并额外验证 bounding 集合为零。

required runner 的构建命令为：

```sh
cargo test --locked -p sandbox-agent --features filesystem-fixtures --test fs_identity --no-run --message-format=json
```

runner 从本次 Cargo compiler-artifact JSON 选择唯一的 `fs_identity` executable，复制到新建的 `/tmp/guest-fs-isolation.XXXXXXXX/`（祖先可遍历、二进制 0755），再以以下入口启动新 namespace：

```sh
timeout --kill-after=5s 90s unshare --user --map-auto --map-root-user --mount --pid --fork --kill-child=KILL --mount-proc --propagation private -- bash "$root/scripts/run-guest-fs-isolation.sh" --inside "$run"
```

其中 `$root` 是本 worktree，`$run` 是本次新建的随机 scratch。没有执行旧 binary 或复用旧运行材料。namespace 内新挂载 `tmpfs` 于本次 scratch 的 `fixtures` 目录，所有测试文件均置于该挂载；namespace 退出后挂载释放，外层 EXIT trap 删除自建的 binary、JSON 和结果文件。内部测试子进程另有 15 秒硬截止、kill/wait；整套由外层 90 秒截止和 PID namespace 的 kill-child 约束。

`identity_fixture_proves_distinct_authorities` 实际 PASS，最终证明如下：

| 项目 | 实际结果 |
|---|---|
| namespace | runner 和每个场景子进程比较 user/mount/PID namespace 标识，均与外层父环境不同 |
| daemon | real/effective UID/GID 0；DAC_OVERRIDE、SETGID、SETUID、SYS_ADMIN 为 effective |
| UID mapping | `0 -> 1000 (1)`，`1 -> 100000 (65536)` |
| GID mapping | `0 -> 100 (1)`，`1 -> 100000 (65536)` |
| workload exec | real/effective/saved UID/GID 全为 1；补充组数量 0 |
| workload capabilities | CapInh / CapPrm / CapEff / CapBnd / CapAmb 全零 |
| 授权文件 | workload 子进程可读取新建的 workload-owned 0600 文件 |
| 私有 sentinel | 新建目录 root-owned 0700、文件 0600；namespace root 可读；workload 实际打开得到 PermissionDenied |
| 未继承 FD | workload 枚举 `/proc/self/fd`，同时按路径及 device/inode 排除 sentinel FD |
| RPC | 实际 `Portal::Workload` + `serve_connection`；配置 UID/GID 1。`fs_read` 私有 sentinel 返回 Permission denied、零数据；正常 `tree_pull` 返回授权 tar 并排除 ledger |

sentinel 是每次 `getrandom` 生成的 64 字节无实际用途测试数据，从未输出字节值。没有读取真实 root 私密文件或凭据。

## 步骤 2：旧源码观察与受控名称变化

先在继承 008 的源码上加入 feature、scratch 调度和测试，保留旧 `tree_pull` 的生命周期：`NamedTempFile` 保持有名；workload 按 `archive_path` 打开 writer；archive_tree 完成后，checkpoint 暂停在 daemon 按路径 reopen 之前。该阶段唯一相关插桩是将临时文件放入注册的私有 scratch 并发送 FD metadata；未加入 unlink、FD clone writer 或 rewind。

随后实际执行：

```sh
CARGO_NET_OFFLINE=true make guest-fs-isolation-e2e
```

结果：make exit **2**，内部 Rust suite exit **101**，**1 PASS / 2 FAIL / 0 ignored**。

- `identity_fixture_proves_distinct_authorities` PASS：真实不同 UID、空组、零 capabilities、fixture 权限拒绝及授权树正向对照已建立。
- `tree_pull_authority_stays_on_created_descriptor` RED：暂停于“archive_tree 已完成、daemon 尚未按路径打开 reader”阶段，`snapshot.links` 实测 **1**，断言要求 **0**，错误文本为 `FD invariant RED: completed archive still has a name before daemon streaming`。
- `tree_pull_descriptor_cleanup_and_cancellation` 在相同的 links=1 断言失败，后续子场景没有执行；不将它写为旧实现清理失败的证据。

因此旧源码的可复核结论是**仍存在临时名称且即将重新获取路径授权**。此次 RED 在名称替换及数据传输之前停止，没有观察到 sentinel 出现在输出、权限泄露或 FIFO 阻塞。不能将静态路径 reopen 或这项 nlink 断言写成已复现漏洞。RED 插桩是同一实施过程的中间状态，没有单独保存为最终可运行源码提交；其阶段、保持不变的旧操作及实际断言结果记录在此。

完成 FD 修改后，该场景在新的私有 scratch 中根据原 FD 的 `/proc/self/fd` 名称重建旧名称，分别放置：指向本次私有 sentinel 的 symlink、无 writer 的 FIFO、含无害替代内容的普通文件。每次都确认替代对象 inode 与归档 inode 不同、原归档 nlink=0 且 owner UID/GID=1。放行后读取实际 RPC，三种情况均得到原授权树，未输出 sentinel，无阻塞；完成后归档 inode 没有残留打开 FD，清理替代对象后 spool 目录为空。

名称操作由 fixture 的 namespace-root 驱动，只作用于它刚创建的 scratch；它验证名称变化不影响归档 FD，不声称普通 workload 在生产目录中具备相同名称操作机会，也不使用不确定性 race 循环。

## 步骤 3：FD 生命周期与清理

生产实现以 workload filesystem identity 创建 `NamedTempFile`，立即调用 `into_file()` unlink 并保留 `File`。`try_clone()` 得到的 writer 交给 workload 身份下的 `archive_tree`；等该 blocking 任务完成、writer 析构后，显式 `Seek::rewind`，再以 `tokio::fs::File::from_std` 读取原 FD。clone 共享 offset，因此 rewind 位于生成完成之后，没有并发读写归档或路径 reopen。流读取响应原有 cancel token，输出仍经原 bounded-frame/backpressure 路径。

只调整 `tree_pull` spool，未修改其他文件操作、cwd、UID/GID 默认值、portal 权限或 xattr parser；tar 内容、1 GiB 预算及私有 ledger 排除继续使用原 archive_tree。

测试设施名称：`Fixture` 是单次注册句柄并持有新建私有 scratch；`Operation` 是匹配目标路径的一次 tree_pull 调度；`Snapshot` 只包含 FD 的名称和 metadata。整个模块和 dispatch 中全部设施调用均由 `#[cfg(feature = "filesystem-fixtures")]` 包围。Cargo 只新增 `filesystem-fixtures = []`，未新增依赖或 default feature。没有生产 RPC 参数或生产环境变量开关，没有库内 cfg(test) 导出 hook。

`tree_pull_descriptor_cleanup_and_cancellation` 实际 PASS：

- 仅有根目录、ledger 被排除的空授权树能够解析为有效 tar。
- 大文件长 `CHUNK * 129 + 7`，总归档大于 CHUNK；完整内容一致，验证 writer offset 已正确 rewind。
- 非空归档开始 streaming 前断开，以及读取首个数据 frame 后断开，均释放归档 FD 和全部 128 个 workload request permits。
- 原始归档 inode 使用 `/proc/self/fd` 对 device/inode 检查；成功及断连后均无活 FD，私有 spool 目录为空。
- 源树含 FIFO 时 archive_tree 返回 unsupported special file，零流数据；错误后 permits 回收且 spool 目录为空。

## 最终门禁

下列为实际命令和最终结果；stdout 的筛选包装不影响命令退出码。offline 环境只禁止获取新依赖，不关闭 native 检查。源码改动后已重跑相关门禁；最后仅补充本文。

| 实际命令 | Outcome |
|---|---|
| `CARGO_NET_OFFLINE=true make guest-fs-isolation-e2e` | exit 0；3 个 required 场景实际 PASS，0 ignored，runner 枚举确认无 SKIP |
| `CARGO_NET_OFFLINE=true cargo test --locked -p sandbox-agent --test tree` | exit 0；8 PASS |
| `CARGO_NET_OFFLINE=true cargo test --locked --workspace --all-targets` | exit 0；30 PASS，0 ignored；默认 feature 下 fs_identity 有 0 个测试，不能以它替代 required runner |
| `cargo fmt --all -- --check` | 最终 exit 0；编写初期一次格式检查 exit 1，经 `cargo fmt --all` 修正 |
| `CARGO_NET_OFFLINE=true cargo clippy --locked --workspace --all-targets -- -D warnings` | exit 0 |
| `CARGO_NET_OFFLINE=true cargo test --locked -p sandbox-agent --features filesystem-fixtures --test tree` | exit 0；8 PASS |
| `CARGO_NET_OFFLINE=true cargo clippy --locked -p sandbox-agent --features filesystem-fixtures --all-targets -- -D warnings` | exit 0 |
| `CARGO_NET_OFFLINE=true GOPROXY=off GOSUMDB=off make integration` | exit 0；本次重新构建 unix_fixture，Go integration 实际执行 |
| `CARGO_NET_OFFLINE=true GOPROXY=off GOSUMDB=off GOFLAGS=-v make integration` | exit 0；为确认实际运行而 verbose 重跑，以下四项全部 PASS、无 SKIP |
| `bash -n scripts/run-guest-fs-isolation.sh` | exit 0 |
| `git diff --check` | exit 0 |

普通 integration 实际 PASS 的测试为 `TestRustAgentThroughSandboxRelay`、`TestRustKeyedExecReplayThroughSandboxRelay`、`TestRustRawForwardHalfClose`、`TestRustPreviewUsesRawForward`。未启动任何 VM。

## Scope、父工作区和限定

019 增量文件只有 `Makefile`、`guest/agent/Cargo.toml`、`guest/agent/src/fs.rs`、`guest/agent/tests/fs_identity.rs`、`scripts/run-guest-fs-isolation.sh`、`guest/README.md` 及本文。`guest/agent/tests/tree.rs`、Cargo.lock、capture runner 未修改。基准至 tip 还包含两个已批准 008 提交继承的 `guest/agent/tests/agent.rs`，该文件没有 019 增量。

Drift 核查命令：

```sh
git diff --stat b93fc24..HEAD -- guest/agent/Cargo.toml guest/agent/src/fs.rs guest/agent/tests/tree.rs guest/agent/tests/fs_identity.rs scripts/run-guest-fs-isolation.sh Makefile guest/README.md plans/evidence/019-fs-identity.md
git diff --stat 89edd5c..HEAD
git status --short
git diff --check
```

父工作区 HEAD 只读核查仍为 `b93fc24a631bb94c9e87484a460703f016742cea`；本计划涉及源码路径相对基准的父工作区 diff stat 为空。父工作区存在其自行维护的计划/验收文档差异，因此不声称父 worktree 整体 clean；没有对其 merge、push、commit、index 或文件执行任何写操作。

没有 sudo、安装、NixOS 修改、真实凭据/运行状态/`.local-run` 或旧材料使用，也没有嵌套委派和 helper function 单测。所有权限和名称操作仅作用于新 namespace 与自建 fixture。这里不证明磁盘/VM 崩溃后的清理或不可取消的底层存储 I/O；保留原契约，不扩大为所有文件 RPC 的权限结论。
