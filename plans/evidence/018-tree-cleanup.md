# Plan 018：tree_push 提交结果与旧目录清理证据

状态：**COMPLETE（当前隔离分支实现、required 双 UID 和全部指定门禁完成；父线程独立复核及索引更新待执行）**。没有缺失的实施门禁。

## 交付来源与边界

- Worktree：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-b0631c43`。
- Branch：`advisor/018-tree-cleanup`，从本 worktree 原有 `t3/tree-push-cleanup-outcomes` 重命名。
- Base：`b93fc24a631bb94c9e87484a460703f016742cea`。
- 按授权顺序 cherry-pick 以下四项，均 exit 0、无冲突；没有引入重复 008 祖先副本。

| 来源提交 | 本分支结果提交 | 内容 |
|---|---|---|
| `054270810044ae60b52225e8a43c9c6a97fb7a5e` | `068e35e71b9b3bda9fc9d783a964a83b2d7be82c` | 008 非阻塞打开与同 FD 文件类型检查 |
| `fba80816a4a38fcb049c64bd2504106fe24462a1` | `be747c9e8a9b47e8d97a29b0a8f58f25a9b54d54` | 008 超过 128 次 FIFO 拒绝与文档限定 |
| `583a9f719047f38453d91c946862b8a80d466740` | `225d4579ebffd90e9cb6ecab7f8ff9daf71be9ad` | 015 自身增量，SCHILY literal xattr 名称 |
| `314c4f7e1599220c3a0b6699bd77f9d1ab9c5533` | `9ea1ea2be2066bd31448ada8cf49cf4ad26fd78b` | 019 自身增量，unlinked archive FD、双 UID required runner |

018 的 **inherited tip** 为 `9ea1ea2be2066bd31448ada8cf49cf4ad26fd78b`。018 实现提交标题为 `guest: report committed tree cleanup outcomes explicitly`，精确 hash 由交付回复报告。独立复核应从该 inherited tip 比较，而不是把继承的 008/015/019 误计入 018 增量。

018 只修改 `guest/agent/src/fs.rs`、`guest/agent/tests/fs_identity.rs`、`scripts/run-guest-fs-isolation.sh`、`guest/README.md`、`protocol/README.md` 和本文。`guest/agent/tests/tree.rs` 保留继承的 015 内容，没有 018 增量。Cargo.toml、Cargo.lock、Makefile、协议 schema、父线程计划索引均未修改。

## RED → GREEN

先在 inherited tip 上增加真实 RPC 的混合 owner 场景和 runner 的通用枚举比对，尚未改变生产 cleanup/commit 实现。实际运行：

```sh
CARGO_NET_OFFLINE=true make guest-fs-isolation-e2e
```

make exit **2**，Rust suite exit **101**：原有三场景 PASS，新场景 FAIL（3 PASS / 1 FAIL / 0 ignored）。新场景在完成独立 workload 权限证明后得到成功的 tree_push terminal，断言失败文本为：

```text
mixed-owner admission RED: tree committed instead of rejecting
```

旧树由自建 workload-owned 0700 目标根及另一个 UID 拥有的 0555 非空子目录组成，归档为普通新文件。此 RED 证明旧实现允许该树提交，没有将后续未执行的断言宣称为旧实现已验证结果。中间 RED 状态没有单独提交；运行证据保存在本次会话 `/tmp/plan018-red.log`，无需该临时日志即可从本文重建场景。

实现后 required runner 实际得到 **7 PASS / 0 failed / 0 ignored / 0 filtered out**，并输出：

```text
required dual-UID scenarios: 7 actual PASS; no skips
```

| required 名称 | 实际结果与覆盖 |
|---|---|
| `identity_fixture_proves_distinct_authorities` | PASS；daemon UID/GID 0、workload 非零身份与私有 fixture 拒绝，真实 Workload RPC |
| `tree_pull_authority_stays_on_created_descriptor` | PASS；unlinked 原 archive FD、symlink/FIFO/regular 替换名称、授权输出及 FD 关闭 |
| `tree_pull_descriptor_cleanup_and_cancellation` | PASS；空/大树、rewind、ledger 排除、断流、错误与 permit/FD 清理 |
| `tree_push_mixed_owner_rejected_before_commit` | PASS；外 UID 0555 目录在提交前拒绝，目标 inode/内容/目录 owner/mode 不变，无 staging 残留；将该 fixture 目录改为 workload-owned 0555 后正常成功 |
| `tree_push_postcommit_cleanup_outcome_is_explicit` | PASS；仅在发布 fsync 成功后注入 cleanup EIO，成功帧包含 committed/durable=true、cleanup_complete=false/required=true、固定原因及父目录 basename；新树已发布，旧树 inode/内容可定位；不同目标不触发，同目标后续操作不重复触发，也不收集上次证据 |
| `tree_push_postcommit_sync_failure_remains_error` | PASS；发布目录 fsync EIO 保持非空 error，同时保留 committed=true/durable=false；existing 旧树 inode 和 0555 mode 保持，证明未做 cleanup；新目标无旧树时正确报告 cleanup_complete=true；等待 RPC 完成后 inode 不再变化，没有第二次提交 |
| `tree_push_cleanup_success_is_reported` | PASS；普通新树、空树、同 UID 只读目录、外 UID 只读普通文件、other/group/ACL 授权的外 UID 可写目录、传入 workload-owned mode000 目录；成功状态准确且目标父目录没有 staging 残留 |

所有新增验证均通过 `serve_connection` / `Portal::Workload` / protobuf Frame / UnixStream 和真实文件系统执行；没有新增 helper function 单测。原十项 tree RPC 回归继续通过，包括 restricted directory modes、zero permissions、hardlinks、断流回滚，以及继承 015 的两项 xattr 名称/冲突验证。继承 008 的 `filesystem_fifo_rejection_releases_rpc_slots` 在默认与 feature suite 中通过。

## 提交与清理语义

`TreePushOutcome` 是只在 rename 成功后才能产生的内部结果类型；普通 `Err` 对应 precommit。`TreeCleanup` 用 `Complete` 或带 basename/固定原因的 `Pending` 记录实际清理结果。类型的不变量表达 committed=true，发布目录 fsync 和 cleanup 独立记录。

- 正常成功：`committed:true, publication_durable:true, cleanup_complete:true, cleanup_required:false`。
- 发布已持久化、旧树 cleanup 失败：仍为成功 terminal，附 `cleanup_complete:false, cleanup_required:true, cleanup_name, cleanup_reason:"old_tree_cleanup_failed"`。实际 cleanup 可能已部分删除，不能把剩余目录称为完整备份。
- rename 后发布目录 fsync 失败：直接构造带原 Struct params 的 terminal，设置非空 `Frame.error`，再调用 `out.send`。不返回普通 Err 交给通用 failure 转换。existing tree 不尝试 cleanup，保留完整旧 stage，原因是 `publication_sync_failed_cleanup_not_attempted`；new tree 没有旧 stage，故 cleanup 已完成。旧客户端仍从 error 判定失败，新客户端同时知道已经提交。
- precommit 错误保留原错误，显式尝试恢复 staging 目录访问并删除，目标不变。即使 staging cleanup 本身失败，也不覆写原 precommit 错误。

stage 创建后立即 `keep()`，解除 `TempDir::Drop` 的隐式删除责任。后续只对 pinned target-parent 下的该随机 basename 恢复必要目录权限并显式 `remove_dir_all`，检查返回值。生产路径全部在现有 workload filesystem identity 的 blocking 作用域内，没有 root chmod/remove、回交换、自动重传、常驻 GC 或持久账本。

准入发生在复制旧树之前，以 `O_PATH` / `O_NOFOLLOW` 获取的目录 FD 遍历，保留原树深度/条目上限及取消检查。只在目录实际缺少 cleanup 访问权时要求该目录属于 workload fsuid；不检查普通文件 owner。访问查询使用 Linux `faccessat2(AT_EMPTY_PATH | AT_EACCESS)`，按当前 fsuid/fsgid 及 ACL 判断，避免按 mode 位猜测而误拒绝 group/ACL 已授权的目录。该调用在本次双 UID 的实际运行中既拒绝了外 owner 只读目录，也接受了 group/ACL 可写目录。若内核缺少该调用或查询失败，提交前返回错误；此 Linux API 前提已写入 guest 文档，没有不安全的权限猜测回退。

故障设施复用 019 默认关闭的 `filesystem-fixtures` 模块及 scratch 创建方式：`PushFixture` 持有新建私有 scratch；`PushOperation` 只匹配其 `tree` 目标并核对 pinned 父目录 device/inode；`PushFailure` 只选择 publication sync 或 cleanup 位置的一次 EIO。注册由精确匹配的一次调用消费。所有注册/取用/故障调用均在 feature cfg 内，默认构建不含此设施；没有 RPC 参数或运行环境变量开关。

## Runner reconciliation 与实际权限证明

按已批准 reconciliation，复用唯一的 `scripts/run-guest-fs-isolation.sh`。只修改场景枚举及结果检查：从本次编译选出的 binary 读取 `--list`，七个名称作为 required 下限，实际 PASS 集合必须与全部编译场景一一相等；零测试、缺项、重复、ignored、SKIP 和过滤运行均不能通过。没有把检查固定为“恰好七项”，未来新编译场景也必须执行。

构建 JSON 选唯一 executable、复制 fresh binary、fresh user/mount/PID namespace、tmpfs、UID/GID mapping、capability 验证、15 秒子进程截止、90 秒 suite 截止及自建资源清理约束全部保留，没有另建 runner。

本次 required 运行实际证明：daemon UID/GID=0，DAC_OVERRIDE/SETGID/SETUID/SYS_ADMIN effective；uid_map `0 -> 1000 (1), 1 -> 100000 (65536)`，gid_map `0 -> 100 (1), 1 -> 100000 (65536)`。独立 exec 的 workload real/effective/saved UID/GID=1，补充组为空，CapInh/Prm/Eff/Bnd/Amb 全零，读取自建 root-private fixture 得到 EACCES，授权文件可读，无 sentinel FD。测试数据仅存在本次 namespace 的新 tmpfs，未读取实际凭据或系统私有数据。

## 最终门禁

所有下列命令最终 exit **0**。Cargo 使用已缓存依赖并设置 offline，不安装依赖；Go integration 禁止模块网络获取。代码最后修改后已完成这些门禁；随后只补充本文。

| 命令 | 结果 |
|---|---|
| `CARGO_NET_OFFLINE=true cargo test --locked -p sandbox-agent --features filesystem-fixtures --test fs_identity -- --list` | 7 tests / 0 benchmarks，四个新增名称和三个继承名称齐全 |
| `CARGO_NET_OFFLINE=true make guest-fs-isolation-e2e` | 7 个 required actual PASS，无 SKIP/ignored/filter |
| `CARGO_NET_OFFLINE=true cargo test --locked -p sandbox-agent --test tree` | 10 PASS |
| `CARGO_NET_OFFLINE=true cargo test --locked --workspace --all-targets` | 32 PASS，0 failed/ignored；默认 fs_identity=0，不能代替 required runner |
| `cargo fmt --all -- --check` | PASS；包含 feature 代码的完整格式检查 |
| `CARGO_NET_OFFLINE=true cargo clippy --locked --workspace --all-targets -- -D warnings` | PASS |
| `CARGO_NET_OFFLINE=true cargo test --locked -p sandbox-agent --features filesystem-fixtures --test tree` | 10 PASS |
| `CARGO_NET_OFFLINE=true cargo test --locked --workspace --features filesystem-fixtures --all-targets` | PASS；普通 Cargo 中 7 个 namespace 场景只走非 required 的提示分支，其 actual PASS 仅计入上面的 required runner |
| `CARGO_NET_OFFLINE=true cargo clippy --locked -p sandbox-agent --features filesystem-fixtures --all-targets -- -D warnings` | PASS |
| `CARGO_NET_OFFLINE=true GOPROXY=off GOSUMDB=off GOFLAGS=-v make integration` | 真实重新构建 unix_fixture，4 actual PASS，无 SKIP |
| `bash -n scripts/run-guest-fs-isolation.sh` | PASS |
| `git diff --check` | PASS |

integration 四项分别为 `TestRustAgentThroughSandboxRelay`、`TestRustKeyedExecReplayThroughSandboxRelay`、`TestRustRawForwardHalfClose`、`TestRustPreviewUsesRawForward`，均由本次 verbose 输出核对，不以构建或 cache 命中代替实际运行。

中途一次 default clippy exit 101：feature 代码被移除后，cleanup 闭包只剩单行调用，触发 redundant_closure_call。已改为 cfg 选择 fault result 后用 `and_then` 执行显式删除，默认与 feature clippy 及相应测试重新通过。没有未解决的编译警告或测试失败。

## Scope、决定与交接

Scope 按 reconciled 018 执行；没有依赖冲突、未批准行为扩展或缺失门禁。具体实施选择是类型本身保证 rename 已发生，以及使用 FD 上的实际 access 查询支持合法可写目录。该查询增加的 `faccessat2` Linux API 前提已明确记录，旧内核不静默降级。mode000 正向场景验证传入目录可发布且不误报 cleanup pending，不声称旧的不可读 mode000 树可以在保持不变的同时被复制。

最终 drift 核对：

```sh
git diff --stat b93fc24..HEAD -- guest/agent/src/fs.rs guest/agent/tests/tree.rs guest/agent/tests/fs_identity.rs scripts/run-guest-fs-isolation.sh guest/README.md protocol/README.md plans/evidence/018-tree-cleanup.md
git diff --stat 9ea1ea2be2066bd31448ada8cf49cf4ad26fd78b..HEAD
git diff 9ea1ea2be2066bd31448ada8cf49cf4ad26fd78b..HEAD
git diff --check
git status --short
```

018 之外的非阻塞打开、SCHILY literal parser、unlinked tree_pull archive FD 均保留。原 caller writer-isolation 前提继续适用；准入不是文件系统快照，也不防并发 writer 改权限。cleanup_complete 是当前调用观察到的删除结果，未额外承诺 cleanup 自身的 crash durability。

没有 parent merge/push/commit/index 或父工作区文件写入，没有安装、host sudo/root 提权、真实 states/`.local-run`/credentials、VM 或嵌套委派。namespace root 只用于该 required runner 的自建 fixture 与权限证明。提交只限本分支。父线程拥有计划索引更新和独立 review；建议复核通过后把 018 标为 DONE，并保留上述 API/并发/崩溃边界。
