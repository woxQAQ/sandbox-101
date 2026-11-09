# Plan 018：明确 tree push 的提交结果与旧目录清理责任

> 按步骤验证，已完成交换后不得假装整体回滚。完成更新索引 018，reviewer 管理时交付证据。Drift check：`git diff --stat b93fc24..HEAD -- guest/agent/src/fs.rs guest/agent/tests/tree.rs guest/agent/tests/fs_identity.rs scripts/run-guest-fs-isolation.sh guest/README.md protocol/README.md plans/evidence/018-tree-cleanup.md`，再核对未提交差异。019 的隔离 fixture/测试专用 feature 为声明依赖，008/015 的其他 fs 改动保留。

## 状态

- Priority：P2；Effort：M；Risk：MED。
- Depends on：019（复用双 UID required runner 和测试专用同步设施）。编号不是执行拓扑；019 先执行。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支复核通过，未合入用户分支）。
- 审计映射：发现 13。

## 问题与目标

tree_push 复制旧树、合并并 RENAME_EXCHANGE 成功后，临时名称指向旧树。若旧树包含其他 UID 的可读但不可写目录，workload 无法 chmod/删除，当前忽略错误仍返回成功，旧完整数据可积累。此场景不需要崩溃或并发 writer。

目标是在提交前拒绝明显无法清理的旧目录，在提交后明确报告“已经提交、清理未完成”的状态，不丢失证据，不扩大 root 删除权。不会引入 guest 常驻 GC 或把用户目录自动交给 root 清理。

## 当前状态与约束

`guest/agent/src/fs.rs:1250`：

```rust
fn cleanup_tree(root: &File) -> Result<()> {
    std::fs::set_permissions(descriptor_path(root),
        std::fs::Permissions::from_mode(0o700))?;
    // 递归处理子目录
```

push_tree 收尾：

```rust
let _ = cleanup_tree(&root);
// After EXCHANGE the staging name contains the old destination tree.
drop(stage);
result
```

`tree.rs` 的 restricted_directory_modes... 测试仅验证同 UID 的只读目录可以恢复 owner 权限清理。README 明确 staging/commit 在 workload 文件系统身份下执行；所有权归一化针对新复制内容，不授予删除任意 owner 旧数据的权限。并发 workload writer 必须由调用者隔离，本计划不新增文件快照保证。

019 提供新 user/mount/PID namespace 内的 UID0 daemon、非零 workload 和无敏感 scratch，required runner 不允许 Skip；测试专用 filesystem-fixtures feature 仅用于可控阶段同步/故障，默认生产构建不含该设施。

## Scope

仅 drift 路径。fs_identity.rs 复用已存在 runner，不再复制新的 namespace 脚本。`scripts/run-guest-fs-isolation.sh` 仅允许将 required 场景枚举/结果比对改为可容纳本项新增四场景的通用检查：保留019三场景为必需下限，从本次编译的 binary 列表核对全部场景实际 PASS，零测试、遗漏、ignored/skip 均失败；不修改构建选择、namespace/UID/capability/截止与自建资源清理约束。protocol/README.md 仅 tree_push 的结果参数说明。

执行前漂移核对（019 实装）：`314c4f7` 的 runner 固定比较恰好三个 required 名称，直接添加018测试会使正确的七场景被 runner 拒绝。因此本项明确允许上述狭义 runner 扩展，避免执行者放宽既有门禁或另建脚本；其余019实现保留。

禁止：自动 root chmod/remove、tar 范围扩展、guest 持久 cleanup daemon/账本、宿主 engine GC、改变 push 的原子交换、用重试自动覆盖已提交目标。

## 目标结果与边界

引入内部提交结果类型，独立记录 rename 是否已发生、发布目录 fsync 是否成功、旧树是否实际清理。普通成功 terminal params 增加 `committed:true, publication_durable:true, cleanup_complete:true`。仅 cleanup 失败、而发布 fsync 已成功时，可返回成功 terminal params `committed:true, publication_durable:true, cleanup_complete:false, cleanup_required:true`，附仅属于该目标父目录的 cleanup 名称和固定脱敏原因，不能自动重传归档。

发布目录 fsync 失败必须保留非空 `Frame.error`，同时用现有 Struct params 携带 `committed:true, publication_durable:false` 以及真实 cleanup_complete/cleanup_required 状态；不能因为 rename 可见就降格为成功。直接构造 terminal reply 后设置 error 并 out.send，不把该类型化结果返回普通 Err 交给 server 的通用 failure 转换，否则 params 会丢失。对这种失败优先保留旧 stage 作为证据，不自动回交换；明确记录尚未清理，不能把已清理或未尝试的状态混称同一个结果。

precommit 错误继续返回原错误帧，保证目标不变。先检验旧树所有需要 chmod 的目录都由 workload fsuid 拥有，且保持 FD 约束；不可清理的混合 owner 树在 exchange 前拒绝，记录为新的显式准入限制。只检查目录，不因其他 UID 的普通文件必然不可 unlink 而扩大拒绝。最终 cleanup 用明确 close/remove 结果，不能依赖 TempDir Drop 忽略错误。检查 error 的旧客户端仍能识别持久化失败，新客户端则同时知道 rename 已发生及证据位置。

## 命令

根目录 Nushell：

```nu
cargo test --locked -p sandbox-agent --test tree
make guest-fs-isolation-e2e
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

双 UID 入口来自 019；缺前提 BLOCKED。新增名称先用 cargo test -- --list 核对。

## 步骤

### 1. 复现混合 owner 残留

在 fs_identity.rs 新增 `tree_push_mixed_owner_rejected_before_commit`：可写目标父目录中，旧树含测试另一 UID 拥有的只读非空子目录；通过真实 tree_push RPC 提交正常归档。目标必须保持原样且不遗留 stage；正向对照是 workload-owned 只读目录仍可成功。全部文件均 fixture 自建，不使用系统目录。

**Verify**：`make guest-fs-isolation-e2e` 中该场景实际执行；旧实现应在“提交前拒绝/无遗留”断言 RED，清理由 fixture 所有者在隔离空间负责，不借助生产 root 路径。

### 2. 实现准入检查与类型化结果

在 push_tree 的旧树归档前，以 pinned descriptors 检查上述可清理性；分别保存 rename、发布 fsync 和 cleanup 的结果，穿过全部返回路径。改 dispatch 发送类型化 terminal frame；precommit 仍 Err，postcommit 不能丢掉提交事实，也不能把发布 fsync 失败从 error 改成成功。显式保留未清理 stage 路径，避免 TempDir Drop 再次悄悄处理；不重新交换回去覆盖可能已被使用的新树。

**Verify**：步骤 1 GREEN；原 restricted_directory_modes、zero_permissions、hardlink、断流回滚行为通过。

### 3. 验证提交后失败和客户端可判定结果

新增 `tree_push_postcommit_cleanup_outcome_is_explicit`：使用 019 的 opt-in 测试专用、按私有 scratch 精确匹配的一次性故障，仅在发布 fsync 已成功后注入 cleanup EIO；通过真实 RPC 验证成功帧带 committed/publication_durable=true、cleanup_complete=false、新树内容和旧树证据。再新增独立的 `tree_push_postcommit_sync_failure_remains_error`：仅注入发布目录 fsync 错误，旧客户端检查 Frame.error 必须失败，新客户端仍读到 committed=true/publication_durable=false 和准确 cleanup 状态，不出现第二次提交。测试设施默认构建不存在，不能成为公共请求开关。

新增 `tree_push_cleanup_success_is_reported` 检查普通成功清理、空树、workload-owned mode000 目录，实际清理成功时不得误报 cleanup pending。更新两份合同文档及 plans/evidence/018-tree-cleanup.md，说明三个独立状态维度与人工处理边界。

**Verify**：

```nu
make guest-fs-isolation-e2e
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
make integration
git diff --check
```

另对 filesystem-fixtures feature 运行 019 规定的 fmt/clippy/测试门禁，确保默认和测试构建都编译。

## 完成标准

- [x] 四个新增 RPC 场景执行，混合 owner 在提交前拒绝，合法只读目录仍可清理。
- [x] rename/发布 fsync/cleanup 状态独立，fsync 失败保留 error；新客户端识别已提交，旧客户端仍能识别持久化失败。
- [x] cleanup 不使用任意 root 权限，失败证据可定位，默认构建无测试后门。
- [x] required 双 UID、Rust门禁、实际 integration、diff/Scope 检查和索引结果完成。

## 实施与复核结果

APPROVE。Worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-b0631c43`，分支 `advisor/018-tree-cleanup`。基准b93fc24，继承008→015→019：`068e35e`→`be747c9`→`225d457`→`9ea1ea2be2066bd31448ada8cf49cf4ad26fd78b`；本项仅 `6c090e81160e59e75280c79ca9f4041d8c0c2343`，整合时不重复祖先。自身六文件Scope符合；runner漂移在派发前已明确reconcile。

父完整读取fs生产/feature增量、四场景真实RPC测试、runner及两份合同/证据。faccessat2按当前filesystem credentials在pinned FD核查目录，实际覆盖foreign/group/ACL可写目录；只需chmod时要求owner，普通外UID文件不扩大拒绝。TempDir明确keep转交责任；发布fsync失败既保留Frame.error又携带committed/durable参数，旧stage不cleanup，cleanup失败仅在发布已持久化时返回成功并给basename，后续push不收集另一调用证据。

父required入口实际7/7 PASS，独立身份证明保持daemon0/workload1、空groups/全capsets零/sentinel EACCES及无继承FD。父重跑默认workspace/alltargets32tests、fmt、默认clippy、feature tree10tests/全targetsclippy、真实Rust fixture integration（0.702s）、shellsyntax与diffcheck均exit0。一次父验证误用不存在guest/Cargo.toml立即exit101，随后从正确根workspace重跑全部通过；不将该失败计为任何验收成功。

RED为旧实现混合owner仍提交，新实现原inode/内容/owner/mode不变且无stage残留。mode000正向覆盖传入的新目录，不宣称可复制不可读旧树；writer隔离、Linux faccessat2前提及cleanup不额外承诺crashdurability保留。证据见该隔离分支plans/evidence/018-tree-cleanup.md；无生产常驻GC/root提权，未合入用户分支。

## STOP 与维护

如果公开客户端明确将任意 EOF 当作 cleanup 完成、需要改变 error wire 格式、或无法保留已经提交的结果状态，先提交合同影响，不擅自升级协议。若清理安全性依赖并发 writer 不修改旧树，必须保留原隔离前提，不能通过事后提权实现。分支 `advisor/018-tree-cleanup`；另获提交授权才使用 `guest: report committed tree cleanup outcomes explicitly`。
