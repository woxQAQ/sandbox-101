# Plan 019：独立验证 tree_pull 的双 UID 文件描述符边界

> 本计划从调查开始，不把静态风险写成已证实漏洞。逐步验证、保存无敏感哨兵证据；完成更新索引 019，reviewer 管理时交付结论。Drift check：`git diff --stat b93fc24..HEAD -- guest/agent/Cargo.toml guest/agent/src/fs.rs guest/agent/tests/tree.rs guest/agent/tests/fs_identity.rs scripts/run-guest-fs-isolation.sh Makefile guest/README.md plans/evidence/019-fs-identity.md`，再核对未提交差异。008/015 已落地时保留其非阻塞打开和格式解析。

## 状态

- Priority：P1（调查）；Effort：M；Risk：LOW；Depends on：无。
- Category：security / investigation；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支调查/实现复核通过；未确认泄露）。
- 审计映射：单列 MED 置信度调查项。018 复用本计划创建的双 UID harness。

## 问题与目标

tree_pull 创建归档时借用 workload filesystem identity，最终 streaming 却以 daemon 身份按名字重开。现有同 UID fixture 无法验证生产 root daemon/非 root workload 的边界。需要以隔离 namespace 中的两个 UID 验证是否能读取与创建时授权不同的对象，并优先让归档全程沿创建时取得的 FD 传递。

调查只使用测试拥有的目录和无敏感哨兵文件，禁止访问真实 root 文件或运行针对真实凭据的读取／复现程序。输出区分“已观察到权限边界失败”“只观察到结构缺口”“反证／环境阻断”，不把非确定性 race 未触发当安全证明。

## 当前状态与约束

`guest/agent/src/fs.rs`：

```rust
let archive = access(identity, || Ok(tempfile::NamedTempFile::new()?)).await?;
let archive_path = archive.path().to_owned();
// 生成归档时在 workload 身份按 archive_path 写入
let mut file = tokio::fs::File::open(archive.path()).await?;
```

`guest/agent/tests/tree.rs` setup：

```rust
uid: unsafe { libc::geteuid() },
gid: unsafe { libc::getegid() },
```

with_identity 只改变 blocking worker 的 fsuid/fsgid；普通 async task 仍是 daemon 身份。打开时以 workload 授权取得的 FD 后续异步读取本身是正确模型；问题仅针对按路径重新取得权限，不能扩大成“所有文件 RPC 都 root 读取”。cwd/降权顺序的静态疑问也不作为本计划已确认漏洞。

已有隔离入口范例 `scripts/run-capture-isolation-acceptance.sh` 用 `unshare --user --map-auto --map-root-user --mount --pid --fork --kill-child=KILL --mount-proc --propagation private`，证明新 namespace 后才运行测试。guest 的 portal Workload/Admin/System 分离必须保留。

## Scope

允许：上述 drift 路径；新增 fs_identity.rs、run-guest-fs-isolation.sh 以及 `make guest-fs-isolation-e2e`；证据文档只在 plans/evidence/019-fs-identity.md。Cargo.toml 仅新增默认关闭的 `filesystem-fixtures = []` feature，不加依赖或修改锁文件。fs.rs 的同步/故障设施必须由 `#[cfg(feature = "filesystem-fixtures")]` 包围，默认生产构建不存在；不使用库内 cfg(test) 向外部 integration test 暴露 hook，因为 cargo 的集成测试依赖库不带该 cfg。该 feature 仅允许私有 scratch、一次性注册和按操作匹配的测试调度，不暴露 RPC 参数或生产环境变量开关。

禁止：生产 AF_VSOCK、UID/GID 默认值、server 权限、ledger、root 特权范围、真实状态、当前 NixOS 配置、sudo、修改现有 capture runner。其他文件操作和 cwd 改动不在此计划。

## 命令与前提

根目录 Nushell，现有：

```nu
cargo test --locked -p sandbox-agent --test tree
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

实际双 UID 入口是本计划新建的 `make guest-fs-isolation-e2e`，不是现有命令。缺 subordinate UID/GID、unshare、可用 /proc 时必须非零失败，记录 BLOCKED，不把当前 UID 分别写两遍冒充隔离。

## 步骤

### 1. 建立受控双 UID RPC harness

fs_identity.rs 使用独立测试子进程。runner 先在正常用户环境以 `cargo test --locked -p sandbox-agent --features filesystem-fixtures --test fs_identity --no-run --message-format=json` 编译并从 compiler-artifact JSON 取测试可执行文件，复制到自己新建、可遍历且不位于私人 home 祖先目录下的 scratch，再进入新 user/mount/PID namespace 执行该二进制。namespace UID 0 启动 daemon fixture，workload 为映射的非零 UID/GID（例如 1）；测试父进程验证 namespace 与父环境不同，workload 子进程验证 real/effective UID/GID 均为非零、空补充组、无相关 capabilities。测试 worker 不得继承 root 的已打开哨兵 FD。普通 cargo test 下不适用的特权子场景可显式说明，但 required runner 必须全部实际执行且不可 Skip。

fixture 经实际 Workload serve_connection 运行 tree_pull，目录/哨兵均来自新建 scratch；root-private sentinel 只含随机测试字节，正常 workload 不能读取。同步用上述 opt-in feature 内的一次性 gate 控制，不能依赖大规模 race 循环或生产环境变量。harness 父进程硬截止并在退出清理全部自建资源；runner 要枚举并确认所需测试都有实际 PASS，不能只相信 test binary 的 exit 0。

**Verify**：新增 `identity_fixture_proves_distinct_authorities` 先断言 namespace、UID、文件权限边界；`make guest-fs-isolation-e2e` 输出该场景实际 PASS。不能取得前提就 BLOCKED，停止权限结论。

### 2. 调查路径重开与归档生命周期

新增 `tree_pull_authority_stays_on_created_descriptor`：在测试控制的临时名称变化场景下，断言输出只能来自最初允许的归档 FD，私有 sentinel 从未出现在 output，且不能因替换对象类型而阻塞。用正向对照证明正常 authorized tree 可以拉取；同 UID 测试不是替代。

记录原实现结果：若观察到读取错误对象/跨权限或阻塞，保留 RED 断言与精确阶段；未触发时记录控制条件和限制，不能写“漏洞不存在”。不输出任何系统敏感值。

**Verify**：required harness 有可复核结果，证据文档记录测试名、UID/mapping、阶段和 outcome；旧实现不满足 FD 不变量时应 RED，而不是依赖私有数据泄露才算失败。

### 3. 收敛为无路径重开的归档流

在 fs.rs 以 workload identity 创建匿名 tempfile 或立即 unlink 的临时 File，将创建时 FD 的 clone 交给 archive_tree，完成后显式 seek 到 0，再用 tokio::fs::File::from_std 读取同一授权文件。clone 可能共享文件 offset，必须在写完成之后 seek；不并发读写归档。临时文件归属、大小预算、断连、错误清理、backpressure 和 ledger 排除保持原契约，不改变 tar 内容。

若因平台/tempfile API 不能保留授权 FD，则 STOP，不以 canonicalize/lstat 再重开代替。即便未观察到实际权限泄露，这个改动可作为已验证的 FD 生命周期简化交付；证据中仍不能声称修复了已运行复现的泄露。

**Verify**：

```nu
make guest-fs-isolation-e2e
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -p sandbox-agent --features filesystem-fixtures --test tree
cargo clippy --locked -p sandbox-agent --features filesystem-fixtures --all-targets -- -D warnings
make integration
git diff --check
```

新增 `tree_pull_descriptor_cleanup_and_cancellation` 覆盖读 offset、断连、空目录、大于 CHUNK 的归档、私有 ledger 排除；required UID 场景必须实际执行。普通集成 fixture 也必须运行。

## 完成标准

- [x] 三个 tree/identity 场景和 namespace 身份正向证明实际执行，无 required skip。
- [x] 证据区分实际观察、结构性不变量与未验证项；无真实敏感文件访问。
- [x] 归档生成/传输只用创建时 FD，写完 seek 后读取，失败/取消不残留资源。
- [x] Rust 全门禁、实际 integration、diff check 和 Scope 检查通过，索引准确。
- [x] 缺隔离前提或调查未完成标 BLOCKED，不以普通测试替代。

## 实施与复核结果

APPROVE，调查限定保持。worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-a9563b37`，分支 `advisor/019-guest-fs-identity`；基准 `b93fc24`，008 继承为 `3dc7d12`→`89edd5c`，本项提交 `314c4f7e1599220c3a0b6699bd77f9d1ab9c5533`。组合到已有008分支只取本项，015另行合并；不得丢其 xattr 解析。

父 reviewer 阅读全部生产/feature增量、runner、580行双 UID 生命周期 fixture 及[调查证据](/home/woxQAQ/.t3/worktrees/sandbox-101/t3-a9563b37/plans/evidence/019-fs-identity.md)，并实际重跑 required runner：三场景 0.25s 全 PASS，无 skip，真实 namespace-root daemon 与 real/effective/saved UID/GID1、空组/五组capabilities为零的 workload 已验证。sentinel仅自建随机无敏感测试数据；原源码仅以 nlink=1 取得结构性 RED，未观察到泄露/阻塞，不能据此声称生产权限泄露被复现。

新实现创建后 unlink，克隆 FD 归档，完成后 rewind 原 FD 再流式读取；测试将旧名称替换为 symlink/FIFO/普通文件仍只输出授权归档，并验证空树/大于CHUNK/ledger排除/断连和FD、permit回收。父另跑普通 workspace30 tests、默认与feature clippy、feature tree8 tests、fmt、实际Rust fixture integration（0.701s）、runner语法和diff检查，全 exit0。测试设施默认构建不存在，无生产RPC/env开关。018可接续，未合入用户分支。

## STOP 与维护

需要 sudo、宽松化 VMM/agent 权限、操作现有 namespace、引入生产路径替换开关或读取真实秘密时停止。后续任何临时文件重开都应审查创建者与重开者身份是否相同；优先传 FD。分支 `advisor/019-guest-fs-identity`；另获提交授权才使用 `guest: retain authorized descriptors through tree pull`。
