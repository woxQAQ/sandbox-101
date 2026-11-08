# Plan 015：按各自格式解析 SCHILY 与 LIBARCHIVE xattr 名称

> 逐步执行并核对实际测试，完成更新索引 015；reviewer 维护时交付证据。Drift check：`git diff --stat b93fc24..HEAD -- guest/agent/src/fs.rs guest/agent/tests/tree.rs guest/README.md`，再核对未提交差异。008/018/019 的其他文件逻辑须保留，不把本项扩展成 tree 重构。

## 状态

- Priority：P2；Effort：S；Risk：LOW；Depends on：无。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（隔离分支复核通过，未合入用户分支）。
- 审计映射：发现 10。

## 问题与目标

SCHILY 的 xattr 名称后缀是原始名称，LIBARCHIVE 后缀使用 percent encoding。当前合并分支对两者都解码，合法含百分号的 SCHILY 名称被错误拒绝或静默改名。目标只在对应格式解码，保持已有元数据边界、二进制值、重复冲突和原子回滚。

## 当前实现与契约

`guest/agent/src/fs.rs:717`：

```rust
} else if let Some(name) = key
    .strip_prefix("SCHILY.xattr.")
    .or_else(|| key.strip_prefix("LIBARCHIVE.xattr."))
{
    // 两者都校验 %xx
    let name = percent_decode_str(name).decode_utf8()?.into_owned();
```

值的分支已区分 LIBARCHIVE base64 与 SCHILY 原始字节，保留它。`guest/README.md` 目录树合同：支持 UTF-8 user.* 名称最长 255 bytes，单值最长 64 KiB；不支持 privileged namespace、ACL、symlink xattrs；冲突表示必须失败。`engine/fsmeta.go:508` 和 image_compat.go 的独立 OCI SCHILY 解析直接保留原始后缀，可作为语义交叉证据，但不要修改 engine。

测试范例 `guest/agent/tests/tree.rs:586` 的 `restricted_directory_modes_and_equivalent_xattrs_are_applied_after_contents`，通过实际 tree_push RPC 与文件系统读取属性。archive/header/regular/xattr helpers 已有；不新增独立 parser 单测。

## Scope

允许：`guest/agent/src/fs.rs` 的 PAX xattr 名称处理，`guest/agent/tests/tree.rs`，`guest/README.md`，索引状态。

禁止：tar crate、依赖、engine、权限提升、放宽名称/长度/namespace 限制、改变 pull 的 LIBARCHIVE 编码格式。

## 命令

根目录 Nushell，现有：

```nu
cargo test --locked -p sandbox-agent --test tree
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
make integration
```

新增测试先 `cargo test --locked -p sandbox-agent --test tree -- --list` 确认名称。缺 xattr 支持时明确 BLOCKED，不能把断言换成 ignore。

## 步骤

### 1. 以 RPC 证明属性名保持原样

新增 `schily_xattr_names_preserve_literal_percent`：导入合法原始百分号名称及形似转义序列的名称，实际 getxattr/listxattr 必须与输入名称字节一致；旧代码错误拒绝/改名应 RED。测试必须检查没有额外产生解码后别名，目标内容和属性值完整。

**Verify**：

```nu
cargo test --locked -p sandbox-agent --test tree schily_xattr_names_preserve_literal_percent -- --exact --nocapture
```

预期修复前失败、正常退出。

### 2. 先区分格式再统一验证

明确识别 SCHILY 与 LIBARCHIVE：前者保留原始 UTF-8 后缀，后者保持 encoded length、合法 percent escape 和 UTF-8 解码校验。然后统一验证实际名称 user.*、非空后缀、<=255 bytes、无 NUL；值的原始/base64 处理不变。按最终实际名称检测不同格式的重复，同值允许、异值拒绝。

**Verify**：上一步 GREEN，原 `restricted_directory_modes_and_equivalent_xattrs_are_applied_after_contents` 继续通过。

### 3. 覆盖等价、冲突与边界回滚

新增 `xattr_format_equivalence_and_conflicts_preserve_names`：同一带百分号属性用两种格式表达，相同值成功、冲突值失败；不同字面名称即使形似编码也不能合并。覆盖 255/256-byte 实际名称、LIBARCHIVE 非法 escape、非 user.* 拒绝，失败后旧目标内容/属性未改变、无 staging 残留。

**Verify**：

```nu
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
make integration
git diff --check
```

预期全部通过。README 明确两格式名称和值的差异，不承诺新的 tar 扩展支持。

## 完成标准

- [x] 两个新增 tree RPC 测试实际执行，字面名称和二进制值保持。
- [x] 等价格式去重正确，冲突/超限回滚，pull 原编码不变。
- [x] Rust 全门禁、实际 integration、diff check 通过；Scope 和索引结果完成。

## 实施与复核结果

APPROVE。worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-d8a72b8e`，分支 `t3/preserve-schily-xattr-names`；源基准 `b93fc24`，继承008为 `edfbe34`→`d483a07`，本项提交 `583a9f719047f38453d91c946862b8a80d466740`。组合到已有008的分支时只接本项提交，勿重复接008副本。

执行者记录旧解析对字面百分号名称返回 invalid xattr name encoding，真实 tree_push 回归 RED→GREEN。父 reviewer 阅读本项完整三文件 diff、确认仅 LIBARCHIVE 执行 percent/UTF-8 解码及长度限制，最终名称共同检查 namespace/255 bytes/NUL，值与冲突路径保持。两个真实 RPC 测试覆盖字面名/无别名、二进制值、两格式同名同值与冲突、255/256-byte、非法 escape/namespace、原内容与属性回滚及无 staging 残留。父重跑 Rust workspace 32 tests、fmt/clippy、真实 fixture integration（0.703s）、diff check，全 exit 0；未修改 tar 依赖、pull 编码或用户分支。

## STOP 与维护

如果 tar crate 在进入此分支前已经转换了 key（与当前读取事实不同），或需接受非 UTF-8／privileged xattr，STOP 重新确认合同。后续增加格式不能把未经同一编码的名称合并解码。分支 `advisor/015-xattr-names`；另获提交授权才使用 `guest: preserve literal SCHILY xattr names`。
