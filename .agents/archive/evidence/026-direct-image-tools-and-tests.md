# 026：直接镜像工具与测试整理证据

归档说明：实施已完成，按用户后续归档指令与计划一同归档。以下验证结果及源码身份保持原记录；归档不表示缺项已通过。剩余验收见 [未完成索引](../../../plans/README.md#待完成验收)。

状态：**IMPLEMENTED / VALIDATION_BLOCKED**。工作基于未提交树，HEAD 为
`6162397891e0e454fd2f6b3fc511c11d004fde5b`；没有提交、推送、部署、旧库迁移或真实 VM 执行。
025 的已有修改继续保留，其历史通过不作为本次通过。

## 实现边界

- `loom/host/fsmeta_tools.go` 直接解析 PATH 中的工具，使用真实宿主路径、私有工作目录、仅含 PATH/LC_ALL 的子进程环境和有界错误展示。镜像路径不选择可执行文件。
- 删除镜像 bwrap/unshare 包装、虚拟挂载描述、tool-root 环境变量和对应 Nix/CI 配置。其他运行时与 kernel/capture runner 的 namespace 机制没有因此改变。
- layered 使用原 tar/digest/路径/字节与条目验证，以及共享 EROFS metadata composition。profile 更新为 `erofs-shared-oci-v2;host-tools`。
- merged 保留 umoci 0.6.0 与实际 ownership/xattr 语义，要求 effective UID 0；在 pending 输出下创建私有 `.unpack-*`，以子进程 umask 077 解包，mkfs 完成后清理，再提取 boot。profile 更新为 `umoci-native-merged-v3`，不再使用 tmpfs/user namespace 或 rootless owner 替换。输入限制与费用预留保留，解包使用宿主 backing filesystem。
- 不可变布局、哈希、只读封存、fsync/rename/SQLite 发布和原 witness 留责顺序保持。执行语义的变化使用新的 builder/cache 身份，不重解释旧产物。
- 应用组装的四个生产文件进入 `haven/internal/application`；controller 仍直接接收用例服务。Haven 根目录现在没有 Go 文件。
- `haven/tests/component` 有 75 个 Go 文件，`haven/tests/native` 有 12 个；`haven/internal/testfixture` 共享 5 个 fixture 文件。组件测试继续使用真实 SQLite/文件系统和显式效果注入，生命周期并发槽仍为 4。KVM 保留原 build tag。
- 架构门禁禁止生产导入 testfixture、controller/service 反向导入 application，以及恢复根 facade。相应正负例通过。
- `TestActualOCIRejectsInvalidToolRoots` 的旧工具根契约随实现删除，改为 `TestActualOCIUsesPrivateWorkDirectory`；原 `unmapped owner` 用例改检查 Linux 无效 UID，取消 root 身份 skip 特例。native gate 仍要求原四项实际 pass，改为 `tests/native` 包，negative 改隐藏 mkfs.erofs。

## 最终源码与环境

`build/026-direct-image-tools/native-gate-source-{before,after}.json` 完全相同；
最终普通入口的 `component-final-source-{before,after}.json` 也完全相同。
570 个列明的 source/config/build/validation 输入 SHA256：

```text
a31b329c9e1f9aaf62aac6647daca9cb76da837007211847b742aab80cfe0635
```

该清单排除文档、build、references、third_party 和运行状态；Nix flake/lock 的
独立文件哈希及调用者 euid 记录在 `environment-inputs.json`。原生工具通过项目
`nix develop --no-write-lock-file` 提供，版本为 mkfs/fsck.erofs 1.9.4、umoci 0.6.0，
见 `native-tools.log`。没有修改系统配置或提升当前调用者权限。

## 验证结果

日志统一位于 `build/026-direct-image-tools/`，`.exit` 保留命令状态；最终 native
门禁完整记录在 `build/native-gate/run.75y_n_ez/`。

| 命令/边界 | 结果 |
|---|---|
| `python3 -I scripts/check-architecture.py` 与 `scripts/test_architecture.py` | PASS：实际图/AST 与正负例 |
| `python3 -I scripts/check-sql.py` 与 `scripts/test_sql_generation.py` | PASS：生成一致性与负例 |
| `python3 -I scripts/test_native_gate.py` | PASS：8 项公共 CLI 回归，含真实 just JSON 保留与精确 missing-mkfs 诊断 |
| `python3 -I scripts/test_runner_environment.py` | PASS：10 项 runner 回归，KVM 包路径与直接工具 PATH 同步 |
| `just check` | PASS：各 module vet、Rust fmt/clippy |
| `golangci-lint run ./haven/... ./loom/...` | PASS：0 issues |
| `GOWORK=off go -C haven test -tags=integration,kvm,privileged ./... -run '^$'` | PASS：各 tag 编译；没有 VM 执行 |
| `just integration` | PASS：真实 Rust Unix fixture，0.366s |
| `just smoke` | PASS：8 组真实 daemon/CLI/离线备份场景 |
| 冻结模型比较 | PASS：51 个模型的字段顺序、类型及 JSON tags 不变 |
| `GOWORK=off go -C loom test -json -race ./... -count=1 -timeout=120s` | PASS：27 项 |
| 最终 `GOWORK=off go -C haven test -json ./... -count=1 -timeout=120s` | FAIL：522 pass、35 skip；component 包在 120.011s 超时 |
| Nix 环境的六项实际 layered/共享/GC 定向 race | PASS：6 项，3.199s，无 bwrap/user namespace |
| Nix 环境 `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C haven test -json -race ./tests/native -count=1 -timeout=300s` | FAIL：73 pass、15 fail、0 skip，16.400s |
| Nix 环境 `python3 -I scripts/check-native-gate.py --negative` | PASS：`run.cmumo0rg`，真实 selected test 因缺 mkfs.erofs 失败 |
| Nix 环境 `python3 -I scripts/check-native-gate.py --preflight` | BLOCKED/实际 FAIL：`run.ah1rz2lt`，当前非 root，没有自动 sudo |
| Nix 环境 `python3 -I scripts/check-native-gate.py` | FAIL：3631 条 JSON，691 pass、15 fail、0 skip；component 537 项全部通过，286.590s；native 73 pass/15 fail，21.243s |

六项定向测试为 `TestActualLayeredEROFSImportAndExtraction`、
`TestActualLayerCacheReuseRebuildAndCorruption`、`TestActualSharedOCIEntryOrderingAndForwardLinks`、
`TestActualSharedOCIRootOpaqueAndReplacement`、`TestActualSharedOCIDeterministicCachesAndMetadata`、
`TestGCActualLayerCacheAndCompletedImport`。执行命令使用 `-json -race ./tests/native`、
以上名称的锚定正则、`-count=1 -timeout=300s` 和 `SANDBOX_REQUIRE_EROFS_TESTS=1`。

最终门禁的 15 个 failure events 包含父测试事件：merged 路径明确拒绝非 root，
`TestActualSharedOCIHardlinksAndMetadata` 的实际提取结果缺少预期的
`security.capability`（`no data available`）。断言保留，没有把 required 失败改成 skip。
因此这次已经证明大量直接 layered 路径，但尚未证明 merged 与完整权限元数据路径。
Haven 失败后 `just test-native` 没有继续后续 modules；Loom 的单独 race 见上表。

## 初轮与未运行项

初轮普通入口在 120.009s 超时，592 pass/35 skip；初轮 Haven race 为
657 pass/0 fail/35 skip，component 298.601s。该轮期间进一步收紧了工具子进程环境和
错误展示，`source-inputs-before/after.json` 不同，因此不把这一轮称为最终树全量通过。
最终源码稳定的 required-native 命令已经重新运行全部 Haven 组件 race，最终普通入口
也重新验证；上述两项最终结果才用于当前状态。

迁移中的编译问题及首次 lint 的 errcheck/gci 问题保留在 `compile-move.log`、
`compile-suites.log`、`lint.log`，最终编译/lint 为通过。

普通 120 秒预算没有增加，native 仍为 300 秒、race 仍为 10 分钟；没有扩大生命周期
并发槽或删除失败断言。KVM、kernel-isolation、真实 merged root 验收与 production
未运行。CI 配置更新不代表 GitHub 已执行；计划保持 VALIDATION_BLOCKED。

收尾时 `git diff --check` 与变更 Markdown 的本地链接检查通过；
`source-verified.json` 与最终 native/普通入口的源码清单完全相同。任务测试进程已结束。
