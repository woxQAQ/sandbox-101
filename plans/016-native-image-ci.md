# Plan 016：将原生 OCI/EROFS 验证与 lint 工具链纳入持续门禁

> 逐步实施并保留真实 CI/本地日志；完成更新索引 016，reviewer 管理时交付证据。Drift check：`git diff --stat b93fc24..HEAD -- .github/workflows/check.yml Makefile flake.nix README.md engine/IMAGE-MATERIALIZATION.md scripts/check-native-gate.nu`，同时检查未提交差异。此计划不授权修改当前宿主配置或运行安装命令。

## 状态

- Priority：P2；Effort：M；Risk：LOW；Depends on：无。
- Category：tests / dx；Planned at：`b93fc24`，2026-11-06；Status：BLOCKED（实现复核通过；真实CI/root路径及完整Nix shell未验收）。
- 审计映射：发现 11。后续 OCI/EROFS 修改应以本门禁为前置。

## 问题与目标

根 CI 只安装 protoc，普通 make test 在缺少 bwrap/erofs/umoci 时可以把主要镜像测试全部 Skip。已有强制环境变量未用于 CI，单次手工原生验收不能提供持续覆盖。Nix devShell 还缺 make lint 所需 golangci-lint。

目标：独立、可重现的 native-image job 必须实际执行共享与 merged materialization、原生 GC 测试；缺工具/namespace/mapping 明确失败。普通快速测试保持原入口，不把 KVM/特权生产验收混入此门禁。

## 当前状态与合同

`.github/workflows/check.yml`：

```yaml
- run: sudo apt-get update && sudo apt-get install -y protobuf-compiler
- run: make test check test-race integration smoke
```

`engine/fsmeta_test.go`：

```go
if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" { t.Fatal(err) }
t.Skipf("actual EROFS tool test unavailable: %v", err)
```

`Makefile` 已有 test/check/lint/integration；`flake.nix` pinned nixpkgs，devShell 含 erofs-utils/e2fsprogs/umoci/bubblewrap 等，未包含 golangci-lint。`engine/IMAGE-MATERIALIZATION.md` 要求匹配 mkfs/fsck >=1.9.4 且 <2、umoci=0.6.0，以及可用 user/mount namespace 和映射。两套格式均不可用时不能改用另一套通过。

实际已确认：普通 make test/check/lint/integration 在审计机器通过；TestActual 全部因工具缺失 Skip。当前本机未准备完整原生前提，不能宣称已验证 CI 方案。

## Scope

允许：drift check 路径；新增 `scripts/check-native-gate.nu`（验证命令包装，不生成源代码），索引状态。

禁止：engine 实现/测试语义、降低 required 环境变量、升级锁定 nixpkgs/flake.lock、修改当前 NixOS、放宽恢复/COW、改动依赖。CI 所需安装只能写在工作流里并在其一次性 runner 执行，不在用户机器上执行。

## 目标入口

新增 `make test-native`：单一明确命令执行 `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test -json -race ./... -count=1 -timeout=180s`，Make recipe 前加 `@`，stdout 仅保留 Go JSON，不混入命令回显。不把环境变量设置藏在调用者 shell 状态。Nix devShell 增加 golangci-lint、nushell、util-linux、coreutils，使 unshare/id 也来自 pinned closure；不引入另一个版本管理器。

native-image job 固定为一次性的 GitHub Ubuntu 24.04 runner，在 pinned Nix shell 中仅 native preflight/test 使用 sudo 切换为 root，再由现有代码在新 namespace 内运行原生工具；这不授权当前用户宿主执行 sudo。只传入 PATH 与 GOTOOLCHAIN=local，不保留 token/全量环境，不全局关闭 AppArmor/namespace 防护。root merged 采用 `--map-users=0:0:4294967295 --map-groups=0:0:4294967295 --keep-caps`，不需要 subordinate 映射助手。唯一允许的 root 专属 skip 为 `TestActualMergedOCIInvalidInputsDoNotPublish/unmapped_owner`，理由必须精确为 root can identity-map all owners；其他 required-native skip 均失败。保留 core/static-guest jobs，lint 仍以普通 runner 用户运行。

## 步骤与验证

### 1. 确认 runner 能力，不把环境失败变成 Skip

选定上述 root CI 模式，不在非 root/root 两条路径间运行时猜测。新增脚本 --preflight 模式（不跑完整 suite），要求 effective UID=0，记录工具版本和源路径，确认 unshare/id 来自 /nix/store；再实际执行：

- shared namespace：用 pinned bwrap 创建全新 user/mount/PID/network namespace，只读绑定 /nix/store、私有 /proc 和 /dev、临时 /tmp，然后运行 pinned `id -u`，预期 0；必须检查 namespace 与外层不同。
- merged mapping：用 pinned unshare 的完整 root identity mapping 参数执行 `nu --no-config-file`，输出 real/effective UID/GID 和 `/proc/self/uid_map`、gid_map，映射起点均 0、长度 4294967295；随后用 `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test -json -run '^TestActualMergedOCIExplicitSelectionAndVerification$' -count=1 -timeout=60s .` 实际走现有 production mapped sandbox/helper，不增加仅版本检查的假 preflight，也不修改测试源码。
- 完整原生共享/merged 场景仍由 required tests 证明；preflight 不替代它们。

**Verify**：在一次性 CI runner 上执行以下 Nushell 可调用命令（其中 bash 仅向 sudo 传递当前 pinned PATH）：

```nu
nix develop --command bash -c 'exec sudo env "PATH=$PATH" GOTOOLCHAIN=local nu scripts/check-native-gate.nu --preflight'
```

这是本计划新增入口，预期版本、身份、实际 namespace 和 mapping 检查均成功。当前用户机器没有明确的隔离 root 执行前提时记录 BLOCKED，不照抄 sudo 到本机。runner 安全限制不满足则失败并报告，不改为 Skip。

### 2. 建立强制入口及失败自检

新增 Make target 和 `scripts/check-native-gate.nu`：脚本只在新建、gitignored 的 build/native-gate 目录存日志，用 Nushell complete 分别保存 test-native 的纯 JSON stdout、stderr 与原始 exit_code；必须同时满足子命令 exit=0 和四个指定测试 pass。分析 Go test JSON，要求 `TestActualLayeredEROFSImportAndExtraction`、`TestActualSharedOCIHardlinksAndMetadata`、`TestActualMergedOCIHardlinksAndMetadata`、`TestGCActualLayerCacheAndCompletedImport` 具有 pass 事件，不能只有包 pass。required-native 的 skip 除上述精确 root 例外全部失败；KVM/capture 等非本 job 范围的其他 test skip 单列记录，不误算原生门禁。

添加负向自检模式：在该临时目录创建仅对本命令有效的工具 PATH 视图，保留必需编译器/基础工具，唯独移除 bwrap；以 -json、required 环境变量执行单个 TestActualLayeredEROFSImportAndExtraction。必须保存实际非零 exit_code，且 JSON/output 包含精确 `required OCI tool missing: bwrap`；编译失败或其他工具缺失的非零不能算正确负向。不要卸载机器工具或修改原测试。

**Verify**（下列为本计划新增入口）：

```nu
nix develop --command bash -c 'exec sudo env "PATH=$PATH" GOTOOLCHAIN=local nu scripts/check-native-gate.nu --negative'
nix develop --command bash -c 'exec sudo env "PATH=$PATH" GOTOOLCHAIN=local nu scripts/check-native-gate.nu'
```

负向脚本自身 exit 0 表示正确观察到子命令失败；正向脚本只在指定原生测试实际 pass 时 exit 0。先检查脚本实现/Make目标存在，不把未创建命令当历史基线。

### 3. 接入 CI 与统一文档

workflow 设置 pinned Nix 环境并执行上述脚本、make lint；保留失败日志 artifact，artifact 不包括运行数据库、token 文件或全量环境。GitHub Actions cache keys 关联 flake.lock/Cargo.lock/module sums，缓存不是跳过验证的依据。README 和 IMAGE-MATERIALIZATION 写出快速/required-native 两种入口及能力前提。

**Verify**：

```nu
nix develop --command make lint
nix develop --command bash -c 'exec sudo env "PATH=$PATH" GOTOOLCHAIN=local nu scripts/check-native-gate.nu'
git diff --check
```

同时需要新工作流在分支 CI 实际成功，保存 run URL/日志。若当前无 CI 推送授权，只交付可审阅工作流和本地隔离结果，并标 BLOCKED（CI 未运行），不自行推送或把静态 YAML 检查算 CI 通过。

## 完成标准

- [x] make test-native 与正/负向脚本存在，required 参数由入口强制设置。
- [x] 指定 native 场景本地实际 pass，缺 bwrap 时子命令必失败；所用工具与本地结果已保存。root namespace/mapping 前提另列未验收。
- [ ] 完整 devShell 执行 make lint；当前仅离线eval、缓存包和同版本host lint通过，core/static guest jobs保留。
- [ ] 新 native-image CI 实际运行成功。当前 BLOCKED：无已授权push/真实run，root-only路径未执行。
- [x] 文档、Scope、diff check、索引状态准确，无当前宿主或源码测试语义修改。

## 实施与复核结果

实现 APPROVE，整体 BLOCKED。实现 worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-166d2501`，分支 `t3/fix-native-gate-validation`，基准b93fc24，采用 `9439544`（r1副本）→`baa5808`；独立 reviewer worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-97e4fc7a`，分支 `t3/review-native-gate-r2`，tree `7a07abe254969f188db6eb92612cfea28c325a68` 与候选相同。仅原六个Scope文件，flake.lock/engine源码及测试语义未变。

首轮复核要求不忽略杂行JSON、精确root skip原因/身份、四namespace与mapping IDs、独占日志/工具视图不入artifact；r2完成。独立reviewer重新跑actual positive（84s，1440行纯JSON，4/4指定pass、零skip）、actual negative（子exit1且精确missing-bwrap诊断、工具view清理）、非root preflight拒绝、混合输出/错原因/非root精确例外/无source前缀/后缀/额外native skip拒绝、lint0issues、离线Nix eval及YAML，全部符合预期。证据在该review worktree `build/native-gate/` 各fresh run目录。

尚缺真实GitHub Ubuntu runner/root preflight/full4294967295 mapping与root例外接受路径，未获CI push授权，无run URL；完整nixdevelop被既有GC移除bootstrap链阻断，原始shell同样缺项，未下载/构建补齐。host工具/离线eval和静态YAML不能替代这些验收；因此不能标DONE。未更改宿主或用户分支。

## STOP 与维护

不能取得匹配 erofs/umoci、无法获得隔离 runner namespace/mapping、Nix 缓存需新增凭据或工作流需宽泛宿主特权时先报告。不要直接升级 nixpkgs 或降低版本/语义合同。未来 native 测试重命名时同步脚本的实际执行检查，升级工具时重新验证两种格式。分支 `advisor/016-native-ci`；另获提交授权才使用 `ci: require native OCI materialization coverage`。
