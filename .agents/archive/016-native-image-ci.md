# Plan 016：将原生 OCI/EROFS 验证与 lint 工具链纳入持续门禁

> 已完成并归档。Python 标准库门禁及官方 lint action 的候选 `741cb9d` 已通过真实 Ubuntu root 验收和全部分支 CI，见[最终证据](evidence/016-native-ci-acceptance.json)与[CI run](https://github.com/woxQAQ/sandbox-101/actions/runs/42636560287)。下方合同、失败记录与历史授权保留其当时含义；当前工具前提见 [README](../../README.md#build-and-local-verification) 和 [image materialization](../../docs/image-materialization.md#validation)。本轮仅发布专用 CI 分支，未合入共享分支、未在当前宿主提权，也未恢复其他计划。

## 状态

- Priority：P2；Effort：M；Risk：LOW；Depends on：无。
- Category：tests / dx；Planned at：`b93fc24`，2026-11-06；Status：DONE（`741cb9dfcf284ef29148e34dee56e6a2aadb0768`，真实 root namespace/full mapping、原生正负向、lint、core/static-guest 及 cache 收尾全部成功；未合入共享分支）。
- 审计映射：发现 11。后续 OCI/EROFS 修改应以本门禁为前置。

## 问题与目标

根 CI 只安装 protoc，普通 make test 在缺少 bwrap/erofs/umoci 时可以把主要镜像测试全部 Skip。已有强制环境变量未用于 CI，单次手工原生验收不能提供持续覆盖。开发环境与 CI 应提供相同的工具合同，不能通过环境装配来源区分验收标准。

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

`Makefile` 已有 test/check/lint/integration。[镜像契约](../../docs/image-materialization.md#validation) 要求匹配 mkfs/fsck >=1.9.4 且 <2、umoci=0.6.0，以及可用 user/mount namespace 和映射。两套格式均不可用时不能改用另一套通过。工具通过受控 PATH 和必要的只读依赖根提供，不检查它们来自哪个包管理器。

实际已确认：普通 make test/check/lint/integration 在审计机器通过；TestActual 全部因工具缺失 Skip。当前本机未准备完整原生前提，不能宣称已验证 CI 方案。

## Scope

允许：`.github/workflows/check.yml`、`Makefile`、`README.md`、`docs/image-materialization.md`；新增 `scripts/check-native-gate.py`（验证命令包装，不生成源代码）和 `scripts/test_native_gate.py`（CLI 行为回归），索引状态。本轮按用户要求移除先前的 Nushell 包装及 CI 依赖。专用环境提供者的装配代码可同步工具合同，但不成为通用 gate 的依赖。

禁止：修改 engine 业务/测试语义、降低 required 环境变量、升级依赖锁、修改当前宿主配置、放宽恢复/COW。CI 所需安装只能写在工作流的环境装配步骤里并在其一次性 runner 执行，不在用户机器上执行；gate 本身不安装、下载或调用环境提供者。

## 目标入口

新增 `make test-native`：单一明确命令执行 `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test -json -race ./... -count=1 -timeout=300s`，Make recipe 前加 `@`，stdout 仅保留 Go JSON，不混入命令回显。不把 required 设置藏在调用者 shell 状态。300s 为完整 Engine race 留出已观察到的运行时间，不减少用例或改变 PASS/SKIP 判定。所有环境装配入口提供相同的 golangci-lint、Python >=3.11、util-linux、coreutils 和原生镜像工具合同；gate 仅用 Python 标准库，不要求 Nushell。

native-image job 固定为一次性的 GitHub Ubuntu 24.04 runner，先在独立装配步骤准备工具，仅 native preflight/test 使用 sudo 切换为 root，再由现有代码在新 namespace 内运行原生工具；这不授权当前用户宿主执行 sudo。只显式传入受控 PATH、必要的 SANDBOX_IMAGE_TOOL_ROOTS 与 GOTOOLCHAIN=local，不保留 token/全量环境，不全局关闭 AppArmor/namespace 防护。root merged 采用 `--map-users=0:0:4294967295 --map-groups=0:0:4294967295 --keep-caps`，不需要 subordinate 映射助手。唯一允许的 root 专属 skip 为 `TestActualMergedOCIInvalidInputsDoNotPublish/unmapped_owner`，理由必须精确为 root can identity-map all owners；其他 required-native skip 均失败。保留 core/static-guest jobs，lint 仍以普通 runner 用户运行。专用提供者 CI 仅替换装配步骤，运行同一 gate。

## 步骤与验证

### 1. 确认 runner 能力，不把环境失败变成 Skip

选定上述 root CI 模式，不在非 root/root 两条路径间运行时猜测。新增脚本 --preflight 模式（不跑完整 suite），要求 effective UID=0，记录并检查工具版本、真实可执行路径及能力；路径只用于来源证据，不按包管理器目录判定 PASS。再实际执行：

- shared namespace：用已准备的 bwrap 创建全新 user/mount/PID/network namespace，只读绑定系统工具目录及显式配置的依赖根、私有 /proc 和 /dev、临时 /tmp，然后运行 `id -u`，预期 0；必须检查 namespace 与外层不同。
- merged mapping：用 pinned unshare 的完整 root identity mapping 参数执行只依赖 POSIX sh、id、readlink 的固定 probe，输出 real/effective UID/GID 和 `/proc/self/uid_map`、gid_map，映射起点均 0、长度 4294967295；随后用 `SANDBOX_REQUIRE_EROFS_TESTS=1 GOWORK=off go -C engine test -json -run '^TestActualMergedOCIExplicitSelectionAndVerification$' -count=1 -timeout=60s .` 实际走现有 production mapped sandbox/helper，不增加仅版本检查的假 preflight，也不修改测试源码。
- 完整原生共享/merged 场景仍由 required tests 证明；preflight 不替代它们。

**Verify**：在已经具备上述环境和 root 身份的一次性 CI runner 上执行；身份切换由 CI 步骤负责，不属于通用命令：

```sh
python3 scripts/check-native-gate.py --preflight
```

这是本计划新增入口，预期版本、身份、实际 namespace 和 mapping 检查均成功。当前用户机器没有明确的隔离 root 执行前提时记录 BLOCKED，不照抄 sudo 到本机。runner 安全限制不满足则失败并报告，不改为 Skip。

### 2. 建立强制入口及失败自检

新增 Make target 和 `scripts/check-native-gate.py`：脚本只在新建、gitignored 的 build/native-gate 目录存日志，用 Python subprocess 分别保存 test-native 的纯 JSON stdout、stderr 原始字节与原始 exit code；必须同时满足子命令 exit=0 和四个指定测试 pass。分析 Go test JSON，要求 `TestActualLayeredEROFSImportAndExtraction`、`TestActualSharedOCIHardlinksAndMetadata`、`TestActualMergedOCIHardlinksAndMetadata`、`TestGCActualLayerCacheAndCompletedImport` 具有 pass 事件，不能只有包 pass。required-native 的 skip 除上述精确 root 例外全部失败；KVM/capture 等非本 job 范围的其他 test skip 单列记录，不误算原生门禁。

添加负向自检模式：在该临时目录创建仅对本命令有效的工具 PATH 视图，保留必需编译器/基础工具，唯独移除 bwrap；以 -json、required 环境变量执行单个 TestActualLayeredEROFSImportAndExtraction。必须保存实际非零 exit_code，且 JSON/output 包含精确 `required OCI tool missing: bwrap`；编译失败或其他工具缺失的非零不能算正确负向。不要卸载机器工具或修改原测试。

**Verify**（下列为本计划新增入口）：

```sh
python3 scripts/check-native-gate.py --negative
python3 scripts/check-native-gate.py
python3 -I scripts/test_native_gate.py
```

负向脚本自身 exit 0 表示正确观察到子命令失败；正向脚本只在指定原生测试实际 pass 时 exit 0。先检查脚本实现/Make目标存在，不把未创建命令当历史基线。

### 3. 接入 CI 与统一文档

workflow 在单独步骤装配满足版本/能力约束的工具，再执行上述脚本、make lint；保留失败日志 artifact，artifact 不包括运行数据库、token 文件或全量环境。GitHub Actions cache keys 关联所选工具链来源、Cargo.lock/module sums，缓存不是跳过验证的依据。README 和 IMAGE-MATERIALIZATION 写出快速/required-native 两种入口及能力前提。提供者专用 CI 的装配验证留在其专用流程，不混入镜像功能门禁。

**Verify**：

```sh
make lint
python3 scripts/check-native-gate.py
git diff --check
```

同时需要新工作流在分支 CI 实际成功，保存 run URL/日志。若当前无 CI 推送授权，只交付可审阅工作流和本地隔离结果，并标 BLOCKED（CI 未运行），不自行推送或把静态 YAML 检查算 CI 通过。

## 完成标准

- [x] 当前候选具有 make test-native 与正/负向脚本，required 参数由入口强制设置。
- [x] 指定 native 场景实际 pass，缺 bwrap 时子命令必失败；工具与结果已保存。
- [x] 实际 root namespace/full identity mapping 已验证。
- [x] 通用 gate 不调用环境提供者，不要求包管理器路径；所有入口使用相同测试和判定。
- [x] 准备好的开发与 CI 工具环境执行 make lint，core/static guest jobs 保留并实际通过。
- [x] 新 native-image CI 实际运行成功，保留精确候选与 run URL。
- [x] 文档、Scope、diff check、索引状态准确，无当前宿主或业务测试语义修改。

## 本次实现与证据（提供者无关合同）

实现基准为 `dacf00e6645153e2d43bd356da509758ea5602f8`；本轮获准将候选提交并推送到 `origin/ci/native-image-016` 运行 CI，不合入 `main`/`dev`。最终范围限于当前 Scope 和计划/索引状态，未更改 flake.nix/flake.lock、engine 源码或测试语义。后续复核使用项目已有 `nix develop` 临时工具环境，未修改系统/用户 profile、执行 sudo 或修改宿主 namespace/AppArmor 设置。

- `make test-native` 固定执行完整 Engine required race JSON suite，300s timeout，recipe 不回显。
- `scripts/check-native-gate.py` 提供完整门禁、`--preflight` 和 `--negative`。每次创建独占日志目录，分别保存 stdout/stderr/原始 exit code，记录源码 revision、入口 SHA256、身份、skip 和最终判定。缺少 HOME 的干净环境自行创建 mode 0700 私有 runtime HOME，不透传 runner 凭据；真实 effective UID 使用 `os.geteuid()`，不从可替换的 PATH 工具输出推断。
- CI 保留 core/static-guest，新增 Ubuntu 24.04 native-image job。装配使用校验 SHA256 的 erofs-utils 1.9.4、unshare 2.41.3 源码，及固定版本 umoci 0.6.0；官方 `golangci/golangci-lint-action@v9.3.0` 以 `install-only` 提供固定 golangci-lint 2.14.0，发行版提供 Python/bubblewrap/coreutils。umoci 因自身 `replace` 声明从校验过的 Go module 源码构建，不使用不支持该声明的 `go install …@version`。`make lint` 仍为单一检查入口，在原生验收之后使用普通用户运行，避免 `go fix` 先行改写待验源码；原生 gate 使用最小 root 环境；artifact 只收诊断文件，不收 runtime HOME、工具视图或数据库。

### 前一 Nushell 候选的真实执行记录

本机为非 root UID 1000。首轮只使用裸 shell：Go 1.27.3、Nushell 0.116.1、golangci-lint 2.14.0，其 PATH 无 bwrap、mkfs/fsck/dump.erofs 和 umoci。这不是项目缺少工具配置；首轮未进入已有 devShell 是验证遗漏，裸 shell 的失败不能代替开发环境验证。本机工具版本不代替 CI 选定工具链的运行证据。

| 命令 | 实际结果 |
|---|---|
| `nu scripts/check-native-gate.nu --negative` | PASS，脚本 exit 0；实际 Go 子命令 exit 1，9 行纯 JSON，包含选定测试的精确 `incompatible: required OCI tool missing: bwrap` 诊断及 fail 事件；工具视图已清理。日志 `build/native-gate/run.Z9ZJN1UMzU/` |
| `nu scripts/check-native-gate.nu --preflight` | 按合同拒绝非 root，exit 1；没有自动提权，没有执行真实 namespace/mapping probe。日志 `build/native-gate/run.GYHuswwhXU/` |
| `nu scripts/check-native-gate.nu` | FAIL / 环境 BLOCKED，脚本 exit 1、make 子命令 exit 2；保存 1608 行纯 Go JSON，24 个原生测试失败诊断均为缺 bwrap，另含父测试/package fail；四个指定 pass 不成立。日志 `build/native-gate/run.aHrVcajNIs/` |
| `make lint` | PASS，exit 0，0 issues；未造成额外源码改动 |
| `nu --no-config-file -c 'nu-check scripts/check-native-gate.nu'` | `true`，语法通过 |
| Python/PyYAML 读取 workflow + 各 `run` 块 `bash -n` | PASS；仅证明 YAML 结构、保留的 jobs 与 shell 语法，不证明 GitHub Actions 或安装步骤成功 |
| `git diff --check` | PASS |

后续按用户指出的项目 `flake.nix` 重新验证：正常 `nix develop --no-write-lock-file` 成功，提供 Go 1.26.8、Nushell 0.116.1、bubblewrap 0.12.0、erofs-utils 1.9.4、umoci 0.6.0、unshare 2.42.3、golangci-lint 2.14.0，且设置 `SANDBOX_IMAGE_TOOL_ROOTS=/nix/store`。事先尝试的 `--offline` 进入在 config.guess/config.sub 装配阶段失败（404），不代表正常 devShell 不可用，也未作为门禁结果。

| devShell 命令 | 实际结果 |
|---|---|
| `nix develop --no-write-lock-file --command nu scripts/check-native-gate.nu --negative` | PASS，脚本 exit 0、实际 Go 子命令 exit 1，精确 missing-bwrap 测试诊断与 fail 事件成立，私有工具视图已清理；日志 `build/native-gate/run.XJx6lBfGYD/` |
| `nix develop --no-write-lock-file --command nu scripts/check-native-gate.nu` | 四个指定 native 测试实际 pass，但完整 suite 因继承的长 TMPDIR 导致 `TestSystemAgentWaitsForGuestVsockListener` 的 Unix socket 路径过长、bind invalid argument 而失败；日志 `build/native-gate/run.sbuafzx5i6/`。不记作完整通过 |
| `nix develop --no-write-lock-file --command nu --no-config-file -c 'with-env {TMPDIR: "/tmp"} { ^nu scripts/check-native-gate.nu }'` | **PASS**，脚本与 make 子命令均 exit 0，1762 行纯 Go JSON，四个指定 native pass、零 test skip、零 fail，完整 Engine race 包耗时 175.328s；日志 `build/native-gate/run.OLBchZNARC/`。仅对本次进程设置短 TMPDIR，所有测试和断言不变 |
| `nix develop --no-write-lock-file --command make lint` | PASS，exit 0、0 issues。既有 `go fix` 自动改写 engine/sandboxd 三处 `fmt.Sprintf` 转换；已只撤回这三处范围外改动，上述最终完整 PASS 在恢复原源码后重跑取得 |

上述裸 shell 与 devShell 原生日志绑定同一套入口 SHA256：

| 入口 | SHA256 |
|---|---|
| `Makefile` | `ecfa603e5c5683c18391252b0a7a9e4a67b82270c9b76c91cc6297cd487ba9d7` |
| `scripts/check-native-gate.nu` | `159385078619191d3dc979c86f38c9cc9e6fb5c69d0262c40edcbf85075b89bd` |
| `.github/workflows/check.yml` | `5e98daf95e4ec61577078830ce947af079fa828be8dc2b3575f669658561c5f6` |

### 入口判定回归与未运行边界

使用 per-turn scratch 中的合成 make/go/id/bwrap/unshare 输出，实际调用完整脚本验证 27 个入口场景：四指定 pass 接受、package-only/缺 pass/非零退出/混合或非法 JSON 拒绝、额外 native skip 拒绝、无关 skip 记录、精确 root skip 例外与非 root/错误原因/无 source/多余后缀拒绝、负向编译错误/错误工具/零退出拒绝，以及版本不匹配、错误身份、短 mapping、未变化 namespace、production probe skip 拒绝。另验证无 HOME/LAST_EXIT_CODE 的干净环境与 mode 0700 runtime HOME，以及正常成功/失败后的工具视图清理。这些仅证明包装器判定，不证明 root 身份、namespace、native tools 或生产导入实际可用；合成日志标记为 synthetic，不挪作原生通过记录。

最初一次完整 gate 冷编译被 terminal 的 120s 外层时限中止，未作为结果；随后以足够外层时限完成上述真实命令。截至该轮本地复核，项目 devShell 的原生 shared/merged/GC 完整正向、实际负向与 lint 已通过，root/分支 CI 尚未运行，因此当时仍 BLOCKED；后续实际结果见下方。该轮未运行 KVM、kernel-isolation、Rust guest fixture、integration/smoke 或 production；完整 Engine suite 内的 Go guest 测试已随正向 gate 执行。

### 分支 CI 验收

本轮获准推送专用分支后，首候选 `cb38d2e723386cc6db890fb3cb22528720490e6a` 已运行 [GitHub CI 42628888664](https://github.com/woxQAQ/sandbox-101/actions/runs/42628888664)。结果 FAIL：static-guest 成功；core 在入口测试发现缺少 `rg`；native-image 的原生工具装配成功，但 golangci-lint 2.11.3 报 gci 格式问题，root gate 步骤因此未运行。此 run 不算 root/native 通过。

后续修复限于工具装配和检查顺序：core 显式提供 ripgrep/build-essential/binutils/file；lint 与开发已通过的 2.14.0 对齐并使用 SHA256 校验的官方包，不修改业务源码或 formatter 规则；`make lint` 移到原生验收之后，使原生证据绑定未被 `go fix` 改写的提交。修复候选仍待真实 CI 验收。

按用户复核意见，进一步以官方 `golangci/golangci-lint-action@v9.3.0` 的 `install-only: true` 替代手写 lint 下载/安装。固定 `version: v2.14.0`，安装路径显式加入受控 PATH；保留后续同一个 `make lint`，不重复维护 package patterns 或改变检查语义。action 自身缓存关闭，项目 Go cache 仍由关联 workflow/module sums/Cargo.lock 的 key 管理。

### Python 入口重整与当前候选验证

按用户要求，不再安装或调用 Nushell。删除 `scripts/check-native-gate.nu`，以 Python 标准库实现 `scripts/check-native-gate.py`，保持同一 Make target、原始输出/退出码、四指定 pass、精确 root skip、实际负向失败和 namespace/mapping 规则。只读 namespace 中的身份探针改用已准备的 POSIX sh/id/readlink，不引入额外解释器。新增 `scripts/test_native_gate.py`，在独占临时 workspace 和封闭 PATH 下验证 CLI，而非导入函数单测；该回归也接入 core。

实际执行：`python3 -I scripts/test_native_gate.py` 的 7 个行为测试及其子场景通过；Python AST、workflow YAML 与 shell 语法检查通过。Python 初稿经 `nix develop --no-write-lock-file --command env TMPDIR=/tmp python3 -I scripts/check-native-gate.py` 实际 PASS，make exit 0，1761 行纯 JSON、四指定 pass、零 test skip、Engine 包 139.604s（`build/native-gate/run.5vfkmy7u/`）；`nix develop --no-write-lock-file --command python3 -I scripts/check-native-gate.py --negative` PASS，实际 Go exit 1、9 行 JSON、精确 missing-bwrap 诊断，视图清理（`build/native-gate/run.vkfmubcq/`）；本机非 root preflight 明确拒绝（`run._sdjc80g/`）。初稿之后只简化了尚待 CI 执行的 namespace 字符串检查；当前候选 root/CI 必须重新验收。

前一官方 action/Nushell 候选 `c7261f285e2e571f08804412c1755d89843f4140` 的 [CI 42631164065](https://github.com/woxQAQ/sandbox-101/actions/runs/42631164065) 中，native root preflight、负向、完整 gate 与普通用户 lint 均成功，但 cache post-step 重新计算 `hashFiles` 时遍历 root 私有 runtime HOME 失败，native job 因而 FAIL；core 则在完整 Sandboxd race 包达到 180s 总预算时失败，static-guest 成功。修复为在特权步骤前冻结 cache key，post-step 只复用固定 key，不遍历私有 HOME，也不放宽其权限。

对 core 超时另实际运行 `nix develop --no-write-lock-file --command env TMPDIR=/tmp make test-race`，试行 300s 包预算仍在 Sandboxd 后续的 `TestProductionOwnerBudgetFiniteHTTP`（该用例仅运行 4s）到达整包上限，结果 FAIL，未记作通过。两个上限下执行推进到不同测试，说明整包预算不足；当前统一将普通 `test-race` 的模块预算设为 Go 标准默认 10m，仍保留所有用例、每个用例自己的 deadline 和生产资源限制，`test-native` 的 300s 不变。最终 CI 的完整 Sandboxd race 包实际以 285.790s 成功，不用预算调整本身宣称通过。

### 最终 Python 候选：真实 CI 全部成功

- 精确候选：`741cb9dfcf284ef29148e34dee56e6a2aadb0768`，分支 `ci/native-image-016`，push 事件、attempt 1；[run 42636560287](https://github.com/woxQAQ/sandbox-101/actions/runs/42636560287) 总结果 success。
- [native-image](https://github.com/woxQAQ/sandbox-101/actions/runs/42636560287/job/129226118710)：root preflight、实际负向、完整 required race、普通用户 lint、artifact 和 cache post-step 全部成功。artifact 已下载到本机 `build/native-gate/ci-42636560287/` 并逐项核验；持久摘要及原始 artifact 标识、digest、入口 SHA256 见[接受收据](evidence/016-native-ci-acceptance.json)。
- root probe 的 real/effective UID/GID 均为 0，shared 的 user/mount/PID/network 四个 namespace ID 均与外层不同；merged 使用另一新 user namespace，UID/GID maps 均精确为 `0 0 4294967295`，实际 production merged fixture pass。
- 完整 Engine race：make exit 0，1763 行纯 JSON，四指定 pass、406 个 test/subtest pass 事件，唯一 test skip 为精确 root `unmapped_owner` 例外，零 fail；Engine 包 53.176s。`changed-paths.stdout` 为空，三入口 SHA256 与提交和最终源码一致。
- 实际 negative：Go exit 1，9 行纯 JSON，选定测试的精确 missing-bwrap source 诊断及 fail 事件成立；脚本 exit 0。artifact 87 个诊断文件，不含 runtime HOME、PATH view、数据库、token 或全量环境。
- [core](https://github.com/woxQAQ/sandbox-101/actions/runs/42636560287/job/129226119279)：入口回归、7 个 Python gate CLI 行为测试、`make test check test-race integration smoke` 全部成功，包含真实 Rust Unix fixture；[static-guest](https://github.com/woxQAQ/sandbox-101/actions/runs/42636560287/job/129226119315) 的 `make guest` 成功。
- 仍不声称本轮运行了 KVM、kernel-isolation 或 production。未更改 engine/sandboxd 源码、测试断言或 flake 锁；023/024 等并行草稿未进入本分支提交。归档只记录已完成事实，不授权合入 `main`/`dev` 或继续其他暂停工作。

## 历史实施与复核结果（原提供者绑定方案）

以下记录仅描述旧隔离分支及其证据，不能替代上方修订合同的实现或验收。旧方案的完整 shell/store 路径检查不再是通用门禁条件，其原始 BLOCKED 状态不改写。

实现 APPROVE，整体 BLOCKED。实现 worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-166d2501`，分支 `t3/fix-native-gate-validation`，基准b93fc24，采用 `9439544`（r1副本）→`baa5808`；独立 reviewer worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-97e4fc7a`，分支 `t3/review-native-gate-r2`，tree `7a07abe254969f188db6eb92612cfea28c325a68` 与候选相同。仅原六个Scope文件，flake.lock/engine源码及测试语义未变。

首轮复核要求不忽略杂行JSON、精确root skip原因/身份、四namespace与mapping IDs、独占日志/工具视图不入artifact；r2完成。独立reviewer重新跑actual positive（84s，1440行纯JSON，4/4指定pass、零skip）、actual negative（子exit1且精确missing-bwrap诊断、工具view清理）、非root preflight拒绝、混合输出/错原因/非root精确例外/无source前缀/后缀/额外native skip拒绝、lint0issues、离线Nix eval及YAML，全部符合预期。证据在该review worktree `build/native-gate/` 各fresh run目录。

尚缺真实GitHub Ubuntu runner/root preflight/full4294967295 mapping与root例外接受路径，未获CI push授权，无run URL；完整nixdevelop被既有GC移除bootstrap链阻断，原始shell同样缺项，未下载/构建补齐。host工具/离线eval和静态YAML不能替代这些验收；因此不能标DONE。未更改宿主或用户分支。

## STOP 与维护

不能取得匹配 erofs/umoci、无法获得隔离 runner namespace/mapping、工具源需新增凭据或工作流需宽泛宿主特权时先报告。不要直接升级依赖锁或降低版本/语义合同。未来 native 测试重命名时同步脚本的实际执行检查，升级工具时重新验证两种格式。历史分支 `advisor/016-native-ci`；另获提交授权才使用 `ci: require native OCI materialization coverage`。
