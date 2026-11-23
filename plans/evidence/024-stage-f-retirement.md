# 024 阶段 F：旧控制边界清除与验收迁移

状态：**STAGE_F_PARTIAL / VM_ACCEPTANCE_BLOCKED**。旧控制模块、双 daemon 和 engine UDS facade 已删除；测试、workspace、CI、native gate、runner 与部署示例已迁到 Haven/Loom。最终组件、完整 race、六项 Rust integration、六组 smoke、静态检查和 required-native 均通过。最终 native `run.9exdrqy5` 为 **677 pass、0 fail、0 skip**，Haven **278.938s**；JSON/race/count=1/300s 与四个原 required pass 名称保持。当前 Haven schema 仍为 **9**，无旧库自动迁移。实际 capture 与 kernel-isolation 通过；KVM boot/production 缺显式材料和预算保持 BLOCKED。017/020/021 继续暂停；没有部署切换、提交、推送、合入、额外材料下载或真实旧资源操作。

## 后续命名整理

阶段 F 全量门槛完成后，按新的命名约束清理自有代码、脚本、配置与文件名中的 `production` / `acceptance`。使用行为和职责命名：

| 原入口/名称 | 当前入口/名称 |
|---|---|
| production-e2e | `runtime-e2e`，完整 Haven daemon/VM 生命周期 |
| PRODUCTION_OCI_LAYOUT / ARTIFACTS_DIR | `RUNTIME_OCI_LAYOUT` / `RUNTIME_ARTIFACTS_DIR` |
| SANDBOX_PRODUCTION_* | `SANDBOX_RUNTIME_*` |
| run-production-acceptance.sh | `scripts/run-runtime.sh` |
| run-kvm/kernel-isolation/capture-isolation-acceptance.sh | `scripts/run-kvm.sh`、`run-kernel-isolation.sh`、`run-capture-isolation.sh` |
| prepare-acceptance-materials.sh / test_acceptance_environment.py | `scripts/prepare-vm-materials.sh` / `test_runner_environment.py` |
| production_* Go fixtures | `integration/runtime_lifecycle_test.go`、`runtime_network_test.go`、`vm_observation_test.go` |
| 带环境标签的 helper | `vmInventory` / `vmObservation` / `startHaven` / `daemonProcess` / `guestExec` / `writeArtifact` 等实际职责名称 |

KVM guest 测试目录、unit、临时身份、idempotency keys 与诊断文本也同步命名，原权限、shape、限额、namespace/mapping、断言和超时不变。当前自有源代码、脚本及文件名扫描无这两个词；历史证据与研究保留原名称，未添加内部兼容入口。

本轮实际验证：10 项 runner 回归、7 项 native gate CLI 回归、architecture、workspace/privileged/kvm 三组 lint、privileged test 发现/编译、两种 KVM rootfs 实际物化与 invalid-tool-root 回归、六项 Rust integration、真实跨 UID capture、真实 kernel cgroup/nft/NAT/TCP/block-I/O 全部 PASS。日志为 `/tmp/haven-naming-checks.log` 和 `/tmp/haven-naming-validation.log`，kernel 私有材料为 `build/kernel-naming.xkKSPeHQ`。VM boot 与完整 daemon runtime 仍缺明确材料和预算，未运行。

**下文 677 pass、完整 race/smoke、source fingerprint 和原 runner 命令绑定命名整理前的 F 树。** 本轮仅运行上述受影响的定向检查，没有将旧全量通过挪用为新树证明。

## F 完成时的代码和入口

| 边界 | 当前实现 |
|---|---|
| 唯一宿主 module | 根 `github.com/woxQAQ/haven`；`go.work` 仅根、operator、protocol/gen；Makefile test/check/race 覆盖根与 protocol，workspace lint 保留 operator |
| 唯一 daemon | `cmd/haven` 真实 host runtime、业务 HTTP 与管理 UDS；旧 engine/sandboxd `.go`、go.mod/go.sum 和三个旧命令路径全部删除 |
| 客户端 | havenctl 与 loomctl 仅 DTO/SDK/配置，不读 SQLite、不直接调用 Loom；没有旧客户端兼容代理 |
| native gate | 仍为 `SANDBOX_REQUIRE_EROFS_TESTS=1`、JSON、race、count=1、300s；四个原 required test 的 pass event 改读 `github.com/woxQAQ/haven/internal/haven` |
| architecture gate | 保留直接/传递依赖检查、真实 runtime 检查，并拒绝旧目录中遗留 Go 源码或 module；README 导航不构成运行边界 |
| VM runners | KVM 指向根 `internal/haven`；kernel 指向 `loom/host`；capture 指向 `loom`；production 只构建一个 Haven 和根 integration binary |
| 部署 | `deploy/host/haven.service`、`haven.example.yaml`、两个 CLI 示例；删除旧 services/configs。保留原 nft table/link/namespace identity，不把内核名称误作第二控制面 |
| 当前文档 | 资源/Sandbox 架构、资源集成、网络、镜像、业务 API、Haven control、host/scripts 与 AGENTS 任务指针同步；研究与历史验收不改写 |

`docs/README.md` 与 `docs/research/sandbox-comparison.md` 按原约束保持不动。为保持其导航有效，`sandboxd/README.md` 维护当前 Sandbox 契约；`sandboxd/egress/README.md`、`deploy/sandboxd/README.md` 只导航当前实现/部署。没有残留 Go module 或 daemon。

## 行为迁移与证明

D 的控制行为映射见 [D 历史矩阵](024-stage-d-control-matrix.md)，E 的 HTTP/SDK/冻结交付/batch fork/真实 Rust 映射见 [E 历史证据](024-stage-e-entrypoints.md)。它们解释迁移来源，不提供当前树通过证明。本轮补齐删除前仍在旧模块执行的边界：

| 原场景 | 当前测试与保留断言 |
|---|---|
| engine/fsmeta、image_shared、image_compat、image_validation | `internal/haven/{fsmeta,image_shared,image_compat,image_validation}_test.go`：原 Test/子场景、实际工具/namespace、全字节和固定 hash、hardlink inode、whiteout/opaque、root/目录/implicit/duplicate/forward、权限/纳秒时间/xattr/ACL/capability、cache-only rebuild/确定性/损坏、安全路径、设备顺序与 16-device 上限、格式/builder/manifest 验证、explicit merged 限制全部保留 |
| 真实 native importer | `native_image_fixture_test.go` 只为 unused VM effects 注入 backend；每次导入进入真实 Haven AdmitImageImport/WaitManagement/SQLite。fixture 不插入 operation、witness、成功结果或费用；旧 catalog/工作路径断言改为当前 images 表及原 operation ID |
| image_catalog | `image_catalog_test.go`：仅 filesystem image 不授予 provision authority，published 输入可供应，file/captured materialization 改变在副作用前拒绝 |
| GCActualLayerCacheAndCompletedImport | `gc_native_test.go`：实际 published caches 保全、原 failed import witness 授权孤立 cache 清理、dry-run 不删、completed work 退休、unknown stage 保留并明确报告、同键并发 cache-hit import 与 GC |
| sandboxd/agent client | `guest_client_test.go`：真实 CH 握手/protobuf、准确 Admin/Workload port、相关 ID/EOF、固定 exec_key/command、原 16 类 structured failure/unknown fence；guest-provided diagnostics 不进入公开错误 |
| KVM | `kvm_integration_test.go` 保留 linux/kvm tag、两种真实 OCI、完整 guest metadata/readonly raw writes/tree transfers、private RAM COW、独立可写盘、source 删除、resnapshot/grandchild 等原断言；TestKVMRootfsOCI 单独验证真实物化，无 VM boot 冒充 |
| privileged lifecycle | 根 `integration/microvm_test.go` 保留 claim/checkpoint/2 branches/pause/8 concurrent resume/identity/live shell/rollback/paused zero-wake release/source 删除后 branch 存活 |
| production | 根 `integration/production_*_test.go`：管理 UDS 真导入、两协议同 store、一个 Haven SIGKILL/restart 后精确 PID/starttime/instance/guest shell 接管，真实 512 MiB、launcher/pivot_root/UID/GID/capability/seccomp、golden/warm、TAP/SNAT source/fork/resume 断言。read-only inventory 只读指定私有 store 并调用真实 inspector，不接纳或执行资源动作 |

KVM 测试放在 `internal/haven/`，复用实际导入和 native OCI fixtures，避免为 integration 导出测试控制 hook；runner/package 定位已同步，原 tag/材料门槛/断言不变。production 的只读 inventory 不等于生产控制 API，也不证明未运行的真实路径。

## 本轮发现与修复

- **未知 cache child 报告缺口**：原 Haven 保留未知 builder 子目录，但 inventory 不报告。现在对受控目录做有界深度只读枚举，未知 child 报为 unknown_artifact；不读取内容重造 owner、不授予删除权。
- **跨 fork 的 writer lock 交接**：组件测试出现 Close 后立即 reopen 的 lock conflict。CLOEXEC 仍允许并发 fork 到 exec 之间短暂保留同一个 open file description。完成 DB/worker shutdown 后显式 LOCK_UN，再 close；review 的独占锁同样显式释放。新增实际 Service/SQLite/flock duplicate-FD 回归，并对原 atomic-completion 矩阵重复五次通过。
- **capture runner TMPDIR 权限**：Nix shell 的 0700 临时祖先导致 mapped writer 无法执行 fixture。runner 现在仅创建自己的 `/tmp/haven-capture.*`、设置 0711 traversal，所有原 fixture/production 权限不变，退出删除自身临时根。新增 closed-PATH runner 回归验证其权限、mapping 参数与 cleanup；实际跨 UID 场景重跑通过。
- **required-native 300s 超时**：迁入的 native 场景最初串行，`run.lc7eienp` 在 300.031s 超时，未观察断言失败。独立私有 native tests 使用 Go parallel 调度，原 timeout/hash/race/assertions 不变；中间树 `run.5rp7h6q6` 已在 289.260s 通过。该中间结果早于 writer-handoff 修复，不能作为最终树 gate。
- **KVM rootfs 相对路径**：实际 rootfs regression 发现迁移后的 guest service 路径仍少一层 `..`，已改成根 guest 路径并重跑两种真实 materializer 通过。tagged lint 也保留 binary tar xattr fixture 的明确说明，并移除未被使用的 manifest 初始化。

## 验证记录

所有宿主工具通过 `nix develop --no-write-lock-file` 提供。大量私有状态使用新建 `/dev/shm/h24f-*`；kernel block-I/O 验收使用新建 block-backed `build/kernel-isolation-f.XasP8itD`。没有访问已有 daemon、`.local-run/`、旧业务 DB 或未知资源。

| 命令/边界 | 当前结果与证据 |
|---|---|
| targeted native materialization | PASS，全部迁入场景真实 OCI/EROFS；后续纳入最终 full gate |
| Service writer handoff + original atomic completion，count=5 | PASS，`/tmp/haven024-f-static.log` |
| `go test -tags=kvm ./internal/haven -run '^TestKVMRootfsOCI$' -count=1`，required EROFS | PASS，`/tmp/haven024-f-last-static.log`；两种真实布局，无 VM |
| workspace lint + privileged/kvm tagged lint | PASS，三组 0 issues，`/tmp/haven024-f-last-static.log` |
| final architecture dependency gate | PASS，`/tmp/haven024-f-last-static.log` |
| acceptance environment CLI regressions | PASS，10 tests，`/tmp/haven024-f-runner-checks.log` |
| native gate CLI regressions | PASS，7 tests，同上 |
| `make capture-isolation-e2e` | PASS，四类实际场景及 writer helper/5 类非普通产物，`/tmp/haven024-f-runner-checks.log`；先前权限失败保留于 `/tmp/haven024-f-capture.log` |
| `make kernel-isolation-e2e KERNEL_ARTIFACTS_DIR=<new private block-backed dir>` | PASS，`/tmp/haven024-f-kernel.log`：CLONE_INTO_CGROUP、pids exhaustion、CPU throttle、memory OOM、实际 259:0 block I/O 1 MiB/s（约 12.6ms → 4.014s）；实际 netns/nft/NAT/TCP allow/deny/flow/revoke/stale/tamper |
| 最终 `make test check test-race` | **PASS**，`/tmp/haven024-f-delivery.log`；Haven 普通组件 71.688s、race **275.937s**，protocol 普通/race、Rust workspace tests/fmt/clippy 同次通过 |
| 最终 `make integration smoke` | **PASS**，六项真实 Rust integration、六组单 Haven/双 CLI/config/restart/backup smoke，含实际 build |
| 最终 native negative/full | **PASS**，负向 `run.dkxkq72_`；完整 `run.9exdrqy5`：**677 pass、0 fail/skip**，Haven **278.938s**，egress 25.816s、Loom 1.118s、host 1.015s、SDK 1.012s；JSON/race/count=1/300s/required EROFS 和原四项 pass 不变 |
| 最终 delivery 顺序命令 | `/tmp/haven024-f-delivery.status` 为 **0**；包含上述组件/Rust/smoke、10 项 runner 回归、7 项 gate 回归、architecture 与 native negative/full |
| 场景发现/文档/保留边界 | 两组 go test -list 发现 129 个 TestHaven 名称，覆盖计划表 19 个实际测试；当前文档文件链接、diff --check 通过。docs/README、研究比较、operator/guest/protocol/third_party 相对 HEAD 无改动；原 dirty reference 子模块保留 |
| `make kvm-e2e` boot | **BLOCKED / 未运行**：未提供显式 KVM_MATERIALS_DIR、KVM_ROOTFS_DIR、KVM_ARTIFACTS_DIR |
| `make production-e2e` | **BLOCKED / 未运行**：未提供材料、PRODUCTION_OCI_LAYOUT/ARTIFACTS_DIR 和 SANDBOX_PRODUCTION_IO_MAX；不推导 sudo、旧资源操作或下载授权 |
| `make privileged-e2e` | 未运行：没有显式准备的私有 Haven 部署及 SANDBOX_E2E 输入 |

环境只读检查确认 UID1000 可访问 KVM/TUN/FUSE；设备可访问不替代材料/预算/部署输入，也不构成 VM 验收。没有安装/下载 VM 材料，没有运行旧验收来替代新树。

## 剩余交付边界

最终组件与 native gate 已通过，剩余为明确材料约束下的当前 Haven KVM boot 与 production 验收。整体保持 PARTIAL/BLOCKED，不标为 STAGE_F_PASSED、不归档计划。deployment example 更新不授权部署切换；017/020/021 后续恢复前须重新映射当前 Haven store/step/观测/诊断设计。

## 最终源码和工具身份

HEAD 仍为 `b7feea385bfc463098551fb24755d931d3efc37c`，所有迁移工作保留未提交 diff。Go `go1.26.8 linux/amd64`，Cargo `1.98.0 (797e8a9bc 2026-08-05)`，golangci-lint `2.14.0`。

最终宿主/guest/runner/module/config 文件集合共 350 项，SHA256 为 `9d559879ae8d60def4244fde84997bff85a7395b232cb9f4247ab472deca3172`。计算范围为 `rg --files api cmd sdk loom internal integration guest protocol scripts deploy image operator` 中 `.go/.rs/.proto/.sh/.py/.toml/.yaml/.service/.nft`、名为 `go.mod/go.sum/Dockerfile` 的文件，以及根 `go.mod/go.sum/go.work/go.work.sum/Cargo.toml/Cargo.lock/Makefile/.golangci.yml/.github/workflows/check.yml`；去重排序后逐项拼接 `path UTF-8 + NUL + SHA256(file) 原始 32 bytes`，对结果取 SHA256。文档/历史与 readonly reference/third_party 不在此集合。native run 另保留原 entrypoint hashes、JSON、stdout/stderr/原 child status。GitHub CI 未执行；已验证的是本地同入口与工作流配置。
