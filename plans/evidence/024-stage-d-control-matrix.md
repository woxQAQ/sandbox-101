# 024 阶段 D 完成时的控制协议与验证

本文件绑定阶段 D 完成时的源码与验证，历史状态为 `STAGE_D_PASSED / STAGE_E_NOT_STARTED`。后续 E 的入口迁移记录在 [阶段 E 证据](024-stage-e-entrypoints.md)，当前 F 的清除、新树验证和 VM BLOCKED 项见 [阶段 F 证据](024-stage-f-retirement.md)。这里的 D 结果不作为后续树通过证据；017/020/021 仍暂停。

阶段 D 完成时的 Haven prototype schema 为 **8**。旧 prototype、旧 engine/sandboxd 格式均拒绝，不做自动迁移。所有测试使用自建私有状态和显式注入的效果；真实 OCI 工具测试单列。这里的通过不证明 KVM、guest boot、kernel-isolation 或 production。

## 已迁入的控制协议

| 行为 | 当前实现 | 实际验证 |
| --- | --- | --- |
| build / golden 发布 | `template_admission.go`、`build.go`、`template_publication.go` | `template_test.go`：scratch/build slot、固定 warmup intent、未知 exec 不重放、源 snapshot/费用保全 |
| refill / ready pool | `refill_admission.go`、`refill.go`、`pool_types.go` | `template_test.go`：零 sandbox slot 的 warm birth、pool occupation、退避与重启 |
| warm 领取 / golden fallback | `warm_claim_*.go`、`warm_resource.go`、`provision_handoff.go` | `warm_claim_test.go`：选择同事务收费、精确回收失败 warm 后才 fallback、immutable SlotAssignment |
| failed / outdated supply cleanup | `supply_cleanup*.go`、`allocation_fence.go` | `supply_cleanup_test.go`：独立 node scope、阻断新资源工作流、原结果不变、pool slot 保留至原 release/discard 完成、真实重开 SQLite |
| checkpoint binding 删除 | `checkpoint_delete.go` | `checkpoint_delete_test.go`：只释放业务引用；GC 删除 canonical publication 后 fork backing 仍保留 D+M，最后 backing 回收才退费用 |
| GC inventory / dry-run / batch | `gc*.go` | `gc_test.go`、`gc_budget_test.go`：dry-run epoch 不变、未知目录保留、内部 sleep snapshot、batch/cancellation、独立 child cleanup |
| OCI 导入 / 发布 | `image_import*.go`、`loom/imageimport/` | `image_import*_test.go`：真实 layered/merged OCI 工具、原 manifest/inode 发布、rename 后恢复、不从源重造输出、相同字节替换目录拒绝 |
| import / cache 独立回收 | `image_cleanup*.go`、`image_import_budget.go` | `image_cleanup_test.go`、`image_cache_boundary_test.go`：cache 独立容量责任、失败导入原终态不变、持久 inode receipt、FD 与替换对象阻断 |
| 同一 metadata backup / review | `backup_linux.go`、`store_linux.go` | `backup_test.go`：停服独占 writer lock、同一 DB/key/HEAD、业务与资源事实同点、恢复先 gate、ack 后仅元数据诊断，pending/GC/数据接纳不获授权 |
| configure 撤权与新配置分离 | `configure.go`、`configure_intent.go`、`activation_failure.go` | `configure_safety_test.go`：runtime/info/activate/bind 失败；settings、running、final settings、fence、result、COMMIT 拒写；原 fence/result/lease 保留 |
| 最终数据接纳与 shutdown | `data_admission.go` | `data_admission_boundary_test.go`：建立连接期间配置、可信实例、租约与关闭变化均拒绝；未结束的 dial 保持 writer lock |
| direct 策略生命周期 | `direct.go`、`direct_retirement.go`、`network_retirement.go`、`data_policy.go` | `direct*_test.go`：off → policy → Admin → on；原 binding/instance 退休；撤权未知保留 pending；重开接续；旧 callback 不影响新实例；实查 policy，观察旧 generation 不能覆写新配置 |
| 模板授权与发布 provenance | `template_admission.go`、`vm_admission.go` | `template_owner_test.go`：root 显式 owner、永久 owner、冻结发布 step 与 catalog 一致；私有 fixture 也通过真实模板接纳建立初始发布 |
| 操作权限与完整请求身份 | `authorization.go`、`sandbox.go` | `authorization_test.go`：sandbox credential 不能查询 fork/其他 sandbox；可查询自己的 lease；HMAC 包含认证 role 与 scope，普通 `root` 租户不能重放管理员 intent |
| 启动前 step / SQLite 只读解释 | `store_workflows.go`、`store_probe_linux.go` | `store_workflows_test.go`：损坏 warm phase 在 writable connection/worker 前拒绝，原 state 字节与目录项不变；无 sidecar 时 immutable，完整 WAL/SHM 在 writer lock 下复制至私有临时 probe；真实 abrupt SQLite 恢复继续通过 |
| binding reconcile / 重启重绑 | `binding_reconcile.go` | `binding_reconcile_test.go`：none 策略清除 tracked/untracked 残留 proxy，保留健康 workload 连接、不重放 Admin；重启重绑先于接纳且不续租；secret 与已有配置 fence 不自动重开 |

节点维护的 GC、image import、image cleanup 和 supply cleanup 使用 `scope_kind=node`、稳定 store identity；不占用或伪造 tenant scope。`GetManagementOperation` / `WaitManagement` 与业务操作查询分开。

`loom/imageimport` 仅接收冻结 OCI plan、路径、目录身份和输出证明，返回实际产物与 cache witness。它不访问 Haven 数据库，不持有控制 operation，不执行自主发现、重试、GC 或恢复决策。`loom/artifact_deletion_linux.go` 只检查原准备目录和明确 producer allowlist，初次缺失不能形成删除证明。

## 原行为的当前映射

以下对应主迁移清单中的控制行为。旧跨库 recovery JSON、Find/Wait receipt 和独立 allocation 采纳已由不可变 step 输入、同库 resource-link 与原子事务替代；旧字段不能继续充当删除授权。它们的 owner/实例/请求/目的地/历史/费用断言分别落在冻结输入约束、关系 proof 和下面的行为检查中。

| 原控制测试来源 | 当前行为检查与结构约束 |
| --- | --- |
| `engine/{engine,lifecycle,operations,operation_completion,admission}_test.go` | `vm_lifecycle_test.go`、`vm_crash_test.go`、`vm_ownership_test.go`、`vm_responsibility_test.go`：完整请求幂等、真实 abrupt SQLite 恢复、late completion、ABORT/IGNORE、缺 owner/来源、已替换磁盘、关闭期间 writer custody；snapshot 预算在 `snapshot_test.go` pause 前检查 |
| `engine/{snapshot,restore,restore_materials,restore_cleanup}_test.go` | `snapshot*_test.go`、`restore*_test.go`、`discard*_test.go`、`capture_discard*_test.go`：捕获/同步/发布区分、固定摘要、lease→backing、源删除后恢复、运行盘不重造、所有 generation 的 stop/cleanup、未知 sibling 保留、原失败终态保留 |
| `engine/{gc,gc_journal,admin}_test.go` | `gc*_test.go`、`image_cleanup_test.go`、`image_cache_boundary_test.go`、`snapshot_delete*_test.go`、`store*_test.go`、`backup_test.go`：规范化根、未知引用阻断、batch/cancel、持久 inode receipt、部分 unlink/reopen、FD/替换对象保留、最后 backing 费用退休、readonly 拒绝与 recovery gate |
| `engine/{image_catalog,image_compat,image_shared,fsmeta}_test.go` 的控制断言 | `image_import*_test.go`、`image_cleanup_test.go`：两种真实布局、digest 的唯一物化选择、固定目录/inode/manifest 发布、rename 后接续、已替换输出拒绝、独立 cache 费用。OCI hardlink/whiteout/opaque/forward-link/设备表/工具隔离仍由同一 `loom/host` 机制执行；现存 Engine 机制测试在本次 native gate 通过，旧入口清理与必要测试文件搬迁归 F |
| `sandboxd/{allocation,allocation_recovery,instance_cleanup,recovery,sandbox_recovery,snapshot_recovery,quota}_test.go` | 同库 `vm_*`、`restore_*`、`discard_*`、`snapshot_delete_*` 与 `lifecycle_test.go`：事先固定候选与历史 witness、真实 SQL 拒写、不以当前查询补历史、独立 collector、slot/磁盘/共享 snapshot/旧版本责任、root 计实际 owner、cleanup 后不改公开终态；`gc_roots.go` 验证 lease/backing 与规范化引用，不从文件名接管 |
| `sandboxd/{lifecycle,checkpoints,fork}_test.go` | `lifecycle_test.go`、`checkpoint_delete_test.go`：失败 capture 保源、paused release 不唤醒、resume 失败回原 sleep、并发透明 resume 合并、running/paused rollback、fork 原 owner、checkpoint 删除后独立 backing 继续有效 |
| `sandboxd/{configure,data_admission,direct,reconcile}_test.go` 的接纳与撤权断言 | `configure_safety_test.go` 的 12 类失败与 SQL/COMMIT 矩阵、`sandbox_activation_test.go` 的 superseding fence/secret restart、`data_admission_boundary_test.go` 的最终 guard、`direct*_test.go` 的原实例撤权/重启/迟到 callback/策略实查；`lease_test.go` 验证不复活过期租约及 stale expiry，不用恢复替代 renew |
| `sandboxd/{template,warm_pool,warm_selection,warm_fallback}_test.go` | `template_test.go`、`template_owner_test.go`、`warm_claim_test.go`、`supply_budget_test.go`、`supply_cleanup_test.go`、`store_workflows_test.go`：永久 owner、显式 root 目标、冻结发布 provenance、预算类别、pool occupation/退避、原 warm birth 与 claim SlotAssignment 分开、坏 warm 实际退休后才 fallback、目标/历史不可变、启动前拒绝损坏 step。旧 mutable attempt/history/dispatch 布尔不再独立授权；对应事实由持久 child 请求和只增不删的 operation/step 历史表示 |
| `sandboxd/{authorization,error_boundary,operations,service,backup,store}_test.go` | `authorization_test.go`、`template_owner_test.go`、`sandbox_activation_test.go`、`backup_test.go`、`store*_test.go`：外来/缺失对象不授予权限、sandbox scope 与 root role、Env 不进入公开 JSON、持久错误公开脱敏、同 DB/key/HEAD 备份、旧/未来/缺失证明在 writable connection 与 worker 前拒绝、review 不重放且不授予数据/GC 权限 |
| `sandboxd/egress/*_test.go` | 已复制至 `internal/egress/` 并纳入同一 native/race gate；保留真实 DNS/TLS/HTTP、secret 注入/审计、撤权和连接预算行为；Loom 不持有业务 secret |

HTTP 路由、Preview、Upgrade、SDK、真实 Rust fixture 的新入口连接与两个 CLI 的同进程验证按计划 E 实施；KVM、kernel-isolation 和 production 的最终迁移验收按 F 实施。它们未被本页的宿主 fixture 或 OCI 工具通过替代。

## 已运行的检查

均通过 `nix develop --no-write-lock-file`，使用新建 `/dev/shm/h24.XXXXXX` 私有短临时根，未增加原 timeout 或修改资源限制。

| 检查 | 结果 | 临时日志 |
| --- | --- | --- |
| 完整 Haven 组件 `go test ./internal/haven/... -count=1 -timeout=120s` | PASS，69.690s；早于后续 configure/direct/权限补充 | `/tmp/haven024-d-complete-components.log` |
| supply cleanup / GC / OCI / backup 专项 race | PASS；OCI 设置 `SANDBOX_REQUIRE_EROFS_TESTS=1` | `/tmp/haven024-d-node-backup-race.log` |
| configure 12 类故障矩阵 | PASS | `/tmp/haven024-d-configure-matrix-race.log` |
| 最终数据 guard、configure 与 backup race | PASS；修正 shutdown 竞态后重跑 | `/tmp/haven024-d-final-guard-fixed-race.log` |
| direct / configure / data / secret race | PASS | `/tmp/haven024-d-direct-admission-race.log` |
| direct 新实例 / 重启 / shutdown 路径 | PASS，6.761s；同次 lint 另报 4 项风格问题，已修正，尚需最终 lint | `/tmp/haven024-d-direct-complete-fixed.log` |
| GC batch / cancellation / cache reader | PASS；同次 replacement 的 fixture cleanup 权限报错，单列修正重跑 | `/tmp/haven024-d-gc-boundaries-race.log` |
| cache replacement 修正后的专项 race | PASS | `/tmp/haven024-d-gc-replacement-fixed.log` |
| 模板 provenance、root owner、sandbox operation 权限及 admission 原子性 race | PASS | `/tmp/haven024-d-template-owner-fixed-race.log` |
| 当前完整 Haven 组件 `go test ./internal/haven/... -count=1 -timeout=120s` | **PASS，79.042s**，包含最后的 binding reconcile / restart 补充 | `/tmp/haven024-d-final-components.log` |
| 当前全 workspace lint | **PASS，0 issues** | `/tmp/haven024-d-workspace-final-lint.log` |
| 当前根模块 vet、Loom 依赖边界、diff/保留文档检查 | **PASS** | `/tmp/haven024-d-final-static.log` |
| 当前完整 required-native / race | **PASS：923 pass，0 fail，0 skip**；Haven **286.917s**，Engine **41.474s**；JSON/race/count=1/每 module 300s 保持不变，覆盖最后的 binding reconcile | `build/native-gate/run.2a99sfv7`；`/tmp/haven024-d-delivery-native.log` |
| 维护补充前的完整 required-native / race | 历史 PASS：919 pass，0 fail/skip；Haven 284.630s、Engine 41.624s | `build/native-gate/run.soayrf_u` |
| 之前供给树完整 required-native | 历史 PASS：861 pass，0 fail/skip；不作为新增实现的通过证据 | `build/native-gate/run.l296boz3` |

保留发现与修正：required-native 曾在 `run.y78is58b` 触发原 300s 超时，后仅对已有私有矩阵使用既有两并发调度，未删断言；数据连接建立与 shutdown 曾出现关闭数据库后的 nil panic，现将进行中的数据调用计入关闭等待并保持已关闭 DB 对象可返回错误；direct shutdown 曾错误借用取消后的 service context，现将显式 cleanup context 贯穿关系与当前实例查询。

## 阶段 D 交付边界

阶段 D 已完成：关键工作流及其持久责任迁入 Haven，旧控制行为已有上表中的当前映射，当前树完整组件、完整 race/native、workspace lint/vet 与 Loom 提取依赖门槛均通过。native 的完整根 module 检查使用 `go -C . test -json -race ./... -count=1 -timeout=300s`，包含完整 `internal/haven`，严格于 D 设计门槛的 10 分钟 race 上限；未另用更宽超时替代。

以下仍属于 E/F，未执行或未声明完成：唯一 `cmd/haven` / 两个 CLI / 管理 adapter、HTTP/SDK 与真实 Rust fixture 的新入口连接、旧 daemon/module 清除、KVM/kernel-isolation/production 迁移验收及部署切换。最终架构 gate 明确未运行，不以 `--phase loom` 的提取通过替代。当前没有运行生产工作负载或停止已有 daemon；不从本阶段完成推导继续暂停计划、发布或切换生产入口的授权。
