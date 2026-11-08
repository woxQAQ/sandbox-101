# Plan 010：为 restore/clone 候选材料建立持久完成证明

> 执行者逐步验证，STOP 时报告，不重建正在使用的磁盘。完成更新索引 010，reviewer 管理时交付证据。先执行 `git diff --stat b93fc24..HEAD -- engine/operations.go engine/engine.go engine/backend.go engine/storage.go engine/restore_materials.go engine/restore_materials_test.go engine/engine_test.go engine/gc.go engine/gc_files.go engine/gc_test.go engine/INTEGRATION.md`，再核对未提交差异。011 后续接续本计划，禁止覆盖其已落地改动。

## 状态

- Priority：P1；Effort：M；Risk：MED；Depends on：无。
- Category：bug；Planned at：`b93fc24`，2026-11-06；Status：DONE（源码独立APPROVE，匹配源码guest的完整双格式KVM及实际Info已通过；未合入用户分支）。
- 审计映射：发现 5。

## 问题与契约

restore 校验完整源快照，却以目标文件“存在”判断复制完成。进程中断可留下未完成 config/state；磁盘 rename 后、同步前掉电可留下损坏副本。重放随后同步并释放源 lease，使受损磁盘与原 RAM/设备状态组合。

目标是在源 lease 内完成候选集的完整性校验、文件与目录同步、持久证明，再转移依赖；证明之后只接管，绝不覆盖已运行的可写盘。内存仍用同文件系统的不可变 hardlink，保持 COW、独立磁盘和源快照可独立删除契约。

## 当前状态与范例

`engine/operations.go`：

```go
if _, x := os.Stat(dst); os.IsNotExist(x) {
    if err = copyFile(filepath.Join(s.Dir, "ch", name), dst, 0o444); err != nil { return err }
}
// disk.raw 同样存在即跳过，随后 syncTree，再将 Journal.Lease=false
```

`engine/storage.go`：

```go
out, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_RDWR, mode)
// 之后 reflink／稀疏复制；失败 defer 无法覆盖 SIGKILL
```

`engine/backend.go` 的 journal 已有 Resource/Old/Lease/Manifest，但没有候选副本完成证明。`copyFile` 故意不 fsync，快照捕获要求源恢复运行后再同步，禁止全局改变此行为。`CloudHypervisor.Prepare` 验证 image，不验证候选磁盘和源快照的等价性。

匹配 `engine/engine_test.go` 的 openTest、awaitStep、ErrInterrupted，以及 `TestSnapshotCrashRejectsLostOrDamagedMaterials` 的 truncate/zero/remove 故障模型；测试必须经过 Engine 生命周期，不单测复制 helper。`TestCloneRecoveryAfterBackingTransferAndSnapshotDeletion` 是依赖转移范例。

## Scope

允许：drift check 列出的路径；`gc.go/gc_files.go/gc_test.go` 仅新候选阶段的归属与依赖处理，`storage.go` 仅必要的内部复用，不改变 copyFile 捕获同步契约。新增 restore_materials.go 和对应生命周期测试文件。

禁止：VMM/launcher/guest、image 格式、快照捕获 005 的协议、放宽 COW proof、自动迁移旧快照。候选不完整时保留来源和证据，不递归删除未知路径。

## 命令

根目录 Nushell，现有：

```nu
with-env {GOWORK: 'off'} { ^go -C engine test . -run 'Test(CloneRecovery|RestoreUses|SnapshotCrash)' -count=1 -timeout=120s }
with-env {GOWORK: 'off'} { ^go -C engine vet ./... }
make lint
```

新增名称以 `go -C engine test . -list '^TestRestoreCandidate'` 核对。原生/特权验收所需材料不可用时保留 BLOCKED，不能借旧日志判 DONE。

## 步骤

### 1. 添加目标副本损坏回归

在 restore_materials_test.go 通过实际 Engine 操作创建源快照，在候选复制／rename／同步边界注入 ErrInterrupted。需要新增明确 Fault checkpoint 以分开“copy 返回”和“完成证明已提交”，不把 callback 留在业务 API。对 config、state、disk 分别模拟截断、等长改写、删除；不接触真实状态。

证明前可从仍受 lease 保护的源重建，或明确失败保留证据；不得成功使用损坏副本。证明后损坏必须明确失败，不重新认证篡改字节。新增 `TestRestoreCandidateInterruptedCopy`、`TestRestoreCandidateDamageBeforeHandoff`。

**Verify**：运行这两个名字的 focused `go -C engine test . -run '^TestRestoreCandidate(InterruptedCopy|DamageBeforeHandoff)$' -count=1 -timeout=90s`；旧行为应 RED。不能假设底层恰好不用 reflink，测试应明确构造待重放的不完整候选来覆盖逻辑边界。

### 2. 实现候选集完成证明

在 journal 新增内部 RestoreMaterials 记录：候选 Resource 身份、config/state/disk 的固定源摘要和候选摘要、阶段完成标志。使用已验证源 manifest 的固定值，不在恢复时以重新 hash 损坏目标生成新的合法摘要。memory hardlink 要继续核对同 inode。

证明前：只在属于当前 operation 的私有版本目录内生成临时副本，同 FD 校验 regular/size/摘要，sync 文件、rename、sync 子目录及版本父目录，再持久化候选集完成证明。仍有源 lease 时遇到旧的不完整候选可安全重新准备，但必须先证明没有该候选的活跃/未知 VMM；不满足则拒绝并保留。

证明后：在同一 SQLite transaction 保存 backing、移除 lease、持久化 ready journal；后续重放不得从源重新复制磁盘。无证明的历史已交接 journal fail closed。证明只用于“交付前的材料”，VMM 运行后的 disk 合法变化不应被当成损坏，禁止每次接管强制比对原磁盘摘要。

必须同时修正通用 finish：当前 operations.go 的 finish 无条件释放 Journal.Lease、清空 VM.OperationID。候选已准备但损坏、在用状态不确定或缺少必要证明时，先持久化内部 RestoreBlocked 标记，再固定原 operation 的 failed 结果，但保留尚在手的源 lease、候选/backing/reservation 以及 VM 操作归属。GC 将其视为未解决资源根；Reconcile 不得因为原操作终态而清除阻断。尚未产生候选且有充分无副作用证明的普通准入错误仍走原收尾。已经转移后不存在的源 lease 不能凭空重建；此时保护已转移的候选与 backing。

显式解除路径仅允许原有 delete 动作：engine.Submit 可对指向终态失败 RestoreBlocked 操作的 VM 接受新 delete 意图，条件校验原 operation、VMID 和调用方实例；新 delete journal 持久记录原 blocked restore ID，原 lease 在物理清理前继续保留。delete 必须 inspect/停止并确认当前、Old、候选所有已知实例和设备都不再使用，再回收整个 VM 的拥有资源。身份未知则保留阻断，不能清记录。delete 失败后新 delete key 只能接续这一原始 blocked ID；不得改变原 restore 的终态结果。确认全体资源删除后，同一事务移除 VM/账本并释放原 lease、更新原 journal 的内部清理完成标记，原 Operation 的 status/error/result/时间保持不变。不能通过普通 start/restore 或 blanket recover 清 fence。

**Verify**：上一步变绿；`TestCloneRecoveryAfterBackingTransferAndSnapshotDeletion`、现有 SnapshotCrash 回归通过。检查测试覆盖了证明前/后两类处理。

### 3. 覆盖交接与使用后的边界

新增 `TestRestoreCandidateHandoffSurvivesSourceDeletion`、`TestRestoreCandidateRunningDiskIsNotRebuilt`：源删除后从保存证明接续；已 launch 的磁盘写入继续保留；未知实例不覆盖、不清理；旧无证明 journal 拒绝。再新增 `TestRestoreCandidateRetentionAndExplicitDeletion`：损坏/不确定错误固定为 failed 后，lease/VM fence 仍在，源删除、新 start/restore 和 GC 被阻止；明确新 delete 在所有实例已证停止后清理，丢响应/失败可用原 blocked ID 接续，最后释放 lease，原失败 Operation 的序列化结果逐字节不变。GC 仅把新阶段登记为既有 operation 资源，不能回收仍受 lease/实例保护的材料。

**Verify**：

```nu
with-env {GOWORK: 'off', SANDBOX_REQUIRE_EROFS_TESTS: '1'} { ^go -C engine test -race ./... -count=1 -timeout=180s }
with-env {GOWORK: 'off'} { ^go -C engine vet ./... }
make lint
git diff --check
```

真实 KVM 命令如下，Nushell 变量必须由隔离验收资源交付明确设置；不存在即 STOP/BLOCKED，不自动查找旧运行材料：

```nu
make kvm-e2e $"KVM_MATERIALS_DIR=($env.PLAN_KVM_MATERIALS)" $"KVM_GUEST_DIR=($env.PLAN_KVM_GUEST_DIR)" $"KVM_ARTIFACTS_DIR=($env.PLAN_KVM_ARTIFACTS)"
```

三个路径分别是已验证的 pinned VMM/kernel 材料、匹配 guest、此次新建且 stat/maps 身份一致的专属 artifact 目录。实际 clone→源删除→resnapshot/restore 必须通过。缺材料只能记录 BLOCKED；不启动重复材料构建、不放宽 COW 检查。

## 完成标准

- [x] 五个 TestRestoreCandidate 测试实际执行，损坏不被成功交接，通用 finish 不偷偷解除来源保护。
- [x] RestoreBlocked 后只有明确 delete 在核实全部实例并完成清理后才解除；原失败公开结果不可变。
- [x] 完成证明在文件/目录同步之后，依赖转移与 ready journal 原子提交。
- [x] 已运行磁盘不重建，源删除后继续接管，未知实例/旧无证明日志拒绝。
- [x] Snapshot/GC/Clone 原回归、race/vet/lint/diff 和真实 KVM 验收通过。
- [x] 文档包含阶段表与历史状态边界；Scope 和索引记录实际结果，缺必要验收标 BLOCKED。

## 已实施与未完成验收

r2源码独立APPROVE，整体仍BLOCKED。候选 `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-2c14242b`，分支 `t3/fix-restore-cleanup-gc`，采用 `270eb47e63bfa28b185b6fc2f88ff2e8b182bcfc`→`7df482995c75a62795eacadde870d82d9e80948d`；tree `74d93ebbd9a6fc3b52e3080f3e9dacded16015d5`。独立r1发现生产Inspect与已知live sibling冲突、历史failed delete使GC永久阻断；r2已分别修复，原公开终态不变。独立r2重跑required native race128.940s零skip、focus37.626s、vet/lint/diff及unknown file/inode替换overlay均过，完整报告 `/tmp/p010-r2-review.VXjVuh/report.md`。

实际KVM依次暴露测试存储前提：默认8GiB tmpfs在merged import容量不足；plain bindfs/Btrfs的0400 O_RDWR FD truncate EACCES；fuse-overlayfs两格式copy_file_range EACCES。r3fresh namespace24GiB tmpfs+bindfs经host proc bridge运行原make，仅对确切artifact根授权realpath adapter；全部文件/COW/map身份探针通过，但Go modernc SQLite WAL与两种实际格式Engine初始化均error14 `unable to open database file`。原make exit2/wall3.630s，零skip且未启动VMM，boot/clone/COW全部未到达。报告 `/tmp/p010-r3.4mkybi8y/report.md`；所有自有mount正常卸载、helper/daemon退出0，材料hash前后一致。

r3当次OCI已仅归档至 `/tmp/p010-r3.4mkybi8y/artifacts/kvm-acceptance-1854451212/oci`，6个blobs/descriptor digest+size重新校验；仅是新鲜完整输入，非boot证明。r4完整原make已实际执行：exit2/wall8.738s，零skip。layered-erofs-v1完整PASS4.86s，boot/clone/source capture删除/独立写盘/COW resnapshot/grandchild restore均到达，两次RAM原inode rw-p覆盖各512MiB。merged-oci-erofs-v1在原builder的嵌套unshare执行newuidmap时EPERM，0.53s/未启动该格式VMM。直接tmpfs的SQLite WAL与原文件探针均过，Go/test/三个VMM实际及namespace UID1000/GID100、所有capsets0、NNP1；材料hash/tree不变，正常unmount/helper0且无进程残留。报告 `/tmp/p010-r4.joIbnoSg/report.md`。完整双格式仍BLOCKED，011不启动。r5仅验证嵌套mapping前提，现已PROBE_PASS：私有nosuid,nodev tmpfs+可表示的既有subID子集，Go/test保持UID1000/GID100、Eff/Prm/Inh/Amb0、普通bounding/NNP0，原host wrapper正常工作，无私有SUID helper；nestedunshare0.061s及两个原actual OCI metadata测试.094s零skip通过，SQLite/文件探针也过。原VMM launcher仍强制NNP1/dropBnd不变。报告 `/tmp/p010-nested._4tfbbaa/report.md`，所有自有mount普通卸载、进程退出。当前VM槽位已交012生产门禁；010完整r6双格式须在其明确释放后新派，前置PASS不等于KVM完成。

## 最新：r6双格式PASS，匹配源码guest补验

父完整核对r6report/cleanup/integrity：候选tree74d...不变，原make exit0/wall12.539s，layered4.65s/merged4.59s实际RUN/PASS零skip，boot/clone/sourcecapture删除/独立disk/COWresnapshot+grandchildrestore完整；四次原inode rw-p各512MiB。六VMM真实host/namespaceUID1000/GID100、Eff/Prm/Inh/Amb0。所有资源普通清理、slotrelease。证据 `/tmp/p010-r6.AL80kzLv/report.md`；主test comm截断使直接身份采样缺失已披露，不用Go/其他test样本冒充。

重要范围更正：原standalone测试:35/797直接exec CH，不运行managedproduction InternalLaunch。父原brief把production NNP/dropBnd当硬条件无源码依据，已同活动round纠正；实际VMM运行NNP1/Bnd普通非零，不能宣称生产isolation证明。原测试/source/hash/COW证明均未改，不是放宽原门禁。

随后012兼容失败静态诊断揭示旧build/guest/sandbox-agent实际为product→workload改名前构建。r6验证file/tree/exec/COW但没有Info身份断言，不能据其PASS声称binary源匹配b93。父找到同源码已缓存Nix产物05dh...（新agentSHA2463ee...ddb，旧5802db...8ad，init未变），已新派prepare-corrected-guest-oci-r1在OWNcheckout新打包，原双格式和实际UNCHANGEDagent.Client.Info验证后再放行最终DONE。旧材料保留，原r6行为证据有效；011已在已批准010源码上隔离实施，但最终批准/组合待此匹配材料门禁。当前唯一VM槽位归新材料任务，012r2无VM授权。

## 最终复核：来源缺口关闭，APPROVE

匹配材料任务MATERIAL_READY后，父已读完整report/handoff/cleanup，并独立核对newguest三文件SHA及production base OCI六个reachable descriptor/digest/size/index。工作树t3-70aca3ea，sourcecommitb777855881487fe6dbd572ab81badb627f66e53d的tree仍74d93ebbd9a6fc3b52e3080f3e9dacded16015d5；26个过滤guest源文件与b93、已存在Nix derivation source逐项相同，缓存package05dh...新agent2463ee...ddb，init/initramfs不变，旧材料32文件hash完全保留。

使用newGuestDir的唯一原make完整双格式exit0/wall14.090s，layered5.27s/merged5.25s实际PASS零skip，四512MiB原inode完整rw-p、所有boot/clone/source删除/独立盘/resnapshot/grandchild原断言通过。六实际VMM全部用未修改agent.Client.Info的2048 portalworkload/protocolfloat64(1) guard成功，不再只凭路径/strings宣称源码匹配。Go/主test/全部VMM双视角UID1000/GID100及Eff/Prm/Inh/Amb0完整；明确standalone不证明managedproduction。source/输入不变，全部owned进程wait、3mount普通卸载/无残留、slotrelease。完整 `/tmp/p012-newguest.vcowRD9Q/report.md`。结合此前源码独立APPROVE与required-native128.940s零skip，010所有必要gate满足，DONE；011前置材料hold解除。

## STOP 与维护

需要修改源快照真实性、扩大 root/VMM 权限、无法区分候选是否已交付，或无法保守判断在用实例时停止。不要以重算摘要、强制清盘、修改 capture 同步顺序解决。未来调整候选布局时同时审查 journal、lease、GC 和实例接管。分支 `advisor/010-restore-materials`；另获提交授权才使用 `engine: persist restore candidate completion proof`。
