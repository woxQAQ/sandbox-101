# Plan 005：隔离 VMM 捕获写入区与 root 快照发布区

> 本文是可执行设计与实现计划，供没有本次审计上下文的执行者使用。完整阅读后逐步执行验证。遇到 STOP 条件先提交证据和受影响约束，不临时降低隔离要求。完成后更新索引 005，除非 reviewer 维护索引。
>
> Drift check：`git diff --stat da513a6..HEAD -- engine/backend.go engine/cloudhypervisor_linux.go engine/launcher_linux.go engine/operations.go engine/storage.go engine/capture_linux.go engine/engine.go engine/engine_test.go engine/gc_files.go engine/gc_journal.go engine/gc_test.go engine/gc_journal_test.go engine/capture_isolation_linux_test.go scripts/run-capture-isolation-acceptance.sh engine/INTEGRATION.md Makefile deploy/host/README.md deploy/ACCEPTANCE.md`。同时检查未提交差异；新建文件也在 Scope 中。本文基于 `da513a6`。

## 状态

- Priority：P1
- Effort：L，含跨 UID 文件系统集成与真实 VMM 回归
- Risk：MED；涉及捕获、封存、持久 journal、空间预算及 GC
- Depends on：无，可在独立 checkout 与 sandboxd 计划并行
- Category：security
- Planned at：`da513a6`，2026-11-06
- Status：DONE；落盘先于固定哈希 proof、损坏拒绝、跨 UID/GC/KVM 及最终组合 production 验收通过，见 [实施与验收记录](IMPLEMENTATION.md)。

## 问题与目标

engine 以 root 准备宿主资源，VMM 在私有 namespace 内以非 root UID/GID 运行。当前整个 capture 父目录都交给 VMM，root 随后用宿主字符串路径 mkdir/chown/封存。VMM 被攻陷时可替换其可写目录项，导致特权文件操作跟随链接到 capture 外部。

本问题以 VMM 进程已被攻陷为前提，不要求调用受信任 engine UDS。修复目标是恢复宿主隔离边界：VMM 可以产生不可信捕获文件，但不能决定 root 操作的宿主目标，不能在快照发布后继续修改发布 inode。

## 当前代码和需要保留的契约

`engine/launcher_linux.go:139` 将整个 capture 可写挂载：

```go
{filepath.Join(s.Resource.Dir, "capture"), "capture", false},
```

`engine/cloudhypervisor_linux.go:636`：

```go
for _, path := range []string{r.Disk, filepath.Join(r.Dir, "capture")} {
    if err := os.Chown(path, int(b.opts.UID), int(b.opts.GID)); err != nil {
        return err
    }
}
```

`CloudHypervisor.Capture` 的顺序：

```go
if err := os.Chown(filepath.Dir(dst), int(b.opts.UID), int(b.opts.GID)); err != nil {
    return err
}
if err := os.Chown(dst, int(b.opts.UID), int(b.opts.GID)); err != nil {
    return err
}
rel, err := filepath.Rel(filepath.Join(r.Dir, "capture"), dst)
```

字符串 containment 检查发生在副作用之后，且不是 filesystem 身份验证。`os.Chown` 跟随符号链接，先 Lstat 再 Chown 也无法解决并发替换。

`engine/operations.go:463` 在 `vms/<VM>/capture/<operation>/ch` 捕获，源暂停期间调用 Capture 并复制磁盘，设置 `Journal.Captured`，恢复源之后才 hash/seal/sync/rename 发布。`storage.go:195` 的 `sealTree` 只对目录 chmod，目录仍可能归 VMM；即便收回目录所有权，VMM 已打开的写句柄仍可修改原 inode。

相关代码职责：

- `backend.go`：Resource、Backend.Capture 和持久 journal；均为未发布的内部接口，可按需要重新设计，不保留多余兼容分支。
- `storage.go`：copy/reflink、hashTree、sealTree、syncTree。
- `operations.go`：捕获阶段、恢复源、发布和重放。
- `gc_files.go`、`gc_journal.go`：capture 路径和节点 inode 的删除责任。
- `engine_test.go` 的 `TestSnapshotCrashBoundaries` 使用 `ErrInterrupted` 在持久步骤中断；必须扩展它，而不是新建只验证 helper 返回值的单元测试。
- `scripts/run-production-acceptance.sh` 使用 `unshare --user --map-auto --map-root-user`，提供映射两个身份的隔离测试范例。

保留架构约束：源 VM 暂停窗口只做必要捕获与独立磁盘完成；hash/fsync/发布在恢复后执行。快照包含独立磁盘与可保活的不可变 RAM backing，严格 COW stat/maps 检查不放宽。namespace、VMM 降权、ExpectedInstance、fail-closed 网络与未知归属保留规则都必须保留。

## 选定设计

### 1. root 保持目录结构的控制权

- capture 根目录及每个 operation 的父目录由 root 持有。它们必须允许 VMM 遍历到指定 CH 写入叶子，但不允许 VMM 增删/替换父目录项；例如 root-owned 0711，具体模式以实际需要为最小值。
- 仅 `ch` 写入叶子授予 VMM UID/GID 写权限。准备叶子时先通过可信目录 fd、逐组件拒绝 symlink 的方式创建/打开，再对 fd 执行 Fchown/Fchmod。不得先使用不可信完整路径执行 Chown。
- 移除 Launch 对整个 capture 根目录的 ownership 转移。launcher 绑定的 capture 根可以保持可写 mount，但访问权限必须阻止 VMM 修改 root-owned 父目录项。不要通过给 VMM CAP_CHOWN、CAP_DAC_OVERRIDE 或宿主额外权限绕过。
- `CloudHypervisor.Capture` 只向已准备且身份核实的叶子提交 CH destination，不再对请求传入的任意 dst 路径执行 ownership 操作。可调整内部 Capture 参数以携带明确捕获上下文/目录句柄；命名解释清楚。

### 2. 将不可信产物复制到 root 独占发布区

- 为每个 snapshot 在 root 独占位置创建稳定的 staging，例如目标 snapshots 父目录内的 operation-scoped staging；命名与路径必须进入持久 journal，支持 crash recovery。
- CH 输出仍是不可信文件系统输入。打开其文件时使用固定可信根 fd、拒绝 symlink/越界/非 regular file 的解析方式；验证与读取针对同一 fd，禁止 check-then-open。
- 使用 O_NONBLOCK 等必要打开约束避免特殊文件导致 root 无限阻塞。限制文件类型、文件名、数量和允许的总大小，依据现有 snapshot shape/topology 契约，不能接受任意 CH 目录树。
- 发布区每个数据文件都是新 inode，允许 reflink 到新 inode，不允许 hardlink。VMM 持有源写 FD、保留源目录 ownership 或后续修改源文件，均不能改变发布结果。
- 在独立发布区写 manifest、hash、seal 和 sync。目录和文件均归 root；最终发布保持原子性。不能将仍由 VMM 控制的目录直接 rename 成权威发布目录。
- 源磁盘仍在暂停窗口中完成独立捕获；恢复源后再处理不必要的 hash/fsync。不要为了安全重构把整段 hash/fsync 放进暂停窗口。
- 必须核对额外 staging/独立文件的最坏空间预算：若增加实际峰值占用，更新已有 reservation 公式与其测试；不移除预算检查，不能默默假定 reflink 总成功。

### 3. 重放、封存与清理

- Journal 明确区分 VMM capture、独立发布材料完成、发布 fsync、最终 rename。重放只能使用 journal 与 inode 身份可证明的材料。
- 中断前材料不完整时保留未发布状态并恢复源；不能从目录存在推断捕获完成。
- GC/显式删除同时识别新 staging 与旧 CH 写入责任。只回收已终态、无在途引用、归属已证明的目录；dry-run 不改元数据。
- 调整固定 capture 路径逻辑及 journal 校验，不保留一个因新布局失效而静默跳过的 GC 分支。
- 若原始旧布局在新安全模型下不能安全恢复，保留证据并返回明确 ManualRecovery/Incompatible，禁止自动信任 VMM-owned 路径。正常新操作不需要保留旧内部 Backend 签名兼容。

## Scope

允许修改或创建：

- `engine/backend.go`、`engine/cloudhypervisor_linux.go`、`engine/launcher_linux.go`
- `engine/operations.go`、`engine/storage.go`
- `engine/capture_linux.go`（可新建，专门承载目录 fd 与捕获交接，不作为泛用 utils）
- `engine/engine.go`（仅确有增加峰值占用时的 reservation 及新 journal 初始化）
- `engine/gc_files.go`、`engine/gc_journal.go`
- `engine/engine_test.go`、`engine/gc_test.go`、`engine/gc_journal_test.go`
- `engine/capture_isolation_linux_test.go`（新建系统集成测试）
- `scripts/run-capture-isolation-acceptance.sh`（新建隔离测试 runner）
- `Makefile`（仅新增 capture-isolation-e2e 目标及 PHONY）
- `engine/INTEGRATION.md`、`deploy/host/README.md`、`deploy/ACCEPTANCE.md`
- `plans/README.md`（状态）

不修改 sandboxd、guest、reference/vendor、镜像 importer、网络规则、COW 校验或宿主 NixOS 配置；不引入新依赖或 VMM fork。不改变公开 API 请求/响应字段。

## 步骤与验证

命令使用 Nushell，从仓库根执行。安全测试只针对新建隔离 scratch 资源，不提供通用攻击脚本，也不触碰真实宿主文件或正在运行的 VM。

### 1. 固定状态图和验证入口

在 `engine/INTEGRATION.md` 用短表说明捕获目录、CH 可写叶子、root staging、最终发布目录的 owner/权限/持久步骤。列出现有 journal 步骤将如何映射到新步骤，以及增加的空间预算。此文档必须具体到后续实现可检查，不只写“使用安全路径”。

新增 `make capture-isolation-e2e`（这是本计划新增目标，当前仓库没有），调用新 runner。runner 编译带 `capture_isolation` build tag 的 engine 测试 binary，再在全新 user/mount/PID namespace 中运行；使用 subordinate UID/GID 映射区分 namespace root 与非 root writer，并验证 namespace ID、映射和清理范围。参考现有 production runner，不需要 cgroup、KVM 或 root 提权。

该套件缺少 userns/subuid/subgid 条件时必须失败并报告，不能 skip 后宣称通过。不要修改宿主映射或 NixOS 配置。

```nu
with-env { GOWORK: "off" } { go -C engine test -tags=capture_isolation -run '^$' -count=1 }
make capture-isolation-e2e
```

编译 exit 0；隔离套件先包含场景建立和身份差异断言，随后逐步加入下述回归。若身份映射不可用，STOP，记录缺少条件。

### 2. 固定可信父目录并消除特权路径跟随

实现 root-owned 父目录、非 root 写入叶子和 fd-based ownership；调整 Prepare/Launch/Capture，不给整个 capture 根 ownership。

新增 `TestCaptureIsolationParentOwnership` 和 `TestCaptureIsolationRejectsEntryReplacement`，由独立非 root writer 操作仅属于该测试的目录项；通过实际生产捕获准备路径验证 root 所有权副作用始终限于正确 inode。测试记录的外部 scratch sentinel 仅用于断言 metadata/content 不变，不使用任何系统敏感路径。

```nu
make capture-isolation-e2e
with-env { GOWORK: "off" } { go -C engine test . -run 'Test(Lifecycle|SnapshotCrash)' -count=1 -timeout=90s }
```

预期跨 UID 场景通过，现有生命周期与中断恢复不退化。

### 3. 独立发布 inode 并验证保留写 FD

实现从已验证 fd 到 root-private staging 的新 inode 交接，并更新 manifest、hash/seal/sync/publish 流程。新增 `TestCaptureIsolationPublishedFilesAreIndependent`：writer 在交接前保留源文件写 FD，发布后继续改变源；最终发布内容、owner、hash 不变，writer 无法更改发布树。

新增 `TestCaptureIsolationRejectsNonregularArtifacts`，通过生产交接流程验证非 regular/路径越界/不允许产物被有界拒绝。测试覆盖错误后源不遗留暂停，且不发布不完整快照。

```nu
make capture-isolation-e2e
with-env { GOWORK: "off" } { go -C engine test -race . -run 'Test(Snapshot|Clone|Reservation|COW)' -count=1 -timeout=120s }
```

预期全部通过；不能以原 inode chmod/chown 后“暂时不可写”代替 retained-FD 独立性测试。

### 4. 完成重放与 GC

扩展 `TestSnapshotCrashBoundaries`，覆盖新 capture/staging 交接的每个持久点，包含原始材料已完成但 staging 未完成、staging 已 fsync 但 publish 未完成、publish 后 db 未提交。重启后不得发布半成品，已完成的独立捕获可收敛，不重复启动 VMM。

扩展现有 GC 服务级测试：新 staging、源 capture、发布目录的归属不同；替换 inode 保留；未知材料保留；部分删除重启恢复；dry-run 不写元数据。测试应调用 Engine operation/CollectGarbage，不只单测 path helper。

```nu
with-env { GOWORK: "off" } { go -C engine test -race . -run 'Test(SnapshotCrash|GC|CloneRecovery|Reservation)' -count=1 -timeout=120s }
with-env { GOWORK: "off" } { go -C engine test -race ./... -count=1 -timeout=180s }
with-env { GOWORK: "off" } { go -C engine vet ./... }
git diff --check
```

预期 exit 0；真实镜像工具测试如果被跳过，必须单独按最后一步强制执行，不能将此处 PASS 当成全部验收。

### 5. 运行真实 VMM 验收并保留边界

先确保环境已提供原 README 定义的 `KVM_MATERIALS_DIR`、当前静态 guest、`PRODUCTION_OCI_LAYOUT` 和已验证的 `SANDBOX_PRODUCTION_IO_MAX`。参数必须来自现有材料 receipt/部署配置，不猜测设备预算。只有这些条件满足时执行；缺失时按 STOP 条件报告阻塞，不能自行改系统。

```nu
with-env { GOWORK: "off", SANDBOX_REQUIRE_EROFS_TESTS: "1" } { go -C engine test -race ./... -count=1 -timeout=180s }
make capture-isolation-e2e
make kvm-e2e
make production-e2e
```

预期全部通过，真实 source snapshot → COW clone → 再快照/恢复仍有效；生产 launcher 仍为非 root VMM，UID/GID/capability 没有放宽，capture 发布后的 owner/inode 证据可核对。长验收应由 harness 异步管理并持续汇报，不在父线程做长时间阻塞等待。

将实际新材料、日志位置、命令和结果加入 ACCEPTANCE。未运行的检查明确写未运行，不沿用旧快照验收哈希冒充当前结果。

## 完成标准

- [x] VMM 无法增删 capture 根及 operation 父目录项，只能写指定 CH 叶子。
- [x] root 对不可信产物的操作使用已验证 fd；无先 Chown 再 containment 检查路径。
- [x] 四个命名跨 UID 集成测试实际执行并通过，无 skip。
- [x] 发布文件为 root 独占新 inode；writer 保留的源写 FD 不能影响发布内容。
- [x] 新持久步骤的 crash replay、材料损坏/丢失、GC identity、dry-run 均通过行为回归。
- [x] 空间预算覆盖新增峰值；不依赖 reflink 总成功，不扩大暂停窗口到 hash/fsync。
- [x] engine race/vet、强制原生工具、真实 KVM 和生产 launcher 验收通过并留下当前证据。
- [x] 无 VMM 额外 capability、无 COW/归属校验降级，修改位于获授权计划的 Scope 并集内。
- [x] 真实材料可用，最终组合 production 已通过，未触发材料缺失的 BLOCKED 条件。

## STOP 条件

- 无法在当前固定版本 CH 上证明仅开放写入叶子仍支持实际 snapshot API；先提交布局替代设计，不能给 VMM 整个 root 发布区写权限。
- 需要新增 VMM capability、放宽 namespace、关闭 COW 证明或复用不可信发布 inode。
- 必须改动未列入 Scope 的 importer/系统配置/依赖；提交具体必要性，不扩大修复。
- userns 多 UID 映射、原生工具或真实 VMM 材料缺失，无法验证相应边界。
- 发现旧 journal 无法安全判断 ownership；保留原数据并报告，不递归 chown 旧树“修复权限”。
- 两次合理修正后验证仍失败。

## Git 与维护

使用派发时绑定的 checkout；否则建议分支 `advisor/005-capture-publication-boundary`。若要求提交，使用 `engine: isolate capture publication ownership`；未授权不提交、推送或开 PR。

后续增加 VMM 可写设备/目录时都应检查父目录控制权和 daemon 的特权文件操作，而不只检查最终文件 mode。目录 fd 固定路径解析目标，新 inode 隔离既有写 FD；这两项解决不同问题，不能相互替代。
