# Haven 资源集成契约

宿主 module 位于 `haven/`，路径为 `github.com/woxQAQ/haven`。`haven` 是唯一持久控制权威；`loom`、`loom/host`、`loom/imageimport` 是同进程显式效果库。业务消费者使用 [types](../types/types.go)、[sdk](../sdk/client.go) 和 HTTP；宿主管理使用 [management DTO](../types/management.go)、管理 UDS 与 `loomctl`。没有独立 engine daemon、资源 CRUD UDS 或跨库 Submit/Find/Wait/adopt facade。

配置、鉴权、CLI、统一离线备份与恢复命令见 [Haven control](haven-control.md)，业务资源和配额见 [Sandbox](../haven/sandbox/api.md)。本文维护资源协议与责任边界；[架构](architecture.md) 说明职责和锁，镜像格式见 [镜像物化](image-materialization.md)。

## 构建和依赖

根目录通过 `go.work` 连接 Haven、Loom、SDK、types、common、tools、protocol 和 operator。各 module 的本地 replacements 指向同一 checkout 中的依赖，支持独立 `GOWORK=off` 检查。普通构建不初始化 reference submodules，也不修改第三方 provenance。

```nu
just build
just test check test-race
just integration
just smoke
```

`just lint` 含 `go fix`；只读检查使用 `golangci-lint run ./haven/... ./loom/... ./sdk/... ./types/... ./common/... ./tools/... ./operator/... ./protocol/gen/...`。测试、runner 与实际材料边界见 [scripts](../scripts/README.md)。

## 错误边界

Loom 与 Haven 使用普通 Go error：固定消息用 `errors.New`，上下文用 `fmt.Errorf`。没有通用错误结构体、错误码类型或 `Fail` 工厂。确实需要分支的固定条件用标准 sentinel 和 `errors.Is`，错误文本不授权重放、absence 或回收。`types.ApiError` 仅由 HTTP adapter 生成，进入响应或公开 operation 的 HTTP JSON；冻结记录不保存该 DTO。未分类宿主诊断不会自动变成 tenant 404。guest `error_code` / `result_state` 继续作为协议 params 传递，SDK 不包装成单独的 Go 错误类型。

## Allocation results and mixed versions

operation、subordinate step、候选资源、预算和依赖在同一 SQLite admission 事务中固定。公开 operation 的终态固定；成功资源 witness 在完成事务内记录，不能从当前 VM 状态或较新实例补造。当前归属和历史 witness 都匹配才可推进实例相关副作用。

公开 API 的认证 scope、稳定键和冻结 result 独立于资源 generation。内部 child 通过同库 parent link 接受请求、预算、完成证明与责任目的地，不复制一个远程 receipt 当作事实。SQL 拒写、零行写入、COMMIT 失败都保留原责任，不宣称半完成。

旧双库、旧 prototype、早期 schema 或缺 witness 明确拒绝并保留人工恢复门禁。当前 format/schema 见 [Haven control](haven-control.md)；无推断式兼容、自动合库或目录扫描接管。

## 操作结果交付

`haven/publication.go` 只负责冻结记录的写入、读取和基础核验；完成工作流的同一事务调用它提交终态和结果。`operation_projection.go` 在该事务中构造业务视图，`operation_query.go` 负责权限核验、同一读 snapshot 内查询与按需 token 交付，`catalog_view.go` 负责业务 DTO 转换。查询终态不重新读取当前资源拼历史结果，fork 父结果使用各分支冻结的交付。错误诊断仍留在内部；失败状态由 HTTP adapter 转为固定 ApiError，不解析错误文本，也不从当前状态补造缺失交付。

## Snapshot and restore persistence

### 捕获

1. 同事务冻结源 VM/instance、candidate paths、capture/staging 身份、保守容量与 source resume-required。
2. 核验精确实例，pause CH 内置块设备和 vCPU，capture 机器状态并复制独立 writable disk。
3. 独立捕获完成即恢复源。耗时 hash、独立 publication copy、fsync 与 sealing 在源恢复后执行。
4. 固定文件大小、摘要、目录/inode 和同步完成证明；rename 后同步父目录，再原子发布 snapshot catalog、holder 与费用目的地。

Captured、Materialized、ArtifactsSynced 和 Published 是不同事实。重启只验证原证明，不重新 hash 损坏文件来补造它。capture 父目录 root-owned 0711，仅 `ch` leaf 可由 VMM 写入；publication 使用新的 root-private inode。保留 raw write FD 不得改变已发布快照。

raw capture 有独立持久 cleanup。源 release 可接过该责任，确认 reader 退出、精确删除后才退休；不能因源 VM 删除就假定 raw 与 publication 都已回收。failed capture 清理绑定原失败 step 和同一实例的成功 stop proof，保留源盘、永久 snapshot owner 和原失败终态。

### Restore、clone 和 generation

source manifest、读取 lease、目标身份、完整 shape、路径与复制预留在效果前固定。`RestorePreparation` 整体传递候选、原 operation、source 路径与固定摘要；clone 准备入口可以创建已分配的新 VM 目录，restore 准备入口要求原逻辑 VM 目录仍存在，缺失时不创建替代物。Loom 复制独立可写盘和私有 config/state，创建不可变 RAM hardlink并返回固定 hash/inode/fsync proof。来源 lease 到长期 backing 在完成事务内连续交接。

运行前及 restart adoption 必须核验真实 `/proc/PID/maps` 的 writable-private COW、完整范围、device/inode/offset；拒绝 shared mapping、缺段或 eager-copy fallback。stat/mapping device identity 不一致的 filesystem 不支持该恢复路径。已经运行的 writable disk 不得按历史 digest 重建或覆盖。

停止目标 restore 目前只接纳未交付、仍 provisioning 的 sandbox，要求原 stop operation 固定旧实例退出证明。新 generation 沿用逻辑 VM、slot/runtime budget，保留旧盘、旧 RAM backing 与 immutable ancestry。release/discard 先核验所有历史/当前/候选实例，unknown sibling 阻断；实际回收后同事务退休每代责任。

snapshot 的 D+M 费用与 catalog/路径是不同事实。显式 snapshot delete 要求 raw cleanup 已退休，先提交删除 fence/原 inode receipt，拒绝新 reader/lease。目录删除后仍有 clone backing 时保留 D+M、镜像依赖与删除证明；最后 backing 实际回收才退休费用。开放 FD 或 mapping 不因 unlink 而消失。

### 容量

保守 admission 使用完整上界，reflink、稀疏分配或预计共享收益均不减免未知效果责任。tenant 的 D/M 费用见 [配额责任](../haven/sandbox/api.md#durable-quota-responsibility)；host reservations 还覆盖 runtime、capture/restore 复制、导入 staging/cache/metadata 与空间 margin。预算不能替代实际 byte/inode quota。

## Reference-aware garbage collection

节点 GC 通过 `loomctl --key KEY gc` 进入同一 Haven。`--dry-run` 只读 inventory，不改变 epoch、fence、费用或删除 journal；`--gc-batch`、`--gc-budget` 限定一次处理预算。background maintenance 使用同一资格检查。

根包括 published image/snapshot、current VM、generation/backing、live/在途使用、未解决 step、source lease、pool 与独立 cleanup。规范化引用与原 step 关系必须完整。损坏/未知根保守阻断；未知目录与 cache child 只报告，不因名称、年龄、内容 hash 而接管。

回收按原持久 authority 接纳，先保存删除 fence 和 inode receipt，Loom 核验无 reader/mapping、目录和所有允许 producer 文件，再 unlink/fsync；之后原子退休责任。`ArtifactTopology` 整体固定 producer 目录、文件类型和历史 inode，现场枚举只用于核验，不补造这些允许项。generation 集合是 VM 回收的唯一检查入口；没有保留单代旧规则。部分 unlink 后重启仍沿同一 receipt。替换 inode、missing history 或未知效果保持费用；初次路径缺失不能自动制造删除证明。

可回收对象包括独立 supply/import/cache cleanup、内部 sleep snapshot 和符合完整 generation 协议的资源。published images、永久 authorization/idempotency/operation 历史保持保留。删除 snapshot catalog 不自动退还 clone backing 的责任，cleanup 也不重写原公开失败结果。

## Guest 与网络

System 2047 执行 ping/reseed/clock_sync：冷启动验证协议/portal，restore 用新 entropy 和直接 syscall 校时。Admin 2046 由 Haven 执行 activate/prepare_template/quiesce；Workload 2048 只经已鉴权数据接纳开放。System ready 与 sandbox delivery 分离。帧和 guest 行为见 [Protocol](../protocol/README.md) 与 [Guest](../guest/README.md)。

direct desired policy、configuration generation 与原实例退休责任保存在 Haven，Loom 执行实际 netns/nft/links。off → policy → Admin/Workload → on → final admission 不变；未知 inspect 不是 NotFound，旧 callback 不能影响新实例。详见 [网络策略](network-policy.md)。

## Backup and explicit recovery

停服后的 `haven --mode backup` 取得同一 writer lock，统一保存完整 `haven.db`、node key、HEAD 和 checksummed receipt。业务和节点管理事实位于同一点，不再协调两份元数据备份。资源字节须独立协调保存。

restore 到新私有目标，保留完整原现场，正常 startup 在 review 前拒绝。显式 review 仅允许元数据诊断；pending replay、数据面和 GC 仍阻断，不从 acknowledgment 补造 witness。具体命令及限制见 [离线备份和恢复](haven-control.md#统一离线备份与恢复)。

writer lock、schema/identity、不可变 request/terminal/publication、完整证明和 HEAD 都在 writable connection/worker 前验证。必要 WAL/SHM 只在持锁下复制到私有 probe 解释；失败不修改原 state。HEAD 不能检测 DB 与全部本地历史一起回退。

## Runtime 与验证范围

真实 entrypoint 始终构造 Cloud Hypervisor。Loom 检查二进制 receipt、设备、namespace/cgroup、进程身份、COW 和网络，保留原生产资源限制。替身只经测试显式注入。guest 镜像、宿主网络与 cgroup 策略、Haven 生命周期的输入见[测试入口](../scripts/README.md)。缺项见[计划 024](../plans/024-haven-loom-single-control-design.md)，旧源码的运行结果见[历史验证记录](verification-history.md)。
