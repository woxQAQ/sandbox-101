# 025：Haven 业务服务、领域分包与 sqlc

## 状态与执行边界

**ARCHIVED / IMPLEMENTED / VALIDATION_BLOCKED**。业务服务、领域 SQL、App 组装与 controller 接入均已完成；按用户本次归档指令归档实施计划。必要验收尚未全部完成，剩余缺项在 [未完成索引](../../plans/README.md#待完成验收) 继续跟踪，归档不表示验收通过。分批历史、最终源码身份及 FAIL/SKIP/BLOCKED 见 [实施证据](evidence/025-services-sqlc.md)。

- 基准：`6162397891e0e454fd2f6b3fc511c11d004fde5b`，2026-11-14；用户已授权持续实施。
- 类型：架构重整 / 数据访问生成；P1，改动风险 HIGH。
- 授权不包含提交、推送、部署、旧数据库迁移或真实 VM 验收。当前交付为可审阅的未提交工作树。
- 017、020、021 继续暂停；024 的 VM 验收缺项不在本计划内。
- 用户追加的 migration 已落实为唯一 v9 baseline；运行时与 sqlc 共用该来源，不提供推断式旧格式升级。
- 包使用原名，优先以准确的局部变量名消除遮蔽，不为单复数偏好增加 alias。

## 已交付的结构

| 所有者 | 当前职责 |
|---|---|
| [App](../../haven/internal/application/app.go) | 构造并显式持有服务；根 receiver 仅 `Close`。无业务转发、嵌入式 facade、旧 Service 或根模型/loader 别名 |
| [services](../../haven/services/README.md) | 实际用例、资源执行、历史证明、父子责任交接、publication、恢复、维护及数据面；各服务依赖明确，无 App/Service 回指 |
| [store](../../haven/store/README.md) | 单一 SQLite authority、writer/key/HEAD、只读 probe、backup/restore、migration、checkpoint；没有业务表的公共全能 repository |
| [execution](../../haven/execution/README.md) | 共享 Lifetime、Workers、具名 Guards、接纳和登记屏障；不另建关闭状态或同名锁域 |
| access | 唯一内存 credential/binding/connection registry，并拥有不可变的进程 epoch |
| authorization / operation / sandbox / template / image / quota / pool / vm / snapshot | 领域模型、静态 SQL 与借用调用者 DBTX/事务的 repository；各自私有 `internal/dbgen` |
| httpapi / management | 直接注入实际用例服务；不持有 App，不绕过用例访问 store 或效果能力 |
| [guest](../../haven/guest/README.md) | 宿主 guest port/client；真实 Rust fixture 验证跨语言 transport |

`SandboxRequests` 只路由命令；`Maintenance` 使用共享 Workers/Lifetime 派发与维护。恢复保留固定优先级：direct → images → GC → supply cleanup → fork → warm → supply → capture cleanup → lifecycle → snapshot delete → capture discard → stop → discard → capture → standalone activation → release，最后进入 provisioning Continue。

`EgressFactory` 只接收实际 Audits/ProxySockets/DirectPolicies 的窄 method values；服务关闭、数据登记和 configuration handoff 使用原共享屏障。生产 daemon 仍构造真实 runtime，替身只经 fixture 显式注入。

sqlc 固定 **v1.30.0**；30 个生成文件由 `just generate-sql` / `just check-sql` 管理。唯一 [v9 migration](../../haven/store/migrations/000009_baseline.up.sql) 与基准 `storeSchema` 除首尾空白外完全一致。metadata 初始化/读取和 epoch 也使用私有 sqlc。

## 保持的不变量

1. 持久意图 → 外部效果 → 核验 → 原子责任交接。未知查询保留资源、费用和责任；当前归属不补造历史 witness。
2. repository 借用原 DBTX/`*sql.Tx`，不自行开始事务、重试或提交。跨领域更新、publication 冻结、Commit、HEAD/checkpoint 与故障点保持原顺序；已提交结果不因 HEAD 失败被改写为未提交。
3. SQL 保留谓词、投影、SET 列及 `UPDATE OF` 语义；保留 NULL/nil、整数计算、exact-one-row 与允许零行 cleanup 的区别。大整数配额和 SQL affinity/NOT 参数等价性有行为证据。
4. v9 schema、trigger、冻结 JSON 的字段顺序/tags/nil 语义、hash 输入、operation 请求/幂等身份与已提交终态保持。51 个冻结模型与基准比对通过。
5. 原 generation/instance/funding/永久 owner/material/deletion/publication 证明与当前归属共同核验；跨领域证明不以“callback 能调用”作为授权。
6. 普通启动和离线入口在原只读阶段执行验证。旧 schema 或证明不完整明确拒绝；review 不授予 replay/data/GC 权限。
7. shutdown 先封闭接纳、撤销连接并等待 worker/setup/handoff，再关闭 egress/backend/store；writer 最后释放。配置锁覆盖最后交付，迟到连接关闭。
8. 测试使用私有状态和有归属的资源，保留既有 daemon、`.local-run/` 与真实旧库。原测试名、断言、timeout、native 要求及资源上限保留。

## 架构与生成门禁

- `scripts/check-architecture.py` 检查真实 Go import graph：Loom/client 不获得控制 authority；领域/store/access/execution 不反向依赖 services/controller；services 不依赖 controller；不允许跨域 dbgen。
- `scripts/check-haven-shape.go` 检查根 App 形状、过渡 alias 和业务 receiver，以及非生成代码中的手写 SQL/原始 Query/Exec/Scan。
- 维护 SQL 仅允许已审阅的精确文件/函数/语句或调用：SQLite 身份/完整性检查、migration、epoch trigger、WAL checkpoint、foreign-key check、VACUUM INTO 和有界 statement delegation；没有整目录豁免。
- 架构正负例使用真实临时 workspace；生成负例覆盖删除、修改、额外生成文件和未展开 sqlc 宏。
- native gate 移除吞掉 Go JSON 的 `just --quiet`，并通过真实 just 的 CLI 回归验证输出保留；required-native 集合和判定不变。

## 验证状态

最终 Haven Go/SQL/YAML SHA256：`ff794cbbd57557cf2bc895d721aaaddcea1cac8094a5e21f506bc90e9aa05969`。其范围不包括 scripts/tools/docs；这些改动的独立验证与身份见实施证据。

| 验证 | 结果与边界 |
|---|---|
| Haven 全量 race，`-count=1 -timeout=10m` | **657 pass、0 fail、35 skip**；根包 **282.350s**。SKIP 不计为原生通过 |
| Haven 普通组件，`-timeout=120s` | 默认磁盘运行 **120.286s 超时**；私有 tmpfs 的复验分别记录，不覆盖失败 |
| integration / smoke | 真实 Rust Unix fixture **0.370s**；8 组 smoke 通过 |
| lint / vet / sqlc / architecture / frozen models | 通过；初始 generator、lint 和 gate 失败保留在历史证据 |
| runner / native-gate 回归 | 10 项 runner、8 项 native-gate 回归通过；真实 negative gate 通过 |
| workspace | `just check`、完整 workspace lint、其他 Go module race、Rust workspace/all-targets 通过；tools usage 文本修正后重新验证 |
| KVM build tag | 仅编译通过，未运行 VM |
| required-native | **BLOCKED**：preflight 要求隔离 runner 的 effective UID 0；实际运行缺 `bwrap`。修正输出后的 gate 为 **652 pass、40 fail、0 skip**，根包 **294.717s**，无 timeout |
| KVM / kernel-isolation / production | 本任务未运行，不使用 024 的历史结果替代 |

66 个私有控制面 fixture 已加入现有四个并发 case 的调度机制，未提高名额；原生串行场景、环境修改场景和 abrupt-exit 子进程入口保留。该调整提高全量 race 吞吐，未使默认磁盘上的 120 秒入口通过。

## 完成条件

- [x] 多个实际业务服务与 controller 接通，根 Service receiver 集和过渡层删除。
- [x] 表、查询及领域所有者明确；静态业务 SQL 经 sqlc，维护例外精确。
- [x] 唯一 migration 来源；v9/trigger/冻结 JSON/hash/旧格式拒绝保持。
- [x] 生成可重复，缺失/修改/新增输出可被 gate 拒绝，CI 已接入。
- [x] 无反向 services、根回指或跨域 dbgen；架构正负例有效。
- [x] startup/offline 保留只读校验；事务交接、publication、HEAD、关闭与撤权行为在全量 race 中通过。
- [x] SDK/wire/Loom/guest 行为未改，未运行的真实环境验收明确列出。
- [x] 当前契约、源码路径、证据和计划索引同步。
- [ ] 普通 120 秒组件入口及 required-native 必要验收全部通过。明确 BLOCKED 不等于整体验收完成；实施归档后，缺项在未完成索引继续跟踪。

## 后续验收边界

先核对当前未提交树和 [runner 契约](../../scripts/README.md)。本节保留 025 收尾时的验收要求；镜像执行方式已由 [026](026-direct-image-tools-and-tests.md) 替换，当前前置条件以现行 runner 契约为准。保留普通入口的超时证据，定位执行预算时不得删断言、提高 timeout、提高 fixture 并发/生产资源上限或重命名测试绕过 gate。原生验收需要具备真实工具、namespace/mapping 及隔离 root 条件的 runner；本任务未修改系统配置或提升权限。

以下为 Nushell 复核入口，不能用历史通过替代当前树：

```nu
with-env {GOWORK: "off"} {
    go -C haven test -json ./... -count=1 -timeout=120s
    go -C haven test -json -race ./... -count=1 -timeout=10m
    go -C haven vet ./...
}
just check-sql
just integration
just smoke
python3 -I scripts/check-architecture.py
python3 -I scripts/test_architecture.py
python3 -I scripts/test_sql_generation.py
python3 -I scripts/test_runner_environment.py
python3 -I scripts/test_native_gate.py
python3 -I scripts/check-native-gate.py --negative
python3 -I scripts/check-native-gate.py --preflight
python3 -I scripts/check-native-gate.py
git diff --check
```

不引入全能 `services.Service`、每领域独立事务、公共全表 dbgen、事件总线或通用 workflow 插件框架。新增用例先确定状态/表所有者；新 cleanup/step kind 同时补只读验证、恢复派发、终态冻结及责任测试。schema、请求 hash、冻结 JSON、wire 或 witness 语义变化属于新的协议工作，不能混入本次重构。
