# Haven Sandbox API 与生命周期

当前实现为 `haven/` 下的 `github.com/woxQAQ/haven` module。入口与配置见 [Haven control](haven-control.md)，宿主部署见 [Host](../deploy/host/README.md)。公开 DTO 在 `types/`，SDK 在 `sdk/`，HTTP adapter 在 `haven/api/`；业务和资源工作流共享 `haven` 的唯一 store。包内职责与事务边界见 [Sandbox 业务责任](../haven/sandbox/README.md)。

## Tokens、幂等和错误

HTTP 使用 `Authorization: Bearer TOKEN`。root 管理 tenant/node status；tenant 管理自身模板、sandbox/checkpoint；sandbox token 仅访问自身。root 修改 owner-scoped 模板或 fork 时，使用 `X-Tenant-ID` 明确选择 owner。外来与缺失对象均返回同一 404。

mutation 使用 1–256 字节稳定 `Idempotency-Key`。同键同参数返回原 operation/冻结 result，包括失败；异参冲突。认证 role/scope 和 secret-safe HMAC 参与请求身份。取消 HTTP 只结束等待。`Prefer: respond-async` 返回 202、operation 与 Location，由同一 authority 查询。sandbox token 只能查询自己的 renew/release operation；终止后仅能读取对应历史，不能进入资源/数据路由。

终态 result 和时间在原工作流事务中冻结。成功操作缺少应交付的 sandbox、分支、checkpoint 或模板时，事务回滚。启动核验同样检查原 publication 的必需对象。后来 renew/configure/release 不修改原 delivery。result 可能含 sandbox token，应私密保存；令牌不以真值写入 DB。

HTTP 错误及 `Operation.Error` 使用 `types.ApiError` 数据，JSON 为 `{"code":"...","message":"..."}`。该 DTO 不实现 Go error，也不携带 Cause；SDK 将失败响应转换为普通 Go error。wire payload 不作为 Loom absence proof。需要 operator 诊断的资源丢失不变成 tenant 404。

| Code | HTTP | 调用方处理 |
|---|---:|---|
| not_found | 404 | 核对 tenant-visible identity/authority |
| forbidden | 403 | 使用所需 root authority |
| invalid | 400 | 修正请求 |
| conflict | 409 | 处理当前接纳/lifecycle 冲突 |
| resource_exhausted | 429 | 减少使用或安排容量 |
| incompatible | 422 | 修正不支持的配置 |
| result_unknown | 503 | 保留原 key，不盲目重放 |
| manual_recovery_required | 503 | 保留责任与现场，安排人工恢复 |
| internal | 500 | 检查私有诊断，不自动重试 mutation |

公开错误不包含私有路径、环境或 secret。raw cause 仅供私有诊断，审计链写入失败不改已提交终态。operation/幂等/授权历史保留，当前查询不重写历史错误或补造 witness。

## Routes

| Route | 行为 |
|---|---|
| GET /healthz | 公开进程健康 |
| GET /v1/status | root store identity/schema/epoch |
| POST /v1/tenants | root tenant/quota/token 管理 |
| GET、POST /v1/templates | 列表、template_put 登记 |
| POST /v1/templates/{id}/build | template_build 构建 |
| POST /v1/sandboxes | 领取 sandbox |
| GET /v1/sandboxes、/v1/sandboxes/{id} | 可见 inventory |
| POST /v1/sandboxes/{id}/{action} | renew/configure/pause/resume/checkpoint/fork/rollback，每个动作显式注册 |
| DELETE /v1/sandboxes/{id} | release 可省略 body。传入非零 `lease_deadline` 时，仅在当前 deadline 与其相等且已到期时释放，否则返回 409 |
| GET /v1/checkpoints、DELETE /v1/checkpoints/{id} | checkpoint 列表/删除 |
| POST /v1/checkpoints/{id}/fork | 从 checkpoint 派生。SDK 使用 `Client.ForkCheckpoint`，CLI 使用 `havenctl --key KEY --request-file FILE fork-checkpoint ID` |
| GET /v1/operations/{id} | 固定 authority 的 operation/history |
| GET /v1/sandboxes/{id}/connect | HTTP/1.1 Upgrade 到 Workload |
| POST /v1/sandboxes/{id}/preview?port=8080&ttl_seconds=60 | `{}` body，生成 signed Preview |
| POST /v1/node/drain | root 停止新 claim/refill |

body 为严格 JSON，上限 1 MiB，未知字段/多余 JSON 拒绝。URL 决定命令与目标；body 中重复出现的 action 或 ID 必须与路径一致。路由不自动 redirect。错误 method 与非 HTTP/1.1 Upgrade 保持明确拒绝。

root 创建 tenant：`{"id":"team","max_sandboxes":20,"max_storage_mib":65536}`。使用该 tenant 注册 template：

```json
{"action":"template_put","template":{"id":"python","image":"sha256:REPLACE_WITH_IMPORTED_MANIFEST_HEX","shape":{"vcpus":1,"memory_mib":512,"disk_mib":512},"target_warm":0}}
```

镜像先通过 `havenctl admin import` 进入同一 store。模板首次接纳永久绑定 owner，失败保留占位，滚动 build 继续服务旧版本；published catalog 必须匹配原 immutable publication step。snapshot published 不授予访问权，`snapshot_owners` 是永久 authorization provenance。root 外部 golden grant 必须明确 tenant，不能转移已有 owner。

`template_build` 固定 build slot/D/D+M，运行一次确定性 keyed warmup，Admin prepare_template 后 capture golden，实际回收 build VM 后发布。unknown warmup 不重放，build 网络保持 off。prepare_template 不通用清理应用内存、RNG 或外部注册。ordinary 首次 target=0、后续保持发布 target；root 选 owner 才能改。refill 使用 owner 的有限普通预算，unknown/pending/cleanup 继续占 target。

claim 示例：

```json
{"template_id":"python","ttl_seconds":300,"on_expire":"pause","auto_renew":true,"policy":{"mode":"none"},"env":{"PROJECT":{"value":"demo"}}}
```

供给依次使用 ready warm、golden、cold。warm 选择、sandbox slot 与磁盘 holder 交接同事务；坏 warm 精确退休后才 fallback。business birth 独立于 warm age 与资源 generations。delivery 前仍需 Admin/Workload、lease/settings/fence 和实际网络接纳。

paused 为完整 sleep snapshot 且无 VMM；并发数据请求合并为一次透明 resume。resume 保留 machine-id、进程/session/ledger，并应用当前 settings/policy。running 发布同事务把 sleep holder 交给独立 cleanup，backing 费用继续保留。pause 失败保源，resume 失败保 sleep，paused release 不启动 VM。

fork 支持 running/paused sandbox 或 checkpoint，count 默认 1、范围 1–32。running 源只捕获一次；整批固定 IDs、births、slot/storage/host peak 并原子承接后才执行。省略 policy 时继承 source policy，env 不继承。分支通过 Admin 重置身份/env/ledger；失败沿原 branch intent 精确补偿。paused checkpoint 直接绑定原完整 sleep，不唤醒。

rollback 保留逻辑 sandbox 身份，但回滚 guest ledger；跨 rollback 的去重需独立 host fence。默认 lease 300s。reaper 冻结并重核预期 deadline，不能处理后来的 renew。auto_renew 只在连接或 Preview admission 刷新，不能复活过期租约；raw stream 内 RPC 不持续续租。

## Raw protocol and SDK

使用 `Connection: Upgrade` / `Upgrade: sandbox-v1`。101 后为有界背压的双向原始流，保留预读字节与 half-close；宿主不解释 Workload frame。System 2047、Admin 2046 不对租户开放，Workload 固定 2048。协议为四字节 big-endian 长度前缀 protobuf，frame 最大 8 MiB、chunk 64 KiB；port_forward 必须是新连接的首请求，ready 后完全 raw duplex，无最终 protobuf frame。详细方法见 [Protocol](../protocol/README.md) 与 [Guest](../guest/README.md)。

```go
import (
    "github.com/woxQAQ/haven/sdk"
    "github.com/woxQAQ/haven/types"
)

client := sdk.New("https://sandbox.example", tenantToken)
result, err := client.Claim(ctx, "persisted-claim-key", types.Command{
    TemplateID: "python", TTLSeconds: 300, Policy: types.Policy{Mode: "none"},
})
```

`Connect` 后使用 `Call`/`Exec`。Call 串行接纳，排队取消不发送、不耗 RPC ID；发送后取消关闭连接，之后不能复用。consume callback 须自行返回，取消不能抢占用户 callback。`Call` 在失败时仍返回终态 params；guest `error_code` 和 `result_state` 从该数据读取，Go error 使用普通 `errors.New`，没有 `RPCError` 包装。keyed exec 先记 intent、同参重放、异参冲突；unknown 不重跑。frame ID 仅关联回复。unkeyed Exec 不去重，SDK 不盲目重试未知副作用。

```nu
havenctl --token-file /private/tenant-token list
havenctl --token-file /private/tenant-token --key claim-001 --request-file /private/claim.json claim
havenctl --token-file /private/sandbox-token exec SANDBOX_ID 'python3 --version'
```

claim JSON 通过 `--request-file /private/claim.json` 显式文件传入，不读取 stdin。flag 放在 command 前。`havenctl exec` 为 unkeyed；持久去重使用 SDK 的 exec_key。

## Durable quota responsibility

`quota_resources` 唯一计总量，`quota_refs` 表示 active_use/engine_dependency/cleanup。授权来源不是费用或 GC root。query error、unknown dispatch、未确认删除继续收费；访问终止、字段清空、catalog/path 消失不自动退款。D 为磁盘 MiB、M 为捕获 RAM MiB，root 也计可信 owner；maintenance/refill 使用 owner 普通预算。

| 资源 | 费用 | 交接与退休 |
|---|---|---|
| sandbox_slot | 1 slot | unknown allocation、VM、必要独占 sleep cleanup 保留；实际退休或可信独立目的地才退出 |
| build_slot | 1 slot | 原 build 的未知/实际资源完全退休或交接后退出 |
| vm_disk | 每独立 generation D | 按原步骤/目录/inode 核验实际回收，旧 restore 盘保守保留 |
| snapshot | 同 owner/SID 去重 D+M | sleep/checkpoint/template/reader/backing/cleanup；最后实际 backing 与必要资源回收才退休 |
| pending_storage | 固定意图上界 | admission 前持久；同事务转入实际责任或可信 never-dispatched/回收目的地 |

| 生命周期 | 原子责任边界 |
|---|---|
| claim/fork | 固定整批 slot、D、source dependency 与必要 D+M；branch admission 同事务接过 forecasts |
| pause | 先保留 VM D 和 D+M capture；sleep publication 与可信 VM 退休分别完成 |
| resume | 保留 sleep SID，预留新 D；交接长期 backing 与独立 sleep cleanup |
| checkpoint/delete | publication 与 holder 同事务；删除 binding 只释放自己的引用 |
| rollback/restore | 新 D/version/backing 与旧 ancestry 连续保留，不用当前 instance 补 witness |
| release/discard | 先终止访问与固定 cleanup；精确回收所有 known generations 后退休费用 |
| build/publish | build D/slot 退休后发布 golden holder，并交接旧 SID cleanup |
| warm | 零 slot birth 固定；claim SlotAssignment、row selection、holder 同事务；unknown 继续占池位 |
| backup/restart | 同一 DB 保存归属/ledger/fence/历史；review 不授予 replay、数据或 GC |

实例清理同时验证冻结历史和当前归属；缺 witness、foreign generation、未知 sibling、SQL 拒写或效果未知保留费用、slot/池位与原 intent。失败 candidate 的显式 discard 使用原 preparation 证明，不伪造成功 witness。operation/step、parent/child 与 cleanup 目的地在同库事务内交接；旧跨库 receipt adoption/Resolution JSON 不再是运行路径。

D=64、M=128 时，cold=64，checkpoint/resume peak=256，running 单 branch fork peak=320，已有共享 checkpoint/backing 再 pause peak=448。63/255/319/447 在效果前拒绝。多 holder 不重复计 SID192；删除一个 source 不退休仍被 branch backing 持有的 SID192。

## Egress、配置与 Preview

proxy 规则、DNS/TLS/HTTP、secret 注入和审计详见 [Egress](../haven/egress/README.md)。secret 真值只在内存，guest 只收 harmless placeholders，注入前需 durable sanitized audit；重启必须显式重供。none/proxy NIC off。direct 为 canonical IPv4 CIDR/flow（默认 256），拒绝 hostname 与 host-only secret；空 CIDRs deny all，`0.0.0.0/0` 仍受 reserved deny。宿主 bandwidth/conntrack 限额由部署承担。

configure admission 持久 fence 并撤销旧数据/proxy，之后才保存新 settings。runtime/guest/bind/SQL 失败保留阻断；自动 rebind/reconcile 不清 fence。最终 running/result/settings/fence retirement 原子提交，失败撤销新 binding。迟到 worker 或旧 direct callback 不影响后继实例。

数据 setup、最终 downstream Hijack 和上下游连接均参与配置 guard、revocation registry 与 shutdown。Preview 绑定 tenant/sandbox/epoch/port/expiry，期限不超 lease；移除 Authorization/Cookie，强制 no-referrer。当前没有每 sandbox 浏览器 origin。

Preview 签发要求 `port` 为 1–65535 的整数。省略 `ttl_seconds` 取 300 秒；显式提供时须为可表示的正整数秒，空值、重复值及越界值返回 400。实际有效期以 sandbox lease 为上限。

每次 Preview HTTP 请求均验签并完成一次 readiness admission，开启 `auto_renew` 时续租一次。同一 tenant、sandbox creation epoch、实例、配置代次和端口可复用已登记的 guest forward。新 forward 建连上限为 5 秒，请求写完后等待上游响应头的上限为 30 秒；两个超时都返回 502。响应体可以继续流式传输，连接仍受 lease、配置撤权与 shutdown 约束。

当前 schema 见 [Haven control](haven-control.md)，旧双库和早期原型明确拒绝，无自动迁移。各运行场景的输入见[测试入口](../scripts/README.md)，历史源码与材料见[验证记录](verification-history.md)。
