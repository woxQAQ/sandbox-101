# Haven 的宿主侧 guest 客户端

`guest.Agent` 是 Haven 调用 VM 内 Rust agent 的 Go 接口。生产 daemon 将 `guest.Client{}` 传给 `application.Options.Guest`。各调用方只声明自己需要的方法。[接口定义](agent.go) · [客户端实现](client.go) · [daemon 装配](../cmd/haven/main.go)

| 调用方 | 方法 | 成功条件 |
|---|---|---|
| `sandbox.ActivationGuest`、`supply.SupplyGuest` | `Info`、`Activate` | `Info` 在 Workload 2048 确认 portal 为 `workload`、协议版本为 1。`Activate` 经 Admin 2046 交付 sandbox 身份、hostname、guest 可见环境、网络模式和 CA，并等待成功回复。 |
| `supply.SupplyGuest` | `Exec`、`PrepareTemplate` | 模板预热在 Workload 2048 使用显式 exec key，只有 `result_state=completed` 且退出码为 0 才成功。准备捕获时，Admin 2046 必须同时返回 `prepared=true` 和 `quiescent=true`。 |
| `dataplane.PlaneGuest` | `Connect`、`Forward` | `Connect` 打开 Workload 2048 字节流。`Forward` 在该门户上请求转发到指定的 guest TCP 端口，收到匹配请求 ID 的 `ready=true` 成功回复后将双向字节流交给调用方。 |

调用方负责核验资源归属。`guest.Socket` 从传入的 `loom.Resource.RunDir` 拼出 `io/vsock` 路径。guest 回复只确认本次协作的结果，工作流依据宿主持久记录决定能否发布或回收资源。数据面负责接纳、租约检查与撤权，通过 `ephemeral.Registry` 跟踪连接。[数据面接纳](../dataplane/admission.go)

每次控制 RPC 使用新的帧 ID。模板工作流提供的 exec key 标识同一次预热，即使重新连接也必须保留。缺少持久完成状态或退出码时，`Client.Exec` 返回 `ErrUnknown`。帧格式、错误码和 guest 端执行记录的语义见 [Wire protocol](../../protocol/README.md) 和 [Guest 契约](../../guest/README.md#durable-keyed-exec)。
