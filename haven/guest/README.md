# 宿主 Guest 能力

`Port` 是业务编排使用的 guest 能力接口；`Client` 实现实际 Admin 2046 与 Workload 2048 通信。这里是宿主 Go adapter，仓库根 `guest/` 是 Rust agent/init。生产 daemon 组装 `Client`，替身通过显式 Port 注入。

调用者先核验冻结 resource、当前归属及接纳条件，再交付 resource。`Socket` 只由该 resource 的 RunDir 形成端点路径，不证明身份、运行状态或归属。端口不能由租户请求选择。连接的登记、撤权与关闭仍属于 access 和应用生命周期。

RPC correlation ID 与持久 exec key 独立。Exec 必须收到显式 key，只有 guest 的 durable completed 结果才确认完成；不明结果保留原 key，不自动补发或替换。协议错误仅映射既有决策 sentinel，私人 guest 诊断不向宿主控制响应泄漏。Activate 只接收 guest 可见环境，宿主 secret 留在 access。`sandbox.Activations` 通过窄 `ActivationGuest`（Info/Activate）端口协调实际交付；原 VM/configuration guards、worker custody、epoch 和 access registry 仍由应用共享，guest adapter 不接管这些状态。

冻结请求、终态 publication、配额与回收证明不属于此包。guest 回复是协作结果，不授权退休责任。帧、错误和半关闭约束见 [Protocol](../../protocol/README.md)，真实 guest 行为见 [Guest 契约](../../guest/README.md)。
