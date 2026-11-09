# 文档索引

按任务阅读对应契约；构建入口在 [项目 README](../README.md)，agent 工作约束在 [AGENTS.md](../AGENTS.md)。

## 架构与跨模块契约

| 需要回答的问题 | 文档 |
|---|---|
| VM、存储、快照、网络与隔离由谁负责？ | [Engine 架构](architecture.md) |
| 模板、租约、分支、配额与数据接纳如何协同？ | [Sandbox 架构](sandbox-architecture.md) |
| Engine 调用、receipt、持久步骤、GC 与恢复怎么使用？ | [Engine 集成](engine-integration.md) |
| OCI 支持哪些语义，如何生成和验证 EROFS？ | [镜像物化](image-materialization.md) |
| Direct CIDR/flow 策略如何安装、核验和撤销？ | [网络策略](network-policy.md) |

## 组件与运行

组件 README 保留在实现或部署材料旁，详细契约各有一个维护位置。

| 任务 | 文档 |
|---|---|
| Sandbox HTTP、令牌、SDK、配额责任、Preview | [sandboxd API](../sandboxd/README.md) |
| Relay、CONNECT、SOCKS、凭据与代理资源限制 | [Egress](../sandboxd/egress/README.md) |
| RPC 编码、方法与权限门户 | [Protocol](../protocol/README.md) |
| Guest 构建、根切换、exec ledger 与树传输 | [Guest](../guest/README.md) |
| Engine 宿主部署、资源预算与恢复 | [Host](../deploy/host/README.md) |
| sandboxd 部署、密钥文件与备份 | [Deployment](../deploy/sandboxd/README.md) |
| 测试工具、输入材料与各 runner 范围 | [Scripts](../scripts/README.md) |
| Kubernetes operator 集成 | [Operator](../operator/README.md) |
| 已运行验收、材料与限制 | [验收记录](acceptance.md) |

## 计划与历史

- [未完成计划](../plans/README.md)：仅列待实施、暂停或受阻工作；存在计划不等于授权执行。
- [已完成计划与证据](../.agents/archive/README.md)：历史审计、实施、复核及精确 source-bound 验收。旧授权、机器路径和成功记录不作为本次运行授权或当前树通过证明。

## 维护边界

- `docs/` 解释当前架构与跨组件契约；源码、配置、协议 schema 和构建入口仍是字段、默认值与命令的事实来源。
- 架构只保留职责、关键协议、理由和不变量；具体 API/格式/状态矩阵维护在对应专项契约，通过链接引用。
- 组件 README 与部署材料就近维护操作说明；文档索引只负责导航，不复制正文。上游依赖 provenance 和参考仓库说明留在其来源旁。
- `plans/` 保存未完成意图；完成后将计划、复核和对应证据一起归档，更新本页入口及入链。历史收据保留原 source/path/hash，Markdown 导航随移动修正。
- 验收记录区分已实现、设计目标、实测证据和宿主待验证项；历史 run 不验证后续源码或 harness 变更。
