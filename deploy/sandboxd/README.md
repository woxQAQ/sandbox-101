# Haven 部署入口

当前只部署一个 Haven；完整宿主准备、配置、service、密钥、恢复与验收说明统一维护在 [宿主部署](../host/README.md)。本路径保留给文档索引导航，没有独立 sandbox daemon、service 或第二份数据库。

业务配置使用 [haven.example.yaml](../host/haven.example.yaml)，客户端示例为 [havenctl](../client/havenctl.example.yaml) 和 [loomctl](../client/loomctl.example.yaml)。业务 API、token、SDK 和配额责任见 [Sandbox 契约](../../sandboxd/README.md)。现有部署及旧 state 没有随源码变更自动切换或迁移。
