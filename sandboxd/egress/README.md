# Haven Egress 契约入口

代理当前由根 module 的 `internal/egress` 实现，完整契约统一维护在 [Egress README](../../internal/egress/README.md)。本路径保留文档索引导航，没有独立 sandboxd/egress Go package。

Haven 管理授权、内存 secret、durable sanitized audit、实例绑定与撤权。Loom 不持有租户凭据，管理 CLI 也不绕过 Haven 直接建立出站授权。
