# 跨能力归属记录

`ownership` 只定义资源协议与 sandbox 共同冻结的三种值，不读取数据库、不核验资格，也不拥有状态推进：

- `Birth`：原 `sandbox.Identity`，表示业务实例的出生信息。
- `Sandbox`：原 `sandbox.Record`，表示被冻结的 sandbox 归属快照。
- `GenerationRef`：原 `vm.GenerationRef`，以 operation ID 和 input hash 引用原 generation。

这些类型被移动而非复制。原字段顺序、JSON tags、指针与零值语义保持；原包不提供兼容 alias。capture 与 resource 的递归冻结模型直接引用同一份定义，避免让 machine 为读取冻结值而依赖 sandbox 的运行用例或查询实现。

该包不接收 runtime、DB、worker、validator 或 repository。当前归属仍由 sandbox 核验，generation 与物理责任仍由 machine 核验；持有这些值不等于拥有修改或退责权限。
