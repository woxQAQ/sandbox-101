# structfmt

`structfmt` 将 Go 花括号内的内容展开，再交给 gofmt：

- struct literal、map literal：每个带 key 的条目独立一行，补齐末尾逗号；map key 可以是任意表达式。
- struct 类型声明：每条字段声明独立一行，保留 tag、嵌入字段和 `X, Y int` 这样的分组声明。
- 显式 array/slice literal（`[N]T{...}`、`[...]T{...}`、`[]T{...}`）：每个元素独立一行，补齐末尾逗号，支持带索引的元素。
- 函数声明、函数 literal 及内部控制流块：每条语句独立一行，右花括号独立一行。

这些场景共用同一个节点序列展开规则。处理只插入换行和必要的逗号，不加载类型信息；注释、字符串和表达式内容保留。空块以及其他包含位置参数的 literal（包括省略类型的 array/slice literal）采用 gofmt 的布局。

从仓库根目录使用 module tool：

```nu
go tool structfmt -w cmd/haven/main.go
```

省略 `-w` 时向 stdout 输出格式化结果。`make fmt` 使用同一入口展开仓库宿主源码并执行 gofmt，再执行 `cargo fmt --all` 格式化 Rust workspace。工具不会扫描 reference submodules 或自行发现目录。CLI 回归验证嵌套 struct/map、显式 array/slice（含索引元素）、字段声明及 tag、函数与控制流块、注释/字符串、重复执行不变、权限保留及语法错误时不写入。
