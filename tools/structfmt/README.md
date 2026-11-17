# structfmt

`structfmt` 将 Go 花括号内的内容展开，再交给 gofmt：

- struct literal、map literal：每个带 key 的条目独立一行，补齐末尾逗号；map key 可以是任意表达式。
- struct 类型声明：每条字段声明独立一行，保留 tag 和嵌入字段；默认保留 `X, Y int` 这样的分组声明，`-split-fields` 将它展开为 `X int` 和 `Y int` 两行，每个字段保留原类型与 tag。
- 显式 array/slice literal（`[N]T{...}`、`[...]T{...}`、`[]T{...}`）：每个元素独立一行，补齐末尾逗号，支持带索引的元素。
- 函数声明、函数 literal 及内部控制流块：每条语句独立一行，右花括号独立一行。

这些场景共用同一个节点序列展开规则。默认只插入换行和必要的逗号；`-split-fields` 还会将字段名之间的逗号替换为类型、tag 和换行，不影响函数参数或返回值的分组声明。工具不加载类型信息；注释、字符串和表达式内容保留。空块以及其他包含位置参数的 literal（包括省略类型的 array/slice literal）采用 gofmt 的布局。

从仓库根目录使用 module tool：

```nu
go -C tools tool structfmt -split-fields -w /absolute/path/to/haven/cmd/haven/main.go
```

省略 `-w` 时向 stdout 输出格式化结果。`just fmt` 使用同一入口并启用 `-split-fields` 展开仓库宿主源码并执行 gofmt，再通过 `golangci-lint fmt` 统一 gofumpt、import 分组与排序，最后执行 `cargo fmt --all` 格式化 Rust workspace。工具不会扫描 reference submodules 或自行发现目录。CLI 回归验证嵌套 struct/map、显式 array/slice（含索引元素）、字段声明及 tag、分组字段拆分（含注释与嵌套匿名 struct，且编译输出）、函数与控制流块、注释/字符串、重复执行不变、权限保留及语法错误时不写入。
