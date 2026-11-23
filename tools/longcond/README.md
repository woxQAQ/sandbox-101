# longcond

`just check-longconds` 检查仓库中自有 Go 模块的源码和测试。默认报告超过 4 个判断项的条件，可通过 `just check-longconds -max 6` 调整上限。从仓库根目录检查单个文件或目录可用 `go tool longcond haven/supply/pool.go`。

工具检查 `if`、`for` 和无标签 `switch` 的每个 `case` 表达式。`&&` 和 `||` 连接的每个操作数各算一项，括号、取反和换行不影响计数。例如 `a && !(b || c)` 有 3 项。比较和函数调用各作为一个整体计数。生成的 Go 文件以及递归遍历时遇到的隐藏目录、`vendor` 和 `testdata` 会被跳过。

每条报告给出条件起点的文件、行、列和实际项数。退出码 `0` 表示通过，`1` 表示发现长条件，`2` 表示参数或扫描出错。
