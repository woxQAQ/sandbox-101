# sqlfmt

`sqlfmt` 格式化 SQLite migration 中的 CREATE 语句：每条语句的首个括号列表按顶层逗号拆分，每个字段独立一行并缩进一个 tab（tabsize 4），语句之间空一行。工具面向 `haven/store/migrations/` 这类只含 CREATE 语句的迁移文件，用逐字节扫描识别括号深度和 `'...'` 字符串（含 `''` 转义），不依赖正则或 SQL 解析器。

布局规则：

- 列表含两个及以上字段时展开，`(` 留在语句头行尾，`)` 独立一行，后接原语句的剩余部分（如索引的 `WHERE` 子句）与 `;`。
- 单字段列表保持单行，但 `(` 前的空格布局同样归一化。
- 空列表原样保留。括号不平衡、字符串未闭合或末尾缺少 `;` 时报错且不写入。

从仓库根目录使用 module tool：

```nu
go -C tools tool sqlfmt -w /absolute/path/to/haven/store/migrations/000000_baseline.up.sql
```

省略 `-w` 时向 stdout 输出格式化结果。测试覆盖紧凑与已展开语句混排、嵌套括号、含逗号与分号的字符串、索引列列表与 `WHERE` 后缀、重复执行不变，以及非法输入不写入。
