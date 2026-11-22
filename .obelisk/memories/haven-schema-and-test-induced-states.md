# Haven 的表清单与测试造成的状态

`haven/store/epochs.go` 的 `epochTables` 历史上用于生成逐表 epoch trigger。提交 `3ad074c` 移除了这些 trigger。当前该清单只在 `State.validate` 中用于按名字检查必需表。`probe_linux_test.go` 的 `missing-table` 场景先关闭正常库，再通过 SQL 删除 `vm_memory_backings`。目前 Haven 正常写入路径没有删业务表的操作。

开发中有另一种可达情形。v0 baseline 直接修改，旧二进制创建的库可能从未有过新表名。Goose 的同一个版本号不能区分这两份 baseline，手工表名清单也无法识别仅列定义不同的 schema。

复审类似逻辑时，应分别指出生产入口能产生什么状态、系统承诺处理什么外部故障，以及测试自己制造了什么状态。故障注入用例只能证明被测行为，不能单独确立线上需求。此次讨论记录了问题，尚未决定如何替换清单。
