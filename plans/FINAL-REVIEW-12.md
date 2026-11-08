# 最终12项组合的review入口

**后续022已完成，当前目标HEAD33a3f829/tree56c1；本轮重组范围47f5..33a3，完整12项加重组范围b93..目标分支共30提交。见[022交付与新职责位置](022-RESULT.md)。下文精确HEAD47f5、57文件和实现/测试文件位置为旧验收快照；部分声明和测试已按022搬文件，旧VM/production/Rust49成绩仅绑定47f5。**

统一源码分支：`t3/final-twelve-source-combination`。

- 精确HEAD：`47f5ce71bfa8de2720e184fec516f0e27ca90f2d`。
- 基准：`b93fc24a631bb94c9e87484a460703f016742cea`。
- 工作树：`/home/woxQAQ/.t3/worktrees/sandbox-101/t3-fe782bd5`。
- 全部12项：006/007/008/009/010/011/012/013/014/015/018/019；不含未完成016/017/020/021。
- 总变化57文件，+29949/-889；包含大量真实行为/故障恢复测试。不能只看013旧分支或只看最后47f5一个提交；最后一个提交本身只是018补丁，整个分支才是完整组合。

**最终组合已验收通过（FINAL12_COMBINEDPASS）**。独立SOURCE_APPROVE/65交互测试、完整sandboxd race678PASS、fresh Rust49及静态检查通过；匹配新guest/OCI的原双格式KVM和唯一完整production5RUN5PASS/141.101s通过。父已独立核13台VMM liveguards、109PID/26PGID、unit/cgroup/mount清理，所有运行授权失效。原KVM材料汇总失败与后续独立只读补证接受分开保留。完整交付见[最终报告](FINAL-COMBINATION-12-RESULT.md)。

## 建议按五组阅读

| 顺序 | 计划与问题 | 先看实现 | review时应回答的问题 |
|---|---|---|---|
| 1 | 010/011：恢复候选材料如何安全交接；012窄加操作成功结果证明 | engine/restore_materials.go、operations.go、api/types.go；对应restore_materials_test/result_instance_test/instance_recovery_test | 只有同一操作、固定原实例与完整持久证明才能接续候选？失败/未知不会冒充成功？完成VM、操作、lease时部分写入能否整个回滚？ |
| 2 | 009/012/013：模板归属、资源占费与恢复 | sandboxd/service.go、quota.go、recovery.go、warm_fallback.go、store.go；template_admission_test/quota_recovery_test/template_quota_test | owner是否在接纳时固定？构建/快照/预热/失败清理中的资源是否始终有人负责？实际回收前会不会退费？warm交付、reader退役、目标资源引用是否同一事务、带完整类型和来源证明？ |
| 3 | 006/007：请求头限制与网络配置撤权 | sandboxd/egress/mitm.go、egress/http.go；bindings.go、reconcile.go、http.go；mitm_test/configure_egress_test | 内层请求头配置1MiB/10秒，正常正文与复用保留？配置切换会先阻断并关闭旧连接？新的内层parser会不会让撤权一直等待？none策略保留合法workload连接，短guard后会不会登记过期请求？ |
| 4 | 008/015/018/019：guest文件与树传输 | guest/agent/src/fs.rs；tests/agent.rs、tree.rs、fs_identity.rs；protocol/README.md | FIFO是否及时拒绝且不耗尽名额？SCHILY名称是否保持字面？tree_pull是否始终用原授权FD？tree_push的已提交、已持久化、已清理三个事实是否独立报告，失败不扩大root权限？ |
| 5 | 014：SDK排队与活动调用的取消 | sandboxd/sdk/client.go、client_test.go | 排队取消是否不发请求、不占RPC编号？已发请求取消是否关闭连接？Close回调真正结束后才交还准入，后继调用不会读到旧流残留？ |

资源账本中新的内部名字如allocationSubject/group/identity，是把“给谁分配、这组操作是什么、哪一个真实资源”分开保存。它们不直接成为公共API。不要只看helper返回值，连着接纳、实际Engine操作、持久恢复和最终资源回收读，才能判断是否丢掉责任。

013保留的一条关键边界：没有不可变实例证明时，不自动发送空ExpectedInstance删除。即使当前查询显示VM不存在，也只能按具体资源的完整证明处理其计费；不能顺便释放未知快照或其它reader仍保护的资源。

## 看完整范围的命令

从仓库任意checkout可用Nushell执行：

```nu
^git diff --stat b93fc24..t3/final-twelve-source-combination
^git log --reverse --oneline b93fc24..t3/final-twelve-source-combination
^git diff b93fc24..t3/final-twelve-source-combination -- engine/restore_materials.go engine/operations.go
^git diff b93fc24..t3/final-twelve-source-combination -- sandboxd/quota.go sandboxd/recovery.go sandboxd/warm_fallback.go
^git diff b93fc24..t3/final-twelve-source-combination -- sandboxd/egress sandboxd/bindings.go sandboxd/reconcile.go sandboxd/http.go
^git diff b93fc24..t3/final-twelve-source-combination -- guest/agent/src/fs.rs sandboxd/sdk/client.go
```

不需要checkout或覆盖当前dev的未提交改动。提交来源见DELIVERY-MAP-006-021.md和/tmp/final12-combination-t3-fe782bd5/mapping.json；正式新独立源码结论见/tmp/final12-source-review-t3-798e69ce/report.md。

## 集成与部署边界

这次只准备完整可review分支及组合验收，不合并或推送父dev，不升级任何现有真实部署。sandboxd schema6没有自动把旧schema4/5库升级的迁移流程；如以后部署到旧状态，需另行安排备份与迁移/重建方案。备份元数据也不等于VM磁盘/RAM全量备份。
