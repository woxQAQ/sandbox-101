# 已完成计划的实际交付分布

**当前源码已完成022重组并更新：统一分支HEAD33a3f829/tree56c1，基于下述47f5完整12项链再追加2个scoped提交，从b93合计30个提交；内容仍含全部12项，职责/test/API整理另见[022结果](022-RESULT.md)。下方47f5/source/local/VM验收叙述保留为原快照历史，旧production/KVM/Rust49不转用。**

**FINAL12_COMBINEDPASS，最终组合已由父接受。** 统一源码分支`t3/final-twelve-source-combination`，HEAD `47f5ce71bfa8de2720e184fec516f0e27ca90f2d`，tree `3e8cc8a12cb70dd36fffc428f9aff83d8712fe73`，worktree `/home/woxQAQ/.t3/worktrees/sandbox-101/t3-fe782bd5`。从b93起28个提交覆盖全部12项；7个补充pure提交无冲突且patch-id一致。SOURCE_APPROVE/65focused、sandboxd race678PASS、fresh Rust49、新guest/OCI的双格式KVM及唯一完整production5RUN5PASS/141.101s均通过，13VMM liveguards与109PID/26PGID/服务/cgroup/mount清理已父核验。所有授权失效、slotFREE，父dev未merge/push。见[最终报告](FINAL-COMBINATION-12-RESULT.md)、[review入口](FINAL-REVIEW-12.md)、[最终接受收据](evidence/final12-47f5-final-acceptance.json)。下文保留组合前原始分布及当前补丁映射；016/017/020/021与无关fixture维护继续暂停。

## 关键结论

`t3/plan013-production-acceptance` HEAD为`1414b4e376a70fec9ed9f16b30db588cd38c4b88`，从b93fc24起有21个提交。它包含007、009、010、011、012、013共6项及必要修订，**不是12项已完成计划的最终总分支**。

原来分别留在其它分支的006、008、014、015、018、019已追加到最终组合；原独立分支仍保留。早期8项和10项临时集成的历史成绩不冒用为当前结果，本次最终组合有新的source/local/guest/KVM/production及清理接受收据。

先前建议直接以013分支作为“这轮全部成果”的合并对象不完整；正式交接以本映射为准。

## 013分支已包含的计划

| 计划 | 原批准提交 → 013分支实际提交 | 核验方式 |
|---|---|---|
| 007 | b45f1b05→10c6fe5d；75c13079→616a8ad6；8d08ba0a→92249e39；1c1e735e→fb5c0578；2a8e9c08→c97543e6 | 各提交stable patch-id逐一相同 |
| 009 | 6521ef6e→ad67cd99；c3c21abe→1ef221d3 | stable patch-id逐一相同 |
| 010 | 270eb47e→3b019520；7df48299→32458d99 | stable patch-id逐一相同 |
| 011 | 5115a9f0→dbdbecdf | stable patch-id相同 |
| 012 | 8729475f、2c15f095、adbd61a2、78bc04fc等为直接祖先；d5654123→90f68b61 | 祖先及stable patch-id核对；完整21项提交见下 |
| 013 | d3be730→da4b7d6→5c530395→1414b4e | 全部直接祖先，最终精确1414生产通过 |

不同提交编号来自跨隔离分支复制提交，不能仅凭“原SHA不在祖先里”判断缺失；上述已按实际补丁内容核对。

## 原独立分支（现均已纳入最终组合）

| 计划 | 最终组合中的补充提交 |
|---|---|
| 006 | ce6154373e134560d4a070a2bfa93fa0e8f01423 |
| 008 | 994421d783296b17c610e4736e78e4cfe3bd65fd → 02948492bbb5e1907efc8e4e816907d1f954a752 |
| 014 | cdcb9248f1d8dc83d31677a0d13f83bf692f6721 |
| 015 | 5da47a948f005b8e1d1a14bc613f65dd86a88954 |
| 019 | 2a883d3d34def4708cc3985e4d166d741ed8625f |
| 018 | 47f5ce71bfa8de2720e184fec516f0e27ca90f2d |

原批准分支与提交保留如下：

| 计划 | 当前实际分支 | 已验收HEAD | 实际工作树 |
|---|---|---|---|
| 006 | t3/bound-connect-inner-http-parsing | 6d490ea850f0a51a25037265e547893a8dc41654 | /home/woxQAQ/.t3/worktrees/sandbox-101/t3-84acdd1c |
| 008 | t3/fifo-rpc-test-doc-revisions | fba80816a4a38fcb049c64bd2504106fe24462a1 | /home/woxQAQ/.t3/worktrees/sandbox-101/t3-2b480da4 |
| 014 | t3/sdk-queued-call-cancellation | 579054ccbd735fc6ae80a673471d0e8c3e081a4d | /home/woxQAQ/.t3/worktrees/sandbox-101/t3-833f0eda |
| 015 | t3/preserve-schily-xattr-names | 583a9f719047f38453d91c946862b8a80d466740 | /home/woxQAQ/.t3/worktrees/sandbox-101/t3-d8a72b8e |
| 018 | advisor/018-tree-cleanup | 6c090e81160e59e75280c79ca9f4041d8c0c2343 | /home/woxQAQ/.t3/worktrees/sandbox-101/t3-b0631c43 |
| 019 | advisor/019-guest-fs-identity | 314c4f7e1599220c3a0b6699bd77f9d1ab9c5533 | /home/woxQAQ/.t3/worktrees/sandbox-101/t3-a9563b37 |

这6项的批准补丁都不在013分支21个新增提交的stable patch-id集合中。另核对其关键文件：013相对b93没有mitm.go、guest fs/process/fs_identity和SDK client的对应变化。008需要其两项自身提交05427081→fba80816；不能把完整旧集成分支或包含重复依赖的历史链盲目再套一次。

## 013分支从b93后的真实提交顺序

```text
10c6fe5 sandboxd: revoke egress before configuration activation
616a8ad sandboxd: finalize fenced configuration delivery atomically
ad67cd9 sandboxd: reserve template ownership at admission
1ef221d sandboxd: validate template admission provenance and publication locking
1689ded sandboxd: retain quota responsibility through resource cleanup
8892e5d sandboxd: reconcile recovery allocations independently of sandbox ownership
3b01952 engine: persist restore candidate completion proof
32458d9 engine: fix blocked restore deletion and resolved GC responsibility
dbdbecd engine: preserve instance fencing during operation recovery
8729475 sandboxd: preserve exact restore cleanup responsibility
2c15f09 sandboxd: retire terminal allocation dependencies with their destination
92249e3 sandboxd: preserve none-policy workload relays during maintenance
fb5c057 sandboxd: confirm none-policy revocation before workload admission
c97543e sandboxd: serialize workload admission with configure fences
adbd61a sandboxd: reclaim witnessed restore results after proven absence
90f68b6 engine: reject incomplete operation completion writes
78bc04f sandboxd: provision finite production owner budget
d3be730 sandboxd: account template builds and retained snapshot resources
da4b7d6 sandboxd: validate warm ownership and cleanup handoffs
5c53039 sandboxd: validate durable warm selection destinations
1414b4e sandboxd: validate typed warm fallback holders
```

## 尚未完成的计划

016代码/本地验证有成果但外部CI/root/Nix验收未齐；017未实现；020/021已有设计/验证稿但最终来源映射未完成。这4项仍暂停，不纳入“12项完成”。

## 最终集成边界

完整12项分支及组合验收现均已完成，精确47f5的新生产通过及清理收据已父接受；历史1414生产PASS仍仅属于其旧树。当前源码仍未合并或推送到父dev，也没有升级现有部署。016/017/020/021和无关fixture维护不因本次组合完成而自动继续。Schema6不自动迁移真实旧schema4/5状态，部署到旧状态需另行安排。
