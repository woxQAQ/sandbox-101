# Machine 资源责任

machine 收拢 VM generation、canonical snapshot、source lease/backing 和物理网络 policy，因为这些资源的原始证明相互引用。sandbox 的业务身份、设置、activation 和 checkpoint binding 仍由 sandbox 负责。

本包拥有冻结资源/capture 模型、历史证明、snapshot 删除、物理 policy、capture/stop/discard/release 执行，以及 provision/restore 的物理推进。VM/snapshot 与资源 step/cleanup 行族使用本包私有 sqlc。业务 admission 与 provisioning coordinator 属于 sandbox，供给用例属于 supply。VMState/SnapshotState 已删除；跨能力仅暴露 ownership/history、publication roots、资源接纳与退休所需的语义入口。

## 证明与事务

所有持久读取、写入与退休都借用调用者原 DBTX/事务，不创建独立数据库。必需写入保留 RequireOne、原 SQL 谓词和 SET 列。NULL current owner 仍以 `Owned=false` 表达，不能仅凭空字符串推断。query/scan 失败保留未知与责任，不能从当前 VM 补造历史 instance/version。

- `Current` 是可变行。`VMRecord`、原 instance、原 version、resource step 和返回 witness 必须相互匹配。
- `InstanceHistory` 与 `VersionHistory` 使用 `GenerationIdentity` 一起指定原 VM/operation；instance witness 与 version step key 仍由调用方显式提供，不从当前 VM 或较新 step 推导。
- version ID 仍为原 operation ID，并匹配 VM、step key、body 和 disk identity。release 持有当前 owner 不代表它成为旧 generation 的出生 owner。
- `SnapshotRecord`、`Manifest`、`Source` 保留原字段顺序、JSON tags 和 nil。`Source` 记录 capture operation/hash 与 publication inode，不以当前目录替代历史来源。
- `SnapshotProofs` 分开核验历史 publication 与当前恢复兼容性；删除旧 snapshot 不额外要求它兼容当前 runtime。
- `ResourceProofs` 核对 ancestry、原费用与 holder、restore 材料及 backing/lease。`quota.Holder` 将责任类型与 ID 一起传递，避免跨 generation 核验时拆散目的地身份；不改变持久费用/引用结构。`Snapshots()` 返回同一历史核验器，不创建新的 authority。

`AvailableNetworkSlot` 在原 admission 事务中选择物理地址 slot；`AdmitCandidate` 按原顺序写入候选、instance、version。`ProvisionIntent` 整体传递原 operation/action、expected instance 与资源输入；`RestoreGenerationClaim` 从同一个 VMRecord 导出逻辑 VM key 与写入 body，并保留原 stopped instance 的 SQL 条件，避免分别指定 key/body。`RetainGenerationIntent` 只写后两项，restore 仍先 claim stopped VM，再更新 sandbox 归属，最后调用它。这些入口都不提交、不 checkpoint、不获取 workflow lock。VM 完成、停止、ownership 转换和 generation 退休直接使用本包生成查询；retained instance/version 也由 release proof 直接读取，不再复制为额外行类型。

`ResourceAdmission` 将 child record、kind、candidate 与 request hash 作为一次接纳的完整事实传递；`ResourceLinkTarget` 单独标识父 operation、role 与 expected instance。二者只用于内部事务调用，不改变冻结 JSON 或数据库 schema。父 intent 与 child link 仍在原事务内核验并写入，不增加可选执行模式。

`ownership.Birth`、`ownership.Sandbox`、`ownership.GenerationRef` 是被两个能力共同冻结的唯一值定义，没有数据库或运行行为。resource/capture 的递归输入继续引用原完整归属内容，未复制或删减模型。

## Snapshot 与 generation 退休

snapshot_owners 在 catalog 删除后仍保留。单独的目录行、计数或 `Backing` 不授予回收权；必须核对原 capture、publication inode、manifest、物理删除 receipt 和全部引用。

snapshot publication、删除、source lease/backing 转换与内部计数直接消费私有生成查询。`admitCapturePublication` 在调用方事务内先写 catalog、再写永久 capture owner；发布与删除仍以原 body、published/deleting 状态和 RequireOne 作条件。restore 与删除证明直接读取原 publication/owner 行。VMRoots/SnapshotRoots 解码 catalog，保留独立 catalog key、原 bytes 的摘要及出错前的根集合；gc 仍负责组合全局候选与未知状态。

source lease 固定 snapshot/operation/step 三元组。只有恢复材料证明完成后才接管 backing，step 推进仍保留原 running 条件。generation 退休按原顺序解除 backing/lease、退 restore reservation、尝试 snapshot 最后退责，再删除原 version/instance。它们使用同一事务；查询失败继续保留费用。

`SnapshotDeletion` 保留原接纳、冻结 intent、物理核验、精确删除与原子退责。sandbox 的 checkpoint_bindings 与资源 snapshot 的 checkpoints 不混为一类。

失败 capture 的 discard 分开核验两个责任边界：接纳前要求原 failed capture 仍持有 blocked cleanup、未设置删除标记及 engine_dependency 费用；执行期要求原 discard 已取得 stopped VM owner、transferred cleanup、删除标记与 cleanup 费用，并核对原 child link 和未提交终态的 discard 身份；discard 结果独立解码，缺失 sandbox/VM/snapshot 字段不能继承原失败结果的值。两者共享不随阶段变化的失败、stop、generation、sandbox 与 snapshot 历史；不使用 `admitting` 模式开关，也不接受另一阶段的责任作为替代。成功 capture 的 raw cleanup 使用 `CaptureCleanupTarget` 显式核对 VM、instance 和原 cleanup 状态，publication 成功本身不退休 raw 责任。

## 物理 policy 与业务绑定

`DirectPolicies` 拥有物理 policy 核验和回收，`sandbox.DirectBindings` 校验当前业务配置、原 activation 与绑定实例。回收闭包固定原 retirement operation；不得因当前 VM 换代而撤销后继实例。

`ActivationProofReader` 是 machine 声明的只读端口。`sandbox.ActivationHistory` 不捕获 App/runtime/store 实例，先执行完整原 loader 的 hash/key/action/JSON 核验，再返回原资源关联。DBTX 本身含写方法，因此只读是实现约束，不能声称由接口静态保证。缺 reader 明确拒绝。

network policy 更新仍只更新原 instance/operation/body 三列。历史退役先核对原 activation 与已知 generation 的 resource operation/hash/instance，再按原身份删除；不通过当前查询修补缺失证明。

## 运行编排与交接

`Provisioning.AdvanceLocked` 只推进物理 generation。调用方 coordinator 从原 VM guard 的获取到 activation 完成都连续持锁；物理推进不重入或释放该锁。原 admission guard、Lifetime、Workers、故障点与 checkpoint 顺序继续共用。

`SandboxOwnershipReader`、`StopOwnershipReader`、`CaptureDiscardOwnershipReader`、`DeletionOwnershipReader` 与 `ProvisionOwnershipReader` 读取调用方事务中的当前归属和原始历史证明。stop 与 capture-discard 的 sandbox 实现仍核对原 lifecycle、child link、冻结 source 和 fence，不能以当前状态替代它们。

`DeletionParticipant` 与 `ProvisionParticipant` 是明确的写参与接口：sandbox 终止、instance body 发布、activation 阻断与 resume slot 恢复发生在资源调用者的原事务中。`SandboxTermination` 保留原 authenticated tenant SQL 条件与 serialized sandbox body 的区分，不以 body 中的 tenant 替换请求授权。参与者不能 Commit、Checkpoint、启动 worker、执行外部效果或另取 workflow lock。必需的 reader/participant/usage 端口缺失时，接纳与恢复入口返回 ManualRecovery，保留 intent/费用且不启动 worker。适配器位于 sandbox，分别保留原历史核验与原事务写入。

`ReleaseParentAuthority` 只选择原父意图的 quota destination；返回的 selected 区分实际选择与保持源输入。nil 父 admission 保留原语义，release 的错误及查询顺序不变。`supply.ResourceUsage` 使用同一查询视图读取 pool/template 使用计数，machine 不导入 supply。

## 验证边界

现有 Service/SQLite 场景覆盖历史 owner 改写、缺失记录、ABORT/IGNORE 原子交接、backing 最后退责和 direct binding。迁移证据见 [027 实施清单](../../plans/evidence/027-capability-map.md)。这些组件证据不代表真实 KVM、kernel isolation 或 production 验收。
