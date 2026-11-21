# Sandbox 业务责任

业务 HTTP、令牌、SDK 与配额责任见 [Sandbox API 契约](../../docs/sandbox-api.md)。

sandbox 拥有 claim/configuration、activation、生命周期、checkpoint binding、warm claim、fork、业务投递与 provisioning coordinator。物理 generation、capture、stop 和删除由 machine 执行，模板与 pool 由 supply 持有。`CommandAdmission` 在分派前校验 sandbox token 的目标与动作，HTTP 和进程内提交共用这项检查。模板和 refill 的接纳由 `supply.Supply` 执行。

## 证明与原子交接

LifecycleAdmission 在资源调用者事务中核验当前 sandbox/settings/fence 与冻结父意图，写入 parent、credential、step、fence、source lease、上层责任和 child link。它不提交、不 checkpoint、不派发效果，也不另取 workflow lock。release/stop/capture-discard 的 history reader 仍检查原 lifecycle、hash、source、child link 和 fence；当前状态不能替代缺失历史。

DeletionHandoff 和 ProvisionHandoff 是显式写参与者，使用调用方原事务更新 sandbox body、activation 或 slot。VMProvisioning 连续持有原 VM guard，在 machine.Provisioning.AdvanceLocked 返回后继续 activation；中间不释放或重入该锁。admission、commit、secret 登记、checkpoint、fault 与 dispatch 仍按原次序发生。

## 生命周期

pause 分别保留 capture publication、sleep 引用/费用与 VM deletion receipt。只有原回收证明和冻结 slot 目的地一致才能完成 paused。resume 失败经原补偿恢复 paused，保留 sleep snapshot；rollback 固定 checkpoint 来源，通过原子链接续接 stop、raw cleanup、restore 和 activation。

renew 使用原 owner、deadline 与 fence。reaper 完整读取租约候选后，提交带 observed deadline 的普通生命周期命令；后续续期能阻断旧回收意图。未知、损坏或 SQL 失败保留资源与费用。

fork 先提交全部目标、slot 与 forecast，再在原事务接纳各 branch；效果只在提交后派发。补偿等待每个 discard/release receipt，父终态消费原 branch publication，不从当前 sandbox 重建历史结果。

warm claim 原子选择 ready pool、接管 slot、settings 与 activation；fallback 保留原 snapshot、零 slot 出生证明与失败 activation。Supply 的 pool 写入和 sandbox 的 slot/归属写入使用同一事务，原 CAS/RequireOne 条件保持。

## 命令、secret 与投递

operation.CommandIdentity 负责原角色 MAC、幂等凭据匹配及 accepted operation 的内存 secret 恢复。AdmissionPolicy 验证环境、policy、TTL，分离 guest 值、host secret 与描述；只有净化后的命令进入 SQLite。完整 secret 仅留在共享 ephemeral.Registry，publication 不保留凭据。

OperationDelivery 组合通用 operation 结果与 sandbox 授权。DirectBindings 核验业务设置和原 activation，再借 machine 执行物理 policy；回收闭包固定原实例与原 operation。配置失败和持久完成失败继续保留 fence，不能误撤后继实例。

## 持久访问

用例直接消费本包私有 dbgen。LoadSandbox/LoadSettings/LoadOwnedCheckpoint 保留原解码和身份检查；CapabilityLookup 只实现授权的历史/活动 capability 查找。RenewActivityLease 在数据面的连续 configuration guard 内比较原 settings bytes 更新 deadline，checkpoint 仍由调用方执行。业务 step、cleanup 的 SQL 谓词、SET 列、RequireOne 与原事务顺序保持。验证见 [027 证据](../../plans/evidence/027-capability-map.md)。
