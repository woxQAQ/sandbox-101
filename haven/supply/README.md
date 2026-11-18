# Supply 模板与供给责任

Supply 拥有 template put/build、refill、模板与 warm publication、失败保留和独立 cleanup。模板与 pool 查询共用本包私有 dbgen；TemplateRecord 与 PoolReservation 保留原冻结字段顺序、tags 和 nil。

TemplateCatalog 从调用方的同一 DBTX 核对 catalog、永久 tenant、原 publisher operation 和 supply step。TemplateState 与纯行复制 TemplatePublisher/TemplateIdentity/Intent 已删除；用例直接消费生成行。LoadTemplate 保留 JSON 解码，CurrentTemplate 区分缺失与损坏，RequireTemplateOwner 核对永久归属。publication 的原条件 upsert、body CAS、SET 列与 RequireOne 保持。

build 在 guest Exec 前持久化 warmup intent；warmup_inflight 的未知结果保留资源。refill 依据原零 slot reservation、generation、instance 与 link 交接 warm/image 责任。SupplyAdmission 在资源调用者事务中写 parent、credential、supply step、image holder、template intent/pool reservation 和 child link，不自行提交或派发。

RawTarget 仅携带新 allocation 的 sandbox ID、budget 和父意图，不允许 activation credentials、复用 delivered sandbox 或已有 slot。RawProvisioner 由原 provisioning coordinator 提供，沿用原 admission 事务与连续 VM guard。`ProvisionRequest` 将 tenant/key、冻结请求字节和预检的 template/image/restore 来源一起传递；coordinator 从原请求字节计算幂等 hash，不接受独立指定的 hash。在原 allocation 事务中先复核来源，再核验 tenant/host budget，随后写入资源意图、归属和费用。模板授权属于 Supply，sandbox 当前归属通过调用方事务中的只读端口核验。

SupplyCleanup 使用独立 node-scoped 意图，冻结原 source、pool、失败 capture 列表及已有 release receipt。它先完成 stop、capture discard 和 VM 回收，最后在原事务解除 collector 自己的 pool/image/fence；未知及完成写失败保留这些责任，重放使用原子操作 receipt。

ResourceUsage 与 UnresolvedTemplates 接收调用方 DBTX，供 machine deletion 和 gc 在同一 snapshot 中核验引用；不另开数据库或事务。CommandIdentity 共享原角色 MAC、幂等与 secret 内存恢复协议；Supply 不依赖 sandbox 的配置对象。

PoolState 已删除。LoadPoolReservation 解码并核对冻结身份；ValidateWarmCatalog/ReadyWarm 核对原 refill 与永久 owner，SelectWarm 在调用方事务中按原顺序消费 ready row、删除 warm 引用并标记 selected。RetireSelectedReservation 仍以原 body 和 selected 状态作 CAS；ObsoleteWarmCandidates 保留候选顺序及出错前已收集的结果。内部计数和状态转换直接使用本包生成查询。

supply/supply_cleanup 的 payload、推进、cleanup 与 backoff 查询均来自本包私有 SQL；StepKey 和 cleanup ID namespace 区分持久记录的归属。最终验证与环境缺项见 [计划](../../plans/027-haven-capability-boundaries.md)。
