# Plan 020：设计供应路径与维护延迟的可验证观测合同

> 这是 direction 设计/验证计划，不直接向生产添加指标。完整完成交付物检查后更新索引 020；reviewer 管理时交付设计结果。Drift check：`git diff --stat b93fc24..HEAD -- sandboxd/http.go sandboxd/lifecycle.go sandboxd/reconcile.go sandboxd/types.go sandboxd/warm_fallback.go sandbox-architecture.md plans/designs/020-provisioning-observability.md plans/designs/020-metrics.json plans/designs/020-scenarios.json plans/designs/020-validate.nu`，并检查未提交差异。源码路径只读用于事实核验。

## 状态

- Priority：P3；Effort：M（设计/spike）；Risk：LOW；Depends on：无。
- Category：direction；Planned at：`b93fc24`，2026-11-06；Status：IN PROGRESS（设计已交付，第四轮表述修订及最终源码来源复核中）。
- 审计映射：方向选项 1。017 的维护预算落地时须更新测量点，但本设计不依赖其实现。

## 意图与边界

项目已有 warm/golden/cold 三条供应路径，设计中的延迟数字仍是目标。当前 metrics 仅计 sandbox 状态，无法区分 admission、engine 供应、guest 激活或维护延迟。此计划形成有界、可测量的指标合同和基准方案，供后续实施；不把 testBackend 的快速返回当真实 VM 性能。

## 当前状态与约束

`sandboxd/http.go:172`：

```go
counts := map[string]int{}
for _, b := range bs { counts[b.State]++ }
// root-only endpoint 输出 sandboxd_sandboxes{state=...}
```

`sandboxd/types.go`：

```go
ProvisionSource string `json:"provision_source,omitempty"`
```

`lifecycle.go` claim 标记 warm/golden/cold，warm_fallback.go 允许 warm 失败降为 golden/cold；不能简单在最初选择池时计最终 warm 成功。reconcile.go 的 maintain 串行执行 reconcile、reap、pools。`sandbox-architecture.md` 1.1 的毫秒级数字是目标，README/ACCEPTANCE 明确不是现有测量。

root-only `/metrics` 不因观测扩展变成 tenant 数据接口。禁止 tenant/sandbox/template/snapshot/operation ID 或 host/path 成为指标标签，禁止环境值、凭据、请求体。观测必须有稳定内存上界，不能对所有历史操作逐项解码形成每次 scrape 新瓶颈。

## Scope

执行者只可新增/修改以下交付物及索引状态：

- `plans/designs/020-provisioning-observability.md`：设计、取舍、采样/计时点及后续实施范围；
- `plans/designs/020-metrics.json`：机器可读的有限指标合同；
- `plans/designs/020-scenarios.json`：场景、预期计数/计时口径；
- `plans/designs/020-validate.nu`：只读检查这两份 JSON 的文档校验器。

源码、依赖、Makefile、部署、现有验收材料全部只读。不启动 VM、读取 `.local-run/`、加入 Prometheus server 或实现 exporter。

## 设计要求

合同 JSON 包含 `format:1`、`kind:"design-spike"`、`baseline_commit:"b93fc24"`、`metrics`、`series_ceiling`、`measurement_profiles`。每个 metric 有 name/type/unit/description/finite_labels/allowed_values/buckets/observation_point；未知/失败的 source 必须明确列举，不能临时用任意错误文本作标签。

最小候选：claim duration（admission/provision/activate/total），最终 source（warm/golden/cold/unknown），result（success/failure）；warm ready/pending/target 全节点聚合；maintenance pass duration 与过期处理滞后。保留现有 state gauge，不承诺 per-tenant 导出。可把不必要维度拆到其他指标以降低基数。

series_ceiling 提案 <=512：逐个计算 histogram 的 bucket+Inf+sum+count 乘有限标签组合，counter/gauge 同样计入。必须列出公式与总量；如果维度不满足预算，删减维度或写显式待决，不偷偷提高上限。

计时使用 monotonic elapsed，仅在当前 daemon 进程内；跨崩溃中断的 operation 单列 interrupted/unknown 口径，不能用跨机器 wall-clock 减法伪装精确阶段耗时。异步 accepted 与请求等待时长分开，幂等重查不重复观察原完成，warm fallback 按最终交付分类。

## 命令与核验入口

已有只读结构核验（根目录 Nushell）：

```nu
rg -n 'func .*metrics|ProvisionSource|func .*claim|func .*activate|func .*maintain' sandboxd/http.go sandboxd/types.go sandboxd/lifecycle.go sandboxd/reconcile.go
```

以下 `nu plans/designs/020-validate.nu` 是本计划新增的文档校验器，不是已有功能。无需运行业务测试或安装新工具。

## 步骤

### 1. 建立计时和事件口径表

只读打开上述源码及 sandbox-architecture/ACCEPTANCE，设计文档逐个列出 metric 对应符号、开始/结束事件、失败和取消处理。说明 warm→fallback、admission 拒绝、幂等重查、client timeout 不取消接受的工作、daemon restart 的分类。标记基准数字 unavailable，不借旧 suite 总耗时推导 claim latency。

**Verify**：`rg -n 'admission|provision|activate|idempot|restart|fallback|unavailable' plans/designs/020-provisioning-observability.md` 必须列出全部主题；文档无未核实的实测 SLO。

### 2. 写有限指标合同与场景

写 metrics.json 和 scenarios.json。后者至少包含 warm hit、warm fallback、golden、cold、quota reject、activation failure、duplicate key、timeout-after-acceptance、restart、reaper backlog 十类场景，每类包含预期计数变化、source/result、计时是否可用及观测位置。标注 root-only、禁用标签列表、series 公式及 fixture-vs-real measurement 分类。

**Verify**：

```nu
open plans/designs/020-metrics.json | get format kind baseline_commit
open plans/designs/020-scenarios.json | get scenarios | length
```

预期分别 1/design-spike/b93fc24，场景数 >=10。

### 3. 校验基数并形成测量交接

实现只读 validate.nu，校验必需字段、指标名唯一、有限标签、bucket 递增/非负、计算 series 总量<=512、所有场景引用已定义值；不只验证 JSON 可解析。提供 --self-check 模式，在内存中构造非法 label/重复名称/逆序 bucket 合同并确认同一校验逻辑拒绝；该模式不写临时文件，不针对每个场景硬编码放行。设计中给出三套测量方案：Service+SQLite 明确替身的组件开销、真实 Rust fixture transport、真实 VM 各供应路径；前两者不能证明第三者。列出所需机器/版本、shape、样本次数、warmup、p50/p95、资源容量、artifact 路径所有权及未满足前提时 BLOCKED 的规则。

给出后续实施文件列表、候选依赖和未决定项，而不是在本计划中实现全部 instrumentation。若维护 budget 017 已实施，使用其真实符号和 pass 截止口径，并记录 inspected HEAD。

**Verify**：

```nu
nu plans/designs/020-validate.nu
nu plans/designs/020-validate.nu --self-check
git diff --check
```

预期 exit 0、输出总 series 和场景数量；校验器不修改任何文件。手工构造一份仅在内存中的非法 label 合同必须被同样校验逻辑拒绝，不能通过给校验器加逐场景硬编码掩盖错误。

## 完成标准

- [ ] 四份设计交付物存在、无秘密或真实运行状态，指标与场景有源码观测点。
- [ ] >=10 类场景，<=512 series 的完整公式和机器校验通过。
- [ ] 接纳/等待/完成、重查、fallback 和跨重启语义明确。
- [ ] 三种测量方案严格区分，真实 VM 性能未被虚假宣称，后续实施范围可独立交接。
- [ ] 文档校验、diff/Scope 检查通过，索引标记“设计完成”，不能标“生产指标已上线”。

## STOP 与维护

若必须导出高基数租户明细、需要读取真实秘密或启动生产 VM、或用户要求把目标数字当 SLA，STOP 记录产品决策。后续新增来源、阶段和错误类别要同步 series 计算；指标不是资源归属证明。分支 `advisor/020-observability-design`；另获提交授权才使用 `plans: specify bounded provisioning observability`。
