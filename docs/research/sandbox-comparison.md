# Microsandbox、Cocoonstack 与 CubeSandbox 的架构比较

三个项目都用独立客体内核隔离工作负载，但它们交付给上层的东西不同。Microsandbox 提供可嵌入应用的 VM 运行时；Cocoonstack 在 VM 管理器上叠加热池、租约和 sandbox 服务；CubeSandbox 将容器运行接口、模板、调度、网络与存储组织成集群平台。

这决定了比较的起点。Microsandbox 的本地运行依赖少，不代表已经具备 CubeSandbox 的集群控制面。CubeSandbox 的服务多，也不能据此推断它的单次快照更完整。Cocoonstack 同时存在普通 VM、无网卡 sandbox 和受控 bridge 出站模式，这几条路径的能力不能合并成一列“全部支持”。

版本范围沿用三篇分析：Microsandbox `0feec232`；Cocoon `b3b40f3`、sandbox `421e475` 与 Cloud Hypervisor fork `a02b114bd`；CubeSandbox `5741deb4`。Cocoonstack 的各仓库独立发布，相邻 checkout 不代表锁定的发布组合。

## 1. 部署单位决定了系统由谁管理

| 问题 | Microsandbox | Cocoonstack | CubeSandbox |
|---|---|---|---|
| 谁拥有一台运行中的 VM？ | 每 VM 一个 runtime 进程，内嵌 VMM | 每 VM 一个独立 VMM 进程，Cocoon 管生命周期 | 每 sandbox 一个 CubeShim，进程内运行 CubeHypervisor |
| 谁组织产品生命周期？ | SDK、CLI 与 runtime | sandboxd 管热池、claim、租约和检查点 | CubeMaster、Cubelet、containerd 与生命周期管理器 |
| 普通本地使用的服务依赖 | 不需要中央 daemon | Cocoon daemon 可选；使用 sandbox 产品时需要 sandboxd | 单节点也需要一组控制面、节点服务与状态依赖 |
| 多节点怎样选位置？ | 参考实现不能从 cloud 客户端推导出完整远端调度结构 | 节点库存与 mesh，或 Kubernetes 聚合 API；claim 由目标 sandboxd 接纳 | CubeMaster 选节点，Cubelet 实际创建与恢复 |
| Kubernetes 管什么？ | 容器包装与设备接入不自动构成调度产品 | 可用 Pod/provider 路径，也可由聚合 API 映射节点库存 | DaemonSet 部署节点基础设施；业务 sandbox 不逐个对应普通 Pod |
| 客体管理通道 | virtio-console 上的 agent 协议 | vsock 上的 cocoon-agent 与 silkd | vsock 上的 cube-agent；envd 另提供业务 API |

### 1.1 Microsandbox 将复杂性集中在每台 VM 的运行时

一个 runtime 同时协调 VMM、用户态网络、文件系统后端和捕获状态。很多本来需要跨进程协商的操作可以在同一运行时内完成，尤其是网络激活、设备静止和快照代次核验。

代价也集中在这个进程。VMM、网络解析和文件系统后端共享进程故障范围。把 SDK 包装成多租户服务时，调用者仍需提供 API 授权、节点调度、宿主权限限制和配额持久化。进程内的网络策略不能替代这些工作。

### 1.2 Cocoonstack 将 VM 管理与 sandbox 交付分开

Cocoon 可以单独管理 VM；sandboxd 再将它们组织成可借出的热容量。Cocoon-agent 处理基础管理，silkd 处理文件、进程、PTY、会话和预览端口。

分层允许两套接口各自演进，也产生了跨层时序约束。Cocoon 已经恢复 VM，并不表示 sandboxd 已经安装产品策略。受控 bridge 模式禁止部分恢复操作，正是这一间隙的实际后果。

### 1.3 CubeSandbox 采用集群平台的责任分配

CubeMaster 保存集群规格与调度信息，Cubelet 管节点实例，containerd 调用 Shim，CubeProxy 和 CubeEgress 分别承担入站与选定的出站流量，TemplateCenter 管制品。可选 CubeS3lvol 再承担节点级块存储。

这套分工覆盖了更多平台职责，也增加了部分成功的状态。VM 创建完成、路由已经可用、SQL 规格已经保存、远端快照已经可读，是不同事实。SQL、Redis、节点文件系统与 VM 没有一个统一事务，部分路径记录持久化错误后仍会继续。

### 1.4 平台支持要区分宿主与客体

Microsandbox 的跨平台主要指 Linux、macOS 与 Windows 宿主运行匹配架构的 Linux 客体，不是跨 ISA 仿真。Cocoonstack 的 Linux sandbox 主路径依赖 Linux/KVM，另有 Linux 宿主上的 macOS 客体产品。CubeSandbox 主路径依赖 Linux/KVM、TC/eBPF 与节点存储配置。

因此，“支持 macOS”在 Microsandbox 和 cocoon-macos 中回答的是两个不同问题。

## 2. 网络最大的区别是连接在哪里终止

三者都让客体 Linux 处理应用的网络调用。区别发生在客体之外。

Microsandbox 让宿主用户态后端处理虚拟网卡报文，再建立宿主连接。Cocoon 的普通 VM 与 CubeSandbox 的普通 NAT 路径继续传递客体 TCP 报文，不替客体建立第二条 TCP 连接。Cocoonstack 的无网卡 sandbox 则通过应用代理和 vsock 请求宿主拨号。

图 1 中，连续蓝线表示一条客体到远端的 TCP 连接。线在代理框处断开，表示代理终止一侧连接并建立另一侧连接。vsock 段只搬运应用字节。

```vega
{
  "$schema":"https://vega.github.io/schema/vega/v6.json",
  "width":1040,"height":680,"padding":20,
  "data":[
    {"name":"zones","values":[
      {"x":15,"w":245,"t":"客体"},
      {"x":280,"w":480,"t":"宿主"},
      {"x":780,"w":245,"t":"远端服务"}
    ]},
    {"name":"links","values":[
      {"x":150,"x2":465,"y":170,"dash":[1,0]},
      {"x":645,"x2":915,"y":170,"dash":[1,0]},
      {"x":150,"x2":465,"y":315,"dash":[7,5]},
      {"x":645,"x2":915,"y":315,"dash":[1,0]},
      {"x":150,"x2":915,"y":460,"dash":[1,0]},
      {"x":150,"x2":915,"y":605,"dash":[1,0]}
    ]},
    {"name":"ends","values":[
      {"x":150,"y":170},{"x":915,"y":170},
      {"x":150,"y":315},{"x":915,"y":315},
      {"x":150,"y":460},{"x":915,"y":460},
      {"x":150,"y":605},{"x":915,"y":605}
    ]},
    {"name":"proxies","values":[
      {"y":140,"t":"用户态 TCP 后端"},
      {"y":285,"t":"HTTP / SOCKS 代理"}
    ]},
    {"name":"labels","values":[
      {"x":35,"y":115,"t":"Microsandbox","s":21},
      {"x":35,"y":260,"t":"Cocoonstack 无网卡","s":21},
      {"x":35,"y":405,"t":"Cocoon 普通 VM","s":21},
      {"x":35,"y":550,"t":"CubeSandbox 普通出站","s":21},
      {"x":305,"y":155,"t":"客体侧 TCP","s":16},
      {"x":650,"y":155,"t":"宿主 TCP","s":16},
      {"x":305,"y":300,"t":"vsock 字节流","s":16},
      {"x":650,"y":300,"t":"宿主 TCP","s":16},
      {"x":345,"y":438,"t":"TAP / TC / CNI，只转发报文","s":18},
      {"x":345,"y":583,"t":"TAP / TC / eBPF，改写地址与端口","s":18},
      {"x":35,"y":208,"t":"客体有网卡，宿主不透明转发任意 IP 协议","s":16},
      {"x":35,"y":353,"t":"客体没有 NIC，应用需使用代理；不提供通用 UDP 出口","s":16},
      {"x":35,"y":498,"t":"路由、NAT 与网络隔离取决于 CNI 和宿主配置","s":16},
      {"x":35,"y":643,"t":"选中 L7 的 TCP 改走 CubeEgress，届时才拆成两侧连接","s":16}
    ]}
  ],
  "marks":[
    {"type":"rect","encode":{"enter":{"width":{"signal":"width"},"height":{"signal":"height"},"fill":{"value":"#f4f4f4"}}}},
    {"type":"text","encode":{"enter":{"x":{"value":15},"y":{"value":30},"text":{"value":"01 / 同样是出站，连接的拥有者不同"},"fontSize":{"value":25},"fontWeight":{"value":"bold"},"fill":{"value":"#1e1e1e"}}}},
    {"type":"rect","from":{"data":"zones"},"encode":{"enter":{"x":{"field":"x"},"y":{"value":55},"width":{"field":"w"},"height":{"value":35},"fill":{"value":"#ffffff"},"stroke":{"value":"#787878"}}}},
    {"type":"text","from":{"data":"zones"},"encode":{"enter":{"x":{"signal":"datum.x+14"},"y":{"value":80},"text":{"field":"t"},"fontSize":{"value":18},"fill":{"value":"#1e1e1e"}}}},
    {"type":"rule","from":{"data":"links"},"encode":{"enter":{"x":{"field":"x"},"x2":{"field":"x2"},"y":{"field":"y"},"stroke":{"value":"#0043ce"},"strokeWidth":{"value":4},"strokeDash":{"field":"dash"}}}},
    {"type":"rect","from":{"data":"proxies"},"encode":{"enter":{"x":{"value":465},"y":{"field":"y"},"width":{"value":180},"height":{"value":60},"fill":{"value":"#c8d4ed"},"stroke":{"value":"#003bb5"}}}},
    {"type":"text","from":{"data":"proxies"},"encode":{"enter":{"x":{"value":555},"y":{"signal":"datum.y+36"},"text":{"field":"t"},"align":{"value":"center"},"fontSize":{"value":17},"fill":{"value":"#1e1e1e"}}}},
    {"type":"symbol","from":{"data":"ends"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"size":{"value":150},"fill":{"value":"#0043ce"}}}},
    {"type":"text","from":{"data":"labels"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"text":{"field":"t"},"fontSize":{"field":"s"},"fill":{"value":"#1e1e1e"}}}}
  ]
}
```

### 2.1 协议能力与权限粒度不能互相替代

| 场景 | Microsandbox | Cocoonstack | CubeSandbox |
|---|---|---|---|
| 普通 TCP | 用户态后端与宿主 socket 中继 | direct 路径传包；受控路径用 HTTP CONNECT 或 SOCKS CONNECT | IPv4 NAT；选中的 TCP 进入透明 L7 代理 |
| 普通 UDP | 专用 relay，有会话与队列限制 | direct 可承载；受控代理不支持 UDP ASSOCIATE | IPv4 UDP NAT，不经过 HTTP 检查 |
| ICMP | 部分 echo 与 MTU 错误处理，受宿主权限和平台限制 | direct 依赖网络栈；受控代理不提供 ping | 主要处理 echo 与 identifier 转换，不是完整 ICMP 路由器 |
| IPv6 | 有地址与协议处理，是否启用取决于配置和宿主条件 | CNI 地址管理不能概括为完整双栈；无 NIC 模式不提供通用 IP 网络 | 当前 TAP 路径丢弃非 IPv4/ARP 报文 |
| DNS | 解析、过滤、响应地址检查、域名绑定；支持 UDP/TCP，DoT 有专用条件 | direct 用普通 DNS；受控代理从请求得到域名并在宿主解析 | 观察 UDP/53 的 IPv4 A 查询和响应，学习临时 IP 规则 |
| TLS | 可透传或 MITM，域名授权结合 authority 检查 | CONNECT 隧道或指定目标 MITM，拦截目前使用 HTTP/1.1 | 普通 TCP 透传；选中 L7 后由 CubeEgress MITM |

如果业务依赖 UDP、QUIC 或原始 IP 行为，Cocoonstack 的无网卡模式不能直接替代网卡。切换到 direct 虽能扩大协议范围，却同时改变了策略执行位置。Microsandbox 和 CubeSandbox 也各有协议限制，不能仅凭“客体有 Linux 网卡”推断外部路径透明。

### 2.2 同一条域名规则表达了不同权限

考虑两个域名共享一个 IP 的情况，只希望访问其中一个域名。

Microsandbox 的 DNS 绑定记录域名与地址关系。默认 strict 域名授权还要求可检查的应用层 authority；单独知道目标 IP，并不自动获得该 IP 上其他服务的访问权。

Cocoonstack 受控代理直接拿到 CONNECT 或 SOCKS 域名目标，由宿主解析并检查实际拨号地址。它不需要从旁路 DNS 报文推断“这个 IP 原本属于哪个域名”。代价是应用必须通过支持的代理协议访问。

CubeSandbox 普通域名规则把解析结果变成 IP 允许项，可以允许这个 IP 的其他端口和服务。其 DNS 学习还有至少 300 秒 TTL，删掉域名规则不会立即清除已经学到的地址。需要 HTTP 主机名、方法和路径限制时，应讨论 L7 路径，不能用普通域名 allow 代替。

三者的规则组合也不同。Microsandbox 使用有序规则与方向默认值，MultiTenant 另加平台限制；Cocoonstack 将池策略与租户 egress class 取交集；CubeVS 的基础判定是 allow 优先于 deny，再到默认允许。相同文字的“白名单”，转换后的权限集合并不相同。

### 2.3 密钥注入的集成方式不同

Microsandbox 向客体提供占位符，在支持的 HTTP header/body 中替换为宿主密钥。Cocoonstack 可把 claim 凭据留在节点私有记录中，经过指定目标代理注入。CubeEgress 根据实例策略注入请求头，先移除客户端同名头。

部署与应用适配各有成本：

- Microsandbox 和 CubeSandbox 的透明路径通常不要求应用配置 HTTP 代理，但 TLS 拦截要求应用信任代理 CA。
- Cocoonstack 的无网卡路径要求应用使用 HTTP/SOCKS 代理，根 CA 与节点 intermediate CA 分开管理。
- 三者都不能仅凭宿主注入保证秘密永不回到客体。上游回显、二次请求和其他输入通道仍会传递数据。

CubeEgress 还区分“允许请求”与“允许注入”。Host/SNI 核对失败可以只取消注入，已允许的请求继续发送。应用授权与凭据授权不能视作一个开关。

### 2.4 入站同样有不同的身份入口

Microsandbox 的端口发布创建宿主 listener，将流量送到客体端口。Cocoonstack 的预览由 silkd 在客体内部连接端口，再通过 vsock 送回 sandboxd，所以没有网卡也可以提供 Web 预览。CubeProxy 根据实例和端口定位节点，检查流量 token，并协调暂停实例的唤醒。

CubeProxy 的 token 保护属于 HTTP 代理入口。直接暴露节点映射端口不会自动得到同一保护。Cocoonstack 的签名预览 URL 也不等于给客体发放一张公网网卡。

## 3. OCI 在三个项目中扮演不同角色

| 问题 | Microsandbox | Cocoonstack 的 OCI VM | CubeSandbox 模板主路径 |
|---|---|---|---|
| 普通容器镜像是否负责 VM 引导？ | 不负责，内核与 agent 由运行时提供 | 镜像必须提供 kernel/initrd 等 VM 引导材料 | 不负责，业务镜像与 Cube 系统镜像分开 |
| 业务只读内容 | 逐层 EROFS，fsmeta 合并目录元信息 | 每个 OCI 层各自成为 EROFS 盘 | 按 OCI 顺序合并内容，制作业务 ext4 |
| 客体如何看到 lower？ | fsmeta 与层文件映射成一块 lower 设备 | 多块设备、多次 EROFS 挂载，再叠加 | 业务 ext4 通过 pmem 作为 lower |
| 私有修改 | ext4 upper，通常受 raw/qcow2 历史层管理 | ext4 sparse raw 中的 upper/work | 私有 ext4 卷中的 upper/work |
| 谁组装业务根？ | agentd | initramfs | cube-agent，在已经引导的管理客体中创建容器 |

图 2 只比较只读镜像如何进入业务根。蓝色上层表示客体的合并视图，底部斜放的片层表示镜像材料。线是数据依赖，不是启动步骤。可写 upper 在三条路径中都另行存在。

```vega
{
  "$schema":"https://vega.github.io/schema/vega/v6.json",
  "width":1080,"height":600,"padding":20,
  "data":[
    {"name":"roots","values":[
      {"x":20,"name":"Microsandbox","sub":"一个合成 lower"},
      {"x":380,"name":"Cocoon OCI","sub":"多个独立 lower"},
      {"x":740,"name":"CubeSandbox","sub":"一个业务 ext4 lower"}
    ]},
    {"name":"devices","values":[
      {"x":30,"w":300,"t":"fsmeta + EROFS 映射","y":265},
      {"x":390,"w":90,"t":"层一盘","y":265},
      {"x":495,"w":90,"t":"层二盘","y":265},
      {"x":600,"w":90,"t":"层三盘","y":265},
      {"x":750,"w":300,"t":"业务 pmem 盘","y":265}
    ]},
    {"name":"layers","values":[
      {"x":45,"y":385,"w":250,"t":"共享 EROFS 层一"},
      {"x":60,"y":425,"w":250,"t":"共享 EROFS 层二"},
      {"x":75,"y":465,"w":250,"t":"共享 EROFS 层三"},
      {"x":400,"y":385,"w":250,"t":"共享 EROFS 层一"},
      {"x":415,"y":425,"w":250,"t":"共享 EROFS 层二"},
      {"x":430,"y":465,"w":250,"t":"共享 EROFS 层三"},
      {"x":765,"y":425,"w":250,"t":"已经合并的业务 ext4"}
    ]},
    {"name":"links","values":[
      {"x":180,"y":195,"x2":180,"y2":265},
      {"x":540,"y":195,"x2":435,"y2":265},
      {"x":540,"y":195,"x2":540,"y2":265},
      {"x":540,"y":195,"x2":645,"y2":265},
      {"x":900,"y":195,"x2":900,"y2":265},
      {"x":110,"y":315,"x2":65,"y2":385},
      {"x":180,"y":315,"x2":80,"y2":425},
      {"x":250,"y":315,"x2":95,"y2":465},
      {"x":435,"y":315,"x2":420,"y2":385},
      {"x":540,"y":315,"x2":435,"y2":425},
      {"x":645,"y":315,"x2":450,"y2":465},
      {"x":900,"y":315,"x2":900,"y2":425}
    ]}
  ],
  "marks":[
    {"type":"rect","encode":{"enter":{"width":{"signal":"width"},"height":{"signal":"height"},"fill":{"value":"#f4f4f4"}}}},
    {"type":"text","encode":{"enter":{"x":{"value":20},"y":{"value":30},"text":{"value":"02 / 同样使用 OverlayFS，只读层的组织方式不同"},"fontSize":{"value":25},"fontWeight":{"value":"bold"},"fill":{"value":"#1e1e1e"}}}},
    {"type":"text","from":{"data":"roots"},"encode":{"enter":{"x":{"field":"x"},"y":{"value":85},"text":{"field":"name"},"fontSize":{"value":23},"fontWeight":{"value":"bold"},"fill":{"value":"#1e1e1e"}}}},
    {"type":"text","from":{"data":"roots"},"encode":{"enter":{"x":{"field":"x"},"y":{"value":116},"text":{"field":"sub"},"fontSize":{"value":18},"fill":{"value":"#565656"}}}},
    {"type":"rule","from":{"data":"links"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"x2":{"field":"x2"},"y2":{"field":"y2"},"stroke":{"value":"#787878"},"strokeWidth":{"value":2}}}},
    {"type":"rect","from":{"data":"roots"},"encode":{"enter":{"x":{"signal":"datum.x+10"},"y":{"value":145},"width":{"value":300},"height":{"value":50},"fill":{"value":"#c8d4ed"},"stroke":{"value":"#003bb5"}}}},
    {"type":"text","from":{"data":"roots"},"encode":{"enter":{"x":{"signal":"datum.x+160"},"y":{"value":177},"text":{"value":"业务根中的合并视图"},"align":{"value":"center"},"fontSize":{"value":19},"fill":{"value":"#1e1e1e"}}}},
    {"type":"rect","from":{"data":"devices"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"width":{"field":"w"},"height":{"value":50},"fill":{"value":"#ffffff"},"stroke":{"value":"#787878"}}}},
    {"type":"text","from":{"data":"devices"},"encode":{"enter":{"x":{"signal":"datum.x+datum.w/2"},"y":{"signal":"datum.y+32"},"text":{"field":"t"},"align":{"value":"center"},"fontSize":{"value":17},"fill":{"value":"#1e1e1e"}}}},
    {"type":"path","from":{"data":"layers"},"encode":{"enter":{"path":{"signal":"'M'+datum.x+','+datum.y+' l'+datum.w+',0 l-20,30 l-'+datum.w+',0 Z'"},"fill":{"value":"#e8e8e8"},"stroke":{"value":"#787878"}}}},
    {"type":"text","from":{"data":"layers"},"encode":{"enter":{"x":{"signal":"datum.x+datum.w/2-10"},"y":{"signal":"datum.y+21"},"text":{"field":"t"},"align":{"value":"center"},"fontSize":{"value":16},"fill":{"value":"#1e1e1e"}}}},
    {"type":"text","encode":{"enter":{"x":{"value":20},"y":{"value":550},"text":{"value":"Microsandbox 在宿主合并元信息；Cocoon 在客体组合多层；Cube 先物化业务文件系统。"},"fontSize":{"value":19},"fill":{"value":"#1e1e1e"}}}},
    {"type":"text","encode":{"enter":{"x":{"value":20},"y":{"value":583},"text":{"value":"Cube 的系统盘和 agent 盘独立于右侧业务根；Cocoon 可用 squash 运行镜像减少层数。"},"fontSize":{"value":17},"fill":{"value":"#565656"}}}}
  ]
}
```

### 3.1 三种布局把成本放在不同地方

Microsandbox 保留逐层数据共享，在宿主预先合并目录元信息。客体只需看到一个合成 lower，代价是宿主需要维护 fsmeta、VMDK extent 和缓存路径绑定。

Cocoon 的设备与 OCI 层直接对应，映射关系更直观。但层数会增加客体设备数、挂载数和根装配工作。sandbox 的 squash 运行镜像将这部分成本提前到构建阶段，以减少运行时层数。

CubeSandbox 默认先将 OCI 内容合成 ext4，再启动构建 VM、运行应用并捕获 APP snapshot。它较少保留运行时的 OCI 层形态，更多复用业务制品、模板内存与可写盘快照。系统与 agent 单独成盘，也使业务镜像不必承担管理客体的引导职责。

这三条路径都可能让多个实例复用只读内容，但不能因此认为具有相同的跨镜像去重粒度。

### 3.2 可写盘的历史有两种不同结构

Microsandbox 的受管理磁盘会封存旧层，再为源实例与分支建立新的 qcow2 可写头。读取某个块可能依赖历史 backing 层，因此搬运和回收必须保持整条物理依赖。

Cocoon 的 OCI raw 盘快照与 CubeCoW 本地快照都是逻辑独立的文件。reflink 让它们共享宿主 extent，写入后由文件系统分离；读取不必遍历快照父子关系。

两者对宿主的要求仍不同。Cocoon 在不能 reflink 时退回 sparse copy，功能可继续，复制成本增加。CubeCoW 本地后端在初始化时要求 FICLONE 能力，不满足就拒绝，避免把低成本克隆静默变成完整复制。

CubeS3lvol 是另一种存储系统。它通过本机 NVMe/TCP 提供块设备，用 WAL、缓存和 S3 对象管理数据。它增加了节点身份、远端引用、读者 lease 和物化任务，不能按本地 reflink 文件的回收规则理解。

## 4. 快照比较要同时看捕获和恢复

### 4.1 谁保证内存与磁盘来自同一时刻？

| 机制 | Microsandbox | Cocoonstack | CubeSandbox |
|---|---|---|---|
| 业务侧冻结 | agent 冻结工作负载，并协调输入记录与输出截点 | 主要在 VMM 暂停边界捕获 | 主要在 VMM 暂停边界捕获 |
| 设备与执行状态 | 核验同一 pause generation，按设备、执行状态、RAM 顺序捕获 | CH 暂停并捕获，暂停期复制受管理磁盘 | 协调路径暂停 vCPU、排空设备 I/O，再保持冻结直到 rootfs 捕获完成 |
| 遇到捕获不了的资源 | 按资源能力拒绝，或使用明确的恢复处理方式 | 拒绝特定设备组合；sandboxd 另限制 dataset 与 egress 模式 | 外部 Volume 单独处理；host-mount 限制恢复位置 |
| 需要注意的其他路径 | disk-only 与 full 的刷盘承诺不同 | CH 与 Firecracker 快照格式、设备约束不同 | 旧版或未知 Shim 走 legacy，RAM 捕获与 rootfs 捕获间可能运行 |

Microsandbox 的协调范围不仅包括 VM 内存和磁盘，还包括宿主管理通道上的记录边界。对交互式 exec 或持续输出，恢复后是否重复输入、丢失输出，与进程能否继续运行同样重要。

Cocoon 与 CubeSandbox 的主要捕获边界在 VMM 暂停期间。CubeSandbox 必须再区分协调路径与旧路径，不能把新协议的冻结保证赋给所有已部署 Shim。

三者都不能用 VM 快照回滚外部数据库或已经完成的远端请求。所谓应用一致性必须说明包含哪些资源，不能只看快照标签。

### 4.2 RAM 共享与增量捕获各自省什么？

三个项目都可以通过不可变内存基底和私有写时复制，减少多个恢复实例对干净页的重复占用。业务写入越多，私有页越多；共享基底不能让持续变化的工作集保持免费。

持久捕获的组织不同：

- Microsandbox 用内存区段清单引用不可变对象，未变区段可以继续引用旧对象，零区段无需实际数据对象。
- Cocoon 的 CH fork 可以先 reflink 原内存文件，再覆盖变化页；条件不满足时写出完整内存。
- CubeSandbox 根据可用基底选择 full、匿名 CoW 页或 soft-dirty 与匿名页交集。写出范围必须与目标基底对应。

Cocoon 和 CubeSandbox 的增量结果仍是逻辑完整的内存文件，不要求恢复时逐份重放 delta。Microsandbox 的清单也描述完整地址空间，只是数据分散在被引用的对象中。

### 4.3 恢复之后，业务何时获准运行？

Microsandbox 对这个问题有显式顺序。内核先运行，工作负载仍冻结；内核确认新 VM Generation ID、随机数和时钟处理后，宿主才解冻业务，最后开放网络。

Cocoon 有 VM Generation ID 设备和 agent 补熵两种机制，但设备依赖客体驱动与发现路径。CLI 的补熵默认在 resume 后异步执行，不构成调用返回前的确认屏障。

CubeSandbox 恢复后同步等待 agent 的时间与补熵 RPC，但此时 vCPU 已经执行。等待 RPC 返回，比后台发起更明确，仍不等于租户代码执行前已经完成。

图 3 比较的是执行许可，不是耗时。横向阶段只表示先后关系，蓝色表示可以运行，灰色表示仍受阻。Cocoon 行展示普通可恢复路径，不包含被拒绝的受控 egress claim。

```vega
{
  "$schema":"https://vega.github.io/schema/vega/v6.json",
  "width":1080,"height":630,"padding":20,
  "data":[
    {"name":"bars","values":[
      {"x":280,"y":145,"w":735,"h":25,"fill":"#c8d4ed"},
      {"x":280,"y":185,"w":410,"h":25,"fill":"#e8e8e8"},
      {"x":690,"y":185,"w":325,"h":25,"fill":"#c8d4ed"},
      {"x":280,"y":225,"w":575,"h":25,"fill":"#e8e8e8"},
      {"x":855,"y":225,"w":160,"h":25,"fill":"#c8d4ed"},
      {"x":280,"y":345,"w":735,"h":25,"fill":"#c8d4ed"},
      {"x":280,"y":385,"w":735,"h":25,"fill":"#c8d4ed"},
      {"x":280,"y":505,"w":735,"h":25,"fill":"#c8d4ed"},
      {"x":280,"y":545,"w":735,"h":25,"fill":"#c8d4ed"}
    ]},
    {"name":"ticks","values":[
      {"x":280,"y":120,"y2":260},{"x":690,"y":120,"y2":260},{"x":855,"y":120,"y2":260},
      {"x":280,"y":320,"y2":420},{"x":620,"y":320,"y2":420},
      {"x":280,"y":480,"y2":580},{"x":620,"y":480,"y2":580},{"x":900,"y":480,"y2":580}
    ]},
    {"name":"labels","values":[
      {"x":20,"y":30,"t":"03 / 恢复了内存，不等于已经允许业务运行","s":25},
      {"x":20,"y":92,"t":"Microsandbox","s":22},
      {"x":20,"y":163,"t":"客体内核","s":18},
      {"x":20,"y":203,"t":"捕获的业务冻结域","s":18},
      {"x":20,"y":243,"t":"网络激活","s":18},
      {"x":280,"y":110,"t":"内核运行","s":17},
      {"x":620,"y":110,"t":"确认代次并解冻","s":17},
      {"x":860,"y":110,"t":"开放网络","s":17},
      {"x":20,"y":300,"t":"Cocoon","s":22},
      {"x":20,"y":363,"t":"客体内核","s":18},
      {"x":20,"y":403,"t":"业务进程","s":18},
      {"x":280,"y":310,"t":"恢复执行","s":17},
      {"x":620,"y":310,"t":"agent 补熵异步进行，无完成屏障","s":17},
      {"x":20,"y":460,"t":"CubeSandbox","s":22},
      {"x":20,"y":523,"t":"客体内核","s":18},
      {"x":20,"y":563,"t":"业务进程","s":18},
      {"x":280,"y":470,"t":"恢复执行","s":17},
      {"x":620,"y":470,"t":"调用时间与补熵 RPC","s":17},
      {"x":900,"y":470,"t":"确认完成","s":17},
      {"x":20,"y":620,"t":"蓝色：允许执行或开放。灰色：仍受阻。阶段长度不代表耗时，也不表示线程一定获得调度。","s":17}
    ]}
  ],
  "marks":[
    {"type":"rect","encode":{"enter":{"width":{"signal":"width"},"height":{"signal":"height"},"fill":{"value":"#f4f4f4"}}}},
    {"type":"rect","from":{"data":"bars"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"width":{"field":"w"},"height":{"field":"h"},"fill":{"field":"fill"},"stroke":{"value":"#787878"}}}},
    {"type":"rule","from":{"data":"ticks"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"y2":{"field":"y2"},"stroke":{"value":"#787878"},"strokeDash":{"value":[4,4]}}}},
    {"type":"text","from":{"data":"labels"},"encode":{"enter":{"x":{"field":"x"},"y":{"field":"y"},"text":{"field":"t"},"fontSize":{"field":"s"},"fill":{"value":"#1e1e1e"}}}}
  ]
}
```

这个差别会改变克隆安全的推理方式。Microsandbox 可以在特定冻结域内说明业务解冻前满足了哪些条件。Cocoon 与 CubeSandbox 不能仅凭 agent 最终补熵成功，推出业务在补熵之前没有执行。

三者的内核 reseed 都不自动重置用户态 PRNG、进程内缓存的令牌、外部租约和应用自己的编号生成器。Microsandbox 的冻结域也不能自动覆盖自定义 init 启动的全部进程，仍要看进程实际归属。

### 4.4 网络选择会反过来限制快照

Cocoonstack 受控 bridge egress 的锁由 sandboxd 安装，底层恢复返回前客体已经 resume。为避免租户状态在重装锁之前出网，当前产品拒绝该模式的 fork、checkpoint、promote、hibernate 和 archive。

无网卡模式没有这个 NIC 绕过窗口，仍可恢复 vsock 代理。受信任 golden 用来补热池又是另一阶段，不能把 golden 可克隆推成任意租户状态也可克隆。

Microsandbox 将网络 activation gate 纳入恢复协调。CubeSandbox 将 TAP、实例代次与 BPF 会话归属纳入网络管理，并在代次失效时清理或 reset 旧连接。两者采用不同机制，不能因为都恢复了 NIC 就认为保存了宿主 socket。

三者的 RAM 恢复均不承诺外部 TCP 会话透明续接。保留进程、恢复管理通道、重新连接远端服务，是不同工作。

## 5. 快照搬到别的节点，需要什么？

| 项目 | 要搬的捕获材料 | 还需要解决的依赖 | “可以交付”的条件 |
|---|---|---|---|
| Microsandbox | checkpoint 对象、内存区段、磁盘历史层、资源描述 | pinned 镜像、构造布局、执行与设备格式、外部挂载授权 | 闭包完整且通过校验，目标资源可重建，激活完成 |
| Cocoonstack | Cocoon 导出包；产品层代次与 metadata；或 OCI snapshot artifact | base image、kernel/initrd、兼容 VMM 与设备、目标路径 | 对应存储格式发布完成，目标取得所需文件并可恢复 |
| CubeSandbox | memory、rootfs、metadata 三类对象 | 系统/agent/业务制品、CPU 与宿主兼容条件、Volume 可达性 | 源节点可恢复；跨节点还要求 S3 remote ready 与放置条件满足 |

Microsandbox 的归档可以选择携带 image 依赖，不携带时接收方仍要准备 pinned base。Cocoon 导出也不会自动装入所有共享 EROFS 层，目标可按 digest 拉取基础镜像。CubeSandbox 的三对象快照包同样不等于把所有启动制品打包在一起。

发布方式也影响读者会看到什么。Microsandbox 验证对象闭包后发布快照；Cocoonstack 的 store 用 metadata 指向完整代次；CubeSandbox 区分本地捕获完成、导出任务受理和全部对象远端就绪。一个快照 ID 或 export UUID 都不能单独证明跨节点可恢复。

CubeSandbox 的远端路径将部分读取延后到运行阶段。目标建立卷与映射后可以按需取数，启动返回并不表示工作集已经下载完。Cocoon 的 mmap 与 ondemand 也会改变读入时机，Microsandbox 的 CoW 恢复则需要准备相应内存基底。

这些差异改变冷缓存时的表现，但没有相同镜像、资源、工作集与就绪标准下的数据，不能据此给出恢复速度排名。

### 5.1 同样叫删除，回收责任不同

Microsandbox 的磁盘历史层与内容对象需要保持被引用的闭包。Cocoon 的内存 mmap 依赖基底 inode 不变，hard link 与跨文件系统 symlink 的保留能力不同。CubeCoW 本地文件依赖文件系统 extent 引用计数，S3 后端另有导出引用、读者 lease 和后台物化。

删除一个管理记录，并不自动解除全部依赖。反过来，某个文件有其他引用，也不意味着它永远不能回收，必须由各自的引用和生命周期规则决定。

### 5.2 外部资源决定分支是否真的独立

Microsandbox 区分运行时拥有的 volume 与外部绑定，恢复时对后者重新授权或明确处理缺失。Cocoonstack 当前对带 dataset 的 sandbox 拒绝一组捕获与分支操作。CubeSandbox 的 host-mount 将恢复固定在原节点，插件 Volume 可以按驱动能力重新 Attach。

这些选择分别是显式重绑定、限制组合和依赖存储后端。外部可写卷若同时接给两个分支，VM 的 RAM 已分开也不代表文件内容分开。把“clone 成功”解释为所有外部资源均已隔离，会得出错误结论。

## 6. 引导与快速创建分别优化了哪些工作？

| 阶段 | Microsandbox | Cocoonstack sandbox | CubeSandbox |
|---|---|---|---|
| 内核来源 | 固件库中打包的 Linux | VM 镜像提供 kernel/initrd | 独立 Cube kernel |
| 冷启动根装配 | 引导根启动 agentd，再挂业务根并切换 | initramfs 挂多个 lower 与可写盘，再进入真实根 | 内核挂系统 pmem0，cube-init 挂 agent 盘并 exec |
| 常驻客体管理 | 默认 agentd 为 PID 1，可 handoff 自定义 init | 真实根中的 init 与服务体系，含 agent、silkd | cube-agent 保留 PID 1，另管理业务容器 |
| 快速创建的主要复用 | 镜像缓存、完整恢复、本地执行分支 | golden、已运行 warm VM、mmap 克隆 | 已运行 APP snapshot、模板副本和 CoW 存储 |
| 产品就绪 | agent 协议与运行时激活 | silkd 可用，加 claim 与策略交付 | 客体管理可用，加业务探针与路由交付 |

Microsandbox 直接服务于普通 OCI 工作负载，agent 不要求原镜像预装。Cocoon 要求 VM 镜像明确提供引导材料，换来内核与发行版环境的可组织性。CubeSandbox 保留独立管理客体，业务容器在它里面启动，业务 `/` 与最初的系统 `/` 不同。

Cocoonstack 的 warm claim 可以交付一台已经运行的 VM。CubeSandbox 的模板创建通常接续已经启动过的应用。Microsandbox 的 cold boot、durable restore 和 live branch 则分别有自己的准备工作。

因此，比较“创建一个 sandbox”之前，必须分清拿到的是已运行实例、从内存恢复的实例，还是从内核重新启动的实例。热池将成本放在请求之前，快照将成本放在模板构建和存储准备中，它们没有消除这些工作。

## 7. 生命周期与故障后的语义

| 场景 | Microsandbox | Cocoonstack | CubeSandbox |
|---|---|---|---|
| 暂停 | resident pause 留住活 VM，与持久快照分开 | hibernate 保留恢复材料并停止 VM；archive 再迁入 store | 产品 Pause 捕获后删除运行 VM，保留恢复记录 |
| 暂停后的资源 | resident pause 不等于释放 RAM | hibernate 释放运行 RAM，仍保留 claim 身份 | 释放物理 RAM；默认调度额度仍可保留 |
| 创建执行分支 | 新 runtime、新可写头，重新激活外部资源 | 新 VM、新 claim，模式与 dataset 限制仍适用 | 新产品实例与宿主资源，客体内部身份可沿用捕获值 |
| 故障后的继续 | 根据捕获产物与运行时恢复结果分别判断 | 部分 claim 可从磁盘冷启动，产品 ID 保留但进程丢失 | 回滚先删除当前 VM，再恢复目标；目标失败不自动找回旧运行态 |

这里最容易混淆的是产品身份与执行状态。Cocoonstack 可以保留 claim ID，却因故障冷启动失去所有原进程。CubeSandbox 可以创建新的 sandbox ID，却保留 RAM 里的客体容器身份。Microsandbox 的分支还要重新建立宿主端点与网络所有权。

外层 ID 没变，不代表执行连续；外层 ID 改了，也不代表客体所有身份都已更新。

## 8. 如何选择与借鉴

### 8.1 以嵌入应用、本地开发与跨宿主平台为主

Microsandbox 更贴近这个目标。SDK 可以组织每 VM 的运行时，不必先部署一组集群服务。其用户态网络与运行时激活协议能一起管理网络权限和恢复顺序。

需要接受的约束是协议中继的兼容范围、固件与运行时的配套关系，以及自建平台层的工作。它不是现成的多节点调度服务。

### 8.2 以热池交付、租约与受控应用出站为主

Cocoonstack 的 sandboxd 模型更直接。无网卡实例仍有 exec、文件、预览与代理访问，热池可以把启动与 warmup 提前完成。

前提是应用能接受 HTTP/SOCKS 代理，并且不要求通用 UDP。若改用带 NIC 的模式，必须重新看捕获、策略安装与恢复限制，不能继续沿用 none 模式的能力结论。多仓库与外围 provider 的版本组合也属于部署成本。

### 8.3 以 Linux 集群、容器接口与模板服务为主

CubeSandbox 提供更完整的集群组件组合。containerd/OCI 接口、CubeMaster 调度、Cubelet 节点管理、CubeProxy 唤醒和 TemplateCenter 可以共同构成服务。

需要承担的是宿主网络与存储准备、服务间状态协调，以及版本和恢复位置约束。启用 S3 块存储还会引入节点级轮询 CPU、WAL、缓存和远端引用管理，不能把它当作无成本的快照上传选项。

### 8.4 若重点是设计参考

| 要研究的设计问题 | 优先看哪一套 | 理由 |
|---|---|---|
| 如何证明同次捕获、输入输出边界与恢复解冻顺序？ | Microsandbox | 代次、工作负载冻结、对象闭包和网络激活在同一运行时内协同 |
| 如何把 VM 变成有租约、热池和产品接口的 sandbox？ | Cocoonstack | VM 管理与产品 claim 分层明确，无网卡模式把数据操作收束到 vsock |
| 如何组织节点网络、模板与集群恢复？ | CubeSandbox | 有 TC/eBPF 数据面、containerd 集成、模板制品与跨节点放置逻辑 |
| 如何控制凭据去向？ | 三者分别比较 | 透明占位符替换、显式代理注入和按实例请求头注入解决不同接入条件 |
| 如何管理 CoW 与远端依赖？ | 三者分别比较 | 磁盘链、独立 reflink 文件、远端逻辑卷的引用与回收规则不同 |

不能直接拼接的部分恰好集中在恢复路径。将 Cocoon 的 CLI 恢复方式接到另一套网络锁，不会自动得到 Microsandbox 的解冻屏障。将 Cube 的 S3 块卷接到文件快照接口，也不能继续使用本地 reflink 的删除语义。

选择时应先确定业务必须拥有的网络能力、需要保存的执行状态、外部数据归属和部署单位。网络决定哪些状态可以安全恢复，存储决定恢复依赖能否搬走，控制面决定谁在失败后继续承担实例与数据的管理责任。
