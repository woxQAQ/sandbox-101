# Haven 宿主部署

本目录提供专用 Linux KVM 宿主的部署示例。一个 `haven` 进程独占控制 store，提供业务 HTTP 和私有管理 UDS；`havenctl`、`loomctl` 只调用协议。配置与离线恢复见 [Haven 控制契约](../../docs/haven-control.md)，业务 API 与配额见 [Sandbox 契约](../../docs/sandbox-api.md)。本次源码迁移没有停止或切换既有部署；旧 state 不自动合并、发现或接管。

## 宿主、VMM 与材料

Cloud Hypervisor 精确 revision 由 [版本文件](cloud-hypervisor-revision) 固定。提供可信供应方构建的静态 x86_64 musl 二进制及 JSON build receipt，包含 `kvm,io_uring` 和该 revision 内置的 seccomp。只有显式需要构建时才运行 [构建脚本](build-cloud-hypervisor.sh)，使用新目录。Loom 校验 revision、特性、完整 SHA256 和静态链接；receipt 是部署信任材料，不是独立构建认证。

宿主需要 root setup、KVM API 12、`/dev/net/tun`、pidfd、`CLONE_INTO_CGROUP`、mount/PID/IPC/UTS/network namespaces、pivot_root、seccomp、cgroup v2、iproute2、nftables、e2fsprogs 和 sysctl。OCI 工具、namespace/mapping 与受控 PATH 见 [镜像物化](../../docs/image-materialization.md#host-tool-installation)。非系统工具的只读依赖根通过 `SANDBOX_IMAGE_TOOL_ROOTS` 提供；示例 service 的环境文件应包含实际 PATH 与依赖根。

Haven setup 使用 root。每个 VMM 使用配置的非零 UID/GID，清空 supplementary groups/capabilities，设置 no_new_privs/seccomp，在独立 namespace、受限 root 和单独 cgroup 中运行。launcher 原地 exec CH，不留下特权 supervisor。NIC 由宿主创建，MTU 1500；冷启动通过预启动 `vm.add-net`/SCM_RIGHTS 交接 TAP FD，保留固定 CH 的设备机制。不要给 VMM 添加 parent-death signal，也不要放入 daemon service cgroup。

为 VMM UID/GID 提供 KVM/TUN 访问权限。默认 GID 65534 通常没有 root:kvm 0660 设备访问权；选择实际 KVM group 或窄设备 ACL。准备前的 helper 使用准确 UID/GID 和空 supplementary groups 验证实际设备打开；不能通过 root VMM 绕过失败。

## 存储与资源预算

`/var/lib/haven` 为 root 私有目录，包含唯一 `haven.db`、node key、HEAD 及归属明确的资源目录。初始化只接受显式 `--init` 和新空目录。保留损坏/旧格式现场，不以初始化覆盖。

文件系统须可靠支持 file/directory fsync、sparse files、hardlinks；镜像、snapshot 与 RAM backing 保持所需同文件系统关系。可写盘始终独立 inode。reflink 减少复制，不减少保守 admission 费用。实际 COW 必须满足 stat 与 `/proc/PID/maps` 的 device/inode 一致；历史开发 Btrfs 环境因身份不一致被拒绝，reflink 能力本身不能证明 restore 兼容。

capture 父目录由 root 持有、模式 0711；只有 `ch` 叶交给 VMM。禁止递归 chown。Loom 将 CH 文件复制到新的 root 私有 publication inode，之后 hash/fsync/seal/rename；原 raw capture 的独立 cleanup 在确认无 reader 后退休。详见 [资源持久协议](../../docs/engine-integration.md#snapshot-and-restore-persistence)。

由宿主 cgroup manager 预置空的 `/sys/fs/cgroup/sandbox-vms`，祖先和该目录启用 `cpu memory io pids`。它须跨 daemon 重启保留，不能在服务停止时清理仍有 VMM 的 subtree。每实例限制保持 RAM+128 MiB、swap=0、pids=256、每请求 vCPU 的 CPU quota、25 ms burst、weight=100 和实际 `io.max`。restore loading 的临时 CPU 策略不改变 resume 前恢复 runtime quota 的要求。

提供外部存储 byte/inode quota、空间 reserve、网络带宽/包速率与 conntrack allocation 限额，再写 root-owned `/etc/haven/host-policy.json`：

```json
{"version":1,"storage_quota":true,"network_quotas":true}
```

该 receipt 仅记录部署者确认，不证明所有外部限额已经生效。`HAVEN_IO_MAX` 必须使用真实 backing-device major:minor 与预算；多设备、device-mapper 和 filesystem 的实际 I/O 计费需独立核验。

## 网络

保留现有宿主机制的 namespace、link aliases 和 nft table identity：guest namespace 内 `192.168.127.0/30`，独立映射范围 `10.200.0.0/17`，veth links `10.201.0.0/16`。每实例拥有 `sbe-<token>` netns、`sb<slot>h` host veth、`uplink` 和 `tap0`。token/inode/alias 共同校验归属。

部署前审查并加载 [宿主防火墙示例](sandbox-engine.nft)，启用 IPv4 forwarding，协调宿主 firewall priorities。`inet sandbox_engine` 是保留的内核资源 identity；不要在 VMM 存活时清空 `active_ifaces`/`active_ips`。IPv6 与 VMM-originated IP 被阻断。

Haven 按 off → 安装并核验策略 → Admin/Workload ready → on → 最终交付执行 direct。`none`/`proxy` NIC 保持关闭。CIDR/flow、精确实例退休及实际规则核验见 [网络策略](../../docs/network-policy.md)。未知 namespace/link/进程或缺失 receipt 保留现场，不能凭目录名删除。

## 安装和首次启动

构建 `just build`，安装 `bin/haven`、`bin/havenctl`、`bin/loomctl`。审查 [配置示例](haven.example.yaml) 和 [service](haven.service)，预置私有 `/etc/haven`、token、host policy、cgroup、firewall 与可信 VMM 材料。token 至少 32 个 trimmed 字符、文件仅管理员可读。环境文件 `/etc/haven/haven.env` 使用 `HAVEN_IO_MAX`、`HAVEN_VMM_UID`、`HAVEN_VMM_GID` 等新前缀；旧前缀不参与配置。

首次手动启动：

```nu
haven --config /etc/haven/config.yaml --init
```

它是前台服务；完成初始化后正常终止，再由 service 启动同一配置。service 不包含 `--init`。配置优先级为显式 flags > `HAVEN_*` > 显式 file > defaults；文件不自动发现。TLS cert/key 成对提供。HTTP 默认 loopback；网关须保留 HTTP/1.1 Upgrade、streaming 与长连接。

管理 socket 为 0600，父目录须受控且私有；Linux `SO_PEERCRED` 仅允许 daemon UID/root。不要向租户开放此 socket。`loomctl status` 和 root 业务 `/v1/status` 应给出相同 store identity。在线导入通过管理端完成：

```nu
loomctl --key import-reviewed-image --oci-layout /private/oci --digest 'sha256:REPLACE_WITH_MANIFEST_HEX' import
```

`--materialization merged-oci-erofs-v1` 显式选择兼容布局。CLI flag 放在 command 前。root 管理租户的模板/分支时，`havenctl --tenant OWNER` 明确指定 owner。

重启保留 state、资源绝对路径、mounts、cgroups、namespaces、CH binary 和 nft sets。`KillMode=process`、`SendSIGKILL=no` 与 VMM 的独立 scope 保持 daemon/VMM 生命周期分离；同时审查父 slice 的退出策略。Haven 只接续有原意图和精确现场证明的实例；host reboot 后丢失的 kernel anchors 不自动重建。

## 备份、恢复与部署验证

使用 [统一离线备份与恢复门禁](../../docs/haven-control.md#统一离线备份与恢复)。备份由停服后的同一 writer lock 排他取得，覆盖完整 DB/key/HEAD，不复制磁盘、镜像、快照或 RAM。恢复 review 只解锁元数据诊断，不授权数据面、工作流 replay 或 GC。DB 与完整本地历史一起回退无法靠本机 HEAD 检出。

`just runtime-e2e` 在新建私有环境中以 512 MiB 默认 guest shape 运行真实 launcher，检查 daemon 重启后接管原 VM、golden/warm 供给及 fork/resume 后的 TAP/SNAT。`just privileged-e2e` 在 `SANDBOX_E2E_*` 指定的私有部署上检查 sandbox 生命周期。记录目标宿主的设备、预算、namespace/mapping 与 delegation，缺少必需输入时标记 BLOCKED。运行条件见[测试入口](../../scripts/README.md)，旧双 daemon 的结果见[历史验证记录](../../docs/verification-history.md)，当前迁移缺项见[计划 024](../../plans/024-haven-loom-single-control-design.md)。
