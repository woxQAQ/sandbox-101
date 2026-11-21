# 026：直接镜像工具与测试边界整理

状态：**ARCHIVED / IMPLEMENTED / VALIDATION_BLOCKED**。按用户本次归档指令归档已完成的实施计划；未通过的验收在 [未完成索引](../../plans/README.md#待完成验收) 继续跟踪。用户明确授权按 Cocoon 的直接宿主工具方式简化镜像物化，并整理测试；此授权取代旧文档中的镜像 bwrap/user namespace 要求。实现、测试分包与入口更新已完成。沿用当前未提交树，没有提交、部署、升级真实旧库或执行真实 VM。

## 范围

- 默认 layered 直接执行 mkfs/fsck.erofs；删除镜像 bwrap/unshare、虚拟挂载路径和工具根配置。
- 显式 merged 保留 umoci 及元数据保真，使用私有工作目录；调用者须具备实际 ownership/xattr 权限，不能用 rootless 静默改写 owner。
- 保留输入 digest/路径/资源验证、不可变布局、责任交接及发布顺序。执行 profile 的变化必须使用不同 builder 身份，不能假装沿用旧语义。
- 测试按应用组件、原生镜像、KVM/transport 归属整理；内部应用组装与私有 fixture 明确边界，不通过公开调试 API 或控制器根回指暴露数据库。
- 更新 native gate、CI、Nix 开发入口及文档；required-native 的工具缺失仍失败，negative gate 改验证实际必需的 EROFS 工具。

## 交付与验证

- 镜像工具直接运行，shared/merged profile 分别更新身份；merged 在私有 `.unpack-*` 下解包并清理，保留原 owner，拒绝非 root。
- Haven 根目录无 Go 文件；四个组装生产文件位于 `internal/application`。组件 75 个 Go 文件、native 12 个、共享 fixture 5 个，原 KVM/transport build tags 保留。生产不可导入 testfixture，controller/service 不可反向依赖 application。
- 生成/架构正负例、native gate 8 项与 runner 10 项回归、workspace check、Haven/Loom lint、各 tag 编译、51 个冻结模型、真实 Rust fixture、8 组 smoke 通过；Loom race 27 项通过。
- 最终源码稳定的 required-native 命令中，组件 race **537 项全过、286.590s**；真实 native **73 pass、15 fail、0 skip**。整个 gate **691 pass、15 fail、0 skip**，失败是 merged 的 UID 0 条件和 shared extraction 的 capability 元数据缺失。negative 缺 mkfs 检查通过，preflight 明确拒绝当前非 root。
- 最终普通 120 秒入口仍 **FAIL：522 pass、35 skip、120.011s timeout**。没有增加超时、生命周期并发槽或弱化断言。
- KVM/kernel-isolation/production 未运行，完整 merged/元数据验收仍需具备真实 ownership/xattr 权限的 runner。实现已完成并归档，验收尚未全绿，缺项保留跟踪。

精确命令、初轮失败、最终输入 SHA256 与日志位置见 [026 证据](evidence/026-direct-image-tools-and-tests.md)。025 的历史结果只绑定其原树。
