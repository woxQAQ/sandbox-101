# 022 required-native 门禁补充与父校正

这是合并合同r1的明确补充，供本轮设计冷审一起评审，尚未运行新的产品测试。父在核对原source `engine/fsmeta_test.go:23 requireEROFS` 和 `image_compat_test.go:22 requireMergedOCI` 后发现：r1末尾普遍禁止namespace与要求完整required-native Engine测试有冲突。原native pipeline本来通过bubblewrap/umoci进入测试自有rootless隔离，不是VM。不能用禁namespace导致跳过20个实际native场景，也不能借此授权任意新namespace或生产资源。

唯一修正：sole source executor未来完整默认Engine race允许原有、未改动的业务测试及生产imageToolSandbox/runMappedImageTool路径创建并清理其测试自有的有界非特权user/mount/PID等namespace；只为原required-native EROFS/OCI测试，不能修改hostroot/subid、设新SUID/caps、进入旧资源、跑kernel/capture/KVM/privileged/production门禁或新的namespace探针。当前只读设计/冷审者仍不运行namespace、产品测试或native探针。VM全无权限，旧grants仍expired。

required-native必须设置`SANDBOX_REQUIRE_EROFS_TESTS=1`，不能在工具或隔离不可用时skip。只用已缓存的原closure `/nix/store/ppifd9prd8f7a4kkbrq5m50ss153r4jr-sandbox-kvm-acceptance-userspace`：其erofs/bin、umoci/bin、bubblewrap/bin、util-linux/bin、e2fsprogs/bin的原链接/targets。对应原工具receipt `/tmp/plan012-r4-engine-finish-37fa8d75/tools.json`（EROFS1.9.4、umoci0.6.0、bubblewrap0.12.0、util-linux2.42.3，Go1.27.3）。只读既有closure，不nix build/develop/fetch/install。执行者必须在launch前精确核对实际可执行路径和原expectedSHA，仅把这些bin在自己测试进程PATH中置前，不改hostPATH配置，不dump全量环境。缺材料先停，不运行到skip再冒称fullPASS。

父已定向解析历史纯Go测试JSON（仅Action/Package/Test，非真实runtime日志），提供已接受完整事件集合出处，避免仅比总数：

- Engine `/tmp/plan012-r4-engine-finish-37fa8d75/engine-final-race.jsonl`，rawSHA256 `25efe6c4eb5524b1e4652bcf9f66ab4cdb021934713f5654cbf3459bfebbb092`，448RUN/448PASS，unique448，run/pass集合相等，fail/skip0。pass pair manifest hash `a2a4959ba24810cb8e30036dd402032de5a2cbf3e2202100326d732d3c6774cc`。
- SB `/tmp/final12-combination-t3-fe782bd5/sandboxd-full-race.stdout`，rawSHA256 `06b02af00762ba58be93a82dcb44b9d22e58ead0023df16974a1563ceb902fbb`，678RUN/678PASS，unique678，run/pass集合相等，fail/skip0。pass pair manifest hash `064c984304643c2f0d6668ad6fd202c8db126495ba7a0996709b725164b7f5da`。
- manifest算法：对每个Action=pass且有Test的event取(Package,Test)，去重按tuple字典序排序，Python `json.dumps(sorted(set(pairs)), ensure_ascii=False, separators=(',', ':')).encode()` 后SHA256。新候选应保留完整相同集合；event次数也须各恰一次。原20个TestActual顶层、36个required-native/proof顶层需单列非零实际PASS；36名单可从原final/status材料定向读取，不能将无子场景的顶层说成没执行。

原Engine布局与body与47f5 Engine subtree相同是历史已接受组件事实，不等于本轮新测试。上列数据仅提供比较oracle，新source必须真正执行Engine300s/SB480s并保留实际结果。新race与Go vet/lint等授权仍按r1，其余禁止项与范围不变。
