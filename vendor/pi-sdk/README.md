# 配套 Pi SDK

本目录提供本项目的未发布配套版本 `0.84.1-speculative.1`。上游底稿为 `earendil-works/pi` v0.84.1，提交 `53fa77ccd8a279eb87e92294ef3687b03ff80112`。九个公开包保持同一版本；源码许可见 [LICENSE](./LICENSE)，变更见 [execution-control.patch](./execution-control.patch)，固定输入见 [source.json](./source.json)，各包 SHA-256 见 [manifest.json](./manifest.json)。没有向上游仓库或 npm 发布这些包。

在本项目根目录运行 `npm ci --ignore-scripts`，使用根目录的 `package-lock.json` 和本地 tarball 完成锁定安装。根目录的 overrides 同时约束传递依赖，避免在工具管线中加载另一份 SDK。然后运行 `npm run build` 和 `npm exec -- pi -e ./src/extension.ts`。请使用该本地入口启动新的宿主进程；安装扩展不会修改已经运行的 Pi。

## 公开执行协议

`@earendil-works/pi-coding-agent` 导出 `SessionExecutionController`、`SessionExecutionScope` 和 `SessionToolCall`。可以在 `createAgentSession({ executionController, ...options })` 创建时设置控制器，或在空闲会话调用 `session.setExecutionController(controller)`。控制器的运行权限与持久数据记录分别管理。

| 接缝 | 调用顺序及实现责任 |
| --- | --- |
| `acquire({ sessionId, sessionFile, operation })` | 在命令、input 扩展、模板读取、认证和模型执行之前取得运行范围；失败向调用者传播。共享存储的所有 owner 必须共用真实的运行互斥。 |
| `assertActive()` | SDK 在效果边界以及扩展处理器之间检查当前范围；会话、分支和运行时资源仍须由控制器验证。 |
| `beforeToolCall(request, signal)` | 在工具开始事件、参数准备和调用钩子之前接纳。请求绑定到原 assistant entry 及其 content index，不能只使用 call ID。返回 `undefined` 执行原管线；返回最终回执则验证并交付该回执。 |
| `persistToolResult(request, receipt, append)` | 收到扩展转换及会话监听器处理后的最终消息。控制器先持久结算，再在确认事务内调用 `append()`，保存其返回的 entry ID；回执同时包含消息与 `terminate` 值。 |
| `close(error?)` | SDK 排空接纳的调用、工具更新、监听器和嵌套会话操作后释放范围。未取得最终回执的已开始调用不能重做未知效果。 |

`session.resumeTools()` 从当前分支上真实的 assistant 工具批次恢复，通过原有顺序或并行管线处理尚未开始的调用。已经保存的结果必须是原请求顺序的前缀；已结算结果重新交付时不再执行工具、参数准备或结果钩子。恢复不发送替代模型请求，也不重放旧 input 和 before-agent-start 事件。全部结果已保存时，恢复校验并确认结果，不重做旧 turn-end。

控制器错误以 `@earendil-works/pi-agent-core` 的 `AgentExecutionInterrupted` 传播，不转换为普通工具错误回执。使用 `await session.close()` 等待所有已接纳工作结束。同步 `dispose()` 不提供可等待的关闭完成点。

SDK 不自行授予恢复权限。控制器需要验证实际实现、资源闭包、一次消费状态和最终回执；旧 FD、旧内存身份、PID 或持久化的回调描述都不能直接取得执行权限。现有扩展仍保留工具绑定包装器，恢复进入工具管线时用同一 Host 完成运行时重绑。本补丁替换了资格适配器对私有结果事件和自动 append 的改写，没有另建工具执行器或 SessionManager。

## 从固定输入重建

准备一份包含上述上游提交的 checkout，并下载 [v0.84.1 官方源码归档](https://github.com/earendil-works/pi/releases/download/v0.84.1/pi-0.84.1-source.tar.gz)。归档仅提供固定模型数据，其摘要由脚本验证。构建使用 Node、npm、Git 和 tar；构建目录和输出目录必须尚不存在。

```sh
node scripts/build-sdk-companion.mjs /path/to/pi-upstream /path/to/pi-0.84.1-source.tar.gz /new/build-directory /new/package-directory
```

脚本从固定提交建立干净 checkout，验证并应用补丁，沿用上游锁文件安装依赖，统一包版本并生成官方锁文件。外部依赖的版本、来源和完整性摘要发生变化时会失败。随后运行 SDK 根目录检查、离线构建、指定的会话与 agent-loop 契约，并打包九个公开包；输出保留各步日志及包摘要。不会运行上游发布脚本、真实模型接口或全量 e2e 测试。

## 验证范围

干净重建通过 SDK 根目录检查、离线构建、23 个 agent-loop 测试和 60 个会话测试（初选 56 项，加四项既有 settled/terminate 回归）。配套包在 SDK 源码目录之外通过 22 个跨进程场景及独立核验，覆盖正常完成、请求发布的两个中断窗口、待执行批次、顺序前缀、并行回执、错误与 terminate 回执、两个 owner 竞争、重复接纳、关闭排空、实现变化和引用损坏。Linux 与 Windows 新目录的锁定安装还通过模块唯一性、CLI 启动、模型列表及既有扩展 off/on 两个真实会话契约。可复现的共同序列见 [bench/sdk-companion](../../bench/sdk-companion/README.md)。

上述恢复场景使用普通 UID 1000、零 capabilities、NNP=1，覆盖共同遵守协议、共享稳定 store 路径及已绑定的自有工具闭包。未知 committing 保持 poisoned，属于失败保证。生产 store 的恢复发现与迁移、任意扩展闭包、一般原生资源、外部锁文件/目录替换、主机重启与断电尚未因此得到证明；扩展没有默认启用通用持久恢复。AgentHarness 的未实现入口也没有被计入通过项。
