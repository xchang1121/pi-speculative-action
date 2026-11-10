# 请求与复用生命周期

Pi 扩展把 Actor、Drafter 根预测、后继判断、续推和读取 Actor 推理的请求放在同一个调度器中。角色由调用方明确传入，Actor 和 Drafter 使用同一个模型时仍然分别调度。共享范围是当前 Node.js 进程；默认限额域是服务 URL 的 origin，不能据此推断其他进程或账户的实际服务端额度。

## 配置

以下字段可写入 `speculative-action.json`，未填写时采用表中默认值。模型容量与 `maxConcurrentActions` 的工具执行容量相互独立。

| 字段 | 默认值 | 含义 |
| --- | ---: | --- |
| `modelRequestMaxConcurrent` | 4 | 同服务域已准入且未确认完成的模型请求上限。 |
| `modelRequestActorReserve` | 1 | 为 Actor 保留的容量，不得大于总容量。 |
| `modelRequestDrainTimeoutMs` | 30000 | 关闭时等候已发送请求结算；`null` 表示不设排空截止时间。 |
| `drafterBudgetInitialFraction` | 0.25 | 首个回合可用的累计 Drafter 预算比例。 |
| `drafterBudgetHorizonTurns` | 16 | 逐回合释放预算，在该回合达到完整任务额度。 |
| `drafterPatternHintsTimeoutMs` | 250 | 可选模式提示的等待上限，仅在 `drafterPatternHints` 开启时读取。 |
| `reuseLookupTimeoutMs` | 5000 | 一次正式调用跨候选等待、验证和重建的额外等待上限。原工具回退仍遵守原工具自己的超时语义。 |

预算比例同时作用于 `drafterTaskMaxRequests` 和 `drafterTaskMaxTokens` 的累计限额；设置初始比例为1可关闭逐回合保留。已发送但缺少可信 usage 的请求继续保留估算预算；只有能证明未发送的取消才退回预留。上下文过长仍可能使后段无法预测，这项调度不压缩上下文，也不扩大模型窗口。

`drafterMaxTokens`、`drafterTaskMaxRequests`、`drafterTaskMaxTokens`、`predictionTimeoutMs`、`modelRequestDrainTimeoutMs` 和 `selfSpeculation.forkMaxTokens` 均接受显式 `null`，分别关闭客户端输出、累计请求、累计 token、预测等待、排空和探测输出限制。省略字段仍使用既有默认值；UI 数值输入接受 `unlimited`。关闭累计额度后，按回合释放不再抑制请求。未知模型容量用 SDK 的0表示，不能拿猜测值拒绝长上下文。无输出上限且缺少 usage 时，快照的 `unreportedRequests` 标出未知请求数，`unreportedTokens` 只能计入输入估算，不能当作完整账单。

宿主通过多次 SDK prompt 完成同一个用户任务时，应从 `core` 创建 `DrafterTaskBudget`，通过扩展依赖的 `drafterBudget` 注入，并用 `budget.withTask(async () => { /* 初次 prompt 与全部内部恢复 */ })` 包住完整任务。内部完成事件不会重置这一作用域的请求、token 或进度额度；作用域结束后，下一个用户任务才重新初始化。嵌套作用域共享同一额度，异常退出也会结束作用域。不同并发用户任务应使用独立预算实例。

## 请求与完成

`ModelRequestScheduler` 从 `core` 导出，可在自定义 Host 的传输层使用。Pi 扩展已经接入 Actor 和 Drafter 传输。生命周期区分 queued、admitted、sent、cancel-requested、remote-finished、usage-settled、remote-unknown 和 not-sent。

排队取消不发送；已发送请求的本地消费者可以结束，但远端请求继续占槽并接收 usage。连接中断或未证明远端完成时，容量保持隔离，直到独立账本通过 `settleRemote` 确认。调度器不会自动重试可能已发送的请求。未知请求也可能降低后续可用容量；关闭时有界排空并输出未结算诊断，不能把停止本地连接解释为服务端已停止计费。

`createSpeculativeActionExtension` 的 `onModelRequest` 回调提供稳定的请求 ID，以及 task、turn、source、sourceRequest、proposal、parent、purpose。出站请求带 `X-Pi-Request-Id` 和 `X-Pi-Request-Role`，便于服务代理关联。`usage-settled` 只表示客户端已收到最终计量对象，不保证供应商给出所有字段；取消后的实际费用仍应以独立上游账本为准。

`assistantCompletion` 区分可见回答、工具调用、仅推理、空输出、长度截断和失败；即使长度截断含有可见文字，也不能当作完成。`recoverAssistantCompletion` 对未完成输出恢复，默认最多两次，`maxAttempts: null` 则持续到可见完成、外部失败或调用方取消。调用方通过 `canContinue` 控制终止，并在 `recover` 中决定工具和提示。它不会自动重发失败请求，也不会替应用规定 finding 格式。漏洞实验的 `provider-only` 配置保留全部原工具，不设回合、时间、收尾阶段或恢复次数上限；传输在 SDK 完成载荷构建后移除输出 token 字段，避免 SDK 自动补入本地默认值。服务端限制和供应商 SDK 原有传输失败仍须单独报告。

`sdkRejectedToolAttempt` 仅识别工具已禁用且 SDK 明确返回该工具不存在的前置拒绝。实际执行仍必须有对应结算，不能用此分类隐藏扩展漏拦截。SDK 绑定失败、可选提示失败、搜索能力缺失和未结算请求会通过 `onDiagnostic` 输出；平台能力变化通过 `onCapabilities` 提供。

## 预测和资源

多工具根预测不等待额外的 scope 判断。后继预测独立等待 scope；失败时保留保守的全批次依赖，后继采用仍需原有的输入、效果和所有权证明。可选模式提示失败或超时不会阻断基础 Drafter。

Captured grep 将文件选择索引保存为不可变准备数据，正则变化时可以复用同一次目录选择。文件内容、名称、忽略规则等继续由原资源证明验证；上下文、排序和截断沿用 Captured 契约。这减少重复目录选择，但不消除第一次捕获的成本，也不把 Captured 与 Native 的截断语义视为相同。

复用查找超时后，未结束的重建保留资源借用直至清理完成。相同输入所有者、执行器和工具的重建超时保留30秒冷却，跨结果投影与输入复用入口生效；原有可直接消费的结果仍可验新并使用。该记忆只决定是否尝试重建，不是输入有效性的证明。

查询取消时会关闭后续借用，并先等待正在获取的文件快照结束，再释放其内容租约，避免较晚到达的捕获脱离清理列表。

回归入口为 `model-request.test.ts`、`budget-scope-extension.test.ts`、`drafter-plan-source.test.ts`、`reuse-deadline.test.ts`、`captured-search-index.test.ts`、`resource-query-lifecycle.test.ts` 和 `agent.system.test.ts`。Linux 原生进程接管仍需 [README](../README.md) 所列的执行域，文件捕获通过不代表该能力已通过。
