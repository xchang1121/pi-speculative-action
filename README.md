# Pi 投机执行

Pi 扩展：在 Actor（执行任务的模型）正式调用工具前预测动作、提前执行，调用到达后验证并采纳已有计算。模型、历史模式和 Actor 探测共用计划与资源调度；实际输入、效果和所有权决定复用。

## 使用

```sh
pi install https://github.com/xchang1121/pi-speculative-action
pi -e /absolute/path/to/pi-speculative-action
```

安装或加载本地 checkout 后，运行 `/speculative-action`，开启投机并选择预测来源，点击 **Apply changes**。在 **Tools & execution → Execution routes** 检查 Replay、Observe、Fork 能力。全局配置为 `<agent-dir>/speculative-action.json`，项目的 `.pi/speculative-action.json` 可覆盖它；版本与依赖见 [package.json](./package.json)。

`drafterMaxDepth` 表示根批次之后最多预测多少个后继批次，0 关闭续推。动作和观测预测共用 Drafter 模型、凭据、任务请求数及 token 预算；并发根候选和 Drafter Actor 探测也使用同一账本，重试与压缩续跑不重置额度。`tools` 只筛选预测候选：省略时使用当前全部工具，空数组关闭工具候选；它不限制已观察输入和子计算的复用。

Linux / WSL 2 可从菜单安装 Bash 后端，或运行 `npm run setup:linux`；WSL checkout 应位于 Linux 原生文件系统。进程后端自动选择通过检查的 OverlayFS，否则回退 Git，并在采纳前核对输入和原始对象身份。

原生子进程交接须保留程序自身的提权语义。普通 Actor 通过 `PI_SPEC_HELD_EXEC_OBSERVER` 指向管理员部署、仅具有 `CAP_SYS_PTRACE` 的专用 `.observer`；已继承 `no_new_privs` 的 Actor 可使用无附加权限的观察器。安装探测只在一次性进程设置此限制。资格不足时，原生执行、缓存回放和投机生产各自保持可用；通用 helper 与映像库始终不附加权限。

ThinkThread 使用 [Profile 安装脚本](./scripts/install-thinkthread-profile.sh)，随后在项目中运行 `tt pi-speculative-action`。

## 预测与执行

[模型 Drafter](./src/drafter-plan-source.ts)、[PatternAware](./src/pattern-plan-source.ts) 和 [Actor 探测](./src/actor-fork-plan-source.ts) 生成统一 `PlanProposal`。[Runtime](./src/runtime-engine.ts) 按 Actor 决策进度、后端给出的资源依赖及实时资源安排准备和执行，各会话共用预算，Actor 优先。PatternAware 的模式、预设、按工具契约校准和原生工具名输出用于提出候选；同批晚到结果更新根预测时，保留未变动作的已有后继及匹配机会，根动作改变或移除则撤销相关后继。采纳学习按实际消费的计算及其生产所有权记账，跨工具或没有动作键的消费均可贡献证据；预测元数据刷新不丢失记录，完全重算和仅借用该成果的候选不冒领信用。`prepareObservedOperations` 独立控制[已观察操作的提前准备](./src/execution-preparation.ts)，默认开启；旧 `recent-command` 预设并入这一执行选项，不再受 PatternAware 开关控制。内部工作保留父工具调用和绑定权限，与预测共用物理资源账本。

Drafter 参照 [TomasuLLM](https://arxiv.org/html/2609.38201v2) 将动作预测与观测预测分开：多调用批次先选择后继各自需要的观测范围，再让分支只读取范围内的真实或预测结果；联合分支登记全部输入依赖，范围无效时保守使用整批。单调用无需选择范围。分支独立续推和取消，任一真实结果到达即可替换对应预测并重推受影响后继，其余分支保留，相同观测不重复请求。范围选择、观测和动作预测共用任务预算；请求失败或迟到也可由真实结果接管。独立后继可先执行，资源依赖沿私有检查点推进；Actor 历史始终只接收真实结果。

观测请求携带实际工具定义、调用参数、绑定的观测格式，以及本批 Drafter 输出和本分支已知观测的固定快照，通过严格校验后才进入私有续推；范围外结果在上下文中固定遮蔽。范围外调用仍保留资源执行关系，由后端判断是否等待其写入并继承检查点，不把其输出变成预测输入。默认精确比较 `content`、`isError` 和 `exitCode`。工具可在 `ToolInvocation.observation` 显式提供 `version`、内容 schema 和纯函数 `canonicalize`；规则改变须更新版本，版本或 schema 改变会使原观测依赖失效。规范化仅用于预测上下文与比较，不改写 Actor 输出或放宽执行证明。各分支只预测仍缺失的观测；未提供论文的多候选评分与独立 OBS 模型，也不把缓存状态摘要当作当前环境证据。

预测血缘与复用证明分开：预测失配不使已经独立校验的真实输入或子计算失效。正式调用可采用完整结果、契约允许的投影，或在封存输入上按当前参数重算；流式预览也走同一验新流程。执行器、权限、作用域、实际依赖及记录须相容，效果只提交一次；部分提交或效果状态不明会终止本次调用。命中的在途工作等待完成，支持调用方取消。

根入口提供 [Pi Host API](./src/index.ts)；[`./core`](./src/core.ts) 提供 Runtime，[`./process-reuse`](./src/process-reuse.ts) 提供进程证明与存储。新增来源实现 `SpeculativePlanSource`，从 `definitions` / 宿主数据获取当前工具；新增后端实现 `ExecutionWorld`，通过 `executionWorlds` 注册。结束时调用 `host.dispose()`。

| 后端 | 方法与边界 |
| --- | --- |
| [封存资源](./src/agent-execution-world.ts) / [输入版本](./src/resource-version.ts) | 捕获实际文件输入、保留内容与路径证明，消费前精确验新。受控 `find/grep` 需显式启用 [统一工具绑定](./src/pi-tool-invocation.ts)。 |
| [私有工作区](./src/workspace-sandbox.ts) / [文件事务](./src/workspace-transaction.ts) | 普通写入先在内存事务执行；硬链接与依赖链使用私有工作区。读取也能沿检查点执行，提交只包含本次操作的效果。 |
| [Linux 进程](./src/linux-process-world.ts) | Sandlock 隔离、strace 记录依赖，证书验新；合格的 x86-64 Linux 可接管已完成计算或转移单线程进程继续后缀。时钟、随机或进程身份等观测按一次性交接处理。 |
| [ThinkThread](./src/thinkthread/execution-world.ts) | 固定 SDK 的快照与 `fs.run` 接入分支协议，文件和目录输入进入公共复用层。默认覆盖 `read/ls/write/edit`；其余调用保留 Actor 原生执行。 |

自定义文件工具可用 `bindResourceOperation(execute, root)`，或提供 `ToolInvocation.filesystem`、`captureInputs`、`ProcessExecutionCoordinator` 绑定。`ExecutionWorld.observeOperations` 可独立观察没有整工具动作键的调用。共享输入和纯子计算按实际依赖查找，工具名称与可选路径提示不构成复用权限。`ToolFilesystemOperations.prepare` 的绑定及计算键须覆盖其余输入，构建器只通过给定视图读依赖，共享值保持不可变。

`inputsOnly` 分支只持有输入及证明，不复制父工具输出，也不授予整工具采纳权限。同一会话的并发调用可在父工具结束前查找已封存的输入和子计算；原始输入与准备结果逐一验新，通过后才登记依赖；过期或撤销的来源不阻止查找其他来源，也不贡献消费收据。消费者在执行前接管已验证的证明，借用结束前保留资源。读取返回独立副本，写后再读重新验证；消费工具执行自己的格式化与剩余操作。已独立完成的输入或纯子计算可在父工具报错、单次取消、工具退出可用集合后保留；失败消费不计复用时长，也不重放效果。预算不足或超出共享根时沿用原生操作。

Pi SDK 回调及接入网关的调用可观察 `spawn/execFile/exec/fork` 及其同步 API。同步交接由工作线程决策，期间不运行调用者的异步回调，也不能加入依赖被阻塞主线程的生产者。同步与异步调用可共享捕获的文件、目录输入；命名路径可重捕获当前资源图，匿名对象、外部队列与投机前置状态保留原生产者约束。第三方函数须先接入网关。

结果、输入和句柄各自保留所有权，跨轮消费仍验新。缓存发布与回收由跨进程事务协调，活跃借用受预算保护；关闭先停止接纳，再排空已接纳工作和句柄。直接使用 process-reuse API 时，以 `withPublication` 包住产物写入至证书封存，在回调外调用 `gc`、`drain` 或 `dispose`。

## 开发与验证

```sh
npm ci
npm run check
npm run build
npm test -- --maxWorkers=1 --no-file-parallelism
npm run bench:check
```

[系统测试](./test) 使用确定性模型回复，验证 SDK 接入、跨轮工作流、乱序执行、观测失配、共享预算与取消，以及实际输出、效果和回收。Linux 原生用例需要已安装的后端；ThinkThread 本地 wire 测试不替代真实 Runtime 资格。

[验证说明](./bench/README.md) 包含后端资格、模型任务与计量口径。`toolSpeedup = T / (T - H)` 描述已消费计算的复用收益；工具等待、协调等待、CPU 与端到端净收益分别评估。工具预测和内部操作分开计数；Drafter 状态另列私有观测预测数量及与已完成投机结果的精确一致率，未完成结果不进入分母。
