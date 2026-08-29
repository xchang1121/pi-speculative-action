# Pi speculative action

这个独立 package 在不修改 Pi 本体的前提下加入投机工具执行。Drafter 与 PatternAware 预测未来工具调用；只有存在可证明安全的隔离路线时才提前执行；Actor 发出等价动作后可以采纳对应结果。

本仓库刻意与 Pi monorepo 解耦：它拥有独立的 Git 历史、构建配置、测试和依赖锁。对 Pi 的依赖仅限于以 peer 形式声明的已发布公共扩展 API；不导入 Pi 源码工作树、不使用 workspace 路径别名，也不要求存在对应的 `main` 分支。

这是一个独立的 GitHub 仓库。其所有可达历史都只包含 speculative-action 相关改动，不包含 Pi monorepo 父提交或源码树。

## 架构

Runtime 分为四个相互独立的层次：

1. **投机源**：Drafter 与 PatternAware 只产生与执行方式无关的 `PlanAction`。
2. **动作身份**：`K(a)` 规范化工具语义、已验证 schema、参数、资源与实际执行器身份；无损投影规则可以证明一个结果覆盖另一个动作。
3. **执行路由**：动作语义只声明可观察效果，由唯一的 `ExecutionWorldRouter` 选择并准备隔离能力。所选路由刻意不进入 `K(a)`。
4. **调度与结算**：Scheduler 决定启动时机与资源竞争；`ExecutionWorld` 管理隔离执行和采纳；唯一的结算生命周期记录匹配、采纳、回退与计时。

执行路线具有固定优先级：

| 优先级 | 路线 | 范围 |
|---|---|---|
| 1 | `runtime_sandbox` | 注入的 Runtime 全局沙箱；对所有启用工具优先 |
| 2 | `resource_snapshot` | `read`、`grep`、`find`、`ls` 的本地后备，通过资源版本证据保证新鲜度 |
| 2 | `workspace_branch` | `write`、`edit` 的本地后备，在私有 Git worktree 中执行并进行冲突检查后提交 |
| 3 | Actor 回退 | 没有安全路线时完全不发起投机工具执行 |

本 package **不再内置进程沙箱**。因此默认 Pi 扩展仍可预测并匹配 `bash`，但不会投机执行 Bash；命令由 Actor 通过 Pi 正常路径执行。嵌入式 Runtime 可以注入一个 runtime scope 的 `ExecutionWorld`，一次性为 Bash 和其余工具提供隔离。

这样，未来 OS 层面的 Agent Runtime 只需接入一个完整执行世界，不需要 Pi 针对每种工具分别维护隔离实现。

## 正确性边界

- 投机源不能选择执行后端。
- 隔离后端变化不会改变 `K(a)`。
- 进行中任务和缓存只在相同执行 route 内复用。
- Actor 采纳仍必须依次通过动作等价、权限、资源新鲜度、World 兼容性、投影与提交检查。
- 同时缺少 Runtime 沙箱和已注册本地后备的工具会被标记为 execution-blocked，但仍可参与匹配、学习和反事实计时。
- 同名自定义工具保持权威；除非宿主显式提供一致的语义与执行能力，否则不会参与投机。

`read` 支持由真实输出覆盖范围证明的无损区间投影；`grep`、`find` 和 `ls` 暂时只做精确 K(a) 匹配。

## 安装与运行

仓库根目录就是 Pi package 根目录。本地 checkout 可以直接加载或安装，不需要构建 Pi，也不会修改 Pi 本体：

```sh
pi -e /absolute/path/to/pi-speculative-action
pi install /absolute/path/to/pi-speculative-action
```

Pi 可以直接安装该仓库：

```sh
pi install https://github.com/xchang1121/pi
```

`pi.extensions` 指向 `src/extension.ts`，由 Pi 的公共 TypeScript 扩展加载器直接加载。因此 Git 安装不依赖已提交的构建产物或 dev dependency。`dist` 只作为 npm 使用时的标准 JavaScript/类型入口，在 `npm pack` 或 `npm publish` 时生成。

在 TUI 中打开 `/speculative-action`。菜单按投机源、调度/缓存、工具/执行分级；工具标签会说明本地后备机制。不存在隔离路线时始终回退 Actor。

配置由 package 自己管理：

- 全局：`<agent-dir>/speculative-action.json`
- 项目：`<workspace>/.pi/speculative-action.json`

示例：

```json
{
  "enabled": true,
  "draftModel": "deepseek/deepseek-chat",
  "drafterGateEnabled": true,
  "candidateLimit": 2,
  "maxConcurrentActions": 8,
  "drafterMaxDepth": 1,
  "tools": ["read", "grep", "find", "ls", "bash", "write", "edit"],
  "patternAware": {
    "enabled": true,
    "multiStepEnabled": true
  },
  "selfSpeculation": {
    "enabled": true,
    "endpoint": "http://127.0.0.1:8010",
    "forkTransport": "sidecar",
    "forkEnabled": true,
    "forkActionEnabled": true,
    "forkActionMinConfidence": 0.9,
    "forkGateEnabled": true,
    "forkGateMinSamples": 4,
    "forkGateWindowSize": 4,
    "forkGateMinNetBenefitMs": 25,
    "forkGateProbeInterval": 4,
    "forkGateFailureThreshold": 2,
    "maxCandidates": 8,
    "maxDraftTokens": 28,
    "draftFormat": "tagged_json",
    "draftBoundary": "<tool_call>",
    "forkMaxTokens": 128,
    "forkTemperature": 0,
    "forkDecoder": "auto",
    "forkForcedPrefix": "<tool_call>",
    "requireLogprobs": true,
    "timeoutMs": 2000
  }
}
```

`candidateLimit` 默认在每次 Actor 决策并发发出两个单动作 Drafter 请求。宽度为 2 时它们组成延迟对冲：首个包含 schema 有效且已启用 `K(a)` 的响应被接纳，并通过 provider `AbortSignal` 取消仍在运行的同伴；错误、空响应和无效调用不会胜出。严格动作与目标 token 回放保留了全部 6 个可用精确命中、完整提前量以及完全相同的 D3 verifier 工作，同时识别出宽度 2 下 7.78% 的 Drafter 服务为可消除的剩余工作。显式设为 3 或更高时仍保留所有完成样本，也没有隐藏上限，供确实能产生有效宽多样性的模型使用。

`drafterGateEnabled` 默认为 `true`。它把并发根请求视为一个批次，滚动学习动作侧净收益：只有由 Drafter 实际拥有并被 Actor 采纳的工作才按真实工具 `executionAheadMs` 计收益，再减去该批所有请求的服务时间总和。前 4 批用于预热；持续负收益时暂停整批请求，但每跳过 4 次仍做一次有界探测，以便工作负载变化后恢复。设为 `false` 即恢复无条件 Drafter 批次；PatternAware 候选和 Drafter 后继请求不受此门控。

`drafterMaxDepth` 表示每个单动作 Drafter 初始请求之后，最多允许多少次利用已完成工具输出的后继请求。后继请求占用该投机源在下一次 Actor 决策上的既有 slot，不会增加每个决策的请求宽度；设为 `0` 即恢复单步 Drafter。

`drafterMaxTokens` 是可选的硬上限。省略该项——或清空 TUI 输入框——会使用服务商默认输出上限，避免长命令和结构化工具参数被截断。

### 目标解码器自投机桥接

`selfSpeculation` 默认关闭，并且同时受 package 顶层 `enabled` 总开关约束。候选通过 schema 校验并完成参数物化后，每个 Drafter 或 PatternAware 预测都会复制到同一个 request-scoped 候选包。解码身份始终使用 Actor 可见的精确 `predictedAction`；为调度和结果复用而扩大的无损 `executionAction` 则独立携带。相同预测 key 只发送一次，并合并来源与 proposal 归因。即使某个动作缺少本地隔离、不能提前执行，它仍可作为边界相对的 tool-call token 交给目标模型验证。

桥接为每次 Actor 决策绑定一个稳定 request ID，只把绝对 decision sequence 与本次请求一致的排序候选包发送到 `POST /self-speculation/candidates`，并在所有候选提交和 fork 完成后调用 `POST /self-speculation/clear`。面向后续决策的预测会保留到对应 Actor 请求启动；同一决策的重试会继承候选包，过期预测则被丢弃。网络或解码失败只会损失加速机会，不会改变 Actor 的正确性路径。

如果目标端在 clear 响应中返回 `verification`，协调器会把真实的 proposed、accepted、rejected 和 unresolved draft token 与注册回执分开统计。candidate ID 与来源会更新按模型、端点、格式、工具和来源分区的 decoder ledger，其平滑验收概率会校准后续候选排序。Runtime 的 Actor 结算则独立训练动作收益：sidecar fork 只有在匹配预测被真实采纳时才获得收益，且按来源分摊实际 `executionAheadMs`。token 拒绝不会改写动作语义概率，单纯 action-key 命中也不再给 fork gate 记收益。为保持 API 兼容，`acceptedDraftTokens` 仍表示注册确认，不能当作目标模型验收。

fork 有两种传输方式：

- `sidecar`：在 Actor 第一个输出片段到达后，把快照和原始请求上下文发送到 `POST /self-speculation/fork`。这是配套 `self-speculation` 仓库实现的可移植参考路径。打开 `forkActionEnabled` 后，每个完整 fork candidate 会作为一个原子 proposal 重新进入普通动作 Runtime：同批并行 tool call 保持在一起，不同 candidate 批次才互为备选。每个调用都通过 Runtime feedback 携带该批次的 candidate ID、来源/proposal 归因、score、call identity、format、fork timing 和 logprob 证据，并复用 Drafter/PatternAware 相同的 schema 校验、K(a) 去重、执行策略、Scheduler 和 Actor 结算。`forkActionMinConfidence` 默认为 `0.9`，只有 SPORK 报告的“已选 token 最低 top-1 概率”达到门槛时才接纳整批动作；调用不完整、证据缺失或格式错误时关闭失败，设为 `0` 可恢复接纳无分数批次。该门控只影响动作交接，已经运行的 fork 与目标解码遥测保持不变，也不会新增推理请求。该模式看不到 Drafter 的私有流，因此 Drafter 动作仍进入统一候选包，但不会进行 Drafter 自 fork。
- `provider`：把版本化的 `self_speculation` 控制对象直接放进 Actor 请求；`drafterEnabled` 打开时也放进每个 Drafter 请求。只有明确实现该 SPORK 协议、并能提供所需 logprob 的 provider 才应使用此模式。普通 OpenAI-compatible 服务可能直接忽略未知字段；仅注入字段并不等于已经实现自投机。

使用正数动作置信度门槛与参考 sidecar 时，应把 `requireLogprobs` 设为 `true`；若引擎无法提供证据，fork 会明确失败，而不会静默执行无分数动作。

在 `sidecar` 模式下，按模型隔离的 fork 门控会滚动学习 `Actor 精确命中的领先时间 - fork 延迟`。默认先放行 4 个样本；持续负收益时暂停请求，但每跳过 4 次仍做一次有界探测，使工作负载改变后可以恢复。连续 2 次 endpoint 失败也进入同一探测回路。上述阈值都可配置；关闭 `forkGateEnabled` 即恢复无条件 fork。同一份 `fork_gate` 策略也会作为 provider/SPORK 提示发送，但 provider 传输需要由推理服务自行执行该策略。

D3 默认上限为 28 个 draft token。严格 DeepSeek tokenizer tape 回放中，相比旧上限 20，28 多提交的 12 个 token 全部被接受、没有增加拒绝 token，并额外减少 10 个 target-step 代理；升到 32 不再增加收益。该值仍可配置，并会再次受推理引擎硬上限约束。

JSON 文件还接受 `requestIDField` 和三条控制路由；JSON 与 TUI 都能配置常用的 endpoint、Bearer token 环境变量名、候选/token 上限、fork 门控策略、tool-call 格式与边界、fork decoder/prefix、温度及 logprob 要求。边界、formatter、decoder 和目标 tokenizer 必须属于同一种模型格式。控制路由能够改变推理执行，应只放在可信网络或受认证代理之后；`apiKeyEnv` 只读取指定环境变量，不会保存 token 值。

PatternAware 多步模式开启后，每个权威 Actor 动作——包括 Actor 采纳的 Drafter 结果——都会连同真实输出一起做一次不修改学习状态的同轮重基准；正式学习仍在权威 batch 边界进行。若跨轮的 `K(a)` 与 horizon 集合完全不变，则沿用提前签发的机会而不重复创建，避免共享命中被采纳后又启动同组落选候选。

旧的 `resourceCached` / `sandbox` / `predictionOnly` 三组对象仅作为迁移输入继续识别，读取后统一规范化为一个 `tools` 数组。

## 接入 Runtime 沙箱

宿主通过 `executionWorlds` 提供执行世界。runtime scope 的 World 在类型上就是覆盖全部工具的 `runtime_sandbox`；只有 fallback World 才按工具效果声明有限能力。Router 在返回 route 前确认后端可用，因此不可用的 Runtime 沙箱会自然降级到兼容的本地后备。每个成功后端——包括内置资源快照和 Git worktree 后备——都返回同一种 `WorldBranch`，由 branch 自己拥有兼容性证据、新鲜度校验、采纳与清理。宿主在未注册资源快照后端时会自动补上内置实现。

```ts
createSpeculativeActionHost(sessionID, {
  cwd,
  executionWorlds: [runtimeSandbox, createWorkspaceSandbox()],
  // 省略模型、权限与工具接入
})
```

第一个可用的 Runtime 全局沙箱对所有工具生效；不存在时，Router 才检查与动作效果兼容的本地后备。两者都不存在时返回空 route，Runtime 将其结算为 `execution:isolation_unavailable` 并回退 Actor。解析、准备、fork 与 dispose 全部经过同一个 Router，工具侧不会持有可绕开的后端对象。

## 计时口径

对被采纳的结果：

- `attemptLeadMs`：投机意图产生到 Actor 调用被拦截。
- `executionAheadMs`：拦截前已完成的投机执行量，上限为实测工具时长。
- `hitLatencyMs`：Actor 调用被拦截到权威结果完成结算。

对因为缺少隔离而阻断的匹配，Actor 执行仍是唯一权威执行，但使用相同分解报告反事实潜力：

```text
executionBlockedPotentialHiddenLatencyMs = min(actorDuration, predictionLead)
executionBlockedPotentialHitLatencyMs    = actorDuration - potentialHidden
```

这些反事实值不会计入真实投机命中数，也不会混入真实隐藏时延。

## 验证

```sh
npm install --ignore-scripts
npm run check
npm run build
npm test
npm run bench:check
npm pack --dry-run
```

单轨迹消融方法见 [bench/README.md](./bench/README.md)。
