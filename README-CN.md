# Pi speculative action

这个独立 package 在不修改 Pi 本体的前提下加入投机工具执行。Drafter 与 PatternAware 预测未来工具调用；只有存在可证明安全的隔离路线时才提前执行；Actor 发出等价动作后可以采纳对应结果。

本仓库刻意与 Pi monorepo 解耦：它拥有独立的 Git 历史、构建配置、测试和依赖锁。对 Pi 的依赖仅限于以 peer 形式声明的已发布公共扩展 API；不导入 Pi 源码工作树、不使用 workspace 路径别名，也不要求存在对应的 `main` 分支。

这是一个独立的 GitHub 仓库。其所有可达历史都只包含 speculative-action 相关改动，不包含 Pi monorepo 父提交或源码树。

## 架构

Runtime 分为四个相互独立的层次：

1. **投机源**：模型 Drafter、Actor probe 与历史模式预测只产生与执行方式无关的 `PlanAction`。
2. **动作身份**：`K(a)` 规范化工具语义、已验证 schema、参数、资源与实际执行器身份；无损投影规则可以证明一个结果覆盖另一个动作。
3. **执行路由**：动作语义只声明可观察效果，由唯一的 `ExecutionWorldRouter` 选择并准备隔离能力。所选路由刻意不进入 `K(a)`。
4. **调度与结算**：Scheduler 决定启动时机与资源竞争；`ExecutionWorld` 只产生封存后的效果载体，由唯一的 `EffectTransaction` 管理验证、采纳、放弃与提交状态；结算只记录一次匹配、采纳、回退和计时。

执行路线具有固定优先级：

| 优先级 | 路线 | 范围 |
|---|---|---|
| 1 | `runtime_sandbox` | 内置 Linux/WSL 进程世界或宿主注入的 Runtime 全局世界；探测通过时优先 |
| 2 | `resource_snapshot` | `read`、`grep`、`find`、`ls` 的本地后备，通过资源版本证据保证新鲜度 |
| 2 | `workspace_branch` | `write`、`edit` 的本地后备，在私有 Git worktree 中执行并进行冲突检查后提交 |
| 3 | Actor 回退 | 没有安全路线时完全不发起投机工具执行 |

在 Linux 与 WSL 2 中，默认扩展会注册一个轻量进程世界。它先使用与变更工具相同的私有工作区原语，再用 Sandlock 的 Landlock/seccomp 策略与虚拟文件系统限制进程。当前实现刻意不创建 user、PID 或 mount namespace，因此命令保留 Actor 的原生身份。任何内核能力、binary 或策略探测失败都会移除这条路线；Windows、macOS、WSL 1 或依赖不完整的 Linux 仍走 Pi 的普通 Actor 执行，不会静默降低隔离强度。

工具策略不按平台或工具名硬编码。启动诊断会把每个执行世界声明的效果保证与工具要求相交：Windows、macOS、WSL 1 和探测不完整的 Linux 默认可配置 `read`、`grep`、`find`、`ls`、`write`、`edit`；Linux/WSL 2 进程世界就绪后再加入 `bash`；宿主注入覆盖全部效果的世界时则可启用全部工具。设置中已选但当前无安全路线的工具会保留偏好但处于 inactive，不会发送给投机源。

进程拦截是结构式的：统一的异步进程出口保留各 Pi 工具自己的参数校验、流式输出、截断和结果格式；Linux 世界只对精确的 exec syscall 做映射，`PATH`、普通文件打开、metadata、目录内容、写入、cwd 与环境都保持原样。Broker 身份由 executable bytes、argv、逻辑 cwd、完整环境、描述符、credential、limit、平台和策略共同决定，而不是由父 Bash 文本或工具名决定，因此不同 Bash 父命令可以复用同一个已完成子进程。

每个可复用结果都是持久化 provenance certificate，包含动态观察到的文件、目录、负查找、symlink、executable/DSO 身份、有序 stdout/stderr、退出状态和原子 regular-file 效果。每次复用都会重新验证全部依赖。外层投机 branch 还会独立记录顶层进程 provenance，并在 Actor 采纳前再次验证；tainted、不完整、过期、交互式、可变宿主输入、网络、IPC 或不支持的观察一律关闭复用。

证书查找采用“精确 exec 弱键 → 动态路径集 → 当前输入强键”。具有同一动态路径集的多代历史证书只捕获一次当前依赖，但文件角色、metadata 策略、负查找父目录和后端私有条目排除仍属于路径集身份，不会为了提高命中率而放宽等价条件。研究依据与跨平台路线记录在 Bash 复用研究说明，真机多历史基准见 WSL2 结果。

Replay 开始前会一次性装载并校验完整的 content-addressed 输出/效果闭包；输出 wire 数据与全部文件效果都在事务提交前准备完成。因此校验之后即使底层 CAS 文件被删除，也不会在部分采纳后退回真实执行。128 MiB 文件效果的真机结果见 WSL2 artifact closure 基准。

嵌套进程 miss 与外层执行世界共享同一个通用工作区事务出口：经过同文件系统时钟栅栏保护的无内容 inode 变化令牌只负责筛选候选路径，不可变基线/既有精确前沿字节与稳定文件描述符读取才是内容权威；事务重叠、文件系统时钟无法前进或 inode 语义不受支持时直接禁止发布而不猜测。事务驱动只在首次打开变更区间时惰性初始化，纯回放分支不支付观察成本。它消除了整树内容快照，并在 WSL2 真机上把 Pi Bash 冷执行降低了 7.4%。

## 正确性边界

- 投机源不能选择执行后端。
- 隔离后端变化不会改变 `K(a)`。
- 进行中任务和缓存只在相同执行 route 内复用。
- 跨父进程、跨轮次复用必须同时通过精确 exec prototype 与全部动态依赖验证；父 shell 命令刻意不进入子进程 key。
- Linux 世界保留原生 UID/GID/进程身份和用户可见 `PATH`，只把私有工作区映射到逻辑源码路径，拒绝读取常见 credential store 与证书仓，并只允许向私有 branch 写入持久效果。
- Broker 遵守 at-most-once：请求可能已经执行后若响应丢失，会返回错误而不是再次运行命令。
- 只读 Actor 回退时，支持结果捕获的 World 会在宿主调用前记录新鲜度基线，再把这一次权威输出封装进共享缓存；它不会再次调用工具。后续轮次仍须重新通过权限、精确新鲜度、兼容性、投影与提交检查。
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

如需启用进程复用，请在 Linux 或 WSL 2 内运行 Pi 与项目。安装 Rust stable、Git、`strace` 与 C build toolchain，然后运行分级资格检查：

```sh
sudo apt-get install git strace build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
npm run setup:linux
```

`setup:linux` 会编译 held-exec helper，把透明 exec 修改应用到精确固定的 Sandlock revision，并在行为探针通过后安装插件专用 binary；source/patch 与实际安装文件的 digest 会一起盖章。在具备 `/dev/fuse` 的 x86-64/aarch64 主机上，它还会安装来自官方 release、经过固定 SHA-256 校验的 `fuse-overlayfs` 静态程序。它不会修改 Pi，也不会安装 daemon。Runtime 每次仍会重新探测 Landlock ABI 6+、Sandlock、strace，以及完整的 OverlayFS copy-up/whiteout/匿名事务时钟/卸载生命周期。共享 lower 快照后，Linux 进程世界只有在探测通过且精确不可变基线至少包含 256 个条目（本机复测后的保守边界）时才自动选择 host-visible COW；小工作区以及通用 `write`/`edit` 后备继续使用 Git。每个 content commit 只预热并共享一份驱动原生 lower 结构快照；外层观察和嵌套事务用它与各自的类型化 upper journal 重建 merged tree。事务时钟是在私有 upper 存储中的匿名 `O_TMPFILE` inode，并由探测证明其与 merged-view 时间戳的顺序关系，因此 Bash 看不到 Runtime 控制路径。若工作区内出现驱动导致的 `EXDEV`、`EOPNOTSUPP`、`ENOTSUP` 或 `ENOSYS`，完整 trace 会使该分支不可采纳；这覆盖 FUSE 无法透明复现的 lower 目录 rename 等操作。二进制、marker、匿名 inode 或时钟投影不受支持，挂载失败或发生可恢复的生命周期异常，都会让后续路线降级到 Git-worktree；无法确认已经卸载的活挂载及其 pool 会被隔离保留，但不会阻塞插件退出。WSL 必须为版本 2，checkout 应放在 WSL 原生 Linux 文件系统中。

`pi.extensions` 指向 `src/extension.ts`，由 Pi 的公共 TypeScript 扩展加载器直接加载。因此 Git 安装不依赖已提交的构建产物或 dev dependency。`dist` 只作为 npm 使用时的标准 JavaScript/类型入口，在 `npm pack` 或 `npm publish` 时生成。

以代码方式接入时，应按层次使用窄入口：`./core` 提供与宿主无关的 Runtime 与效果事务契约，`./process-reuse` 提供 provenance certificate、规划与 CAS，`./pattern-aware` 提供学习层，`./extension` 提供 Pi 接入。根入口继续作为兼容聚合入口。测试会递归确认 `./core` 与 `./process-reuse` 的依赖闭包不包含任何 Pi package。

在 TUI 中打开 `/speculative-action`。第一层只保留总开关、保存位置、模型 Drafter/Actor fork/历史模式三类预测源和工具策略；采样、解码协议、收益门控、调度与存储容量统一放在“Advanced settings”。关闭的门控参数以及当前 transport 不会使用的动作交接项会自动隐藏。菜单不再暴露 L1/L2 或内部 `sandbox` 类型，而是在逐工具矩阵中分开显示 **Predict、Replay、Observe、Fork**，不再把“允许预测”误写成“当前平台能够提前执行”。能力诊断在会话启动时运行；显式打开“Execution routes”会刷新全部提供者，而应用总开关时会先刷新 Actor Bash 路线，再更新完成提示与 footer。关闭 Bash 预测不会关闭 Actor Bash 历史重放。包括 Enabled 和 Restore defaults 在内的修改都要到 Apply 才生效；切换“All projects”/“This project”会重新载入该层，而项目文件只保存相对规范化共享配置的差异。Actor 路径的 Bash 复用与投机分支内部的进程复用分别计数；状态把“生产者实测、此次无需重跑的进程工作量”和“Actor 路径延迟估计”分开显示，在取得先前权威执行样本前明确显示 `Actor timing unavailable`，不再虚构省时。同次运行的重叠仍标为 observed overlap，而不是因果加速。JSON 容量单位是字节，TUI 内存输入单位是 MiB；没有安全路线的工作始终由 Actor 执行。

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
  "resourceCacheMaxEntries": 512,
  "resourceCacheMaxBytes": 268435456,
  "executionStoreMaxEntries": 4096,
  "executionStoreMaxBytes": 2147483648,
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
    "draftBoundary": "auto",
    "forkMaxTokens": 128,
    "forkTemperature": 0,
    "forkDecoder": "auto",
    "forkForcedPrefix": "auto",
    "timeoutMs": 2000
  }
}
```

`candidateLimit` 默认在每次 Actor 决策并发发出两个单动作 Drafter 请求。宽度为 2 时它们组成延迟对冲：首个包含 schema 有效且已启用 `K(a)` 的响应被接纳，并通过 provider `AbortSignal` 取消仍在运行的同伴；错误、空响应和无效调用不会胜出。严格动作与目标 token 回放保留了全部 6 个可用精确命中、完整提前量以及完全相同的 D3 verifier 工作，同时识别出宽度 2 下 7.78% 的 Drafter 服务为可消除的剩余工作。显式设为 3 或更高时仍保留所有完成样本，也没有隐藏上限，供确实能产生有效宽多样性的模型使用。

`drafterGateEnabled` 默认为 `true`。它把并发根请求视为一个批次，滚动学习动作侧净收益：只有由 Drafter 实际拥有并被 Actor 采纳的工作才按真实工具 `executionAheadMs` 计收益，再减去该批所有请求的服务时间总和。前 4 批用于预热；持续负收益时暂停整批请求，但每跳过 4 次仍做一次有界探测，以便工作负载变化后恢复。设为 `false` 即恢复无条件 Drafter 批次；PatternAware 候选和 Drafter 后继请求不受此门控。

`drafterMaxDepth` 表示每个单动作 Drafter 初始请求之后，最多允许多少次利用已完成工具输出的后继请求。后继请求占用该投机源在下一次 Actor 决策上的既有 slot，不会增加每个决策的请求宽度；设为 `0` 即恢复单步 Drafter。

`drafterMaxTokens` 是可选的硬上限。省略该项——或清空 TUI 输入框——会使用服务商默认输出上限，避免长命令和结构化工具参数被截断。

Drafter 始终接收与 Actor 相同的完整历史，并采用 Pi 模型元数据允许的最低思考强度（支持关闭时即关闭）。每次根请求及基于工具输出的后继请求发出前，投机源都会用 Drafter 自身的 `contextWindow` 检查完整历史和输出额度；较短模型无法容纳时直接在本地跳过，不会截断、摘要，也不会为 Drafter 触发第二条压缩路径。

### Actor probe 与目标验证

`selfSpeculation` 默认关闭，并且同时受 package 顶层 `enabled` 总开关约束。Actor probe 只从权威 Actor 推理流派生，独立的模型 Drafter 请求永远不会被自分叉。同一个 request-scoped 协调器还会把每个通过 schema 校验并完成参数物化的模型 Drafter 或 PatternAware 预测复制到目标验证候选包。解码身份始终使用 Actor 可见的精确 `predictedAction`；为调度和结果复用而扩大的无损 `executionAction` 则独立携带。相同预测 key 只发送一次，并合并来源与 proposal 归因。即使某个动作缺少本地隔离、不能提前执行，它仍可作为边界相对的 tool-call token 交给目标模型验证。

协调器为每次 Actor 决策绑定一个稳定 request ID，只把绝对 decision sequence 与本次请求一致的排序候选包发送到 `POST /self-speculation/candidates`，并在所有候选提交和 probe 完成后调用 `POST /self-speculation/clear`。面向后续决策的预测会保留到对应 Actor 请求启动；同一决策的重试会继承候选包，过期预测则被丢弃。网络或解码失败只会损失加速机会，不会改变 Actor 的正确性路径。

如果目标端在 clear 响应中返回 `verification`，协调器会把真实的 proposed、accepted、rejected 和 unresolved draft token 与注册回执分开统计。candidate ID 与来源会更新按模型、端点、格式、工具和来源分区的 decoder ledger，其平滑验收概率会校准后续候选排序。Runtime 的 Actor 结算则独立训练动作收益：sidecar fork 只有在匹配预测被真实采纳时才获得收益，且按来源分摊实际 `executionAheadMs`。token 拒绝不会改写动作语义概率，单纯 action-key 命中也不再给 fork gate 记收益。为保持 API 兼容，`acceptedDraftTokens` 仍表示注册确认，不能当作目标模型验收。

fork 有两种传输方式：

- `sidecar`：在 Actor 第一个输出片段到达后，把快照和原始请求上下文发送到 `POST /self-speculation/fork`。低置信结果仍保留给 D3，并等待更新的 Actor 快照后再次探测；任一时刻只运行一个 probe，默认每 50 个非空流更新推进一次，最多 5 次。这是配套 `self-speculation` 仓库实现的可移植参考路径。打开 `forkActionEnabled` 后，最早达到置信门槛的完整 probe candidate 会作为一个原子 proposal 重新进入普通动作 Runtime：同批并行 tool call 保持在一起，不同 candidate 批次才互为备选。每个调用都通过 Runtime feedback 携带该批次的 candidate ID、来源/proposal 归因、score、call identity、format、probe timing 和 logprob 证据，并复用 Drafter/PatternAware 相同的 schema 校验、K(a) 去重、执行策略、Scheduler 和 Actor 结算。`forkActionMinConfidence` 默认为 `0.9`，只计算工具名 token 的最低 top-1 概率，参数 token 的不确定性不会误伤动作接纳；调用不完整、证据缺失或格式错误时关闭失败，设为 `0` 可恢复接纳无分数批次。
- `provider`：只把版本化的 `self_speculation` 控制对象放进权威 Actor 请求，其中包括 D2 的 5 次上限、50-token 步长和工具名最低概率门。只有明确实现该 SPORK 协议、并能提供所需 logprob 的 provider 才应使用此模式；Drafter 模型请求不会被修改。普通 OpenAI-compatible 服务可能直接忽略未知字段；仅注入字段并不等于已经实现自投机。

正数工具名置信度门槛会自动请求 token 概率。`requireLogprobs` 仅作为 JSON 兼容覆盖项保留，用于关闭提前执行后仍想收集证据的场景，不再需要独立的 TUI 开关。

在 `sidecar` 模式下，按模型隔离的 fork 门控会滚动学习 `Actor 精确命中的领先时间 - fork 延迟`。默认先放行 4 个样本；持续负收益时暂停请求，但每跳过 4 次仍做一次有界探测，使工作负载改变后可以恢复。连续 2 次 endpoint 失败也进入同一探测回路。上述阈值都可配置；关闭 `forkGateEnabled` 即恢复无条件 fork。同一份 `fork_gate` 策略也会作为 provider/SPORK 提示发送，但 provider 传输需要由推理服务自行执行该策略。

D3 默认上限为 28 个 draft token。严格 DeepSeek tokenizer tape 回放中，相比旧上限 20，28 多提交的 12 个 token 全部被接受、没有增加拒绝 token，并额外减少 10 个 target-step 代理；升到 32 不再增加收益。该值仍可配置，并会再次受推理引擎硬上限约束。

JSON 文件还接受 `requestIDField` 和三条控制路由；JSON 与 TUI 都能配置常用的 endpoint、Bearer token 环境变量名、候选/token 上限、fork 门控策略、tool-call 格式、decoder、温度和专家级语法覆盖。边界与强制前缀默认都是 `auto`，由推理适配器从同一种模型格式派生 CoT 闭合、对齐到工具名的 probe 前缀、解析 framing 和 D3 边界；显式覆盖时必须仍属于该格式。控制路由能够改变推理执行，应只放在可信网络或受认证代理之后；`apiKeyEnv` 只读取指定环境变量，不会保存 token 值。

PatternAware 多步模式开启后，每个权威 Actor 动作——包括 Actor 采纳的 Drafter 结果——都会连同真实输出一起做一次不修改学习状态的同轮重基准；正式学习仍在权威 batch 边界进行。若跨轮的 `K(a)` 与 horizon 集合完全不变，则沿用提前签发的机会而不重复创建，避免共享命中被采纳后又启动同组落选候选。

旧的 `resourceCached` / `sandbox` / `predictionOnly` 三组对象仅作为迁移输入继续识别，读取后统一规范化为一个 `tools` 数组。

## ThinkThread Profile（Linux）

可选入口 `./thinkthread-extension` 通过 ThinkThread 提前执行四个自包含 stock tools，并观察另外两个依赖外部进程的搜索工具的 Actor 结果，不修改 Pi 本体或默认 Linux 后端。在 Linux（macOS 使用 Orb）中安装、启动：

```sh
./scripts/install-thinkthread-profile.sh
cd /path/to/project
tt pi-speculative-action
```

安装脚本支持 `--agent-posix-package /path/to/sdk.tgz`、`--speculative-action-package /path/to/spec.tgz` 使用预构建包，以及重复的 `--model provider/model` 授权。它固定 Agent POSIX SDK 0.1.0，校验 protocol 2 和契约指纹，安装 schema-4 Profile，并把独立运行时放在 `~/.local/share/pi-speculative-action`。环境需要预先提供 Profile 可访问的 Pi、Node、`fd` 和 `rg`。Profile 配置保存在安装目录的 `config` 下，项目 `.pi/speculative-action.json` 仍可覆盖。

适配器提供两条独立能力：

- `speculation.execute`：`read`、`ls`、`write`、`edit` 同轮共享 BASE，执行封存的 `fs.run`，再通过 `fs.verify` / 带冲突检查的 `fs.apply` 采纳；八个执行可共享 BASE。Actor 变更回退会在 Runtime 结算、启动后继动作前使 BASE 失效。
- `observation.capture`：在 Actor 的 `read`、`grep`、`find`、`ls` 执行前创建新快照，封装这一次 Actor 输出，供后续验证复用，不重复执行工具，也不依赖投机 runner。两条路径都由 `EffectTransaction` 统一管理采纳状态。

依赖外部进程的 `grep`、`find` 留在 Actor 路径，但只要捕获的目录树未变化，其已完成的 Actor 结果仍可复用。本 Profile 暂不支持默认 Linux 后端的持久进程证书回放、跨 Bash 父命令复用子进程或 held-exec 接管。它替换默认后端，不在 ThinkThread 内再嵌套 Git/namespace/ptrace 实现。状态会按工具显示独立的 ThinkThread 路线；适配器没有暴露证书仓，因此可复用命令历史的存储操作不可用。普通源码加载仍使用原默认后端，不加载可选 SDK。

`fs.run` 继承 Profile 的固定网络策略（附带配置为 `all`），时间和随机数仍是真实值。因此该 world 不提前执行进程依赖超出工作区证明范围的 `grep`、`find`、`bash`。工作区验证不等于完整的动态进程依赖证书；不宣称单次网络收窄、时间/随机数虚拟化或严格进程证书等价。Supervisor 持有的请求支持持久恢复和终态记录清理，但适配器不会跨 Pi 进程崩溃持久化 request ID。

开发这个可选适配器时，先运行 `npm install --ignore-scripts --save-dev --package-lock=false /path/to/sdk.tgz` 安装匹配的 SDK，再运行 `npm pkg delete 'devDependencies.@thinkthread/agent-posix'`，然后执行检查或打包。Profile 默认两个 Drafter 请求、八个并发工具执行，可通过 `/speculative-action` 调整。

## 接入 Runtime 沙箱

Pi 扩展默认先注册 Linux 进程世界，再注册 Git 工作区 fallback；宿主也可以通过 `executionWorlds` 替换这组世界。所有 World 都按 effect capability 而不是工具名声明能力；内置 runtime world 覆盖进程调用，宿主仍可注入覆盖范围更大的 runtime sandbox。Router 在返回 route 前确认后端可用，因此不可用的 Runtime 沙箱会自然降级到兼容的本地后备。每个成功后端——包括进程 provenance、资源快照和 Git worktree——都返回同一种封存 `WorldBranch` 载体；Gateway 再将其包装为唯一的 `EffectTransaction`，由事务独占新鲜度验证、采纳、放弃和提交状态，载体只保留兼容性证据与后端局部清理职责。

```ts
createSpeculativeActionHost(sessionID, {
  cwd,
  executionWorlds: [runtimeSandbox, createWorkspaceSandbox()],
  // 省略模型、权限与工具接入
})
```

第一个可用的 Runtime 全局沙箱会覆盖所有能够证明 execution context 的进程型工具；不存在时，Router 才检查与动作效果兼容的本地后备。两者都不存在时返回空 route，Runtime 将其结算为 `execution:isolation_unavailable` 并回退 Actor。解析、准备、fork 与 dispose 全部经过同一个 Router，工具侧不会持有可绕开的后端对象。持久进程证书位于 `<agent-dir>/speculative-action/process-reuse`，使用 content address 与策略版本隔离，可以随时删除。

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
# 仅 Linux/WSL：真实 Pi Bash 工具与 process world 资格测试
npm run bench:linux-process
npm pack --dry-run
```

单轨迹消融方法见 [bench/README.md](./bench/README.md)。
