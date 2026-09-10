# Pi 投机执行

Pi 独立插件通过 Drafter、Actor probe 与 PatternAware 预测工具调用；具备安全隔离能力才提前执行，Actor 发出等价动作后采纳结果。

仓库独立维护 Git 历史、构建、测试和依赖锁，Pi package 作为 peer 依赖。路径身份惰性使用已安装 Pi 的路径解析器（尚非公共 export），布局不兼容时放弃缓存；不依赖 Pi 源码树、workspace 别名或特定 `main` 分支。

## 架构

Runtime 分为四个相互独立的层次：

1. **投机源**：模型 Drafter、Actor probe 与历史模式预测只产生与执行方式无关的 `PlanAction`。
2. **动作身份**：Pi 参数只准备一次，然后封存 `K(a)` 与实际执行器绑定。键包含工具语义、已验证 schema、参数和资源名称；动态资源版本属于执行证据。相同输入上的新查询由原执行器求值，不按参数相似度推断输出等价。
3. **执行路由**：动作语义只声明可观察效果，由唯一的 `ExecutionWorldRouter` 选择并准备隔离能力。所选路由刻意不进入 `K(a)`。
4. **调度与结算**：Scheduler 决定启动时机与资源竞争；`ExecutionWorld` 只产生封存后的效果载体，由唯一的 `EffectTransaction` 管理验证、采纳、放弃与提交状态；结算只记录一次匹配、采纳、回退和计时。

执行路线具有固定优先级：

| 优先级 | 路线 | 范围 |
|---|---|---|
| 1 | 已安装的统一执行环境 | 宿主注入的 Runtime 全局世界；探测通过且能够覆盖当前工具时优先 |
| 2 | 本地 `runtime_sandbox` | 合格的 Linux/WSL 进程世界；独立于统一环境开关 |
| 2 | `resource_snapshot` | 观察 Actor 结果，或让显式绑定的原版 `read`、`ls` 操作读取封存输入；不授权提前执行任意 host function |
| 2 | `workspace_branch` | 显式文件操作绑定的本地后备；私有工作区封存输入与效果，同锁验证后提交；默认接入 `write`、`edit` |
| 3 | Actor 回退 | 没有安全路线时完全不发起投机工具执行 |

在 Linux 与 WSL 2 中，默认扩展会注册一个轻量进程世界。它先使用与变更工具相同的私有工作区原语，再用 Sandlock 的 Landlock/seccomp 策略与虚拟文件系统限制进程。当前实现刻意不创建 user、PID 或 mount namespace，因此命令保留 Actor 的原生身份。任何内核能力、binary 或策略探测失败都会移除这条路线；Windows、macOS、WSL 1 或依赖不完整的 Linux 仍走 Pi 的普通 Actor 执行，不会静默降低隔离强度。

能力由提供者的效果保证、已验证的工具绑定和当前设置共同决定：Pi 0.84.1 的封存资源绑定支持 `read`、`ls`，Git 支持 `write`、`edit`，合格 Linux/WSL 2 进程世界再支持 `bash`。这不是按操作系统写一套工具规则；Windows 与 Linux 共用文件操作实现，macOS 使用同一实现但尚缺真机资格测试。没有安全路线的工具保留预测偏好，但不会提前执行。

资源按实际访问采集，沿用同一个 token 的预算、证据、所有权和释放机制，不增加缓存层；原版 Pi 负责图片、截断和结果格式。封存后停止采集，新查询只能使用已有输入，不能扩张权限；预算超限、链接逸出、特殊文件内容或输入语义变化均拒绝采纳。原生 Actor 输出则必须额外证明完整执行窗口，包括祖先路径绑定。Windows 暂不提供这项观察能力：把同一个目录/junction 移走再移回可以保持 inode 和时间戳，不能据此证明 Actor 未读到中间状态；受控 `read`、`ls` 提前执行及其跨轮次复用仍保留，Actor 原生执行不变。图片选项和模型图片能力进入执行身份；路径/MIME helper 按 Pi 版本验收，其他版本只在有证明时保留观察。原生 `grep/find` 仍需完整进程提供者，不能遗漏配置、子进程或 Pi 回读文件。

统一进程出口保留 Pi 的校验、流式输出、截断和结果格式；Linux 只映射精确 exec 调用。x86-64 Linux 在真实子进程第一条用户态指令前暂停 `execve`，共用可执行字节、argv、逻辑 cwd、环境、描述符、凭据、限制、平台和策略证据。不同父 Bash 可复用同一子进程，未命中让原子进程继续一次。运行中接续沿用统一收益准入：Actor 耗时未知不设零等待预算，真实期限和取消仍有效。

进程证书封存动态依赖、有序 stdout/stderr、退出状态和可表示的文件效果，复用前验证完整依赖。可重复结果可以持久保存；已消费的时间、随机数、PID/描述符输入只允许同轮一次性转交。父分支与子进程共享消费状态：子进程被采纳后，父结果不能再整体提交；整体提交期间，子进程不能再接管。只有明确回滚成功才恢复资格。不完整、过期、交互式、网络、IPC 或未建模观察仍拒绝相应复用。

证书查找采用“精确 exec 弱键 → 动态路径集 → 当前输入强键”。具有同一动态路径集的多代历史证书只捕获一次当前依赖，但文件角色、metadata 策略、负查找父目录和后端私有条目排除仍属于路径集身份，不会为了提高命中率而放宽等价条件。研究依据与跨平台路线记录在 Bash 复用研究说明，真机多历史基准见 WSL2 结果。

Replay 开始前会一次性装载并校验完整的 content-addressed 输出/效果闭包；输出 wire 数据与全部文件效果都在事务提交前准备完成。因此校验之后即使底层 CAS 文件被删除，也不会在部分采纳后退回真实执行。128 MiB 文件效果的真机结果见 WSL2 artifact closure 基准。

嵌套进程 miss 与外层执行世界共享同一个通用工作区事务出口：经过同文件系统时钟栅栏保护的无内容 inode 变化令牌只负责筛选候选路径，不可变基线/既有精确前沿字节与稳定文件描述符读取才是内容权威；事务重叠、文件系统时钟无法前进或 inode 语义不受支持时直接禁止发布而不猜测。事务驱动只在首次打开变更区间时惰性初始化，纯回放分支不支付观察成本。它消除了整树内容快照，并在 WSL2 真机上把 Pi Bash 冷执行降低了 7.4%。

## 正确性边界

- 投机源不能选择执行后端。
- 隔离后端变化不会改变 `K(a)`。
- 进行中和已完成工作只能在消费者接受生产者保证时复用；观察到隔离状态的结果不能未经证明跨入 Actor 环境。
- 跨父进程、跨轮次复用必须同时通过精确 exec prototype 与全部动态依赖验证；父 shell 命令刻意不进入子进程 key。
- Linux 世界保留原生 UID/GID/进程身份和用户可见 `PATH`，只把私有工作区映射到逻辑源码路径，拒绝读取常见 credential store 与证书仓，并只允许向私有 branch 写入持久效果。
- Broker 遵守 at-most-once：请求可能已经执行后若响应丢失，会返回错误而不是再次运行命令。
- 只读 Actor 回退时，支持结果捕获的 World 会在宿主调用前记录新鲜度基线，再把这一次权威输出封装进共享缓存；它不会再次调用工具。后续轮次仍须重新通过权限、精确新鲜度、兼容性、投影与提交检查。
- K(a) 相等或显式证明请求覆盖才算预测命中；输入召回不把不同查询记成命中。新查询只在封存输入上求值，随后在提交前验证一次新鲜度。成功结果按完整 K(a) 随原候选有界保留，重复求值可跳过计算；所有结果共用原证据、字节预算和回收，新鲜度、权限、兼容性与提交检查不会跳过。
- 同时缺少 Runtime 沙箱和已注册本地后备的工具会被标记为 execution-blocked，但仍可参与匹配、学习和反事实计时。
- 同名自定义工具保持权威；除非宿主显式提供一致的语义与执行能力，否则不会参与投机。
- 输入证据保护已绑定文件操作的内容和路径语义，不保证宿主元数据零副作用：采样可能更新访问时间。已知特殊文件的内容读取会在打开前拒绝，但 JavaScript 捕获不是防止恶意并发替换为设备文件的内核边界。要求 metadata/事件级无干扰的任务需要另行验收的快照或隔离提供者；当前资源路线不提供这项保证。

资源指纹采集失败后停止领取新项并排空读取与句柄。文件绑定的新目录按 Actor 当前 umask、默认 ACL 和继承策略创建，不复制私有目录权限；新建后回读或访问检查若缺少权限证明，则交还 Actor 执行。会话关闭等待执行、输入、封存、事务与清理结束，再回收工作区。

完整流式意图可在 Actor 继续输出时，提前转换已完成候选的封存数据，沿用原结果缓存和预算。让出一轮事件处理后正式调用已到达便跳过准备；采纳只等待相同动作与执行器的准备，并重新授权、验证新鲜度和提交。准备不发布成功或宿主效果。Actor 写入和 watcher 事件只作失效提示，不能代替精确证明；未完成工作及 checkpoint 后代仍保守失效。

默认主机使用通用封存输入求值，不启用 `read` 范围裁剪；已删除自动扩大预测动作的接口和未完成参数的补齐逻辑。新参数首次仍运行原计算，不声称能自动增量化任意黑盒程序。已有 `PI_READ_RANGE_PROJECTION_RULE` 仅供显式选择：它需配合真实输出覆盖证据，适用于大文件首次窄范围查询。元数据按实际消费字段取证；名字/类型查询不依赖文件大小，内容查询另行证明。Bash 依赖精确命令或原生进程证书，不能从 `| tail -n 20` 等文本推断等价。

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

Linux / WSL 2 的能力分别探测，不要求消费路径具备全部生产依赖：

| 能力 | 最小条件 |
| --- | --- |
| 已有整条命令证书重放 | Node、证书存储和当前资源/事务验证；命中路径不要求 Landlock 或 `strace` |
| 跨父 Bash 子进程重放或运行中接管 | x86-64 Linux、通过当前协议资格的原生 helper、`ptrace`，以及 Pi 管道所需的 `pidfd_getfd` |
| 提前执行 Bash 并生产证书 | Git、支持 `--kill-on-exit` 的 `strace`、合格 Sandlock/Landlock/seccomp 和透明 exec 边界；从源码构建另需 C/Rust 工具链 |
| 加速大型工作区事务 | 可选且通过完整探测的 `fuse-overlayfs`；否则保留 Git |

如确实需要安装可选 Linux 后端，可准备现有安装器所需环境，再运行分级资格检查：

```sh
sudo apt-get install git strace build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
npm run setup:linux
```

生产者资格检查脚本是否从第一个字节完整执行。未通过探测的 binary 不可作为生产者；只有符合当前执行契约的证据才能复用，不转换旧证书，也不主动清空存储。无合格生产者且没有可接受证书时，Actor 正常执行，但不能自行预热进程证书。

`setup:linux` 会编译 held-exec helper，把透明 exec 修改应用到精确固定的 Sandlock revision，并在行为探针通过后安装插件专用 binary；source/patch 与实际安装文件的 digest 会一起盖章。在具备 `/dev/fuse` 的 x86-64/aarch64 主机上，它还会安装来自官方 release、经过固定 SHA-256 校验的 `fuse-overlayfs` 静态程序。它不会修改 Pi，也不会安装 daemon。Runtime 每次仍会重新探测 Landlock ABI 6+、Sandlock、strace，以及完整的 OverlayFS copy-up/whiteout/匿名事务时钟/卸载生命周期。共享 lower 快照后，Linux 进程世界只有在探测通过且精确不可变基线至少包含 256 个条目（本机复测后的保守边界）时才自动选择 host-visible COW；小工作区以及通用 `write`/`edit` 后备继续使用 Git。每个 content commit 只预热并共享一份驱动原生 lower 结构快照；外层观察和嵌套事务用它与各自的类型化 upper journal 重建 merged tree。事务时钟是在私有 upper 存储中的匿名 `O_TMPFILE` inode，并由探测证明其与 merged-view 时间戳的顺序关系，因此 Bash 看不到 Runtime 控制路径。若工作区内出现驱动导致的 `EXDEV`、`EOPNOTSUPP`、`ENOTSUP` 或 `ENOSYS`，完整 trace 会使该分支不可采纳；这覆盖 FUSE 无法透明复现的 lower 目录 rename 等操作。二进制、marker、匿名 inode 或时钟投影不受支持，挂载失败或发生可恢复的生命周期异常，都会让后续路线降级到 Git-worktree；无法确认已经卸载的活挂载及其 pool 会被隔离保留，但不会阻塞插件退出。WSL 必须为版本 2，checkout 应放在 WSL 原生 Linux 文件系统中。

在 **Tools & execution → Execution routes → Search execution** 选择 **Captured search**，再 Apply，让 Actor 与投机共享显式搜索执行器（`searchExecution: "captured"`）；默认仍为 **Native Pi**。受控 `find` 只使用 Pi 已安装的 minimatch/ignore 组件和 Node，处理工作区内分层 .gitignore，保留文件名原文，使用确定的大小写敏感 glob 匹配和排序，不等价于原生 fd。

受控 `grep` 还可使用已有的 rg：Windows x64 15.2.0 或 Linux x64 14.1.0。准备时只读查找 Pi 的二进制目录/PATH，固定可执行文件字节，并在 5 秒期限内验证私有副本；不安装、不下载。两侧使用固定引擎、绑定的 HOME 和原始已准备参数。此 profile 按路径排序，禁用 `RIPGREP_CONFIG_PATH` 和全局 Git ignore，仍捕获父级忽略规则，**不是 Native Pi 默认语义**。已有输入 token 保存所选原始字节、具名祖先/Git 配置及正负元数据，rg 在私有目录中自行选择文件，不递归读取被忽略子树或 Git 对象。原生默认、未验收的平台/版本或缺少 rg 时保留 Actor 路线；缺少 rg 不会关闭受控 find。

准入、采纳和封存输入重算仍由已有 resource/candidate store 负责；宿主 Actor 观察不能为 captured-only profile 授权。显式刷新可重新检查可用性。已绑定调用即使失败也保留执行器：producer 被拒绝可恰好回退一次到同 profile 的 Actor，不能暗中改回 Native Pi。输入/结果预算为 8 MiB/1 MiB，worker 请求含启动期限为 5 秒；调用取消和池关闭还须等待输入准备、原生进程/流关闭及私有目录清理。工作进程按需启动，关闭插件排空 Actor、取消 producer 并恢复原生 Pi；旧设置直接回到默认值。Windows/WSL x64 生产路线及脚本驱动的真实 TUI 回调已验收；macOS/ARM64 和真实 ThinkThread Runtime 仍未验收。这是固定可信执行器，不是任意 Bash 或恶意 JavaScript 沙箱。见[有界资格命令](./bench/README.md#受控搜索资格)：启动与输入准备单独计时，候选就绪不代表值得采纳。

`pi.extensions` 指向 `src/extension.ts`，由 Pi 的公共 TypeScript 扩展加载器直接加载。因此 Git 安装不依赖已提交的构建产物或 dev dependency。`dist` 只作为 npm 使用时的标准 JavaScript/类型入口，在 `npm pack` 或 `npm publish` 时生成。

以代码方式接入时，应按层次使用窄入口：`./core` 提供与宿主无关的 Runtime 与效果事务契约，`./process-reuse` 提供 provenance certificate、规划与 CAS，`./pattern-aware` 提供学习层，`./extension` 提供 Pi 接入。根入口提供公共 API 聚合。测试会递归确认 `./core` 与 `./process-reuse` 的依赖闭包不包含任何 Pi package。

在 TUI 中打开 `/speculative-action`。第一层只保留总开关、保存位置、模型 Drafter/Actor fork/历史模式三类预测源和工具策略；采样、解码协议、收益门控、调度与存储容量统一放在“Advanced settings”。关闭的门控参数以及当前 transport 不会使用的动作交接项会自动隐藏。“Execution routes”按真实顺序显示统一执行环境、本地安全 fallback（封存输入、工作区事务或合格进程） 和始终可用的 Actor；前两层可以分别暂存开关，且不会连带关闭 Actor 观察或 Bash 历史重放。菜单不再暴露 L1/L2 或内部 `sandbox` 类型，而是在逐工具矩阵中分开显示 **Predict、Replay、Observe、Fork**，不再把“允许预测”误写成“当前平台能够提前执行”。能力诊断只在插件启用时初始化：显式打开“Execution routes”会刷新已启用的提供者，关闭状态下的路线保持未探测，也不会启动 helper。应用总开关或层级策略时，会先完成路由诊断更新，再提示完成并刷新 footer。回合登记只修改内存，支持回合中途启用思程层；显式刷新也会重新检查已经断开的 Runtime。关闭 Bash 预测不会关闭 Actor Bash 历史重放。包括 Enabled 和 Restore defaults 在内的修改都要到 Apply 才生效；切换“All projects”/“This project”会重新载入该层，而项目文件只保存相对规范化共享配置的差异。Actor 路径的 Bash 复用与投机分支内部的进程复用分别计数；状态把“生产者实测、此次无需重跑的进程工作量”和“Actor 路径延迟估计”分开显示，在取得先前权威执行样本前明确显示 `Actor timing unavailable`，不再虚构省时。同次运行的重叠仍标为 observed overlap，而不是因果加速。JSON 容量单位是字节，TUI 内存输入单位是 MiB；没有安全路线的工作始终由 Actor 执行。

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
  "executionRouting": { "primary": true, "nativeFallback": true },
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
    "actorProfile": "auto",
    "draftFormat": "auto",
    "draftBoundary": "auto",
    "forkMaxTokens": 128,
    "forkTemperature": 0,
    "forkDecoder": "auto",
    "forkForcedPrefix": "auto",
    "timeoutMs": 2000
  }
}
```

`candidateLimit` 默认在每次 Actor 决策并发发出两个单动作 Drafter 请求。宽度为 2 时，首个完成一次参数准备、校验与执行身份绑定的有效动作胜出，并通过 provider `AbortSignal` 取消仍在运行的同伴；空响应、绑定失败与取消后才完成的绑定不会胜出，也不重新准备参数。预测竞胜不代替隔离和采纳证明。显式设为 3 或更高时保留所有完成样本，额外请求仍计成本。

`drafterGateEnabled` 默认为 `true`，按模型与端点合并根请求、后继及采纳成本。仅给拥有执行且匹配预测的 Drafter 记采纳；没有历史回退耗时的命中收益保持未知，继续既有额度内的探索，不记零、不为测量强制重跑 Actor。已有估计仅作请求预算参考：回退本身可能包含下层复用，请求耗时也可与 Actor 重叠，均不是真实端到端净收益。前 4 批预热，可估收益持续不足或连续失配时暂停根请求，每跳过 4 次探测；设为 `false` 关闭门控。跨轮反馈修改原样本，退出窗口后不再修改；未发请求不记样本。后继仍受深度、时限和调度约束。

`drafterMaxDepth` 表示每个单动作 Drafter 初始请求之后，最多允许多少次利用已完成工具输出的后继请求。后继请求占用该投机源在下一次 Actor 决策上的既有 slot，不会增加每个决策的请求宽度；设为 `0` 即恢复单步 Drafter。

`drafterMaxTokens` 是可选的硬上限。省略该项——或清空 TUI 输入框——会使用服务商默认输出上限，避免长命令和结构化工具参数被截断。

Drafter 始终接收与 Actor 相同的完整历史，并采用 Pi 模型元数据允许的最低思考强度（支持关闭时即关闭）。每次根请求及基于工具输出的后继请求发出前，投机源都会用 Drafter 自身的 `contextWindow` 检查完整历史和输出额度；较短模型无法容纳时直接在本地跳过，不会截断、摘要，也不会为 Drafter 触发第二条压缩路径。

### Actor probe 与目标验证

`selfSpeculation` 默认关闭，并且同时受 package 顶层 `enabled` 总开关约束。Actor probe 只从权威 Actor 推理流派生，独立的模型 Drafter 请求永远不会被自分叉。同一个 request-scoped 协调器还会把每个通过 schema 校验并完成参数物化的模型 Drafter 或 PatternAware 预测复制到目标验证候选包。解码身份始终使用 Actor 可见的精确 `predictedAction`；为调度和结果复用而扩大的无损 `executionAction` 则独立携带。相同预测 key 只发送一次，并合并来源与 proposal 归因。即使某个动作缺少本地隔离、不能提前执行，它仍可作为边界相对的 tool-call token 交给目标模型验证。

协调器为每次 Actor 决策绑定一个稳定 request ID，只把绝对 decision sequence 与本次请求一致的排序候选包发送到 `POST /self-speculation/candidates`，并在所有候选提交和 probe 完成后调用 `POST /self-speculation/clear`。面向后续决策的预测会保留到对应 Actor 请求启动；同一决策的重试会继承候选包，过期预测则被丢弃。网络或解码失败只会损失加速机会，不会改变 Actor 的正确性路径。

如果目标端在 clear 响应中返回 `verification`，协调器单独统计真实的 proposed、accepted、rejected 和 unresolved draft token。candidate ID 与来源更新按模型、端点、格式、工具和来源分区的 decoder ledger，校准后续排序。Actor 结算独立训练动作采纳概率和收益；token 拒绝不改写动作语义概率，单纯键匹配不计收益。`acceptedDraftTokens` 仍是注册确认，不能当作目标模型验收。

fork 有两种传输方式：

- `sidecar`：在 Actor 第一个输出片段到达后，把快照和原始请求上下文发送到 `POST /self-speculation/fork`。低置信结果仍保留给 D3，并等待更新的 Actor 快照后再次探测；任一时刻只运行一个 probe，默认每 50 个非空流更新推进一次，最多 5 次。这是配套 `self-speculation` 仓库实现的可移植参考路径。打开 `forkActionEnabled` 后，最早达到置信门槛的完整 probe candidate 会作为一个原子 proposal 重新进入普通动作 Runtime：同批并行 tool call 保持在一起，不同 candidate 批次才互为备选。每个调用都通过 Runtime feedback 携带该批次的 candidate ID、来源/proposal 归因、score、call identity、format、probe timing 和 logprob 证据，并复用 Drafter/PatternAware 相同的 schema 校验、K(a) 去重、执行策略、Scheduler 和 Actor 结算。`forkActionMinConfidence` 默认为 `0.9`，只计算工具名 token 的最低 top-1 概率，参数 token 的不确定性不会误伤动作接纳；调用不完整、证据缺失或格式错误时关闭失败，设为 `0` 可恢复接纳无分数批次。
- `provider`：只把 `self_speculation` 控制对象放进权威 Actor 请求，其中包括 D2 的 5 次上限、50-token 步长和工具名最低概率门。只有明确实现该 SPORK 协议、并能提供所需 logprob 的 provider 才应使用此模式；Drafter 模型请求不会被修改。普通 OpenAI-compatible 服务可能直接忽略未知字段；仅注入字段并不等于已经实现自投机。

正数工具名置信度门槛会自动请求 token 概率。`requireLogprobs` 是 JSON 证据收集选项，用于关闭提前执行后仍想收集证据的场景，不再需要独立的 TUI 开关。

`sidecar` 与 Drafter 共用上述未知收益及成本口径，按匹配来源分摊。默认 4 个预热样本、每跳过 4 次探测；连续 2 次端点失败进入探测回路，阈值可配置。关闭 `forkGateEnabled` 恢复无条件 fork。`fork_gate` 也作为 provider/SPORK 提示发送，由推理服务执行。

D3 默认上限为 28 个 draft token，可显式配置，并再次受推理引擎硬上限约束；token 验收和动作侧收益分别计量。

JSON 文件还接受 `requestIDField` 和三条控制路由；JSON 与 TUI 都能配置常用的 endpoint、Bearer token 环境变量名、候选/token 上限、fork 门控策略、Actor 模型 Profile、tool-call 格式、decoder、温度和专家级语法覆盖。`actorProfile` 决定最终按什么协议为 Actor 格式化和编码 D3 candidate，默认是 `auto`。显式 Actor Profile 优先于 `draftFormat` 格式覆盖；候选始终按 Actor 的格式和 tokenizer 编码。边界与强制前缀默认都是 `auto`，由推理适配器从同一种模型格式派生 CoT 闭合、对齐到工具名的 probe 前缀、解析 framing 和 D3 边界；显式覆盖时必须仍属于该格式。控制路由能够改变推理执行，应只放在可信网络或受认证代理之后；`apiKeyEnv` 只读取指定环境变量，不会保存 token 值。

对于 Qwen3.5 系列（包括 Qwen3.8 部署别名），如果 Actor provider 使用模型原生的
`tools=` chat template，应把 `actorProfile` 设为 `qwen35_xml`。只有 Actor 请求本身也通过配套
`self-speculation` 包按论文 JSON 协议渲染时，才使用 `qwen35_tagged_json`。Qwen3 的
SPORK/Hermes JSON 路径使用 `qwen3_tagged_json`。只把 sidecar fork 改成
JSON 会导致其 token 前缀与 Actor 不同，无法精确复用 KV cache。Pi 不内置 Qwen 边界 token
ID；推理集成始终使用目标 tokenizer 派生边界。

DeepSeek V4 原生 DSML 路径使用 `actorProfile: "deepseek_v4_dsml"`。Pi 只透传 Profile
和记录服务端返回的实际解析结果；DSML parser、formatter、boundary 以及 D3 能力校验由
`self-speculation` 实现。Pi 提交的是 provider-neutral 的结构化 tool call，因此无论 Drafter
最初使用什么文本协议，都由 Actor Profile 重新格式化并使用 Actor tokenizer 编码，不能直接
注入 Drafter token。

PatternAware 多步模式开启后，每个权威 Actor 动作——包括 Actor 采纳的 Drafter 结果——都会连同真实输出一起做一次不修改学习状态的同轮重基准；正式学习仍在权威 batch 边界进行。若跨轮的 `K(a)` 与 horizon 集合完全不变，则沿用提前签发的机会而不重复创建，避免共享命中被采纳后又启动同组落选候选。

学习库只接受当前持久化格式：事件去重存储，样本引用事件索引，且 gap 必须与所属样本池一致。读取不匹配格式时不迁移、不改写原文件；后续真实动作会重新积累并保存当前格式的学习结果。恢复的提示仍须经过当前 schema、K(a)、权限与执行证据验证，不直接授权采纳。

工具策略只接受一个 `tools` 字符串数组，不按执行后端分组。配置层合并后，只有缺省字段使用默认工具列表；显式空数组、旧分组对象或无效选择均不启动工具预测，不再自动迁移或补齐。有效数组按输入顺序去重并过滤未注册工具，不改变 Actor 正常执行、历史重放或分层执行权限。

## ThinkThread Profile（Linux）

可选入口 `./thinkthread-extension` 通过 ThinkThread 提前执行 `read`、`ls`、`write`、`edit`，不修改 Pi 本体，也不替换默认 Linux 后端。在 Linux（macOS 使用 Orb）中安装、启动：

```sh
./scripts/install-thinkthread-profile.sh
cd /path/to/project
tt pi-speculative-action
```

安装脚本支持 `--agent-posix-package /path/to/sdk.tgz`、`--speculative-action-package /path/to/spec.tgz` 使用预构建包，以及重复的 `--model provider/model` 授权。它固定 Agent POSIX SDK 0.1.0，校验 protocol 2 和契约指纹，安装 schema-4 Profile，并把独立运行时放在 `~/.local/share/pi-speculative-action`。环境需要预先提供 Profile 可访问的 Pi、Node、`fd` 和 `rg`。Profile 配置保存在安装目录的 `config` 下，项目 `.pi/speculative-action.json` 仍可覆盖。

Profile 共享已有的执行与观察边界：

- `speculation.execute`：`read`、`ls`、`write`、`edit` 使用 Pi 原生实现，同轮共享 BASE，执行封存的 `fs.run`，再通过 `fs.verify` / 带冲突检查的 `fs.apply` 采纳；八个执行可共享 BASE。Actor 变更回退会在 Runtime 结算、启动后继动作前使 BASE 失效。
- Actor 的 `read`、`ls` 统一使用宿主既有 resource observation，包括完整执行窗口的稳定性证明。思程 snapshot/content 相等不能证明 Actor 没有读到 A→B→A 的中间状态，因此不再维护第二套思程 Actor 结果快照。这条路径不需要 SDK 或投机 runner；两条路径仍由 `EffectTransaction` 管理采纳状态。

Profile 会先尝试思程 world，并保留原生 Linux 进程 provider 与 Git 工作区 provider。Bash 不进入可移植 runner，只有当前环境的探测通过后才使用原生进程 world。注册 fallback 不代表外层思程允许嵌套 tracing、helper 或 handoff；公开思程 Runtime 当前只有 aarch64 包，Actor held-exec 实现却限定 x86-64 Linux。缺失的能力回退 Actor，不能保证思程中的完整能力或性能不低于原生路径。普通源码加载仍使用原来的默认 provider，不加载可选 SDK。

`fs.run` 继承 Profile 的固定网络策略（附带配置为 `all`），时间和随机数仍是真实值。该 world 只接收已具备证明的固定 stock-tool runner。原生 `grep/find` 继承外部配置和可执行程序（rg 配置还可指定预处理程序）；工作区快照和当前思程验证均不能证明完整闭包。因此它们声明真实的宿主进程效果，由能力矩阵自动阻止无证据的提前执行和结果复用，Actor 行为保持原样。只有提供实际进程依赖/效果证明的 provider 才能重新启用，不维护配置文件黑名单。Bash 同样需要原生进程证明。工作区验证不等于完整的动态进程依赖证书；不宣称单次网络收窄、时间/随机数虚拟化或严格进程证书等价。Supervisor 持有的请求支持持久恢复和终态记录清理，但适配器不会跨 Pi 进程崩溃持久化 request ID。

固定版本的 Agent POSIX SDK 归档现在既是 lockfile 管理的开发依赖，也是安装器的默认载荷；干净 checkout 只需 `npm ci` 即可检查、测试、构建和打包可选适配器，不依赖兄弟仓，也不再临时改写 manifest。`--agent-posix-package` 仍可显式覆盖为离线包。Profile 默认两个 Drafter 请求、八个并发工具执行，可通过 `/speculative-action` 调整。

## 接入 Runtime 沙箱

Pi 扩展先注册配置的 runtime provider，再保留原生 Linux 进程世界和 Git 工作区 fallback；因此 `createExecutionWorlds` 扩展执行层级，而不替换安全后备。更底层的 Host API 仍接受显式 `executionWorlds` 列表。World 同时声明 effect guarantee 与必要的工具作用域；没有具体 action 的预热也遵守作用域。Router 在返回 route 前确认后端可用，执行前再次检查层级策略；首选 provider 不可用时在执行前降级，已经执行失败的动作不会盲目换环境重跑。每个成功后端返回同一种 `WorldBranch`，Gateway 将其包装为 `EffectTransaction`，统一管理新鲜度验证、采纳、放弃与提交；载体保留兼容性证据与后端局部清理职责。

```ts
const workspace = new WorkspaceSandboxService();
const host = createSpeculativeActionHost(sessionID, {
  cwd,
  executionWorlds: [runtimeSandbox, workspace.createExecutionWorld()],
  // 省略模型、权限与工具接入
});
// 所属会话结束时：
try { await host.dispose(); } finally { await workspace.dispose(); }
```

第一个可用的 Runtime 全局沙箱会覆盖所有能够证明 execution context 的进程型工具；不存在时，Router 才检查与动作效果兼容的本地后备。两者都不存在时返回空 route，Runtime 将其结算为 `execution:isolation_unavailable` 并回退 Actor。解析、准备、fork 与 dispose 全部经过同一个 Router，工具侧不会持有可绕开的后端对象。持久进程证书位于 `<agent-dir>/speculative-action/process-reuse`，使用内容寻址与当前生产者执行契约隔离，可以随时删除。

## 计时口径

对被采纳的结果：

- `attemptLeadMs`：投机意图产生到 Actor 调用被拦截。
- `executionAheadMs`：拦截前已完成的投机执行量，上限为实测工具时长。
- `hitLatencyMs`：Actor 拦截至采纳及结果保留；会话回收另计。
- `expectedActorMs`：历史回退服务耗时参考，可能含下层复用；无样本时缺省，不能冒充关闭投机的直接耗时。

候选准入比较剩余等待、采纳和回退成本；已付成本不重复扣除。已完成候选至少取得 4 个同动作、同采纳路径样本才按收益否决，已缓存查询与首次转换分别采样；稀疏或宽泛样本不作否决依据，Actor 耗时未知不强制重跑。任务计时按实际被采纳的生产者与查询转换去重：缓存重复命中只计一次，淘汰后重算另计，上一任务的计算不重复计入；准备后被拒绝不算权威工具工作。加速比为反事实串行总时长/端到端时长，反事实保留实际开销，仅消除已采纳计算的重叠。另须对照相同任务和初态的投机开关，计入失败、争用及完整回收，验证实际净收益。

对缺少隔离而阻断的匹配，以下分解只报告反事实潜力，不计实际命中或节省：

```text
executionBlockedPotentialHiddenLatencyMs = min(actorDuration, predictionLead)
executionBlockedPotentialHitLatencyMs    = actorDuration - potentialHidden
```

## 验证

```sh
npm install --ignore-scripts
npm run check
npm run build
npm test -- --maxWorkers=1 --no-file-parallelism
npm run bench:check
# 仅 Linux/WSL：真实 Pi Bash 工具与 process world 资格测试
npm run bench:linux-process
npm pack --dry-run
```

日常验证优先使用现有小型夹具与单 worker，两平台顺序运行；大输入、模型请求和性能压力测试按阶段单独执行，不随结构性修正反复运行。完整消融方法及资格命令见 [验证说明](./bench/README.md)。
