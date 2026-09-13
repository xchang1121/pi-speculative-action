# Pi 投机执行

Pi 独立插件通过 Drafter、Actor probe 与 PatternAware 预测工具调用；具备安全隔离能力才提前执行，Actor 发出等价动作后采纳结果。

仓库独立维护 Git 历史、构建、测试和依赖锁，Pi package 作为 peer 依赖。路径身份惰性使用已安装 Pi 的路径解析器（尚非公共 export），布局不兼容时放弃缓存；不依赖 Pi 源码树、workspace 别名或特定 `main` 分支。

## 架构

Runtime 分为四层：

1. **投机源**：模型 Drafter、Actor probe 与历史模式预测只产生与执行方式无关的 `PlanAction`。
2. **动作身份**：参数准备一次，封存 `K(a)` 与执行器绑定。键包含语义、schema、参数和资源名称，动态版本另属执行证据。仅自有、递归冻结的无损纯数据树可携带身份；无法证明时保留 Actor 原调用和错误语义。新查询由原执行器在封存输入上求值。
3. **执行路由**：动作语义只声明可观察效果，由唯一的 `ExecutionWorldRouter` 选择并准备隔离能力。所选路由刻意不进入 `K(a)`。
4. **调度与结算**：Actor 独立绑定、选择；候选持有认领与借用。Scheduler 比较采纳与原生执行、捕获/封存成本；就绪结果持续亏损则回退，每四次决策探测恢复，同次枚举共用判定。`EffectTransaction` 验证提交。`host.execute` 用 `prepareActorCall` 的专属句柄结算，`ActorAction` 持有证据，选中后不能回退。轮次仅保留未结算调用，归属不依赖 ID 或完成顺序。

执行路线具有固定优先级：

| 优先级 | 路线 | 范围 |
|---|---|---|
| 1 | 已安装的统一执行环境 | 宿主注入的 Runtime 全局世界；探测通过且能够覆盖当前工具时优先 |
| 2 | 本地 `runtime_sandbox` | 合格的 Linux/WSL 进程世界；独立于统一环境开关 |
| 2 | `resource_snapshot` | 观察 Actor 结果，或让显式绑定的原版 `read`、`ls` 操作读取封存输入；不授权提前执行任意 host function |
| 2 | `workspace_branch` | 显式文件操作绑定的本地后备；私有工作区封存输入与效果，同锁验证后提交；默认接入 `write`、`edit` |
| 3 | Actor 回退 | 没有安全路线时完全不发起投机工具执行 |

Linux/WSL 2 进程世界共用文件工具的私有工作区，再以 Sandlock 的 Landlock/seccomp 和虚拟文件系统约束进程。不创建 user/PID/mount namespace，保留 Actor 原生身份。内核、binary 或策略探测失败便移除路线；其他平台保持原生 Actor，不降低隔离强度。

能力由提供者的效果保证、已验证的工具绑定和当前设置共同决定：Pi 0.84.1 的封存资源绑定支持 `read`、`ls`，Git 支持 `write`、`edit`，合格 Linux/WSL 2 进程世界再支持 `bash`。这不是按操作系统写一套工具规则；Windows 与 Linux 共用文件操作实现，macOS 使用同一实现但尚缺真机资格测试。没有安全路线的工具保留预测偏好，但不会提前执行。

资源按实际访问采集；token 统一拥有预算和证据，已捕获字节、目录名可供后续元数据查询使用。原版 Pi 负责图片、截断和格式。封存后停止采集；预算超限、链接逸出、特殊文件内容或语义变化均拒绝采纳。原生 Actor 观察另需祖先路径窗口证明。Windows 的目录/junction 移回可保持 inode 与时间戳，因此关闭原生观察；受控 `read/ls` 提前执行及跨轮复用保留。图片设置和模型能力进入身份，路径/MIME helper 按 Pi 版本验收。原生 `grep/find` 仍须完整进程证明，包括配置、子进程和回读文件。

统一进程出口保留 Pi 的校验、流式输出、截断和结果格式；Linux 只映射精确 exec 调用。x86-64 Linux 在真实子进程第一条用户态指令前暂停 `execve`，共用可执行字节、argv、逻辑 cwd、环境、描述符、凭据、限制、平台和策略证据。不同父 Bash 可复用同一子进程，未命中让原子进程继续一次。运行中接续沿用统一收益准入：Actor 耗时未知不设零等待预算，真实期限和取消仍有效。

进程证书封存动态依赖、有序 stdout/stderr、退出状态和可表示的文件效果，复用前验证完整依赖。可重复结果可以持久保存；已消费的时间、随机数、PID/描述符输入只允许同轮一次性转交。父分支与子进程共享消费状态：子进程被采纳后，父结果不能再整体提交；整体提交期间，子进程不能再接管。只有明确回滚成功才恢复资格。不完整、过期、交互式、网络、IPC 或未建模观察仍拒绝相应复用。

证书按“精确 exec 弱键 → 动态路径集 → 当前输入强键”查找。同路径集的多代证书共享一次当前依赖捕获，仍保留文件角色、metadata 策略、负查找父目录与私有条目排除。详见 [Bash 复用研究](./docs/bash-reuse-research.md)。

重放先装载并校验整个 CAS 输出/效果闭包，提交前准备 wire 与文件效果；之后删除底层 CAS 不会导致部分采纳后重跑。产物闭包的验证入口见[基准说明](./bench/README.md)。

嵌套进程 miss 共用外层工作区事务。同文件系统时钟栅栏后的 inode 变化令牌只筛选路径；不可变基线、精确前沿字节及稳定描述符读取才作内容权威。重叠、时钟不前进或 inode 语义不支持时禁止发布。驱动在首次变更时初始化，纯重放不付观察成本。

## 正确性边界

- 投机源不能选择执行后端。
- 隔离后端变化不会改变 `K(a)`。
- 运行中复用保持相同路线与父世界；已完成共享结果跨路线仍须执行绑定兼容和新鲜度证明。独立副作用各自执行，独占结果只允许一次采纳。
- 跨父进程、跨轮次复用必须同时通过精确 exec prototype 与全部动态依赖验证；父 shell 命令刻意不进入子进程 key。
- Linux 世界保留原生 UID/GID/进程身份和用户可见 `PATH`，只把私有工作区映射到逻辑源码路径，拒绝读取常见 credential store 与证书仓，并只允许向私有 branch 写入持久效果。
- Broker 遵守 at-most-once：请求可能已经执行后若响应丢失，会返回错误而不是再次运行命令。
- 支持观察的 World 在只读 Actor 回退前记录新鲜度基线，用该次权威输出建立共享结果，不重跑工具。后续采纳仍检查权限、精确新鲜度、兼容性、投影与提交。
- 预测命中要求 K(a) 相等或显式请求覆盖；输入召回不改变命中判定。新查询由原执行器在封存输入上求值，成功结果按完整 K(a) 共用原候选的证据、字节预算与回收。缓存命中仍通过上述采纳检查。
- 同时缺少 Runtime 沙箱和已注册本地后备的工具会被标记为 execution-blocked，但仍可参与匹配、学习和反事实计时。
- 同名自定义工具保持权威；除非宿主显式提供一致的语义与执行能力，否则不会参与投机。
- 输入采样可能更新访问时间，不保证宿主元数据零副作用。Linux x64/ARM64 先以 O_PATH 绑定 inode，确认为普通文件才开放内容读取；缺少 `/proc/self/fd` 时拒绝捕获。其他平台不具备这项并发替换防护。metadata/事件级无干扰仍需独立验收的隔离提供者；ARM64 尚未真机验收。

文件绑定按需构造能力；关闭拒绝新读取，排空操作、句柄及所属图像 worker，不等待旁路 Actor。工作区在异步边界前持有路径、字节与目录状态副本，检查点仅公开身份。新目录遵循 Actor 的 umask、默认 ACL 和继承策略，缺少新目录观察权限证明则回退。会话按原始标识持有轮次，以同一队列创建、替换和关闭；排空执行、封存、事务及清理后回收工作区。

完整流式意图可提前准备；新建候选或转换前让出事件处理，正式调用已到达便跳过。预测与各预览独立持有需求，仅当前需求参与调度；无主的未启动工作退役，共享结果可保留。普通生产者按事件轮次合并启动，明确意图保留即时路径。准备不产生权威输出或宿主效果，采纳仍独立绑定执行器并通过上述检查。见[执行边界](./docs/bash-reuse-capability-lattice.md)。

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

准入、采纳和封存输入重算仍由已有 resource/candidate store 负责；宿主 Actor 观察不能为 captured-only profile 授权。显式刷新可重新检查可用性。已绑定调用即使失败也保留执行器：producer 被拒绝可恰好回退一次到同 profile 的 Actor，不能暗中改回 Native Pi。输入/结果预算为 8 MiB/1 MiB，worker 请求含启动期限为 5 秒；调用取消和池关闭还须等待输入准备、原生进程/流关闭及私有目录清理。工作进程按需启动，关闭插件排空 Actor、取消 producer 并恢复原生 Pi；旧设置直接回到默认值。Windows/WSL x64 生产路线及脚本驱动的真实 TUI 回调已验收；macOS/ARM64 及思程内的受控搜索仍未验收。这是固定可信执行器，不是任意 Bash 或恶意 JavaScript 沙箱。见[有界资格命令](./bench/README.md#受控搜索资格)：启动与输入准备单独计时，候选就绪不代表值得采纳。

`pi.extensions` 指向 `src/extension.ts`，由 Pi 的公共 TypeScript 扩展加载器直接加载。因此 Git 安装不依赖已提交的构建产物或 dev dependency。`dist` 只作为 npm 使用时的标准 JavaScript/类型入口，在 `npm pack` 或 `npm publish` 时生成。

以代码方式接入时，应按层次使用窄入口：`./core` 提供与宿主无关的 Runtime 与效果事务契约，`./process-reuse` 提供 provenance certificate、规划与 CAS，`./pattern-aware` 提供学习层，`./extension` 提供 Pi 接入。根入口提供公共 API 聚合。测试会递归确认 `./core` 与 `./process-reuse` 的依赖闭包不包含任何 Pi package。

在 TUI 中打开 `/speculative-action`。第一层提供总开关、保存位置、模型 Drafter/Actor fork/历史模式和工具策略；采样、协议、收益门控、调度与容量放在“Advanced settings”。关闭的门控参数和当前 transport 不使用的交接项隐藏。所有修改，包括 Enabled 和 Restore defaults，都在 Apply 后生效；切换“All projects”/“This project”会重载该层，项目文件只保存相对共享配置的差异。

“Execution routes”依次显示统一执行环境、本地安全 fallback（封存输入、工作区事务或合格进程）和 Actor。前两层可分别开关，不连带关闭 Actor 观察或 Bash 历史重放；逐工具显示 **Predict、Replay、Observe、Fork**。插件启用后，打开路线页会实际刷新已启用提供者；应用开关或策略也先等待诊断，再提示完成并刷新 footer。未启用的路线不探测、不启动 helper。回合登记只改内存，可中途启用思程；显式刷新会重查断开的 Runtime。

Linux Bash 在证书目录为空且后端尚未启动时直接运行 Actor，省去重放身份准备和子进程 helper 启动；每次重查目录，空分片或 I/O 不确定时仍走完整检查。已有或正在准备的后端也保留该路线。显式刷新仍运行 helper 功能自检，未探测不报可用；关闭 Bash 预测仍保留历史重放。

Actor Bash 与投机分支内的复用分别计数。状态分开显示生产者实测工作量和 Actor 延迟估计；没有权威样本时显示 `Actor timing unavailable`，不记零成本或虚构省时。同次重叠标为 observed overlap，不作因果加速。JSON 容量单位是字节，TUI 内存单位是 MiB；无安全路线时由 Actor 执行。

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

Drafter 始终接收与 Actor 相同的完整历史，默认采用 Pi 模型元数据允许的最低思考强度（支持关闭时即关闭）。代码接入可通过 `getDraftOptions` 显式设置 `reasoning`；强度按 Drafter 模型能力规范化，工具调用续轮沿用，且不继承 Actor 的强度或输出上限。thinking 请求使用 `tool_choice: auto`，允许模型结束预测并兼容不支持强制工具调用的提供者。每次根请求及基于工具输出的后继请求发出前，投机源都会用 Drafter 自身的 `contextWindow` 检查完整历史和输出额度；较短模型无法容纳时直接在本地跳过，不会截断、摘要，也不会为 Drafter 触发第二条压缩路径。

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

JSON 文件还接受 `requestIDField` 和三条控制路由；JSON 与 TUI 均可配置端点、Bearer token 环境变量、候选/token 上限、fork 门控、Actor Profile、tool-call 格式、decoder、温度和语法覆盖。`actorProfile` 默认 `tagged_json`，保留旧版 provider v1 payload 和 `draftFormat`；显式选择 `auto` 或模型专用 Profile 才使用 v2，专用 Profile 优先于格式覆盖。候选始终按 Actor 的格式与 tokenizer 编码。边界和强制前缀默认 `auto`，由推理适配器从同一模型格式派生 CoT 闭合、工具名前缀、解析 framing 和 D3 边界，显式覆盖也须属于该格式。控制路由应位于可信网络或受认证代理之后；`apiKeyEnv` 只读取指定环境变量，不保存 token 值。

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

PatternAware 在权威 batch 边界学习；多步模式对权威动作及真实输出做同轮续推，包括已采纳的 Drafter 结果。跨轮 `K(a)`/horizon 不变且原机会已准入时沿用；关闭中的提案和绑定拒绝不会阻止下一轮重新预测。动作回退只取当前会话已观察的权威动作，假设续推不重复扩展这份样本。真实失配和匹配后拒绝更新原样本的概率，迟到反馈不进入新会话或替代样本；未观察的失败单独保留。入库事件与公开快照独立持有，续推历史不随后来的学习漂移；绑定缓存仅用于库内数据，每个对象最多 128 项，公开推导每次重新分析可变参数。

持久化仅接受当前格式：事件去重、样本引用索引，gap 与样本池一致。不兼容文件不迁移或改写；后续权威动作重新积累。恢复的预测仍须通过当前 schema、K(a)、权限与执行证据。

工具策略只接受一个 `tools` 字符串数组，不按执行后端分组。配置层合并后，只有缺省字段使用默认工具列表；显式空数组、旧分组对象或无效选择均不启动工具预测，不再自动迁移或补齐。有效数组按输入顺序去重并过滤未注册工具，不改变 Actor 正常执行、历史重放或分层执行权限。

## ThinkThread Profile（Linux）

可选入口 `./thinkthread-extension` 接入 ThinkThread 的 `read`、`ls`、`write`、`edit`；仅注册时不加载 SDK，首次准备或指纹检查才加载。普通源码入口保留默认 provider。[alpha4](https://gitcode.com/aideveloper/capsule_public) 提供 ARM64/x86_64 Linux RPM；Windows 使用 WSL2，macOS 使用 Orb。先准备 Runtime 和可访问的 Pi、Node，再安装 Profile：

```sh
./scripts/install-thinkthread-profile.sh
cd /path/to/project
tt pi-speculative-action
```

安装器支持 `--agent-posix-package /path/to/sdk.tgz`、`--speculative-action-package /path/to/spec.tgz` 和重复的 `--model provider/model` 授权；校验 SDK 0.1.0、protocol 2 及契约指纹，写入 schema-4 Profile 和 `~/.local/share/pi-speculative-action`。配置位于安装目录的 `config`，项目 `.pi/speculative-action.json` 可覆盖。原生搜索另需 Profile 可访问的 `fd`/`rg`。

- 默认 Linux 入口对根观察的普通文件内容依赖使用 SDK 不可变输入，重新绑定 Pi 0.84.1 原版操作；不执行传入的宿主闭包。输入及路径元数据共限 8 MiB；内容验新前后和采纳时检查当前读取权限、ACL 变化迹象及祖先路径绑定。
- 目录、缺失项、超预算输入、父检查点和自定义 runner/Node 配置保留 `fs.run`；开始输入证明后发现权限或内容变化则拒绝候选。两条路径共用 1 MiB 请求、512 KiB 响应及绑定的图像设置，未知执行器拒绝该后端。输入路线等待操作和线程结束后完成取消，`fs.run` 保留 120 秒执行上限。
- 投机同轮共享 BASE；共享观察首次采纳合并验新与只读提交，后续逐次验新，写入保留 `fs.apply` 冲突检查。关闭等待进行中的验证与采纳；Actor 变更结算前使 BASE 失效。
- Actor 的 `read/ls` 仍由宿主资源观察证明完整执行窗口；snapshot/content 相等不足以排除 A→B→A。此路径不使用 SDK/runner，不维护独立思程结果快照，与投机路径共用 `EffectTransaction`。

`fs.run` 继承封存的 Profile 网络策略（默认 `all`），不虚拟时间/随机数，也不提供单次网络收窄；这里只接入固定原版工具 runner。原生 `grep/find` 的外部配置、预处理器和子进程，以及 Bash，仍须完整进程依赖/效果证明，不能凭工作区或 snapshot 内容相等授权。缺少证明时保留 Actor。SDK 协调持久请求及终态清理，适配器不跨 Pi 进程崩溃保存 request ID。

本机 alpha4 x86_64 的 `read/ls/edit` 采纳通过；新文件 `write` 因异步 `realpath` 返回 `EACCES` 仍回退 Actor 一次。SDK 按需加载减少启动和冷回退成本，仍有相对纯 Actor 的额外开销。TUI 的 Ready 表示连接与路线准备成功，资格与性能边界见[真实 Runtime 检查](./bench/README.md#thinkthread-真实-runtime-资格)。

SDK 归档由 lockfile 和安装器共用；干净 checkout 用 `npm ci` 即可检查、测试、构建和打包，无需兄弟仓或改写 manifest。Profile 默认两个 Drafter 请求、八个并发工具执行，可通过 `/speculative-action` 调整。

## 接入 Runtime 沙箱

Pi 扩展依次注册 Runtime、Linux 进程和 Git fallback；`createExecutionWorlds` 与 Host 的 `executionWorlds` 可扩展层级。World 声明效果保证和工具作用域。仅已准入的 Drafter/sidecar 请求预热已注册的选中工具；模型返回后仍响应所属请求与回合的取消，Git 在阶段边界停止。完整 Actor 意图可提前准备，但让出事件处理后须确认正式调用尚未到达。

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

Router 在选路、执行前查能力，依次选择 Runtime、本地后备；无安全路线以 `execution:isolation_unavailable` 回退 Actor，执行失败不换环境重跑。后端返回 `WorldBranch` 并持有证据与句柄；Gateway 的 `EffectTransaction` 管理验新、采纳和回收。每次验新排队取得新证明，提交预留后的验证等待提交完成。`validateAndCommit` 仅供共享观察合并验新与无效果提交，独占分支忽略它。持久证书按内容及生产者契约保存在 `<agent-dir>/speculative-action/process-reuse`，可随时删除。

## 计时口径

对被采纳的结果：

- `attemptLeadMs`：投机意图产生到 Actor 调用被拦截。
- `executionAheadMs`：拦截前已完成的投机执行量，上限为实测工具时长。
- `hitLatencyMs`：Actor 拦截至采纳及结果保留；会话回收另计。
- `expectedActorMs`：历史回退服务耗时参考，可能含下层复用；无样本时缺省，不能冒充关闭投机的直接耗时。

候选准入比较剩余等待、采纳和回退成本，已付成本不重复扣除。已完成候选至少有 4 个同动作、同采纳路径样本才按收益否决；首次转换与缓存查询分别采样，稀疏或宽泛样本不否决，Actor 耗时未知不强制重跑。任务按实际计算去重：原生执行、观察结果提升为缓存及多次采纳共用不可变区间，只计一次；独立生产、查询转换和淘汰重算另计。计时器仅保留数值端点与计算的弱引用，旧任务和未采用准备不算本次权威工作。主加速比为同次运行的反事实串行总时长/完整端到端时长；反事实保留实际开销，仅消除权威计算的重叠，故无重叠为 1×，有重叠大于 1×。独立开关对照另验隔离、预测、争用与回收的额外成本；开启更慢的条件须保留并继续减负，不能代入主分子。1×不表示没有额外成本。

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
