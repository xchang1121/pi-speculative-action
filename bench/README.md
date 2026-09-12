# 投机执行验证与消融

验证以同一机器、同一初始工作区和真实生产入口为准。完整依赖矩阵见能力与验收边界；下列单项通过不代表所有消融行均已完成。

私有录制、预注册材料和每次运行的原始 JSON 保留在仓库外；仓库只保留可复现脚本与使用说明。

## 文件工具采纳延迟

`adoption-latency.mjs` 从完整 `Host.execute` 入口计到 Actor 结果返回，分别报告准备时间、原生调用和 fallback；不把内部 `hitLatencyMs` 当成完整交付时间。使用真实 read/ls/write/edit 和共同绑定的 captured find/grep，确定性模型只提供已知动作，API 请求数为零。

```sh
npm run build
node bench/adoption-latency.mjs . /absolute/output/ready.json 20 ready read,ls,write,edit,find,grep,native-find,native-grep
node bench/adoption-latency.mjs . /absolute/output/running.json 3 running read,ls,write,edit,find,grep
```

每次新建 Host，核对同路径原生输出、文件内容与 POSIX mode，禁止提前写入；采纳与回退分别检查执行次数。`running` 在 Actor 加入后释放受控生产者，单列剩余执行与完成后的交付时间；它验证运行中复用，不代表自然任务加速。报告分别保留命中、回退时长和拒绝原因，不能把正确回退当成测试失败，也不能把所有调用的 p50 当作命中 p50。

Linux/WSL 使用真实进程后端；既有 helper 可通过 `PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 指定，不自动安装。`bash-child` 的父命令不同，共同调用编译后的 CPU/文件效果夹具，Actor 经过真正 held-exec 边界消费子进程。默认并发额度 2 允许接入运行中工作；`--concurrency=1` 验证抢占后的单次回退。子进程就绪时间取自 handoff 完成，不能使用稍后结束的整个父分支时间。

```sh
node bench/adoption-latency.mjs . /absolute/output/bash.json 15 ready bash,bash-child --backend=linux-process
node bench/adoption-latency.mjs . /absolute/output/child-running.json 3 running bash-child --backend=linux-process
node bench/adoption-latency.mjs . /absolute/output/child-preemption.json 2 running bash-child --backend=linux-process --concurrency=1
```

ThinkThread 要在真实 Profile 的 `THINKTHREAD_FS` 根目录运行并通过控制连接检查，使用 `--backend=thinkthread` 和 `read,ls,write,edit,native-find,native-grep,bash`；输出路径也须由 Profile 允许写入。使用生产默认封存输入/runner 路线。write 的 EACCES 回退、edit 的既有权限/身份缺口和不支持的进程路线照实报告；内容/mode 比较不能授予完整写入资格。

`--profile` 临时插桩分段调用，`--fs-profile` 进一步统计 I/O。`producerSpans` 记录准备及生产调用，`spans` 记录 Actor 调用；broker 和 held-exec socket 回调分别归各自所有者。running 模式的生产记录会延续到 Actor 到达之后，不能把这些时长当成到达前的准备时间，也不能相加嵌套或重叠时长。性能结论使用不插桩的运行。`--resource-baseline`、`--workspace-baseline`、`--handoff-baseline` 接受对应旧构建模块的绝对路径，用于同一驱动下的相邻对照。结果文件必须不存在。

## PatternAware 历史与绑定

```sh
node bench/pattern-latency.mjs . /absolute/output/pattern.json /absolute/old-pattern-aware.js
```

旧模块须来自同一依赖版本的构建；省略时只测当前版本。默认比较搜索结果宽度 0、96、512 的三组历史，每组五轮交替顺序，逐项核对预测前沿和最终学习状态。可追加真实事件 JSON 路径：扁平项沿用 `observe`，显式数组项使用 `observeBatch`，不把同批工具误当连续决策。计时包含学习、预测、快照和结算，单列 observe/predict；它是组件对照，不发 API。

## 受控搜索资格

先完成 `npm run build`，再按需要选择低负载命令：

```sh
node --experimental-strip-types bench/grep-captured-qualification.mjs --semantics-only
node --experimental-strip-types bench/grep-captured-qualification.mjs --links-only
node --experimental-strip-types bench/grep-captured-qualification.mjs --cost-only --case=repository
node bench/portable-kernel.mjs
```

- 语义模式每个平台覆盖 46 个小型案例：分层 ignore、原生 glob 优先级、显式被忽略根、UTF-8/UTF-16/二进制原始输入、完整 Pi 格式化、上下文回读与截断。
- 输入、ignore 或负查询变化后，必须回到同 profile 的原工作区 Actor 且恰好执行一次；被忽略的 16 MiB 稀疏文件不能进入内容证据。
- 真实 rg 的取消/结果限额 barrier 必须证明原生关闭和借入输入清理先于结算。已退出的小进程不必再次被杀死，尚被借用的操作仍要等待取消。
- `--case=<标签>[,<标签>...]` 选择案例。链接特征化使用 16 次进程调用，分别检查 `--files` 与实际搜索；不能仅凭文件列表跳过会让搜索报错的 dangling junction。
- 覆盖具名同卷链接、POSIX 链接 ignore 文件、祖先配置、Git 间接路径和显式 Git 数据搜索。HOME、`@`、绝对路径、file URL 与编码空白使用 Pi 原解析器。
- 断裂目标、跨卷输入、cwd 别名与 glob 组合仍未取得资格；原工作区 Actor 保留原始行为及错误。macOS/ARM64 尚未验收。

生产 `createClosedSearchProfile` 同时提供 producer 和 Actor，资格脚本不维护另一份物化器。它只接受已有 Windows x64 rg 15.2.0 或 Linux x64 rg 14.1.0，固定可执行字节、SHA-256 和所属私有副本；不安装或下载。缺少 rg 可以跳过对应资格，不能把不支持的引擎当作成功。

元数据和 ignore 证据驱动 rg 自行选择文件，只复制选中的原始字节。目录按需展开，以私有唯一 marker 检查下一个浅层前沿；不自行解释用户 glob，不枚举被忽略/负 glob 子树，也不隐式遍历 Git 对象。生成规则在执行前移除，被选中配置恢复原始内容。

具名祖先配置可捕获到声明的只读卷根，但不会枚举祖先或递归监听整卷。私有路径几何保留锚定规则，`gitdir`/`commondir` 只指向自有控制文件；新增原先缺失的 commondir 或改动 exclude 会使采纳失效。物理配置身份与逻辑查询拼写分开保存。保持原生仓库识别，不使用会破坏 Git 间接路径的 `--no-require-git`。

该 profile 共同绑定 Actor 与投机的 HOME、原始已准备参数、路径排序及禁用环境 rg/全局 ignore 的设置；祖先 ignore 仍是输入。只在已经物化完整的私有命名空间禁用原生父级加载。它不是 Native Pi 默认等价，也不是任意 Bash 或恶意 JavaScript 沙箱。

`--cost-only --case=repository` 使用约 1 MB 输入、三个查询、每个查询三次采纳尝试。同一个 Host 先关闭预测、实测原 Actor；fallback 必须执行一次，并如实报告 `candidate_join_not_profitable`。不修改收益门或强迫命中。未筛选的默认模式含更大输入、每查询五次采纳及 20 次取消/限额 barrier，仅用于性能阶段。

Native Pi、固定排序 Pi、profile 准备、producer、worker 启动和 Actor 到达后的采纳分别计时。小语义/TUI 夹具的耗时不构成加速声明。报告中的进程计数只覆盖被插桩的参考 worker 与取消探针，不包含普通 Pi 基线或所有生产 profile 进程。

`portable-kernel.mjs` 使用完整原版 Pi find、输入代理、独立 Actor/producer 进程、现有 Runtime 和真实生产 TUI 回调，仅模型与界面输入由脚本提供。它覆盖完成/跨轮次采纳、封存输入重算、running join、并发 producer、名字查询与内容预算、ignore/链接变化、单次 fallback、关闭时排空 Actor 和取消 producer。

deadline/abort 还覆盖已进入的不合作循环及拥有独立进程的输入操作；工作进程和输入回收后才允许 fallback，晚到结果必须被消费。V8 堆限额不是整个进程的 RSS 保证。原生 fd 与共同配置 Actor 分别采样，不用更换执行语义制造收益。

## Linux/WSL 进程复用资格

前提是所选本机组件已通过资格探测；安装与分级依赖见[主说明](../README.md#安装与运行)。可复用已有离线构建，无需为测试重新安装。下面的进程基准支持用 `PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 显式选择已有 binary；这不是默认扩展的环境变量配置接口。

```sh
npm run bench:linux-process -- --output /absolute/output/child-reuse.json
npm run bench:linux-inflight -- --output /absolute/output/inflight.json
```

进程基准经过真实 `createBashTool`、通用进程出口、Linux world、工作区分支、采纳前新鲜度验证和 commit。先比较直接执行与生产 tracing 形态的完整输出、退出和文件效果，报告 trace 成本、污染及严格证书资格；再验证 Actor 消费者不能误接不兼容沙箱证书，并要求同父/不同父 Bash 的子进程命中，改变动态输入后必须 miss。

in-flight 基准让 Actor 在观察 PID 的 Bash 候选运行中到达，再重复同一个 Actor 调用。Runtime 必须仅接管一次；第二次正常执行，单次产物不得进入持久历史或另一份顶层接管表。测试绕过模型请求只是为了固定动作身份，不代表模型质量或端到端工作负载收益。

以下大输入命令仅用于专门的成本阶段：

```sh
npm run bench:linux-pathset -- --output /absolute/output/pathset.json
npm run bench:linux-artifacts -- --output /absolute/output/artifacts.json
npm run bench:linux-topology -- --mode direct --output /absolute/output/topology-direct.json
npm run bench:linux-topology -- --mode reuse --output /absolute/output/topology-reuse.json
```

| 命令 | 必须证明的事实 | 成本边界 |
| --- | --- | --- |
| `linux-pathset` | 一个 32 MiB 输入的八个历史状态都可参与查询，相同路径集只捕获一次当前依赖 | 不按历史证书重复哈希大输入 |
| `linux-artifacts` | 三个父命令的 128 MiB 文件效果重放均命中，摘要一致；开始效果前验证完整产物闭包 | 单独记录校验产物数量与字节 |
| `linux-topology` | 子进程创建两个目录和 32 MiB 确定性产物；三个父 Bash 恢复精确目录/文件状态，外层验证并提交 | direct 是收益对照，保留重放更慢的短任务 |

拓扑基准的 `--rounds N` 范围为 0–4096、默认 96；两侧使用同值扫描交叉点。旧报告驱动的 `linux-admission` 分析入口已移除；收益决策由 `test/scheduler.test.ts` 验证，实际加入/回退使用前述完整 Host 采纳基准。

进程与拓扑基准支持 `--workspace-driver git` / `overlayfs`，用于同机器 A/B；`--source-files N` 扫描目录规模。auto 只有在 binary、FUSE、copy-up、whiteout、opaque 目录、匿名事务时钟、跨视图时间顺序、可见性及卸载均通过，且精确基线至少 256 项时选择 OverlayFS。小树保留 Git；驱动导致的不支持结果会污染证据。`routePreparationMs` 与 fork/hit 分开报告。

```sh
npm run bench:overlay-probe
npm run bench:exec-boundary -- --output /absolute/output/exec-boundary.json
```

Overlay 探针描述宿主非特权能力，不是生产路线切换许可。生产 FUSE upperdir 中的 whiteout/opaque 记录必须按类型化前沿解码，不能当普通目录合并。exec 边界基准分别计量仅 fork/exec 事件的 ptrace、普通进程过滤 strace、seccomp-BPF 加速，并验证 x86-64 held exec 在耗时子进程开始前可被替换，同时记录可观察的 `TracerPid` 差异。

## ThinkThread 真实 Runtime 资格

使用已安装真实 Agent POSIX SDK、已构建的源码 checkout 和运行中的 `tt pi-speculative-action` Profile。在 `THINKTHREAD_FS` 根目录通过 Pi 的 Bash 工具运行：

```sh
node bench/adoption-latency.mjs . /allowed/output/thinkthread.json 3 ready read,ls,write,edit,native-find,native-grep,bash --backend=thinkthread
```

创建夹具前通过 SDK `selfView/fs.stat` 确认 Runtime，环境变量不足为证。统一使用完整 Host 和生产默认执行路线；显式覆写 runner 的旧入口已移除。本地 wire runner 的六工具语义对照保留在 `test/thinkthread-tool-runner.test.ts`，不是 Runtime 通过证明。

SDK 在首次准备或指纹检查时加载；缺失 SDK 时注册成功，自检报告不可用，真实 Actor 仍完成一次原生调用。根普通文件使用封存输入与原版 Pi 绑定；其余输入保留 `fs.run`，两者共用请求/响应预算。

TUI 修改须 Apply；开放 helper 读取后，嵌套 ptrace 仍报 `EPERM`，Bash 提前执行不可用，Actor 回退正确。文件路线、wire 和快照检查不授予嵌套 tracing/handoff、Host Checkpoint/restore、ARM64 或完整进程闭包资格。

## 录制与模型消融

外部 `pi-llm-tape` 仅将完整消息上下文相同的 Actor/Drafter 请求配对，工具名和全部参数须精确匹配：

```sh
npm run bench:tape -- \
  --tape /private/path/deepseek.json \
  --actor-model deepseek-v4-pro --drafter-model deepseek-v4-flash
```

报告区分原始/唯一 K(a)、重复请求、精确命中、Actor 完成前就绪、解码领先及 Drafter 总服务时间，并按派发顺序对宽度 1、2、3、8 对照。请求成本按 Actor 决策计一次，动作覆盖单独计量，不能让并行工具重复放大请求费用。

竞速统计也按获胜响应的完整可解码工具批次计算，重复动作增加候选数量，但不重复增加覆盖；空响应不取消同伴，只有获胜后剩余服务可计为模拟节省。这里分析可解码录制，不代替生产请求的工具资格和执行绑定检查。

Drafter 的一个响应可提供一批工具预测；整批结果按响应顺序反馈，只占一次后续模型请求。`run.ts` 的 `drafterPredictions` 保存完整 `calls` 与每次请求的 `usage`，不再只记录第一项。

## 计时与验收规则

`actualEndToEndMs` 从工具/Host 初始化前计至终态结算、Host 与工作区回收完成；`setupMs`、`agentPromptMs`、`teardownMs` 构成这一总时长。数据集下载、checkout 和最终补丁检查在计时外。Drafter 根请求直接接收 Actor 即将提交的完整上下文，不自行重建首轮消息。

独立 Agent 对照使用纯 Actor+原版工具、关闭的当前 Host，以及开启的父版和当前 Host。提示词、schema、调用 ID、参数分片、完整输出与后续上下文须一致，仅归一化夹具路径与时间戳。父版提交、执行次数及逐轮结果随报告保存，不沿用其他提交的样本量或收益。

独立进程从 `execFile` 计至真实退出，包含导入、首次 SDK 初始化、任务、回收与共同摘要；纯 Actor 不加载投机模块。夹具、oracle 和对称的 SDK 状态检查在父进程计时外执行。顺序提前平衡，保留全部负收益、原生回退与波动。构造流、API 为零，不等于自然模型、Pi TUI 或冷 OS 缓存性能。

采纳的主指标是完整 `Host.execute` 到结果返回的时长。候选已完成时直接测量；运行中候选分别报告接入时间、剩余执行时间和完成后的交付时间。准备能否隐藏取决于实际 Actor/Drafter 重叠，不能把启动和剩余执行混入替换开销，也不能把内部 `hitLatencyMs` 当成完整返回时间。

端到端净加速使用相同任务的独立关闭/开启对照：`disabledMs / enabledMs`，两侧都包含各自初始化和回收。`serializedCounterfactualMs / actualEndToEndMs` 只描述同次轨迹的重叠；它为 1×不表示低开销，大于 1×也不证明比关闭投机快。计算身份去重，未采用预测和旧任务缓存不计入，不能累加所有 `executionAheadMs` 代替重叠。

1. 固定任务、初态、模型、候选数和超时，独立测量开/关投机，计入失败、争用与清理。
2. 要求 `git diff --check` 通过并保留完成信息，比较完整时长、命中、重叠、工具工作量与模型成本。
3. 延迟比较要求 `patchCandidate=true`：低于轮次上限、无超时/Agent 错误、补丁非空且干净，并与 gold patch 文件有交集。
4. `patchCandidate` 只是筛选，不是正确性结论；仍须由数据集工具链/容器执行 `FAIL_TO_PASS`、`PASS_TO_PASS`。
5. 性能收益须重复验证，且结果、权限和回收不退化；安全修正单独说明必要性，负收益行仍保留。

套件串行运行，失败即停止，结果汇入 `suite-result.json`。分别汇总独立对照和同轨迹重叠，标明分子来源；95% bootstrap 按任务聚类，不拆散同任务重复，p95 使用最近秩。独立对照使用 `pairedLatencyStatistics`。

汇总命中率为总命中/总 Actor 动作。未通过 `patchCandidate` 的运行保留失败原因，但不纳入汇总延迟和命中率，仍不能据此代替官方正确性评分。

