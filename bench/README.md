# 投机执行验证与消融

验证以同一机器、同一初始工作区和真实生产入口为准。完整依赖矩阵见能力与验收边界；下列单项通过不代表所有消融行均已完成。

日常改动优先重构现有小型夹具，使用单 worker，Windows/WSL 顺序验证。大输入、原生压力、模型请求和性能重复属于单独阶段，不随普通结构性修正反复运行。这里的命令不会被文档整理自动执行。

私有录制、预注册材料和每次运行的原始 JSON 保留在仓库外；仓库只保留可复现脚本与使用说明。

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
npm run bench:linux-admission -- \
  --direct /absolute/output/topology-direct.json \
  --reuse /absolute/output/topology-reuse.json \
  --expect join --expect-ready join --output /absolute/output/admission.json
```

| 命令 | 必须证明的事实 | 成本边界 |
| --- | --- | --- |
| `linux-pathset` | 一个 32 MiB 输入的八个历史状态都可参与查询，相同路径集只捕获一次当前依赖 | 不按历史证书重复哈希大输入 |
| `linux-artifacts` | 三个父命令的 128 MiB 文件效果重放均命中，摘要一致；开始效果前验证完整产物闭包 | 单独记录校验产物数量与字节 |
| `linux-topology` | 子进程创建两个目录和 32 MiB 确定性产物；三个父 Bash 恢复精确目录/文件状态，外层验证并提交 | direct 是收益对照，保留重放更慢的短任务 |
| `linux-admission` | 使用生产调度器的 Actor、投机执行和采纳分布决定加入或 fallback | 不新造 Bash 专属时长阈值 |

拓扑基准的 `--rounds N` 范围为 0–4096、默认 96；两侧使用同值扫描交叉点。准入分析的 `--elapsed-ms N` 表示候选已执行部分时间，显式预期决策不一致时验证失败。

进程与拓扑基准支持 `--workspace-driver git` / `overlayfs`，用于同机器 A/B；`--source-files N` 扫描目录规模。auto 只有在 binary、FUSE、copy-up、whiteout、opaque 目录、匿名事务时钟、跨视图时间顺序、可见性及卸载均通过，且精确基线至少 256 项时选择 OverlayFS。小树保留 Git；驱动导致的不支持结果会污染证据。`routePreparationMs` 与 fork/hit 分开报告。

```sh
npm run bench:overlay-probe
npm run bench:exec-boundary -- --output /absolute/output/exec-boundary.json
```

Overlay 探针描述宿主非特权能力，不是生产路线切换许可。生产 FUSE upperdir 中的 whiteout/opaque 记录必须按类型化前沿解码，不能当普通目录合并。exec 边界基准分别计量仅 fork/exec 事件的 ptrace、普通进程过滤 strace、seccomp-BPF 加速，并验证 x86-64 held exec 在耗时子进程开始前可被替换，同时记录可观察的 `TracerPid` 差异。

## ThinkThread 真实 Runtime 资格

使用已安装真实 Agent POSIX SDK、已构建的源码 checkout 和运行中的 `tt pi-speculative-action` Profile。在 `THINKTHREAD_FS` 根目录通过 Pi 的 Bash 工具运行：

```sh
npm run bench:thinkthread-tools
```

命令在创建夹具前必须取得真实 SDK `selfView` 与 `fs.stat`；仅设置环境变量不算 Runtime。每个获准 stock 路线比较 Actor 基线、原生 fallback，以及经过生产 Gateway 的真实 `fs.run`、validate、commit：同 cwd/path 的序列化输出、全部文件内容/模式与采纳前工作区不变均须相等。

基线中的原生 read/grep/find/ls 仅在 Actor 授权后执行，不代表可以提前运行宿主函数。首选路线选择失败就是资格失败，不能用原生通过替代。准备、producer、已完成采纳是分开的单样本；poisoned 采纳保留所属夹具，不继续写工作区。

普通测试复用的是本地 wire runner，不是真实 Runtime。Profile 内 Bash 仍须另行通过原生进程/in-flight 资格；冲突、取消恢复和隐式依赖闭包也是独立验收项。

## 录制与模型消融

私有 `pi-llm-tape` 只作分析，不回放或公开提示词。仅完整消息上下文相同的 Actor/Drafter 请求配对，工具名和全部参数必须精确匹配：

```sh
npm run bench:tape -- \
  --tape /private/path/deepseek.json \
  --actor-model deepseek-v4-pro --drafter-model deepseek-v4-flash
```

报告区分原始/唯一 K(a)、重复请求、精确命中、Actor 完成前就绪、解码领先及 Drafter 总服务时间，并按派发顺序对宽度 1、2、3、8 进行对照。请求成本按 Actor 决策计一次，动作覆盖单独计量，不能让并行工具重复放大请求费用。录制不复制进仓库。

模型套件选择 [Claw-SWE-Bench Lite](https://huggingface.co/datasets/TokenRhythm/Claw-SWE-Bench) 的真实问题，只取得选定 base commit；每次创建新的 detached 工作区，不把 gold patch 给 Agent。

先在环境中提供 `DEEPSEEK_API_KEY`，再显式运行需要模型/网络的阶段：

```sh
npm run bench:ablation -- --instance axios__axios-5316 --label baseline --latency remote --candidate-limit 8
npm run bench:suite -- --suite swe_diverse --repeats 3 --label baseline --latency remote --candidate-limit 8
```

PowerShell 可以用隐藏输入设置密钥，完成后移除环境项：

```powershell
$env:DEEPSEEK_API_KEY = Read-Host -MaskInput "DeepSeek API 密钥"
npm run bench:ablation -- --instance axios__axios-5316 --label baseline --latency remote
Remove-Item Env:DEEPSEEK_API_KEY
```

API key 只从环境读取，不写入产物或交给基准 shell 子进程。工作区与 JSON 默认位于系统临时目录，`--output-root` 可显式选定位置。`--prepare-only` 只检查数据集与新 checkout，不发模型请求。

默认预算为 128 轮；`turnLimitReached=true` 表示未完成，不能用于保留算法的决策。`--drafter-max-depth N` 默认 1、0 为单步；后继占用下一决策既有额度而不扩大宽度。`--drafter-disabled --pattern-aware --pattern-state <目录>` 隔离历史模式实验；显式状态目录共享逻辑仓库身份与学习结果，不共享工作区文件，省略时各次运行隔离。

延迟配置对 Actor 和投机加同一确定性延迟：native 不加延迟，remote/sandbox/heavy 表示不同工具成本。本模型 runner 使用资源路线与生产 Git 世界，不把 Bash 预测未执行算作命中；原生 Linux 进程资格使用上一节独立入口。

## 计时与验收规则

runner 只测量一次真实投机 Agent 轨迹，并从同一个权威时间线重建串行反事实，不另跑一条不同 Actor 轨迹作比较：

```text
serializedCounterfactualMs = nonToolMs + authoritativeToolMs
```

`authoritativeToolMs` 只包括本次启动、最终进入 Actor 路径的工具工作，包括被采纳候选的服务时间；不计未采用预测和更早任务的缓存工作。Agent 边界开销属于非工具时间，Agent 完成后的 teardown 单列、不混入该端到端值。

`hiddenLatencyMs` 可以包含原生 Actor 并行工具的重叠；`executionAheadMs` 只记录被采纳投机工作的实测提前量。两者分开报告，不相减成虚构因果收益。缺隔离的匹配只报告有上限的 `executionBlockedPotentialHiddenLatencyMs` 反事实，不加进真实命中或隐藏时延。

1. 固定任务、模型、候选数、延迟配置和超时，每次只改变一个算法因素。
2. 要求 `git diff --check` 通过并保留任务完成信息；同时比较命中率、串行/实际时间比、重叠、提前量、工具工作量与模型成本。
3. 延迟比较要求 `patchCandidate=true`：低于轮次上限、无超时/Agent 错误、补丁非空且干净，并与 gold patch 文件有交集。
4. `patchCandidate` 只是筛选，不是正确性结论；仍须由数据集工具链/容器执行 `FAIL_TO_PASS`、`PASS_TO_PASS`。
5. 只有重复测量改善时延、且正确性和资源没有退化时，才支持保留实现；必须保留负收益边界。

套件串行运行，任务进程失败即停止，各任务保存独立结果并汇总 `suite-result.json`。汇总加速比是平均串行时间/平均实际时间（亦即总和之比），不是单任务比值的平均；提供带种子的 95% 任务聚类 bootstrap 区间，同任务重复不能拆散，p95 使用最近秩。

汇总命中率为总命中/总 Actor 动作。未通过 `patchCandidate` 的运行保留失败原因，但不纳入汇总延迟和命中率，仍不能据此代替官方正确性评分。

