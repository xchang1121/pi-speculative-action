# Pi 投机执行

Pi 插件通过模型 Drafter、历史模式和可选 Actor probe 预测工具调用，在合格的隔离环境中提前执行；Actor 到达后验证并采纳结果。支持整项结果、封存输入上的新查询，以及 Linux 子进程的已完成结果和运行中接管。

## 安装与配置

```sh
pi install https://github.com/xchang1121/pi
# 或直接加载本地 checkout
pi -e /absolute/path/to/pi-speculative-action
```

插件直接加载 TypeScript 扩展，不需要构建 Pi。Node 要求见 [package.json](./package.json)，工具绑定按 Pi 0.84.1 验证；不兼容的绑定关闭相应能力，保留 Actor。

在 TUI 输入 `/speculative-action`，选择总开关、Drafter 模型、历史模式和工具，再选择 **Apply changes**。高级参数位于 **Advanced settings**；执行能力位于 **Tools & execution → Execution routes**。路线页会自检并显示各工具的预测、重放、观察和提前执行能力。

配置保存于 `<agent-dir>/speculative-action.json` 或项目的 `.pi/speculative-action.json`。项目配置覆盖全局配置，密钥通过现有模型配置或环境变量提供。

```json
{
  "enabled": true,
  "draftModel": "deepseek/deepseek-chat",
  "candidateLimit": 2,
  "drafterMaxDepth": 1,
  "tools": ["read", "ls", "write", "edit", "find", "grep", "bash"],
  "patternAware": { "enabled": true, "multiStepEnabled": true }
}
```

默认两个 Drafter 请求竞争首个有效提案，仍在运行的同伴会被取消；更大的候选宽度增加模型请求成本。一个响应中的工具批次保持完整，整批结果反馈后才续推。`drafterMaxDepth: 0` 关闭 Drafter 自身续推；收益门控默认开启。工具列表只限制预测，不改变 Actor 的正常工具权限。

调度器结合 Actor 提前量与实测成本决定是否预执行；已启动但取消的工作提供同一动作的耗时下限，避免反复低估慢任务。取消不计为成功或失败样本，排队和后续回收时间不混入该下限；仅有取消记录的候选超过下限后，按既有频率试探等待，避免反复延迟 Actor。更充足的提前量仍可准入，成功执行后更新估计。尚无投机服务样本和具体时长预测时，以已知 Actor 成本限定首次等待，不把未知时长当作 1 ms。

开启历史模式的多步预测后，PatternAware 可以接续 Drafter 已完成的根工具批次，提前绑定后续步骤的参数。整批结果仅作为假设上下文，父子预测分别结算；只有真实 Actor 批次进入历史学习。真实结果触发的下一步准备可跨越正常轮次关闭；调用方取消、下一次 Actor 决策到达或会话关闭时，尚未完成的准备失效。学习、查找和反馈共用注册的投影规则，整文件预测可匹配覆盖范围内的局部读取；实际采纳仍须内容覆盖与新鲜度证明。文本候选列表保留返回顺序；学习状态按工作目录、工具契约和投影规则隔离，不匹配的旧状态自动重新学习。联动沿用深度、并发、依赖验证和取消限制，不额外发起模型请求。

## 执行能力

| 路线 | 当前能力与条件 |
| --- | --- |
| 封存资源 | Windows/WSL 的原版 `read`、`ls`；只采集实际访问的输入，保留 Pi 输出格式 |
| 私有工作区 | Git 支持 `write`、`edit`；验新后事务提交文件效果 |
| 受控搜索 | 显式共同绑定 Actor 与投机的 `find`、`grep`，见下文 |
| Linux 进程 | 合格后端支持 Bash 整体、跨父命令子进程和运行中复用 |
| ThinkThread | 可选 Linux Profile；已知限制见下文 |

执行环境按已安装 Runtime、本地安全后备、Actor 的顺序选择。缺少某项能力只关闭依赖它的层级，不允许无隔离提前执行。Windows/WSL 的验证不能代表 macOS、ARM64 或其他后端已经验收。

### 受控搜索

搜索默认使用 **Native Pi**。在路线页选择 **Search execution → Captured search** 并 Apply 后，Actor 与投机使用同一执行器。

- `find` 使用 Pi 已有的 minimatch/ignore，保留文件名原文，采用固定的大小写敏感匹配和路径排序。
- `grep` 使用已经存在且通过探测的 rg：Windows x64 15.2.0、Linux x64 14.1.0。固定可执行字节，按路径排序，禁用环境 rg 配置和全局 Git ignore，捕获所用祖先/Git 配置。
- 这是显式搜索语义，不等同于 Native Pi 默认行为。缺少合格 rg 不影响受控 find；绑定后的失败回退仍使用同一 profile。Actor 可借用预测端的空闲工作进程，预测不会占用专留给 Actor 的空闲进程；每次调用仍独立绑定输入，取消和关闭按当前调用归属处理。
- 输入总预算 8 MiB、结果预算 1 MiB；首次捕获时可在原资源预算内保留私有搜索目录，同一路径与 glob 的新搜索词可复用筛选和目录准备，仍执行当前查询并验证原始依赖。私有目录准备只创建一次相同目录，已有原始配置不重复复制，筛选时改写的配置在执行前恢复。预算不足或准备身份不符时按需重建；目录随资源版本回收，取消单个查询不撤销其他消费者的输入。取消和关闭等待工作进程及借用的输入操作结束。这是固定可信工具执行器，不是任意 JavaScript 沙箱。

### Linux / WSL 2 Bash

提前执行需要 Git、支持 `--kill-on-exit` 的 strace，以及通过行为探测的 Sandlock/Landlock/seccomp。跨父命令接管还需要 x86-64 Linux、合格 held-exec helper、ptrace 和相应描述符能力。

需要安装后端时，先准备 Git、strace、C/Rust 工具链、make 和 tar，再显式运行：

```sh
npm run setup:linux
```

安装器构建并验证固定版本的后端，包括带捕获入口的 strace 和同源 held-exec 共享库；Runtime 自检显示持续 I/O 接管是否可用。捕获层不可用时仍保留已完成结果和运行中等待复用。更新后端补丁或提示 helper 协议不匹配时，重新运行上述安装命令。可选 fuse-overlayfs 通过完整资格后用于大型工作区，不可用时保留 Git。WSL checkout 应放在 Linux 原生文件系统中。

原生程序可以通过 CPU 指令或 ELF 启动状态取得时钟和随机输入，系统调用观察不能证明它们未被使用。当前 Linux 后端保留这些输入限制，已执行或仍在运行的计算只可转交一次；Plan 仍有有效消费者时可跨轮转交，消费者取消或过期后跨轮资格失效。没有 Plan 所有权的直接调用仍限于同轮，不发布为跨轮、跨会话重放的历史结果；旧观察契约的证书拒绝采纳。可执行绑定仍可跨轮保留，在新轮次重新预执行。

Linux 后端可在同一会话内保留真实原生执行或已封存子进程的启动绑定。PatternAware 从原生完成和真实采纳记录学习，在空闲容量中预执行已绑定的内部单元；父 Bash 独立执行。内部单元直接使用已绑定的可执行文件，父工具需要的 PATH 拦截资源按需准备。驱动选择与预热共用不可变快照及变化通知，省去准备阶段的整树捕获和复验。普通工作区分配仍核对当前内容与暂存字节；独立子进程可以借用快照，在采纳时验证实际输入与效果。独立子进程与外层共用已封存的事务前后态；OverlayFS 底层文件保留实际字节来源，封存时将内容与同一资源的身份匹配。隔离子进程由已有原生 helper 直接启动，省去中间 Node 进程，保留参数、标准流路由和信号语义。首次学习固定实际映像的文件描述符，并与原生计算并行校验；原生计算结束时校验仍未完成则取消学习，关闭句柄。绑定只用于新隔离执行，不能替代结果证书；一次性结果消费后仍可保留绑定。原始参数和环境仅驻留内存，与运行中映像共用条目数及最多 65 MiB 的驻留预算（同时受用户缓存预算限制），清理或关闭后撤销。预执行重新检查父动作权限，采纳仍验证完整执行身份和当前依赖，内部命中单列统计。加入运行中子进程前，仅在源文件收到变化提示时，先读取一个有界事务前态并比较当前内容，仅在字节不同后查阅已有跟踪记录；确认子进程访问过该路径才跳过该候选，避免等到封存后才拒绝。无变化不增加文件读取，内容相同不解码日志；目录通知、缺少前态或提示不足沿用原流程；无关变化、内容恢复或正确前驱效果不会仅因通知而被拒绝。

继承资源统一表示为进程 FD 表 → OFD → 内核对象：FD 复制、关闭、CLOEXEC 和受控进程树内传递保留引用关系；OFD 保存共享偏移、状态标志及可证明的 flock/OFD 锁，文件保存内容和硬链接别名，队列保存两端及生产者状态。普通文件、null、目录/O_PATH 和枚举游标、字节管道、内部 Unix socketpair、eventfd 共用捕获、隔离执行、一次性移交及取消回收流程。SCM_RIGHTS、批量消息和 pidfd_getfd 使用原生内核身份证明，包含消息中最后来源已关闭的句柄。文件及目录的封存资源还进入已有 ResourceReadView，供 read/ls/find/grep 查询和 write/edit/Bash 的准备与失效使用。

消费、窥读、写入、短写/错误、poll/select/内部 epoll、splice/tee、消息及控制消息、关闭和半关闭使用同一有序资源日志，验证后提交一次；提交后失败终止进程树，避免重复执行副作用。Unix datagram/seqpacket 支持提前执行中新产生的包及其边界、空包、截断和窥读。已有非空包队列、外部网络或未知持有者仍拒绝移交。日志前缀受 2 MiB 输入、每端 4096 字节／16 次写入及 1024 条操作预算限制，写入须满足实际容量。

在合格的 x86-64 Linux 上，单线程提前执行可暂停在 read/readv/recvfrom/recvmsg 或 write/writev/sendto/sendmsg，封存内存、寄存器、TLS、信号与最终 FD 表，然后通过已有事务移交到 Actor 原本的子进程。后续使用真实 PID、FD/OFD 和对端，持续接收新输入并输出；前缀的标准输出/错误仍通过既有缓冲路由交付，并保留管道与 socket 的类型、合并路由及背压。私有文件映射保留真实文件对象和每页写时复制状态；复制、关闭和重编号沿用同一资源图。映像只驻留内存、一次消费，等待输入的时间不记作复用计算收益。当前只接受原有 OFD 构成的最终 FD 表，拒绝已读取 PID/TID、未封闭线程/进程树、共享映射、未建模内核对象等状态；恢复后可创建新线程。Windows 原生 HANDLE 移交尚未实现。此版本使用 helper 协议 34、FD6 输入／FD4 输出清单和证书版本 16，旧 helper 需重新构建。

### Actor probe

`selfSpeculation` 默认关闭，只对权威 Actor 流生效。`sidecar` 需要实现 `/self-speculation/fork`、`candidates`、`clear` 的服务；`provider` 需要真正支持相应 SPORK 协议和概率证据的推理端，普通兼容 API 不因此获得自投机能力。

端点、传输方式、Actor Profile、置信度和预算在 TUI 配置。provider 控制载荷、候选及 sidecar options 显式传递 `actor_profile`。Profile 和格式覆盖必须与 Actor 实际模板和 tokenizer 一致；认证使用 `apiKeyEnv`。目标端 token 验收与工具采纳分别统计，候选注册确认不算 token 验收。

### ThinkThread Profile

准备可用的 Runtime、Pi 和 Node 后运行：

```sh
./scripts/install-thinkthread-profile.sh
cd /path/to/project
tt pi-speculative-action
```

可选入口 `./thinkthread-extension` 使用随包固定的 Agent POSIX SDK。安装器选项见 `--help`；TUI 的 Ready 仅说明连接和路线准备成功。

Linux x86_64 alpha4 的 `read/ls/edit` 有采纳验证；新文件 `write` 的异步路径解析存在 `EACCES` 差异，会拒绝候选并由 Actor 执行。写入权限/身份、嵌套进程跟踪及其他平台的完整资格仍未完成。`fs.run` 继承 Profile 网络策略，不虚拟时间或随机数；快照相等不能授权任意 Bash 或原生搜索复用。

## 复用与安全边界

- 预测键只用于检索。采纳还需执行器身份、权限、作用域、等价性、动态依赖及精确新鲜度证明；执行路由不改变动作键。
- 原版 Pi 负责参数与输出语义。候选封存的资源名称进入原缓存索引，当前工具可跨工具、跨轮次在这些输入上执行自己的语义；资源不完整时回到原生执行。当前执行器及资源边界须有证明，路径不存在的观察保留其父路径解析；可追溯到原始观察时，只验证本次使用的输入及根、路径和别名证据，否则验证完整输入。查询证明不授权原结果提交。结果封存复用已有证据，并发加入同一候选共用正在进行的验证，采纳时独立精确验新，原生观察仍须封闭执行窗口。默认不裁剪 read 结果，也不从命令文本猜测等价性。
- 自定义后端用 `inputResources: [{ path, descendants? }]` 提供封存输入的绝对名称提示，动作的 `resourceRoot` 用于解析逻辑资源名称。`path` 默认只检索该名称；只有后端能继续解析子路径时才声明 `descendants: true`，资源视图仅为已跟随的别名提供此提示。输入检索只用于当前绑定语义为观察的动作，提示不授予读取或采纳权限。`reconstruct` 默认只支持源执行器；声明 `reconstructionScope: "current_action"` 后可尝试当前动作，并返回 `{ output, compatibility, validate, capturedBytes? }`。当前执行器证明仅随查询验证能力传递，额外索引和证明存储计入原缓存预算。查询证明超预算时可保留结果并回到完整输入验证。输入检索取代旧 `RESOURCE_INPUT_ACTION_KEY_PROJECTOR`，`inputs` 采纳单列统计，不算作预测匹配或整项结果投影。
- 私有文件效果在同锁验新后提交，提交前持有完整输出与效果闭包。证明不足可干净回退一次；不确定或部分提交失败禁止重跑。
- Git 快照保留原始字节，独立管理索引、配置和属性；不继承用户 hook、过滤器或索引路径。准备阶段只记录变化通知，不建立整树内容证明；通知不能授权输入读取或结果采纳。独立子进程可借用同一工作区池的不可变准备结果，当前输入和效果仍须通过完整证明。绑定文件操作也可复用准备快照，并在提交事务中校验实际读取、访问权限和效果前态；未封闭输入的普通分叉保持整树精确验新。
- 同一会话可跨轮等待候选封存；一次性输入仅在同作用域转交，跨轮结果仍需完整复用验证。父分支与子进程不能重复消费；关闭排空执行、借用输入、封存、事务和清理。
- Linux 进程观察记录 `fcntl/flock`；无法封闭的锁、租约、共享标志等状态禁止结果复用，旧观察契约的证书不再采纳。
- metadata、watcher 事件或缓存命中不能替代内容证明。Windows 原生 Actor 观察因目录替换窗口限制而关闭，受控文件路线保留。输入读取可能影响访问时间。

程序接入使用 `./core`、`./process-reuse`、`./pattern-aware`、`./extension` 等窄入口。自定义执行环境通过 Host 的 `executionWorlds` 注册，并由所属会话调用 `host.dispose()`；自定义工具未经明确绑定不会自动获得投机资格。

直接使用核心 Runtime 时，以 `disposeSession(sessionID)` 清理单个会话，以 `dispose()` 清理全部会话。

计时使用 `TaskTimeline` 累积 Actor 阶段和已记录的 `TimelineInterval`，端点取自同一单调时钟。Runtime 回退通过 `prepared.settle(toolExecution, output)` 结算，耗时从该区间计算；重复采纳共享同一个计算区间对象。

## 计时与验证

实际端到端时延与乐观收益分开报告：`optimisticAccelerationRatio = 1 + estimatedSavingsMs / actualEndToEndMs`，多次运行先累加分子和分母。每次成功采纳以已有计算区间（含复用的子进程）与调度器已有的同操作原生耗时估计中较大者为参考，扣除本次实际等待、验证和采纳时间；跨任务缓存和重复采纳分别计入。原生父进程只补入被复用的子计算，扣除其已经包含的计算或等待区间，不把普通子进程并发算作投机收益。只遍历已有区间，不添加计时后端、历史库或额外执行。

TUI 页脚和状态明细显示 `End-to-End SpeedUp +12.1%`，百分比为 `(optimisticAccelerationRatio - 1) × 100%`。页脚按累计已完成任务的总时间计算，并显示实际端到端时延；尚无已完成任务或旧记录缺少数据时显示 `n/a`。

这是乐观估计，不是关闭投机后的实测节省，也不保证是严格上界：历史样本、来源计算与当前原生执行可能有差异。封存输入上的重算只用当前查询或同操作原生历史，不计来源工具未使用的计算；两者均无证据时不猜测预热收益。原有 `hiddenLatencyMs` 和 `serializedCounterfactualMs / actualEndToEndMs` 保留用于比较计算重叠；后者按计算身份去重、不计前一任务的计算，不能覆盖全部复用收益。新旧口径不可混作性能提升。

采纳成本测量完整 `Host.execute` 返回；准备、争用与回收计入任务总时长。原生/Host、独立开关或相邻版本对照用于成本归因，不能代入主加速比。组件或 mock 时序改善不代表自然任务收益。

```sh
npm ci
npm run check
npm run build
npm test -- --maxWorkers=1 --no-file-parallelism
npm run bench:check
```

搜索、Linux 进程资格和模型套件的命令见 [验证说明](./bench/README.md)。
