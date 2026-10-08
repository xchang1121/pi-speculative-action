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
  "drafterMaxDepth": 3,
  "tools": ["read", "ls", "write", "edit", "find", "grep", "bash"],
  "patternAware": {
    "enabled": true,
    "multiStepEnabled": true,
    "presets": ["reported-files", "edited-file", "recent-reads", "recent-command"]
  }
}
```

在 **Prediction sources → Learned patterns → Prebuilt modes** 可独立选择十一种预建模式。原有四种默认开启，其余七种默认关闭；旧配置未写 `presets` 时仍选择原有四种，已有数组则保留其明确选择。选择后回到主菜单 **Apply changes** 保存，取消未应用的修改不会改变配置。**Restore defaults** 重置调优参数时保留模式选择。

| 模式 | 配置 ID | 默认 | 预测依据 |
| --- | --- | --- | --- |
| Reported files | `reported-files` | 开 | 最新工具结果中报告、尚未读取的文件 |
| Edited files | `edited-file` | 开 | Actor 刚编辑或写入的文件 |
| Recent reads | `recent-reads` | 开 | 最近读过、可能再次读取的文件 |
| Recent command | `recent-command` | 开 | 编辑后，针对 Actor 实际执行过的命令准备已失效的原生工作单元 |
| Reported lines | `reported-lines` | 关 | 工具结果实际报告的文件与行号附近的读取窗口 |
| Companion files | `companion-files` | 关 | 已观察到的同名源码与测试文件配对，不猜测新路径 |
| Edits after failure | `failure-edits` | 关 | 失败后读取最近成功编辑、且编辑后尚未重新读取的文件 |
| Continue reading | `continue-read` | 关 | 读取结果明确记录可继续的截断位置时，准备下一段内容 |
| Retry failed command | `retry-failed-command` | 关 | 真实失败的 Bash 命令报告的文件被成功编辑后，在没有合格原生准备时原样准备该命令 |
| Recheck search | `recheck-search` | 关 | 成功修改 grep 曾报告的文件后，原样准备最近一次相关搜索；每批最多一次 |
| Result neighbors | `result-neighbors` | 关 | Actor 读取先前 find/grep 报告的文件后，按原返回顺序准备同批尚未读过的最多两个文件 |

读取模式的先验概率按真实预测的匹配和采纳反馈校正。各预建模式的排序还结合实际消费的毛计算收益与已启动工作的生产耗时；失败、取消的已启动工作计入成本，取消沿用截至中止请求的执行耗时，不含后续清理。共享执行只归属首次生产者，部分复用、迟到消费和独立再次消费仍反馈原生产模式。冷样本保留先验与有限探测机会，反馈不会自动开启模式或跳过收益、资源和采纳门控，也不改变毛收益统计口径。这些模式可用于提前准备可能的后续工作；实际收益取决于匹配、资源占用和验证结果。失败命令模式只使用实际执行过的原始命令与参数，不改写 shell 命令。

显式设置 `"presets": []` 只关闭预建模式，已学习的模式和历史仍保留并继续使用；`patternAware.enabled: false` 关闭整个 PatternAware 预测来源，同时保留模式选择。新选择随下一轮预测配置生效，已启动的工作沿用原有生命周期。所有模式沿用原有收益、资源预算、隔离与采纳验证。

默认最多两个 Drafter 请求竞争首个有效提案，取消仍在运行的同伴。Drafter 可在一次响应中预测已知参数的有序工作流程，在私有工作区中依次提前执行；需要未知工具结果时停下，取得真实结果后再续推。`drafterMaxDepth` 默认 3，设为 0 时只预测当前批次。普通工具批次保持完整，整批结果反馈后才续推。宽度与深度均为上限，收益门控会收缩无效请求并保留低频恢复探测；工具列表只限制预测，不改变 Actor 权限。

Drafter 每个用户任务默认最多 `drafterTaskMaxRequests: 32` 次请求、`drafterTaskMaxTokens: 262144` 个输入与输出 token；根预测、续推和 Drafter 模式的 Actor 探测共用预算。发请求前预留完整上下文与输出额度，依据预期节省毫秒及实际采纳收益/token 分配预算，为已观测到昂贵工具服务的后续阶段保留一次探索机会。未知收益只作有限探索，服务失败共用退避；历史提示需由实际收益校正。返回 usage 后按实际量结算，缺失 usage 的失败或取消仍计入预留额，迟到采纳修正原样本。估算不是账单硬上限；最后一笔可缩小输出额度。只有任务结束（Pi `agent_settled`）后才重新计额，历史学习保留。TUI 可配置上限并查看已报告、缺失 usage 和在途预留。

模型与工具契约分别维护最近 32 个可观察预测的匹配记录，以及最近 32 个已匹配预测的采纳记录，使用 Beta(1, 1) 先验。滑出窗口的旧记录不再影响概率；未观察预测和主动校准回退分别不进入匹配统计和采纳统计。

执行容量按资源单位计算：进程隔离和递归目录扫描默认占两单位，普通文件快照占一单位；默认权重不超过总容量，来源显式提供的 `resourceDemand` 保留原值。Actor 回退执行期间持续预留容量，并发 Actor 分别计入；被取消的投机任务在执行器完成清理后才释放容量。Actor 不等待投机回收，当前调用可在原生路径接管的内部计算继续运行，其余新预测受剩余容量限制。

未知收益保留为未知；默认四次观察后改为低频探测，连续未知或亏损时逐步延长探测间隔。同一动作连续四次采纳后仍缺少 Actor 服务样本时，下一次正常 Actor 执行用于校准，随后恢复按实测成本决策；校准回退单独标记，不重复提交投机效果。

调度器结合 Actor 提前量与实测成本决定是否预执行；已启动但取消的工作提供同一动作的耗时下限，避免反复低估慢任务。取消不计为成功或失败样本，排队和后续回收时间不混入该下限；仅有取消记录的候选超过下限后，按既有频率试探等待，避免反复延迟 Actor。更充足的提前量仍可准入，成功执行后更新估计。同一动作有 Actor 服务样本时，以该成本限定未知投机时长的首次等待；只有异质动作类别样本且没有具体预测时，使用有界预热探测。整工具和内部操作的服务样本按执行契约分开。

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
- 输入总预算 8 MiB、结果预算 1 MiB。目录/glob 筛选与文件匹配共用原资源准备缓存，分别记录依赖；匹配契约固定执行器、源文件、pattern、ignoreCase 和 literal，允许不同 context、limit、glob 和搜索路径共享同一文件的完整匹配，仍由原版 Pi 处理上下文、截断和格式。一次只将缺失文件交给同一批 rg 计算，原生匹配输出上限 8 MiB，中间结果计入原缓存预算。
- 精确验新发现内容变化时撤销对应输入和计算，保留未变文件的匹配；文件增删或忽略规则变化只重做受影响的筛选和匹配。组合证明保留非拥有的失效引用，能够撤销实际借用来源的过期输入，不延长其生命周期。失效保留独立有效的类型与路径信息，write/edit 后仍能找到未变输入。新计算由当前查询持有，原准备缓存可继续借用，所属查询回收时撤销借用入口。有独立准备存活时，当前观察调用最多重建一次并重新验新，失败或再次变化回退。增量粒度为文件，不推断任意正则局部等价或 Bash 参数等价，K(a) 完全等价路线保留。私有目录与中间结果共用预算、借用、取消和回收流程；取消和关闭等待工作进程及输入操作结束。这是固定可信工具执行器，不是任意 JavaScript 沙箱。

### Linux / WSL 2 Bash

提前执行需要 Git、支持 `--kill-on-exit` 的 strace，以及通过行为探测的 Sandlock/Landlock/seccomp。跨父命令接管还需要 x86-64 Linux、合格 held-exec helper、ptrace 和相应描述符能力。

在 Linux / WSL 2 的 TUI 打开 `/speculative-action → Tools & execution → Check Linux environment`，可检查发行版（包括 openEuler）、架构、工具链、静态 libc、已安装 helper、FUSE 权限及内核策略线索，并刷新已启用后端的运行时资格检查。工具已安装、能启动与通过运行时检查分别报告。检查不会安装系统软件或更改权限；openEuler 报告提供 DNF 查询指导。

需要安装后端时，先准备 Git、C/Rust 工具链、make、tar 和 xz。在同一菜单选择 **Install / update Linux dependencies**，安装前显示环境摘要，确认后显示实时安装输出；取消会停止安装进程，已安装组件保留。结束后刷新能力检测，不改投机配置；重启 Pi 可加载更新后的 helper 并重新检测工作区驱动。系统依赖及 FUSE 的 `fusermount`、`/dev/fuse` 权限仍需自行准备，部分组件不可用会明确提示。也可在插件目录运行：

```sh
npm run setup:linux
```

安装器构建并验证固定版本的后端，包括带捕获入口的 strace 和同源 held-exec 共享库；Runtime 自检显示持续 I/O 接管是否可用。捕获层不可用时仍保留已完成结果和运行中等待复用。更新后端补丁或提示 helper 协议不匹配时，重新运行上述安装命令。可选 fuse-overlayfs 通过完整资格后用于大型工作区，不可用时保留 Git。WSL checkout 应放在 Linux 原生文件系统中。

原生程序可以通过 CPU 指令或 ELF 启动状态取得时钟和随机输入，系统调用观察不能证明它们未被使用。当前 Linux 后端保留这些输入限制，已执行或仍在运行的计算只可转交一次；Plan 仍有有效消费者时可跨轮转交，消费者取消或过期后跨轮资格失效。没有 Plan 所有权的直接调用仍限于同轮，不发布为跨轮、跨会话重放的历史结果；旧观察契约的证书拒绝采纳。可执行绑定仍可跨轮保留，在新轮次重新预执行。

Linux 后端保留同一会话中已学到的子进程启动绑定，PatternAware 按实际输入、采纳收益和空闲容量提前执行。Actor 修改工作区后，合格且无需旧输入句柄的子进程可直接针对当前文件准备；依赖父流程或实时输入的工作保持原有顺序。父 Bash 可以改变包装命令，子进程仍须执行身份、参数、环境、描述符和当前依赖全部匹配才能复用。父步骤已提交时，有效私有后续成果仍可保留；外部修改、取消和回收继续按原有证明与所有权处理。

`recent-command` 也响应共享文件监听器记录的工作区变化，因此 Bash 写文件后可触发昂贵子进程准备。仅缺少旧证书或执行未修改文件的 Bash 不会单独触发；只探测少量已观察、可针对当前工作区执行的昂贵绑定，仍逐项验新。`.git` 变化和不确定的监听记录不作为这一路的触发依据。

如果一个仅依赖当前工作区的原生启动绑定已完成独立准备，没有派生子请求，且封存证明无法支持结果复用，该绑定停止重复准备。合法的一次性结果、运行中续执行、可复用子结果及需要后续输入的子启动仍保留；落盘失败本身不触发停用。相同启动上下文的再次观察或普通文件编辑不会清除这条负反馈；上下文改变、原有保留记录被回收或会话结束后可重新学习。Actor 正常执行和已有结果的采纳不受影响。

绑定只允许新的隔离执行，不能代替结果证书。内部单元直接使用已绑定的可执行文件；权限重新检查，完整输入和效果在采纳时验新，内部命中单列统计。一次性结果消费后仍可保留绑定；原始参数和环境只驻留内存，与运行中映像共用条目数及最多 65 MiB 的预算（同时受用户缓存预算限制），清理或关闭时撤销。工作区快照、封存输入和事务复用已有资源管理；内容变化仍须复核，通知本身不能证明输入有效或无效。自动路由拒绝逃出工作区的目录链接，依赖目录应位于工作区内。

继承资源统一表示为进程 FD 表 → OFD → 内核对象：FD 复制、关闭、CLOEXEC 和受控进程树内传递保留引用关系；OFD 保存共享偏移、状态标志及可证明的 flock/OFD 锁，文件保存内容和硬链接别名，队列保存两端及生产者状态。普通文件、null、目录/O_PATH 和枚举游标、字节管道、内部 Unix socketpair、eventfd 共用捕获、隔离执行、一次性移交及取消回收流程。SCM_RIGHTS、批量消息和 pidfd_getfd 使用原生内核身份证明，包含消息中最后来源已关闭的句柄。文件及目录的封存资源还进入已有 ResourceReadView，供 read/ls/find/grep 查询和 write/edit/Bash 的准备与失效使用。

消费、窥读、写入、短写/错误、poll/select/内部 epoll、splice/tee、消息及控制消息、关闭和半关闭使用同一有序资源日志，验证后提交一次；提交后失败终止进程树，避免重复执行副作用。Unix datagram/seqpacket 支持提前执行中新产生的包及其边界、空包、截断和窥读。已有非空包队列、外部网络或未知持有者仍拒绝移交。日志前缀受 2 MiB 输入、每端 4096 字节／16 次写入及 1024 条操作预算限制，写入须满足实际容量。

在合格的 x86-64 Linux 上，单线程提前执行可暂停在 read/readv/recvfrom/recvmsg 或 write/writev/sendto/sendmsg，封存内存、寄存器、TLS、信号与最终 FD 表，然后通过已有事务移交到 Actor 原本的子进程。后续使用真实 PID、FD/OFD 和对端，持续接收新输入并输出；前缀的标准输出/错误仍通过既有缓冲路由交付，并保留管道与 socket 的类型、合并路由及背压。私有文件映射保留真实文件对象和每页写时复制状态；复制、关闭和重编号沿用同一资源图。映像只驻留内存、一次消费，等待输入的时间不记作复用计算收益。当前只接受原有 OFD 构成的最终 FD 表，拒绝已读取 PID/TID、未封闭线程/进程树、共享映射、未建模内核对象等状态；恢复后可创建新线程。Windows 原生 HANDLE 移交尚未实现。内部协议和缓存只维护当前数据结构，不设版本号或迁移分支。更新原生实现后运行 `npm run setup:linux`，安装器按源码和二进制摘要重新构建及验证；不匹配的缓存重新学习。

### Actor probe

`selfSpeculation` 默认关闭，只对权威 Actor 流生效。`sidecar` 需要实现 `/self-speculation/fork`、`candidates`、`clear` 的服务，可选的 `capabilities` 以 `{ "capabilities": { "fork", "candidates", "logprobs", "provider" } }` 声明所服务的请求（声明后未列为 true 的不再发出；尚未对照真实 vLLM sidecar 验证）；`provider` 需要真正支持相应 SPORK 协议和概率证据的推理端，普通兼容 API 不因此获得自投机能力；`drafter` 不需要任何服务：在同样的 D1/D2 探测点把 Actor 已输出的推理交给 Drafter 模型（关闭思考、强制工具调用），由它给出 Actor 接下来的调用，无概率证据，不注册候选；只有 Actor 与 `endpoint` 同源时才在其请求中加入 `request_id` 等字段，托管 API 的请求保持原样。

端点、传输方式、Actor Profile、置信度和预算在 TUI 配置。provider 控制载荷、候选及 sidecar options 显式传递 `actor_profile`。Profile 和格式覆盖必须与 Actor 实际模板和 tokenizer 一致；认证使用 `apiKeyEnv`。目标端 token 验收与工具采纳分别统计，候选注册确认不算 token 验收。

### ThinkThread Profile

准备可用的 Runtime、Pi 和 Node 后运行：

```sh
./scripts/install-thinkthread-profile.sh
cd /path/to/project
tt pi-speculative-action
```

可选入口 `./thinkthread-extension` 使用随包固定的 Agent POSIX SDK。安装器选项见 `--help`；TUI 的 Ready 仅说明连接和路线准备成功。

该 Profile 面向 Linux x86-64，为 `read/ls/edit/write` 提供隔离执行；默认配置的原生 `bash/grep/find` 回退到 Actor。Runtime 以 ptrace 监管 Agent，不能嵌套使用进程后端或 held-exec；`fs.run` 不虚拟时间或随机数，快照相等不足以授权任意 Bash 或原生搜索复用。WSL 中应让 Linux 的 `pi` 先于 Windows PATH 被找到。

## 复用与安全边界

- 普通绑定文件编辑和写入先使用内存事务，不预热整树 Git 快照；硬链接、后续分支等确实需要私有工作区时再建立快照。显式关闭内存事务的执行环境保留预热。
- 预测键只用于检索。采纳还需执行器身份、权限、作用域、等价性、动态依赖及精确新鲜度证明；执行路由不改变动作键。
- 原版 Pi 负责参数与输出语义。候选封存的资源名称进入原缓存索引，当前工具可跨工具、跨轮次在这些输入上执行自己的语义；资源不完整时回到原生执行。当前执行器及资源边界须有证明，路径不存在的观察保留其父路径解析；可追溯到原始观察时，只验证本次使用的输入及根、路径和别名证据，否则验证完整输入。查询证明不授权原结果提交。结果封存复用已有证据，并发加入同一候选共用正在进行的验证，采纳时独立精确验新，原生观察仍须封闭执行窗口。默认不裁剪 read 结果，也不从命令文本猜测等价性。
- 自定义后端用 `inputResources: [{ path, descendants? }]` 提供封存输入的绝对名称提示，动作的 `resourceRoot` 用于解析逻辑资源名称。`path` 默认只检索该名称；只有后端能继续解析子路径时才声明 `descendants: true`，资源视图仅为已跟随的别名提供此提示。输入检索只用于当前绑定语义为观察的动作，提示不授予读取或采纳权限。`reconstruct` 默认只支持源执行器；声明 `reconstructionScope: "current_action"` 后可尝试当前动作，并返回 `{ output, compatibility, validate, capturedBytes? }`。当前执行器证明仅随查询验证能力传递，额外索引和证明存储计入原缓存预算。查询证明超预算时可保留结果并回到完整输入验证。输入检索取代旧 `RESOURCE_INPUT_ACTION_KEY_PROJECTOR`，`inputs` 采纳单列统计，不算作预测匹配或整项结果投影。
- 私有文件效果在同锁验新后提交，提交前持有完整输出与效果闭包。证明不足可干净回退一次；不确定或部分提交失败禁止重跑。
- Git 快照保留原始字节，独立管理索引、配置和属性；不继承用户 hook、过滤器或索引路径。准备阶段只记录变化通知，不建立整树内容证明；通知不能授权输入读取或结果采纳。独立子进程可借用同一工作区池的不可变准备结果，当前输入和效果仍须通过完整证明。绑定文件操作也可复用准备快照，并在提交事务中校验实际读取、访问权限和效果前态；未封闭输入的普通分叉保持整树精确验新。
- 同一会话可跨轮等待候选封存；一次性输入仅在同作用域转交，跨轮结果仍需完整复用验证。父分支与子进程不能重复消费；关闭排空执行、借用输入、封存、事务和清理。
- Linux 进程观察记录 `fcntl/flock`：不等待取得或查得空闲的锁成为依赖，采纳时按 `/proc/locks` 确认无人持有；被拒的锁、租约、共享标志等无法封闭的状态禁止结果复用。重放恢复文件的纳秒级修改时间，未知程序看到的 inode、ctime 等物化身份记为一次性观察。整条命令以它开始时的工作区为依据：子进程看到的若是本命令先前写下的内容（临时文件、构建中途的目录列表），改记该路径在命令开始时的状态；写入区间与兄弟进程重叠、效果无法单独归属的子进程，其观察并入整条命令。旧观察契约的证书不再采纳。
- metadata、watcher 事件或缓存命中不能替代内容证明。Windows 原生 Actor 观察因目录替换窗口限制而关闭，受控文件路线保留。输入读取可能影响访问时间。

根入口只提供 Pi Host API；程序接入按需使用 `./core`、`./process-reuse`、`./pattern-aware`、`./extension` 等窄入口。自定义执行环境通过 Host 的 `executionWorlds` 注册，并由所属会话调用 `host.dispose()`；自定义工具未经明确绑定不会自动获得投机资格。

直接使用核心 Runtime 时，以 `disposeSession(sessionID)` 清理单个会话，以 `dispose()` 清理全部会话。

计时使用 `TaskTimeline` 汇总每次已结算 Actor 调用实际消费的计算凭据，区间保留所属的单调时钟标识，避免进程重启后的坐标碰撞。Runtime 回退通过 `prepared.settle(toolExecution, output)` 结算；准备工作、子计算及其复用关系由已记录的 `TimelineInterval` 关联。

## 计时与验证

工具加速比为 `toolSpeedup = (E + R) / E`。`E = actorComputeMs` 是 Actor 当前调用仍需完成的实际计算耗时，不包含验证、采纳和交付；`R = reusedExecutionMs` 是当前调用实际消费的复用计算在首次执行时记录的毛耗时。多次任务先分别累加完整测量样本的 E 和 R，再计算比值。

TUI 主显 `gross tool time saved`（`reusedExecutionMs`）：每次实际复用计入原始计算的完整已记录耗时，不扣除验证、采纳和交付成本。整条结果、实际消费的 Bash 子进程、文件或目录输入及相关准备工作均可贡献；同一调用内部共享计算按身份去重，属于同次原始执行的父子区间按并集计算，不把未消费的兄弟进程计入。同一结果被独立的后续调用再次复用，会再次记账。

计算来源可随证书跨轮保留，包含已记录的准备、子进程及开销排除区间。历史证书只有未分离开销的进程耗时而没有计算来源时，不把该标量当作计算收益；保留其他可证明的贡献，并用 `reusedExecutionIncomplete` 标记“已知收益下界”。

记录完整、`E = 0` 且 `R > 0` 时显示完全复用（`fullyReused`），不输出有限加速比；E 未知、复用记录不完整或 E、R 均为零时显示 `n/a`。无法区分计算与采纳成本的样本不进入主要加速比汇总，但保留已有收益下界及诊断数据。`toolWaitMs` 仅用于诊断：它是 Actor 从工具调用开始到返回的实际等待区间并集，包含准备、验证、采纳、回退和结算，不包含模型思考时间，也不作为计算加速比的分母。

独立开关、原生/Host 和相邻版本对照用于成本归因；组件或 mock 时序改善不代表自然任务收益。

```sh
npm ci
npm run check
npm run build
npm test -- --maxWorkers=1 --no-file-parallelism
npm run bench:check
```

搜索、Linux 进程资格和模型套件的命令见 [验证说明](./bench/README.md)。
