# Pi 投机执行

Pi 扩展：在 Actor（执行任务的模型）正式调用工具前预测动作、提前执行，调用到达后验证并采纳已有计算。模型、历史模式和 Actor 探测共用计划与资源调度；实际输入、效果和所有权决定复用。

## 使用

```sh
pi install git:github.com/xchang1121/pi-speculative-action@speculative-action
pi
```

本项目采用纯扩展部署，使用未修改的 Pi；开发依赖和兼容验证基线为原版 `0.84.1`。也可用 `pi -e /absolute/path/to/pi-speculative-action` 加载本地 checkout。

安装或加载本地 checkout 后，运行 `/speculative-action`，开启投机并选择预测来源，点击 **Apply changes**。在 **Tools & execution → Execution routes** 检查 Replay、Observe、Fork 能力。全局配置为 `<agent-dir>/speculative-action.json`，项目的 `.pi/speculative-action.json` 可覆盖它；版本与依赖见 [package.json](./package.json)。

恢复工作针对扩展自身的计算和资源：持久数据经完整性、当前输入与实现绑定验证后，由后续工具调用采用。当前已验证的输入版本和纯子计算范围见下文；自有检查点的生产恢复仍在完善。**不支持宿主级透明续跑**，包括原 Pi 会话中断工具批次的原样续接、覆盖其他扩展全部回调的运行接管，以及原工具最终消息的完整跨重启交付协议。未知副作用不会自动重做；任意原生进程、多线程和断电恢复也尚未得到证明。

`drafterMaxDepth` 表示根批次之后最多预测多少个后继批次，0 关闭续推。动作和观测预测共用 Drafter 模型、凭据、任务请求数及 token 预算；并发根候选和 Drafter Actor 探测也使用同一账本，重试与压缩续跑不重置额度。`tools` 只筛选预测候选：省略时使用当前全部工具，空数组关闭工具候选；它不限制已观察输入和子计算的复用。

`/speculative-action` 主菜单提供两个开关：**Speculative production**（`enabled`，默认关闭）控制额外产物的提前生产，包括调用预测、流式预览和已观察操作的准备；**Cache & reuse**（`reuseEnabled`，默认开启）控制工具、子计算和资源的缓存与采纳。只开后者时，直接执行也会积累可复用成果。关闭复用后，调用直接执行，停止新增准备与缓存；既有持久记录可通过存储菜单单独清理，已进入提交阶段的调用正常完成。两个开关都不关闭实际调用的观察与模式学习。各预测来源的开关只停止该来源的新预测，不清空成果，也不切断已认领调用的反馈。`prepareObservedOperations` 单独选择是否准备已观察操作；会话释放由 `host.dispose()` 负责。

Linux / WSL 2 可从菜单安装 Bash 后端，或运行 `npm run setup:linux`；WSL checkout 应位于 Linux 原生文件系统。进程后端自动选择通过检查的 OverlayFS，否则回退 Git，并在采纳前核对输入和原始对象身份。OverlayFS 从校验后的固定源代码及随包补丁构建，需要 C 编译器、make、autoconf、automake、libtool、pkg-config 和包含静态库的 libfuse3 开发包；补丁在挂载前将工作区内普通文件的硬链接组复制为同一私有对象，保留跨目录别名、重命名及持有句柄的链接数；复制硬链接时不更新源文件访问时间，二进制内容进入执行指纹。

Linux 文件资源捕获会先初始化并检查原生读取桥接，再使用独立的只读根视图，统一文件内容、符号链接、目录名称和监听扫描，避免捕获改变源访问时间。已验证闭包为 x86-64 Linux 的 ext4 根挂载及可表示为 UTF-8 的路径；直接或经符号链接跨入其他挂载会在读取前被拒绝。升级后须重建 Linux 后端。该视图保留对象身份，但不冻结外部写入；它不扩展原生私有目录的 statx 等价、活进程恢复或并发原子性保证。独立文件工具也执行同一初始化；桥接不可用或过期时不会退回普通源读取。自定义进程后端的 helper 路径同时用于资源捕获。工作区的源基线导入、元数据扫描、live-lower 输入和捕获时的源路径校验也使用同一个只读适配器；结构扫描复用稳定条目捕获。依赖证书的源文件、目录、缺失、符号链接和指定元数据字段的捕获与验证也复用这个适配器；私有工作区的读取视图由当前执行世界显式提供，不从持久证书恢复权限。内存变更提案与宿主采用预检也复用该源视图；合格 Linux 源上的前态、目录、别名和权限证明通过后，才建立暂存文件或打开可写句柄。私有挂载及其他平台保留原有句柄捕获顺序，仍核对完整身份；预检通过后发生的失败不保证恢复全部动态元数据。该范围包含同一 ext4 挂载内的祖先符号链接与私有执行失败清理，仍不保证私有挂载的物化或原生 statx 全部语义。

Windows x64 的私有工作区使用随包提供的 NTFS 对象租约：保留对象身份时仍允许父目录重命名，不额外占用硬链接名。资格范围是普通用户可按文件 ID 打开的本地 NTFS 文件和目录，不含重解析点、网络路径、其他架构或文件系统。租约只保活对象，不冻结内容，也不作为跨进程恢复权限。构建者可在 Linux/WSL 使用既有 MinGW-w64 和 Node N-API 头文件运行 `PI_SPEC_WINDOWS_TOOLCHAIN=/path/to/toolchain/usr npm run build:windows`；源码、构建脚本、二进制和构建摘要均随包交付，使用者无需编译或安装系统组件。

原生子进程交接须保留程序自身的提权语义。普通 Actor 通过 `PI_SPEC_HELD_EXEC_OBSERVER` 指向管理员部署、仅具有 `CAP_SYS_PTRACE` 的专用 `.observer`；已继承 `no_new_privs` 的 Actor 可使用无附加权限的观察器。安装探测只在一次性进程设置此限制。资格不足时，原生执行、缓存回放和投机生产各自保持可用；通用 helper 与映像库始终不附加权限。

普通用户运行 `npm run setup:linux` 后，可由管理员部署刚编译的专用观察器：

```sh
observer=/usr/local/libexec/pi-speculative-action/pi-speculative-held-exec.observer
sudo install -d -o root -g root -m 0755 /usr/local/libexec/pi-speculative-action
sudo install -o root -g "$(id -gn)" -m 0750 "$HOME/.local/bin/pi-speculative-held-exec.observer" "$observer"
sudo setcap cap_sys_ptrace=ep "$observer"
export PI_SPEC_HELD_EXEC_OBSERVER="$observer"
```

将最后一行改为完整路径后写入 shell 启动文件，重开终端并重启 Pi，再检查 **Execution routes → Observe**。更新 helper 后也要重新部署观察器。WSL 2 可通过 `wsl -d <发行版> -u root` 执行管理员步骤，此时源路径和组名须填写普通用户的值；编译、Pi 和测试仍使用普通用户。无需修改系统 `ptrace_scope` 或给整个 Pi 进程设置 `no_new_privs`。

ThinkThread 使用 [Profile 安装脚本](./scripts/install-thinkthread-profile.sh)，随后在项目中运行 `tt pi-speculative-action`。

## 预测与执行

[模型 Drafter](./src/drafter-plan-source.ts)、[PatternAware](./src/pattern-plan-source.ts) 和 [Actor 探测](./src/actor-fork-plan-source.ts) 生成统一 `PlanProposal`。[Runtime](./src/runtime-engine.ts) 按 Actor 决策进度、后端给出的资源依赖及实时资源安排准备和执行，各会话共用预算，Actor 优先。PatternAware 的模式、预设、按工具契约校准和原生工具名输出用于提出候选；同批晚到结果更新根预测时，保留未变动作的已有后继及匹配机会，根动作改变或移除则撤销相关后继。Drafter 与 PatternAware 的采纳学习共用结算规则，按实际消费及生产所有权记账，跨工具或无动作键的消费均可贡献证据，但不伪造精确匹配；原生回退在消费或放弃后反馈，已声明的匹配机会跨晚到替换保持有效。预测元数据刷新不丢失记录，完全重算和仅借用该成果的候选不冒领信用。`prepareObservedOperations` 独立控制[已观察操作的提前准备](./src/execution-preparation.ts)，默认开启；旧 `recent-command` 预设并入这一执行选项，不再受 PatternAware 开关控制。内部工作保留父工具调用和绑定权限，与预测共用物理资源账本。

Drafter 参照 [TomasuLLM](https://arxiv.org/html/2609.38201v2) 将动作预测与观测预测分开：多调用批次先选择后继各自需要的观测范围，再让分支只读取范围内的真实或预测结果；联合分支登记全部输入依赖，范围无效时保守使用整批。单调用无需选择范围。分支独立续推和取消，任一真实结果到达即可替换对应预测并重推受影响后继，其余分支保留，相同观测不重复请求。范围选择、观测和动作预测共用任务预算；请求失败或迟到也可由真实结果接管。独立后继可先执行，资源依赖沿私有检查点推进；Actor 历史始终只接收真实结果。

观测请求携带实际工具定义、调用参数、绑定的观测格式，以及本批 Drafter 输出和本分支已知观测的固定快照，通过严格校验后才进入私有续推；范围外结果在上下文中固定遮蔽。范围外调用仍保留资源执行关系，由后端判断是否等待其写入并继承检查点，不把其输出变成预测输入。默认精确比较 `content`、`isError` 和 `exitCode`。工具可在 `ToolInvocation.observation` 显式提供 `version`、内容 schema 和纯函数 `canonicalize`；规则改变须更新版本，版本或 schema 改变会使原观测依赖失效。规范化仅用于预测上下文与比较，不改写 Actor 输出或放宽执行证明。各分支只预测仍缺失的观测；未提供论文的多候选评分与独立 OBS 模型，也不把缓存状态摘要当作当前环境证据。

预测血缘与复用证明分开：预测失配不使已经独立校验的真实输入或子计算失效。正式调用可采用完整结果、契约允许的投影，或在封存输入上按当前参数重算；流式预览也走同一验新流程。执行器、权限、作用域、实际依赖及记录须相容，效果只提交一次；部分提交或效果状态不明会终止本次调用。命中的在途工作等待完成，支持调用方取消。

根入口提供 [Pi Host API](./src/index.ts)；[`./core`](./src/core.ts) 提供 Runtime，[`./process-reuse`](./src/process-reuse.ts) 提供进程证明与存储。新增来源实现 `SpeculativePlanSource`，只提出计划或续推，从 `definitions` / 宿主数据获取当前工具。观察、归因与学习通过 `runtime.subscribe()` 订阅 `actor_observed`、`prediction_feedback`、`execution_settled` 等类型化事件，返回函数用于退订；订阅异常相互隔离，学习队列在 Actor 结算之外按序排空。来源标识用于反馈与诊断，缓存按实际证明、使用情况和资源预算保留成果。观察者需要提出计划时显式调用 `submitPlan(event.startInput, update)`，运行时重新检查来源开关和轮次身份，拒绝过期提案。新增后端实现 `ExecutionWorld`，通过 `executionWorlds` 注册。结束时调用 `host.dispose()`。

模块内部按所有权引用契约：哈希与文件系统证据不依赖进程证书，资源视图拥有文件操作契约，核心网关不引用 Pi 的调用类型，异步等待不依赖调度器。预测源交换调用事实而不引用另一来源的存储类型；宿主明确传入模型服务，Pattern 提示由宿主内部连接。`SpeculativeCandidate` 只暴露调用事实与可选 `executionMs`，不暴露内部工作、预留或状态机层级。原生线程的通信与连接生命周期由 [native-owner](./src/native-owner.ts) 管理，[native-worker](./src/native-worker.ts) 组合工作区与进程后端，各自的业务协议留在所属模块；这些内部模块不作为包入口导出。

| 后端 | 方法与边界 |
| --- | --- |
| [封存资源](./src/agent-execution-world.ts) / [输入版本](./src/resource-version.ts) | 捕获实际文件输入、保留内容与路径证明，消费前精确验新。受控 `find/grep` 需显式启用 [统一工具绑定](./src/pi-tool-invocation.ts)。 |
| [私有工作区](./src/workspace-sandbox.ts) / [文件事务](./src/workspace-transaction.ts) | 普通写入先在内存事务执行；硬链接与依赖链使用私有工作区。读取也能沿检查点执行，提交只包含本次操作的效果。 |
| [Linux 进程](./src/linux-process-world.ts) | Sandlock 隔离、strace 记录依赖，证书验新；合格的 x86-64 Linux 可接管已完成计算或转移单线程进程继续后缀。时钟、随机或进程身份等观测按一次性交接处理。 |
| [ThinkThread](./src/thinkthread/execution-world.ts) | 固定 SDK 的快照与 `fs.run` 接入分支协议，文件和目录输入进入公共复用层。默认覆盖 `read/ls/write/edit`；其余调用保留 Actor 原生执行。 |

自定义文件工具可用 `bindResourceOperation(execute, root)`，或提供 `ToolInvocation.filesystem`、`captureInputs`、`ProcessExecutionCoordinator` 绑定。`ExecutionWorld.observeOperations` 可独立观察没有整工具动作键的调用。共享输入和纯子计算按实际依赖查找，工具名称与可选路径提示不构成复用权限。`ToolFilesystemOperations.prepare` 的绑定及计算键须覆盖其余输入，构建器只通过给定视图读依赖，共享值保持不可变。

`inputsOnly` 分支只持有输入及证明，不复制父工具输出，也不授予整工具采纳权限。同一会话的并发调用可在父工具结束前查找已封存的输入和子计算；文件读取、目录列表、存在性和元数据查询共用逐一验新的入口，通过后才登记依赖；过期或撤销的来源不阻止查找其他来源，也不贡献消费收据。元数据保留原生字段与链接语义，每个字段须有对应证明，权限检查仍执行实际访问。消费者在执行前接管已验证的证明，借用结束前保留资源。读取返回独立副本，写后再读重新验证；消费工具执行自己的格式化与剩余操作。已独立完成的输入或纯子计算可在父工具报错、单次取消、工具退出可用集合后保留；失败消费不计复用时长，也不重放效果。预算不足或超出共享根时沿用原生操作。

Pi SDK 回调及接入网关的调用可观察 `spawn/execFile/exec/fork` 及其同步 API。同步交接由工作线程决策，期间不运行调用者的异步回调，也不能加入依赖被阻塞主线程的生产者。同步与异步调用可共享捕获的文件、目录输入；命名路径可重捕获当前资源图，匿名对象、外部队列与投机前置状态保留原生产者约束。第三方函数须先接入网关。

结果、输入和句柄各自保留所有权，跨轮消费仍验新。缓存发布与回收由跨进程事务协调，活跃借用受预算保护；关闭先停止接纳，再排空已接纳工作和句柄。直接使用 process-reuse API 时，以 `withPublication` 包住产物写入至证书封存，在回调外调用 `gc`、`drain` 或 `dispose`。

Linux 输入版本可显式配置 `ResourceVersionManager(root, { persistentInputs: { store, limits } })`，其中 `store` 是现有 `ProvenanceCertificateStore`，缓存目录必须位于源树之外。`capture(dependencies, retainBytes)` 保留分块输入，`persist(token)` 返回记录摘要；重启后用新 manager 调用 `restore(id, retainBytes)`，完整核对内容和当前路径观察后才返回新的输入令牌。记录缺失或源已变化时返回 `undefined`，损坏记录拒绝恢复。`view.readFile(path, maxBytes)` 读取有界前缀，`view.readFileRange(path, offset, length)` 支持逐块消费；使用结束后等待 `token.release()`。此选项目前只适用于上述已验证 Linux 输入闭包，扩展默认配置仍使用原内存输入策略。

Linux 分块捕获先保存在现有匿名文件租约中，不创建缓存目录、数据库或命名 CAS 文件；完整租约传递也无需发布。显式 `publish` 或 `persist` 才把数据和根写入同一 SQLite/CAS。匿名 backing 使用缓存路径最近的现存物理目录，仍受保存字节量、描述符和读取预算限制；该目录的文件系统须支持 `O_TMPFILE`。输入发现只查询现存数据库，空缓存不会被创建；旧 schema 没有输入版本，升级仍由既有证书入口或显式发布负责。冷恢复使用可取消的读取租约，不取得发布锁或推进 revision，`restore(id, retainBytes, signal)` 可传入取消信号。Linux CAS 的存在性检查、直接读取与冷块加载共用已配置原生提供者的 readonly/noatime 视图，跟随源别名也不改变源和链接 atime，提供者缺失不会回退为普通读取。Linux 已提交 SQLite 元数据由独立查询进程读取：活动 WAL 使用真正只读的源视图，已关闭冷库使用自有 ext4 文件读租约内复制并密封的匿名对象；每次查询重新打开数据库，不复用旧快照或宿主可写 SHM。旧格式迁移、发布和回收仍会写缓存；原源树隔离限制继续生效，默认自动恢复和任意名称空间隔离尚未完成。

升级后需重新运行 `npm run setup:linux`，由同一安装器构建 native helper 和 SQLite VFS 扩展；构建只使用校验固定摘要的官方 SQLite 头文件，查询仍由 Node 内置引擎执行。`ProvenanceStoreOptions.maxReadBytes` 独立限制元数据副本和单次返回数据，默认 2 GiB，不随缓存保留配额 `maxBytes` 缩小，以便读取超额记录并回收；冷主库首次捕获是全量复制。原生读租约未覆盖非自有文件、DrvFS、任意外部名称变化或已有不配合写 FD；缺少桥接时不会普通读取回退。Windows 延续原平台只读连接，不属于这项 Linux 无源副作用保证。

不另存记录 ID 时，可用 `for await (const { id, token } of manager.recover(retainBytes, { maxCandidates, signal }))` 发现同一源根的已有输入和纯计算。发现先取得有限候选快照，再逐条走相同恢复验证；调用方负责释放每个返回的 token。默认最多检查当前记录预算与 4096 的较小值，显式 `maxCandidates` 为 1–65536，其他源根和无效行也计入该扫描上限。新增记录留给下次扫描，损坏、过期或已清理的候选不阻止检查其他记录；取消与关闭会终结扫描，并释放尚未交付的租约。当前实现绑定与实际查询仍在消费时验证，恢复不授予旧调用的执行权限，也不把历史耗时当作恢复计算。

同一输入记录也保存已登记、闭包完整的纯计算数据，目前接入逐文件 rg 匹配。使用 `capture(undefined, retainBytes)` 按需捕获，完成查询并调用 `token.view.seal()` 后再持久化；冷恢复重新核对输入和当前实现绑定，匹配成功值可跨进程消费。绑定覆盖实际加载代码、Node 版本、rg 二进制及环境和查询参数；临时目录仍由新 owner 重建。只保存属于同一输入记录的有界不可变 JSON 数据，单项编码最多 1 MiB，按编码长度的 64 倍加固定开销计入 `retainBytes`；超出保留预算的结果仍可供当次调用使用。任意回调、外部资源或不完整的跨根输入闭包不因此获得持久恢复能力。

查询可以从一个视图发起、在另一个视图保留准备；持久化按完整证明判断闭包，不要求两者是同一临时对象。准备依赖的所有证明必须具有相同根与实际根，每项观察都由该输入记录以相同指纹覆盖，恢复时仍核验实际输入。默认会话的自动发布与恢复接线尚未完成；不能直接缩小目录搜索根来跳过祖先忽略规则，也不能让缓存写入被捕获的源树。

分块输入默认每块 1 MiB、单文件逻辑上限 2 GiB、每视图唯一保存块上限 1 GiB、块句柄上限 1024、单次返回上限 16 MiB；`retainBytes` 另限输入元数据和准备数据，I/O 按视图串行且使用固定块缓冲。首次捕获全量读取，完整冷校验也可能重新读取源；零块编码保留逻辑字节，不承诺物理稀疏布局。Linux 内容租约在支持匿名文件的存储卷上流式建立独立磁盘块副本，再只保留只读句柄；清理或损坏命名 CAS 文件不改变已签发版本，活 worker 借用同一独立块。匿名副本另占磁盘空间，读取仍校验摘要。SQLite 存储版本仍为 3，所有共享该目录的客户端应使用兼容版本，`maxCertificates` 共同限制证书和输入记录。输入块和准备数据作为同一回收根发布；CAS 读取按声明长度设限，损坏文件扩大不会触发无界读取。该路径不持久化准备回调、计时收据或原生对象权限，也不恢复旧监听窗口、机器状态或断电持久性保证。

已退出进程的 Linux 私有 Git 工作区与文件工具变更使用同一分块内容协议。首次运行前建立 Git 基线，结束后流式捕获前后版本，经现有对象与来源证明验证后按变化块写入，保留硬链接和旧 FD 的原位写语义；截断后扩展、`copy_file_range`、已结束的共享映射写入共用该路径。首次基线及前后摘要仍可能全量读取；临时 Git 仓库保留导入 pack，以 16 MiB 窗口和 32 MiB 总映射预算读取，过期对象按原五分钟宽限回收。Git 基线、命名 CAS、活匿名副本分别占用磁盘，OS 页缓存不属于进程 RSS。此能力限于已验证的 ext4 存储、Git 2.43.0 和受跟踪执行树；动态元数据、仍活跃的共享映射、外部写入者隔离及 Windows 大文件租约尚未因此成立。原生结果仍遵守时钟、随机输入等一次采用限制，不能从块数据可冷恢复推导出原生结果可跨重启复用。

活结果缓存分别核算内存和内容租约保存量，后者使用已有存储 `maxBytes` 限额；每个结果闭包还限制为 1024 个内容对象。正在借用的记录释放后才参与淘汰。用户持有的检查点与输入视图按各自的视图预算保有内容，服务关闭会排空它们；这些匿名副本的空间与命名 CAS 占用分别计算。

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
