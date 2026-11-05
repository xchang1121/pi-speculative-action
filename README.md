# Pi 投机执行与缓存复用

纯 Pi 扩展：提前执行可能需要的工作，让正式工具调用复用已经完成的部分，保留原工具的输出和副作用语义。

## 主线架构

**投机决定提前做什么，缓存复用决定已有工作能否使用。** 两者由同一运行时编排；缓存既接收投机产物，也接收正常执行留下的可复用成果。

```mermaid
flowchart LR
    P[预测来源] --> S[计划与调度]
    S --> E[提前执行]
    E --> R[成果与依赖、效果、所有权证明]
    A[Actor 正式调用] --> V[查找与验新]
    R --> V
    V -->|可采用| U[复用已有部分，完成剩余工作]
    V -->|未命中或证明不足| N[正常执行]
    N -->|捕获可复用部分| R
```

- **投机生产**：Drafter、PatternAware 和 Actor 信号提供候选，计划与调度负责依赖、预算、并发和取消，执行后端负责受控执行。提前准备已观察的子操作也属于这一层。预测观测只用于私有续推，Actor 历史只接收真实结果。
- **缓存复用**：保存、发现并验证工具结果、输入、纯子计算及受支持的进程状态。正式调用按实际操作、依赖、效果和当前所有权采用成果，继续执行剩余部分；工具名称和预测来源不授予复用权限。

预测匹配、成果有效性和实际消费分别记账。关闭投机后，正常调用仍可积累并使用缓存；预测失配也不自动作废独立有效的成果。效果只提交一次，消费期间保留资源，取消、淘汰和关闭按所有权释放。收益只计实际消费的计算，等待和未消费的工作另行统计。

复用沿实际调用边界从外到内进行：完整工具或 Bash 分支有效时直接采用；整体失效后执行剩余流程，并在子进程、可证明的同步函数及输入资源边界继续查找。父级失效不会直接清空仍有自身证明的子成果；父级命中则跳过内部调用与查找。同模块纯函数可以组合成多层计算，改变外层参数只重算外层，改变子函数代码或闭包数据会重新核验所有依赖它的层级。各层共用原存储、预算、所有权与消费计量，父子重叠计算只计一次。Bash 的层级来自实际观察的进程，不能仅靠拆分命令字符串授权复用；这也不保证小计算的采纳成本低于重算。

源码主线：[Host 接入](./src/agent-integration.ts) → [运行时编排](./src/runtime-engine.ts) → [统一执行入口](./src/tool-execution-gateway.ts)。[执行世界](./src/execution-world.ts)提供捕获、验证和提交契约；[资源版本](./src/resource-version.ts)与[存储](./src/reuse-store.ts)管理可复用成果及证明。

## 使用

需要 Node.js ≥ 22.19；使用未修改的 Pi，当前兼容验证基线为 `0.84.1`。

```sh
pi install git:github.com/xchang1121/pi-speculative-action@speculative-action
pi
```

运行 `/speculative-action`，调整开关和预测来源后选择 **Apply changes**。

| 开关 | 默认值 | 职责 |
| --- | --- | --- |
| **Speculative production** (`enabled`) | 关闭 | 启用额外的提前生产，包括调用预测、流式预览和已观察操作的准备。 |
| **Cache & reuse** (`reuseEnabled`) | 开启 | 捕获并采用可复用成果；关闭后停止缓存采用及新的提前生产。 |

两个开关都保留实际调用的观察与模式学习。`tools` 和各预测来源开关只筛选提前生产，不限制其他工具复用合格成果。

`grep/find` 默认采用通过版本检查的 Captured 搜索契约，让 Actor 和提前执行共享同一搜索实现；资格不足时仍由原工具执行。保存 `searchExecution: "native"` 可显式选择 Native Pi。这个选择决定搜索语义，开启投机与否不会切换 Actor 的实现。

全局配置位于 `<agent-dir>/speculative-action.json`，项目配置位于 `.pi/speculative-action.json`。安装准备阶段使用随依赖保留的 TypeScript 编译器生成入口，兼容 Pi 默认省略开发依赖的 Git 安装；类型检查在开发验证阶段单独运行。Pi 加载已编译入口；本地修改源码后运行 `npm run build`，再用 `pi -e /absolute/path/to/pi-speculative-action` 加载。

Linux / WSL 2 需在扩展目录运行 `npm run setup:linux`，升级后重新运行；WSL checkout 放在 Linux 原生文件系统。Windows x64 使用随包提供的本地 NTFS 对象租约，无需另行编译。

Linux 的透明子进程观察还需要合格的观察助手：通过 `PI_SPEC_HELD_EXEC_OBSERVER` 指向管理员部署、普通用户不可修改且具有 `CAP_SYS_PTRACE` 的独立 `.observer` 文件。助手核对调用者及目标进程身份，并在附着后释放该能力；工作负载本身不获得此权限。若启动环境已经设置 `no_new_privs`，助手也可在该约束下工作，但这会阻止依赖提权的 FUSE 挂载，不能用作完整验证环境的通用替代。权限不足时原工具正常执行，透明复用不可用。
Linux 进程复用统一要求 cgroup v2、内核 BTF 与所需 eBPF tracing/kprobe hooks、`bpf_loop`；构建采集器还需要 Clang 和 libbpf、libelf、zlib 开发库。安装后，保留上面配置的观察助手，通过前台执行域启动 Pi：

```sh
sudo ~/.local/bin/pi-speculative-domain --user "$(id -u)" env \
  PATH="$PATH" PI_SPEC_HELD_EXEC_OBSERVER="$PI_SPEC_HELD_EXEC_OBSERVER" "$(command -v pi)"
```

执行域负责对象版本、描述符身份、元数据观察和进程回收，Pi 与工具仍以原用户运行；进程接管、资源所有权、效果提交和计量沿用现有执行世界。仅配置旧观察助手已不足以启用 Linux 进程复用；普通启动或内核条件不足时，工具照常执行，文件输入和纯计算仍按各自能力工作。

同一采集器共享已加载的观察探针，执行结束只释放其借用；最多保留 256 个探针对象，容量满时淘汰空闲对象，退出时统一关闭。执行域最多同时持有 256 个观察所有者，稀疏的内核对象表按需分配，各有 65,536 条上限；分配失败、溢出、丢失或观察中断使相关证明失效，不能以不完整证据交付资源。重新启动执行域可重新建立观察代际。

## 支持边界

资格由实际操作、依赖、效果和所有权决定。下表描述可验收的能力；某个工具名称或“只读”声明不会授权缓存其完整结果。

| 能力 | 成立条件与不满足时的行为 | 公开入口验收 |
| --- | --- | --- |
| 共享输入、纯子计算 | 实际操作绑定、有效依赖证明；可跨工具消费，缺少绑定或证明时正常执行。 | `agent.system`、`persistent-inputs` |
| 普通工具内部计算 | 自动接入同一已加载 JS 模块内可证明的同步数据函数和原生 `JSON.parse`；以当前代码、完整参数、闭包数据和内建实现核验，外层工具继续执行。未知调用、访问器、代理、别名、预算不足或取消时正常执行。 | `resource-computation`，真实 Pi 注册工具与独立调用计数 |
| 工作区效果 | 私有工作区隔离、验新、事务提交及所有权释放；提交前失效可回退，已产生效果且恢复不明时终止。 | `process-reuse.system`、`host-closure`、`linux-overlayfs` |
| Linux 完成成果与进程接管 | 已验证 x86-64、同一 ext4 挂载、合格 Bash 分支、已观察子操作及可封存的单线程进程；需要当前构建的执行域、观察助手和通过行为探测的后端。缺少执行域时进程复用不可用。 | `extension.system`、`linux-reuse-chain`、`held-exec` |
| 检查点跨进程恢复 | 另限同一次系统启动、同一 Pi 会话、工作区和原缓存位置；身份、输入、所有权或镜像不符时拒绝恢复。 | `process-reuse.system` |
| Windows | 本地 NTFS 文件资源与私有工作区；没有 Linux 同等进程接管能力。 | `filesystem-lease`、`host-closure` |

普通工具内部计算通过进程内 V8 LiveEdit 接入，不打开调试端口。当前自动证明限于普通有限 JSON 数据、已审查的内建操作及同模块不可变函数绑定组成的无环调用链，不覆盖任意导入库、异步计算或函数源码反射；含已识别源码反射的模块不接入，外部调试器仍可能看到临时包装。代码、闭包数据或文件内容改变不会沿用旧计算，返回的数据副本允许调用者独立修改。结果继续使用现有资源视图、预算、消费计时和回收链，仅保留在当前会话；显式绑定的持久化接口保持原有契约。它不会提前运行不透明工具的外层副作用，也不保证每个小计算都有净耗时收益。

进程观察根允许空格、反斜杠和 UTF-8 字符；须为规范化后的绝对目录、归调用者所有且组/其他用户不可写。路径须小于 Linux `PATH_MAX`（4,096 字节），每条祖先链最多 64 项（包括根自身和 `/`），同一观察所有者最多 4 个根、64 个去重父目录。限额耗尽或权限不符会给出具体拒绝原因。`linux-environment` 验证诊断与真实准入一致，`extension.system` 验证这些路径下的实际消费。

菜单中的 **Check Linux environment** 使用运行时同一执行域握手、工作区准入及观察助手探测，分别列出构建条件、当前缺口和未验证事项；**Refresh and show capabilities** 显示已启用后端的实际资格。构建工具或库存在并不等于操作已获复用资格。升级后须重新 `setup:linux` 并重启前台执行域；仅保留旧观察助手不能恢复进程复用。

任意原生程序、多线程续接、Windows 原生进程续接及宿主中断工具批次的透明恢复尚不支持；保持当前纯扩展部署，不引入检查点服务或虚拟机来扩大这一边界。外部服务状态与副作用不属于投机范围。Linux 实测环境为 WSL 2 / Ubuntu 24.04、x86-64 和原生 ext4，不外推其他内核、文件系统或架构。上表用例位于 `test/`，平台专属跳过不计为支持证据。

验新不提供任意外部并发写入的隔离保证。证明不足时沿原路径执行；若提交已产生副作用且无法证明恢复，则报错终止，避免重复执行。

## 开发

```sh
npm ci
npm run check
npm run build
npm test -- --maxWorkers=1 --no-file-parallelism
npm run bench:check
```

Windows 开发可在 WSL 中运行 `scripts/wsl-verify.sh`。它将当前提交和未提交改动复制到独立 Linux checkout，构建私有助手，再通过 `sudo` 启动该构建的前台执行域；测试仍以原用户运行。先按上文配置 `PI_SPEC_HELD_EXEC_OBSERVER`，脚本不安装持久服务或替换用户助手。

扩展接口见 [Pi Host API](./src/index.ts)、[core](./src/core.ts) 和 [process-reuse](./src/process-reuse.ts)。新预测来源提交计划，新执行后端实现执行世界契约；反馈通过 `runtime.subscribe()` 接收，结束时调用 `host.dispose()`。

公共 Host 接纳通过 `registerExecutionWorld(implementation, qualification)` 注册的运行时后端，通用 Linux 工厂也走此入口。注册声明具体入口并提供 `verify({ cwd, operation, signal, refresh })` 行为探测，返回当前隔离实现的 fingerprint 及 `externalEffects: "excluded"`；这是受信任后端对实际隔离、观察与回收能力的证明接口，不能用常量替代探测。执行前重新检查资格和版本，失败时回到可用路径；逐成果验新、效果事务和所有权仍由原流程检查。复制或修改后的实现须重新注册，关闭即撤销资格。

[验证与收益口径](./bench/README.md) · [变更记录](./CHANGELOG.md)
