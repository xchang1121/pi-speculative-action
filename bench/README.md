# 投机执行验证

这里保留维护中的资格入口、模型套件和报告工具。日常回归使用 `npm test`；研究笔记、临时场景、构建快照和逐轮测量保存在本地临时目录，试验结束后清理。

## 回归与受控搜索资格

本地检查命令见[开发与验证](../README.md#开发与验证)，Windows 与 WSL 顺序运行。文档修改检查格式与链接；性能压力、模型请求和后端资格按相关改动与评估需求执行。原版工具资格共用 `stock-tool-qualification.ts`；Linux 进程场景共用 `test/linux-process-fixture.ts`。

```sh
node bench/portable-kernel.mjs
node bench/grep-captured-qualification.mjs
```

两项使用当前完整构建，并共用 `search-journey.mjs` 的 Host 轮次、候选与 Actor 回退流程。第一项覆盖原版 find、生产 Host/TUI 路线、已完成与运行中采纳、输入变化、取消和关闭。grep 覆盖配置、目录、链接、编码与 Unicode 排序、正则错误处理、输出、局部变化后的增量采纳、取消、输入预算及原生输入进程回收；同一工作区共用已准备的执行器，切换工作区时先回收旧执行器，各 Host 独立捕获和验证输入。这里报告语义与回收结果，性能使用完整任务报告。

运行前准备好合格的 Pi rg，入口使用本地可执行文件。可用 `--case=<名称>` 选择 grep 场景。资格针对显式 captured profile；Native Pi 默认语义、macOS、ARM64 和 ThinkThread 的结论需由各自环境的独立验证支持。

## Linux / WSL 进程资格

在 Linux 原生文件系统中的 checkout 运行，先使用当前后端自检：

```sh
npm run bench:exec-boundary -- --reporter=json --outputFile=/tmp/exec-boundary.json
npm run bench:overlay-probe
```

在 Windows 上可用 `wsl.exe -e bash -lc 'bash "$(wslpath "<checkout>")/scripts/wsl-verify.sh" [vitest 参数]'`：它把当前 HEAD 与工作区改动同步到 WSL 原生文件系统的克隆中，在隔离 HOME 中构建本机助手后运行测试（`PI_SPEC_WSL_WORK`/`PI_SPEC_WSL_CHECKOUT`/`PI_SPEC_WSL_HOME` 可改位置）。

`PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 可指定已经验证匹配的 binary。exec 入口直接运行进程测试，输出 Vitest 报告，检查真实退出、描述符、输出与文件效果、跨父命令 completed/running 接管、一次消费及改变输入后的回退。场景、运行器与报告格式和日常测试共用。OverlayFS 入口复用生产驱动的能力、隔离和回收测试，能力检查决定执行或带原因跳过；完整进程资格由 exec 入口验收。

失败时保留原错误和最小复现信息；时钟相关的采纳资格由原始观测和完整校验决定。

## 模型套件

真实模型套件需要显式提供 `DEEPSEEK_API_KEY`，会产生网络和 API 成本：

```sh
npm run bench:ablation -- --instance axios__axios-5316 --prepare-only
npm run bench:ablation -- --instance axios__axios-5316 --prepared-run /path/to/prepared-run --label prepared
npm run bench:ablation -- --instance axios__axios-5316 --label baseline --speculation-disabled
npm run bench:suite -- --suite swe_smoke --paired --repeats 1 --max-turns 32 --timeout-ms 360000 --label validation
npm run bench:suite -- --suite swe_diverse --repeats 3 --label speculative
```

先用 `--prepare-only` 准备数据集和 checkout，在返回的工作区安装依赖，再用 `--prepared-run` 指定其上一级运行目录。入口要求源码处于数据集基线、工作区状态干净，每个已准备目录用于一次模型任务；依赖安装在任务计时开始前完成。质量验证应在任务结束后用独立副本应用数据集测试，分别检查原始基线和模型补丁。

套件见 `suite.json`；`--output-root` 指定产物目录，默认使用系统临时目录。密钥通过环境变量提供，转存 Pi 内存凭据后随即从进程环境移除，运行期间由 SDK 持有；录制与报告保存任务事件和计量数据。测试工作区、录制和报告使用后清理，归档内容为必要结论与待解决失败的最小证据。

运行经 Pi SDK 加载已安装的扩展（Pi 默认工具与系统提示、Linux 进程复用、沙箱与快照路线），设置写入运行专属 agent 目录；Linux 路线需在 WSL/Linux 中运行并提供 `PI_SPEC_SANDLOCK`/`PI_SPEC_HELD_EXEC`/`PI_SPEC_STRACE`。`bench:suite -- --paired` 对每个实例与重复交替先后运行开/关两臂，报告 `pairedToolWaitRatio`（关的工具等待总时长 / 开的工具等待总时长，可 < 1）及各臂汇总。该配对等待比是原始延迟诊断，与计算加速比 `toolSpeedup` 分开。

常用开关：`--drafter-disabled` 关闭 Drafter，`--drafter-max-depth 0` 关闭续推，`--pattern-aware --pattern-state <目录>` 启用并持久化模式学习，`--self-speculation` 启用经 Drafter 读取 Actor 推理的 fork（`forkTransport: "drafter"`），`--drafter-pattern-hints` 让 Drafter 看到 PatternAware 预期的调用（A/B）。模式学习状态跨运行复用，各运行使用独立工作区；默认最多 128 轮，达到上限时以轮次耗尽结束运行。

`--drafter-task-max-requests` 和 `--drafter-task-max-tokens` 约束整个任务的预测、续推和 Actor 探测。报告的 Drafter usage 与费用包含全部收到的响应；`drafterBudget` 另外列出 usage 待补齐的预留，精确账单以提供商记录为准。普通来源的预测 token 统计与这份完整请求统计分开。

日常回归从完整工作流的可观察行为验证功能，按改动选择相关入口。真实模型运行用于评估任务表现与收益；`swe_smoke` 覆盖 Axios FormData 修复任务，完整自然任务结论需基于多实例、多次重复。

| 场景 | 入口 | 验收内容 |
| --- | --- | --- |
| 搜索密集与输入频繁变化 | `bench/portable-kernel.mjs`、`bench/grep-captured-qualification.mjs` | 逐步输出、输入变化后的回退或重算、取消与关闭 |
| Pi 会话与文件工作流 | `test/extension.system.test.ts`、`test/agent.system.test.ts` | 真实 SDK 加载扩展、跨轮读写、学习状态重开、变更后验新和投机效果隔离 |
| 多生产者与历史结果 | `test/process-reuse.system.test.ts` | 后完成的生产者唤醒等待者、损坏缓存隔离、有效历史结果与输入变更 |
| 原生进程接管 | `test/linux-process-world.test.ts`、`test/linux-exec-boundary.test.ts`、`test/linux-reuse-chain.test.ts` | 真实进程与系统调用、跨轮接管、描述符与文件效果、取消后的资源释放 |
| 自然修改与验证任务 | `bench:suite -- --suite swe_smoke --paired` | 最终回复、补丁、数据集指定测试、完整计时和实测 token/费用 |

## 计时与验收规则

工具加速比为 `T / (T - H)`，采用乐观的可观测执行边界口径。`T = toolComputeMs` 包括当前调用实际消费的复用前缀、成果首次生成所需的证明和封存、生产者在 Actor 发出调用后继续完成的计算，以及 Actor 自己执行的剩余部分。T 采用执行区间的墙钟时间：Actor 的执行区间统一扣除接管控制区间及其中的兄弟进程活动，被消费的原始生产者按自身执行区间计入。回执增量合并为区间并集，保留空档。`H = hiddenComputeMs` 计入实际复用且在本次 Actor 发出调用前完成的部分，逐段在调用时间截断。同一调用按计算身份去重，同次原始执行的并行父子区间取并集；后续调用再次复用时按自己的调用时间记账。采纳当次的验新、拷贝、恢复和交付按等待指标计量。记录完整、T=H>0 时报告 `fullyHidden`，比值为 `null`；T 缺失、H 覆盖不足或 T=H=0 时显示 `n/a`。

`toolWaitMs` 用于原始延迟诊断，按 Actor 调用起止区间取并集，包含准备、验证、采纳、回退和结算。`adoptionWaitMs` 由调用区间扣除已消费的执行区间得到，记录准备、验新、恢复、交付和结算中的纯协调等待；并行调用按等待区间取并集。两者作为工具调用层面的独立等待指标报告，精确的采纳服务耗时与反事实净开销需通过专项实验评估。

关闭投机的基线运行通过 SDK 工具事件记录等待，这些事件覆盖验证、交付和排队。该运行的计算计量按 T=未知、H=0 记录，计算加速比为 `null`；原始工具等待用于 `pairedToolWaitRatio`。

任务计时从 Host/工具初始化前到终态结算和回收完成，包含准备、预测、执行、验证、采纳、拒绝与清理。数据集下载和 checkout 先于任务计时，最终补丁检查在任务计时结束后进行。完整 Host 等待由基准独立记录，用于延迟诊断；复用准入依据执行证明，计算收益依据 T、H。命中的运行中工作等待完成，支持调用方取消。

停留在预测阶段的候选记录阻塞和匹配事实，节省量依据已执行并采纳的计算计量。`traces` 的来源请求、预测、候选和 Actor 调用保留原事件结构，导出时清理 `cache`/`sessionID`；通过 turn、decision、prediction 和 candidate ID 关联，预算或门控拒绝保存在来源请求的 `empty.cause`。事件和工具等待端点使用 epoch 毫秒；`provider.toolExecution` 的单调端点加 `metadata.monotonicTimeOrigin` 后可对齐。报告从既有记录还原计时和汇总。

`traces.slowCalls` 将等待至少 500ms 的调用按实际等待时长排序，连接唯一 Actor 结算、预测与候选记录，并标出部分复用。同一决策的来源请求作为上下文保留；调用失配原因依据该调用的实际结算单独归因，调用 ID 重复时标记归因歧义。`summary.slowCallCoverage` 报告慢调用占全部已完成调用等待总和的比例，采用逐调用累计口径；并行等待采用区间并集口径单独计量。

正确性先于计时：核对完整输入输出、后续模型 payload、thinking、工具批次、预算、usage、逐步文件效果、最终回复、单次执行及零残留。Pattern 按各自实际批次顺序进入真实 Store，以实际顺序比较结果。报告纳入成功、失败、较慢和零命中样本，自然任务收益通过完整任务流程验证。

模型报告的 `patchCandidate` 标记已结束、补丁干净且文件有交集的运行；正确性由数据集的 `FAIL_TO_PASS`/`PASS_TO_PASS` 验证。prompt 或回收抛错时，单次报告保留计时、usage 和各阶段的 `benchmarkErrors`，写出后以失败状态退出。套件保留本次 runner 失败前写出的报告和原始退出错误，停止后续任务；已有单次输出保持原样，summary 的计时按读取到的证据记录。

套件总表与分任务表均纳入所有有完整 T、H 测量的样本，包括失败、慢样本和零命中；先累计 T、H，再计算 `ΣT / Σ(T-H)`。证据缺失、校验失败或覆盖不足的样本列入 `unmeasuredRuns`；`diagnostics` 保留已确认 H、原始等待、协调等待及其覆盖数，失败原因保留在 `invalidRuns`。计算证据完整保存原始时钟、原点和依赖图，并保留全部节点、关系、片段和分组；深层图用迭代遍历保存和恢复。旧证据缺失或损坏时，计时记为未知，复用依据执行证明继续判断。新报告标记 `actor_issue_boundary_compute_v3`，按计量版本分别比较；H 使用逐段计算边界的实测记录。任务正确性由数据集测试验收，端到端净收益由完整任务计时评估。
