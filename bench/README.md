# 投机执行验证

这里保留维护中的资格入口、模型套件和报告工具。日常回归使用 `npm test`；研究笔记、临时场景、构建快照和逐轮测量不提交到仓库，试验结束后清理。

## 本地回归

```sh
npm run check
npm run build
npm run bench:check
npm test -- --maxWorkers=1 --no-file-parallelism
```

Windows 与 WSL 顺序运行。性能压力、模型请求和后端资格按需执行，不随文档修改重复运行。原版工具资格共用 `stock-tool-qualification.ts`；Linux 进程场景共用 `test/linux-process-fixture.ts`。

## 受控搜索资格

```sh
node bench/portable-kernel.mjs
node bench/grep-captured-qualification.mjs
```

两项使用当前完整构建，并共用 `search-journey.mjs` 的 Host 轮次、候选与 Actor 回退流程。第一项覆盖原版 find、生产 Host/TUI 路线、已完成与运行中采纳、输入变化、取消和关闭。grep 覆盖配置、目录、链接、编码与 Unicode 排序、无效正则、输出、局部变化后的增量采纳、取消、输入预算及原生输入进程回收；同一工作区共用已准备的执行器，切换工作区时先回收旧执行器，各 Host 独立捕获和验证输入。这里只报告语义与回收结果，性能使用完整任务报告。

需要已有、合格的 Pi rg，不安装或下载。可用 `--case=<名称>` 选择 grep 场景。资格针对显式 captured profile，不能外推 Native Pi 默认语义、macOS、ARM64 或 ThinkThread。

## Linux / WSL 进程资格

在 Linux 原生文件系统中的 checkout 运行，先使用当前后端自检：

```sh
npm run bench:exec-boundary -- --reporter=json --outputFile=/tmp/exec-boundary.json
npm run bench:overlay-probe
```

在 Windows 上可用 `wsl.exe -e bash -lc 'bash "$(wslpath "<checkout>")/scripts/wsl-verify.sh" [vitest 参数]'`：它把当前 HEAD 与未提交修改同步到 WSL 原生文件系统的克隆中，在隔离 HOME 中构建本机助手后运行测试（`PI_SPEC_WSL_WORK`/`PI_SPEC_WSL_CHECKOUT`/`PI_SPEC_WSL_HOME` 可改位置）。

`PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 可指定已经验证匹配的 binary。exec 入口直接运行进程测试，输出 Vitest 报告，检查真实退出、描述符、输出与文件效果、跨父命令 completed/running 接管、一次消费及改变输入后的回退。场景与日常测试共用，不再维护另一套运行器或组件计时报告。OverlayFS 入口复用生产驱动的能力、隔离和回收测试，缺少能力时保留跳过原因；它不代替完整进程资格。

失败时保留原错误和最小复现信息；时钟证明拒绝不能通过延长等待或跳过检查消除。

## 模型套件

`bench:tape` 离线 SSE 相似度分析入口已退役；不再维护独立的协议解析、上下文配对和潜在命中统计。实际命中、工具批次、usage、失败和耗时由下述完整运行报告保留，既有原始录制与阶段材料仍在仓库外保存。

真实模型套件需要显式提供 `DEEPSEEK_API_KEY`，会产生网络和 API 成本：

```sh
npm run bench:ablation -- --instance axios__axios-5316 --prepare-only
npm run bench:ablation -- --instance axios__axios-5316 --prepared-run /path/to/prepared-run --label prepared
npm run bench:ablation -- --instance axios__axios-5316 --label baseline --speculation-disabled
npm run bench:suite -- --suite swe_smoke --paired --repeats 1 --max-turns 32 --timeout-ms 360000 --label validation
npm run bench:suite -- --suite swe_diverse --repeats 3 --label speculative
```

先用 `--prepare-only` 准备源码，在返回的工作区安装依赖，再用 `--prepared-run` 指定其上一级运行目录。入口要求源码仍处于数据集基线且无未提交改动，每个已准备目录只启动一次模型任务；安装时间不计入任务时钟。质量验证应在任务结束后用独立副本应用数据集测试，分别检查原始基线和模型补丁。

`--prepare-only` 只准备数据集和 checkout。套件见 `suite.json`；`--output-root` 指定产物目录，默认使用系统临时目录。密钥只从环境读取，移入 Pi 的内存凭据后从进程环境删除（Actor 的 shell 读不到），不写入录制或报告。测试工作区、录制和报告使用后清理，只保留必要结论和未解决失败的最小证据。

运行经 Pi SDK 加载已安装的扩展（Pi 默认工具与系统提示、Linux 进程复用、沙箱与快照路线），设置写入运行专属 agent 目录；Linux 路线需在 WSL/Linux 中运行并提供 `PI_SPEC_SANDLOCK`/`PI_SPEC_HELD_EXEC`/`PI_SPEC_STRACE`。`bench:suite -- --paired` 对每个实例与重复交替先后运行开/关两臂，报告 `pairedRatio`（关的实际总耗时 / 开的实际总耗时，可 < 1）及各臂汇总。

常用开关：`--drafter-disabled` 关闭 Drafter，`--drafter-max-depth 0` 关闭续推，`--pattern-aware --pattern-state <目录>` 启用并持久化模式学习，`--self-speculation` 启用经 Drafter 读取 Actor 推理的 fork（`forkTransport: "drafter"`），`--drafter-pattern-hints` 让 Drafter 看到 PatternAware 预期的调用（A/B）。共享模式状态不共享工作区文件；默认最多 128 轮，达到上限属于未完成。

`--drafter-task-max-requests` 和 `--drafter-task-max-tokens` 约束整个任务的预测、续推和 Actor 探测。报告的 Drafter usage 与费用包含全部收到的响应；`drafterBudget` 另外列出缺少 usage 的预留，无法据此声称精确账单。普通来源的预测 token 统计与这份完整请求统计分开。

每次调度策略变更至少覆盖以下矩阵；`swe_smoke` 只运行 Axios FormData 修复任务，用于限制首次真实模型验证的范围。完整自然任务结论仍需扩大到多实例、多次重复。

| 场景 | 入口 | 验收内容 |
| --- | --- | --- |
| 搜索密集与输入频繁变化 | `bench/portable-kernel.mjs`、`bench/grep-captured-qualification.mjs` | 逐步输出、输入变化后的回退或重算、取消与关闭 |
| 多步、低命中和未知收益 | `test/faux-llm-e2e.test.ts`、`test/drafter-adaptation.test.ts` | 完整 Agent 任务、有限探索、额外候选与续推层的收缩和恢复、延迟反馈 |
| Actor 资源争用与内部进程接管 | `test/runtime-engine.test.ts`、`test/linux-process-world.test.ts` | 并发与跨轮资源预留、物理回收、当前调用的内部计算保留与一次采纳 |
| 自然修改与验证任务 | `bench:suite -- --suite swe_smoke --paired` | 最终回复、补丁、数据集指定测试、完整计时和实测 token/费用 |

汇总时保留低命中、失败、超时及较慢样本，分别报告同次运行加速比、端到端均值/P95、命中率、token 和任务正确性。构造的模型时序、组件资格和单个真实任务分别报告。

## 计时与验收规则

主加速比为同次运行的 `serializedCounterfactualMs / actualEndToEndMs`。反事实保留实际开销，仅移除权威计算重叠并去重；无重叠 1×，有重叠大于 1×，不另跑真实 Actor 串行基线。独立开关、原生/Host 和相邻版本对照只用于成本归因。

任务计时从 Host/工具初始化前到终态结算和回收完成，包含准备、预测、执行、验证、采纳、拒绝与清理。数据集下载、checkout 和最终补丁检查在计时外。完整 Host 返回与内部 `hitLatencyMs` 分开；running 接管还需区分接入、剩余执行与完成后交付。

未执行的预测只报告阻塞和匹配事实，不推算潜在节省。模型套件保留真实工具调用计数及原始 Drafter 记录，不重复采集 `toolIntentMs`、`rawActorToolServiceMs`，也不再输出可从预测记录还原的 `drafterStopReasons`、`drafterToolCalls`、`drafterNoToolStopReasons`。

正确性先于计时：核对完整输入输出、后续模型 payload、thinking、工具批次、预算、usage、逐步文件效果、最终回复、单次执行及零残留。Pattern 按各自实际批次顺序进入真实 Store，不随意重排以掩盖差异。保留较慢、失败和未命中样本，不将组件或构造时序推广为自然任务收益。

模型报告的 `patchCandidate` 仅标记已结束、补丁干净且文件有交集的运行；正确性仍需数据集的 `FAIL_TO_PASS`/`PASS_TO_PASS`。prompt 或回收抛错时，单次报告保留计时、usage 和各阶段的 `benchmarkErrors`，写出后以失败状态退出。套件保留本次 runner 失败前写出的报告和原始退出错误，停止后续任务；已有单次输出不会被覆盖，无法读取的 summary 不补造计时。

套件总表与分任务表均纳入所有有完整计时的样本，包括失败和慢样本；加速比为总反事实串行耗时除以总实际耗时，同时报告均值、P95 和样本数。缺失或无效计时单列为 `unmeasuredRuns`，失败原因保留在 `invalidRuns`。这些反事实汇总不证明因果提速或任务正确性。
冻结的 [既有发布资格说明](./results/release-qualification-2026-09-03.md) 仅对应其原版本，不代表当前代码已再次验收。
