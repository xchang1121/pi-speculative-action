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

`PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 可指定已经验证匹配的 binary。exec 入口直接运行进程测试，输出 Vitest 报告，检查真实退出、描述符、输出与文件效果、跨父命令 completed/running 接管、一次消费及改变输入后的回退。场景与日常测试共用，不再维护另一套运行器或组件计时报告。OverlayFS 入口复用生产驱动的能力、隔离和回收测试，缺少能力时保留跳过原因；它不代替完整进程资格。

失败时保留原错误和最小复现信息；时钟证明拒绝不能通过延长等待或跳过检查消除。

## 模型套件

`bench:tape` 离线 SSE 相似度分析入口已退役；不再维护独立的协议解析、上下文配对和潜在命中统计。实际命中、工具批次、usage、失败和耗时由下述完整运行报告保留，既有原始录制与阶段材料仍在仓库外保存。

真实模型套件需要显式提供 `DEEPSEEK_API_KEY`，会产生网络和 API 成本：

```sh
npm run bench:ablation -- --instance axios__axios-5316 --prepare-only
npm run bench:ablation -- --instance axios__axios-5316 --label baseline --speculation-disabled
npm run bench:suite -- --suite swe_diverse --repeats 3 --label speculative
```

`--prepare-only` 只准备数据集和 checkout。套件见 `suite.json`；`--output-root` 指定产物目录，默认使用系统临时目录。密钥只从环境读取，不写入录制或报告。测试工作区、录制和报告使用后清理，只保留必要结论和未解决失败的最小证据。

常用开关：`--drafter-disabled` 关闭 Drafter，`--drafter-max-depth 0` 关闭续推，`--pattern-aware --pattern-state <目录>` 启用并持久化模式学习。共享模式状态不共享工作区文件；默认最多 128 轮，达到上限属于未完成。

## 计时与验收规则

主加速比为同次运行的 `serializedCounterfactualMs / actualEndToEndMs`。反事实保留实际开销，仅移除权威计算重叠并去重；无重叠 1×，有重叠大于 1×，不另跑真实 Actor 串行基线。独立开关、原生/Host 和相邻版本对照只用于成本归因。

任务计时从 Host/工具初始化前到终态结算和回收完成，包含准备、预测、执行、验证、采纳、拒绝与清理。数据集下载、checkout 和最终补丁检查在计时外。完整 Host 返回与内部 `hitLatencyMs` 分开；running 接管还需区分接入、剩余执行与完成后交付。

未执行的预测只报告阻塞和匹配事实，不推算潜在节省。模型套件保留真实工具调用计数及原始 Drafter 记录，不重复采集 `toolIntentMs`、`rawActorToolServiceMs`，也不再输出可从预测记录还原的 `drafterStopReasons`、`drafterToolCalls`、`drafterNoToolStopReasons`。

正确性先于计时：核对完整输入输出、后续模型 payload、thinking、工具批次、预算、usage、逐步文件效果、最终回复、单次执行及零残留。Pattern 按各自实际批次顺序进入真实 Store，不随意重排以掩盖差异。保留较慢、失败和未命中样本，不将组件或构造时序推广为自然任务收益。

模型报告的 `patchCandidate` 仅标记已结束、补丁干净且文件有交集的运行；正确性仍需数据集的 `FAIL_TO_PASS`/`PASS_TO_PASS`。prompt 或回收抛错时，单次报告保留计时、usage 和各阶段的 `benchmarkErrors`，写出后以失败状态退出。套件保留本次 runner 失败前写出的报告和原始退出错误，停止后续任务；已有单次输出不会被覆盖，无法读取的 summary 不补造计时。

套件总表与分任务表均纳入所有有完整计时的样本，包括失败和慢样本；加速比为总反事实串行耗时除以总实际耗时，同时报告均值、P95 和样本数。缺失或无效计时单列为 `unmeasuredRuns`，失败原因保留在 `invalidRuns`；补丁成功数量不再决定计时样本。报告不再生成 bootstrap 置信区间，这些反事实汇总不证明因果提速或任务正确性。

冻结的 [既有发布资格说明](./results/release-qualification-2026-09-03.md) 仅对应其原版本，不代表当前代码已再次验收。
