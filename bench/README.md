# 投机执行验证

这里保留维护中的资格入口、模型套件和报告工具。日常回归使用 `npm test`；研究笔记、临时场景、构建快照和逐轮测量不提交到仓库，试验结束后清理。

## 本地回归

```sh
npm run check
npm run build
npm run bench:check
npm test -- --maxWorkers=1 --no-file-parallelism
```

Windows 与 WSL 顺序运行。性能压力、模型请求和后端资格按需执行，不随文档修改重复运行。测试使用的 `stock-tool-qualification.ts` 和 `linux-process-harness.ts` 是共享夹具。

## 受控搜索资格

```sh
node bench/portable-kernel.mjs
node --import tsx bench/grep-captured-qualification.mjs --semantics-only
node --import tsx bench/grep-captured-qualification.mjs --cost-only
```

第一项覆盖原版 find、生产 Host/TUI 路线、已完成与运行中采纳、输入变化、取消和关闭。grep 语义资格覆盖配置、目录、链接、输出及原生输入进程的回收；成本模式独立测量配置一致的原生调用、Host 和采纳。

需要已有、合格的 Pi rg，不安装或下载。可用 `--case=<名称>` 选择 grep 场景。资格针对显式 captured profile，不能外推 Native Pi 默认语义、macOS、ARM64 或 ThinkThread。

## Linux / WSL 进程资格

在 Linux 原生文件系统中的 checkout 运行，先使用当前后端自检：

```sh
npm run bench:exec-boundary -- --output /tmp/exec-boundary.json
npm run bench:overlay-probe
```

`PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 可指定已经验证匹配的 binary。exec 入口检查真实退出、描述符、输出与文件效果、跨父命令 completed/running 接管、一次消费及改变输入后的单次回退。OverlayFS 入口复用生产驱动的能力、隔离和回收测试，缺少能力时保留跳过原因；它不代替完整进程资格。

失败时保留原错误和最小复现信息；时钟证明拒绝不能通过延长等待或跳过检查消除。

## 录制与模型套件

已有录制由外部 `pi-llm-tape` 提供；本工具只分析文件，不发送模型请求：

```sh
npm run bench:tape -- --tape /private/path/tape.json --actor-model actor-id --drafter-model draft-id
```

入口分析 Chat Completions SSE，保留请求 payload、完整工具批次、usage 及失败记录，按请求累加服务耗时，按相同 messages 和工具定义比较动作参数。相同上下文只表示可比较，不证明候选归属或实际采纳。tape v1 的时间是请求内相对时间，因此不从中推算到达顺序、领先时间、策略收益或主加速比；这些证据来自完整运行报告。

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

正确性先于计时：核对完整输入输出、后续模型 payload、thinking、工具批次、预算、usage、逐步文件效果、最终回复、单次执行及零残留。Pattern 按各自实际批次顺序进入真实 Store，不随意重排以掩盖差异。保留较慢、失败和未命中样本，不将组件或构造时序推广为自然任务收益。

模型报告的 `patchCandidate` 仅筛选已结束、补丁干净且文件有交集的运行；正确性仍需数据集的 `FAIL_TO_PASS`/`PASS_TO_PASS`。套件按任务聚类汇总重复样本，失败不包装成性能收益。

冻结的 [既有发布资格说明](./results/release-qualification-2026-09-03.md) 仅对应其原版本，不代表当前代码已再次验收。
