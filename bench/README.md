# 投机执行验证

日常回归、后端资格和真实模型评估使用同一生产 Host。常规检查见[计量与验证](../README.md#计量与验证)，Windows 与 WSL 顺序运行；研究笔记、临时场景和构建快照在本地临时目录保存，结束后清理。

## PatternAware 预测回放

`node --import tsx bench/pattern-workflows.mjs` 运行可复现的合成工作流、混噪训练与位置偏好切换实验。

`node --import tsx bench/pattern-trace-replay.mjs /path/to/tasks/*/record.session.jsonl` 读取每条原始会话及同名的 `record.json`。每折留出一个完整仓库，按原始 Actor 批次先预测再学习。报告按宿主的工具契约区分原始参数相同、规范键精确匹配和范围投影，统计首位命中、误报及覆盖的调用数。每个被覆盖的调用只累计一次原录制耗时。测量 Actor 实际等待需在运行中的 Host 重放相同调用。

## 后端资格

```sh
node bench/portable-kernel.mjs
node bench/grep-captured-qualification.mjs
npm run bench:exec-boundary -- --reporter=json --outputFile=/tmp/exec-boundary.json
npm run bench:overlay-probe
```

前两项使用当前完整构建、合格的本地 Pi rg 和共享 Host 流程，验证原版 find、显式 captured grep 的输出、编码、排序、链接、输入变化、增量重算、取消及回收；grep 支持 `--case=<名称>`。

Linux 进程资格在原生文件系统的 checkout 运行。Windows 可用 `wsl.exe -e bash -lc 'bash "$(wslpath "<checkout>")/scripts/wsl-verify.sh" [vitest 参数]'` 同步当前代码、构建助手并执行测试；`PI_SPEC_WSL_WORK/CHECKOUT/HOME` 调整隔离目录。已验证的 binary 可通过 `PI_SPEC_SANDLOCK`、`PI_SPEC_HELD_EXEC` 指定，观察器资格见[Linux 进程复用](../README.md#linux-进程复用)。

exec 入口验证真实退出、描述符、输出、文件效果、已完成结果重放和输入变化后的回退。显式受控执行器负责子进程入口。OverlayFS 按生产能力检查执行或带原因跳过。共享场景位于 `test/linux-process-fixture.ts`，资格以当前构建为准。

## 真实模型任务

模型套件需要显式提供 `DEEPSEEK_API_KEY`，会产生网络与 API 成本。密钥转存 Pi 内存凭据后从进程环境移除，日志保存任务事件与计量数据。

```sh
npm run bench:ablation -- --instance axios__axios-5316 --prepare-only
npm run bench:ablation -- --instance axios__axios-5316 --prepared-run /path/to/prepared-run --label prepared
npm run bench:suite -- --suite swe_smoke --paired --repeats 1 --max-turns 32 --timeout-ms 360000 --label validation
```

先准备数据集和 checkout，在返回的工作区安装依赖，再传入其上一级运行目录。源码必须保持数据集基线且工作区干净，一个准备目录只运行一次任务。套件定义见 `suite.json`，默认产物写入系统临时目录，`--output-root` 可改位置；使用后清理工作区、录制及报告，只保留必要结论和失败的最小证据。运行通过 SDK 加载已安装扩展，Linux 路线需在 WSL/Linux 中提供合格后端。

`--speculation-disabled` 运行关闭提前生产的基线，保留直接执行的缓存、复用、观察和学习；`--drafter-disabled` 只关闭模型预测。`--drafter-max-depth 0` 关闭乱序续推，正数限制后继批次数。`--pattern-aware --pattern-state <目录>` 跨运行保留学习状态，`--drafter-pattern-hints` 添加模式提示，`--self-speculation` 让 Drafter 根据 Actor 推理进行探测。插件不再预设请求数或输出 token 上限；`drafterBudget` 只统计请求数、已报告 token 和尚无 usage 的请求数。

`--paired` 对各实例和重复交替运行开/关两臂，报告 `pairedToolWaitRatio`（关 / 开的工具等待总时长，允许小于 1）。用 `swe_diverse` 与 `--repeats` 扩大评估；默认最多 128 轮。单个 `swe_smoke` 任务只用于检查流程，多实例、多次重复才支持自然任务收益结论。

## 验收与计量

正确性检查完整输入输出、后续模型 payload、thinking、工具批次、预算、usage、逐步文件效果、最终回复、一次执行与零残留。乱序用例暂停前驱，确认独立后继提前完成、依赖写入保持私有，错误观测只撤销依赖预测；Pattern 使用真实批次顺序进入 Store。模型任务结束后在独立副本验证原始基线和补丁的 `FAIL_TO_PASS/PASS_TO_PASS`；`patchCandidate` 只表示运行结束、补丁干净且文件有交集。

`T = toolComputeMs` 包含调用实际消费的执行、首次证明与封存，以及生产者在 Actor 发出调用后完成的部分。`H = hiddenComputeMs` 是其中实际复用且在调用发出前已完成的部分，包含跨轮次缓存命中。同一计算按身份去重，并行父子区间取并集。采纳当次的验新、拷贝与交付计入等待。按 `ΣT / Σ(T-H)` 汇总已测量的 `toolSpeedup`；T=H>0 记 `fullyHidden`、比值为 null，T=H=0 显示 n/a。`hiddenComputeIncomplete` 单独标记缺失证据。报告同时记录 `actionHitRate`、`subcomputationHits` 与 `reusedComputeMs`。`subcomputations` 按 namespace 汇总步骤的 `hits`、`misses`、`hitRate` 和 `reusedMs`。套件报告按各次运行的命中数与调用数计算汇总命中率。

`toolWaitMs` 为 Actor 调用区间的并集，覆盖准备至结算；`adoptionWaitMs` 扣除已消费的执行区间，表示纯协调等待。开关两臂都从实际事件记录计算、复用与回退，关闭预测不把已发生的复用记为零；缺少任务事件时，计算时长保持未知。任务时间从 Host 初始化前到终态回收完成；数据集下载、checkout、依赖安装及结束后的补丁验证不计入。CPU、反事实开销及端到端净收益须独立测量。

命令复用链逐项输出 T、H、未掩盖计算 T−H、掩盖比例 H/T 和计算加速比。直接执行、Actor 等待及采纳等待放在 `diagnostics` 中，均不参与该加速比；失败后回退时，未被最终消费的借用计算不计入 H。

报告包含成功、失败、较慢和零命中样本。原计算来源在产生时固定，借用与恢复保留归属，失配结算不因晚到复用改写，真实复用仍计 H。`traces` 保留来源请求、预测、候选及 Actor 原事件，以 turn/decision/prediction/candidate ID 关联；预算拒绝见 `source_request.empty.cause`。事件时间为 epoch 毫秒，单调计算端点加 `metadata.monotonicTimeOrigin` 对齐；完整计算图、区间和原点按 `actor_issue_boundary_compute_v3` 保存并按版本比较。

`traces.slowCalls` 排列等待至少 500ms 的调用，连接唯一结算及相关预测，重复调用 ID 标记歧义；`summary.slowCallCoverage` 按逐调用累计等待计占比，与并行区间并集分开。缺失或损坏计时证据列入 `unmeasuredRuns`，失败列入 `invalidRuns`，已确认 H 和等待覆盖保留在 `diagnostics`；复用仍由执行证明决定。prompt 或回收失败时保留 `benchmarkErrors`、计时和 usage 后以失败退出，套件保留已有报告并停止后续任务。
