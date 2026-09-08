# Speculative-action ablation benchmark

The process dependency combinations and mandatory Direct/Trace/Publish/Whole-hit/Child-hit/
Child-join/Fork-miss/Fork-hit comparison are specified in
`../docs/bash-reuse-capability-lattice.md`. Existing Linux
qualifications below cover a subset of that matrix; a report must not claim the full dependency
ablation until every applicable row has been measured on the same machine and initial snapshot.

This runner measures one speculative execution and reconstructs its serialized
counterfactual from the same authoritative timeline. It never compares against
a separately generated Actor trajectory.

Ablation preregistrations, private recordings, and raw JSON runs are intentionally
kept outside this repository. The checked-in runners remain the reproducible
measurement surface.

Measure the cost and semantic boundary of pass-through process tracing separately from provenance
capture. The runner compares a fork/exec-event-only ptrace loop, ordinary process-filtered strace,
and strace's seccomp-BPF acceleration; it also proves that a held x86-64 exec can be replaced before a
five-second child begins, while recording the observable `TracerPid` difference:

```sh
npm run bench:exec-boundary -- --output bench/results/local-exec-boundary.json
```

## Dependency-free grep feasibility probe

```sh
node --experimental-strip-types bench/grep-captured-qualification.mjs --semantics-only
node --experimental-strip-types bench/grep-captured-qualification.mjs --links-only
```

The low-load mode checks small nested fixtures, native glob/ignore precedence, explicit ignored roots,
mixed raw UTF-8/UTF-16/binary inputs, exact stock-Pi formatting/context/truncation, complete host adoption,
and exactly-once native Actor fallback after content, ignore-rule or negative-name changes. An ignored 16 MiB sparse
file must never enter the payload evidence. One actual-rg abort barrier verifies native close and borrowed
input cleanup before settlement; a tiny query may finish before its result-limit cancellation arrives.
`--case=<label>[,<label>...]` selects semantic cases. The separate 16-process link characterization checks both
`--files` and actual search: on this Windows rg, a discovered dangling junction is absent from the file
list but makes search fail. No-follow captured entries therefore cannot alone authorize skipping it.
The 33 semantic cases per OS cover named same-volume links, POSIX linked ignore files and skipped FIFO/
discovered links, ancestor configurations, Git indirection and explicit Git-directory/data searches.
Broken targets, cross-volume inputs and directory aliases combined with globs remain unqualified; the
original-workspace Actor runs once, including its original error. No native-default behavior changes.

Only the already-installed rg is used: a stable capture pins its SHA-256 and a task-owned executable copy.
The original tool runs in separate bounded Actor/producer processes through the existing input protocol.
Metadata and ignore-file evidence drive rg's own file selection; only selected raw bytes are copied.
Directory expansion is lazy: temporary, uniquely named marker files test the next frontier with rg;
the final marker override matches only these files, never their parent directory names. Ordinary,
caller-glob and query-local selections retain native directory precedence and explicit-root admission.
Ignored and negative-glob subtrees must not be enumerated, including `.git` unless explicitly queried.
Named parent configurations are captured up to the declared read-only volume root, without enumerating
ancestors or recursively watching that root. Private path geometry preserves anchored rules; Git
`gitdir`/`commondir` pointers address only owned controls, while selected configuration data retain their
original bytes. Creating a formerly absent commondir or changing its exclude file invalidates adoption.
A tiny ignored-subtree mutation retains adoption without extra Actor execution.
This bounds ignored-tree metadata without a large fixture, but adds selection processes per depth.
Provider-owned ancestor-directory overrides preserve explicit-root admission without interpreting user
globs. Native [root admission](https://github.com/BurntSushi/ripgrep/blob/15.1.0/crates/ignore/src/walk.rs) and
[override semantics](https://github.com/BurntSushi/ripgrep/blob/15.1.0/crates/ignore/src/overrides.rs) remain the reference.
Keep native repository detection: `--no-require-git` also disables `.git` file indirection in the tested
engine. Windows rg 15.2.0 and WSL rg 14.1.0 differ on `.jj` recognition; the same captured marker is
compared against each pinned native engine, not a cross-version assumption.

Omit `--semantics-only` only for a performance-stage run: larger flat fixtures, five host adoptions per
query, and 20 abort/output-budget barriers. Native Pi, fixed-sort host Pi, startup, producer and ready-hit
timings stay separate; the small-mode timings are not performance claims. The proposed profile explicitly
disables ambient rg/global ignore configuration and sorts output; ancestor ignore files remain inputs.
Native parent loading is disabled only on the fully materialized private namespace, preventing access
to uncaptured host ancestors. This is **not** native-default equivalence or production admission:
private materialization/selection cost, alias/glob namespaces and existing benefit gates still need
qualification. Nothing is installed or downloaded; missing rg skips.

## ThinkThread real Runtime qualification

From a source checkout with its real Agent POSIX SDK installed and `npm run build` complete,
start the installed `tt pi-speculative-action` profile. Ask Pi's Bash tool to run, at the
`THINKTHREAD_FS` root (the checkout):

```sh
npm run bench:thinkthread-tools
```

The command requires a live SDK `selfView` and `fs.stat` before writing any fixture; setting
`THINKTHREAD_FS` alone is not sufficient. Each qualified stock route compares an Actor baseline,
its native fallback, and actual ThinkThread `fs.run`/validate/commit through the production gateway.
It checks exact serialized output at the same cwd/path, all fixture entries/contents/modes, and
no fixture changes before adoption. Native read/grep/find/ls are explicitly **Actor only**, not
early host execution. A failed primary selection is a failed qualification, never a native pass.
Preparation, producer execution, and ready-candidate adoption timings are separate single samples;
the Actor/adoption ratio is not an end-to-end workload guarantee. Poisoned adoption retains the
owned fixture without further workspace writes.

The regular unit suite reuses this fixture only with the **local wire runner**, not a Runtime.
Bash under the real profile must separately pass the native process/in-flight qualifications below;
conflict, cancellation/recovery, and implicit dependency closure also remain separate release gates.

## Linux/WSL process reuse qualification

Run the production `createBashTool`, generic process outlet, Linux execution
world, capability-selected workspace branch, adoption-time freshness validation, and commit path
against real Linux processes:

```sh
npm run setup:linux
npm run bench:linux-process -- --output bench/results/local.json
npm run bench:linux-inflight -- --output bench/results/local-inflight.json
```

The in-flight qualification lets one Actor arrive while a PID-observing speculative Bash is running,
then repeats the same Actor action. The Runtime must validate and claim the branch exactly once; the
second call must execute normally, and the one-shot result must never enter persistent history or a
second top-level transfer registry.

Pass `--workspace-driver git` or `--workspace-driver overlayfs` to the process
and topology qualifications for a same-machine A/B. `auto` uses OverlayFS only
after its binary, FUSE device, copy-up, 0/0 whiteout, opaque-directory, private
anonymous clock, cross-view timestamp ordering, namespace visibility, and unmount
lifecycle all pass and the exact baseline contains at least 256 entries. Small
trees retain Git. Use `--source-files N` with `bench:linux-topology` to reproduce
the storage-driver crossover; driver-induced unsupported filesystem results are
trace-tainted and cannot be adopted. Reports include the one-time
`routePreparationMs` separately from fork/hit latency.

The fixture first runs one workload directly and under the production strace shape, requiring equal
output, exit and file effects while reporting trace cost, observed taints and strict-certificate
eligibility. It then requires the direct Actor outlet to reject the sandbox certificate, repeats the
command inside the full world, and uses a different parent around the same compiled child to require
child-process hits. Finally it changes a dynamic input and requires a miss. This is a substrate
benchmark, not a model-quality benchmark; avoiding an LLM request makes the command identity and cache
decision deterministic and repeatable.

Exercise the weak-key / dynamic-pathset / strong-key lookup with eight historical
contents for one 32 MiB input, then return to the oldest state under a different
parent Bash command:

```sh
npm run bench:linux-pathset -- --output bench/results/local-pathset.json
```

The hit must consider all eight certificates while capturing their shared
dependency pathset once. This catches a common cache pathology where reverting a
large input causes the validator to hash the same files once per historical
certificate.

Exercise artifact-closure verification and replay with a 128 MiB regular-file
effect and three different parent Bash commands:

```sh
npm run bench:linux-artifacts -- --output bench/results/local-artifacts.json
```

Every replay must hit and reproduce the input digest. A closure-capable backend
also reports the number and total size of artifacts integrity-checked before any
workspace effect begins.

Exercise typed directory topology plus a regular-file artifact from one child
process, either through completed replay or the direct stock Pi Bash control:

```sh
npm run bench:linux-topology -- --mode direct --output bench/results/local-topology-direct.json
npm run bench:linux-topology -- --mode reuse --output bench/results/local-topology-reuse.json
```

The helper creates two directories and deterministically transforms a 32 MiB
input. Reuse mode requires three different parent Bash commands to hit the same
child certificate, restore exact directory states and artifact bytes, validate
the outer branch, and commit it. Direct mode is the profitability control; the
checked-in qualification also preserves shorter workloads for which replay is
neutral or slower.

Use `--rounds N` (0 through 4096, default 96) on both commands to sweep the
execution/replay crossover. Feed the paired reports through the production
candidate-adoption policy rather than inventing a Bash-specific duration gate:

```sh
npm run bench:linux-admission -- \
  --direct bench/results/local-topology-direct.json \
  --reuse bench/results/local-topology-reuse.json \
  --expect join \
  --expect-ready join \
  --output bench/results/local-admission.json
```

`--elapsed-ms N` evaluates a partially completed speculative execution. The
analyzer learns separate direct Actor, speculative-world, and adoption-cost
distributions from every measured run and fails if an optional expected
decision differs. The checked-in WSL2 report includes both retained wins and
negative boundary results.

Probe whether the Linux host supports unprivileged OverlayFS in a private user
and mount namespace:

```sh
npm run bench:overlay-probe
```

This inspects upper-layer records for the retained production driver. The rejected
private-mount experiment depended on `/proc/<pid>/root`; a later PID namespace can
remount `/proc` and lose that path, so its standalone probe has been removed. The
production driver uses a host-visible, unprivileged FUSE mount. Its upperdir still
contains whiteouts and opaque markers and is decoded as a typed frontier; it is
never merged into a workspace as an ordinary directory tree.

Analyze a private `pi-llm-tape` recording without replaying or exposing its
prompts. Actor and Drafter requests are paired only when their complete message
contexts match; tool name and all parsed arguments must match exactly:

```sh
npm run bench:tape -- \
  --tape /private/path/deepseek.json \
  --actor-model deepseek-v4-pro \
  --drafter-model deepseek-v4-flash
```

The report distinguishes raw and unique K(a), duplicate Drafter work, exact
hits, candidates ready before Actor completion, decode lead, and aggregate
Drafter service time. It also replays static Drafter request widths 1, 2, 3, and
8 in request-dispatch order. Width costs are charged once per Actor turn while
exact coverage remains action-scoped, so a multi-tool Actor response cannot
artificially multiply Drafter work. Recordings remain private and are never
copied into the repository.

The checked-in suites select real GitHub issue-resolution tasks from
[Claw-SWE-Bench Lite](https://huggingface.co/datasets/TokenRhythm/Claw-SWE-Bench).
The runner fetches only the selected base commit into a bare cache, creates a
fresh detached workspace for every run, and never exposes the gold patch to the
agent.

Run one task from the standalone repository root:

```sh
DEEPSEEK_API_KEY=... npm run bench:ablation -- \
  --instance axios__axios-5316 \
  --label baseline \
  --latency remote \
  --candidate-limit 8
```

The default 128-turn budget is deliberate: both 16- and 64-turn runs repeatedly
ended before the Agent completed otherwise plausible patches. A result with
`turnLimitReached=true` is incomplete and cannot support an algorithm-retention
decision.

Use `--drafter-max-depth N` to ablate output-informed Drafter successors. The
default is `1`; `0` preserves single-step behavior. Continuations replace the
same source's request for the next decision instead of increasing request width.

PowerShell:

```powershell
$env:DEEPSEEK_API_KEY = Read-Host -MaskInput "DeepSeek API key"
npm run bench:ablation -- --instance axios__axios-5316 --label baseline --latency remote
Remove-Item Env:DEEPSEEK_API_KEY
```

Latency profiles add the same deterministic delay to Actor and speculative
executions. `native` adds nothing; `remote`, `sandbox`, and `heavy` model
increasingly remote or isolated tools. The runner uses resource-version routes
for read-only tools and the production Git-worktree world for file mutations.
No process sandbox is bundled, so Bash predictions are matched but execute only
through the Actor path unless the embedding host injects a runtime-wide world.

Use `--drafter-disabled --pattern-aware --pattern-state <directory>` to isolate
PatternAware. The explicit state directory also selects a stable logical
repository identity, so training and evaluation runs in fresh temporary
checkouts share learned patterns without sharing workspace files. Without
`--pattern-state`, PatternAware state is intentionally isolated to one run.

`actualEndToEndMs` is the single speculative Agent invocation. Its serialized
counterfactual is reconstructed from that same run as:

```text
serializedCounterfactualMs = nonToolMs + authoritativeToolMs
```

`authoritativeToolMs` includes only tool work started during this measured task
that produced results on the Actor's final path, including the service time of
an adopted speculative execution. It excludes unused predictions and cache work
from an earlier task. Agent-boundary overhead is counted as non-tool time, while
teardown after the Agent has completed is excluded.

`hiddenLatencyMs` is the total overlap exposed by this serialization and can
include native parallel Actor tool calls. `executionAheadMs` is the narrower,
directly observed execution head start of adopted speculative work. The two are
reported separately and are not subtracted into an invented causal estimate.

For a matched action whose execution was blocked by missing isolation,
`executionBlockedPotentialHiddenLatencyMs` reports the capped portion of the
authoritative Actor duration that the observed prediction lead could have
covered. It is a counterfactual and is never added to actual hits or
`hiddenLatencyMs`.

Required ablation discipline:

1. Keep task, model, candidate count, latency profile, and task timeout fixed.
2. Change one algorithmic factor per commit.
3. Require a clean patch (`git diff --check`) and retain task completion signals.
4. Compare hit rate, `serializedCounterfactualMs / actualEndToEndMs`, serialized
   overlap, execution ahead, tool work, and model cost together.
5. Require `patchCandidate=true` before using a run for latency comparison. This
   means the Agent ended below its turn limit with a clean, non-empty patch that
   overlaps a gold-patch file and no timeout or Agent error.
6. Treat `patchCandidate` as a screening gate, not correctness proof. Grade the
   recorded `FAIL_TO_PASS` and `PASS_TO_PASS` tests with the dataset harness.
7. Retain an implementation only when repeated runs improve latency without a
   correctness or resource regression.

The API key is read only from `DEEPSEEK_API_KEY`; it is never written to an
artifact or exposed to benchmark shell processes. Workspaces and JSON results
default to the operating-system temporary directory.

Use `--prepare-only` to verify dataset lookup and the fresh checkout without a
model request. Patch cleanliness and changed-file overlap are integrity signals,
not an official SWE-bench correctness score; official grading still requires the
dataset's language toolchain and container harness.

Run a checked-in suite serially with identical arguments:

```sh
npm run bench:suite -- \
  --suite swe_diverse \
  --repeats 3 \
  --label baseline \
  --latency remote \
  --candidate-limit 8
```

The suite runner fails immediately if a task process fails, writes every task to
its own result file, and emits `suite-result.json`. Pooled acceleration is the
ratio of mean serialized time to mean actual time (equivalently, the ratio of
their totals), never the mean of per-task ratios. The report includes a seeded
95% task-cluster bootstrap interval that keeps repeats of one instance together,
plus nearest-rank p95 latency. Pooled hit rate is total hits divided by total
Actor actions. Runs that fail `patchCandidate` remain listed with explicit reasons
but are excluded from pooled latency and hit-rate statistics. This screening is
not an official correctness grade; use the dataset harness for that guardrail.
Use `--output-root` to choose the artifact directory.

## 无新增依赖的受控搜索验收

先执行 `npm run build`，然后运行：

```text
node bench/portable-kernel.mjs
```

只使用 Pi 已安装的 minimatch/ignore 和 Node，不需要额外包、模块文件或安装步骤。
实际完整 Pi find、输入代理、Actor/producer 独立进程、现有 Runtime 和 TUI 均参与验收；
仅模型和界面输入使用脚本。覆盖完成/跨轮次采纳、封存输入重算、运行中 join、双 producer
并发、超预算的内容增长及 Actor 无关写入仍保留名字查询、ignore 变化与逃逸链接拒绝、Actor 单次回退及关闭时排空 Actor/取消 producer。
deadline/abort 还测试已进入的不合作循环及拥有独立进程的输入操作；guest 与输入均完成回收后
才允许回退，晚到成功/失败均被消费。V8 堆限额不是整个进程的 RSS 保证，不声称任意脚本沙箱。

原生 fd 与共同配置 Actor 分别采样，报告中列出两者的语义差异，不用投机端偷偷替换原生语义。
结果复用时间只与同配置 Actor 到达后的基线比较，不冒充整体任务加速；未校准时如实记录。
此资格不包括 grep、macOS 或 ARM64 ThinkThread Runtime；已有 Linux Bash benchmark 保持独立。
