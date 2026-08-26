# Speculative-action ablation benchmark

This runner measures one speculative execution and reconstructs its serialized
counterfactual from the same authoritative timeline. It never compares against
a separately generated Actor trajectory.

Ablation reports, preregistrations, and private recordings are intentionally
kept outside this repository. The checked-in runner and suites remain the
reproducible measurement surface.

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
ratio of total serialized time to total actual time, and pooled hit rate is total
hits divided by total Actor actions. Runs that fail `patchCandidate` remain
listed with explicit reasons but are excluded from pooled latency and hit-rate
statistics. Use `--output-root` to choose the artifact directory.
