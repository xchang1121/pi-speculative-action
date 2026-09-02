# Pi speculative action

This standalone package adds speculative tool execution to Pi without modifying Pi core. It predicts future tool calls with Drafter and PatternAware, executes only actions with a proven isolation route, and lets the Actor adopt a matching result.

The repository is deliberately independent of the Pi monorepo: it has its own Git history, build configuration, tests, and dependency lock. Its only Pi dependencies are the published public extension APIs declared as peers. It does not import a Pi source checkout, use workspace path aliases, or require a matching `main` branch.

This is a standalone GitHub repository. Its reachable history contains only speculative-action changes and no Pi monorepo parent or source tree.

## Architecture

The runtime has four independent layers:

1. **Sources** — the model Drafter, Actor probe, and learned patterns emit source-neutral `PlanAction` values.
2. **Action identity** — `K(a)` canonicalizes tool semantics, validated schema, arguments, resources, and executor identity. Lossless projection rules may prove that one result covers another action.
3. **Execution routing** — action semantics declare only observable effects; one `ExecutionWorldRouter` selects and prepares an isolation capability. The selected route is deliberately not part of `K(a)`.
4. **Scheduling and settlement** — the Scheduler controls launch timing and resource pressure; `ExecutionWorld` produces a sealed effect artifact, while one `EffectTransaction` owns validation, adoption, abort, and commit state. Settlement records match, adoption, fallback, and timing once.

Execution routes use this fixed priority:

| Priority | Route | Scope |
|---|---|---|
| 1 | `runtime_sandbox` | The built-in Linux/WSL process world or an injected runtime-wide world; preferred when its probes pass |
| 2 | `resource_snapshot` | Local fallback for `read`, `grep`, `find`, and `ls` using versioned resource evidence |
| 2 | `workspace_branch` | Local fallback for `write` and `edit` using a private Git worktree and conflict-checked commit |
| 3 | Actor fallback | If no safe route exists, no speculative tool invocation occurs |

On Linux and WSL 2, the default extension includes a lightweight process world. It forks the operation into the same private Git workspace primitive used by mutation tools, then confines the process with user/PID/network/IPC/UTS and mount namespaces plus Sandlock's Landlock/seccomp policy. Failure of any kernel, binary, mount, or policy probe removes this route; Windows, macOS, WSL 1, and incomplete Linux installations therefore keep Pi's ordinary Actor execution rather than silently weakening isolation.

Tool policy is not hard-coded by platform or tool name. Startup diagnostics intersect each execution world's effect guarantees with each tool's requirements: Windows, macOS, WSL 1, and incompletely qualified Linux hosts configure `read`, `grep`, `find`, `ls`, `write`, and `edit` by default; a ready Linux/WSL 2 process world adds `bash`; and a host-injected all-effect world enables every tool. A selected tool with no safe route keeps its preference but remains inactive and is not sent to prediction sources.

Process interception is structural. A single async process outlet preserves each Pi tool's validation, streaming, truncation, and result formatting. Inside the Linux world, mount-namespace views leave the command's `PATH` and environment unchanged while routing PATH-resolved execs to one broker. The broker identifies an exec by executable bytes, argv, logical cwd, complete environment, descriptors, credentials, limits, platform, and policy—not by its parent Bash text or tool name. This lets different Bash parents reuse one completed child process.

Each reusable result is a persisted provenance certificate containing dynamically observed files, directories, negative lookups, symlinks, executable/DSO identities, ordered stdout/stderr, exit status, and atomic regular-file effects. Every dependency is revalidated before reuse. The enclosing speculative branch separately records top-level process provenance and revalidates it immediately before Actor adoption; tainted, incomplete, stale, interactive, mutable-host, network, IPC, or unsupported observations fail closed.

Lookup follows a weak-exec-key / dynamic-pathset / strong-input-key hierarchy. Historical certificates that observed the same pathset share one current-world capture; observation semantics such as dependency role, metadata policy, negative-parent enumeration, and private-entry exclusions remain part of the pathset identity. See the research and platform notes and the WSL2 multi-history qualification.

Before replay begins, the complete content-addressed output/effect closure is loaded and integrity checked once. Output wire data and all file-effect bytes are materialized before the transactional commit, so replay neither reopens a deleted CAS blob nor begins a fallback-prone partial adoption. The 128 MiB WSL2 qualification measures the resulting I/O reduction.

Nested process misses use the same generic workspace-transaction outlet as the outer execution world. Fenced, content-free inode change tokens select candidate paths; immutable baseline/prior-frontier bytes and stable descriptor reads remain the exact authority. Overlap, a non-advancing filesystem clock, or unsupported inode semantics disables publication instead of guessing. The transaction driver initializes lazily, so replay-only branches do not pay its observation cost. This removed whole-tree content snapshots and reduced qualified cold Pi Bash latency by 7.4% on WSL2.

## Correctness boundaries

- Prediction sources never choose an execution backend.
- `K(a)` never changes because a different isolation backend is available.
- In-flight and cached work is reused only within an identical execution route.
- Cross-parent process results are reusable across turns only after exact prototype matching and full dynamic-dependency validation; the parent shell command is deliberately absent from that nested key.
- The Linux world preserves the visible `PATH`, mounts the private workspace over its logical source path only inside the namespace, denies common credential stores and the certificate store, and permits persistent writes only in the private branch.
- Broker uncertainty is at-most-once: after a request may have executed, a lost reply returns an error instead of re-running the command.
- On a read-only Actor fallback, a capture-capable world snapshots freshness before the host call and seals that same authoritative output into the shared cache. It never invokes the tool a second time; later turns still repeat authorization, exact freshness validation, compatibility, projection, and commit.
- Actor adoption still requires action equivalence, permission, fresh resource evidence, compatible world evidence, successful projection, and successful commit.
- A tool with neither a runtime sandbox nor a registered local fallback is execution-blocked but remains matchable for learning and counterfactual measurement.
- Same-name custom tools remain authoritative and are excluded unless the host explicitly supplies matching semantics and execution capability.

`read` supports lossless range projection backed by realized output coverage. `grep`, `find`, and `ls` remain exact-key-only.

## Install and use

The repository root is the Pi package root. A local checkout can be loaded or installed directly, without building Pi or editing its source:

```sh
pi -e /absolute/path/to/pi-speculative-action
pi install /absolute/path/to/pi-speculative-action
```

Pi can install the repository directly:

```sh
pi install https://github.com/xchang1121/pi
```

To enable process reuse, run Pi and the project inside Linux or WSL 2. Install Rust stable, Git, `strace`, and `util-linux`, then build the pinned Sandlock revision:

```sh
sudo apt-get install git strace util-linux build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
npm run setup:linux
```

`setup:linux` installs the pinned `sandlock` CLI and, on x86-64/aarch64 hosts with `/dev/fuse`, an official hash-verified `fuse-overlayfs` static release under `~/.local`; it does not alter Pi or install a daemon. The runtime probes Landlock ABI 6+, unprivileged namespaces, bind mounts, Sandlock, strace, and the complete OverlayFS copy-up/whiteout/anonymous-transaction-clock/unmount lifecycle again on every host. The Linux process world selects the host-visible COW route only when that probe passes and the exact immutable baseline contains at least 256 entries, the conservative measured same-machine boundary after lower-snapshot sharing; small workspaces and generic `write`/`edit` fallbacks retain Git. One driver-native lower structure snapshot is prewarmed and shared per content commit; outer and nested observers reconstruct each merged tree from that snapshot plus its typed upper journal. Its transaction clock is an unnamed `O_TMPFILE` in private upper-layer storage whose ordering against merged-view timestamps is proven by the probe, so Bash cannot observe a runtime control path. Driver-induced `EXDEV`, `EOPNOTSUPP`, `ENOTSUP`, or `ENOSYS` results inside the workspace taint the complete trace and prevent adoption; this covers operations such as lower-directory rename that FUSE cannot transparently reproduce. Any missing binary, unsupported marker, anonymous inode or clock projection, mount failure, or recovered lifecycle fault demotes later routes to the existing Git-worktree driver. An unresolved live mount and its pool are quarantined and retained rather than reclaimed, without blocking plugin shutdown. WSL must be version 2, and the checkout should live in its native Linux filesystem.

The `pi.extensions` manifest points to `src/extension.ts`, which Pi loads through its public TypeScript extension loader. Git installation therefore does not depend on checked-in build artifacts or dev dependencies. `dist` is only the conventional JavaScript/types entry point for npm consumers and is generated during `npm pack` or `npm publish`.

Programmatic consumers should use the narrow npm entry matching their layer: `./core` for the host-neutral runtime and effect transaction contracts, `./process-reuse` for provenance certificates/planning/CAS, `./pattern-aware` for learning, and `./extension` for Pi integration. The root entry remains as a compatibility aggregate. The dependency closure of `./core` and `./process-reuse` is tested to contain no Pi package.

Open `/speculative-action` in the TUI. The first level contains only the main switch, save location, the model-Drafter/Actor-fork/learned-pattern source choices, and tool policy. Sampling, decoder protocol, benefit gates, scheduling, and storage limits live under **Advanced settings**; disabled gates and transport-inapplicable action-handoff fields are hidden. The menu no longer exposes L1/L2 or internal `sandbox` types; it names live speculative results, reusable command history, and current execution routes by purpose. It reuses the capability diagnostics completed at session startup, so opening tool policy performs no new probe; only explicitly opening **Execution routes** refreshes backends. Prediction sources receive only registered tools with a safe current route. The tool-policy title explains `[x]` active, `[~]` selected but inactive here, and `[ ]` off. Every edit—including Enabled and Restore defaults—is staged until Apply; switching “All projects”/“This project” reloads that layer, while a project file stores only differences from normalized shared settings. Footer/status lead with reuse across all tool calls. When Bash launches reusable child commands, a separate secondary summary reports their hit rate, origin, and matched-command time saved; child-command and tool-call counters are never added together. Same-run overlap is labelled as observed overlap. JSON byte limits use bytes; TUI memory inputs use MiB; tools without a safe route remain with the Actor.

Settings are owned by the package:

- global: `<agent-dir>/speculative-action.json`
- project: `<workspace>/.pi/speculative-action.json`

Example:

```json
{
  "enabled": true,
  "draftModel": "deepseek/deepseek-chat",
  "drafterGateEnabled": true,
  "candidateLimit": 2,
  "maxConcurrentActions": 8,
  "resourceCacheMaxEntries": 512,
  "resourceCacheMaxBytes": 268435456,
  "executionStoreMaxEntries": 4096,
  "executionStoreMaxBytes": 2147483648,
  "drafterMaxDepth": 1,
  "tools": ["read", "grep", "find", "ls", "bash", "write", "edit"],
  "patternAware": {
    "enabled": true,
    "multiStepEnabled": true
  },
  "selfSpeculation": {
    "enabled": true,
    "endpoint": "http://127.0.0.1:8010",
    "forkTransport": "sidecar",
    "forkEnabled": true,
    "forkActionEnabled": true,
    "forkActionMinConfidence": 0.9,
    "forkGateEnabled": true,
    "forkGateMinSamples": 4,
    "forkGateWindowSize": 4,
    "forkGateMinNetBenefitMs": 25,
    "forkGateProbeInterval": 4,
    "forkGateFailureThreshold": 2,
    "maxCandidates": 8,
    "maxDraftTokens": 28,
    "draftFormat": "tagged_json",
    "draftBoundary": "<tool_call>",
    "forkMaxTokens": 128,
    "forkTemperature": 0,
    "forkDecoder": "auto",
    "forkForcedPrefix": "<tool_call>",
    "timeoutMs": 2000
  }
}
```

`candidateLimit` defaults to two concurrent one-action Drafter requests per Actor decision. At width two they act as a latency hedge: the first response containing a schema-valid enabled `K(a)` is admitted and its still-running peer is canceled through the provider `AbortSignal`; errors, empty responses, and invalid calls do not win. Strict action and target-token replay retained all 6 available exact hits, the complete lead, and identical D3 verifier work, while identifying 7.78% of width-two Drafter service as removable residual work. Widths above two retain every completed sample and remain available without a hidden cap for models that produce useful wider diversity.

`drafterGateEnabled` defaults to `true`. It treats the concurrent root requests as one batch and learns their action-side net utility: actual tool `executionAheadMs` credited only to Drafter-owned adopted work, minus the summed service time of every request in the batch. Four warm-up batches precede suppression; one bounded probe every four skipped decisions lets a changed workload recover. Set it to `false` to restore unconditional Drafter batches. PatternAware candidates and Drafter continuations are not gated.

`drafterMaxDepth` is the number of output-informed successor requests allowed after each initial one-action Drafter request. A successor occupies that source's existing slot for the next Actor decision rather than increasing per-decision request width; set it to `0` for single-step Drafter behavior.

`drafterMaxTokens` is an optional hard cap. Omit it—or clear the TUI field—to use the provider's output limit, which avoids truncating long commands and structured tool arguments.

The Drafter always receives the same complete history as the Actor. Before either a root or output-informed request, the source compares that history plus the requested output allowance with the Drafter model's own `contextWindow`. A shorter model that cannot fit it is skipped locally: the plugin never truncates, summarizes, or triggers a second compaction path for the Drafter.

### Actor probe and target verification

`selfSpeculation` is opt-in and is also gated by the package-level `enabled` switch. Its Actor probe is derived only from the authoritative Actor inference stream; the separate model Drafter is never self-forked. The same request-scoped coordinator also copies every schema-valid concrete model-Drafter or PatternAware prediction into one target-verification bundle. Decoder identity remains the exact Actor-visible `predictedAction`; a wider lossless `executionAction` is carried separately for scheduling and result reuse. Identical predicted keys are sent once with merged source/proposal provenance, including predictions that cannot be executed locally, so the target model can verify their boundary-relative tool-call tokens.

The coordinator binds one stable request ID to each Actor decision, submits the ranked bundle for that exact absolute decision sequence to `POST /self-speculation/candidates`, and clears it with `POST /self-speculation/clear` after all pending submissions and probes settle. Predictions for later decisions stay buffered until the matching Actor request starts; an unchanged decision retry inherits its bundle, while older predictions are discarded. Network and decoding failures are best-effort acceleration failures and never replace Actor behavior.

When the target returns a clear-time `verification` object, the coordinator records real proposed, accepted, rejected, and unresolved draft tokens separately from registration receipts. Candidate IDs and sources update a model/endpoint/format/tool/source decoder ledger, whose smoothed acceptance probability calibrates later candidate ordering. Runtime Actor settlement independently trains action utility: a sidecar fork receives benefit only when a matching prediction is actually adopted, using its source-attributed share of realized `executionAheadMs`. Token rejection never changes semantic action probabilities, and an action-key match alone no longer pays the fork gate. `acceptedDraftTokens` remains the registration acknowledgement counter for API compatibility and must not be interpreted as target acceptance.

There are two fork transports:

- `sidecar` posts the first Actor output snapshot and its original request context to `POST /self-speculation/fork`. A low-confidence result is retained for D3 and retried from a later Actor snapshot, with one probe in flight, a 50-update progress step, and a five-attempt bound. This is the portable reference path implemented by the companion `self-speculation` package. With `forkActionEnabled`, the earliest confident complete probe candidate re-enters the ordinary action Runtime as one atomic proposal: parallel tool calls stay together, while distinct candidate batches remain alternatives. Each call carries the batch's candidate IDs, source/proposal provenance, score, call identity, format, probe timing, and logprob evidence through Runtime feedback. The calls use the same schema validation, K(a) deduplication, execution policy, Scheduler, and Actor settlement as Drafter and PatternAware actions. The default `forkActionMinConfidence` of `0.9` admits the whole batch only when the minimum top-1 probability across its tool-name tokens reaches the threshold; argument-token uncertainty is deliberately excluded. Incomplete calls and missing or malformed evidence fail closed. Set the threshold to `0` to admit unscored batches.
- `provider` places a versioned `self_speculation` control object only in the authoritative Actor provider payload, including D2's five-attempt, 50-token cadence and minimum tool-name probability gate. Use it only with a provider that explicitly implements this SPORK contract and can expose the requested logprobs. Drafter-model requests are deliberately left untouched. Ordinary OpenAI-compatible servers may ignore unknown fields; field injection alone is not an implementation.

A positive tool-name confidence threshold automatically requests token probabilities. `requireLogprobs` remains a JSON compatibility override for collecting that evidence when early execution is disabled; it no longer needs a separate TUI switch.

For `sidecar`, the model-scoped fork gate learns a rolling net utility of `exact Actor lead - fork latency`. It allows four warm-up observations by default, suppresses a persistently negative fork, and still sends one bounded probe every four skipped decisions so a changed workload can recover. Two consecutive endpoint failures use the same probe circuit. All thresholds are configurable above; disabling `forkGateEnabled` restores unconditional forks. The same `fork_gate` policy is included as a provider/SPORK hint, but a provider transport must enforce that hint itself.

The default D3 cap is 28 draft tokens. In the strict DeepSeek-tokenizer tape replay, raising the former cap of 20 to 28 added 12 accepted tokens, no rejected tokens, and 10 saved target-step proxies; 32 added nothing further. This remains configurable and is bounded again by the inference engine.

The JSON file additionally accepts `requestIDField` and all three route paths. JSON and TUI expose the common endpoint, bearer-token environment-variable name, limits, fork-gate policy, tool-call format/boundary, fork decoder/prefix, temperature, and logprob requirement. The boundary, formatter, decoder, and target tokenizer must describe the same model format. These control routes can alter inference and should remain private or sit behind an authenticated proxy; `apiKeyEnv` reads only the named environment variable and never stores its value.

When PatternAware multi-step mode is enabled, each authoritative Actor action—including a Drafter result adopted by the Actor—is projected together with its actual output and used for a non-mutating, same-turn prediction rebase. Learning remains deferred to the normal authoritative batch boundary. An unchanged cross-turn `K(a)`/horizon set is carried forward instead of re-issued, preventing a losing alternative from restarting after a shared winner is adopted.

The former `resourceCached` / `sandbox` / `predictionOnly` object is accepted only as a migration input and is normalized to the single `tools` list.

## Runtime sandbox integration

The Pi extension registers the Linux process world followed by the Git workspace fallback by default. Hosts may replace that list through `executionWorlds`. Every world advertises effect capabilities rather than tool names; the built-in runtime world covers process invocation, while a host may inject a broader runtime sandbox. The router confirms backend availability before returning a route, so an unavailable runtime sandbox naturally falls through to a compatible local fallback. Every successful backend—including process provenance, resource snapshots, and Git worktrees—returns the same sealed `WorldBranch` artifact. The gateway wraps it in one `EffectTransaction`, which exclusively owns freshness validation, adoption, abort, and commit state while the artifact retains compatibility evidence and backend-local cleanup.

```ts
createSpeculativeActionHost(sessionID, {
  cwd,
  executionWorlds: [runtimeSandbox, createWorkspaceSandbox()],
  // model, policy, and tool integration omitted
})
```

The first available runtime-wide world wins for every process-backed tool whose execution context can be proven. Without one, the router considers fallback worlds compatible with the action's declared effects. Absence of both is represented by an undefined route, which the Runtime turns into `execution:isolation_unavailable` and an Actor fallback. Resolution, preparation, fork, and disposal all pass through the same router; tools cannot retain a direct backend handle. Persisted process certificates live under `<agent-dir>/speculative-action/process-reuse` and are content-addressed, policy-versioned, and safe to discard.

## Timing

For an adopted result:

- `attemptLeadMs`: speculative intent to Actor interception.
- `executionAheadMs`: speculative execution completed before interception, capped by measured tool duration.
- `hitLatencyMs`: interception to authoritative settlement.

For an isolation-blocked match, the Actor execution is authoritative. The same decomposition is reported as counterfactual potential:

```text
executionBlockedPotentialHiddenLatencyMs = min(actorDuration, predictionLead)
executionBlockedPotentialHitLatencyMs    = actorDuration - potentialHidden
```

These values never inflate actual speculative hits or actual hidden latency.

## Validation

```sh
npm install --ignore-scripts
npm run check
npm run build
npm test
npm run bench:check
# Linux/WSL only: production Pi Bash tool + process-world qualification
npm run bench:linux-process
npm run bench:linux-pathset
npm run bench:linux-artifacts
npm pack --dry-run
```

See [bench/README.md](./bench/README.md) for the single-trajectory ablation methodology.
