# Pi speculative action

This standalone package adds speculative tool execution to Pi without modifying Pi core. It predicts future tool calls with Drafter and PatternAware, executes only actions with a proven isolation route, and lets the Actor adopt a matching result.

The repository is deliberately independent of the Pi monorepo: it has its own Git history, build configuration, tests, and dependency lock. Pi packages are declared as peers. Canonical path identity lazily uses the installed Pi path resolver (not currently a public export); an incompatible layout declines caching. It does not import a Pi source checkout, use workspace path aliases, or require a matching `main` branch.

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
| 1 | Installed unified environment | An injected runtime-wide world; preferred when qualified for this tool |
| 2 | Local `runtime_sandbox` | The qualified Linux/WSL process world; independent of the unified-environment switch |
| 2 | `resource_snapshot` | Observe Actor reads, or execute explicitly bound stock `read` / `ls` operations over bounded, sealed inputs; never permission to pre-execute an arbitrary host function |
| 2 | `workspace_branch` | Local fallback for `write` and `edit` using a private Git worktree and conflict-checked commit |
| 3 | Actor fallback | If no safe route exists, no speculative tool invocation occurs |

On Linux and WSL 2, the default extension includes a lightweight process world. It forks the operation into the same private workspace primitive used by mutation tools, then confines it with Sandlock's Landlock/seccomp policy and virtual filesystem. The current provider deliberately creates no user, PID, or mount namespace, so commands retain the Actor's native identity. Failure of any kernel, binary, or policy probe removes this route; Windows, macOS, WSL 1, and incomplete Linux installations therefore keep Pi's ordinary Actor execution rather than silently weakening isolation.

Startup diagnostics intersect each execution world's effect guarantees and explicit executor bindings with each tool's requirements. With qualified Pi 0.84.1 bindings, the portable resource route enables `read` / `ls`; Git enables `write` / `edit`; a ready Linux/WSL 2 process world adds `bash`. An injected world can enable more tools when it supplies their required guarantees. A selected tool with no safe route keeps its preference but remains inactive and is not sent to prediction sources. Native Windows and WSL are separate execution environments; macOS still requires real-host qualification.

The resource route reuses Pi's own file-tool implementation, including image handling and output formatting. The existing token owns bounded inputs captured on access, their evidence, and their lifetime; there is no additional cache. Sealing stops further capture: another query may recompute over these inputs, but cannot expand their authority. Exceeded budgets, escaping links, unsupported content reads, or changed input semantics reject adoption. Host Actor output needs a separately proven execution window, including ancestor path bindings. This observation capability is unavailable on Windows: restoring the same renamed directory/junction can preserve its inode and timestamps. Controlled `read` / `ls` speculation and its cross-turn reuse remain available; native Actor execution stays unchanged. Image options and model image support enter executor identity. Pi's private path/MIME helpers are version-qualified; other versions get observation only where proven. Native `grep` / `find` still require a complete process provider, including configuration, subprocesses, and context-file reads.

Process interception is structural. A single async process outlet preserves each Pi tool's validation, streaming, truncation, and result formatting. Inside the Linux world, exact exec-only mappings leave `PATH`, file opens, metadata, directory contents, writes, cwd, and environment unchanged while routing executable launches to one broker. On x86-64 Linux, a native Actor boundary also stops real child `execve` events before their first instruction. Both routes use the same executable/argv/cwd/environment/process-context key, dependency certificates, result journal, and planner—not a Bash-text cache. A validated child can therefore be reused under a different parent Bash string, while a miss continues the Actor child exactly once.

Each reusable result is a persisted provenance certificate containing dynamically observed files, directories, negative lookups, symlinks, executable/DSO identities, ordered stdout/stderr, exit status, and atomic regular-file effects. Every dependency is revalidated before reuse. The enclosing speculative branch separately records top-level process provenance and revalidates it immediately before Actor adoption; tainted, incomplete, stale, interactive, mutable-host, network, IPC, unsupported, or observably different confinement state fails closed.

Lookup follows a weak-exec-key / dynamic-pathset / strong-input-key hierarchy. Historical certificates that observed the same pathset share one current-world capture; observation semantics such as dependency role, metadata policy, negative-parent enumeration, and private-entry exclusions remain part of the pathset identity. See the research and platform notes and the WSL2 multi-history qualification.

Before replay begins, the complete content-addressed output/effect closure is loaded and integrity checked once. Output wire data and all file-effect bytes are materialized before the transactional commit, so replay neither reopens a deleted CAS blob nor begins a fallback-prone partial adoption. The 128 MiB WSL2 qualification measures the resulting I/O reduction.

Nested process misses use the same generic workspace-transaction outlet as the outer execution world. Fenced, content-free inode change tokens select candidate paths; immutable baseline/prior-frontier bytes and stable descriptor reads remain the exact authority. Overlap, a non-advancing filesystem clock, or unsupported inode semantics disables publication instead of guessing. The transaction driver initializes lazily, so replay-only branches do not pay its observation cost. This removed whole-tree content snapshots and reduced qualified cold Pi Bash latency by 7.4% on WSL2.

## Correctness boundaries

- Prediction sources never choose an execution backend.
- `K(a)` never changes because a different isolation backend is available.
- In-flight and cached work is reused only when its producer proof is accepted by the consumer; observations of confinement state remain reusable inside the same confinement domain but cannot cross into the Actor domain.
- Cross-parent process results are reusable across turns only after exact prototype matching and full dynamic-dependency validation; the parent shell command is deliberately absent from that nested key.
- The Linux world preserves native UID/GID/process identity and the visible `PATH`, maps only the private workspace onto its logical source path, denies common credential stores and the certificate store, and permits persistent writes only in the private branch.
- Broker uncertainty is at-most-once: after a request may have executed, a lost reply returns an error instead of re-running the command.
- On a read-only Actor fallback, a capture-capable world snapshots freshness before the host call and seals that same authoritative output into the shared cache. It never invokes the tool a second time; later turns still repeat authorization, exact freshness validation, compatibility, projection, and commit.
- Actor adoption still requires action equivalence, permission, fresh resource evidence, compatible world evidence, successful projection, and successful commit.
- Shared results require a backend freshness proof on every reuse; missing or failed validation declines adoption. Actor writes and watcher events cannot replace that proof or blindly evict sealed results. Pending work and checkpoint descendants still invalidate conservatively; watchers remain evidence against an unstable Actor observation window.
- A tool with neither a runtime sandbox nor a registered local fallback is execution-blocked but remains matchable for learning and counterfactual measurement.
- Same-name custom tools remain authoritative and are excluded unless the host explicitly supplies matching semantics and execution capability.
- Input evidence protects the bound file operations' content/path semantics, not zero host metadata effects: sampling can update access times. Content reads of known non-regular inputs are rejected before opening, but this JavaScript capture is not kernel protection against concurrent hostile replacement with a device. Workloads requiring metadata/event-level noninterference need a separately qualified snapshot or isolation provider; the resource route does not provide that guarantee.

`read` retains lossless range projection backed by realized output coverage. Other queries may be recomputed only from a world's sealed inputs; an input lookup is not output equivalence. Metadata evidence follows consumed fields: `ls` and captured `find` need names/types, not file sizes, so content-only growth need not invalidate them. A subsequent content read still requires its own size/content proof. Bash reuse requires an exact whole-command match or native process certificates. A textual suffix such as `| tail -n 20` is not a proof: `tail` may be a function or a different executable with different output, effects, or exit status.

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

Linux and WSL 2 expose independent capabilities rather than one all-or-nothing backend:

| Capability | Minimum host support |
| --- | --- |
| Replay an existing complete command certificate | Node.js and the local certificate store; no Landlock or `strace` on the hit path |
| Reuse a completed or running child under a different Actor Bash | x86-64 Linux, the small native helper, `ptrace`, and `pidfd_getfd` for Pi's pipe descriptors |
| Produce new speculative Bash certificates | Git, `strace`, a C/Rust build toolchain, the packaged pinned Sandlock build, and a successfully qualified Landlock/seccomp virtual-root policy |
| Accelerate large workspace transactions | Optional, hash-verified `fuse-overlayfs`; Git remains the safe fallback |

For all tiers, install the available system dependencies and run the best-effort qualifier:

```sh
sudo apt-get install git strace build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
npm run setup:linux
```

`setup:linux` qualifies and reports each row separately. It builds the packaged held-exec helper, applies the packaged transparent-exec change to the exact pinned Sandlock revision, installs the resulting plugin-specific binary only after behavioral probes pass, and optionally installs an official hash-verified `fuse-overlayfs` release under `~/.local`; it installs no daemon and does not alter Pi. Source/patch and installed-binary digests are stamped together. Missing Landlock, Rust, `strace`, FUSE, or the native helper disables only the dependent operation. Runtime probes repeat before use and fail closed. WSL must be version 2, and performance-sensitive workspaces should live in its native Linux filesystem. Detailed policy, transaction, and storage qualification is documented in the capability lattice.

Cross-platform captured-input `find` uses only Pi's already-installed minimatch/ignore components and Node; it adds no package, binary, download or setup command. Select **Tools & execution → Execution routes → Search execution → Captured find**, then Apply. Actor and speculation share this explicit executor (`searchExecution: "captured"`); **Native Pi** is the default. It respects workspace .gitignore layers, preserves filename spelling (including Unicode), uses deterministic case-sensitive glob matching and sorting, and is not identical to native fd. `grep` remains native or uses an available unified execution environment; there is currently no qualified local cross-platform grep pre-execution route.

The existing resource/candidate store owns adoption and retained-input recomputation. Missing or unqualified optional search components quietly bind Native Pi, without installation or error prompts; explicit refresh checks again. Once a captured executor is bound, unproven inputs or budget limits (8 MiB input, 1 MiB result, 5-second execution deadline including startup) still fail that call, never switch its semantics. Cancellation also waits for owned input operations to finish cleanup. Workers start on demand; disabling drains Actors, cancels producers and restores Native Pi. Obsolete search settings fall back to the default without migration prompts. Windows and WSL are qualified; macOS still needs a real-host run. This fixed trusted executor is not an arbitrary Bash or hostile-JavaScript sandbox.

The `pi.extensions` manifest points to `src/extension.ts`, which Pi loads through its public TypeScript extension loader. Git installation therefore does not depend on checked-in build artifacts or dev dependencies. `dist` is only the conventional JavaScript/types entry point for npm consumers and is generated during `npm pack` or `npm publish`.

Programmatic consumers should use the narrow npm entry matching their layer: `./core` for the host-neutral runtime and effect transaction contracts, `./process-reuse` for provenance certificates/planning/CAS, `./pattern-aware` for learning, and `./extension` for Pi integration. The root entry remains as a compatibility aggregate. The dependency closure of `./core` and `./process-reuse` is tested to contain no Pi package.

Open `/speculative-action` in the TUI. The first level contains only the main switch, save location, the model-Drafter/Actor-fork/learned-pattern source choices, and tool policy. Sampling, decoder protocol, benefit gates, scheduling, and storage limits live under **Advanced settings**; disabled gates and transport-inapplicable action-handoff fields are hidden. The menu no longer exposes L1/L2 or internal `sandbox` types. **Execution routes** exposes the actual hierarchy: an installed unified environment, local safe fallback (sealed inputs, workspace transactions, or qualified processes), then an Actor route that is always available. The first two layers can be staged independently; disabling them does not disable Actor observation or Bash history replay. Its per-tool matrix separates **Predict**, **Replay**, **Observe**, and **Fork**, so enabling a prediction source is not confused with the platform's ability to execute that tool early. Capability diagnostics initialize only while the plugin is enabled: opening **Execution routes** refreshes enabled providers, while disabled routes remain unprobed and start no helper. Applying the main switch or route policy settles route diagnostics before the UI reports completion. Turn registration is memory-only, so a layer can be enabled mid-turn. Explicit refresh rechecks a previously disconnected Runtime. Disabling Bash prediction does not disable Actor Bash history replay. Every edit—including Enabled and Restore defaults—is staged until Apply; switching “All projects”/“This project” reloads that layer, while a project file stores only differences from normalized shared settings. Actor-path Bash reuse and process reuse inside speculative branches have separate counters. Status reports producer-observed process work reused separately from an Actor-path latency estimate; until prior authoritative samples calibrate that estimate it says `Actor timing unavailable` instead of inventing saved time. Same-run overlap remains labelled as observed overlap, not causal speedup. JSON byte limits use bytes, TUI memory inputs use MiB, and work without a safe route remains with the Actor.

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
  "executionRouting": { "primary": true, "nativeFallback": true },
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
    "draftBoundary": "auto",
    "forkMaxTokens": 128,
    "forkTemperature": 0,
    "forkDecoder": "auto",
    "forkForcedPrefix": "auto",
    "timeoutMs": 2000
  }
}
```

`candidateLimit` defaults to two concurrent one-action Drafter requests per Actor decision. At width two they act as a latency hedge: the first response containing a schema-valid enabled `K(a)` is admitted and its still-running peer is canceled through the provider `AbortSignal`; errors, empty responses, and invalid calls do not win. Strict action and target-token replay retained all 6 available exact hits, the complete lead, and identical D3 verifier work, while identifying 7.78% of width-two Drafter service as removable residual work. Widths above two retain every completed sample and remain available without a hidden cap for models that produce useful wider diversity.

`drafterGateEnabled` defaults to `true`. It treats the concurrent root requests as one batch and learns their action-side net utility: actual tool `executionAheadMs` credited only to Drafter-owned adopted work, minus the summed service time of every request in the batch. Four warm-up batches precede suppression; one bounded probe every four skipped decisions lets a changed workload recover. Set it to `false` to restore unconditional Drafter batches. PatternAware candidates and Drafter continuations are not gated.

`drafterMaxDepth` is the number of output-informed successor requests allowed after each initial one-action Drafter request. A successor occupies that source's existing slot for the next Actor decision rather than increasing per-decision request width; set it to `0` for single-step Drafter behavior.

`drafterMaxTokens` is an optional hard cap. Omit it—or clear the TUI field—to use the provider's output limit, which avoids truncating long commands and structured tool arguments.

The Drafter always receives the same complete history as the Actor and uses the lowest thinking level its Pi model metadata permits (`off` when supported). Before either a root or output-informed request, the source compares that history plus the requested output allowance with the Drafter model's own `contextWindow`. A shorter model that cannot fit it is skipped locally: the plugin never truncates, summarizes, or triggers a second compaction path for the Drafter.

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

The JSON file additionally accepts `requestIDField` and all three route paths. JSON and TUI expose the common endpoint, bearer-token environment-variable name, limits, fork-gate policy, tool-call format, decoder, temperature, and expert syntax overrides. Boundary and forced-prefix overrides default to `auto`, so the inference adapter derives CoT closure, the name-aligned probe prefix, parser framing, and D3 boundary from one model format. An explicit override must describe that same format. These control routes can alter inference and should remain private or sit behind an authenticated proxy; `apiKeyEnv` reads only the named environment variable and never stores its value.

For Qwen3.5-family checkpoints, including Qwen3.8 deployment aliases, set
`draftFormat` to `qwen_xml` when the Actor provider uses the model's native
`tools=` chat template. Keep `tagged_json` only when the Actor request itself is
rendered with the paper-aligned JSON protocol provided by the companion
`self-speculation` package. Changing only the sidecar fork to JSON would make its
prefix differ from the Actor and prevent exact KV-cache reuse. Pi never embeds a
Qwen boundary token ID; the inference integration derives it with the target
tokenizer.

When PatternAware multi-step mode is enabled, each authoritative Actor action—including a Drafter result adopted by the Actor—is projected together with its actual output and used for a non-mutating, same-turn prediction rebase. Learning remains deferred to the normal authoritative batch boundary. An unchanged cross-turn `K(a)`/horizon set is carried forward instead of re-issued, preventing a losing alternative from restarting after a shared winner is adopted.

The former `resourceCached` / `sandbox` / `predictionOnly` object is accepted only as a migration input and is normalized to the single `tools` list.

## ThinkThread profile (Linux)

The optional `./thinkthread-extension` entry uses ThinkThread to pre-execute `read`, `ls`, `write`, and `edit` without changing Pi or replacing the default Linux backends. Install and launch it from Linux (Orb on macOS):

```sh
./scripts/install-thinkthread-profile.sh
cd /path/to/project
tt pi-speculative-action
```

The installer accepts `--agent-posix-package /path/to/sdk.tgz` and `--speculative-action-package /path/to/spec.tgz` for prebuilt packages, and repeatable `--model provider/model` delegation. It pins Agent POSIX SDK 0.1.0, verifies protocol 2 and the contract fingerprint, and installs a schema-4 profile plus a private runtime under `~/.local/share/pi-speculative-action`. Pi, Node, `fd`, and `rg` must already be available to the profile. Profile settings live under that installation's `config` directory; project `.pi/speculative-action.json` overrides still apply.

The profile shares the existing execution and observation boundaries:

- `speculation.execute`: `read`, `ls`, `write`, and `edit` use Pi's stock implementations against a turn-shared BASE, sealed `fs.run`, then `fs.verify`/conflict-checked `fs.apply`. Eight executions can share a BASE. Actor mutation fallbacks invalidate it before Runtime settlement can launch successors.
- Actor `read` and `ls` results use the host's existing resource observation provider, including its stable execution-window proof. ThinkThread snapshot/content equality alone cannot prove that a live Actor call did not observe an intermediate A→B→A state, so there is no second ThinkThread Actor-result cache. This path needs neither the SDK nor the speculative runner. `EffectTransaction` owns adoption state for both operations.

The profile's ThinkThread world is tried first; the native Linux process provider and Git workspace provider remain downstream routes. Bash is outside the portable runner and uses the native process world only when its actual environment passes qualification. Registration does not guarantee that an outer ThinkThread sandbox permits nested tracing, helpers, or handoff. In particular, the public ThinkThread Runtime is currently aarch64-only while Actor held-exec supports x86-64 Linux. Unavailable capabilities fall back to the Actor; full feature or performance parity under ThinkThread is not yet qualified. Ordinary source loading still uses the unchanged default provider and never loads the optional SDK.

`fs.run` inherits the profile's fixed network policy (`all` in the supplied profile); time and randomness remain real. Only the qualified fixed stock-tool runner is admitted. Stock `grep/find` inherit executable and external configuration inputs (including rg preprocessors); neither workspace-only observation nor current ThinkThread verification proves their closure. Their declared host-process effects therefore block these routes, leaving Actor execution unchanged. A provider must prove the actual process dependencies/effects before re-enabling them; there is no configuration-file blacklist. Bash likewise requires the native process proof. Workspace verification is not a complete dynamic process-dependency certificate, and no per-run network narrowing, time/random virtualization, or strict process-certificate equivalence is claimed. Supervisor-owned requests support durable recovery and terminal-record cleanup, but this adapter does not persist request IDs across a Pi-process crash.

The pinned Agent POSIX SDK archive is a locked development dependency and the installer's default payload, so a clean `npm ci` can check, test, build, and pack the optional adapter without a sibling checkout or manifest mutation. `--agent-posix-package` remains available for an explicit offline override. The profile defaults to two Drafter requests and eight concurrent tool executions; these remain configurable through `/speculative-action`.

## Runtime sandbox integration

The Pi extension registers configured runtime providers first, then preserves the native Linux process world and Git workspace fallback. `createExecutionWorlds` therefore extends the route hierarchy instead of replacing its safety fallback. The lower-level host API still accepts an explicit `executionWorlds` list for embedders that own the complete lifecycle. Each world declares effect guarantees and, where needed, a supported tool scope; both constrain routing, including warm-up before a concrete action exists. The router confirms availability before returning a route and rechecks routing policy before execution. An unavailable primary falls through before execution; a failed execution is not blindly rerun in another provider. Every successful backend—including process provenance, resource snapshots and Git worktrees—returns the same sealed `WorldBranch` artifact. The gateway wraps it in one `EffectTransaction`, which exclusively owns freshness validation, adoption, abort and commit state while the artifact retains compatibility evidence and backend-local cleanup.

```ts
const workspace = new WorkspaceSandboxService();
const host = createSpeculativeActionHost(sessionID, {
  cwd,
  executionWorlds: [runtimeSandbox, workspace.createExecutionWorld()],
  // model, policy, and tool integration omitted
});
// At the end of the owning session:
try { await host.dispose(); } finally { await workspace.dispose(); }
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
