# Changelog

## [Unreleased]

### Breaking Changes

- Replaced the separate `keyProjectors` and `projectOutput` integration hooks with unified `projectionRules`; each lossless rule now owns its key relation, realized coverage proof, output reconstruction, and directed in-flight compatibility.
- Added required `ActionKey.schemaHash`; canonical keys now include execution class and validated input schema, preventing reuse after an in-session tool contract change.
- Removed the deprecated `ToolCache`, `maxCandidates`, `liveReadonly`, `minEmpiricalProbability`, and fixed single-step `mode` compatibility surfaces.
- Replaced inferred hit `savedMs`, `waitedMs`, `consumeOverheadMs`, `actorLeadMs`, and wall-clock speedup telemetry with monotonic `executionAheadMs`, `hitLatencyMs`, and diagnostic `attemptLeadMs`; execution overlap is capped by measured execution time, and matching leases no longer rewrite the timing owner of a deduplicated or cached job.
- Replaced copied plan/candidate statuses and split feedback callbacks with authoritative execution, prediction, and Actor-action settlements. Preparation hints no longer emit Actor prediction outcomes.
- Renamed ambiguous summary and inspection fields to `predictionRejectedAfterMatch`, `actorCandidateRejections`, `candidateTerminalCauses`, and `exclusiveCandidates`/`sharedCandidates`.
- Renamed the plan dependency and continuation signal from `actor_confirmed` to `actor_adopted`; a key match without a usable result no longer satisfies downstream work.
- Renamed source request horizons and `requestLifetime: "actor_action"` to decision-batch units; one Actor response may contain several parallel tool actions but advances prediction time only once.
- Split action identity from execution isolation. `ActionKey` no longer contains an execution class; adapters now resolve a `SpeculativeExecutionRoute` after K(a) materialization.
- Replaced the three tool groups (`resourceCached`, `sandbox`, and `predictionOnly`) with one prediction-enabled `tools` list. Legacy grouped settings are accepted only at the configuration migration boundary.
- Replaced the single sandbox option with `executionWorlds`. A runtime-scoped world takes priority for every tool.
- Renamed prediction-only fallback telemetry to the general execution-blocked terminology.
- Unified every successful speculative tool exit as a backend-owned `WorldBranch`. Removed the adapter's parallel resource-version callbacks and the redundant `ResourceVersionPolicy`; validation, invalidation, commit, and disposal now follow the branch lifecycle.
- Replaced tool-to-backend `localIsolation` declarations and opaque route handles with action-effect declarations and one `ExecutionWorldRouter`; runtime worlds cover the full tool surface, unavailable worlds fall through during resolution, and all preparation, fork, and disposal stays behind the router.

### Added

- Added a bounded, turn-scoped handoff that admits complete sidecar-fork tool calls to the ordinary speculative-action Runtime without issuing another inference request.
- Added a batch-atomic action-Drafter utility gate that charges all root proposal service, credits only realized Drafter-owned tool execution ahead, and retains bounded recovery probes; it is independently configurable from target-decoder self-speculation.
- Added clear-time target-verification telemetry with per-candidate IDs, Drafter/PatternAware source correlation, and separate resolved, accepted, rejected, and unresolved draft-token counters.
- Added a model-scoped net-benefit gate for sidecar forks with bounded recovery probes, endpoint-failure backoff, strict Actor K(a) outcome telemetry, and tape replay analysis.
- Added an opt-in SPORK/self-speculation bridge with stable Actor request IDs, provider and sidecar fork transports, bounded control requests, configurable model-format boundaries, and package/TUI settings.
- Added one ranked target-decoder bundle for every validated concrete K(a), merging identical Drafter and PatternAware predictions with source/proposal provenance even when local execution is unavailable.
- Added non-mutating PatternAware prediction rebasing after authoritative Actor actions and adopted Drafter results, allowing actual tool outputs to revise later same-turn actions before the normal learning boundary.
- Added a standalone repository boundary with its own TypeScript configuration, dependency lock, and Pi public-loader test; development no longer requires a Pi monorepo checkout.
- Added a speculative tool runtime with a host adapter, conservative action keys, resource validation, lifecycle events, and running or ready result reuse.
- Added canonical sandbox keys, an explicit sandbox host boundary, temporary `write`/`edit` execution, verified adoption, and opt-in bash process execution.
- Added private Git worktree snapshots, multi-file bash change capture, transactional world commit with rollback, and normalized cache/actual telemetry.
- Added a source-built Rust sandbox backend for Linux, macOS, and Windows, plus a versioned TypeScript broker, hash-verified packaged asset discovery, and fail-closed native process execution.
- Added an installable zero-modification Pi package that uses public lifecycle events, same-name tool overrides, public stock tool factories, and package-owned settings.
- Added a hierarchical Pi TUI for prediction, scheduling/cache, tool policy, and isolation, with configured, active, OCI, and native backend health reported separately.
- Added PatternAware online action-pattern learning with late-bound templates, future-gap leases, compact persistence, preparation hints, and utility-based resource scheduling.
- Added gap-weighted and decayed PatternAware inference, suffix backoff, collection mappers, retryable persisted analyzers, and multi-step frontiers.
- Added watcher-backed resource versions with exact-validation fallback, eager invalidation, byte-bounded per-session caches, pooled Git workspaces, and phase-level runtime metrics.
- Added an indexed per-session tool cache with atomic exact-key registration for cross-turn single-flight reuse.
- Added production Pi `read` range projection backed by structured realized-output coverage; grep and find remain exact-key-only.
- Added probation/protected speculative cache tiers: new results remain eviction-first until a successful actor hit promotes them, with bounded protected occupancy by entries and bytes.
- Added aggregate, input-free K(a) rejection counts to hit and miss telemetry.
- Added one `ExecutionWorldRouter` with the priority `runtime_sandbox` → registered local fallback → Actor fallback.
- Added `resource_snapshot` as the local route for `read`, `grep`, `find`, and `ls`, and `workspace_branch` as the Git-worktree route for `write` and `edit`.
- Added an explicit `execution_blocked` plan state. Blocked predictions remain matchable without being confused with an impossible dependency.
- Added counterfactual timing for isolation-blocked matches using the same capped lead-time decomposition as adopted speculative work.

### Changed

- Width-two Drafter requests now use first-valid hedged admission: the first schema-valid enabled action cancels only still-running initial-proposal siblings through their existing provider signals. Empty, failed, and invalid responses cannot win; explicitly wider sampling still admits all responses. Recorded strict hits, lead, and D3 verifier work are unchanged while 7.78% of width-two service is removable residual work.
- Reduced the default independent Drafter request width from eight to the tape-derived Pareto point of two; explicit `candidateLimit` settings remain uncapped, while the retained width preserves all recorded exact hits and lead with 30% fewer requests than the full recorded cohort.
- Raised the default target-decoder action draft cap from 20 to the tape-derived Pareto saturation point of 28 tokens; explicit user and engine caps still win.
- PatternAware now carries an unchanged K(a)/horizon set across the provider-turn boundary instead of issuing a duplicate prediction opportunity; changed authoritative feedback still emits a fresh candidate set for normal runtime arbitration.
- Detached the repository from its former GitHub fork network while preserving the standalone speculative-action history; `xchang1121/pi` is now an independent repository.
- The Pi package manifest now loads `src/extension.ts` directly, so Git installation works without monorepo build artifacts; npm packing still builds the exported `dist` library.
- Actor stream previews now start lossless covering reads when a complete path field is decoded; final K(a), realized coverage, and freshness remain authoritative.
- Read projection now reuses Pi coding-agent's public truncation contract and compact, in-memory coverage descriptors; projected single-flight reuse also requires explicit opt-in.
- World commit now serializes overlapping targets, uses staged atomic replacement, preserves file modes, and removes newly created directories during rollback.
- Drafter candidates now reuse exact and containing-read cache entries, deduplicate in-flight work, and replace stale resource entries before execution.
- Resource candidates now use probation/protected, value-aware eviction, and sandbox failures fall back without mutating the real workspace.
- Renamed read-only execution telemetry and settings to `resource_cached`/`resourceCached`.
- Pattern-aware and drafter predictions now deduplicate onto shared jobs, learned actions can be admitted immediately after authoritative results, and in-flight work is preempted by explicit utility and per-session cache budgets.
- PatternAware persistence now stores shared events once and references them from inference pools, preserving cross-process mapper evidence without repeatedly serializing tool payloads.
- PatternAware now calibrates each co-occurring target against one provider decision opportunity, preserving marginal confidence for parallel tool batches and migrating persisted counters.
- PatternAware now learns executable tool payloads separately from K(a), retains multiple replayable argument mappings for one control context, and ranks each branch against all observed target-tool alternatives for that context.
- PatternAware now launches at the first quartile of its learned future-gap distribution while retaining the largest observed gap as the miss deadline, increasing useful lead time without widening the match window.
- Actor lookup cascades across authorization, freshness, execution, compatibility, projection, and world-commit failures before falling back to real execution.
- Compatible cache insertion is now atomic and directed, so a broader running read can single-flight a narrower request without ever coalescing in the unsafe reverse direction.
- Native sandbox protocol v3 now forwards the configured shell and requires an explicit process-isolation attestation; Linux recursively enforces read-only host mounts and readiness probes verify mount isolation.
- K(a) remains a uniform canonical action key; registered projection rules now require both a potential key relation and validated realized-output coverage before reconstructing an actor result.
- Drafter rounds now issue `candidateLimit` independent one-action requests concurrently, retain one low-temperature accuracy sample and diverse remaining samples, and rely on the existing K(a) relation to deduplicate execution.
- Drafter output budgets and arbitrary-count sampling are now configurable recommendations; configured request and OCI worker counts no longer have hidden implementation caps.
- Drafter and PatternAware now emit only source-neutral actions; neither source can select an execution mechanism.
- Scheduler forecasts, resource arbitration, events, and cache snapshots derive isolation from the resolved route instead of K(a).
- In-flight and retained candidates are reused only when both K(a) compatibility and execution-route identity hold.
- The Pi TUI now exposes one tool policy and explains the runtime-sandbox, resource-snapshot, Git-worktree, and Actor-fallback boundary.
- Documentation and benchmark output now distinguish actual hidden latency from execution-blocked counterfactual potential.

### Fixed

- Target-decoder candidate bundles are now routed by the Plan Runtime's absolute expected Actor decision sequence, so output-informed and multi-step predictions cannot be submitted to an already-finished request; same-decision retries retain the bundle and stale decisions are dropped.
- Self-speculation cleanup now waits for in-flight candidate and sidecar-fork submissions, fences the package-level disabled state, and keeps Drafter request identities isolated from the Actor request.
- Output-informed Drafter continuations now permit a terminal response instead of forcing an unrelated tool call after the speculative branch has completed.
- A newly captured exact-key result remains reachable when an older cache generation cannot be freshness-validated; bounded cache retention now owns both generations until one is proven stale or evicted.
- Continuation work can be reused immediately, but its prediction is anchored to the first causally eligible Actor decision; same-batch reuse is no longer misattributed as a future-step match.
- A continuation targeting the next Actor decision is no longer cancelled by a sibling tool call from the same parallel Actor response.
- Probation cache aging now advances after each Actor decision batch, after matching, so a completed multi-step prediction remains adoptable through its declared deadline even when a batch contains multiple tool calls.
- Query K(a) now preserves `read`'s omitted-limit semantics and rejects numeric views that Pi tools do not interpret as stable integer ranges.
- Tool error settlements are discarded before they can become reusable speculative cache entries.
- Package shutdown now releases owned workspace sandbox pools immediately instead of retaining Windows file watchers until the idle timeout.
- Native release manifests and smoke requests now use protocol v4 and verify the isolation attestation.
- Shell dispatch recognizes Windows-style executable paths even when protocol tests run on a non-Windows host.
- Missing Docker or Podman no longer produces a global sandbox warning when the native fallback is ready.
- Cache lookup, telemetry, and speculative cleanup failures can no longer replace an authoritative stock Pi tool result.
- Same-name tools registered during normal extension initialization now remain authoritative and are excluded from speculation regardless of load order; stock tool renderers are preserved for both TUI and HTML output.
- Execution-blocked predictions remain K(a)-matchable and no longer enter the dependency-impossible settlement path.
- Execution-world warm-up now prepares the same healthy backend selected by route resolution when an earlier registered capability is unavailable.

### Removed

- Removed the bundled OCI, native process, and Windows AppContainer backends and their setup paths. The package no longer installs or launches Docker/Podman and no longer mutates OS sandbox state.
- Removed process-backend routing and installation settings from the Pi extension; embedding runtimes can inject a runtime-wide `ExecutionWorld` instead.
