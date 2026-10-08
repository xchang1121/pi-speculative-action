import { existsSync } from "node:fs";
import path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import { type ActionSemanticsRegistry, widenReadGuess } from "./action-semantics.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import type { ExecutionOperationBinding } from "./execution-world.ts";
import { agentBatchKey, type AgentPlanSource, type AgentStartInput } from "./agent-runtime-types.ts";
import { acquirePatternAwareStore, PATTERN_AWARE_DEFAULTS, asPatternAwareRuntimeContext, type OutputLocation, type PatternAwareCandidate,
	type PatternAwareEventInput, type PatternAwareRuntimeContext, type PatternAwareSettings, type PatternAwareStore, type PatternAwareStoreLease,
	patternAwareActionSemantics, patternAwareAnalyzerKey, patternAwareRuntimeContext, patternAwareSettings, failureClass,
	projectPatternAwareObservation } from "./pattern-aware.ts";
import type { PlanAction } from "./plan-proposal.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { asRecord } from "./stable-json.ts";
import type { ActorActionFeedback, SpeculativeActionSettings, SpeculativeCandidate } from "./runtime.ts";
import { candidateExecutionMs, candidateToolNames } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import type { ToolSettlement } from "./tool-settlement.ts";
import { observeResourceChanges, type ResourceVersionToken } from "./resource-version.ts";

type ObservedOperation = { readonly key: string; readonly parentHash: string; readonly binding: ExecutionOperationBinding };
type PatternPlanFeedback = PatternAwareRuntimeContext & {
	readonly patternIDs: ReadonlyArray<string>;
	readonly presetID?: PatternAwareCandidate["presetID"];
	readonly operation?: ObservedOperation;
};
type CarriedPrediction = { readonly signature: string; readonly pending: Set<PatternPlanFeedback>; abandoned: boolean };
type ObservedCommand = {
	readonly parentHash: string; readonly tool: string; readonly input: Readonly<Record<string, unknown>>; readonly schemaHash?: string;
	readonly failure?: { readonly paths: readonly string[]; readonly durationMs: number };
};
type CommandRerunState = { readonly native: boolean; readonly failed: boolean; workspaceChanged: boolean; observedChange?: boolean;
	changes?: Promise<ResourceVersionToken | undefined>; preparing?: Promise<unknown>; retry?: ObservedCommand;
	issued?: { readonly observedOnly: boolean; readonly presetID: "recent-command" | "retry-failed-command" } };

export interface PatternPlanSourceController {
	readonly source: AgentPlanSource;
	readonly turnStarted: (startInput: AgentStartInput, settings: SpeculativeActionSettings) => void;
	readonly turnFinished: (startInput: AgentStartInput, settings: SpeculativeActionSettings, terminal: boolean) => void;
	readonly actorActionSettled: (feedback: ActorActionFeedback<string>) => void;
	/** The calls PatternAware expects next, without proposing them. */
	readonly hints: (input: { readonly sessionID: string; readonly schemaHashes: Readonly<Record<string, string>>; readonly settings: SpeculativeActionSettings })
		=> Promise<readonly Pick<PatternAwareCandidate, "tool" | "input" | "horizon" | "expectedLatencyBenefitMs">[]>;
	readonly finishSession: () => Promise<void>;
	readonly dispose: () => Promise<void>;
}

export function createPatternPlanSource({
	sessionID, cwd, actionSemantics, projectionRules, stateDirectory, workspaceIdentity, store: providedStore,
}: {
	readonly sessionID: string;
	readonly cwd: string;
	readonly actionSemantics: ActionSemanticsRegistry;
	readonly projectionRules: readonly ActionProjectionRule<ToolSettlement>[];
	readonly stateDirectory?: string;
	readonly workspaceIdentity?: string;
	readonly store?: PatternAwareStore | Promise<PatternAwareStore>;
}): PatternPlanSourceController {
	cwd = path.resolve(cwd);
	const patternActionSemantics = patternAwareActionSemantics(actionSemantics, cwd, projectionRules, existsSync);
	let openedStore: { readonly key: string; readonly lease: Promise<PatternAwareStoreLease> } | undefined;
	const ownedStores = new Map<string, Promise<PatternAwareStoreLease>>();
	const lifecycle = new RuntimeLifecycleLane();
	const authoritativeBatches = new Map<string, Map<number, PatternAwareEventInput>>();
	const revisions = new Map<string, number>();
	const carriedPredictions = new Map<string, CarriedPrediction>();
	const predictionBatches = new WeakMap<PatternPlanFeedback, CarriedPrediction>(), served = new WeakSet<PatternPlanFeedback>();
	const issuedParents = new WeakSet<object>();
	const rerunOperations = new WeakMap<object, ObservedOperation>();
	// Capabilities stay in this session; the persisted Pattern store receives only real tool batches.
	const operationBindings = new BoundedRecencyMap<string, ObservedOperation>(PATTERN_AWARE_DEFAULTS.maxPatterns);
	// Exact authoritative inputs stay session-local; the persisted history may shorten large payloads.
	const learnedCommands = new BoundedRecencyMap<string, ObservedCommand>(4);
	let commandRerun: CommandRerunState | undefined;
	let analysisTail: Promise<void> = Promise.resolve();
	const releaseChanges = (state = commandRerun) => state?.changes && lifecycle.track(state.changes.then(token => token?.release()));

	const sourceSettings = (settings: SpeculativeActionSettings): PatternAwareSettings => {
		const patternSettings = patternAwareSettings(settings.sourceConfig?.patternAware);
		// Dropping this owner also fences work admitted before the preset was disabled.
		const native = patternSettings.presets.includes("recent-command"), failed = patternSettings.presets.includes("retry-failed-command");
		if (!settings.enabled || !patternSettings.enabled || !native && !failed) { releaseChanges(); commandRerun = undefined; }
		else if (commandRerun?.native !== native || commandRerun.failed !== failed) {
			releaseChanges(); commandRerun = { native, failed, workspaceChanged: false };
		}
		return patternSettings;
	};
	const admit = <Value>(settings: SpeculativeActionSettings,
		operation: (settings: PatternAwareSettings, rerun: CommandRerunState | undefined) => Promise<Value>): Promise<Value> => {
		try {
			// Capture before admission yields; a sealed owner must not read caller configuration.
			const patternSettings = lifecycle.sealed ? undefined : sourceSettings(settings);
			const rerun = commandRerun;
			return lifecycle.admit(() => operation(patternSettings!, rerun));
		} catch (error) { return Promise.reject(error); }
	};
	const nextRevision = (sessionID: string, turnID: string): number => {
		const key = agentBatchKey(sessionID, turnID);
		const revision = (revisions.get(key) ?? -1) + 1;
		revisions.set(key, revision);
		return revision;
	};
	const resolveStore = async (patternSettings: PatternAwareSettings): Promise<PatternAwareStore> => {
		if (providedStore) return providedStore;
		const configurationKey = patternAwareAnalyzerKey(patternSettings);
		if (!openedStore || openedStore.key !== configurationKey) {
			const previous = openedStore;
			const retained = ownedStores.get(configurationKey);
			const opening = { key: configurationKey, lease: Promise.resolve().then(async () => {
				if (previous) await previous.lease.then(({ store }) => store.finishSession(sessionID))
					.catch(() => undefined); // Failed loading cannot poison the next analyzer.
				return retained ?? acquirePatternAwareStore(workspaceIdentity ?? cwd, patternSettings, stateDirectory, patternActionSemantics);
			}) };
			// Predictions retain their analyzer for late feedback, including after returning to this configuration.
			if (!retained) ownedStores.set(configurationKey, opening.lease);
			openedStore = opening;
			void opening.lease.catch(() => {
				if (ownedStores.get(configurationKey) === opening.lease) ownedStores.delete(configurationKey);
				if (openedStore === opening) openedStore = undefined;
			});
		}
		return (await openedStore.lease).store;
	};
	const flushStores = async (finish = false): Promise<void> => {
		await analysisTail;
		const stores = providedStore ? [Promise.resolve(providedStore)] : [...ownedStores.values()].map(async lease => (await lease).store);
		const results = await Promise.allSettled(stores.map(async pending => {
			const store = await pending;
			if (finish) store.finishSession(sessionID);
			await store.flush();
		}));
		const failure = results.find(result => result.status === "rejected");
		if (failure) throw failure.reason;
	};
	const eventData = (tool: string, input: Readonly<Record<string, unknown>>, output: ToolSettlement | undefined, durationMs: number) => ({
		tool, input: structuredClone(input), outcome: output?.isError ? "failure" as const : "success" as const,
		...projectPatternAwareObservation(output?.result, extractOutputPaths(tool, input, output?.result), cwd, extractOutputLocations(tool, input, output?.result)),
		...(output?.isError ? { errorClass: failureClass(output.result.content.flatMap((item) => item.type === "text" ? [item.text] : []).join("\n")) } : {}),
		durationMs,
		...(typeof input.operation === "string" ? { operation: input.operation } : {}),
	});
	const predictedEvent = (startInput: AgentStartInput, action: Pick<SpeculativeCandidate, "key" | "input">,
		output: ToolSettlement, durationMs: number): PatternAwareEventInput => ({
		sessionID: startInput.sessionID, turnID: startInput.turnID,
		...eventData(action.key.tool, action.input, output, durationMs), schemaHash: action.key.schemaHash, learnTarget: false,
	});
	const resourcePath = (target: string) => patternActionSemantics.actionKey("read", { path: target })?.resources[0];
	const currentOperation = (binding: ExecutionOperationBinding) => binding.available !== false && binding.executionMs > 0 &&
		binding.preparation === "current_workspace" && !binding.fed && typeof binding.stale === "function";
	const observeWorkspace = async (state: CommandRerunState | undefined) => {
		if (!state?.native || state !== commandRerun || ![...operationBindings.values()].some(({ binding }) => currentOperation(binding))) return;
		// One pooled watcher cursor follows actual changes, including Bash writes. Notification is a scheduling hint, never freshness proof.
		// Serialize cursor replacement so concurrent authoritative completions cannot rewind it or retain an orphaned cursor.
		state.changes = (state.changes ?? Promise.resolve(undefined)).then(async previous => {
			const current = await observeResourceChanges(cwd).catch(() => undefined);
			const changes = previous?.manager.changesSince(previous);
			await previous?.release();
			if (state !== commandRerun) { await current?.release(); return undefined; }
			if (changes && !changes.uncertain && changes.paths.some(target => !path.relative(cwd, target).split(path.sep).includes(".git"))) state.observedChange = true;
			return current;
		});
		await state.changes;
	};
	const planActions = (candidates: readonly PatternAwareCandidate[], store: PatternAwareStore,
		schemaHashes: Readonly<Record<string, string>>, operationLimit: number, dependsOn?: PlanAction["dependsOn"], parentID?: string) => {
		let operations: Map<string, ObservedOperation[]> | undefined;
		return candidates.flatMap(candidate => {
			const action = patternPlanAction(candidate, store, patternPlanActionID(candidate.actionIdentity, parentID), dependsOn);
			// Both recurring predictions and background probes may reuse a root's native
			// operations. Dependent steps still need their parent's complete tool result.
			if (dependsOn?.length || !operationBindings.size) return [action];
			const parentHash = patternActionSemantics.actionKey(candidate.tool, candidate.input, schemaHashes[candidate.tool])?.hash;
			if (!operations) {
				operations = new Map();
				for (const item of operationBindings.values()) {
					if (item.binding.available === false) operationBindings.delete(item.key);
					else if (item.binding.executionMs > 0) {
						const choices = operations.get(item.parentHash) ?? [];
						choices.push(item); operations.set(item.parentHash, choices);
					}
				}
				for (const choices of operations.values()) choices.sort((left, right) => right.binding.executionMs - left.binding.executionMs);
			}
			const choices = parentHash === undefined ? undefined : operations.get(parentHash);
			if (!choices?.length) return [action];
			return choices.slice(0, operationLimit).map(operation => ({ ...action,
				id: `${action.id}:operation:${operation.binding.identity}`, type: "operation" as const, operation: operation.binding,
				// Waiting on its producer, it takes only capacity nothing else wants.
				...(operation.binding.fed ? { background: true } : {}),
				expectedDurationMs: operation.binding.expectedDurationMs,
				expectedLatencyBenefitMs: Math.min(candidate.expectedLatencyBenefitMs, candidate.empiricalProbability * operation.binding.executionMs),
				feedback: { ...action.feedback, operation },
			}));
		});
	};

	/** Prepare a stale native launch against the Actor's edited files, without repeating its parent's earlier setup.
	 * Captured-resource launches keep their parent ordering. Every result still requires current dependency evidence at adoption. */
	const prepareRerun = async (state: CommandRerunState | undefined, schemaHashes: Readonly<Record<string, string>>, operationLimit: number): Promise<readonly [rerun: PlanAction[], apart: (action: PlanAction) => boolean]> => {
		if (!state || state !== commandRerun || !state.workspaceChanged && !state.observedChange && !state.retry && !state.issued) return NO_RERUN;
		const retry = state.retry;
		const observedOnly = !state.workspaceChanged && !retry && (state.observedChange === true || state.issued?.observedOnly === true);
		state.workspaceChanged = false; state.observedChange = false; state.retry = undefined; state.issued = undefined;
		const commands = [...learnedCommands.values()].reverse();
		if (retry) commands.sort((left, right) => Number(right === retry) - Number(left === retry));
		else if (observedOnly) {
			const costs = new Map<string, number>();
			for (const { parentHash, binding } of operationBindings.values()) if (currentOperation(binding))
				costs.set(parentHash, Math.max(costs.get(parentHash) ?? 0, binding.executionMs));
			commands.sort((left, right) => (costs.get(right.parentHash) ?? 0) - (costs.get(left.parentHash) ?? 0));
		}
		for (const command of commands) {
			if (!state.native && command !== retry || command.schemaHash !== schemaHashes[command.tool]) continue;
			const children = [...operationBindings.values()].filter(item => item.parentHash === command.parentHash && item.binding.available !== false && (!observedOnly || currentOperation(item.binding)))
				.sort((left, right) => right.binding.executionMs - left.binding.executionMs).slice(0, observedOnly ? operationLimit : undefined);
			const stale = (await Promise.all(children.map(async ({ binding }) => {
				const changed = binding.executionMs > 0 ? await binding.stale?.() : false;
				return (observedOnly ? changed === true : changed !== false) ? binding : undefined;
			})))
				.filter((binding): binding is ExecutionOperationBinding => !!binding).sort((left, right) => right.executionMs - left.executionMs);
			if (state !== commandRerun) return NO_RERUN;
			if (![...learnedCommands.values()].includes(command)) continue;
			const fallback = command === retry && command.failure && !children.some(({ binding }) => binding.executionMs > 0);
			if (!stale.length && !fallback) continue;
			const operation = stale.find(binding => binding.preparation === "current_workspace" && !binding.fed);
			if (command === retry) state.retry = command;
			const mode = command === retry ? "retry-failed-command" : "recent-command";
			const feedback = state.issued = { observedOnly, presetID: mode };
			if (operation) rerunOperations.set(feedback, children.find(item => item.binding === operation)!);
			return [[{ id: `rerun:${command.parentHash}`, type: operation ? "operation" : "tool_call", ...(operation ? { operation } : {}),
				tool: command.tool, input: command.input, horizon: 0, producesOperations: true, mode, feedback,
				...(fallback ? { empiricalProbability: 0.25, conditionalProbability: 0.25 } : {}),
				expectedLatencyBenefitMs: fallback ? command.failure!.durationMs * 0.25 : operation?.executionMs ?? stale.reduce((total, binding) => total + binding.executionMs, 0),
				expectedDurationMs: fallback ? command.failure!.durationMs : operation?.expectedDurationMs ?? Math.max(...children.map(({ binding }) => binding.expectedDurationMs)) }],
				action => (action.type !== "tool_call" || patternActionSemantics.actionKey(action.tool, asRecord(action.input) ?? {}, schemaHashes[action.tool])?.hash !== command.parentHash) &&
					(operation ? action.operation?.identity !== operation.identity : !children.some(({ binding }) => binding.identity === action.operation?.identity && !binding.fed))];
		}
		return NO_RERUN;
	};
	const reruns = async (state: CommandRerunState | undefined, schemaHashes: Readonly<Record<string, string>>, operationLimit: number) => {
		if (!state || state !== commandRerun) return NO_RERUN;
		// A closing observation may still be checking staleness. Its next decision must see the issued result, not the cleared trigger.
		const preparing = (state.preparing ?? Promise.resolve()).catch(() => {}).then(() => prepareRerun(state, schemaHashes, operationLimit));
		state.preparing = preparing;
		try { return await preparing; } finally { if (state.preparing === preparing) state.preparing = undefined; }
	};

	const source: AgentPlanSource = {
		id: "pattern_aware",
		enabled: (settings) => !lifecycle.sealed && sourceSettings(settings).enabled,
		observesOperations: action => actionSemantics.definition(action)?.requirements.capabilities.includes("invocation.process") === true,
		multiStepEnabled: (settings) => sourceSettings(settings).multiStepEnabled,
		requestLifetime: "actor_decision",
		propose: ({ startInput, data, settings, signal }) => admit(settings, async (patternSettings, rerunState) => {
			if (!patternSettings.enabled) return undefined;
			await analysisTail;
			if (signal.aborted) return undefined;
			const store = await resolveStore(patternSettings);
			if (signal.aborted) return undefined;
			const candidates = store.predict(startInput.sessionID, data.schemaHashes, patternSettings);
			const signature = patternPredictionSignature(candidates);
			await rerunState?.changes;
			if (signal.aborted) return undefined;
			const carried = carriedPredictions.get(startInput.sessionID), prepared = await reruns(rerunState, data.schemaHashes, patternSettings.beamWidth);
			if (signal.aborted) return undefined;
			const [rerun, apart] = rerunState === commandRerun ? prepared : NO_RERUN;
			carriedPredictions.delete(startInput.sessionID);
			const repeated = !candidates.length || carried?.signature === signature && !carried.pending.size && !carried.abandoned;
			if (repeated && !rerun.length) return undefined;
			return { id: `pattern:${startInput.turnID}`, source: "pattern_aware", revision: nextRevision(startInput.sessionID, startInput.turnID),
				actions: [...repeated ? [] : planActions(candidates, store, data.schemaHashes, patternSettings.beamWidth).filter(apart), ...rerun] };
		}),
		continueFrom: ({ startInput, data, settings, batch, signal }) => admit(settings, async (patternSettings) => {
			await analysisTail;
			if (signal.aborted) return undefined;
			const store = await resolveStore(patternSettings);
			if (signal.aborted) return undefined;
			const id = `pattern:peer:${stableValueHash(batch.map(({ identity }) => identity.id))}`;
			const candidates = store.predictAfterBatch(startInput.sessionID,
				batch.map(({ candidate, output }) => predictedEvent(startInput, candidate, output, candidateExecutionMs(candidate))),
				data.schemaHashes, patternSettings,
				// No calibrated joint confidence is supplied for the foreign batch; use a neutral prior.
				{ visitedPatternIDs: [id], pathProbability: 0.5 });
			if (!candidates.length) return undefined;
			const dependencies = batch.map(({ identity }) => ({ proposalID: identity.proposalID, actionID: identity.actionID,
				identity: identity.id, condition: "execution_succeeded" as const }));
			return { id, source: "pattern_aware", revision: 0, actions: planActions(candidates, store, data.schemaHashes, patternSettings.beamWidth, dependencies) };
		}),
		continue: ({
			startInput,
			data,
			settings,
			candidate,
			adoptedAction,
			proposalID,
			actionID,
			revision,
			feedback,
			output,
			trigger,
			signal,
		}) => admit(settings, async (patternSettings) => {
			if (signal.aborted) return undefined;
			const context = asPatternPlanFeedback(feedback);
			if (!context || context.operation) return undefined;
			const action = adoptedAction ?? candidate;
			const next = context.store.continue(
				context.continuation,
				predictedEvent(startInput, action, output, candidateExecutionMs(candidate)),
				data.schemaHashes,
				trigger === "actor_adopted",
				patternSettings,
			);
			if (!next.length) return undefined;
			return {
				proposalID,
				source: "pattern_aware",
				revision,
				upsert: planActions(next, context.store, data.schemaHashes, patternSettings.beamWidth, [{ actionID, condition: "execution_succeeded" }], actionID),
			};
		}),
		observe: ({ data, settings, consumeInput, action, tool, concrete, output, durationMs, order, operations, signal, reserveRevision }) => admit(settings, async (patternSettings, rerunState) => {
			if (!patternSettings.enabled) return undefined;
			const schemaHash = action?.schemaHash ?? data.schemaHashes[tool];
			const observed = eventData(tool, concrete, output, durationMs);
			const parentHash = (operations?.length || tool === "bash") && patternActionSemantics.actionKey(tool, concrete, schemaHash)?.hash;
			let bound = false;
			if (parentHash) for (const binding of operations ?? []) {
				if (binding.available === false || binding.permissionHash !== action?.hash) continue;
				const key = `${parentHash}:${binding.backend}:${binding.identity}`;
				operationBindings.set(key, { key, parentHash, binding });
				bound = true;
			}
			// A command mentioning a path is not evidence that its failure reported that file.
			const paths = tool === "bash" && output?.isError ? [...new Set([...(extractOutputPaths(tool, {}, output.result) ?? []), ...(observed.outputLocations ?? []).map(location => location.path)]
				.flatMap(target => resourcePath(target) ?? []))] : [];
			const failure = paths.length && Number.isFinite(durationMs) && durationMs > 0 ? { paths, durationMs } : undefined;
			if (parentHash && (bound || learnedCommands.get(parentHash) || patternSettings.presets.includes("retry-failed-command") && failure))
				learnedCommands.set(parentHash, { parentHash, tool, input: observed.input, ...(schemaHash === undefined ? {} : { schemaHash }), ...(failure ? { failure } : {}) });
			if (rerunState && rerunState === commandRerun && actionSemantics.toolNames("workspace_mutation").includes(tool)) {
				rerunState.workspaceChanged = rerunState.native;
				const edited = typeof concrete.path === "string" && output && !output.isError ? resourcePath(concrete.path) : undefined;
				rerunState.retry = rerunState.failed && edited ? [...learnedCommands.values()].reverse().find(command => command.failure?.paths.includes(edited)) : undefined;
				rerunState.issued = undefined;
			}
			const key = agentBatchKey(consumeInput.sessionID, consumeInput.turnID);
			const batch = authoritativeBatches.get(key) ?? new Map();
			const event: PatternAwareEventInput = {
				sessionID: consumeInput.sessionID,
				turnID: consumeInput.turnID,
				...observed,
				...(schemaHash === undefined ? {} : { schemaHash }),
				learnTarget: candidateToolNames(settings, actionSemantics).includes(tool),
			};
			batch.set(order, event);
			authoritativeBatches.set(key, batch);
			await observeWorkspace(rerunState);
			// A closing turn drops these updates and the next turn predicts afresh (a rerun then goes with it): never hold turn closure for them.
			let [rerun, apart] = signal?.aborted ? NO_RERUN : await reruns(rerunState, data.schemaHashes, patternSettings.beamWidth);
			if (rerunState !== commandRerun) [rerun, apart] = NO_RERUN;
			if (!patternSettings.multiStepEnabled && !rerun.length || signal?.aborted) return undefined;
			let actions = rerun;
			if (patternSettings.multiStepEnabled) {
				await analysisTail;
				if (signal?.aborted) return undefined;
				const store = await resolveStore(patternSettings);
				const ordered = [...batch.entries()].sort(([left], [right]) => left - right).map(([, item]) => item);
				const candidates = store.predictAfterBatch(consumeInput.sessionID, ordered, data.schemaHashes, patternSettings);
				if (rerunState !== commandRerun) [rerun, apart] = NO_RERUN;
				const predicted = planActions(candidates, store, data.schemaHashes, patternSettings.beamWidth).filter(apart);
				// An observation can finish after its turn closes, or lose individual actions during admission.
				const carried = { signature: patternPredictionSignature(candidates), pending: new Set(predicted.map((action) => action.feedback)), abandoned: false };
				for (const action of predicted) predictionBatches.set(action.feedback, carried);
				carriedPredictions.set(consumeInput.sessionID, carried);
				actions = [...predicted, ...rerun];
			}
			// Its own namespace never supersedes the turn's still-pending predictions; the runtime's revision covers continuations.
			const id = `pattern:${consumeInput.turnID}:after`, minimum = nextRevision(consumeInput.sessionID, id);
			const revision = reserveRevision?.(id, minimum) ?? minimum;
			revisions.set(agentBatchKey(consumeInput.sessionID, id), revision);
			return { id, source: "pattern_aware", revision, actions };
		}),
		onAdmitted: ({ feedback }) => {
			if (lifecycle.sealed) return;
			if (commandRerun && commandRerun.issued === feedback) { commandRerun.issued = undefined; commandRerun.retry = undefined; }
			const context = asPatternPlanFeedback(feedback);
			if (context) predictionBatches.get(context)?.pending.delete(context);
		},
		onIssued: ({ feedback }) => {
			if (lifecycle.sealed) return;
			const context = asPatternPlanFeedback(feedback);
			// A parent's operation choices issue its prediction once, so its one-time probe clears as a tool call's does.
			if (!context || context.operation && issuedParents.has(context.continuation)) return;
			issuedParents.add(context.continuation);
			for (const support of [context.continuation, ...context.patternIDs]) context.store.issued(support);
		},
		onSettled: ({ feedback, settlement }) => {
			if (lifecycle.sealed) return;
			const context = asPatternPlanFeedback(feedback);
			const operation = context?.operation ?? rerunOperations.get(feedback as object);
			const carried = context && predictionBatches.get(context);
			if (carried && settlement.observation === "unobserved") carried.abandoned = true;
			if (operation) {
				// Execution failure retires this preparation hint; absence of an OS observation is not a negative example.
				if (settlement.observation === "unobserved" && settlement.cause.stage === "execution" &&
					operationBindings.get(operation.key) === operation) operationBindings.delete(operation.key);
				if (settlement.observation === "unobserved") return; // An adopted child credits its parent pattern.
			}
			if (context) for (const support of [context.continuation, ...context.patternIDs]) context.store.settled(support, settlement, served.has(context));
		},
		flush: () => lifecycle.run(() => flushStores()),
	};

	const observeTurn = (startInput: AgentStartInput, settings: SpeculativeActionSettings, terminal?: boolean): void => {
		if (lifecycle.sealed) return;
		const key = agentBatchKey(startInput.sessionID, startInput.turnID);
		const batch = authoritativeBatches.get(key);
		authoritativeBatches.delete(key);
		revisions.delete(key);
		if (terminal) carriedPredictions.delete(startInput.sessionID);
		const patternSettings = sourceSettings(settings);
		if (!settings.enabled || !patternSettings.enabled || lifecycle.sealed) { carriedPredictions.delete(startInput.sessionID); return; }
		// Only a completed turn contributes its authoritative batch; entry discards any stale batch.
		const events = terminal !== undefined && batch?.size ? [...batch.entries()].sort(([left], [right]) => left - right).map(([, event]) => event) : [];
		analysisTail = analysisTail.then(() => new Promise<void>(setImmediate))
			.then(async () => {
				const store = await resolveStore(patternSettings);
				if (events.length) store.observeBatch(events);
				store.observeTurn();
				if (terminal) store.finishSession(startInput.sessionID);
			})
			.catch(() => {
				// Optional learning cannot poison later observations or the Actor lifecycle.
			});
	};
	return {
		source,
		hints: ({ sessionID, schemaHashes, settings }) => admit(settings, async (patternSettings) => {
			await analysisTail;
			return !patternSettings.enabled ? [] : (await resolveStore(patternSettings)).predict(sessionID, schemaHashes, patternSettings)
				.slice(0, 4).map(({ tool, input, horizon, expectedLatencyBenefitMs }) => ({ tool, input, horizon, expectedLatencyBenefitMs }));
		}),
		turnStarted: observeTurn,
		turnFinished: observeTurn,
		// Serving the Actor is recorded before the owning prediction settles, which credits it even when unmatched.
		actorActionSettled: ({ settlement, candidateFeedback: feedback }) => {
			if (settlement.provider.kind === "speculative" && asPatternPlanFeedback(feedback)) served.add(feedback as PatternPlanFeedback);
		},
		finishSession: () => lifecycle.run(async () => {
			await lifecycle.drain();
			const closing = commandRerun;
			commandRerun = undefined;
			await releaseChanges(closing);
			revisions.clear();
			carriedPredictions.clear();
			operationBindings.clear();
			learnedCommands.clear();
			clearAuthoritativeSession(authoritativeBatches, sessionID);
			try {
				await flushStores(true);
			} catch {
				// Persistence failure must not change Agent lifecycle semantics.
			}
		}),
		dispose: () => lifecycle.close(async () => {
			await analysisTail;
			await lifecycle.drain();
			const closing = commandRerun;
			commandRerun = undefined;
			await releaseChanges(closing);
			const leases = [...ownedStores.values()];
			ownedStores.clear();
			openedStore = undefined;
			authoritativeBatches.clear();
			operationBindings.clear();
			learnedCommands.clear();
			revisions.clear();
			carriedPredictions.clear();
			await Promise.allSettled(leases.map(async lease => (await lease).release()));
		}),
	};
}

export function patternPlanActionID(actionIdentity: string, parentActionID = "root"): string {
	return `pattern:${stableValueHash({ actionIdentity, parentActionID }).slice(0, 16)}`;
}

const NO_RERUN: readonly [rerun: PlanAction[], apart: (action: PlanAction) => boolean] = [[], () => true];

function patternPredictionSignature(candidates: readonly PatternAwareCandidate[]): string {
	return JSON.stringify(
		candidates.map((candidate) => [candidate.actionIdentity, candidate.horizon, candidate.latestHorizon] as const)
			.sort(([left], [right]) => left.localeCompare(right)),
	);
}

function clearAuthoritativeSession(batches: Map<string, Map<number, PatternAwareEventInput>>, sessionID: string): void {
	for (const [key, batch] of batches) if (batch.values().next().value?.sessionID === sessionID) batches.delete(key);
}

function asPatternPlanFeedback(value: unknown): PatternPlanFeedback | undefined {
	const context = asPatternAwareRuntimeContext(value);
	const patternIDs = (value as { patternIDs?: unknown })?.patternIDs;
	if (!context || !Array.isArray(patternIDs) || !patternIDs.every((item) => typeof item === "string")) return undefined;
	return value as PatternPlanFeedback;
}

function patternPlanAction(
	candidate: PatternAwareCandidate,
	store: PatternAwareStore,
	id: string,
	dependsOn?: PlanAction["dependsOn"],
): PlanAction & { readonly feedback: PatternPlanFeedback } {
	return {
		id,
		type: "tool_call",
		tool: candidate.tool,
		...(candidate.presetID ? { mode: candidate.presetID } : {}),
		input: widenReadGuess(candidate.tool, candidate.input),
		horizon: candidate.horizon,
		latestHorizon: candidate.latestHorizon,
		empiricalProbability: candidate.empiricalProbability,
		conditionalProbability: candidate.conditionalProbability,
		expectedDurationMs: candidate.expectedDurationMs,
		expectedLatencyBenefitMs: candidate.expectedLatencyBenefitMs,
		...(candidate.background ? { background: true } : {}),
		depth: candidate.depth,
		...(dependsOn?.length ? { dependsOn } : {}),
		feedback: { ...patternAwareRuntimeContext(store, candidate), patternIDs: candidate.supportingPatternIDs,
			...(candidate.presetID ? { presetID: candidate.presetID } : {}) },
	};
}

/** Files a run's text names as compilers, test runners and stack traces print them: with a directory, or with a line after them. */
const RUN_OUTPUT_LOCATION = /(?<![\w.@/\\:-])((?:[A-Za-z]:)?[\w.@-]{0,128}(?:[\\/][\w.@-]{1,128}){0,16}\.[A-Za-z]\w{0,9})(?::(\d+)|\((\d+)|",? line (\d+))/gu;
const RUN_OUTPUT_PATH = /(?<![\w.@/\\:-])((?:[A-Za-z]:)?[\w.@-]{0,128}(?:[\\/][\w.@-]{1,128}){0,16}\.[A-Za-z]\w{0,9})(:\d|\(\d|",? line \d)?/gu;

/** `path:line` facts: grep's matches, and a command's reported locations (compiler errors, stack traces, test failures). */
function extractOutputLocations(tool: string, actionInput: Readonly<Record<string, unknown>>, result: AgentToolResult<unknown> | undefined): OutputLocation[] {
	if ((tool !== "grep" && tool !== "bash") || !result) return [];
	const text = result.content.flatMap((item) => item.type === "text" ? [item.text] : []).join("\n");
	const searchRoot = typeof actionInput.path === "string" && actionInput.path ? actionInput.path : ".";
	const located = tool === "grep" ? [...text.matchAll(/^(.+?):(\d+)(?::\d+)?:/gmu)] : [...text.matchAll(RUN_OUTPUT_LOCATION)];
	return located.slice(0, 256).flatMap(([, file, ...lines]) => {
		const line = lines.find(Boolean);
		const number = Number(line);
		if (!file || !Number.isSafeInteger(number) || number < 1) return [];
		return [{ path: tool === "bash" || path.isAbsolute(file) ? file : path.basename(searchRoot) === file ? searchRoot : path.join(searchRoot, file), line: number }];
	});
}

function extractOutputPaths(
	tool: string,
	actionInput: Readonly<Record<string, unknown>>,
	result: AgentToolResult<unknown> | undefined,
): readonly string[] | undefined {
	if ((tool !== "find" && tool !== "grep" && tool !== "bash") || !result) return undefined;
	const searchRoot = typeof actionInput.path === "string" && actionInput.path ? actionInput.path : ".";
	const text = result.content.flatMap((item) => item.type === "text" ? [item.text] : []).join("\n");
	if (tool === "bash") { // The files a command names as it runs, and those its output reports.
		// A git diff names each file as a/path and b/path.
		const diff = /^(?:diff --git|\+\+\+|---) [ab]\//mu.test(text), named = [...`${actionInput.command}\n${text}`.matchAll(RUN_OUTPUT_PATH)]
			.flatMap((match) => match[2] || /[\\/]/u.test(match[1]!) ? [diff ? match[1]!.replace(/^[ab]\//u, "") : match[1]!] : []);
		return named.length ? [...new Set(named)].slice(0, 32) : undefined;
	}
	const paths = text.split(/\r?\n/)
		.map((line) => {
			const trimmed = line.trim();
			if (!trimmed || trimmed.startsWith("[") || /^No files found\b/.test(trimmed)) return undefined;
			if (tool === "find") return trimmed;
			return /^(.*?):\d+(?::\d+)?:/.exec(trimmed)?.[1];
		})
		.filter((item): item is string => typeof item === "string" && item.length > 0)
		.map((item) => {
			if (path.isAbsolute(item)) return item;
			if (tool === "grep" && path.basename(searchRoot) === item) return searchRoot;
			return path.join(searchRoot, item);
		});
	return paths.length ? [...new Set(paths)] : undefined;
}
