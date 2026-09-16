import path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import type { ActionSemanticsRegistry } from "./action-semantics.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import type { ExecutionOperationBinding } from "./execution-world.ts";
import {
	agentBatchKey,
	type AgentPlanSource,
	type AgentStartInput,
} from "./agent-runtime-types.ts";
import {
	acquirePatternAwareStore,
	PATTERN_AWARE_DEFAULTS,
	asPatternAwareRuntimeContext,
	type PatternAwareCandidate,
	type PatternAwareEventInput,
	type PatternAwareRuntimeContext,
	type PatternAwareSettings,
	type PatternAwareStore,
	type PatternAwareStoreLease,
	patternAwareActionSemantics,
	patternAwareAnalyzerKey,
	patternAwareRuntimeContext,
	patternAwareSettings,
	projectPatternAwareObservation,
} from "./pattern-aware.ts";
import type { PlanAction } from "./plan-proposal.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { SpeculativeActionSettings, SpeculativeCandidate } from "./runtime.ts";
import { candidateExecutionMs, candidateToolNames } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

type ObservedOperation = { readonly key: string; readonly parentHash: string; readonly binding: ExecutionOperationBinding };
type PatternPlanFeedback = PatternAwareRuntimeContext & {
	readonly patternIDs: ReadonlyArray<string>;
	readonly operation?: ObservedOperation;
};
type CarriedPrediction = { readonly signature: string; readonly pending: Set<PatternPlanFeedback>; abandoned: boolean };

export interface PatternPlanSourceController {
	readonly source: AgentPlanSource;
	readonly turnStarted: (startInput: AgentStartInput, settings: SpeculativeActionSettings) => void;
	readonly turnFinished: (
		startInput: AgentStartInput,
		settings: SpeculativeActionSettings,
		terminal: boolean,
	) => void;
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
	const patternActionSemantics = patternAwareActionSemantics(actionSemantics, cwd, projectionRules);
	let openedStore: { readonly key: string; readonly lease: Promise<PatternAwareStoreLease> } | undefined;
	const ownedStores = new Map<string, Promise<PatternAwareStoreLease>>();
	const lifecycle = new RuntimeLifecycleLane();
	const authoritativeBatches = new Map<string, Map<number, PatternAwareEventInput>>();
	const revisions = new Map<string, number>();
	const carriedPredictions = new Map<string, CarriedPrediction>();
	const predictionBatches = new WeakMap<PatternPlanFeedback, CarriedPrediction>();
	// Capabilities stay in this session; the persisted Pattern store receives only real tool batches.
	const operationBindings = new BoundedRecencyMap<string, ObservedOperation>(PATTERN_AWARE_DEFAULTS.maxPatterns);
	let analysisTail: Promise<void> = Promise.resolve();

	const sourceSettings = (settings: SpeculativeActionSettings): PatternAwareSettings =>
		patternAwareSettings(settings.sourceConfig?.patternAware);
	const admit = <Value>(settings: SpeculativeActionSettings,
		operation: (settings: PatternAwareSettings) => Promise<Value>): Promise<Value> => {
		try {
			// Capture before admission yields; a sealed owner must not read caller configuration.
			const patternSettings = lifecycle.sealed ? undefined : sourceSettings(settings);
			return lifecycle.admit(() => operation(patternSettings!));
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
				return retained ?? acquirePatternAwareStore(workspaceIdentity ?? cwd, patternSettings,
					stateDirectory, patternActionSemantics);
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
	const predictedEvent = (startInput: AgentStartInput, action: Pick<SpeculativeCandidate, "key" | "input">,
		output: ToolSettlement, durationMs: number): PatternAwareEventInput => ({
		sessionID: startInput.sessionID, turnID: startInput.turnID, tool: action.key.tool,
		input: structuredClone(action.input), outcome: output.isError ? "failure" : "success",
		...projectPatternAwareObservation(output.result, extractOutputPaths(action.key.tool, action.input, output.result), cwd),
		durationMs, schemaHash: action.key.schemaHash,
		...(typeof action.input.operation === "string" ? { operation: action.input.operation } : {}),
		learnTarget: false,
	});
	const planAction = (candidate: PatternAwareCandidate, store: PatternAwareStore, id: string,
		schemaHashes: Readonly<Record<string, string>>, dependsOn?: PlanAction["dependsOn"]) => {
		const action = patternPlanAction(candidate, store, id, dependsOn);
		// Preserve established whole-action paths. Uncertain idle-capacity probes may prepare a known smaller unit.
		if (!candidate.background || dependsOn?.length || !operationBindings.size) return action;
		const parentHash = patternActionSemantics.actionKey(candidate.tool, candidate.input, schemaHashes[candidate.tool])?.hash;
		let operation: ObservedOperation | undefined;
		for (const item of operationBindings.values()) {
			if (item.binding.available === false) operationBindings.delete(item.key);
			else if (item.parentHash === parentHash && item.binding.executionMs > (operation?.binding.executionMs ?? 0)) operation = item;
		}
		if (!operation) return action;
		const { binding } = operation;
		return { ...action, id: `${id}:operation:${binding.identity}`, type: "operation" as const, operation: binding,
			expectedDurationMs: binding.expectedDurationMs,
			expectedLatencyBenefitMs: Math.min(candidate.expectedLatencyBenefitMs, candidate.empiricalProbability * binding.executionMs),
			feedback: { ...action.feedback, operation },
		};
	};
	const planActions = (candidates: readonly PatternAwareCandidate[], store: PatternAwareStore,
		schemaHashes: Readonly<Record<string, string>>, dependsOn?: PlanAction["dependsOn"], parentID?: string) =>
		candidates.map(candidate => planAction(candidate, store, patternPlanActionID(candidate.actionIdentity, parentID), schemaHashes, dependsOn));

	const source: AgentPlanSource = {
		id: "pattern_aware",
		enabled: (settings) => !lifecycle.sealed && sourceSettings(settings).enabled,
		observesOperations: true,
		multiStepEnabled: (settings) => sourceSettings(settings).multiStepEnabled,
		requestLifetime: "actor_decision",
		propose: ({ startInput, data, settings, signal }) => admit(settings, async (patternSettings) => {
			if (!patternSettings.enabled) return undefined;
			await analysisTail;
			if (signal.aborted) return undefined;
			const store = await resolveStore(patternSettings);
			if (signal.aborted) return undefined;
			const candidates = store.predict(startInput.sessionID, data.schemaHashes, patternSettings);
			const signature = patternPredictionSignature(candidates);
			const carried = carriedPredictions.get(startInput.sessionID);
			carriedPredictions.delete(startInput.sessionID);
			if (!candidates.length || carried?.signature === signature && !carried.pending.size && !carried.abandoned) return undefined;
			return {
				id: `pattern:${startInput.turnID}`,
				source: "pattern_aware",
				revision: nextRevision(startInput.sessionID, startInput.turnID),
				actions: planActions(candidates, store, data.schemaHashes),
			};
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
			return { id, source: "pattern_aware", revision: 0, actions: planActions(candidates, store, data.schemaHashes, dependencies) };
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
				upsert: planActions(next, context.store, data.schemaHashes, [{ actionID, condition: "execution_succeeded" }], actionID),
			};
		}),
		observe: ({ data, settings, consumeInput, action, tool, concrete, output, durationMs, order, operations }) => admit(settings, async (patternSettings) => {
			if (!patternSettings.enabled) return undefined;
			const schemaHash = action?.schemaHash ?? data.schemaHashes[tool];
			const parentHash = operations?.length && patternActionSemantics.actionKey(tool, concrete, schemaHash)?.hash;
			if (parentHash) for (const binding of operations ?? []) {
				if (binding.available === false || binding.permissionHash !== action?.hash) continue;
				const key = `${parentHash}:${binding.backend}:${binding.identity}`;
				operationBindings.set(key, { key, parentHash, binding });
			}
			const observation = projectPatternAwareObservation(
				output?.result,
				extractOutputPaths(tool, concrete, output?.result),
				cwd,
			);
			const key = agentBatchKey(consumeInput.sessionID, consumeInput.turnID);
			const batch = authoritativeBatches.get(key) ?? new Map();
			const event: PatternAwareEventInput = {
				sessionID: consumeInput.sessionID,
				turnID: consumeInput.turnID,
				tool,
				input: structuredClone(concrete),
				outcome: output?.isError ? "failure" : "success",
				...observation,
				durationMs,
				...(typeof concrete.operation === "string" ? { operation: concrete.operation } : {}),
				...(schemaHash === undefined ? {} : { schemaHash }),
				learnTarget: candidateToolNames(settings, actionSemantics).includes(tool),
			};
			batch.set(order, event);
			authoritativeBatches.set(key, batch);
			if (!patternSettings.multiStepEnabled) return undefined;
			await analysisTail;
			const store = await resolveStore(patternSettings);
			const ordered = [...batch.entries()].sort(([left], [right]) => left - right).map(([, item]) => item);
			const candidates = store.predictAfterBatch(
				consumeInput.sessionID,
				ordered,
				data.schemaHashes,
				patternSettings,
			);
			const actions = planActions(candidates, store, data.schemaHashes);
			// An observation can finish after its turn closes, or lose individual actions during admission.
			const carried = { signature: patternPredictionSignature(candidates),
				pending: new Set(actions.map((action) => action.feedback)), abandoned: false };
			for (const action of actions) predictionBatches.set(action.feedback, carried);
			carriedPredictions.set(consumeInput.sessionID, carried);
			return {
				id: `pattern:${consumeInput.turnID}`,
				source: "pattern_aware",
				revision: nextRevision(consumeInput.sessionID, consumeInput.turnID),
				actions,
			};
		}),
		onAdmitted: ({ feedback }) => {
			if (lifecycle.sealed) return;
			const context = asPatternPlanFeedback(feedback);
			if (context) predictionBatches.get(context)?.pending.delete(context);
		},
		onIssued: ({ feedback }) => {
			if (lifecycle.sealed) return;
			const context = asPatternPlanFeedback(feedback);
			if (context?.operation) return;
			if (context) context.store.issued(context.continuation);
			for (const patternID of context?.patternIDs ?? []) context?.store.issued(patternID);
		},
		onSettled: ({ feedback, settlement }) => {
			if (lifecycle.sealed) return;
			const context = asPatternPlanFeedback(feedback);
			const carried = context && predictionBatches.get(context);
			if (carried && settlement.observation === "unobserved") carried.abandoned = true;
			if (context?.operation) {
				// Execution failure retires this preparation hint; absence of an OS observation is not a negative example.
				if (settlement.observation === "unobserved" && settlement.cause.stage === "execution" &&
					operationBindings.get(context.operation.key) === context.operation) operationBindings.delete(context.operation.key);
				return;
			}
			if (context) context.store.settled(context.continuation, settlement);
			for (const patternID of context?.patternIDs ?? []) context?.store.settled(patternID, settlement);
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
		const patternSettings = settings.enabled ? sourceSettings(settings) : undefined;
		if (!patternSettings?.enabled || lifecycle.sealed) {
			carriedPredictions.delete(startInput.sessionID);
			return;
		}
		// Only a completed turn contributes its authoritative batch; entry discards any stale batch.
		const events = terminal !== undefined && batch?.size
			? [...batch.entries()].sort(([left], [right]) => left - right).map(([, event]) => event)
			: [];
		analysisTail = analysisTail
			.then(() => new Promise<void>(setImmediate))
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
		turnStarted: observeTurn,
		turnFinished: observeTurn,
		finishSession: () => lifecycle.run(async () => {
			await lifecycle.drain();
			revisions.clear();
			carriedPredictions.clear();
			operationBindings.clear();
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
			const leases = [...ownedStores.values()];
			ownedStores.clear();
			openedStore = undefined;
			authoritativeBatches.clear();
			operationBindings.clear();
			revisions.clear();
			carriedPredictions.clear();
			await Promise.allSettled(leases.map(async lease => (await lease).release()));
		}),
	};
}

export function patternPlanActionID(actionIdentity: string, parentActionID = "root"): string {
	return `pattern:${stableValueHash({ actionIdentity, parentActionID }).slice(0, 16)}`;
}

function patternPredictionSignature(candidates: readonly PatternAwareCandidate[]): string {
	return JSON.stringify(
		candidates
			.map((candidate) => [candidate.actionIdentity, candidate.horizon, candidate.latestHorizon] as const)
			.sort(([left], [right]) => left.localeCompare(right)),
	);
}

function clearAuthoritativeSession(
	batches: Map<string, Map<number, PatternAwareEventInput>>,
	sessionID: string,
): void {
	for (const [key, batch] of batches) {
		if (batch.values().next().value?.sessionID === sessionID) batches.delete(key);
	}
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
		input: candidate.input,
		diagnostic: candidate.diagnostic,
		horizon: candidate.horizon,
		latestHorizon: candidate.latestHorizon,
		empiricalProbability: candidate.empiricalProbability,
		conditionalProbability: candidate.conditionalProbability,
		expectedDurationMs: candidate.expectedDurationMs,
		expectedLatencyBenefitMs: candidate.expectedLatencyBenefitMs,
		...(candidate.background ? { background: true } : {}),
		depth: candidate.depth,
		...(dependsOn?.length ? { dependsOn } : {}),
		feedback: { ...patternAwareRuntimeContext(store, candidate), patternIDs: candidate.supportingPatternIDs },
	};
}

function extractOutputPaths(
	tool: string,
	actionInput: Readonly<Record<string, unknown>>,
	result: AgentToolResult<unknown> | undefined,
): readonly string[] | undefined {
	if ((tool !== "find" && tool !== "grep") || !result) return undefined;
	const searchRoot = typeof actionInput.path === "string" && actionInput.path ? actionInput.path : ".";
	const text = result.content
		.filter((item): item is Extract<(typeof result.content)[number], { type: "text" }> => item.type === "text")
		.map((item) => item.text)
		.join("\n");
	const paths = text
		.split(/\r?\n/)
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
