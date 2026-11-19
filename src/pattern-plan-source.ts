import path from "node:path";
import type { ActionProjectionRule } from "./action-semantics.ts";
import { type ActionSemanticsRegistry } from "./action-semantics.ts";
import {
	agentBatchKey,
	type AgentPlanSource,
	type AgentRuntime,
	type AgentRuntimeEvents,
	type AgentStartInput,
} from "./agent-integration.ts";
import type { ToolSettlement } from "./execution-world.ts";
import {
	acquirePatternAwareStore,
	patternAwareActionSemantics,
	patternAwareAnalyzerKey,
	patternAwareSettings,
	projectPatternAwareObservation,
	type PatternAwareCandidate,
	type PatternAwareEventInput,
	type PatternAwareSettings,
	type PatternAwareStore,
	type PatternAwareStoreLease,
} from "./pattern-aware.ts";
import { nonNegativeFinite, stableValueHash } from "./primitives.ts";
import type { SpeculativeActionSettings, SpeculativeCandidate } from "./runtime-engine.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { PlanAction } from "./settlement.ts";
type PatternPlanFeedback = Pick<PatternAwareCandidate, "continuation" | "supportingPatternIDs"> & {
	readonly store: PatternAwareStore;
};
type CarriedPrediction = {
	readonly signature: string;
	readonly pending: Set<PatternPlanFeedback>;
	abandoned: boolean;
};
type PatternTurn = {
	readonly events: Map<number, PatternAwareEventInput>;
	revision: number;
	observationRevision: number;
};
export interface PatternPlanSourceController {
	readonly source: AgentPlanSource;
	readonly connect: (runtime: AgentRuntime) => () => void;
	/** The calls PatternAware expects next, without proposing them. */
	readonly hints: (input: {
		readonly sessionID: string;
		readonly schemaHashes: Readonly<Record<string, string>>;
		readonly patternIdentities?: Readonly<Record<string, string>>;
		readonly settings: SpeculativeActionSettings;
	}) => Promise<readonly Pick<PatternAwareCandidate, "tool" | "input" | "horizon">[]>;
	readonly dispose: () => Promise<void>;
}
export function createPatternPlanSource({
	sessionID,
	cwd,
	actionSemantics,
	projectionRules,
	stateDirectory,
	reservedRoot,
	workspaceIdentity,
	store: providedStore,
}: {
	readonly sessionID: string;
	readonly cwd: string;
	readonly actionSemantics: ActionSemanticsRegistry;
	readonly projectionRules: readonly ActionProjectionRule<ToolSettlement>[];
	readonly stateDirectory?: string;
	readonly reservedRoot?: boolean;
	readonly workspaceIdentity?: string;
	readonly store?: PatternAwareStore | Promise<PatternAwareStore>;
}): PatternPlanSourceController {
	cwd = path.resolve(cwd);
	const patternActionSemantics = patternAwareActionSemantics(actionSemantics, cwd, projectionRules);
	let openedStore:
		| {
				readonly key: string;
				readonly lease: Promise<PatternAwareStoreLease>;
		  }
		| undefined;
	const ownedStores = new Map<string, Promise<PatternAwareStoreLease>>();
	const lifecycle = new RuntimeLifecycleLane();
	const turns = new Map<string, PatternTurn>();
	const carriedPredictions = new Map<string, CarriedPrediction>();
	const feedbacks = new WeakMap<
		object,
		{
			readonly context?: PatternPlanFeedback;
			carried?: CarriedPrediction;
		}
	>();
	let analysisTail: Promise<void> = Promise.resolve();
	const sourceSettings = (settings: SpeculativeActionSettings): PatternAwareSettings => {
		const pattern = patternAwareSettings(settings.sourceConfig?.patternAware);
		return pattern;
	};
	const admit = <Value>(
		settings: SpeculativeActionSettings,
		operation: (settings: PatternAwareSettings) => Promise<Value>,
	): Promise<Value> => {
		try {
			const pattern = sourceSettings(settings);
			return lifecycle.admit(() => operation(pattern));
		} catch (error) {
			return Promise.reject(error);
		}
	};
	const turnState = (sessionID: string, turnID: string): PatternTurn => {
		const key = agentBatchKey(sessionID, turnID);
		let turn = turns.get(key);
		if (!turn) turns.set(key, (turn = { events: new Map(), revision: -1, observationRevision: -1 }));
		return turn;
	};
	const resolveStore = async (patternSettings: PatternAwareSettings): Promise<PatternAwareStore> => {
		if (providedStore) return providedStore;
		const configurationKey = patternAwareAnalyzerKey(patternSettings);
		if (!openedStore || openedStore.key !== configurationKey) {
			const previous = openedStore;
			const retained = ownedStores.get(configurationKey);
			const opening = {
				key: configurationKey,
				lease: Promise.resolve().then(async () => {
					if (previous)
						await previous.lease.then(({ store }) => store.finishSession(sessionID)).catch(() => undefined); // Failed loading cannot poison the next analyzer.
					return (
						retained ??
						acquirePatternAwareStore(
							workspaceIdentity ?? cwd,
							patternSettings,
							stateDirectory,
							patternActionSemantics,
							reservedRoot,
						)
					);
				}),
			};
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
	const predictionStore = async (settings: PatternAwareSettings, signal?: AbortSignal) => {
		await analysisTail;
		if (signal?.aborted) return;
		const store = await resolveStore(settings);
		return signal?.aborted ? undefined : store;
	};
	const flushStores = async (finish = false): Promise<void> => {
		await analysisTail;
		const stores = providedStore
			? [Promise.resolve(providedStore)]
			: [...ownedStores.values()].map(async (lease) => (await lease).store);
		const results = await Promise.allSettled(
			stores.map(async (pending) => {
				const store = await pending;
				if (finish) store.finishSession(sessionID);
				await store.flush();
			}),
		);
		const failure = results.find((result) => result.status === "rejected");
		if (failure) throw failure.reason;
	};
	const eventData = (
		tool: string,
		input: Readonly<Record<string, unknown>>,
		output: ToolSettlement | undefined,
		durationMs: number,
		patternIdentity?: string,
	) => {
		const facts = projectPatternAwareObservation(output?.result, { tool, input, failed: output?.isError }, cwd);
		const exit = output?.exitCode === undefined ? undefined : `exit:${output.exitCode}`;
		return {
			tool,
			...(patternIdentity ? { patternIdentity } : {}),
			input: structuredClone(input),
			durationMs,
			...facts,
			outcome: output?.isError ? ("failure" as const) : ("success" as const),
			...(output?.isError
				? { errorClass: [exit, facts.errorClass].filter(Boolean).join(" ") || "failed" }
				: {}),
			...(typeof input.operation === "string" ? { operation: input.operation } : {}),
		};
	};
	const predictedEvent = (
		startInput: AgentStartInput,
		action: Pick<SpeculativeCandidate, "key" | "input">,
		output: ToolSettlement,
		durationMs: number,
		identities?: Readonly<Record<string, string>>,
	): PatternAwareEventInput => {
		if (!action.key) throw new Error("prediction_key_required");
		return {
			sessionID: startInput.sessionID,
			turnID: startInput.turnID,
			...eventData(
				action.key.tool,
				action.input,
				output,
				durationMs,
				identities && Object.hasOwn(identities, action.key.tool)
					? identities[action.key.tool]
					: action.key.semantics?.patternIdentity,
			),
			schemaHash: action.key.schemaHash,
			learnTarget: false,
		};
	};
	const planActions = (
		candidates: readonly PatternAwareCandidate[],
		store: PatternAwareStore,
		dependsOn?: PlanAction["dependsOn"],
		parentID?: string,
	) =>
		candidates.map((candidate) => {
			const action = patternPlanAction(
				candidate,
				store,
				patternPlanActionID(candidate.actionIdentity, parentID),
				dependsOn,
			);
			feedbacks.set(action.feedback, { context: action.feedback });
			return action;
		});
	const source: AgentPlanSource = {
		id: "pattern_aware",
		enabled: (settings) => !lifecycle.sealed && sourceSettings(settings).enabled,
		multiStepEnabled: (settings, feedback) => {
			const pattern = sourceSettings(settings);
			return (
				pattern.multiStepEnabled &&
				(feedback === undefined ||
					(feedbacks.get(feedback as object)?.context?.continuation.visitedPatternIDs.length ?? Infinity) <
						pattern.maxPredictionDepth)
			);
		},
		requestLifetime: "actor_decision",
		propose: ({ startInput, data, settings, signal }) =>
			admit(settings, async (patternSettings) => {
				if (!patternSettings.enabled) return undefined;
				const turn = turnState(startInput.sessionID, startInput.turnID);
				const store = await predictionStore(patternSettings, signal);
				if (!store) return undefined;
				const candidates = await store.predict(
					startInput.sessionID,
					data.schemaHashes,
					patternSettings,
					data.patternIdentities,
				);
				const carried = carriedPredictions.get(startInput.sessionID);
				if (signal.aborted) return undefined;
				carriedPredictions.delete(startInput.sessionID);
				const repeated =
					!candidates.length ||
					(carried?.signature === patternPredictionSignature(candidates) &&
						!carried.pending.size &&
						!carried.abandoned);
				if (repeated) return undefined;
				return {
					id: `pattern:${startInput.turnID}`,
					source: "pattern_aware",
					revision: ++turn.revision,
					actions: planActions(candidates, store),
				};
			}),
		continueFrom: ({ startInput, data, settings, batch, signal }) =>
			admit(settings, async (patternSettings) => {
				const store = await predictionStore(patternSettings, signal);
				if (!store) return undefined;
				const id = `pattern:peer:${stableValueHash(batch.map(({ identity }) => identity.id))}`;
				const candidates = await store.predictAfterBatch(
					startInput.sessionID,
					batch.map(({ candidate, output }) =>
						predictedEvent(
							startInput,
							candidate,
							output,
							nonNegativeFinite(candidate.executionMs),
							data.patternIdentities,
						),
					),
					data.schemaHashes,
					patternSettings,
					// No calibrated joint confidence is supplied for the foreign batch; use a neutral prior.
					{ visitedPatternIDs: [id], pathProbability: 0.5 },
					data.patternIdentities,
				);
				if (!candidates.length) return undefined;
				const dependencies = batch.map(({ identity }) => ({
					proposalID: identity.proposalID,
					actionID: identity.actionID,
					identity: identity.id,
					condition: "execution_succeeded" as const,
				}));
				return {
					id,
					source: "pattern_aware",
					revision: 0,
					actions: planActions(candidates, store, dependencies),
				};
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
		}) =>
			admit(settings, async (patternSettings) => {
				const context = feedbacks.get(feedback as object)?.context;
				if (
					!context ||
					signal.aborted ||
					output === undefined ||
					context.continuation.visitedPatternIDs.length >= patternSettings.maxPredictionDepth
				)
					return undefined;
				const next = await context.store.continue(
					context.continuation,
					predictedEvent(
						startInput,
						adoptedAction ?? candidate,
						output,
						nonNegativeFinite(candidate.executionMs),
						data.patternIdentities,
					),
					data.schemaHashes,
					trigger === "actor_adopted",
					patternSettings,
					data.patternIdentities,
				);
				if (!next.length) return undefined;
				return {
					proposalID,
					source: "pattern_aware",
					revision,
					upsert: planActions(next, context.store, [{ actionID, condition: "execution_succeeded" }], actionID),
				};
			}),
	};
	const observe = (
		{
			data,
			settings,
			consumeInput,
			action,
			tool,
			concrete,
			output,
			durationMs,
			order,
			signal,
		}: AgentRuntimeEvents["actor_observed"],
		reserveRevision: (id: string, minimum: number) => number,
	) =>
		admit(settings, async (patternSettings) => {
			const schemaHash = action?.schemaHash ?? data.schemaHashes[tool];
			const observed = eventData(
				tool,
				concrete,
				output,
				durationMs,
				data.patternIdentities && Object.hasOwn(data.patternIdentities, tool)
					? data.patternIdentities[tool]
					: action?.semantics?.patternIdentity,
			);
			const turn = turnState(consumeInput.sessionID, consumeInput.turnID);
			const batch = turn.events;
			batch.set(order, {
				sessionID: consumeInput.sessionID,
				turnID: consumeInput.turnID,
				...observed,
				...(schemaHash === undefined ? {} : { schemaHash }),
				learnTarget: data.tools.has(tool),
			});
			if (!settings.enabled || !patternSettings.enabled) return undefined;
			let actions: PlanAction[] = [];
			if ((!patternSettings.multiStepEnabled && !actions.length) || signal?.aborted) return undefined;
			if (patternSettings.multiStepEnabled) {
				const store = await predictionStore(patternSettings, signal);
				if (!store) return undefined;
				const ordered = [...batch.entries()].sort(([left], [right]) => left - right).map(([, item]) => item);
				const candidates = await store.predictAfterBatch(
					consumeInput.sessionID,
					ordered,
					data.schemaHashes,
					patternSettings,
					undefined,
					data.patternIdentities,
				);
				const predicted = planActions(candidates, store);
				// An observation can finish after its turn closes, or lose individual actions during admission.
				const carried = {
					signature: patternPredictionSignature(candidates),
					pending: new Set(predicted.map((action) => action.feedback)),
					abandoned: false,
				};
				for (const action of predicted) feedbacks.get(action.feedback)!.carried = carried;
				carriedPredictions.set(consumeInput.sessionID, carried);
				actions = [...predicted, ...actions];
			}
			// Its own namespace never supersedes the turn's still-pending predictions; the runtime's revision covers continuations.
			const id = `pattern:${consumeInput.turnID}:after`;
			const minimum = ++turn.observationRevision;
			const revision = reserveRevision(id, minimum);
			turn.observationRevision = revision;
			return {
				id,
				source: "pattern_aware",
				revision,
				actions,
				retainDescendants: true as const,
			};
		});
	const observeTurn = (
		startInput: AgentStartInput,
		settings: SpeculativeActionSettings,
		terminal?: boolean,
	): void => {
		if (lifecycle.sealed) return;
		const key = agentBatchKey(startInput.sessionID, startInput.turnID);
		const batch = turns.get(key)?.events;
		turns.delete(key);
		if (terminal) carriedPredictions.delete(startInput.sessionID);
		const patternSettings = sourceSettings(settings);
		if (!settings.enabled || !patternSettings.enabled) carriedPredictions.delete(startInput.sessionID);
		// Only a completed turn contributes its authoritative batch; entry discards any stale batch.
		const events =
			terminal !== undefined && batch?.size
				? [...batch.entries()].sort(([left], [right]) => left - right).map(([, event]) => event)
				: [];
		analysisTail = analysisTail
			.then(() => new Promise<void>(setImmediate))
			.then(async () => {
				const store = await resolveStore(patternSettings);
				if (events.length) store.observeBatch(events);
				store.observeTurn();
				if (terminal) store.finishSession(startInput.sessionID, true);
			})
			.catch(() => {
				// Optional learning cannot poison later observations or the Actor lifecycle.
			});
	};
	const clearSession = async () => {
		await lifecycle.drain();
		carriedPredictions.clear();
		for (const [key, turn] of turns) {
			turn.revision = turn.observationRevision = -1;
			if (!turn.events.size || turn.events.values().next().value?.sessionID === sessionID) turns.delete(key);
		}
	};
	return {
		source,
		connect: (runtime) =>
			runtime.subscribe({
				actor_observed: async (event) => {
					const update = await observe(event, (id, minimum) =>
						runtime.reservePlanRevision(event.sessionID, id, minimum),
					);
					await runtime.submitPlan(event.startInput, update);
				},
				prediction_admitted: ({ feedback, issued }) => {
					if (lifecycle.sealed) return;
					const state = feedbacks.get(feedback as object);
					if (state?.context) {
						state.carried?.pending.delete(state.context);
						if (issued)
							for (const support of state.context.supportingPatternIDs) state.context.store.issued(support);
					}
				},
				prediction_feedback: ({ feedback, settlement, served }) => {
					if (lifecycle.sealed) return;
					const { context, carried } = feedbacks.get(feedback as object) ?? {};
					if (carried && settlement.observation === "unobserved") carried.abandoned = true;
					if (context)
						for (const support of context.supportingPatternIDs)
							context.store.settled(support, settlement, served);
				},
				turn_started: ({ startInput, settings }) => observeTurn(startInput, settings),
				turn_finished: async ({ startInput, settings, terminal }) => {
					observeTurn(startInput, settings, terminal);
					await lifecycle.run(() => flushStores());
				},
				session_closed: () =>
					lifecycle.run(async () => {
						await clearSession();
						await flushStores(true);
					}),
			}),
		hints: ({ sessionID, schemaHashes, settings, patternIdentities }) =>
			admit(settings, async (patternSettings) => {
				if (!settings.enabled || !patternSettings.enabled) return [];
				const store = await predictionStore(patternSettings);
				return (await store!.predict(sessionID, schemaHashes, patternSettings, patternIdentities))
					.slice(0, speculativeConfig().prediction.maxHints)
					.map(({ tool, input, horizon }) => ({ tool, input, horizon }));
			}),
		dispose: () =>
			lifecycle.close(async () => {
				await analysisTail;
				await clearSession();
				const leases = [...ownedStores.values()];
				ownedStores.clear();
				openedStore = undefined;
				turns.clear();
				await Promise.allSettled(leases.map(async (lease) => (await lease).release()));
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
function patternPlanAction(
	candidate: PatternAwareCandidate,
	store: PatternAwareStore,
	id: string,
	dependsOn?: PlanAction["dependsOn"],
): PlanAction & {
	readonly feedback: PatternPlanFeedback;
} {
	const {
		tool,
		input,
		horizon,
		latestHorizon,
		empiricalProbability,
		conditionalProbability,
		confidence,
		background,
		depth,
		presetID,
		supportingPatternIDs,
	} = candidate;
	return {
		id,
		type: "tool_call",
		tool,
		input,
		horizon,
		latestHorizon,
		empiricalProbability,
		conditionalProbability,
		confidence,
		depth,
		...(presetID ? { mode: presetID } : {}),
		...(background ? { background: true } : {}),
		...(dependsOn?.length ? { dependsOn } : {}),
		feedback: {
			store,
			continuation: candidate.continuation,
			supportingPatternIDs,
		},
	};
}
import { speculativeConfig } from "./config-schema.ts";
