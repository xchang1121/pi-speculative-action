import path from "node:path";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
import type { AgentRuntime, AgentRuntimeEvents, AgentStartInput } from "./agent-integration.ts";
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
import { stableValueHash } from "./primitives.ts";
import { SourceRequestSuppressed, type SpeculativeActionSettings } from "./settlement.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { CallGenerator, CompletedCall, GeneratorTurn } from "./call-generators.ts";
import { speculativeConfig } from "./config-schema.ts";
export function coversDrafterRoot(
	hint: Pick<PatternAwareCandidate, "horizon" | "confidence" | "background" | "immediateConfidence">,
) {
	return !hint.background && hint.horizon === 0 && (hint.immediateConfidence ?? hint.confidence) >= 0.8;
}
export interface PatternPlanSourceController {
	readonly source: CallGenerator;
	readonly connect: (runtime: AgentRuntime) => () => void;
	/** The calls PatternAware expects next, without proposing them. */
	readonly hints: (input: {
		readonly sessionID: string;
		readonly startInput?: AgentStartInput;
		readonly schemaHashes: Readonly<Record<string, string>>;
		readonly patternIdentities?: Readonly<Record<string, string>>;
		readonly settings: SpeculativeActionSettings;
	}) => Promise<readonly Pick<PatternAwareCandidate, "tool" | "input" | "horizon" | "confidence">[]>;
	readonly dispose: () => Promise<void>;
}
export function createPatternPlanSource({
	sessionID,
	cwd,
	actionSemantics,
	stateDirectory,
	reservedRoot,
	workspaceIdentity,
	store: providedStore,
}: {
	readonly sessionID: string;
	readonly cwd: string;
	readonly actionSemantics: ActionSemanticsRegistry;
	readonly stateDirectory?: string;
	readonly reservedRoot?: boolean;
	readonly workspaceIdentity?: string;
	readonly store?: PatternAwareStore | Promise<PatternAwareStore>;
}): PatternPlanSourceController {
	cwd = path.resolve(cwd);
	const patternActionSemantics = patternAwareActionSemantics(actionSemantics, cwd);
	let openedStore:
		| {
				readonly key: string;
				readonly lease: Promise<PatternAwareStoreLease>;
		  }
		| undefined;
	const ownedStores = new Map<string, Promise<PatternAwareStoreLease>>();
	const lifecycle = new RuntimeLifecycleLane();
	const turns = new Map<string, Map<number, PatternAwareEventInput>>();
	const generators = new WeakMap<AgentStartInput, GeneratorTurn>();
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
	const roots = new WeakMap<
		AgentStartInput,
		Promise<{ store: PatternAwareStore; candidates: PatternAwareCandidate[] }>
	>();
	const rootPrediction = (
		sessionID: string,
		startInput: AgentStartInput | undefined,
		settings: PatternAwareSettings,
		schemas: Readonly<Record<string, string>>,
		identities?: Readonly<Record<string, string>>,
	) => {
		const predict = async () => {
			await analysisTail;
			const store = await resolveStore(settings);
			return { store, candidates: await store.predict(sessionID, schemas, settings, identities) };
		};
		const existing = startInput && roots.get(startInput);
		if (existing) return existing;
		const pending = predict();
		if (startInput) roots.set(startInput, pending);
		return pending;
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
		const facts = projectPatternAwareObservation(
			output?.result,
			{ tool, input, failed: output?.isError },
			cwd,
		);
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
		key: ActionKey,
		output: ToolSettlement,
		durationMs: number,
		identities?: Readonly<Record<string, string>>,
	): PatternAwareEventInput => {
		return {
			sessionID: startInput.sessionID,
			turnID: startInput.turnID,
			...eventData(
				key.tool,
				key.input,
				output,
				durationMs,
				identities && Object.hasOwn(identities, key.tool)
					? identities[key.tool]
					: key.semantics?.patternIdentity,
			),
			schemaHash: key.schemaHash,
			learnTarget: false,
		};
	};
	const emit = async (
		turn: GeneratorTurn,
		store: PatternAwareStore,
		candidates: readonly PatternAwareCandidate[],
		group: string,
		after?: string,
	) => {
		await Promise.all(
			candidates
				.filter((c) => !c.background)
				.map(async (c) => {
					const id = patternPredictionID(c.actionIdentity, group);
					const issued = await turn.issue(
						{
							...c,
							group,
							id,
							source: "pattern_aware",
							mode: c.presetID,
							onIssued: () => {
								for (const support of c.supportingPatternIDs) store.issued(support);
							},
							onFeedback: (settlement, served) => {
								for (const support of c.supportingPatternIDs) store.settled(support, settlement, served);
							},
						},
						{ after },
					);
					if (!issued) return;
					void issued.run.completion
						.then(async (result) => {
							if (result.status !== "completed" || turn.signal.aborted) return;
							if (coversDrafterRoot(c) && group === "pattern:" + turn.startInput.turnID)
								turn.peers("pattern_aware", [{ ...issued, output: result.output }]);
							const policy = sourceSettings(turn.settings);
							if (
								!policy.multiStepEnabled ||
								c.continuation.visitedPatternIDs.length >= policy.maxPredictionDepth
							)
								return;
							const event = predictedEvent(
								turn.startInput,
								issued.key,
								result.output,
								result.toolExecution.completedAt - result.toolExecution.startedAt,
								turn.data.patternIdentities,
							);
							const next = await store.continue(
								c.continuation,
								event,
								turn.data.schemaHashes,
								false,
								policy,
								turn.data.patternIdentities,
							);
							if (next.length)
								await emit(turn, store, next, id, issued.effect === "observation" ? after : issued.run.runID);
						})
						.catch(() => {});
				}),
		);
	};
	const source: CallGenerator = {
		async start(turn) {
			generators.set(turn.startInput, turn);
			const policy = sourceSettings(turn.settings);
			if (!turn.settings.enabled || !policy.enabled) return;
			await turn.request("pattern_aware", "proposal", turn.signal, async () => {
				const { store, candidates } = await rootPrediction(
					sessionID,
					turn.startInput,
					policy,
					turn.data.schemaHashes,
					turn.data.patternIdentities,
				);
				if (!candidates.some((candidate) => !candidate.background))
					throw new SourceRequestSuppressed({ stage: "source",
						code: store.patternCount ? "no_foreground_match" : "pattern_pool_empty" });
				await emit(turn, store, candidates, "pattern:" + turn.startInput.turnID);
				return candidates.filter((c) => !c.background).length;
			});
		},
		async peers(turn, source, calls: readonly CompletedCall[]) {
			const policy = sourceSettings(turn.settings);
			if (
				source === "pattern_aware" ||
				!policy.enabled ||
				!policy.multiStepEnabled ||
				!turn.settings.enabled ||
				!calls.length
			)
				return;
			const store = await predictionStore(policy, turn.signal);
			if (!store) return;
			const group = "pattern:peer:" + calls.map((p) => p.run.runID).join(",");
			const next = await store.predictAfterBatch(
				sessionID,
				calls.map((p) =>
					predictedEvent(
						turn.startInput,
						p.key,
						p.output,
						0,
						turn.data.patternIdentities,
					),
				),
				turn.data.schemaHashes,
				policy,
				{ visitedPatternIDs: [group], pathProbability: 0.5 },
				turn.data.patternIdentities,
			);
			const writers = calls.filter((p) => p.effect !== "observation");
			if (writers.length <= 1) await emit(turn, store, next, group, writers[0]?.run.runID);
		},
	};
	const observe = async (event: AgentRuntimeEvents["actor_observed"]) => {
		const { settings, data, consumeInput, action, tool, concrete, output, durationMs, order } = event;
		const schemaHash = action?.schemaHash ?? data.schemaHashes[tool];
		const key = consumeInput.turnID;
		let batch = turns.get(key);
		if (!batch) turns.set(key, (batch = new Map()));
		batch.set(order, {
			sessionID: consumeInput.sessionID,
			turnID: consumeInput.turnID,
			...eventData(
				tool,
				concrete,
				output,
				durationMs,
				data.patternIdentities?.[tool] ?? action?.semantics?.patternIdentity,
			),
			schemaHash,
			learnTarget: data.tools.has(tool),
		});
		const turn = generators.get(event.startInput);
		const policy = sourceSettings(settings);
		if (!turn || turn.signal.aborted || !settings.enabled || !policy.enabled || !policy.multiStepEnabled)
			return;
		const store = await predictionStore(policy, turn.signal);
		if (!store) return;
		const ordered = [...batch.entries()].sort(([a], [b]) => a - b).map(([, v]) => v);
		const next = await store.predictAfterBatch(
			sessionID,
			ordered,
			data.schemaHashes,
			policy,
			undefined,
			data.patternIdentities,
		);
		await emit(turn, store, next, "pattern:" + consumeInput.turnID + ":after");
	};
	const observeTurn = (
		startInput: AgentStartInput,
		settings: SpeculativeActionSettings,
		terminal?: boolean,
	): void => {
		if (lifecycle.sealed) return;
		const key = startInput.turnID;
		const batch = turns.get(key);
		turns.delete(key);
		const patternSettings = sourceSettings(settings);
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
	};
	return {
		source,
		connect: (runtime) =>
			runtime.subscribe({
				actor_observed: observe,
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
		hints: ({ sessionID, startInput, schemaHashes, settings, patternIdentities }) =>
			admit(settings, async (patternSettings) => {
				if (!settings.enabled || !patternSettings.enabled) return [];
				const { candidates } = await rootPrediction(
					sessionID,
					startInput,
					patternSettings,
					schemaHashes,
					patternIdentities,
				);
				return candidates
					.filter(
						({ background, tool }) =>
							!background &&
							(!startInput || Object.hasOwn(schemaHashes, tool)) &&
							(!settings.tools || settings.tools.includes(tool)),
					)
					.map(({ tool, input, horizon, confidence, immediateConfidence }) => ({
						tool,
						input,
						horizon,
						confidence: horizon === 0 ? (immediateConfidence ?? confidence) : confidence,
					}))
					.sort((a, b) => a.horizon - b.horizon || b.confidence - a.confidence)
					.slice(0, speculativeConfig().prediction.maxHints);
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
function patternPredictionID(actionIdentity: string, parentActionID = "root"): string {
	return `pattern:${stableValueHash({ actionIdentity, parentActionID }).slice(0, 16)}`;
}
