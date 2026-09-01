import path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import type { ActionSemanticsRegistry } from "./action-semantics.ts";
import {
	agentBatchKey,
	definitionSchemaHashes,
	type AgentPlanSource,
	type AgentStartInput,
} from "./agent-runtime-types.ts";
import {
	acquirePatternAwareStore,
	asPatternAwareRuntimeContext,
	type PatternAwareCandidate,
	type PatternAwareEventInput,
	type PatternAwareRuntimeContext,
	type PatternAwareSettings,
	type PatternAwareStore,
	type PatternAwareStoreLease,
	patternAwareAnalyzerKey,
	patternAwareRuntimeContext,
	patternAwareSettings,
	projectPatternAwareObservation,
} from "./pattern-aware.ts";
import type { PlanAction } from "./plan-proposal.ts";
import type { SpeculativeActionSettings } from "./runtime.ts";
import { candidateExecutionMs, candidateToolNames } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

type PatternPlanFeedback = PatternAwareRuntimeContext & { readonly patternIDs: ReadonlyArray<string> };

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

export function createPatternPlanSource(input: {
	readonly sessionID: string;
	readonly cwd: string;
	readonly actionSemantics: ActionSemanticsRegistry;
	readonly projectionRules: readonly ActionProjectionRule<ToolSettlement>[];
	readonly stateDirectory?: string;
	readonly workspaceIdentity?: string;
	readonly store?: PatternAwareStore | Promise<PatternAwareStore>;
}): PatternPlanSourceController {
	const patternActionSemantics = {
		namespace: "pi-action-semantics-v1",
		actionKey: (tool: string, actionInput: Readonly<Record<string, unknown>>, schemaHash?: string) =>
			input.actionSemantics.buildKey(tool, actionInput, input.cwd, schemaHash),
		projectors: input.projectionRules,
	};
	let openedStore: Promise<PatternAwareStoreLease> | undefined;
	let openedStoreKey: string | undefined;
	const authoritativeBatches = new Map<string, Map<number, PatternAwareEventInput>>();
	const revisions = new Map<string, number>();
	const carriedPredictions = new Map<string, string>();
	let analysisTail: Promise<void> = Promise.resolve();

	const queueAnalysis = (analysis: () => void | Promise<void>): void => {
		analysisTail = analysisTail
			.then(() => new Promise<void>((resolve) => setTimeout(resolve, 0)))
			.then(analysis)
			.catch(() => {
				// Optional learning cannot poison later observations or the Actor lifecycle.
			});
	};
	const sourceSettings = (settings: SpeculativeActionSettings): PatternAwareSettings =>
		patternAwareSettings(settings.sourceConfig?.patternAware);
	const nextRevision = (sessionID: string, turnID: string): number => {
		const key = agentBatchKey(sessionID, turnID);
		const revision = (revisions.get(key) ?? -1) + 1;
		revisions.set(key, revision);
		return revision;
	};
	const resolveStore = async (settings: SpeculativeActionSettings): Promise<PatternAwareStore> => {
		if (input.store) return input.store;
		const patternSettings = sourceSettings(settings);
		const configurationKey = patternAwareAnalyzerKey(patternSettings);
		if (openedStore && openedStoreKey !== configurationKey) {
			const previous = await openedStore;
			previous.store.finishSession(input.sessionID);
			await previous.release();
			openedStore = undefined;
		}
		openedStoreKey = configurationKey;
		openedStore ??= acquirePatternAwareStore(
			input.workspaceIdentity ?? input.cwd,
			patternSettings,
			input.stateDirectory,
			patternActionSemantics,
		);
		return (await openedStore).store;
	};

	const source: AgentPlanSource = {
		id: "pattern_aware",
		enabled: (settings) => sourceSettings(settings).enabled,
		multiStepEnabled: (settings) => sourceSettings(settings).multiStepEnabled,
		requestLifetime: "actor_decision",
		propose: async ({ startInput, settings, definitions }) => {
			const patternSettings = sourceSettings(settings);
			if (!patternSettings.enabled) return undefined;
			await analysisTail;
			const store = await resolveStore(settings);
			const candidates = store.predict(startInput.sessionID, definitionSchemaHashes(definitions), patternSettings);
			const signature = patternPredictionSignature(candidates);
			const carried = carriedPredictions.get(startInput.sessionID);
			carriedPredictions.delete(startInput.sessionID);
			if (carried === signature || !candidates.length) return undefined;
			return {
				id: `pattern:${startInput.turnID}`,
				source: "pattern_aware",
				revision: nextRevision(startInput.sessionID, startInput.turnID),
				actions: candidates.map((candidate) =>
					patternPlanAction(candidate, store, patternPlanActionID(candidate.actionIdentity)),
				),
			};
		},
		continue: async ({
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
		}) => {
			if (signal.aborted) return undefined;
			const context = asPatternPlanFeedback(feedback);
			if (!context) return undefined;
			const action = adoptedAction ?? candidate;
			const observation = projectPatternAwareObservation(
				output.result,
				extractOutputPaths(action.key.tool, action.input, output.result),
				input.cwd,
			);
			const next = context.store.continue(
				context.continuation,
				{
					sessionID: startInput.sessionID,
					turnID: startInput.turnID,
					tool: action.key.tool,
					input: structuredClone(action.input) as Record<string, unknown>,
					outcome: output.isError ? "failure" : "success",
					...observation,
					durationMs: candidateExecutionMs(candidate),
					schemaHash: action.key.schemaHash,
					...(typeof action.input.operation === "string" ? { operation: action.input.operation } : {}),
					learnTarget: false,
				},
				data.schemaHashes,
				trigger === "actor_adopted",
				sourceSettings(settings),
			);
			if (!next.length) return undefined;
			return {
				proposalID,
				source: "pattern_aware",
				revision,
				upsert: next.map((item) =>
					patternPlanAction(item, context.store, patternPlanActionID(item.actionIdentity, actionID), [
						{ actionID, condition: "execution_succeeded" },
					]),
				),
			};
		},
		observe: async ({ startInput, data, settings, consumeInput, tool, concrete, output, durationMs, order }) => {
			const patternSettings = sourceSettings(settings);
			if (!patternSettings.enabled) return undefined;
			const definition = startInput.tools.find((item) => item.name === tool);
			const observation = projectPatternAwareObservation(
				output?.result,
				extractOutputPaths(tool, concrete, output?.result),
				input.cwd,
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
				...(definition ? { schemaHash: stableValueHash(definition.parameters) } : {}),
				learnTarget: candidateToolNames(settings, input.actionSemantics).includes(tool),
			};
			batch.set(order, event);
			authoritativeBatches.set(key, batch);
			if (!patternSettings.multiStepEnabled) return undefined;
			await analysisTail;
			const store = await resolveStore(settings);
			const ordered = [...batch.entries()].sort(([left], [right]) => left - right).map(([, item]) => item);
			const candidates = store.predictAfterBatch(
				consumeInput.sessionID,
				ordered,
				data.schemaHashes,
				patternSettings,
			);
			carriedPredictions.set(consumeInput.sessionID, patternPredictionSignature(candidates));
			return {
				id: `pattern:${consumeInput.turnID}`,
				source: "pattern_aware",
				revision: nextRevision(consumeInput.sessionID, consumeInput.turnID),
				actions: candidates.map((candidate) =>
					patternPlanAction(candidate, store, patternPlanActionID(candidate.actionIdentity)),
				),
			};
		},
		onIssued: ({ feedback }) => {
			const context = asPatternPlanFeedback(feedback);
			for (const patternID of context?.patternIDs ?? []) context?.store.issued(patternID);
		},
		onSettled: ({ feedback, settlement }) => {
			const context = asPatternPlanFeedback(feedback);
			for (const patternID of context?.patternIDs ?? []) context?.store.settled(patternID, settlement);
		},
		flush: async () => {
			await analysisTail;
			if (openedStore) await (await openedStore).store.flush();
			if (input.store) await (await input.store).flush();
		},
	};

	return {
		source,
		turnStarted: (startInput, settings) => {
			const key = agentBatchKey(startInput.sessionID, startInput.turnID);
			authoritativeBatches.delete(key);
			revisions.delete(key);
			if (!settings.enabled || !sourceSettings(settings).enabled) {
				carriedPredictions.delete(startInput.sessionID);
				return;
			}
			queueAnalysis(async () => {
				const store = await resolveStore(settings);
				store.observeTurn();
			});
		},
		turnFinished: (startInput, settings, terminal) => {
			const key = agentBatchKey(startInput.sessionID, startInput.turnID);
			const batch = authoritativeBatches.get(key);
			authoritativeBatches.delete(key);
			revisions.delete(key);
			if (terminal) carriedPredictions.delete(startInput.sessionID);
			if (!settings.enabled || !sourceSettings(settings).enabled) {
				carriedPredictions.delete(startInput.sessionID);
				return;
			}
			const events = batch?.size
				? [...batch.entries()].sort(([left], [right]) => left - right).map(([, event]) => event)
				: [];
			queueAnalysis(async () => {
				const store = await resolveStore(settings);
				if (events.length) store.observeBatch(events);
				store.observeTurn();
				if (terminal) store.finishSession(startInput.sessionID);
			});
		},
		finishSession: async () => {
			revisions.clear();
			carriedPredictions.clear();
			clearAuthoritativeSession(authoritativeBatches, input.sessionID);
			await analysisTail;
			const store = input.store
				? await input.store
				: openedStore
					? (await openedStore).store
					: undefined;
			if (!store) return;
			store.finishSession(input.sessionID);
			try {
				await store.flush();
			} catch {
				// Persistence failure must not change Agent lifecycle semantics.
			}
		},
		dispose: async () => {
			if (!openedStore) return;
			try {
				await (await openedStore).release();
			} catch {
				// Persistence failure must not change Agent uninstall semantics.
			}
		},
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
	return { ...context, patternIDs };
}

function patternPlanAction(
	candidate: PatternAwareCandidate,
	store: PatternAwareStore,
	id: string,
	dependsOn?: PlanAction["dependsOn"],
): PlanAction {
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
