import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { type ActionKey, type ActionKeyProjector, actionKeyCovers } from "./action-semantics.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import {
	patternSessionBudgets,
	type PatternPendingValidation,
	type PatternRecurrentAction,
	PatternSessionRegistry,
	type PatternSessionState,
} from "./pattern-session-state.ts";
import { PpmCountTrie, type PpmCountTrieRow, type PpmProbabilityEstimate } from "./ppm-count-trie.ts";
import type { PredictionSettlement, ResolutionStage } from "./settlement.ts";
import { stableEqual as sameValue, stableStringify } from "./stable-json.ts";

export type PatternAwareSettings = {
	readonly enabled: boolean;
	/** Admit future-gap/preparation candidates and expand completed predictions into a multi-step frontier. */
	readonly multiStepEnabled: boolean;
	readonly maxContextLength: number;
	/** Maximum competing concrete actions retained per tool at each PatternAware frontier. */
	readonly beamWidth: number;
	/** Maximum number of recursively predicted actions on one branch. */
	readonly maxPredictionDepth: number;
	readonly maxFutureGap: number;
	/** Weighted future-gap quantile used as the expected launch horizon; the deadline keeps full observed support. */
	readonly futureGapCoverage: number;
	readonly decayHalfLifeEvents: number;
	/** Support required to promote a relation after its single bounded first-recurrence probe. */
	readonly minOccurrences: number;
	/** Minimum historical replay precision required for a concrete argument mapper. */
	readonly minBindingReplayProbability: number;
	readonly maxPatterns: number;
};

export type PatternAwareEventSignature = {
	readonly tool: string;
	readonly outcome: "success" | "failure";
	readonly operation?: string;
	readonly outputShape?: string;
};

export type PatternAwareEventInput = {
	readonly sessionID: string;
	readonly turnID: string;
	readonly tool: string;
	readonly input: Record<string, unknown>;
	readonly outcome: "success" | "failure";
	readonly output?: unknown;
	readonly outputPaths?: ReadonlyArray<string>;
	readonly durationMs: number;
	readonly operation?: string;
	readonly schemaHash?: string;
	readonly learnTarget?: boolean;
};

export type PatternAwareActionSemantics = {
	/** Stable persistence namespace for the action-key contract. */
	readonly namespace?: string;
	/** Deterministic K(a) projection for one namespace; repeated inputs may be memoized. */
	readonly actionKey: (
		tool: string,
		input: Readonly<Record<string, unknown>>,
		schemaHash?: string,
	) => ActionKey | undefined;
	readonly projectors?: readonly ActionKeyProjector[];
};

export type PatternAwareEvent = PatternAwareEventInput & {
	readonly sequence: number;
	readonly batchID?: string;
	readonly batchIndex?: number;
	readonly batchSize?: number;
};

export type PatternAwarePath = ReadonlyArray<string | number>;

export type PatternAwareDependencySource = {
	readonly relativeEvent: number;
	readonly field: "input" | "output" | "outputPaths";
	readonly path: PatternAwarePath;
	readonly itemPath?: PatternAwarePath;
};

export type PatternAwareDependency = {
	readonly targetPath: PatternAwarePath;
	readonly sources: ReadonlyArray<PatternAwareDependencySource>;
};

export type PatternAwareBinding = (
	| {
			readonly type: "event";
			readonly relativeEvent: number;
			readonly field: "input" | "output" | "outputPaths";
			readonly path: PatternAwarePath;
	  }
	| {
			readonly type: "each";
			readonly relativeEvent: number;
			readonly field: "input" | "output" | "outputPaths";
			readonly path: PatternAwarePath;
			readonly itemPath: PatternAwarePath;
	  }
	| {
			readonly type: "constant";
			readonly value: unknown;
	  }
	| {
			readonly type: "transform";
			readonly operation: "dirname" | "basename" | "normalize_path";
			readonly source: PatternAwareBinding;
	  }
	| {
			readonly type: "coalesce";
			readonly sources: ReadonlyArray<PatternAwareBinding>;
	  }
	| {
			readonly type: "template";
			readonly source: PatternAwareBinding;
			readonly prefix: string;
			readonly suffix: string;
	  }
	| {
			readonly type: "join";
			readonly operation: "join_path";
			readonly left: PatternAwareBinding;
			readonly right: PatternAwareBinding;
	  }
) & {
	readonly variantCounts?: Readonly<Record<string, number>>;
};

export type PatternAwarePattern = {
	readonly id: string;
	readonly context: ReadonlyArray<PatternAwareEventSignature>;
	readonly targetTool: string;
	readonly bindings: Readonly<Record<string, PatternAwareBinding>>;
	readonly dependencies?: ReadonlyArray<PatternAwareDependency>;
	readonly targetSchemaHash?: string;
	readonly gapCounts: Readonly<Record<string, number>>;
	readonly gapLastSeen?: Readonly<Record<string, number>>;
	readonly occurrences: number;
	readonly replayMatches: number;
	readonly historicalOpportunities: number;
	readonly historicalMatches: number;
	readonly empiricalProbability: number;
	readonly adoptionProbability: number;
	readonly feedback: PatternAwareFeedback;
	readonly averageDurationMs: number;
	readonly lastSeenSequence: number;
};

export type PatternAwareFeedback = {
	readonly issued: number;
	readonly observed: number;
	readonly matched: number;
	readonly adopted: number;
	readonly rejectedAfterMatch: Readonly<Partial<Record<ResolutionStage, number>>>;
	readonly unobserved: Readonly<Record<string, number>>;
	readonly recentMatchedWeight: number;
	readonly recentMismatchedWeight: number;
	readonly recentAdoptedWeight: number;
	readonly recentRejectedWeight: number;
	readonly sequence: number;
};

export type PatternAwareCandidate = {
	readonly type: "tool_call";
	readonly source: "pattern_aware";
	readonly tool: string;
	readonly input: Record<string, unknown>;
	readonly patternID: string;
	/** Canonical action identity; plan support adds the parent path while K(a) remains the execution identity. */
	readonly actionIdentity: string;
	readonly supportingPatternIDs: ReadonlyArray<string>;
	readonly horizon: number;
	readonly latestHorizon: number;
	readonly empiricalProbability: number;
	readonly conditionalProbability: number;
	readonly adoptionProbability: number;
	readonly expectedDurationMs: number;
	readonly expectedLatencyBenefitMs: number;
	readonly background?: boolean;
	readonly dependencies: ReadonlyArray<PatternAwareDependency>;
	readonly continuation: PatternAwareContinuation;
	readonly depth: number;
	readonly diagnostic: string;
};

export type PatternAwareContinuation = {
	readonly history: ReadonlyArray<PatternAwareEvent>;
	readonly visitedPatternIDs: ReadonlyArray<string>;
	readonly pathProbability: number;
};

export type PatternAwareRuntimeContext = {
	readonly store: PatternAwareStore;
	readonly continuation: PatternAwareContinuation;
};

export type PatternAwareObservation = {
	readonly output?: unknown;
	readonly outputPaths?: ReadonlyArray<string>;
};

type MutablePattern = {
	id: string;
	context: PatternAwareEventSignature[];
	targetTool: string;
	bindings: Record<string, PatternAwareBinding>;
	dependencies: PatternAwareDependency[];
	targetSchemaHash?: string;
	gapCounts: Record<string, number>;
	gapLastSeen: Record<string, number>;
	occurrences: number;
	replayMatches: number;
	historicalOpportunities: number;
	historicalMatches: number;
	feedback: MutablePatternFeedback;
	averageDurationMs: number;
	lastSeenSequence: number;
};

type MutablePatternFeedback = {
	issued: number;
	observed: number;
	matched: number;
	adopted: number;
	rejectedAfterMatch: Partial<Record<ResolutionStage, number>>;
	unobserved: Record<string, number>;
	recentMatchedWeight: number;
	recentMismatchedWeight: number;
	recentAdoptedWeight: number;
	recentRejectedWeight: number;
	sequence: number;
};

type PersistedPatternSample = {
	readonly context: ReadonlyArray<number>;
	readonly target: number;
	readonly gap: number;
};

type PersistedPatternPool = Omit<PatternPool, "samples"> & {
	readonly samples: ReadonlyArray<PersistedPatternSample>;
};

type PersistedState = {
	readonly version: number;
	readonly patterns: ReadonlyArray<PatternAwarePattern>;
	readonly events?: ReadonlyArray<PatternAwareEvent>;
	readonly pools: ReadonlyArray<unknown>;
	readonly sequenceCounts: ReadonlyArray<PpmCountTrieRow>;
};

type PatternSample = {
	readonly context: ReadonlyArray<PatternAwareEvent>;
	readonly target: PatternAwareEvent;
	readonly gap: number;
};

type PatternPool = {
	readonly key: string;
	readonly context: ReadonlyArray<PatternAwareEventSignature>;
	readonly targetTool: string;
	readonly targetSchemaHash?: string;
	readonly gap: number;
	readonly samples: PatternSample[];
	patternIDs?: string[];
};

type TrieNode = {
	readonly children: Map<string, TrieNode>;
	readonly patterns: Set<string>;
};

export const PATTERN_AWARE_DEFAULTS: PatternAwareSettings = {
	enabled: true,
	multiStepEnabled: true,
	maxContextLength: 4,
	beamWidth: 4,
	maxPredictionDepth: 6,
	maxFutureGap: 2,
	futureGapCoverage: 0.25,
	decayHalfLifeEvents: 2048,
	minOccurrences: 2,
	minBindingReplayProbability: 0.75,
	maxPatterns: 4096,
};

const MAX_BINDING_VARIANTS = 32;
const MAX_PATH_SOURCES = 24;
// Bound crash-loss while amortizing full-state serialization across active tool loops.
// Terminal/dispose paths still flush immediately.
const PERSIST_CHECKPOINT_INTERVAL_MS = 30_000;
const PERSISTENCE_VERSION = 18;
const MIN_MIGRATABLE_PERSISTENCE_VERSION = 13;
const INDEXED_POOL_PERSISTENCE_VERSION = 16;
const COMPATIBLE_PATTERN_VERSION = 17;
const BATCH_CONTROL_OPPORTUNITY_VERSION = 18;

class PredictiveContextTrie {
	private readonly root: TrieNode = { children: new Map(), patterns: new Set() };

	insert(pattern: Pick<MutablePattern, "id" | "context">) {
		let node = this.root;
		for (let index = pattern.context.length - 1; index >= 0; index--) {
			const token = trieToken(pattern.context[index]!);
			const child = node.children.get(token) ?? { children: new Map(), patterns: new Set() };
			node.children.set(token, child);
			node = child;
		}
		node.patterns.add(pattern.id);
	}

	matching(history: ReadonlyArray<PatternAwareEvent>) {
		const result = new Set<string>();
		let node = this.root;
		for (let index = history.length - 1; index >= 0; index--) {
			const token = trieToken(signature(history[index]!));
			const child = node.children.get(token);
			if (!child) break;
			for (const patternID of child.patterns) result.add(patternID);
			node = child;
		}
		return result;
	}
}

export class PatternAwareStore {
	private readonly patterns = new Map<string, MutablePattern>();
	private readonly pools = new Map<string, PatternPool>();
	private readonly controlOpportunitiesByContext = new Map<string, Map<string, number>>();
	private readonly sessions: PatternSessionRegistry<PatternAwareEvent>;
	private readonly observedActionKeys = new WeakMap<PatternAwareEvent, ActionKey | null>();
	private readonly resolvedActionKeys: BoundedRecencyMap<string, ActionKey | null>;
	private readonly patternSupportSessions = new Map<string, ReadonlySet<string>>();
	private trie = new PredictiveContextTrie();
	private sequenceModel: PpmCountTrie;
	private indexDirty = true;
	private clock = 0;
	private write: Promise<void> = Promise.resolve();
	private writeError?: unknown;
	private dirty = false;
	private persistTimer?: ReturnType<typeof setTimeout>;
	private loaded = false;
	private readonly settings: PatternAwareSettings;
	private readonly persistenceFile?: string;
	private readonly actionSemantics?: PatternAwareActionSemantics;

	constructor(
		settings: PatternAwareSettings,
		persistenceFile?: string,
		actionSemantics?: PatternAwareActionSemantics,
	) {
		this.settings = settings;
		this.sessions = new PatternSessionRegistry(patternSessionBudgets(settings.maxPatterns));
		this.resolvedActionKeys = new BoundedRecencyMap(settings.maxPatterns);
		this.sequenceModel = new PpmCountTrie(settings.maxContextLength);
		this.persistenceFile = persistenceFile;
		this.actionSemantics = actionSemantics;
	}

	async load() {
		if (this.loaded) return;
		this.loaded = true;
		if (!this.persistenceFile) return;
		const parsed = await fs
			.readFile(this.persistenceFile, "utf8")
			.then((value) => JSON.parse(value) as PersistedState)
			.catch(() => undefined);
		if (
			!parsed ||
			!Number.isInteger(parsed.version) ||
			parsed.version < MIN_MIGRATABLE_PERSISTENCE_VERSION ||
			parsed.version > PERSISTENCE_VERSION ||
			!Array.isArray(parsed.patterns) ||
			!Array.isArray(parsed.pools) ||
			!Array.isArray(parsed.sequenceCounts)
		)
			return;
		if (parsed.version >= COMPATIBLE_PATTERN_VERSION) {
			for (const item of parsed.patterns) {
				const pattern = mutablePattern(item);
				if (
					!pattern ||
					pattern.context.length > this.settings.maxContextLength ||
					pattern.context.some((event) => event.tool === "$llm")
				)
					continue;
				this.patterns.set(pattern.id, pattern);
				this.clock = Math.max(this.clock, pattern.lastSeenSequence);
			}
		}
		const restoredPools =
			parsed.version >= INDEXED_POOL_PERSISTENCE_VERSION
				? mutableIndexedPools(parsed.events, parsed.pools)
				: parsed.pools.flatMap((item) => mutablePool(item) ?? []);
		for (const pool of restoredPools) {
			if (
				!pool ||
				pool.context.length > this.settings.maxContextLength ||
				pool.context.some((event) => event.tool === "$llm")
			)
				continue;
			for (const [gap, samples] of samplesByGap(pool.samples)) {
				const key = patternPoolKey(pool.context, pool.targetTool, pool.targetSchemaHash, gap);
				const compatible = parsed.version >= COMPATIBLE_PATTERN_VERSION && pool.gap === gap;
				const restored = {
					key,
					context: pool.context,
					targetTool: pool.targetTool,
					...(pool.targetSchemaHash ? { targetSchemaHash: pool.targetSchemaHash } : {}),
					gap,
					samples,
					...(compatible && pool.patternIDs?.length ? { patternIDs: pool.patternIDs } : {}),
				};
				this.pools.set(key, restored);
				this.addControlOpportunities(restored, restored.samples, 1);
				for (const patternID of restored.patternIDs ?? []) {
					if (this.patterns.get(patternID)?.dependencies.length === 0) {
						this.patternSupportSessions.set(patternID, new Set(samples.map((sample) => sample.target.sessionID)));
					}
				}
			}
			for (const sample of pool.samples) {
				this.clock = Math.max(this.clock, sample.target.sequence, ...sample.context.map((event) => event.sequence));
			}
		}
		if (parsed.version < BATCH_CONTROL_OPPORTUNITY_VERSION) this.migrateBatchControlOpportunities();
		this.sequenceModel.restore(parsed.sequenceCounts);
		this.sequenceModel.trim(this.settings.maxPatterns);
		this.indexDirty = true;
		this.trimPools();
		this.trimPatterns();
	}

	observe(input: PatternAwareEventInput) {
		this.observeEvents([input]);
	}

	observeBatch(inputs: ReadonlyArray<PatternAwareEventInput>) {
		const first = inputs[0];
		if (!first) return;
		if (inputs.some((input) => input.sessionID !== first.sessionID || input.turnID !== first.turnID)) {
			throw new Error("PatternAware batch actions must belong to one provider turn");
		}
		const ordered = inputs
			.map((input, index) => ({ input, index, key: canonicalBatchActionKey(input) }))
			.sort((left, right) => left.key.localeCompare(right.key) || left.index - right.index)
			.map((item) => item.input);
		this.observeEvents(ordered, first.turnID);
	}

	private observeEvents(inputs: ReadonlyArray<PatternAwareEventInput>, batchID?: string) {
		if (!this.settings.enabled) return;
		const first = inputs[0];
		if (!first) return;
		const events = inputs.map(
			(input, index): PatternAwareEvent => ({
				...input,
				sequence: ++this.clock,
				...(batchID ? { batchID, batchIndex: index, batchSize: inputs.length } : {}),
			}),
		);
		const { state: session, evicted } = this.sessions.ensure(first.sessionID);
		if (evicted) this.finishSessionState(evicted);
		const history = session.history;
		this.resolvePendingBatch(session, events);
		const prior = history;
		for (const event of events) {
			if (event.learnTarget !== false) {
				this.sequenceModel.observe(
					prior.map((item) => signatureToken(signature(item))),
					event.tool,
					event.sequence,
					this.settings.decayHalfLifeEvents,
				);
				this.learn(prior, event);
				this.observeRecurrentAction(session, event);
			}
		}
		history.push(...events);
		this.startPending(session, history);
		this.trimSessionHistory(history);
		this.trimPools();
		this.trimPatterns();
		this.sequenceModel.trim(this.settings.maxPatterns);
		this.persist();
	}

	observeTurn() {
		if (this.settings.enabled) this.clock++;
	}

	finishSession(sessionID: string) {
		const session = this.sessions.finish(sessionID);
		if (session) this.finishSessionState(session);
		this.persist();
	}

	ingestTrace(trace: ReadonlyArray<PatternAwareEventInput>) {
		const sessions = new Set<string>();
		for (const event of trace) this.observe(event);
		for (const event of trace) sessions.add(event.sessionID);
		for (const sessionID of sessions) this.finishSession(sessionID);
	}

	registerValidatedPattern(input: PatternAwarePattern) {
		const pattern = mutablePattern(input);
		if (!pattern || !structurallyEligible(pattern, this.settings)) return false;
		this.patterns.set(pattern.id, pattern);
		this.patternSupportSessions.delete(pattern.id);
		this.clock = Math.max(this.clock, pattern.lastSeenSequence);
		this.indexDirty = true;
		this.trimPatterns();
		this.persist();
		return true;
	}

	predict(
		sessionID: string,
		schemaHashes: Readonly<Record<string, string>> = {},
		predictionSettings: PatternAwareSettings = this.settings,
	) {
		if (!predictionSettings.enabled) return [];
		const history = this.sessions.get(sessionID)?.history ?? [];
		return this.predictHistory(
			history,
			schemaHashes,
			{
				history,
				visitedPatternIDs: [],
				pathProbability: 1,
			},
			predictionSettings,
		);
	}

	predictAfterBatch(
		sessionID: string,
		inputs: ReadonlyArray<PatternAwareEventInput>,
		schemaHashes: Readonly<Record<string, string>> = {},
		predictionSettings: PatternAwareSettings = this.settings,
	) {
		if (!predictionSettings.enabled || !inputs.length) return [];
		if (inputs.some((input) => input.sessionID !== sessionID)) {
			throw new Error("PatternAware hypothetical actions must belong to the active session");
		}
		const turnID = inputs[0]!.turnID;
		if (inputs.some((input) => input.turnID !== turnID)) {
			throw new Error("PatternAware hypothetical actions must belong to one provider turn");
		}
		const ordered = inputs
			.map((input, index) => ({ input, index, key: canonicalBatchActionKey(input) }))
			.sort((left, right) => left.key.localeCompare(right.key) || left.index - right.index)
			.map((item) => item.input);
		const history = [...(this.sessions.get(sessionID)?.history ?? [])];
		const sequence = history.at(-1)?.sequence ?? this.clock;
		history.push(
			...ordered.map(
				(input, index): PatternAwareEvent => ({
					...input,
					sequence: sequence + index + 1,
					batchID: turnID,
					batchIndex: index,
					batchSize: ordered.length,
					learnTarget: false,
				}),
			),
		);
		this.trimSessionHistory(history);
		return this.predictHistory(
			history,
			schemaHashes,
			{ history, visitedPatternIDs: [], pathProbability: 1 },
			predictionSettings,
		);
	}

	continue(
		continuation: PatternAwareContinuation,
		input: PatternAwareEventInput,
		schemaHashes: Readonly<Record<string, string>> = {},
		parentConfirmed = false,
		predictionSettings: PatternAwareSettings = this.settings,
	) {
		if (!predictionSettings.enabled) return [];
		const event: PatternAwareEvent = {
			...input,
			sequence: (continuation.history.at(-1)?.sequence ?? this.clock) + 1,
			learnTarget: false,
		};
		const history = [...continuation.history, event];
		this.trimSessionHistory(history);
		return this.predictHistory(
			history,
			schemaHashes,
			{
				history,
				visitedPatternIDs: continuation.visitedPatternIDs,
				pathProbability: parentConfirmed ? 1 : continuation.pathProbability,
			},
			predictionSettings,
		);
	}

	private predictHistory(
		history: ReadonlyArray<PatternAwareEvent>,
		schemaHashes: Readonly<Record<string, string>>,
		continuation: PatternAwareContinuation,
		settings: PatternAwareSettings,
	) {
		if (continuation.visitedPatternIDs.length >= settings.maxPredictionDepth) return [];
		const predictiveHistory = history;
		const activeSessionID = predictiveHistory.at(-1)?.sessionID;
		const sequenceContext = predictiveHistory.map((event) => signatureToken(signature(event)));
		const result: PatternAwareCandidate[] = [];
		const groups = new Map<
			string,
			Array<{
				readonly pattern: MutablePattern;
				readonly input: Record<string, unknown>;
				readonly variantProbability: number;
			}>
		>();
		this.ensureIndex();
		for (const patternID of this.trie.matching(predictiveHistory)) {
			const pattern = this.patterns.get(patternID);
			if (!pattern || continuation.visitedPatternIDs.includes(patternID) || !structurallyEligible(pattern, settings))
				continue;
			const supportingSessions = this.patternSupportSessions.get(patternID);
			if (
				activeSessionID !== undefined &&
				supportingSessions &&
				pattern.dependencies.length === 0 &&
				!supportingSessions.has(activeSessionID) &&
				supportingSessions.size < settings.minOccurrences
			)
				continue;
			if (pattern.targetSchemaHash && schemaHashes[pattern.targetTool] !== pattern.targetSchemaHash) continue;
			if (!matchesSuffix(predictiveHistory, pattern.context)) continue;
			const context = predictiveHistory.slice(-pattern.context.length);
			for (const applied of applyBindingsPartialWeightedVariants(pattern.bindings, context)) {
				if (applied.missing.length) continue;
				const action = this.resolveActionKey(
					pattern.targetTool,
					applied.input,
					pattern.targetSchemaHash ?? schemaHashes[pattern.targetTool],
				);
				const identity = action
					? JSON.stringify({ actionKey: action.key, type: "tool_call" })
					: stableStringify({
							type: "tool_call",
							tool: pattern.targetTool,
							input: applied.input,
						});
				const group = groups.get(identity) ?? [];
				group.push({
					pattern,
					input: applied.input,
					variantProbability: applied.probability,
				});
				groups.set(identity, group);
			}
		}
		const ppmEstimates = new Map<string, PpmProbabilityEstimate | undefined>();
		const estimatePpm = (tool: string) => {
			if (!ppmEstimates.has(tool))
				ppmEstimates.set(
					tool,
					this.sequenceModel.estimate(sequenceContext, tool, this.clock, settings.decayHalfLifeEvents),
				);
			return ppmEstimates.get(tool);
		};
		const predictions = [...groups.entries()].map(([identity, group]) => {
			const ordered = [...group].sort(
				(left, right) =>
					right.pattern.context.length - left.pattern.context.length ||
					right.pattern.occurrences - left.pattern.occurrences,
			);
			const representative = ordered[0]!;
			const patterns = ordered.map((item) => item.pattern);
			const horizon = learnedGroupHorizon(patterns, settings, this.clock);
			const latestHorizon = Math.max(horizon, learnedGroupHorizon(patterns, settings, this.clock, 1));
			const gapCoverage = groupGapCoverage(patterns, horizon, settings, this.clock);
			const replayProbability = backoffProbability(patterns, this.clock, settings.decayHalfLifeEvents);
			const targetTool = representative.pattern.targetTool;
			const ppmEstimate = estimatePpm(targetTool);
			const totalWeight = ordered.reduce(
				(total, item) =>
					total +
					Math.max(1, item.pattern.occurrences) *
						recencyWeight(item.pattern.lastSeenSequence, this.clock, settings.decayHalfLifeEvents),
				0,
			);
			const variantProbability =
				ordered.reduce(
					(total, item) =>
						total +
						item.variantProbability *
							Math.max(1, item.pattern.occurrences) *
							recencyWeight(item.pattern.lastSeenSequence, this.clock, settings.decayHalfLifeEvents),
					0,
				) / Math.max(1, totalWeight);
			const expectedDurationMs =
				ordered.reduce(
					(total, item) =>
						total +
						Math.max(0, item.pattern.averageDurationMs) *
							Math.max(1, item.pattern.occurrences) *
							recencyWeight(item.pattern.lastSeenSequence, this.clock, settings.decayHalfLifeEvents),
					0,
				) / Math.max(1, totalWeight);
			const adoptionProbability = patternAdoptionProbability(patterns, this.clock, settings.decayHalfLifeEvents);
			const conditionalProbability = clampProbability(replayProbability * variantProbability);
			const empiricalProbability = clampProbability(continuation.pathProbability * conditionalProbability);
			const mapperComplexity = Math.min(...ordered.map((item) => bindingMapComplexity(item.pattern.bindings)));
			const mapperConfidence = totalWeight / (totalWeight + mapperComplexity);
			const expectedLatencyBenefitMs =
				empiricalProbability *
				adoptionProbability *
				mapperConfidence *
				Math.max(1, Math.max(0, expectedDurationMs));
			const background = patterns.every((pattern) => {
				const feedback = feedbackEvidence(pattern, this.clock, settings.decayHalfLifeEvents);
				return pattern.occurrences < settings.minOccurrences || feedback.mismatched > feedback.matched;
			});
			return {
				background,
				actionIdentity: hash(identity),
				type: "tool_call" as const,
				tool: representative.pattern.targetTool,
				input: representative.input,
				patternID: representative.pattern.id,
				supportingPatternIDs: [...new Set(ordered.map((item) => item.pattern.id))],
				context: representative.pattern.context,
				dependencies: representative.pattern.dependencies,
				horizon,
				latestHorizon,
				gapCoverage,
				replayProbability,
				variantProbability,
				conditionalProbability,
				empiricalProbability,
				adoptionProbability,
				expectedDurationMs,
				ppmEstimate,
				mapperConfidence,
				expectedLatencyBenefitMs,
			};
		});
		for (const recurrent of this.recurrentPredictions(
			activeSessionID,
			schemaHashes,
			estimatePpm,
			continuation,
			settings,
		)) {
			const index = predictions.findIndex((prediction) => prediction.actionIdentity === recurrent.actionIdentity);
			if (index < 0) {
				predictions.push(recurrent);
				continue;
			}
			const existing = predictions[index]!;
			const preferred =
				recurrent.background !== existing.background
					? recurrent.background
						? existing
						: recurrent
					: recurrent.expectedLatencyBenefitMs > existing.expectedLatencyBenefitMs
						? recurrent
						: existing;
			predictions[index] = {
				...preferred,
				background: existing.background && recurrent.background,
				supportingPatternIDs: [...new Set([...existing.supportingPatternIDs, ...recurrent.supportingPatternIDs])],
			};
		}
		const comparePredictions = (left: (typeof predictions)[number], right: (typeof predictions)[number]) =>
			Number(left.background) - Number(right.background) ||
			right.expectedLatencyBenefitMs - left.expectedLatencyBenefitMs ||
			right.empiricalProbability - left.empiricalProbability ||
			right.conditionalProbability - left.conditionalProbability ||
			left.horizon - right.horizon ||
			left.patternID.localeCompare(right.patternID) ||
			left.actionIdentity.localeCompare(right.actionIdentity);
		const selected = perToolBeam(
			predictions.sort(comparePredictions),
			settings.beamWidth,
			(prediction) => prediction.tool,
		);
		const emittedPerTool = new Map<string, number>();
		for (const prediction of selected) {
			const beamRank = (emittedPerTool.get(prediction.tool) ?? 0) + 1;
			emittedPerTool.set(prediction.tool, beamRank);
			const nextContinuation: PatternAwareContinuation = {
				history: predictiveHistory,
				visitedPatternIDs: [...continuation.visitedPatternIDs, prediction.patternID],
				pathProbability: prediction.empiricalProbability,
			};
			result.push({
				type: "tool_call",
				source: "pattern_aware",
				tool: prediction.tool,
				input: prediction.input,
				patternID: prediction.patternID,
				actionIdentity: prediction.actionIdentity,
				supportingPatternIDs: prediction.supportingPatternIDs,
				horizon: prediction.horizon,
				latestHorizon: prediction.latestHorizon,
				empiricalProbability: prediction.empiricalProbability,
				conditionalProbability: prediction.conditionalProbability,
				adoptionProbability: prediction.adoptionProbability,
				expectedDurationMs: prediction.expectedDurationMs,
				expectedLatencyBenefitMs: prediction.expectedLatencyBenefitMs,
				...(prediction.background ? { background: true } : {}),
				dependencies: prediction.dependencies,
				continuation: nextContinuation,
				depth: nextContinuation.visitedPatternIDs.length,
				diagnostic: JSON.stringify(
					{
						source: "pattern_aware",
						patternID: prediction.patternID,
						supportingPatterns: prediction.supportingPatternIDs,
						context: prediction.context,
						tool: prediction.tool,
						input: prediction.input,
						empiricalProbability: prediction.empiricalProbability,
						conditionalProbability: prediction.conditionalProbability,
						adoptionProbability: prediction.adoptionProbability,
						replayProbability: prediction.replayProbability,
						horizon: prediction.horizon,
						latestHorizon: prediction.latestHorizon,
						ppmProbability: prediction.ppmEstimate?.probability,
						ppmOrder: prediction.ppmEstimate?.order,
						ppmEvidence: prediction.ppmEstimate?.evidence,
						ppmEscapeMass: prediction.ppmEstimate?.escapeMass,
						mapperConfidence: prediction.mapperConfidence,
						variantProbability: prediction.variantProbability,
						expectedLatencyBenefitMs: prediction.expectedLatencyBenefitMs,
						background: prediction.background === true,
						beamRank,
						beamWidth: settings.beamWidth,
						gapCoverage: prediction.gapCoverage,
						expectedDurationMs: prediction.expectedDurationMs,
						dependencies: prediction.dependencies,
						depth: nextContinuation.visitedPatternIDs.length,
					},
					null,
					2,
				),
			});
		}
		return result;
	}

	private recurrentPredictions(
		sessionID: string | undefined,
		schemaHashes: Readonly<Record<string, string>>,
		estimatePpm: (tool: string) => PpmProbabilityEstimate | undefined,
		continuation: PatternAwareContinuation,
		settings: PatternAwareSettings,
	) {
		const actions = sessionID ? this.sessions.get(sessionID)?.recurrentActions : undefined;
		if (!actions) return [];
		const values = [...actions].filter((item) => {
			const current = schemaHashes[item.action.tool];
			return current === undefined || current === item.action.schemaHash;
		});
		const massByTool = new Map<string, number>();
		for (const item of values) {
			const mass = item.count * recencyWeight(item.lastSeenSequence, this.clock, settings.decayHalfLifeEvents);
			massByTool.set(item.action.tool, (massByTool.get(item.action.tool) ?? 0) + mass);
		}
		const rank = (item: PatternRecurrentAction) =>
			recencyWeight(item.lastSeenSequence, this.clock, settings.decayHalfLifeEvents) *
			Math.max(item.count, item.totalDurationMs);
		const provenTools = new Set(
			values.filter((item) => item.count >= settings.minOccurrences).map((item) => item.action.tool),
		);
		const candidates = perToolBeam(
			values
				.filter((item) => item.count >= settings.minOccurrences || provenTools.has(item.action.tool))
				.filter((item) => !continuation.visitedPatternIDs.includes(`action-backoff:${hash(item.action.key)}`))
				.sort(
					(left, right) =>
						Number(right.count >= settings.minOccurrences) - Number(left.count >= settings.minOccurrences) ||
						rank(right) - rank(left) ||
						left.action.key.localeCompare(right.action.key),
				),
			settings.beamWidth,
			(item) => item.action.tool,
		);
		return candidates.map((item) => {
			const patternID = `action-backoff:${hash(item.action.key)}`;
			const mass = item.count * recencyWeight(item.lastSeenSequence, this.clock, settings.decayHalfLifeEvents);
			const conditionalProbability = clampProbability(mass / Math.max(mass, massByTool.get(item.action.tool) ?? 0));
			const empiricalProbability = clampProbability(continuation.pathProbability * conditionalProbability);
			const expectedDurationMs = item.totalDurationMs / Math.max(1, item.count);
			const ppmEstimate = estimatePpm(item.action.tool);
			const expectedLatencyBenefitMs =
				empiricalProbability * (ppmEstimate?.probability ?? 1) * Math.max(1, expectedDurationMs);
			return {
				background: item.count < settings.minOccurrences,
				actionIdentity: hash(JSON.stringify({ actionKey: item.action.key, type: "tool_call" })),
				type: "tool_call" as const,
				tool: item.action.tool,
				input: structuredClone(item.input),
				patternID,
				supportingPatternIDs: [] as string[],
				context: [] as PatternAwareEventSignature[],
				dependencies: [] as PatternAwareDependency[],
				horizon: 0,
				latestHorizon: 0,
				gapCoverage: 1,
				replayProbability: conditionalProbability,
				variantProbability: 1,
				conditionalProbability,
				empiricalProbability,
				adoptionProbability: 1,
				expectedDurationMs,
				ppmEstimate,
				mapperConfidence: 1,
				expectedLatencyBenefitMs,
			};
		});
	}

	issued(patternID: string) {
		const pattern = this.patterns.get(patternID);
		if (!pattern) return;
		pattern.feedback.issued++;
		this.persist();
	}

	settled(patternID: string, settlement: PredictionSettlement) {
		const pattern = this.patterns.get(patternID);
		if (!pattern) return;
		const recent = feedbackEvidence(pattern, this.clock, this.settings.decayHalfLifeEvents);
		pattern.feedback.recentMatchedWeight = recent.matched;
		pattern.feedback.recentMismatchedWeight = recent.mismatched;
		pattern.feedback.recentAdoptedWeight = recent.adopted;
		pattern.feedback.recentRejectedWeight = recent.rejected;
		pattern.feedback.sequence = this.clock;
		if (settlement.observation === "unobserved") {
			const key = `${settlement.cause.stage}:${settlement.cause.code}`;
			pattern.feedback.unobserved[key] = (pattern.feedback.unobserved[key] ?? 0) + 1;
		} else {
			pattern.feedback.observed++;
			if (!settlement.match.matched) {
				pattern.feedback.recentMismatchedWeight++;
			} else {
				pattern.feedback.matched++;
				pattern.feedback.recentMatchedWeight++;
				if (settlement.match.adoption.status === "adopted") {
					pattern.feedback.adopted++;
					pattern.feedback.recentAdoptedWeight++;
				} else {
					const stage = settlement.match.adoption.cause.stage;
					pattern.feedback.rejectedAfterMatch[stage] = (pattern.feedback.rejectedAfterMatch[stage] ?? 0) + 1;
					pattern.feedback.recentRejectedWeight++;
				}
			}
		}
		this.persist();
	}

	snapshot(): ReadonlyArray<PatternAwarePattern> {
		return [...this.patterns.values()].map((pattern) =>
			readonlyPattern(pattern, this.clock, this.settings.decayHalfLifeEvents),
		);
	}

	recent(sessionID: string): ReadonlyArray<PatternAwareEvent> {
		return [...(this.sessions.get(sessionID)?.history ?? [])];
	}

	async flush(): Promise<void> {
		while (true) {
			if (this.persistTimer) {
				clearTimeout(this.persistTimer);
				this.persistTimer = undefined;
			}
			this.enqueuePersist();
			await this.write;
			if (this.writeError) {
				const error = this.writeError;
				this.writeError = undefined;
				throw error;
			}
			if (!this.dirty) return;
		}
	}

	private ensureIndex() {
		if (!this.indexDirty) return;
		this.trie = new PredictiveContextTrie();
		for (const pattern of this.patterns.values()) this.trie.insert(pattern);
		this.indexDirty = false;
	}

	private observeRecurrentAction(session: PatternSessionState<PatternAwareEvent>, event: PatternAwareEvent) {
		const action = this.resolveActionKey(event.tool, event.input, event.schemaHash);
		if (!action) return;
		const existing = session.recurrentAction(action.key);
		const durationMs =
			event.outcome === "success" && Number.isFinite(event.durationMs) ? Math.max(0, event.durationMs) : 0;
		if (existing) {
			existing.count = Math.min(Number.MAX_SAFE_INTEGER, existing.count + 1);
			existing.totalDurationMs = Math.min(Number.MAX_VALUE / 2, existing.totalDurationMs + durationMs);
			existing.lastSeenSequence = event.sequence;
		} else {
			session.rememberRecurrentAction(action.key, {
				action,
				input: structuredClone(event.input),
				count: 1,
				totalDurationMs: durationMs,
				lastSeenSequence: event.sequence,
			});
		}
	}

	private learn(history: ReadonlyArray<PatternAwareEvent>, target: PatternAwareEvent) {
		const batches = actionBatches(history);
		const maxGap = Math.min(this.settings.maxFutureGap, Math.max(0, batches.length - 1));
		for (let gap = 0; gap <= maxGap; gap++) {
			const contextEnd = batches.length - gap;
			const maxLength = Math.min(this.settings.maxContextLength, contextEnd);
			for (let length = 1; length <= maxLength; length++) {
				const context = batches.slice(contextEnd - length, contextEnd).flat();
				if (context.length > this.settings.maxContextLength) break;
				this.learnOccurrence(context, target, gap);
			}
		}
	}

	private actionInputCovers(
		speculativeTool: string,
		speculativeInput: Readonly<Record<string, unknown>>,
		speculativeSchemaHash: string | undefined,
		actorTool: string,
		actorInput: Readonly<Record<string, unknown>>,
		actorSchemaHash: string | undefined,
	) {
		if (speculativeTool !== actorTool) return false;
		const speculative = this.resolveActionKey(speculativeTool, speculativeInput, speculativeSchemaHash);
		const actor = this.resolveActionKey(actorTool, actorInput, actorSchemaHash);
		if (!speculative || !actor) return sameValue(speculativeInput, actorInput);
		return actionKeyCovers(speculative, actor, this.actionSemantics?.projectors ?? []);
	}

	private resolveActionKey(tool: string, input: Readonly<Record<string, unknown>>, schemaHash: string | undefined) {
		if (!this.actionSemantics) return undefined;
		let cacheKey: string;
		try {
			cacheKey = hash(stableStringify([tool, schemaHash, input]));
		} catch {
			return undefined;
		}
		const cached = this.resolvedActionKeys.get(cacheKey);
		if (cached !== undefined) return cached ?? undefined;
		let resolved: ActionKey | undefined;
		try {
			resolved = this.actionSemantics.actionKey(tool, input, schemaHash);
		} catch {
			return undefined;
		}
		this.resolvedActionKeys.set(cacheKey, resolved ?? null);
		return resolved;
	}

	private bindingsCoverSample(
		bindings: Readonly<Record<string, PatternAwareBinding>>,
		targetTool: string,
		targetSchemaHash: string | undefined,
		sample: PatternSample,
	) {
		if (targetTool !== sample.target.tool) return false;
		let actor = this.observedActionKeys.get(sample.target);
		if (actor === undefined && !this.observedActionKeys.has(sample.target)) {
			actor =
				this.resolveActionKey(
					sample.target.tool,
					sample.target.input,
					sample.target.schemaHash ?? targetSchemaHash,
				) ?? null;
			this.observedActionKeys.set(sample.target, actor);
		}
		return applyBindingsVariants(bindings, sample.context).some((input) => {
			const speculative = this.resolveActionKey(targetTool, input, targetSchemaHash);
			if (!speculative || !actor) return sameValue(input, sample.target.input);
			return actionKeyCovers(speculative, actor, this.actionSemantics?.projectors ?? []);
		});
	}

	private minimizeProjectedBindings(bindings: Readonly<Record<string, PatternAwareBinding>>, pool: PatternPool) {
		const supportingSamples = (candidate: Readonly<Record<string, PatternAwareBinding>>) =>
			pool.samples.filter((sample) =>
				this.bindingsCoverSample(candidate, pool.targetTool, pool.targetSchemaHash, sample),
			);
		if (!this.actionSemantics) return { bindings, support: supportingSamples(bindings) };
		const minimized = { ...bindings };
		let support = supportingSamples(minimized);
		for (const key of Object.keys(bindings)) {
			const binding = minimized[key];
			if (!binding) continue;
			delete minimized[key];
			const next = supportingSamples(minimized);
			if (next.length > support.length || (support.length > 0 && next.length === support.length)) {
				support = next;
				continue;
			}
			minimized[key] = binding;
		}
		return { bindings: minimized, support };
	}

	private learnOccurrence(context: ReadonlyArray<PatternAwareEvent>, target: PatternAwareEvent, gap: number) {
		const signatures = context.map(signature);
		const poolKey = patternPoolKey(signatures, target.tool, target.schemaHash, gap);
		const pool = this.pools.get(poolKey) ?? {
			key: poolKey,
			context: signatures,
			targetTool: target.tool,
			...(target.schemaHash ? { targetSchemaHash: target.schemaHash } : {}),
			gap,
			samples: [],
		};
		// Internal contexts are immutable; preserving identity lets binding inference reuse its WeakMap cache.
		const sample = { context, target, gap };
		pool.samples.push(sample);
		const sampleLimit = patternPoolSampleLimit(this.settings);
		const removed =
			pool.samples.length > sampleLimit ? pool.samples.splice(0, pool.samples.length - sampleLimit) : [];
		this.pools.set(poolKey, pool);
		this.addControlOpportunities(pool, removed, -1);
		this.addControlOpportunities(pool, [sample], 1);
		const firstRecurrenceProbe = gap === 0 && context.length === 1 && pool.samples.length === 1;
		const probationary =
			firstRecurrenceProbe ||
			(pool.patternIDs ?? []).some(
				(patternID) =>
					(this.patterns.get(patternID)?.occurrences ?? this.settings.minOccurrences) <
					this.settings.minOccurrences,
			);
		if (pool.samples.length < this.settings.minOccurrences && !probationary) {
			this.retirePoolPatterns(pool, new Set());
			return;
		}
		const minimumSupport = probationary ? pool.samples.length : this.settings.minOccurrences;
		const candidates = new Map<string, Record<string, PatternAwareBinding>>();
		const remember = (bindings: Record<string, PatternAwareBinding> | undefined) => {
			if (!bindings) return;
			candidates.set(stableStringify(bindingMapStructure(bindings)), bindings);
		};
		const hasBindingEvidence = (bindings: Readonly<Record<string, PatternAwareBinding>>) =>
			hasSufficientBindingProvenance(bindings, pool.samples, bindingEvidenceThreshold(this.settings));
		for (const patternID of pool.patternIDs ?? []) {
			const bindings = this.patterns.get(patternID)?.bindings;
			if (bindings && hasBindingEvidence(bindings)) remember(bindings);
		}
		const currentBindings = inferBindings(context, target.input);
		if (firstRecurrenceProbe || hasBindingEvidence(currentBindings)) {
			remember(currentBindings);
		}
		remember(
			inferBindingsFromSamples(
				pool.samples,
				bindingEvidenceThreshold(this.settings),
				this.actionSemantics !== undefined,
			),
		);

		const retained = new Set<string>();
		for (const candidate of candidates.values()) {
			const { bindings, support } = this.minimizeProjectedBindings(candidate, pool);
			if (support.length < minimumSupport) continue;
			const id = hash(
				stableStringify({
					context: signatures,
					targetTool: target.tool,
					bindings: bindingMapStructure(bindings),
					targetSchemaHash: target.schemaHash,
					gap,
				}),
			);
			if (retained.has(id)) continue;
			retained.add(id);
			const dependencies = bindingDependencies(bindings);
			if (dependencies.length === 0) {
				this.patternSupportSessions.set(id, new Set(support.map((sample) => sample.target.sessionID)));
			} else this.patternSupportSessions.delete(id);
			const historicalOpportunities = this.controlOpportunities(pool);
			const lastSeenSequence = Math.max(...support.map((sample) => sample.target.sequence));
			const existing = this.patterns.get(id);
			if (existing) {
				existing.bindings = bindings;
				existing.dependencies = dependencies;
				existing.occurrences = support.length;
				existing.replayMatches = support.length;
				existing.gapCounts = sampleGapCounts(support);
				existing.gapLastSeen = sampleGapLastSeen(support);
				existing.averageDurationMs = averageTargetDuration(support);
				existing.lastSeenSequence = lastSeenSequence;
				continue;
			}
			this.patterns.set(id, {
				id,
				context: signatures,
				targetTool: target.tool,
				bindings,
				dependencies,
				...(target.schemaHash ? { targetSchemaHash: target.schemaHash } : {}),
				gapCounts: sampleGapCounts(support),
				gapLastSeen: sampleGapLastSeen(support),
				occurrences: support.length,
				replayMatches: support.length,
				historicalOpportunities,
				historicalMatches: controlOpportunityCount(support),
				feedback: emptyPatternFeedback(lastSeenSequence),
				averageDurationMs: averageTargetDuration(support),
				lastSeenSequence,
			});
			this.indexDirty = true;
		}
		this.retirePoolPatterns(pool, retained);
	}

	private controlOpportunities(pool: PatternPool) {
		return Math.max(1, this.controlOpportunitiesByContext.get(patternControlKey(pool.context, pool.gap))?.size ?? 0);
	}

	private addControlOpportunities(pool: PatternPool, samples: ReadonlyArray<PatternSample>, delta: 1 | -1) {
		if (!samples.length) return;
		const key = patternControlKey(pool.context, pool.gap);
		const references = this.controlOpportunitiesByContext.get(key) ?? new Map<string, number>();
		for (const sample of samples) {
			const opportunity = controlOpportunityID(sample.target);
			const count = (references.get(opportunity) ?? 0) + delta;
			if (count > 0) references.set(opportunity, count);
			else references.delete(opportunity);
		}
		if (references.size) this.controlOpportunitiesByContext.set(key, references);
		else this.controlOpportunitiesByContext.delete(key);
	}

	private migrateBatchControlOpportunities() {
		const oldCounts = new Map<string, number>();
		for (const pool of this.pools.values()) {
			const key = patternControlKey(pool.context, pool.gap);
			oldCounts.set(key, (oldCounts.get(key) ?? 0) + pool.samples.length);
		}
		for (const pool of this.pools.values()) {
			const key = patternControlKey(pool.context, pool.gap);
			const duplicateOpportunities = Math.max(0, (oldCounts.get(key) ?? 0) - this.controlOpportunities(pool));
			for (const patternID of pool.patternIDs ?? []) {
				const pattern = this.patterns.get(patternID);
				if (!pattern) continue;
				const support = pool.samples.filter((sample) =>
					this.bindingsCoverSample(pattern.bindings, pattern.targetTool, pattern.targetSchemaHash, sample),
				);
				const duplicateMatches = support.length - controlOpportunityCount(support);
				pattern.historicalMatches = Math.max(0, pattern.historicalMatches - duplicateMatches);
				pattern.historicalOpportunities = Math.max(
					pattern.historicalMatches,
					pattern.historicalOpportunities - duplicateOpportunities,
				);
			}
		}
	}

	private retirePoolPatterns(pool: PatternPool, retained: ReadonlySet<string>) {
		for (const patternID of pool.patternIDs ?? []) {
			if (retained.has(patternID)) continue;
			if (this.patterns.delete(patternID)) this.indexDirty = true;
			this.patternSupportSessions.delete(patternID);
			this.sessions.removePattern(patternID);
		}
		pool.patternIDs = [...retained];
	}

	private resolvePendingBatch(
		session: PatternSessionState<PatternAwareEvent>,
		events: ReadonlyArray<PatternAwareEvent>,
	) {
		const pending = session.pending;
		if (!pending?.length) return;
		const remaining: PatternPendingValidation[] = [];
		for (const item of pending) {
			const pattern = this.patterns.get(item.patternID);
			if (!pattern) continue;
			const matched = events.some((event) =>
				item.expectedInputs.some((expectedInput) =>
					this.actionInputCovers(
						pattern.targetTool,
						expectedInput,
						pattern.targetSchemaHash,
						event.tool,
						event.input,
						event.schemaHash,
					),
				),
			);
			if (matched) {
				this.recordValidation(item.patternID, true);
				continue;
			}
			if (item.remaining <= 0) {
				this.recordValidation(item.patternID, false);
				continue;
			}
			item.remaining--;
			remaining.push(item);
		}
		session.replacePending(remaining);
	}

	private startPending(
		session: PatternSessionState<PatternAwareEvent>,
		history: ReadonlyArray<PatternAwareEvent>,
	) {
		const pending = [...session.pending];
		const triggerSequence = history.at(-1)?.sequence;
		if (triggerSequence === undefined) return;
		this.ensureIndex();
		for (const patternID of this.trie.matching(history)) {
			const pattern = this.patterns.get(patternID);
			if (!pattern) continue;
			if (!structurallyEligible(pattern, this.settings) || !matchesSuffix(history, pattern.context)) continue;
			if (pending.some((item) => item.patternID === pattern.id && item.triggerSequence === triggerSequence))
				continue;
			const context = history.slice(-pattern.context.length);
			pending.push({
				patternID: pattern.id,
				triggerSequence,
				expectedInputs: applyBindingsVariants(pattern.bindings, context),
				remaining: learnedGroupHorizon([pattern], this.settings, this.clock, 1),
			});
		}
		session.replacePending(pending);
	}

	private finishSessionState(session: PatternSessionState<PatternAwareEvent>) {
		for (const item of session.pending) this.recordValidation(item.patternID, false);
		session.replacePending([]);
	}

	private recordValidation(patternID: string, matched: boolean) {
		const pattern = this.patterns.get(patternID);
		if (!pattern) return;
		pattern.historicalOpportunities++;
		if (matched) pattern.historicalMatches++;
	}

	private trimSessionHistory(history: PatternAwareEvent[]) {
		const limit = this.settings.maxContextLength + this.settings.maxFutureGap + 1;
		const batches = indexedActionBatches(history);
		if (batches.length <= limit) return;
		const keep = new Set(batches.slice(-limit).flat());
		history.splice(0, history.length, ...history.filter((_, index) => keep.has(index)));
	}

	private trimPools() {
		const limit = Math.max(1, Math.floor(this.settings.maxPatterns * 2));
		if (this.pools.size <= limit) return;
		const evicted = [...this.pools.values()]
			.sort((left, right) => left.samples.length - right.samples.length)
			.slice(0, this.pools.size - limit);
		for (const pool of evicted) {
			this.pools.delete(pool.key);
			this.addControlOpportunities(pool, pool.samples, -1);
		}
	}

	private trimPatterns() {
		const limit = Math.max(1, Math.floor(this.settings.maxPatterns));
		if (this.patterns.size <= limit) return;
		const evicted = [...this.patterns.values()]
			.sort(
				(left, right) =>
					patternRank(left, this.clock, this.settings.decayHalfLifeEvents) -
						patternRank(right, this.clock, this.settings.decayHalfLifeEvents) ||
					left.lastSeenSequence - right.lastSeenSequence,
			)
			.slice(0, this.patterns.size - limit);
		for (const pattern of evicted) {
			this.patterns.delete(pattern.id);
			this.patternSupportSessions.delete(pattern.id);
			this.sessions.removePattern(pattern.id);
		}
		if (evicted.length) this.indexDirty = true;
	}

	private persist() {
		if (!this.persistenceFile || !this.loaded) return;
		this.dirty = true;
		if (this.persistTimer) return;
		this.persistTimer = setTimeout(() => {
			this.persistTimer = undefined;
			this.enqueuePersist();
		}, PERSIST_CHECKPOINT_INTERVAL_MS);
		this.persistTimer.unref?.();
	}

	private enqueuePersist() {
		if (!this.persistenceFile || !this.loaded || !this.dirty) return;
		this.dirty = false;
		const learning = this.persistedLearningState();
		const state: PersistedState = {
			version: PERSISTENCE_VERSION,
			patterns: this.snapshot(),
			events: learning.events,
			pools: learning.pools,
			sequenceCounts: this.sequenceModel.snapshot(this.settings.maxPatterns),
		};
		const target = this.persistenceFile;
		this.write = this.write
			.catch(() => undefined)
			.then(async () => {
				await fs.mkdir(path.dirname(target), { recursive: true });
				const temporary = `${target}.${process.pid}.tmp`;
				await fs.writeFile(temporary, `${JSON.stringify(state)}\n`, "utf8");
				await fs.rename(temporary, target).catch(async () => {
					await fs.rm(target, { force: true });
					await fs.rename(temporary, target);
				});
				this.writeError = undefined;
			})
			.catch((error) => {
				this.writeError = error;
				this.dirty = true;
			});
	}

	private persistedLearningState(): {
		readonly events: ReadonlyArray<PatternAwareEvent>;
		readonly pools: ReadonlyArray<PersistedPatternPool>;
	} {
		const sampleLimit = patternPoolSampleLimit(this.settings);
		const events: PatternAwareEvent[] = [];
		const eventIDs = new Map<string, number>();
		const reference = (event: PatternAwareEvent) => {
			const key = persistedEventIdentity(event);
			const existing = eventIDs.get(key);
			if (existing !== undefined) return existing;
			const id = events.length;
			events.push(event);
			eventIDs.set(key, id);
			return id;
		};
		const pools = [...this.pools.values()]
			.sort(
				(left, right) =>
					(right.samples.at(-1)?.target.sequence ?? 0) - (left.samples.at(-1)?.target.sequence ?? 0) ||
					right.samples.length - left.samples.length,
			)
			.slice(0, this.settings.maxPatterns)
			.map(
				(pool): PersistedPatternPool => ({
					...pool,
					samples: pool.samples.slice(-sampleLimit).map((sample) => ({
						context: sample.context.map(reference),
						target: reference(sample.target),
						gap: sample.gap,
					})),
				}),
			);
		return { events, pools };
	}
}

type PooledPatternAwareStore = {
	readonly store: Promise<PatternAwareStore>;
	references: number;
};

export type PatternAwareStoreLease = {
	readonly store: PatternAwareStore;
	readonly release: () => Promise<void>;
};

const stores = new Map<string, PooledPatternAwareStore>();

export async function acquirePatternAwareStore(
	workspace: string,
	settings: PatternAwareSettings,
	stateDirectory?: string,
	actionSemantics?: PatternAwareActionSemantics,
): Promise<PatternAwareStoreLease> {
	const analyzerKey = patternAwareAnalyzerKey(settings);
	const semanticsKey = patternSemanticsKey(actionSemantics);
	const file = configuredPersistenceFile(
		patternAwarePersistenceFile(workspace, stateDirectory),
		analyzerKey,
		semanticsKey,
	);
	const poolKey = `${file}\0${analyzerKey}\0${semanticsKey}`;
	let pooled = stores.get(poolKey);
	if (!pooled) {
		const store = Promise.resolve(new PatternAwareStore(settings, file, actionSemantics)).then(async (value) => {
			await value.load();
			return value;
		});
		pooled = { store, references: 0 };
		stores.set(poolKey, pooled);
	}
	pooled.references++;
	let store: PatternAwareStore;
	try {
		store = await pooled.store;
	} catch (error) {
		pooled.references--;
		if (pooled.references === 0 && stores.get(poolKey) === pooled) stores.delete(poolKey);
		throw error;
	}
	let released = false;
	return {
		store,
		release: async () => {
			if (released) return;
			released = true;
			pooled.references = Math.max(0, pooled.references - 1);
			try {
				await store.flush();
			} finally {
				if (pooled.references === 0 && stores.get(poolKey) === pooled) stores.delete(poolKey);
			}
		},
	};
}

export function patternAwareAnalyzerKey(settings: PatternAwareSettings): string {
	return stableStringify({
		maxContextLength: settings.maxContextLength,
		maxFutureGap: settings.maxFutureGap,
		decayHalfLifeEvents: settings.decayHalfLifeEvents,
		minOccurrences: settings.minOccurrences,
		minBindingReplayProbability: settings.minBindingReplayProbability,
		maxPatterns: settings.maxPatterns,
	});
}

function patternSemanticsKey(semantics: PatternAwareActionSemantics | undefined): string {
	if (!semantics) return "default";
	return (
		semantics.namespace ??
		hash(
			stableStringify({
				actionKey: semantics.actionKey.toString(),
				projectors: (semantics.projectors ?? []).map((projector) => projector.id).sort(),
			}),
		)
	);
}

function configuredPersistenceFile(file: string, analyzerKey: string, semanticsKey: string): string {
	if (analyzerKey === patternAwareAnalyzerKey(PATTERN_AWARE_DEFAULTS) && semanticsKey === "pi-action-semantics-v1") {
		return file;
	}
	const parsed = path.parse(file);
	return path.join(parsed.dir, `${parsed.name}.${hash(`${semanticsKey}\0${analyzerKey}`).slice(0, 12)}${parsed.ext}`);
}

export function patternAwareSettings(value: unknown): PatternAwareSettings {
	const record = asRecord(value);
	return {
		enabled: typeof record?.enabled === "boolean" ? record.enabled : PATTERN_AWARE_DEFAULTS.enabled,
		multiStepEnabled:
			typeof record?.multiStepEnabled === "boolean"
				? record.multiStepEnabled
				: PATTERN_AWARE_DEFAULTS.multiStepEnabled,
		maxContextLength: positiveInteger(record?.maxContextLength, PATTERN_AWARE_DEFAULTS.maxContextLength),
		beamWidth: positiveInteger(record?.beamWidth, PATTERN_AWARE_DEFAULTS.beamWidth),
		maxPredictionDepth: positiveInteger(record?.maxPredictionDepth, PATTERN_AWARE_DEFAULTS.maxPredictionDepth),
		maxFutureGap: nonNegativeInteger(record?.maxFutureGap, PATTERN_AWARE_DEFAULTS.maxFutureGap),
		futureGapCoverage: probabilitySetting(record?.futureGapCoverage, PATTERN_AWARE_DEFAULTS.futureGapCoverage),
		decayHalfLifeEvents: positiveInteger(record?.decayHalfLifeEvents, PATTERN_AWARE_DEFAULTS.decayHalfLifeEvents),
		minOccurrences: positiveInteger(record?.minOccurrences, PATTERN_AWARE_DEFAULTS.minOccurrences),
		minBindingReplayProbability: probabilitySetting(
			record?.minBindingReplayProbability,
			PATTERN_AWARE_DEFAULTS.minBindingReplayProbability,
		),
		maxPatterns: positiveInteger(record?.maxPatterns, PATTERN_AWARE_DEFAULTS.maxPatterns),
	};
}

export function patternAwarePersistenceFile(workspace: string, stateDirectory?: string) {
	const root = stateDirectory
		? path.resolve(stateDirectory)
		: process.env.PI_STATE_DIR
			? path.resolve(process.env.PI_STATE_DIR)
			: process.platform === "win32"
				? path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"), "pi")
				: path.join(process.env.XDG_STATE_HOME || path.join(os.homedir(), ".local", "state"), "pi");
	return path.join(root, "pattern-aware", `${hash(path.resolve(workspace))}.json`);
}

export function patternAwareRuntimeContext(
	store: PatternAwareStore,
	candidate: Pick<PatternAwareCandidate, "continuation">,
): PatternAwareRuntimeContext {
	return { store, continuation: candidate.continuation };
}

export function asPatternAwareRuntimeContext(value: unknown): PatternAwareRuntimeContext | undefined {
	const record = asRecord(value);
	if (!(record?.store instanceof PatternAwareStore)) return;
	const continuation = asRecord(record.continuation);
	if (!continuation || !Array.isArray(continuation.history) || !Array.isArray(continuation.visitedPatternIDs)) return;
	if (
		typeof continuation.pathProbability !== "number" ||
		!Number.isFinite(continuation.pathProbability) ||
		continuation.pathProbability < 0 ||
		continuation.pathProbability > 1
	)
		return;
	return value as PatternAwareRuntimeContext;
}

export function projectPatternAwareObservation(
	output: unknown,
	outputPaths: ReadonlyArray<string> = [],
	resourceRoot?: string,
): PatternAwareObservation {
	const structured = normalizeStructuredPaths(structuredOutput(output), "", resourceRoot);
	const paths = uniqueStrings(
		[...outputPaths, ...structuredPaths(structured)].map((item) => normalizeResourcePath(item, resourceRoot)),
	).sort();
	return {
		...(structured !== undefined ? { output: structured } : {}),
		...(paths.length ? { outputPaths: paths } : {}),
	};
}

export function inferBindings(
	context: ReadonlyArray<PatternAwareEvent>,
	target: Record<string, unknown>,
): Record<string, PatternAwareBinding> {
	const bindings: Record<string, PatternAwareBinding> = {};
	for (const [targetPath, value] of leaves(target)) {
		const key = encodePath(targetPath);
		bindings[key] = findBinding(context, value, targetPath) ?? { type: "constant", value };
	}
	return bindings;
}

function inferBindingsFromSamples(
	samples: ReadonlyArray<PatternSample>,
	constantSupport = 4,
	allowProjectedOmissions = false,
): Record<string, PatternAwareBinding> | undefined {
	if (!samples.length) return;
	const bindings: Record<string, PatternAwareBinding> = {};
	const targetPaths = new Map(
		samples.flatMap((sample) =>
			leaves(sample.target.input).map(([targetPath]) => [encodePath(targetPath), targetPath] as const),
		),
	);
	for (const [encodedPath, targetPath] of [...targetPaths].sort(([left], [right]) => left.localeCompare(right))) {
		const targets = samples.map((sample) => getPath(sample.target.input, targetPath));
		if (targets.some((value) => value === MISSING)) {
			if (allowProjectedOmissions) continue;
			return;
		}
		const firstTarget = targets[0];
		const constant = targets.every((value) => sameValue(value, firstTarget));
		if (constant && !requiresProvenance(targetPath, firstTarget)) {
			bindings[encodedPath] = { type: "constant", value: firstTarget };
			continue;
		}
		const targetIsPath = isPathField(String(targetPath.at(-1) ?? ""));
		const direct = uniqueBindings(
			samples.flatMap((sample, index) => candidateBindings(sample.context, targets[index], false, targetIsPath)),
		);
		const completeDirect = direct.find((candidate) =>
			samples.every((sample, index) => bindingMatches(candidate, sample.context, targets[index])),
		);
		const candidates = completeDirect
			? []
			: uniqueBindings([
					...direct,
					...samples.flatMap((sample, index) =>
						typeof targets[index] === "string"
							? candidateBindings(sample.context, targets[index], true, targetIsPath)
							: [],
					),
				]);
		const fallbackSources = uniqueBindings(
			direct.filter((binding) => binding.type === "event" || binding.type === "transform"),
		);
		if (!completeDirect && fallbackSources.length > 1) {
			candidates.push({ type: "coalesce", sources: fallbackSources });
		}
		let selected = completeDirect;
		let selectedReplay = completeDirect ? samples.length : -1;
		for (const candidate of candidates) {
			const replay = samples.reduce(
				(matches, sample, index) => matches + Number(bindingMatches(candidate, sample.context, targets[index])),
				0,
			);
			if (replay <= selectedReplay) continue;
			selected = candidate;
			selectedReplay = replay;
		}
		if (selected) selected = withObservedVariantCounts(selected, samples, targets);
		if (!selected && constant && stablePayloadConstant(samples, constantSupport)) {
			selected = { type: "constant", value: firstTarget };
		}
		if (!selected) {
			if (allowProjectedOmissions) continue;
			return;
		}
		bindings[encodedPath] = selected;
	}
	return bindings;
}

function withObservedVariantCounts(
	binding: PatternAwareBinding,
	samples: ReadonlyArray<PatternSample>,
	targets: ReadonlyArray<unknown>,
): PatternAwareBinding {
	const counts = new Map<number, number>();
	let width = 1;
	for (const [index, sample] of samples.entries()) {
		const values = bindingValues(binding, sample.context);
		width = Math.max(width, values.length);
		const selected = values.findIndex((value) => sameValue(value, targets[index]));
		if (selected >= 0) counts.set(selected, (counts.get(selected) ?? 0) + 1);
	}
	if (width <= 1 || counts.size === 0) return binding;
	return {
		...binding,
		variantCounts: Object.fromEntries([...counts.entries()].map(([index, count]) => [String(index), count])),
	};
}

function requiresProvenance(targetPath: PatternAwarePath, value: unknown): boolean {
	const key = String(targetPath.at(-1) ?? "")
		.toLowerCase()
		.replaceAll("_", "");
	if (isPathField(key)) return true;
	if (typeof value !== "string") return false;
	return [
		"command",
		"content",
		"newstring",
		"oldstring",
		"patch",
		"pattern",
		"query",
		"replacement",
		"script",
		"text",
		"url",
	].some((name) => key === name || key.endsWith(name));
}

function stablePayloadConstant(samples: ReadonlyArray<PatternSample>, minimum: number): boolean {
	if (samples.length < minimum) return false;
	return new Set(samples.map((sample) => `${sample.target.sessionID}:${sample.target.turnID}`)).size >= minimum;
}

function hasSufficientBindingProvenance(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	samples: ReadonlyArray<PatternSample>,
	minimum: number,
) {
	return Object.entries(bindings).every(([encodedPath, binding]) => {
		const targetPath = decodePath(encodedPath);
		return (
			!targetPath ||
			binding.type !== "constant" ||
			!requiresProvenance(targetPath, binding.value) ||
			stablePayloadConstant(samples, minimum)
		);
	});
}

const MISSING = Symbol("missing");
const MULTI = Symbol("multi");
type MultiValue = { readonly [MULTI]: true; readonly values: ReadonlyArray<unknown> };

export function applyBindingsVariants(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	context: ReadonlyArray<PatternAwareEvent>,
	limit = MAX_BINDING_VARIANTS,
): ReadonlyArray<Record<string, unknown>> {
	return applyBindingsPartialWeightedVariants(bindings, context, limit)
		.filter((variant) => variant.missing.length === 0)
		.map((variant) => variant.input);
}

export function applyBindings(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	context: ReadonlyArray<PatternAwareEvent>,
): Record<string, unknown> | undefined {
	return applyBindingsVariants(bindings, context, 1)[0];
}

function applyBindingsPartialWeightedVariants(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	context: ReadonlyArray<PatternAwareEvent>,
	limit = MAX_BINDING_VARIANTS,
): ReadonlyArray<{
	readonly input: Record<string, unknown>;
	readonly missing: ReadonlyArray<PatternAwarePath>;
	readonly probability: number;
}> {
	let variants: Array<{ input: Record<string, unknown>; missing: PatternAwarePath[]; probability: number }> = [
		{ input: {}, missing: [], probability: 1 },
	];
	for (const [encoded, binding] of Object.entries(bindings)) {
		const targetPath = decodePath(encoded);
		const values = weightedBindingValues(binding, context);
		if (!values.length) {
			for (const variant of variants) variant.missing.push(targetPath);
			continue;
		}
		if (values.length === 1) {
			for (const variant of variants) {
				const input = withPath(variant.input, targetPath, values[0]!.value);
				variant.input = input ?? variant.input;
				variant.probability *= values[0]!.probability;
				if (!input) variant.missing.push(targetPath);
			}
			continue;
		}
		const next: Array<{ input: Record<string, unknown>; missing: PatternAwarePath[]; probability: number }> = [];
		for (const variant of variants) {
			for (const value of values) {
				const input = withPath(variant.input, targetPath, value.value);
				const candidate = {
					input: input ?? variant.input,
					missing: [...variant.missing],
					probability: variant.probability * value.probability,
				};
				if (!input) candidate.missing.push(targetPath);
				next.push(candidate);
			}
		}
		variants = next.sort((left, right) => right.probability - left.probability).slice(0, limit);
	}
	return variants;
}

function weightedBindingValues(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>) {
	const values = bindingValues(binding, context);
	if (values.length <= 1) return values.map((value) => ({ value, probability: 1 }));
	const counts = binding.variantCounts;
	if (!counts) {
		const probability = 1 / values.length;
		return values.map((value) => ({ value, probability }));
	}
	const smoothing = 0.5;
	const total = values.reduce<number>((sum, _, index) => sum + variantCount(counts, index), 0);
	const denominator = total + smoothing * values.length;
	return values.map((value, index) => ({
		value,
		probability: (variantCount(counts, index) + smoothing) / denominator,
	}));
}

function variantCount(counts: Readonly<Record<string, number>>, index: number): number {
	const value = counts[String(index)];
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}

function findBinding(
	context: ReadonlyArray<PatternAwareEvent>,
	target: unknown,
	targetPath: PatternAwarePath,
): PatternAwareBinding | undefined {
	return candidateBindings(context, target, true, isPathField(String(targetPath.at(-1) ?? "")))[0];
}

type CandidateBindingSlots = Array<ReadonlyArray<PatternAwareBinding> | undefined>;
type CandidateBindingCache = {
	readonly stringTargets: Map<string, CandidateBindingSlots>;
	otherTargets?: Map<string, CandidateBindingSlots>;
};

const candidateBindingCache = new WeakMap<ReadonlyArray<PatternAwareEvent>, CandidateBindingCache>();

function candidateBindings(
	context: ReadonlyArray<PatternAwareEvent>,
	target: unknown,
	includeComposites = true,
	targetIsPath = false,
): PatternAwareBinding[] {
	const stringTarget = typeof target === "string";
	const cacheKey = stringTarget ? target : `${typeof target}:${stableStringify(target)}`;
	let cache = candidateBindingCache.get(context);
	if (!cache) {
		cache = { stringTargets: new Map() };
		candidateBindingCache.set(context, cache);
	}
	const targets = stringTarget ? cache.stringTargets : (cache.otherTargets ??= new Map());
	const option = Number(includeComposites) * 2 + Number(targetIsPath);
	let slots = targets.get(cacheKey);
	const cached = slots?.[option];
	if (cached) return [...cached];
	const result = inferCandidateBindings(context, target, includeComposites, targetIsPath);
	if (!slots) {
		slots = [];
		targets.set(cacheKey, slots);
	}
	slots[option] = result;
	return result;
}

function inferCandidateBindings(
	context: ReadonlyArray<PatternAwareEvent>,
	target: unknown,
	includeComposites: boolean,
	targetIsPath: boolean,
): PatternAwareBinding[] {
	if (!includeComposites) return indexedBindings(context, target, targetIsPath);
	const result: PatternAwareBinding[] = [];
	const pathSources: Array<{ readonly binding: PatternAwareBinding; readonly value: string }> = [];
	for (let index = context.length - 1; index >= 0; index--) {
		const event = context[index]!;
		const relativeEvent = index - context.length;
		const fields = [
			["input", event.input],
			["output", event.output],
			["outputPaths", event.outputPaths],
		] as const;
		for (const [field, value] of fields) {
			for (const [sourcePath, source] of leaves(value)) {
				const direct: PatternAwareBinding = { type: "event", relativeEvent, field, path: sourcePath };
				const pathSource = typeof source === "string" && isPathSource(field, sourcePath, source);
				if (sameValue(source, target) && (!targetIsPath || pathSource)) result.push(direct);
				if (typeof source !== "string" || typeof target !== "string") continue;
				const sources: Array<{ readonly binding: PatternAwareBinding; readonly value: string }> = [
					{ binding: direct, value: source },
				];
				if (pathSource) {
					if (pathSources.length < MAX_PATH_SOURCES) pathSources.push({ binding: direct, value: source });
					for (const operation of ["dirname", "basename", "normalize_path"] as const) {
						const transformed: PatternAwareBinding = { type: "transform", operation, source: direct };
						const value = transform(operation, source);
						if (value === target) result.push(transformed);
						if (operation !== "basename") sources.push({ binding: transformed, value });
						if (pathSources.length < MAX_PATH_SOURCES) pathSources.push({ binding: transformed, value });
					}
				}
				if (targetIsPath) continue;
				for (const { binding, value } of sources) {
					if (value.length < 3) continue;
					const offset = target.indexOf(value);
					if (offset < 0) continue;
					result.push({
						type: "template",
						source: binding,
						prefix: target.slice(0, offset),
						suffix: target.slice(offset + value.length),
					});
				}
			}
			for (const item of collectionBindings(value, target)) {
				if (
					targetIsPath &&
					typeof target === "string" &&
					!isPathSource(field, [...item.path, ...item.itemPath], target)
				)
					continue;
				result.push({
					type: "each",
					relativeEvent,
					field,
					path: item.path,
					itemPath: item.itemPath,
				});
			}
		}
	}
	if (targetIsPath && typeof target === "string") {
		const sources = uniquePathSources(pathSources);
		const normalizedTarget = normalizePath(target);
		const joinMatches = new Map<string, Map<string, boolean>>();
		for (const left of sources) {
			for (const right of sources) {
				if (left === right) continue;
				const matchesByRight = joinMatches.get(left.value) ?? new Map<string, boolean>();
				joinMatches.set(left.value, matchesByRight);
				let matches = matchesByRight.get(right.value);
				if (matches === undefined) {
					matches = joinPath(left.value, right.value) === normalizedTarget;
					matchesByRight.set(right.value, matches);
				}
				if (!matches) continue;
				result.push({ type: "join", operation: "join_path", left: left.binding, right: right.binding });
			}
		}
	}
	return uniqueBindings(result);
}

function indexedBindings(
	context: ReadonlyArray<PatternAwareEvent>,
	target: unknown,
	targetIsPath: boolean,
): PatternAwareBinding[] {
	const result: PatternAwareBinding[] = [];
	for (let index = context.length - 1; index >= 0; index--) {
		const event = context[index]!;
		const relativeEvent = index - context.length;
		const fields = [
			["input", event.input],
			["output", event.output],
			["outputPaths", event.outputPaths],
		] as const;
		for (const [field, value] of fields) {
			for (const sourcePath of indexedLeaves(value, target)) {
				if (targetIsPath && typeof target === "string" && !isPathSource(field, sourcePath, target)) continue;
				result.push({ type: "event", relativeEvent, field, path: sourcePath });
			}
			for (const item of indexedCollections(value, target)) {
				if (
					targetIsPath &&
					typeof target === "string" &&
					!isPathSource(field, [...item.path, ...item.itemPath], target)
				)
					continue;
				result.push({
					type: "each",
					relativeEvent,
					field,
					path: item.path,
					itemPath: item.itemPath,
				});
			}
		}
	}
	return uniqueBindings(result);
}

type ValueIndex = ReadonlyMap<string, ReadonlyArray<PatternAwarePath>>;

const leafIndexCache = new WeakMap<object, ValueIndex>();

function indexedLeaves(value: unknown, target: unknown): ReadonlyArray<PatternAwarePath> {
	return valueIndex(value, leaves, leafIndexCache).get(stableStringify(target)) ?? [];
}

type CollectionValueIndex = ReadonlyMap<
	string,
	ReadonlyArray<{ readonly path: PatternAwarePath; readonly itemPath: PatternAwarePath }>
>;

const collectionIndexCache = new WeakMap<object, CollectionValueIndex>();

function indexedCollections(
	value: unknown,
	target: unknown,
): ReadonlyArray<{ readonly path: PatternAwarePath; readonly itemPath: PatternAwarePath }> {
	if (isObject(value)) {
		const cached = collectionIndexCache.get(value);
		if (cached) return cached.get(stableStringify(target)) ?? [];
	}
	const mutable = new Map<string, Array<{ path: PatternAwarePath; itemPath: PatternAwarePath }>>();
	for (const entry of collectionEntries(value)) {
		const key = stableStringify(entry.value);
		const paths = mutable.get(key) ?? [];
		paths.push({ path: entry.path, itemPath: entry.itemPath });
		mutable.set(key, paths);
	}
	if (isObject(value)) collectionIndexCache.set(value, mutable);
	return mutable.get(stableStringify(target)) ?? [];
}

function valueIndex(
	value: unknown,
	entries: (value: unknown) => ReadonlyArray<[PatternAwarePath, unknown]>,
	cache: WeakMap<object, ValueIndex>,
): ValueIndex {
	if (isObject(value)) {
		const cached = cache.get(value);
		if (cached) return cached;
	}
	const mutable = new Map<string, PatternAwarePath[]>();
	for (const [sourcePath, item] of entries(value)) {
		const key = stableStringify(item);
		const paths = mutable.get(key) ?? [];
		paths.push(sourcePath);
		mutable.set(key, paths);
	}
	if (isObject(value)) cache.set(value, mutable);
	return mutable;
}

function collectionBindings(
	value: unknown,
	target: unknown,
): Array<{ readonly path: PatternAwarePath; readonly itemPath: PatternAwarePath }> {
	const paths = new Map<string, PatternAwarePath>();
	const result: Array<{ readonly path: PatternAwarePath; readonly itemPath: PatternAwarePath }> = [];
	for (const item of collectionEntries(value)) {
		if (!sameValue(item.value, target)) continue;
		const key = `${encodePath(item.path)}:${encodePath(item.itemPath)}`;
		if (paths.has(key)) continue;
		paths.set(key, item.itemPath);
		result.push({ path: item.path, itemPath: item.itemPath });
	}
	return result;
}

type CollectionEntry = {
	readonly path: PatternAwarePath;
	readonly itemPath: PatternAwarePath;
	readonly value: unknown;
};

const collectionCache = new WeakMap<object, ReadonlyArray<CollectionEntry>>();

function collectionEntries(value: unknown, prefix: PatternAwarePath = []): ReadonlyArray<CollectionEntry> {
	const cacheable = prefix.length === 0 && isObject(value);
	if (cacheable) {
		const cached = collectionCache.get(value);
		if (cached) return cached;
	}
	let result: ReadonlyArray<CollectionEntry>;
	if (Array.isArray(value)) {
		result = value.flatMap((item) =>
			leaves(item).map(([itemPath, candidate]) => ({ path: prefix, itemPath, value: candidate })),
		);
	} else {
		const record = asRecord(value);
		result = record ? Object.entries(record).flatMap(([key, item]) => collectionEntries(item, [...prefix, key])) : [];
	}
	if (cacheable) collectionCache.set(value, result);
	return result;
}

function structuredOutput(value: unknown): unknown {
	const record = asRecord(value);
	if (!record) return value;
	if ("structured" in record) return record.structured;
	if ("metadata" in record) return record.metadata;
	if ("output" in record && asRecord(record.output)) return structuredOutput(record.output);
	const result = asRecord(record.result);
	if (result && "value" in result) return result.value;
	if (
		"details" in record &&
		Array.isArray(record.content) &&
		record.content.every((item) => {
			const content = asRecord(item);
			return !!content && typeof content.type === "string";
		})
	) {
		if (record.details !== undefined) return record.details;
		const values = uniqueStrings(
			record.content.flatMap((item) => {
				const content = asRecord(item);
				if (content?.type !== "text" || typeof content.text !== "string") return [];
				return content.text
					.split(/\r?\n/)
					.map((line) => line.trim())
					.filter((line) => line.length >= 3 && !/\s/.test(line));
			}),
		).sort();
		return values.length ? { values } : undefined;
	}
	return value;
}

function structuredPaths(value: unknown, key = ""): string[] {
	if (typeof value === "string") return isPathField(key) ? [value] : [];
	if (Array.isArray(value)) return value.flatMap((item) => structuredPaths(item, key));
	const record = asRecord(value);
	if (!record) return [];
	return Object.entries(record).flatMap(([name, item]) => structuredPaths(item, name));
}

function normalizeStructuredPaths(value: unknown, key: string, resourceRoot?: string): unknown {
	if (typeof value === "string") return isPathField(key) ? normalizeResourcePath(value, resourceRoot) : value;
	if (Array.isArray(value)) return value.map((item) => normalizeStructuredPaths(item, key, resourceRoot));
	if (ArrayBuffer.isView(value)) return value;
	const record = asRecord(value);
	if (!record) return value;
	return Object.fromEntries(
		Object.entries(record).map(([name, item]) => [name, normalizeStructuredPaths(item, name, resourceRoot)]),
	);
}

function normalizeResourcePath(value: string, resourceRoot?: string) {
	if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) return value;
	if (!resourceRoot || !path.isAbsolute(value)) return normalizePath(value);
	const relative = path.relative(resourceRoot, value);
	if (relative === "") return ".";
	if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
		return normalizePath(value);
	return normalizePath(relative);
}

function isPathField(key: string) {
	const normalized = key.toLowerCase();
	return (
		normalized === "path" ||
		normalized === "paths" ||
		normalized === "file" ||
		normalized === "files" ||
		normalized === "filepath" ||
		normalized === "filename" ||
		normalized === "directory" ||
		normalized === "cwd" ||
		normalized === "root" ||
		normalized === "uri" ||
		normalized.endsWith("path") ||
		normalized.endsWith("paths")
	);
}

function uniquePathSources(values: ReadonlyArray<{ readonly binding: PatternAwareBinding; readonly value: string }>) {
	const seen = new Set<string>();
	return values.filter((item) => {
		const key = stableStringify(bindingStructure(item.binding));
		if (seen.has(key)) return false;
		seen.add(key);
		return true;
	});
}

const bindingStructureKeys = new WeakMap<object, string>();

function uniqueBindings(bindings: ReadonlyArray<PatternAwareBinding>) {
	const seen = new Set<string>();
	return bindings.filter((binding) => {
		let key = bindingStructureKeys.get(binding);
		if (key === undefined) {
			key = stableStringify(bindingStructure(binding));
			bindingStructureKeys.set(binding, key);
		}
		if (seen.has(key)) return false;
		seen.add(key);
		return true;
	});
}

function bindingMapStructure(bindings: Readonly<Record<string, PatternAwareBinding>>) {
	return Object.fromEntries(Object.entries(bindings).map(([key, binding]) => [key, bindingStructure(binding)]));
}

const bindingMapComplexityCache = new WeakMap<object, number>();

function bindingMapComplexity(bindings: Readonly<Record<string, PatternAwareBinding>>): number {
	const cached = bindingMapComplexityCache.get(bindings);
	if (cached !== undefined) return cached;
	const complexity = Object.entries(bindings).reduce(
		(total, [encoded, binding]) => total + bindingComplexity(binding, decodePath(encoded)),
		0,
	);
	bindingMapComplexityCache.set(bindings, complexity);
	return complexity;
}

function bindingComplexity(binding: PatternAwareBinding, targetPath: PatternAwarePath): number {
	if (binding.type === "constant") return Number(requiresProvenance(targetPath, binding.value));
	if (binding.type === "event" || binding.type === "each") return 0;
	if (binding.type === "coalesce")
		return 1 + binding.sources.reduce((total, source) => total + bindingComplexity(source, targetPath), 0);
	if (binding.type === "join")
		return 1 + bindingComplexity(binding.left, targetPath) + bindingComplexity(binding.right, targetPath);
	return 1 + bindingComplexity(binding.source, targetPath);
}

function bindingStructure(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(bindingStructure);
	const record = asRecord(value);
	if (!record) return value;
	return Object.fromEntries(
		Object.entries(record)
			.filter(([key]) => key !== "variantCounts")
			.map(([key, item]) => [key, bindingStructure(item)]),
	);
}

function bindingDependencies(bindings: Readonly<Record<string, PatternAwareBinding>>): PatternAwareDependency[] {
	return Object.entries(bindings).flatMap(([encoded, binding]) => {
		const sources = uniqueDependencySources(bindingSources(binding));
		return sources.length ? [{ targetPath: decodePath(encoded), sources }] : [];
	});
}

function bindingSources(binding: PatternAwareBinding): PatternAwareDependencySource[] {
	if (binding.type === "event") {
		return [
			{
				relativeEvent: binding.relativeEvent,
				field: binding.field,
				path: binding.path,
			},
		];
	}
	if (binding.type === "each") {
		return [
			{
				relativeEvent: binding.relativeEvent,
				field: binding.field,
				path: binding.path,
				itemPath: binding.itemPath,
			},
		];
	}
	if (binding.type === "constant") return [];
	if (binding.type === "coalesce") return binding.sources.flatMap(bindingSources);
	if (binding.type === "join") return [...bindingSources(binding.left), ...bindingSources(binding.right)];
	return bindingSources(binding.source);
}

function uniqueDependencySources(sources: ReadonlyArray<PatternAwareDependencySource>) {
	const seen = new Set<string>();
	return sources.filter((source) => {
		const key = stableStringify(source);
		if (seen.has(key)) return false;
		seen.add(key);
		return true;
	});
}

function isPathSource(field: "input" | "output" | "outputPaths", sourcePath: PatternAwarePath, value: string) {
	if (field === "outputPaths") return true;
	if (!value.length || /[\r\n"'|&<>]/.test(value)) return false;
	const key = String(sourcePath.at(-1) ?? "").toLowerCase();
	if (
		field === "output" &&
		["content", "diff", "message", "output", "preview", "stderr", "stdout", "text"].includes(key)
	)
		return false;
	return (
		key.includes("path") ||
		key.includes("file") ||
		key.includes("dir") ||
		key === "cwd" ||
		key === "root" ||
		key === "name" ||
		/[\\/]/.test(value)
	);
}

const bindingEvaluationCache = new WeakMap<PatternAwareBinding, WeakMap<ReadonlyArray<PatternAwareEvent>, unknown>>();

function evaluateBinding(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>): unknown {
	let contexts = bindingEvaluationCache.get(binding);
	if (contexts?.has(context)) return contexts.get(context);
	contexts ??= new WeakMap();
	bindingEvaluationCache.set(binding, contexts);
	const value = evaluateBindingUncached(binding, context);
	contexts.set(context, value);
	return value;
}

function evaluateBindingUncached(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>): unknown {
	if (binding.type === "constant") return binding.value;
	if (binding.type === "each") {
		const index = context.length + binding.relativeEvent;
		const event = context[index];
		if (!event) return MISSING;
		const collection = getPath(event[binding.field], binding.path);
		if (!Array.isArray(collection)) return MISSING;
		const values = collection.map((item) => getPath(item, binding.itemPath)).filter((value) => value !== MISSING);
		return values.length ? multiValue(values) : MISSING;
	}
	if (binding.type === "join") {
		const left = evaluateBinding(binding.left, context);
		const right = evaluateBinding(binding.right, context);
		const values = bindingValuesFromResult(left).flatMap((leftValue) =>
			bindingValuesFromResult(right).flatMap((rightValue) =>
				typeof leftValue === "string" && typeof rightValue === "string" ? [joinPath(leftValue, rightValue)] : [],
			),
		);
		return values.length > 1 ? multiValue(values) : (values[0] ?? MISSING);
	}
	if (binding.type === "coalesce") {
		for (const source of binding.sources) {
			const value = evaluateBinding(source, context);
			if (value !== MISSING) return value;
		}
		return MISSING;
	}
	if (binding.type === "template") {
		const source = evaluateBinding(binding.source, context);
		const values = bindingValuesFromResult(source).flatMap((value) =>
			typeof value === "string" ? [`${binding.prefix}${value}${binding.suffix}`] : [],
		);
		return values.length > 1 ? multiValue(values) : (values[0] ?? MISSING);
	}
	if (binding.type === "transform") {
		const source = evaluateBinding(binding.source, context);
		const values = bindingValuesFromResult(source).flatMap((value) =>
			typeof value === "string" ? [transform(binding.operation, value)] : [],
		);
		return values.length > 1 ? multiValue(values) : (values[0] ?? MISSING);
	}
	const index = context.length + binding.relativeEvent;
	const event = context[index];
	if (!event) return MISSING;
	return getPath(event[binding.field], binding.path);
}

function bindingValues(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>) {
	return bindingValuesFromResult(evaluateBinding(binding, context));
}

function bindingValuesFromResult(value: unknown): ReadonlyArray<unknown> {
	if (value === MISSING) return [];
	return isMultiValue(value) ? value.values : [value];
}

function bindingMatches(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>, target: unknown) {
	return bindingValues(binding, context).some((value) => sameValue(value, target));
}

function multiValue(values: ReadonlyArray<unknown>): MultiValue {
	const seen = new Set<string>();
	return {
		[MULTI]: true,
		values: values.filter((value) => {
			const key = stableStringify(value);
			if (seen.has(key)) return false;
			seen.add(key);
			return true;
		}),
	};
}

function isMultiValue(value: unknown): value is MultiValue {
	return Boolean(value && typeof value === "object" && MULTI in value);
}

function transform(operation: "dirname" | "basename" | "normalize_path", value: string) {
	if (operation === "dirname") return path.dirname(value);
	if (operation === "basename") return path.basename(value);
	return path.normalize(value).replaceAll("\\", "/");
}

const PATH_OPERATION_CACHE_LIMIT = 128;
const normalizedPaths = new Map<string, string>();
const joinedPaths = new Map<string, string>();

function cachePathResult(cache: Map<string, string>, key: string, value: string) {
	if (cache.size >= PATH_OPERATION_CACHE_LIMIT) cache.delete(cache.keys().next().value!);
	cache.set(key, value);
	return value;
}

function joinPath(left: string, right: string) {
	const key = `${left.length}:${left}${right}`;
	const cached = joinedPaths.get(key);
	if (cached !== undefined) return cached;
	const root = normalizePath(left);
	const output = normalizePath(right);
	const result =
		output === root || output.startsWith(`${root}/`)
			? output
			: path.posix.basename(root) === output
				? root
				: normalizePath(path.join(root, output));
	return cachePathResult(joinedPaths, key, result);
}

function getPath(value: unknown, segments: PatternAwarePath): unknown {
	let current = value;
	for (const segment of segments) {
		if (current === null || typeof current !== "object") return MISSING;
		if (typeof segment === "number") {
			if (!Array.isArray(current) || segment < 0 || segment >= current.length) return MISSING;
			current = current[segment];
			continue;
		}
		if (!(segment in current)) return MISSING;
		current = (current as Record<string, unknown>)[segment];
	}
	return current;
}

function withPath(
	target: Readonly<Record<string, unknown>>,
	segments: PatternAwarePath,
	value: unknown,
): Record<string, unknown> | undefined {
	if (!segments.length || segments.some(unsafePathSegment)) return undefined;
	const update = (current: unknown, index: number): Record<string, unknown> | unknown[] => {
		const segment = segments[index]!;
		const container: Record<string, unknown> | unknown[] =
			typeof segment === "number"
				? Array.isArray(current)
					? [...current]
					: []
				: asRecord(current)
					? { ...(current as Record<string, unknown>) }
					: {};
		const child =
			index === segments.length - 1
				? value
				: update((current as Record<string | number, unknown> | undefined)?.[segment], index + 1);
		if (typeof segment === "number") (container as unknown[])[segment] = child;
		else (container as Record<string, unknown>)[segment] = child;
		return container;
	};
	return update(target, 0) as Record<string, unknown>;
}

function unsafePathSegment(segment: string | number) {
	return segment === "__proto__" || segment === "prototype" || segment === "constructor";
}

const leavesCache = new WeakMap<object, ReadonlyArray<[PatternAwarePath, unknown]>>();

function leaves(value: unknown, prefix: Array<string | number> = []): Array<[Array<string | number>, unknown]> {
	const cacheable = prefix.length === 0 && isObject(value);
	if (cacheable) {
		const cached = leavesCache.get(value);
		if (cached) return cached.map(([path, item]) => [[...path], item]);
	}
	let result: Array<[Array<string | number>, unknown]>;
	if (Array.isArray(value)) {
		result = value.length ? value.flatMap((item, index) => leaves(item, [...prefix, index])) : [[prefix, []]];
	} else if (value && typeof value === "object") {
		const entries = Object.entries(value);
		result = entries.length ? entries.flatMap(([key, item]) => leaves(item, [...prefix, key])) : [[prefix, {}]];
	} else {
		result = [[prefix, value]];
	}
	if (cacheable) leavesCache.set(value, result);
	return result;
}

function structurallyEligible(pattern: MutablePattern, settings: PatternAwareSettings) {
	return (
		pattern.context.length <= settings.maxContextLength &&
		(pattern.occurrences >= settings.minOccurrences ||
			(pattern.occurrences === 1 &&
				pattern.context.length === 1 &&
				pattern.gapCounts["0"] === 1 &&
				pattern.feedback.issued === 0)) &&
		pattern.replayMatches / Math.max(1, pattern.occurrences) >= settings.minBindingReplayProbability
	);
}

function learnedGroupHorizon(
	patterns: ReadonlyArray<MutablePattern>,
	settings: PatternAwareSettings,
	clock: number,
	coverage = settings.futureGapCoverage,
) {
	const gaps = combineWeightedGaps(patterns, settings, clock);
	if (!gaps.length) return 0;
	const total = gaps.reduce((sum, [, weight]) => sum + weight, 0);
	const target = total * coverage;
	let covered = 0;
	for (const [gap, weight] of gaps) {
		covered += weight;
		if (covered >= target) return gap;
	}
	return gaps.at(-1)?.[0] ?? 0;
}

function groupGapCoverage(
	patterns: ReadonlyArray<MutablePattern>,
	horizon: number,
	settings: PatternAwareSettings,
	clock: number,
) {
	const gaps = combineWeightedGaps(patterns, settings, clock);
	const total = gaps.reduce((sum, [, weight]) => sum + weight, 0);
	if (total <= 0) return 0;
	return Math.max(
		0,
		Math.min(1, gaps.filter(([gap]) => gap <= horizon).reduce((sum, [, weight]) => sum + weight, 0) / total),
	);
}

function combineWeightedGaps(patterns: ReadonlyArray<MutablePattern>, settings: PatternAwareSettings, clock: number) {
	const combined = new Map<number, number>();
	for (const pattern of patterns) {
		for (const [gap, weight] of weightedGaps(pattern, settings, clock)) {
			if (gap > settings.maxFutureGap) continue;
			combined.set(gap, (combined.get(gap) ?? 0) + weight);
		}
	}
	return [...combined.entries()].sort(([left], [right]) => left - right);
}

function weightedGaps(pattern: MutablePattern, settings: PatternAwareSettings, clock: number) {
	return Object.entries(pattern.gapCounts)
		.map(([value, count]) => {
			const gap = Number.parseInt(value, 10);
			const lastSeen = pattern.gapLastSeen[value] ?? pattern.lastSeenSequence;
			return [
				Number.isFinite(gap) ? Math.max(0, gap) : 0,
				Math.max(0, count) * recencyWeight(lastSeen, clock, settings.decayHalfLifeEvents),
			] as const;
		})
		.filter(([, weight]) => weight > 0)
		.sort(([left], [right]) => left - right);
}

function matchesSuffix(history: ReadonlyArray<PatternAwareEvent>, context: ReadonlyArray<PatternAwareEventSignature>) {
	if (!context.length || history.length < context.length) return false;
	return sameSignatures(history.slice(-context.length).map(signature), context);
}

function canonicalBatchActionKey(input: PatternAwareEventInput) {
	return stableStringify({
		tool: input.tool,
		outcome: input.outcome,
		...(input.operation ? { operation: input.operation } : {}),
		input: input.input,
	});
}

function persistedEventIdentity(event: PatternAwareEvent) {
	return stableStringify({
		sessionID: event.sessionID,
		turnID: event.turnID,
		sequence: event.sequence,
		tool: event.tool,
		...(event.batchID ? { batchID: event.batchID } : {}),
		...(event.batchIndex !== undefined ? { batchIndex: event.batchIndex } : {}),
	});
}

function actionBatches(history: ReadonlyArray<PatternAwareEvent>) {
	return indexedActionBatches(history).map((batch) => batch.map((index) => history[index]!));
}

function indexedActionBatches(history: ReadonlyArray<PatternAwareEvent>) {
	const batches: number[][] = [];
	let activeBatchID: string | undefined;
	for (const [index, event] of history.entries()) {
		if (event.batchID && event.batchID === activeBatchID) {
			batches.at(-1)!.push(index);
			continue;
		}
		batches.push([index]);
		activeBatchID = event.batchID;
	}
	return batches;
}

const signatureCache = new WeakMap<PatternAwareEvent, PatternAwareEventSignature>();

function signature(event: PatternAwareEvent): PatternAwareEventSignature {
	const cached = signatureCache.get(event);
	if (cached) return cached;
	const outputShape = semanticOutputShape(event.output);
	const value = {
		tool: event.tool,
		outcome: event.outcome,
		...(event.operation ? { operation: event.operation } : {}),
		...(outputShape ? { outputShape } : {}),
	};
	signatureCache.set(event, value);
	return value;
}

function sameSignatures(
	left: ReadonlyArray<PatternAwareEventSignature>,
	right: ReadonlyArray<PatternAwareEventSignature>,
) {
	if (left.length !== right.length) return false;
	return left.every((item, index) => {
		const expected = right[index]!;
		return (
			item.tool === expected.tool &&
			item.outcome === expected.outcome &&
			item.operation === expected.operation &&
			(expected.outputShape === undefined ||
				item.outputShape === undefined ||
				item.outputShape === expected.outputShape)
		);
	});
}

function signatureToken(value: PatternAwareEventSignature) {
	return JSON.stringify({
		...(value.operation ? { operation: value.operation } : {}),
		outcome: value.outcome,
		tool: value.tool,
	});
}

function trieToken(value: PatternAwareEventSignature) {
	return JSON.stringify([value.tool, value.outcome, value.operation ?? null]);
}

function semanticOutputShape(value: unknown) {
	const discriminants: string[] = [];
	const visit = (item: unknown, key = "", depth = 0) => {
		if (depth > 4) return;
		if (
			(typeof item === "string" || typeof item === "number" || typeof item === "boolean") &&
			(key === "kind" || key === "operation" || key === "status" || key === "type")
		) {
			discriminants.push(`${key}:${item}`);
			return;
		}
		if (Array.isArray(item)) {
			if (item.length) visit(item[0], key, depth + 1);
			return;
		}
		const record = asRecord(item);
		if (record) for (const [name, child] of Object.entries(record)) visit(child, name, depth + 1);
	};
	visit(value);
	if (!discriminants.length) return;
	return hash(stableStringify(discriminants.sort()));
}

function backoffProbability(patterns: ReadonlyArray<MutablePattern>, clock: number, halfLife: number) {
	const byLength = new Map<number, MutablePattern>();
	for (const pattern of patterns) {
		const current = byLength.get(pattern.context.length);
		if (!current || current.historicalOpportunities < pattern.historicalOpportunities)
			byLength.set(pattern.context.length, pattern);
	}
	let estimate = 0.5;
	for (const pattern of [...byLength.values()].sort((left, right) => left.context.length - right.context.length)) {
		const weight = recencyWeight(pattern.lastSeenSequence, clock, halfLife);
		const feedback = feedbackEvidence(pattern, clock, halfLife);
		const opportunities = Math.max(
			1,
			pattern.historicalOpportunities * weight + feedback.matched + feedback.mismatched,
		);
		const matches = Math.min(opportunities, pattern.historicalMatches * weight + feedback.matched);
		const local = matches / opportunities;
		const escapeProbability = 1 / (opportunities + 1);
		estimate = local * (1 - escapeProbability) + estimate * escapeProbability;
	}
	return Math.max(0, Math.min(1, estimate));
}

function clampProbability(value: number) {
	return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}

function patternRank(pattern: MutablePattern, clock: number, halfLife: number) {
	const feedback = feedbackEvidence(pattern, clock, halfLife);
	return (
		(feedback.matched * 4 + pattern.replayMatches * 2 + probability(pattern) - feedback.mismatched) *
		recencyWeight(pattern.lastSeenSequence, clock, halfLife)
	);
}

function feedbackEvidence(pattern: MutablePattern, clock: number, halfLife: number) {
	const weight = recencyWeight(pattern.feedback.sequence, clock, halfLife);
	return {
		matched: pattern.feedback.recentMatchedWeight * weight,
		mismatched: pattern.feedback.recentMismatchedWeight * weight,
		adopted: pattern.feedback.recentAdoptedWeight * weight,
		rejected: pattern.feedback.recentRejectedWeight * weight,
	};
}

function patternAdoptionProbability(patterns: ReadonlyArray<MutablePattern>, clock: number, halfLife: number) {
	let adopted = 0;
	let rejected = 0;
	for (const pattern of patterns) {
		const evidence = feedbackEvidence(pattern, clock, halfLife);
		adopted += evidence.adopted;
		rejected += evidence.rejected;
	}
	return clampProbability((1 + adopted) / (1 + adopted + rejected));
}

function recencyWeight(lastSeen: number, clock: number, halfLife: number) {
	if (halfLife <= 0) return 1;
	return 2 ** (-Math.max(0, clock - lastSeen) / halfLife);
}

function perToolBeam<Value>(values: readonly Value[], width: number, tool: (value: Value) => string) {
	const counts = new Map<string, number>();
	return values.filter((value) => {
		const name = tool(value);
		const count = counts.get(name) ?? 0;
		if (count >= width) return false;
		counts.set(name, count + 1);
		return true;
	});
}

function readonlyPattern(pattern: MutablePattern, clock: number, halfLife: number): PatternAwarePattern {
	return {
		...pattern,
		empiricalProbability: backoffProbability([pattern], clock, halfLife),
		adoptionProbability: patternAdoptionProbability([pattern], clock, halfLife),
		feedback: structuredClone(pattern.feedback),
		context: pattern.context.map((item) => ({ ...item })),
		bindings: structuredClone(pattern.bindings),
		dependencies: structuredClone(pattern.dependencies),
		gapCounts: { ...pattern.gapCounts },
		gapLastSeen: { ...pattern.gapLastSeen },
	};
}

function mutablePattern(value: PatternAwarePattern): MutablePattern | undefined {
	const record = asRecord(value);
	const bindings = asRecord(record?.bindings);
	const gapCounts = numericRecord(record?.gapCounts);
	const feedback = mutablePatternFeedback(record?.feedback);
	if (
		!record ||
		typeof record.id !== "string" ||
		!Array.isArray(record.context) ||
		!record.context.every(isEventSignature) ||
		typeof record.targetTool !== "string" ||
		!bindings ||
		!gapCounts ||
		!feedback ||
		![
			record.occurrences,
			record.replayMatches,
			record.historicalOpportunities,
			record.historicalMatches,
			record.averageDurationMs,
			record.lastSeenSequence,
		].every((metric) => isFiniteNumber(metric) && metric >= 0) ||
		(record.targetSchemaHash !== undefined && typeof record.targetSchemaHash !== "string") ||
		!Object.entries(bindings).every(
			([encoded, binding]) => parsePath(encoded) !== undefined && isPatternAwareBinding(binding),
		)
	)
		return;
	const safeBindings = structuredClone(bindings) as Record<string, PatternAwareBinding>;
	return {
		id: record.id,
		context: structuredClone(record.context) as PatternAwareEventSignature[],
		targetTool: record.targetTool,
		bindings: safeBindings,
		dependencies: bindingDependencies(safeBindings),
		...(value.targetSchemaHash ? { targetSchemaHash: value.targetSchemaHash } : {}),
		gapCounts,
		gapLastSeen: Object.fromEntries(
			Object.keys(gapCounts).map((gap) => [
				gap,
				isFiniteNumber(value.gapLastSeen?.[gap]) ? value.gapLastSeen[gap]! : finite(value.lastSeenSequence),
			]),
		),
		occurrences: finite(value.occurrences),
		replayMatches: finite(value.replayMatches),
		historicalOpportunities: Math.max(1, value.historicalOpportunities),
		historicalMatches: value.historicalMatches,
		feedback,
		averageDurationMs: finite(value.averageDurationMs),
		lastSeenSequence: finite(value.lastSeenSequence),
	};
}

function emptyPatternFeedback(sequence: number): MutablePatternFeedback {
	return {
		issued: 0,
		observed: 0,
		matched: 0,
		adopted: 0,
		rejectedAfterMatch: {},
		unobserved: {},
		recentMatchedWeight: 0,
		recentMismatchedWeight: 0,
		recentAdoptedWeight: 0,
		recentRejectedWeight: 0,
		sequence: Math.max(0, sequence),
	};
}

function mutablePatternFeedback(value: unknown): MutablePatternFeedback | undefined {
	const feedback = asRecord(value);
	const rejectedAfterMatch = numericRecord(feedback?.rejectedAfterMatch);
	const unobserved = numericRecord(feedback?.unobserved);
	if (
		!feedback ||
		!rejectedAfterMatch ||
		!unobserved ||
		![
			feedback.issued,
			feedback.observed,
			feedback.matched,
			feedback.adopted,
			feedback.recentMatchedWeight,
			feedback.recentMismatchedWeight,
			feedback.recentAdoptedWeight,
			feedback.recentRejectedWeight,
			feedback.sequence,
		].every((metric) => isFiniteNumber(metric) && metric >= 0)
	)
		return;
	return {
		issued: feedback.issued as number,
		observed: feedback.observed as number,
		matched: feedback.matched as number,
		adopted: feedback.adopted as number,
		rejectedAfterMatch: rejectedAfterMatch as Partial<Record<ResolutionStage, number>>,
		unobserved,
		recentMatchedWeight: feedback.recentMatchedWeight as number,
		recentMismatchedWeight: feedback.recentMismatchedWeight as number,
		recentAdoptedWeight: feedback.recentAdoptedWeight as number,
		recentRejectedWeight: feedback.recentRejectedWeight as number,
		sequence: feedback.sequence as number,
	};
}

function numericRecord(value: unknown): Record<string, number> | undefined {
	const record = asRecord(value);
	if (!record || Object.values(record).some((count) => !isFiniteNumber(count) || count < 0)) return;
	return record as Record<string, number>;
}

function mutablePool(value: unknown): PatternPool | undefined {
	const record = asRecord(value);
	if (!record || !Array.isArray(record.samples)) return;
	const samples = record.samples.flatMap((item) => {
		const sample = asRecord(item);
		if (
			!sample ||
			!Array.isArray(sample.context) ||
			!sample.context.every(isPersistedEvent) ||
			!isPersistedEvent(sample.target) ||
			!isFiniteNumber(sample.gap)
		)
			return [];
		return [
			{
				context: structuredClone(sample.context) as PatternAwareEvent[],
				target: structuredClone(sample.target) as PatternAwareEvent,
				gap: Math.max(0, Math.floor(sample.gap)),
			},
		];
	});
	return mutablePoolRecord(record, samples);
}

function mutableIndexedPools(eventsValue: unknown, pools: ReadonlyArray<unknown>): PatternPool[] {
	if (!Array.isArray(eventsValue)) return [];
	const events = eventsValue.map((item) =>
		isPersistedEvent(item) ? (structuredClone(item) as PatternAwareEvent) : undefined,
	);
	return pools.flatMap((value) => {
		const record = asRecord(value);
		if (!record || !Array.isArray(record.samples)) return [];
		const samples = record.samples.flatMap((item) => {
			const sample = asRecord(item);
			if (
				!sample ||
				!Array.isArray(sample.context) ||
				!sample.context.every(isEventReference) ||
				!isEventReference(sample.target) ||
				!isFiniteNumber(sample.gap)
			)
				return [];
			const context = sample.context.map((id) => events[id as number]);
			const target = events[sample.target as number];
			if (!target || context.some((event) => !event)) return [];
			return [{ context: context as PatternAwareEvent[], target, gap: Math.max(0, Math.floor(sample.gap)) }];
		});
		return mutablePoolRecord(record, samples) ?? [];
	});
}

function mutablePoolRecord(record: Record<string, unknown>, samples: PatternSample[]): PatternPool | undefined {
	if (
		typeof record.key !== "string" ||
		typeof record.targetTool !== "string" ||
		!Array.isArray(record.context) ||
		!record.context.every(isEventSignature) ||
		!samples.length
	)
		return;
	const patternIDs = Array.isArray(record.patternIDs)
		? record.patternIDs.filter((item): item is string => typeof item === "string")
		: typeof record.patternID === "string"
			? [record.patternID]
			: [];
	return {
		key: record.key,
		context: structuredClone(record.context) as PatternAwareEventSignature[],
		targetTool: record.targetTool,
		...(typeof record.targetSchemaHash === "string" ? { targetSchemaHash: record.targetSchemaHash } : {}),
		gap: isFiniteNumber(record.gap) ? Math.max(0, Math.floor(record.gap)) : samples[0]!.gap,
		samples,
		...(patternIDs.length ? { patternIDs: [...new Set(patternIDs)] } : {}),
	};
}

function isEventReference(value: unknown): value is number {
	return Number.isSafeInteger(value) && (value as number) >= 0;
}

function patternPoolKey(
	context: ReadonlyArray<PatternAwareEventSignature>,
	targetTool: string,
	targetSchemaHash: string | undefined,
	gap: number,
) {
	return hash(stableStringify({ context, targetTool, targetSchemaHash, gap }));
}

function patternControlKey(context: ReadonlyArray<PatternAwareEventSignature>, gap: number) {
	return stableStringify({ context, gap });
}

function controlOpportunityID(event: PatternAwareEvent) {
	return event.batchID !== undefined
		? stableStringify({ sessionID: event.sessionID, batchID: event.batchID })
		: persistedEventIdentity(event);
}

function controlOpportunityCount(samples: ReadonlyArray<PatternSample>) {
	return new Set(samples.map((sample) => controlOpportunityID(sample.target))).size;
}

function samplesByGap(samples: ReadonlyArray<PatternSample>) {
	const groups = new Map<number, PatternSample[]>();
	for (const sample of samples) {
		const group = groups.get(sample.gap) ?? [];
		group.push(sample);
		groups.set(sample.gap, group);
	}
	return groups;
}

function patternPoolSampleLimit(settings: Pick<PatternAwareSettings, "minOccurrences" | "maxContextLength">) {
	return Math.max(settings.minOccurrences * 4, settings.maxContextLength * 4);
}

function sampleGapCounts(samples: ReadonlyArray<PatternSample>) {
	const counts: Record<string, number> = {};
	for (const sample of samples) counts[String(sample.gap)] = (counts[String(sample.gap)] ?? 0) + 1;
	return counts;
}

function sampleGapLastSeen(samples: ReadonlyArray<PatternSample>) {
	const lastSeen: Record<string, number> = {};
	for (const sample of samples) {
		const gap = String(sample.gap);
		lastSeen[gap] = Math.max(lastSeen[gap] ?? 0, sample.target.sequence);
	}
	return lastSeen;
}

function averageTargetDuration(samples: ReadonlyArray<PatternSample>) {
	return (
		samples.reduce(
			(total, sample) => total + (sample.target.outcome === "success" ? Math.max(0, sample.target.durationMs) : 0),
			0,
		) / Math.max(1, samples.length)
	);
}

function bindingEvidenceThreshold(settings: Pick<PatternAwareSettings, "minOccurrences">) {
	return Math.max(4, settings.minOccurrences * 2);
}

function isEventSignature(value: unknown): value is PatternAwareEventSignature {
	const record = asRecord(value);
	return (
		!!record &&
		typeof record.tool === "string" &&
		(record.outcome === "success" || record.outcome === "failure") &&
		(record.operation === undefined || typeof record.operation === "string") &&
		(record.outputShape === undefined || typeof record.outputShape === "string")
	);
}

function isPatternAwareBinding(value: unknown, depth = 0): value is PatternAwareBinding {
	if (depth > 16) return false;
	const record = asRecord(value);
	if (!record || typeof record.type !== "string") return false;
	const source = () => isPatternAwareBinding(record.source, depth + 1);
	const eventSource = () =>
		Number.isInteger(record.relativeEvent) &&
		(record.relativeEvent as number) < 0 &&
		(record.field === "input" || record.field === "output" || record.field === "outputPaths") &&
		isPatternAwarePath(record.path);
	switch (record.type) {
		case "constant":
			return true;
		case "event":
			return eventSource();
		case "each":
			return eventSource() && isPatternAwarePath(record.itemPath);
		case "transform":
			return ["dirname", "basename", "normalize_path"].includes(String(record.operation)) && source();
		case "coalesce":
			return (
				Array.isArray(record.sources) &&
				record.sources.length > 0 &&
				record.sources.every((item) => isPatternAwareBinding(item, depth + 1))
			);
		case "template":
			return typeof record.prefix === "string" && typeof record.suffix === "string" && source();
		case "join":
			return (
				record.operation === "join_path" &&
				isPatternAwareBinding(record.left, depth + 1) &&
				isPatternAwareBinding(record.right, depth + 1)
			);
		default:
			return false;
	}
}

function isPatternAwarePath(value: unknown): value is PatternAwarePath {
	return (
		Array.isArray(value) &&
		value.every(
			(segment) => typeof segment === "string" || (Number.isSafeInteger(segment) && (segment as number) >= 0),
		)
	);
}

function isPersistedEvent(value: unknown): value is PatternAwareEvent {
	const record = asRecord(value);
	return (
		!!record &&
		typeof record.sessionID === "string" &&
		typeof record.turnID === "string" &&
		typeof record.tool === "string" &&
		!!asRecord(record.input) &&
		(record.outcome === "success" || record.outcome === "failure") &&
		isFiniteNumber(record.durationMs) &&
		isFiniteNumber(record.sequence)
	);
}

function encodePath(segments: PatternAwarePath) {
	return JSON.stringify(segments);
}

function decodePath(value: string): PatternAwarePath {
	return parsePath(value) ?? [];
}

const parsedPaths = new Map<string, PatternAwarePath | null>();

function parsePath(value: string): PatternAwarePath | undefined {
	const cached = parsedPaths.get(value);
	if (cached !== undefined) return cached ?? undefined;
	try {
		const parsed: unknown = JSON.parse(value);
		const result = isPatternAwarePath(parsed) ? parsed : null;
		parsedPaths.set(value, result);
		return result ?? undefined;
	} catch {
		parsedPaths.set(value, null);
		return undefined;
	}
}

function uniqueStrings(values: ReadonlyArray<string>) {
	return [...new Set(values.filter((value) => value.length > 0))];
}

function normalizePath(value: string) {
	const cached = normalizedPaths.get(value);
	if (cached !== undefined) return cached;
	return cachePathResult(normalizedPaths, value, path.normalize(value).replaceAll("\\", "/"));
}

function hash(value: string) {
	return createHash("sha256").update(value).digest("hex").slice(0, 32);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function isObject(value: unknown): value is object {
	return value !== null && typeof value === "object";
}

function positiveInteger(value: unknown, fallback: number) {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function nonNegativeInteger(value: unknown, fallback: number) {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.floor(value) : fallback;
}

function probabilitySetting(value: unknown, fallback: number) {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1 ? value : fallback;
}

function probability(pattern: Pick<MutablePattern, "historicalMatches" | "historicalOpportunities">) {
	return Math.max(0, Math.min(1, pattern.historicalMatches / Math.max(1, pattern.historicalOpportunities)));
}

function finite(value: unknown) {
	return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function isFiniteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}
