import { clampProbability, nonNegativeFinite } from "./number-utils.ts";
import { hash as cryptoHash } from "node:crypto";
import { writeJsonFile } from "./filesystem-evidence.ts";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { type ActionKey, type ActionKeyProjector, type ActionSemanticsRegistry, actionKeyCovers, ownActionKeyProjector } from "./action-semantics.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { patternSessionBudgets, type PatternPendingValidation, type PatternRecurrentAction, type PatternSessionState } from "./pattern-session-state.ts";
import { containsLogicalPath, relativeFilesystemPath } from "./path-utils.ts";
import { PpmCountTrie, type PpmCountTrieRow, type PpmProbabilityEstimate } from "./ppm-count-trie.ts";
import type { PredictionSettlement, ResolutionStage } from "./settlement.ts";
import { asRecord, stableEqual as sameValue, stableStringify } from "./stable-json.ts";
import { booleanOr, nonNegativeInteger, positiveInteger, probability as probabilitySetting, settingsParser } from "./setting-input.ts";

export type PatternAwareSettings = Readonly<typeof patternAwareDefaults>;

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
	/** Equal namespaces promise the same canonicalization and projection contract, including captured inputs. */
	readonly namespace: string;
	/** Deterministic K(a) projection for one namespace; repeated inputs may be memoized. */
	readonly actionKey: (
		tool: string,
		input: Readonly<Record<string, unknown>>,
		schemaHash?: string,
	) => ActionKey | undefined;
	readonly projectors?: readonly ActionKeyProjector[];
};

export function patternAwareActionSemantics(
	registry: ActionSemanticsRegistry, cwd: string, projectors: readonly ActionKeyProjector[] = [],
): PatternAwareActionSemantics {
	cwd = path.resolve(cwd);
	const rules = Object.freeze(projectors.map(ownActionKeyProjector));
	return Object.freeze({
		namespace: stableStringify([cwd,
			[...registry.toolNames()].sort().map(tool => [tool, registry.definition(tool)!.epoch]), rules.map(rule => rule.id).sort()]),
		actionKey: (tool: string, input: Readonly<Record<string, unknown>>, schemaHash?: string) => registry.buildKey(tool, input, cwd, schemaHash),
		projectors: rules,
	});
}

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
	| (Omit<PatternAwareDependencySource, "itemPath"> & { readonly type: "event" })
	| (Required<PatternAwareDependencySource> & { readonly type: "each" })
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

export type PatternAwarePattern = Readonly<Omit<MutablePattern, "context" | "bindings" | "dependencies" | "gapCounts" | "gapLastSeen" | "feedback">> & {
	readonly context: ReadonlyArray<PatternAwareEventSignature>;
	readonly bindings: Readonly<Record<string, PatternAwareBinding>>;
	readonly dependencies?: ReadonlyArray<PatternAwareDependency>;
	readonly gapCounts: Readonly<Record<string, number>>;
	readonly gapLastSeen?: Readonly<Record<string, number>>;
	readonly empiricalProbability: number;
	readonly adoptionProbability: number;
	readonly feedback: PatternAwareFeedback;
};

export type PatternAwareFeedback = Readonly<PatternFeedbackCounters> & {
	readonly rejectedAfterMatch: Readonly<Partial<Record<ResolutionStage, number>>>;
	readonly unobserved: Readonly<Record<string, number>>;
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

const PATTERN_FEEDBACK_COUNTERS = ["issued", "observed", "matched", "adopted", "recentMatchedWeight",
	"recentMismatchedWeight", "recentAdoptedWeight", "recentRejectedWeight", "sequence"] as const;
type PatternFeedbackCounters = Record<typeof PATTERN_FEEDBACK_COUNTERS[number], number>;
type MutablePatternFeedback = PatternFeedbackCounters & {
	rejectedAfterMatch: Partial<Record<ResolutionStage, number>>;
	unobserved: Record<string, number>;
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
	readonly version: typeof PERSISTENCE_VERSION;
	readonly patterns: ReadonlyArray<PatternAwarePattern>;
	readonly events: ReadonlyArray<PatternAwareEvent>;
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
	readonly patterns: Set<MutablePattern>;
};

const patternAwareDefaults = {
	enabled: true,
	/** Admit future-gap/preparation candidates and expand completed predictions into a multi-step frontier. */
	multiStepEnabled: true,
	maxContextLength: 4,
	/** Maximum competing concrete actions retained per tool at each PatternAware frontier. */
	beamWidth: 4,
	/** Maximum number of recursively predicted actions on one branch. */
	maxPredictionDepth: 6,
	maxFutureGap: 2,
	/** Weighted future-gap quantile used as the expected launch horizon; the deadline keeps full observed support. */
	futureGapCoverage: 0.25,
	decayHalfLifeEvents: 2048,
	/** Support required to promote a relation after its single bounded first-recurrence probe. */
	minOccurrences: 2,
	/** Minimum historical replay precision required for a concrete argument mapper. */
	minBindingReplayProbability: 0.75,
	maxPatterns: 4096,
};

export const PATTERN_AWARE_DEFAULTS: PatternAwareSettings = patternAwareDefaults;

const parsePatternSettings = settingsParser(patternAwareDefaults, {
	enabled: booleanOr,
	multiStepEnabled: booleanOr,
	maxContextLength: positiveInteger,
	beamWidth: positiveInteger,
	maxPredictionDepth: positiveInteger,
	maxFutureGap: nonNegativeInteger,
	futureGapCoverage: probabilitySetting,
	decayHalfLifeEvents: positiveInteger,
	minOccurrences: positiveInteger,
	minBindingReplayProbability: probabilitySetting,
	maxPatterns: positiveInteger,
});

const MAX_BINDING_VARIANTS = 32;
const MAX_PATH_SOURCES = 24;
// Bound crash-loss while amortizing full-state serialization across active tool loops.
// Terminal/dispose paths still flush immediately.
const PERSIST_CHECKPOINT_INTERVAL_MS = 30_000;
const PERSISTENCE_VERSION = 20;

class PredictiveContextTrie {
	private readonly root: TrieNode = { children: new Map(), patterns: new Set() };

	insert(pattern: MutablePattern) {
		let node = this.root;
		for (let index = pattern.context.length - 1; index >= 0; index--) {
			const token = trieToken(pattern.context[index]!);
			const child = node.children.get(token) ?? { children: new Map(), patterns: new Set() };
			node.children.set(token, child);
			node = child;
		}
		node.patterns.add(pattern);
	}

	*matching(history: ReadonlyArray<PatternAwareEvent>) {
		let node = this.root;
		for (let index = history.length - 1; index >= 0; index--) {
			const token = trieToken(signature(history[index]!));
			const child = node.children.get(token);
			if (!child) break;
			node = child;
			if (!node.patterns.size) continue;
			const context = history.slice(index);
			// Trie edges already match tool/outcome/operation; output shapes retain wildcard semantics.
			const shapes = context.map((event) => signature(event).outputShape);
			for (const pattern of node.patterns) {
				if (pattern.context.every((expected, offset) => expected.outputShape === undefined ||
					shapes[offset] === undefined || shapes[offset] === expected.outputShape)) yield { pattern, context };
			}
		}
	}
}

export class PatternAwareStore {
	private readonly patterns = new Map<string, MutablePattern>();
	private readonly bindingAnalysis = new PatternBindingAnalysis();
	private readonly pools = new Map<string, PatternPool>();
	private readonly controlOpportunitiesByContext = new Map<string, Map<string, number>>();
	private readonly sessions: BoundedRecencyMap<string, PatternSessionState<PatternAwareEvent>>;
	private readonly sessionBudgets: ReturnType<typeof patternSessionBudgets>;
	private readonly observedActionKeys = new WeakMap<PatternAwareEvent, ActionKey | null>();
	private readonly recurrentFeedback = new WeakMap<PatternRecurrentAction | PatternAwareContinuation, MutablePatternFeedback>();
	private readonly resolvedActionKeys: BoundedRecencyMap<string, ActionKey | null>;
	private readonly patternSupportSessions = new Map<string, ReadonlySet<string>>();
	private trie = new PredictiveContextTrie();
	private sequenceModel: PpmCountTrie;
	private indexDirty = true;
	private clock = 0;
	private write: Promise<void> = Promise.resolve();
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
		settings = { ...settings };
		this.settings = settings;
		this.sessionBudgets = patternSessionBudgets(settings.maxPatterns);
		this.sessions = new BoundedRecencyMap(this.sessionBudgets.sessions);
		this.resolvedActionKeys = new BoundedRecencyMap(settings.maxPatterns);
		this.sequenceModel = new PpmCountTrie(settings.maxContextLength);
		this.persistenceFile = persistenceFile;
		this.actionSemantics = actionSemantics && { ...actionSemantics,
			projectors: actionSemantics.projectors?.map(ownActionKeyProjector) };
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
			parsed.version !== PERSISTENCE_VERSION ||
			!Array.isArray(parsed.patterns) ||
			!Array.isArray(parsed.events) ||
			!Array.isArray(parsed.pools) ||
			!Array.isArray(parsed.sequenceCounts)
		)
			return;
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
		for (const pool of mutablePools(parsed.events, parsed.pools)) {
			if (
				pool.context.length > this.settings.maxContextLength ||
				pool.context.some((event) => event.tool === "$llm")
			)
				continue;
			this.pools.set(pool.key, pool);
			this.addControlOpportunities(pool, pool.samples, 1);
			for (const patternID of pool.patternIDs ?? []) {
				if (this.patterns.get(patternID)?.dependencies.length === 0) {
					this.patternSupportSessions.set(patternID, new Set(pool.samples.map((sample) => sample.target.sessionID)));
				}
			}
			for (const sample of pool.samples) {
				this.clock = Math.max(this.clock, sample.target.sequence, ...sample.context.map((event) => event.sequence));
			}
		}
		this.sequenceModel.restore(parsed.sequenceCounts);
		this.sequenceModel.trim(this.settings.maxPatterns);
		this.indexDirty = true;
		this.trimPools();
		this.trimPatterns();
	}

	observe(input: PatternAwareEventInput) {
		if (this.settings.enabled) this.observeEvents(structuredClone([input]));
	}

	observeBatch(inputs: ReadonlyArray<PatternAwareEventInput>) {
		const batch = ownBatch(inputs);
		this.observeEvents(batch, batch[0]?.turnID);
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
		let session = this.sessions.get(first.sessionID);
		if (!session) {
			session = { history: [], pending: [], recurrentActions: new BoundedRecencyMap(this.sessionBudgets.recurrentActionsPerSession) };
			const evicted = this.sessions.set(first.sessionID, session)?.value;
			if (evicted) this.finishSessionState(evicted);
		}
		const history = session.history;
		this.resolvePendingBatch(session, events);
		const learningTargets = events.filter((event) => event.learnTarget !== false);
		const contexts = learningTargets.length ? [...this.learningContexts(history)] : [];
		const contextTokens = learningTargets.length ? history.map((item) => signatureToken(signature(item))) : [];
		for (const event of learningTargets) {
			this.sequenceModel.observe(contextTokens, event.tool, event.sequence, this.settings.decayHalfLifeEvents);
			for (const { context, gap } of contexts) this.learnOccurrence(context, event, gap);
			this.observeRecurrentAction(session, event);
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
		const session = this.sessions.get(sessionID);
		if (session) {
			this.sessions.delete(sessionID);
			this.finishSessionState(session);
		}
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
		seed?: Pick<PatternAwareContinuation, "visitedPatternIDs" | "pathProbability">,
	) {
		if (!predictionSettings.enabled || !inputs.length) return [];
		const ordered = ownBatch(inputs, sessionID), turnID = ordered[0]!.turnID;
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
			{ history, visitedPatternIDs: [], pathProbability: 1, ...seed },
			predictionSettings,
			seed === undefined,
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
		const history = structuredClone([...continuation.history, event]);
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
			parentConfirmed,
		);
	}

	private predictHistory(
		history: ReadonlyArray<PatternAwareEvent>,
		schemaHashes: Readonly<Record<string, string>>,
		continuation: PatternAwareContinuation,
		settings: PatternAwareSettings,
		authoritative = true,
	) {
		if (continuation.visitedPatternIDs.length >= settings.maxPredictionDepth) return [];
		const activeSessionID = history.at(-1)?.sessionID;
		const groups = new Map<
			string,
			Array<{
				readonly pattern: MutablePattern;
				readonly input: Record<string, unknown>;
				variantProbability: number;
			}>
		>();
		this.ensureIndex();
		for (const { pattern, context } of this.trie.matching(history)) {
			const patternID = pattern.id;
			if (continuation.visitedPatternIDs.includes(patternID) || !structurallyEligible(pattern, settings))
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
			for (const applied of this.bindingAnalysis.applyWeightedBindings(pattern.bindings, context)) {
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
				// A pattern emits its variants together; aliases share one evidence weight.
				const support = group.at(-1);
				if (support?.pattern === pattern) support.variantProbability += applied.probability;
				else group.push({ pattern, input: applied.input, variantProbability: applied.probability });
				groups.set(identity, group);
			}
		}
		let ppmEstimates: ReadonlyMap<string, PpmProbabilityEstimate> | undefined;
		const estimatePpm = (tool: string) => (ppmEstimates ??=
			this.sequenceModel.distribution(history.map((event) => signatureToken(signature(event))),
				this.clock, settings.decayHalfLifeEvents)).get(tool);
		const contextEvidence = new Map<number, Map<string, number>>();
		for (const group of groups.values()) for (const { pattern } of group) {
			const gaps = contextEvidence.get(pattern.context.length) ?? new Map<string, number>();
			const evidence = pattern.historicalOpportunities * recencyWeight(pattern.lastSeenSequence, this.clock, settings.decayHalfLifeEvents);
			for (const [gap, count] of Object.entries(pattern.gapCounts)) {
				if (count > 0 && Number(gap) <= settings.maxFutureGap) gaps.set(gap, Math.max(gaps.get(gap) ?? 0, evidence));
			}
			contextEvidence.set(pattern.context.length, gaps);
		}
		const predictions = new Map([...groups.entries()].map(([identity, group]) => {
			const ordered = group.sort(
				(left, right) =>
					right.pattern.context.length - left.pattern.context.length ||
					right.pattern.occurrences - left.pattern.occurrences,
			);
			const representative = ordered[0]!;
			const patterns = ordered.map((item) => item.pattern);
			const { horizon, latestHorizon, gapCoverage } = groupGapTiming(patterns, settings, this.clock);
			const replayProbability = backoffProbability(patterns, this.clock, settings.decayHalfLifeEvents, contextEvidence);
			const targetTool = representative.pattern.targetTool;
			const ppmEstimate = estimatePpm(targetTool);
			let totalWeight = 0, weightedVariants = 0, weightedDuration = 0;
			for (const item of ordered) {
				const occurrences = Math.max(1, item.pattern.occurrences);
				const decay = recencyWeight(item.pattern.lastSeenSequence, this.clock, settings.decayHalfLifeEvents);
				totalWeight += occurrences * decay;
				weightedVariants += item.variantProbability * occurrences * decay;
				weightedDuration += Math.max(0, item.pattern.averageDurationMs) * occurrences * decay;
			}
			const variantProbability = weightedVariants / Math.max(1, totalWeight);
			const expectedDurationMs = weightedDuration / Math.max(1, totalWeight);
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
			const actionIdentity = hash(identity);
			return [actionIdentity, {
				background,
				recurrentFeedback: undefined as MutablePatternFeedback | undefined,
				actionIdentity,
				type: "tool_call" as const,
				tool: representative.pattern.targetTool,
				input: representative.input,
				patternID: representative.pattern.id,
				supportingPatternIDs: patterns.map((pattern) => pattern.id),
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
			}] as const;
		}));
		// Session frequency supports another Actor opportunity, not a transition from hypothetical output.
		for (const recurrent of authoritative ? this.recurrentPredictions(
			activeSessionID,
			schemaHashes,
			estimatePpm,
			continuation,
			settings,
		) : []) {
			const existing = predictions.get(recurrent.actionIdentity);
			if (!existing) {
				predictions.set(recurrent.actionIdentity, recurrent);
				continue;
			}
			const preferred =
				recurrent.background !== existing.background
					? recurrent.background
						? existing
						: recurrent
					: recurrent.expectedLatencyBenefitMs > existing.expectedLatencyBenefitMs
						? recurrent
						: existing;
			predictions.set(recurrent.actionIdentity, {
				...preferred,
				recurrentFeedback: recurrent.recurrentFeedback,
				background: existing.background && recurrent.background,
				supportingPatternIDs: [...new Set([...existing.supportingPatternIDs, ...recurrent.supportingPatternIDs])],
			});
		}
		const ranked = [...predictions.values()].sort((left, right) =>
			Number(left.background) - Number(right.background) ||
			right.expectedLatencyBenefitMs - left.expectedLatencyBenefitMs ||
			right.empiricalProbability - left.empiricalProbability ||
			right.conditionalProbability - left.conditionalProbability ||
			left.horizon - right.horizon ||
			left.patternID.localeCompare(right.patternID) ||
			left.actionIdentity.localeCompare(right.actionIdentity));
		const beamWidth = settings.beamWidth;
		const selected: PatternAwareCandidate[] = [];
		let continuationHistory: typeof history | undefined;
		const emittedPerTool = new Map<string, number>();
		for (const prediction of ranked) {
			const count = emittedPerTool.get(prediction.tool) ?? 0;
			if (count >= beamWidth) continue;
			const beamRank = count + 1;
			emittedPerTool.set(prediction.tool, beamRank);
			const { input, dependencies, background, context, recurrentFeedback, ppmEstimate,
				mapperConfidence, variantProbability, gapCoverage, replayProbability, ...candidate } = prediction;
			const { type: _type, actionIdentity: _identity, supportingPatternIDs, ...diagnostic } = candidate;
			const nextContinuation: PatternAwareContinuation = {
				history: continuationHistory ??= structuredClone(history),
				visitedPatternIDs: [...continuation.visitedPatternIDs, prediction.patternID],
				pathProbability: prediction.empiricalProbability,
			};
			if (recurrentFeedback) this.recurrentFeedback.set(nextContinuation, recurrentFeedback);
			selected.push({
				...candidate,
				source: "pattern_aware",
				input: structuredClone(input),
				...(background ? { background: true } : {}),
				dependencies: structuredClone(dependencies),
				continuation: nextContinuation,
				depth: nextContinuation.visitedPatternIDs.length,
				diagnostic: JSON.stringify(
					{
						...diagnostic,
						source: "pattern_aware",
						supportingPatterns: supportingPatternIDs,
						context,
						input,
						replayProbability,
						ppmProbability: ppmEstimate?.probability,
						ppmOrder: ppmEstimate?.order,
						ppmEvidence: ppmEstimate?.evidence,
						ppmEscapeMass: ppmEstimate?.escapeMass,
						mapperConfidence,
						variantProbability,
						background: background === true,
						beamRank,
						beamWidth: settings.beamWidth,
						gapCoverage,
						dependencies,
						depth: nextContinuation.visitedPatternIDs.length,
					},
					null,
					2,
				),
			});
		}
		return selected;
	}

	private recurrentPredictions(
		sessionID: string | undefined,
		schemaHashes: Readonly<Record<string, string>>,
		estimatePpm: (tool: string) => PpmProbabilityEstimate | undefined,
		continuation: PatternAwareContinuation,
		settings: PatternAwareSettings,
	) {
		const actions = sessionID ? this.sessions.get(sessionID)?.recurrentActions.values() : undefined;
		if (!actions) return [];
		const values = [...actions].filter((item) => {
			const current = schemaHashes[item.action.tool];
			return current === undefined || current === item.action.schemaHash;
		}).map((item) => {
			let feedback = this.recurrentFeedback.get(item);
			if (!feedback) {
				feedback = emptyPatternFeedback(this.clock);
				this.recurrentFeedback.set(item, feedback);
			}
			return { ...item, feedback, patternID: `action-backoff:${hash(item.action.key)}`,
				mass: item.weightedCount * recencyWeight(item.lastSeenSequence, this.clock, settings.decayHalfLifeEvents) };
		});
		const massByTool = new Map<string, number>();
		for (const item of values) {
			massByTool.set(item.action.tool, (massByTool.get(item.action.tool) ?? 0) + item.mass);
		}
		const provenTools = new Set(
			values.filter((item) => item.count >= settings.minOccurrences).map((item) => item.action.tool),
		);
		// The final beam ranks merged contextual and recurrent support using the same settled evidence.
		const candidates = values.filter((item) => provenTools.has(item.action.tool) && !continuation.visitedPatternIDs.includes(item.patternID));
		return candidates.map((item) => {
			const { patternID, mass } = item;
			const evidence = feedbackEvidence(item, this.clock, settings.decayHalfLifeEvents);
			const conditionalProbability = clampProbability((mass + evidence.matched) /
				(Math.max(mass, massByTool.get(item.action.tool) ?? 0) + evidence.matched + evidence.mismatched));
			const empiricalProbability = clampProbability(continuation.pathProbability * conditionalProbability);
			const expectedDurationMs = item.weightedDurationMs / item.weightedCount;
			const ppmEstimate = estimatePpm(item.action.tool);
			const adoptionProbability = patternAdoptionProbability([item], this.clock, settings.decayHalfLifeEvents);
			const expectedLatencyBenefitMs =
				empiricalProbability * adoptionProbability * (ppmEstimate?.probability ?? 1) * Math.max(1, expectedDurationMs);
			return {
				background: item.count < settings.minOccurrences || evidence.mismatched > evidence.matched,
				recurrentFeedback: item.feedback,
				actionIdentity: hash(JSON.stringify({ actionKey: item.action.key, type: "tool_call" })),
				type: "tool_call" as const,
				tool: item.action.tool,
				input: item.input,
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
				adoptionProbability,
				expectedDurationMs,
				ppmEstimate,
				mapperConfidence: 1,
				expectedLatencyBenefitMs,
			};
		});
	}

	/** Use a persisted pattern ID or the original candidate continuation for session backoff feedback. */
	issued(support: string | PatternAwareContinuation) {
		const feedback = this.supportFeedback(support);
		if (!feedback) return;
		feedback.issued++;
		if (typeof support === "string") this.persist();
	}

	settled(support: string | PatternAwareContinuation, settlement: PredictionSettlement) {
		const feedback = this.supportFeedback(support);
		if (!feedback) return;
		const recent = feedbackEvidence({ feedback }, this.clock, this.settings.decayHalfLifeEvents);
		feedback.recentMatchedWeight = recent.matched;
		feedback.recentMismatchedWeight = recent.mismatched;
		feedback.recentAdoptedWeight = recent.adopted;
		feedback.recentRejectedWeight = recent.rejected;
		feedback.sequence = this.clock;
		if (settlement.observation === "unobserved") {
			const key = `${settlement.cause.stage}:${settlement.cause.code}`;
			feedback.unobserved[key] = (feedback.unobserved[key] ?? 0) + 1;
		} else {
			feedback.observed++;
			if (!settlement.match.matched) {
				feedback.recentMismatchedWeight++;
			} else {
				feedback.matched++;
				feedback.recentMatchedWeight++;
				if (settlement.match.adoption.status === "adopted") {
					feedback.adopted++;
					feedback.recentAdoptedWeight++;
				} else {
					const stage = settlement.match.adoption.cause.stage;
					feedback.rejectedAfterMatch[stage] = (feedback.rejectedAfterMatch[stage] ?? 0) + 1;
					feedback.recentRejectedWeight++;
				}
			}
		}
		if (typeof support === "string") this.persist();
	}

	private supportFeedback(support: string | PatternAwareContinuation): MutablePatternFeedback | undefined {
		// Continuation identity keeps late feedback attached to the original bounded session sample.
		return typeof support === "string" ? this.patterns.get(support)?.feedback : this.recurrentFeedback.get(support);
	}

	snapshot(): ReadonlyArray<PatternAwarePattern> {
		return [...this.patterns.values()].map((pattern) =>
			readonlyPattern(pattern, this.clock, this.settings.decayHalfLifeEvents),
		);
	}

	recent(sessionID: string): ReadonlyArray<PatternAwareEvent> {
		return structuredClone(this.sessions.get(sessionID)?.history ?? []);
	}

	async flush(): Promise<void> {
		while (true) {
			if (this.persistTimer) {
				clearTimeout(this.persistTimer);
				this.persistTimer = undefined;
			}
			this.enqueuePersist();
			await this.write;
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
		const existing = session.recurrentActions.get(action.key);
		const durationMs =
			event.outcome === "success" && Number.isFinite(event.durationMs) ? Math.max(0, event.durationMs) : 0;
		if (existing) {
			const decay = recencyWeight(existing.lastSeenSequence, event.sequence, this.settings.decayHalfLifeEvents);
			existing.count = Math.min(Number.MAX_SAFE_INTEGER, existing.count + 1);
			existing.weightedCount = Math.min(Number.MAX_SAFE_INTEGER, existing.weightedCount * decay + 1);
			existing.weightedDurationMs = Math.min(Number.MAX_VALUE / 2, existing.weightedDurationMs * decay + durationMs);
			existing.lastSeenSequence = event.sequence;
		} else {
			session.recurrentActions.set(action.key, {
				action,
				input: structuredClone(event.input),
				count: 1,
				weightedCount: 1,
				weightedDurationMs: durationMs,
				lastSeenSequence: event.sequence,
			});
		}
	}

	/** Batch members share immutable context slices without retaining links to older windows. */
	private *learningContexts(history: ReadonlyArray<PatternAwareEvent>) {
		const batches = actionBatchStarts(history);
		const maxGap = Math.min(this.settings.maxFutureGap, Math.max(0, batches.length - 1));
		for (let gap = 0; gap <= maxGap; gap++) {
			const contextEnd = batches.length - gap;
			const end = batches[contextEnd] ?? history.length;
			const maxLength = Math.min(this.settings.maxContextLength, contextEnd);
			for (let length = 1; length <= maxLength; length++) {
				const start = batches[contextEnd - length]!;
				if (end - start > this.settings.maxContextLength) break;
				yield { context: history.slice(start, end), gap };
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
		return this.bindingAnalysis.applyWeightedBindings(bindings, sample.context).some(({ input }) => {
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
		const currentBindings = this.bindingAnalysis.inferBindings(context, target.input);
		if (firstRecurrenceProbe || hasBindingEvidence(currentBindings)) {
			remember(currentBindings);
		}
		remember(
			this.bindingAnalysis.inferBindingsFromSamples(
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
			const observed = {
				bindings,
				dependencies,
				gapCounts: {} as Record<string, number>,
				gapLastSeen: {} as Record<string, number>,
				occurrences: support.length,
				replayMatches: support.length,
				averageDurationMs: 0,
				lastSeenSequence: -Infinity,
			};
			for (const { gap, target } of support) {
				observed.gapCounts[gap] = (observed.gapCounts[gap] ?? 0) + 1;
				observed.gapLastSeen[gap] = Math.max(observed.gapLastSeen[gap] ?? 0, target.sequence);
				observed.lastSeenSequence = Math.max(observed.lastSeenSequence, target.sequence);
				observed.averageDurationMs += target.outcome === "success" ? Math.max(0, target.durationMs) : 0;
			}
			observed.averageDurationMs /= Math.max(1, support.length);
			const existing = this.patterns.get(id);
			if (existing) {
				Object.assign(existing, observed);
				continue;
			}
			this.patterns.set(id, {
				id,
				context: signatures,
				targetTool: target.tool,
				...observed,
				...(target.schemaHash ? { targetSchemaHash: target.schemaHash } : {}),
				historicalOpportunities: this.controlOpportunities(pool),
				historicalMatches: controlOpportunityCount(support),
				feedback: emptyPatternFeedback(observed.lastSeenSequence),
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

	private retirePoolPatterns(pool: PatternPool, retained: ReadonlySet<string>) {
		for (const patternID of pool.patternIDs ?? []) {
			if (retained.has(patternID)) continue;
			this.removePattern(patternID);
		}
		pool.patternIDs = [...retained];
	}

	private removePattern(patternID: string) {
		if (this.patterns.delete(patternID)) this.indexDirty = true;
		this.patternSupportSessions.delete(patternID);
		for (const session of this.sessions.values()) {
			if (session.pending.some(item => item.patternID === patternID))
				session.pending = session.pending.filter(item => item.patternID !== patternID);
		}
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
		session.pending = remaining;
	}

	private startPending(
		session: PatternSessionState<PatternAwareEvent>,
		history: ReadonlyArray<PatternAwareEvent>,
	) {
		const pending = [...session.pending];
		const triggerSequence = history.at(-1)?.sequence;
		if (triggerSequence === undefined) return;
		this.ensureIndex();
		for (const { pattern, context } of this.trie.matching(history)) {
			if (!structurallyEligible(pattern, this.settings)) continue;
			if (pending.some((item) => item.patternID === pattern.id && item.triggerSequence === triggerSequence))
				continue;
			pending.push({
				patternID: pattern.id,
				triggerSequence,
				expectedInputs: this.bindingAnalysis.applyWeightedBindings(pattern.bindings, context).map(({ input }) => input),
				remaining: groupGapTiming([pattern], this.settings, this.clock).latestHorizon,
			});
		}
		session.pending = pending.slice(-this.sessionBudgets.pendingValidationsPerSession);
	}

	private finishSessionState(session: PatternSessionState<PatternAwareEvent>) {
		for (const item of session.pending) this.recordValidation(item.patternID, false);
		session.pending = [];
	}

	private recordValidation(patternID: string, matched: boolean) {
		const pattern = this.patterns.get(patternID);
		if (!pattern) return;
		pattern.historicalOpportunities++;
		if (matched) pattern.historicalMatches++;
	}

	private trimSessionHistory(history: PatternAwareEvent[]) {
		const limit = this.settings.maxContextLength + this.settings.maxFutureGap + 1;
		const batches = actionBatchStarts(history);
		if (batches.length <= limit) return;
		history.splice(0, batches[batches.length - limit]!);
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
		for (const pattern of evicted) this.removePattern(pattern.id);
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
		this.write = this.write.catch(() => undefined).then(() => writeJsonFile(target, state));
		void this.write.catch(() => { this.dirty = true; });
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
	settings = { ...settings };
	actionSemantics = actionSemantics && { ...actionSemantics };
	const analyzerKey = patternAwareAnalyzerKey(settings);
	if (actionSemantics && (typeof actionSemantics.namespace !== "string" || !actionSemantics.namespace)) {
		throw new Error("Pattern action semantics require an explicit namespace");
	}
	const semanticsKey = actionSemantics ? JSON.stringify(actionSemantics.namespace) : "default";
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
	let released: Promise<void> | undefined;
	return {
		store,
		release: () => released ??= Promise.resolve().then(async () => {
			pooled.references = Math.max(0, pooled.references - 1);
			try {
				await store.flush();
			} finally {
				if (pooled.references === 0 && stores.get(poolKey) === pooled) stores.delete(poolKey);
			}
		}),
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

function configuredPersistenceFile(file: string, analyzerKey: string, semanticsKey: string): string {
	const parsed = path.parse(file);
	return path.join(parsed.dir, `${parsed.name}.${hash(`${semanticsKey}\0${analyzerKey}`).slice(0, 12)}${parsed.ext}`);
}

export function patternAwareSettings(value: unknown): PatternAwareSettings {
	return parsePatternSettings(asRecord(value));
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

/** Derived state belongs to one analyzer; public helpers get a fresh scope for mutable caller data. */
class PatternBindingAnalysis {
	private readonly derived = new WeakMap<object, BoundedRecencyMap<unknown, readonly [unknown]>>();

	private memo<Value>(owner: unknown, key: unknown, create: () => Value): Value {
		if (!isObject(owner)) return create();
		let cache = this.derived.get(owner);
		if (!cache) this.derived.set(owner, cache = new BoundedRecencyMap(128));
		const cached = cache.get(key);
		if (cached) return cached[0] as Value;
		const value = create();
		cache.set(key, [value]);
		return value;
	}

	private valueIndex(value: unknown) {
		return this.memo(value, "value-index", () => {
			const index = new Map<string, { leaves: PatternAwarePath[]; collections: CollectionLocation[] }>();
			for (const [path, item] of this.leaves(value)) {
				const key = stableStringify(item);
				let locations = index.get(key);
				if (!locations) index.set(key, locations = { leaves: [], collections: [] });
				locations.leaves.push(path);
				const collection = path.findIndex(segment => typeof segment === "number");
				if (collection >= 0) locations.collections.push({ path: path.slice(0, collection), itemPath: path.slice(collection + 1) });
			}
			return index;
		});
	}

	inferBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: Record<string, unknown>,
	): Record<string, PatternAwareBinding> {
		const bindings: Record<string, PatternAwareBinding> = {};
		for (const [targetPath, value] of this.leaves(target)) {
			const key = encodePath(targetPath);
			bindings[key] = this.candidateBindings(context, value, "first", isPathField(String(targetPath.at(-1) ?? "")))[0] ?? { type: "constant", value };
		}
		return bindings;
	}

	inferBindingsFromSamples(
		samples: ReadonlyArray<PatternSample>,
		constantSupport = 4,
		allowProjectedOmissions = false,
	): Record<string, PatternAwareBinding> | undefined {
		if (!samples.length) return;
		const bindings: Record<string, PatternAwareBinding> = {};
		const targetPaths = new Map<string, PatternAwarePath>();
		for (const sample of samples) for (const [targetPath] of this.leaves(sample.target.input)) {
			targetPaths.set(encodePath(targetPath), targetPath);
		}
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
				samples.flatMap((sample, index) => this.candidateBindings(sample.context, targets[index], "direct", targetIsPath)),
			);
			let selected = direct.find((candidate) =>
				samples.every((sample, index) => this.bindingMatches(candidate, sample.context, targets[index])),
			);
			if (!selected) {
				const candidates = uniqueBindings([
					...direct,
					...samples.flatMap((sample, index) => typeof targets[index] === "string"
						? this.candidateBindings(sample.context, targets[index], "all", targetIsPath) : []),
				]);
				const fallbackSources = direct.filter((binding) => binding.type === "event");
				if (fallbackSources.length > 1) candidates.push({ type: "coalesce", sources: fallbackSources });
				let selectedReplay = -1;
				for (const candidate of candidates) {
					const replay = samples.reduce(
						(matches, sample, index) => matches + Number(this.bindingMatches(candidate, sample.context, targets[index])),
						0,
					);
					if (replay <= selectedReplay) continue;
					selected = candidate;
					selectedReplay = replay;
					if (replay === samples.length) break;
				}
			}
			if (selected) selected = this.withObservedVariantCounts(selected, samples, targets);
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

	withObservedVariantCounts(
		binding: PatternAwareBinding,
		samples: ReadonlyArray<PatternSample>,
		targets: ReadonlyArray<unknown>,
	): PatternAwareBinding {
		const counts = new Map<number, number>();
		let width = 1;
		for (const [index, sample] of samples.entries()) {
			const values = this.bindingValues(binding, sample.context);
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

	applyWeightedBindings(
		bindings: Readonly<Record<string, PatternAwareBinding>>,
		context: ReadonlyArray<PatternAwareEvent>,
		limit = MAX_BINDING_VARIANTS,
	): ReadonlyArray<{
		readonly input: Record<string, unknown>;
		readonly probability: number;
	}> {
		let variants: Array<{ input: Record<string, unknown>; probability: number }> = [
			{ input: {}, probability: 1 },
		];
		for (const [encoded, binding] of Object.entries(bindings)) {
			const targetPath = decodePath(encoded);
			if (!targetPath.length || targetPath.some(unsafePathSegment)) return [];
			const values = this.weightedBindingValues(binding, context);
			if (!values.length) return [];
			if (values.length === 1) {
				for (const variant of variants) {
					variant.input = withPath(variant.input, targetPath, values[0]!.value);
					variant.probability *= values[0]!.probability;
				}
				continue;
			}
			const ranked = variants.flatMap((variant) => values.map((value) => ({
				variant, value: value.value, probability: variant.probability * value.probability,
			})));
			variants = ranked.sort((left, right) => right.probability - left.probability).slice(0, limit)
				.map(({ variant, value, probability }) => ({ input: withPath(variant.input, targetPath, value), probability }));
		}
		return variants;
	}

	weightedBindingValues(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>) {
		const values = this.bindingValues(binding, context);
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

	candidateBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: unknown,
		mode: "direct" | "all" | "first" = "all",
		targetIsPath = false,
	): readonly PatternAwareBinding[] {
		const key = "bindings:" + mode + Number(targetIsPath) + ":" + typeof target + ":" +
			(typeof target === "string" ? target : stableStringify(target));
		return this.memo(context, key, () => {
			const bindings = this.inferCandidateBindings(context, target, mode !== "direct", targetIsPath);
			if (mode === "first") for (const binding of bindings) return [binding];
			return uniqueBindings([...bindings]);
		});
	}

	*inferCandidateBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: unknown,
		includeComposites: boolean,
		targetIsPath: boolean,
	): Generator<PatternAwareBinding, undefined> {
		if (!includeComposites) { yield* this.indexedBindings(context, target, targetIsPath); return; }
		const pathSources: Array<{ readonly binding: PatternAwareBinding; readonly value: string }> = [];
		for (const [relativeEvent, field, value] of reverseContextFields(context)) {
			for (const [sourcePath, source] of this.leaves(value)) {
				const direct: PatternAwareBinding = { type: "event", relativeEvent, field, path: sourcePath };
				const pathSource = typeof source === "string" && isPathSource(field, sourcePath, source);
				if (sameValue(source, target) && (!targetIsPath || pathSource)) yield direct;
				if (typeof source !== "string" || typeof target !== "string") continue;
				const sources: Array<{ readonly binding: PatternAwareBinding; readonly value: string }> = [
					{ binding: direct, value: source },
				];
				if (pathSource) {
					if (pathSources.length < MAX_PATH_SOURCES) pathSources.push({ binding: direct, value: source });
					for (const operation of ["dirname", "basename", "normalize_path"] as const) {
						const transformed: PatternAwareBinding = { type: "transform", operation, source: direct };
						const value = transform(operation, source);
						if (value === target) yield transformed;
						if (operation !== "basename") sources.push({ binding: transformed, value });
						if (pathSources.length < MAX_PATH_SOURCES) pathSources.push({ binding: transformed, value });
					}
				}
				if (targetIsPath) continue;
				for (const { binding, value } of sources) {
					if (value.length < 3) continue;
					const offset = target.indexOf(value);
					if (offset < 0) continue;
					yield {
						type: "template",
						source: binding,
						prefix: target.slice(0, offset),
						suffix: target.slice(offset + value.length),
					};
				}
			}
			yield* collectionBindings(
				this.valueIndex(value).get(stableStringify(target))?.collections ?? [], relativeEvent, field, target, targetIsPath,
			);
		}
		if (targetIsPath && typeof target === "string") {
			const normalizedTarget = normalizePath(target);
			const joinMatches = new Map<string, Map<string, boolean>>();
			for (const left of pathSources) {
				for (const right of pathSources) {
					if (left === right) continue;
					const matchesByRight = joinMatches.get(left.value) ?? new Map<string, boolean>();
					joinMatches.set(left.value, matchesByRight);
					let matches = matchesByRight.get(right.value);
					if (matches === undefined) {
						matches = joinPath(left.value, right.value) === normalizedTarget;
						matchesByRight.set(right.value, matches);
					}
					if (!matches) continue;
					yield { type: "join", operation: "join_path", left: left.binding, right: right.binding };
				}
			}
		}
	}

	*indexedBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: unknown,
		targetIsPath: boolean,
	): Generator<PatternAwareBinding, undefined> {
		for (const [relativeEvent, field, value] of reverseContextFields(context)) {
			const locations = this.valueIndex(value).get(stableStringify(target));
			if (!locations) continue;
			for (const sourcePath of locations.leaves) {
				if (targetIsPath && typeof target === "string" && !isPathSource(field, sourcePath, target)) continue;
				yield { type: "event", relativeEvent, field, path: sourcePath };
			}
			yield* collectionBindings(locations.collections, relativeEvent, field, target, targetIsPath);
		}
	}

	bindingValues(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>): ReadonlyArray<unknown> {
		return this.memo(context, binding, () => this.evaluateBindingValues(binding, context));
	}

	evaluateBindingValues(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>): ReadonlyArray<unknown> {
		if (binding.type === "constant") return [binding.value];
		if (binding.type === "coalesce") {
			for (const source of binding.sources) {
				const values = this.bindingValues(source, context);
				if (values.length) return values;
			}
			return [];
		}
		let values: ReadonlyArray<unknown>;
		if (binding.type === "event" || binding.type === "each") {
			const index = context.length + binding.relativeEvent;
			const event = context[index];
			if (!event) return [];
			const collection = getPath(event[binding.field], binding.path);
			if (binding.type === "event") return collection === MISSING ? [] : [collection];
			values = Array.isArray(collection)
				? collection.map((item) => getPath(item, binding.itemPath)).filter((value) => value !== MISSING) : [];
		} else if (binding.type === "join") {
			const left = this.bindingValues(binding.left, context);
			const right = this.bindingValues(binding.right, context);
			values = left.flatMap((leftValue) =>
				right.flatMap((rightValue) =>
					typeof leftValue === "string" && typeof rightValue === "string" ? [joinPath(leftValue, rightValue)] : [],
				),
			);
		} else if (binding.type === "template" || binding.type === "transform") {
			values = this.bindingValues(binding.source, context).flatMap((value) =>
				typeof value === "string" ? [binding.type === "template"
					? `${binding.prefix}${value}${binding.suffix}` : transform(binding.operation, value)] : [],
			);
		} else return [];
		return uniqueBy(values, stableStringify);
	}

	bindingMatches(binding: PatternAwareBinding, context: ReadonlyArray<PatternAwareEvent>, target: unknown) {
		return this.bindingValues(binding, context).some((value) => sameValue(value, target));
	}

	*leaves(value: unknown, segments: PatternAwarePath = []): Generator<readonly [PatternAwarePath, unknown], undefined> {
		if (!isObject(value)) { yield [segments, value]; return; }
		const array = Array.isArray(value);
		const entries: Array<[string | number, unknown]> = array
			? value.map((item, index) => [index, item]) : Object.entries(value);
		if (!entries.length) yield [segments, this.memo(value, "empty-leaf", () => array ? [] : {})];
		for (const entry of entries) if (entry) yield* this.leaves(entry[1], [...segments, entry[0]]);
	}
}

export function inferBindings(
	context: ReadonlyArray<PatternAwareEvent>,
	target: Record<string, unknown>,
): Record<string, PatternAwareBinding> {
	return new PatternBindingAnalysis().inferBindings(context, target);
}

export function applyBindingsVariants(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	context: ReadonlyArray<PatternAwareEvent>,
	limit = MAX_BINDING_VARIANTS,
): ReadonlyArray<Record<string, unknown>> {
	return new PatternBindingAnalysis().applyWeightedBindings(bindings, context, limit).map(({ input }) => input);
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

export function applyBindings(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	context: ReadonlyArray<PatternAwareEvent>,
): Record<string, unknown> | undefined {
	return applyBindingsVariants(bindings, context, 1)[0];
}

function variantCount(counts: Readonly<Record<string, number>>, index: number): number {
	return nonNegativeFinite(counts[String(index)]);
}

function* reverseContextFields(
	context: ReadonlyArray<PatternAwareEvent>,
): Generator<readonly [number, PatternAwareDependencySource["field"], unknown]> {
	for (let index = context.length - 1; index >= 0; index--) {
		const event = context[index]!;
		const relativeEvent = index - context.length;
		yield [relativeEvent, "input", event.input];
		yield [relativeEvent, "output", event.output];
		yield [relativeEvent, "outputPaths", event.outputPaths];
	}
}

type CollectionLocation = { readonly path: PatternAwarePath; readonly itemPath: PatternAwarePath };

function* collectionBindings(
	items: ReadonlyArray<CollectionLocation>,
	relativeEvent: number,
	field: PatternAwareDependencySource["field"],
	target: unknown,
	targetIsPath: boolean,
): Generator<PatternAwareBinding, undefined> {
	for (const item of items) {
		if (targetIsPath && typeof target === "string" && !isPathSource(field, [...item.path, ...item.itemPath], target))
			continue;
		yield { type: "each", relativeEvent, field, path: item.path, itemPath: item.itemPath };
	}
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
		);
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
	const relative = relativeFilesystemPath(resourceRoot, value);
	return relative === undefined ? normalizePath(value) : normalizePath(relative || ".");
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

function uniqueBy<Value>(values: readonly Value[], keyFor: (value: Value) => string): Value[] {
	const seen = new Set<string>();
	return values.filter((item) => {
		const key = keyFor(item);
		if (seen.has(key)) return false;
		seen.add(key);
		return true;
	});
}

const bindingStructureKeys = new WeakMap<object, string>();

function bindingStructureKey(binding: PatternAwareBinding): string {
	let key = bindingStructureKeys.get(binding);
	if (key === undefined) {
		key = stableStringify(bindingStructure(binding));
		bindingStructureKeys.set(binding, key);
	}
	return key;
}

function uniqueBindings(bindings: ReadonlyArray<PatternAwareBinding>) {
	return uniqueBy(bindings, bindingStructureKey);
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
		const sources = uniqueBy(bindingSources(binding), stableStringify);
		return sources.length ? [{ targetPath: decodePath(encoded), sources }] : [];
	});
}

function bindingSources(binding: PatternAwareBinding): PatternAwareDependencySource[] {
	if (binding.type === "event" || binding.type === "each") {
		return [
			{
				relativeEvent: binding.relativeEvent,
				field: binding.field,
				path: binding.path,
				...(binding.type === "each" ? { itemPath: binding.itemPath } : {}),
			},
		];
	}
	if (binding.type === "constant") return [];
	if (binding.type === "coalesce") return binding.sources.flatMap(bindingSources);
	if (binding.type === "join") return [...bindingSources(binding.left), ...bindingSources(binding.right)];
	return bindingSources(binding.source);
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

function transform(operation: "dirname" | "basename" | "normalize_path", value: string) {
	if (operation === "dirname") return path.dirname(value);
	if (operation === "basename") return path.basename(value);
	return path.normalize(value).replaceAll("\\", "/");
}

const PATH_OPERATION_CACHE_LIMIT = 128;
const normalizedPaths = new BoundedRecencyMap<string, string>(PATH_OPERATION_CACHE_LIMIT);
const joinedPaths = new BoundedRecencyMap<string, string>(PATH_OPERATION_CACHE_LIMIT);

function cachePathResult(cache: BoundedRecencyMap<string, string>, key: string, value: string) {
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
		containsLogicalPath(root, output)
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
): Record<string, unknown> {
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

function groupGapTiming(patterns: ReadonlyArray<MutablePattern>, settings: PatternAwareSettings, clock: number) {
	const combined = new Map<number, number>();
	for (const pattern of patterns) {
		for (const [value, count] of Object.entries(pattern.gapCounts)) {
			const parsed = Number.parseInt(value, 10);
			const gap = Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
			const lastSeen = pattern.gapLastSeen[value] ?? pattern.lastSeenSequence;
			const weight = Math.max(0, count) * recencyWeight(lastSeen, clock, settings.decayHalfLifeEvents);
			if (!(weight > 0) || gap > settings.maxFutureGap) continue;
			combined.set(gap, (combined.get(gap) ?? 0) + weight);
		}
	}
	const gaps = [...combined.entries()].sort(([left], [right]) => left - right);
	const total = gaps.reduce((sum, [, weight]) => sum + weight, 0);
	const target = total * settings.futureGapCoverage;
	const latestHorizon = gaps.at(-1)?.[0] ?? 0;
	let horizon = latestHorizon, covered = 0;
	for (const [gap, weight] of gaps) {
		covered += weight;
		if (covered >= target) { horizon = gap; break; }
	}
	return {
		horizon,
		latestHorizon,
		gapCoverage: total <= 0 ? 0 : Math.max(0, Math.min(1, covered / total)),
	};
}

function ownBatch(inputs: ReadonlyArray<PatternAwareEventInput>, sessionID?: string) {
	inputs = structuredClone(inputs);
	sessionID ??= inputs[0]?.sessionID;
	if (inputs.some((input) => input.sessionID !== sessionID || input.turnID !== inputs[0]!.turnID)) {
		throw new Error("PatternAware batch actions must belong to one session and provider turn");
	}
	// Bindings use member positions: argument changes must not reorder distinct tools.
	return inputs
		.map((input) => ({ input, key: stableStringify([input.tool, input.outcome, input.operation ?? "", input.input]) }))
		.sort((left, right) => left.key < right.key ? -1 : left.key > right.key ? 1 : 0)
		.map((item) => item.input);
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

function actionBatchStarts(history: ReadonlyArray<PatternAwareEvent>) {
	const batches: number[] = [];
	let activeBatchID: string | undefined;
	for (const [index, event] of history.entries()) {
		if (!event.batchID || event.batchID !== activeBatchID) batches.push(index);
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

function backoffProbability(
	patterns: ReadonlyArray<MutablePattern>,
	clock: number,
	halfLife: number,
	contexts?: ReadonlyMap<number, ReadonlyMap<string, number>>,
) {
	const byLength = new Map<number, MutablePattern>();
	for (const pattern of patterns) {
		const current = byLength.get(pattern.context.length);
		if (!current || current.historicalOpportunities < pattern.historicalOpportunities)
			byLength.set(pattern.context.length, pattern);
	}
	let estimate = 0.5;
	const gaps = contexts ? [...new Set(patterns.flatMap((pattern) =>
		Object.entries(pattern.gapCounts).filter(([, count]) => count > 0).map(([gap]) => gap)))] : [];
	for (const order of [...new Set([...byLength.keys(), ...(contexts?.keys() ?? [])])].sort((left, right) => left - right)) {
		const pattern = byLength.get(order);
		if (!pattern) {
			// An unseen action must escape an observed longer context; require evidence for every supported gap.
			const evidence = gaps.length ? Math.min(...gaps.map((gap) => contexts?.get(order)?.get(gap) ?? 0)) : 0;
			if (evidence > 0) estimate /= evidence + 1;
			continue;
		}
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

function patternRank(pattern: MutablePattern, clock: number, halfLife: number) {
	const feedback = feedbackEvidence(pattern, clock, halfLife);
	return (
		(feedback.matched * 4 + pattern.replayMatches * 2 + probability(pattern) - feedback.mismatched) *
		recencyWeight(pattern.lastSeenSequence, clock, halfLife)
	);
}

function feedbackEvidence(pattern: Pick<MutablePattern, "feedback">, clock: number, halfLife: number) {
	const weight = recencyWeight(pattern.feedback.sequence, clock, halfLife);
	return {
		matched: pattern.feedback.recentMatchedWeight * weight,
		mismatched: pattern.feedback.recentMismatchedWeight * weight,
		adopted: pattern.feedback.recentAdoptedWeight * weight,
		rejected: pattern.feedback.recentRejectedWeight * weight,
	};
}

function patternAdoptionProbability(patterns: ReadonlyArray<Pick<MutablePattern, "feedback">>, clock: number, halfLife: number) {
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

function readonlyPattern(pattern: MutablePattern, clock: number, halfLife: number): PatternAwarePattern {
	return {
		...structuredClone(pattern),
		empiricalProbability: backoffProbability([pattern], clock, halfLife),
		adoptionProbability: patternAdoptionProbability([pattern], clock, halfLife),
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
	const safeBindings = bindings as Record<string, PatternAwareBinding>;
	return structuredClone({
		id: record.id,
		context: record.context as PatternAwareEventSignature[],
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
	});
}

function emptyPatternFeedback(sequence: number): MutablePatternFeedback {
	const counters = Object.fromEntries(PATTERN_FEEDBACK_COUNTERS.map((key) => [key, 0])) as PatternFeedbackCounters;
	return {
		...counters,
		rejectedAfterMatch: {},
		unobserved: {},
		sequence: Math.max(0, sequence),
	};
}

function mutablePatternFeedback(value: unknown): MutablePatternFeedback | undefined {
	const feedback = asRecord(value);
	const rejectedAfterMatch = numericRecord(feedback?.rejectedAfterMatch);
	const unobserved = numericRecord(feedback?.unobserved);
	const counters = numericRecord(Object.fromEntries(PATTERN_FEEDBACK_COUNTERS.map((key) => [key, feedback?.[key]]))) as PatternFeedbackCounters | undefined;
	if (!rejectedAfterMatch || !unobserved || !counters) return;
	return {
		...counters,
		rejectedAfterMatch: rejectedAfterMatch as Partial<Record<ResolutionStage, number>>,
		unobserved,
	};
}

function numericRecord(value: unknown): Record<string, number> | undefined {
	const record = asRecord(value);
	if (!record || Object.values(record).some((count) => !isFiniteNumber(count) || count < 0)) return;
	return record as Record<string, number>;
}

function mutablePools(eventsValue: ReadonlyArray<unknown>, pools: ReadonlyArray<unknown>): PatternPool[] {
	const events = eventsValue.map((item) =>
		isPersistedEvent(item) ? (structuredClone(item) as PatternAwareEvent) : undefined,
	);
	return pools.flatMap((value) => {
		const record = asRecord(value);
		if (!record || !Array.isArray(record.samples) || typeof record.key !== "string" ||
			typeof record.targetTool !== "string" || !Array.isArray(record.context) ||
			!record.context.every(isEventSignature) || !isNonNegativeInteger(record.gap)) return [];
		const samples = record.samples.flatMap((item) => {
			const sample = asRecord(item);
			if (
				!sample ||
				!Array.isArray(sample.context) ||
				!sample.context.every(isNonNegativeInteger) ||
				!isNonNegativeInteger(sample.target) ||
				sample.gap !== record.gap
			)
				return [];
			const context = sample.context.map((id) => events[id as number]);
			const target = events[sample.target as number];
			if (!target || context.some((event) => !event)) return [];
			return [{ context: context as PatternAwareEvent[], target, gap: record.gap as number }];
		});
		if (!samples.length) return [];
		const context = structuredClone(record.context) as PatternAwareEventSignature[];
		const targetSchemaHash = typeof record.targetSchemaHash === "string" ? record.targetSchemaHash : undefined;
		const patternIDs = Array.isArray(record.patternIDs) ? record.patternIDs.filter((item): item is string => typeof item === "string") : [];
		return [{
			key: patternPoolKey(context, record.targetTool, targetSchemaHash, record.gap),
			context,
			targetTool: record.targetTool,
			...(targetSchemaHash ? { targetSchemaHash } : {}),
			gap: record.gap,
			samples,
			...(patternIDs.length ? { patternIDs: [...new Set(patternIDs)] } : {}),
		}];
	});
}

function isNonNegativeInteger(value: unknown): value is number {
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

function patternPoolSampleLimit(settings: Pick<PatternAwareSettings, "minOccurrences" | "maxContextLength">) {
	return Math.max(settings.minOccurrences * 4, settings.maxContextLength * 4);
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

const parsedPaths = new BoundedRecencyMap<string, PatternAwarePath | null>(PATH_OPERATION_CACHE_LIMIT);

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
	return cryptoHash("sha256", value).slice(0, 32);
}

function isObject(value: unknown): value is object {
	return value !== null && typeof value === "object";
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
