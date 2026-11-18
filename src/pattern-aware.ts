import { hash as cryptoHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { ActionKey, ActionKeyProjector, ActionSemanticsRegistry } from "./action-semantics.ts";
import { actionKeyCovers, ownActionKeyProjector } from "./action-semantics.ts";
import {
	DEFAULT_SPECULATIVE_CONFIG,
	speculativeConfig,
	nonNegativeInteger,
	probability as probabilitySetting,
	settingsParser,
} from "./config-schema.ts";
import { writeJsonFile } from "./filesystem-evidence.ts";
import {
	asRecord,
	BoundedRecencyMap,
	clampProbability,
	finiteNumber,
	isFiniteNumber,
	isObject,
	isSafeCount,
	nonNegativeCount,
	relativeFilesystemPath,
	stableEqual as sameValue,
	stableStringify,
} from "./primitives.ts";
import type { PredictionSettlement, ResolutionStage } from "./settlement.ts";
import { predictionLearning } from "./settlement.ts";
export type PpmCountTrieRow = {
	readonly context: readonly string[];
	readonly counts: Readonly<Record<string, number>>;
	readonly lastSeen: number;
	/** Older snapshots use the context timestamp for every target. */
	readonly targetLastSeen?: Readonly<Record<string, number>>;
};
type TargetCount = {
	count: number;
	lastSeen: number;
};
type ContextCounts = {
	readonly context: readonly string[];
	readonly targets: Map<string, TargetCount>;
	lastSeen: number;
};
/** Bounded suffix evidence with interpolated PPM escape. Only populated contexts own state. */
export class PpmCountTrie {
	private readonly counts = new Map<string, ContextCounts>();
	private readonly order: number;
	constructor(maxOrder: number) {
		this.order = nonNegativeCount(maxOrder);
	}
	get size(): number {
		return this.counts.size;
	}
	observe(history: readonly string[], target: string, sequence = 0, halfLife = 0): void {
		if (!target) return;
		const lastSeen = nonNegativeCount(sequence);
		for (const context of this.suffixes(history)) {
			const current = this.context(context);
			// Advance all sufficient statistics before adding the new observation.
			if (halfLife > 0 && lastSeen > current.lastSeen) {
				for (const value of current.targets.values()) {
					value.count = decayedCount(value, lastSeen, halfLife);
					value.lastSeen = lastSeen;
				}
			}
			const previous = current.targets.get(target);
			this.put(
				current,
				target,
				Math.min(Number.MAX_SAFE_INTEGER, (previous?.count ?? 0) + 1),
				Math.max(previous?.lastSeen ?? 0, lastSeen),
			);
		}
	}
	/** Replace one exact context's evidence, without changing any suffix counts. */
	setCount(context: readonly string[], target: string, count: number, lastSeen = 0): void {
		if (target && context.length <= this.order && Number.isFinite(count) && count > 0)
			this.put(
				this.context(context),
				target,
				Math.min(Number.MAX_SAFE_INTEGER, count),
				nonNegativeCount(lastSeen),
			);
	}
	distribution(history: readonly string[], sequence = 0, halfLife = 0): ReadonlyMap<string, number> {
		const estimates = new Map<string, number>();
		if (!this.counts.has("[]")) return estimates;
		const suffixes = [...this.suffixes(history)].flatMap(
			(context) => this.counts.get(JSON.stringify(context)) ?? [],
		);
		let remaining = 1;
		for (const suffix of suffixes.reverse()) {
			const weighted = [...suffix.targets].map(
				([target, value]) => [target, decayedCount(value, sequence, halfLife)] as const,
			);
			const total = weighted.reduce((sum, [, count]) => sum + count, 0);
			const distinct = weighted.filter(([, count]) => count > 0).length;
			if (total <= 0 || distinct <= 0) continue;
			for (const [target, count] of weighted)
				if (count > 0)
					estimates.set(target, (estimates.get(target) ?? 0) + remaining * (count / (total + distinct)));
			remaining *= distinct / (total + distinct);
		}
		for (const [target, probability] of estimates) estimates.set(target, clampProbability(probability));
		return estimates;
	}
	snapshot(maxContexts = Number.POSITIVE_INFINITY): readonly PpmCountTrieRow[] {
		const limit = Number.isFinite(maxContexts)
			? Math.max(1, Math.floor(maxContexts))
			: Number.POSITIVE_INFINITY;
		return this.ranked()
			.slice(0, limit)
			.map(([, row]) => ({
				context: [...row.context],
				counts: Object.fromEntries(
					[...row.targets]
						.sort(([left], [right]) => left.localeCompare(right))
						.map(([target, value]) => [target, value.count]),
				),
				lastSeen: row.lastSeen,
				...([...row.targets.values()].some((value) => value.lastSeen !== row.lastSeen)
					? {
							targetLastSeen: Object.fromEntries(
								[...row.targets].map(([target, value]) => [target, value.lastSeen]),
							),
						}
					: {}),
			}));
	}
	restore(rows: readonly unknown[]): void {
		this.counts.clear();
		for (const value of rows) {
			const row = value as Partial<PpmCountTrieRow> | null;
			if (
				!row ||
				!Array.isArray(row.context) ||
				!row.context.every((token) => typeof token === "string") ||
				!row.counts ||
				typeof row.counts !== "object" ||
				Array.isArray(row.counts)
			)
				continue;
			for (const [target, count] of Object.entries(row.counts))
				this.setCount(
					row.context,
					target,
					count,
					typeof row.targetLastSeen?.[target] === "number"
						? row.targetLastSeen[target]
						: typeof row.lastSeen === "number"
							? row.lastSeen
							: 0,
				);
		}
	}
	trim(maxContexts: number): void {
		const limit = Math.max(1, Math.floor(maxContexts));
		if (this.size <= limit) return;
		if (!Number.isFinite(limit)) {
			this.restore(this.snapshot(limit));
			return;
		}
		for (const [key] of this.ranked().slice(limit)) this.counts.delete(key);
	}
	private *suffixes(history: readonly string[]) {
		for (let length = 0; length <= Math.min(history.length, this.order); length++)
			yield history.slice(history.length - length);
	}
	private context(context: readonly string[]): ContextCounts {
		const key = JSON.stringify(context);
		let current = this.counts.get(key);
		if (!current)
			this.counts.set(key, (current = { context: [...context], targets: new Map(), lastSeen: 0 }));
		return current;
	}
	private put(current: ContextCounts, target: string, count: number, lastSeen: number): void {
		current.targets.set(target, { count, lastSeen });
		current.lastSeen = Math.max(current.lastSeen, lastSeen);
	}
	private ranked() {
		return [...this.counts]
			.map(
				([key, row]) =>
					[key, row, [...row.targets.values()].reduce((sum, value) => sum + value.count, 0)] as const,
			)
			.sort(
				([leftKey, left, leftTotal], [rightKey, right, rightTotal]) =>
					Number(right.context.length === 0) - Number(left.context.length === 0) ||
					rightTotal - leftTotal ||
					right.context.length - left.context.length ||
					right.lastSeen - left.lastSeen ||
					leftKey.localeCompare(rightKey),
			);
	}
}
function decayedCount(value: TargetCount, sequence: number, halfLife: number): number {
	return !Number.isFinite(sequence) || !Number.isFinite(halfLife) || halfLife <= 0
		? value.count
		: value.count * 2 ** (-Math.max(0, sequence - value.lastSeen) / halfLife);
}
export type PatternAwarePresetID =
	| "reported-files"
	| "edited-file"
	| "recent-reads"
	| "reported-lines"
	| "companion-files"
	| "failure-edits"
	| "continue-read"
	| "retry-failed-command"
	| "recheck-search"
	| "result-neighbors";
/** Retained public configuration shape; built-in prediction presets have been removed. */
export const PATTERN_AWARE_PRESETS: readonly Readonly<{
	id: PatternAwarePresetID;
	label: string;
	description: string;
	defaultOff: boolean;
}>[] = Object.freeze([]);
export type PatternAwareSettings = Readonly<typeof patternAwareDefaults>;
function presetSelection(
	value: unknown,
	fallback: readonly PatternAwarePresetID[],
): readonly PatternAwarePresetID[] {
	return Object.freeze(
		Array.isArray(value)
			? PATTERN_AWARE_PRESETS.filter((preset) => value.includes(preset.id)).map((preset) => preset.id)
			: [...fallback],
	);
}
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
	/** Typed `path:line` facts a result reports (grep matches, compiler and test locations). */
	readonly outputLocations?: ReadonlyArray<OutputLocation>;
	/** A failure's exit code and error kind (`exit:1 TypeError`): a guard telling apart what follows different failures. */
	readonly errorClass?: string;
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
	/** Whether a workspace file exists now: a read prediction of a missing one never runs. Omitted, every read stays a candidate. */
	readonly exists?: (target: string) => boolean;
};
export function patternAwareActionSemantics(
	registry: ActionSemanticsRegistry,
	cwd: string,
	projectors: readonly ActionKeyProjector[] = [],
	exists?: (target: string) => boolean,
): PatternAwareActionSemantics {
	cwd = path.resolve(cwd);
	const rules = Object.freeze(projectors.map(ownActionKeyProjector));
	return Object.freeze({
		namespace: stableStringify([
			cwd,
			[...registry.toolNames()].sort().map((tool) => [tool, registry.definition(tool)!.epoch]),
			rules.map((rule) => rule.id).sort(),
		]),
		actionKey: (tool: string, input: Readonly<Record<string, unknown>>, schemaHash?: string) =>
			registry.buildKey(tool, input, cwd, schemaHash),
		projectors: rules,
		...(exists ? { exists } : {}),
	});
}
export type PatternAwareEvent = PatternAwareEventInput & {
	readonly sequence: number;
	readonly batchID?: string;
	readonly batchIndex?: number;
	readonly batchSize?: number;
};
export type PatternAwarePath = ReadonlyArray<string | number>;
export type OutputLocation = {
	readonly path: string;
	readonly line: number;
};
export type PatternAwareDependencySource = {
	readonly relativeEvent: number;
	readonly field: "input" | "output" | "outputPaths" | "outputLocations";
	readonly path: PatternAwarePath;
	readonly itemPath?: PatternAwarePath;
};
export type PatternAwareDependency = {
	readonly targetPath: PatternAwarePath;
	readonly sources: ReadonlyArray<PatternAwareDependencySource>;
};
/** Runtime bindings are event, each or constant; legacy variants retain their public type shape. */
export type PatternAwareBinding = (
	| (Omit<PatternAwareDependencySource, "itemPath"> & {
			readonly type: "event";
	  })
	| (Required<PatternAwareDependencySource> & {
			readonly type: "each";
	  })
	| {
			readonly type: "constant";
			readonly value: unknown;
	  }
	| {
			readonly type: "transform";
			readonly operation: PathTransform;
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
	/** Two to four values spliced into literal text in order, `parts[0] + first + parts[1] + second + ...` (`pytest <file>::<test>`,
	 * a command whose several arguments each come from earlier events). */
	| {
			readonly type: "splice";
			readonly sources: readonly PatternAwareBinding[];
			readonly parts: readonly string[];
	  }
	/** A path with its leading directories `from` replaced by `to`, the rest kept: lib/a/b.js → test/unit/a/b.js. */
	| {
			readonly type: "rebase";
			readonly source: PatternAwareBinding;
			readonly from: string;
			readonly to: string;
	  }
	/** A line the source reports, matching a target within `tolerance` lines: a read's window around a reported location. */
	| {
			readonly type: "near";
			readonly source: PatternAwareBinding;
			readonly tolerance: number;
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
type PathTransform = "dirname" | "basename" | "normalize_path" | "stem" | "strip_extension";
export type PatternAwarePattern = Readonly<
	Omit<MutablePattern, "context" | "bindings" | "dependencies" | "gapCounts" | "gapLastSeen" | "feedback">
> & {
	readonly context: ReadonlyArray<PatternAwareEventSignature>;
	readonly bindings: Readonly<Record<string, PatternAwareBinding>>;
	readonly dependencies?: ReadonlyArray<PatternAwareDependency>;
	readonly gapCounts: Readonly<Record<string, number>>;
	readonly gapLastSeen: Readonly<Record<string, number>>;
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
	/** Selected built-in relation, when this candidate has no learned support yet. */
	readonly presetID?: PatternAwarePresetID;
	/** Canonical action identity; plan support adds the parent path while K(a) remains the execution identity. */
	readonly actionIdentity: string;
	readonly supportingPatternIDs: ReadonlyArray<string>;
	readonly horizon: number;
	readonly latestHorizon: number;
	readonly empiricalProbability: number;
	readonly conditionalProbability: number;
	readonly adoptionProbability: number;
	readonly confidence: number;
	readonly background?: boolean;
	readonly dependencies: ReadonlyArray<PatternAwareDependency>;
	readonly continuation: PatternAwareContinuation;
	readonly depth: number;
};
export type PatternAwareContinuation = {
	readonly history: ReadonlyArray<PatternAwareEvent>;
	readonly visitedPatternIDs: ReadonlyArray<string>;
	readonly pathProbability: number;
};
export type PatternAwareObservation = Pick<
	PatternAwareEventInput,
	"output" | "outputPaths" | "outputLocations"
>;
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
	historicalOpportunities: number;
	historicalMatches: number;
	feedback: MutablePatternFeedback;
	lastSeenSequence: number;
};
const PATTERN_FEEDBACK_COUNTERS = [
	"issued",
	"observed",
	"matched",
	"adopted",
	"recentMatchedWeight",
	"recentMismatchedWeight",
	"recentAdoptedWeight",
	"recentRejectedWeight",
	"sequence",
] as const;
type PatternFeedbackCounters = Record<(typeof PATTERN_FEEDBACK_COUNTERS)[number], number>;
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
	readonly patterns: ReadonlyArray<PatternAwarePattern>;
	readonly events: ReadonlyArray<PatternAwareEvent>;
	readonly pools: ReadonlyArray<unknown>;
	readonly sequenceCounts: ReadonlyArray<PpmCountTrieRow>;
};
type PatternSample = {
	readonly context: ReadonlyArray<PatternAwareEvent>;
	readonly target: PatternAwareEvent;
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
const { defaults: patternAwareDefaults, parse: parsePatternSettings } = settingsParser(
	DEFAULT_SPECULATIVE_CONFIG.pattern,
	{ presets: presetSelection, maxFutureGap: nonNegativeInteger, futureGapCoverage: probabilitySetting },
);
export const PATTERN_AWARE_DEFAULTS: PatternAwareSettings = patternAwareDefaults;

export class PatternAwareStore {
	private readonly patterns = new Map<string, MutablePattern>();
	private readonly bindingAnalysis = new PatternBindingAnalysis();
	private readonly pools = new Map<string, PatternPool>();

	private readonly sessions: BoundedRecencyMap<string, { history: PatternAwareEvent[] }>;

	private readonly patternSupportSessions = new Map<string, ReadonlySet<string>>();
	private contexts?: Map<string, MutablePattern[]>;
	private sequenceModel: PpmCountTrie;
	private clock = 0;
	private write?: Promise<void>;
	private dirty = false;
	private persistTimer?: ReturnType<typeof setTimeout>;
	private loaded = false;
	private readonly settings: PatternAwareSettings;
	private readonly persistenceFile?: string;
	/** Shared by every workspace: validated patterns with no workspace path in them, learned in one and usable in all. */
	private readonly portableFile?: string;
	private readonly actionSemantics?: PatternAwareActionSemantics;
	private readonly reservedRoot: boolean;
	constructor(
		settings: PatternAwareSettings,
		persistenceFile?: string,
		actionSemantics?: PatternAwareActionSemantics,
		portableFile?: string,
		reservedRoot = false,
	) {
		settings = {
			...settings,
			presets: presetSelection(settings.presets, patternAwareDefaults.presets),
		};
		this.settings = settings;
		this.sessions = new BoundedRecencyMap(
			Math.min(settings.maxPatterns, speculativeConfig().prediction.sessions),
		);
		this.sequenceModel = new PpmCountTrie(settings.maxContextLength);
		this.persistenceFile = persistenceFile;
		this.portableFile = portableFile;
		this.reservedRoot = reservedRoot;
		this.actionSemantics = actionSemantics && {
			...actionSemantics,
			projectors: actionSemantics.projectors?.map(ownActionKeyProjector),
		};
	}
	async load() {
		if (this.loaded) return;
		this.loaded = true;
		await this.loadWorkspace();
		// Patterns another workspace validated start here as fresh, keeping their evidence; local feedback moves them from there.
		const portable = this.portableFile ? await readPatterns(this.portableFile) : [];
		for (const pattern of portable)
			if (
				!this.patterns.has(pattern.id) &&
				this.portable(pattern) &&
				pattern.context.length <= this.settings.maxContextLength
			)
				this.patterns.set(pattern.id, {
					...pattern,
					lastSeenSequence: this.clock,
				});
		if (portable.length) {
			this.contexts = undefined;
			this.trimPatterns();
		}
	}
	private async loadWorkspace() {
		if (!this.persistenceFile) return;
		const parsed = await fs
			.readFile(this.persistenceFile, "utf8")
			.then((value) => JSON.parse(value) as PersistedState)
			.catch(() => undefined);
		if (
			!parsed ||
			Object.keys(parsed).length !== 4 ||
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
			for (const patternID of pool.patternIDs ?? []) {
				const pattern = this.patterns.get(patternID);
				if (pattern?.dependencies.length === 0) {
					this.patternSupportSessions.set(
						patternID,
						new Set(this.supportingSamples(pattern.bindings, pool).map((sample) => sample.target.sessionID)),
					);
				}
			}
			for (const sample of pool.samples) {
				this.clock = Math.max(
					this.clock,
					sample.target.sequence,
					...sample.context.map((event) => event.sequence),
				);
			}
		}
		this.sequenceModel.restore(parsed.sequenceCounts);
		this.sequenceModel.trim(this.settings.maxPatterns);
		this.contexts = undefined;
		this.trimPools();
		this.trimPatterns();
	}
	observe(input: PatternAwareEventInput) {
		this.observeEvents(structuredClone([input]));
	}
	observeBatch(inputs: ReadonlyArray<PatternAwareEventInput>) {
		const batch = ownBatch(inputs);
		this.observeEvents(batch, batch[0]?.turnID);
	}
	private observeEvents(inputs: ReadonlyArray<PatternAwareEventInput>, batchID?: string) {
		const first = inputs[0];
		if (!first) return;
		const events = inputs.map((input, index): PatternAwareEvent => ({
			...boundedEventPayload(input),
			sequence: ++this.clock,
			...(batchID ? { batchID, batchIndex: index, batchSize: inputs.length } : {}),
		}));
		let session = this.sessions.get(first.sessionID);
		if (!session) {
			session = {
				history: [],
			};
			this.sessions.set(first.sessionID, session);
		}
		const history = session.history;
		const learningTargets = events.filter((event) => event.learnTarget !== false);
		const contexts = learningTargets.length ? [...this.learningContexts(history)] : [];
		const contextTokens = learningTargets.length
			? history.map((item) => signatureToken(signature(item)))
			: [];
		for (const event of learningTargets) {
			this.sequenceModel.observe(
				contextTokens,
				event.tool,
				event.sequence,
				this.settings.decayHalfLifeEvents,
			);
			for (const { context, gap } of contexts) this.learnOccurrence(context, event, gap);
		}
		history.push(...events);
		this.trimSessionHistory(history);
		this.trimPools();
		this.trimPatterns();
		this.sequenceModel.trim(this.settings.maxPatterns);
		this.persist();
	}
	observeTurn() {
		this.clock++;
	}
	finishSession(sessionID: string) {
		const session = this.sessions.get(sessionID);
		if (session) {
			this.sessions.delete(sessionID);
		}
		this.persist();
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
			{ history, visitedPatternIDs: [], pathProbability: 1 },
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
		const ordered = ownBatch(inputs, sessionID);
		const turnID = ordered[0]!.turnID;
		const history = [...(this.sessions.get(sessionID)?.history ?? [])];
		const sequence = history.at(-1)?.sequence ?? this.clock;
		history.push(
			...ordered.map((input, index): PatternAwareEvent => ({
				...input,
				sequence: sequence + index + 1,
				batchID: turnID,
				batchIndex: index,
				batchSize: ordered.length,
				learnTarget: false,
			})),
		);
		this.trimSessionHistory(history);
		return this.predictHistory(
			history,
			schemaHashes,
			{ history, visitedPatternIDs: [], pathProbability: 1, ...seed },
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
		if (
			!predictionSettings.enabled ||
			continuation.visitedPatternIDs.length >= predictionSettings.maxPredictionDepth
		)
			return [];
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
		);
	}
	private predictHistory(
		history: ReadonlyArray<PatternAwareEvent>,
		schemaHashes: Readonly<Record<string, string>>,
		continuation: PatternAwareContinuation,
		settings: PatternAwareSettings,
	): PatternAwareCandidate[] {
		if (continuation.visitedPatternIDs.length >= settings.maxPredictionDepth) return [];
		const sessionID = history.at(-1)?.sessionID;
		const ppm = this.sequenceModel.distribution(
			history.map((event) => signatureToken(signature(event))),
			this.clock,
			settings.decayHalfLifeEvents,
		);
		const predictions = new Map<string, PatternAwareCandidate>();
		for (const { pattern, context } of this.matchingPatterns(history)) {
			if (continuation.visitedPatternIDs.includes(pattern.id) || !structurallyEligible(pattern, settings))
				continue;
			const sessions = this.patternSupportSessions.get(pattern.id);
			if (sessionID && sessions && !sessions.has(sessionID) && sessions.size < settings.minOccurrences)
				continue;
			if (pattern.targetSchemaHash && schemaHashes[pattern.targetTool] !== pattern.targetSchemaHash) continue;
			const { horizon, latestHorizon } = groupGapTiming([pattern], settings, this.clock);
			if (horizon && !settings.multiStepEnabled) continue;
			const feedback = feedbackEvidence(pattern, this.clock, settings.decayHalfLifeEvents);
			const precision =
				(pattern.historicalMatches + feedback.matched) /
				Math.max(1, pattern.historicalOpportunities + feedback.matched + feedback.mismatched);
			const adoptionProbability = patternAdoptionProbability(
				[pattern],
				this.clock,
				settings.decayHalfLifeEvents,
			);
			for (const applied of this.bindingAnalysis.applyWeightedBindings(pattern.bindings, context)) {
				const action = this.resolveActionKey(
					pattern.targetTool,
					applied.input,
					pattern.targetSchemaHash ?? schemaHashes[pattern.targetTool],
				);
				const actionIdentity = hash(
					stableStringify(
						action ? { actionKey: action.key } : { tool: pattern.targetTool, input: applied.input },
					),
				);
				const conditionalProbability = clampProbability(
					(ppm.get(pattern.targetTool) ?? 0) * precision * applied.probability,
				);
				const empiricalProbability = continuation.pathProbability * conditionalProbability;
				const confidence = empiricalProbability * adoptionProbability;
				const candidate: PatternAwareCandidate = {
					type: "tool_call",
					source: "pattern_aware",
					tool: pattern.targetTool,
					input: structuredClone(applied.input),
					patternID: pattern.id,
					actionIdentity,
					supportingPatternIDs: [pattern.id],
					horizon,
					latestHorizon,
					conditionalProbability,
					empiricalProbability,
					adoptionProbability,
					confidence,
					...(pattern.occurrences < settings.minOccurrences || feedback.mismatched > feedback.matched
						? { background: true }
						: {}),
					dependencies: structuredClone(pattern.dependencies),
					continuation: {
						history: structuredClone(history),
						visitedPatternIDs: [...continuation.visitedPatternIDs, pattern.id],
						pathProbability: empiricalProbability,
					},
					depth: continuation.visitedPatternIDs.length + 1,
				};
				const previous = predictions.get(actionIdentity);
				if (!previous) predictions.set(actionIdentity, candidate);
				else {
					const preferred = confidence > previous.confidence ? candidate : previous;
					predictions.set(actionIdentity, {
						...preferred,
						supportingPatternIDs: [...new Set([...previous.supportingPatternIDs, pattern.id])],
					});
				}
			}
		}
		const counts = new Map<string, number>();
		return [...predictions.values()]
			.sort(
				(left, right) =>
					Number(left.background ?? false) - Number(right.background ?? false) ||
					right.confidence - left.confidence ||
					left.horizon - right.horizon ||
					left.actionIdentity.localeCompare(right.actionIdentity),
			)
			.filter((candidate) => {
				const count = counts.get(candidate.tool) ?? 0;
				counts.set(candidate.tool, count + 1);
				return count < settings.beamWidth;
			});
	}
	/** Use a persisted pattern ID or the original candidate continuation for session backoff feedback. */
	issued(support: string | PatternAwareContinuation) {
		const feedback = this.supportFeedback(support);
		if (!feedback) return;
		feedback.issued++;
		if (typeof support === "string") this.persist();
	}
	settled(support: string | PatternAwareContinuation, settlement: PredictionSettlement, served?: boolean) {
		const feedback = this.supportFeedback(support);
		if (!feedback) return;
		const recent = feedbackEvidence({ feedback }, this.clock, this.settings.decayHalfLifeEvents);
		const { matched, adopted } = predictionLearning(settlement, served);
		feedback.recentMatchedWeight = recent.matched + Number(matched === true);
		feedback.recentMismatchedWeight = recent.mismatched + Number(matched === false);
		feedback.recentAdoptedWeight = recent.adopted + Number(adopted === true);
		feedback.recentRejectedWeight = recent.rejected + Number(adopted === false);
		feedback.sequence = this.clock;
		feedback.adopted += Number(adopted === true);
		feedback.matched += Number(matched === true);
		feedback.observed += Number(settlement.observation === "observed");
		if (settlement.observation === "unobserved") {
			const key = `${settlement.cause.stage}:${settlement.cause.code}`;
			feedback.unobserved[key] = (feedback.unobserved[key] ?? 0) + 1;
		} else if (settlement.match.matched && !adopted && settlement.match.adoption.status === "rejected") {
			const stage = settlement.match.adoption.cause.stage;
			feedback.rejectedAfterMatch[stage] = (feedback.rejectedAfterMatch[stage] ?? 0) + 1;
		}
		if (typeof support === "string") this.persist();
	}
	private supportFeedback(support: string | PatternAwareContinuation): MutablePatternFeedback | undefined {
		const id = typeof support === "string" ? support : support.visitedPatternIDs.at(-1);
		return id ? this.patterns.get(id)?.feedback : undefined;
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
		const target = this.persistenceFile;
		if (!target || !this.loaded) return;
		while (true) {
			if (this.persistTimer) {
				clearTimeout(this.persistTimer);
				this.persistTimer = undefined;
			}
			if (!this.write && !this.dirty) return;
			this.write ??= Promise.resolve()
				.then(async () => {
					this.dirty = false;
					// Learned events quote tool inputs and outputs: keep them owner-readable only.
					await writeJsonFile(
						target,
						{
							patterns: this.snapshot(),
							...this.persistedLearningState(),
							sequenceCounts: this.sequenceModel.snapshot(this.settings.maxPatterns),
						} satisfies PersistedState,
						undefined,
						0o600,
						this.reservedRoot,
					);
					if (this.portableFile) await this.sharePortable(this.portableFile);
				})
				.catch((error) => {
					this.dirty = true;
					throw error;
				})
				.finally(() => {
					this.write = undefined;
				});
			await this.write;
		}
	}
	/** Index only populated contexts; prediction and learning use the same suffix and wildcard rules. */
	private *matchingPatterns(history: readonly PatternAwareEvent[]) {
		if (!this.contexts) {
			this.contexts = new Map();
			for (const pattern of this.patterns.values()) {
				const key = JSON.stringify(pattern.context.map(trieToken));
				const group = this.contexts.get(key) ?? [];
				group.push(pattern);
				this.contexts.set(key, group);
			}
		}
		const signatures = history.map(signature);
		const tokens = signatures.map(trieToken);
		const starts = this.contextStarts(history);
		for (let index = history.length - 1; index >= 0; index--) {
			if (starts?.has(index) === false) continue;
			const patterns = this.contexts.get(JSON.stringify(tokens.slice(index)));
			if (!patterns) continue;
			const context = history.slice(index);
			for (const pattern of patterns)
				if (
					pattern.context.every(
						(expected, offset) =>
							expected.outputShape === undefined ||
							signatures[index + offset]!.outputShape === undefined ||
							signatures[index + offset]!.outputShape === expected.outputShape,
					)
				)
					yield { pattern, context };
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
				if (end - start <= this.settings.maxContextLength) yield { context: history.slice(start, end), gap };
				else if (length === 1)
					yield {
						context: history.slice(end - this.settings.maxContextLength, end),
						gap,
					};
				if (end - start >= this.settings.maxContextLength) break;
			}
		}
	}
	/** A final batch wider than a context matches only from its newest calls, as learning truncates it; a narrower one keeps suffix matching. */
	private contextStarts(history: ReadonlyArray<PatternAwareEvent>): ReadonlySet<number> | undefined {
		const width = history.length - (actionBatchStarts(history).at(-1) ?? 0);
		return width > this.settings.maxContextLength
			? new Set([history.length - this.settings.maxContextLength])
			: undefined;
	}
	private resolveActionKey(
		tool: string,
		input: Readonly<Record<string, unknown>>,
		schemaHash: string | undefined,
	) {
		try {
			return this.actionSemantics?.actionKey(tool, input, schemaHash);
		} catch {
			return undefined;
		}
	}
	private supportingSamples(
		bindings: Readonly<Record<string, PatternAwareBinding>>,
		{ targetTool, targetSchemaHash, samples }: PatternPool,
	) {
		return samples.filter((sample) => {
			if (targetTool !== sample.target.tool) return false;
			const actor = this.resolveActionKey(
				targetTool,
				sample.target.input,
				sample.target.schemaHash ?? targetSchemaHash,
			);
			return this.bindingAnalysis.applyWeightedBindings(bindings, sample.context).some(({ input }) => {
				const speculative = this.resolveActionKey(targetTool, input, targetSchemaHash);
				if (!speculative || !actor) return sameValue(input, sample.target.input);
				return actionKeyCovers(speculative, actor, this.actionSemantics?.projectors ?? []);
			});
		});
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
		const sample = { context, target };
		pool.samples.push(sample);
		const sampleLimit = patternPoolSampleLimit(this.settings);
		if (pool.samples.length > sampleLimit) pool.samples.splice(0, pool.samples.length - sampleLimit);
		this.pools.set(poolKey, pool);
		if (pool.samples.length < this.settings.minOccurrences) return;
		const constantSupport = Math.max(
			speculativeConfig().prediction.bindingConstantMinimum,
			this.settings.minOccurrences * speculativeConfig().prediction.bindingSupportMultiplier,
		);
		const candidates = new Map<string, Record<string, PatternAwareBinding>>();
		const remember = (bindings: Record<string, PatternAwareBinding> | undefined) => {
			if (!bindings || !hasSufficientBindingProvenance(bindings, pool.samples, constantSupport)) return;
			candidates.set(stableStringify(bindings), bindings);
		};
		for (const patternID of pool.patternIDs ?? []) remember(this.patterns.get(patternID)?.bindings);
		remember(this.bindingAnalysis.inferBindings(context, target.input));
		remember(
			this.bindingAnalysis.inferBindingsFromSamples(
				pool.samples,
				constantSupport,
				this.actionSemantics !== undefined,
			),
		);
		const retained = new Set<string>();
		for (const candidate of candidates.values()) {
			const bindings = candidate;
			const support = this.supportingSamples(bindings, pool);
			if (
				support.length < this.settings.minOccurrences ||
				!hasSufficientBindingProvenance(bindings, support, constantSupport)
			)
				continue;
			const id = hash(
				stableStringify({
					context: signatures,
					targetTool: target.tool,
					bindings: bindings,
					targetSchemaHash: target.schemaHash,
					gap,
				}),
			);
			if (retained.has(id)) continue;
			retained.add(id);
			const { dependencies } = analyzeBindings(bindings);
			if (dependencies.length === 0) {
				this.patternSupportSessions.set(id, new Set(support.map((sample) => sample.target.sessionID)));
			} else this.patternSupportSessions.delete(id);
			const lastSeenSequence = support.reduce(
				(latest, { target }) => Math.max(latest, target.sequence),
				-Infinity,
			);
			const observed = {
				bindings,
				dependencies,
				gapCounts: { [gap]: support.length },
				gapLastSeen: { [gap]: lastSeenSequence },
				occurrences: support.length,
				lastSeenSequence,
				historicalOpportunities: pool.samples.length,
				historicalMatches: support.length,
			};
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
				feedback: emptyPatternFeedback(observed.lastSeenSequence),
			});
			this.contexts = undefined;
		}
		this.retirePoolPatterns(pool, retained);
	}

	private retirePoolPatterns(pool: PatternPool, retained: ReadonlySet<string>) {
		for (const patternID of pool.patternIDs ?? []) {
			if (retained.has(patternID)) continue;
			this.removePattern(patternID);
		}
		pool.patternIDs = [...retained];
	}
	private removePattern(patternID: string) {
		if (this.patterns.delete(patternID)) this.contexts = undefined;
		this.patternSupportSessions.delete(patternID);
	}
	private trimSessionHistory(history: PatternAwareEvent[]) {
		const limit = this.settings.maxContextLength + this.settings.maxFutureGap + 1;
		const batches = actionBatchStarts(history);
		if (batches.length <= limit) return;
		history.splice(0, batches[batches.length - limit]!);
	}
	private trimPools() {
		if (this.pools.size <= Math.max(1, this.settings.maxPatterns)) return;
		for (const pool of this.rankedPools().slice(Math.max(1, this.settings.maxPatterns))) {
			this.pools.delete(pool.key);
			this.retirePoolPatterns(pool, new Set()); // No pattern outlives the samples that support it.
		}
	}
	/** Memory keeps exactly the pools the file does: recency-weighted support, so a new relation can displace stale ones. */
	private rankedPools(): PatternPool[] {
		const last = (pool: PatternPool) => pool.samples.at(-1)?.target.sequence ?? Number.NEGATIVE_INFINITY;
		return [...this.pools.values()]
			.map(
				(pool) =>
					[
						pool,
						pool.samples.length * recencyWeight(last(pool), this.clock, this.settings.decayHalfLifeEvents),
					] as const,
			)
			.sort(([left, leftScore], [right, rightScore]) => rightScore - leftScore || last(right) - last(left))
			.map(([pool]) => pool);
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
	/** Merge this workspace's portable patterns into the shared file (this workspace's copy wins) and keep the strongest. */
	private async sharePortable(file: string) {
		const merged = new Map(
			(await readPatterns(file))
				.filter((pattern) => this.portable(pattern))
				.map((pattern) => [pattern.id, pattern]),
		);
		for (const pattern of this.patterns.values()) if (this.portable(pattern)) merged.set(pattern.id, pattern);
		const kept = [...merged.values()]
			.sort(
				(left, right) =>
					patternRank(right, this.clock, this.settings.decayHalfLifeEvents) -
					patternRank(left, this.clock, this.settings.decayHalfLifeEvents),
			)
			.slice(0, this.settings.maxPatterns);
		await writeJsonFile(
			file,
			{
				patterns: kept.map((pattern) =>
					readonlyPattern(pattern, this.clock, this.settings.decayHalfLifeEvents),
				),
			},
			undefined,
			0o600,
			this.reservedRoot,
		);
	}
	private portable(pattern: MutablePattern) {
		return (
			pattern.occurrences >= this.settings.minOccurrences &&
			Object.values(pattern.bindings).every(
				(binding) =>
					binding.type === "event" ||
					binding.type === "each" ||
					(binding.type === "constant" && typeof binding.value !== "string"),
			)
		);
	}
	private persist() {
		if (!this.persistenceFile || !this.loaded) return;
		this.dirty = true;
		if (this.persistTimer) return;
		this.persistTimer = setTimeout(() => {
			this.persistTimer = undefined;
			void this.flush().catch(() => undefined);
		}, speculativeConfig().prediction.persistIntervalMs);
		this.persistTimer.unref?.();
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
		const pools = this.rankedPools()
			.slice(0, this.settings.maxPatterns)
			.map((pool): PersistedPatternPool => ({
				...pool,
				samples: pool.samples.slice(-sampleLimit).map((sample) => ({
					context: sample.context.map(reference),
					target: reference(sample.target),
					gap: pool.gap,
				})),
			}));
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
	reservedRoot = false,
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
	const poolKey = `${file}\0${analyzerKey}\0${semanticsKey}\0${reservedRoot}`;
	let pooled = stores.get(poolKey);
	if (!pooled) {
		const portable = path.join(path.dirname(file), `portable.${hash(analyzerKey).slice(0, 12)}.json`);
		const store = Promise.resolve(
			new PatternAwareStore(settings, file, actionSemantics, portable, reservedRoot),
		).then(async (value) => {
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
		release: () =>
			(released ??= Promise.resolve().then(async () => {
				pooled.references = Math.max(0, pooled.references - 1);
				try {
					await store.flush();
				} finally {
					if (pooled.references === 0 && stores.get(poolKey) === pooled) stores.delete(poolKey);
				}
			})),
	};
}
export function patternAwareAnalyzerKey(settings: PatternAwareSettings): string {
	return stableStringify({
		maxContextLength: settings.maxContextLength,
		maxFutureGap: settings.maxFutureGap,
		decayHalfLifeEvents: settings.decayHalfLifeEvents,
		minOccurrences: settings.minOccurrences,
		bindingModel: "structured-fields-v1",
		maxPatterns: settings.maxPatterns,
	});
}
async function readPatterns(file: string): Promise<MutablePattern[]> {
	const parsed = await fs.readFile(file, "utf8").then(
		(value) =>
			JSON.parse(value) as {
				patterns?: unknown;
			},
		() => undefined,
	);
	return (Array.isArray(parsed?.patterns) ? parsed.patterns : []).flatMap(
		(item) => mutablePattern(item as PatternAwarePattern) ?? [],
	);
}
function configuredPersistenceFile(file: string, analyzerKey: string, semanticsKey: string): string {
	const parsed = path.parse(file);
	return path.join(
		parsed.dir,
		`${parsed.name}.${hash(`${semanticsKey}\0${analyzerKey}`).slice(0, 12)}${parsed.ext}`,
	);
}
export function patternAwareSettings(value: unknown): PatternAwareSettings {
	return parsePatternSettings(asRecord(value), speculativeConfig().pattern);
}
export function patternAwarePersistenceFile(workspace: string, stateDirectory?: string) {
	let root;
	if (stateDirectory) {
		root = path.resolve(stateDirectory);
	} else {
		if (process.env.PI_STATE_DIR) {
			root = path.resolve(process.env.PI_STATE_DIR);
		} else {
			if (process.platform === "win32") {
				root = path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"), "pi");
			} else {
				root = path.join(process.env.XDG_STATE_HOME || path.join(os.homedir(), ".local", "state"), "pi");
			}
		}
	}
	return path.join(root, "pattern-aware", `${hash(path.resolve(workspace))}.json`);
}
export function projectPatternAwareObservation(
	output: unknown,
	outputPaths: ReadonlyArray<string> = [],
	resourceRoot?: string,
	outputLocations: ReadonlyArray<OutputLocation> = [],
): PatternAwareObservation {
	const locations = [
		...uniqueBy(
			outputLocations.map(({ path, line }) => ({
				path: normalizeResourcePath(path, resourceRoot),
				line,
			})),
			stableStringify,
		),
	].slice(0, speculativeConfig().prediction.maxValueItems);
	const paths = uniqueStrings(outputPaths.map((item) => normalizeResourcePath(item, resourceRoot))).sort();
	const structured = structuredOutput(output);
	return {
		...(structured !== undefined ? { output: structured } : {}),
		...(paths.length ? { outputPaths: paths } : {}),
		...(locations.length ? { outputLocations: locations } : {}),
	};
}
/** Derived state belongs to one analyzer; public helpers get a fresh scope for mutable caller data. */
class PatternBindingAnalysis {
	private readonly indexes = new WeakMap<object, Map<string, PatternAwarePath[]>>();
	private valueIndex(value: unknown) {
		const cached = isObject(value) ? this.indexes.get(value) : undefined;
		if (cached) return cached;
		const index = new Map<string, PatternAwarePath[]>();
		for (const [path, item] of this.leaves(value)) {
			const key = stableStringify(item);
			const locations = index.get(key) ?? [];
			locations.push(path);
			index.set(key, locations);
		}
		if (isObject(value)) this.indexes.set(value, index);
		return index;
	}
	inferBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: Record<string, unknown>,
	): Record<string, PatternAwareBinding> {
		const bindings: Record<string, PatternAwareBinding> = {};
		for (const [targetPath, value] of this.leaves(target)) {
			const key = encodePath(targetPath);
			bindings[key] = this.candidateBindings(context, value, "first")[0] ?? {
				type: "constant",
				value,
			};
		}
		return bindings;
	}
	inferBindingsFromSamples(
		samples: ReadonlyArray<PatternSample>,
		constantSupport = speculativeConfig().prediction.bindingConstantMinimum,
		allowProjectedOmissions = false,
	): Record<string, PatternAwareBinding> | undefined {
		const first = samples[0];
		if (!first) return;
		const bindings: Record<string, PatternAwareBinding> = {};
		for (const [targetPath, value] of this.leaves(first.target.input)) {
			const selected = this.candidateBindings(first.context, value).find((binding) =>
				samples.every((sample) =>
					this.bindingValues(binding, sample.context).some((candidate) =>
						sameValue(candidate, getPath(sample.target.input, targetPath)),
					),
				),
			);
			if (selected) bindings[encodePath(targetPath)] = selected;
			else if (
				samples.every((sample) => sameValue(value, getPath(sample.target.input, targetPath))) &&
				samples.length >= constantSupport
			) {
				bindings[encodePath(targetPath)] = { type: "constant", value };
			} else if (!allowProjectedOmissions) return;
		}
		return bindings;
	}
	applyWeightedBindings(
		bindings: Readonly<Record<string, PatternAwareBinding>>,
		context: ReadonlyArray<PatternAwareEvent>,
		limit = speculativeConfig().prediction.maxBindingVariants,
	): ReadonlyArray<{ readonly input: Record<string, unknown>; readonly probability: number }> {
		if (limit <= 0) return [];
		const entries = Object.entries(bindings);
		const paths = entries.map(([key]) => decodePath(key));
		if (paths.some((path) => !path.length || path.some(unsafePathSegment))) return [];
		const groups = entries.map(([, binding]) => this.bindingRows(binding, context));
		const variants: Array<{ input: Record<string, unknown>; probability: number }> = [];
		const identities = new Set<string>();
		for (const row of joinBindingRows(groups)) {
			const input = paths.reduce((value, path, index) => withPath(value, path, row.values[index]), {});
			const key = stableStringify(input);
			if (identities.has(key)) continue;
			identities.add(key);
			variants.push({ input, probability: 1 });
			if (variants.length >= limit) break;
		}
		for (const variant of variants) variant.probability = 1 / variants.length;
		return variants;
	}
	candidateBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: unknown,
		mode: "direct" | "all" | "first" = "all",
	): readonly PatternAwareBinding[] {
		const candidates = this.inferCandidateBindings(context, target);
		if (mode !== "first") return [...candidates];
		const first = candidates.next().value;
		return first ? [first] : [];
	}
	*inferCandidateBindings(
		context: ReadonlyArray<PatternAwareEvent>,
		target: unknown,
	): Generator<PatternAwareBinding, undefined> {
		const key = stableStringify(target);
		for (const [relativeEvent, field, value] of reverseContextFields(context))
			for (const path of this.valueIndex(value).get(key) ?? [])
				yield { type: "event", relativeEvent, field, path };
	}
	bindingValues(
		binding: PatternAwareBinding,
		context: ReadonlyArray<PatternAwareEvent>,
	): ReadonlyArray<unknown> {
		return this.bindingRows(binding, context).map((row) => row.values[0]);
	}
	private bindingRows(
		binding: PatternAwareBinding,
		context: ReadonlyArray<PatternAwareEvent>,
	): readonly BindingRow[] {
		const row = (value: unknown, sources: ReadonlyMap<string, number> = new Map()): BindingRow => ({
			values: [value],
			sources,
		});
		if (binding.type === "constant") return [row(binding.value)];
		if (binding.type === "event" || binding.type === "each") {
			const event = context[context.length + binding.relativeEvent];
			if (!event) return [];
			const collection = getPath(event[binding.field], binding.path);
			if (binding.type === "event") return collection === MISSING ? [] : [row(collection)];
			const key = stableStringify([binding.relativeEvent, binding.field, binding.path]);
			return Array.isArray(collection)
				? collection.flatMap((item, index) => {
						const value = getPath(item, binding.itemPath);
						return value === MISSING ? [] : [row(value, new Map([[key, index]]))];
					})
				: [];
		}
		return [];
	}
	*leaves(
		value: unknown,
		segments: PatternAwarePath = [],
	): Generator<readonly [PatternAwarePath, unknown], undefined> {
		if (!isObject(value)) {
			yield [segments, value];
			return;
		}
		const array = Array.isArray(value);
		const entries: Array<[string | number, unknown]> = array
			? value.map((item, index) => [index, item])
			: Object.entries(value);
		if (!entries.length) yield [segments, array ? [] : {}];
		for (const entry of entries) if (entry) yield* this.leaves(entry[1], [...segments, entry[0]]);
	}
}
function hasSufficientBindingProvenance(
	bindings: Readonly<Record<string, PatternAwareBinding>>,
	samples: ReadonlyArray<PatternSample>,
	minimum: number,
) {
	return samples.length >= minimum || Object.values(bindings).every((binding) => binding.type !== "constant");
}
const MISSING = Symbol("missing");
type BindingRow = {
	readonly values: readonly unknown[];
	readonly sources: ReadonlyMap<string, number>;
};
/** A source row is an identity, even when two rows contain the same value. */
function* joinBindingRows(groups: readonly (readonly BindingRow[])[]): Generator<BindingRow, undefined> {
	const pending: Array<{
		group: number;
		row: BindingRow;
	}> = [{ group: 0, row: { values: [], sources: new Map() } }];
	while (pending.length) {
		const { group, row } = pending.pop()!;
		if (group === groups.length) {
			yield row;
			continue;
		}
		const choices = groups[group]!;
		for (let index = choices.length - 1; index >= 0; index--) {
			const next = choices[index]!;
			if (
				[...next.sources].some(
					([source, position]) => row.sources.has(source) && row.sources.get(source) !== position,
				)
			)
				continue;
			pending.push({
				group: group + 1,
				row: {
					values: [...row.values, ...next.values],
					sources: new Map([...row.sources, ...next.sources]),
				},
			});
		}
	}
}

function* reverseContextFields(
	context: ReadonlyArray<PatternAwareEvent>,
): Generator<readonly [number, PatternAwareDependencySource["field"], unknown]> {
	for (let index = context.length - 1; index >= 0; index--) {
		const event = context[index]!;
		const relativeEvent = index - context.length;
		yield [relativeEvent, "input", event.input];
		yield [relativeEvent, "output", event.output];
		if (event.outputLocations) yield [relativeEvent, "outputLocations", event.outputLocations];
		yield [relativeEvent, "outputPaths", event.outputPaths];
	}
}

function structuredOutput(value: unknown): unknown {
	const record = asRecord(value);
	return record && Array.isArray(record.content) ? record.details : value;
}
function normalizeResourcePath(value: string, resourceRoot?: string) {
	if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) return value;
	if (!resourceRoot || !path.isAbsolute(value)) return normalizePath(value);
	const relative = relativeFilesystemPath(resourceRoot, value);
	return relative === undefined ? normalizePath(value) : normalizePath(relative || ".");
}
function* uniqueBy<Value>(values: Iterable<Value>, keyFor: (value: Value) => string): Generator<Value> {
	const seen = new Set<string>();
	for (const item of values) {
		const key = keyFor(item);
		if (seen.has(key)) continue;
		seen.add(key);
		yield item;
	}
}
function analyzeBindings(bindings: Readonly<Record<string, PatternAwareBinding>>) {
	const dependencies: PatternAwareDependency[] = [];
	for (const [encoded, binding] of Object.entries(bindings)) {
		if (binding.type !== "event" && binding.type !== "each") continue;
		dependencies.push({
			targetPath: decodePath(encoded),
			sources: [
				{
					relativeEvent: binding.relativeEvent,
					field: binding.field,
					path: binding.path,
					...(binding.type === "each" ? { itemPath: binding.itemPath } : {}),
				},
			],
		});
	}
	return { dependencies };
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
		pattern.context.length <= settings.maxContextLength && pattern.occurrences >= settings.minOccurrences
	);
}
function groupGapTiming(
	patterns: ReadonlyArray<MutablePattern>,
	settings: PatternAwareSettings,
	clock: number,
) {
	const combined = new Map<number, number>();
	for (const pattern of patterns) {
		for (const [value, count] of Object.entries(pattern.gapCounts)) {
			const parsed = Number.parseInt(value, 10);
			const gap = Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
			const lastSeen = pattern.gapLastSeen[value]!;
			const weight = Math.max(0, count) * recencyWeight(lastSeen, clock, settings.decayHalfLifeEvents);
			if (!(weight > 0) || gap > settings.maxFutureGap) continue;
			combined.set(gap, (combined.get(gap) ?? 0) + weight);
		}
	}
	const gaps = [...combined.entries()].sort(([left], [right]) => left - right);
	const total = gaps.reduce((sum, [, weight]) => sum + weight, 0);
	const target = total * settings.futureGapCoverage;
	const latestHorizon = gaps.at(-1)?.[0] ?? 0;
	let horizon = latestHorizon;
	let covered = 0;
	for (const [gap, weight] of gaps) {
		covered += weight;
		if (covered >= target) {
			horizon = gap;
			break;
		}
	}
	return { horizon, latestHorizon };
}
function ownBatch(inputs: ReadonlyArray<PatternAwareEventInput>, sessionID?: string) {
	inputs = structuredClone(inputs);
	sessionID ??= inputs[0]?.sessionID;
	if (inputs.some((input) => input.sessionID !== sessionID || input.turnID !== inputs[0]!.turnID)) {
		throw new Error("PatternAware batch actions must belong to one session and provider turn");
	}
	// Bindings use member positions: argument changes must not reorder distinct tools.
	return inputs
		.map((input) => ({
			input,
			key: stableStringify([input.tool, input.outcome, input.operation ?? "", input.input]),
		}))
		.sort((left, right) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0))
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
	const outputShape = semanticOutputShape(event.output, event.errorClass);
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
function semanticOutputShape(value: unknown, errorClass?: string) {
	const discriminants: string[] = errorClass ? [`error:${errorClass}`] : [];
	const visit = (item: unknown, key = "", depth = 0) => {
		if (depth > speculativeConfig().prediction.structuralDepth) return;
		if (
			(typeof item === "string" || typeof item === "number" || typeof item === "boolean") &&
			speculativeConfig().prediction.outputDiscriminants.includes(key)
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

function patternRank(pattern: MutablePattern, clock: number, halfLife: number) {
	return pattern.occurrences * recencyWeight(pattern.lastSeenSequence, clock, halfLife);
}
function feedbackEvidence({ feedback }: Pick<MutablePattern, "feedback">, clock: number, halfLife: number) {
	const weight = recencyWeight(feedback.sequence, clock, halfLife);
	return {
		matched: feedback.recentMatchedWeight * weight,
		mismatched: feedback.recentMismatchedWeight * weight,
		adopted: feedback.recentAdoptedWeight * weight,
		rejected: feedback.recentRejectedWeight * weight,
	};
}
function patternAdoptionProbability(
	patterns: ReadonlyArray<Pick<MutablePattern, "feedback">>,
	clock: number,
	halfLife: number,
) {
	let adopted = 0;
	let rejected = 0;
	for (const pattern of patterns) {
		const evidence = feedbackEvidence(pattern, clock, halfLife);
		adopted += evidence.adopted;
		rejected += evidence.rejected;
	}
	const { calibrationSuccessPrior, calibrationFailurePrior } = speculativeConfig().prediction;
	return clampProbability(
		(calibrationSuccessPrior + adopted) /
			(calibrationSuccessPrior + calibrationFailurePrior + adopted + rejected),
	);
}
function recencyWeight(lastSeen: number, clock: number, halfLife: number) {
	if (halfLife <= 0) return 1;
	return 2 ** (-Math.max(0, clock - lastSeen) / halfLife);
}
function readonlyPattern(pattern: MutablePattern, clock: number, halfLife: number): PatternAwarePattern {
	return {
		...structuredClone(pattern),
		empiricalProbability: probability(pattern),
		adoptionProbability: patternAdoptionProbability([pattern], clock, halfLife),
	};
}
function mutablePattern(value: PatternAwarePattern): MutablePattern | undefined {
	const record = asRecord(value);
	const bindings = asRecord(record?.bindings);
	const gapCounts = numericRecord(record?.gapCounts);
	const gapLastSeen = numericRecord(record?.gapLastSeen);
	const feedback = mutablePatternFeedback(record?.feedback);
	if (
		!record ||
		typeof record.id !== "string" ||
		!Array.isArray(record.context) ||
		!record.context.every(isEventSignature) ||
		typeof record.targetTool !== "string" ||
		!bindings ||
		!gapCounts ||
		!gapLastSeen ||
		Object.keys(gapCounts).some((gap) => gapLastSeen[gap] === undefined) ||
		!feedback ||
		![
			record.occurrences,
			record.historicalOpportunities,
			record.historicalMatches,
			record.lastSeenSequence,
		].every((metric) => isFiniteNumber(metric) && metric >= 0) ||
		(record.targetSchemaHash !== undefined && typeof record.targetSchemaHash !== "string") ||
		!Object.entries(bindings).every(
			([encoded, binding]) => parsePath(encoded) !== undefined && isPatternAwareBinding(binding),
		)
	)
		return;
	const safeBindings = bindings as Record<string, PatternAwareBinding>;
	const pattern = structuredClone({
		id: record.id,
		context: record.context as PatternAwareEventSignature[],
		targetTool: record.targetTool,
		bindings: safeBindings,
		...(value.targetSchemaHash ? { targetSchemaHash: value.targetSchemaHash } : {}),
		gapCounts,
		gapLastSeen,
		occurrences: finiteNumber(value.occurrences) ?? 0,
		historicalOpportunities: Math.max(1, value.historicalOpportunities),
		historicalMatches: value.historicalMatches,
		feedback,
		lastSeenSequence: finiteNumber(value.lastSeenSequence) ?? 0,
	});
	return {
		...pattern,
		dependencies: analyzeBindings(pattern.bindings).dependencies,
	};
}
function emptyPatternFeedback(sequence: number): MutablePatternFeedback {
	const counters = Object.fromEntries(
		PATTERN_FEEDBACK_COUNTERS.map((key) => [key, 0]),
	) as PatternFeedbackCounters;
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
	const counters = numericRecord(
		Object.fromEntries(PATTERN_FEEDBACK_COUNTERS.map((key) => [key, feedback?.[key]])),
	) as PatternFeedbackCounters | undefined;
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
		if (
			!record ||
			!Array.isArray(record.samples) ||
			typeof record.key !== "string" ||
			typeof record.targetTool !== "string" ||
			!Array.isArray(record.context) ||
			!record.context.every(isEventSignature) ||
			!isSafeCount(record.gap)
		)
			return [];
		const samples = record.samples.flatMap((item) => {
			const sample = asRecord(item);
			if (
				!sample ||
				!Array.isArray(sample.context) ||
				!sample.context.every((value) => isSafeCount(value)) ||
				!isSafeCount(sample.target) ||
				sample.gap !== record.gap
			)
				return [];
			const context = sample.context.map((id) => events[id as number]);
			const target = events[sample.target as number];
			if (!target || context.some((event) => !event)) return [];
			return [{ context: context as PatternAwareEvent[], target }];
		});
		if (!samples.length) return [];
		const context = structuredClone(record.context) as PatternAwareEventSignature[];
		const targetSchemaHash =
			typeof record.targetSchemaHash === "string" ? record.targetSchemaHash : undefined;
		const patternIDs = Array.isArray(record.patternIDs)
			? record.patternIDs.filter((item): item is string => typeof item === "string")
			: [];
		return [
			{
				key: patternPoolKey(context, record.targetTool, targetSchemaHash, record.gap),
				context,
				targetTool: record.targetTool,
				...(targetSchemaHash ? { targetSchemaHash } : {}),
				gap: record.gap,
				samples,
				...(patternIDs.length ? { patternIDs: [...new Set(patternIDs)] } : {}),
			},
		];
	});
}
function patternPoolKey(
	context: ReadonlyArray<PatternAwareEventSignature>,
	targetTool: string,
	targetSchemaHash: string | undefined,
	gap: number,
) {
	return hash(stableStringify({ context, targetTool, targetSchemaHash, gap }));
}
function patternPoolSampleLimit(settings: Pick<PatternAwareSettings, "minOccurrences" | "maxContextLength">) {
	return (
		Math.max(settings.minOccurrences, settings.maxContextLength) *
		speculativeConfig().prediction.sampleMultiplier
	);
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
function isPatternAwareBinding(value: unknown): value is PatternAwareBinding {
	const record = asRecord(value);
	if (!record) return false;
	if (record.type === "constant") return "value" in record;
	if (record.type !== "event" && record.type !== "each") return false;
	return (
		Number.isSafeInteger(record.relativeEvent) &&
		Number(record.relativeEvent) < 0 &&
		["input", "output", "outputPaths", "outputLocations"].includes(String(record.field)) &&
		isPatternAwarePath(record.path) &&
		(record.type !== "each" || isPatternAwarePath(record.itemPath))
	);
}

function isPatternAwarePath(value: unknown): value is PatternAwarePath {
	return (
		Array.isArray(value) && value.every((segment) => typeof segment === "string" || isSafeCount(segment))
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
function parsePath(value: string): PatternAwarePath | undefined {
	try {
		const parsed: unknown = JSON.parse(value);
		return isPatternAwarePath(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}
function uniqueStrings(values: ReadonlyArray<string>) {
	return [...new Set(values.filter((value) => value.length > 0))];
}
function normalizePath(value: string) {
	return path.normalize(value).replaceAll("\\", "/");
}
function hash(value: string) {
	return cryptoHash("sha256", value).slice(0, 32);
}
/** Keeps a digest of an oversized string and a list's head; an action whose input was shortened is context only, never predicted. */
function boundedEventPayload(event: PatternAwareEventInput): PatternAwareEventInput {
	let shortened = false;
	const bound = (value: unknown): unknown => {
		if (typeof value === "string" && value.length > speculativeConfig().prediction.maxValueCharacters) {
			shortened = true;
			return `sha256:${hash(value)}`;
		}
		if (Array.isArray(value)) {
			shortened ||= value.length > speculativeConfig().prediction.maxValueItems;
			return value.slice(0, speculativeConfig().prediction.maxValueItems).map(bound);
		}
		return isObject(value)
			? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, bound(item)]))
			: value;
	};
	const input = bound(event.input) as Record<string, unknown>;
	const learnTarget = shortened ? { learnTarget: false } : {};
	return {
		...event,
		input,
		...learnTarget,
		...(event.output === undefined ? {} : { output: bound(event.output) }),
		...(event.outputPaths
			? {
					outputPaths: event.outputPaths.slice(0, speculativeConfig().prediction.maxValueItems),
				}
			: {}),
		...(event.outputLocations
			? {
					outputLocations: event.outputLocations.slice(0, speculativeConfig().prediction.maxValueItems),
				}
			: {}),
	};
}
function probability(pattern: Pick<MutablePattern, "historicalMatches" | "historicalOpportunities">) {
	return Math.max(0, Math.min(1, pattern.historicalMatches / Math.max(1, pattern.historicalOpportunities)));
}
