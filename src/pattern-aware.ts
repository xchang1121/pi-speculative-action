import { statSync } from "node:fs";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import os from "node:os";
import path from "node:path";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
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
	isFiniteNumber,
	isObject,
	isSafeCount,
	nonNegativeCount,
	relativeFilesystemPath,
	sha256Digest,
	slash,
	stableValueHash,
	stableEqual as sameValue,
	stableStringify,
} from "./primitives.ts";
import type { PredictionSettlement, ResolutionStage } from "./settlement.ts";
import { predictionLearning } from "./settlement.ts";
export type PpmCountTrieRow = {
	readonly context: readonly string[];
	readonly counts: Readonly<Record<string, number>>;
	readonly lastSeen: number;
	/** Only stored when target timestamps differ from the context timestamp. */
	readonly targetLastSeen?: Readonly<Record<string, number>>;
};
const SESSION_END = "\0end";
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
	private sequence = 0;
	private halfLife = 0;
	constructor(maxOrder: number) {
		this.order = nonNegativeCount(maxOrder);
	}
	get size(): number {
		return this.counts.size;
	}
	observe(history: readonly string[], target: string, sequence = 0, halfLife = 0): void {
		for (const context of this.suffixes(history)) this.observeContext(context, target, sequence, halfLife);
	}
	observeContext(
		context: readonly string[],
		target: string,
		sequence: number,
		halfLife: number,
		targetLimit = Infinity,
	) {
		if (!target) return;
		this.halfLife = halfLife;
		const current = this.context(context);
		const previous = current.targets.get(target);
		// Evicted alternatives still count in the denominator. Only their individual names are dropped.
		if (!previous && target !== "\0" && target !== SESSION_END && current.targets.size >= targetLimit) {
			let victim: string | undefined;
			let least = Infinity;
			for (const [name, entry] of current.targets) {
				const mass = decayedCount(entry, sequence, halfLife);
				if (name !== "\0" && name !== SESSION_END && mass < least) {
					victim = name;
					least = mass;
				}
			}
			if (victim !== undefined) {
				const other = current.targets.get("\0");
				current.targets.delete(victim);
				this.put(current, "\0", least + (other ? decayedCount(other, sequence, halfLife) : 0), sequence);
			}
		}
		const count = previous ? decayedCount(previous, sequence, halfLife) : 0;
		this.put(current, target, Math.min(Number.MAX_SAFE_INTEGER, count + 1), sequence);
	}
	evidence(context: readonly string[], target: string, clock: number, halfLife: number) {
		const row = this.counts.get(JSON.stringify(context));
		if (!row) return;
		let hits = 0,
			stops = 0,
			total = 0;
		for (const [tool, entry] of row.targets) {
			const count = decayedCount(entry, clock, halfLife);
			total += count;
			if (tool === target) hits = count;
			if (tool === SESSION_END) stops = count;
		}
		return { hits, total, stops };
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
	snapshot(
		maxContexts = Infinity,
		sequence = this.sequence,
		halfLife = this.halfLife,
		contexts: Iterable<string> = this.counts.keys(),
	): readonly PpmCountTrieRow[] {
		const limit = Number.isFinite(maxContexts)
			? Math.max(1, Math.floor(maxContexts))
			: Number.POSITIVE_INFINITY;
		return this.ranked(contexts, sequence, halfLife)
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
		this.sequence = 0;
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
	trim(
		maxContexts: number,
		sequence = this.sequence,
		halfLife = this.halfLife,
		required?: ReadonlySet<string>,
	): void {
		const limit = Math.max(1, Math.floor(maxContexts));
		if (!(this.size > limit)) return;
		// Only rows required by an active pool may survive outside the highest-mass contexts.
		for (const [key] of this.ranked(this.counts.keys(), sequence, halfLife).slice(limit))
			if (!required?.has(key)) this.counts.delete(key);
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
		this.sequence = Math.max(this.sequence, lastSeen);
		current.targets.set(target, { count, lastSeen });
		current.lastSeen = Math.max(current.lastSeen, lastSeen);
	}
	private ranked(contexts: Iterable<string>, sequence: number, halfLife: number) {
		const rows: [string, ContextCounts, number][] = [];
		for (const key of contexts) {
			const row = this.counts.get(key);
			if (!row) continue;
			let mass = 0;
			for (const target of row.targets.values()) mass += decayedCount(target, sequence, halfLife);
			rows.push([key, row, mass]);
		}
		return rows.sort(
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
type ResultCardinality = "empty" | "singleton" | "few" | "truncated";
export type PatternAwareEventSignature = {
	readonly cardinality?: ResultCardinality;
	readonly tool: string;
	readonly outcome: "success" | "failure";
	readonly operation?: string;
	readonly outputShape?: string;
};
export type PatternAwareEventInput = {
	/** Stable, publisher-declared identity used only to learn and route predictions. */
	readonly patternIdentity?: string;
	readonly textLines?: readonly string[];
	readonly sessionID: string;
	readonly turnID: string;
	readonly tool: string;
	readonly input: Record<string, unknown>;
	readonly outcome: "success" | "failure";
	readonly output?: unknown;
	readonly cardinality?: ResultCardinality;
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
	/** Equal namespaces promise the same canonicalization contract, including captured inputs. */
	readonly namespace: string;
	readonly acceptsTextCandidate?: (tool: string, action: ActionKey | undefined) => boolean;
	/** Deterministic K(a) for one namespace; repeated inputs may be memoized. */
	readonly actionKey: (
		tool: string,
		input: Readonly<Record<string, unknown>>,
		schemaHash?: string,
	) => ActionKey | undefined;
	/** Whether a workspace file exists now: a read prediction of a missing one never runs. Omitted, every read stays a candidate. */
	readonly exists?: (target: string) => boolean;
};
export function patternAwareActionSemantics(
	registry: ActionSemanticsRegistry,
	cwd: string,
	exists?: (target: string) => boolean,
): PatternAwareActionSemantics {
	cwd = path.resolve(cwd);
	return Object.freeze({
		namespace: stableStringify([
			cwd,
			[...registry.toolNames()].sort().map((tool) => [tool, registry.definition(tool)!.epoch]),
		]),
		acceptsTextCandidate: (tool: string, action: ActionKey | undefined) => {
			const definition = registry.definition(tool);
			return !definition || (!!action && definition.effect !== "unbounded");
		},
		actionKey: (tool: string, input: Readonly<Record<string, unknown>>, schemaHash?: string) =>
			registry.buildKey(tool, input, cwd, schemaHash),
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
	readonly field: "input" | "output" | "outputPaths" | "outputLocations" | "textLines";
	readonly path: PatternAwarePath;
	readonly itemPath?: PatternAwarePath;
};
export type PatternAwareDependency = {
	readonly targetPath: PatternAwarePath;
	readonly sources: ReadonlyArray<PatternAwareDependencySource>;
};
type TextSpan = { left: string; occurrence: number; right: string; number: boolean };
type RowCounts = Record<string, number[]>;
export type PatternAwareBinding =
	| (Omit<PatternAwareDependencySource, "itemPath"> & { readonly type: "event" })
	| (Required<PatternAwareDependencySource> & { readonly type: "each"; readonly span?: TextSpan })
	| { readonly type: "constant"; readonly value: unknown };
export type PatternAwarePattern = Readonly<
	Omit<MutablePattern, "context" | "bindings" | "dependencies" | "feedback">
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
	/** Confidence contributed by gap zero when several horizons share this action. */
	readonly immediateConfidence?: number;
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
	"output" | "outputPaths" | "outputLocations" | "textLines" | "cardinality" | "errorClass"
>;
type MutablePattern = {
	id: string;
	context: PatternAwareEventSignature[];
	targetTool: string;
	bindings: Record<string, PatternAwareBinding>;
	dependencies: PatternAwareDependency[];
	targetSchemaHash?: string;
	occurrences: number;
	historicalOpportunities: number;
	historicalStops: number;
	historicalMatches: number;
	bindingPrecision: number;
	rowCounts?: RowCounts;
	gap: number;
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
};
type PersistedPatternPool = Omit<PatternPool, "samples" | "key"> & {
	readonly samples: ReadonlyArray<PersistedPatternSample>;
};
type PersistedState = {
	readonly clock: number;
	readonly patterns: ReadonlyArray<unknown>;
	readonly events: ReadonlyArray<PatternAwareEvent>;
	readonly pools: ReadonlyArray<unknown>;
	readonly transitions: ReadonlyArray<PpmCountTrieRow>;
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
	patternID?: string;
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
	private pooledTransitions?: Set<string>;

	private readonly sessions: BoundedRecencyMap<
		string,
		{
			history: PatternAwareEvent[];
			calls: { tool: string; identity: string }[];
		}
	>;

	private readonly patternSupportSessions = new Map<string, ReadonlySet<string>>();
	private contexts?: Map<string, MutablePattern[]>;
	private readonly sequenceModel: PpmCountTrie;
	private readonly firstSeen: BoundedRecencyMap<string, number>;
	private readonly recentTargets: BoundedRecencyMap<
		number,
		{ history: readonly PatternAwareEvent[]; target: PatternAwareEvent }
	>;
	private shared = new Set<string>();
	private sharedSelectors?: Set<string>;
	private readonly importedCounts = new Map<string, RowCounts>();
	private readonly portableFeedback = new Map<string, MutablePattern>();
	private readonly confusion: BoundedRecencyMap<string, MutablePatternFeedback>;
	private clock = 0;
	private write?: Promise<void>;
	private dirty = false;
	private persistTimer?: ReturnType<typeof setTimeout>;
	private loading?: Promise<void>;
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
		this.sequenceModel = new PpmCountTrie(settings.maxContextLength + 1);
		this.confusion = new BoundedRecencyMap(settings.maxPatterns);
		const window = Math.max(8, Math.ceil(Math.sqrt(settings.maxPatterns)));
		this.recentTargets = new BoundedRecencyMap(window);
		this.firstSeen = new BoundedRecencyMap(window * settings.maxContextLength * (settings.maxFutureGap + 1));
		this.persistenceFile = persistenceFile;
		this.portableFile = portableFile;
		this.reservedRoot = reservedRoot;
		this.actionSemantics = actionSemantics && { ...actionSemantics };
	}
	load(): Promise<void> {
		return (this.loading ??= this.loadWorkspace().then(async () => {
			// Patterns another workspace validated start here as fresh, keeping their evidence; local feedback moves them from there.
			const portable = this.portableFile ? await readPatterns(this.portableFile) : [];
			this.shared = new Set(portable.map((pattern) => pattern.id));
			for (const pattern of portable) {
				const clock = patternEvidenceClock(pattern);
				if (!this.portable(pattern, clock)) continue;
				if (pattern.rowCounts) this.importedCounts.set(pattern.id, pattern.rowCounts);
				if (this.patterns.has(pattern.id) || pattern.context.length > this.settings.maxContextLength) continue;
				const recent = feedbackEvidence(pattern, clock, this.settings.decayHalfLifeEvents);
				this.patterns.set(pattern.id, {
					...pattern,
					rowCounts: undefined,
					lastSeenSequence: this.clock,
					feedback: {
						...pattern.feedback,
						sequence: this.clock,
						recentMatchedWeight: recent.matched,
						recentMismatchedWeight: recent.mismatched,
						recentAdoptedWeight: recent.adopted,
						recentRejectedWeight: recent.rejected,
					},
				});
			}
			if (portable.length) {
				this.contexts = undefined;
				this.trimPatterns();
			}
		}));
	}
	private async loadWorkspace() {
		if (!this.persistenceFile) return;
		const parsed = await fs
			.readFile(this.persistenceFile, "utf8")
			.then((value) => JSON.parse(value) as PersistedState)
			.catch(() => undefined);
		if (
			!parsed ||
			Object.keys(parsed).length !== 5 ||
			!isSafeCount(parsed.clock) ||
			!Array.isArray(parsed.patterns) ||
			!Array.isArray(parsed.events) ||
			!Array.isArray(parsed.pools) ||
			!Array.isArray(parsed.transitions)
		)
			return;
		this.clock = Math.max(this.clock, parsed.clock);
		for (const item of parsed.patterns) {
			const pattern = mutablePattern(item);
			if (
				!pattern ||
				pattern.context.length > this.settings.maxContextLength ||
				pattern.context.some((event) => event.tool === "$llm")
			)
				continue;
			this.patterns.set(pattern.id, pattern);
		}
		for (const pool of mutablePools(parsed.events, parsed.pools)) {
			if (
				pool.context.length > this.settings.maxContextLength ||
				pool.context.some((event) => event.tool === "$llm")
			)
				continue;
			this.pools.set(pool.key, pool);
			const pattern = pool.patternID && this.patterns.get(pool.patternID);
			if (pattern && !pattern.dependencies.length) {
				this.patternSupportSessions.set(
					pattern.id,
					new Set(
						this.supportingSamples(pattern.bindings, pool).samples.map((sample) => sample.target.sessionID),
					),
				);
			}
		}
		this.sequenceModel.restore(parsed.transitions);
		this.pooledTransitions = undefined;
		this.contexts = undefined;
		this.trimPools();
		this.trimPatterns();
		this.trimTransitions();
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
		const events = inputs.map(
			(input, index): PatternAwareEvent => ({
				...this.eventPayload(input),
				sequence: ++this.clock,
				...(batchID ? { batchID, batchIndex: index, batchSize: inputs.length } : {}),
			}),
		);
		let session = this.sessions.get(first.sessionID);
		if (!session) {
			session = {
				history: [],
				calls: [],
			};
			this.sessions.set(first.sessionID, session);
		}
		const history = session.history;
		const contexts = [...this.learningContexts(history)].map(({ context, gap }) => {
			const signatures = context.map(signature);
			return { context, signatures, gap, tokens: transitionContext(signatures, gap) };
		});
		const before = history.slice();
		for (const event of events) {
			for (const { context, signatures, gap, tokens } of contexts) {
				const witnessed =
					event.learnTarget !== false && (!gap || this.bindingAnalysis.witness(context, event.input));
				// Ineligible tools and unwitnessed future targets still consume an opportunity at this gap.
				this.observeTransition(tokens, witnessed ? predictionTool(event) : "\0", event.sequence);
				if (witnessed) this.learnOccurrence(context, signatures, event, gap, tokens);
			}
			if (event.learnTarget !== false) {
				this.recentTargets.set(event.sequence, { history: before, target: event });
				const key = this.resolveActionKey(event.tool, event.input, event.schemaHash);
				session.calls.push({ tool: event.tool, identity: predictionIdentity(event.tool, event.input, key) });
			}
		}
		const limit = speculativeConfig().runtime.eventHistory;
		if (session.calls.length > limit) session.calls.splice(0, session.calls.length - limit);
		history.push(...events);
		this.trimSessionHistory(history);
		this.trimPools();
		this.trimPatterns();
		this.trimTransitions();
		this.persist();
	}
	observeTurn() {
		this.clock++;
	}
	finishSession(sessionID: string, terminal = false) {
		const session = this.sessions.get(sessionID);
		if (session && terminal) {
			for (const { context, gap } of this.learningContexts(session.history))
				this.observeTransition(transitionContext(context.map(signature), gap), SESSION_END, this.clock);
			this.trimTransitions();
		}
		this.sessions.delete(sessionID);
		this.persist();
	}
	predict(
		sessionID: string,
		schemaHashes: Readonly<Record<string, string>> = {},
		predictionSettings: PatternAwareSettings = this.settings,
		identities?: Readonly<Record<string, string>>,
	) {
		if (!predictionSettings.enabled) return [];
		const history = this.sessions.get(sessionID)?.history ?? [];
		return this.predictHistory(
			history,
			schemaHashes,
			{ history, visitedPatternIDs: [], pathProbability: 1 },
			predictionSettings,
			identities,
		);
	}
	predictAfterBatch(
		sessionID: string,
		inputs: ReadonlyArray<PatternAwareEventInput>,
		schemaHashes: Readonly<Record<string, string>> = {},
		predictionSettings: PatternAwareSettings = this.settings,
		seed?: Pick<PatternAwareContinuation, "visitedPatternIDs" | "pathProbability">,
		identities?: Readonly<Record<string, string>>,
	) {
		if (!predictionSettings.enabled || !inputs.length) return [];
		const ordered = ownBatch(inputs, sessionID);
		const turnID = ordered[0]!.turnID;
		const history = [...(this.sessions.get(sessionID)?.history ?? [])];
		const sequence = history.at(-1)?.sequence ?? this.clock;
		history.push(
			...ordered.map(
				(input, index): PatternAwareEvent => ({
					...this.eventPayload(input),
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
			identities,
		);
	}
	continue(
		continuation: PatternAwareContinuation,
		input: PatternAwareEventInput,
		schemaHashes: Readonly<Record<string, string>> = {},
		parentConfirmed = false,
		predictionSettings: PatternAwareSettings = this.settings,
		identities?: Readonly<Record<string, string>>,
	) {
		if (
			!predictionSettings.enabled ||
			continuation.visitedPatternIDs.length >= predictionSettings.maxPredictionDepth
		)
			return [];
		const event: PatternAwareEvent = {
			...this.eventPayload(input),
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
			identities,
		);
	}
	private predictHistory(
		history: ReadonlyArray<PatternAwareEvent>,
		schemaHashes: Readonly<Record<string, string>>,
		continuation: PatternAwareContinuation,
		settings: PatternAwareSettings,
		identities?: Readonly<Record<string, string>>,
	): PatternAwareCandidate[] {
		if (continuation.visitedPatternIDs.length >= settings.maxPredictionDepth) return [];
		const sessionID = history.at(-1)?.sessionID;
		const predictions = new Map<string, Map<number, PatternAwareCandidate>>();
		const calls = sessionID && this.sessions.get(sessionID)?.calls;
		const localTools = new Map<string, string | undefined>();
		for (const [tool, identity] of Object.entries(identities ?? {})) {
			const key = predictionTool({ tool, patternIdentity: identity });
			localTools.set(key, localTools.has(key) ? undefined : tool);
		}
		for (const { pattern, context } of this.matchingPatterns(history)) {
			if (continuation.visitedPatternIDs.includes(pattern.id) || !structurallyEligible(pattern, settings))
				continue;
			const sessions = this.patternSupportSessions.get(pattern.id);
			if (sessionID && sessions && !sessions.has(sessionID) && sessions.size < settings.minOccurrences)
				continue;
			const tool = pattern.targetTool.startsWith(rolePrefix)
				? localTools.get(pattern.targetTool)
				: identities && Object.hasOwn(identities, pattern.targetTool)
					? undefined
					: pattern.targetTool;
			if (!tool || (pattern.targetSchemaHash && schemaHashes[tool] !== pattern.targetSchemaHash)) continue;
			const horizon = pattern.gap;
			if (horizon > settings.maxFutureGap) continue;
			if (horizon && !settings.multiStepEnabled) continue;
			const feedback = feedbackEvidence(pattern, this.clock, settings.decayHalfLifeEvents);
			if (
				pattern.context.length > 1 &&
				!usesOldest(pattern.dependencies, pattern.context.length) &&
				this.hasSuffixGain(pattern.context, horizon, pattern.targetTool) === false
			)
				continue;
			const calibrated = this.measured(pattern);
			const precision =
				(probability(calibrated) * (1 + feedback.matched)) / (1 + feedback.matched + feedback.mismatched);
			if (precision <= 0) continue;
			const adoptionProbability = patternAdoptionProbability(pattern, this.clock, settings.decayHalfLifeEvents);
			const readsText = Object.values(pattern.bindings).some(
				(binding) => binding.type === "each" && binding.span,
			);
			const variants = this.bindingAnalysis
				.applyWeightedBindings(pattern.bindings, context, {
					counts: pattern.rowCounts,
					prior: this.importedCounts.get(pattern.id),
				})
				.flatMap(({ input, probability }) => {
					const key = this.resolveActionKey(tool, input, pattern.targetSchemaHash ?? schemaHashes[tool]);
					if (readsText && this.actionSemantics?.acceptsTextCandidate?.(tool, key) === false) return [];
					return [{ input, probability, identity: predictionIdentity(tool, input, key) }];
				});
			if (calls && variants.length > 1)
				weightRevisits(
					variants,
					calls.filter((call) => call.tool === tool),
				);
			for (const { input, probability: weight, identity: actionIdentity } of variants) {
				const conditionalProbability = clampProbability(precision * weight);
				if (!conditionalProbability) continue;
				const empiricalProbability = clampProbability(continuation.pathProbability * conditionalProbability);
				const confidence = empiricalProbability * adoptionProbability;
				const candidate: PatternAwareCandidate = {
					type: "tool_call",
					source: "pattern_aware",
					tool,
					input,
					patternID: pattern.id,
					actionIdentity,
					supportingPatternIDs: [pattern.id],
					horizon,
					latestHorizon: horizon,
					conditionalProbability,
					empiricalProbability,
					adoptionProbability,
					confidence,
					...(confused(feedback) || calibrated.historicalStops > calibrated.historicalOpportunities / 2
						? { background: true }
						: {}),
					dependencies: pattern.dependencies,
					continuation: {
						history,
						visitedPatternIDs: [...continuation.visitedPatternIDs, pattern.id],
						pathProbability: empiricalProbability,
					},
					depth: continuation.visitedPatternIDs.length + 1,
				};
				let gaps = predictions.get(actionIdentity);
				if (!gaps) predictions.set(actionIdentity, (gaps = new Map()));
				const previous = gaps.get(horizon);
				if (
					!previous ||
					Number(!!candidate.background) < Number(!!previous.background) ||
					(candidate.background === previous.background && confidence > previous.confidence)
				)
					gaps.set(horizon, candidate);
			}
		}
		const counts = new Map<string, number>();
		return [...predictions.values()]
			.map((gaps) => mergePredictionGaps(gaps, settings.futureGapCoverage, continuation.pathProbability))
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
			})
			.map((candidate) => structuredClone(candidate));
	}
	/** Use a persisted pattern ID or the original candidate continuation for session backoff feedback. */
	issued(support: string | PatternAwareContinuation) {
		const feedback = this.supportFeedback(support);
		if (!feedback) return;
		feedback.issued++;
		if (typeof support === "string") this.persist();
	}
	settled(support: string | PatternAwareContinuation, settlement: PredictionSettlement, served?: boolean) {
		const id = typeof support === "string" ? support : support.visitedPatternIDs.at(-1);
		const pattern = id && this.patterns.get(id);
		if (!pattern) return;
		const feedback = pattern.feedback;
		const sharedBefore = this.sharedSelectors && this.portable(pattern);
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
		if (
			this.portableFile &&
			settlement.observation === "observed" &&
			(this.shared.has(pattern.id) || pattern.feedback.adopted >= this.settings.minOccurrences)
		)
			this.portableFeedback.set(pattern.id, pattern);
		if (sharedBefore !== undefined && sharedBefore !== this.portable(pattern)) this.sharedSelectors = undefined;
		this.persist();
	}
	private supportFeedback(support: string | PatternAwareContinuation): MutablePatternFeedback | undefined {
		const id = typeof support === "string" ? support : support.visitedPatternIDs.at(-1);
		return id ? this.patterns.get(id)?.feedback : undefined;
	}
	snapshot(): ReadonlyArray<PatternAwarePattern> {
		return [...this.patterns.values()].map((pattern) =>
			readonlyPattern(this.measured(pattern), this.clock, this.settings.decayHalfLifeEvents),
		);
	}
	recent(sessionID: string): ReadonlyArray<PatternAwareEvent> {
		return structuredClone(this.sessions.get(sessionID)?.history ?? []);
	}
	async flush(): Promise<void> {
		const target = this.persistenceFile;
		if (!target || !this.loading) return;
		await this.loading;
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
							clock: this.clock,
							patterns: [...this.patterns.values()].map((pattern) => this.persistedPattern(pattern)),
							...this.persistedLearningState(),
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
				const key = JSON.stringify(pattern.context.map(signatureToken));
				const group = this.contexts.get(key) ?? [];
				group.push(pattern);
				this.contexts.set(key, group);
			}
		}
		const signatures = history.map(signature);
		const tokens = signatures.map(signatureToken);
		const starts = this.contextStarts(history);
		for (
			let index = history.length - 1;
			index >= Math.max(0, history.length - this.settings.maxContextLength);
			index--
		) {
			if (starts?.has(index) === false) continue;
			const patterns = this.contexts.get(JSON.stringify(tokens.slice(index)));
			if (!patterns) continue;
			const context = history.slice(index);
			for (const pattern of patterns) yield { pattern, context };
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
	private eventPayload(event: PatternAwareEventInput): PatternAwareEventInput {
		const fields = Object.entries(event.input).filter(
			([, value]) => typeof value === "string" && path.isAbsolute(value),
		);
		if (fields.length) {
			const key = this.resolveActionKey(event.tool, event.input, event.schemaHash);
			if (key) {
				const input = { ...event.input };
				let changed = false;
				for (const [field, raw] of fields) {
					const resource = key.input[field];
					if (typeof resource === "string" && resource !== raw && key.resources.includes(resource)) {
						input[field] = resource;
						changed = true;
					}
				}
				if (changed && this.resolveActionKey(event.tool, input, event.schemaHash)?.key === key.key)
					event = { ...event, input };
			}
		}
		return boundedEventPayload(event);
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
		const rowCounts: RowCounts = {};
		const supported = samples.filter((sample) => {
			if (targetTool !== predictionTool(sample.target)) return false;
			const actual = sample.target.input;
			const tool = sample.target.tool;
			const actor = this.resolveActionKey(tool, actual, sample.target.schemaHash ?? targetSchemaHash);
			const positions = new Map<string, number>();
			const covers = ({ input }: { input: Record<string, unknown> }) => {
				const speculative = this.resolveActionKey(tool, input, targetSchemaHash);
				return speculative && actor
					? speculative.key === actor.key
					: sameValue(input, actual);
			};
			const exact = this.bindingAnalysis
				.applyWeightedBindings(bindings, sample.context, { actual, positions })
				.some(covers);
			if (!exact)
				return !!actor && this.bindingAnalysis.applyWeightedBindings(bindings, sample.context).some(covers);
			for (const [key, position] of positions) {
				const counts = (rowCounts[key] ??= []);
				while (counts.length <= position) counts.push(0);
				counts[position]!++;
			}
			return true;
		});
		return { samples: supported, rowCounts };
	}

	private requiredTransitions(): ReadonlySet<string> {
		if (this.pooledTransitions) return this.pooledTransitions;
		const keys = new Set<string>();
		for (const { context, gap } of this.pools.values())
			for (let start = 0; start < context.length; start++)
				keys.add(JSON.stringify(transitionContext(context.slice(start), gap)));
		return (this.pooledTransitions = keys);
	}
	private trimTransitions() {
		const { maxPatterns, decayHalfLifeEvents } = this.settings;
		if (this.sequenceModel.size > maxPatterns)
			this.sequenceModel.trim(maxPatterns, this.clock, decayHalfLifeEvents, this.requiredTransitions());
	}
	private observeTransition(tokens: readonly string[], target: string, sequence: number) {
		this.sequenceModel.observeContext(
			tokens,
			target,
			sequence,
			this.settings.decayHalfLifeEvents,
			patternPoolSampleLimit(this.settings),
		);
	}
	private evidence(tokens: readonly string[], tool: string) {
		return this.sequenceModel.evidence(tokens, tool, this.clock, this.settings.decayHalfLifeEvents);
	}
	private hasSuffixGain(context: readonly PatternAwareEventSignature[], gap: number, tool: string) {
		const local = this.evidence(transitionContext(context, gap), tool);
		const suffix = this.evidence(transitionContext(context.slice(1), gap), tool);
		if (local && suffix)
			return (local.hits + 1) / (local.total + 2) - (suffix.hits + 1) / (suffix.total + 2) >= 0.15;
	}
	private persistedPattern(pattern: MutablePattern) {
		const { dependencies: _derived, ...fields } = this.measured(pattern);
		return fields;
	}
	private measured(pattern: MutablePattern): MutablePattern {
		const evidence = this.evidence(transitionContext(pattern.context, pattern.gap), pattern.targetTool);
		return evidence
			? {
					...pattern,
					historicalMatches: evidence.hits * pattern.bindingPrecision,
					historicalOpportunities: evidence.total,
					historicalStops: evidence.stops,
				}
			: pattern;
	}
	private learnOccurrence(
		context: readonly PatternAwareEvent[],
		signatures: PatternAwareEventSignature[],
		target: PatternAwareEvent,
		gap: number,
		tokens: readonly string[],
	) {
		const targetTool = predictionTool(target);
		const poolKey = patternPoolKey(signatures, targetTool, target.schemaHash, gap);
		const evidence = this.evidence(tokens, targetTool)!;
		const lift = context.length > 1 && this.hasSuffixGain(signatures, gap, targetTool);
		let pool = this.pools.get(poolKey);
		if (!pool) {
			if (context.length > 1 && !lift && !this.bindingAnalysis.witness(context.slice(0, 1), target.input))
				return;
			const sequence = this.firstSeen.get(poolKey);
			if (
				sequence === undefined ||
				(this.settings.decayHalfLifeEvents > 0 &&
					target.sequence - sequence > this.settings.decayHalfLifeEvents)
			) {
				this.firstSeen.set(poolKey, target.sequence);
				return;
			}
			this.firstSeen.delete(poolKey);
			const previous = this.recentTargets.get(sequence);
			const first =
				previous &&
				[...this.learningContexts(previous.history)].find(
					(window) => window.gap === gap && window.context.length === context.length,
				);
			pool = {
				key: poolKey,
				context: signatures,
				targetTool,
				targetSchemaHash: target.schemaHash,
				gap,
				samples: first && previous ? [{ context: first.context, target: previous.target }] : [],
			};
			this.pools.set(poolKey, pool);
			this.pooledTransitions = undefined;
		}
		pool.samples.push({ context, target });
		const sampleLimit = patternPoolSampleLimit(this.settings);
		if (pool.samples.length > sampleLimit) pool.samples.splice(0, pool.samples.length - sampleLimit);
		if (pool.samples.length < this.settings.minOccurrences) return;
		const constantSupport = Math.max(
			speculativeConfig().prediction.bindingConstantMinimum,
			this.settings.minOccurrences * speculativeConfig().prediction.bindingSupportMultiplier,
		);
		const inferred = this.bindingAnalysis.inferBindingsFromSamples(
			pool.samples,
			constantSupport,
			!!this.actionSemantics,
			this.confirmedSelectors(),
		);
		let learned: string | undefined;
		if (inferred) {
			const bindings = inferred;
			const { samples: support, rowCounts } = this.supportingSamples(bindings, pool);
			const { dependencies } = analyzeBindings(bindings);
			if (
				support.length < Math.max(this.settings.minOccurrences, Math.ceil((2 * pool.samples.length) / 3)) ||
				!hasSufficientBindingProvenance(bindings, support, constantSupport) ||
				(context.length > 1 && !lift && !usesOldest(dependencies, context.length))
			) {
				this.setPoolPattern(pool, learned);
				return;
			}
			const id = stableValueHash({
				context: signatures,
				targetTool,
				bindings,
				targetSchemaHash: target.schemaHash,
				gap,
			});
			learned = id;
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
				occurrences: support.length,
				lastSeenSequence,
				gap,
				bindingPrecision: support.length / pool.samples.length,
				rowCounts: Object.keys(rowCounts).length ? rowCounts : undefined,
				historicalOpportunities: evidence.total,
				historicalStops: evidence.stops,
				historicalMatches: (evidence.hits * support.length) / pool.samples.length,
			};
			const existing = this.patterns.get(id);
			if (existing) {
				Object.assign(existing, observed);
			} else {
				this.patterns.set(id, {
					id,
					context: signatures,
					targetTool,
					...observed,
					...(target.schemaHash ? { targetSchemaHash: target.schemaHash } : {}),
					feedback: this.confusion.get(id) ?? emptyPatternFeedback(observed.lastSeenSequence),
				});
				this.contexts = undefined;
			}
		}
		this.setPoolPattern(pool, learned);
	}

	private setPoolPattern(pool: PatternPool, id?: string) {
		if (pool.patternID && pool.patternID !== id) this.removePattern(pool.patternID);
		pool.patternID = id;
	}
	private removePattern(patternID: string) {
		const pattern = this.patterns.get(patternID);
		if (pattern && confused(feedbackEvidence(pattern, this.clock, this.settings.decayHalfLifeEvents)))
			this.confusion.set(patternID, pattern.feedback);
		if (this.patterns.delete(patternID)) {
			this.contexts = undefined;
			this.sharedSelectors = undefined;
		}
		this.patternSupportSessions.delete(patternID);
		this.importedCounts.delete(patternID);
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
			this.pooledTransitions = undefined;
			this.setPoolPattern(pool); // No pattern outlives the samples that support it.
		}
	}
	/** Memory keeps exactly the pools the file does: recency-weighted support, so a new relation can displace stale ones. */
	private rankedPools(): PatternPool[] {
		const last = (pool: PatternPool) => pool.samples.at(-1)?.target.sequence ?? Number.NEGATIVE_INFINITY;
		return [...this.pools.values()]
			.map((pool) => {
				const pattern = pool.patternID && this.patterns.get(pool.patternID);
				const score = pattern
					? patternRank(this.measured(pattern), this.clock, this.settings.decayHalfLifeEvents)
					: Math.min(pool.samples.length, this.settings.minOccurrences) *
						recencyWeight(last(pool), this.clock, this.settings.decayHalfLifeEvents);
				return [pool, score] as const;
			})
			.sort(([left, leftScore], [right, rightScore]) => rightScore - leftScore || last(right) - last(left))
			.map(([pool]) => pool);
	}
	private trimPatterns() {
		const limit = Math.max(1, Math.floor(this.settings.maxPatterns));
		if (this.patterns.size <= limit) return;
		const evicted = [...this.patterns.values()]
			.map((pattern) => this.measured(pattern))
			.sort(
				(left, right) =>
					patternRank(left, this.clock, this.settings.decayHalfLifeEvents) -
						patternRank(right, this.clock, this.settings.decayHalfLifeEvents) ||
					left.lastSeenSequence - right.lastSeenSequence,
			)
			.slice(0, this.patterns.size - limit);
		for (const pattern of evicted) this.removePattern(pattern.id);
	}
	/** Only fresh local settlements can publish or withdraw a shared pattern. Serialize concurrent workspace flushes. */
	private async sharePortable(file: string) {
		if (!this.portableFeedback.size) return;
		const updates = new Map(this.portableFeedback);
		this.portableFeedback.clear();
		const write = (portableWrites.get(file) ?? Promise.resolve())
			.catch(() => {})
			.then(async () => {
				const merged = new Map((await readPatterns(file)).map((pattern) => [pattern.id, pattern]));
				for (const [id, pattern] of updates)
					merged.set(id, {
						...this.measured(pattern),
						rowCounts: pattern.rowCounts ?? this.importedCounts.get(id),
					});
				const kept = [...merged.values()]
					.flatMap((pattern) => {
						const clock = updates.has(pattern.id) ? this.clock : patternEvidenceClock(pattern);
						return this.portable(pattern, clock)
							? [{ pattern, rank: patternRank(pattern, clock, this.settings.decayHalfLifeEvents) }]
							: [];
					})
					.sort((a, b) => b.rank - a.rank)
					.slice(0, this.settings.maxPatterns)
					.map(({ pattern }) => pattern);
				await writeJsonFile(
					file,
					{ patterns: kept.map(({ dependencies: _derived, ...pattern }) => pattern) },
					undefined,
					0o600,
					this.reservedRoot,
				);
				this.shared = new Set(kept.map((pattern) => pattern.id));
			});
		portableWrites.set(file, write);
		try {
			await write;
		} catch (error) {
			for (const [id, pattern] of updates)
				if (!this.portableFeedback.has(id)) this.portableFeedback.set(id, pattern);
			throw error;
		} finally {
			if (portableWrites.get(file) === write) portableWrites.delete(file);
		}
	}
	private confirmedSelectors(): ReadonlySet<string> {
		if (this.sharedSelectors) return this.sharedSelectors;
		const selectors = new Set<string>();
		for (const pattern of this.patterns.values()) {
			if (!this.portable(pattern)) continue;
			for (const [slot, binding] of Object.entries(pattern.bindings))
				if (binding.type === "each" && binding.relativeEvent === -1 && binding.field !== "input")
					selectors.add(stableStringify([slot, binding]));
		}
		return (this.sharedSelectors = selectors);
	}
	private portable(pattern: MutablePattern, clock = this.clock) {
		return (
			pattern.occurrences >= this.settings.minOccurrences &&
			pattern.feedback.adopted >= this.settings.minOccurrences &&
			!confused(feedbackEvidence(pattern, clock, this.settings.decayHalfLifeEvents)) &&
			pattern.feedback.adopted >
				pattern.feedback.observed -
					pattern.feedback.matched +
					Object.values(pattern.feedback.rejectedAfterMatch).reduce((total, count) => total + count, 0) &&
			Object.values(pattern.bindings).every(
				(binding) =>
					binding.type === "event" ||
					binding.type === "each" ||
					(binding.type === "constant" &&
						(binding.value === null || typeof binding.value === "boolean" || isFiniteNumber(binding.value))),
			)
		);
	}
	private persist() {
		if (!this.persistenceFile || !this.loading) return;
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
		readonly transitions: readonly PpmCountTrieRow[];
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
		const pools = this.rankedPools().map(
			({ key: _key, samples, ...pool }): PersistedPatternPool => ({
				...pool,
				samples: samples.slice(-sampleLimit).map((sample) => ({
					context: sample.context.map(reference),
					target: reference(sample.target),
				})),
			}),
		);
		const transitions = this.sequenceModel.snapshot(
			Infinity,
			this.clock,
			this.settings.decayHalfLifeEvents,
			this.requiredTransitions(),
		);
		return { events, pools, transitions };
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
const portableWrites = new Map<string, Promise<void>>();
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
		const portable = path.join(path.dirname(file), `portable.${sha256Digest(analyzerKey).slice(7, 19)}.json`);
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
		bindingModel: "structured-fields",
		maxPatterns: settings.maxPatterns,
	});
}
async function readPatterns(file: string): Promise<MutablePattern[]> {
	const parsed = await fs
		.readFile(file, "utf8")
		.then(
			(value) =>
				JSON.parse(value) as {
					patterns?: unknown;
				},
		)
		.catch(() => undefined);
	return (Array.isArray(parsed?.patterns) ? parsed.patterns : []).flatMap((item) => mutablePattern(item) ?? []);
}
function configuredPersistenceFile(file: string, analyzerKey: string, semanticsKey: string): string {
	const parsed = path.parse(file);
	return path.join(
		parsed.dir,
		`${parsed.name}.${sha256Digest(`${semanticsKey}\0${analyzerKey}`).slice(7, 19)}${parsed.ext}`,
	);
}
export function patternAwareSettings(value: unknown): PatternAwareSettings {
	return parsePatternSettings(asRecord(value), speculativeConfig().pattern);
}
export function patternAwarePersistenceFile(workspace: string, stateDirectory?: string) {
	const root = stateDirectory
		? path.resolve(stateDirectory)
		: process.env.PI_STATE_DIR
			? path.resolve(process.env.PI_STATE_DIR)
			: process.platform === "win32"
				? path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"), "pi")
				: path.join(process.env.XDG_STATE_HOME || path.join(os.homedir(), ".local", "state"), "pi");
	return path.join(root, "pattern-aware", `${sha256Digest(path.resolve(workspace)).slice(7, 39)}.json`);
}
/** Project bounded facts from the tool’s displayed result. */
export function projectPatternAwareObservation(
	output: unknown,
	source: readonly string[] | { tool: string; input: Readonly<Record<string, unknown>>; failed?: boolean } = [],
	resourceRoot?: string,
	outputLocations: readonly OutputLocation[] = [],
): PatternAwareObservation {
	const { maxValueCharacters, maxValueItems } = speculativeConfig().prediction;
	const record = asRecord(output);
	const blocks = Array.isArray(record?.content) ? record.content : undefined;
	const structured = blocks ? record?.details : output;
	const paths = new Set<string>();
	const locations = new Map<string, OutputLocation>();
	let truncated = false;
	let report = false;
	let textLines: string[] | undefined;
	let errorClass: string | undefined;
	const add = (name: string, line?: number) => {
		if (!name || name.length > maxValueCharacters || /[\x00-\x1f\x7f]/.test(name)) return;
		if (paths.size >= maxValueItems && !paths.has(name)) {
			truncated = true;
			return;
		}
		paths.add(name);
		if (line === undefined || !isSafeCount(line, 1)) return;
		const key = JSON.stringify([name, line]);
		if (locations.size < maxValueItems) locations.set(key, { path: name, line });
		else if (!locations.has(key)) truncated = true;
	};
	if ("tool" in source) {
		const { tool, input, failed } = source;
		report = !!blocks && !!resourceRoot && ["grep", "find", "ls", "bash"].includes(tool);
		if (blocks && !report) {
			const texts = blocks
				.slice(0, maxValueItems)
				.flatMap((block) =>
					block?.type === "text" && typeof block.text === "string" ? [block.text as string] : [],
				);
			textLines = boundedTextRows(texts, asRecord(asRecord(structured)?.truncation)?.truncated === true);
		}
		if (report) {
			const details = asRecord(structured);
			truncated = !!(
				asRecord(details?.truncation)?.truncated ||
				details?.matchLimitReached ||
				details?.entryLimitReached ||
				details?.resultLimitReached ||
				details?.linesTruncated
			);
			const root = path.resolve(resourceRoot!);
			const base = path.resolve(root, tool !== "bash" && typeof input.path === "string" ? input.path : ".");
			let grepBaseIsFile: boolean | undefined;
			let remaining = maxValueCharacters;
			for (const block of blocks!.slice(0, maxValueItems)) {
				if (block?.type !== "text" || typeof block.text !== "string") continue;
				let text = block.text.slice(0, remaining);
				if (text.length < block.text.length) {
					truncated = true;
					if (block.text[text.length] !== "\n") text = text.slice(0, Math.max(0, text.lastIndexOf("\n")));
				}
				remaining -= Math.min(remaining, block.text.length);
				for (const raw of text.split(/\r?\n/)) {
					const clean = raw.replace(/\x1b\[[0-9;]*m/g, "");
					const row = tool === "bash" ? clean.trim() : clean;
					if (failed) errorClass ??= /\b([A-Z]\w*Error|TS\d+|ENOENT|EACCES)\b/.exec(row)?.[1];
					let name: string | undefined;
					let line: number | undefined;
					if (tool === "grep" || tool === "bash") {
						const match = tool === "grep" ? /^(.+?):([1-9]\d*):(?:\s|$)/.exec(row) : diagnosticLocation(row);
						if (match) {
							name = match[1];
							line = Number(match[2]);
						}
					} else if (
						!failed &&
						row &&
						row !== "(empty directory)" &&
						row !== "No files found matching pattern" &&
						!/^\[.*(?:limit reached|truncated).*\]$/i.test(row)
					)
						name = row;
					if (!name) continue;
					if (name.startsWith("file:///")) {
						try {
							name = fileURLToPath(name);
						} catch {
							continue;
						}
					}
					if (
						/^(?:~|\\\\|\/\/)/.test(name) ||
						(/^[a-z][\w+.-]*:/i.test(name) && !(path.sep === "\\" && /^[a-z]:[\\/]/i.test(name))) ||
						(path.sep === "/" && name.includes("\\"))
					)
						continue;
					let target = path.resolve(base, name);
					// Pi uses the basename for a single-file grep. Only that ambiguous spelling needs a filesystem query.
					if (
						tool === "grep" &&
						name === path.basename(base) &&
						relativeFilesystemPath(root, base) !== undefined
					) {
						try {
							grepBaseIsFile ??= statSync(base, { throwIfNoEntry: false })?.isFile() ?? false;
						} catch {
							continue;
						}
						if (grepBaseIsFile) target = base;
					}
					const relative = relativeFilesystemPath(root, target);
					if (relative) add(slash(relative), line);
				}
			}
			truncated ||= blocks!.length > maxValueItems;
		}
	} else {
		report = source.length > 0 || outputLocations.length > 0;
		for (const name of source.slice(0, maxValueItems)) add(normalizeResourcePath(name, resourceRoot));
		for (const { path, line } of outputLocations.slice(0, maxValueItems))
			add(normalizeResourcePath(path, resourceRoot), line);
		truncated = source.length > maxValueItems || outputLocations.length > maxValueItems;
	}
	const count = locations.size || paths.size;
	return {
		...(structured !== undefined ? { output: structured } : {}),
		...(textLines?.length ? { textLines } : {}),
		...(errorClass ? { errorClass } : {}),
		...(paths.size ? { outputPaths: [...paths] } : {}),
		...(locations.size ? { outputLocations: [...locations.values()] } : {}),
		...(report
			? { cardinality: truncated ? "truncated" : count === 0 ? "empty" : count === 1 ? "singleton" : "few" }
			: {}),
	};
}
/** Preserve only complete displayed rows within the same budget as structured observations. */
function boundedTextRows(blocks: readonly string[], truncated = false): string[] {
	const { maxValueCharacters, maxValueItems } = speculativeConfig().prediction;
	const rows: string[] = [];
	let remaining = maxValueCharacters;
	for (const [index, block] of blocks.slice(0, maxValueItems).entries()) {
		if (!remaining) break;
		const head = block.slice(0, remaining);
		remaining -= head.length;
		const lines = head.split(/\r?\n/);
		if (head.length < block.length ? block[head.length] !== "\n" : truncated && index === blocks.length - 1)
			lines.pop();
		for (const line of lines) {
			if (line && !/[\x00-\x08\x0b-\x1f\x7f]/.test(line)) rows.push(line);
			if (rows.length === maxValueItems) return rows;
		}
	}
	return rows;
}
function spanDelimiter(value: unknown): value is string {
	return typeof value === "string" && (value === "" || /^[\p{P}\p{S}\t ]$/u.test(value));
}
function extractSpan(row: unknown, span: TextSpan): unknown {
	if (typeof row !== "string") return MISSING;
	let start = 0;
	for (let i = 0; i < span.occurrence; i++) {
		const next = row.indexOf(span.left, start);
		if (next < 0) return MISSING;
		start = next + 1;
	}
	const end = span.right ? row.indexOf(span.right, start) : row.length;
	if (end <= start) return MISSING;
	const value = row.slice(start, end);
	if (!span.number) return value;
	const number = Number(value);
	return Number.isSafeInteger(number) && String(number) === value ? number : MISSING;
}
function diagnosticLocation(row: string) {
	const python = /^File "([^"]+)", line ([1-9]\d*)/.exec(row);
	if (python) return python;
	// Node stack frames, TypeScript diagnostics, and compiler/test file:line[:column] records.
	const frame = row.replace(/^at\s+(?:.* \((?=.+\)$)|(?:async )?)|^(?:-->|❯)\s*/, "");
	return (
		/^(.+?\.[\w+-]+):([1-9]\d*)(?::\d+)?(?=[:)\s]|$)/.exec(frame) ??
		/^(.+?\.[\w+-]+)\(([1-9]\d*),\d+\):/.exec(frame)
	);
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
	witness(context: readonly PatternAwareEvent[], input: Record<string, unknown>): boolean {
		for (const [, value] of this.leaves(input)) {
			if (typeof value !== "string" || !value.trim() || value === ".") continue;
			for (const binding of this.inferCandidateBindings(context, value))
				if (binding.field === "outputPaths" || binding.field === "outputLocations" || value.length >= 3)
					return true;
		}
		return false;
	}
	inferBindingsFromSamples(
		samples: readonly PatternSample[],
		constantSupport: number,
		allowCanonicalOmissions: boolean,
		confirmed: ReadonlySet<string>,
	) {
		const votes = new Map<
			string,
			Map<string, { binding: PatternAwareBinding; count: number; first: unknown; varied: boolean }>
		>();
		const quorum = Math.ceil((2 * samples.length) / 3);
		for (const { context, target } of samples) {
			for (const [segments, value] of this.leaves(target.input)) {
				if (!segments.length) continue;
				const slot = encodePath(segments);
				let choices = votes.get(slot);
				if (!choices) votes.set(slot, (choices = new Map()));
				const counted = new Set<string>();
				for (const binding of [
					...this.inferCandidateBindings(context, value),
					{ type: "constant" as const, value },
				]) {
					const key = stableStringify(binding);
					if (counted.has(key)) continue;
					counted.add(key);
					const vote = choices.get(key);
					if (vote) {
						vote.count++;
						vote.varied ||= !sameValue(vote.first, value);
					} else choices.set(key, { binding, count: 1, first: value, varied: false });
				}
			}
		}
		const bindings: Record<string, PatternAwareBinding> = {};
		for (const [slot, choices] of votes) {
			const preferred = (binding: PatternAwareBinding) =>
				confirmed.size > 0 && binding.type === "each" && confirmed.has(stableStringify([slot, binding]));
			// Prefer locally witnessed structure, then an adopted selector, then a fixed field.
			const ranked = [...choices.values()]
				.filter(
					({ binding, count, varied }) =>
						count >= quorum &&
						(binding.type !== "constant" || count >= constantSupport) &&
						(binding.type !== "each" || !binding.span || varied),
				)
				.sort(
					(a, b) =>
						b.count - a.count ||
						Number(a.binding.type === "constant") - Number(b.binding.type === "constant") ||
						Number(!!("span" in a.binding && a.binding.span)) -
							Number(!!("span" in b.binding && b.binding.span)) ||
						Number(preferred(b.binding)) - Number(preferred(a.binding)) ||
						Number(a.binding.type === "each") - Number(b.binding.type === "each"),
				);
			const best = ranked[0];
			if (best?.binding.type === "each" && best.binding.span) {
				if (
					ranked
						.slice(1)
						.some(({ binding, count }) => count === best.count && binding.type === "each" && binding.span)
				)
					return;
			}
			if (best) bindings[slot] = best.binding;
			else if (
				!allowCanonicalOmissions &&
				samples.filter(({ target }) => getPath(target.input, decodePath(slot)) !== MISSING).length >= quorum
			)
				return;
		}
		return bindings;
	}
	applyWeightedBindings(
		bindings: Readonly<Record<string, PatternAwareBinding>>,
		context: ReadonlyArray<PatternAwareEvent>,
		options: {
			counts?: RowCounts;
			prior?: RowCounts;
			actual?: Record<string, unknown>;
			positions?: Map<string, number>;
		} = {},
	): ReadonlyArray<{ readonly input: Record<string, unknown>; readonly probability: number }> {
		const limit = speculativeConfig().prediction.maxBindingVariants;
		if (limit <= 0) return [];
		let base: Record<string, unknown> = {};
		const collections = new Map<
			string,
			{
				items: readonly unknown[];
				fields: { target: PatternAwarePath; item: PatternAwarePath; span?: TextSpan }[];
			}
		>();
		for (const [encoded, binding] of Object.entries(bindings)) {
			const target = decodePath(encoded);
			if (!target.length || target.some(unsafePathSegment)) return [];
			if (binding.type === "constant") {
				base = withPath(base, target, binding.value);
				continue;
			}
			const event = context[context.length + binding.relativeEvent];
			if (!event) return [];
			const value = getPath(event[binding.field], binding.path);
			if (value === MISSING) return [];
			if (binding.type === "event") base = withPath(base, target, value);
			else {
				if (!Array.isArray(value)) return [];
				const key = stableStringify([binding.relativeEvent, binding.field, binding.path]);
				let group = collections.get(key);
				if (!group) collections.set(key, (group = { items: value, fields: [] }));
				group.fields.push({ target, item: binding.itemPath, span: binding.span });
			}
		}
		let variants = [{ input: base, probability: 1 }];
		for (const [group, { items, fields }] of collections) {
			const counts = options.counts?.[group];
			const prior = options.prior?.[group];
			const expected = options.actual && fields.map((field) => getPath(options.actual, field.target));
			let unique: number | undefined;
			const priorMass = items.reduce<number>((sum, _, i) => sum + (prior?.[i] ?? 0) + 1 / (i + 1) ** 2, 0);
			// Correlated fields come from one row. Repeated equal rows contribute probability to the same action.
			const choices = new Map<string, { values: unknown[]; weight: number }>();
			let total = 0;
			for (const [position, item] of items.entries()) {
				const values = fields.map((field) =>
					field.span ? extractSpan(item, field.span) : getPath(item, field.item),
				);
				if (values.includes(MISSING)) continue;
				if (expected && !sameValue(values, expected)) continue;
				unique = unique === undefined ? position : -1;
				const weight =
					(counts?.[position] ?? 0) + ((prior?.[position] ?? 0) + 1 / (position + 1) ** 2) / priorMass;
				const key = stableStringify(values);
				const entry = choices.get(key);
				if (entry) entry.weight += weight;
				else choices.set(key, { values, weight });
				total += weight;
			}
			if (unique !== undefined && unique >= 0) options.positions?.set(group, unique);
			const ranked = [...choices.values()].sort((a, b) => b.weight - a.weight).slice(0, limit);
			variants = variants
				.flatMap((variant) =>
					ranked.map(({ values, weight }) => ({
						input: fields.reduce(
							(input, field, index) => withPath(input, field.target, values[index]),
							variant.input,
						),
						probability: (variant.probability * weight) / total,
					})),
				)
				.sort((a, b) => b.probability - a.probability)
				.slice(0, limit);
		}
		return variants;
	}
	private *inferCandidateBindings(
		context: readonly PatternAwareEvent[],
		target: unknown,
	): Generator<Exclude<PatternAwareBinding, { type: "constant" }>> {
		const key = stableStringify(target);
		for (const [relativeEvent, field, value] of reverseContextFields(context))
			for (const path of this.valueIndex(value).get(key) ?? []) {
				yield { type: "event" as const, relativeEvent, field, path };
				const index = path.findIndex((segment) => typeof segment === "number");
				if (index >= 0)
					yield {
						type: "each" as const,
						relativeEvent,
						field,
						path: path.slice(0, index),
						itemPath: path.slice(index + 1),
					};
			}
		const numeric = typeof target === "number" && Number.isSafeInteger(target);
		if (!numeric && (typeof target !== "string" || !target.trim())) return;
		const needle = String(target);
		for (let i = context.length - 1; i >= 0; i--) {
			for (const line of context[i]!.textLines ?? []) {
				let span: TextSpan | undefined;
				for (let at = line.indexOf(needle); at >= 0; at = line.indexOf(needle, at + 1)) {
					const left = line[at - 1] ?? "",
						right = line[at + needle.length] ?? "";
					if (!spanDelimiter(left) || !spanDelimiter(right)) continue;
					if (span) {
						span = undefined;
						break;
					}
					const occurrence = left ? line.slice(0, at).split(left).length - 1 : 0;
					span = { left, occurrence, right, number: numeric };
				}
				if (span && extractSpan(line, span) === target)
					yield {
						type: "each",
						relativeEvent: i - context.length,
						field: "textLines",
						path: [],
						itemPath: [],
						span,
					};
			}
		}
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
	const fields = Object.values(bindings);
	return (
		samples.length >= minimum || (fields.length > 0 && fields.every((binding) => binding.type !== "constant"))
	);
}
const MISSING = Symbol("missing");
function* reverseContextFields(
	context: ReadonlyArray<PatternAwareEvent>,
): Generator<readonly [number, PatternAwareDependencySource["field"], unknown]> {
	for (let index = context.length - 1; index >= 0; index--) {
		const event = context[index]!;
		const relativeEvent = index - context.length;
		if (event.outputLocations?.length) yield [relativeEvent, "outputLocations", event.outputLocations];
		if (event.outputPaths?.length) yield [relativeEvent, "outputPaths", event.outputPaths];
		yield [relativeEvent, "input", event.input];
		if (event.output !== undefined) yield [relativeEvent, "output", event.output];
	}
}

function normalizeResourcePath(value: string, resourceRoot?: string) {
	if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) return value;
	const relative = resourceRoot && path.isAbsolute(value) ? relativeFilesystemPath(resourceRoot, value) : undefined;
	return slash(path.normalize(relative ?? value), "\\");
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
		if (!Object.hasOwn(current, segment)) return MISSING;
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
function usesOldest(dependencies: readonly PatternAwareDependency[], length: number) {
	return dependencies.some((entry) => entry.sources.some((source) => source.relativeEvent === -length));
}
function transitionContext(context: readonly PatternAwareEventSignature[], gap: number) {
	return [String(gap), ...context.map(signatureToken)];
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
			key: stableStringify([predictionTool(input), input.outcome, input.operation ?? "", input.input]),
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
function predictionIdentity(
	tool: string,
	input: Readonly<Record<string, unknown>>,
	key: ActionKey | undefined,
) {
	return stableValueHash(key ? { actionKey: key.key } : { tool, input });
}

/** Competing contexts at one gap describe the same event. Distinct gaps contribute timing evidence. */
function mergePredictionGaps(
	byGap: ReadonlyMap<number, PatternAwareCandidate>,
	coverage: number,
	pathProbability: number,
): PatternAwareCandidate {
	if (byGap.size === 1) return byGap.values().next().value!;
	const candidates = [...byGap.values()];
	const foreground = candidates.filter((candidate) => !candidate.background);
	const gaps = foreground.length ? foreground : candidates;
	const best = gaps.reduce((a, b) => (b.confidence > a.confidence ? b : a));
	if (gaps.length === 1) return best;
	gaps.sort((a, b) => a.horizon - b.horizon);
	const total = gaps.reduce((sum, p) => sum + p.conditionalProbability, 0);
	const adoptionProbability = clampProbability(
		gaps.reduce((sum, p) => sum + p.conditionalProbability * p.adoptionProbability, 0) / total,
	);
	const conditionalProbability = clampProbability(total);
	const empiricalProbability = clampProbability(pathProbability * conditionalProbability);
	const latestHorizon = gaps.at(-1)!.horizon;
	let mass = 0;
	let horizon = latestHorizon;
	for (const gap of gaps) {
		mass += gap.conditionalProbability;
		if (mass >= total * coverage) {
			horizon = gap.horizon;
			break;
		}
	}
	return {
		...best,
		horizon,
		latestHorizon,
		conditionalProbability,
		empiricalProbability,
		adoptionProbability,
		confidence: empiricalProbability * adoptionProbability,
		immediateConfidence: gaps[0]!.horizon === 0 ? gaps[0]!.confidence : 0,
		continuation: { ...best.continuation, pathProbability: empiricalProbability },
	};
}

/** Partition this pattern's own probability mass between new and repeated calls, using one prior observation. */
function weightRevisits(
	variants: { identity: string; probability: number }[],
	calls: readonly { identity: string }[],
) {
	if (!calls.length) return;
	const seen = new Set(calls.map((call) => call.identity));
	let repeated = 0;
	let fresh = 0;
	for (const variant of variants) {
		if (seen.has(variant.identity)) repeated += variant.probability;
		else fresh += variant.probability;
	}
	if (!repeated || !fresh) return;
	const total = repeated + fresh;
	const share = (calls.length - seen.size + repeated / total) / (calls.length + 1);
	for (const variant of variants)
		variant.probability *= total * (seen.has(variant.identity) ? share / repeated : (1 - share) / fresh);
}

const rolePrefix = "\0role:";
function predictionTool(event: Pick<PatternAwareEventInput, "tool" | "patternIdentity">) {
	return event.patternIdentity ? rolePrefix + event.patternIdentity : event.tool;
}
const signatureCache = new WeakMap<PatternAwareEvent, PatternAwareEventSignature>();
function signature(event: PatternAwareEvent): PatternAwareEventSignature {
	const cached = signatureCache.get(event);
	if (cached) return cached;
	const outputShape = semanticOutputShape(event.output, event.errorClass);
	const value = {
		tool: predictionTool(event),
		outcome: event.outcome,
		...(event.operation ? { operation: event.operation } : {}),
		...(event.cardinality ? { cardinality: event.cardinality } : {}),
		...(outputShape ? { outputShape } : {}),
	};
	signatureCache.set(event, value);
	return value;
}
function signatureToken(value: PatternAwareEventSignature) {
	return JSON.stringify([value.tool, value.outcome, value.operation, value.cardinality, value.outputShape]);
}
function semanticOutputShape(value: unknown, errorClass?: string) {
	const fields = speculativeConfig().prediction.outputDiscriminants;
	if (!fields.length) return errorClass;
	const discriminants: string[] = errorClass ? [`error:${errorClass}`] : [];
	const visit = (item: unknown, key = "", depth = 0) => {
		if (depth > speculativeConfig().prediction.structuralDepth) return;
		if (
			(typeof item === "string" || typeof item === "number" || typeof item === "boolean") &&
			fields.includes(key)
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
	return stableValueHash(discriminants.sort());
}

function patternEvidenceClock(pattern: MutablePattern): number {
	return Math.max(pattern.lastSeenSequence, pattern.feedback.sequence);
}
function patternRank(pattern: MutablePattern, clock: number, halfLife: number) {
	const feedback = feedbackEvidence(pattern, clock, halfLife);
	return (
		(pattern.occurrences *
			recencyWeight(pattern.lastSeenSequence, clock, halfLife) *
			probability(pattern) *
			(1 + feedback.adopted)) /
		(1 + feedback.adopted + feedback.mismatched + feedback.rejected) /
		(1 + feedback.mismatched + feedback.rejected)
	);
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
function confused(feedback: ReturnType<typeof feedbackEvidence>) {
	const failures = feedback.mismatched + feedback.rejected;
	return failures >= 1.5 && failures > Math.max(0, feedback.matched - feedback.rejected);
}
function patternAdoptionProbability(
	pattern: Pick<MutablePattern, "feedback">,
	clock: number,
	halfLife: number,
) {
	const { adopted, rejected } = feedbackEvidence(pattern, clock, halfLife);
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
		gapCounts: { [pattern.gap]: pattern.occurrences },
		gapLastSeen: { [pattern.gap]: pattern.lastSeenSequence },
		empiricalProbability: probability(pattern),
		adoptionProbability: patternAdoptionProbability(pattern, clock, halfLife),
	};
}
function mutablePattern(value: unknown): MutablePattern | undefined {
	const record = asRecord(value);
	const bindings = asRecord(record?.bindings);
	const feedback = mutablePatternFeedback(record?.feedback);
	if (
		!record ||
		typeof record.id !== "string" ||
		!Array.isArray(record.context) ||
		!record.context.every(isEventSignature) ||
		typeof record.targetTool !== "string" ||
		!bindings ||
		!feedback ||
		!isSafeCount(record.gap) ||
		(record.rowCounts !== undefined &&
			(!asRecord(record.rowCounts) ||
				!Object.values(record.rowCounts as object).every(
					(row: unknown) =>
						Array.isArray(row) &&
						row.length <= speculativeConfig().prediction.maxValueItems &&
						row.every((n) => isSafeCount(n)),
				))) ||
		!isFiniteNumber(record.bindingPrecision) ||
		record.bindingPrecision < 0 ||
		record.bindingPrecision > 1 ||
		![
			record.occurrences,
			record.historicalOpportunities,
			record.historicalStops,
			record.historicalMatches,
			record.lastSeenSequence,
		].every((metric) => isFiniteNumber(metric) && metric >= 0) ||
		(record.targetSchemaHash !== undefined && typeof record.targetSchemaHash !== "string") ||
		!Object.entries(bindings).every(
			([encoded, binding]) => parsePath(encoded) !== undefined && isPatternAwareBinding(binding),
		)
	)
		return;
	const pattern = structuredClone({
		id: record.id,
		context: record.context as PatternAwareEventSignature[],
		targetTool: record.targetTool,
		bindings: bindings as Record<string, PatternAwareBinding>,
		...(typeof record.targetSchemaHash === "string" ? { targetSchemaHash: record.targetSchemaHash } : {}),
		gap: record.gap,
		bindingPrecision: record.bindingPrecision,
		occurrences: Number(record.occurrences),
		historicalOpportunities: Number(record.historicalOpportunities),
		historicalStops: Number(record.historicalStops),
		historicalMatches: Number(record.historicalMatches),
		feedback,
		lastSeenSequence: Number(record.lastSeenSequence),
		...(record.rowCounts ? { rowCounts: record.rowCounts as RowCounts } : {}),
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
				!isSafeCount(sample.target)
			)
				return [];
			const context = sample.context.map((id) => events[id as number]);
			const target = events[sample.target as number];
			if (!target || context.some((event) => !event)) return [];
			return [{ context: context as PatternAwareEvent[], target }];
		});
		if (!samples.length) return [];
		const context = structuredClone(record.context) as PatternAwareEventSignature[];
		const targetSchemaHash = typeof record.targetSchemaHash === "string" ? record.targetSchemaHash : undefined;
		return [
			{
				key: patternPoolKey(context, record.targetTool, targetSchemaHash, record.gap),
				context,
				targetTool: record.targetTool,
				...(targetSchemaHash ? { targetSchemaHash } : {}),
				gap: record.gap,
				samples,
				...(typeof record.patternID === "string" ? { patternID: record.patternID } : {}),
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
	return stableValueHash({ context, targetTool, targetSchemaHash, gap });
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
		(record.cardinality === undefined ||
			["empty", "singleton", "few", "truncated"].includes(String(record.cardinality))) &&
		(record.outputShape === undefined || typeof record.outputShape === "string")
	);
}
function isPatternAwareBinding(value: unknown): value is PatternAwareBinding {
	const record = asRecord(value);
	if (!record) return false;
	if (record.type === "constant") return "value" in record;
	if (record.type !== "event" && record.type !== "each") return false;
	if (record.field === "textLines" || record.span !== undefined) {
		const span = asRecord(record.span);
		return (
			record.type === "each" &&
			record.field === "textLines" &&
			!!span &&
			Number.isSafeInteger(record.relativeEvent) &&
			Number(record.relativeEvent) < 0 &&
			Array.isArray(record.path) &&
			!record.path.length &&
			Array.isArray(record.itemPath) &&
			!record.itemPath.length &&
			spanDelimiter(span.left) &&
			spanDelimiter(span.right) &&
			typeof span.number === "boolean" &&
			isSafeCount(span.occurrence) &&
			span.occurrence <= speculativeConfig().prediction.maxValueCharacters &&
			(span.left === "" ? span.occurrence === 0 : span.occurrence > 0)
		);
	}
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
		Array.isArray(value) &&
		value.every((segment) => (typeof segment === "string" ? !unsafePathSegment(segment) : isSafeCount(segment)))
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
		(record.patternIdentity === undefined || typeof record.patternIdentity === "string") &&
		(record.textLines === undefined ||
			(Array.isArray(record.textLines) &&
				record.textLines.length <= speculativeConfig().prediction.maxValueItems &&
				record.textLines.every((line: unknown) => typeof line === "string") &&
				record.textLines.reduce((sum: number, line: string) => sum + line.length, 0) <=
					speculativeConfig().prediction.maxValueCharacters)) &&
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
/** Keeps a digest of an oversized string and a list's head; an action whose input was shortened is context only, never predicted. */
function boundedEventPayload(event: PatternAwareEventInput): PatternAwareEventInput {
	let shortened = false;
	const bound = (value: unknown): unknown => {
		if (typeof value === "string" && value.length > speculativeConfig().prediction.maxValueCharacters) {
			shortened = true;
			return sha256Digest(value).slice(0, 39);
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
		...(event.textLines ? { textLines: boundedTextRows(event.textLines) } : {}),
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
