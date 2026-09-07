import type { ActionKey, ActionKeyProjector, ProjectedActionKeyMatch } from "./action-semantics.ts";

export { READ_RANGE_ACTION_KEY_PROJECTOR, readRangesShareInFlight } from "./action-semantics.ts";

export interface ActionProjectionCoverage {
	readonly rule: string;
	readonly value: unknown;
}

/** A key relation uses either branch-owned inputs or optional lossless output coverage. */
export interface ActionProjectionRule<Output> extends ActionKeyProjector {
	/** Optional covering K(a) to execute; the runtime accepts it only when this rule proves containment. */
	readonly coveringAction?: (predicted: ActionKey) => ActionKey | undefined;
	readonly captureCoverage?: (action: ActionKey, output: Output) => unknown | undefined;
	readonly projectOutput?: (input: {
		readonly speculative: ActionKey;
		readonly actor: ActionKey;
		readonly output: Output;
		readonly coverage: unknown;
		readonly keyMatch: ProjectedActionKeyMatch;
	}) => Output | undefined | Promise<Output | undefined>;
}

/** In-memory-only metadata; symbol keys never leak into persisted tool-result details. */
export const READ_RANGE_COVERAGE_DETAILS_KEY: unique symbol = Symbol("pi.speculative.readRange");

/** Compact descriptor for the text prefix already present in Pi's read output. */
export interface ReadRangeCoverage {
	readonly kind: "text";
	readonly startLine: number;
	readonly endLineExclusive: number;
	readonly totalLines: number;
	/** UTF-16 length of the file payload before any continuation notice. */
	readonly payloadTextLength: number;
	readonly maxLines: number;
	readonly maxBytes: number;
}
