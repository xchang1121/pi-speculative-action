import path from "node:path";
import { serialize } from "node:v8";
import {
	actionKeyMatch,
	type ActionEffect,
	type ActionKey,
	type ActionKeyMatch,
} from "./action-semantics.ts";
import { speculativeConfig } from "./config-schema.ts";
import type {
	ExecutionResourceContext,
	SpeculativeExecutionRoute,
	WorldBranch,
	WorldCompatibilityEvidence,
} from "./execution-world.ts";
import { effectCommitFailure, isPoisonedEffectCommit, validateWorldBranch } from "./execution-world.ts";
import type {
	ExecutionCall,
	ExecutionSession,
	ExecuteResult,
	RunCompletion,
	SpeculativeRun,
} from "./execution-session.ts";
import {
	cloneSharedData,
	errorDetail,
	filesystemPathKey,
	nonNegativeFinite as finiteValue,
	positiveCount,
} from "./primitives.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import {
	CandidateExecution,
	cause,
	type AuthoritativeResultCapture,
	type CandidatePreflight,
	type CandidateRejection,
	type ResolutionCause,
	type SpeculativeActionSettings,
	type SpeculativeCacheSnapshot,
} from "./settlement.ts";
import { TimelineInterval, type TimelineDependency } from "./task-timing.ts";
interface CandidateStoreEntry {
	readonly id: string;
	readonly key?: ActionKey;
	readonly estimatedBytes: number;
}
interface IndexedEntry<Entry> {
	readonly entry: Entry;
	readonly memberships: Set<string>;
	inputs?: boolean;
	recency: number;
	segment?: "cold" | "hot";
}
interface ResultCacheLimits {
	readonly maxEntries: number;
	readonly maxBytes: number;
}
const inputPartition = (resource: string, descendants = false) =>
	`inputs:${descendants ? "tree" : "path"}:` + filesystemPathKey(resource);

/** Exact calls, reusable inputs and already reconstructed results share one session-local index. */
class CandidateStore<Entry extends CandidateStoreEntry> {
	private readonly entries = new Map<string, IndexedEntry<Entry>>();
	private readonly partitions = new Map<string, Set<IndexedEntry<Entry>>>();
	private recency = 0;
	private record(entry: Entry) {
		const indexed = this.entries.get(entry.id);
		return indexed?.entry === entry ? indexed : undefined;
	}
	has(entry: Entry) {
		return !!this.record(entry);
	}
	values() {
		return [...this.entries.values()].map(({ entry }) => entry);
	}
	cached() {
		return [...this.entries.values()].filter((row) => row.segment).map(({ entry }) => entry);
	}
	private index(row: IndexedEntry<Entry>, name: string) {
		if (row.memberships.has(name)) return;
		let members = this.partitions.get(name);
		if (!members) this.partitions.set(name, (members = new Set()));
		members.add(row);
		row.memberships.add(name);
	}
	private unindex(row: IndexedEntry<Entry>, name: string) {
		if (!row.memberships.delete(name)) return;
		const members = this.partitions.get(name)!;
		members.delete(row);
		if (!members.size) this.partitions.delete(name);
	}
	insert(entry: Entry) {
		let row = this.entries.get(entry.id);
		if (!row) {
			row = { entry, memberships: new Set(), recency: ++this.recency };
			this.entries.set(entry.id, row);
			if (entry.key) this.index(row, `exact:${entry.key.key}`);
		}
		return row;
	}
	delete(entry: Entry) {
		const row = this.record(entry);
		if (!row) return;
		this.entries.delete(entry.id);
		for (const name of row.memberships) this.unindex(row, name);
	}
	settle(entry: Entry, branch: WorldBranch<unknown>, shared: boolean) {
		const row = this.insert(entry);
		row.segment ??= "cold";
		if (shared && (branch.inputSource || branch.reconstruct)) {
			row.inputs = true;
			for (const input of branch.inputResources ?? [])
				this.index(row, inputPartition(input.path, input.descendants));
		}
	}
	indexView(entry: Entry, key: string, retained: boolean) {
		const row = this.record(entry);
		if (!row) return;
		if (retained) this.index(row, `view:${key}`);
		else this.unindex(row, `view:${key}`);
	}
	invalidateInputs(entry: Entry, paths: readonly string[]) {
		const row = this.record(entry);
		if (!row) return;
		for (const path of paths) {
			this.unindex(row, inputPartition(path));
			this.unindex(row, inputPartition(path, true));
		}
	}
	private inputRecords(resources?: readonly string[]) {
		const distances = new Map<IndexedEntry<Entry>, number>();
		const range = speculativeConfig().runtime.inputDistanceRange;
		for (const row of this.entries.values()) if (row.inputs) distances.set(row, range);
		for (const resource of resources ?? []) {
			let current = path.resolve(resource);
			for (let depth = 0; ; depth++) {
				const keys = depth
					? [inputPartition(current, true)]
					: [inputPartition(current), inputPartition(current, true)];
				for (const key of keys)
					for (const row of this.partitions.get(key) ?? [])
						distances.set(row, Math.min(distances.get(row) ?? range, depth));
				const parent = path.dirname(current);
				if (parent === current) break;
				current = parent;
			}
		}
		return distances;
	}
	lookupInputs(resources?: readonly string[]) {
		return [...this.inputRecords(resources)]
			.sort(([a, x], [b, y]) => x - y || b.recency - a.recency)
			.map(([{ entry }]) => entry);
	}
	lookup(action: ActionKey, includeInputs = true) {
		const paths = action.resources.flatMap((resource) =>
			action.resourceRoot !== undefined || path.isAbsolute(resource)
				? [path.resolve(action.resourceRoot ?? "", resource)]
				: [],
		);
		const inputs = includeInputs ? this.inputRecords(paths) : new Map<IndexedEntry<Entry>, number>();
		const views = this.partitions.get(`view:${action.key}`);
		const candidates = new Set([
			...(this.partitions.get(`exact:${action.key}`) ?? []),
			...(views ?? []),
			...inputs.keys(),
		]);
		const range = speculativeConfig().runtime.inputDistanceRange;
		const result: { entry: Entry; match: ActionKeyMatch; recency: number }[] = [];
		for (const row of candidates) {
			const match =
				(row.entry.key && actionKeyMatch(row.entry.key, action)) ??
				(views?.has(row) || inputs.has(row)
					? { kind: "inputs" as const, distance: Number.MAX_SAFE_INTEGER - range + (inputs.get(row) ?? 0) }
					: undefined);
			if (match) result.push({ entry: row.entry, match, recency: row.recency });
		}
		return result.sort((a, b) => a.match.distance - b.match.distance || b.recency - a.recency);
	}
	recordHit(entry: Entry, limits: ResultCacheLimits) {
		const row = this.record(entry);
		if (!row?.segment) return;
		row.segment = "hot";
		row.recency = ++this.recency;
		const fraction = speculativeConfig().runtime.hotFraction;
		this.retireExcess(
			[...this.entries.values()].filter((item) => item.segment === "hot"),
			{
				maxEntries: limits.maxEntries && fraction ? Math.max(1, Math.floor(limits.maxEntries * fraction)) : 0,
				maxBytes: Math.floor(limits.maxBytes * fraction),
			},
			(item) => {
				item.segment = "cold";
			},
		);
	}
	hotEntries() {
		let count = 0;
		for (const row of this.entries.values()) if (row.segment === "hot") count++;
		return count;
	}
	trim(limits: ResultCacheLimits, canEvict: (entry: Entry) => boolean) {
		const retired: Entry[] = [];
		this.retireExcess(
			[...this.entries.values()].filter((row) => row.segment),
			limits,
			(row) => {
				this.delete(row.entry);
				retired.push(row.entry);
			},
			canEvict,
		);
		return retired;
	}
	private retireExcess(
		rows: IndexedEntry<Entry>[],
		limits: ResultCacheLimits,
		retire: (row: IndexedEntry<Entry>) => void,
		canEvict?: (entry: Entry) => boolean,
	) {
		let count = rows.length;
		let bytes = rows.reduce((sum, row) => sum + finiteValue(row.entry.estimatedBytes), 0);
		if (count <= limits.maxEntries && bytes <= limits.maxBytes) return;
		rows.sort((a, b) => Number(a.segment === "hot") - Number(b.segment === "hot") || a.recency - b.recency);
		for (const row of rows) {
			if (count <= limits.maxEntries && bytes <= limits.maxBytes) break;
			if (canEvict && !canEvict(row.entry)) continue;
			count--;
			bytes -= finiteValue(row.entry.estimatedBytes);
			retire(row);
		}
	}
}
function estimateValueBytes(value: unknown): number {
	try {
		return serialize(value).byteLength;
	} catch {
		return Number.MAX_SAFE_INTEGER;
	}
}
function inputIndexBytes(branch: WorldBranch<unknown>): number {
	return estimateValueBytes(branch.inputResources ?? []);
}
interface ViewResource {
	readonly dispose: () => void | Promise<void>;
	references: number;
}
type ResultView<Output> =
	| {
			readonly ok: true;
			readonly output: Output;
			readonly execution?: TimelineInterval;
			readonly reused?: true;
			readonly compatibility?: WorldBranch<Output>["compatibility"];
			readonly validate?: WorldBranch<Output>["validate"];
			readonly capturedBytes?: number;
			readonly requiresQueryValidation?: true;
			readonly resource?: ViewResource;
	  }
	| {
			readonly ok: false;
			readonly cause: ResolutionCause;
	  };
type RetainedResultView<Output> = Omit<
	Extract<
		ResultView<Output>,
		{
			ok: true;
		}
	>,
	"ok" | "execution" | "reused"
> & {
	readonly bytes: number;
	readonly execution: TimelineInterval;
};

interface Entry<Output> extends CandidateStoreEntry {
	call: ExecutionCall;
	route: SpeculativeExecutionRoute;
	work: CandidateExecution<WorldBranch<Output>>;
	parent?: Entry<Output>;
	handles: Set<string>;
	pins: number;
	adopted: boolean;
	retained: boolean;
	inputSource?: object;
	estimatedBytes: number;
	resultViews?: Map<string, RetainedResultView<Output>>;
}
function candidateBranch<Output>(entry: Entry<Output>) {
	const execution = entry.work.execution;
	return execution.status === "succeeded" ? execution.output : undefined;
}
function compatible(evidence: WorldCompatibilityEvidence, key: ActionKey) {
	return evidence.status === "compatible" && evidence.executionFingerprint === key.executionFingerprint;
}
async function resolveResultView<Output>(
	candidate: Entry<Output>,
	actor: ActionKey,
	match: ActionKeyMatch,
	request: Parameters<NonNullable<WorldBranch<Output>["reconstruct"]>>[0],
	mode: "cached" | "output" | "inputs" = "cached",
): Promise<ResultView<Output>> {
	const branch = candidateBranch(candidate)!;
	if (branch.inputsOnly && match.kind !== "inputs" && mode !== "inputs")
		return { ok: false, cause: cause("reconstruction", "input_only_branch") };
	const retained = mode === "cached" && candidate.resultViews?.get(actor.key);
	// A view kept without its proof is served only by a branch that can still commit and validate as a whole.
	if (retained && (retained.validate || !branch.inputsOnly)) {
		const output = cloneSharedData(retained.output);
		if (retained.resource) retained.resource.references++;
		return { ok: true, ...retained, output, reused: true };
	}
	if (!branch.inputsOnly && match.kind === "exact" && mode !== "inputs")
		return { ok: true, output: branch.output };
	let rebuilt: Awaited<ReturnType<NonNullable<typeof branch.reconstruct>>>;
	try {
		const { computation: execution } = await TimelineInterval.measure(async () => {
			rebuilt = await branch.reconstruct?.(request);
		});
		if ((rebuilt?.requiresQueryValidation || mode === "inputs") && !rebuilt?.validate)
			return { ok: false, cause: cause("reconstruction", "query_proof_missing") };
		if (rebuilt?.output === undefined)
			return { ok: false, cause: cause("reconstruction", "view_not_covered") };
		const resource = rebuilt.dispose ? { dispose: rebuilt.dispose.bind(rebuilt), references: 1 } : undefined;
		const result: ResultView<Output> = {
			ok: true,
			output: rebuilt.output,
			execution,
			compatibility: rebuilt.compatibility,
			validate: rebuilt.validate,
			capturedBytes: rebuilt.capturedBytes,
			requiresQueryValidation: rebuilt.requiresQueryValidation,
			resource,
		};
		rebuilt = undefined;
		return result;
	} catch (error) {
		return {
			ok: false,
			cause: cause("reconstruction", "reconstruction_failed", errorDetail(error)),
		};
	} finally {
		if (rebuilt?.dispose)
			try {
				await rebuilt.dispose();
			} catch {
				/* A discarded reconstruction cannot change the selected result. */
			}
	}
}

export interface LocalExecutionHooks<Output> {
	settings(): Pick<
		SpeculativeActionSettings,
		"maxConcurrentActions" | "resourceCacheMaxEntries" | "resourceCacheMaxBytes" | "reuseEnabled"
	>;
	effect?(key: ActionKey): ActionEffect | undefined;
	key(call: ExecutionCall, actor: boolean): Promise<ActionKey | undefined>;
	route(
		call: ExecutionCall,
		key: ActionKey,
		signal: AbortSignal,
		parent?: WorldBranch<Output>,
	): Promise<SpeculativeExecutionRoute | undefined>;
	authorize(
		call: ExecutionCall,
		key: ActionKey,
		route: SpeculativeExecutionRoute,
		signal: AbortSignal,
		recheck: boolean,
	): Promise<CandidatePreflight>;
	fork(
		call: ExecutionCall,
		key: ActionKey,
		route: SpeculativeExecutionRoute,
		runID: string,
		signal: AbortSignal,
		resources: ExecutionResourceContext,
		parent?: WorldBranch<Output>,
	): Promise<WorldBranch<Output>>;
	capture(
		call: ExecutionCall,
		key: ActionKey | undefined,
		signal: AbortSignal,
	): Promise<AuthoritativeResultCapture<Output> | undefined>;
	native(
		call: ExecutionCall,
		key: ActionKey | undefined,
		signal: AbortSignal,
		resources: ExecutionResourceContext,
		dependencies: TimelineDependency[],
	): Promise<Output>;
	reject?(output: Output): boolean;
	writes?(paths: readonly string[]): void;
	observed?(call: ExecutionCall, key: ActionKey, branch?: WorldBranch<Output>): Promise<void>;
	event?(
		call: ExecutionCall,
		id: string,
		route: SpeculativeExecutionRoute,
		state: "running" | "succeeded" | "failed" | "cancelled",
		ms: number,
		branch?: WorldBranch<Output>,
		failure?: ResolutionCause,
	): void;
	rejection?(call: ExecutionCall, rejection: CandidateRejection): void;
}
interface Handle<Output> {
	id: string;
	controller: AbortController;
	parentUsers: number;
	released: boolean;
	settled?: boolean;
	entry?: Entry<Output>;
	completion: Promise<RunCompletion<Output>>;
	detach?: () => void;
}
/** The same resource owners serve authoritative and private executions. Handles own interest in work. */
export class LocalExecutionSession<Output> implements ExecutionSession<Output> {
	private readonly pool = new CandidateStore<Entry<Output>>();
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly controller = new AbortController();
	private readonly handles = new Map<string, Handle<Output>>();
	private readonly captures = new Set<object>();
	private readonly capturing = new Map<string, Promise<void>>();
	private readonly admitting = new Map<string, Set<Promise<void>>>();
	private readonly running = new Set<Entry<Output>>();
	private sequence = 0;
	private readonly hooks: LocalExecutionHooks<Output>;
	constructor(hooks: LocalExecutionHooks<Output>) {
		this.hooks = hooks;
	}
	private limits() {
		const settings = this.hooks.settings();
		return {
			maxEntries: settings.reuseEnabled === false ? 0 : positiveCount(settings.resourceCacheMaxEntries),
			maxBytes: settings.resourceCacheMaxBytes ?? speculativeConfig().runtime.resourceCacheMaxBytes,
		};
	}
	private atCapacity() {
		return (
			this.running.size >=
			(this.hooks.settings().maxConcurrentActions ?? speculativeConfig().runtime.maxConcurrentActions)
		);
	}
	private nextID() {
		return "run-" + ++this.sequence;
	}
	private remove(entry: Entry<Output>) {
		this.pool.delete(entry);
		for (const key of entry.resultViews?.keys() ?? []) this.setView(entry, key);
		for (const id of entry.handles) {
			const handle = this.handles.get(id);
			handle?.detach?.();
			this.handles.delete(id);
		}
		entry.handles.clear();
		void this.lifetime.release(candidateBranch(entry));
	}
	private trim() {
		for (const entry of this.pool.trim(
			this.limits(),
			(entry) =>
				!entry.pins &&
				!this.running.has(entry) &&
				![...entry.handles].some((id) => this.handles.get(id)?.parentUsers),
		))
			this.remove(entry);
	}
	private trimHandles() {
		const capacity = this.limits().maxEntries + this.running.size;
		if (this.handles.size <= capacity) return;
		for (const handle of this.handles.values()) {
			if (!handle.settled || handle.parentUsers) continue;
			if (handle.entry && !handle.released) handle.entry.retained = true;
			this.handles.delete(handle.id);
			handle.entry?.handles.delete(handle.id);
			if (this.handles.size <= capacity) break;
		}
	}
	private releaseView(resource?: ViewResource) {
		if (resource && --resource.references === 0) void this.lifetime.release(resource);
	}
	private setView(entry: Entry<Output>, key: string, view?: RetainedResultView<Output>) {
		const prior = entry.resultViews?.get(key);
		if (view?.resource) view.resource.references++;
		if (view) (entry.resultViews ??= new Map()).set(key, view);
		else entry.resultViews?.delete(key);
		entry.estimatedBytes += (view?.bytes ?? 0) - (prior?.bytes ?? 0);
		this.pool.indexView(entry, key, !!view);
		this.releaseView(prior?.resource);
	}
	private retainView(entry: Entry<Output>, key: ActionKey, view: Extract<ResultView<Output>, { ok: true }>) {
		if (!view.execution || view.reused) return;
		const limits = this.limits();
		const output = cloneSharedData(view.output);
		const bytes = estimateValueBytes(output) + Buffer.byteLength(key.key) + finiteValue(view.capturedBytes);
		this.setView(entry, key.key);
		while (
			entry.resultViews?.size &&
			(entry.resultViews.size >= limits.maxEntries || entry.estimatedBytes + bytes > limits.maxBytes)
		)
			this.setView(entry, entry.resultViews.keys().next().value!);
		if (limits.maxEntries && entry.estimatedBytes + bytes <= limits.maxBytes)
			this.setView(entry, key.key, { ...view, execution: view.execution, output, bytes });
	}
	private borrow(exclude?: Entry<Output>) {
		const leases = new Set<Entry<Output>>();
		const session = this;
		return {
			lookup: function* (path?: string): Iterable<object> {
				if (!session.limits().maxEntries) return;
				for (const entry of [...session.pool.lookupInputs(path ? [path] : undefined), ...session.running]) {
					if (entry === exclude || entry.parent || entry.route.reuse !== "shared_result") continue;
					const source = candidateBranch(entry)?.inputSource ?? entry.inputSource;
					if (!source) continue;
					if (!leases.has(entry)) {
						entry.pins++;
						leases.add(entry);
					}
					yield source;
				}
				yield* session.captures;
			},
			release: () => {
				for (const entry of leases) entry.pins--;
				leases.clear();
				this.trim();
			},
		};
	}
	private async view(
		entry: Entry<Output>,
		call: ExecutionCall,
		key: ActionKey,
		match: ActionKeyMatch,
		signal: AbortSignal,
	) {
		const branch = candidateBranch(entry)!;
		const inputs = this.borrow(entry);
		try {
			let mode: "cached" | "output" | "inputs" =
				branch.inputsOnly && match.kind === "exact" ? "inputs" : "cached";
			while (!signal.aborted) {
				const result = await resolveResultView(
					entry,
					key,
					mode === "inputs" ? { kind: "inputs", distance: match.distance } : match,
					{
						action: key,
						args: call.input,
						callID: call.id ?? key.hash,
						signal,
						inputs: inputs.lookup,
					},
					mode,
				);
				if (!result.ok) return result;
				const evidence =
					match.kind === "inputs" && branch.reconstructionScope === "current_action"
						? (result.compatibility ?? branch.compatibility)
						: branch.compatibility;
				if (!compatible(evidence, key)) {
					this.releaseView(result.resource);
					return { ok: false as const, cause: cause("compatibility", "execution_fingerprint_changed") };
				}
				const proof = await validateWorldBranch(
					result.validate ? { validate: result.validate } : branch,
					entry.route.reuse,
				);
				if (proof.status === "valid") return result;
				this.releaseView(result.resource);
				if (proof.status === "stale" && result.reused) {
					this.setView(entry, key.key);
					mode = "output";
				} else if (
					proof.status === "stale" &&
					mode !== "inputs" &&
					branch.reconstructionScope === "current_action" &&
					entry.route.reuse === "shared_result" &&
					(this.hooks.effect?.(key) ?? key.semantics?.effect) === "observation"
				)
					mode = "inputs";
				else return { ok: false as const, cause: proof.cause };
			}
			return { ok: false as const, cause: cause("control", "aborted") };
		} finally {
			inputs.release();
		}
	}
	private attach(handle: Handle<Output>, entry: Entry<Output>) {
		handle.entry = entry;
		entry.handles.add(handle.id);
	}
	private makeEntry(
		call: ExecutionCall,
		key: ActionKey | undefined,
		route: SpeculativeExecutionRoute,
		id = this.nextID(),
	): Entry<Output> {
		return {
			call,
			id,
			key,
			route,
			work: new CandidateExecution(route.reuse === "exclusive_branch" ? "exclusive" : "shared"),
			handles: new Set(),
			pins: 0,
			estimatedBytes: 0,
			adopted: false,
			retained: false,
		};
	}
	private retain(entry: Entry<Output>, branch: WorldBranch<Output>, execution: TimelineInterval) {
		entry.estimatedBytes = estimateValueBytes(branch.output) + inputIndexBytes(branch) + branch.capturedBytes;
		entry.work.succeed(branch, execution, execution.completedAt - execution.startedAt);
		this.pool.settle(entry, branch, entry.route.reuse === "shared_result");
	}
	private async keepInputs(
		entry: Entry<Output>,
		branch: WorldBranch<Output>,
		timing: TimelineInterval,
		committed: boolean,
	) {
		const limits = this.limits();
		if (!limits.maxEntries || !limits.maxBytes || entry.parent) return;
		const inputs = await (
			committed ? branch.takeCommittedInputs?.(limits.maxBytes) : branch.takeReadInputs?.(limits.maxBytes)
		)?.catch(() => undefined);
		if (!inputs) return;
		const owner = this.makeEntry(entry.call, undefined, {
			...entry.route,
			backend: inputs.backend,
			isolation: "resource_snapshot",
			reuse: "shared_result",
		});
		owner.work.start(timing.startedAt);
		owner.retained = true;
		this.retain(owner, inputs, timing);
	}
	private recheckWrites(key: ActionKey, branch?: WorldBranch<Output>) {
		if ((this.hooks.effect?.(key) ?? key.semantics?.effect) === "observation") return;
		const paths = (branch?.resources ?? key.resources).flatMap((value) =>
			key.resourceRoot || path.isAbsolute(value) ? [path.resolve(key.resourceRoot ?? "", value)] : [],
		);
		if (!paths.length) return;
		this.hooks.writes?.(paths);
		for (const entry of this.pool.cached()) {
			const revoked = candidateBranch(entry)?.invalidateInputs?.(paths);
			if (revoked?.length) this.pool.invalidateInputs(entry, revoked);
		}
	}
	private async run(entry: Entry<Output>, parent?: WorldBranch<Output>) {
		const { call, key, route, id, work } = entry;
		const started = performance.now();
		const inputs = this.borrow(entry);
		let branch: WorldBranch<Output> | undefined;
		try {
			work.start(started);
			this.hooks.event?.(call, id, route, "running", 0);
			const evaluation = await TimelineInterval.measure(
				() =>
					this.hooks.fork(
						call,
						key!,
						route,
						id,
						work.controller.signal,
						{
							inputs: !parent ? inputs.lookup : undefined,
							onInputSource: (source) => {
								entry.inputSource = source;
							},
						},
						parent,
					),
				(b) => b.computationDependencies ?? [],
				{ id, source: "speculation" },
			);
			branch = evaluation.output;
			work.controller.signal.throwIfAborted();
			if (branch.inputsOnly || this.hooks.reject?.(branch.output)) {
				await this.keepInputs(entry, branch, evaluation.computation, false);
				throw new Error("output_rejected");
			}
			if (route.reuse === "exclusive_branch") {
				await this.keepInputs(entry, branch, evaluation.computation, false);
				work.controller.signal.throwIfAborted();
			}
			this.retain(entry, branch, evaluation.computation);
			this.hooks.event?.(call, id, route, "succeeded", performance.now() - started, branch);
		} catch (error) {
			const failure = cause("execution", "candidate_failed", errorDetail(error));
			work.fail(failure, performance.now() - started);
			this.hooks.event?.(
				call,
				id,
				route,
				work.controller.signal.aborted ? "cancelled" : "failed",
				performance.now() - started,
				undefined,
				failure,
			);
			if (branch && !candidateBranch(entry)) await this.lifetime.release(branch);
			this.remove(entry);
		} finally {
			this.running.delete(entry);
			entry.inputSource = undefined;
			inputs.release();
		}
	}
	private async select(
		call: ExecutionCall,
		key: ActionKey,
		signal: AbortSignal,
		actor: boolean,
		parent?: Entry<Output>,
		handle?: Handle<Output>,
	) {
		if (!this.limits().maxEntries) return;
		for (const { entry, match } of this.pool.lookup(key, actor)) {
			if (signal.aborted) return;
			let base = entry.parent;
			if (!parent) while (base?.adopted) base = base.parent;
			if (entry.adopted && entry.route.reuse === "exclusive_branch") continue;
			if (base !== parent || (parent && match.kind === "inputs")) continue;
			entry.pins++;
			let lease: ReturnType<typeof entry.work.acquire>;
			let resultView: Extract<ResultView<Output>, { ok: true }> | undefined;
			try {
				const authorization = await this.hooks.authorize(call, key, entry.route, signal, true);
				if (signal.aborted || !authorization.ok || !this.pool.has(entry)) {
					if (!authorization.ok)
						this.hooks.rejection?.(call, {
							candidateID: entry.id,
							match,
							cause: cause("authorization", authorization.reason),
						});
					continue;
				}
				if (actor) {
					lease = entry.work.acquire(this.nextID());
					if (!lease) continue;
				}
				const waiting = await waitForCompletion(entry.work.completion, signal);
				if (waiting.status !== "completed" || waiting.value.status !== "succeeded" || signal.aborted)
					continue;
				const execution = waiting.value;
				const view = await this.view(entry, call, key, match, signal);
				if (!view.ok) {
					this.hooks.rejection?.(call, { candidateID: entry.id, match, cause: view.cause });
					continue;
				}
				resultView = view;
				signal.throwIfAborted();
				let output = view.output;
				if (actor && !view.validate) {
					try {
						const committed = await execution.output.commit();
						if (match.kind === "exact") output = committed;
					} catch (error) {
						if (isPoisonedEffectCommit(effectCommitFailure(error, "poisoned"))) throw error;
						continue;
					}
				}
				if (actor && entry.route.reuse === "exclusive_branch") {
					lease!.adopt();
					entry.adopted = true;
					this.recheckWrites(key, execution.output);
					await this.keepInputs(entry, execution.output, execution.toolExecution, true);
					for (const key of entry.resultViews?.keys() ?? []) this.setView(entry, key);
					this.pool.delete(entry);
				} else {
					if (actor) entry.retained = true;
					if (handle) this.attach(handle, entry);
					try {
						this.retainView(entry, key, view);
					} catch {
						/* An opaque result cannot be retained. */
					}
					this.pool.recordHit(entry, this.limits());
				}
				if (actor) await this.hooks.observed?.(call, key, execution.output).catch(() => {});
				return {
					output,
					served: true,
					reuseKind: view.execution ? ("inputs" as const) : match.kind,
					producerRunID: entry.id,
					toolExecution: view.execution ?? execution.toolExecution,
					reusedInterval: !view.execution || !!view.reused,
				};
			} finally {
				this.releaseView(resultView?.resource);
				lease?.release();
				entry.pins--;
				if (!entry.pins && !entry.handles.size && this.running.has(entry)) entry.work.controller.abort();
				if (entry.adopted && !this.pool.has(entry) && !entry.pins)
					await this.lifetime.release(candidateBranch(entry));
				this.trim();
			}
		}
	}

	speculate(
		call: ExecutionCall,
		options: { after?: string; signal?: AbortSignal } = {},
	): SpeculativeRun<Output> {
		const id = this.nextID();
		const controller = new AbortController();
		const parent = options.after ? this.handles.get(options.after) : undefined;
		const handle: Handle<Output> = {
			id,
			controller,
			parentUsers: 0,
			released: false,
			completion: Promise.resolve({ status: "cancelled", reason: "closed" }),
		};
		if (
			this.lifetime.sealed ||
			this.controller.signal.aborted ||
			options.signal?.aborted ||
			(options.after && (!parent || parent.released))
		)
			return {
				runID: id,
				completion: Promise.resolve({
					status: "rejected",
					reason: options.after ? "parent_unavailable" : "cancelled",
				}),
			};
		this.handles.set(id, handle);
		if (parent) parent.parentUsers++;
		const abort = () => this.release(id);
		options.signal?.addEventListener("abort", abort, { once: true });
		handle.detach = () => options.signal?.removeEventListener("abort", abort);
		const signal = AbortSignal.any([controller.signal, this.controller.signal]);
		const admission = Promise.withResolvers<void>();
		let waitingKey: string | undefined;
		let pinned: Entry<Output> | undefined;
		const admitted = () => {
			admission.resolve();
			if (waitingKey === undefined) return;
			const waiters = this.admitting.get(waitingKey);
			waiters?.delete(admission.promise);
			if (!waiters?.size) this.admitting.delete(waitingKey);
			waitingKey = undefined;
		};
		handle.completion = this.lifetime.track(
			(async (): Promise<RunCompletion<Output>> => {
				try {
					const key = await this.hooks.key(call, false);
					signal.throwIfAborted();
					if (!key) return { status: "rejected", reason: "action_not_keyable" };
					let base: Entry<Output> | undefined;
					if (parent) {
						if (!parent.settled && this.atCapacity())
							return { status: "rejected", reason: "concurrency_limit" };
						const ready = await waitForCompletion(parent.completion, signal);
						if (ready.status !== "completed" || ready.value.status !== "completed" || !parent.entry)
							return { status: "rejected", reason: "parent_unavailable" };
						base = parent.entry;
						while (base && (base.route.reuse === "shared_result" || base.adopted)) base = base.parent;
						if (base) {
							pinned = base;
							pinned.pins++;
						}
					}
					if (!base) {
						const opening = this.capturing.get(key.key);
						if (opening) await waitForCompletion(opening, signal);
						waitingKey = key.key;
						const waiting = this.admitting.get(key.key) ?? new Set();
						waiting.add(admission.promise);
						this.admitting.set(key.key, waiting);
					}
					const seen = new Set(this.pool.lookup(key, false).map(({ entry }) => entry));
					const joined = await this.select(call, key, signal, false, base, handle);
					if (joined) return { status: "completed", ...joined };
					signal.throwIfAborted();
					if (
						this.atCapacity() &&
						!this.pool.lookup(key, false).some(({ entry }) => entry.parent === base && !seen.has(entry))
					)
						return { status: "rejected", reason: "concurrency_limit" };
					const branch = base && candidateBranch(base);
					const route = await this.hooks.route(call, key, signal, branch);
					signal.throwIfAborted();
					if (!route || (base && (!branch?.checkpoint || route.reuse === "shared_result")))
						return { status: "rejected", reason: "execution_route_missing" };
					const permission = await this.hooks.authorize(call, key, route, signal, false);
					signal.throwIfAborted();
					if (!permission.ok) return { status: "rejected", reason: permission.reason };
					signal.throwIfAborted();
					// An authorized duplicate may have finished during route preparation. Validate its sealed result.
					while (true) {
						const fresh = this.pool
							.lookup(key, false)
							.filter(
								({ entry }) =>
									entry.parent === base && !seen.has(entry) && entry.work.execution.status === "succeeded",
							);
						if (!fresh.length) break;
						for (const { entry } of fresh) seen.add(entry);
						const hit = await this.select(call, key, signal, false, base, handle);
						if (hit) return { status: "completed", ...hit };
						signal.throwIfAborted();
					}
					const existing = this.pool
						.lookup(key, false)
						.find(
							({ entry }) =>
								entry.parent === base &&
								entry.work.execution.status === "running" &&
								entry.route.fingerprint === route.fingerprint,
						)?.entry;
					let entry = existing;
					if (!entry) {
						if (this.atCapacity()) return { status: "rejected", reason: "concurrency_limit" };
						entry = this.makeEntry(call, key, route, id);
						entry.parent = base;
						this.pool.insert(entry);
						this.running.add(entry);
						this.attach(handle, entry);
						this.lifetime.track(this.run(entry, branch));
					} else this.attach(handle, entry);
					admitted();
					const ready = await waitForCompletion(entry.work.completion, signal);
					if (ready.status !== "completed") return { status: "cancelled", reason: "released" };
					const done = ready.value;
					signal.throwIfAborted();
					if (done.status !== "succeeded") return { status: "rejected", reason: done.cause.code };
					if (done.output.inputsOnly) return { status: "rejected", reason: "inputs_only" };
					return {
						status: "completed",
						output: done.output.output,
						toolExecution: done.toolExecution,
						served: !!existing,
						reuseKind: existing && "exact",
						producerRunID: entry.id,
					};
				} catch (error) {
					return {
						status: signal.aborted ? "cancelled" : "rejected",
						reason: signal.aborted ? "released" : errorDetail(error),
					};
				} finally {
					admitted();
					if (pinned && --pinned.pins === 0 && pinned.adopted && !this.pool.has(pinned))
						await this.lifetime.release(candidateBranch(pinned));
					handle.detach?.();
					handle.settled = true;
					this.trimHandles();
					if (!handle.entry) this.release(id);
					if (parent && --parent.parentUsers === 0 && parent.released) this.release(parent.id);
				}
			})(),
		);
		return { runID: id, completion: handle.completion };
	}
	execute(call: ExecutionCall, options: { signal?: AbortSignal } = {}): Promise<ExecuteResult<Output>> {
		return this.lifetime.admit(async () => {
			const signal = options.signal
				? AbortSignal.any([options.signal, this.controller.signal])
				: this.controller.signal;
			signal.throwIfAborted();
			const key = await this.hooks.key(call, true);
			while (key && this.limits().maxEntries) {
				signal.throwIfAborted();
				const seen = new Set(this.pool.lookup(key, false).map(({ entry }) => entry));
				const selected = await this.select(call, key, signal, true);
				if (selected) return selected;
				const opening = this.capturing.get(key.key);
				const admissions = [...(this.admitting.get(key.key) ?? [])];
				if (opening) admissions.push(opening);
				if (admissions.length) {
					await waitForCompletion(Promise.all(admissions), signal);
					continue;
				}
				if (!this.pool.lookup(key, false).some(({ entry }) => !seen.has(entry))) break;
			}
			signal.throwIfAborted();
			const id = this.nextID();
			const started = performance.now();
			const ready = Promise.withResolvers<void>();
			if (key) this.capturing.set(key.key, ready.promise);
			let capture: AuthoritativeResultCapture<Output> | undefined;
			let pending: Entry<Output> | undefined;
			try {
				if (this.limits().maxEntries)
					capture = await this.hooks.capture(call, key, signal).catch(() => undefined);
				if (key && capture && !capture.inputsOnly && capture.route.reuse === "shared_result") {
					pending = this.makeEntry(call, key, capture.route, id);
					pending.retained = true;
					pending.work.start(started);
					this.pool.insert(pending);
				}
			} finally {
				ready.resolve();
				if (key && this.capturing.get(key.key) === ready.promise) this.capturing.delete(key.key);
			}
			const inputs = this.borrow();
			if (capture?.inputSource) this.captures.add(capture.inputSource);
			const dependencies: TimelineDependency[] = [];
			try {
				const { output, computation } = await TimelineInterval.measure(
					() => this.hooks.native(call, key, signal, { inputs: inputs.lookup }, dependencies),
					() => [...dependencies, ...(capture?.computationDependencies ?? [])],
					{ id, source: "actor_result" },
				);
				if (key) this.recheckWrites(key);
				if (
					capture &&
					this.limits().maxEntries &&
					(capture.inputsOnly || capture.inputSource || !this.hooks.reject?.(output))
				) {
					try {
						const branch = await capture.seal(this.hooks.reject?.(output) ? undefined : output);
						const entry = pending ?? this.makeEntry(call, key, capture.route, id);
						if (entry.route.reuse === "exclusive_branch") {
							// The native call has already applied these effects.
							try {
								await this.keepInputs(entry, branch, computation, true);
							} finally {
								await this.lifetime.release(branch);
							}
						} else {
							entry.retained = true;
							entry.work.start(computation.startedAt);
							this.retain(entry, branch, computation);
						}
						pending = undefined;
					} catch {
						/* Optional capture cannot change the authoritative result. */
					}
				}
				if (key) await this.hooks.observed?.(call, key).catch(() => {});
				return { output, served: false, toolExecution: computation, producerRunID: id };
			} finally {
				if (pending) {
					pending.work.fail(cause("execution", "actor_result_unavailable"), performance.now() - started);
					this.remove(pending);
				}
				if (capture?.inputSource) this.captures.delete(capture.inputSource);
				await this.lifetime.release(capture);
				inputs.release();
			}
		});
	}
	release(id: string): void {
		const handle = this.handles.get(id);
		if (!handle) return;
		handle.released = true;
		handle.detach?.();
		if (handle.parentUsers) return;
		this.handles.delete(id);
		handle.controller.abort();
		const entry = handle.entry;
		if (!entry) return;
		entry.handles.delete(id);
		if (!entry.handles.size && !entry.pins) {
			if (this.running.has(entry)) entry.work.controller.abort();
			else if (!entry.retained && !entry.adopted) this.remove(entry);
		}
	}
	snapshot(): SpeculativeCacheSnapshot {
		const hot = this.pool.hotEntries();
		const entries = this.pool.cached();
		const exclusive = entries.filter((entry) => entry.route.reuse === "exclusive_branch");
		return {
			cacheCapacity: this.limits().maxEntries,
			cacheByteCapacity: this.limits().maxBytes,
			cacheCold: entries.length - hot,
			cacheHot: hot,
			inFlightJobs: this.running.size,
			resultEntries: entries.length - exclusive.length,
			resultBytes: entries
				.filter((entry) => entry.route.reuse === "shared_result")
				.reduce((sum, entry) => sum + entry.estimatedBytes, 0),
			branchEntries: exclusive.length,
			branchBytes: exclusive.reduce((sum, entry) => sum + entry.estimatedBytes, 0),
			exclusiveCandidates: exclusive.length,
			sharedCandidates: entries.length - exclusive.length,
		};
	}
	async flush() {
		await this.lifetime.drain();
	}
	close() {
		this.controller.abort();
		for (const handle of this.handles.values()) this.release(handle.id);
		for (const entry of this.running) entry.work.controller.abort();
		return this.lifetime.close(async () => {
			await this.lifetime.drain();
			for (const entry of this.pool.values()) this.remove(entry);
			this.handles.clear();
		});
	}
}
