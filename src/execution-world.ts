import type { ActionEffect, ActionKey } from "./action-semantics.ts";
import {
	effectCapabilitiesCover,
	type EffectCapabilities,
	type EffectRequirements,
} from "./effect-model.ts";
import type { ResourceValidation } from "./settlement.ts";

/** Concrete isolation used for one speculative execution. */
export type SpeculativeExecution = "runtime_sandbox" | "resource_snapshot" | "workspace_branch";
export type WorldReuseStrategy = "shared_result" | "exclusive_branch";
/** @deprecated Use WorldReuseStrategy; this names world isolation policy, not an action-key reuse relation. */
export type ActionReuseKind = WorldReuseStrategy;
export type ExecutionWorldScope = "runtime" | "fallback";

/** Correlation scope carried through every execution world without affecting cache identity. */
export interface ExecutionScope {
	readonly sessionID: string;
	readonly turnID: string;
}

/** Tool effects are resolved independently from K(a) and prediction source. */
export interface ExecutionWorldRequest {
	readonly effect: ActionEffect;
	readonly requirements: EffectRequirements;
	/** Present for concrete candidates; omitted for best-effort turn warm-up. */
	readonly action?: ActionKey;
}

/** One resolved execution capability. Absence of a route means speculation is blocked. */
export interface SpeculativeExecutionRoute {
	readonly isolation: SpeculativeExecution;
	readonly reuse: WorldReuseStrategy;
	readonly scope: ExecutionWorldScope;
	readonly backend: string;
	readonly fingerprint: string;
}

export function sameSpeculativeExecutionRoute(
	left: SpeculativeExecutionRoute,
	right: SpeculativeExecutionRoute,
): boolean {
	return (
		left.isolation === right.isolation &&
		left.reuse === right.reuse &&
		left.scope === right.scope &&
		left.backend === right.backend &&
		left.fingerprint === right.fingerprint
	);
}

/** Backend-neutral accounting for validated result reuse inside one execution world. */
export interface WorldReuseMetrics {
	readonly requests: number;
	readonly hits: number;
	readonly joinedHits: number;
	readonly sameTurnHits: number;
	readonly crossTurnHits: number;
	readonly unattributedHits: number;
	readonly misses: number;
	readonly bypasses: number;
	readonly published: number;
	readonly tainted: number;
	readonly validationMs: number;
	readonly validationCandidates: number;
	readonly validationPathsets: number;
	readonly validationFilesRead: number;
	readonly validationBytesRead: number;
	readonly validationArtifactsLoaded: number;
	readonly validationArtifactBytesRead: number;
	readonly replayMs: number;
	readonly executionMs: number;
	readonly lastError?: string;
}

export function emptyWorldReuseMetrics(): WorldReuseMetrics {
	return {
		requests: 0,
		hits: 0,
		joinedHits: 0,
		sameTurnHits: 0,
		crossTurnHits: 0,
		unattributedHits: 0,
		misses: 0,
		bypasses: 0,
		published: 0,
		tainted: 0,
		validationMs: 0,
		validationCandidates: 0,
		validationPathsets: 0,
		validationFilesRead: 0,
		validationBytesRead: 0,
		validationArtifactsLoaded: 0,
		validationArtifactBytesRead: 0,
		replayMs: 0,
		executionMs: 0,
	};
}

export interface WorldExecutionMetrics {
	/** Time spent materializing an isolated world before the tool could start. */
	readonly setupMs?: number;
	/** Time spent sealing observable persistent effects after the tool completed. */
	readonly captureMs?: number;
	/** Validated result reuse performed by the world while executing the action. */
	readonly reuse?: WorldReuseMetrics;
}

export interface WorldCommitMetrics {
	readonly durationMs: number;
	readonly validationMs: number;
	readonly bytesValidated: number;
	readonly resourcesValidated: number;
	readonly resourcesCommitted: number;
}

/** Backend-issued evidence; policy decides whether it matches the Actor world. */
export type WorldCompatibilityEvidence =
	| {
			readonly status: "compatible";
			readonly backend: string;
			readonly executionFingerprint: string;
	  }
	| {
			readonly status: "incompatible" | "indeterminate";
			readonly backend: string;
			readonly code: string;
			readonly detail?: string;
	  };

/** Immutable execution state from which a later speculative action may derive. */
export interface WorldCheckpoint {
	readonly backend: string;
	readonly id: string;
	readonly lineage: string;
	readonly depth: number;
}

/**
 * A sealed speculative execution artifact.
 *
 * The tool output and promotable persistent effects are captured together. Ephemeral process,
 * environment, and network state never crosses the branch boundary. Backends provide an
 * idempotent commit primitive; EffectTransaction exclusively owns validation and adoption state.
 */
export interface WorldBranch<Output> {
	readonly output: Output;
	readonly backend: string;
	readonly checkpoint?: WorldCheckpoint;
	readonly resources: readonly string[];
	/** Captured persistent-effect bytes, excluding the serialized tool output. */
	readonly capturedBytes: number;
	readonly executionMetrics: WorldExecutionMetrics;
	readonly compatibility: WorldCompatibilityEvidence;
	readonly commitMetrics?: WorldCommitMetrics;
	/** Optional freshness proof owned by the backend that captured the branch. */
	readonly validate?: () => Promise<ResourceValidation>;
	/** Subscribe to invalidation; the branch owns and releases the subscription. */
	readonly watch?: (onInvalidated: (changedPath?: string) => void) => void;
	readonly commit: () => Promise<Output>;
	/** Idempotently release every branch-local handle. Must be safe before or after commit. */
	readonly dispose: () => void | Promise<void>;
}

/** Pre-execution evidence that can seal one externally executed authoritative result. */
export interface WorldResultCapture<Output> {
	/** Transfer the captured baseline into a normal branch. May be called at most once. */
	readonly seal: (output: Output) => WorldBranch<Output> | Promise<WorldBranch<Output>>;
	/** Release an unsealed baseline. Idempotent; a sealed branch owns its own cleanup. */
	readonly dispose: () => void | Promise<void>;
}

export interface CapturedExecutionWorldResult<Output> {
	readonly route: SpeculativeExecutionRoute;
	readonly capture: WorldResultCapture<Output>;
}

export interface ExecutionWorldPreparation {
	readonly cwd: string;
	readonly signal?: AbortSignal;
}

export type ExecutionWorldHealthState = "registered" | "ready" | "degraded" | "unavailable";

/** Backend-owned health independent from whether one concrete action has selected this world. */
export interface ExecutionWorldDiagnosticReport {
	readonly state: ExecutionWorldHealthState;
	readonly detail: string;
	readonly fingerprint?: string;
	readonly attributes?: Readonly<Record<string, string | number | boolean>>;
	readonly storage?: {
		readonly entries: number;
		readonly maxEntries: number;
		readonly bytes: number;
		readonly maxBytes: number;
		readonly orphanArtifacts?: number;
		readonly overBudget: boolean;
	};
}

export interface ExecutionWorldDiagnosticsContext extends ExecutionWorldPreparation {
	/** Re-run backend capability probes instead of using their cached result. */
	readonly refresh?: boolean;
}

/** Source-neutral world status consumed by hosts and UIs. */
export interface ExecutionWorldDiagnosticSnapshot extends ExecutionWorldDiagnosticReport {
	readonly id: string;
	readonly scope: ExecutionWorldScope;
	readonly isolation: SpeculativeExecution;
	readonly observedAt: number;
}

interface ExecutionWorldLifecycle<Context, Output> {
	readonly id: string;
	/** Atomic effects this backend can safely contain, virtualize, or validate. */
	readonly capabilities: EffectCapabilities;
	/** Stable identity of the concrete isolation backend used for route-local reuse. */
	readonly fingerprint?: (request: ExecutionWorldRequest) => string | Promise<string>;
	/** Idempotent and concurrency-safe; reject while unavailable so resolution can try the next world. */
	readonly prepare?: (input: ExecutionWorldPreparation) => Promise<void>;
	/** Read-only health and diagnostics. It must not weaken or bypass route preparation. */
	readonly diagnostics?: (
		input: ExecutionWorldDiagnosticsContext,
	) => ExecutionWorldDiagnosticReport | Promise<ExecutionWorldDiagnosticReport>;
	readonly fork: (context: Context) => Promise<WorldBranch<Output>>;
	/** Capture freshness before a host-authoritative execution without executing the tool again. */
	readonly captureAuthoritativeResult?: (context: Context) => Promise<WorldResultCapture<Output>>;
	/** Abort and drain backend-owned forks and branch cleanup before resolving. */
	readonly dispose?: () => Promise<void>;
}

/** Source-independent lifecycle for isolating, sealing, and committing speculative effects. */
export type ExecutionWorld<Context, Output> = ExecutionWorldLifecycle<Context, Output> &
	(
		| {
				/** A runtime world is preferred when its advertised guarantees cover the operation. */
				readonly scope: "runtime";
				readonly isolation: "runtime_sandbox";
		  }
		| {
				/** A host-local fallback advertises the same source-neutral effect guarantees. */
				readonly scope: "fallback";
				readonly isolation: Exclude<SpeculativeExecution, "runtime_sandbox">;
		  }
	);

/** The only authority allowed to resolve, prepare, fork, and dispose speculative tool execution. */
export class ExecutionWorldRouter<Context, Output> {
	private readonly worlds: readonly ExecutionWorld<Context, Output>[];
	private readonly worldsByID = new Map<string, ExecutionWorld<Context, Output>>();
	private readonly routeObservations = new Map<string, ExecutionWorldDiagnosticReport & { readonly cwd: string }>();

	constructor(worlds: readonly ExecutionWorld<Context, Output>[]) {
		this.worlds = [...new Set(worlds)];
		for (const world of this.worlds) {
			if (!world.id.trim()) throw new Error("execution world id must not be empty");
			if (this.worldsByID.has(world.id)) throw new Error(`duplicate execution world ${world.id}`);
			this.worldsByID.set(world.id, world);
		}
	}

	/** Runtime sandbox first, then local fallback; unavailable worlds are skipped. */
	async resolve(
		request: ExecutionWorldRequest,
		preparation: ExecutionWorldPreparation,
	): Promise<SpeculativeExecutionRoute | undefined> {
		return this.select(request, preparation, (_world, route) => route);
	}

	fork(route: SpeculativeExecutionRoute, context: Context): Promise<WorldBranch<Output>> {
		const world = this.world(route);
		return world.fork(context);
	}

	/** Select a capture-capable world and snapshot its baseline before host execution. */
	async captureAuthoritativeResult(
		request: ExecutionWorldRequest,
		preparation: ExecutionWorldPreparation,
		context: Context,
	): Promise<CapturedExecutionWorldResult<Output> | undefined> {
		return this.select(
			request,
			preparation,
			async (world, route) => {
				if (!world.captureAuthoritativeResult) return undefined;
				const capture = await world.captureAuthoritativeResult(context);
				return Object.freeze({ route, capture });
			},
			(world) => world.captureAuthoritativeResult !== undefined,
		);
	}

	async dispose(): Promise<void> {
		await Promise.allSettled(this.worlds.map((world) => world.dispose?.()));
	}

	/** Inspect every registered world without attempting a speculative action. */
	async diagnostics(input: ExecutionWorldDiagnosticsContext): Promise<readonly ExecutionWorldDiagnosticSnapshot[]> {
		return Promise.all(
			this.worlds.map(async (world) => {
				const observedAt = Date.now();
				let report: ExecutionWorldDiagnosticReport | undefined;
				try {
					report = await world.diagnostics?.(input);
				} catch (error) {
					report = { state: "unavailable", detail: errorDetail(error) };
				}
				const route = this.routeObservations.get(world.id);
				if (route?.cwd === input.cwd && route.state === "unavailable") report = route;
				return Object.freeze({
					id: world.id,
					scope: world.scope,
					isolation: world.isolation,
					...(report ??
						(route?.cwd === input.cwd ? route : undefined) ?? {
							state: "registered",
							detail: "Registered; availability is checked during route preparation",
						}),
					observedAt,
				});
			}),
		);
	}

	private world(route: SpeculativeExecutionRoute): ExecutionWorld<Context, Output> {
		const world = this.worldsByID.get(route.backend);
		if (!world || world.scope !== route.scope || world.isolation !== route.isolation) {
			throw new Error(`Execution world ${route.backend} is unavailable for ${route.isolation}`);
		}
		return world;
	}

	private async select<Selected>(
		request: ExecutionWorldRequest,
		preparation: ExecutionWorldPreparation,
		select: (
			world: ExecutionWorld<Context, Output>,
			route: SpeculativeExecutionRoute,
		) => Selected | undefined | Promise<Selected | undefined>,
		eligible: (world: ExecutionWorld<Context, Output>) => boolean = () => true,
	): Promise<Selected | undefined> {
		for (const scope of ["runtime", "fallback"] as const) {
			for (const world of this.worlds) {
				if (world.scope !== scope || !eligible(world)) continue;
				try {
					if (!effectCapabilitiesCover(world.capabilities, request.requirements)) continue;
					const fingerprint = (await world.fingerprint?.(request)) ?? `${world.id}:${world.isolation}`;
					await world.prepare?.(preparation);
					this.routeObservations.set(
						world.id,
						Object.freeze({
							state: "ready",
							cwd: preparation.cwd,
							detail: "Route prepared successfully",
							fingerprint,
						}),
					);
					const route = Object.freeze({
						isolation: world.isolation,
						reuse: request.effect === "observation" ? "shared_result" : "exclusive_branch",
						scope,
						backend: world.id,
						fingerprint,
					});
					const selection = select(world, route);
					const selected = isPromiseLike(selection) ? await selection : selection;
					if (selected !== undefined) return selected;
				} catch (error) {
					if (preparation.signal?.aborted) throw error;
					this.routeObservations.set(
						world.id,
						Object.freeze({
							state: "unavailable",
							cwd: preparation.cwd,
							detail: errorDetail(error),
						}),
					);
					// Unavailable worlds are skipped in explicit capability order.
				}
			}
		}
		return undefined;
	}
}

function errorDetail(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function isPromiseLike<Value>(value: Value | Promise<Value>): value is Promise<Value> {
	return Boolean(value && typeof value === "object" && "then" in value && typeof value.then === "function");
}
