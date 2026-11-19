import { filesystemModeType } from "./filesystem-evidence.ts";
import { serialize } from "node:v8";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { AsyncLocalStorage } from "node:async_hooks";
import type { BigIntStats } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { PI_ACTION_SEMANTICS, type ActionKey, type ActionSemanticsRegistry } from "./action-semantics.ts";
import { DEFAULTS } from "./config-schema.ts";
import type {
	ExecutionResourceContext,
	ExecutionScope,
	ExecutionWorld,
	ToolInvocation,
	ToolSettlement,
	WorldBranch,
	WorldCheckpoint,
	WorldOutput,
	WorldResultCapture,
} from "./execution-world.ts";
import {
	effectCapabilitiesCover,
	effectCommitFailure,
	isPoisonedEffectCommit,
	RESOURCE_OBSERVATION_EFFECTS,
} from "./execution-world.ts";
import {
	CapturedFilesystemObject,
	sameFilesystemIdentity,
	withFilesystemReadView,
	type StableFilesystemCapture,
} from "./filesystem-evidence.ts";
import { relativeFilesystemPath } from "./primitives.ts";
import {
	captureResourceVersion,
	collectResourceComputation,
	invalidateResourceInputs,
	releaseResourceVersion,
	ResourceInputSession,
	resourceProofBytes,
	ResourceReadView,
	validateResourceVersion,
	type ResourceInput,
	type ResourceInputPersistence,
	type ResourceObservation,
	type ResourceVersionToken,
	type ToolFilesystemOperations,
	type ToolFilesystemStat,
} from "./resource-version.ts";
import { cause } from "./settlement.ts";
import { TimelineInterval } from "./task-timing.ts";
const resourceOperationContext = new AsyncLocalStorage<
	| {
			readonly inputs?: (path?: string) => Iterable<object>;
			readonly maxBytes: () => number;
			readonly persistentInputs?: ResourceInputPersistence;
	  }
	| undefined
>();
/** Host tool call supplied to any OS sandbox or safe local substitute. */
export type SpeculativeToolExecutionContext = Parameters<
	NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>
>[0] &
	ExecutionResourceContext & {
		readonly cwd: string;
		readonly tool: AgentTool;
		readonly toolName: string;
		readonly executionScope?: ExecutionScope;
		/** Optional immutable parent state for source-neutral multi-step execution. */
		readonly parentCheckpoint?: WorldCheckpoint;
	};
export type ActorToolExecutionContext = Omit<SpeculativeToolExecutionContext, "action"> & {
	readonly action?: ActionKey;
	readonly executionContext?: ToolInvocation;
};
export type AgentExecutionWorld = ExecutionWorld<
	SpeculativeToolExecutionContext,
	ToolSettlement,
	ActorToolExecutionContext
>;
export type SpeculativeAgentExecutionWorld = AgentExecutionWorld &
	Required<Pick<AgentExecutionWorld, "speculation">>;
/** Observe Actor reads; only explicitly bound operations may execute ahead over sealed resource data. */
export function createResourceSnapshotExecutionWorld(
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
	operations: {
		readonly maxBytes: () => number;
		readonly persistentInputs?: ResourceInputPersistence;
	} = { maxBytes: () => DEFAULTS.resourceCacheMaxBytes },
): AgentExecutionWorld {
	const persistence = operations.persistentInputs;
	const route = {
		capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
		fingerprint: () => "resource-version",
		diagnostics: () => ({
			state: "ready" as const,
			detail: "Bound file inputs and deterministic computations; unbound host results are not reusable",
		}),
	};
	return {
		id: "resource_version",
		observeOperations: ({ inputs, reuseEnabled }, execute) =>
			resourceOperationContext.run(
				reuseEnabled === false
					? undefined
					: {
							inputs,
							maxBytes: operations.maxBytes,
							persistentInputs: persistence,
						},
				execute,
			),
		scope: "fallback",
		isolation: "resource_snapshot",
		observation: {
			...route,
			inputsOnly: () => true,
			capture: async (context) => {
				const invocation =
					context.executionContext ?? (context.action?.executionContext as ToolInvocation | undefined);
				const session = persistence && new ResourceInputSession(persistence, operations.maxBytes());
				try {
					if (invocation?.captureInputs)
						return invocation.captureInputs(context.action, operations.maxBytes(), context.callID, session);
					const root = invocation?.filesystemRoot ?? context.cwd;
					const budget = operations.maxBytes();
					const version = session
						? await session.capture(undefined, root, actionSemantics, budget)
						: await captureResourceVersion(undefined, root, actionSemantics, budget);
					return resourceCapture(
						[version],
						context.action,
						actionSemantics,
						true,
						undefined,
						undefined,
						undefined,
						session,
					);
				} catch (error) {
					await session?.dispose();
					throw error;
				}
			},
		},
		speculation: {
			...route,
			fingerprint: (request) => {
				if (request.action && !readonlyResourceInvocation(request.action, actionSemantics))
					throw new Error("Resource execution requires an explicitly bound read-only operation");
				return route.fingerprint();
			},
			execute: async (context) => {
				const invocation = readonlyResourceInvocation(context.action, actionSemantics);
				const execute = invocation?.filesystem;
				if (!execute || context.parentCheckpoint)
					throw new Error("Resource execution context is not supported");
				await invocation.checkCaptureBudget?.(context);
				const session = persistence && new ResourceInputSession(persistence, operations.maxBytes());
				try {
					const evaluate = () =>
						evaluateResourceQuery(
							undefined,
							context,
							invocation?.filesystemRoot ?? context.cwd,
							(view) => execute(view, context),
							actionSemantics,
							operations.maxBytes(),
							true,
							undefined,
							undefined,
							session,
						);
					const query = await (session ? session.run(evaluate) : evaluate());
					if (!query) throw new Error("resource_snapshot_budget_exceeded");
					return resourceSnapshotBranch(
						{ output: query.output },
						query.versions,
						context.action,
						actionSemantics,
						query.inputSource,
						persistence,
					);
				} finally {
					await session?.dispose();
				}
			},
		},
	};
}
function readonlyResourceInvocation(
	action: ActionKey,
	semantics: ActionSemanticsRegistry,
): ToolInvocation | undefined {
	const invocation = action.executionContext as ToolInvocation | undefined;
	const definition = action.semantics ?? semantics.definition(action);
	return invocation?.filesystem &&
		definition?.effect === "observation" &&
		definition.resourceScope &&
		effectCapabilitiesCover(RESOURCE_OBSERVATION_EFFECTS.capabilities, definition.requirements)
		? invocation
		: undefined;
}
const resourceVersions = new WeakMap<object, () => readonly ResourceVersionToken[]>();
const inputBytes = (versions: readonly ResourceVersionToken[]) =>
	versions.reduce((bytes, token) => bytes + (token.view?.bytes ?? 0), 0);
/** Native inputs use the same owners and exact object validation on either side of a thread boundary. */
export class ResourceObjectInputs {
	private readonly views: (target?: string) => Iterable<ResourceReadView>;
	constructor(
		inputs: ((target?: string) => Iterable<object>) | ResourceReadView,
		_produced?: WeakSet<CapturedFilesystemObject>,
	) {
		this.views =
			inputs instanceof ResourceReadView
				? () => [inputs]
				: function* (target) {
						for (const source of inputs(target))
							for (const version of resourceVersions.get(source)?.() ?? [])
								if (version.view?.retained) yield version.view;
					};
	}
	async borrow(
		target: string,
		expected: BigIntStats,
		maxBytes: number,
		accept: (capture: StableFilesystemCapture) => boolean | Promise<boolean> = () => true,
	): Promise<StableFilesystemCapture | undefined> {
		for (const view of this.views(target)) {
			if (!view.retained) continue;
			try {
				const captured = await view.borrowObject(target, (capture, handle) =>
					TimelineInterval.outside(async () => {
						if (
							!capture.content ||
							capture.bytesRead > maxBytes ||
							!sameFilesystemIdentity(expected, capture.stat) ||
							!sameFilesystemIdentity(capture.stat, await handle.stat({ bigint: true })) ||
							!(await accept(capture))
						)
							return undefined;
						return {
							...capture,
							content: Buffer.from(capture.content),
							shared: true as const,
						};
					}),
				);
				if (captured) return captured;
			} catch {
				/* Revocation or a different version leaves the normal FD capture authoritative. */
			}
		}
		return undefined;
	}
	retain(target: string, capture: StableFilesystemCapture): boolean {
		for (const view of this.views(target)) if (view.retainObject(target, capture)) return true;
		return false;
	}
}
/** Whole queries and independent Actor inputs share proof composition; completed inputs survive consumer failure. */
export async function evaluateResourceQuery<Output>(
	inputs: readonly ResourceVersionToken[] | undefined,
	{
		signal,
		inputs: lookup,
		onInputSource: publish,
	}: ExecutionResourceContext & {
		readonly signal: AbortSignal;
	},
	root: string,
	execute: (view: ResourceReadView) => Promise<Output>,
	semantics: ActionSemanticsRegistry,
	maxBytes: number,
	retainMetadata = false,
	validation?: "prepared" | "output",
	transfer?: (versions: ResourceVersionToken[]) => boolean,
	session?: ResourceInputSession,
	acceptObservation?: (observation: ResourceObservation) => boolean,
) {
	signal.throwIfAborted();
	const recover = session?.reader(root, signal);
	const capture = () =>
		session
			? session.capture(undefined, root, semantics, maxBytes)
			: captureResourceVersion(undefined, root, semantics, maxBytes);
	const initial = inputs ? undefined : await capture();
	initial?.view?.seal();
	const versions = inputs ?? [initial!];
	const version = versions[0]!;
	const view = version.view!;
	const retained: ResourceVersionToken[] = [];
	let missing: Promise<ResourceVersionToken> | undefined;
	let captured: ResourceVersionToken | undefined;
	let sealed: ResourceVersionToken[] | undefined;
	const inputSource = publish && Object.freeze({});
	// Keep the initiating view first: it owns the query's root resolution, even when another source answers first.
	const proofs = new Map<ResourceVersionToken, Map<string, ResourceObservation>>([[version, new Map()]]);
	const observe = (
		token: ResourceVersionToken,
		dependencies: ReadonlySet<string> | undefined,
		commit = true,
	) => {
		const observations = (commit && proofs.get(token)) || new Map<string, ResourceObservation>();
		for (const key of dependencies ?? token.observations.keys()) {
			const entry = token.observations.get(key);
			if (!entry) throw new Error("resource_input_proof_missing");
			observations.set(key, entry);
		}
		if (commit) proofs.set(token, observations);
		return { ...token, observations, view: undefined };
	};
	const observeOwner = (
		tokens: readonly ResourceVersionToken[],
		dependencies: ReadonlySet<string> | undefined,
		commit = true,
	) =>
		dependencies
			? [observe(tokens[0]!, dependencies, commit)]
			: tokens.map((token) => observe(token, undefined, commit));
	try {
		return await collectResourceComputation(
			async () => {
				if (inputSource) {
					resourceVersions.set(inputSource, () => sealed ?? (captured ? [...versions, captured] : versions));
					publish!(inputSource);
				}
				view.assertComplete(true);
				let output: Output;
				let completed = false;
				try {
					output = await view.evaluate(
						execute,
						(dependencies, commit) => observeOwner(versions, dependencies, commit),
						root,
						async function* (target, preparation) {
							const owners = [
								versions,
								...[...(lookup?.(target) ?? [])].map((source) => resourceVersions.get(source)?.() ?? []),
							];
							for (const tokens of owners)
								for (const token of tokens)
									if (token.view)
										yield {
											view: token.view.readOnly(),
											observed: (dependencies, commit) =>
												dependencies
													? [observe(token, dependencies, commit)]
													: observeOwner(tokens, undefined, commit),
										};
							for await (const token of recover?.(target, preparation) ?? [])
								if (token.view)
									yield {
										view: token.view.readOnly(),
										observed: (dependencies, commit) => [observe(token, dependencies, commit)],
									};
						},
						async () => {
							const token = await (missing ??= capture().then((token) => (captured = token)));
							if (!token.view) throw new Error("resource_snapshot_budget_exceeded");
							return {
								view: token.view,
								observed: (dependencies, commit) => [observe(token, dependencies, commit)],
							};
						},
						(inputs) =>
							inputs.map((input) => {
								const proof = session ? session.retain(input) : input.manager.retain(input);
								retained.push(proof);
								observe(proof, undefined);
								return proof;
							}),
						async (proofs) => {
							if (
								acceptObservation &&
								proofs.some((proof) =>
									[...proof.observations.values()].some((observation) => !acceptObservation(observation)),
								)
							)
								throw new Error("resource_input_changed_in_transaction");
							await validateResourceInputs(proofs, true);
						},
						session?.memory,
					);
					completed = true;
				} finally {
					await TimelineInterval.outside(async () => {
						if (!completed && !transfer) return;
						try {
							let bytes = captured?.view?.bytes ?? 0;
							captured?.view?.seal();
							const owned = captured ? [captured] : [];
							for (const [token, observations] of proofs)
								if (observations.size && token !== captured) {
									const selected =
										observations.size === token.observations.size ? token : { ...token, observations };
									const metadataBytes = retainMetadata
										? Math.min(maxBytes - bytes, session?.remainingBytes ?? maxBytes)
										: undefined;
									const proof = retained.includes(selected)
										? selected
										: session
											? session.retain(selected, metadataBytes)
											: selected.manager.retain(selected, metadataBytes);
									if (proof !== selected) retained.push(proof);
									owned.push(proof);
									bytes += proof.view?.bytes ?? 0;
								}
							if (completed && validation === "output") await validateResourceInputs(owned, true);
							if (!transfer) signal.throwIfAborted();
							if (
								(!retainMetadata || resourceProofBytes(owned) + bytes <= maxBytes) &&
								(!session || session.bytes <= session.maxBytes)
							) {
								if (captured)
									await session?.publish(
										captured,
										signal,
										[...proofs].map(([token, observations]) => ({
											...token,
											observations,
										})),
									);
								if (!transfer || transfer(owned)) {
									captured = undefined;
									retained.length = 0;
								}
								sealed = owned;
							} else if (transfer) sealed = []; // Successful consumption does not require future cache retention.
							if (completed && transfer && validation !== "prepared") signal.throwIfAborted();
						} catch (error) {
							if (completed) throw error;
						} // Optional input retention preserves the original execution failure.
					});
				}
				return sealed && { output, versions: sealed, inputSource };
			},
			(branch) => branch !== undefined,
		);
	} finally {
		await TimelineInterval.outside(async () => {
			if (inputSource && !sealed) resourceVersions.delete(inputSource);
			await Promise.allSettled([...retained, captured, initial].map(releaseResourceVersion));
		});
	}
}
/** Committed poststates enter the same read view and exact validation as captured inputs. */
export async function createCommittedResourceInputs(
	action: ActionKey,
	root: string,
	inputs: ReadonlyMap<string, ResourceInput>,
	maxBytes: number,
): Promise<
	WorldBranch<ToolSettlement> & {
		readonly inputsOnly: true;
	}
> {
	const version = await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, maxBytes, inputs);
	try {
		return resourceSnapshotBranch({ inputsOnly: true }, [version], action, PI_ACTION_SEMANTICS);
	} catch (error) {
		await version.release();
		throw error;
	}
}
export function resourceSnapshotBranch<Outcome extends WorldOutput<ToolSettlement>>(
	outcome: Outcome,
	versions: readonly ResourceVersionToken[],
	action: ActionKey | undefined,
	semantics: ActionSemanticsRegistry,
	inputSource = Object.freeze({}),
	persistence?: ResourceInputPersistence,
) {
	for (const version of versions) version.view?.seal();
	if (
		outcome.inputsOnly &&
		!versions.some((token) => token.view?.resources.length || token.view?.hasPreparedInputs)
	)
		throw new Error("resource_inputs_empty");
	// Input revocation releases data, not the old result's immutable freshness evidence.
	const version = versions[0]!;
	const proofBytes = resourceProofBytes(versions);
	const executionFingerprint = action?.executionFingerprint;
	resourceVersions.set(inputSource, () => versions);
	let owned: readonly ResourceVersionToken[] | undefined = versions;
	const branch: Omit<WorldBranch<ToolSettlement>, "output" | "inputsOnly"> = {
		backend: "resource_version",
		inputSource,
		resources: Object.freeze([]),
		invalidateInputs: (paths) => {
			if (!owned) return [];
			const removed = invalidateResourceInputs(owned, paths);
			if (!removed.length) return removed;
			const remaining = new Set(
				owned.flatMap((token) => token.view?.resources.map((input) => input.path) ?? []),
			);
			return removed.filter((target) => !remaining.has(target));
		},
		inputResources: version.view && versions.flatMap((token) => token.view?.resources ?? []),
		reconstructionScope: "current_action",
		get capturedBytes() {
			return proofBytes + inputBytes(versions);
		},
		executionMetrics: Object.freeze({}),
		compatibility: Object.freeze(
			executionFingerprint === undefined
				? {
						status: "indeterminate",
						backend: "resource_version",
						code: "input_only_capture",
					}
				: {
						status: "compatible",
						backend: "resource_version",
						executionFingerprint,
					},
		),
		validate: async () => {
			const { expired, reason, changed, ...metrics } = await validateResourceInputs(owned);
			return expired
				? {
						status: "stale",
						cause: cause("freshness", reason ?? "resource_changed"),
						metrics,
						...(changed?.length && owned?.some((token) => token.view?.hasPreparedInputs)
							? { reconstruct: true as const }
							: {}),
					}
				: { status: "valid", metrics };
		},
		...(version.view
			? {
					reconstruct: async (
						request: Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0],
					) => {
						request.signal.throwIfAborted();
						const invocation = readonlyResourceInvocation(request.action, semantics);
						const execute = invocation?.filesystem;
						const root =
							invocation?.filesystemRoot ??
							(request.action.executionFingerprint === executionFingerprint ? version.root : undefined);
						if (!owned || !execute || !root) return undefined;
						await invocation.checkCaptureBudget?.(request);
						const maxBytes = Math.max(0, version.view!.remainingBytes - inputBytes(versions.slice(1)));
						const session =
							persistence && maxBytes > 0 ? new ResourceInputSession(persistence, maxBytes) : undefined;
						try {
							const evaluate = () =>
								evaluateResourceQuery(
									versions,
									request,
									root,
									(view) => execute(view, request),
									semantics,
									maxBytes,
									false,
									undefined,
									undefined,
									session,
								);
							const query = await (session ? session.run(evaluate) : evaluate());
							return (
								query &&
								Object.assign(
									resourceSnapshotBranch(
										{ output: query.output },
										query.versions,
										request.action,
										semantics,
										query.inputSource,
										persistence,
									),
									{ requiresQueryValidation: true as const },
								)
							);
						} finally {
							await session?.dispose();
						}
					},
				}
			: {}),
		commit: async () => {
			// Nothing was applied: an input-only branch can only be declined, never poison the Actor call.
			if (outcome.inputsOnly) throw effectCommitFailure(new Error("input_only_branch"), "recoverable");
			if (!owned) throw new Error("resource snapshot is disposed");
			return outcome.output;
		},
		dispose: () => {
			owned = undefined;
			resourceVersions.delete(inputSource);
			return versions.length === 1
				? version.release()
				: Promise.allSettled(versions.map(releaseResourceVersion)).then(() => {});
		},
	};
	return Object.assign(branch, outcome);
}
async function validateResourceInputs(tokens?: readonly ResourceVersionToken[], revokeUnknown = false) {
	const result = await validateResourceVersion(tokens);
	if (result.changed?.length || (result.expired && revokeUnknown))
		for (const token of tokens ?? [])
			for (const view of new Set([token.view, token.inputView?.deref()]))
				view?.invalidate(new Set(result.changed ?? token.observations.keys()));
	if (revokeUnknown && result.expired) throw new Error(result.reason ?? "resource_operation_stale");
	return result;
}
/** One transfer boundary for observed windows, native inputs and explicit Actor preparations. */
function resourceCapture(
	versions: ResourceVersionToken[],
	action: ActionKey | undefined,
	semantics: ActionSemanticsRegistry,
	inputsOnly = false,
	beforeSeal?: () => Promise<void>,
	close?: () => void,
	readInputs = () => versions,
	session?: ResourceInputSession,
	observedResult?: (output: ToolSettlement | undefined) => boolean,
): WorldResultCapture<ToolSettlement> {
	const inputSource = Object.freeze({});
	let phase: "open" | "sealing" | "disposed" = "open";
	resourceVersions.set(inputSource, readInputs);
	const dispose = async () => {
		phase = "disposed";
		if (resourceVersions.delete(inputSource)) close?.();
		await session?.dispose();
		await Promise.allSettled(versions.splice(0).map(releaseResourceVersion));
	};
	return {
		...(inputsOnly ? { inputsOnly: true as const } : {}),
		...(inputsOnly || observedResult ? { inputSource } : {}),
		dispose,
		seal: async (output) => {
			if (phase !== "open") throw new Error("resource_snapshot_capture_consumed");
			phase = "sealing";
			try {
				if (!inputsOnly && !observedResult && output === undefined)
					throw new Error("authoritative_result_missing");
				await beforeSeal?.();
				if (!resourceVersions.has(inputSource)) throw new Error("resource_snapshot_capture_disposed");
				for (const version of versions) version.view?.seal();
				for (const version of versions) await session?.publish(version, undefined, versions);
				if (!resourceVersions.has(inputSource)) throw new Error("resource_snapshot_capture_disposed");
				const branch = resourceSnapshotBranch(
					inputsOnly || (observedResult && !observedResult(output))
						? { inputsOnly: true }
						: { output: output! },
					[...versions],
					action,
					semantics,
					undefined,
					session?.configuration,
				);
				versions.length = 0;
				return branch;
			} finally {
				await dispose();
			}
		},
	};
}
/** Explicit callbacks share inputs; qualified deterministic observations may also retain their complete result. */
export function bindResourceOperation(
	execute: NonNullable<ToolInvocation["filesystem"]>,
	root: string,
	native?: NonNullable<ToolInvocation["authoritative"]>,
	completeObservation: boolean | "single_query" = false,
	checkCaptureBudget?: ToolInvocation["checkCaptureBudget"],
): Pick<ToolInvocation, "authoritative" | "captureInputs" | "filesystem" | "filesystemRoot" | "checkCaptureBudget"> {
	type Capture = {
		maxBytes: number;
		inputs: Map<string, ResourceInput>;
		versions: ResourceVersionToken[];
		queries: Set<object>;
		session?: ResourceInputSession;
		phase: "available" | "observing" | "incomplete";
	};
	const active = new Map<string, Capture>();
	const bytes = (owned: Capture, inputs = true) =>
		resourceProofBytes(owned.versions) +
		inputBytes(owned.versions) +
		(inputs ? serialize([...owned.inputs]).byteLength : 0);
	return {
		filesystem: execute,
		filesystemRoot: root,
		checkCaptureBudget,
		captureInputs: (action, maxBytes, callID, session) => {
			if (active.has(callID) || !Number.isFinite(maxBytes) || maxBytes <= 0)
				throw new Error("resource_snapshot_budget_invalid");
			const owned: Capture = {
				phase: "available",
				maxBytes,
				inputs: new Map(),
				versions: [],
				queries: new Set(),
				session,
			};
			active.set(callID, owned);
			const output =
				completeObservation &&
				process.platform !== "win32" &&
				!!action &&
				PI_ACTION_SEMANTICS.effect(action) === "observation" &&
				effectCapabilitiesCover(
					RESOURCE_OBSERVATION_EFFECTS.capabilities,
					PI_ACTION_SEMANTICS.requirements(action)!,
				);
			return resourceCapture(
				owned.versions,
				action,
				PI_ACTION_SEMANTICS,
				!output,
				async () => {
					if (active.get(callID) === owned) active.delete(callID);
					// Infer entries only for directories this callback actually created.
					const names = new Map(
						[...owned.inputs]
							.filter(([, input]) => input && !(input instanceof Uint8Array) && !Object.keys(input).length)
							.map(([target]) => [target, [] as string[]]),
					);
					for (const target of owned.inputs.keys())
						names.get(path.dirname(target))?.push(path.basename(target));
					for (const [target, entries] of names) owned.inputs.set(target, { names: entries });
					if (owned.inputs.size) {
						const budget = Math.max(0, maxBytes - bytes(owned, false));
						owned.versions.push(
							await (session
								? session.capture(undefined, root, PI_ACTION_SEMANTICS, budget, owned.inputs)
								: captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, budget, owned.inputs)),
						);
					}
				},
				() => {
					if (active.get(callID) === owned) active.delete(callID);
					owned.inputs.clear();
					owned.queries.clear();
				},
				() => [
					...owned.versions,
					...[...owned.queries].flatMap((source) => resourceVersions.get(source)?.() ?? []),
				],
				session,
				output ? (result) => owned.phase !== "incomplete" && !!result && !result.isError : undefined,
			);
		},
		authoritative: async (request) => {
			const capture = active.get(request.callID);
			const owned = capture?.phase === "available" ? capture : undefined;
			const scope = resourceOperationContext.getStore();
			if (!owned && !scope && native) return native(request);
			if (owned) owned.phase = "observing";
			let observe = !!(owned || scope);
			const unproven = () => {
				if (owned) owned.phase = "incomplete";
				if (completeObservation === "single_query") observe = false;
			};
			const session =
				owned?.session ??
				(scope?.persistentInputs && new ResourceInputSession(scope.persistentInputs, scope.maxBytes()));
			const query = async <Value>(
				execute: (view: ResourceReadView) => Promise<Value>,
				signal = request.signal,
				validation: "prepared" | "output" = "prepared",
			) => {
				if (!observe) return;
				let source: object | undefined;
				const evaluate = () =>
					TimelineInterval.outside(() =>
						evaluateResourceQuery(
							undefined,
							{
								signal,
								inputs: scope?.inputs,
								onInputSource:
									owned &&
									((value) => {
										source = value;
										if (active.get(request.callID) === owned) owned.queries.add(value);
									}),
							},
							root,
							execute,
							PI_ACTION_SEMANTICS,
							owned ? Math.max(0, owned.maxBytes - bytes(owned)) : scope!.maxBytes(),
							true,
							validation,
							(versions) =>
								!!owned && active.get(request.callID) === owned && !!owned.versions.push(...versions),
							session,
						),
					);
				try {
					const run = () => (session ? session.run(evaluate) : evaluate());
					const result = await (completeObservation === "single_query"
						? withFilesystemReadView(undefined, run)
						: run());
					if (!result?.versions.length) unproven();
					return result;
				} finally {
					if (source) owned?.queries.delete(source);
				}
			};
			const read = async <Value>(
				execute: (view: ResourceReadView) => Promise<Value>,
				native: () => Promise<Value>,
			) => {
				try {
					await checkCaptureBudget?.(request);
					const result = await query(execute, request.signal, "output");
					if (result) return result.output;
				} catch {
					/* Unproven inputs leave the native operation authoritative. */
				}
				unproven();
				request.signal.throwIfAborted();
				return native();
			};
			const retain = (target: string, input: ResourceInput | string) => {
				if (
					!owned ||
					active.get(request.callID) !== owned ||
					relativeFilesystemPath(root, target) === undefined
				)
					return;
				owned.inputs.delete(target);
				if (typeof input === "string" || input instanceof Uint8Array) {
					if (Buffer.byteLength(input) > owned.maxBytes) return;
					input = Buffer.from(input);
				}
				owned.inputs.set(target, input);
				if (bytes(owned) > owned.maxBytes) owned.inputs.delete(target);
			};
			let effectsStarted = false;
			const host: ToolFilesystemOperations = {
				readFile: (target, maxBytes) =>
					read(
						(view) => view.readFile(target, maxBytes),
						async () => {
							const content = await fs.readFile(target);
							retain(target, content);
							return content.subarray(0, maxBytes);
						},
					),
				access: (target, writable) => {
					if (writable) {
						unproven();
						return fs.access(target, fs.constants.R_OK | fs.constants.W_OK);
					}
					return read(
						(view) => view.access(target),
						() => fs.access(target, fs.constants.R_OK),
					);
				},
				exists: (target) =>
					read(
						(view) => view.exists(target),
						async () => {
							try {
								await fs.access(target);
								return true;
							} catch {
								return false;
							}
						},
					),
				stat: (target, fields, includeSize = true) =>
					read<ToolFilesystemStat>(
						(view) => view.stat(target, fields, includeSize),
						async () => {
							const info = await (fields === "entry" ? fs.lstat(target) : fs.stat(target));
							const kind = filesystemModeType(info.mode);
							const type = kind === "other" ? "special" : kind;
							return {
								isDirectory: () => info.isDirectory(),
								size: includeSize ? info.size : undefined,
								type,
								...(type === "symlink"
									? { link: await fs.readlink(target) }
									: { realPath: await fs.realpath(target) }),
							};
						},
					),
				readdir: (target) =>
					read(
						(view) => view.readdir(target),
						async () => {
							const names = await fs.readdir(target);
							retain(target, { names });
							return names;
						},
					),
				mkdir: async (target) => {
					effectsStarted = true;
					unproven();
					const created = await fs.mkdir(target, { recursive: true });
					if (created)
						for (
							let directory = target;
							relativeFilesystemPath(path.toNamespacedPath(created), path.toNamespacedPath(directory)) !==
							undefined;
							directory = path.dirname(directory)
						) {
							retain(directory, {});
							if (path.dirname(directory) === directory) break;
						}
				},
				writeFile: async (target, content) => {
					effectsStarted = true;
					unproven();
					await fs.writeFile(target, content, "utf-8");
					retain(target, content);
				},
				prepare: async (binding, key, build, consume, hint, signal = request.signal) => {
					let consumption: ReturnType<typeof consume> | undefined;
					const use: typeof consume = (value) => (consumption = (async () => consume(value))());
					try {
						await query((view) => view.prepare(binding, key, build, use, hint, signal), signal);
					} catch (error) {
						unproven();
						if (consumption)
							return consumption.catch(() => {
								throw error;
							});
					}
					if (consumption) return consumption;
					unproven();
					signal.throwIfAborted();
					const resource = await build(host, signal);
					try {
						return await consume(resource.value);
					} finally {
						await resource.dispose();
					}
				},
			};
			try {
				// An observation shares one input budget and publishes one proof for the whole call.
				if (completeObservation === "single_query" && (owned || scope))
					return await read(
						(view) => execute(view, request),
						() => native ? native(request) : execute(host, request),
					);
				return await (native && completeObservation
					? collectResourceComputation(() => execute(host, request))
					: execute(host, request));
			} catch (error) {
				// Only an explicitly supplied observation fallback may repeat an effect-free attempt.
				unproven();
				if (!native || completeObservation !== true || effectsStarted || isPoisonedEffectCommit(error)) throw error;
				request.signal.throwIfAborted();
				return await native(request);
			} finally {
				if (session !== owned?.session) await session?.dispose();
			}
		},
	};
}
