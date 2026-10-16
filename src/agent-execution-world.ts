import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs/promises";
import path from "node:path";
import { filesystemPathsOverlap, relativeFilesystemPath } from "./path-utils.ts";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { BigIntStats } from "node:fs";
import { CapturedFilesystemObject, sameFilesystemIdentity, type CapturedFilesystemTransfer, type StableFilesystemCapture } from "./filesystem-evidence.ts";
import { PI_ACTION_SEMANTICS, type ActionKey, type ActionSemanticsRegistry } from "./action-semantics.ts";
import type { ExecutionWorld, ExecutionScope, ExecutionOperationAdoption, WorldBranch, WorldOutput, WorldCheckpoint, WorldResultCapture } from "./execution-world.ts";
import { effectCommitFailure } from "./effect-transaction.ts";
import { effectCapabilitiesCover, RESOURCE_OBSERVATION_EFFECTS, WORKSPACE_PATH_MUTATION_EFFECTS } from "./effect-model.ts";
import { captureResourceVersion, collectResourceComputation, invalidateResourceInputs, releaseResourceVersion, resourceProofBytes, validateResourceVersion,
	ResourceReadView, type ResourceInput, type ResourceObservation, type ResourceVersionToken } from "./resource-version.ts";
import { cause } from "./settlement.ts";
import type { ToolFilesystemOperations, ToolFilesystemStat, ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import { TimelineInterval } from "./task-timing.ts";

/** Host tool call supplied to any OS sandbox or safe local substitute. */
export type SpeculativeToolExecutionContext = Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0] & {
	readonly cwd: string;
	readonly tool: AgentTool;
	readonly toolName: string;
	readonly executionScope?: ExecutionScope;
	readonly onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
	/** Live acquisition capability used only to avoid cancelling work the current Actor can join. */
	readonly onOperationJoinable?: (available: () => boolean) => void;
	readonly acceptOperationScope?: (scope: ExecutionScope, salvage?: boolean) => boolean;
	/** Optional immutable parent state for source-neutral multi-step execution. */
	readonly parentCheckpoint?: WorldCheckpoint;
};

export type ActorToolExecutionContext = Omit<SpeculativeToolExecutionContext, "action"> & { readonly action?: ActionKey; readonly executionContext?: ToolInvocation };
export type AgentExecutionWorld = ExecutionWorld<SpeculativeToolExecutionContext, ToolSettlement, ActorToolExecutionContext>;
export type SpeculativeAgentExecutionWorld = AgentExecutionWorld & Required<Pick<AgentExecutionWorld, "speculation">>;

/** Known path independence permits early work; captured versions still decide whether its output is usable. */
export async function filesystemExecutionDependsOn(action: ActionKey, parent: ActionKey): Promise<boolean> {
	if (!action.resourceRoot || !parent.resourceRoot || !action.resources.length || !parent.resources.length) return true;
	return (await Promise.all(action.resources.flatMap(target => parent.resources.map(prior =>
		filesystemPathsOverlap(path.resolve(action.resourceRoot!, target), path.resolve(parent.resourceRoot!, prior)))))).some(Boolean);
}

/** Observe Actor reads; only explicitly bound operations may execute ahead over sealed resource data. */
export function createResourceSnapshotExecutionWorld(
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
	operations?: { readonly maxBytes: () => number },
): AgentExecutionWorld {
	const canObserve = process.platform !== "win32";
	const capturesInputs = (action: ActionKey | undefined, invocation: ToolInvocation | undefined) => !!operations &&
		(!!invocation?.captureInputs || process.platform === "linux" && (!!invocation?.process || !action || actionSemantics.effect(action) !== "observation"));
	const route = {
		capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
		fingerprint: () => "resource-version",
		diagnostics: () => ({
			state: "ready" as const,
			detail: "Sealed file inputs; host reads may update access times (not an OS snapshot)",
		}),
	};
	return {
		id: "resource_version",
		observeOperations: ({ inputs }, execute) => actorResourceInputs.run(operations && inputs ? { inputs, maxBytes: operations.maxBytes } : undefined, execute),
		scope: "fallback",
		isolation: "resource_snapshot",
		observation: { ...route, capabilities: [...(canObserve ? route.capabilities : []), ...(operations ? WORKSPACE_PATH_MUTATION_EFFECTS.capabilities : [])],
			inputsOnly: request => capturesInputs(request.action, (request.executionContext ?? request.action?.executionContext) as ToolInvocation | undefined),
			diagnostics: () => canObserve ? route.diagnostics() : { state: operations ? "ready" : "unavailable", detail: "Explicit input and computation bindings are available; host read windows remain unproven on Windows" },
			capture: async (context) => {
				const invocation = context.executionContext ?? context.action?.executionContext as ToolInvocation | undefined;
				if (operations && invocation?.captureInputs) return invocation.captureInputs(context.action, operations.maxBytes(), context.callID);
				const inputsOnly = capturesInputs(context.action, invocation);
				if (!inputsOnly && (!context.action || !canObserve || actionSemantics.definition(context.action)?.effect !== "observation")) throw new Error("Actor input capture requires an explicit binding");
				const root = inputsOnly ? invocation?.filesystemRoot ?? context.cwd : context.cwd;
				const version = await captureResourceVersion(inputsOnly ? undefined : context.action, root, actionSemantics, operations && (inputsOnly || invocation?.filesystem) ? operations.maxBytes() : undefined);
				return resourceCapture([version], context.action, actionSemantics, inputsOnly, inputsOnly ? undefined : async () => {
					const validation = await version.manager.seal(version);
					if (validation.expired) throw new Error(validation.reason ?? "resource observation window changed");
				});
			} },
		...(operations ? { speculation: {
			...route,
			dependsOn: filesystemExecutionDependsOn,
			fingerprint: (request) => {
				if (request.action && !(request.action.executionContext as ToolInvocation | undefined)?.filesystem) throw new Error("Resource execution requires an explicitly bound operation");
				return route.fingerprint();
			},
			execute: async (context) => {
				const invocation = context.action.executionContext as ToolInvocation | undefined, execute = invocation?.filesystem;
				if (!execute || context.parentCheckpoint) throw new Error("Resource execution context is not supported");
				const query = await evaluateResourceQuery(undefined, context, invocation?.filesystemRoot ?? context.cwd, view => execute(view, context), actionSemantics, operations.maxBytes(), true);
				if (!query) throw new Error("resource_snapshot_budget_exceeded");
				return resourceSnapshotBranch({ output: query.output }, query.versions, context.action, actionSemantics);
			},
		} } : {}),
	};
}

const resourceVersions = new WeakMap<object, readonly ResourceVersionToken[]>();
const actorResourceInputs = new AsyncLocalStorage<{ inputs: (path?: string) => Iterable<object>; maxBytes: () => number } | undefined>();

/** Native inputs use the same owners and exact object validation on either side of a thread boundary. */
export class ResourceObjectInputs {
	private readonly views: (target?: string) => Iterable<ResourceReadView>;
	private readonly produced?: WeakSet<CapturedFilesystemObject>;
	constructor(inputs: ((target?: string) => Iterable<object>) | ResourceReadView, produced?: WeakSet<CapturedFilesystemObject>) {
		this.views = inputs instanceof ResourceReadView ? () => [inputs] : function* (target) {
			for (const source of inputs(target)) for (const version of resourceVersions.get(source) ?? []) if (version.view?.retained) yield version.view;
		};
		this.produced = produced;
	}
	async borrow(target: string, expected: BigIntStats, maxBytes: number): Promise<StableFilesystemCapture | undefined> {
		for (const view of this.views(target)) {
			if (!view.retained) continue;
			try {
				const captured = await view.borrowObject(target, (capture, handle) => TimelineInterval.outside(async () => {
					if (!capture.content || capture.bytesRead > maxBytes || !sameFilesystemIdentity(expected, capture.stat) ||
						!sameFilesystemIdentity(capture.stat, await handle.stat({ bigint: true }))) return undefined;
					return { ...capture, content: Buffer.from(capture.content), shared: true as const };
				}));
				if (captured) return captured;
			} catch { /* Revocation or a different version leaves the normal FD capture authoritative. */ }
		}
		return undefined;
	}
	retain(target: string, capture: StableFilesystemCapture): boolean {
		for (const view of this.views(target)) if (view.retainObject(target, capture)) return true;
		return false;
	}
	lend(maxBytes: number) {
		const loans: { readonly transfer: CapturedFilesystemTransfer; readonly release: () => void }[] = [], pins = new Set<number>();
		try {
			for (const view of new Set(this.views())) for (const loan of view.lendObjects(object => !this.produced || this.produced.has(object))) {
				if (pins.has(loan.transfer.fd) || loans.length >= 64 || loan.transfer.bytesRead > maxBytes) { loan.release(); continue; }
				pins.add(loan.transfer.fd); maxBytes -= loan.transfer.bytesRead; loans.push(loan);
			}
		} catch (error) { for (const loan of loans) loan.release(); throw error; }
		return { transfer: loans.map(loan => loan.transfer), release: () => { for (const loan of loans) loan.release(); } };
	}
	/** Call-local transport owns pins until acknowledged; only the caller's original version can retain the returned inputs. */
	static receive(transfers: readonly CapturedFilesystemTransfer[], maxBytes: number) {
		const produced = new WeakSet<CapturedFilesystemObject>();
		const view = new ResourceReadView(maxBytes, undefined, undefined, (target, input) => { view.captureObject(target, input); if (input.object) produced.add(input.object); });
		for (const transfer of transfers) { const input = CapturedFilesystemObject.receive(transfer); if (input) view.captureObject(input.realPath, input); }
		return { inputs: new ResourceObjectInputs(view, produced), dispose: () => view.dispose() };
	}
	receive(transfers: readonly CapturedFilesystemTransfer[]): void {
		for (const transfer of transfers) {
			const input = CapturedFilesystemObject.receive(transfer);
			if (input && !this.retain(input.realPath, input)) void input.object?.dispose();
		}
	}
}

/** Whole queries and independent Actor inputs share proof composition; completed inputs survive consumer failure. */
async function evaluateResourceQuery<Output>(
	inputs: readonly ResourceVersionToken[] | undefined, request: Pick<Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0], "signal" | "inputs">,
	root: string, execute: (view: ResourceReadView) => Promise<Output>, semantics: ActionSemanticsRegistry, maxBytes: number,
	retainMetadata = false, validation?: "prepared" | "output", transfer?: (versions: ResourceVersionToken[]) => boolean,
) {
	request.signal.throwIfAborted();
	const initial = inputs ? undefined : await captureResourceVersion(undefined, root, semantics, maxBytes);
	initial?.view?.seal();
	const versions = inputs ?? [initial!], version = versions[0]!, view = version.view!;
	const retained: ResourceVersionToken[] = [];
	let missing: Promise<ResourceVersionToken> | undefined, captured: ResourceVersionToken | undefined;
	// Keep the initiating view first: it owns the query's root resolution, even when another source answers first.
	const proofs = new Map<ResourceVersionToken, Map<string, ResourceObservation>>([[version, new Map()]]);
	const observe = (token: ResourceVersionToken, dependencies: ReadonlySet<string> | undefined, commit = true) => {
		const observations = commit && proofs.get(token) || new Map<string, ResourceObservation>();
		for (const key of dependencies ?? token.observations.keys()) {
			const entry = token.observations.get(key);
			if (!entry) throw new Error("resource_input_proof_missing");
			observations.set(key, entry);
		}
		if (commit) proofs.set(token, observations);
		return { ...token, observations, view: undefined };
	};
	const observeOwner = (tokens: readonly ResourceVersionToken[], dependencies: ReadonlySet<string> | undefined, commit = true) =>
		dependencies ? [observe(tokens[0]!, dependencies, commit)] : tokens.map(token => observe(token, undefined, commit));
	try { return await collectResourceComputation(async () => {
		view.assertComplete(true);
		let output: Output, completed = false, sealed: ResourceVersionToken[] | undefined;
		try { output = await view.evaluate(execute, (dependencies, commit) => observeOwner(versions, dependencies, commit), root,
			function* (target) {
				const owners = [versions, ...[...request.inputs?.(target) ?? []].flatMap(source => resourceVersions.has(source) ? [resourceVersions.get(source)!] : [])];
				for (const tokens of owners) for (const token of tokens) if (token.view) yield { view: token.view,
					observed: (dependencies, commit) => dependencies ? [observe(token, dependencies, commit)] : observeOwner(tokens, undefined, commit) };
			}, async () => {
				const token = await (missing ??= captureResourceVersion(undefined, root, semantics, maxBytes).then(token => captured = token));
				if (!token.view) throw new Error("resource_snapshot_budget_exceeded");
				return { view: token.view, observed: (dependencies, commit) => [observe(token, dependencies, commit)] };
			}, inputs => inputs.map(input => {
				const proof = input.manager.retain(input); retained.push(proof); observe(proof, undefined); return proof;
			}), async proofs => { await validateResourceInputs(proofs, true); }); completed = true;
		} finally { await TimelineInterval.outside(async () => {
			if (!completed && !transfer) return;
			try {
				let bytes = captured?.view?.bytes ?? 0;
				captured?.view?.seal();
				const owned = captured ? [captured] : [];
				for (const [token, observations] of proofs) if (observations.size && token !== captured) {
					const selected = observations.size === token.observations.size ? token : { ...token, observations };
					const proof = retained.includes(selected) ? selected : selected.manager.retain(retainMetadata ? selected : { ...selected, view: undefined }, retainMetadata ? maxBytes - bytes : undefined);
					if (proof !== selected) retained.push(proof);
					owned.push(proof); bytes += proof.view?.bytes ?? 0;
				}
				if (retainMetadata && resourceProofBytes(owned) + bytes > maxBytes) return undefined;
				if (completed && validation === "output") await validateResourceInputs(owned, true);
				if (!transfer) request.signal.throwIfAborted();
				if (!transfer || transfer(owned)) { captured = undefined; retained.length = 0; }
				if (completed && transfer && validation !== "prepared") request.signal.throwIfAborted();
				sealed = owned;
			} catch (error) { if (completed) throw error; } // Optional input retention preserves the original execution failure.
		}); }
		return sealed && { output, versions: sealed };
	}, branch => branch !== undefined); }
	finally { await TimelineInterval.outside(async () => { await Promise.allSettled(retained.map(releaseResourceVersion)); await captured?.release(); await initial?.release(); }); }
}

/** Committed poststates enter the same read view and exact validation as captured inputs. */
export async function createCommittedResourceInputs(
	action: ActionKey, root: string, inputs: ReadonlyMap<string, ResourceInput>, maxBytes: number,
): Promise<WorldBranch<ToolSettlement> & { readonly inputsOnly: true }> {
	const version = await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, maxBytes, inputs);
	try { return resourceSnapshotBranch({ inputsOnly: true }, [version], action, PI_ACTION_SEMANTICS); }
	catch (error) { await version.release(); throw error; }
}

function resourceSnapshotBranch<Outcome extends WorldOutput<ToolSettlement>>(
	outcome: Outcome, versions: readonly ResourceVersionToken[], action: ActionKey | undefined, semantics: ActionSemanticsRegistry,
) {
	for (const version of versions) version.view?.seal();
	if (outcome.inputsOnly && !versions.some(token => token.view?.resources.length || token.view?.hasPreparedInputs)) throw new Error("resource_inputs_empty");
	// Input revocation releases data, not the old result's immutable freshness evidence.
	const version = versions[0]!, proofBytes = resourceProofBytes(versions);
	const executionFingerprint = action?.executionFingerprint;
	const inputSource = Object.freeze({});
	resourceVersions.set(inputSource, versions);
	let owned: readonly ResourceVersionToken[] | undefined = versions;
	const branch: Omit<WorldBranch<ToolSettlement>, "output" | "inputsOnly"> = {
		backend: "resource_version", inputSource, resources: Object.freeze([]),
		invalidateInputs: paths => {
			if (!owned) return [];
			const removed = invalidateResourceInputs(owned, paths);
			if (!removed.length) return removed;
			const remaining = new Set(owned.flatMap(token => token.view?.resources.map(input => input.path) ?? []));
			return removed.filter(target => !remaining.has(target));
		},
		inputResources: version.view && versions.flatMap(token => token.view?.resources ?? []),
		reconstructionScope: "current_action",
		get capturedBytes() { return versions.reduce((bytes, token) => bytes + (token.view?.bytes ?? 0), proofBytes); },
		executionMetrics: Object.freeze({}),
		compatibility: Object.freeze(executionFingerprint === undefined ? { status: "indeterminate", backend: "resource_version", code: "input_only_capture" }
			: { status: "compatible", backend: "resource_version", executionFingerprint }),
		validate: async () => {
			const { expired, reason, changed, ...metrics } = await validateResourceInputs(owned);
			return expired ? { status: "stale", cause: cause("freshness", reason ?? "resource_changed"), metrics,
				...(changed?.length && owned?.some(token => token.view?.hasPreparedInputs) ? { reconstruct: true as const } : {}) } : { status: "valid", metrics };
		},
		...(version.view ? { reconstruct: async (request: Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0]) => {
			request.signal.throwIfAborted();
			const invocation = request.action.executionContext as ToolInvocation | undefined, definition = request.action.semantics ?? semantics.definition(request.action);
			const execute = invocation?.filesystem, root = invocation?.filesystemRoot ?? (request.action.executionFingerprint === executionFingerprint ? version.root : undefined);
			if (!owned || !execute || !root || definition?.effect !== "observation" || !definition.resourceScope || !effectCapabilitiesCover(RESOURCE_OBSERVATION_EFFECTS.capabilities, definition.requirements)) return undefined;
			const query = await evaluateResourceQuery(versions, request, root, view => execute(view, request), semantics,
				Math.max(0, version.view!.remainingBytes - versions.slice(1).reduce((bytes, token) => bytes + (token.view?.bytes ?? 0), 0)));
			return query && Object.assign(resourceSnapshotBranch({ output: query.output }, query.versions, request.action, semantics), { requiresQueryValidation: true as const });
		} } : {}),
		commit: async () => {
			// Nothing was applied: an input-only branch can only be declined, never poison the Actor call.
			if (outcome.inputsOnly) throw effectCommitFailure(new Error("input_only_branch"), "recoverable");
			if (!owned) throw new Error("resource snapshot is disposed");
			return outcome.output;
		},
		dispose: () => { owned = undefined; resourceVersions.delete(inputSource);
			return versions.length === 1 ? version.release() : Promise.allSettled(versions.map(releaseResourceVersion)).then(() => {});
		},
	};
	return Object.assign(branch, outcome);
}

async function validateResourceInputs(tokens?: readonly ResourceVersionToken[], revokeUnknown = false) {
	const result = await validateResourceVersion(tokens);
	if (result.changed?.length || result.expired && revokeUnknown) for (const token of tokens ?? [])
		for (const view of new Set([token.view, token.inputView?.deref()])) view?.invalidate(new Set(result.changed ?? token.observations.keys()));
	if (revokeUnknown && result.expired) throw new Error(result.reason ?? "resource_operation_stale");
	return result;
}

/** One transfer boundary for observed windows, native inputs and explicit Actor preparations. */
function resourceCapture(versions: ResourceVersionToken[], action: ActionKey | undefined, semantics: ActionSemanticsRegistry, inputsOnly = false,
	beforeSeal?: () => Promise<void>, close?: () => void): WorldResultCapture<ToolSettlement> {
	const inputSource = Object.freeze({}); let open = true, sealing = false;
	resourceVersions.set(inputSource, versions);
	const dispose = async () => {
		open = false; close?.(); resourceVersions.delete(inputSource); await Promise.allSettled(versions.splice(0).map(releaseResourceVersion));
	};
	return { ...(inputsOnly ? { inputsOnly: true as const, inputSource } : {}), dispose, seal: async output => {
		if (!open || sealing) throw new Error("resource_snapshot_capture_consumed");
		sealing = true;
		try {
			if (!inputsOnly && output === undefined) throw new Error("authoritative_result_missing");
			await beforeSeal?.();
			if (!open) throw new Error("resource_snapshot_capture_disposed");
			const branch = resourceSnapshotBranch(inputsOnly ? { inputsOnly: true } : { output: output! }, [...versions], action, semantics);
			versions.length = 0; return branch;
		} finally { await dispose(); }
	} };
}

/** Explicit Actor callback: native effects execute once; only proven inputs and pure preparations are shared. */
export function bindResourceOperation(execute: NonNullable<ToolInvocation["filesystem"]>, root: string): Pick<ToolInvocation, "authoritative" | "captureInputs" | "filesystem" | "filesystemRoot"> {
	type Capture = { maxBytes: number; inputs: Map<string, ResourceInput>; versions: ResourceVersionToken[]; started?: boolean };
	const active = new Map<string, Capture>();
	const bytes = (owned: Capture, inputs = true) => resourceProofBytes(owned.versions) + owned.versions.reduce((sum, token) => sum + (token.view?.bytes ?? 0), 0) +
		(inputs ? [...owned.inputs].reduce((sum, [target, input]) => sum + target.length * 2 + 128 + (input instanceof Uint8Array ? input.length :
			input && "names" in input ? input.names?.reduce((sum, name) => sum + name.length * 2 + 16, 0) ?? 0 : 0), 0) : 0);
	return { filesystem: execute, filesystemRoot: root,
		captureInputs: (action, maxBytes, callID) => {
			if (active.has(callID) || !Number.isFinite(maxBytes) || maxBytes <= 0) throw new Error("resource_snapshot_budget_invalid");
			const owned: Capture = { maxBytes, inputs: new Map(), versions: [] };
			active.set(callID, owned);
			return resourceCapture(owned.versions, action, PI_ACTION_SEMANTICS, true, async () => {
				if (active.get(callID) === owned) active.delete(callID);
				// Infer entries only for directories this callback actually created.
				const names = new Map([...owned.inputs].filter(([, input]) => input && !(input instanceof Uint8Array) && !Object.keys(input).length).map(([target]) => [target, [] as string[]]));
				for (const target of owned.inputs.keys()) names.get(path.dirname(target))?.push(path.basename(target));
				for (const [target, entries] of names) owned.inputs.set(target, { names: entries });
				if (owned.inputs.size) owned.versions.push(await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, Math.max(0, maxBytes - bytes(owned, false)), owned.inputs));
			}, () => { if (active.get(callID) === owned) active.delete(callID); owned.inputs.clear(); });
		},
		authoritative: async request => {
			const owned = active.get(request.callID)?.started ? undefined : active.get(request.callID), scope = actorResourceInputs.getStore();
			if (owned) owned.started = true;
			const query = <Value>(execute: (view: ResourceReadView) => Promise<Value>, signal = request.signal, validation: "prepared" | "output" = "prepared") =>
				!owned && !scope ? Promise.resolve(undefined) : TimelineInterval.outside(() =>
					evaluateResourceQuery(undefined, { signal, inputs: scope?.inputs }, root, execute, PI_ACTION_SEMANTICS,
						owned ? Math.max(0, owned.maxBytes - bytes(owned)) : scope!.maxBytes(), true, validation,
						versions => !!owned && active.get(request.callID) === owned && !!owned.versions.push(...versions)));
			const read = async <Value>(execute: (view: ResourceReadView) => Promise<Value>, native: () => Promise<Value>) => {
				try { const result = await query(execute, request.signal, "output"); if (result) return result.output; } catch { /* Unproven inputs leave the native operation authoritative. */ }
				request.signal.throwIfAborted(); return native();
			};
			const retain = (target: string, input: ResourceInput | string) => {
				if (!owned || active.get(request.callID) !== owned || relativeFilesystemPath(root, target) === undefined) return;
				owned.inputs.delete(target);
				if (typeof input === "string" || input instanceof Uint8Array) { if (Buffer.byteLength(input) > owned.maxBytes) return; input = Buffer.from(input); }
				owned.inputs.set(target, input); if (bytes(owned) > owned.maxBytes) owned.inputs.delete(target);
			};
			const host: ToolFilesystemOperations = {
				readFile: (target, maxBytes) => read(view => view.readFile(target, maxBytes), async () => { const content = await fs.readFile(target); retain(target, content); return content.subarray(0, maxBytes); }),
				access: (target, writable) => fs.access(target, fs.constants.R_OK | (writable ? fs.constants.W_OK : 0)),
				exists: target => read(view => view.exists(target), async () => { try { await fs.access(target); return true; } catch { return false; } }),
				stat: (target, fields, includeSize = true) => read<ToolFilesystemStat>(view => view.stat(target, fields, includeSize), async () => {
					const info = await (fields === "entry" ? fs.lstat(target) : fs.stat(target));
					const type = info.isDirectory() ? "directory" : info.isFile() ? "file" : info.isSymbolicLink() ? "symlink" : "special";
					return { isDirectory: () => info.isDirectory(), size: includeSize ? info.size : undefined, type, ...(type === "symlink" ? { link: await fs.readlink(target) } : { realPath: await fs.realpath(target) }) };
				}),
				readdir: target => read(view => view.readdir(target), async () => { const names = await fs.readdir(target); retain(target, { names }); return names; }),
				mkdir: async target => {
					const created = await fs.mkdir(target, { recursive: true });
					if (created) for (let directory = target; relativeFilesystemPath(path.toNamespacedPath(created), path.toNamespacedPath(directory)) !== undefined; directory = path.dirname(directory)) {
						retain(directory, {}); if (path.dirname(directory) === directory) break;
					}
				},
				writeFile: async (target, content) => { await fs.writeFile(target, content, "utf-8"); retain(target, content); },
				prepare: async (binding, key, build, consume, hint, signal = request.signal) => {
					let consuming = false, completed: { value: Awaited<ReturnType<typeof consume>> } | undefined;
					const use: typeof consume = async value => { consuming = true; completed = { value: await consume(value) }; return completed.value; };
					try { await query(view => view.prepare(binding, key, build, use, hint, signal), signal); }
					catch (error) { if (consuming && !completed) throw error; }
					if (completed) return completed.value;
					signal.throwIfAborted(); const resource = await build(host, signal);
					try { return await use(resource.value); } finally { await resource.dispose(); }
				},
			};
			return execute(host, request);
		},
	};
}
