import type { AgentTool } from "@earendil-works/pi-agent-core";
import path from "node:path";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type {
	ExecutionWorld,
	ExecutionScope,
	ExecutionOperationAdoption,
	WorldBranch,
	WorldCheckpoint,
	WorldResultCapture,
} from "./execution-world.ts";
import { effectCapabilitiesCover, RESOURCE_OBSERVATION_EFFECTS } from "./effect-model.ts";
import {
	captureResourceVersion,
	type ResourceReadView,
	type ResourceObservation,
	type ResourceVersionToken,
	releaseResourceVersion,
	validateResourceVersion,
} from "./resource-version.ts";
import { cause } from "./settlement.ts";
import type { ToolInvocation, ToolSettlement } from "./tool-settlement.ts";

/** Host tool call supplied to any OS sandbox or safe local substitute. */
export interface SpeculativeToolExecutionContext {
	readonly cwd: string;
	readonly tool: AgentTool;
	readonly toolName: string;
	readonly args: unknown;
	readonly action: ActionKey;
	readonly callID: string;
	readonly signal: AbortSignal;
	/** Input owners remain leased for this execution; derived results must own their evidence. */
	readonly inputs?: (path: string) => Iterable<object>;
	readonly executionScope?: ExecutionScope;
	readonly onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
	readonly acceptOperationScope?: (scope: ExecutionScope) => boolean;
	/** Optional immutable parent state for source-neutral multi-step execution. */
	readonly parentCheckpoint?: WorldCheckpoint;
}

export type AgentExecutionWorld = ExecutionWorld<SpeculativeToolExecutionContext, ToolSettlement>;
export type SpeculativeAgentExecutionWorld = AgentExecutionWorld & {
	readonly speculation: NonNullable<AgentExecutionWorld["speculation"]>;
};

/** Observe Actor reads; only explicitly bound operations may execute ahead over sealed resource data. */
export function createResourceSnapshotExecutionWorld(
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
	operations?: { readonly tools: readonly string[]; readonly maxBytes: () => number },
): AgentExecutionWorld {
	const canObserve = process.platform !== "win32";
	const route = {
		capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
		fingerprint: () => "resource-version",
		diagnostics: () => ({
			state: "ready" as const,
			detail: "Sealed file inputs; host reads may update access times (not an OS snapshot)",
		}),
	};
	const capture = async (context: SpeculativeToolExecutionContext, retainBytes?: number, onDemand = false): Promise<WorldResultCapture<ToolSettlement> & { readonly view?: ResourceReadView }> => {
		if (!onDemand && !canObserve) throw new Error("Windows path binding stamps cannot certify host execution windows");
		const setupStarted = performance.now();
		const root = onDemand ? (context.action.executionContext as ToolInvocation | undefined)?.filesystemRoot ?? context.cwd : context.cwd;
		let version: ResourceVersionToken | undefined = await captureResourceVersion(onDemand ? undefined : context.action, root, actionSemantics, retainBytes);
		let disposal: void | Promise<void>;
		const setupMs = Math.max(0, performance.now() - setupStarted);
		return {
			view: version.view,
			seal: async (output) => {
				const owned = version;
				version = undefined;
				if (!owned) throw new Error("resource snapshot capture is already consumed");
				try {
					owned.view?.seal();
					// Bound execution already owns its inputs; adoption checks their current versions.
					if (!onDemand) {
						const validation = await owned.manager.seal(owned);
						if (validation.expired) throw new Error(validation.reason ?? "resource observation window changed");
					}
					return resourceSnapshotBranch(output, [owned], context.action.executionFingerprint, setupMs, actionSemantics);
				} catch (error) {
					await releaseResourceVersion(owned);
					throw error;
				}
			},
			dispose: () => {
				const released = version;
				version = undefined;
				return disposal ??= releaseResourceVersion(released);
			},
		};
	};
	return {
		id: "resource_version",
		scope: "fallback",
		isolation: "resource_snapshot",
		observation: { ...route, capabilities: canObserve ? route.capabilities : [],
			diagnostics: () => canObserve ? route.diagnostics() : { state: "unavailable", detail: "Host path-binding observation is unproven on Windows; captured-input execution remains available" },
			capture: (context) => capture(context,
			operations?.tools.includes(context.toolName) && (context.action.executionContext as ToolInvocation | undefined)?.filesystem
				? operations.maxBytes() : undefined) },
		...(operations?.tools.length ? { speculation: {
			...route,
			tools: operations.tools,
			fingerprint: (request) => {
				if (request.action && !(request.action.executionContext as ToolInvocation | undefined)?.filesystem) {
					throw new Error("Resource execution requires an explicitly bound operation");
				}
				return route.fingerprint();
			},
			execute: async (context) => {
				const execute = (context.action.executionContext as ToolInvocation | undefined)?.filesystem;
				if (!execute || context.parentCheckpoint) throw new Error("Resource execution context is not supported");
				context.signal.throwIfAborted();
				const attempted = new Set<object>();
				for (const resource of context.action.resources) for (const source of context.inputs?.(path.resolve(context.action.resourceRoot ?? context.cwd, resource)) ?? []) {
					if (attempted.has(source)) continue;
					attempted.add(source);
					const owner = resourceVersions.get(source);
					if (!owner) continue;
					const retained: ResourceVersionToken[] = [];
					try {
						const query = await evaluateResourceInputs(owner, context, actionSemantics);
						if (!query || query.capturedBytes > operations.maxBytes()) continue;
						for (const version of query.versions) retained.push(version.manager.retain(version));
						return resourceSnapshotBranch(query.output, retained, context.action.executionFingerprint, 0, actionSemantics, query.capturedBytes);
					} catch {
						await Promise.allSettled(retained.map(releaseResourceVersion));
						context.signal.throwIfAborted();
						// Incomplete input coverage falls through to the same bound capture executor.
					}
				}
				const owned = await capture(context, operations.maxBytes(), true);
				try {
					if (!owned.view) throw new Error("resource_snapshot_budget_exceeded");
					const output = await execute(owned.view, context);
					context.signal.throwIfAborted();
					return await owned.seal(output);
				} finally { await owned.dispose(); }
			},
		} } : {}),
	};
}

type ResourceInputOwner = { readonly version: ResourceVersionToken; readonly executionFingerprint: string };
const resourceVersions = new WeakMap<object, ResourceInputOwner>();

/** Actor reconstruction and predicted execution use the same confined inputs and dependency proof. */
async function evaluateResourceInputs(
	{ version, executionFingerprint }: ResourceInputOwner,
	request: Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0], semantics: ActionSemanticsRegistry,
) {
	request.signal.throwIfAborted();
	const invocation = request.action.executionContext as ToolInvocation | undefined;
	const execute = invocation?.filesystem, definition = request.action.semantics ?? semantics.definition(request.action);
	const root = invocation?.filesystemRoot ?? (request.action.executionFingerprint === executionFingerprint ? version.root : undefined);
	if (!version.view || !execute || !root || definition?.effect !== "observation" || !definition.resourceScope) return undefined;
	if (!effectCapabilitiesCover(RESOURCE_OBSERVATION_EFFECTS.capabilities, definition.requirements)) return undefined;
	const proofs = new Map<ResourceVersionToken, Map<string, ResourceObservation>>();
	let capturedBytes = 0;
	const observe = (token: ResourceVersionToken, dependencies: ReadonlySet<string> | undefined) => {
		let observations = proofs.get(token);
		if (!observations) proofs.set(token, observations = new Map());
		for (const key of dependencies ?? token.observations.keys()) {
			const entry = token.observations.get(key);
			if (!entry) throw new Error("resource_input_proof_missing");
			if (!observations.has(key)) capturedBytes += key.length * 2 + 64;
			observations.set(key, entry);
		}
	};
	const output = await version.view.evaluate(view => execute(view, request), dependencies => observe(version, dependencies), root,
		request.inputs && function* (target) {
			for (const source of request.inputs!(target)) {
				const token = resourceVersions.get(source)?.version;
				if (token?.view) yield { view: token.view, observed: dependencies => observe(token, dependencies) };
			}
		});
	request.signal.throwIfAborted();
	return { output, capturedBytes, versions: [...proofs].map(([token, observations]) => ({ ...token, observations })) };
}

function resourceSnapshotBranch(
	output: ToolSettlement, versions: readonly ResourceVersionToken[], executionFingerprint: string, setupMs: number, semantics: ActionSemanticsRegistry,
	capturedBytes = versions[0]!.view?.bytes ?? 0,
): WorldBranch<ToolSettlement> {
	const version = versions[0]!;
	const inputSource = Object.freeze({});
	resourceVersions.set(inputSource, { version, executionFingerprint });
	let owned: readonly ResourceVersionToken[] | undefined = versions;
	const validate = async (token: ResourceVersionToken | readonly ResourceVersionToken[] | undefined) => {
		const { expired, reason, ...metrics } = await validateResourceVersion(owned && token);
		return expired
			? { status: "stale" as const, cause: cause("freshness", reason ?? "resource_changed"), metrics }
			: { status: "valid" as const, metrics };
	};
	return {
		backend: "resource_version", output, inputSource, resources: Object.freeze([]),
		inputResources: version.view?.resources,
		reconstructionScope: "current_action",
		capturedBytes,
		executionMetrics: Object.freeze({ setupMs }),
		compatibility: Object.freeze({ status: "compatible", backend: "resource_version", executionFingerprint }),
		validate: () => validate(owned),
		...(version.view ? { reconstruct: async (request: Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0]) => {
			if (!owned) return undefined;
			const query = await evaluateResourceInputs({ version, executionFingerprint }, request, semantics);
			return query && { output: query.output, validate: () => validate(query.versions), capturedBytes: query.capturedBytes,
				...(query.versions.length > 1 ? { requiresQueryValidation: true as const } : {}),
				compatibility: { status: "compatible", backend: "resource_version", executionFingerprint: request.action.executionFingerprint } };
		} } : {}),
		commit: async () => {
			if (!owned) throw new Error("resource snapshot is disposed");
			return output;
		},
		dispose: () => {
			owned = undefined;
			resourceVersions.delete(inputSource);
			return versions.length === 1 ? version.release() : Promise.allSettled(versions.map(releaseResourceVersion)).then(() => {});
		},
	};
}
