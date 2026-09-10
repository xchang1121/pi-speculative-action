import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type {
	ExecutionWorld,
	ExecutionScope,
	WorldBranch,
	WorldCheckpoint,
	WorldResultCapture,
} from "./execution-world.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "./effect-model.ts";
import {
	captureResourceVersion,
	type ResourceReadView,
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
	readonly executionScope?: ExecutionScope;
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
		fingerprint: () => "resource-version:v3",
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
					const validation = await (onDemand ? owned.manager.validate(owned) : owned.manager.seal(owned));
					if (validation.expired) throw new Error(validation.reason ?? "resource observation window changed");
					return resourceSnapshotBranch(output, owned, context.action.executionFingerprint, setupMs);
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

function resourceSnapshotBranch(
	output: ToolSettlement, version: ResourceVersionToken, executionFingerprint: string, setupMs: number,
): WorldBranch<ToolSettlement> {
	let owned: ResourceVersionToken | undefined = version;
	return {
		backend: "resource_version", output, resources: Object.freeze([]),
		capturedBytes: version.view?.bytes ?? 0,
		executionMetrics: Object.freeze({ setupMs }),
		compatibility: Object.freeze({ status: "compatible", backend: "resource_version", executionFingerprint }),
		validate: async () => {
			const { expired, reason, ...metrics } = await validateResourceVersion(owned);
			return expired
				? { status: "stale", cause: cause("freshness", reason ?? "resource_changed"), metrics }
				: { status: "valid", metrics };
		},
		...(version.view ? { reconstruct: async (request: Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0]) => {
			request.signal.throwIfAborted();
			const execute = (request.action.executionContext as ToolInvocation | undefined)?.filesystem;
			if (!owned?.view || !execute || request.action.executionFingerprint !== executionFingerprint) return undefined;
			const result = await owned.view.evaluate((view) => execute(view, request));
			request.signal.throwIfAborted();
			return result;
		} } : {}),
		commit: async () => {
			if (!owned) throw new Error("resource snapshot is disposed");
			return output;
		},
		dispose: () => {
			owned = undefined;
			return version.release();
		},
	};
}
