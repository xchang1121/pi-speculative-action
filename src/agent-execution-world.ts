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
	watchResourceVersion,
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
	const route = {
		capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
		fingerprint: () => "resource-version:v2",
		diagnostics: () => ({
			state: "ready" as const,
			detail: "Resource-version snapshots are available",
		}),
	};
	const capture = async (context: SpeculativeToolExecutionContext, retainBytes?: number): Promise<WorldResultCapture<ToolSettlement> & { readonly view?: ResourceReadView }> => {
		const setupStarted = performance.now();
		let version: ResourceVersionToken | undefined = await captureResourceVersion(context.action, context.cwd, actionSemantics, retainBytes);
		const setupMs = Math.max(0, performance.now() - setupStarted);
		return {
			view: version.view,
			seal: async (output) => {
				const owned = version;
				version = undefined;
				if (!owned) throw new Error("resource snapshot capture is already consumed");
				try {
					const validation = await owned.manager.seal(owned);
					if (validation.expired) throw new Error(validation.reason ?? "resource observation window changed");
					owned.view?.assertComplete();
					return resourceSnapshotBranch(output, owned, context.action.executionFingerprint, setupMs);
				} catch (error) {
					releaseResourceVersion(owned);
					throw error;
				}
			},
			dispose: () => { if (version) releaseResourceVersion(version); version = undefined; },
		};
	};
	return {
		id: "resource_version",
		scope: "fallback",
		isolation: "resource_snapshot",
		observation: { ...route, capture },
		...(operations?.tools.length ? { speculation: {
			...route,
			tools: operations.tools,
			fingerprint: (request) => {
				if (request.action && !(request.action.executionContext as ToolInvocation | undefined)?.resources) {
					throw new Error("Resource execution requires an explicitly bound operation");
				}
				return route.fingerprint();
			},
			execute: async (context) => {
				const execute = (context.action.executionContext as ToolInvocation | undefined)?.resources;
				if (!execute || context.parentCheckpoint) throw new Error("Resource execution context is not supported");
				context.signal.throwIfAborted();
				const owned = await capture(context, operations.maxBytes());
				try {
					const output = await execute(owned.view!, context);
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
	let stopWatcher: (() => void) | undefined;
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
		watch: (onInvalidated) => {
			if (owned && !stopWatcher) stopWatcher = watchResourceVersion(owned, onInvalidated);
		},
		commit: async () => {
			if (!owned) throw new Error("resource snapshot is disposed");
			return output;
		},
		dispose: () => {
			const released = owned;
			owned = undefined;
			stopWatcher?.();
			stopWatcher = undefined;
			releaseResourceVersion(released);
		},
	};
}
