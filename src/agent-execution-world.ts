import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type {
	ExecutionWorld,
	ExecutionScope,
	WorldBranch,
	WorldCheckpoint,
	WorldCompatibilityEvidence,
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
import type { ResourceValidation } from "./settlement.ts";
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
					return new ResourceSnapshotBranch(output, owned, context.action.executionFingerprint, setupMs);
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

class ResourceSnapshotBranch implements WorldBranch<ToolSettlement> {
	readonly backend = "resource_version" as const;
	readonly resources: readonly string[] = Object.freeze([]);
	readonly capturedBytes: number;
	readonly executionMetrics: { readonly setupMs: number };
	readonly compatibility: WorldCompatibilityEvidence;
	readonly output: ToolSettlement;
	private readonly version: ResourceVersionToken;
	private stopWatcher?: () => void;
	private disposed = false;

	constructor(output: ToolSettlement, version: ResourceVersionToken, executionFingerprint: string, setupMs: number) {
		this.output = output;
		this.version = version;
		this.capturedBytes = version.view?.bytes ?? 0;
		this.executionMetrics = Object.freeze({ setupMs });
		this.compatibility = Object.freeze({
			status: "compatible",
			backend: this.backend,
			executionFingerprint,
		});
	}

	async validate(): Promise<ResourceValidation> {
		const validation = await validateResourceVersion(this.version);
		const metrics = {
			durationMs: validation.durationMs,
			bytesRead: validation.bytesRead,
			filesRead: validation.filesRead,
			mode: validation.mode,
		};
		return validation.expired
			? {
					status: "stale",
					cause: cause("freshness", validation.reason ?? "resource_changed"),
					metrics,
				}
			: { status: "valid", metrics };
	}

	watch(onInvalidated: (changedPath?: string) => void): void {
		if (this.disposed || this.stopWatcher) return;
		this.stopWatcher = watchResourceVersion(this.version, onInvalidated);
	}

	async commit(): Promise<ToolSettlement> {
		return this.output;
	}

	dispose(): void {
		if (this.disposed) return;
		this.disposed = true;
		this.stopWatcher?.();
		this.stopWatcher = undefined;
		releaseResourceVersion(this.version);
	}
}
