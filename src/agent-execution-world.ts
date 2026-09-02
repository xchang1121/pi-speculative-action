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
	type ResourceVersionToken,
	releaseResourceVersion,
	validateResourceVersion,
	watchResourceVersion,
} from "./resource-version.ts";
import type { ResourceValidation } from "./settlement.ts";
import { cause } from "./settlement.ts";
import { toolErrorSettlement, type ToolSettlement } from "./tool-settlement.ts";

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

export type SpeculativeAgentExecutionWorld = ExecutionWorld<SpeculativeToolExecutionContext, ToolSettlement>;

/** Read-only local substitute: execute now, then prove the observed resources are still current. */
export function createResourceSnapshotExecutionWorld(
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
): SpeculativeAgentExecutionWorld {
	const route = {
		capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
		fingerprint: () => "resource-version:v1",
		diagnostics: () => ({
			state: "ready" as const,
			detail: "Resource-version snapshots are available",
		}),
	};
	const capture = async (context: SpeculativeToolExecutionContext): Promise<WorldResultCapture<ToolSettlement>> => {
		const setupStarted = performance.now();
		const version = await captureResourceVersion(context.action, context.cwd, actionSemantics);
		return new ResourceSnapshotCapture(
			version,
			context.action.executionFingerprint,
			Math.max(0, performance.now() - setupStarted),
		);
	};
	return {
		id: "resource_version",
		scope: "fallback",
		isolation: "resource_snapshot",
		observation: { ...route, capture },
		speculation: {
			...route,
			execute: async (context) => {
				const captured = await capture(context);
				let output: ToolSettlement;
				try {
					output = {
						result: await context.tool.execute(context.callID, context.args as never, context.signal),
						isError: false,
					};
				} catch (error) {
					output = toolErrorSettlement(error);
				}
				return captured.seal(output);
			},
		},
	};
}

class ResourceSnapshotCapture implements WorldResultCapture<ToolSettlement> {
	private state: "open" | "sealed" | "disposed" = "open";
	private readonly version: ResourceVersionToken;
	private readonly executionFingerprint: string;
	private readonly setupMs: number;

	constructor(version: ResourceVersionToken, executionFingerprint: string, setupMs: number) {
		this.version = version;
		this.executionFingerprint = executionFingerprint;
		this.setupMs = setupMs;
	}

	async seal(output: ToolSettlement): Promise<WorldBranch<ToolSettlement>> {
		if (this.state !== "open") throw new Error(`resource snapshot capture is already ${this.state}`);
		const branch = new ResourceSnapshotBranch(output, this.version, this.executionFingerprint, this.setupMs);
		this.state = "sealed";
		return branch;
	}

	dispose(): void {
		if (this.state !== "open") return;
		this.state = "disposed";
		releaseResourceVersion(this.version);
	}
}

class ResourceSnapshotBranch implements WorldBranch<ToolSettlement> {
	readonly backend = "resource_version" as const;
	readonly resources: readonly string[] = Object.freeze([]);
	readonly capturedBytes = 0;
	readonly executionMetrics: { readonly setupMs: number };
	readonly compatibility: WorldCompatibilityEvidence;
	readonly output: ToolSettlement;
	private readonly version: ResourceVersionToken;
	private stopWatcher?: () => void;
	private disposed = false;

	constructor(output: ToolSettlement, version: ResourceVersionToken, executionFingerprint: string, setupMs: number) {
		this.output = output;
		this.version = version;
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
