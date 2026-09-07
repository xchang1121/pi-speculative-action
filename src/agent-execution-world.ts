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
import type { ToolSettlement } from "./tool-settlement.ts";

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

/** Observe Actor-authorized reads and retain their result while the exact resources remain current. */
export function createResourceSnapshotExecutionWorld(
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
): AgentExecutionWorld {
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
		let version: ResourceVersionToken | undefined = await captureResourceVersion(context.action, context.cwd, actionSemantics);
		const setupMs = Math.max(0, performance.now() - setupStarted);
		return {
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
