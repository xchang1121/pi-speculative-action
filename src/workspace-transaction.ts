import type { WorkspaceStructureSnapshot } from "./workspace-state.ts";

/** Exact regular-file transition captured around one workspace operation. */
export interface WorkspaceRegularDelta {
	readonly relativePath: string;
	readonly before?: Uint8Array;
	readonly after?: Uint8Array;
	readonly beforeMode?: number;
	readonly afterMode?: number;
}

export type WorkspaceTransactionDelta =
	| {
			readonly complete: true;
			readonly changes: readonly WorkspaceRegularDelta[];
			readonly before: WorkspaceStructureSnapshot;
			readonly after: WorkspaceStructureSnapshot;
	  }
	| {
			readonly complete: false;
			readonly changes: readonly WorkspaceRegularDelta[];
			readonly reason: string;
			readonly before?: WorkspaceStructureSnapshot;
			readonly after?: WorkspaceStructureSnapshot;
	  };

/**
 * One mutation interval in a workspace. A driver may reject attribution when another interval
 * overlaps, but it must never alter or re-execute the operation itself.
 */
export interface WorkspaceTransactionCapture {
	readonly finish: () => Promise<WorkspaceTransactionDelta>;
	readonly abort: () => Promise<void>;
}

/** Generic mutation journal installed by the concrete workspace implementation. */
export interface WorkspaceTransactionDriver {
	readonly begin: () => Promise<WorkspaceTransactionCapture>;
}

/**
 * Keep transaction machinery off read-only/replay paths. Concurrent first users share one driver,
 * and a failed construction remains failed rather than silently changing observation policy.
 */
export function deferredWorkspaceTransactionDriver(
	create: () => Promise<WorkspaceTransactionDriver>,
): WorkspaceTransactionDriver {
	let driver: Promise<WorkspaceTransactionDriver> | undefined;
	return {
		begin: async () => (await (driver ??= create())).begin(),
	};
}
