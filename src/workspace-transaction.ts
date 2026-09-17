import type { WorkspaceStructureSnapshot } from "./workspace-state.ts";

/** Apply removals child first and creations parent first, preserving each caller's path order. */
export function orderWorkspaceChanges<Value>(
	values: readonly Value[],
	order: (value: Value) => {
		readonly change: { readonly kind?: "file" | "directory"; readonly after?: unknown };
		readonly depth: number;
		readonly key: string;
	},
): Value[] {
	if (values.length < 2) return [...values];
	return values.map(value => {
		const { change, depth, key } = order(value);
		const removed = change.after === undefined;
		return { value, key, depth: removed ? -depth : depth,
			phase: change.kind === "directory" ? (removed ? 1 : 2) : (removed ? 0 : 3) };
	}).sort((left, right) => left.phase - right.phase || left.depth - right.depth || left.key.localeCompare(right.key))
		.map(({ value }) => value);
}

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
	/** Borrow a bounded copy of an active interval's original regular-file bytes. */
	readonly readBefore?: (relativePath: string, maxBytes: number) => Promise<Uint8Array | undefined>;
	readonly finish: () => Promise<WorkspaceTransactionDelta>;
	readonly abort: () => Promise<void>;
}

/** Generic mutation journal installed by the concrete workspace implementation. */
export interface WorkspaceTransactionDriver {
	readonly begin: () => Promise<WorkspaceTransactionCapture>;
	/** Release driver-owned journal handles before the enclosing workspace is removed. */
	readonly dispose: () => Promise<void>;
}

/** Content-free structure view supplied by the concrete workspace storage driver. */
export interface WorkspaceStructureDriver {
	readonly capture: () => Promise<WorkspaceStructureSnapshot>;
}

/**
 * Keep transaction machinery off read-only/replay paths. Concurrent first users share one driver,
 * and a failed construction remains failed rather than silently changing observation policy.
 */
export function deferredWorkspaceTransactionDriver(
	create: () => Promise<WorkspaceTransactionDriver>,
): WorkspaceTransactionDriver {
	let driver: Promise<WorkspaceTransactionDriver> | undefined;
	let disposal: Promise<void> | undefined;
	return {
		begin: async () => {
			if (!disposal) {
				const resolved = await (driver ??= Promise.resolve().then(create));
				if (!disposal) return resolved.begin();
				await disposal;
			}
			throw new Error("workspace transaction driver is disposed");
		},
		dispose: () => disposal ??= driver ? driver.then(resolved => resolved.dispose()) : Promise.resolve(),
	};
}
