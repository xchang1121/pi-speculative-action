import type { FsSnapshotId } from "@thinkthread/agent-posix";
import type { ExecutionScope, WorldCheckpoint } from "../execution-world.ts";
import type { DurableFsExecutor } from "./durable-fs.ts";
import { TimelineInterval } from "../task-timing.ts";

interface ActiveTurn {
	readonly scope: ExecutionScope;
	generation: number;
	base?: Promise<SnapshotLease>;
	closed: boolean;
}

type SnapshotData = { readonly snapshotId: FsSnapshotId; readonly computation?: TimelineInterval };
type SnapshotResource = SnapshotData & { references: number; removal?: Promise<void> };

export interface SnapshotLease {
	readonly resource: SnapshotResource;
	readonly id: FsSnapshotId;
	readonly computation?: TimelineInterval;
	retain(): SnapshotLease;
	release(): Promise<void>;
}

export class ThinkThreadSnapshotPool {
	private readonly durable: DurableFsExecutor;
	private readonly resources = new Map<FsSnapshotId, SnapshotResource>();
	private readonly checkpoints = new WeakMap<WorldCheckpoint, SnapshotLease>();
	private active?: ActiveTurn;
	private disposed = false;

	constructor(durable: DurableFsExecutor) {
		this.durable = durable;
	}

	async acquireRoot(scope: ExecutionScope): Promise<{ readonly lease: SnapshotLease; readonly lineage: string; readonly depth: 0 }> {
		if (this.disposed) throw new Error("ThinkThread snapshot pool is disposed");
		let turn = this.active;
		if (!turn || turn.scope.sessionID !== scope.sessionID || turn.scope.turnID !== scope.turnID) {
			const cleanup = this.finishActive();
			// Publish ownership before awaiting cleanup; concurrent acquisitions see one BASE.
			this.active = turn = { scope, generation: 0, closed: false };
			await cleanup;
		}
		if (turn.closed) throw new Error("ThinkThread execution scope was closed");
		if (!turn.base) {
			const attempt = this.createBase();
			turn.base = attempt;
			attempt.catch(() => {
				if (turn.base === attempt) turn.base = undefined;
			});
		}
		const generation = turn.generation;
		const owner = await turn.base;
		if (turn.closed) throw new Error("ThinkThread execution scope was closed");
		return {
			lease: owner.retain(),
			lineage: JSON.stringify([scope.sessionID, scope.turnID, generation, owner.id]),
			depth: 0,
		};
	}

	acquireCheckpoint(checkpoint: WorldCheckpoint): {
		readonly lease: SnapshotLease;
		readonly lineage: string;
		readonly depth: number;
	} {
		const snapshot = this.checkpoints.get(checkpoint);
		if (
			!snapshot ||
			checkpoint.backend !== "ThinkThread" ||
			!this.resources.has(checkpoint.id as FsSnapshotId)
		) {
			throw new Error("Execution world checkpoint belongs to another backend");
		}
		return {
			lease: snapshot.retain(),
			lineage: checkpoint.lineage,
			depth: checkpoint.depth,
		};
	}

	checkpoint(snapshot: SnapshotLease, lineage: string, depth: number): WorldCheckpoint {
		if (this.resources.get(snapshot.id) !== snapshot.resource) {
			throw new Error(`ThinkThread snapshot ${snapshot.id} is not owned by this execution world`);
		}
		const checkpoint = { backend: "ThinkThread", id: snapshot.id, lineage, depth };
		this.checkpoints.set(checkpoint, snapshot);
		return checkpoint;
	}

	ownSnapshot(view: SnapshotData): SnapshotLease {
		if (this.resources.has(view.snapshotId)) {
			throw new Error(`ThinkThread snapshot ${view.snapshotId} is already owned by this execution world`);
		}
		const resource = { snapshotId: view.snapshotId, computation: view.computation, references: 0 };
		this.resources.set(view.snapshotId, resource);
		return this.retain(resource);
	}

	async invalidate(): Promise<void> {
		const turn = this.active;
		if (!turn || turn.closed) return;
		const base = turn.base;
		turn.base = undefined;
		turn.generation++;
		if (base) await (await base).release();
	}

	async finishTurn(turnID: string): Promise<void> {
		if (this.active?.scope.turnID !== turnID) return;
		await this.finishActive();
	}

	async dispose(): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;
		await this.finishActive();
		for (let round = 0; round < 3 && this.resources.size > 0; round++) {
			await Promise.allSettled([...this.resources.values()].map((resource) => this.cleanup(resource)));
		}
		await this.durable.drainCleanup();
	}

	private async createBase(): Promise<SnapshotLease> {
		const capture = await TimelineInterval.measure(() => this.durable.snapshotCreate());
		return this.ownSnapshot({ ...capture.output, computation: TimelineInterval.own(capture.computation) });
	}

	private async finishActive(): Promise<void> {
		const turn = this.active;
		this.active = undefined;
		if (!turn || turn.closed) return;
		turn.closed = true;
		if (turn.base) await (await turn.base).release();
	}

	private retain(resource: SnapshotResource): SnapshotLease {
		if (resource.removal) throw new Error(`ThinkThread snapshot ${resource.snapshotId} is already being removed`);
		resource.references++;
		let released = false;
		return {
			resource, id: resource.snapshotId, computation: resource.computation,
			retain: () => this.retain(resource),
			release: async () => {
				if (released) return;
				released = true;
				if (--resource.references === 0) await this.cleanup(resource);
			},
		};
	}

	private async cleanup(resource: SnapshotResource): Promise<void> {
		if (resource.references !== 0) return;
		if (!resource.removal) {
			const removal = this.durable.snapshotRemove(resource.snapshotId).then(() => { this.resources.delete(resource.snapshotId); });
			resource.removal = removal;
			void removal.catch(() => { if (resource.removal === removal) resource.removal = undefined; });
		}
		await resource.removal;
	}
}
