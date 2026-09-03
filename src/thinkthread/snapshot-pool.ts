import type { FsSnapshotId } from "@thinkthread/agent-posix";
import type { WorldCheckpoint } from "../execution-world.ts";
import type { DurableFsExecutor } from "./durable-fs.ts";

interface ActiveTurn {
	readonly id: string;
	generation: number;
	base?: Promise<SnapshotLease>;
	closed: boolean;
}

class SnapshotResource {
	readonly id: FsSnapshotId;
	readonly logicalBytes: number;
	private readonly remove: (snapshotID: FsSnapshotId) => Promise<void>;
	private references = 0;
	private removal?: Promise<void>;

	constructor(
		view: { readonly snapshotId: FsSnapshotId; readonly logicalBytes: number },
		remove: (snapshotID: FsSnapshotId) => Promise<void>,
	) {
		this.id = view.snapshotId;
		this.logicalBytes = view.logicalBytes;
		this.remove = remove;
	}

	retain(): SnapshotLease {
		if (this.removal) throw new Error(`ThinkThread snapshot ${this.id} is already being removed`);
		this.references++;
		return new SnapshotLease(this);
	}

	async release(): Promise<void> {
		if (this.references <= 0) return;
		this.references--;
		if (this.references !== 0) return;
		await this.cleanup();
	}

	async cleanup(): Promise<void> {
		if (this.references !== 0) return;
		if (!this.removal) {
			const removal = this.remove(this.id);
			this.removal = removal;
			removal.catch(() => {
				if (this.removal === removal) this.removal = undefined;
			});
		}
		await this.removal;
	}
}

export class SnapshotLease {
	readonly resource: SnapshotResource;
	private released = false;

	constructor(resource: SnapshotResource) {
		this.resource = resource;
	}

	get id(): FsSnapshotId {
		return this.resource.id;
	}

	get logicalBytes(): number {
		return this.resource.logicalBytes;
	}

	retain(): SnapshotLease {
		return this.resource.retain();
	}

	async release(): Promise<void> {
		if (this.released) return;
		this.released = true;
		await this.resource.release();
	}
}

export class ThinkThreadCheckpoint implements WorldCheckpoint {
	readonly backend = "ThinkThread";
	readonly id: string;
	readonly lineage: string;
	readonly depth: number;
	private readonly snapshot: SnapshotLease;
	private readonly owner: object;

	constructor(snapshot: SnapshotLease, lineage: string, depth: number, owner: object) {
		this.snapshot = snapshot;
		this.id = snapshot.id;
		this.lineage = lineage;
		this.depth = depth;
		this.owner = owner;
	}

	retainSnapshot(): SnapshotLease {
		return this.snapshot.retain();
	}

	belongsTo(owner: object): boolean {
		return this.owner === owner;
	}
}

export class ThinkThreadSnapshotPool {
	private readonly durable: DurableFsExecutor;
	private readonly resources = new Map<FsSnapshotId, SnapshotResource>();
	private readonly identity = {};
	private active?: ActiveTurn;
	private disposed = false;

	constructor(durable: DurableFsExecutor) {
		this.durable = durable;
	}

	async beginTurn(turnID: string): Promise<void> {
		if (this.disposed) throw new Error("ThinkThread snapshot pool is disposed");
		if (this.active?.id === turnID && !this.active.closed) return;
		await this.finishActive();
		this.active = { id: turnID, generation: 0, closed: false };
	}

	async acquireRoot(): Promise<{ readonly lease: SnapshotLease; readonly lineage: string; readonly depth: 0 }> {
		const turn = this.active;
		if (!turn || turn.closed) throw new Error("ThinkThread execution world has no active turn");
		if (!turn.base) {
			const attempt = this.createBase();
			turn.base = attempt;
			attempt.catch(() => {
				if (turn.base === attempt) turn.base = undefined;
			});
		}
		const owner = await turn.base;
		return {
			lease: owner.retain(),
			lineage: `${turn.id}:${turn.generation}:${owner.id}`,
			depth: 0,
		};
	}

	acquireCheckpoint(checkpoint: WorldCheckpoint): {
		readonly lease: SnapshotLease;
		readonly lineage: string;
		readonly depth: number;
	} {
		if (
			!(checkpoint instanceof ThinkThreadCheckpoint) ||
			checkpoint.backend !== "ThinkThread" ||
			!checkpoint.belongsTo(this.identity) ||
			!this.resources.has(checkpoint.id as FsSnapshotId)
		) {
			throw new Error("Execution world checkpoint belongs to another backend");
		}
		return {
			lease: checkpoint.retainSnapshot(),
			lineage: checkpoint.lineage,
			depth: checkpoint.depth,
		};
	}

	checkpoint(snapshot: SnapshotLease, lineage: string, depth: number): ThinkThreadCheckpoint {
		if (this.resources.get(snapshot.id) !== snapshot.resource) {
			throw new Error(`ThinkThread snapshot ${snapshot.id} is not owned by this execution world`);
		}
		return new ThinkThreadCheckpoint(snapshot, lineage, depth, this.identity);
	}

	ownSnapshot(view: { readonly snapshotId: FsSnapshotId; readonly logicalBytes: number }): SnapshotLease {
		if (this.resources.has(view.snapshotId)) {
			throw new Error(`ThinkThread snapshot ${view.snapshotId} is already owned by this execution world`);
		}
		const resource = new SnapshotResource(view, (snapshotID) => this.remove(snapshotID));
		this.resources.set(view.snapshotId, resource);
		return resource.retain();
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
		if (this.active?.id !== turnID) return;
		await this.finishActive();
	}

	async dispose(): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;
		await this.finishActive();
		for (let round = 0; round < 3 && this.resources.size > 0; round++) {
			await Promise.allSettled([...this.resources.values()].map((resource) => resource.cleanup()));
		}
		await this.durable.drainCleanup();
	}

	resourceCount(): number {
		return this.resources.size;
	}

	private async createBase(): Promise<SnapshotLease> {
		return this.ownSnapshot(await this.durable.snapshotCreate());
	}

	private async finishActive(): Promise<void> {
		const turn = this.active;
		this.active = undefined;
		if (!turn || turn.closed) return;
		turn.closed = true;
		if (turn.base) await (await turn.base).release();
	}

	private async remove(snapshotID: FsSnapshotId): Promise<void> {
		await this.durable.snapshotRemove(snapshotID);
		this.resources.delete(snapshotID);
	}
}
