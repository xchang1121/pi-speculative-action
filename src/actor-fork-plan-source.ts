import type { AgentPlanSource } from "./agent-runtime-types.ts";

export interface ActorForkActionCall {
	readonly id: string;
	readonly index: number;
	readonly callID?: string;
	readonly format?: string;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
}

export interface ActorForkActionEvidence {
	readonly candidateIDs: readonly string[];
	readonly sources: readonly string[];
	readonly provenance: readonly unknown[];
	readonly actionIdentities: readonly unknown[];
	readonly draftTokenCount: number;
	readonly confidence?: number;
	readonly score?: Readonly<Record<string, unknown>>;
	readonly fork?: Readonly<Record<string, unknown>>;
}

export interface ActorForkActionBatch {
	readonly id: string;
	readonly calls: readonly ActorForkActionCall[];
	readonly evidence: readonly ActorForkActionEvidence[];
}

interface PendingFork {
	readonly promise: Promise<readonly ActorForkActionBatch[]>;
	readonly resolve: (batches: readonly ActorForkActionBatch[]) => void;
	readonly controller: AbortController;
	settled: boolean;
}

/** One turn-scoped Actor probe source, including result delivery and cancellation. */
export class ActorForkPlanSource {
	private readonly pending = new Map<string, PendingFork>();
	readonly source: AgentPlanSource = {
		id: "self-speculation",
		enabled: (settings) => settings.sourceConfig?.actorForkActionEnabled === true,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "actor_decision",
		propose: async ({ startInput, candidateNames, signal }) => {
			const batches = await this.waitForBatches(startInput.turnID, signal);
			const allowed = new Set(candidateNames);
			return batches
				.filter((batch) => batch.calls.length > 0 && batch.calls.every((call) => allowed.has(call.tool)))
				.map((batch) => ({
					id: `self-speculation:${startInput.turnID}:${batch.id}`,
					source: "self-speculation",
					revision: 0,
					actions: batch.calls.map((call) => ({
						id: call.id,
						type: "tool_call" as const,
						tool: call.tool,
						input: call.input,
						feedback: { batchID: batch.id, callID: call.id, callIndex: call.index, evidence: batch.evidence },
					})),
				}));
		},
	};

	startTurn(turnID: string): void {
		this.closeTurn(turnID);
		let resolve!: (batches: readonly ActorForkActionBatch[]) => void;
		const promise = new Promise<readonly ActorForkActionBatch[]>((settle) => {
			resolve = settle;
		});
		this.pending.set(turnID, { promise, resolve, controller: new AbortController(), settled: false });
	}

	probeSignal(turnID: string): AbortSignal | undefined {
		return this.pending.get(turnID)?.controller.signal;
	}

	publish(turnID: string, batches: readonly ActorForkActionBatch[]): void {
		const pending = this.pending.get(turnID);
		if (!pending || pending.settled) return;
		pending.settled = true;
		pending.resolve(structuredClone(batches));
	}

	closeTurn(turnID: string): void {
		const pending = this.pending.get(turnID);
		if (!pending) return;
		pending.controller.abort();
		if (!pending.settled) pending.resolve([]);
		this.pending.delete(turnID);
	}

	reset(): void {
		for (const turnID of [...this.pending.keys()]) this.closeTurn(turnID);
	}

	waitForBatches(turnID: string, signal: AbortSignal): Promise<readonly ActorForkActionBatch[]> {
		const pending = this.pending.get(turnID);
		if (!pending || signal.aborted) return Promise.resolve([]);
		return new Promise((resolve) => {
			const aborted = () => {
				pending.controller.abort();
				resolve([]);
			};
			signal.addEventListener("abort", aborted, { once: true });
			void pending.promise.then((batches) => {
				signal.removeEventListener("abort", aborted);
				resolve(batches);
			});
		});
	}
}

export function createActorForkPlanSource(): ActorForkPlanSource {
	return new ActorForkPlanSource();
}
