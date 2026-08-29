export interface SelfSpeculationActionCandidate {
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
}

/** One call inside an atomic sidecar-generated Actor tool batch. */
export interface SelfSpeculationActionCall extends SelfSpeculationActionCandidate {
	readonly id: string;
	readonly index: number;
	readonly callID?: string;
	readonly format?: string;
}

/** Unmodified control-plane evidence for one candidate observation. */
export interface SelfSpeculationActionEvidence {
	readonly candidateIDs: readonly string[];
	readonly sources: readonly string[];
	readonly provenance: readonly unknown[];
	readonly actionIdentities: readonly unknown[];
	readonly draftTokenCount: number;
	readonly confidence?: number;
	readonly score?: Readonly<Record<string, unknown>>;
	readonly fork?: Readonly<Record<string, unknown>>;
}

/** Calls decoded from one complete fork candidate; calls are alternatives only across batches. */
export interface SelfSpeculationActionBatch {
	readonly id: string;
	readonly calls: readonly SelfSpeculationActionCall[];
	readonly evidence: readonly SelfSpeculationActionEvidence[];
}

interface PendingActionBatches {
	readonly promise: Promise<readonly SelfSpeculationActionBatch[]>;
	readonly resolve: (batches: readonly SelfSpeculationActionBatch[]) => void;
	settled: boolean;
}

/** Turn-scoped handoff from a completed sidecar fork to ordinary runtime source slots. */
export class SelfSpeculationActionBridge {
	private readonly pending = new Map<string, PendingActionBatches>();

	startTurn(turnID: string): void {
		this.closeTurn(turnID);
		this.pending.set(turnID, pendingActionBatches());
	}

	waitForBatches(turnID: string, signal: AbortSignal): Promise<readonly SelfSpeculationActionBatch[]> {
		if (signal.aborted) return Promise.resolve([]);
		const pending = this.pending.get(turnID);
		if (!pending) return Promise.resolve([]);
		return new Promise((resolve) => {
			let settled = false;
			const finish = (batches: readonly SelfSpeculationActionBatch[] = []) => {
				if (settled) return;
				settled = true;
				signal.removeEventListener("abort", aborted);
				resolve(cloneBatches(batches));
			};
			const aborted = () => finish();
			signal.addEventListener("abort", aborted, { once: true });
			void pending.promise.then(finish);
		});
	}

	/** Compatibility projection for consumers that intentionally flatten batches. */
	async waitForCandidates(turnID: string, signal: AbortSignal): Promise<readonly SelfSpeculationActionCandidate[]> {
		const batches = await this.waitForBatches(turnID, signal);
		return Object.freeze(
			batches.flatMap((batch) =>
				batch.calls.map((call) =>
					Object.freeze({ tool: call.tool, input: structuredClone(call.input) }),
				),
			),
		);
	}

	publishBatches(turnID: string, batches: readonly SelfSpeculationActionBatch[]): void {
		const pending = this.pending.get(turnID);
		if (!pending || pending.settled) return;
		pending.settled = true;
		pending.resolve(cloneBatches(batches));
	}

	/** Compatibility adapter for legacy producers that do not expose batch evidence. */
	publish(turnID: string, candidates: readonly SelfSpeculationActionCandidate[]): void {
		this.publishBatches(
			turnID,
			candidates.map((candidate, index) => ({
				id: `legacy:${index}`,
				calls: [{ id: `${index}:legacy`, index, tool: candidate.tool, input: candidate.input }],
				evidence: [],
			})),
		);
	}

	closeTurn(turnID: string): void {
		const pending = this.pending.get(turnID);
		if (!pending) return;
		if (!pending.settled) {
			pending.settled = true;
			pending.resolve([]);
		}
		this.pending.delete(turnID);
	}

	reset(): void {
		for (const turnID of [...this.pending.keys()]) this.closeTurn(turnID);
	}
}

function pendingActionBatches(): PendingActionBatches {
	let resolve!: (batches: readonly SelfSpeculationActionBatch[]) => void;
	const promise = new Promise<readonly SelfSpeculationActionBatch[]>((settle) => {
		resolve = settle;
	});
	return { promise, resolve, settled: false };
}

function cloneBatches(batches: readonly SelfSpeculationActionBatch[]): readonly SelfSpeculationActionBatch[] {
	return Object.freeze(
		batches.map((batch) =>
			Object.freeze({
				id: batch.id,
				calls: Object.freeze(
					batch.calls.map((call) =>
						Object.freeze({
							id: call.id,
							index: call.index,
							...(call.callID ? { callID: call.callID } : {}),
							...(call.format ? { format: call.format } : {}),
							tool: call.tool,
							input: structuredClone(call.input),
						}),
					),
				),
				evidence: Object.freeze(
					batch.evidence.map((evidence) =>
						Object.freeze({
							candidateIDs: Object.freeze([...evidence.candidateIDs]),
							sources: Object.freeze([...evidence.sources]),
							provenance: Object.freeze(structuredClone([...evidence.provenance])),
							actionIdentities: Object.freeze(structuredClone([...evidence.actionIdentities])),
							draftTokenCount: evidence.draftTokenCount,
							...(evidence.confidence !== undefined ? { confidence: evidence.confidence } : {}),
							...(evidence.score ? { score: structuredClone(evidence.score) } : {}),
							...(evidence.fork ? { fork: structuredClone(evidence.fork) } : {}),
						}),
					),
				),
			}),
		),
	);
}
