export interface SelfSpeculationActionCandidate {
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
}

interface CandidateBatch {
	readonly promise: Promise<readonly SelfSpeculationActionCandidate[]>;
	readonly resolve: (candidates: readonly SelfSpeculationActionCandidate[]) => void;
	settled: boolean;
}

/** Turn-scoped handoff from a completed sidecar fork to ordinary runtime source slots. */
export class SelfSpeculationActionBridge {
	private readonly batches = new Map<string, CandidateBatch>();

	startTurn(turnID: string): void {
		this.closeTurn(turnID);
		this.batches.set(turnID, candidateBatch());
	}

	waitForCandidates(turnID: string, signal: AbortSignal): Promise<readonly SelfSpeculationActionCandidate[]> {
		if (signal.aborted) return Promise.resolve([]);
		const batch = this.batches.get(turnID);
		if (!batch) return Promise.resolve([]);
		return new Promise((resolve) => {
			let settled = false;
			const finish = (candidates: readonly SelfSpeculationActionCandidate[] = []) => {
				if (settled) return;
				settled = true;
				signal.removeEventListener("abort", aborted);
				resolve(candidates);
			};
			const aborted = () => finish();
			signal.addEventListener("abort", aborted, { once: true });
			void batch.promise.then(finish);
		});
	}

	publish(turnID: string, candidates: readonly SelfSpeculationActionCandidate[]): void {
		const batch = this.batches.get(turnID);
		if (!batch || batch.settled) return;
		batch.settled = true;
		batch.resolve(
			Object.freeze(
				candidates.map((candidate) =>
					Object.freeze({ tool: candidate.tool, input: structuredClone(candidate.input) }),
				),
		),
		);
	}

	closeTurn(turnID: string): void {
		const batch = this.batches.get(turnID);
		if (!batch) return;
		if (!batch.settled) {
			batch.settled = true;
			batch.resolve([]);
		}
		this.batches.delete(turnID);
	}

	reset(): void {
		for (const turnID of [...this.batches.keys()]) this.closeTurn(turnID);
	}
}

function candidateBatch(): CandidateBatch {
	let resolve!: (candidates: readonly SelfSpeculationActionCandidate[]) => void;
	const promise = new Promise<readonly SelfSpeculationActionCandidate[]>((settle) => {
		resolve = settle;
	});
	return { promise, resolve, settled: false };
}
