import type { AgentPlanSource } from "./agent-runtime-types.ts";
import type { AssistantMessageEvent } from "@earendil-works/pi-ai";

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

export interface ActorProbeSnapshot {
	readonly attempt: number;
	readonly generatedText: string;
	readonly content: string;
	readonly reasoning: string;
	readonly outputChunks: number;
}

export interface ActorProbeSchedule {
	readonly maxAttempts: number;
	/** Non-empty Actor stream updates required between sidecar retries. */
	readonly retryStreamUpdates: number;
}

export const ACTOR_PROBE_SCHEDULE: ActorProbeSchedule = Object.freeze({
	maxAttempts: 5,
	retryStreamUpdates: 50,
});

interface PendingFork {
	readonly promise: Promise<readonly ActorForkActionBatch[]>;
	readonly resolve: (batches: readonly ActorForkActionBatch[]) => void;
	readonly controller: AbortController;
	generatedText: string;
	content: string;
	reasoning: string;
	outputChunks: number;
	requestBound: boolean;
	attempts: number;
	lastProbeOutputChunks: number;
	probeInFlight: boolean;
	settled: boolean;
}

/** One turn-scoped Actor probe source, including result delivery and cancellation. */
export class ActorForkPlanSource {
	private readonly pending = new Map<string, PendingFork>();
	readonly schedule: ActorProbeSchedule;
	constructor(schedule: Partial<ActorProbeSchedule> = {}) {
		const normalized = { ...ACTOR_PROBE_SCHEDULE, ...schedule };
		for (const [field, value] of Object.entries(normalized)) {
			if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${field} must be a positive integer`);
		}
		this.schedule = Object.freeze(normalized);
	}
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
		this.pending.set(turnID, {
			promise,
			resolve,
			controller: new AbortController(),
			generatedText: "",
			content: "",
			reasoning: "",
			outputChunks: 0,
			requestBound: false,
			attempts: 0,
			lastProbeOutputChunks: 0,
			probeInFlight: false,
			settled: false,
		});
	}

	bindActorRequest(turnID: string): void {
		const pending = this.pending.get(turnID);
		if (pending) pending.requestBound = true;
	}

	observeActorDelta(turnID: string, event: AssistantMessageEvent): ActorProbeSnapshot | undefined {
		const pending = this.pending.get(turnID);
		if (!pending || pending.settled || !pending.requestBound) return undefined;
		if ((event.type !== "text_delta" && event.type !== "thinking_delta") || !event.delta) return undefined;
		if (event.type === "text_delta") pending.content += event.delta;
		else pending.reasoning += event.delta;
		pending.generatedText += event.delta;
		pending.outputChunks++;
		return this.claimProbe(pending);
	}

	finishProbe(turnID: string): boolean {
		const pending = this.pending.get(turnID);
		if (!pending || pending.settled) return true;
		pending.probeInFlight = false;
		return pending.attempts >= this.schedule.maxAttempts;
	}

	claimPendingProbe(turnID: string): ActorProbeSnapshot | undefined {
		const pending = this.pending.get(turnID);
		return pending ? this.claimProbe(pending) : undefined;
	}

	private claimProbe(pending: PendingFork): ActorProbeSnapshot | undefined {
		if (
			pending.settled ||
			pending.probeInFlight ||
			!pending.requestBound ||
			pending.attempts >= this.schedule.maxAttempts ||
			pending.outputChunks <
				pending.lastProbeOutputChunks + (pending.attempts === 0 ? 1 : this.schedule.retryStreamUpdates)
		)
			return undefined;
		pending.probeInFlight = true;
		pending.lastProbeOutputChunks = pending.outputChunks;
		pending.attempts++;
		return {
			attempt: pending.attempts,
			generatedText: pending.generatedText,
			content: pending.content,
			reasoning: pending.reasoning,
			outputChunks: pending.outputChunks,
		};
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

export function createActorForkPlanSource(schedule: Partial<ActorProbeSchedule> = {}): ActorForkPlanSource {
	return new ActorForkPlanSource(schedule);
}
