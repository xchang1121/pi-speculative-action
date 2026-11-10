import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { waitForCompletion } from "./runtime-lifecycle.ts";

export interface ModelRequestContext {
	readonly taskID: string;
	readonly turnID?: string;
	readonly source?: string;
	readonly sourceRequestID?: string;
	readonly proposalID?: string;
	readonly parentID?: string;
	readonly purpose?: "actor" | "root" | "scope" | "continuation" | "observation" | "actor-reasoning-probe";
}
export const modelRequestContext = new AsyncLocalStorage<ModelRequestContext>();
/** Response identity is private metadata, never inserted into model-visible messages. */
export const modelResponseRequestIDs = new WeakMap<object, string>();
export type ModelRequestPhase = "queued" | "admitted" | "sent" | "cancel-requested" | "not-sent" | "remote-finished" | "usage-settled" | "remote-unknown";
export interface ModelRequestEvent extends ModelRequestContext {
	readonly id: string;
	readonly domain: string;
	readonly role: "actor" | "drafter";
	readonly phase: ModelRequestPhase;
	readonly timestamp: number;
	readonly usage?: unknown;
	readonly providerRequestID?: string;
}
export interface ModelRequestPolicy { readonly maxConcurrent: number; readonly actorReserve: number; }
export class ModelRequestNotSent extends Error {
	constructor() { super("Model request cancelled before dispatch"); }
}
interface Entry {
	readonly event: Omit<ModelRequestEvent, "phase" | "timestamp">;
	readonly policy: ModelRequestPolicy;
	readonly controller: AbortController;
	readonly notify: (event: ModelRequestEvent) => void;
	readonly signal?: AbortSignal;
	readonly admit: () => void;
	readonly reject: (error: unknown) => void;
	phase: ModelRequestPhase;
	removeAbort?: () => void;
	completion?: Promise<unknown>;
}

/** Capacity belongs to a transport, not its consumer. Models sharing an origin share a limit domain. */
export class ModelRequestScheduler {
	private readonly queued: Entry[] = [];
	private readonly active = new Set<Entry>();
	private emit(entry: Entry, phase: ModelRequestPhase, detail: Partial<ModelRequestEvent> = {}) {
		entry.phase = phase;
		try { entry.notify({ ...entry.event, ...detail, phase, timestamp: performance.timeOrigin + performance.now() }); } catch { /* Observers have no scheduling authority. */ }
	}
	private pump() {
		// Actors have priority over queued speculative requests; reserve is enforced even before an Actor queues.
		for (const entry of [...this.queued].sort((a, b) => Number(a.event.role !== "actor") - Number(b.event.role !== "actor"))) {
			const peers = [...this.active].filter(peer => peer.event.domain === entry.event.domain);
			const policies = [...peers, ...this.queued.filter(peer => peer.event.domain === entry.event.domain)].map(peer => peer.policy);
			const maximum = Math.min(...policies.map(policy => Math.max(1, Math.floor(policy.maxConcurrent))));
			const reserve = Math.min(maximum, Math.max(...policies.map(policy => Math.max(0, Math.floor(policy.actorReserve)))));
			if (peers.length >= maximum || entry.event.role === "drafter" && peers.filter(peer => peer.event.role === "drafter").length >= maximum - reserve) continue;
			this.queued.splice(this.queued.indexOf(entry), 1); this.active.add(entry);
			this.emit(entry, "admitted"); entry.admit();
		}
	}
	async run<Value>(input: {
		readonly domain: string; readonly role: "actor" | "drafter"; readonly context: ModelRequestContext;
		readonly policy: ModelRequestPolicy; readonly signal?: AbortSignal;
		readonly onEvent?: (event: ModelRequestEvent) => void;
		/** The supplied signal is transport-owned; local cancellation after dispatch only detaches consumption. */
		readonly execute: (request: { readonly id: string; readonly signal: AbortSignal }) => Promise<Value>;
		/** False means the transport ended without proof the upstream request finished. Its capacity stays quarantined. */
		readonly settled?: (value: Value) => { readonly remoteFinished: boolean; readonly usage?: unknown; readonly providerRequestID?: string };
	}): Promise<Value> {
		if (!Number.isSafeInteger(input.policy.maxConcurrent) || input.policy.maxConcurrent < 1 ||
			!Number.isSafeInteger(input.policy.actorReserve) || input.policy.actorReserve < 0 || input.policy.actorReserve > input.policy.maxConcurrent)
			throw new Error("Invalid model request capacity policy");
		let admit!: () => void, reject!: (error: unknown) => void;
		const admission = new Promise<void>((resolve, fail) => { admit = resolve; reject = fail; });
		const entry: Entry = { event: { ...input.context, id: randomUUID(), domain: input.domain, role: input.role },
			policy: input.policy, controller: new AbortController(), notify: input.onEvent ?? (() => {}), signal: input.signal, admit, reject, phase: "queued" };
		const cancel = () => {
			if (entry.phase === "queued" || entry.phase === "admitted") {
				const index = this.queued.indexOf(entry); if (index >= 0) this.queued.splice(index, 1);
				this.active.delete(entry); this.emit(entry, "not-sent"); entry.reject(new ModelRequestNotSent()); this.pump();
			} else if (entry.phase === "sent") this.emit(entry, "cancel-requested");
		};
		this.emit(entry, "queued"); this.queued.push(entry);
		input.signal?.addEventListener("abort", cancel, { once: true });
		entry.removeAbort = () => input.signal?.removeEventListener("abort", cancel);
		if (input.signal?.aborted) cancel(); else this.pump();
		try {
			await admission;
			if (input.signal?.aborted || entry.phase === "not-sent") throw new ModelRequestNotSent();
			this.emit(entry, "sent");
			const completion = input.execute({ id: entry.event.id, signal: entry.controller.signal });
			entry.completion = completion;
			const value = await completion;
			const result = input.settled?.(value) ?? { remoteFinished: true };
			if (result.remoteFinished) {
				this.emit(entry, "remote-finished", { providerRequestID: result.providerRequestID });
				this.emit(entry, "usage-settled", { usage: result.usage, providerRequestID: result.providerRequestID });
				this.active.delete(entry);
			} else this.emit(entry, "remote-unknown", { usage: result.usage });
			return value;
		} catch (error) {
			if (entry.phase === "sent" || entry.phase === "cancel-requested") this.emit(entry, "remote-unknown");
			throw error;
		} finally { entry.removeAbort(); this.pump(); }
	}
	snapshot(taskID?: string): readonly ModelRequestEvent[] {
		return [...this.queued, ...this.active].filter(entry => !taskID || entry.event.taskID === taskID)
			.map(entry => ({ ...entry.event, phase: entry.phase, timestamp: performance.timeOrigin + performance.now() }));
	}
	async drain(taskID: string, timeoutMs?: number): Promise<readonly ModelRequestEvent[]> {
		await waitForCompletion(Promise.allSettled([...this.active].filter(entry => entry.event.taskID === taskID).flatMap(entry => entry.completion ? [entry.completion] : [])), undefined, timeoutMs);
		return this.snapshot(taskID);
	}
	/** A transport/usage ledger can release quarantined capacity after independently observing remote completion. */
	settleRemote(id: string, detail: { readonly usage?: unknown; readonly providerRequestID?: string } = {}): boolean {
		const entry = [...this.active].find(entry => entry.event.id === id && entry.phase === "remote-unknown");
		if (!entry) return false;
		this.emit(entry, "remote-finished", detail); this.emit(entry, "usage-settled", detail);
		this.active.delete(entry); this.pump(); return true;
	}
	/** Session teardown stops owned sockets but cannot claim remote cancellation or recycle unknown capacity. */
	retire(taskID: string): void {
		for (const entry of [...this.queued, ...this.active].filter(entry => entry.event.taskID === taskID)) {
			if (entry.phase === "queued") { this.queued.splice(this.queued.indexOf(entry), 1); this.emit(entry, "not-sent"); entry.reject(new ModelRequestNotSent()); }
			else { this.emit(entry, "remote-unknown"); entry.controller.abort(); }
		}
		this.pump();
	}
}

export const sharedModelRequestScheduler = new ModelRequestScheduler();
export function modelRequestDomain(baseUrl: string): string {
	try { return new URL(baseUrl).origin; } catch { return baseUrl; }
}
