import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { serialize } from "node:v8";
import { CLOSED_SEARCH_PROFILE } from "./closed-search-kernel.mjs";

/** Reuse execution capacity, never results. Busy producers and Actors own independent reservations. */
export class ClosedSearchProcessPool {
	#workers = new Map(); #idle = new Map(); #retirement;
	async request(role, input, options = {}) {
		assert.ok(!this.#retirement && (role === "actor" || role === "producer"), "search pool retired or invalid role");
		options.signal?.throwIfAborted();
		const worker = this.#idle.get(role) ?? launchClosedSearchWorker();
		this.#idle.delete(role);
		if (!this.#workers.has(worker)) void worker.closure.then(() => {
			this.#workers.delete(worker); if (this.#idle.get(role) === worker) this.#idle.delete(role);
		});
		const execution = worker.request(input, options), lease = { role, execution };
		this.#workers.set(worker, lease);
		try { return await execution; }
		finally {
			lease.execution = undefined;
			if (this.#retirement || worker.closed() || this.#idle.has(role)) await worker.dispose();
			else this.#idle.set(role, worker);
		}
	}
	dispose() {
		if (this.#retirement) return this.#retirement;
		this.#idle.clear();
		return this.#retirement = Promise.all([...this.#workers].map(async ([worker, lease]) => {
			if (lease.role === "actor") await lease.execution?.catch(() => {});
			await worker.dispose();
		})).then(() => {});
	}
}

/** Owns the worker AND its borrowed input operations; callbacks must settle after their signal's cleanup. */
export function launchClosedSearchWorker(entry = new URL("./closed-search-kernel.mjs", import.meta.url)) {
	const { limits } = CLOSED_SEARCH_PROFILE, started = performance.now();
	const child = fork(entry, [], {
		execArgv: ["--max-old-space-size=128"], serialization: "advanced", silent: true, windowsHide: true,
		env: { ...CLOSED_SEARCH_PROFILE.environment, ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}) },
	});
	const ready = Promise.withResolvers(), closure = Promise.withResolvers();
	let pending, failure, closed = false, prepared = false, nextID = 0, diagnosticBytes = 0, diagnostic = "";
	const stop = (reason) => { failure ??= { reason }; pending?.input?.controller.abort(reason); if (!closed) child.kill("SIGKILL"); };
	const send = (message) => { try { child.send(message, (error) => { if (error) stop(error); }); } catch (error) { stop(error); } };
	const startup = setTimeout(() => stop(new Error("worker preparation deadline")), 15_000);
	void ready.promise.catch(() => {}); // An idle/preparing worker still owns its failure before a caller awaits it.
	child.once("error", stop);
	child.once("close", async () => {
		closed = true; clearTimeout(startup);
		const error = failure ? failure.reason : new Error(`worker closed before completion: ${diagnostic}`);
		const admitted = pending;
		admitted?.input?.controller.abort(error);
		await admitted?.input?.completion;
		admitted?.settle({ error }); ready.reject(error); closure.resolve();
	});
	for (const stream of [child.stdout, child.stderr]) stream.on("data", (bytes) => {
		diagnostic = (diagnostic + bytes.toString()).slice(-4096);
		if ((diagnosticBytes += bytes.length) > limits.resultBytes) stop(new Error("worker diagnostic budget"));
	});
	child.on("message", (message) => {
		if (failure || closed) return;
		try {
			assert.ok(serialize(message).byteLength <= limits.requestBytes, "worker frame budget");
			if (message.type === "ready") {
				assert.ok(!prepared, "duplicate worker readiness"); assert.deepEqual(message.profile, CLOSED_SEARCH_PROFILE);
				prepared = true; clearTimeout(startup); ready.resolve({ ...message, preparationMs: performance.now() - started }); return;
			}
			assert.ok(pending && message.id === pending.id, "unowned worker response");
			if (message.type === "started") pending.onStarted?.();
			else if (message.type === "input") {
				const admitted = pending;
				assert.ok(!admitted.input && Number.isSafeInteger(message.sequence) && message.sequence > admitted.sequence, "unowned input request");
				admitted.sequence = message.sequence;
				const controller = new AbortController();
				const completion = Promise.resolve().then(() => { controller.signal.throwIfAborted(); return admitted.onInput(message.operation, message.target, controller.signal); }).then((value) => ({ value }),
					(error) => ({ error: String(error?.message ?? error).slice(0, 8192), code: error?.code })).then((response) => {
					if (pending !== admitted || failure || closed) return;
					if ((admitted.inputBytes += serialize(response).byteLength) > limits.inputBytes) response = { error: "input byte budget" };
					admitted.input = undefined;
					send({ type: "input", id: admitted.id, sequence: message.sequence, ...response });
				}).catch(stop);
				admitted.input = { controller, completion };
			} else {
				assert.ok(message.type === "result" && !pending.input, "unexpected worker response");
				assert.ok(serialize(message.result).byteLength <= limits.resultBytes, "result frame budget");
				pending.settle(Object.hasOwn(message, "error") ? { error: new Error(message.error) } : message);
			}
		} catch (error) { stop(error); }
	});
	return {
		ready: ready.promise, closure: closure.promise, closed: () => closed,
		dispose: async () => { stop(new Error("worker disposed")); await closure.promise; },
		request: (input, { signal, timeoutMs = 5000, onStarted, onInput } = {}) => new Promise((resolve, reject) => {
			if (signal?.aborted) { reject(signal.reason); return; }
			if (failure || closed || pending) { reject(new Error("worker unavailable/busy")); return; }
			assert.ok(Number.isFinite(timeoutMs) && timeoutMs > 0, "invalid worker deadline");
			assert.ok(serialize(input).byteLength <= limits.requestBytes, "request frame budget");
			const id = ++nextID, abort = () => stop(signal.reason);
			const timer = setTimeout(() => stop(new Error("worker execution deadline")), timeoutMs);
			const admitted = pending = { id, onStarted, onInput, inputBytes: 0, input: undefined, sequence: 0, settle: (settlement) => {
				clearTimeout(timer); signal?.removeEventListener("abort", abort); pending = undefined;
				if (Object.hasOwn(settlement, "error")) reject(settlement.error); else resolve(settlement.result);
			} };
			signal?.addEventListener("abort", abort, { once: true });
			void ready.promise.then(() => { if (!failure && pending === admitted) send({ type: "request", id, input }); }, () => {});
		}),
	};
}
