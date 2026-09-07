import assert from "node:assert/strict";
import { fork } from "node:child_process";
import path from "node:path";
import { serialize } from "node:v8";
import { CLOSED_SEARCH_PROFILE } from "./closed-search-kernel.mjs";

/** Owns preparation, one admitted invocation, and hard retirement of a trusted search worker. */
export function launchClosedSearchWorker(moduleFile, entry = new URL("./closed-search-kernel.mjs", import.meta.url)) {
	const { limits } = CLOSED_SEARCH_PROFILE, started = performance.now();
	const child = fork(entry, [path.resolve(moduleFile)], {
		execArgv: ["--wasm-max-mem-pages=1024", "--max-old-space-size=128"], serialization: "advanced", silent: true, windowsHide: true,
		env: { ...CLOSED_SEARCH_PROFILE.environment, ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}) },
	});
	const ready = Promise.withResolvers(), closure = Promise.withResolvers();
	let pending, failure, closed = false, prepared = false, nextID = 0, diagnosticBytes = 0, diagnostic = "";
	const stop = (reason) => { failure ??= { reason }; if (!closed) child.kill("SIGKILL"); };
	const send = (message) => { try { child.send(message, (error) => { if (error) stop(error); }); } catch (error) { stop(error); } };
	const startup = setTimeout(() => stop(new Error("worker preparation deadline")), 15_000);
	void ready.promise.catch(() => {}); // An idle/preparing worker still owns its failure before a caller awaits it.
	child.once("error", stop);
	child.once("close", () => {
		closed = true; clearTimeout(startup);
		const error = failure ? failure.reason : new Error(`worker closed before completion: ${diagnostic}`);
		pending?.settle({ error }); ready.reject(error); closure.resolve();
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
				assert.ok(!admitted.inputPending && Number.isSafeInteger(message.sequence) && message.sequence > admitted.sequence, "unowned input request");
				admitted.inputPending = true; admitted.sequence = message.sequence;
				Promise.resolve().then(() => admitted.onInput(message.operation, message.target)).then((value) => ({ value }),
					(error) => ({ error: String(error?.message ?? error).slice(0, 8192), code: error?.code })).then((response) => {
					if (pending !== admitted || failure || closed) return;
					if ((admitted.inputBytes += serialize(response).byteLength) > limits.inputBytes) response = { error: "input byte budget" };
					admitted.inputPending = false;
					send({ type: "input", id: admitted.id, sequence: message.sequence, ...response });
				}).catch(stop);
			} else {
				assert.ok(message.type === "result" && !pending.inputPending, "unexpected worker response");
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
			const admitted = pending = { id, onStarted, onInput, inputBytes: 0, inputPending: false, sequence: 0, settle: (settlement) => {
				clearTimeout(timer); signal?.removeEventListener("abort", abort); pending = undefined;
				if (Object.hasOwn(settlement, "error")) reject(settlement.error); else resolve(settlement.result);
			} };
			signal?.addEventListener("abort", abort, { once: true });
			void ready.promise.then(() => { if (!failure && pending === admitted) send({ type: "request", id, input }); }, () => {});
		}),
	};
}
