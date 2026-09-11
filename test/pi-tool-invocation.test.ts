import { deferred } from "./async.ts";
import { describe, expect, it } from "vitest";
import { Worker } from "node:worker_threads";
import process from "node:process";
import { setImmediate as nextTurn } from "node:timers/promises";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { ToolFilesystemOperations } from "../src/tool-settlement.ts";

describe("stock Pi invocation identity", () => {
	it.each(["read", "ls", "write", "edit"] as const)("borrows only the filesystem capabilities needed by %s", async (tool) => {
		const allowed = { read: ["access", "readFile"], ls: ["exists", "stat", "readdir"],
			write: ["writeFile", "mkdir"], edit: ["access", "readFile", "writeFile"] }[tool];
		const written: string[] = [];
		const operations: ToolFilesystemOperations = {
			access: async () => {}, readFile: async () => Buffer.from("before\n"),
			exists: async () => true, stat: async () => ({ isDirectory: () => true }), readdir: async () => [],
			writeFile: async (_target, value) => { written.push(String(value)); }, mkdir: async () => {},
		};
		const view = new Proxy(operations, { get(target, key, receiver) {
			if (typeof key !== "string" || !allowed.includes(key)) throw new Error(`Capability was not granted: ${String(key)}`);
			return Reflect.get(target, key, receiver);
		} });
		const args = { path: "owned.txt", ...(tool === "write" ? { content: "after" } : {}),
			...(tool === "edit" ? { edits: [{ oldText: "before", newText: "after" }] } : {}) };
		const invocation = resolvePiToolInvocation(tool, args, { cwd: process.cwd(), environment: {} })!;
		const output = await invocation.filesystem!(view, { callID: tool, args, signal: new AbortController().signal });
		expect(output.isError).toBe(false);
		expect(output.result.content.length).toBeGreaterThan(0);
		expect(written).toEqual(tool === "write" ? ["after"] : tool === "edit" ? ["after\n"] : []);
	});

	it("binds exact execution semantics independently of call arguments and owns filesystem completion", async () => {
		const options = { cwd: process.cwd(), environment: { PATH: "tools", BENCHMARK: "true" },
			shellPath: process.execPath, shellCommandPrefix: "set -e" };
		const invocation = resolvePiToolInvocation("bash", { command: "printf ok", timeout: 2.5 }, options)!;
		expect(invocation.process).toEqual({
			command: "set -e\nprintf ok", cwd: options.cwd, environment: options.environment,
			shell: process.execPath, shellArgs: ["-c"], commandTransport: "argv", timeout: 2.5,
		});
		const { command: _command, timeout: _timeout, ...processIdentity } = invocation.process!;
		expect(invocation.identity).toEqual({ ...processIdentity, executor: "pi.bash.local.v2", commandPrefix: "set -e" });
		expect(resolvePiToolInvocation("bash", { command: "printf other", timeout: 9 }, options)?.identity).toEqual(invocation.identity);
		const read = resolvePiToolInvocation("read", { path: "a.ts" }, options)!;
		expect(read.filesystem).toBeTypeOf("function");
		expect(read.process).toBeUndefined();
		expect(resolvePiToolInvocation("read", { path: "b.ts", offset: 2 }, options)?.identity).toEqual(read.identity);
		for (const setting of [{ autoResizeImages: false }, { modelSupportsImages: false }]) {
			expect(resolvePiToolInvocation("read", { path: "a.ts" }, { ...options, ...setting })?.identity).not.toEqual(read.identity);
		}
		expect(resolvePiToolInvocation("bash", { command: 1 }, options)).toBeUndefined();
		await verifyFilesystemCompletion(read);
	});
});

async function verifyFilesystemCompletion(invocation: NonNullable<ReturnType<typeof resolvePiToolInvocation>>) {
	const listeners = process.listenerCount("worker"), workers: Worker[] = [];
	const launch = () => {
		const worker = new Worker("require('node:worker_threads').parentPort.once('message', () => {});", { eval: true, execArgv: [] });
		workers.push(worker);
		return worker;
	};
	const foreign = launch(), input = Buffer.from("owned input\n");
	const start = (mode: string) => {
		const entered = deferred<void>(), proceed = deferred<void>(), created = deferred<Worker>();
		const read = deferred<void>(), abort = new AbortController();
		let settled = false, reads = 0;
		const output = invocation.filesystem!({
			access: async () => { entered.resolve(); if (mode === "cancel-before-worker") await proceed.promise; created.resolve(launch()); },
			readFile: async () => {
				if (++reads === 2) { read.resolve(); if (mode === "error") throw new Error("owned read failure"); }
				return input;
			},
		}, { callID: "owned", args: { path: "owned.txt" }, signal: abort.signal })
			.then((value) => ({ value, error: undefined }), (error: unknown) => ({ value: undefined, error }))
			.finally(() => { settled = true; });
		return { entered, proceed, created, read, abort, output, settled: () => settled };
	};
	const sibling = start("success");
	const siblingWorker = await sibling.created.promise;
	const running: ReturnType<typeof start>[] = [sibling];
	try {
		for (const mode of ["success", "cancel-before-worker", "cancel-with-worker", "error"]) {
			const request = start(mode); running.push(request); await request.entered.promise;
			if (mode === "cancel-before-worker") {
				request.abort.abort(new Error("owned cancellation"));
				await nextTurn(); expect(request.settled()).toBe(false); request.proceed.resolve();
			}
			const worker = await request.created.promise; await request.read.promise;
			if (mode === "cancel-with-worker") request.abort.abort(new Error("owned cancellation"));
			await nextTurn(); await nextTurn();
			expect(request.settled()).toBe(false); expect(sibling.settled()).toBe(false);
			worker.postMessage("finish");
			const result = await request.output;
			expect(worker.threadId).toBe(-1); expect(siblingWorker.threadId).not.toBe(-1); expect(foreign.threadId).not.toBe(-1);
			if (mode === "success") expect(result.value?.result.content).toEqual([{ type: "text", text: "owned input\n" }]);
			else expect(String(result.error)).toContain(mode === "error" ? "owned read failure" : "owned cancellation");
		}
		siblingWorker.postMessage("finish"); await sibling.output;
		expect(process.listenerCount("worker")).toBe(listeners);
	} finally {
		for (const request of running) request.proceed.resolve();
		await Promise.all(workers.map(worker => worker.terminate()));
		await Promise.all(running.map(request => request.output));
	}
}
