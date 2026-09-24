import { deferred } from "./async.ts";
import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs/promises";
import path from "node:path";
import { temporaryDirectories } from "./filesystem.ts";
import { buildPiActionKey } from "../src/action-semantics.ts";
import { Worker } from "node:worker_threads";
import process from "node:process";
import { setImmediate as nextTurn } from "node:timers/promises";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { ToolFilesystemOperations } from "../src/tool-settlement.ts";

const directories = temporaryDirectories("pi-actor-write-");
afterEach(() => directories.dispose());

describe("stock Pi invocation identity", () => {
	it.each(["write", "nested", "edit", "over-budget", "failed", "aborted", "ignored", "foreign", "disposed"])("retains only completed Actor write poststates (%s)", async (mode) => {
		const cwd = await directories.create(), target = mode === "nested" ? "new/nested/input.txt" : "input.txt";
		const file = path.join(cwd, target), original = "\uFEFFbefore\r\nsecond\r\n";
		if (mode !== "nested") await fs.writeFile(file, original);
		const tool = mode === "edit" || mode === "failed" ? "edit" : "write";
		const args = { path: "@" + target, ...(tool === "write" ? { content: original.replace("before", "after") }
			: { edits: [{ oldText: mode === "failed" ? "absent" : "before", newText: "after" }] }) };
		const invocation = resolvePiToolInvocation(tool, args, { cwd, environment: {} })!;
		const capture = invocation.captureInputs!(buildPiActionKey(tool, args, cwd)!, mode === "over-budget" ? 1 : 65536, mode);
		const abort = new AbortController(), writes = vi.spyOn(fs, "writeFile"), reads = vi.spyOn(fs, "readFile"), opened = vi.spyOn(fs, "open"), scanned = vi.spyOn(fs, "readdir");
		let branch: Awaited<ReturnType<typeof capture.seal>> | undefined;
		try {
			if (mode === "aborted") abort.abort(new Error("cancelled"));
			if (mode === "disposed") await capture.dispose();
			let output: Awaited<ReturnType<NonNullable<typeof invocation.authoritative>>> = { result: { content: [], details: {} }, isError: false };
			if (mode !== "ignored") {
				const pending = invocation.authoritative!({ callID: mode === "foreign" ? "other" : mode, args, signal: abort.signal });
				if (mode === "failed" || mode === "aborted") await expect(pending).rejects.toThrow();
				else output = await pending;
			}
			expect(writes).toHaveBeenCalledTimes(["failed", "aborted", "ignored"].includes(mode) ? 0 : 1);
			const readsBefore = reads.mock.calls.length, openedBefore = opened.mock.calls.length;
			if (mode === "write" || mode === "nested" || mode === "edit") {
				const sealing = capture.seal(output);
				await expect(capture.seal(output)).rejects.toThrow("unavailable");
				branch = await sealing;
				expect(branch.inputsOnly).toBe(true);
				expect(reads.mock.calls.length).toBe(readsBefore); expect(opened.mock.calls.length).toBe(openedBefore);
				if (mode === "nested") for (const directory of ["new", "new/nested"]) {
					const args = { path: directory }, action = buildPiActionKey("ls", args, cwd)!;
					const query = await branch.reconstruct!({ action: { ...action, executionContext: resolvePiToolInvocation("ls", args, { cwd, environment: {} }) },
						args, callID: "listing", signal: abort.signal });
					expect(query?.output.result.content).toEqual([{ type: "text", text: directory === "new" ? "nested/" : "input.txt" }]);
				}
				expect(scanned).not.toHaveBeenCalled();
				expect((await branch.validate!()).status).toBe("valid");
				if (mode === "nested") {
					await fs.writeFile(path.join(cwd, "new/extra"), "external"); expect((await branch.validate!()).status).toBe("stale");
				}
				await expect(branch.commit()).rejects.toThrow("input_only");
			} else await expect(capture.seal(output)).rejects.toThrow("unavailable");
			expect(await fs.readFile(file, "utf8")).toBe(["failed", "aborted", "ignored"].includes(mode) ? original : original.replace("before", "after"));
		} finally { await branch?.dispose(); await capture.dispose(); writes.mockRestore(); reads.mockRestore(); opened.mockRestore(); scanned.mockRestore(); }
	});

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

	it("declines a read answered from a filename variant Pi guessed outside the view", async () => {
		const cwd = await directories.create(), args = { path: "it's notes.txt" }, signal = new AbortController().signal;
		const view: ToolFilesystemOperations = { access: target => fs.access(target), readFile: target => fs.readFile(target) };
		const read = () => resolvePiToolInvocation("read", args, { cwd, environment: {} })!.filesystem!(view, { callID: "variant", args, signal });
		await fs.writeFile(path.join(cwd, "it\u2019s notes.txt"), "VARIANT\n");
		await expect(read()).rejects.toThrow("read_path_variant");
		await fs.writeFile(path.join(cwd, args.path), "ORIGINAL\n");
		expect((await read()).result.content).toMatchObject([{ text: expect.stringContaining("ORIGINAL") }]);
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
		expect(invocation.identity).toEqual({ ...processIdentity, executor: "pi.bash.local", commandPrefix: "set -e" });
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
