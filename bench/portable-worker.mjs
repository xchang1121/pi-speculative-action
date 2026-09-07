import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { brotliDecompressSync } from "node:zlib";
import { serialize } from "node:v8";
import { isMainThread, Worker, parentPort, workerData, MessageChannel, receiveMessageOnPort } from "node:worker_threads";

import { CLOSED_SEARCH_PROFILE, createClosedSearchKernel } from "../dist/closed-search-kernel.mjs";

// Qualification entry, not a production provider: fixed engines, no guest host ports, one invocation at a time.
if (isMainThread) {
	assert.ok(process.send, "Start through portable-kernel.mjs");
	// Own the single synchronous input mailbox outside the guest's blocked JS/WASM stack.
	const control = new Int32Array(new SharedArrayBuffer(4)), { port1, port2 } = new MessageChannel();
	const guest = new Worker(new URL(import.meta.url), { argv: process.argv.slice(2), workerData: { control, inputPort: port2 }, transferList: [port2] });
	let pending;
	guest.on("message", (message) => {
		if (message.type === "input") { assert.ok(!pending); pending = message; }
		process.send(message);
	});
	process.on("message", (message) => {
		if (message.type !== "input") { guest.postMessage(message); return; }
		assert.ok(pending && message.id === pending.id && message.sequence === pending.sequence, "unowned input response");
		port1.postMessage(message); pending = undefined;
		Atomics.store(control, 0, message.sequence); Atomics.notify(control, 0);
	});
	guest.on("error", (error) => { throw error; });
	guest.on("exit", (code) => process.exit(code));
} else {
	// The CLI-bearing npm package stays OUTSIDE this repository and the Actor's PATH.
	const require = createRequire(path.join(process.argv[2], "package.json"));
	const { getCompressedBytes } = await import(new URL("./_rg.wasm.mjs", pathToFileURL(require.resolve("ripgrep"))).href);
	const rg = brotliDecompressSync(getCompressedBytes());
	await assert.rejects(createClosedSearchKernel(Buffer.from("unqualified")), /Requalify the search module/);
	const supplied = Buffer.from(rg), preparing = createClosedSearchKernel(supplied);
	supplied.fill(0); // The kernel must own the bytes it hashed across asynchronous initialization.
	await assert.rejects(createClosedSearchKernel(rg), /own worker lifetime/);
	const profile = CLOSED_SEARCH_PROFILE, { limits } = profile, kernel = await preparing;
	const { WasiShim } = await import("wasi-sh/shim"), { memoryFs } = await import("wasi-sh/fs");
	const binaries = { sh: await readFile(new URL(import.meta.resolve("wasi-sh/busybox.wasm"))), rg };
	const modules = {}, engines = {};
	for (const [name, bytes] of Object.entries(binaries)) {
		const started = performance.now(); modules[name] = name === "rg" ? kernel.module : await WebAssembly.compile(bytes);
		const supplied = new WasiShim({}).imports();
		assert.deepEqual(WebAssembly.Module.imports(modules[name]).filter((entry) => !supplied[entry.module]?.[entry.name]), []);
		engines[name] = { bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex"), compileMs: name === "rg" ? undefined : performance.now() - started };
	}
	// _start announces actual guest entry, then loops without further imports. Import quotas cannot cancel it.
	const spin = await WebAssembly.compile(Buffer.from("0061736d01000000010401600000020f0103656e7607656e7465726564000003020100070a01065f737461727400010a0b010900100003400c000b0b", "hex"));
	const mutations = ["createFileSync", "mkdirSync", "writeSync", "touchSync", "unlinkSync", "rmdirSync", "renameSync", "linkSync"];
	let sequence = 0;

	function readInput(operation, target) {
		const expected = ++sequence;
		parentPort.postMessage({ type: "input", id: active.id, sequence: expected, operation, target });
		Atomics.wait(workerData.control, 0, expected - 1);
		const response = receiveMessageOnPort(workerData.inputPort)?.message;
		assert.ok(response?.id === active.id && response.sequence === expected, "unowned input frame");
		if (response.error) throw Object.assign(new Error(response.error), { code: response.code });
		return response.value;
	}

	function imageStore(image) {
		assert.ok(Object.keys(image.files).length + image.directories.length <= limits.entries, "input entry budget");
		assert.ok(Object.values(image.files).reduce((size, bytes) => size + bytes.length, 0) <= limits.inputBytes, "input byte budget");
		const store = memoryFs(image.files);
		for (const directory of image.directories) {
			try { store.statSync(directory); } catch (error) { assert.equal(error.code, "ENOENT"); store.mkdirSync(directory); }
		}
		let failure, allocation = 0;
		const guard = (condition) => { if (!condition) throw failure ??= new Error("private filesystem budget/authority exceeded"); };
		for (const operation of mutations) {
			const perform = store[operation].bind(store);
			store[operation] = (...args) => {
				// Fixed writable fixtures only; not a quota proof for arbitrary shell mutations/open-fd snapshots.
				guard(!["unlinkSync", "renameSync", "linkSync"].includes(operation));
				const size = operation === "writeSync" ? args[2] + args[1].length : operation === "touchSync" ? args[1]?.size ?? 0 : 0;
				guard(Number.isSafeInteger(size) && size >= 0 && (allocation += size) <= limits.inputBytes);
				return perform(...args);
			};
		}
		return { store, verify: () => { if (failure) throw failure; } };
	}

	let active;
	parentPort.on("message", async (message) => {
		if (message.type === "resume") { if (active?.id === message.id) active.resume?.(); return; }
		assert.ok(!active && message.type === "request", "unowned guest invocation");
		active = { id: message.id };
		const { id, input } = message;
		try {
			if (input.kind === "spin") (await WebAssembly.instantiate(spin, { env: { entered: () => parentPort.postMessage({ type: "started", id }) } })).exports._start();
			parentPort.postMessage({ type: "started", id });
			let result;
			if (input.kind === "kernel") {
				const filesystem = imageStore(input.image);
				if (input.probeSparse) filesystem.store.writeSync("/workspace/a.txt", Buffer.from("x"), 16 * 1024 * 1024);
				result = [];
				for (const command of input.commands) result.push(await kernel.run(modules[command.name], [command.name, ...command.args], filesystem.store));
				filesystem.verify();
			} else {
				result = await kernel.execute(input.kind, input.root, input.args, readInput);
				if (input.pause) await new Promise((resolve) => { active.resume = resolve; parentPort.postMessage({ type: "checkpoint", id }); });
			}
			assert.ok(serialize(result).byteLength <= limits.resultBytes, "result frame budget");
			parentPort.postMessage({ type: "result", id, result });
		} catch (error) { parentPort.postMessage({ type: "result", id, error: String(error?.message ?? error).slice(0, 8192) }); }
		finally { active = undefined; }
	});
	parentPort.postMessage({ type: "ready", profile, engines });
}
