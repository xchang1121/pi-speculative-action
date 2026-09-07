import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { serialize } from "node:v8";
import { parentPort } from "node:worker_threads";

import { CLOSED_SEARCH_PROFILE, createClosedSearchKernel, connectClosedSearchWorker } from "../dist/closed-search-kernel.mjs";

// Qualification entry, not a production provider: fixed engines, no guest host ports, one invocation at a time.
const readChannel = connectClosedSearchWorker(import.meta.url);
if (readChannel) {
	const rg = await readFile(process.argv[2]);
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
				result = await kernel.execute(input.kind, input.root, input.args, (operation, target) => readChannel(id, operation, target));
			}
			assert.ok(serialize(result).byteLength <= limits.resultBytes, "result frame budget");
			parentPort.postMessage({ type: "result", id, result });
		} catch (error) { parentPort.postMessage({ type: "result", id, error: String(error?.message ?? error).slice(0, 8192) }); }
		finally { active = undefined; }
	});
	parentPort.postMessage({ type: "ready", profile, engines });
}
