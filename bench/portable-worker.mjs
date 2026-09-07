import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import { createRequire, registerHooks } from "node:module";
import path from "node:path";
import { PassThrough } from "node:stream";
import { pathToFileURL } from "node:url";
import { brotliDecompressSync } from "node:zlib";
import { serialize } from "node:v8";
import { isMainThread, Worker, parentPort, workerData, MessageChannel, receiveMessageOnPort } from "node:worker_threads";

// Qualification entry, not a production provider: fixed engines, no guest host ports, one invocation at a time.
if (isMainThread) {
	assert.ok(process.send, "Start through portable-kernel.mjs");
	// Own the single synchronous input mailbox outside the guest's blocked JS/WASM stack.
	const control = new Int32Array(new SharedArrayBuffer(4)), { port1, port2 } = new MessageChannel();
	const guest = new Worker(new URL(import.meta.url), { argv: process.argv.slice(2),
		workerData: { control, inputPort: port2 }, transferList: [port2] });
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
	const dependencyRoot = process.argv[2], require = createRequire(path.join(dependencyRoot, "package.json"));
	const load = (name) => import(pathToFileURL(require.resolve(name)).href);
	for (const [name, version] of [["wasi-sh", "0.11.0"], ["ripgrep", "0.3.1"]]) {
		assert.equal(JSON.parse(await readFile(path.join(dependencyRoot, "node_modules", name, "package.json"), "utf8")).version, version);
	}
	const { WasiShim, WasiExit } = await load("wasi-sh/shim"), { memoryFs, fsError } = await load("wasi-sh/fs");
	const { getCompressedBytes } = await import(new URL("./_rg.wasm.mjs", pathToFileURL(require.resolve("ripgrep"))).href);
	const binaries = { sh: await readFile(require.resolve("wasi-sh/busybox.wasm")), rg: brotliDecompressSync(getCompressedBytes()) };
	const modules = {}, engines = {}, current = new AsyncLocalStorage();
	for (const [name, bytes] of Object.entries(binaries)) {
		const started = performance.now(); modules[name] = await WebAssembly.compile(bytes);
		const supplied = new WasiShim({}).imports();
		assert.deepEqual(WebAssembly.Module.imports(modules[name]).filter((entry) => !supplied[entry.module]?.[entry.name]), []);
		engines[name] = { bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex"), compileMs: performance.now() - started };
	}
	// _start announces actual guest entry, then loops without further imports. Import quotas cannot cancel it.
	const spin = await WebAssembly.compile(Buffer.from("0061736d01000000010401600000020f0103656e7607656e7465726564000003020100070a01065f737461727400010a0b010900100003400c000b0b", "hex"));
	const { version: VERSION } = JSON.parse(await readFile(new URL("../package.json", import.meta.resolve("@earendil-works/pi-coding-agent")), "utf8"));
	assert.equal(VERSION, "0.84.1", "Requalify the stock tool import and operation boundary");
	const limits = { inputBytes: 8 * 1024 * 1024, entries: 4096, writeBytes: 1024 * 1024, readBytes: 64 * 1024 * 1024,
		requestBytes: 9 * 1024 * 1024, resultBytes: 1024 * 1024, hostCalls: 100_000 };
	const profile = { id: "qualification.closed-grep.v3", pi: VERSION, rg: engines.rg.sha256, runtime: "wasi-sh@0.11.0",
		platform: process.platform, node: process.version, environment: { PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" },
		filesystem: "readonly input broker; virtual metadata/first-access inode; no ambient home/config", dateMs: 1_700_000_000_000, seed: "qualification", limits };
	Date.now = () => profile.dateMs;
	const mutations = ["createFileSync", "mkdirSync", "writeSync", "touchSync", "unlinkSync", "rmdirSync", "renameSync", "linkSync"];
	let sequence = 0;

	function inputStore() {
		// Invocation-private materialization, never shared across invocations or allowed to read the host.
		const entries = new Map(); let failure, inode = 0;
		const read = (operation, target) => {
			if (failure) throw failure;
			const key = JSON.stringify([operation, target]);
			if (!entries.has(key)) {
				try {
					assert.ok(entries.size < limits.entries, "input entry budget");
					const expected = ++sequence;
					parentPort.postMessage({ type: "input", id: active.id, sequence: expected, operation, target });
					Atomics.wait(workerData.control, 0, expected - 1);
					const response = receiveMessageOnPort(workerData.inputPort)?.message;
					assert.ok(response?.id === active.id && response.sequence === expected, "unowned input frame");
					if (response.error) throw Object.assign(new Error(response.error), { code: response.code });
					entries.set(key, { value: response.value });
				} catch (error) {
					if (!["ENOENT", "ENOTDIR", "EISDIR"].includes(error.code)) failure = error;
					entries.set(key, { error });
				}
			}
			const entry = entries.get(key); if (entry.error) throw entry.error; return entry.value;
		};
		const store = {
			statSync: (target) => {
				const stat = read("stat", target);
				return stat.inode ??= { ino: ++inode, nlink: stat.directory ? 2 : 1, mode: stat.directory ? 0o040755 : 0o100644,
					size: stat.size, uid: 0, gid: 0, atimeMs: profile.dateMs, mtimeMs: profile.dateMs, ctimeMs: profile.dateMs };
			},
			readdirSync: (target) => [...read("readdir", target)],
			readSync: (target, buffer, start, end) => buffer.set(read("readFile", target).subarray(start, end)),
			syncSync: () => {},
		};
		for (const operation of mutations)
			store[operation] = () => { throw failure ??= fsError("EROFS"); };
		return { store, verify: () => { if (failure) throw failure; } };
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

	async function run(name, args, filesystem) {
		const started = performance.now(), stdout = [], stderr = [], calls = {};
		let written = 0, read = 0, importsUsed = 0, ticks = 0, randomBlock = 0;
		const shim = new WasiShim({ args: [name, ...args], fs: filesystem, env: profile.environment,
			stdout: (bytes) => stdout.push(Buffer.from(bytes)), stderr: (bytes) => stderr.push(Buffer.from(bytes)),
			input: { read: () => new Uint8Array(), pollReadable: () => true, closed: () => true } });
		const imports = shim.imports();
		imports.wasi_snapshot_preview1.clock_time_get = (id, _precision, out) => {
			new DataView(shim.mem.buffer).setBigUint64(out, id === 0 ? BigInt(profile.dateMs) * 1_000_000n : BigInt(++ticks), true); return 0;
		};
		imports.wasi_snapshot_preview1.random_get = (offset, length) => {
			const bytes = new Uint8Array(shim.mem.buffer, offset, length);
			for (let at = 0; at < length; at += 32) bytes.set(createHash("sha256").update(`${profile.seed}:${randomBlock++}`).digest().subarray(0, length - at), at);
			return 0;
		};
		for (const functions of Object.values(imports)) for (const [operation, perform] of Object.entries(functions)) {
			functions[operation] = (...values) => {
				assert.ok(++importsUsed <= limits.hostCalls && shim.fds.size <= 256, "host call/descriptor budget");
				calls[operation] = (calls[operation] ?? 0) + 1;
				if (operation === "fd_read" || operation === "fd_write") {
					const view = new DataView(shim.mem.buffer); let size = 0;
					assert.ok(values[2] <= 1024, "iovec budget");
					for (let index = 0; index < values[2]; index++) size += view.getUint32(values[1] + index * 8 + 4, true);
					assert.ok(operation === "fd_read" ? (read += size) <= limits.readBytes : (written += size) <= limits.writeBytes, "I/O byte budget");
				}
				return perform(...values);
			};
		}
		const instance = await WebAssembly.instantiate(modules[name], imports);
		shim.bindMemory(instance.exports.memory);
		let exitCode = 0;
		try { instance.exports._start(); } catch (error) { if (error instanceof WasiExit) exitCode = error.code; else throw error; }
		assert.throws(() => instance.exports.memory.grow(1024), RangeError);
		return { exitCode, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString(),
			durationMs: performance.now() - started, clocks: calls.clock_time_get ?? 0, random: calls.random_get ?? 0 };
	}

	const virtual = (target) => {
		const relative = path.relative(current.getStore().root, target);
		assert.ok(!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`), "unproven path");
		return path.posix.join("/workspace", relative.split(path.sep).join("/"));
	};
	const logical = (target) => path.resolve(current.getStore().root, path.posix.relative("/workspace", target));
	const entry = new URL("./core/tools/grep.js?closed-profile", import.meta.resolve("@earendil-works/pi-coding-agent")).href;
	globalThis.__piQualificationSpawn = (command, input) => {
		assert.equal(command, "qualified:rg");
		const child = new EventEmitter(), state = current.getStore();
		child.stdout = new PassThrough(); child.stderr = new PassThrough(); child.killed = false;
		child.kill = () => { child.killed = true; return true; };
		Promise.resolve().then(async () => {
			try {
				const args = [...input]; args[args.length - 1] = virtual(args.at(-1));
				const result = await run("rg", args, state.store);
				await state.checkpoint?.();
				const output = result.stdout.split("\n").filter(Boolean).map((line) => {
					const event = JSON.parse(line);
					if (event.data?.path?.text) event.data.path.text = logical(event.data.path.text);
					return JSON.stringify(event);
				}).join("\n");
				child.stdout.end(output ? `${output}\n` : ""); child.stderr.end(result.stderr); child.emit("close", result.exitCode);
			} catch (error) { child.stdout.end(); child.stderr.end(); child.emit("error", error); }
		});
		return child;
	};
	registerHooks({ resolve(specifier, context, next) {
		if (context.parentURL === entry && ["node:child_process", "child_process"].includes(specifier))
			return { url: "data:text/javascript,export const spawn = globalThis.__piQualificationSpawn", shortCircuit: true };
		if (context.parentURL === entry && specifier.endsWith("/tools-manager.js"))
			return { url: 'data:text/javascript,export const ensureTool = async () => "qualified:rg"', shortCircuit: true };
		return next(specifier, context);
	} });
	const { createGrepToolDefinition } = await import(entry);
	let active;
	parentPort.on("message", async (message) => {
		if (message.type === "resume") { if (active?.id === message.id) active.resume?.(); return; }
		assert.ok(!active && message.type === "request", "unowned guest invocation");
		active = { id: message.id };
		const { id, input } = message;
		try {
			if (input.kind === "spin") (await WebAssembly.instantiate(spin, { env: { entered: () => parentPort.postMessage({ type: "started", id }) } })).exports._start();
			parentPort.postMessage({ type: "started", id });
			const filesystem = input.kind === "kernel" ? imageStore(input.image) : inputStore();
			if (input.probeSparse) filesystem.store.writeSync("/workspace/a.txt", Buffer.from("x"), 16 * 1024 * 1024);
			let result;
			if (input.kind === "kernel") {
				result = [];
				for (const command of input.commands) result.push(await run(command.name, command.args, filesystem.store));
				filesystem.verify();
			} else {
				const state = { root: input.root, store: filesystem.store, contextReads: 0,
					checkpoint: input.pause ? () => new Promise((resolve) => { active.resume = resolve; parentPort.postMessage({ type: "checkpoint", id }); }) : undefined };
				const tool = createGrepToolDefinition(input.root, { operations: {
					isDirectory: (target) => (state.store.statSync(virtual(target)).mode & 0o170000) === 0o040000,
					readFile: (target) => {
						state.contextReads++; const file = virtual(target), bytes = Buffer.alloc(state.store.statSync(file).size);
						state.store.readSync(file, bytes, 0, bytes.length); return bytes.toString("utf8");
					},
				} });
				result = { result: await current.run(state, () => tool.execute(String(id), input.args)).finally(filesystem.verify), isError: false, contextReads: state.contextReads };
			}
			assert.ok(serialize(result).byteLength <= limits.resultBytes, "result frame budget");
			parentPort.postMessage({ type: "result", id, result });
		} catch (error) { parentPort.postMessage({ type: "result", id, error: String(error?.message ?? error).slice(0, 8192) }); }
		finally { active = undefined; }
	});
	parentPort.postMessage({ type: "ready", profile, engines });
}
