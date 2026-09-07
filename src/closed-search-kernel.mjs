import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { constants } from "node:fs";
import { open, readFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import path from "node:path";
import { PassThrough } from "node:stream";
import { fileURLToPath } from "node:url";
import { serialize } from "node:v8";
import { isMainThread, Worker, parentPort, workerData, MessageChannel, receiveMessageOnPort } from "node:worker_threads";

// A common Actor/producer contract, NOT equivalence with ambient native rg/fd.
export const CLOSED_SEARCH_PROFILE = Object.freeze({
	id: "pi.closed-search.v1", pi: "0.84.1", rg: "0b39774ab9fe912a277f3b921f7619c937dc0398eacf184683d2112919cb6943",
	runtime: "wasi-sh@0.11.0", platform: process.platform, node: process.version,
	find: Object.freeze({ globby: "16.2.4", gitignore: true, globalGitignore: false, caseSensitiveMatch: true, onlyFiles: false,
		dot: true, expandDirectories: false, followSymbolicLinks: false, baseNameMatch: true, markDirectories: true, absolute: true }),
	environment: Object.freeze({ PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" }),
	filesystem: "readonly broker; normalized in-root aliases; virtual metadata/first-access inode; no ambient home/config",
	dateMs: 1_700_000_000_000, seed: "pi.closed-search.v1",
	limits: Object.freeze({ inputBytes: 8 * 1024 * 1024, entries: 4096, writeBytes: 1024 * 1024, readBytes: 64 * 1024 * 1024,
		requestBytes: 9 * 1024 * 1024, resultBytes: 1024 * 1024, hostCalls: 100_000 }),
});

let owned = false;

/** One synchronous input mailbox, relayed outside the guest's blocked JS/WASM stack. */
export function connectClosedSearchWorker(entry) {
	if (isMainThread) {
		assert.ok(process.send && ["--wasm-max-mem-pages=1024", "--max-old-space-size=128"].every((flag) => process.execArgv.includes(flag)), "Search requires a bounded child process");
		const control = new Int32Array(new SharedArrayBuffer(4)), { port1, port2 } = new MessageChannel();
		const guest = new Worker(new URL(entry), { argv: process.argv.slice(2), workerData: { control, inputPort: port2 }, transferList: [port2] });
		let pending;
		guest.on("message", (message) => {
			assert.ok(serialize(message).byteLength <= CLOSED_SEARCH_PROFILE.limits.requestBytes, "worker frame budget");
			if (message.type === "input") { assert.ok(!pending, "overlapping input requests"); pending = message; }
			process.send(message, (error) => { if (error) throw error; });
		});
		process.on("message", (message) => {
			assert.ok(serialize(message).byteLength <= CLOSED_SEARCH_PROFILE.limits.requestBytes, "host frame budget");
			if (message.type !== "input") { guest.postMessage(message); return; }
			assert.ok(pending && message.id === pending.id && message.sequence === pending.sequence, "unowned input response");
			port1.postMessage(message); pending = undefined;
			Atomics.store(control, 0, 1); Atomics.notify(control, 0);
		});
		process.once("disconnect", () => process.exit(1));
		guest.on("error", (error) => { throw error; });
		guest.on("exit", (code) => process.exit(code));
		return;
	}
	let sequence = 0;
	return (id, operation, target) => {
		const expected = ++sequence;
		Atomics.store(workerData.control, 0, 0);
		parentPort.postMessage({ type: "input", id, sequence: expected, operation, target });
		while (Atomics.load(workerData.control, 0) === 0) Atomics.wait(workerData.control, 0, 0);
		const response = receiveMessageOnPort(workerData.inputPort)?.message;
		assert.ok(response?.id === id && response.sequence === expected, "unowned input frame");
		if (response.error) throw Object.assign(new Error(response.error), { code: response.code });
		return response.value;
	};
}

// Direct IPC entry exposes fixed searches only; importing the kernel never starts workers.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	const readInput = connectClosedSearchWorker(import.meta.url);
	if (readInput) {
		const handle = await open(process.argv[2], constants.O_RDONLY | constants.O_NONBLOCK), bytes = Buffer.alloc(4 * 1024 * 1024 + 1);
		let size = 0;
		try {
			assert.ok((await handle.stat()).isFile(), "search module must be a regular file");
			while (size < bytes.length) {
				const { bytesRead } = await handle.read(bytes, size, bytes.length - size, size);
				if (!bytesRead) break; size += bytesRead;
			}
			assert.ok(size < bytes.length, "search module byte budget");
		} finally { await handle.close(); }
		const kernel = await createClosedSearchKernel(bytes.subarray(0, size));
		let active = false;
		parentPort.on("message", async ({ type, id, input }) => {
			assert.ok(!active && type === "request" && Number.isSafeInteger(id) && id > 0, "unowned search invocation");
			active = true;
			try {
				parentPort.postMessage({ type: "started", id });
				const result = await kernel.execute(input.kind, input.root, input.args, (operation, target) => readInput(id, operation, target));
				assert.ok(serialize(result).byteLength <= CLOSED_SEARCH_PROFILE.limits.resultBytes, "result frame budget");
				parentPort.postMessage({ type: "result", id, result });
			} catch (error) { parentPort.postMessage({ type: "result", id, error: String(error?.message ?? error).slice(0, 8192) }); }
			finally { active = false; }
		});
		parentPort.postMessage({ type: "ready", profile: CLOSED_SEARCH_PROFILE });
	}
}

/** Trusted, fixed search engines. The caller owns input evidence and hard process termination. */
export async function createClosedSearchKernel(moduleBytes) {
	assert.ok(!isMainThread && !owned, "Search kernels require their own worker lifetime");
	const bytes = Buffer.from(moduleBytes), profile = CLOSED_SEARCH_PROFILE, { limits } = profile;
	assert.equal(createHash("sha256").update(bytes).digest("hex"), profile.rg, "Requalify the search module");
	owned = true; // Reserve before the first await; failed preparation retires this worker.
	for (const [name, version, relative] of [["wasi-sh", "0.11.0", "../package.json"], ["globby", "16.2.4", "./package.json"]]) {
		assert.equal(JSON.parse(await readFile(new URL(relative, import.meta.resolve(name)), "utf8")).version, version);
	}
	const { WasiShim, WasiExit } = await import("wasi-sh/shim"), { fsError } = await import("wasi-sh/fs");
	const { globbySync } = await import("globby");
	Date.now = () => profile.dateMs;
	const module = await WebAssembly.compile(bytes), supplied = new WasiShim({}).imports(), current = new AsyncLocalStorage();
	assert.deepEqual(WebAssembly.Module.imports(module).filter((entry) => !supplied[entry.module]?.[entry.name]), []);
	assert.equal(JSON.parse(await readFile(new URL("../package.json", import.meta.resolve("@earendil-works/pi-coding-agent")), "utf8")).version, profile.pi,
		"Requalify Pi's private grep process boundary");
	const virtual = (target) => {
		const relative = path.relative(current.getStore().root, target);
		if (path.isAbsolute(relative) || relative === ".." || relative.startsWith(`..${path.sep}`)) throw fsError("ENOENT"); // Closed namespace, not host absence.
		return path.posix.join("/workspace", relative.split(path.sep).join("/"));
	};
	const logical = (target) => path.resolve(current.getStore().root, path.posix.relative("/workspace", target));
	const entry = new URL("./core/tools/grep.js?closed-profile", import.meta.resolve("@earendil-works/pi-coding-agent")).href;
	const hook = registerHooks({ resolve(specifier, context, next) {
		if (context.parentURL === entry && ["node:child_process", "child_process"].includes(specifier))
			return { url: "data:text/javascript,export const spawn = globalThis.__piClosedSearchSpawn", shortCircuit: true };
		if (context.parentURL === entry && specifier.endsWith("/tools-manager.js"))
			return { url: 'data:text/javascript,export const ensureTool = async () => "closed:rg"', shortCircuit: true };
		return next(specifier, context);
	} });
	globalThis.__piClosedSearchSpawn = (command, input) => {
		assert.equal(command, "closed:rg");
		const child = new EventEmitter(), state = current.getStore();
		child.stdout = new PassThrough(); child.stderr = new PassThrough(); child.killed = false;
		child.kill = () => { child.killed = true; return true; };
		Promise.resolve().then(async () => {
			try {
				const args = [...input]; args[args.length - 1] = virtual(args.at(-1));
				const result = await run(module, ["rg", ...args], state.store);
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
	let createGrepToolDefinition, createFindToolDefinition;
	try {
		({ createGrepToolDefinition } = await import(entry));
		({ createFindToolDefinition } = await import(new URL("./core/tools/find.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href));
	} finally { hook.deregister(); }
	const execute = async (kind, root, args, readInput) => {
		assert.ok(kind === "grep" || kind === "find", "closed search operation denied");
		// Invocation-private materialization. Even errors swallowed by Pi/globby invalidate this execution.
		const inputs = new Map(); let failure, inode = 0;
		const read = (operation, target) => {
			if (failure) throw failure;
			const key = JSON.stringify([operation, target]);
			if (!inputs.has(key)) {
				try {
					assert.ok(inputs.size < limits.entries, "input entry budget");
					inputs.set(key, { value: readInput(operation, target) });
				} catch (error) {
					if (!["ENOENT", "ENOTDIR", "EISDIR"].includes(error.code)) failure = error;
					inputs.set(key, { error });
				}
			}
			const entry = inputs.get(key); if (entry.error) throw entry.error; return entry.value;
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
			...Object.fromEntries(["createFileSync", "mkdirSync", "writeSync", "touchSync", "unlinkSync", "rmdirSync", "renameSync", "linkSync"]
				.map((operation) => [operation, () => { throw failure ??= fsError("EROFS"); }])),
		};
		const state = { root, store, contextReads: 0 }, operations = nodeFilesystem(store);
		const tool = kind === "find" ? createFindToolDefinition(root, { operations: {
			exists: (target) => { try { operations.statSync(target); return true; } catch (error) { if (error.code === "ENOENT") return false; throw error; } },
			glob: async (pattern, cwd, options) => globbySync(pattern, { ...profile.find, cwd, fs: operations, ignore: options.ignore }).sort().slice(0, options.limit),
		} }) : createGrepToolDefinition(root, { operations: {
			isDirectory: (target) => (store.statSync(virtual(target)).mode & 0o170000) === 0o040000,
			readFile: (target) => { state.contextReads++; return operations.readFileSync(target, "utf8"); },
		} });
		return { result: await current.run(state, () => tool.execute("closed-search", args)).finally(() => { if (failure) throw failure; }),
			isError: false, contextReads: state.contextReads };
	};
	return { execute, run, module };

	function nodeFilesystem(store) {
		const statSync = (target) => {
			const stat = store.statSync(virtual(target));
			return { ...stat, name: path.basename(target), parentPath: path.dirname(target),
				...Object.fromEntries(Object.entries({ isFile: 0o100000, isDirectory: 0o040000, isSymbolicLink: 0o120000,
					isFIFO: 0o010000, isSocket: 0o140000, isCharacterDevice: 0o020000, isBlockDevice: 0o060000 })
					.map(([name, mode]) => [name, () => (stat.mode & 0o170000) === mode])) };
		};
		const operations = { stat: statSync, lstat: statSync,
			readdir: (target, options) => store.readdirSync(virtual(target)).map((name) => options?.withFileTypes ? statSync(path.join(target, name)) : name),
			readFile: (target, encoding) => {
				const file = virtual(target), bytes = Buffer.alloc(store.statSync(file).size);
				store.readSync(file, bytes, 0, bytes.length); return encoding ? bytes.toString(encoding) : bytes;
			} };
		// No globby/fast-glob sync, callback or promise operation may fall back to ambient fs.
		return { promises: Object.fromEntries(Object.entries(operations).map(([name, run]) => [name, async (...args) => run(...args)])),
			...Object.fromEntries(Object.entries(operations).flatMap(([name, run]) => [[name + "Sync", run], [name, (...args) => {
				const callback = args.pop(); let value;
				try { value = run(...args); } catch (error) { callback(error); return; } callback(null, value);
			}]])) };
	}
	async function run(module, args, filesystem) {
		const started = performance.now(), stdout = [], stderr = [], calls = {};
		let written = 0, read = 0, importsUsed = 0, ticks = 0, randomBlock = 0;
		const shim = new WasiShim({ args, fs: filesystem, env: profile.environment,
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
		const instance = await WebAssembly.instantiate(module, imports);
		shim.bindMemory(instance.exports.memory);
		let exitCode = 0;
		try { instance.exports._start(); } catch (error) { if (error instanceof WasiExit) exitCode = error.code; else throw error; }
		assert.throws(() => instance.exports.memory.grow(1024), RangeError);
		return { exitCode, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString(),
			durationMs: performance.now() - started, clocks: calls.clock_time_get ?? 0, random: calls.random_get ?? 0 };
	}
}
