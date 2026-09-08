import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { serialize } from "node:v8";
import { isMainThread, Worker, parentPort, workerData, MessageChannel, receiveMessageOnPort } from "node:worker_threads";

// Explicit shared Actor/producer semantics, NOT equivalence with ambient native fd.
export const CLOSED_SEARCH_PROFILE = Object.freeze({
	id: "pi.captured-find.v2", pi: "0.84.1", platform: process.platform, node: process.version,
	find: Object.freeze({ minimatch: "10.2.5", ignore: "7.0.5", gitignore: "workspace ancestors and descendants; no global config",
		platform: "linux", nocase: false, dot: true, matchBase: true, nocomment: true, nonegate: true, braceExpandMax: 10_000 }),
	environment: Object.freeze({ PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" }),
	filesystem: "readonly /workspace namespace; exact spelling; normalized in-root aliases; no ambient filesystem fallback",
	limits: Object.freeze({ inputBytes: 8 * 1024 * 1024, entries: 4096, requestBytes: 9 * 1024 * 1024, resultBytes: 1024 * 1024 }),
});
let owned = false;

/** Relay synchronous engine reads outside its blocked worker, retaining hard process cancellation. */
export function connectClosedSearchWorker(entry) {
	if (isMainThread) {
		assert.ok(process.send && process.execArgv.includes("--max-old-space-size=128"), "Search requires a bounded child process");
		const control = new Int32Array(new SharedArrayBuffer(4)), { port1, port2 } = new MessageChannel();
		const guest = new Worker(new URL(entry), { workerData: { control, inputPort: port2 }, transferList: [port2] });
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

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	const readInput = connectClosedSearchWorker(import.meta.url);
	if (readInput) {
		const kernel = await createClosedSearchKernel();
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

/** Only Pi's installed dependencies: no CLI discovery, download, native child or regex reimplementation. */
export async function loadSearchEngines() {
	const pi = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent")), profile = CLOSED_SEARCH_PROFILE;
	assert.equal(JSON.parse(await readFile(new URL("../package.json", import.meta.resolve("@earendil-works/pi-coding-agent")), "utf8")).version, profile.pi);
	for (const [name, version] of [["minimatch", profile.find.minimatch], ["ignore", profile.find.ignore]])
		assert.equal(pi(`${name}/package.json`).version, version, `Requalify Pi's installed ${name}`);
	const { Minimatch } = pi("minimatch"), ignore = pi("ignore");
	return { Minimatch, ignore };
}

export async function createClosedSearchKernel() {
	assert.ok(!isMainThread && !owned, "Search kernels require their own worker lifetime");
	owned = true;
	const { Minimatch, ignore } = await loadSearchEngines(), profile = CLOSED_SEARCH_PROFILE, namespace = path.posix;
	const { createFindToolDefinition } = await import(new URL("./core/tools/find.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	return { execute: async (kind, root, args, readInput) => {
		assert.equal(kind, "find", "closed search operation denied");
		const inputs = new Map(), rules = new Map(), decisions = new Map(); let failure;
		const read = (operation, target) => {
			if (failure) throw failure;
			const normalized = namespace.normalize(target), key = JSON.stringify([operation, normalized]);
			if (!inputs.has(key)) {
				try {
					assert.ok(inputs.size < profile.limits.entries, "input entry budget");
					inputs.set(key, { value: readInput(operation, normalized) });
				} catch (error) {
					if (!["ENOENT", "ENOTDIR", "EISDIR"].includes(error.code)) failure = error;
					inputs.set(key, { error });
				}
			}
			const entry = inputs.get(key); if (entry.error) throw entry.error; return entry.value;
		};
		const layers = (directory) => {
			if (!rules.has(directory)) {
				const inherited = directory === "/workspace" ? [] : layers(namespace.dirname(directory));
				let text = "";
				try { text = Buffer.from(read("readFile", namespace.join(directory, ".gitignore"))).toString("utf8"); }
				catch (error) { if (error.code !== "ENOENT") throw error; }
				rules.set(directory, [...inherited, { directory, matcher: ignore({ ignorecase: false }).add(text) }]);
			}
			return rules.get(directory);
		};
		const ignored = (target, directory) => {
			if (target === "/workspace") return false;
			if (!decisions.has(target)) {
				const parent = namespace.dirname(target); let excluded = ignored(parent, true);
				if (!excluded) for (const layer of layers(parent)) {
					const relative = namespace.relative(layer.directory, target) + (directory ? "/" : "");
					const match = layer.matcher.test(relative);
					if (match.ignored || match.unignored) excluded = match.ignored;
				}
				decisions.set(target, excluded);
			}
			return decisions.get(target);
		};
		const tool = createFindToolDefinition(root, { operations: {
			exists: (target) => { try { read("stat", readInput("resolve", target)); return true; } catch (error) { if (error.code === "ENOENT") return false; throw error; } },
			glob: async (pattern, cwd, options) => {
				const base = readInput("resolve", cwd), matches = [], matcher = new Minimatch(pattern, profile.find);
				const excluded = options.ignore.map((pattern) => new Minimatch(pattern, profile.find));
				const walk = (target) => {
					const { directory } = read("stat", target), relative = namespace.relative(base, target);
					if (ignored(target, directory) || excluded.some((rule) => rule.match(target + (directory ? "/" : "")))) return;
					const spellings = [relative, `./${relative}`, target];
					if (relative && spellings.some((value) => matcher.match(value + (directory ? "/" : ""))))
						matches.push(path.resolve(root, namespace.relative("/workspace", target)) + (directory ? path.sep : ""));
					// The matcher owns grammar and prefix admission; input names never pass through an identity-folding filesystem cache.
					if (directory && (!relative || matcher.globParts.some((parts) => parts.length === 1) || spellings.some((value) => matcher.match(value, true))))
						for (const name of read("readdir", target)) walk(namespace.join(target, name));
				};
				walk(base);
				return matches.sort().slice(0, options.limit);
			},
		} });
		return { result: await tool.execute("captured-find", args).finally(() => { if (failure) throw failure; }), isError: false };
	} };
}
