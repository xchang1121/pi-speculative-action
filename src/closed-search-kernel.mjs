import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import { createRequire, registerHooks } from "node:module";
import path from "node:path";
import { PassThrough } from "node:stream";
import { fileURLToPath } from "node:url";
import { getSystemErrorMap } from "node:util";
import { serialize } from "node:v8";

// Explicit shared Actor/producer semantics, NOT equivalence with ambient native fd.
export const CLOSED_SEARCH_PROFILE = Object.freeze({
	id: "pi.captured-search.v4", pi: "0.84.1", platform: process.platform, node: process.version,
	find: Object.freeze({ minimatch: "10.2.5", ignore: "7.0.5", gitignore: "workspace ancestors and descendants; no global config",
		platform: "linux", nocase: false, dot: true, matchBase: true, nocomment: true, nonegate: true, braceExpandMax: 10_000 }),
	grep: Object.freeze({ process: "caller-owned pinned rg", filesystem: "caller-granted stat and readFile; no ambient fallback" }),
	bootstrapEnvironment: Object.freeze({ PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" }),
	filesystem: "readonly /workspace namespace; exact spelling; normalized in-root aliases; no ambient filesystem fallback",
	limits: Object.freeze({ inputBytes: 8 * 1024 * 1024, entries: 4096, requestBytes: 9 * 1024 * 1024, resultBytes: 1024 * 1024 }),
});
let owned = false;
const systemErrors = new Set([...getSystemErrorMap().values()].map(([code]) => code));

/** One bounded process owns invocation and asynchronous input correspondence; no synchronous relay thread. */
export function serveClosedSearchWorker(execute) {
	assert.ok(process.send && process.execArgv.includes("--max-old-space-size=128"), "Search requires a bounded child process");
	let active, pending, sequence = 0;
	const send = (message) => {
		assert.ok(serialize(message).byteLength <= CLOSED_SEARCH_PROFILE.limits.requestBytes, "worker frame budget");
		process.send(message, (error) => { if (error) throw error; });
	};
	process.once("disconnect", () => process.exit(1));
	process.on("message", async ({ type, id, input, ...response }) => {
		assert.ok(serialize({ type, id, input, ...response }).byteLength <= CLOSED_SEARCH_PROFILE.limits.requestBytes, "host frame budget");
		if (type === "input") {
			assert.ok(pending && id === active && response.sequence === pending.sequence, "unowned input response");
			if (Object.hasOwn(response, "chunk")) { assert.equal(typeof pending.onChunk, "function", "unobserved input chunk"); pending.onChunk(response.chunk); return; }
			const request = pending; pending = undefined; request.cleanup();
			if (Object.hasOwn(response, "error")) request.reject(Object.assign(new Error(response.error), { code: response.code }));
			else request.resolve(response.value);
			return;
		}
		assert.ok(active === undefined && type === "request" && Number.isSafeInteger(id) && id > 0, "unowned search invocation");
		active = id;
		try {
			send({ type: "started", id });
			const result = await execute(input, (operation, target, { signal, onChunk } = {}) => new Promise((resolve, reject) => {
				signal?.throwIfAborted();
				assert.ok(active === id && !pending, "unowned input request");
				const inputSequence = ++sequence, abort = () => send({ type: "input_cancel", id, sequence: inputSequence });
				pending = { sequence: inputSequence, resolve, reject, onChunk, cleanup: () => signal?.removeEventListener("abort", abort) };
				signal?.addEventListener("abort", abort, { once: true });
				send({ type: "input", id, sequence: inputSequence, operation, target });
			}));
			assert.ok(!pending && serialize(result).byteLength <= CLOSED_SEARCH_PROFILE.limits.resultBytes, "result frame budget or pending input");
			send({ type: "result", id, result });
		} catch (error) { send({ type: "result", id, error: String(error?.message ?? error).slice(0, 8192) }); }
		finally { active = undefined; }
	});
	send({ type: "ready", profile: CLOSED_SEARCH_PROFILE });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	const kernel = await createClosedSearchKernel();
	serveClosedSearchWorker(kernel.execute);
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
	assert.ok(process.send && process.execArgv.includes("--max-old-space-size=128") && !owned, "Search kernels require their own bounded process lifetime");
	owned = true;
	const { Minimatch, ignore } = await loadSearchEngines(), profile = CLOSED_SEARCH_PROFILE, namespace = path.posix;
	let invokeProcess;
	// Pi's spawn stays byte-for-byte stock; only the fixed tool's process seam is caller-owned.
	const outlet = {
		getToolPath: (name) => { assert.equal(name, "rg"); return "rg"; }, // A caller-owned capability, never ambient tool discovery.
		spawn: (file, args, options) => {
			assert.ok(invokeProcess, "no owning invocation");
			const child = new EventEmitter(), controller = new AbortController();
			child.stdout = new PassThrough(); child.stderr = new PassThrough(); child.killed = false;
			child.kill = () => { child.killed = true; controller.abort(); return true; };
			void invokeProcess("process", { file, args, options }, { signal: controller.signal, onChunk: ({ fd, data }) => {
				assert.ok(fd === 1 || fd === 2); (fd === 1 ? child.stdout : child.stderr).write(data);
			} }).then(({ code, signal }) => {
				child.stdout.end(); child.stderr.end(); child.emit("exit", code, signal);
				queueMicrotask(() => child.emit("close", code, signal));
			}, (error) => { child.stdout.end(); child.stderr.end(); child.emit("error", error); child.emit("close", null, null); });
			return child;
		},
	};
	const exports = Object.keys(await import("node:child_process")).filter((name) => name !== "default");
	for (const name of exports) outlet[name] ??= () => { throw new Error(`process capability not granted: ${name}`); };
	globalThis.__closedSearchProcess = outlet;
	const module = new URL("./closed-search-process-capability.mjs", import.meta.url).href;
	const source = `export const { ${exports.join(",")} } = globalThis.__closedSearchProcess; export default globalThis.__closedSearchProcess;`;
	const manager = new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href;
	registerHooks({ resolve(specifier, context, next) {
		return ["child_process", "node:child_process"].includes(specifier) ? { url: module, format: "module", shortCircuit: true } : next(specifier, context);
	}, load(url, context, next) {
		if (url === module) return { format: "module", source, shortCircuit: true };
		if (url === manager) return { format: "module", source: "export const { getToolPath } = globalThis.__closedSearchProcess; export const ensureTool = getToolPath;", shortCircuit: true };
		return next(url, context);
	} });
	process.env.PI_OFFLINE = "1";
	globalThis.fetch = () => { throw new Error("no search downloads"); };
	const { createFindToolDefinition } = await import(new URL("./core/tools/find.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	const { createGrepToolDefinition } = await import(new URL("./core/tools/grep.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	return { execute: async ({ kind, root, args, home }, readInput) => {
		assert.ok(kind === "find" || kind === "grep", "closed search operation denied");
		assert.ok(typeof home === "string" && path.isAbsolute(home), "search home binding required");
		process.env.HOME = process.env.USERPROFILE = home; // Every invocation owns its resolver, including a reused worker.
		const inputs = new Map(), rules = new Map(), decisions = new Map(); let failure;
		const read = async (operation, target) => {
			if (failure) throw failure;
			const normalized = kind === "find" ? namespace.normalize(target) : target, key = JSON.stringify([operation, normalized]);
			if (!inputs.has(key)) {
				try {
					assert.ok(inputs.size < profile.limits.entries, "input entry budget");
					inputs.set(key, { value: await readInput(operation, normalized) });
				} catch (error) {
					if (!systemErrors.has(error.code)) failure = error; // Observed OS errors keep Pi's behavior; missing authority poisons the invocation.
					inputs.set(key, { error });
				}
			}
			const entry = inputs.get(key); if (entry.error) throw entry.error; return entry.value;
		};
		if (kind === "grep") {
			invokeProcess = readInput;
			try {
				const tool = createGrepToolDefinition(root, { operations: {
					isDirectory: async (target) => (await read("stat", target)).directory,
					readFile: async (target) => Buffer.from(await read("readFile", target)).toString("utf8"),
				} });
				return { result: await tool.execute("captured-grep", args).finally(() => { if (failure) throw failure; }), isError: false };
			} finally { invokeProcess = undefined; }
		}
		const layers = async (directory) => {
			if (!rules.has(directory)) {
				const inherited = directory === "/workspace" ? [] : await layers(namespace.dirname(directory));
				let text = "";
				try { text = Buffer.from(await read("readFile", namespace.join(directory, ".gitignore"))).toString("utf8"); }
				catch (error) { if (error.code !== "ENOENT") throw error; }
				rules.set(directory, [...inherited, { directory, matcher: ignore({ ignorecase: false }).add(text) }]);
			}
			return rules.get(directory);
		};
		const ignored = async (target, directory) => {
			if (target === "/workspace") return false;
			if (!decisions.has(target)) {
				const parent = namespace.dirname(target); let excluded = await ignored(parent, true);
				if (!excluded) for (const layer of await layers(parent)) {
					const relative = namespace.relative(layer.directory, target) + (directory ? "/" : "");
					const match = layer.matcher.test(relative);
					if (match.ignored || match.unignored) excluded = match.ignored;
				}
				decisions.set(target, excluded);
			}
			return decisions.get(target);
		};
		const tool = createFindToolDefinition(root, { operations: {
			exists: async (target) => { try { await read("stat", await readInput("resolve", target)); return true; } catch (error) { if (error.code === "ENOENT") return false; throw error; } },
			glob: async (pattern, cwd, options) => {
				const base = await readInput("resolve", cwd), matches = [], matcher = new Minimatch(pattern, profile.find);
				const excluded = options.ignore.map((pattern) => new Minimatch(pattern, profile.find));
				const walk = async (target) => {
					const { directory } = await read("stat", target), relative = namespace.relative(base, target);
					if (await ignored(target, directory) || excluded.some((rule) => rule.match(target + (directory ? "/" : "")))) return;
					const spellings = [relative, `./${relative}`, target];
					if (relative && spellings.some((value) => matcher.match(value + (directory ? "/" : ""))))
						matches.push(path.resolve(root, namespace.relative("/workspace", target)) + (directory ? path.sep : ""));
					// The matcher owns grammar and prefix admission; input names never pass through an identity-folding filesystem cache.
					if (directory && (!relative || matcher.globParts.some((parts) => parts.length === 1) || spellings.some((value) => matcher.match(value, true))))
						for (const name of await read("readdir", target)) await walk(namespace.join(target, name));
				};
				await walk(base);
				return matches.sort().slice(0, options.limit);
			},
		} });
		return { result: await tool.execute("captured-find", args).finally(() => { if (failure) throw failure; }), isError: false };
	} };
}
