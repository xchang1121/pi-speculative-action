import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { serialize } from "node:v8";

// Explicit shared Actor/producer semantics, NOT equivalence with ambient native fd.
export const CLOSED_SEARCH_PROFILE = Object.freeze({
	id: "pi.captured-find.v3", pi: "0.84.1", platform: process.platform, node: process.version,
	find: Object.freeze({ minimatch: "10.2.5", ignore: "7.0.5", gitignore: "workspace ancestors and descendants; no global config",
		platform: "linux", nocase: false, dot: true, matchBase: true, nocomment: true, nonegate: true, braceExpandMax: 10_000 }),
	environment: Object.freeze({ PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" }),
	filesystem: "readonly /workspace namespace; exact spelling; normalized in-root aliases; no ambient filesystem fallback",
	limits: Object.freeze({ inputBytes: 8 * 1024 * 1024, entries: 4096, requestBytes: 9 * 1024 * 1024, resultBytes: 1024 * 1024 }),
});
let owned = false;

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
	serveClosedSearchWorker((input, readInput) => kernel.execute(input.kind, input.root, input.args, readInput));
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
	const { createFindToolDefinition } = await import(new URL("./core/tools/find.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	return { execute: async (kind, root, args, readInput) => {
		assert.equal(kind, "find", "closed search operation denied");
		const inputs = new Map(), rules = new Map(), decisions = new Map(); let failure;
		const read = async (operation, target) => {
			if (failure) throw failure;
			const normalized = namespace.normalize(target), key = JSON.stringify([operation, normalized]);
			if (!inputs.has(key)) {
				try {
					assert.ok(inputs.size < profile.limits.entries, "input entry budget");
					inputs.set(key, { value: await readInput(operation, normalized) });
				} catch (error) {
					if (!["ENOENT", "ENOTDIR", "EISDIR"].includes(error.code)) failure = error;
					inputs.set(key, { error });
				}
			}
			const entry = inputs.get(key); if (entry.error) throw entry.error; return entry.value;
		};
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
