import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs, { readFile } from "node:fs/promises";
import { AsyncLocalStorage } from "node:async_hooks";
import { EventEmitter } from "node:events";
import { createRequire, registerHooks } from "node:module";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { fileURLToPath, pathToFileURL } from "node:url";
import { brotliDecompressSync } from "node:zlib";

// Fixed-command qualification, not a plugin provider or an arbitrary-script sandbox.
// Dependencies are explicitly installed outside this package; never downloaded by this script.
const dependencyRoot = process.argv[2];
const environment = { PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" };
assert.ok(dependencyRoot, "Usage: node bench/portable-kernel.mjs <directory containing node_modules>");
if (process.argv[3] !== "--child") {
	const started = performance.now();
	const child = spawnSync(process.execPath, ["--wasm-max-mem-pages=1024", "--max-old-space-size=128",
		fileURLToPath(import.meta.url), path.resolve(dependencyRoot), "--child", ...process.argv.slice(3)], {
		encoding: "utf8", timeout: 15_000, maxBuffer: 1024 * 1024, windowsHide: true,
	});
	assert.ifError(child.error);
	assert.equal(child.status, 0, child.stderr || `kernel qualification exited with ${child.signal}`);
	console.log(JSON.stringify({ ...JSON.parse(child.stdout), processTotalMs: performance.now() - started }, null, 2));
} else {
	const require = createRequire(path.join(path.resolve(dependencyRoot), "package.json"));
	const load = (name) => import(pathToFileURL(require.resolve(name)).href);
	for (const [name, version] of [["wasi-sh", "0.11.0"], ["ripgrep", "0.3.1"]]) {
		const manifest = JSON.parse(await readFile(path.join(dependencyRoot, "node_modules", name, "package.json"), "utf8"));
		assert.equal(manifest.version, version, `${name}: requalify changed engines`);
	}
	const { WasiShim, WasiExit } = await load("wasi-sh/shim");
	const { memoryFs } = await load("wasi-sh/fs");
	const { getCompressedBytes } = await import(new URL("./_rg.wasm.mjs", pathToFileURL(require.resolve("ripgrep"))).href);
	const binaries = {
		sh: await readFile(require.resolve("wasi-sh/busybox.wasm")),
		rg: brotliDecompressSync(getCompressedBytes()),
	};
	const modules = {}, engines = {};
	for (const [name, bytes] of Object.entries(binaries)) {
		const started = performance.now();
		modules[name] = await WebAssembly.compile(bytes);
		const supplied = new WasiShim({}).imports();
		assert.deepEqual(WebAssembly.Module.imports(modules[name]).filter((entry) => !supplied[entry.module]?.[entry.name]), []);
		engines[name] = { bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex"),
			compileMs: performance.now() - started };
	}
	const seed = { "/workspace/a.txt": Buffer.from("before\nneedle\nafter\n"),
		"/workspace/.git/HEAD": "ref: refs/heads/main\n", "/workspace/.gitignore": "ignored.txt\n",
		"/workspace/ignored.txt": "needle ignored\n", "/workspace/sub/b.txt": "needle two\n" };
	const store = memoryFs(seed);
	const run = async (name, args, filesystem = store, profile) => {
		const started = performance.now(), stdout = [], stderr = [], calls = {};
		const shim = new WasiShim({ args: [name, ...args], fs: filesystem,
			env: profile?.environment ?? environment,
			stdout: (bytes) => stdout.push(Buffer.from(bytes)), stderr: (bytes) => stderr.push(Buffer.from(bytes)),
			input: { read: () => new Uint8Array(), pollReadable: () => true, closed: () => true } });
		const imports = shim.imports();
		if (profile) {
			let ticks = 0, randomBlock = 0;
			imports.wasi_snapshot_preview1.clock_time_get = (id, _precision, out) => {
				new DataView(shim.mem.buffer).setBigUint64(out, id === 0 ? BigInt(profile.dateMs) * 1_000_000n : BigInt(++ticks), true);
				return 0;
			};
			imports.wasi_snapshot_preview1.random_get = (offset, length) => {
				const bytes = new Uint8Array(shim.mem.buffer, offset, length);
				for (let at = 0; at < length; at += 32) bytes.set(createHash("sha256").update(`${profile.seed}:${randomBlock++}`).digest().subarray(0, length - at), at);
				return 0;
			};
		}
		let writtenBytes = 0;
		for (const [name, operation] of Object.entries(imports.wasi_snapshot_preview1)) {
			imports.wasi_snapshot_preview1[name] = (...args) => {
				calls[name] = (calls[name] ?? 0) + 1;
				// Includes pipe/file writes, not just final stdout. Only a fixture guard, not a complete memory quota.
				if (name === "fd_write") {
					const view = new DataView(shim.mem.buffer);
					for (let index = 0; index < args[2]; index++) writtenBytes += view.getUint32(args[1] + index * 8 + 4, true);
					assert.ok(writtenBytes <= 1024 * 1024, "fixture exceeded its write budget");
				}
				return operation(...args);
			};
		}
		const instance = await WebAssembly.instantiate(modules[name], imports);
		shim.bindMemory(instance.exports.memory);
		let exitCode = 0;
		try { instance.exports._start(); }
		catch (error) { if (error instanceof WasiExit) exitCode = error.code; else throw error; }
		// Exercise the real module's exported memory; Worker.resourceLimits does not cover this allocation.
		assert.throws(() => instance.exports.memory.grow(1024), RangeError);
		return { exitCode, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString(),
			durationMs: performance.now() - started, clocks: calls.clock_time_get ?? 0, random: calls.random_get ?? 0 };
	};
	const pipeline = await run("sh", ["-c", "cat /workspace/a.txt | grep needle > /workspace/derived.txt"]);
	assert.equal(pipeline.exitCode, 0, pipeline.stderr);
	const search = await run("rg", ["--json", "--line-number", "--color=never", "--sort=path", "--hidden", "--no-config", "--", "needle", "/workspace"]);
	assert.equal(search.exitCode, 0, search.stderr);
	const matches = search.stdout.trim().split("\n").map(JSON.parse).filter((event) => event.type === "match")
		.map((event) => [event.data.path.text, event.data.line_number, event.data.lines.text]);
	assert.deepEqual(matches, [["/workspace/a.txt", 2, "needle\n"], ["/workspace/derived.txt", 1, "needle\n"], ["/workspace/sub/b.txt", 1, "needle two\n"]]);
	const overwrite = await run("sh", ["-c", "printf changed > /workspace/a.txt; cat /workspace/a.txt"]);
	assert.equal(overwrite.stdout, "changed");
	assert.equal(seed["/workspace/a.txt"].toString(), "before\nneedle\nafter\n");
	for (const command of ["cat /etc/hostname", "cat /dev/host", "cat /dev/hostreq", "curl https://example.com"]) {
		assert.notEqual((await run("sh", ["-c", command])).exitCode, 0, `${command}: unexpected host capability`);
	}
	// Deterministically reproduce identical input bytes acquiring different observable timestamps.
	const metadata = [];
	for (const now of [1_000_000, 2_000_000]) {
		const original = Date.now;
		let image;
		try { Date.now = () => now; image = memoryFs(seed); } finally { Date.now = original; }
		metadata.push((await run("sh", ["-c", "stat -c %Y /workspace/a.txt"], image)).stdout);
	}
	assert.deepEqual(metadata, ["1000\n", "2000\n"]);
	assert.ok(pipeline.clocks > 0 && search.clocks > 0 && search.random > 0);
	const pi = process.argv.includes("--pi-tools") ? await qualifyPiSearch(run, memoryFs, engines) : undefined;
	console.log(JSON.stringify({ platform: process.platform, node: process.version, engines,
		assertions: { sharedFilesystem: true, copyOnWrite: true, noGrantedHostPorts: true, moduleMemoryCapMiB: 64 },
		pipeline, search: { ...search, stdout: undefined, matches }, metadata, pi,
		admission: "not production-qualified: bounded full-tool fixtures do not prove general imports, quotas, cancellation or native equivalence" }));
}

/** Fixed full-tool qualification in the supervised child. Never installs hooks in the Pi Actor. */
async function qualifyPiSearch(run, memoryFs, engines) {
	const preparing = performance.now();
	const { createGrepToolDefinition: nativeGrep, VERSION } = await import("@earendil-works/pi-coding-agent");
	assert.equal(VERSION, "0.84.1", "Requalify the stock tool import and operation boundary");
	const { ActionSemanticsRegistry, PI_ACTION_SEMANTICS } = await import("../dist/action-semantics.js");
	const { RESOURCE_OBSERVATION_EFFECTS } = await import("../dist/effect-model.js");
	const { createResourceSnapshotExecutionWorld } = await import("../dist/agent-execution-world.js");
	const { ToolExecutionGateway } = await import("../dist/tool-execution-gateway.js");
	const profile = { id: "qualification.closed-grep.v1", pi: VERSION, rg: engines.rg.sha256,
		runtime: "wasi-sh@0.11.0", platform: process.platform, node: process.version, environment,
		filesystem: "complete-workspace-image; virtual metadata; no ambient home/config", dateMs: 1_700_000_000_000, seed: "qualification" };
	const fingerprint = createHash("sha256").update(JSON.stringify(profile)).digest("hex");
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-portable-profile-")), current = new AsyncLocalStorage();
	const virtual = (target) => {
		const relative = path.relative(root, target);
		assert.ok(!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`), "unproven path");
		return path.posix.join("/workspace", relative.split(path.sep).join("/"));
	};
	const logical = (target) => path.resolve(root, path.posix.relative("/workspace", target));
	const entry = new URL("./core/tools/grep.js?closed-profile", import.meta.resolve("@earendil-works/pi-coding-agent")).href;
	globalThis.__piQualificationSpawn = (command, input) => {
		assert.equal(command, "qualified:rg");
		const child = new EventEmitter(), state = current.getStore();
		child.stdout = new PassThrough(); child.stderr = new PassThrough(); child.killed = false;
		child.kill = () => { child.killed = true; return true; };
		Promise.resolve().then(async () => {
			try {
				const args = [...input]; args[args.length - 1] = virtual(args.at(-1));
				const result = await run("rg", args, state.store, profile);
				await state.afterSearch?.(); // Deterministic fault boundary before Pi's own context reread.
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
	const hooks = registerHooks({ resolve(specifier, context, next) {
		if (context.parentURL === entry && ["node:child_process", "child_process"].includes(specifier))
			return { url: "data:text/javascript,export const spawn = globalThis.__piQualificationSpawn", shortCircuit: true };
		if (context.parentURL === entry && specifier.endsWith("/tools-manager.js"))
			return { url: 'data:text/javascript,export const ensureTool = async () => "qualified:rg"', shortCircuit: true };
		return next(specifier, context);
	} });
	const { createGrepToolDefinition } = await import(entry);
	const tool = createGrepToolDefinition(root, { operations: {
		isDirectory: async (target) => (current.getStore().store.statSync(virtual(target)).mode & 0o170000) === 0o040000,
		readFile: async (target) => {
			const state = current.getStore(), file = virtual(target);
			state.contextReads++;
			const bytes = Buffer.alloc(state.store.statSync(file).size);
			state.store.readSync(file, bytes, 0, bytes.length);
			return bytes.toString("utf8");
		},
	} });
	const collect = async (source, target = root, image = { files: {}, directories: [] }) => {
		if ((await source.stat(target)).isDirectory()) {
			image.directories.push(virtual(target));
			for (const name of await source.readdir(target)) await collect(source, path.join(target, name), image);
		} else image.files[virtual(target)] = await source.readFile(target);
		return image;
	};
	let executions = 0, contextReads = 0, afterSearch;
	const execute = async (image, request) => {
		executions++;
		const store = memoryFs(image.files);
		for (const directory of image.directories) {
			try { store.statSync(directory); } catch (error) { assert.equal(error.code, "ENOENT"); store.mkdirSync(directory); }
		}
		const state = { store, contextReads: 0, afterSearch };
		const result = await current.run(state, () => tool.execute(request.callID, request.args, request.signal));
		contextReads += state.contextReads;
		return { result, isError: false };
	};
	const definition = { ...PI_ACTION_SEMANTICS.definition("grep"), epoch: profile.id,
		effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "tree_content" };
	const semantics = new ActionSemanticsRegistry([definition]);
	const invocation = { executor: profile.id, identity: profile, filesystem: async (view, request) => execute(await collect(view), request) };
	const world = createResourceSnapshotExecutionWorld(semantics, { tools: ["grep"], maxBytes: () => 8 * 1024 * 1024 });
	const gateway = new ToolExecutionGateway([world]), signal = new AbortController().signal, cases = [];
	const args = { pattern: "needle", path: ".", context: 1, limit: 1000 }, clock = Date.now;
	const profileSetupMs = performance.now() - preparing;
	try {
		await fs.mkdir(path.join(root, ".git")); await fs.mkdir(path.join(root, "empty"));
		await fs.writeFile(path.join(root, ".git/HEAD"), "ref: refs/heads/main\n");
		await fs.writeFile(path.join(root, ".gitignore"), "ignored.txt\n");
		await fs.writeFile(path.join(root, "ignored.txt"), "needle ignored\n");
		await fs.writeFile(path.join(root, "notes.txt"), "before\nneedle\nafter\n");
		for (let index = 0; index < 16; index++) await fs.writeFile(path.join(root, `data-${index}.txt`), "no match\n".repeat(8192) + `needle ${index}\n`);
		// Explicit virtual time is local to this supervised child; the default Actor is unchanged.
		Date.now = () => profile.dateMs;
		const sample = async (execute) => {
			const times = []; let output;
			for (let index = 0; index < 3; index++) { const started = performance.now(); output = await execute(); times.push(performance.now() - started); }
			return { output, medianMs: times.sort((a, b) => a - b)[1] };
		};
		const baseline = await sample(async () => execute(await collect(fs), { args, callID: "profile-actor", signal }));
		const native = await sample(() => nativeGrep(root).execute("native", args, signal));
		const expected = baseline.output;
		const action = semantics.buildKey("grep", args, root, "", { fingerprint, context: invocation });
		const operation = { tool: "grep", input: args, action, callID: "profile-speculation", signal };
		const route = await gateway.resolve({ operation, ...definition }, { cwd: root }); assert.ok(route);
		const started = performance.now();
		const branch = await gateway.executeSpeculative(operation, route, () => ({ cwd: root, tool, toolName: "grep", args, action, callID: operation.callID, signal }));
		const speculativeMs = performance.now() - started, beforeHit = executions;
		try {
			const arrived = performance.now(); assert.equal((await branch.validate()).status, "valid");
			assert.deepEqual(await branch.commit(), expected);
			const readyAdoptionMs = performance.now() - arrived;
			assert.equal(executions, beforeHit, "adoption executed the search again");
			for (const query of [{ ...args, pattern: "after", limit: 1 }, { ...args, pattern: "absent" }, { ...args, glob: "*.txt", context: 0 }]) {
				const actor = await execute(await collect(fs), { args: query, callID: "other-query", signal });
				const key = semantics.buildKey("grep", query, root, "", { fingerprint, context: invocation });
				assert.equal((await branch.validate()).status, "valid");
				assert.deepEqual(await branch.reconstruct({ action: key, args: query, callID: "retained-inputs", signal }), actor);
				assert.equal((await branch.validate()).status, "valid"); cases.push(query);
			}
			await fs.writeFile(path.join(root, "notes.txt"), "changed\n");
			assert.equal((await branch.validate()).status, "stale");
			let fallbacks = 0;
			const stale = await gateway.executeAuthoritative(operation, async () => {
				fallbacks++; return execute(await collect(fs), { args, callID: "stale-actor", signal });
			}, { reuse: async () => (await branch.validate()).status === "valid" ? branch.commit() : undefined });
			assert.equal(fallbacks, 1); assert.notDeepEqual(stale, expected);
			assert.equal(await fs.readFile(path.join(root, "notes.txt"), "utf8"), "changed\n");
			afterSearch = () => fs.writeFile(path.join(root, "notes.txt"), "changed during search\n");
			await assert.rejects(gateway.executeSpeculative(operation, route, () => ({ cwd: root, tool, toolName: "grep", args, action, callID: "changing", signal })), /resource_observation_window_changed/);
			afterSearch = undefined;
			assert.ok(contextReads > 0, "the original Pi context reread was not exercised");
			return { profile, profileSetupMs, nativeActorMs: native.medianMs, profileWarmActorMs: baseline.medianMs,
				speculativeMs, readyAdoptionMs, retainedInputQueries: cases.length, staleActorExecutions: fallbacks,
				contextReads, freshAdoption: true, changedInputRejected: true, changedDuringSearchRejected: true,
				scope: "explicit common-profile qualification; not native equivalence, scheduler/IPC/cancellation qualification, or production enablement" };
		} finally { await branch.dispose(); }
	} finally {
		Date.now = clock; hooks.deregister(); delete globalThis.__piQualificationSpawn;
		await gateway.dispose(); assert.equal(path.dirname(root), path.resolve(os.tmpdir())); await fs.rm(root, { recursive: true, force: true });
	}
}
