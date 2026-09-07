import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { brotliDecompressSync } from "node:zlib";

// Fixed-command qualification, not a plugin provider or an arbitrary-script sandbox.
// Dependencies are explicitly installed outside this package; never downloaded by this script.
const dependencyRoot = process.argv[2];
assert.ok(dependencyRoot, "Usage: node bench/portable-kernel.mjs <directory containing node_modules>");
if (process.argv[3] !== "--child") {
	const started = performance.now();
	const child = spawnSync(process.execPath, ["--wasm-max-mem-pages=1024", "--max-old-space-size=128",
		fileURLToPath(import.meta.url), path.resolve(dependencyRoot), "--child"], {
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
	const run = async (name, args, filesystem = store) => {
		const started = performance.now(), stdout = [], stderr = [], calls = {};
		const shim = new WasiShim({ args: [name, ...args], fs: filesystem,
			env: { PWD: "/workspace", HOME: "/workspace", LC_ALL: "C" },
			stdout: (bytes) => stdout.push(Buffer.from(bytes)), stderr: (bytes) => stderr.push(Buffer.from(bytes)),
			input: { read: () => new Uint8Array(), pollReadable: () => true, closed: () => true } });
		const imports = shim.imports();
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
	console.log(JSON.stringify({ platform: process.platform, node: process.version, engines,
		assertions: { sharedFilesystem: true, copyOnWrite: true, noGrantedHostPorts: true, moduleMemoryCapMiB: 64 },
		pipeline, search: { ...search, stdout: undefined, matches }, metadata,
		admission: "not qualified: full tool contract, deterministic evidence, complete host-memory quotas and adoption remain unproven" }));
}
