import { execFile, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import * as evidence from "../src/filesystem-evidence.ts";
import { copyFile, lstat, mkdir, readFile, readlink, symlink, writeFile } from "node:fs/promises";
import { connect } from "node:net";
import os from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";
import { afterEach, expect, test, vi } from "vitest";
import { Type } from "typebox";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import { parseJsonConfig } from "../examples/config-parsing.ts";
import { bindResourceOperation } from "../src/agent-execution-world.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { createActorForkPlanSource } from "../src/self-speculation.ts";
import { digestObject } from "../src/primitives.ts";
import { toolUse } from "./model.ts";
import { subcomputeEnvironment } from "../src/index.ts";
import { SpeculativeConfiguration } from "../src/config-schema.ts";
import { ContentChunkPool } from "../src/content-chunks.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { compilerBootstrap, PythonImportStep } from "../src/python-compilation.ts";
import { controlledRuntimeEnvironment, controlledSubcomputeChannel, SubcomputeService } from "../src/subcompute.ts";
import { systemHost } from "./agent-system-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-subcompute-boundary-");
afterEach(directories.dispose);

test("disconnecting during computation releases the controlled channel busy count", async () => {
	const cwd = await directories.create();
	const tool = {
		name: "abandoned_step",
		label: "Abandoned step",
		description: "Keep the context after an abandoned client",
		parameters: Type.Object({}),
		execute: async () => {
			const environment = await subcomputeEnvironment();
			const channel = controlledSubcomputeChannel(environment)!;
			const peer = client(environment);
			peer.send({
				op: "compute",
				context: environment.PI_SUBCOMPUTE_CONTEXT,
				namespace: "abandoned",
				key: "work",
			});
			try {
				expect(await peer.read()).toMatchObject({ state: "miss" });
				await expect(channel.validate()).rejects.toThrow("subcompute_process_inputs_unproven");
			} finally {
				peer.close();
			}
			await expect
				.poll(() =>
					channel.validate().then(
						() => true,
						() => false,
					),
				)
				.toBe(true);
			return { content: [], details: { validated: true } };
		},
	};
	const host = systemHost(
		cwd,
		[tool],
		{ executionWorlds: [], resolveInvocation: () => undefined },
		{ enabled: false },
	);
	try {
		await host.start();
		expect((await host.execute(tool.name, {})).details).toEqual({ validated: true });
	} finally {
		await host.dispose();
	}
});

test("one protocol connection reuses steps, revalidates snapshots and omits large input content by default", async () => {
	const cwd = await directories.create();
	const target = path.join(cwd, "large.bin");
	await writeFile(target, Buffer.alloc(1024 * 1024, 65));
	const tool = {
		name: "sequential_steps",
		label: "Sequential steps",
		description: "Read several steps on one connection",
		parameters: Type.Object({}),
		execute: async () => {
			const environment = await subcomputeEnvironment();
			const peer = client(environment);
			const compute = async (key: string, includeInputContent?: boolean) => {
				peer.send({
					op: "compute",
					context: environment.PI_SUBCOMPUTE_CONTEXT,
					namespace: "sequential",
					key,
					includeInputContent,
					inputResources: [{ path: "large.bin", scope: "content" }],
				});
				const initial = await peer.read();
				if (initial.state === "miss") {
					peer.send({ op: "publish", token: initial.token, durationMs: 1, value: { encoding: "json", value: key } });
					expect(await peer.read()).toMatchObject({ state: "published", value: { value: key } });
				}
				return initial;
			};
			try {
				const first = await compute("large");
				expect(first).toMatchObject({ state: "miss", inputResources: [{ path: target, size: 1024 * 1024 }] });
				expect(first.inputResources[0]).not.toHaveProperty("content");
				expect(Buffer.byteLength(JSON.stringify(first))).toBeLessThan(1024);
				for (let step = 0; step < 4; step++) expect(await compute("large")).toMatchObject({ state: "hit" });
				await writeFile(target, "updated");
				expect(await compute("large")).toMatchObject({ state: "miss", inputResources: [{ size: 7 }] });
				expect(await compute("bytes", true)).toMatchObject({
					state: "miss",
					inputResources: [
						{ content: { encoding: "base64", value: Buffer.from("updated").toString("base64") } },
					],
				});
				expect(await compute("bytes", true)).toMatchObject({ state: "hit" });
				const other = await subcomputeEnvironment();
				peer.send({ op: "compute", context: other.PI_SUBCOMPUTE_CONTEXT });
				expect(await peer.read()).toMatchObject({ state: "error", error: "subcompute_context_changed" });
				return { content: [], details: { checked: true } };
			} finally {
				peer.close();
			}
		},
	};
	const host = systemHost(
		cwd,
		[tool],
		{
			executionWorlds: [],
			resolveInvocation: () => undefined,
			config: { subcomputation: { socketRequestBytes: 2048 } },
		},
		{ enabled: false },
	);
	try {
		await host.start();
		expect((await host.execute(tool.name, {})).details).toEqual({ checked: true });
	} finally {
		await host.dispose();
	}
}, 30000);

test("JavaScript and Python observe a discovered include, share the result and invalidate on edit", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const service = new SubcomputeService(cwd, store, () => 1024 * 1024);
	const header = path.join(cwd, "header.inc");
	await writeFile(path.join(cwd, "input.bin"), "header.inc");
	await writeFile(header, "one");
	try {
		const env = { ...process.env, ...(await service.environment()) };
		const run = async (language: "py" | "mjs") => {
			const script = fileURLToPath(new URL("./subcompute-client." + language, import.meta.url));
			const exe = language === "mjs" ? process.execPath : process.platform === "win32" ? "python" : "python3";
			const output = await promisify(execFile)(exe, [script, "include", "--include"], { cwd, env });
			const { hit, value } = JSON.parse(output.stdout);
			return { hit, text: Buffer.from(value.value, "base64").toString() };
		};
		expect(await run("mjs")).toEqual({ hit: false, text: "one" });
		expect(await run("py")).toEqual({ hit: true, text: "one" });
		await writeFile(header, "two");
		expect(await run("py")).toEqual({ hit: false, text: "two" });
		expect(await run("mjs")).toEqual({ hit: true, text: "two" });
		const steps = service.drainMetrics()["cross-language.reverse"]!;
		expect(steps).toMatchObject({ hits: 2, misses: 2 });
		expect(steps.reusedMs).toBeGreaterThan(0);
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test("publish validates trailing, repeated and out-of-workspace observations and consumes its token", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const service = new SubcomputeService(cwd, store, () => 1024 * 1024);
	const file = path.join(cwd, "dynamic");
	await writeFile(file, "A");
	const open = async (key: string) => {
		const env = await service.environment();
		const peer = client(env);
		peer.send({ op: "compute", context: env.PI_SUBCOMPUTE_CONTEXT, namespace: "frontend", key });
		const reply = await peer.read();
		return { peer, reply };
	};
	const reads = [{ path: "dynamic", scope: "content" }];
	const value = { encoding: "json", value: 3 };
	try {
		const { peer, reply } = await open("tail");
		expect(reply.state).toBe("miss");
		peer.send({ op: "publish", token: reply.token, durationMs: 2, value, reads });
		expect(await peer.read()).toMatchObject({ state: "published" });
		peer.close();
		const warm = await open("tail");
		expect(warm.reply).toMatchObject({ state: "hit", value });
		warm.peer.close();
		await writeFile(file, "B");
		const stale = await open("tail");
		expect(stale.reply.state).toBe("miss");
		stale.peer.close();
		for (const violation of ["change", "escape", "token", "publish"]) {
			const { peer, reply } = await open(violation);
			const token = reply.token;
			peer.send({ op: "observe", token, reads: [...reads, ...reads] });
			expect(await peer.read()).toEqual({ state: "observed", token });
			if (violation === "change" || violation === "publish") await writeFile(file, violation);
			if (violation === "publish") peer.send({ op: "publish", token, durationMs: 2, value });
			else peer.send({
				op: "observe", token: violation === "token" ? "other" : token,
				reads: violation === "escape" ? [{ path: "../escape", scope: "content" }] : reads,
			});
			expect(await peer.read()).toMatchObject({ state: "error" });
			peer.close();
			const retry = await open(violation);
			expect(retry.reply.state).toBe("miss");
			retry.peer.close();
		}
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test("different step keys borrow the same signed file token until a write or explicit invalidation", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const service = new SubcomputeService(cwd, store, () => 1024 * 1024);
	const target = path.join(cwd, "source");
	await writeFile(target, "initial");
	const realNow = Date.now;
	const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + 10_000);
	const pools = vi.spyOn(ContentChunkPool.prototype, "capture");
	const capture = vi.spyOn(evidence, "captureStableFile");
	const reads = () => capture.mock.calls.filter(([file]) => file === target).length;
	try {
		const env = await service.environment();
		const peer = client(env);
		try {
			for (let key = 0; key < 5; key++) {
				if (key === 3) await writeFile(target, "updated");
				if (key === 4) service.invalidate([target]);
				peer.send({
					op: "compute", context: env.PI_SUBCOMPUTE_CONTEXT, namespace: "cache", key,
					inputResources: [{ path: "source", scope: "content" }],
				});
				const reply = await peer.read();
				expect(reply.state).toBe("miss");
				peer.send({ op: "publish", token: reply.token, value: { encoding: "json", value: key }, durationMs: 1 });
				expect(await peer.read()).toMatchObject({ state: "published" });
				expect(reads()).toBe(key < 3 ? 1 : key - 1);
			}
			expect(new Set(pools.mock.contexts).size).toBe(1);
			expect(pools.mock.contexts[0]).toMatchObject({ openChunks: 2 });
		} finally {
			peer.close();
		}
	} finally {
		clock.mockRestore();
		pools.mockRestore();
		capture.mockRestore();
		await service.dispose();
		await store.dispose();
	}
});

test("a full shared chunk pool keeps outstanding snapshots readable and admits the next file", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const settings = new SpeculativeConfiguration({ filesystem: { maxOpenChunks: 2 } });
	const service = settings.run(() => new SubcomputeService(cwd, store, () => 1024 * 1024));
	await writeFile(path.join(cwd, "a"), "first");
	await writeFile(path.join(cwd, "b"), "second");
	await writeFile(path.join(cwd, "c"), "third");
	const env = await service.environment();
	const first = client(env);
	const second = client(env);
	const request = (file: string) => ({
		op: "compute", context: env.PI_SUBCOMPUTE_CONTEXT, namespace: "bounded-pool", key: file,
		inputResources: [{ path: file, scope: "content" }], includeInputContent: true,
	});
	try {
		first.send(request("a"));
		const pending = await first.read();
		expect(pending.state).toBe("miss");
		second.send(request("b"));
		const other = await second.read();
		expect(other.state).toBe("miss");
		expect(Buffer.from(other.inputResources[0].content.value, "base64").toString()).toBe("second");
		second.send({ op: "publish", token: other.token, durationMs: 1, value: { encoding: "json", value: "second" } });
		expect(await second.read()).toMatchObject({ state: "published" });
		second.send(request("c"));
		const overflow = await second.read();
		expect(overflow.state).toBe("miss");
		expect(Buffer.from(overflow.inputResources[0].content.value, "base64").toString()).toBe("third");
		first.send({ op: "publish", token: pending.token, durationMs: 1, value: { encoding: "json", value: "first" } });
		expect(await first.read()).toMatchObject({ state: "published" });
		first.send(request("a"));
		expect(await first.read()).toMatchObject({ state: "hit", value: { value: "first" } });
	} finally {
		first.close();
		second.close();
		await service.dispose();
		await store.dispose();
	}
});

test("publication rereads a same-size rewrite whose timestamps matched at capture", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const service = new SubcomputeService(cwd, store, () => 1024 * 1024);
	const target = path.join(cwd, "source");
	await writeFile(target, "before");
	const tick = await lstat(target, { bigint: true });
	// Model a coarse filesystem that reports one stat tuple for both real file images.
	const captureEntry = evidence.captureFilesystemEntry;
	const captureFile = evidence.captureStableFile;
	const entries = vi.spyOn(evidence, "captureFilesystemEntry").mockImplementation(async (...args) => {
		const entry = await captureEntry(...args);
		return args[0] === target ? { ...entry, info: tick } : entry;
	});
	const files = vi.spyOn(evidence, "captureStableFile").mockImplementation(async (file, limit, retain, observed, signal) => {
		if (file !== target) return captureFile(file, limit, retain, observed, signal);
		const capture = await captureFile(file, limit, retain, { ...observed, stat: undefined }, signal);
		return { ...capture, stat: tick };
	});
	const takenAt = Number((tick.mtimeNs > tick.ctimeNs ? tick.mtimeNs : tick.ctimeNs) / 1_000_000n);
	const clock = vi.spyOn(Date, "now").mockReturnValue(takenAt);
	const env = await service.environment();
	const open = () => {
		const peer = client(env);
		peer.send({
			op: "compute", context: env.PI_SUBCOMPUTE_CONTEXT, namespace: "coarse", key: 1,
			inputResources: [{ path: "source", scope: "content" }], includeInputContent: true,
		});
		return peer;
	};
	const first = open();
	try {
		const miss = await first.read();
		expect(miss.state).toBe("miss");
		expect(Buffer.from(miss.inputResources[0].content.value, "base64").toString()).toBe("before");
		await writeFile(target, "edited");
		clock.mockReturnValue(takenAt + 10_000);
		first.send({ op: "publish", token: miss.token, durationMs: 1, value: { encoding: "json", value: "before" } });
		expect(await first.read()).toMatchObject({ state: "error", error: "subcompute_inputs_changed" });
		const next = open();
		try {
			const retry = await next.read();
			expect(retry.state).toBe("miss");
			expect(Buffer.from(retry.inputResources[0].content.value, "base64").toString()).toBe("edited");
		} finally {
			next.close();
		}
	} finally {
		first.close();
		await service.dispose();
		await store.dispose();
		clock.mockRestore();
		files.mockRestore();
		entries.mockRestore();
	}
});

test("shutdown prevents late channel creation and concurrent callers share its completion", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(cwd);
	const service = new SubcomputeService(cwd, store, () => 1024);
	try {
		const socket = await service.socket();
		expect(socket).toBeTruthy();
		const channel = service.environment();
		const closing = service.dispose();
		expect(service.dispose()).toBe(closing);
		await expect(channel).rejects.toThrow("subcompute_service_closed");
		await closing;
		await expect(service.socket()).rejects.toThrow("subcompute_service_closed");
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test.skipIf(process.platform !== "linux")("controlled imports use host-owned steps and reject untrusted interpreters before requesting bytes", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const service = new SubcomputeService(cwd, store, () => 1024 * 1024, [new PythonImportStep(cwd)]);
	try {
		await service.run(async () => {
			const env = await controlledRuntimeEnvironment({});
			const peer = client(env);
			try {
				const archive = (await compilerBootstrap())!;
				expect(controlledSubcomputeChannel(env)?.readOnlyDirectories).toEqual([path.dirname(archive)]);
				const main = "import sys,zipimport;exec(zipimport.zipimporter(sys.argv[1]).get_code('sitecustomize'))";
				const engine = JSON.parse(execFileSync("python3", ["-I", "-S", "-B", "-c", main, archive], { input: "", encoding: "utf8" }));
				const executable = execFileSync("python3", ["-c", "import os,sys; print(os.path.realpath(sys.executable))"], { encoding: "utf8" }).trim();
				const key = { engine, executable, filename: path.join(cwd, "x.py"), optimize: 0 };
				const namespace = "python.source_to_code";
				const source = Buffer.from("output = 42\n");
				const compile = (options = key) => trustedCompute(peer, env, namespace, options, source);
				const first = await compile();
				expect(first.state).toBe("published");
				expect(first.value.encoding).toBe("base64");
				const other = path.join(cwd, "python3");
				await copyFile(executable, other);
				expect(await compile({ ...key, executable: other })).toEqual({ state: "unavailable" });
				expect(await compile({ ...key, engine: "0".repeat(64) })).toEqual({ state: "unavailable" });
				expect(await compile()).toMatchObject({ state: "hit", value: first.value });
				peer.send({ op: "compute", namespace: "other", context: env.PI_SUBCOMPUTE_CONTEXT, key: null });
				expect(await peer.read()).toMatchObject({ state: "error" });
				expect(service.drainMetrics()[namespace]).toMatchObject({ hits: 1, misses: 1 });
			} finally { peer.close(); }
		});
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test.skipIf(process.platform !== "linux")("concurrent venv imports share one connection per process and reopen it after fork", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const service = new SubcomputeService(cwd, store, () => 1024 * 1024, [new PythonImportStep(cwd)]);
	try {
		const hostPython = execFileSync("python3", ["-c", "import sys; print(sys.executable)"], { encoding: "utf8" }).trim();
		const venvBin = path.join(cwd, ".venv", "bin");
		await mkdir(venvBin, { recursive: true });
		const venvPython = path.join(venvBin, "python3");
		await symlink(hostPython, venvPython);
		for (const [index, name] of ["mod_a", "mod_b", "mod_c", "mod_d"].entries())
			await writeFile(path.join(cwd, `${name}.py`), `value = ${(index + 1) * 10}\n`);

		let connections = 0;
		await service.socket();
		(service as unknown as { server: import("node:net").Server }).server.on("connection", () => {
			connections++;
		});

		await service.run(async () => {
			const env = await controlledRuntimeEnvironment({});
			const program = `import _thread, os, sys, mod_a
names = ("mod_b", "mod_c", "mod_d")
locks = [_thread.allocate_lock() for _ in names]
gate = _thread.allocate_lock()
values = [0] * len(names)
def load(i):
    try:
        with gate:
            pass
        values[i] = __import__(names[i]).value
    finally:
        locks[i].release()
with gate:
    for i, lock in enumerate(locks):
        lock.acquire()
        _thread.start_new_thread(load, (i,))
for lock in locks:
    lock.acquire()
if len(sys.argv) > 1:
    pid = os.fork()
    if pid == 0:
        import mod_child
        sys.exit(0 if mod_child.value == 50 else 1)
    assert os.waitpid(pid, 0)[1] == 0
print(sum(values) + mod_a.value)`;
			const runVenv = async (...args: string[]) => {
				const { stdout, stderr } = await promisify(execFile)(venvPython, ["-B", "-W", "error", "-c", program, ...args], {
					cwd, encoding: "utf8", timeout: 10000, env: { PATH: "/usr/bin:/bin", HOME: os.homedir(), ...env },
				});
				expect(stderr).toBe("");
				return stdout.trim();
			};

			connections = 0;
			expect(await runVenv()).toBe("100");
			expect(connections).toBe(1);
			expect(service.drainMetrics()["python.source_to_code"]).toMatchObject({ hits: 0, misses: 4 });

			connections = 0;
			expect(await runVenv()).toBe("100");
			expect(connections).toBe(1);
			expect(service.drainMetrics()["python.source_to_code"]).toMatchObject({ hits: 4, misses: 0 });

			await writeFile(path.join(cwd, "mod_child.py"), "value = 50\n");
			connections = 0;
			expect(await runVenv("fork")).toBe("100");
			expect(connections).toBe(2);
			expect(service.drainMetrics()["python.source_to_code"]).toMatchObject({ hits: 4, misses: 1 });

		});
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test.skipIf(process.platform !== "linux")("four compiler slots keep active imports pinned across interpreter changes", async () => {
	const installations = await directories.create(os.homedir());
	const cwd = await directories.create();
	vi.stubEnv("UV_PYTHON_INSTALL_DIR", installations);
	const compiler = new PythonImportStep(cwd);
	const source = Buffer.from("value = 10\n");
	const gates = Array.from({ length: 4 }, () => Promise.withResolvers<void>());
	const active = gates.map(() => Promise.withResolvers<void>());
	try {
		const bootstrap = (await compilerBootstrap())!;
		const main = "import sys,zipimport;exec(zipimport.zipimporter(sys.argv[1]).get_code('sitecustomize'))";
		const requests = await Promise.all(Array.from({ length: 5 }, async (_, index) => {
			const bin = path.join(installations, String(index), "bin");
			await mkdir(bin, { recursive: true });
			const executable = path.join(bin, "python3");
			await copyFile("/usr/bin/python3", executable);
			const engine = JSON.parse(execFileSync(executable, ["-I", "-S", "-B", "-c", main, bootstrap], { input: "", encoding: "utf8" }));
			return { executable, engine, optimize: 0, filename: path.join(cwd, "x.py") };
		}));
		const get = (i: number) => compiler.build({ key: requests[i], maxResultBytes: 65536, signal: new AbortController().signal, input: async () => {
			if (i < gates.length) {
				active[i]!.resolve();
				await gates[i]!.promise;
			}
			const ids = (await readFile(`/proc/self/task/${process.pid}/children`, "utf8")).trim().split(/\s+/);
			const images = await Promise.all(ids.map((id) => readlink(`/proc/${id}/exe`).catch(() => "")));
			expect(images.filter((file) => file.startsWith(installations + "/")).length).toBeLessThanOrEqual(4);
			return source;
		} });
		const first = gates.map((_, i) => get(i));
		await Promise.all(active.map((item) => item.promise));
		const fifth = get(4);
		gates[3]!.resolve();
		expect(await fifth).toBeInstanceOf(Buffer);
		for (const gate of gates) gate.resolve();
		for (const result of await Promise.all(first)) expect(result).toBeInstanceOf(Buffer);
	} finally {
		for (const gate of gates) gate.resolve();
		await compiler.dispose();
		vi.unstubAllEnvs();
	}
});

function client(environment: Readonly<Record<string, string>>) {
	const socket = connect(environment.PI_SUBCOMPUTE_SOCKET!);
	socket.on("error", () => {});
	socket.setTimeout(3000, () => socket.destroy());
	const lines = createInterface({ input: socket });
	const iterator = lines[Symbol.asyncIterator]();
	return {
		socket,
		send: (value: unknown) => socket.write(JSON.stringify(value) + "\n"),
		read: async () => {
			const next = await iterator.next();
			return next.done ? undefined : JSON.parse(next.value);
		},
		close: () => {
			socket.destroy();
			lines.close();
		},
	};
}

async function trustedCompute(peer: ReturnType<typeof client>, environment: Readonly<Record<string, string>>, namespace: string, key: unknown, bytes: Buffer) {
	peer.send({ op: "compute", namespace, key, context: environment.PI_SUBCOMPUTE_CONTEXT, inputDigest: createHash("sha256").update(bytes).digest("hex") });
	let reply = await peer.read();
	if (reply.state === "input") {
		peer.send({ op: "input", token: reply.token, value: { encoding: "base64", value: bytes.toString("base64") } });
		reply = await peer.read();
	}
	if (reply.state === "hit" || reply.state === "published") {
		peer.send({ op: "consumed", token: reply.token });
		expect(await peer.read()).toEqual({ state: "ready" });
	}
	return reply;
}

test("trusted configuration parsing publishes once, survives restart and isolates host evidence from client publication", async () => {
	const cwd = await directories.create();
	const storage = await directories.create();
	const bytes = Buffer.from('{"target":"library"}');
	const namespace = parseJsonConfig.namespace;
	const digest = createHash("sha256").update(bytes).digest("hex");
	const build = vi.fn(parseJsonConfig.build);
	let store = new ProvenanceCertificateStore(storage);
	const open = (implementation: string) => new SubcomputeService(cwd, store, () => 1024 * 1024, [{ namespace, implementation, build }]);
	let service = new SubcomputeService(cwd, store, () => 1024 * 1024);
	const peerFor = async () => {
		const env = await service.environment();
		return { env, peer: client(env) };
	};
	try {
		const key = { format: "json" };
		const publicClient = await peerFor();
		publicClient.peer.send({ op: "compute", context: publicClient.env.PI_SUBCOMPUTE_CONTEXT, namespace,
			key: digestObject([namespace, parseJsonConfig.implementation, key, digest]) });
		const miss = await publicClient.peer.read();
		publicClient.peer.send({ op: "publish", token: miss.token, durationMs: 1, value: { encoding: "json", value: "unverified" } });
		expect((await publicClient.peer.read()).state).toBe("published");
		publicClient.peer.close();
		await service.dispose();
		service = open(parseJsonConfig.implementation);
		const packet = { op: "compute", namespace, key, inputDigest: digest };
		for (const mode of ["wrong bytes", "client publication", "disconnected"] as const) {
			const { peer, env } = await peerFor();
			peer.send({ ...packet, context: env.PI_SUBCOMPUTE_CONTEXT });
			const prompt = await peer.read();
			expect(prompt.state).toBe("input");
			if (mode !== "disconnected") {
				peer.send({ op: mode === "wrong bytes" ? "input" : "publish", token: prompt.token,
					value: { encoding: "base64", value: Buffer.from("different").toString("base64") } });
				expect((await peer.read()).state).toBe("error");
			}
			peer.close();
		}
		build.mockClear();
		const use = async (payload = bytes) => {
			const { env, peer } = await peerFor();
			try { return await trustedCompute(peer, env, namespace, key, payload); }
			finally { peer.close(); }
		};
		const replies = await Promise.all([use(), use()]);
		expect(replies.map((reply) => reply.state).sort()).toEqual(["hit", "published"]);
		for (const reply of replies) expect(reply.value).toEqual({ encoding: "json", value: { target: "library" } });
		expect(build).toHaveBeenCalledTimes(1);
		const offered = await peerFor();
		offered.peer.send({ ...packet, context: offered.env.PI_SUBCOMPUTE_CONTEXT });
		expect((await offered.peer.read()).state).toBe("hit");
		offered.peer.close();
		expect(service.drainMetrics()[namespace]).toMatchObject({ misses: 1, hits: 1 });
		await service.dispose();
		await store.dispose();
		store = new ProvenanceCertificateStore(storage);
		service = open(parseJsonConfig.implementation);
		expect((await use()).state).toBe("hit");
		expect((await use(Buffer.from('{"target":"application"}')))).toMatchObject({ state: "published", value: { value: { target: "application" } } });
		await service.dispose();
		service = open("JSON with a different decoder");
		expect((await use()).state).toBe("published");
		expect(build).toHaveBeenCalledTimes(3);
		// JSON.parse preserves negative zero, which a JSON response frame would turn into zero.
		expect((await use(Buffer.from('{"weight":-0}'))).state).toBe("local");
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test("one connection can repeatedly consume a large trusted result within one snapshot budget", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const step = { namespace: "bytes.copy", implementation: "identity", build: vi.fn(({ input }: import("../src/subcompute.ts").TrustedStepBuild) => input()) };
	const service = new SubcomputeService(cwd, store, () => 64 * 1024, [step]);
	const env = await service.environment();
	const peer = client(env);
	try {
		const payload = Buffer.alloc(12 * 1024, 42);
		for (let i = 0; i < 16; i++) {
			const reply = await trustedCompute(peer, env, step.namespace, null, payload);
			expect(reply.state, JSON.stringify(reply.error)).toBe(i ? "hit" : "published");
		}
		expect(step.build).toHaveBeenCalledTimes(1);
	} finally {
		peer.close();
		await service.dispose();
		await store.dispose();
	}
});

test("large prepared values restore from verified CAS blocks and rebuild a corrupt block", async () => {
	const cwd = await directories.create();
	const storage = await directories.create();
	const config = new SpeculativeConfiguration({ storage: { maxRecordBytes: 2048 } });
	const store = config.run(() => new ProvenanceCertificateStore(storage));
	const publish = vi.spyOn(store, "putInputVersion");
	const build = vi.fn(async ({ input }: import("../src/subcompute.ts").TrustedStepBuild) => input());
	const step = { namespace: "bytes.copy", implementation: "identity", build };
	const open = () => config.run(() => new SubcomputeService(cwd, store, () => 65536, [step]));
	const bytes = Buffer.alloc(8192, 42);
	let service = open();
	const use = async () => {
		const env = await service.environment();
		const peer = client(env);
		try {
			const reply = await trustedCompute(peer, env, step.namespace, null, bytes);
			expect(reply.value).toEqual({ encoding: "base64", value: bytes.toString("base64") });
			return reply.state;
		} finally { peer.close(); }
	};
	try {
		expect(await use()).toBe("published");
		const record = await publish.mock.results[0]!.value;
		expect(record.artifacts).toHaveLength(1);
		await service.dispose();
		service = open();
		expect(await use()).toBe("hit");
		await service.dispose();
		const ref = record.artifacts[0]!;
		await writeFile(store.artifacts.artifactPath(ref.digest), Buffer.alloc(ref.size));
		service = open();
		expect(await use()).toBe("published");
		expect(build).toHaveBeenCalledTimes(2);
	} finally {
		await service.dispose();
		await store.dispose();
	}
});

test("consumed preparations survive a cold scan and evicted results return from the shared store", async () => {
	const cwd = await directories.create();
	const store = new ProvenanceCertificateStore(await directories.create());
	const lookup = vi.spyOn(store, "findPreparedInputCandidates");
	const build = vi.fn(parseJsonConfig.build);
	const service = new SubcomputeService(cwd, store, () => 65536, [{ ...parseJsonConfig, build }]);
	service.runtime = () => ({ resourceCacheMaxEntries: 2, resourceCacheMaxBytes: 65536 });
	const env = await service.environment();
	const peer = client(env);
	const use = (name: string) => trustedCompute(peer, env, parseJsonConfig.namespace, null, Buffer.from(JSON.stringify(name)));
	try {
		expect((await use("warm")).state).toBe("published");
		expect((await use("warm")).state).toBe("hit");
		expect((await use("cold")).state).toBe("published");
		expect((await use("another")).state).toBe("published");
		lookup.mockClear();
		expect((await use("warm")).state).toBe("hit");
		expect(lookup).not.toHaveBeenCalled();
		expect((await use("cold")).state).toBe("hit");
		expect(lookup).toHaveBeenCalledTimes(1);
		expect(build).toHaveBeenCalledTimes(3);
	} finally {
		peer.close();
		await service.dispose();
		await store.dispose();
	}
});

test.each(["drafter", "pattern_aware", "self-speculation"])("%s and the Actor consume each other's parsed configuration", async (source) => {
	const cwd = await directories.create();
	const storage = await directories.create();
	for (const [file, names] of Object.entries({ "warm.json": ["a"], "producer.json": ["a", "b"], "consumer.json": ["b"] }))
		await writeFile(path.join(cwd, file), JSON.stringify(names));
	const tool = createReadTool(cwd);
	const seen = new Map<string, string[]>();
	const build = vi.fn(parseJsonConfig.build);
	const operation = bindResourceOperation(async (fs, request) => {
		const { path: file } = request.args as { path: string };
		const names: string[] = JSON.parse((await fs.readFile(path.resolve(cwd, file))).toString());
		const env = await subcomputeEnvironment();
		const peer = client(env);
		try {
			const states: string[] = [];
			const values = [];
			for (const name of names) {
				const bytes = Buffer.from(JSON.stringify({ name }));
				const reply = await trustedCompute(peer, env, parseJsonConfig.namespace, null, bytes);
				states.push(reply.state);
				values.push(reply.value.value.name);
			}
			seen.set(file, states);
			return { isError: false, result: { content: [{ type: "text", text: values.join(",") }], details: {} } };
		} finally { peer.close(); }
	}, cwd);
	const options = { config: { storage: { root: storage } }, trustedSteps: [{ ...parseJsonConfig, build }],
		resolveInvocation: () => ({ executor: "test.config-reader", identity: "config-reader", ...operation }) };
	const warm = systemHost(cwd, [tool], options, { enabled: false });
	try {
		await warm.start();
		expect((await warm.execute("read", { path: "warm.json" })).content).toEqual([{ type: "text", text: "a" }]);
	} finally { await warm.dispose(); }
	const policy = { ...PATTERN_AWARE_DEFAULTS, enabled: source === "pattern_aware", maxFutureGap: 0, maxContextLength: 1, multiStepEnabled: false };
	const pattern = new PatternAwareStore(policy);
	if (policy.enabled) {
		for (let i = 0; i < 8; i++) {
			const fact = { sessionID: String(i), turnID: "train", durationMs: 1, outcome: "success" as const };
			pattern.observe({ ...fact, tool: "catalog", input: {}, outputPaths: [i + ".json"] });
			pattern.observe({ ...fact, tool: "read", input: { path: i + ".json" } });
			pattern.finishSession(fact.sessionID);
		}
		pattern.observe({ sessionID: "live", turnID: "previous", durationMs: 1, outcome: "success", tool: "catalog", input: {}, outputPaths: ["producer.json"] });
	}
	const fork = createActorForkPlanSource();
	const self = source === "self-speculation";
	const h = systemHost(cwd, [tool], { ...options, patternStore: pattern, actorForkPlanSource: self ? fork : undefined,
		complete: async () => toolUse("read", { path: "producer.json" }) }, {
		drafterEnabled: source === "drafter", drafterMaxDepth: 0, patternAware: policy,
		selfSpeculation: { enabled: self, forkEnabled: self, forkActionEnabled: self, forkTransport: "sidecar" },
	}, "live");
	try {
		if (self) fork.startTurn("first");
		await h.start();
		if (self) fork.publish("first", [{ id: "fork", calls: [{ id: "config", index: 0, tool: "read", input: { path: "producer.json" } }] }]);
		await h.ready(["producer.json"], source);
		expect((await h.execute("read", { path: "consumer.json" })).content).toEqual([{ type: "text", text: "b" }]);
		expect(Object.fromEntries(seen)).toEqual({ "warm.json": ["published"], "producer.json": ["hit", "published"], "consumer.json": ["hit"] });
		expect(build).toHaveBeenCalledTimes(2);
		expect(h.completed("producer.json", source)[0]?.subcomputations?.[parseJsonConfig.namespace]).toMatchObject({ hits: 1, misses: 1 });
		expect(h.actors.at(-1)?.subcomputations?.[parseJsonConfig.namespace]).toMatchObject({ hits: 1, misses: 0 });
	} finally { await h.dispose(); }
});

test("public protocol rejects abandoned and oversized work and revalidates changing inputs", async () => {
	const cwd = await directories.create();
	await writeFile(path.join(cwd, "input.bin"), "before");
	let leaked: Readonly<Record<string, string>> = {};
	const tool = {
		name: "protocol_boundary",
		label: "Protocol boundary",
		description: "Exercise one explicit computation",
		parameters: Type.Object({ key: Type.String(), mode: Type.String() }),
		execute: async (_id: string, input: unknown) => {
			const { key, mode } = input as { key: string; mode: string };
			const environment = (leaked = await subcomputeEnvironment());
			const peer = client(environment);
			try {
				if (mode === "frame") {
					peer.socket.write("x".repeat(4096));
					expect(await peer.read()).toBeUndefined();
					return { content: [], details: { disconnected: true } };
				}
				peer.send({
					op: "compute",
					context: environment.PI_SUBCOMPUTE_CONTEXT,
					namespace: "boundary",
					key,
					inputResources: [{ path: "input.bin", scope: "content" }],
					includeInputContent: true,
				});
				const initial = await peer.read();
				if (initial.state === "hit") return { content: [], details: initial };
				expect(initial.state).toBe("miss");
				if (mode === "disconnect") return { content: [], details: initial };
				if (mode === "mutate") await writeFile(path.join(cwd, "input.bin"), "after");
				peer.send({
					op: "publish",
					token: initial.token,
					durationMs: 1,
					value: {
						encoding: "base64",
						value:
							mode === "oversize"
								? Buffer.alloc(512).toString("base64")
								: initial.inputResources[0].content.value,
					},
				});
				const published = await peer.read();
				return { content: [], details: { initial: initial.state, published } };
			} finally {
				peer.close();
			}
		},
	};
	const host = systemHost(
		cwd,
		[tool],
		{
			executionWorlds: [],
			resolveInvocation: () => undefined,
			config: {
				subcomputation: { maxStepResultBytes: 256, socketRequestBytes: 2048 },
			},
		},
		{ enabled: false },
	);
	try {
		await host.start();
		expect((await host.execute(tool.name, { key: "abandoned", mode: "disconnect" })).details).toMatchObject({
			state: "miss",
		});
		expect((await host.execute(tool.name, { key: "abandoned", mode: "normal" })).details).toMatchObject({
			initial: "miss",
		});
		expect((await host.execute(tool.name, { key: "abandoned", mode: "normal" })).details).toMatchObject({
			state: "hit",
		});
		expect((await host.execute(tool.name, { key: "size", mode: "oversize" })).details).toMatchObject({
			published: {
				state: "error",
				error: "compute_step_result_budget_exceeded",
			},
		});
		expect((await host.execute(tool.name, { key: "size", mode: "normal" })).details).toMatchObject({
			initial: "miss",
		});
		await host.execute(tool.name, { key: "changing", mode: "mutate" });
		expect((await host.execute(tool.name, { key: "changing", mode: "normal" })).details).toMatchObject({
			initial: "miss",
			published: { value: { value: Buffer.from("after").toString("base64") } },
		});
		const expired = client(leaked);
		try {
			expired.send({
				op: "compute",
				context: leaked.PI_SUBCOMPUTE_CONTEXT,
				namespace: "boundary",
				key: "late",
			});
			expect(await expired.read()).toMatchObject({
				state: "error",
				error: "subcompute_context_unavailable",
			});
		} finally {
			expired.close();
		}
		await host.execute(tool.name, { key: "frame", mode: "frame" });
		expect((await host.execute(tool.name, { key: "size", mode: "normal" })).details).toMatchObject({
			initial: "miss",
		});
	} finally {
		await host.dispose();
	}
}, 30000);

test("trusted steps degrade cleanly at serialization limits, accept internal store roots, and scope speculative forks", async () => {
	const cwd = await directories.create();
	const internalStore = path.join(cwd, ".pi", "store");
	const store = new ProvenanceCertificateStore(internalStore);
	const service = new SubcomputeService(
		cwd,
		store,
		() => 1024 * 1024,
		[
			{
				namespace: "boundary.step",
				implementation: "boundary-transform",
				build: async ({ key, input, maxResultBytes }) => {
					await input();
					if (key === "exact-limit") return Buffer.alloc(maxResultBytes, 7);
					if (key === "non-plain") return new Date(0);
					return { ok: true };
				},
			},
		],
	);
	service.configuration.update({
		...service.configuration.current,
		subcomputation: { ...service.configuration.current.subcomputation, maxStepResultBytes: 1024, socketTimeoutMs: 50 },
	});
	try {
		await service.run(async () => {
			const env = await controlledRuntimeEnvironment({});
			const channel = controlledSubcomputeChannel(env)!;
			const peer = client(env);
			try {
				const bytes = Buffer.from("payload");
				expect(await trustedCompute(peer, env, "boundary.step", "exact-limit", bytes)).toEqual({ state: "local" });
				expect(await trustedCompute(peer, env, "boundary.step", "non-plain", bytes)).toEqual({ state: "local" });
				expect(await trustedCompute(peer, env, "boundary.step", "ok", bytes)).toMatchObject({
					state: "published",
					value: { encoding: "json", value: { ok: true } },
				});
				await expect(channel.validate()).resolves.toBeUndefined();
			} finally {
				peer.close();
			}
		});

		// A task outliving an outer service.run receives a timeout-bounded scope instead of leaking an unowned token.
		let detachedPromise!: Promise<Readonly<Record<string, string>>>;
		await service.run(async () => {
			detachedPromise = new Promise<void>((resolve) => setTimeout(resolve, 10)).then(() => service.controlledEnvironment({}));
		});
		const detached = await detachedPromise;
		expect(controlledSubcomputeChannel(detached)).toBeDefined();
		await expect.poll(() => controlledSubcomputeChannel(detached), { timeout: 1000 }).toBeUndefined();
	} finally {
		await service.dispose();
		await store.dispose();
	}

	// Speculative forks keep their controlled channel alive after startTurn returns and release it upon settlement.
	await writeFile(path.join(cwd, "target.json"), '{"name":"spec"}');
	const readTool = createReadTool(cwd);
	let forkEnv: Readonly<Record<string, string>> = {};
	const started = Promise.withResolvers<void>();
	const releaseFork = Promise.withResolvers<void>();
	const operation = bindResourceOperation(async (fs, request) => {
		const { path: file } = request.args as { path: string };
		const raw = await fs.readFile(path.resolve(cwd, file));
		forkEnv = await controlledRuntimeEnvironment({});
		started.resolve();
		await releaseFork.promise;
		await expect(controlledSubcomputeChannel(forkEnv)?.validate()).resolves.toBeUndefined();
		const peer = client(forkEnv);
		try {
			const reply = await trustedCompute(peer, forkEnv, parseJsonConfig.namespace, null, raw);
			return { isError: false, result: { content: [{ type: "text" as const, text: reply.value.value.name }], details: {} } };
		} finally {
			peer.close();
		}
	}, cwd);
	let calls = 0;
	const host = systemHost(
		cwd,
		[readTool],
		{
			config: { storage: { root: path.join(cwd, ".pi", "host-store") } },
			trustedSteps: [parseJsonConfig],
			resolveInvocation: () => ({ executor: "test.fork-scope", identity: "fork-scope", ...operation }),
			complete: async () => {
				if (calls++ > 0) {
					await started.promise;
					return { ...toolUse("read", { path: "target.json" }), content: [] };
				}
				return toolUse("read", { path: "target.json" });
			},
		},
		{ drafterEnabled: true, drafterMaxDepth: 0, candidateLimit: 2 },
		"fork-scope",
	);
	try {
		await host.start();
		await started.promise;
		// startTurn has now returned, while the speculative fork is still awaiting releaseFork!
		await expect(controlledSubcomputeChannel(forkEnv)?.validate()).resolves.toBeUndefined();
		releaseFork.resolve();
		await host.ready(["target.json"], "drafter");
		await expect.poll(() => controlledSubcomputeChannel(forkEnv), { timeout: 1000 }).toBeUndefined();
	} finally {
		releaseFork.resolve();
		await host.dispose();
	}
});
