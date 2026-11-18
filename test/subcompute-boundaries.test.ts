import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import * as evidence from "../src/filesystem-evidence.ts";
import { lstat, writeFile } from "node:fs/promises";
import { connect } from "node:net";
import path from "node:path";
import { createInterface } from "node:readline";
import { afterEach, expect, test, vi } from "vitest";
import { Type } from "typebox";
import { subcomputeEnvironment } from "../src/index.ts";
import { SpeculativeConfiguration } from "../src/config-schema.ts";
import { ContentChunkPool } from "../src/content-chunks.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { controlledSubcomputeChannel, SubcomputeService } from "../src/subcompute.ts";
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
