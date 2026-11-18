import { writeFile } from "node:fs/promises";
import { connect } from "node:net";
import path from "node:path";
import { createInterface } from "node:readline";
import { afterEach, expect, test } from "vitest";
import { Type } from "typebox";
import { subcomputeEnvironment } from "../src/index.ts";
import { controlledSubcomputeChannel } from "../src/subcompute.ts";
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
				protocol: 1,
				op: "compute",
				context: environment.PI_SUBCOMPUTE_CONTEXT,
				namespace: "abandoned",
				version: "1",
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
					protocol: 1,
					op: "compute",
					context: environment.PI_SUBCOMPUTE_CONTEXT,
					namespace: "sequential",
					version: "1",
					key,
					includeInputContent,
					inputResources: [{ path: "large.bin", scope: "content" }],
				});
				const initial = await peer.read();
				if (initial.state === "miss") {
					peer.send({ op: "publish", durationMs: 1, value: { encoding: "json", value: key } });
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
				peer.send({ protocol: 1, op: "compute", context: other.PI_SUBCOMPUTE_CONTEXT });
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
					protocol: 1,
					op: "compute",
					context: environment.PI_SUBCOMPUTE_CONTEXT,
					namespace: "boundary",
					version: "1",
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
				protocol: 1,
				op: "compute",
				context: leaked.PI_SUBCOMPUTE_CONTEXT,
				namespace: "boundary",
				version: "1",
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
