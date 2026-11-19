import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { Type } from "typebox";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
	createAgentSession,
	DefaultResourceLoader,
	ModelRuntime,
	SessionManager,
	SettingsManager,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import {
	createSpeculativeActionHost,
	defineSpeculativeStep,
	defineSpeculativeTool,
	registerSpeculativeTool,
} from "@earendil-works/pi-speculative-action";
import {
	PATTERN_AWARE_DEFAULTS,
	PatternAwareStore,
	projectPatternAwareObservation,
} from "../src/pattern-aware.ts";
import { createSpeculativeActionExtension } from "@earendil-works/pi-speculative-action/extension";
import { afterEach, expect, test, vi } from "vitest";
import type { SpeculativeActionEvent } from "../src/settlement.ts";
import { definitionSchemaHashes } from "../src/agent-integration.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred, onExit } from "./async.ts";
import { inspectDataset, writeDatasetReport } from "../examples/speculative-tool.ts";

const directories = temporaryDirectories("pi-template-");
afterEach(async () => {
	vi.unstubAllEnvs();
	await directories.dispose();
});
async function fixture(
	tools: ToolDefinition<any, any>[],
	enabled = false,
	config: import("../src/config-schema.ts").SpeculativeConfigInput = {},
	patternStore?: PatternAwareStore,
) {
	const cwd = await directories.create();
	const agentDir = await directories.create();
	await mkdir(path.join(cwd, "data"));
	await writeFile(path.join(cwd, "data/rows.json"), "[2,3]");
	await writeFile(
		path.join(agentDir, "speculative-action.json"),
		JSON.stringify({
			enabled,
			reuseEnabled: true,
			drafterEnabled: false,
			prepareObservedOperations: false,
			patternAware: { enabled: !!patternStore },
			resourceCacheMaxBytes: 16 * 1024 * 1024,
			config,
		}),
	);
	vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	const provider = fauxProvider({
		provider: "template",
		models: [{ id: "actor" }],
		tokensPerSecond: 4000,
	});
	const events: SpeculativeActionEvent<string>[] = [];
	let host!: ReturnType<typeof createSpeculativeActionHost>;
	let turn!: Parameters<typeof host.startTurn>[0];
	let reuse = true;
	const extension = createSpeculativeActionExtension({
		createHost: (id, options) => {
			host = createSpeculativeActionHost(id, {
				...options,
				patternStore,
				getSettings: () => ({
					...options.getSettings?.(),
					reuseEnabled: reuse,
				}),
				onEvent: (event) => {
					events.push(event);
					return options.onEvent?.(event);
				},
			});
			return {
				...host,
				startTurn: async (input, signal) => {
					turn = input;
					return host.startTurn(input, signal);
				},
			};
		},
	});
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const resourceLoader = new DefaultResourceLoader({
		cwd,
		agentDir,
		settingsManager,
		noExtensions: true,
		extensionFactories: [
			(pi) => {
				for (const tool of tools) registerSpeculativeTool(pi, tool);
			},
			extension,
		],
	});
	await resourceLoader.reload();
	const modelRuntime = await ModelRuntime.create({
		authPath: path.join(agentDir, "auth.json"),
		modelsPath: null,
		refreshOnCreate: false,
	});
	modelRuntime.registerNativeProvider(provider.provider);
	const { session } = await createAgentSession({
		cwd,
		agentDir,
		model: provider.getModel(),
		modelRuntime,
		settingsManager,
		resourceLoader,
		sessionManager: SessionManager.inMemory(cwd),
		tools: tools.map((tool) => tool.name),
	});
	await session.bindExtensions({
		mode: "print",
		onError: (error) => {
			throw error;
		},
	});
	let sequence = 0;
	return {
		cwd,
		events,
		abort: () => session.abort(),
		setReuse: (value: boolean) => {
			reuse = value;
		},
		execute: async (
			name: string,
			args: Record<string, unknown>,
			before?: (preview: (input?: Record<string, unknown>) => Promise<void>) => Promise<void>,
			error = false,
		) => {
			const id = `template-${++sequence}`;
			let previewFailure: unknown;
			provider.setResponses([
				async () => {
					await before?.(async (input = args) => {
						await host.previewActorCall({
							turnID: turn.turnID,
							id,
							tool: name,
							args: input,
							tools: turn.tools,
						});
					}).catch((error) => {
						previewFailure = error;
						throw error;
					});
					return fauxAssistantMessage(fauxToolCall(name, args, { id }), {
						stopReason: "toolUse",
					});
				},
				fauxAssistantMessage("done"),
			]);
			await session.prompt(`Run ${name}.`);
			if (previewFailure) throw previewFailure;
			const result = session.messages.find(
				(message) => message.role === "toolResult" && message.toolCallId === id,
			);
			expect(result?.role).toBe("toolResult");
			if (result?.role !== "toolResult") throw new Error("Missing tool result");
			expect(result.isError, JSON.stringify(result)).toBe(error);
			return result;
		},
		dispose: async () => {
			try {
				await session.abort();
				await session.extensionRunner.emit({
					type: "session_shutdown",
					reason: "quit",
				});
			} finally {
				session.dispose();
			}
		},
	};
}

const parameters = Type.Object({
	path: Type.String(),
	factor: Type.Number(),
	label: Type.String(),
});
test("public computeStep retains async binary results above one MiB using the configured byte budget", async () => {
	let builds = 0;
	const tool = defineSpeculativeTool({
		name: "large_step",
		label: "Large step",
		description: "Explicit binary result",
		parameters: Type.Object({ tag: Type.String() }),
		effect: "actor",
		steps: {},
		execute: async ({ computeStep }, args) => {
			const value = await computeStep(
				{
					namespace: "large.binary",
					key: "shared",
					inputResources: [{ path: "data/rows.json", scope: "content" }],
				},
				async () => {
					builds++;
					await Promise.resolve();
					return Buffer.alloc(2 * 1024 * 1024, 71);
				},
			);
			expect(value).toBeInstanceOf(Buffer);
			const details = {
				tag: args.tag,
				bytes: value.byteLength,
				first: value[0],
			};
			value.fill(0);
			return { content: [], details };
		},
	});
	const f = await fixture([tool], false, {
		subcomputation: { maxStepResultBytes: 3 * 1024 * 1024 },
		filesystem: { chunkBytes: 16384 },
	});
	await using _cleanup = onExit(f.dispose);
	expect((await f.execute(tool.name, { tag: "cold" })).details).toEqual({
		tag: "cold",
		bytes: 2 * 1024 * 1024,
		first: 71,
	});
	expect((await f.execute(tool.name, { tag: "warm" })).details).toEqual({
		tag: "warm",
		bytes: 2 * 1024 * 1024,
		first: 71,
	});
	expect(builds).toBe(1);
});

test.for([
	{ target: "new.txt", warm: false },
	{ target: "data/rows.json", warm: false },
	{ target: "data/rows.json", warm: true },
])(
	"a mutation step reads its pending write once ($target, cached baseline=$warm)",
	async ({ target, warm }) => {
		let stepBuilds = 0;
		let computeBuilds = 0;
		const step = defineSpeculativeStep({
			id: "transaction.read-written",
			dependencies: {},
			run: async ({ fs }, input: { path: string }) => {
				stepBuilds++;
				return (await fs.readFile(input.path)).toString();
			},
		});
		const tool = defineSpeculativeTool({
			name: "read_pending_write",
			label: "Read pending write",
			description: "Read the current transaction through both step APIs",
			parameters: Type.Object({ text: Type.String(), output: Type.String() }),
			effect: "workspace_mutation",
			steps: { step },
			execute: async ({ fs, step, computeStep }, args) => {
				if (warm) expect(await step("step", { path: target })).toBe("[2,3]");
				await fs.writeFile(target, args.text);
				const first = await step("step", { path: target });
				const second = await computeStep(
					{
						namespace: "transaction.compute-written",
						key: target,
						inputResources: [{ path: target, scope: "content" }],
					},
					async (view) => {
						computeBuilds++;
						return (await view.readFile(path.join(f.cwd, target))).toString();
					},
				);
				await fs.writeFile(args.output, first + second);
				return { content: [], details: { first, second } };
			},
		});
		const f = await fixture([tool], true);
		await using _cleanup = onExit(f.dispose);
		const result = await f.execute(tool.name, { text: "actual", output: "actual.txt" }, async (preview) => {
			await preview({ text: "private", output: "discarded.txt" });
			await expect
				.poll(() => f.events.filter((event) => event.type === "candidate").map((event) => event.state), {
					message: "transaction candidate must complete",
				})
				.toContainEqual(expect.objectContaining({ status: "succeeded" }));
			expect(stepBuilds).toBe(warm ? 2 : 1);
			expect(computeBuilds).toBe(1);
			expect(await readFile(path.join(f.cwd, target), "utf8").catch(() => undefined)).toBe(
				target === "new.txt" ? undefined : "[2,3]",
			);
		});
		expect(result.details).toEqual({ first: "actual", second: "actual" });
		expect({ stepBuilds, computeBuilds }).toEqual({ stepBuilds: warm ? 3 : 2, computeBuilds: 2 });
		expect(await readFile(path.join(f.cwd, "actual.txt"), "utf8")).toBe("actualactual");
		expect(await readFile(path.join(f.cwd, "discarded.txt")).catch(() => undefined)).toBeUndefined();
	},
);

test("a rejected outer mutation retains baseline steps after unrelated private writes", async () => {
	let builds = 0;
	const tool = defineSpeculativeTool({
		name: "rollback_step",
		label: "Rollback step",
		description: "Keep baseline computation",
		parameters: Type.Object({ output: Type.String() }),
		effect: "workspace_mutation",
		steps: {},
		execute: async ({ computeStep, fs }, args) => {
			await fs.writeFile("scratch.txt", "private");
			const result = await computeStep(
				{
					namespace: "rollback.rows",
					key: "sum",
					inputResources: [
						{ path: "data/rows.json", scope: "content" },
						{ path: "data", scope: "names" },
					],
				},
				async (view) => {
					builds++;
					expect(await view.readdir!(path.join(f.cwd, "data"))).toEqual(["rows.json"]);
					expect((await view.stat!(path.join(f.cwd, "data/rows.json"))).isDirectory()).toBe(false);
					return JSON.parse((await view.readFile(path.join(f.cwd, "data/rows.json"))).toString()).reduce(
						(a: number, b: number) => a + b,
						0,
					);
				},
			);
			await fs.writeFile(args.output, String(result));
			return { content: [], details: result };
		},
	});
	const f = await fixture([tool], true);
	await using _cleanup = onExit(f.dispose);
	const result = await f.execute(tool.name, { output: "actual.txt" }, async (preview) => {
		await preview({ output: "discarded.txt" });
		await expect
			.poll(() => f.events.some((event) => event.type === "candidate" && event.state.status === "succeeded"))
			.toBe(true);
		expect(await readFile(path.join(f.cwd, "scratch.txt")).catch(() => undefined)).toBeUndefined();
	});
	expect(result.details).toBe(5);
	expect(builds).toBe(1);
	expect(await readFile(path.join(f.cwd, "actual.txt"), "utf8")).toBe("5");
	expect(await readFile(path.join(f.cwd, "discarded.txt")).catch(() => undefined)).toBeUndefined();
	const actor = f.events.filter((event) => event.type === "actor_action").at(-1)!;
	expect(actor.computation?.subcomputationHits).toBeGreaterThan(0);
	expect(actor.computation?.reusedComputeMs).toBeGreaterThan(0);
});

test.each(["parent", "sibling"])("a dirty %s preserves clean nested steps after rollback", async (mode) => {
	let cleanBuilds = 0;
	let parentBuilds = 0;
	let dirtyRead = Promise.withResolvers<void>();
	let releaseDirty = Promise.withResolvers<void>();
	const clean = defineSpeculativeStep({
		id: "rollback.clean-child",
		dependencies: {},
		run: async ({ fs }) => {
			cleanBuilds++;
			return (await fs.readFile("data/rows.json")).toString();
		},
	});
	const dirty = defineSpeculativeStep({
		id: "rollback.dirty-child",
		dependencies: {},
		run: async ({ fs }) => {
			const value = (await fs.readFile("scratch.txt")).toString();
			dirtyRead.resolve();
			await releaseDirty.promise;
			return value;
		},
	});
	const parent = defineSpeculativeStep({
		id: "rollback.parent",
		dependencies: { clean, dirty },
		run: async ({ fs, step }) => {
			parentBuilds++;
			if (mode === "parent") {
				await fs.readFile("scratch.txt");
				return step("clean", {});
			}
			dirtyRead = Promise.withResolvers<void>();
			releaseDirty = Promise.withResolvers<void>();
			const sibling = step("dirty", {});
			await dirtyRead.promise;
			try {
				return await step("clean", {});
			} finally {
				releaseDirty.resolve();
				await sibling;
			}
		},
	});
	const tool = defineSpeculativeTool({
		name: "nested_rollback",
		label: "Nested rollback",
		description: "Retain only clean children",
		parameters: Type.Object({ output: Type.String() }),
		effect: "workspace_mutation",
		steps: { parent },
		execute: async ({ fs, step }, args) => {
			await fs.writeFile("scratch.txt", args.output);
			const value = await step("parent", {});
			await fs.writeFile(args.output, value);
			return { content: [], details: value };
		},
	});
	const f = await fixture([tool], true);
	await using _cleanup = onExit(f.dispose);
	const result = await f.execute(tool.name, { output: "actual.txt" }, async (preview) => {
		await preview({ output: "discarded.txt" });
		await expect
			.poll(() => f.events.some((event) => event.type === "candidate" && event.state.status === "succeeded"))
			.toBe(true);
	});
	expect(result.details).toBe("[2,3]");
	expect({ cleanBuilds, parentBuilds }).toEqual({ cleanBuilds: 1, parentBuilds: 2 });
	expect(await readFile(path.join(f.cwd, "actual.txt"), "utf8")).toBe("[2,3]");
	expect(await readFile(path.join(f.cwd, "discarded.txt")).catch(() => undefined)).toBeUndefined();
});

test("registered templates share declared asynchronous children across tools", async () => {
	let loads = 0;
	let indexes = 0;
	let reports = 0;
	let effects = 0;
	const rows = defineSpeculativeStep({
		id: "example.rows",
		dependencies: {},
		run: async ({ fs }, input: { path: string }) => {
			loads++;
			return JSON.parse((await fs.readFile(input.path)).toString("utf8")) as number[];
		},
	});
	const index = defineSpeculativeStep({
		id: "example.index",
		dependencies: { rows },
		run: async ({ step }, input: { path: string; factor: number }) => {
			indexes++;
			return (await step("rows", { path: input.path })).reduce((sum, value) => sum + value * input.factor, 0);
		},
	});
	const report = defineSpeculativeStep({
		id: "example.report",
		dependencies: { index },
		run: async ({ step }, input: { path: string; factor: number; label: string }) => {
			reports++;
			return {
				label: input.label,
				total: await step("index", { path: input.path, factor: input.factor }),
			};
		},
	});
	const tools = ["template_summary", "template_export"].map((name) =>
		defineSpeculativeTool({
			name,
			label: name,
			description: name,
			parameters,
			effect: "actor",
			steps: { report },
			resources: (args) => [args.path],
			execute: async ({ step, fs }, args) => {
				const result = await step("report", args);
				effects++;
				await fs.writeFile(`${name}.txt`, `${effects}`);
				return {
					content: [{ type: "text", text: `${name}:${result.total}` }],
					details: result,
				};
			},
		}),
	);
	const f = await fixture(tools);
	await using _cleanup = onExit(f.dispose);
	const input = { path: "data/rows.json", factor: 2, label: "first" };
	expect((await f.execute(tools[0]!.name, input)).details).toEqual({
		total: 10,
		label: "first",
	});
	const second = await f.execute(tools[1]!.name, input);
	expect(second.details).toEqual({ total: 10, label: "first" });
	expect({ loads, indexes, reports, effects }).toEqual({
		loads: 1,
		indexes: 1,
		reports: 1,
		effects: 2,
	});
	expect(await readFile(path.join(f.cwd, "template_export.txt"), "utf8")).toBe("2");
	(second.details as { total: number }).total = -1;
	expect((await f.execute(tools[0]!.name, input)).details).toEqual({
		total: 10,
		label: "first",
	});
	expect({ loads, indexes, reports, effects }).toEqual({
		loads: 1,
		indexes: 1,
		reports: 1,
		effects: 3,
	});
	const changed = { ...input, label: "second" };
	expect((await f.execute(tools[1]!.name, changed)).details).toEqual({
		total: 10,
		label: "second",
	});
	expect({ loads, indexes, reports, effects }).toEqual({
		loads: 1,
		indexes: 1,
		reports: 2,
		effects: 4,
	});
	changed.factor = 3;
	expect((await f.execute(tools[0]!.name, changed)).details).toEqual({
		total: 15,
		label: "second",
	});
	expect({ loads, indexes, reports, effects }).toEqual({
		loads: 1,
		indexes: 2,
		reports: 3,
		effects: 5,
	});
	await writeFile(path.join(f.cwd, input.path), "[5,7]");
	expect((await f.execute(tools[0]!.name, changed)).details).toEqual({
		total: 36,
		label: "second",
	});
	expect({ loads, indexes, reports, effects }).toEqual({
		loads: 2,
		indexes: 3,
		reports: 4,
		effects: 6,
	});
	f.setReuse(false);
	expect((await f.execute(tools[0]!.name, changed)).details).toEqual({
		total: 36,
		label: "second",
	});
	expect({ loads, indexes, reports, effects }).toEqual({
		loads: 3,
		indexes: 4,
		reports: 5,
		effects: 7,
	});
});

test("cancelled registered work publishes no result and leaves subsequent calls usable", async () => {
	const entered = deferred();
	let builds = 0;
	let effects = 0;
	const value = defineSpeculativeStep({
		id: "cancel.value",
		dependencies: {},
		run: async ({ signal }, input: { value: number }) => {
			if (++builds === 1) {
				entered.resolve();
				await new Promise<void>((resolve) =>
					signal.addEventListener("abort", () => resolve(), { once: true }),
				);
			}
			signal.throwIfAborted();
			return input.value;
		},
	});
	const tool = defineSpeculativeTool({
		name: "template_cancel",
		label: "Cancel",
		description: "Cancel",
		effect: "actor",
		parameters: Type.Object({ value: Type.Number() }),
		steps: { value },
		execute: async ({ step }, args) => {
			const result = await step("value", args);
			effects++;
			return { content: [], details: result };
		},
	});
	const f = await fixture([tool]);
	await using _cleanup = onExit(f.dispose);
	const cancelled = f.execute(tool.name, { value: 7 }, undefined, true);
	await Promise.race([
		entered.promise,
		cancelled.then(() => {
			throw new Error("Step did not start");
		}),
	]);
	await f.abort();
	await cancelled;
	expect(effects).toBe(0);
	for (let i = 0; i < 2; i++) expect((await f.execute(tool.name, { value: 7 })).details).toBe(7);
	expect({ builds, effects }).toEqual({ builds: 2, effects: 2 });
});

test("the packaged example runs without the extension and enforces its data and path boundary", async () => {
	const cwd = await directories.create();
	const controller = new AbortController();
	const actor = { cwd } as Parameters<typeof inspectDataset.execute>[4];
	await writeFile(path.join(cwd, "rows.json"), "[2,3]");
	const args = { path: "rows.json", multiplier: 2, title: "Report" };
	expect((await inspectDataset.execute("read", args, controller.signal, undefined, actor)).details).toEqual({
		count: 2,
		total: 10,
		text: "Report: 2 rows, total 10",
	});
	await writeDatasetReport.execute(
		"write",
		{ ...args, output: "result.txt" },
		controller.signal,
		undefined,
		actor,
	);
	expect(await readFile(path.join(cwd, "result.txt"), "utf8")).toBe("Report: 2 rows, total 10\n");
	await expect(
		inspectDataset.execute(
			"escape",
			{ ...args, path: "../outside.json" },
			controller.signal,
			undefined,
			actor,
		),
	).rejects.toThrow("outside its workspace");
	await writeFile(path.join(cwd, "rows.json"), '["invalid"]');
	await expect(inspectDataset.execute("invalid", args, controller.signal, undefined, actor)).rejects.toThrow(
		"finite numbers",
	);
	controller.abort(new Error("cancelled"));
	await expect(
		writeDatasetReport.execute(
			"cancel",
			{ ...args, output: "result.txt" },
			controller.signal,
			undefined,
			actor,
		),
	).rejects.toThrow("cancelled");
	expect(await readFile(path.join(cwd, "result.txt"), "utf8")).toBe("Report: 2 rows, total 10\n");
});

test.each(["observation", "workspace_mutation", "actor"] as const)(
	"uses the declared execution boundary (%s)",
	async (effect) => {
		let builds = 0;
		let runs = 0;
		const calculated = defineSpeculativeStep({
			id: "boundary.value",
			dependencies: {},
			run: async ({ fs }, input: { path: string }) => {
				builds++;
				return (await fs.readFile(input.path)).toString("utf8");
			},
		});
		const tool = defineSpeculativeTool({
			name: `template_${effect}`,
			label: effect,
			description: effect,
			parameters: Type.Object({ path: Type.String() }),
			effect,
			steps: { calculated },
			resources: (args) => [args.path],
			execute: async ({ step, fs }, args) => {
				runs++;
				const value = await step("calculated", args);
				if ("writeFile" in fs) await fs.writeFile("effect.txt", "once");
				return { content: [{ type: "text", text: value }], details: {} };
			},
		});
		const f = await fixture([tool], true);
		await using _cleanup = onExit(f.dispose);
		await f.execute(tool.name, { path: "data/rows.json" }, async (preview) => {
			await preview();
			if (effect !== "actor")
				await expect
					.poll(() =>
						f.events.some((event) => event.type === "candidate" && event.state.status === "succeeded"),
					)
					.toBe(true);
			expect(await readFile(path.join(f.cwd, "effect.txt"), "utf8").catch(() => "missing")).toBe("missing");
			expect(runs).toBe(effect === "actor" ? 0 : 1);
		});
		expect({ builds, runs }).toEqual({ builds: 1, runs: 1 });
		expect(await readFile(path.join(f.cwd, "effect.txt"), "utf8").catch(() => "missing")).toBe(
			effect === "observation" ? "missing" : "once",
		);
	},
);


test("registered SDK tools route a learned transition through declared identities after a rename", async () => {
	const store = new PatternAwareStore(PATTERN_AWARE_DEFAULTS);
	const source = "sdk.catalog";
	const target = "sdk.open";
	const parameters = Type.Object({ path: Type.String() });
	const schemaHash = definitionSchemaHashes([{ name: "old_open", inputSchema: parameters }]).old_open;
	const rows = (value: string) => ({ content: [{ type: "text" as const, text: value }], details: {} });
	for (let i = 0; i < 8; i++) {
		const id = "training-" + i;
		const file = "data/" + i + ".json";
		const event = { sessionID: id, turnID: id, outcome: "success" as const, durationMs: 1 };
		store.observe({
			...event,
			tool: "old_catalog",
			patternIdentity: source,
			input: {},
			...projectPatternAwareObservation(rows(file), { tool: "old_catalog", input: {} }),
		});
		store.observe({ ...event, tool: "old_open", patternIdentity: target, schemaHash, input: { path: file } });
		store.finishSession(id, true);
	}
	const catalog = defineSpeculativeTool({
		name: "listing",
		label: "Listing",
		description: "Show a dataset",
		patternIdentity: source,
		parameters: Type.Object({}),
		effect: "observation",
		steps: {},
		execute: async () => rows("data/rows.json"),
	});
	const open = defineSpeculativeTool({
		name: "show_dataset",
		label: "Show",
		description: "Read a dataset",
		patternIdentity: target,
		parameters,
		effect: "observation",
		steps: {},
		execute: async ({ fs }, input) => rows((await fs.readFile(input.path)).toString()),
	});
	const plans = vi.spyOn(store, "predictAfterBatch");
	const f = await fixture([catalog, open], true, {}, store);
	await using _cleanup = onExit(f.dispose);
	await f.execute(catalog.name, {});
	await expect
		.poll(() => plans.mock.results.flatMap((entry) => entry.value ?? []))
		.toContainEqual(expect.objectContaining({ tool: open.name, input: { path: "data/rows.json" } }));
});
