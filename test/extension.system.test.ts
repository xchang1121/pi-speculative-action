import { chmod, mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { fauxAssistantMessage, fauxProvider, fauxToolCall, fauxText } from "@earendil-works/pi-ai";
import {
	AgentSession,
	convertToLlm,
	createAgentSession,
	DefaultResourceLoader,
	ModelRuntime,
	SessionManager,
	SettingsManager,
	type ExtensionAPI,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { createSpeculativeActionHost, registerSpeculativeTool } from "../src/agent-integration.ts";
import { ProcessExecutionCoordinator } from "../src/linux-process-world.ts";
import { currentExecutionScope } from "../src/execution-world.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import type { PredictionSettlement } from "../src/settlement.ts";
import { inspectProcessOwner } from "./linux-process-fixture.ts";
import type { SpeculativeActionEvent } from "../src/runtime-engine.ts";
import { readText, temporaryDirectories } from "./filesystem.ts";
import { idleResources } from "./agent-system-fixture.ts";
import { Agent, type AgentTool } from "@earendil-works/pi-agent-core";
import { deferred, onExit } from "./async.ts";
import { toolUse as response } from "./model.ts";

const adoptions = (settlement: PredictionSettlement) =>
	settlement.observation === "observed" &&
	settlement.match.matched &&
	settlement.match.adoption.status === "adopted"
		? [
				{
					prediction: settlement.prediction,
					relation: settlement.match.relation,
				},
			]
		: [];

const directories = temporaryDirectories("pi-extension-system-");
afterEach(async () => {
	vi.unstubAllEnvs();
	vi.restoreAllMocks();
	await directories.dispose();
});

test
	.skipIf(process.platform !== "linux")
	.each([
		"reuse",
		"space-root",
		"backslash-root",
		"deep-root",
		"long-root",
		"shared-root",
		"stale",
		"stale-inputs",
		"environment",
		"outside-mount",
		"disabled",
		"routing-off",
		"no-domain",
		"lost-domain",
		"retired-domain",
	])("consumes finished Bash through the default Pi extension (%s)", { timeout: 60_000 }, async (mode) => {
	const roots: Record<string, string> = {
		"space-root": "with space 中",
		"backslash-root": "with\\backslash",
		"deep-root": "d/".repeat(18) + "leaf",
		"long-root": "long/" + "x".repeat(240),
	};
	const cwd = path.join(await directories.create(os.homedir()), roots[mode] ?? "");
	const agentDir = await directories.create(os.homedir());
	await mkdir(cwd, { recursive: true });
	if (mode === "shared-root") await chmod(cwd, 0o775);
	const reusable = mode === "reuse" || mode in roots;
	vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	vi.stubEnv("PWD", mode === "outside-mount" ? "/proc" : cwd);
	vi.stubEnv("OLDPWD", cwd);
	vi.stubEnv("BASH_TEST_VERSION", "first");
	if (mode === "no-domain") vi.stubEnv("PI_SPEC_DOMAIN_SOCKET", undefined);
	if (mode === "lost-domain") vi.stubEnv("PI_SPEC_DOMAIN_SOCKET", path.join(agentDir, "missing-collector"));
	await writeFile(
		path.join(agentDir, "speculative-action.json"),
		JSON.stringify({
			enabled: true,
			drafterEnabled: false,
			reuseEnabled: mode !== "disabled",
			prepareObservedOperations: true,
			patternAware: { enabled: false },
			config: {
				processReuse: {
					replayAcceptedTaints: ["clock", "random", "pid_observation", "descriptor_observation"],
				},
			},
			executionRouting: { nativeFallback: mode !== "routing-off" },
			predictionTimeoutMs: 30_000,
		}),
	);
	await writeFile(
		path.join(cwd, "main.c"),
		'#include <stdio.h>\nint main(void){puts("built:42");return 0;}\n',
	);
	await writeFile(path.join(cwd, "check.py"), 'print(open("value").read(), end="")\n');
	await writeFile(path.join(cwd, "value"), "one\n");
	const command =
		'cc main.c -o app && ./app && python3 -B check.py; printf "%s\\n" "$BASH_TEST_VERSION"; printf once >> marker';
	expect(execFileSync("/bin/bash", ["-c", command], { cwd, encoding: "utf8" })).toBe(
		"built:42\none\nfirst\n",
	);
	await writeFile(path.join(cwd, "marker"), "");

	const events: SpeculativeActionEvent<string>[] = [];
	const fallbacks: string[] = [];
	let inspection: ReturnType<typeof inspectProcessOwner> | undefined;
	const nativeAdoptions: Promise<unknown>[] = [];
	const ownerPrototype = LinuxProcessReuseBackend.prototype;
	const nativeDiagnostics = ownerPrototype.nativeDiagnostics;
	vi.spyOn(ownerPrototype, "nativeDiagnostics").mockImplementation(function (
		this: LinuxProcessReuseBackend,
		enabled,
	) {
		if (!inspection) {
			inspection = inspectProcessOwner(() => this);
			nativeAdoptions.push(
				inspection.inOwner(async (state) => {
					const Backend = (await state.load("../dist/linux-process-backend.js")).LinuxProcessReuseBackend;
					const decide = Backend.prototype.decideHeldExec;
					state.adopted = [];
					state.method(Backend.prototype, "decideHeldExec", async function (this: unknown, ...args: any[]) {
						const executable = await state.fs.readlink(`/proc/${args[0].pid}/exe`);
						const decision = await decide.apply(this, args);
						return decision.kind !== "replay"
							? decision
							: {
									...decision,
									adopted: () => {
										decision.adopted?.();
										state.adopted.push(executable);
									},
								};
					});
				}),
			);
		}
		return nativeDiagnostics.call(this, enabled);
	});
	let host!: ReturnType<typeof createSpeculativeActionHost>;
	let turn!: Parameters<typeof host.startTurn>[0];
	const extension = createSpeculativeActionExtension({
		createHost: (id, options) => {
			host = createSpeculativeActionHost(id, {
				...options,
				resources: idleResources(2),
				onEvent: (event) => {
					events.push(event);
					return options.onEvent?.(event);
				},
			});
			return {
				...host,
				startTurn: (input, signal) => {
					turn = input;
					return host.startTurn(input, signal);
				},
				execute: (input, signal, executor) =>
					host.execute(input, signal, (operation) => {
						fallbacks.push(input.tool);
						return executor(operation);
					}),
			};
		},
	});
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const loader = new DefaultResourceLoader({
		cwd,
		agentDir,
		settingsManager,
		noExtensions: true,
		extensionFactories: [extension],
	});
	await loader.reload();
	const actor = fauxProvider({
		provider: "default-bash",
		models: [{ id: "actor" }],
		tokensPerSecond: 4_000,
	});
	const modelRuntime = await ModelRuntime.create({
		authPath: path.join(agentDir, "auth.json"),
		modelsPath: null,
		refreshOnCreate: false,
	});
	modelRuntime.registerNativeProvider(actor.provider);
	const { session } = await createAgentSession({
		cwd,
		agentDir,
		model: actor.getModel(),
		modelRuntime,
		settingsManager,
		resourceLoader: loader,
		sessionManager: SessionManager.inMemory(cwd),
		tools: ["bash"],
	});
	await using _session = onExit(async () => {
		try {
			await session.extensionRunner.emit({
				type: "session_shutdown",
				reason: "quit",
			});
			session.dispose();
		} finally {
			inspection?.dispose();
		}
	});
	let retiredSocket: string | undefined;
	await using _collector = onExit(async () => {
		if (retiredSocket) await rename(`${retiredSocket}.retired`, retiredSocket);
	});
	await session.bindExtensions({
		mode: "print",
		onError: (error) => {
			throw error;
		},
	});
	if (mode === "shared-root")
		expect(await host.executionWorldDiagnostics(true)).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					state: "unavailable",
					detail: expect.stringContaining("root_permissions"),
				}),
			]),
		);
	actor.setResponses([
		async () => {
			await host.previewActorCall({
				turnID: turn.turnID,
				id: "bash-default",
				tool: "bash",
				args: { command },
				tools: turn.tools,
			});
			if (!["disabled", "routing-off", "no-domain", "lost-domain", "shared-root"].includes(mode))
				await expect
					.poll(
						() => events.some((event) => event.type === "candidate" && event.state.status === "succeeded"),
						{ timeout: 30_000 },
					)
					.toBe(true);
			expect(await readText(cwd, "marker")).toBe("");
			if (mode.startsWith("stale")) await writeFile(path.join(cwd, "value"), "two\n");
			if (mode === "stale-inputs")
				await writeFile(
					path.join(cwd, "main.c"),
					'#include <stdio.h>\nint main(void){puts("built:43");return 0;}\n',
				);
			if (mode === "environment") vi.stubEnv("BASH_TEST_VERSION", "second");
			if (mode === "retired-domain") {
				retiredSocket = process.env.PI_SPEC_DOMAIN_SOCKET!;
				await rename(retiredSocket, `${retiredSocket}.retired`);
			}
			return response("bash", { command }, { id: "bash-default" });
		},
		fauxAssistantMessage("done"),
	]);
	await session.prompt("Build the program and run the Python check.");
	const result = session.messages.find((message) => message.role === "toolResult");
	expect(result, JSON.stringify(events)).toMatchObject({
		isError: false,
		content: [
			fauxText(
				`built:${mode === "stale-inputs" ? 43 : 42}\n${mode.startsWith("stale") ? "two" : "one"}\n${mode === "environment" ? "second" : "first"}\n`,
			),
		],
	});
	expect(await readText(cwd, "marker")).toBe("once");
	const settled = events.filter((event) => event.type === "actor_action").at(-1)!;
	expect(fallbacks, JSON.stringify(events)).toEqual(reusable ? [] : ["bash"]);
	// Completed whole-command adoption records the computation hidden by the preview.
	if (
		reusable ||
		[
			"environment",
			"disabled",
			"routing-off",
			"no-domain",
			"lost-domain",
			"retired-domain",
			"shared-root",
		].includes(mode)
	)
		expect((settled.computation?.hiddenComputeMs ?? 0) > 0).toBe(reusable);
	if (reusable)
		expect(settled.settlement.provider).toMatchObject({
			kind: "actor",
			origin: "preview",
		});
	await Promise.all(nativeAdoptions);
	const adopted = inspection ? await inspection.inOwner((state) => state.adopted as string[]) : [];
	if (reusable) expect(adopted).toEqual([]);
	if (mode === "stale") {
		expect(adopted).toContain(path.join(cwd, "app"));
		expect(settled.computation?.reusedComputeMs).toBeGreaterThan(0);
		expect(settled.settlement.provider).toMatchObject({
			kind: "actor",
			origin: "fallback",
		});
	}
	if (mode.startsWith("stale")) expect(adopted).not.toContainEqual(expect.stringMatching(/\/python[^/]*$/));
	if (mode === "stale-inputs")
		expect(adopted).not.toContainEqual(expect.stringMatching(/\/(?:cc|(?:[\w-]+-)?gcc(?:-\d+)?|cc1|app)$/));
});

test.each(["off", "on"])(
	"runs Pi session contracts through the default entry (%s)",
	{ timeout: 60_000 },
	async (mode) => {
		const enabled = mode !== "off";
		const cwd = await directories.create();
		const agentDir = path.join(cwd, "agent");
		await mkdir(agentDir, { recursive: true });
		vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
		await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\n");
		const tools = ["read", "write", "edit", "format_rows", "late_rows"];
		await writeFile(
			path.join(agentDir, "speculative-action.json"),
			JSON.stringify({
				enabled,
				drafterEnabled: false,
				patternAware: { enabled: false },
				prepareObservedOperations: false,
				maxConcurrentActions: 4,
				predictionTimeoutMs: 10000,
			}),
		);
		const actor = fauxProvider({
			provider: "functional-actor",
			models: [{ id: "actor" }],
			tokensPerSecond: 4_000,
			tokenSize: { min: 1, max: 2 },
		});
		actor.setResponses([
			response("read", { path: "notes.txt" }),
			response("write", { path: "result.txt", content: "first\n" }),
			response("edit", {
				path: "result.txt",
				edits: [{ oldText: "first", newText: "final" }],
			}),
			response("read", { path: "result.txt" }),
			fauxAssistantMessage("done"),
		]);
		let api: ExtensionAPI;
		let registered = false;
		const calls: string[] = [];
		const prepared: string[] = [];
		const hooks: string[] = [];
		const updates: string[] = [];
		const updated = { b: deferred(), d: deferred() };
		const callback = (name: string): ToolDefinition => ({
			name,
			label: name,
			description: "Formats its own rows",
			executionMode: "parallel",
			parameters: {
				type: "object",
				properties: { text: { type: "string" } },
				required: ["text"],
			},
			prepareArguments: (input) => {
				const text = (input as { text: string }).text;
				prepared.push(text);
				return { text: text + ":prepared" };
			},
			async execute(callID, args, signal, onUpdate, context) {
				expect(this.name).toBe(name);
				expect(currentExecutionScope()).toMatchObject({
					sessionID: context.sessionManager.getSessionId(),
					callID,
				});
				expect(signal).toBe(context.signal);
				expect(Object.isFrozen(args)).toBe(false);
				calls.push(callID);
				if (!registered) {
					registered = true;
					registerSpeculativeTool(api, callback("read"));
					registerSpeculativeTool(api, callback("late_rows"));
				}
				if (callID === "a" || callID === "c") await updated[callID === "a" ? "b" : "d"].promise;
				await new Promise((resolve) => setImmediate(resolve));
				const output = (args as { text: string }).text;
				onUpdate?.({
					content: [fauxText("partial:" + callID)],
					details: { callID },
				});
				if (callID === "b" || callID === "d") updated[callID].resolve();
				return {
					content: [fauxText("own:" + name + ":" + output)],
					details: { callID },
				};
			},
		});
		const events: SpeculativeActionEvent<string>[] = [];
		const served: [string, boolean][] = [];

		const extension = createSpeculativeActionExtension({
			createHost: (id, options) => {
				const host = createSpeculativeActionHost(id, {
					...options,
					resources: {
						initial: { cpuCount: 4, idleCpuCount: 4 },
						sample: () => new Promise(() => {}),
					},
					onEvent: (event) => {
						events.push(event);
						return options.onEvent?.(event);
					},
				});
				host.runtime.subscribe({
					prediction_feedback: ({ settlement, served: consumed }) => {
						if (settlement.prediction.source === "execution_preparation" && adoptions(settlement).length)
							served.push([settlement.prediction.id, consumed]);
					},
				});
				return host;
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
					api = pi;
					registerSpeculativeTool(pi, callback("format_rows"));
					pi.on("tool_call", (event) => {
						if (!("text" in event.input)) return;
						hooks.push("before:" + event.toolCallId);
						if (event.toolCallId === "blocked") return { block: true, reason: "blocked-on-purpose" };
					});
					pi.on("tool_result", (event) => {
						if (!event.input.text) return;
						hooks.push("after:" + event.toolCallId);
						return {
							content: [fauxText("hook:" + (event.content[0] as { text: string }).text)],
						};
					});
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
		modelRuntime.registerNativeProvider(actor.provider);
		const sessionManager = SessionManager.inMemory(cwd);
		const { session } = await createAgentSession({
			cwd,
			agentDir,
			model: actor.getModel(),
			modelRuntime,
			settingsManager,
			resourceLoader,
			sessionManager,
			tools,
		});
		const originalPrompt = session.agent.prompt;
		const originalContinue = session.agent.continue;
		const toolResults = (owner = session) =>
			owner.messages.filter((message) => message.role === "toolResult");
		let inspection: ReturnType<typeof inspectProcessOwner> | undefined;
		await using _session = onExit(async () => {
			for (const gate of Object.values(updated)) gate.resolve();
			await session.extensionRunner.emit({
				type: "session_shutdown",
				reason: "quit",
			});
			session.dispose();
			inspection?.dispose();
			expect(session.agent.prompt).toBe(originalPrompt);
			expect(session.agent.continue).toBe(originalContinue);
			expect(
				events.filter((event) => event.type === "candidate" && event.candidate.source === "pattern_aware"),
			).toEqual([]);
		});
		await session.bindExtensions({
			mode: "print",
			onError: (error) => {
				throw error;
			},
		});

		const registeredTools = session.agent.state.tools;
		await session.prompt("Read notes.txt, write and edit result.txt, then read the final file.");
		expect(session.agent.state.tools).toBe(registeredTools);
		expect(await readText(cwd, "notes.txt")).toBe("one\ntwo\n");
		expect(await readText(cwd, "result.txt")).toBe("final\n");
		const results = toolResults();
		expect(results).toHaveLength(4);
		expect(results.every((message) => !message.isError)).toBe(true);
		expect(results.at(-1)?.content).toEqual([fauxText("final\n")]);
		const registry = session.agent.state.tools;
		const read = registry.find((tool) => tool.name === "read")!;
		const originalExecute = read.execute;
		for (const copy of [true, false]) {
			const tool = copy ? { ...read } : read;
			tool.execute = async function () {
				expect(this).toBe(tool);
				return { content: [fauxText("replaced")], details: {} };
			};
			session.agent.state.tools = registry.map((current) => (current === read ? tool : current));
			actor.setResponses([response("read", { path: "notes.txt" }), fauxAssistantMessage("done")]);
			await session.prompt("Execute the current read implementation.");
			expect(toolResults().at(-1)?.content).toEqual([fauxText("replaced")]);
		}
		read.execute = originalExecute;
		session.agent.state.tools = registry;
		const originalTools = session.agent.state.tools;
		const originalNext = session.agent.prepareNextTurnWithContext;
		for (const tool of originalTools) Object.freeze(tool);
		Object.freeze(originalTools);
		session.subscribe((event) => {
			if (event.type === "tool_execution_update") updates.push(event.toolCallId);
		});
		actor.setResponses([
			fauxAssistantMessage(
				[
					fauxToolCall("format_rows", { text: "first" }, { id: "a" }),
					fauxToolCall("format_rows", { text: "second" }, { id: "b" }),
				],
				{ stopReason: "toolUse" },
			),
			fauxAssistantMessage(
				[
					fauxToolCall("read", { text: "third" }, { id: "c" }),
					fauxToolCall("late_rows", { text: "fourth" }, { id: "d" }),
					fauxToolCall("format_rows", { text: "skip" }, { id: "blocked" }),
				],
				{ stopReason: "toolUse" },
			),
			fauxAssistantMessage("done"),
		]);
		await session.prompt("Execute the registered callbacks.");
		expect(prepared).toEqual(["first", "second", "third", "fourth", "skip"]);
		expect([calls.slice(0, 2).sort(), calls.slice(2).sort()]).toEqual([
			["a", "b"],
			["c", "d"],
		]);
		expect(updates).toEqual(["b", "a", "d", "c"]);
		expect(hooks.slice(0, 2)).toEqual(["before:a", "before:b"]);
		expect(hooks.slice(2, 4).sort()).toEqual(["after:a", "after:b"]);
		expect(hooks.slice(4, 7)).toEqual(["before:c", "before:d", "before:blocked"]);
		expect(hooks.slice(7).sort()).toEqual(["after:c", "after:d"]);
		expect(
			toolResults()
				.slice(-5)
				.map((message) => [message.toolCallId, message.content, message.isError]),
		).toEqual([
			["a", [fauxText("hook:own:format_rows:first:prepared")], false],
			["b", [fauxText("hook:own:format_rows:second:prepared")], false],
			["c", [fauxText("hook:own:read:third:prepared")], false],
			["d", [fauxText("hook:own:late_rows:fourth:prepared")], false],
			["blocked", [fauxText("blocked-on-purpose")], true],
		]);
		expect(session.agent.prepareNextTurnWithContext).toBe(originalNext);
		expect(events.filter((event) => event.type === "actor_action")).toHaveLength(8);
		await session.reload();
		modelRuntime.registerNativeProvider(actor.provider);
		const entered = deferred();
		const scopes: (string | undefined)[] = [];
		const otherActor = fauxProvider({
			provider: "override",
			models: [{ id: "actor" }],
		});
		const base: AgentTool = {
			...read,
			execute: async function (id, _args, signal) {
				expect(this).toBe(base);
				scopes.push(currentExecutionScope()?.sessionID);
				if (id === "cancel") {
					entered.resolve();
					await new Promise<void>((resolve) =>
						signal!.addEventListener("abort", () => resolve(), { once: true }),
					);
				}
				return { content: [fauxText("base override")], details: {} };
			},
		};
		const loader = new DefaultResourceLoader({
			cwd,
			agentDir,
			settingsManager,
			noExtensions: true,
			extensionFactories: [
				(pi) => {
					registerSpeculativeTool(pi, {
						...base,
						execute: (id, args, signal, update) => base.execute(id, args, signal, update),
					});
				},
				extension,
			],
		});
		await loader.reload();
		modelRuntime.registerNativeProvider(otherActor.provider);
		const other: AgentSession = new AgentSession({
			cwd,
			modelRuntime,
			settingsManager,
			resourceLoader: loader,
			sessionManager: SessionManager.inMemory(cwd),
			initialActiveToolNames: ["read"],
			allowedToolNames: ["read"],
			baseToolsOverride: { read: base },
			agent: new Agent({
				initialState: { model: otherActor.getModel() },
				convertToLlm,
				streamFn: modelRuntime.streamSimple.bind(modelRuntime),
				transformContext: (messages) => other.extensionRunner.emitContext(messages),
			}),
		});
		await using _other = onExit(async () => {
			await other.abort();
			await other.extensionRunner.emit({
				type: "session_shutdown",
				reason: "quit",
			});
			other.dispose();
		});
		await other.bindExtensions({ mode: "print" });
		otherActor.setResponses([response("read", { path: "notes.txt" }, { id: "cancel" })]);
		const cancelled = other.prompt("Read until cancelled.");
		await Promise.race([
			entered.promise,
			cancelled.then(() => {
				throw new Error("cancelled tool never entered");
			}),
		]);
		await other.abort();
		await cancelled;
		actor.setResponses([response("format_rows", { text: "reloaded" }), fauxAssistantMessage("done")]);
		otherActor.setResponses([response("read", { path: "notes.txt" }), fauxAssistantMessage("done")]);
		await Promise.all([
			session.prompt("Execute after reloading extensions."),
			other.prompt("Read through the supplied base implementation."),
		]);
		expect(toolResults(other).at(-1)?.content).toEqual([fauxText("base override")]);
		expect(scopes).toEqual([other.sessionId, other.sessionId]);
		expect(calls).toHaveLength(5);
		const failure = new Error("process launch failed");
		const coordinator = new ProcessExecutionCoordinator({
			execute: async () => ({ exitCode: 0 }),
		});
		await coordinator.runWith(
			{
				execute: () => {
					throw failure;
				},
			},
			async () => {
				for (const execute of [
					() =>
						coordinator.execute({
							command: "",
							cwd,
							environment: {},
							onData: () => {},
						}),
					() => coordinator.operations.exec("", cwd, { onData: () => {} }),
				])
					await expect(execute()).rejects.toBe(failure);
			},
		);
	},
);
