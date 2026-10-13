import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { execFileSync, spawn } from "node:child_process";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { AgentSession, convertToLlm, createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, type ExtensionAPI, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { currentExecutionScope } from "../src/execution-world.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import type { ProcessExecutionBinding, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { SynchronousProcessObserver } from "../src/linux-process-sync.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import { commitBenchmarkFixture, compileBenchmarkHelper, holdProcessPublication } from "./linux-process-fixture.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { Agent, type AgentTool } from "@earendil-works/pi-agent-core";
import { deferred, onExit } from "./async.ts";

const directories = temporaryDirectories("pi-extension-system-");
afterEach(async () => { vi.unstubAllEnvs(); vi.restoreAllMocks(); await directories.dispose(); });

test.each(["off", "on", ...(process.platform === "linux" ? ["native", "native-nested", "native-stale", "native-running", "native-retired", "native-unkeyed", "native-inputs", "native-inputs-stale",
	"native-sync-producer", "native-sync-consumer", "native-sync-both", "native-sync-stale", "native-sync-lost", "native-sync-inputs", "native-sync-inputs-stale"] : [])])("runs the shipped extension through a real Pi session (%s)", async mode => {
	const enabled = mode !== "off", native = mode.startsWith("native"), running = mode === "native-running" || mode === "native-retired";
	const stale = mode.endsWith("stale"), inputs = mode.includes("inputs"), synchronous = (callID: string) => mode.startsWith("native-sync") &&
		(!mode.endsWith("producer") && !mode.endsWith("consumer") || callID === (mode.endsWith("producer") ? "seed" : "check"));
	const cwd = await directories.create(native ? os.homedir() : undefined), agentDir = path.join(cwd, "agent");
	await mkdir(agentDir); vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\n");
	const tools = native ? ["produce_notes", "format_rows", "write", "read"] : ["read", "write", "edit", "format_rows", "late_rows"];
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({ enabled, drafterEnabled: false,
		patternAware: { enabled: native, multiStepEnabled: false, presets: ["recent-command"] }, tools, maxConcurrentActions: 4, predictionTimeoutMs: 10_000 }));
	const actor = fauxProvider({ provider: "functional-actor", models: [{ id: "actor" }], tokensPerSecond: 4_000, tokenSize: { min: 1, max: 2 } });
	const response = (name: string, args: Record<string, unknown>) => fauxAssistantMessage(fauxToolCall(name, args), { stopReason: "toolUse" });
	actor.setResponses([
		response("read", { path: "notes.txt" }),
		response("write", { path: "result.txt", content: "first\n" }),
		response("edit", { path: "result.txt", edits: [{ oldText: "first", newText: "final" }] }),
		response("read", { path: "result.txt" }), fauxAssistantMessage("done"),
	]);
	let api: ExtensionAPI, registered = false;
	const calls: string[] = [], prepared: string[] = [], hooks: string[] = [], updates: string[] = [];
	const callback = (name: string): ToolDefinition => ({ name, label: name, description: "Formats its own rows", executionMode: "parallel",
		parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
		prepareArguments: input => { const text = (input as { text: string }).text; prepared.push(text);
			return { text: text + (native ? "" : ":prepared"), ...(mode === "native-unkeyed" && name === "format_rows" ? { opaque: 1n } : {}) }; },
		async execute(callID, args, signal, onUpdate, context) {
			expect(this.name).toBe(name); expect(currentExecutionScope()).toMatchObject({ sessionID: context.sessionManager.getSessionId(), callID });
			expect(signal).toBe(context.signal); expect(Object.isFrozen(args)).toBe(false); calls.push(callID);
			if (!native && !registered) { registered = true; api.registerTool(callback("read")); api.registerTool(callback("late_rows")); }
			await new Promise(resolve => setImmediate(resolve));
			const text = (args as { text: string }).text, options = { cwd, env: { PATH: "/usr/local/bin:/usr/bin:/bin", HOME: os.homedir(), LANG: "C.UTF-8" }, stdio: ["ignore", "pipe", "pipe"] as ["ignore", "pipe", "pipe"] };
			const output = synchronous(callID) ? execFileSync("/bin/bash", ["-c", text], { ...options, encoding: "utf8" }) : native ? await new Promise<string>((resolve, reject) => {
				const child = spawn("/bin/bash", ["-c", text], { ...options, signal });
				let output = ""; for (const stream of [child.stdout, child.stderr]) stream.on("data", bytes => { output += bytes; });
				child.once("error", reject); child.once("close", code => code ? reject(new Error(`callback exited ${code}`)) : resolve(output));
			}) : text;
			onUpdate?.({ content: [{ type: "text", text: "partial:" + callID }], details: { callID } });
			return { content: [{ type: "text", text: "own:" + name + ":" + output }], details: { callID } };
		},
	});
	const events: SpeculativeActionEvent<string>[] = [];
	const extension = createSpeculativeActionExtension({ createHost: (id, options) => createSpeculativeActionHost(id, {
		...options, resources: { initial: { cpuCount: 4, idleCpuCount: 4 }, sample: () => new Promise(() => {}) },
		onEvent: event => { events.push(event); return options.onEvent?.(event); },
	}) });
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [pi => {
		api = pi; pi.registerTool(callback("format_rows")); if (native) pi.registerTool(callback("produce_notes"));
		pi.on("tool_call", event => { if (!("text" in event.input)) return; hooks.push("before:" + event.toolCallId);
			if (event.toolCallId === "blocked") return { block: true, reason: "blocked-on-purpose" }; });
		pi.on("tool_result", event => { if (!event.input.text) return; hooks.push("after:" + event.toolCallId);
			return { content: [{ type: "text", text: "hook:" + (event.content[0] as { text: string }).text }] }; });
	}, extension] });
	await resourceLoader.reload();
	const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
	modelRuntime.registerNativeProvider(actor.provider);
	const { session } = await createAgentSession({ cwd, agentDir, model: actor.getModel(), modelRuntime, settingsManager, resourceLoader,
		sessionManager: SessionManager.inMemory(cwd), tools });
	const originalPrompt = session.agent.prompt, originalContinue = session.agent.continue;
	let publication: ReturnType<typeof holdProcessPublication> | undefined;
	try {
		await session.bindExtensions({ mode: "print", onError: error => { throw error; } });
		if (native) {
			let backend: LinuxProcessReuseBackend | undefined;
			let inputBinding: ProcessExecutionBinding | undefined;
			if (mode === "native-sync-lost") {
				const attach = SynchronousProcessObserver.prototype.attach;
				vi.spyOn(SynchronousProcessObserver.prototype, "attach").mockImplementation(function (this: SynchronousProcessObserver, request) {
					const attachment = attach.call(this, request);
					return { ...attachment, finish: () => { const result = attachment.finish();
						if (result.adoptions.length) { expect(result.metrics.hits).toBe(1); expect([...new Int32Array(request.handoffs.claims)]).toContain(2);
							void Reflect.get(this, "worker").terminate(); throw new Error("lost acknowledged synchronous handoff"); }
						return result;
					} };
				});
			}
			const observe = LinuxProcessReuseBackend.prototype.observeBindings, nested = !inputs && !mode.startsWith("native-sync") && !["native", "native-stale"].includes(mode);
			vi.spyOn(LinuxProcessReuseBackend.prototype, "observeBindings").mockImplementation(function (this: LinuxProcessReuseBackend, scope, execute, receive, ...args) {
				backend = this; return observe.call(this, scope, execute, (bindings, computations) => {
					if (inputs && scope.callID === "seed") {
						const registry = Reflect.get(this, "handoffs") as ProcessHandoffRegistry<{ readonly executable: string }>;
						inputBinding = bindings.find(binding => registry.resolveBinding(binding, scope)?.executable === path.join(cwd, "slow"));
					}
					return receive(nested && scope.callID === "seed" ? bindings.slice(0, 1) : bindings, computations);
				}, ...args);
			});
			await compileBenchmarkHelper(cwd, { source: "slow.c", output: "slow", code: "#include <stdio.h>\n#include <fcntl.h>\n#include <unistd.h>\nint main(int n,char**v){int f=fcntl(3,F_GETFD)<0?open(v[1],O_RDONLY):3;if(f<0)return 1;unsigned long h=5381;char c;while(read(f,&c,1)==1)h=h*33+c;close(f);for(volatile unsigned long i=0;i<40000000ul;i++)h^=i;printf(\"%s %lx\\n\",v[1],h);return 0;}\n" });
			await commitBenchmarkFixture(cwd, "sdk-callback");
			const command = "./slow notes.txt" + (inputs ? " 3<notes.txt" : "");
			const initial = `printf initial:; ${command}; :`, remaining = `printf remaining:; ${command}; printf once >> marker`;
			const call = (tool: string, text: string, id: string) => fauxAssistantMessage(fauxToolCall(tool, { text }, { id }), { stopReason: "toolUse" });
			actor.setResponses([call("produce_notes", initial, "seed"), ...(inputs ? [response("read", { path: "notes.txt", offset: 2 })] : []), async () => {
				if (running) publication = holdProcessPublication(backend!);
				return response("write", { path: "notes.txt", content: "changed\n" });
			}, ...(mode === "native-retired" ? [async () => {
				await expect.poll(publication!.reached, { timeout: 5000 }).toBe(true); session.setActiveToolsByName(tools.filter(name => name !== "produce_notes"));
				return response("read", { path: "notes.txt" });
			}] : []), async () => {
				if (inputs) expect(inputBinding).toBeDefined();
				await expect.poll(() => publication ? publication.reached() : inputs ? backend!.bindingStale(inputBinding!).then(stale => !stale) :
					events.some(event => event.type === "candidate" && event.candidate.kind === "operation" && event.state.status === "succeeded"), { timeout: 5000 }).toBe(true);
				await expect(readFile(path.join(cwd, "marker"))).rejects.toMatchObject({ code: "ENOENT" });
				if (stale) await writeFile(path.join(cwd, "notes.txt"), "fresh\n");
				return call("format_rows", remaining, "check");
			}, fauxAssistantMessage("done")]);
			await session.prompt("Produce notes, edit the input, then render the remaining work.");
			// A named inherited input prepares a new version after the edit; adoption still checks the actual held graph.
			const results = session.messages.filter(message => message.role === "toolResult"), reused = !stale;
			if (mode === "native-sync-lost") {
				expect(results.at(-1)).toMatchObject({ isError: true }); expect(JSON.stringify(results.at(-1))).toContain("synchronous native handoff failed");
				expect(await readFile(path.join(cwd, "marker"), "utf8")).toBe("once"); expect(calls).toEqual(["seed", "check"]); return;
			}
			expect(results.at(-1)?.content, JSON.stringify(session.messages)).toEqual([{ type: "text", text: "hook:own:format_rows:remaining:" + execFileSync("./slow", ["notes.txt"], { cwd, encoding: "utf8" }) }]);
			expect(results.every(result => !result.isError)).toBe(true); expect(await readFile(path.join(cwd, "marker"), "utf8")).toBe("once");
			expect(calls).toEqual(["seed", "check"]); expect(prepared).toEqual([initial, remaining]);
			if (inputs) expect(summarizeSpeculativeTrace(events).inputReuseHits).toBe(1);
			const settled = events.filter(event => event.type === "actor_action").at(-1)!, timing = settled.computation!;
			expect(backend!.actorMetrics().hits).toBe(Number(reused)); expect(timing.hiddenComputeMs > 0).toBe(reused);
			expect((timing.hiddenByMode ?? []).reduce((total, part) => total + part.hiddenComputeMs, 0)).toBeCloseTo(timing.hiddenComputeMs, 5);
			const predictions = events.filter(event => event.type === "operation_prediction"), adopted = predictions.flatMap(event => event.settlement.observation === "observed" && event.settlement.match.matched && event.settlement.match.adoption.status === "adopted" ? [event.settlement.match.relation] : []);
			expect(adopted).toHaveLength(Number(reused && mode !== "native-retired")); if (nested && adopted.length) expect(adopted[0]).toMatchObject({ kind: "projected", projector: "owned-operation" });
			if (mode === "native-retired") expect(predictions.filter(event => event.settlement.prediction.id === predictions[0]!.settlement.prediction.id)).toMatchObject([{ settlement: { observation: "unobserved", cause: { code: "operation_not_observed" } } }]);
			expect(!!settled.settlement.actionKeyHash).toBe(mode !== "native-unkeyed");
			if (publication) expect(publication.joined()).toBe(true);
			expect(events.filter(event => event.type === "candidate" && event.state.status === "cancelled" && event.state.cause.code === "tool_disabled" && event.state.executionMs > 0)).toEqual([]);
			return;
		}
		const registeredTools = session.agent.state.tools;
		await session.prompt("Read notes.txt, write and edit result.txt, then read the final file.");
		expect(session.agent.state.tools).toBe(registeredTools);
		expect(await readFile(path.join(cwd, "notes.txt"), "utf8")).toBe("one\ntwo\n");
		expect(await readFile(path.join(cwd, "result.txt"), "utf8")).toBe("final\n");
		const results = session.messages.filter(message => message.role === "toolResult");
		expect(results).toHaveLength(4);
		expect(results.every(message => !message.isError)).toBe(true);
		expect(results.at(-1)?.content).toEqual([{ type: "text", text: "final\n" }]);
		const registry = session.agent.state.tools, read = registry.find(tool => tool.name === "read")!, originalExecute = read.execute;
		for (const copy of [true, false]) {
			const tool = copy ? { ...read } : read;
			tool.execute = async function (id) { expect(this).toBe(tool); expect(currentExecutionScope()?.callID).toBe(id); return { content: [{ type: "text", text: "replaced" }], details: {} }; };
			session.agent.state.tools = registry.map(current => current === read ? tool : current);
			actor.setResponses([response("read", { path: "notes.txt" }), fauxAssistantMessage("done")]);
			await session.prompt("Execute the current read implementation.");
			expect(session.messages.filter(message => message.role === "toolResult").at(-1)?.content).toEqual([{ type: "text", text: "replaced" }]);
		}
		read.execute = originalExecute; session.agent.state.tools = registry;
		const originalTools = session.agent.state.tools, originalNext = session.agent.prepareNextTurnWithContext;
		for (const tool of originalTools) Object.freeze(tool);
		Object.freeze(originalTools);
		session.subscribe(event => { if (event.type === "tool_execution_update") updates.push(event.toolCallId); });
		actor.setResponses([
			fauxAssistantMessage([fauxToolCall("format_rows", { text: "first" }, { id: "a" }), fauxToolCall("format_rows", { text: "second" }, { id: "b" })], { stopReason: "toolUse" }),
			fauxAssistantMessage([fauxToolCall("read", { text: "third" }, { id: "c" }), fauxToolCall("late_rows", { text: "fourth" }, { id: "d" }),
				fauxToolCall("format_rows", { text: "skip" }, { id: "blocked" })], { stopReason: "toolUse" }), fauxAssistantMessage("done"),
		]);
		await session.prompt("Execute the registered callbacks.");
		expect(prepared).toEqual(["first", "second", "third", "fourth", "skip"]);
		expect(calls).toEqual(["a", "b", "c", "d"]); expect(updates).toEqual(calls);
		expect(hooks).toEqual(["before:a", "before:b", "after:a", "after:b", "before:c", "before:d", "before:blocked", "after:c", "after:d"]);
		expect(session.messages.filter(message => message.role === "toolResult").slice(-5).map(message => [message.toolCallId, message.content, message.isError])).toEqual([
			["a", [{ type: "text", text: "hook:own:format_rows:first:prepared" }], false], ["b", [{ type: "text", text: "hook:own:format_rows:second:prepared" }], false],
			["c", [{ type: "text", text: "hook:own:read:third:prepared" }], false], ["d", [{ type: "text", text: "hook:own:late_rows:fourth:prepared" }], false],
			["blocked", [{ type: "text", text: "blocked-on-purpose" }], true],
		]);
		expect(session.agent.prepareNextTurnWithContext).toBe(originalNext);
		expect(events.filter(event => event.type === "actor_action")).toHaveLength(enabled ? 10 : 0);
		await session.reload(); modelRuntime.registerNativeProvider(actor.provider);
		const entered = deferred(), scopes: (string | undefined)[] = [], otherActor = fauxProvider({ provider: "override", models: [{ id: "actor" }] });
		const base: AgentTool = { ...read, execute: async function (id, _args, signal) {
			expect(this).toBe(base); scopes.push(currentExecutionScope()?.sessionID);
			if (id === "cancel") { entered.resolve(); await new Promise<void>(resolve => signal!.addEventListener("abort", () => resolve(), { once: true })); }
			return { content: [{ type: "text", text: "base override" }], details: {} };
		} };
		const loader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [extension] });
		await loader.reload(); modelRuntime.registerNativeProvider(otherActor.provider);
		const other: AgentSession = new AgentSession({ cwd, modelRuntime, settingsManager, resourceLoader: loader, sessionManager: SessionManager.inMemory(cwd),
			initialActiveToolNames: ["read"], allowedToolNames: ["read"], baseToolsOverride: { read: base }, agent: new Agent({
				initialState: { model: otherActor.getModel() }, convertToLlm, streamFn: modelRuntime.streamSimple.bind(modelRuntime),
				transformContext: messages => other.extensionRunner.emitContext(messages),
			}) });
		await using _other = onExit(async () => { await other.abort(); await other.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); other.dispose(); });
		await other.bindExtensions({ mode: "print" });
		otherActor.setResponses([fauxAssistantMessage(fauxToolCall("read", { path: "notes.txt" }, { id: "cancel" }), { stopReason: "toolUse" })]);
		const cancelled = other.prompt("Read until cancelled.");
		await Promise.race([entered.promise, cancelled.then(() => { throw new Error("cancelled tool never entered"); })]); await other.abort(); await cancelled;
		actor.setResponses([response("format_rows", { text: "reloaded" }), fauxAssistantMessage("done")]);
		otherActor.setResponses([response("read", { path: "notes.txt" }), fauxAssistantMessage("done")]);
		await Promise.all([session.prompt("Execute after reloading extensions."), other.prompt("Read through the supplied base implementation.")]);
		expect(other.messages.filter(message => message.role === "toolResult").at(-1)?.content).toEqual([{ type: "text", text: "base override" }]);
		expect(scopes).toEqual([other.sessionId, other.sessionId]);
		expect(calls).toHaveLength(5);
		const failure = new Error("process launch failed"), coordinator = new ProcessExecutionCoordinator({ execute: async () => ({ exitCode: 0 }) });
		await coordinator.runWith({ execute: () => { throw failure; } }, async () => {
			for (const execute of [() => coordinator.execute({ command: "", cwd, environment: {}, onData: () => {} }), () => coordinator.operations.exec("", cwd, { onData: () => {} })]) await expect(execute()).rejects.toBe(failure);
		});
	} finally {
		publication?.close();
		await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); session.dispose();
		expect(session.agent.prompt).toBe(originalPrompt); expect(session.agent.continue).toBe(originalContinue);
	}
});
