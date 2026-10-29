import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { execFileSync, spawn } from "node:child_process";
import { fauxAssistantMessage, fauxProvider, fauxToolCall, fauxText } from "@earendil-works/pi-ai";
import { AgentSession, convertToLlm, createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, type ExtensionAPI, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { currentExecutionScope } from "../src/execution-world.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import type { ProcessExecutionBinding, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import * as operationPreparation from "../src/execution-preparation.ts";
import type { PredictionSettlement } from "../src/settlement.ts";
import { SynchronousProcessObserver } from "../src/linux-process-sync.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import { commitBenchmarkFixture, compileBenchmarkHelper, holdOwnedProcessPublication, inspectProcessOwner } from "./linux-process-fixture.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { readText, temporaryDirectories } from "./filesystem.ts";
import { idleResources } from "./agent-system-fixture.ts";
import { Agent, type AgentTool } from "@earendil-works/pi-agent-core";
import { deferred, onExit } from "./async.ts";
import { toolUse as response } from "./model.ts";
import { storeMetadata, storePeer } from "./process-fixture.ts";
import { digestObject, sha256Digest } from "../src/stable-value-hash.ts";

const adoptions = (settlement: PredictionSettlement) => settlement.observation === "observed" && settlement.match.matched && settlement.match.adoption.status === "adopted" ? [{ prediction: settlement.prediction, relation: settlement.match.relation }] : [];

const directories = temporaryDirectories("pi-extension-system-");
afterEach(async () => { vi.unstubAllEnvs(); vi.restoreAllMocks(); await directories.dispose(); });

test.skipIf(process.platform !== "linux").each(["reuse", "stale", "environment", "outside-mount", "disabled", "routing-off"])(
	"consumes finished Bash through the default Pi extension (%s)", { timeout: 60_000 }, async mode => {
	const cwd = await directories.create(os.homedir()), agentDir = await directories.create(os.homedir());
	vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	vi.stubEnv("PWD", mode === "outside-mount" ? "/proc" : cwd);
	vi.stubEnv("BASH_TEST_VERSION", "first");
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({ enabled: true, drafterEnabled: false,
		reuseEnabled: mode !== "disabled", prepareObservedOperations: true, patternAware: { enabled: false },
		executionRouting: { nativeFallback: mode !== "routing-off" }, predictionTimeoutMs: 30_000 }));
	await writeFile(path.join(cwd, "main.c"), '#include <stdio.h>\nint main(void){puts("built:42");return 0;}\n');
	await writeFile(path.join(cwd, "check.py"), 'print(open("value").read(), end="")\n');
	await writeFile(path.join(cwd, "value"), "one\n");
	const command = 'cc main.c -o app && ./app && python3 -B check.py; printf "%s\\n" "$BASH_TEST_VERSION"';
	expect(execFileSync("/bin/bash", ["-c", command], { cwd, encoding: "utf8" })).toBe("built:42\none\nfirst\n");
	const nativeEntries = ["completedReplayExecutor", "refreshNativeObservation"] as const;
	const forbidden = nativeEntries.map(entry => vi.spyOn(LinuxProcessReuseBackend.prototype, entry).mockImplementation(() => { throw new Error(`unsupported entry: ${entry}`); }));
	const events: SpeculativeActionEvent<string>[] = [], fallbacks: string[] = [];
	let host!: ReturnType<typeof createSpeculativeActionHost>, turn!: Parameters<typeof host.startTurn>[0];
	const extension = createSpeculativeActionExtension({ createHost: (id, options) => {
		host = createSpeculativeActionHost(id, { ...options, resources: idleResources(2), onEvent: event => { events.push(event); return options.onEvent?.(event); } });
		return { ...host, startTurn: (input, signal) => { turn = input; return host.startTurn(input, signal); },
			execute: (input, signal, executor) => host.execute(input, signal, operation => { fallbacks.push(input.tool); return executor(operation); }) };
	} });
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const loader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [extension] });
	await loader.reload();
	const actor = fauxProvider({ provider: "default-bash", models: [{ id: "actor" }], tokensPerSecond: 4_000 });
	const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
	modelRuntime.registerNativeProvider(actor.provider);
	const { session } = await createAgentSession({ cwd, agentDir, model: actor.getModel(), modelRuntime, settingsManager, resourceLoader: loader,
		sessionManager: SessionManager.inMemory(cwd), tools: ["bash"] });
	await using _session = onExit(async () => { await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); });
	await session.bindExtensions({ mode: "print", onError: error => { throw error; } });
	actor.setResponses([async () => {
		await host.previewActorCall({ turnID: turn.turnID, id: "bash-default", tool: "bash", args: { command }, tools: turn.tools });
		if (!["disabled", "routing-off"].includes(mode)) await expect.poll(() => events.some(event => event.type === "candidate" && event.state.status === "succeeded"), { timeout: 30_000 }).toBe(true);
		if (mode === "stale") await writeFile(path.join(cwd, "value"), "two\n");
		if (mode === "environment") vi.stubEnv("BASH_TEST_VERSION", "second");
		return response("bash", { command }, { id: "bash-default" });
	}, fauxAssistantMessage("done")]);
	await session.prompt("Build the program and run the Python check.");
	const result = session.messages.find(message => message.role === "toolResult");
	expect(result, JSON.stringify(events)).toMatchObject({ isError: false, content: [fauxText(`built:42\n${mode === "stale" ? "two" : "one"}\n${mode === "environment" ? "second" : "first"}\n`)] });
	const settled = events.filter(event => event.type === "actor_action").at(-1)!;
	expect(fallbacks, JSON.stringify(events)).toEqual(mode === "reuse" ? [] : ["bash"]);
	// A rejected whole branch can still consume unchanged, independently validated child work.
	if (["reuse", "environment", "disabled", "routing-off"].includes(mode))
		expect((settled.computation?.hiddenComputeMs ?? 0) > 0).toBe(mode === "reuse");
	if (mode === "reuse") expect(settled.settlement.provider).toMatchObject({ kind: "actor", origin: "preview" });
	for (const entry of forbidden) expect(entry).not.toHaveBeenCalled();
});

test.each(["off", "on", ...(process.platform === "linux" ? ["", "-nested", "-stale", "-running", "-retired", "-unkeyed", "-inputs", "-inputs-stale", "-sync-producer", "-sync-consumer", "-sync-both", "-sync-stale", "-sync-lost", "-sync-inputs", "-sync-inputs-stale", "-sync-inputs-preempted", "-sync-inputs-preempted-stale", "-cold", "-cold-stale", "-sync-cold", "-sync-cold-stale", "-continuation", "-continuation-stale", "-continuation-cancel", "-continuation-cold", "-continuation-cold-stale", "-continuation-cold-session", "-continuation-cold-damaged", "-continuation-cold-context", "-continuation-cold-lost"].map(mode => `native${mode}`) : [])])("runs Pi session contracts through the default entry (%s)", { timeout: 60_000 }, async mode => {
	const enabled = mode !== "off", native = mode.startsWith("native"), running = mode === "native-running" || mode === "native-retired";
	const live = mode.startsWith("native-continuation");
	const restart = live && mode.includes("cold"), phase = restart ? process.env.PI_TEST_CONTINUATION_PHASE : undefined;
	if (restart && !phase) {
		const root = await directories.create(os.homedir());
		for (const current of ["produce", "consume", ...(mode === "native-continuation-cold" ? ["repeat"] : [])]) {
			if (current === "consume" && /damaged|context|lost/.test(mode)) {
				const saved = JSON.parse(await readText(root, "produced.json"));
				if (mode.endsWith("damaged")) { const bytes = await readFile(saved.imagePath); bytes[0] ^= 1; await writeFile(saved.imagePath, bytes); }
				if (mode.endsWith("context")) storeMetadata(saved.cache, database => {
					const { id: _id, ...body } = saved.record; body.context = sha256Digest("a different boot or helper build");
					database.prepare("UPDATE process_checkpoints SET value=? WHERE ticket=?").run(JSON.stringify({ ...body, id: digestObject(body) }), body.ticket);
				});
				if (mode.endsWith("lost")) {
					const peer = storePeer(saved.cache, saved.record.certificate, { ticket: saved.record.ticket, action: "hold" });
					try { expect(await peer.noticed).toBe("claimed"); } finally { peer.child.kill("SIGKILL"); await peer.completed.catch(() => {}); }
				}
			}
			await new Promise<void>((resolve, reject) => {
			const child = spawn(process.execPath, ["node_modules/vitest/vitest.mjs", "--run", "test/extension.system.test.ts", "--testNamePattern", `^runs Pi session contracts through the default entry \\(${mode}\\)$`, "--maxWorkers=1", "--no-file-parallelism"], {
				env: { ...process.env, PI_TEST_CONTINUATION_PHASE: current, PI_TEST_CONTINUATION_ROOT: root }, stdio: ["ignore", "pipe", "pipe"] });
			let output = ""; for (const stream of [child.stdout, child.stderr]) stream.on("data", bytes => { output += bytes; });
			child.once("error", reject); child.once("close", code => code ? reject(new Error(`${current}: ${output}`)) : resolve());
			});
		}
		const produced = JSON.parse(await readText(root, "produced.json")), consumed = JSON.parse(await readText(root, "consumed.json"));
		expect(produced.pid).not.toBe(consumed.pid); expect(produced.session === consumed.session).toBe(!mode.endsWith("session"));
		return;
	}
	const consuming = restart && phase !== "produce";
	const stale = mode.endsWith("stale"), inputs = mode.includes("inputs"), cold = mode.includes("cold"), preempted = mode.includes("preempted"), synchronous = (callID: string) => mode.startsWith("native-sync") &&
		(!mode.endsWith("producer") && !mode.endsWith("consumer") || callID === (mode.endsWith("producer") ? "seed" : "check"));
	const cwd = restart ? process.env.PI_TEST_CONTINUATION_ROOT! : await directories.create(native ? os.homedir() : undefined), agentDir = path.join(cwd, "agent");
	await mkdir(agentDir, { recursive: true }); vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	if (!consuming) await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\n");
	const tools = native ? ["produce_notes", "format_rows", "write", "read"] : ["read", "write", "edit", "format_rows", "late_rows"];
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({ enabled, drafterEnabled: false,
		patternAware: { enabled: false }, prepareObservedOperations: native && !consuming, candidateLimit: preempted ? 2 : undefined,
		maxConcurrentActions: 4, predictionTimeoutMs: 10_000 }));
	const actor = fauxProvider({ provider: "functional-actor", models: [{ id: "actor" }], tokensPerSecond: 4_000, tokenSize: { min: 1, max: 2 } });
	actor.setResponses([
		response("read", { path: "notes.txt" }),
		response("write", { path: "result.txt", content: "first\n" }),
		response("edit", { path: "result.txt", edits: [{ oldText: "first", newText: "final" }] }),
		response("read", { path: "result.txt" }), fauxAssistantMessage("done"),
	]);
	let api: ExtensionAPI, registered = false;
	const calls: string[] = [], prepared: string[] = [], hooks: string[] = [], updates: string[] = [];
	const updated = { b: deferred(), d: deferred() };
	const callback = (name: string): ToolDefinition => ({ name, label: name, description: "Formats its own rows", executionMode: "parallel",
		parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
		prepareArguments: input => { const text = (input as { text: string }).text; prepared.push(text);
			return { text: text + (native ? "" : ":prepared"), ...(mode === "native-unkeyed" && name === "format_rows" ? { opaque: 1n } : {}) }; },
		async execute(callID, args, signal, onUpdate, context) {
			expect(this.name).toBe(name); expect(currentExecutionScope()).toMatchObject({ sessionID: context.sessionManager.getSessionId(), callID });
			expect(signal).toBe(context.signal); expect(Object.isFrozen(args)).toBe(false); calls.push(callID);
			if (!native && !registered) { registered = true; api.registerTool(callback("read")); api.registerTool(callback("late_rows")); }
			if (callID === "a" || callID === "c") await updated[callID === "a" ? "b" : "d"].promise;
			await new Promise(resolve => setImmediate(resolve));
			const text = (args as { text: string }).text, options = { cwd, env: { PATH: "/usr/local/bin:/usr/bin:/bin", HOME: os.homedir(), LANG: "C.UTF-8" }, stdio: ["ignore", "pipe", "pipe"] as ["ignore", "pipe", "pipe"] };
			const output = synchronous(callID) ? execFileSync("/bin/bash", ["-c", text], { ...options, encoding: "utf8" }) : native ? await new Promise<string>((resolve, reject) => {
				const child = spawn("/bin/bash", ["-c", text], { ...options, signal });
				let output = ""; for (const stream of [child.stdout, child.stderr]) stream.on("data", bytes => { output += bytes; });
				child.once("error", reject); child.once("close", code => code ? reject(new Error(`callback exited ${code}`)) : resolve(output));
			}) : text;
			onUpdate?.({ content: [fauxText("partial:" + callID)], details: { callID } });
			if (callID === "b" || callID === "d") updated[callID].resolve();
			return { content: [fauxText("own:" + name + ":" + output)], details: { callID } };
		},
	});
	const events: SpeculativeActionEvent<string>[] = [];
	const served: [string, boolean][] = [], prepare = operationPreparation.createExecutionPreparation;
	vi.spyOn(operationPreparation, "createExecutionPreparation").mockImplementation((...args) => {
		const owner = prepare(...args);
		return { ...owner, connect: runtime => {
			const disconnect = owner.connect(runtime), unsubscribe = runtime.subscribe({ prediction_feedback: feedback => {
				if (feedback.settlement.prediction.source === owner.source.id && adoptions(feedback.settlement).length)
					served.push([feedback.settlement.prediction.id, feedback.served]);
			} });
			return () => { disconnect(); unsubscribe(); };
		} };
	});

	let qualificationHost: ReturnType<typeof createSpeculativeActionHost>;
	const extension = createSpeculativeActionExtension({
		createHost: (id, options) => qualificationHost = createSpeculativeActionHost(id, {
		...options, resources: { initial: { cpuCount: 4, idleCpuCount: 4 }, sample: () => new Promise(() => {}) },
		onEvent: event => { events.push(event); return options.onEvent?.(event); },
	}) });
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [pi => {
		api = pi; pi.registerTool(callback("format_rows")); if (native) pi.registerTool(callback("produce_notes"));
		pi.on("tool_call", event => { if (!("text" in event.input)) return; hooks.push("before:" + event.toolCallId);
			if (event.toolCallId === "blocked") return { block: true, reason: "blocked-on-purpose" }; });
		pi.on("tool_result", event => { if (!event.input.text) return; hooks.push("after:" + event.toolCallId);
			return { content: [fauxText("hook:" + (event.content[0] as { text: string }).text)] }; });
	}, extension] });
	await resourceLoader.reload();
	const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
	modelRuntime.registerNativeProvider(actor.provider);
	const sessionManager = !restart ? SessionManager.inMemory(cwd) : phase === "produce" || mode.endsWith("session") ? SessionManager.create(cwd, path.join(agentDir, "sessions")) :
		SessionManager.open(JSON.parse(await readText(cwd, "produced.json")).file);
	const { session } = await createAgentSession({ cwd, agentDir, model: actor.getModel(), modelRuntime, settingsManager, resourceLoader, sessionManager, tools });
	const originalPrompt = session.agent.prompt, originalContinue = session.agent.continue;
	const toolResults = (owner = session) => owner.messages.filter(message => message.role === "toolResult");
	let inspection: ReturnType<typeof inspectProcessOwner> | undefined;
	let inspectionReady: Promise<void> | undefined;
	let publication: Awaited<ReturnType<typeof holdOwnedProcessPublication>> | undefined;
	await using _session = onExit(async () => {
		for (const gate of Object.values(updated)) gate.resolve();
		if (preempted) await inspection?.inOwner(state => state.publishedGate?.resolve()); await publication?.close();
		await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); inspection?.dispose();
		expect(session.agent.prompt).toBe(originalPrompt); expect(session.agent.continue).toBe(originalContinue);
		expect(events.filter(event => event.type === "candidate" && event.candidate.source === "pattern_aware")).toEqual([]);
	});
	await session.bindExtensions({ mode: "print", onError: error => { throw error; } });
	if (native) {
		let backend: LinuxProcessReuseBackend | undefined;
		const ownerPrototype = LinuxProcessReuseBackend.prototype as unknown as { processOwner(): SynchronousProcessObserver };
		const processOwner = ownerPrototype.processOwner;
		vi.spyOn(ownerPrototype, "processOwner").mockImplementation(function (this: LinuxProcessReuseBackend) {
			if (!inspection) {
				inspection = inspectProcessOwner(() => processOwner.call(this));
				if (live) inspectionReady = inspection.inOwner(async (state, delayedSettlement) => {
					state.errors = state.method((await state.load("../src/linux-process-backend.ts")).LinuxProcessReuseBackend.prototype, "setError");
					state.publications = state.method((await state.load("../src/process-handoff.ts")).ProcessHandoffRegistry.prototype, "publish");
					if (delayedSettlement) {
						const Store = (await state.load("../src/reuse-store.ts")).ProvenanceCertificateStore, claim = Store.prototype.claimProcessCheckpoint;
						state.method(Store.prototype, "claimProcessCheckpoint", async function (this: unknown, ...args: unknown[]) {
							const settle = await claim.apply(this, args); return settle && (async (consumed: boolean) => {
								await new Promise(resolve => setTimeout(resolve, 25)); await settle(consumed); state.settledAt = performance.now();
							});
						});
						const Backend = (await state.load("../src/linux-process-backend.ts")).LinuxProcessReuseBackend, decide = Backend.prototype.decideHeldExec;
						state.method(Backend.prototype, "decideHeldExec", async function (this: unknown, ...args: unknown[]) {
							const decision = await decide.apply(this, args); return decision.kind !== "replay" ? decision : { ...decision,
								observeTiming: (timing: unknown) => { state.timing = timing; decision.observeTiming?.(timing); } };
						});
					}
				}, mode === "native-continuation");
			}
			return processOwner.call(this);
		});
		let inputBinding: ProcessExecutionBinding | undefined;
		if (mode === "native-sync-lost") {
			const attach = SynchronousProcessObserver.prototype.attach;
			vi.spyOn(SynchronousProcessObserver.prototype, "attach").mockImplementation(function (this: SynchronousProcessObserver, request) {
				const attachment = attach.call(this, request);
				return { ...attachment, finish: () => { const result = attachment.finish();
					if (result.adoptions.length) { expect(result.metrics.hits).toBe(1); expect(Atomics.load(new Int32Array(request.claims), 0)).toBeGreaterThan(0);
						void Reflect.get(this, "connection").worker.terminate(); throw new Error("lost acknowledged synchronous handoff"); }
					return result;
				} };
			});
		}
		const observe = LinuxProcessReuseBackend.prototype.observeBindings, nested = !live && !cold && !inputs && !mode.startsWith("native-sync") && !["native", "native-stale"].includes(mode);
		vi.spyOn(LinuxProcessReuseBackend.prototype, "observeBindings").mockImplementation(function (this: LinuxProcessReuseBackend, scope, execute, receive, ...args) {
			backend = this; return observe.call(this, scope, execute, (bindings, computations) => {
				if (scope.callID === "seed") {
					const registry = Reflect.get(this, "handoffs") as ProcessHandoffRegistry<{ readonly executable: string }>;
					inputBinding = bindings.find(binding => registry.resolveBinding(binding, scope)?.executable === path.join(cwd, live ? "worker" : "slow"));
				}
				return receive(nested && scope.callID === "seed" ? bindings.slice(0, 1) : bindings, computations);
			}, ...args);
		});
		if (live) {
			if (!consuming) {
			await compileBenchmarkHelper(cwd, "worker", { code: `#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
int main(void) {
 char input[3]; if (read(3, input, 1) != 1) return 71;
 volatile uint64_t hash = 1; for (unsigned i = 0; i < 250000000; i++) hash = hash * 33 + i;
 if (write(4, "R", 1) != 1) return 72;
 for (int i = 1; i < 3; i++) if (read(3, input + i, 1) != 1) return 73;
 printf("%c%c%c:%llu\\n", input[0], input[1], input[2], (unsigned long long)hash); return 37;
}` });
			await compileBenchmarkHelper(cwd, "launch", { code: `#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
int main(int argc, char **argv) {
 int input[2], ack[2], start[2]; if (argc != 2 || pipe(input) || pipe(ack) || pipe(start)) return 61;
 if (write(input[1], argv[1][0] == 'c' ? "D" : "A", 1) != 1) return 62;
 int child = fork(); if (child < 0) return 63;
 if (!child) { close(start[1]); char go; if (read(start[0], &go, 1) != 1 || close(start[0])) _exit(64);
  int a = fcntl(input[0], F_DUPFD_CLOEXEC, 10), b = fcntl(ack[1], F_DUPFD_CLOEXEC, 10);
  if (a < 0 || b < 0 || dup2(a, 3) != 3 || dup2(b, 4) != 4) _exit(64);
  close_range(5, ~0U, 0); execl("./worker", "worker", NULL); _exit(65); }
 close(input[0]); close(ack[1]); close(start[0]);
 if (write(start[1], "S", 1) != 1 || close(start[1])) return 66;
 char ready; if (read(ack[0], &ready, 1) != 1 || ready != 'R') return 67;
 if (write(input[1], argv[1][0] == 's' ? "BC" : "XY", 2) != 2) return 68;
 close(input[1]); close(ack[0]); int status;
 return waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 37;
}` });
			await commitBenchmarkFixture(cwd, "default-live-continuation");
			}
			const availableImage = () => inspection!.inOwner(state => state.publications.mock.calls.some((call: any) => {
				const [key, work, certificate, , continuation] = call.arguments;
				return certificate?.result.continuation && continuation?.image.length > 0 && call.this.results(key, work.scope).some((current: any) => current.id === certificate.id);
			}));
			let preparationError: unknown;
			const checkID = phase === "repeat" ? "check-again" : "check";
			const actual = response("format_rows", { text: `./launch ${stale ? "changed" : "actual"}; printf once >> marker` }, { id: checkID });
			actor.setResponses([...(consuming ? [actual] : [response("produce_notes", { text: "./launch seed; :" }, { id: "seed" }), async () => {
				try {
				expect(inputBinding).toBeDefined();
				await inspectionReady;
				await expect.poll(availableImage, { timeout: 10_000 }).toBe(true);
				if (mode.endsWith("cancel")) await qualificationHost.runtime.disposeSession(qualificationHost.sessionID);
				return phase === "produce" ? fauxAssistantMessage("prepared") : actual;
				} catch (error) { preparationError = error; throw error; }
			}]), fauxAssistantMessage("done")]);
			await session.prompt("Prepare the calculation, then finish it using the new input.");
			if (preparationError) throw preparationError;
			if (phase === "produce") {
				expect(calls).toEqual(["seed"]);
				const checkpoint = await inspection!.inOwner(async state => {
					const call = state.publications.mock.calls.find((call: any) => call.arguments[4]?.checkpoint), store = Reflect.get(call.this, "store");
					for await (const record of store.processCheckpoints(call.arguments[0], call.arguments[2].prototype.executablePath, call.arguments[1].scope.sessionID))
						if (record.ticket === call.arguments[4].checkpoint) return { cache: store.root, record, imagePath: store.artifacts.artifactPath(record.image.digest) };
					throw new Error("published checkpoint is missing");
				});
				await writeFile(path.join(cwd, "produced.json"), JSON.stringify({ pid: process.pid, session: sessionManager.getSessionId(), file: sessionManager.getSessionFile(), ...checkpoint }));
				return;
			}
			const results = toolResults(), reused = !stale && !mode.endsWith("cancel") && (!restart || mode === "native-continuation-cold" && phase !== "repeat");
			expect(results, JSON.stringify({ messages: session.messages, events, metrics: backend?.actorMetrics() })).toHaveLength(mode.endsWith("session") ? 1 : phase === "repeat" ? 3 : 2);
			expect(results.every(result => result.isError === false)).toBe(true);
			if (!mode.endsWith("session")) expect(results[0]!.content).toEqual([fauxText("hook:own:produce_notes:ABC:7107042143181080257\n")]);
			expect(results.at(-1)!.content).toEqual([fauxText(`hook:own:format_rows:${stale ? "D" : "A"}XY:7107042143181080257\n`)]);
			expect(calls).toEqual(consuming ? [checkID] : ["seed", "check"]); expect(await readText(cwd, "marker")).toBe(phase === "repeat" ? "onceonce" : "once");
			expect(backend!.actorMetrics().hits, JSON.stringify({ metrics: backend!.actorMetrics(), owner: inspection && await inspection.inOwner(state => ({
				errors: state.errors.mock.calls.map((call: any) => call.arguments[1]),
				images: state.publications.mock.calls.flatMap((call: any) => { const [key, work, certificate] = call.arguments;
					return certificate?.result.continuation ? [{ key, scope: work.scope, aborted: work.signal.aborted }] : []; }) })), events })).toBe(Number(reused));
			expect(events.filter(event => event.type === "actor_action").at(-1)!.computation!.hiddenComputeMs > 0).toBe(reused);
			if (reused) await expect.poll(availableImage).toBe(false);
			if (mode === "native-continuation") expect(await inspection!.inOwner(state => state.timing.completedAt >= state.settledAt)).toBe(true);
			if (phase === "consume") await writeFile(path.join(cwd, "consumed.json"), JSON.stringify({ pid: process.pid, session: sessionManager.getSessionId() }));
			return;
		}
		await compileBenchmarkHelper(cwd, "slow", { code: "#include <stdio.h>\n#include <fcntl.h>\n#include <unistd.h>\nint main(int n,char**v){int f=fcntl(3,F_GETFD)<0?open(v[1],O_RDONLY):3;if(f<0)return 1;unsigned long h=5381;char c;while(read(f,&c,1)==1)h=h*33+c;close(f);for(volatile unsigned long i=0;i<40000000ul;i++)h^=i;printf(\"%s %lx\\n\",v[1],h);return 0;}\n" });
		await commitBenchmarkFixture(cwd, "sdk-callback");
		const command = "./slow notes.txt" + (inputs ? " 3<notes.txt" : "");
		const initial = `printf initial:; ${command}; :`, remaining = `printf remaining:; ${command}; printf once >> marker`;
		const call = (tool: string, text: string, id: string) => response(tool, { text }, { id });
		actor.setResponses([call("produce_notes", initial, "seed"), ...(inputs ? [response("read", { path: "notes.txt", offset: 2 })] : []), ...cold ? [] : [async () => {
			if (running) publication = await holdOwnedProcessPublication(inspection!);
			if (preempted) {
				await inspection!.inOwner(async state => {
					const { ProvenanceCertificateStore } = await state.load("../src/reuse-store.ts"), publish = ProvenanceCertificateStore.prototype.withPublication;
					state.publishedGate = state.deferred();
					state.method(ProvenanceCertificateStore.prototype, "withPublication", async function (this: unknown, ...args: unknown[]) {
						const result = await publish.apply(this, args); await state.publishedGate.promise; return result;
					});
				});
			}
			return response("write", { path: "notes.txt", content: "changed\n" });
		}], ...(mode === "native-retired" ? [async () => {
			await publication!.ready(); session.setActiveToolsByName(tools.filter(name => name !== "produce_notes"));
			return response("read", { path: "notes.txt" });
		}] : []), async () => {
			expect(inputBinding).toBeDefined();
			if (publication) await publication.ready(); else await expect.poll(() => backend!.bindingStale(inputBinding!), { timeout: 5000 }).toBe(false);
			await expect(readFile(path.join(cwd, "marker"))).rejects.toMatchObject({ code: "ENOENT" });
			if (stale) await writeFile(path.join(cwd, "notes.txt"), "fresh\n");
			return call("format_rows", remaining, "check");
		}, fauxAssistantMessage("done")]);
		await session.prompt("Produce notes, edit the input, then render the remaining work.");
		// A named inherited input prepares a new version after the edit; adoption still checks the actual held graph.
		const results = toolResults(), reused = !stale;
		if (mode === "native-sync-lost") {
			expect(results.at(-1)).toMatchObject({ isError: true }); expect(JSON.stringify(results.at(-1))).toContain("native handoff failed");
			expect(await readText(cwd, "marker")).toBe("once"); expect(calls).toEqual(["seed", "check"]); return;
		}
		expect(results.at(-1)?.content, JSON.stringify({ messages: session.messages, events, worlds: await qualificationHost!.executionWorldDiagnostics() })).toEqual([fauxText("hook:own:format_rows:remaining:" + execFileSync("./slow", ["notes.txt"], { cwd, encoding: "utf8" }))]);
		expect(results.every(result => !result.isError)).toBe(true); expect(await readText(cwd, "marker")).toBe("once");
		expect(calls).toEqual(["seed", "check"]); expect(prepared).toEqual([initial, remaining]);
		if (inputs) expect(summarizeSpeculativeTrace(events).inputReuseHits).toBe(1);
		const settled = events.filter(event => event.type === "actor_action").at(-1)!, timing = settled.computation!;
		expect(backend!.actorMetrics().hits).toBe(Number(reused)); expect(timing.hiddenComputeMs > 0, JSON.stringify({ timing, reuse: backend!.actorMetrics() })).toBe(reused);
		expect((timing.hiddenByMode ?? []).reduce((total, part) => total + part.hiddenComputeMs, 0)).toBeCloseTo(timing.hiddenComputeMs, 5);
		const predictions = events.filter(event => event.type === "operation_prediction"), adopted = predictions.flatMap(event => adoptions(event.settlement));
		if (preempted && reused) expect(events.some(event => event.type === "candidate" && event.state.status === "cancelled" && event.state.cause.code === "scheduler_preempted")).toBe(true);
		const credit = new Map(served), exact = { child: true, kind: "exact", distance: 0, served: true },
			owned = { child: inputs, kind: "projected", distance: 1, projector: "owned-operation", served: true };
		expect(served.map(([id]) => id).sort()).toEqual(adopted.map(item => item.prediction.id).sort());
		expect(!reused || mode === "native-retired" ? [[]] : nested || inputs ? [[owned]] : [[exact], [{ ...exact, served: false }, owned]])
			.toContainEqual(adopted.map(({ prediction, relation }) => ({ child: prediction.actionID.endsWith(inputBinding!.key), ...relation,
				served: credit.get(prediction.id) })).sort((a, b) => a.kind.localeCompare(b.kind)));
		if (mode === "native-retired") expect(predictions.filter(event => event.settlement.prediction.id === predictions[0]!.settlement.prediction.id)).toMatchObject([{ settlement: { observation: "unobserved", cause: { code: "operation_not_observed" } } }]);
		expect(!!settled.settlement.actionKeyHash).toBe(mode !== "native-unkeyed");
		if (publication) expect(await publication.joined()).toBe(true);
		expect(events.filter(event => event.type === "candidate" && event.state.status === "cancelled" && event.state.cause.code === "tool_disabled" && event.state.executionMs > 0)).toEqual([]);
		return;
	}
	const registeredTools = session.agent.state.tools;
	await session.prompt("Read notes.txt, write and edit result.txt, then read the final file.");
	expect(session.agent.state.tools).toBe(registeredTools);
	expect(await readText(cwd, "notes.txt")).toBe("one\ntwo\n");
	expect(await readText(cwd, "result.txt")).toBe("final\n");
	const results = toolResults();
	expect(results).toHaveLength(4);
	expect(results.every(message => !message.isError)).toBe(true);
	expect(results.at(-1)?.content).toEqual([fauxText("final\n")]);
	const registry = session.agent.state.tools, read = registry.find(tool => tool.name === "read")!, originalExecute = read.execute;
	for (const copy of [true, false]) {
		const tool = copy ? { ...read } : read;
		tool.execute = async function (id) { expect(this).toBe(tool); expect(currentExecutionScope()?.callID).toBe(id); return { content: [fauxText("replaced")], details: {} }; };
		session.agent.state.tools = registry.map(current => current === read ? tool : current);
		actor.setResponses([response("read", { path: "notes.txt" }), fauxAssistantMessage("done")]);
		await session.prompt("Execute the current read implementation.");
		expect(toolResults().at(-1)?.content).toEqual([fauxText("replaced")]);
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
	expect([calls.slice(0, 2).sort(), calls.slice(2).sort()]).toEqual([["a", "b"], ["c", "d"]]); expect(updates).toEqual(["b", "a", "d", "c"]);
	expect(hooks.slice(0, 2)).toEqual(["before:a", "before:b"]); expect(hooks.slice(2, 4).sort()).toEqual(["after:a", "after:b"]);
	expect(hooks.slice(4, 7)).toEqual(["before:c", "before:d", "before:blocked"]); expect(hooks.slice(7).sort()).toEqual(["after:c", "after:d"]);
	expect(toolResults().slice(-5).map(message => [message.toolCallId, message.content, message.isError])).toEqual([
		["a", [fauxText("hook:own:format_rows:first:prepared")], false], ["b", [fauxText("hook:own:format_rows:second:prepared")], false],
		["c", [fauxText("hook:own:read:third:prepared")], false], ["d", [fauxText("hook:own:late_rows:fourth:prepared")], false],
		["blocked", [fauxText("blocked-on-purpose")], true],
	]);
	expect(session.agent.prepareNextTurnWithContext).toBe(originalNext);
	expect(events.filter(event => event.type === "actor_action")).toHaveLength(10);
	await session.reload(); modelRuntime.registerNativeProvider(actor.provider);
	const entered = deferred(), scopes: (string | undefined)[] = [], otherActor = fauxProvider({ provider: "override", models: [{ id: "actor" }] });
	const base: AgentTool = { ...read, execute: async function (id, _args, signal) {
		expect(this).toBe(base); scopes.push(currentExecutionScope()?.sessionID);
		if (id === "cancel") { entered.resolve(); await new Promise<void>(resolve => signal!.addEventListener("abort", () => resolve(), { once: true })); }
		return { content: [fauxText("base override")], details: {} };
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
	otherActor.setResponses([response("read", { path: "notes.txt" }, { id: "cancel" })]);
	const cancelled = other.prompt("Read until cancelled.");
	await Promise.race([entered.promise, cancelled.then(() => { throw new Error("cancelled tool never entered"); })]); await other.abort(); await cancelled;
	actor.setResponses([response("format_rows", { text: "reloaded" }), fauxAssistantMessage("done")]);
	otherActor.setResponses([response("read", { path: "notes.txt" }), fauxAssistantMessage("done")]);
	await Promise.all([session.prompt("Execute after reloading extensions."), other.prompt("Read through the supplied base implementation.")]);
	expect(toolResults(other).at(-1)?.content).toEqual([fauxText("base override")]);
	expect(scopes).toEqual([other.sessionId, other.sessionId]);
	expect(calls).toHaveLength(5);
	const failure = new Error("process launch failed"), coordinator = new ProcessExecutionCoordinator({ execute: async () => ({ exitCode: 0 }) });
	await coordinator.runWith({ execute: () => { throw failure; } }, async () => {
		for (const execute of [() => coordinator.execute({ command: "", cwd, environment: {}, onData: () => {} }), () => coordinator.operations.exec("", cwd, { onData: () => {} })]) await expect(execute()).rejects.toBe(failure);
	});
});
