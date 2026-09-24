import { textResult } from "./result.ts";
import { deferred, gated, nextTurn } from "./async.ts";
import { writeFile } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import { testModel } from "./model.ts";
import path from "node:path";

import {
	type ExtensionAPI,
	type ExtensionCommandContext,
	type ExtensionContext,
	type SourceInfo,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeActionHost } from "../src/agent-integration.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import {
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import { ExecutionWorldRouter, type ExecutionWorldDiagnosticSnapshot } from "../src/execution-world.ts";
import {
	createSpeculativeActionExtension,
	formatSpeculativeActionEvent,
	type SpeculativeSettingsStore,
} from "../src/extension.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import type { ProcessExecutionRequest } from "../src/process-execution.ts";
import * as piTools from "../src/pi-tool-invocation.ts";
import type { PiToolDefinition } from "../src/pi-tool-invocation.ts";
import { SelfSpeculationCoordinator } from "../src/self-speculation.ts";
import type { SpeculativeActionPackageSettings } from "../src/settings-store.ts";
import type { ToolSettlement } from "../src/tool-settlement.ts";
import { TimelineInterval } from "../src/task-timing.ts";

const directories = temporaryDirectories("pi-spec-extension-");
const hosts: SpeculativeActionHost[] = [];

beforeEach(async () => { vi.stubEnv("PI_CODING_AGENT_DIR", await directories.create()); });
afterEach(async () => {
	await Promise.all(hosts.splice(0).map((host) => host.dispose()));
	await directories.dispose();
	vi.unstubAllEnvs();
});

describe("zero-modification Pi extension", () => {
	it("registers stock overrides, previews the stream without claiming it, then adopts once", async () => {
		const fixture = await createFixture({ reuse: { result: textResult("cached"), isError: false } });
		await fixture.emit("session_start");
		expect([...fixture.tools.keys()].sort()).toEqual(["bash", "edit", "find", "grep", "ls", "read", "write"]);
		const read = fixture.tools.get("read")!;
		expect(read).toMatchObject({ name: "read", label: "read" });
		await fixture.emit("context", { messages: [] });
		const partial = { content: [{ type: "toolCall", id: "actor-read", name: "read", arguments: {} }] };
		await fixture.emit("message_update", { assistantMessageEvent: { type: "toolcall_start", contentIndex: 0, partial } });
		expect(fixture.host.previewActorTool).toHaveBeenCalledWith({ turnID: "turn_1", tool: "read" }, undefined);
		await fixture.emit(
			"message_update",
			{ assistantMessageEvent: { type: "toolcall_delta", contentIndex: 0, delta: '{"path":', partial } },
		);
		expect(fixture.host.previewActorTool).toHaveBeenCalledOnce();
		expect(fixture.host.previewActorCall).not.toHaveBeenCalled();
		await fixture.emit(
			"message_update",
			{ assistantMessageEvent: { type: "toolcall_delta", contentIndex: 0, delta: '"notes.txt"}', partial } },
		);
		await vi.waitFor(() => expect(fixture.host.previewActorCall).toHaveBeenCalledOnce());
		await fixture.emit(
			"message_update",
			{
				assistantMessageEvent: {
					type: "toolcall_end",
					contentIndex: 0,
					toolCall: { type: "toolCall", id: "actor-read", name: "read", arguments: { path: "notes.txt" } },
					partial,
				},
			},
		);
		expect(fixture.host.previewActorCall).toHaveBeenCalledWith(
			{ turnID: "turn_1", id: "actor-read", tool: "read", args: { path: "notes.txt" }, tools: expect.any(Array) },
			undefined,
		);
		expect(fixture.host.runtime.prepareActorCall).not.toHaveBeenCalled();
		const result = await read.execute("actor-read", { path: "notes.txt" }, undefined, undefined, fixture.context);
		expect(result.content).toEqual([{ type: "text", text: "cached" }]);
		expect(fixture.host.runtime.prepareActorCall).toHaveBeenCalledOnce();
		expect(fixture.settle).not.toHaveBeenCalled();
	});

	it("leads task timing with the optimistic gain and keeps the signed net estimate beside it", () => {
		const timing = { endToEndMs: 1000, optimisticSavingsMs: 300, estimatedSavingsMs: -50, hiddenLatencyMs: 100, toolExecutionMs: 400 };
		expect(formatSpeculativeActionEvent({ type: "task", sessionID: "s", turnID: "t", timing } as never)).toContain(
			"1s wall; 300ms optimistic savings (net -50ms); End-to-End SpeedUp +30.0% (net -5.0%); Tool time speed up 25.0%; 100ms of 400ms tool time hidden");
	});

	it("sends Drafter requests as simple options through the provider with registry auth", async () => {
		const fixture = await createFixture(), message = { role: "assistant" }, streamSimple = vi.fn(() => ({ result: async () => message }));
		await fixture.emit("session_start");
		Object.assign(fixture.context.modelRegistry, { getProvider: () => ({ streamSimple }),
			getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "key", headers: { a: "1" }, baseUrl: "http://proxy" }) });
		await expect(fixture.drafterComplete(testModel("mock"), { messages: [] }, { reasoning: "low", headers: { b: "2" } })).resolves.toBe(message);
		expect(streamSimple).toHaveBeenCalledWith({ ...testModel("mock"), baseUrl: "http://proxy" }, { messages: [] },
			{ reasoning: "low", apiKey: "key", headers: { a: "1", b: "2" }, env: {} });
	});

	it("warns once per unavailable Drafter model while drafting with the active model", async () => {
		const fixture = await createFixture({ settings: { enabled: true, draftModel: "missing/model" } }), actor = testModel("actor");
		await fixture.emit("session_start");
		expect([fixture.drafterModel(actor), fixture.drafterModel(actor), fixture.drafterModel(actor)]).toEqual([actor, actor, actor]);
		expect(fixture.ui.notify.mock.calls.filter(([, level]) => level === "warning")).toEqual([["Drafter model missing/model is unavailable; drafting with the active model.", "warning"]]);
	});

	it("ends fork probing when Pi finalizes an assistant message", async () => {
		const fixture = await createFixture(), finish = vi.spyOn(SelfSpeculationCoordinator.prototype, "finishActorOutput");
		await fixture.emit("session_start");
		for (const role of ["user", "assistant"]) await fixture.emit("message_end", { message: { role } });
		expect(finish).toHaveBeenCalledOnce();
	});

	it("keeps same-name extension tools authoritative and excludes them from speculation", async () => {
		const fixture = await createFixture({ overriddenTools: ["read"] });
		const customRead = fixture.customTools.get("read") as ToolDefinition | undefined;
		await fixture.emit("session_start");

		expect(fixture.tools.has("read")).toBe(false);
		expect(fixture.actorTools.get("read")).toBe(customRead);
		await fixture.emit("context", { messages: [] });
		const turn = vi.mocked(fixture.host.startTurn).mock.calls[0]?.[0];
		expect(turn?.tools.map((tool) => tool.name)).not.toContain("read");

		const result = await customRead?.execute("actor-read", { path: "notes.txt" }, undefined, undefined, fixture.context);
		expect(result?.content).toEqual([{ type: "text", text: "custom read" }]);
		expect(fixture.host.runtime.prepareActorCall).not.toHaveBeenCalled();
		expect(fixture.settle).not.toHaveBeenCalled();

		await fixture.commands.get("speculative-action")?.handler("status", fixture.context as ExtensionCommandContext);
		expect(fixture.ui.notify).toHaveBeenLastCalledWith(
			expect.stringContaining("read (cli: custom-read.ts); excluded from speculation"),
			"warning",
		);
	});

	it.each(["cache", "telemetry"])("preserves the stock Actor result when %s fails", async (mode) => {
		const fixture = await createFixture();
		await writeFile(path.join(fixture.cwd, "notes.txt"), "authoritative", "utf8");
		if (mode === "cache") vi.mocked(fixture.host.runtime.prepareActorCall).mockRejectedValue(new Error("cache failed"));
		else fixture.settle.mockRejectedValue(new Error("telemetry failed"));
		vi.mocked(fixture.host.finishTurn).mockRejectedValue(new Error("cleanup failed"));
		await fixture.emit("session_start");
		await fixture.emit("context", { messages: [] });

		const result = await fixture.tools
			.get("read")
			?.execute("actor-read", { path: "notes.txt" }, undefined, undefined, fixture.context);
		await expect(fixture.emit("turn_end")).resolves.toBeUndefined();

		expect(result?.content).toEqual([{ type: "text", text: "authoritative" }]);
		expect(fixture.host.runtime.prepareActorCall).toHaveBeenCalledWith(expect.objectContaining({
			tool: "read", args: { path: "notes.txt" },
		}), undefined);
		if (mode === "cache") expect(fixture.settle).not.toHaveBeenCalled();
		else expect(fixture.settle).toHaveBeenCalledWith(expect.any(TimelineInterval), { result, isError: false });
	});

	it("binds only prepared searches, quietly retains native Actor otherwise, and retires on refresh or disable", async () => {
		const fixture = await createFixture({ settings: { searchExecution: "captured" } });
		vi.stubEnv("PI_CODING_AGENT_DIR", fixture.cwd);
		const prepare = vi.spyOn(piTools, "createClosedSearchProfile").mockRejectedValue(new Error("Requalify Pi's installed minimatch"));
		const native = vi.fn(async () => textResult("native find")); fixture.baseTools.get("find")!.execute = native;
		const definitions = vi.spyOn(piTools, "createPiToolDefinitions").mockReturnValue(fixture.baseTools);
		const command = (input: string) => fixture.commands.get("speculative-action")!.handler(input, fixture.context as ExtensionCommandContext);
		try {
			await fixture.emit("session_start");
			for (const tool of ["grep", "find"]) expect(await fixture.resolveInvocation(tool, {})).toBeUndefined();
			expect(prepare).not.toHaveBeenCalled();
			await command("on");
			expect(fixture.ui.notify.mock.calls.flat().join(" ")).not.toMatch(/Requalify|setup:search/u);
			expect(await fixture.resolveInvocation("find", {})).toBeUndefined();
			expect(await fixture.tools.get("find")!.execute("missing", { pattern: "x" }, undefined, undefined, fixture.context)).toEqual(textResult("native find"));
			expect(native).toHaveBeenCalledOnce();
			expect(prepare).toHaveBeenCalledOnce();
			const profile = searchProfile(), { run: authoritative, dispose } = profile.pool;
			prepare.mockResolvedValue(profile);
			await command("status"); // Refresh retires the old executor generation, not its captured inputs.
			expect((await fixture.tools.get("find")!.execute("bound", { pattern: "x" }, undefined, undefined, fixture.context)).content).toEqual(textResult("selected search").content);
			expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringMatching(/find\s+On\s+Ready\s+Ready\s+Ready/u), "info");
			expect(authoritative).toHaveBeenCalledOnce();
			expect(await fixture.resolveInvocation("grep", { pattern: "x" })).toBeUndefined();
			authoritative.mockRejectedValueOnce(new Error("unproven selected input"));
			await expect(fixture.tools.get("find")!.execute("bound-error", { pattern: "x" }, undefined, undefined, fixture.context)).rejects.toThrow("unproven selected input");
			expect(native).toHaveBeenCalledOnce(); // Binding is immutable: an admitted profile failure must not silently change semantics.
			profile.invocations.set("grep", { ...profile.invocations.get("find")!, semantics: { ...profile.invocations.get("find")!.semantics, tool: "grep" } });
			await command("status");
			expect(dispose).toHaveBeenCalledOnce();
			expect((await fixture.tools.get("grep")!.execute("grep-bound", { pattern: "x" }, undefined, undefined, fixture.context)).content).toEqual(textResult("selected search").content);
			await command("off");
			expect(dispose).toHaveBeenCalledTimes(2);
			for (const tool of piTools.PI_CLOSED_SEARCH_TOOLS) expect(await fixture.resolveInvocation(tool, {})).toBeUndefined();
			await command("on");
			vi.spyOn(fixture.store, "flush").mockRejectedValueOnce(new Error("settings persistence failed"));
			const closeHost = vi.spyOn(fixture.host, "dispose");
			await fixture.emit("session_shutdown");
			expect(closeHost).toHaveBeenCalledOnce();
			expect(dispose).toHaveBeenCalledTimes(3);
		} finally {
			await fixture.emit("session_shutdown");
			prepare.mockRestore(); definitions.mockRestore(); vi.unstubAllEnvs();
		}
	});

	it.each(["starting", "retiring", "ready", "rejected", "thrown"] as const)("owns session installation and every admitted refresh through shutdown (%s)", async (state) => {
		const fixture = await createFixture({ settings: { enabled: true, searchExecution: "captured" } });
		const profiles: ReturnType<typeof searchProfile>[] = [];
		const searchGate = gated(), actorGate = gated();
		const prepare = vi.spyOn(piTools, "createClosedSearchProfile").mockImplementation(async () => {
			if (state === "starting") await searchGate.wait();
			const profile = searchProfile(); profiles.push(profile); return profile;
		});
		const actor = vi.spyOn(LinuxProcessReuseBackend.prototype, "prepareActorReplay").mockImplementation(async () => {
			await actorGate.wait(); return { state: "unavailable", detail: "test route" };
		});
		const closeHost = vi.spyOn(fixture.host, "dispose");
		let refresh: Promise<unknown> | undefined, shutdown: Promise<void> | undefined;
		try {
			if (state !== "starting") await fixture.emit("session_start");
			if (state === "retiring") profiles[0]!.pool.dispose.mockImplementationOnce(searchGate.wait);
			else if (state !== "starting") prepare.mockImplementationOnce(async () => {
				await searchGate.wait(); const profile = searchProfile(); profiles.push(profile); return profile;
			});
			vi.mocked(fixture.host.executionWorldDiagnostics).mockImplementationOnce(() => {
				if (state === "thrown") throw new Error("diagnostics failed synchronously");
				return state === "rejected" ? Promise.reject(new Error("diagnostics rejected")) : Promise.resolve(portableDiagnostics());
			});
			let refreshed = false, closed = false;
			refresh = Promise.resolve(state === "starting" ? fixture.emit("session_start")
				: fixture.commands.get("speculative-action")!.handler("status", fixture.context as ExtensionCommandContext))
				.then(() => { refreshed = true; });
			await searchGate.entered;
			await nextTurn();
			const whileSearchPending = { refreshed, actor: actor.mock.calls.length,
				diagnostics: vi.mocked(fixture.host.executionWorldDiagnostics).mock.calls.length };
			shutdown = fixture.emit("session_shutdown").then(() => { closed = true; });
			await nextTurn();
			const whileClosing = { closed, host: closeHost.mock.calls.length };
			searchGate.release();
			await nextTurn();
			if (state !== "starting" && state !== "retiring") expect({ refreshed, closed }).toEqual({ refreshed: false, closed: false });
			actorGate.release();
			await Promise.all([refresh, shutdown]);
			const prepared = state === "starting" || state === "retiring" ? 1 : 2;
			expect(whileSearchPending).toEqual({ refreshed: false, actor: prepared - 1, diagnostics: prepared });
			expect(whileClosing).toEqual({ closed: false, host: state === "retiring" ? 1 : 0 });
			expect(prepare).toHaveBeenCalledTimes(prepared);
			expect(profiles.map((profile) => profile.pool.dispose.mock.calls.length)).toEqual(Array(prepared).fill(1));
		} finally {
			searchGate.release(); actorGate.release();
			await Promise.allSettled([refresh, shutdown]);
			await fixture.emit("session_shutdown");
			for (const profile of profiles) await profile.pool.dispose();
			prepare.mockRestore(); actor.mockRestore();
		}
	});

	it("preserves provider priority and admits only bound process tools before probing", async () => {
		const primary = {
			id: "primary_runtime", scope: "runtime", isolation: "runtime_sandbox",
			speculation: { capabilities: [] },
		} as unknown as SpeculativeAgentExecutionWorld;
		const fixture = await createFixture({ executionWorlds: [primary] });
		await fixture.emit("session_start");

		const worlds = fixture.executionWorlds();
		expect(worlds.map((world) => world.id)).toEqual(["primary_runtime", "linux_process_reuse", "git_worktree", "resource_version"]);
		const native = worlds[1]!.speculation!;
		const prepare = vi.spyOn(native, "prepare").mockResolvedValue(undefined);
		vi.spyOn(native, "fingerprint").mockReturnValue("qualified-test-process");
		const router = new ExecutionWorldRouter(worlds);
		for (const tool of ["grep", "find", "bash"]) {
			// Even a process-only request needs a binding; effect coverage alone must not launch a probe.
			const route = await router.resolve({ tool, effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS }, { cwd: fixture.cwd });
			expect(route?.backend).toBe(tool === "bash" ? "linux_process_reuse" : undefined);
		}
		expect(prepare).toHaveBeenCalledOnce();
	});

	it("applies the primary and native pre-execution layers independently", async () => {
		const primary = {
			id: "primary_runtime", scope: "runtime", isolation: "runtime_sandbox",
			speculation: { capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities, tools: ["read"], prepare: vi.fn(async () => {}) },
		} as unknown as SpeculativeAgentExecutionWorld;
		const fixture = await createFixture({ executionWorlds: [primary], settings: { enabled: true } });
		const router = new ExecutionWorldRouter([primary], (backend) => fixture.executionWorldEnabled(backend) === true);
		vi.mocked(fixture.host.executionWorldDiagnostics).mockImplementation((refresh) => router.diagnostics({ cwd: fixture.cwd, refresh }));
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Tools & execution", "Apply changes", "Close"],
			"Tools & execution": ["Execution routes", "Back"],
			"Execution routes": ["[x] Unified execution environment", "[x] Local safe fallback", "Back"],
		});
		await fixture.emit("session_start");
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);

		expect(menus.get("Execution routes")).toEqual(expect.arrayContaining([
			expect.stringMatching(/^\[ \] Unified execution environment/u),
			expect.stringMatching(/^\[ \] Local safe fallback/u),
			"Actor execution · always available",
		]));
		expect(fixture.store.effective()?.executionRouting).toEqual({ primary: false, nativeFallback: false });
		expect(fixture.executionWorldEnabled("primary_runtime")).toBe(false);
		expect(fixture.executionWorldEnabled("linux_process_reuse")).toBe(false);
		await expect(fixture.host.executionWorldDiagnostics()).resolves.toMatchObject([{ state: "unavailable", tools: ["read"] }]);
		driveSettingsMenus(fixture, {
			"Speculative action": ["Tools & execution", "Apply changes", "Close"],
			"Tools & execution": ["Execution routes", "Back"],
			"Execution routes": ["[ ] Unified execution environment", "Back"],
		});
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);
		expect(fixture.executionWorldEnabled("primary_runtime")).toBe(true);
		expect(fixture.executionWorldEnabled("linux_process_reuse")).toBe(false);
		await expect(fixture.host.executionWorldDiagnostics()).resolves.toMatchObject([{ state: "ready" }]);
	});

	it("keeps tool execution policy hierarchical and explains the fallback boundary", async () => {
		const fixture = await createFixture({ settings: { enabled: true, resourceCacheMaxEntries: 37 }, defaultExecutionWorlds: true });
		vi.mocked(fixture.host.executionWorldDiagnostics).mockResolvedValue(portableDiagnostics({
			entries: 3, maxEntries: 32, bytes: 2048, maxBytes: 4096, orphanArtifacts: 1, overBudget: false,
		}));
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Tools & execution", "Prediction sources", "Apply changes", "Status", "Enabled", "Discard changes", "Save settings to", "Close"],
			"Tools & execution": ["Tool policy", "Execution routes", "Back"],
			"Tool policy · [x] prediction on · [ ] prediction off": ["[x] bash", "Back"],
			"Prediction sources": ["Actor probe", "Back"],
			"Actor probe": ["Back"],
			"Save settings to": ["This project"],
		});
		await fixture.emit("session_start");
		const command = fixture.commands.get("speculative-action");
		await command?.handler("", fixture.context as ExtensionCommandContext);

		expect(menus.get("Speculative action")).toEqual(
			expect.arrayContaining([
				expect.stringMatching(/^Prediction sources/),
				expect.stringMatching(/^Tools & execution/),
				expect.stringMatching(/^Advanced settings/),
			]),
		);
		expect(menus.get("Tools & execution")).toEqual(
			expect.arrayContaining(["Tool policy › 6/7 enabled for prediction", "Execution routes"]),
		);
		expect(menus.get("Tool policy · [x] prediction on · [ ] prediction off")).toEqual(expect.arrayContaining([
			expect.stringMatching(/^\[ \] bash · Predict Off · Replay Check · Observe Unavailable · Fork Unavailable/u),
			expect.stringMatching(/read · Predict On · Replay Ready · Observe Ready · Fork Ready/u),
			expect.stringMatching(/find · Predict On · Replay Unavailable · Observe Unavailable · Fork Unavailable/u),
		]));
		expect(menus.get("Actor probe")).toEqual(expect.arrayContaining(["Actor probe prediction: Off"]));
		expect(menus.get("Actor probe")?.some((label) => /^(Use forked calls|Minimum tool-name confidence)/u.test(label))).toBe(false);
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Replay, Observe, and Fork are independent"), "info");
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Tool   Predict  Replay"), "info");
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringMatching(/bash\s+Off\s+(Ready|Unavailable)/u), "info");
		expect(fixture.ui.notify).not.toHaveBeenCalledWith(expect.stringContaining("bash cannot be enabled here"), "warning");
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("storage 3/32, 2 KiB/4 KiB, 1 orphan artifacts"), "info");
		expect(JSON.stringify([...menus.values()])).not.toContain("sandbox");
		expect((await fixture.hostSettings())?.tools).not.toContain("bash");
		const footer = vi.mocked(fixture.ui.setStatus).mock.calls.at(-1)?.[1] ?? "";
		expect(footer).toContain("tools reused 0/0 (n/a)");
		expect(footer).toContain("reuse history 3 entries (2 KiB)");
		expect(footer).toMatch(/providers \d\/4 ready/u);
		expect(footer).toContain("live results 0/37");
		expect(fixture.store.effective()).toMatchObject({ enabled: true });
		expect(fixture.store.effective()?.tools).not.toContain("bash");
		expect(fixture.store.scope).toBe("project");
	});

	it("keeps each Actor scope across tool binding and overlapping turns", async () => {
		const executeProcess = vi.fn(async (_request: ProcessExecutionRequest) => ({ exitCode: 0 }));
		const prepare = vi.spyOn(LinuxProcessReuseBackend.prototype, "prepareActorReplay").mockResolvedValue({
			state: "ready", detail: "ready", executor: { execute: executeProcess },
		});
		const gate = deferred(), pending: Promise<unknown>[] = [];
		try {
			const fixture = await createFixture({ settings: { enabled: true } });
			await fixture.emit("session_start");
			const execute = fixture.host.execute;
			vi.spyOn(fixture.host, "execute").mockImplementation(async (...args) => { await gate.promise; return execute(...args); });
			const invoke = (command: string) => fixture.tools.get("bash")!.execute(command, { command }, undefined, undefined, fixture.context);
			pending.push(invoke(": unscoped"));
			await fixture.emit("context", { messages: [] });
			pending.push(invoke(": first-turn"));
			await fixture.emit("context", { messages: [] });
			gate.resolve(); await Promise.all(pending);
			expect(executeProcess).toHaveBeenCalledTimes(2);
			expect(prepare.mock.results).toHaveLength(1);
			expect(executeProcess.mock.calls.map(([request]) => [request.command, request.scope]).sort()).toEqual([
				[": first-turn", { sessionID: "session", turnID: "turn_1" }], [": unscoped", undefined],
			]);
		} finally { gate.resolve(); await Promise.allSettled(pending); prepare.mockRestore(); }
	});

	it("publishes applied settings only after the Actor route refresh settles", async () => {
		type Prepared = Awaited<ReturnType<LinuxProcessReuseBackend["prepareActorReplay"]>>;
		for (const [state, label] of [["ready", "Ready"], ["degraded", "Limited"], ["unavailable", "Unavailable"]] as const) {
			const { promise: pending, resolve: release } = deferred<Prepared>();
			const prepare = vi.spyOn(LinuxProcessReuseBackend.prototype, "prepareActorReplay").mockReturnValue(pending);
			try {
				const fixture = await createFixture({ settings: { enabled: false }, defaultExecutionWorlds: true });
				driveSettingsMenus(fixture, {
					"Speculative action": ["Tools & execution", "Enabled", "Apply changes", "Status", "Close"],
					"Tools & execution": ["Execution routes", "Back"],
				});
				await fixture.emit("session_start");
				const applying = Promise.resolve(
					fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext),
				);
				await vi.waitFor(() => expect(prepare).toHaveBeenCalledOnce());
				expect(prepare).toHaveBeenCalledWith(expect.anything(), expect.anything(), true);
				expect(vi.mocked(fixture.host.executionWorldDiagnostics).mock.calls.map(([refresh]) => refresh)).toEqual([false, false, true]);
				expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Diagnostics refresh enabled providers only"), "info");
				expect(fixture.ui.notify).not.toHaveBeenCalledWith("Speculative-action settings applied.", "info");
				release({ state, detail: "qualified test route", executor: { execute: async () => ({ exitCode: 0 }) } });
				await applying;
				expect(fixture.ui.notify).toHaveBeenCalledWith("Speculative-action settings applied.", "info");
				expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining(`Actor Bash history: ${label}`), "info");
				expect(vi.mocked(fixture.ui.setStatus).mock.calls.at(-1)?.[1]).toContain(`providers ${state === "unavailable" ? 1 : 2}/4 ready`);
			} finally {
				prepare.mockRestore();
			}
		}
	});

	it("binds typed inputs through the advanced hierarchy", async () => {
		const configure = vi.fn();
		const maintain = vi.fn(async () => ({ removedEntries: 2, removedArtifacts: 3, removedBytes: 4096 }));
		const fixture = await createFixture({
			settings: { drafterMaxTokens: 10 },
			executionWorlds: [{ storage: { configure, maintain } } as unknown as SpeculativeAgentExecutionWorld],
		});
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Advanced settings", "Prediction sources", "Apply changes", "Close"],
			"Advanced settings": ["Scheduling and storage", "Actor probe and target verification", "Learned-pattern tuning", "Back"],
			"Scheduling and storage": ["Prediction wait limit", "Live result memory", "Reusable command history entries", "Reusable command history memory", "Reclaim", "Clear", "Clear", "Back"],
			"Actor probe advanced": ["Integration and authentication", "Fork decoding", "Target verification", "Benefit control", "Back"],
			"Integration and authentication": ["Integration", "Control service URL", "Back"],
			"Fork decoding": ["Back"],
			"Target verification": ["Back"],
			"Benefit control": ["Back"],
			"Learned-pattern advanced": ["Learning history", "Multi-step search", "Back", "Back"],
			"Learning history": ["Early-prediction coverage", "Back"],
			"Multi-step search": ["Back"],
			"Prediction sources": ["Model Drafter", "Actor probe", "Learned patterns", "Back"],
			"Learned patterns": ["Enabled", "Predict follow-up tool steps", "Advanced settings", "Back"],
			"Model Drafter": ["Advanced settings", "Back"],
			"Model Drafter advanced": ["Maximum output tokens", "Follow-up tool steps", "Back"],
			"Actor probe": ["Minimum tool-name confidence", "Back"],
			"Actor probe integration": ["Sidecar service"],
		});
		fixture.ui.input = async (title) =>
			({
				"Prediction wait limit (ms)": "1",
				"Maximum Drafter output tokens": "512",
				"Live result memory (MiB)": "96",
				"Reusable command history entries": "2048",
				"Reusable command history memory (MiB)": "768",
				"Control service URL": "file:///unsafe",
				"Minimum tool-name confidence": "0.75",
				"Early-prediction coverage (0-1)": "0.8",
			} as Readonly<Record<string, string>>)[title];
		let clearConfirmations = 0;
		fixture.ui.confirm = async (title) => title === "Clear reusable command history?" && ++clearConfirmations === 2;

		await fixture.emit("session_start");
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);

		expect(fixture.store.effective()).toMatchObject({
			predictionTimeoutMs: 1, drafterMaxDepth: 1, drafterMaxTokens: 512,
			resourceCacheMaxBytes: 96 * 1024 * 1024,
			executionStoreMaxEntries: 2048,
			executionStoreMaxBytes: 768 * 1024 * 1024,
			selfSpeculation: {
				endpoint: "http://127.0.0.1:8000",
				forkTransport: "sidecar",
				forkActionMinConfidence: 0.75,
			},
			patternAware: { futureGapCoverage: 0.8, enabled: false, multiStepEnabled: false },
		});
		expect(clearConfirmations).toBe(2);
		expect(menus.get("Learned-pattern advanced")?.some((label) => label.startsWith("Multi-step search"))).toBe(false);
		expect(menus.get("Model Drafter")).toEqual(expect.arrayContaining([
			"Enabled: On", expect.stringMatching(/^Model ›/), "Candidate requests per decision: 2",
		]));
		expect(menus.get("Model Drafter")).not.toEqual(expect.arrayContaining([expect.stringMatching(/^Sampling temperature:/)]));
		expect(menus.get("Model Drafter advanced")).toEqual(expect.arrayContaining([
			"Pause drafts on estimated negative utility: On", "Follow-up tool steps: 1", "Maximum output tokens: 512",
			"Temperature-0 candidates: 1", "Sampling temperature: 0.7-0.7",
		]));
		expect(configure).toHaveBeenLastCalledWith({ maxEntries: 2048, maxBytes: 768 * 1024 * 1024 });
		expect(maintain.mock.calls).toEqual([["gc"], ["clear"]]);
		expect(menus.get("Fork decoding")).not.toEqual(
			expect.arrayContaining([expect.stringMatching(/^Require token probabilities/)]),
		);
		expect(fixture.ui.notify).toHaveBeenCalledWith("Reusable command history cleared: 2 entries, 3 artifacts, 4 KiB.", "info");
		expect(fixture.ui.notify).toHaveBeenCalledWith("Endpoint must be an absolute HTTP(S) URL.", "warning");
		expect(menus.get("Actor probe")).toEqual(
			expect.arrayContaining([
				"Use forked calls for tool pre-execution: On",
				"Minimum tool-name confidence: 75%",
			]),
		);
	});
});

function searchProfile() {
	const authoritative = vi.fn(async () => ({ result: textResult("selected search"), isError: false }));
	return { profile: { id: "test-search", pi: "0.84.1", limits: { inputBytes: 1024 }, grep: { versions: {}, flags: [] } },
		pool: { prepare: vi.fn(), run: authoritative, dispose: vi.fn(async () => {}) }, invocations: new Map(["find"].map((tool) => [tool, {
			executor: "test-search", authoritative, filesystem: authoritative,
			semantics: { ...PI_ACTION_SEMANTICS.definition(tool)!, effect: "observation" as const, requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "captured_inputs" as const },
		}])) };
}

interface FixtureOptions {
	readonly reuse?: ToolSettlement;
	readonly defaultExecutionWorlds?: boolean;
	readonly executionWorlds?: readonly SpeculativeAgentExecutionWorld[];
	readonly overriddenTools?: readonly string[];
	readonly settings?: SpeculativeActionPackageSettings;
}

async function createFixture(options: FixtureOptions = {}) {
	const cwd = await directories.create();
	const handlers = new Map<string, Array<(event: never, context: ExtensionContext) => unknown>>();
	const tools = new Map<string, ToolDefinition>();
	const baseTools = piTools.createPiToolDefinitions(cwd);
	const actorTools = new Map<string, PiToolDefinition | ToolDefinition>(baseTools);
	const toolSources = new Map<string, SourceInfo>(
		[...baseTools.keys()].map((name) => [name, sourceInfo(`<builtin:${name}>`, "builtin")]),
	);
	const customTools = new Map<string, PiToolDefinition>();
	for (const name of options.overriddenTools ?? []) {
		const base = baseTools.get(name);
		if (!base) throw new Error(`Unknown fixture tool override: ${name}`);
		const custom = {
			...base,
			label: `custom ${name}`,
			execute: vi.fn(async () => textResult(`custom ${name}`)),
		} as PiToolDefinition;
		customTools.set(name, custom);
		actorTools.set(name, custom);
		toolSources.set(name, sourceInfo(`custom-${name}.ts`, "cli"));
	}
	const commands = new Map<
		string,
		{ handler: (args: string, context: ExtensionCommandContext) => Promise<void> | void }
	>();
	let hostOptions: CreateSpeculativeActionHostOptions | undefined;
	const resolveInvocation: NonNullable<CreateSpeculativeActionHostOptions["resolveInvocation"]> = (tool, input) => hostOptions?.resolveInvocation?.(tool, input);
	const host = createSpeculativeActionHost("session", { cwd, complete: vi.fn(), resolveInvocation, executionWorlds: [] });
	hosts.push(host);
	const settle = vi.fn(async (_execution: TimelineInterval, _output?: ToolSettlement) => undefined);
	vi.spyOn(host.runtime, "prepareActorCall").mockResolvedValue({ settle, ...(options.reuse ? { output: options.reuse } : {}) });
	vi.spyOn(host.runtime, "settingsChanged").mockResolvedValue();
	for (const method of ["startTurn", "previewActorTool", "previewActorCall", "finishTurn"] as const) vi.spyOn(host, method).mockResolvedValue();
	vi.spyOn(host, "executionWorldDiagnostics").mockImplementation(async () => portableDiagnostics());
	const ui = {
		select: async (_title: string, _options: string[]) => undefined as string | undefined,
		confirm: async (_title: string, _message?: string) => false,
		input: async (_title: string, _placeholder?: string) => undefined as string | undefined,
		notify: vi.fn(),
		setStatus: vi.fn(),
	};
	const context = {
		cwd,
		mode: "tui",
		hasUI: true,
		ui,
		model: testModel("mock"),
		modelRegistry: {
			getAvailable: () => [testModel("mock")],
		},
		sessionManager: { getSessionId: () => "session", getSessionFile: () => undefined },
		isProjectTrusted: () => true,
		getSystemPrompt: () => "system",
		signal: undefined,
		thinkingLevel: "off",
	} as unknown as ExtensionContext;
	const store = memorySettingsStore(options.settings);
	const pi = {
		on: (event: string, handler: (event: never, context: ExtensionContext) => unknown) => {
			handlers.set(event, [...(handlers.get(event) ?? []), handler]);
		},
		registerTool: (tool: ToolDefinition) => {
			tools.set(tool.name, tool);
			actorTools.set(tool.name, tool);
			toolSources.set(tool.name, sourceInfo("speculative-action.ts", "cli"));
		},
		registerCommand: (name: string, command: typeof commands extends Map<string, infer T> ? T : never) =>
			commands.set(name, command),
		getActiveTools: () => [...actorTools.keys()],
		getAllTools: () =>
			[...actorTools.values()].map((tool) => ({
				...tool,
				sourceInfo: toolSources.get(tool.name) ?? sourceInfo("unknown"),
			})),
	} as unknown as ExtensionAPI;
	const createExecutionWorlds = vi.fn(() => options.executionWorlds ?? []);
	const factory = createSpeculativeActionExtension({
		createHost: (_sessionID, configured) => {
			hostOptions = configured;
			return host;
		},
		createSettingsStore: () => store,
		...(options.defaultExecutionWorlds ? {} : { createExecutionWorlds }),
	});
	await factory(pi);
	const emit = async (event: string, payload: object = {}) => {
		for (const handler of handlers.get(event) ?? []) await handler(payload as never, context);
	};
	return {
		actorTools, baseTools, commands, context, createExecutionWorlds, customTools, cwd, emit, handlers, host, settle,
		executionWorlds: () => hostOptions?.executionWorlds ?? [],
		drafterComplete: (...args: Parameters<CreateSpeculativeActionHostOptions["complete"]>) => hostOptions!.complete(...args),
		drafterModel: (actor: ReturnType<typeof testModel>) => (hostOptions!.draftModel as (actor: unknown) => unknown)(actor),
		executionWorldEnabled: (backend: string) => hostOptions?.speculativeExecutionWorldEnabled?.(backend),
		hostSettings: async () => hostOptions?.getSettings?.(), resolveInvocation, store, tools, ui,
	};
}

function driveSettingsMenus(
	fixture: Awaited<ReturnType<typeof createFixture>>,
	routes: Readonly<Record<string, readonly string[]>>,
): Map<string, string[]> {
	const pending = new Map(Object.entries(routes).map(([title, choices]) => [title, [...choices]]));
	const menus = new Map<string, string[]>();
	fixture.ui.select = async (title, options) => {
		menus.set(title, [...options]);
		const prefix = pending.get(title)?.shift();
		return prefix ? options.find((option) => option === prefix || option.startsWith(prefix)) : undefined;
	};
	return menus;
}

function sourceInfo(path: string, source = "test"): SourceInfo {
	return { path, source, scope: "temporary", origin: "top-level" };
}

function portableDiagnostics(
	storage?: NonNullable<ExecutionWorldDiagnosticSnapshot["storage"]>,
): readonly ExecutionWorldDiagnosticSnapshot[] {
	return [
		{
			id: "linux_process_reuse", scope: "runtime", isolation: "runtime_sandbox",
			capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			state: "unavailable", detail: "Linux host required", ...(storage ? { storage } : {}),
		},
		{
			id: "git_worktree", scope: "fallback", isolation: "workspace_branch",
			capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
			state: "registered", detail: "Checked on first use",
		},
		{
			id: "resource_version", scope: "fallback", isolation: "resource_snapshot",
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities, tools: ["read", "ls", ...piTools.PI_CLOSED_SEARCH_TOOLS], state: "ready", detail: "Sealed file inputs ready",
			observation: {
				capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
				state: "ready", detail: "Resource validation ready",
			},
		},
	];
}

function memorySettingsStore(initial: SpeculativeActionPackageSettings = { enabled: false }): SpeculativeSettingsStore {
	let value: SpeculativeActionPackageSettings | undefined = initial;
	let scope: "global" | "project" = "global";
	return {
		get scope() {
			return scope;
		},
		load: async () => undefined,
		effective: () => value,
		editable: () => value,
		setEffective: (next) => {
			value = next;
		},
		clear: () => {
			value = undefined;
		},
		setScope: (next) => {
			scope = next;
		},
		flush: async () => undefined,
	};
}
