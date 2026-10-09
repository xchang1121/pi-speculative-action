// Constructed Actor sequences through the real host and Linux process world, PatternAware only (no model): which reuse path
// serves the predicted command, and how long the Actor waits. The scenario table takes minutes of CPU; run it with PI_SPEC_REUSE_CHAIN=1.
import { execSync } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createWriteTool } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, type AssistantMessage } from "@earendil-works/pi-ai";
import { expect, test } from "vitest";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareActionSemantics, patternAwareSettings } from "../src/pattern-aware.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import type { ExecutionResourceMonitor } from "../src/system-resources.ts";
import { testModel } from "./model.ts";
import { commitBenchmarkFixture, compileBenchmarkHelper, createLinuxProcessBenchmark, holdProcessPublication, prepareLinuxProcessReuse, textOutput } from "./linux-process-fixture.ts";

/** The Actor runs `first`, then (its next step) `next`, three times; the measured episode thinks `think` ms before `actual`. A `detour`
 * step first does something else, `during` changes the workspace while the Actor thinks; `reuse` is the expected outcome. */
type Scenario = { readonly first: string; readonly next: string; readonly actual?: string; readonly think?: number; readonly detour?: string;
	readonly during?: string; readonly reuse: boolean };
const SCENARIOS: Record<string, Scenario> = {
	"C": { first: "git status --short", next: "./slow a.txt", reuse: true },
	"C, unrelated edit": { first: "git status --short", next: "./slow a.txt", during: "echo x >> c.txt", reuse: true },
	"C, input edit": { first: "git status --short", next: "./slow a.txt", during: "echo changed >> a.txt", reuse: false },
	"python": { first: "ls", next: "python3 stats.py", reuse: true },
	"node": { first: "ls", next: "node stats.js", reuse: true },
	"make (posix_spawn)": { first: "git status --short", next: "make -s", reuse: true },
	"sh script": { first: "ls", next: "sh check.sh", reuse: true },
	"cd prefix": { first: "ls", next: "./slow b.txt", actual: "cd WORKSPACE && ./slow b.txt", reuse: true },
	"another parent": { first: "ls", next: "./slow b.txt", actual: "./slow b.txt && echo done", reuse: true },
	"a later turn": { first: "ls", next: "./slow b.txt", detour: "cat c.txt", reuse: true },
	"a later turn, input edit": { first: "ls", next: "./slow b.txt", detour: "cat c.txt", during: "echo changed >> b.txt", reuse: false },
	"running, joined": { first: "git status --short", next: "./slow a.txt", think: 400, reuse: true },
	"running, a later turn": { first: "ls", next: "./slow b.txt", detour: "cat c.txt", think: 100, reuse: true },
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A committed workspace holding `files` and `./slow` (`loops` rounds of CPU over a file's bytes: a stand-in for a test run or a
 * build step), a host over it with PatternAware alone, and one Actor step, a command or a write: prediction starts with it and
 * runs while the Actor model generates for `think` ms. */
async function chainWorld(files: readonly (readonly [string, string])[], loops: string, draft?: () => Promise<AssistantMessage>, resources?: ExecutionResourceMonitor) {
	// Under the user's home, as a real workspace is: /tmp is each sandbox's own, so what a runtime observes of the workspace's parents
	// there (Node's package.json probes) could never be validated.
	const fixture = await createLinuxProcessBenchmark("pi-chain-", "overlayfs", {}, path.join(os.homedir(), ".cache", "pi-speculative-action", "chain")), { workspace } = fixture;
	try {
		for (const [file, text] of [...files, [".gitignore", "slow\n"], ["slow.c", "#include <stdio.h>\n#include <fcntl.h>\n#include <unistd.h>\nint main(int argc, char **argv) { int f = open(argv[1], O_RDONLY); if (f < 0) return 1;" +
			` unsigned long h = 5381; unsigned char c;\n while (read(f, &c, 1) == 1) h = h * 33 + c; close(f);\n for (volatile unsigned long i = 0; i < ${loops}ul; i++) h ^= i;` +
			" if (argc > 2) getpid(); printf(\"%s %lx\\n\", argv[1], h); return 0; }\n"]] as const) await writeFile(path.join(workspace, file), text);
		await compileBenchmarkHelper(workspace, { source: "slow.c", output: "slow" });
		await commitBenchmarkFixture(workspace, "chain");
		await prepareLinuxProcessReuse(fixture);
		const route = await fixture.prepareActorReplay(), writer = createWriteTool(workspace), tools = [fixture.tool, writer];
		const settings = patternAwareSettings({ enabled: true, multiStepEnabled: false });
		const events: SpeculativeActionEvent<string>[] = [];
		const host = createSpeculativeActionHost("chain", { cwd: workspace, resources, complete: draft ?? (async () => { throw new Error("no inference"); }), onEvent: event => { events.push(event); },
			patternStore: new PatternAwareStore(settings, undefined, patternAwareActionSemantics(PI_ACTION_SEMANTICS, workspace)),
			getSettings: () => ({ enabled: true, drafterEnabled: !!draft, drafterMaxDepth: 1, candidateLimit: draft ? 1 : 4, maxConcurrentActions: 4, tools: draft ? ["bash", "write"] : ["bash"], patternAware: settings }),
			preflight: () => true, executionWorlds: [fixture.world, fixture.workspaceSandbox.createExecutionWorld()],
			resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd: workspace, environment: fixture.environment, shellPath: fixture.shellPath }) });
		const step = async (turnID: string, call: string | { readonly path: string; readonly content: string }, think: number | (() => Promise<unknown>), during?: string) => {
			await host.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined, context: { systemPrompt: "chain", messages: [], tools } });
			await (typeof think === "number" ? sleep(think) : think());
			if (during) execSync(during, { cwd: workspace, shell: "/bin/bash" });
			const before = fixture.backend.actorMetrics(), started = performance.now();
			const result = typeof call !== "string" ? await host.execute({ turnID, id: turnID, tool: "write", args: call, tools }, undefined, () => writer.execute(turnID, call))
				: await host.execute({ turnID, id: turnID, tool: "bash", args: { command: call }, tools }, undefined, () => fixture.coordinator.runWith(
					{ execute: (request) => route.executor.execute({ ...request, scope: { sessionID: "chain", turnID } }) }, () => fixture.tool.execute(turnID, { command: call })));
			const ms = performance.now() - started, after = fixture.backend.actorMetrics();
			await host.finishTurn(turnID);
			return { ms, text: textOutput(result as never), hits: after.hits - before.hits };
		};
		return { fixture, workspace, host, step, tools, events };
	} catch (error) { await fixture.dispose(); throw error; }
}

test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_REUSE_CHAIN)("reuse chain", { timeout: 3_600_000 }, async () => {
	const rows: string[] = [];
	for (const [name, scenario] of Object.entries(SCENARIOS)) {
		const { fixture, workspace, host, step } = await chainWorld([["a.txt", "alpha\nbeta\n"], ["b.txt", "gamma\n"], ["c.txt", "c\n"],
			["Makefile", "all:\n\t@./slow a.txt\n\t@wc -l < b.txt\n"], ["check.sh", "set -e\ngrep -c a a.txt\n./slow b.txt\n"],
			["stats.py", "text = open('a.txt').read()\ns = 0\nfor i in range(20_000_000): s += i % 7\nprint(len(text.split()), s)\n"],
			["stats.js", "const t = require('fs').readFileSync('a.txt', 'utf8'); let s = 0; for (let i = 0; i < 1e9; i++) s += i % 7; console.log(t.length, s);\n"]], "3500000000");
		try {
			for (let index = 0; index < 3; index++) { await step(`learn-${index}-a`, scenario.first, 0); await step(`learn-${index}-b`, scenario.next, 1500); }
			await step("measured-a", scenario.first, 0);
			if (scenario.detour) await step("detour", scenario.detour, scenario.think ?? 4000);
			const command = (scenario.actual ?? scenario.next).replace("WORKSPACE", workspace);
			const measured = await step("measured", command, scenario.detour ? 100 : scenario.think ?? 4000, scenario.during);
			const started = performance.now(), native = execSync(command, { cwd: workspace, encoding: "utf8", shell: "/bin/bash" }), nativeMs = performance.now() - started;
			rows.push(`${name.padEnd(26)} native ${nativeMs.toFixed(0).padStart(5)} ms  Actor ${measured.ms.toFixed(0).padStart(5)} ms  ${measured.hits ? "reused" : "native"}`);
			expect(measured.text.trim(), name).toBe(native.trim());
			expect(measured.hits > 0, name).toBe(scenario.reuse);
		} finally { await host.dispose(); await fixture.dispose(); }
	}
	console.log(rows.join("\n"));
});

test.skipIf(process.platform !== "linux").for(["build", "current_workspace", "changed_after_preparation", "after_horizon", "changed_after_horizon", "bash_mutation", "bash_changed_after_preparation"] as const)("prepares learned work after an Actor edit (%s)", { timeout: 120_000 }, async mode => {
	const horizon = mode.endsWith("horizon"), stale = mode.includes("changed_"), bashMutation = mode.startsWith("bash_"), worker = `./slow a.txt${horizon ? " identity" : ""}`;
	// This retention scenario needs capacity for the detour and producer; CPU preemption has separate coverage.
	const { fixture, workspace, host, step, events } = await chainWorld([["a.txt", "alpha\n"], ["Makefile", "all:\n\t@./slow a.txt\n"]], "600000000", undefined,
		horizon ? { initial: { cpuCount: 4, idleCpuCount: 4 }, sample: async () => ({ cpuCount: 4, idleCpuCount: 4 }) } : undefined);
	let publication: ReturnType<typeof holdProcessPublication> | undefined;
	try {
		const command = mode === "build" ? "make -s all" : `${worker} | tail -1${horizon ? `; ${worker} | tail -2` : ""}`;
		await step("build", mode === "build" ? "make -s" : `cat > a.txt <<'EOF'\nalpha\nEOF\n${worker} | tail -2`, 0);
		if (bashMutation) {
			await step("unchanged", "printf unchanged", 0);
			expect(events.some(event => event.type === "candidate" && event.candidate.kind === "operation")).toBe(false);
		}
		if (horizon) publication = holdProcessPublication(fixture.backend);
		await step("edit", bashMutation ? "printf 'alpha\\nbeta\\n' > a.txt" : { path: "a.txt", content: "alpha\nbeta\n" }, 0);
		if (publication) {
			await step("detour", "printf detour", () => expect.poll(publication!.reached, { timeout: 15_000 }).toBe(true));
			const operation = events.filter(event => event.type === "candidate").find(event => event.candidate.kind === "operation")!.candidate.id;
			const states = () => events.filter(event => event.type === "candidate").filter(event => event.candidate.id === operation).map(event => event.state.status);
			expect(states(), JSON.stringify(events.filter(event => event.type === "candidate" || event.type === "operation_prediction"))).toEqual(["running"]);
			expect(events.filter(event => event.type === "operation_prediction").map(event => event.settlement)).toContainEqual(expect.objectContaining({ observation: "unobserved", cause: expect.objectContaining({ code: "operation_not_observed" }) }));
			publication.close(); publication = undefined;
			await expect.poll(states, { timeout: 15_000 }).toEqual(["running", "succeeded"]);
		}
		// A PID-observing child has a one-shot result: the second native exec must run even after the first reuses it.
		const measured = await step("check", command, bashMutation ? () => expect.poll(() => events.some(event => event.type === "candidate" &&
			event.candidate.kind === "operation" && event.state.status === "succeeded"), { timeout: 15_000 }).toBe(true) : horizon ? 0 : 3000,
			stale ? "printf 'newest\\n' > a.txt" : undefined);
		expect([measured.text.trim(), measured.hits], JSON.stringify({ events: events.filter(event => event.type === "candidate" || event.type === "operation_prediction"), metrics: fixture.backend.metrics() }))
			.toEqual([execSync(command, { cwd: workspace, encoding: "utf8" }).trim(), Number(!stale)]);
	} finally { publication?.close(); await host.dispose(); await fixture.dispose(); }
});

test.skipIf(process.platform !== "linux")("prepares a process from a predicted mutation before either Actor call, then adopts across turns", { timeout: 30_000 }, async () => {
	const steps = [{ tool: "write", input: { path: "a.txt", content: "next\n" } }, { tool: "bash", input: { command: "./slow a.txt" } }];
	let requests = 0;
	const { fixture, workspace, host, tools, events } = await chainWorld([["a.txt", "original\n"]], "600000000", async () => {
		requests++;
		return fauxAssistantMessage([{ type: "toolCall", id: "workflow", name: "speculative_workflow", arguments: { steps } }], { stopReason: "toolUse" });
	});
	const start = (turnID: string) => host.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined, context: { messages: [], tools } });
	try {
		await start("write");
		await expect.poll(() => events.filter(event => event.type === "candidate" && event.state.status === "succeeded"), { timeout: 15_000 }).toHaveLength(2);
		expect(await readFile(path.join(workspace, "a.txt"), "utf8")).toBe("original\n");
		for (const step of steps) {
			if (step.tool === "bash") await start(step.tool);
			const output = await host.execute({ turnID: step.tool, id: step.tool, tool: step.tool, args: step.input, tools }, undefined,
				() => tools.find(tool => tool.name === step.tool)!.execute(step.tool, step.input as never));
			if (step.tool === "bash") expect(textOutput(output as never).trim()).toBe(execSync(step.input.command!, { cwd: workspace, encoding: "utf8" }).trim());
			await host.finishTurn(step.tool, step.tool === "bash");
		}
		const settled = events.filter(event => event.type === "actor_action").map(event => event.settlement);
		expect(settled.map(event => event.provider.kind), JSON.stringify(settled)).toEqual(["speculative", "speculative"]);
		expect(requests).toBe(1);
	} finally { await host.dispose(); await fixture.dispose(); }
});
