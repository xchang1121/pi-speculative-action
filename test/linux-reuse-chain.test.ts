import { onExit } from "./async.ts";
// Constructed Actor sequences through the real host and Linux process world, PatternAware only (no model): which reuse path
// serves the predicted command, and how long the Actor waits. The scenario table takes minutes of CPU; run it with PI_SPEC_REUSE_CHAIN=1.
import { execSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createWriteTool } from "@earendil-works/pi-coding-agent";
import { expect, test } from "vitest";
import { patternAwareSettings } from "../src/pattern-aware.ts";
import type { ExecutionResourceMonitor } from "../src/system-resources.ts";
import { commitBenchmarkFixture, compileBenchmarkHelper, createLinuxProcessBenchmark, holdOwnedProcessPublication, patternProcessHost, prepareLinuxProcessReuse, textOutput } from "./linux-process-fixture.ts";

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
async function chainWorld(files: readonly (readonly [string, string])[], loops: string, resources?: ExecutionResourceMonitor) {
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
		const { host, events, start } = patternProcessHost(fixture, "chain", { candidateLimit: 4, maxConcurrentActions: 4,
			patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false }) },
			{ resources, executionWorlds: [fixture.world, fixture.workspaceSandbox.createExecutionWorld()] }, tools);
		const step = async (turnID: string, call: string | { readonly path: string; readonly content: string }, think: number | (() => Promise<unknown>), during?: string) => {
			await start(turnID, undefined, "chain");
			await (typeof think === "number" ? sleep(think) : think());
			if (during) execSync(during, { cwd: workspace, shell: "/bin/bash" });
			const before = fixture.backend.actorMetrics(), started = performance.now();
			const result = typeof call !== "string" ? await host.execute({ turnID, id: turnID, tool: "write", args: call, tools }, undefined, () => writer.execute(turnID, call))
				: await host.execute({ turnID, id: turnID, tool: fixture.tool.name, args: { command: call }, tools }, undefined, () => fixture.coordinator.runWith(route.executor, () => fixture.tool.execute(turnID, { command: call })));
			const ms = performance.now() - started, after = fixture.backend.actorMetrics();
			const timing = events.filter(event => event.type === "actor_action").at(-1)?.computation;
			expect(timing).toMatchObject({ toolComputeMs: expect.any(Number), hiddenComputeMs: expect.any(Number), adoptionWaitMs: expect.any(Number) });
			expect(timing!.toolComputeMs!).toBeGreaterThanOrEqual(timing!.hiddenComputeMs);
			expect(timing!.adoptionWaitMs!).toBeGreaterThanOrEqual(0); expect(timing!.adoptionWaitMs!).toBeLessThanOrEqual(ms);
			await host.finishTurn(turnID);
			return { ms, text: textOutput(result as never), hits: after.hits - before.hits, hiddenMs: timing!.hiddenComputeMs };
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
		await using _cleanup = onExit(async () => { await host.dispose(); await fixture.dispose(); });
		for (let index = 0; index < 3; index++) { await step(`learn-${index}-a`, scenario.first, 0); await step(`learn-${index}-b`, scenario.next, 1500); }
		await step("measured-a", scenario.first, 0);
		if (scenario.detour) await step("detour", scenario.detour, scenario.think ?? 4000);
		const command = (scenario.actual ?? scenario.next).replace("WORKSPACE", workspace);
		const measured = await step("measured", command, scenario.detour ? 100 : scenario.think ?? 4000, scenario.during);
		const started = performance.now(), native = execSync(command, { cwd: workspace, encoding: "utf8", shell: "/bin/bash" }), nativeMs = performance.now() - started;
		rows.push(`${name.padEnd(26)} native ${nativeMs.toFixed(0).padStart(5)} ms  Actor ${measured.ms.toFixed(0).padStart(5)} ms  ${measured.hits ? "reused" : "native"}`);
		expect(measured.text.trim(), name).toBe(native.trim());
		expect(measured.hits > 0, name).toBe(scenario.reuse);
	}
	console.log(rows.join("\n"));
});

test.skipIf(process.platform !== "linux").for(["build", "build_changed_after_preparation", "current_workspace", "changed_after_preparation", "after_horizon", "changed_after_horizon", "bash_mutation", "bash_changed_after_preparation"] as const)("prepares learned work after an Actor edit (%s)", { timeout: 120_000 }, async mode => {
	const horizon = mode.endsWith("horizon"), stale = mode.includes("changed_"), bashMutation = mode.startsWith("bash_"), build = mode.startsWith("build"), worker = `./slow a.txt${horizon ? " identity" : ""}`;
	// This retention scenario needs capacity for the detour and producer; CPU preemption has separate coverage.
	const { fixture, workspace, host, step, events } = await chainWorld([["a.txt", "alpha\n"], ["Makefile", "all:\n\t@./slow a.txt\n"]], "600000000",
		horizon ? { initial: { cpuCount: 4, idleCpuCount: 4 }, sample: async () => ({ cpuCount: 4, idleCpuCount: 4 }) } : undefined);
	let publication: Awaited<ReturnType<typeof holdOwnedProcessPublication>> | undefined;
	await using _cleanup = onExit(async () => { await publication?.close(); await host.dispose(); await fixture.dispose(); });
	const command = build ? "make -s all" : `${worker} | tail -1${horizon ? `; ${worker} | tail -2` : ""}`;
	await step("build", build ? "make -s" : `cat > a.txt <<'EOF'\nalpha\nEOF\n${worker} | tail -2`, 0);
	if (bashMutation) {
		await step("unchanged", "printf unchanged", 0);
		expect(events.some(event => event.type === "candidate" && event.candidate.kind === "operation")).toBe(false);
	}
	if (horizon) publication = await holdOwnedProcessPublication(fixture);
	await step("edit", bashMutation ? "printf 'alpha\\nbeta\\n' > a.txt" : { path: "a.txt", content: "alpha\nbeta\n" }, 0);
	if (publication) {
		await step("detour", "printf detour", () => publication!.ready());
		const operation = events.filter(event => event.type === "candidate").find(event => event.candidate.kind === "operation")!.candidate.id;
		const states = () => events.filter(event => event.type === "candidate").filter(event => event.candidate.id === operation).map(event => event.state.status);
		expect(states(), JSON.stringify(events.filter(event => event.type === "candidate" || event.type === "operation_prediction"))).toEqual(["running"]);
		expect(events.filter(event => event.type === "operation_prediction").map(event => event.settlement)).toContainEqual(expect.objectContaining({ observation: "unobserved", cause: expect.objectContaining({ code: "operation_not_observed" }) }));
		await publication.close(); publication = undefined;
		await expect.poll(states, { timeout: 15_000 }).toEqual(["running", "succeeded"]);
	}
	// A PID-observing child has a one-shot result: the second native exec must run even after the first reuses it.
	const measured = await step("check", command, bashMutation ? () => expect.poll(() => events.some(event => event.type === "candidate" &&
		event.candidate.kind === "operation" && event.state.status === "succeeded"), { timeout: 15_000 }).toBe(true) : horizon ? 0 : 3000,
		stale ? "printf 'newest\\n' > a.txt" : undefined);
	expect([measured.text.trim(), measured.hits, measured.hiddenMs > 0], JSON.stringify({ events: events.filter(event => event.type === "candidate" || event.type === "operation_prediction"), metrics: fixture.backend.metrics() }))
		.toEqual([execSync(command, { cwd: workspace, encoding: "utf8" }).trim(), Number(!stale), !stale]);
});
