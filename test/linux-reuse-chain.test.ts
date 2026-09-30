// Constructed Actor sequences through the real host and Linux process world, PatternAware only (no model): which reuse path
// serves the predicted command, and how long the Actor waits. The scenario table takes minutes of CPU; run it with PI_SPEC_REUSE_CHAIN=1.
import { execSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { createWriteTool } from "@earendil-works/pi-coding-agent";
import { expect, test } from "vitest";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareActionSemantics, patternAwareSettings } from "../src/pattern-aware.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { testModel } from "./model.ts";
import { commitBenchmarkFixture, compileBenchmarkHelper, createLinuxProcessBenchmark, prepareLinuxProcessReuse, textOutput } from "./linux-process-fixture.ts";

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
async function chainWorld(files: readonly (readonly [string, string])[], loops: string) {
	const fixture = await createLinuxProcessBenchmark("pi-chain-", "overlayfs", { cheapChildMs: 50 }), { workspace } = fixture;
	try {
		for (const [file, text] of [...files, [".gitignore", "slow\n"], ["slow.c", "#include <stdio.h>\nint main(int argc, char **argv) { FILE *f = fopen(argv[1], \"r\"); if (!f) return 1;" +
			` unsigned long h = 5381; int c;\n while ((c = fgetc(f)) != EOF) h = h * 33 + c; fclose(f);\n for (volatile unsigned long i = 0; i < ${loops}ul; i++) h ^= i;` +
			" printf(\"%s %lx\\n\", argv[1], h); return 0; }\n"]] as const) await writeFile(path.join(workspace, file), text);
		await compileBenchmarkHelper(workspace, { source: "slow.c", output: "slow" });
		await commitBenchmarkFixture(workspace, "chain");
		await prepareLinuxProcessReuse(fixture);
		const route = await fixture.prepareActorReplay(), writer = createWriteTool(workspace), tools = [fixture.tool, writer];
		const settings = patternAwareSettings({ enabled: true, multiStepEnabled: false });
		const host = createSpeculativeActionHost("chain", { cwd: workspace, complete: async () => { throw new Error("no inference"); },
			patternStore: new PatternAwareStore(settings, undefined, patternAwareActionSemantics(PI_ACTION_SEMANTICS, workspace)),
			getSettings: () => ({ enabled: true, drafterEnabled: false, candidateLimit: 4, maxConcurrentActions: 4, tools: ["bash"], patternAware: settings }),
			preflight: () => true, executionWorlds: [fixture.world],
			resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd: workspace, environment: fixture.environment, shellPath: fixture.shellPath }) });
		const step = async (turnID: string, call: string | { readonly path: string; readonly content: string }, think: number, during?: string) => {
			await host.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined, context: { systemPrompt: "chain", messages: [], tools } });
			await sleep(think);
			if (during) execSync(during, { cwd: workspace, shell: "/bin/bash" });
			const before = fixture.backend.actorMetrics(), started = performance.now();
			const result = typeof call !== "string" ? await host.execute({ turnID, id: turnID, tool: "write", args: call, tools }, undefined, () => writer.execute(turnID, call))
				: await host.execute({ turnID, id: turnID, tool: "bash", args: { command: call }, tools }, undefined, () => fixture.coordinator.runWith(
					{ execute: (request) => route.executor.execute({ ...request, scope: { sessionID: "chain", turnID } }) }, () => fixture.tool.execute(turnID, { command: call })));
			const ms = performance.now() - started, after = fixture.backend.actorMetrics();
			await host.finishTurn(turnID);
			return { ms, text: textOutput(result as never), hits: after.hits - before.hits, validationMs: after.validationMs - before.validationMs };
		};
		return { fixture, workspace, host, step };
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
			rows.push(`${name.padEnd(26)} native ${nativeMs.toFixed(0).padStart(5)} ms  Actor ${measured.ms.toFixed(0).padStart(5)} ms  ${measured.hits ? "reused" : "native"}` +
				`  validation ${measured.validationMs.toFixed(0)} ms`);
			expect(measured.text.trim(), name).toBe(native.trim());
			expect(measured.hits > 0, name).toBe(scenario.reuse);
		} finally { await host.dispose(); await fixture.dispose(); }
	}
	console.log(rows.join("\n"));
});

test.skipIf(process.platform !== "linux")("reruns a learned build after an Actor edit, so another command reuses the step it redid", { timeout: 120_000 }, async () => {
	const { fixture, workspace, host, step } = await chainWorld([["a.txt", "alpha\n"], ["Makefile", "all:\n\t@./slow a.txt\n"]], "600000000");
	try {
		await step("build", "make -s", 0);
		await step("edit", { path: "a.txt", content: "alpha\nbeta\n" }, 0);
		const measured = await step("check", "make -s all", 3000);
		expect([measured.text.trim(), measured.hits]).toEqual([execSync("make -s all", { cwd: workspace, encoding: "utf8" }).trim(), 1]);
	} finally { await host.dispose(); await fixture.dispose(); }
});
