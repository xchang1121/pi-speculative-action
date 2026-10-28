import { writeFile } from "node:fs/promises";
import path from "node:path";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import type { AgentExecutionWorld } from "../src/agent-execution-world.ts";
import { resourceWorld, systemHost, expectHiddenCompute } from "./agent-system-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { onExit } from "./async.ts";
import { textOutput } from "./linux-process-fixture.ts";

const directories = temporaryDirectories("pi-host-closure-");
afterEach(directories.dispose);

test.for(["native", "external"] as const)("rejects every unsupported execution entry before provider work (%s)", async kind => {
	const cwd = await directories.create(), probes: string[] = [];
	await writeFile(path.join(cwd, "input.txt"), "bound input");
	const entered = async (entry: string): Promise<never> => { probes.push(entry); throw new Error("outside resource closure"); };
	let disposed = 0;
	const outside: AgentExecutionWorld = {
		...resourceWorld(), id: `outside-${kind}`,
		scope: "runtime", isolation: "runtime_sandbox",
		speculation: { ...resourceWorld().speculation!, fingerprint: () => entered("fingerprint"), prepare: () => entered("prepare"),
			diagnostics: () => entered("diagnostics"), execute: () => entered("execute") },
		observation: { ...resourceWorld().observation!, fingerprint: () => entered("capture-fingerprint"), capture: () => entered("capture"),
			diagnostics: () => entered("observation-diagnostics") },
		observeOperations: () => entered("observeOperations"),
		actorFallbackSettled: () => entered("actorFallbackSettled"), finishTurn: () => entered("finishTurn"),
		dispose: async () => { disposed++; },
	};
	const f = systemHost(cwd, [createReadTool(cwd)], { executionWorlds: [outside] });
	await using _cleanup = onExit(f.dispose);
	await f.host.executionWorldDiagnostics(true);
	await f.start();
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("bound input");
	await f.finish(); await f.start("second");
	await f.preview("read", { path: "input.txt" });
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("bound input");
	expectHiddenCompute(f.actors.at(-1)!.computation!, true);
	await f.finish(); await f.dispose();
	expect(probes).toEqual([]); expect(disposed).toBe(1);
});
