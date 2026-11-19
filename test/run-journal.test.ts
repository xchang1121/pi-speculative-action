import path from "node:path";
import { appendFile } from "node:fs/promises";
import { afterEach, expect, test } from "vitest";
import { openRunJournal, recoverRun } from "../bench/run-journal.ts";
import { temporaryDirectories } from "./filesystem.ts";

const dirs = temporaryDirectories("pi-journal-");
afterEach(dirs.dispose);

test("recovers an interrupted tool wait without inventing compute or hidden time", async () => {
	const file = path.join(await dirs.create(), "run.jsonl");
	const journal = openRunJournal(file, "instance", "commit");
	const args = { path: "tests", pattern: "missing" };
	journal.write({ topic: "tool_start", id: "first", tool: "grep", args });
	journal.write({ topic: "timeout" });
	journal.close();
	await appendFile(file, '{"topic":"tool_end"');
	expect(await recoverRun(file)).toMatchObject({
		implementationCommit: "commit", ended: false, timedOut: true, starts: 1, timedCalls: 0,
		toolComputeMs: null, hiddenComputeMs: null, hiddenFraction: null,
		calls: [{ id: "first", args, completed: false, waitMs: expect.any(Number), hiddenComputeMs: null }],
	});
});
