import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

test("ordinary tools share proven internal calculations and preserve data, effects and ownership", async () => {
	const { stdout } = await promisify(execFile)(process.execPath, ["--disable-warning=ExperimentalWarning",
		fileURLToPath(new URL("./resource-computation.mjs", import.meta.url))], { timeout: 30_000 });
	expect(JSON.parse(stdout)).toMatchObject({ counts: { effects: 5, reads: 5, renders: 5 }, indexExecutions: 3 });
}, 35_000);

test("intrinsics replaced before extension loading remain ordinary calls", async () => {
	for (const [flag, calls] of [["--preload", 2], ["--preload-opaque", 0]] as const) {
		const { stdout } = await promisify(execFile)(process.execPath, ["--disable-warning=ExperimentalWarning",
			fileURLToPath(new URL("./resource-computation.mjs", import.meta.url)), flag], { timeout: 30_000 });
		expect(JSON.parse(stdout)).toEqual({ preloadedCalls: calls });
	}
}, 35_000);
