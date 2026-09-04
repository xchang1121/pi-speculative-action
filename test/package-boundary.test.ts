import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";
import { describe, expect, test } from "vitest";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const execFileAsync = promisify(execFile);

describe("speculative action package boundary", () => {
	test("runs the root, core, and process-reuse public entries", async () => {
		const [root, core, processReuse] = await Promise.all([
			import("../src/index.ts"),
			import("../src/core.ts"),
			import("../src/process-reuse.ts"),
		]);

		expect(core.zeroValidationMetrics()).toEqual({
			durationMs: 0,
			bytesRead: 0,
			filesRead: 0,
			mode: "exact",
		});
		expect(processReuse.digestObject({ command: "printf ready" })).toMatch(/^sha256:[a-f0-9]{64}$/);
		expect(root.makeSpeculativeActionRuntime).toBe(core.makeSpeculativeActionRuntime);
		expect(root.ProcessReusePlanner).toBe(processReuse.ProcessReusePlanner);
	});

	test("keeps host-neutral entries executable without Pi packages", async () => {
		await importWithBlockedDependencies(["src/core.ts", "src/process-reuse.ts"], ["@earendil-works/pi-"]);
	});

	test("loads the default Pi entry while the ThinkThread SDK is unavailable", async () => {
		await importWithBlockedDependencies(["src/index.ts", "src/extension.ts"], ["@thinkthread/agent-posix"]);
	});

	test("loads ThinkThread only through its opt-in entry when the SDK is installed", async () => {
		const thinkThread = await import("../src/thinkthread/index.ts");
		expect(thinkThread.createThinkThreadExecutionWorld).toBeTypeOf("function");
		expect(thinkThread.createThinkThreadProfileExtension).toBeTypeOf("function");

		const manifest = JSON.parse(await fs.readFile(path.join(packageRoot, "package.json"), "utf8"));
		expect(manifest.exports["./thinkthread-extension"]).toBeDefined();
		expect(manifest.peerDependenciesMeta["@thinkthread/agent-posix"]).toEqual({ optional: true });
		await expect(fs.stat(path.join(packageRoot, "scripts", "install-thinkthread-profile.sh"))).resolves.toBeDefined();
	});

	test("loads from its package manifest through Pi's public extension loader", async () => {
		const temporaryRoot = await fs.mkdtemp(path.join(os.tmpdir(), "pi-speculative-package-"));
		const cwd = path.join(temporaryRoot, "workspace");
		const agentDir = path.join(temporaryRoot, "agent");
		await Promise.all([fs.mkdir(cwd), fs.mkdir(agentDir)]);
		try {
			const loaded = await discoverAndLoadExtensions([packageRoot], cwd, agentDir);
			expect(loaded.errors).toEqual([]);
			expect(loaded.extensions).toHaveLength(1);
		} finally {
			await fs.rm(temporaryRoot, { recursive: true, force: true });
		}
	});
});

async function importWithBlockedDependencies(entries: readonly string[], blockedPrefixes: readonly string[]) {
	const urls = entries.map((entry) => pathToFileURL(path.join(packageRoot, entry)).href);
	const script = `
		import { registerHooks } from "node:module";
		const blocked = ${JSON.stringify(blockedPrefixes)};
		registerHooks({
			resolve(specifier, context, nextResolve) {
				if (blocked.some((prefix) => specifier.startsWith(prefix))) {
					throw new Error(\`blocked package boundary: \${specifier}\`);
				}
				return nextResolve(specifier, context);
			},
		});
		await Promise.all(${JSON.stringify(urls)}.map((entry) => import(entry)));
	`;
	const result = await execFileAsync(process.execPath, ["--import", "tsx", "--input-type=module", "--eval", script], {
		cwd: packageRoot,
		windowsHide: true,
	});
	expect(result.stderr).toBe("");
}
