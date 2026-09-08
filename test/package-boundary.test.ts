import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";
import { createClosedSearchProfile, runCapturedSearchProcess } from "../src/pi-tool-invocation.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
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

	test.each([
		["host-neutral core", ["src/core.ts", "src/process-reuse.ts"], ["@earendil-works/pi-"]],
		["default Pi entry", ["src/index.ts", "src/extension.ts", "src/closed-search-process.mjs"], ["@thinkthread/agent-posix", "wasi-sh", "ripgrep", "globby"]],
	] as const)("loads %s without forbidden dependencies", async (_label, entries, blocked) => importWithBlockedDependencies(entries, blocked));

	test("loads ThinkThread only through its opt-in entry when the SDK is installed", async () => {
		const thinkThread = await import("../src/thinkthread/index.ts");
		expect(thinkThread.createThinkThreadExecutionWorld).toBeTypeOf("function");
		expect(thinkThread.createThinkThreadProfileExtension).toBeTypeOf("function");

		const manifest = JSON.parse(await fs.readFile(path.join(packageRoot, "package.json"), "utf8"));
		expect(manifest.peerDependenciesMeta["@thinkthread/agent-posix"]).toEqual({ optional: true });
	});

	test.runIf(process.platform === "linux")("restores installer publications or retains their recovery journal", async () => {
		const script = await fs.readFile(path.join(packageRoot, "scripts/install-thinkthread-profile.sh"), "utf8");
		const journal = script.slice(script.indexOf("replacements=()"), script.indexOf("package_output="));
		for (const fault of ["none", "installed", "payload", "staged-profile", "rollback"]) {
			const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-install-journal-"));
			try {
				for (const name of ["txn", "installed", "payload"]) await fs.mkdir(path.join(root, name));
				for (const [name, value] of [["installed/value", "old"], ["payload/value", "new"], ["profile", "old"], ["staged-profile", "new"]])
					await fs.writeFile(path.join(root, name!), value!);
				const result = await execFileAsync("bash", ["-c", `set -e
fixture_root="$1"; transaction_root="$1/txn"; fault="$2"
${journal}
mv() {
  if [[ "$2" == "$fixture_root/$fault" || ( "$fault" == rollback && ( "$2" == "$fixture_root/payload" || "$2" == "$transaction_root/previous-0" ) ) ]]; then return 17; fi
  command mv "$@"
}
replace_path "$fixture_root/payload" "$fixture_root/installed"
replace_path "$fixture_root/staged-profile" "$fixture_root/profile"
success=true`, "journal", root, fault]).then(() => true, () => false);
				expect(result, fault).toBe(fault === "none");
				expect(await fs.readFile(path.join(root, fault === "rollback" ? "txn/previous-0/value" : "installed/value"), "utf8"))
					.toBe(fault === "none" ? "new" : "old");
				expect(await fs.readFile(path.join(root, "profile"), "utf8")).toBe(fault === "none" ? "new" : "old");
				if (fault !== "rollback") await expect(fs.stat(path.join(root, "txn"))).rejects.toThrow();
			} finally { await fs.rm(root, { recursive: true, force: true }); }
		}
	});

	test("loads Pi and executes captured find with only Pi's installed dependencies", async () => {
		const temporaryRoot = await fs.mkdtemp(path.join(os.tmpdir(), "pi-speculative-package-"));
		const cwd = path.join(temporaryRoot, "workspace");
		const agentDir = path.join(temporaryRoot, "agent");
		await Promise.all([fs.mkdir(cwd), fs.mkdir(agentDir)]);
		try {
			const loaded = await discoverAndLoadExtensions([packageRoot], cwd, agentDir);
			expect(loaded.errors).toEqual([]);
			expect(loaded.extensions).toHaveLength(1);
			await fs.writeFile(path.join(cwd, "notes.txt"), "captured");
			for (const phase of ["preparation", "cleanup"]) {
				const environment = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
				const bound = (async () => {
					process.env.HOME = process.env.USERPROFILE = cwd;
					try { return await createClosedSearchProfile(cwd); }
					finally { for (const [name, value] of Object.entries(environment)) if (value === undefined) delete process.env[name]; else process.env[name] = value; }
				})();
				const { pool, invocations } = await bound;
				let reached!: () => void, resume!: () => void;
				const started = new Promise<void>((resolve) => { reached = resolve; }), paused = new Promise<void>((resolve) => { resume = resolve; });
				try {
					const find = invocations.get("find")!;
					expect(find.identity).toMatchObject({ home: cwd });
					const result = await find.authoritative!({ callID: "find", args: { pattern: "*.txt" }, signal: new AbortController().signal });
					expect(result.result.content).toEqual([{ type: "text", text: "notes.txt" }]);
					expect(await fs.readdir(agentDir)).toEqual([]);
					const homeResult = () => find.authoritative!({ callID: "home", args: { pattern: "*.txt", path: "~" }, signal: new AbortController().signal }).catch((error: Error) => error.message);
					const homeBefore = await homeResult();
					for (const target of ["~", "@.", pathToFileURL(cwd).href]) {
						const args = { pattern: "*.txt", path: target };
						const key = PI_ACTION_SEMANTICS.buildKey("find", args, cwd, "", { fingerprint: "bound-home", semantics: find.semantics })!;
						expect(key?.input).toEqual(args); // Indexing must not rewrite already-prepared execution inputs.
						expect(key.resources).toEqual(["."]);
						expect(await find.authoritative!({ callID: "key", args: key.input, signal: new AbortController().signal })).toEqual(result);
					}
					expect(homeBefore).toEqual(result); // Parent HOME changed back after binding; both key and worker retain it.
					for (const code of [undefined, "EACCES", "EIO"]) {
						const requested: string[] = [];
						await expect(pool.run("actor", (worker, signal) => worker.request({ kind: "grep", root: cwd, args: { pattern: "captured" }, home: agentDir }, {
							signal, onInput: async (operation) => { requested.push(operation); throw Object.assign(new Error("ungranted input"), { code }); },
						}))).rejects.toThrow(code ? `Path not found: ${cwd}` : "ungranted input");
						expect(requested).toEqual(["stat"]); // No process or ambient file access before the caller grants it.
					}
					expect(await homeResult()).toEqual(homeBefore); // A grep home binding cannot drift a later find invocation.
					if (phase === "cleanup") await expect(runCapturedSearchProcess(process.execPath,
						["-e", "setInterval(() => {}, 1000); process.stdout.write('owned');"], cwd,
						process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}, AbortSignal.timeout(3000), () => { throw 0; })).rejects.toBe(0);
					let entered = 0, retired = false;
					const executions = Promise.allSettled((["actor", "producer"] as const).map((role) => pool.run(role, async (worker, signal) => {
						if (phase === "cleanup") await worker.dispose(); // A closed worker must not erase its still-active cleanup owner.
						if (++entered === 2) reached();
						await paused; signal.throwIfAborted();
						return { result: { content: [], details: undefined }, isError: false };
					})));
					await Promise.race([started, executions]); expect(entered).toBe(2);
					const retirement = pool.dispose(); expect(pool.dispose()).toBe(retirement);
					void retirement.then(() => { retired = true; });
					await new Promise<void>((resolve) => setImmediate(resolve)); expect(retired, phase).toBe(false);
					resume();
					const results = await executions;
					expect(results.map((result) => result.status)).toEqual(["fulfilled", "rejected"]);
					expect(results[1]).toMatchObject({ reason: new Error("worker disposed") });
					await retirement;
					await expect(pool.run("actor", async () => { throw new Error("unexpected admission"); })).rejects.toThrow("search pool retired");
				} finally { resume(); await pool.dispose(); }
			}
		} finally {
			await fs.rm(temporaryRoot, { recursive: true, force: true });
		}
	});
});

async function importWithBlockedDependencies(entries: readonly string[], blockedPrefixes: readonly string[]) {
	const urls = entries.map((entry) => pathToFileURL(path.join(packageRoot, entry)).href);
	const emptyTools = entries.includes("src/closed-search-process.mjs") ? await fs.mkdtemp(path.join(os.tmpdir(), "pi-no-search-tools-")) : undefined;
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
		globalThis.fetch = () => { throw new Error("installation denied"); };
		await Promise.all(${JSON.stringify(urls)}.map((entry) => import(entry)));
		if (${!!emptyTools}) {
			const { createClosedSearchProfile } = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "src/pi-tool-invocation.ts")).href)});
			const search = await createClosedSearchProfile(process.cwd());
			try { if ([...search.invocations.keys()].join() !== "find") throw new Error("missing rg must not disable find or enable grep"); }
			finally { await search.pool.dispose(); }
		}
	`;
	try {
		const result = await execFileAsync(process.execPath, ["--import", "tsx", "--input-type=module", "--eval", script], {
			cwd: packageRoot, windowsHide: true,
			...(emptyTools ? { env: { ...process.env, PATH: "", PI_CODING_AGENT_DIR: emptyTools } } : {}),
		});
		expect(result.stderr).toBe("");
	} finally { if (emptyTools) await fs.rm(emptyTools, { recursive: true, force: true }); }
}
