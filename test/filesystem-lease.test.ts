import { afterAll, expect, test, vi } from "vitest";
import { link, lstat, mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { ResourceVersionManager } from "../src/resource-version.ts";
import { deferred, nextTurn } from "./async.ts";
import { temporaryDirectories, readText } from "./filesystem.ts";
import { createReadTool, createWriteTool, createEditTool } from "@earendil-works/pi-coding-agent";
import { SpeculativeConfiguration } from "../src/config-schema.ts";
import { windowsFilesystem } from "../src/windows-filesystem.ts";
import { systemHost } from "./agent-system-fixture.ts";

const directories = temporaryDirectories("pi-object-lease-");
afterAll(() => directories.dispose());
const output = { result: { content: [], details: {} }, isError: false };

test.skipIf(process.platform !== "win32").for(["missing", "store-stub"])(
	"ordinary file tools remain usable when the Windows lease interpreter is %s",
	async (mode) => {
		const cwd = await directories.create();
		const starts = path.join(cwd, "starts");
		const configuration = new SpeculativeConfiguration({
			filesystem: {
				windowsObjectLeaseCommand:
					mode === "missing"
						? [path.join(cwd, "missing-python.exe")]
						: [
								process.execPath,
								"-e",
								"require('node:fs').appendFileSync(process.argv[1], 'start\\n'); process.exit(9009)",
								"--",
								starts,
							],
				objectLeaseStartupTimeoutMs: 300,
				objectLeaseRetryMs: 30000,
			},
		});
		const service = configuration.run(() => new WorkspaceSandboxService());
		const world = service.createExecutionWorld({ inPlaceMutations: false });
		const host = systemHost(cwd, [createReadTool(cwd), createWriteTool(cwd), createEditTool(cwd)], {
			configuration,
			executionWorlds: [world],
		});
		await writeFile(path.join(cwd, "input.txt"), "base");
		try {
			await host.start();
			let failures = 0;
			for (const [tool, args, expected] of [
				["write", { path: "input.txt", content: "changed" }, "changed"],
				["edit", { path: "input.txt", edits: [{ oldText: "changed", newText: "final" }] }, "final"],
			] as const) {
				await host.preview(tool, args);
				await expect
					.poll(
						() =>
							host.events.filter((event) => event.type === "candidate" && event.state.status === "failed")
								.length,
					)
					.toBe(++failures);
				await host.execute(tool, args);
				expect(await readText(cwd, "input.txt")).toBe(expected);
				const read = await host.execute("read", { path: "input.txt" });
				expect(JSON.stringify(read.content)).toContain(expected);
			}
			if (mode === "store-stub") expect(await readText(starts)).toBe("start\n");
			expect(windowsFilesystem().processIDs()).toEqual([]);
		} finally {
			await host.dispose();
			await service.dispose();
		}
	},
);

test.for(["fresh", "stale", "closing", "alias", "alias-stale", "binary"])(
	"transfers workspace inputs through their owner (%s)",
	async (mode) => {
		const root = await directories.create();
		const service = new WorkspaceSandboxService();
		const target = path.join(root, "input");
		const entered = deferred<void>();
		const release = deferred<void>();
		let disposed = 0;
		let closed = false;
		const before = Buffer.from(mode === "binary" ? [0, 1, 255] : "before");
		const after = Buffer.from(mode === "binary" ? [0, 2, 254] : "after");
		await writeFile(target, before);
		if (mode.startsWith("alias")) await link(target, path.join(root, "alias"));
		const snapshot = await service.capture({
			cwd: root,
			driver: "in_memory",
			execute: async (workspace) => {
				await writeFile(path.join(workspace.sandboxRoot, "input"), after);
				return output;
			},
		});
		const action = PI_ACTION_SEMANTICS.buildKey(
			"write",
			{ path: "input", content: "after" },
			root,
			"input-transfer",
		)!;
		const branch = service.branch(
			{ cwd: root, action },
			{
				...snapshot,
				dispose: async () => {
					disposed++;
					await snapshot.dispose?.();
				},
			},
		);
		const validate = ResourceVersionManager.prototype.validate;
		const spy =
			mode === "closing"
				? vi.spyOn(ResourceVersionManager.prototype, "validate").mockImplementation(async function (
						this: ResourceVersionManager,
						token,
					) {
						entered.resolve();
						await release.promise;
						return validate.call(this, token);
					})
				: undefined;
		let inputs: Awaited<ReturnType<NonNullable<typeof branch.takeReadInputs>>>;
		try {
			if (mode.endsWith("stale"))
				await writeFile(mode === "alias-stale" ? path.join(root, "alias") : target, "changed");
			const reading = branch.takeReadInputs!(1024 * 1024);
			if (mode === "closing") {
				await Promise.race([
					entered.promise,
					reading.then(() => {
						throw new Error("input capture ended before validation");
					}),
				]);
				const closing = Promise.resolve(branch.dispose()).then(() => {
					closed = true;
				});
				await nextTurn();
				expect(closed).toBe(false);
				expect(disposed).toBe(0);
				release.resolve();
				await closing;
			}
			inputs = await reading;
			expect(!!inputs).toBe(!mode.endsWith("stale") && mode !== "closing");
			if (inputs) {
				expect((await inputs.validate!()).status).toBe("valid");
				await branch.commit();
				expect(await readFile(target)).toEqual(after);
				const committed = await branch.takeCommittedInputs!(1024 * 1024);
				try {
					expect(committed).toBeDefined();
					expect((await committed!.validate!()).status).toBe("valid");
				} finally {
					await committed?.dispose();
				}
			}
			expect(await branch.takeReadInputs!(1024 * 1024)).toBeUndefined();
		} finally {
			release.resolve();
			spy?.mockRestore();
			await inputs?.dispose();
			await branch.dispose();
			await branch.dispose();
			await service.dispose();
		}
		expect(disposed).toBe(1);
	},
);

test.for([false, true])(
	"adopts a renamed private directory and its checkpoint successor (checkpoint=%s)",
	async (checkpoint) => {
		const bare = await directories.create();
		const root = await directories.create();
		const service = new WorkspaceSandboxService();
		const setup = async (cwd: string) => {
			await mkdir(path.join(cwd, "directory"));
			await writeFile(path.join(cwd, "directory/input"), "base");
			await link(path.join(cwd, "directory/input"), path.join(cwd, "alias"));
		};
		const operate = async (cwd: string) => {
			await rename(path.join(cwd, "directory"), path.join(cwd, "moved"));
			await writeFile(path.join(cwd, "moved/input"), "changed");
		};
		await setup(bare);
		await setup(root);
		await operate(bare);
		try {
			const captured = await service.capture({
				cwd: root,
				driver: "in_memory",
				execute: async (workspace) => {
					await operate(workspace.sandboxRoot);
					return output;
				},
			});
			expect(await readText(root, "directory/input")).toBe("base");
			expect(await readdir(root)).not.toContain("moved");
			const action = PI_ACTION_SEMANTICS.buildKey(
				"write",
				{ path: "moved/input", content: "changed" },
				root,
				"lease",
			)!;
			const branch = service.branch({ cwd: root, action }, captured);
			try {
				const successor = checkpoint
					? await service.capture(
							{
								cwd: root,
								driver: "in_memory",
								execute: async (workspace) => {
									expect(await readText(workspace.sandboxRoot, "moved/input")).toBe("changed");
									await writeFile(path.join(workspace.sandboxRoot, "moved/input"), "successor");
									return output;
								},
							},
							service.checkpoint(branch.checkpoint, root),
						)
					: undefined;
				await branch.commit();
				await branch.commit();
				for (const name of ["moved/input", "alias"])
					expect(await readText(root, name)).toBe(await readText(bare, name));
				expect((await lstat(path.join(root, "moved/input"), { bigint: true })).ino).toBe(
					(await lstat(path.join(root, "alias"), { bigint: true })).ino,
				);
				if (successor) {
					await service.commitDelta(successor);
					expect(await readText(root, "alias")).toBe("successor");
				}
			} finally {
				await branch.dispose();
			}
		} finally {
			await service.dispose();
		}
	},
);

test.skipIf(process.platform !== "win32")(
	"qualifies Windows file-ID leases, hardlink capacity and owner cleanup",
	{ timeout: 90_000 },
	async () => {
		const { stdout } = await promisify(execFile)(process.execPath, [
			"--expose-gc",
			fileURLToPath(new URL("./windows-filesystem.fixture.mjs", import.meta.url)),
		]);
		const result = JSON.parse(stdout);
		expect(result.results).toHaveLength(13);
		expect(
			result.results.every((entry: { status: string }) => entry.status === "passed"),
			stdout,
		).toBe(true);
		expect(result.cleaned).toBe(true);
		expect(result.liveLeases).toBe(0);
		expect(result.processes).toEqual([]);
	},
);

test("the legacy git driver selects the same immutable snapshot without a Git executable", async () => {
	const service = new WorkspaceSandboxService();
	try {
		const legacy = await service.qualify({ driver: "git", gitBinary: "not-installed" }, process.cwd());
		expect(legacy).toEqual(await service.qualify({ driver: "in_memory" }));
		expect(legacy.driver).toBe("in_memory");
	} finally {
		await service.dispose();
	}
});

test("closes every acquired sandbox lease even when concurrency is 1 and the first close fails", async () => {
	const fsEvidence = await import("../src/filesystem-evidence.ts");
	let closed = 0;
	const configuration = new SpeculativeConfiguration({ filesystem: { concurrency: 1 } });
	await expect(
		configuration.run(() =>
			fsEvidence.settleFilesystem([0, 1, 2, 3, 4, 5, 6], async (index) => {
				closed++;
				if (index === 0) throw new Error("first lease close failed");
			}),
		),
	).rejects.toThrow("first lease close failed");
	expect(closed).toBe(7);
});
