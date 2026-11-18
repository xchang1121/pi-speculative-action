import { chmod, link, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { expect, test } from "vitest";
import { FilesystemReadView } from "../src/filesystem-evidence.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { inspectLinuxEnvironment, privateReplay } from "../src/linux-process-backend.ts";
import {
	executionDomainCollector,
	executionDomainProof,
	resolveLinuxExecHelper,
} from "../src/linux-held-exec.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { sha256Digest } from "../src/primitives.ts";

test.skipIf(process.platform !== "linux").each([
	["with space 中", ""],
	["with\\backslash", ""],
	["long/" + "x".repeat(240), ""],
	["d/".repeat(18) + "leaf", ""],
	["shared", "root_permissions"],
	["d/".repeat(70) + "leaf", "root_depth_limit"],
])("uses the same workspace admission for diagnostics and reuse (%s)", async (suffix, reason) => {
	const base = await mkdtemp(path.join(os.homedir(), "pi-domain-admission-"));
	const root = path.join(base, suffix!);
	const binary = await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC);
	nativeProcessLibrary(binary);
	const collector = await executionDomainCollector(binary);
	const before = (await readdir("/proc/self/fd")).length;
	let proof: ReturnType<typeof executionDomainProof> | undefined;
	try {
		await mkdir(root, { recursive: true });
		await chmod(root, reason === "root_permissions" ? 0o775 : 0o700);
		await writeFile(path.join(root, "input"), "original");
		proof = executionDomainProof(collector, root);
		const valid = proof.seal(
			[
				{
					kind: "file",
					path: path.join(root, "input"),
					role: "input",
					contentDigest: sha256Digest("original"),
				},
			],
			(target) => target,
		);
		expect(valid(), proof.reason).toBe(!reason);
		const report = await inspectLinuxEnvironment(root);
		expect(report.text).toContain(reason || "Workspace admission: available");
		if (reason) {
			expect(proof.reason).toContain(reason);
			expect(report.warnings).toBe(true);
		} else {
			await writeFile(path.join(root, "input"), "changed");
			expect(valid()).toBe(false);
		}
	} finally {
		proof?.close();
		await rm(base, { recursive: true, force: true });
	}
	expect((await readdir("/proc/self/fd")).length).toBe(before);
});

test("reports the unavailable execution domain without claiming process qualification", async () => {
	const socket = process.env.PI_SPEC_DOMAIN_SOCKET;
	try {
		delete process.env.PI_SPEC_DOMAIN_SOCKET;
		const report = await inspectLinuxEnvironment();
		expect(report.warnings).toBe(true);
		expect(report.text).toContain(
			process.platform === "linux" ? "execution_domain_missing" : "Linux or WSL 2",
		);
	} finally {
		if (socket !== undefined) process.env.PI_SPEC_DOMAIN_SOCKET = socket;
	}
});

test.skipIf(process.platform !== "linux")(
	"replays creations, deletions and hard links in the private upper",
	async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-private-replay-"));
		const storage = path.join(root, "branch");
		const service = new WorkspaceSandboxService();
		await using view = FilesystemReadView.ambient();
		const primary = path.join(root, "original");
		const alias = path.join(root, "alias");
		const added = path.join(root, "new");
		const address = (target: string) => ({ target, root, resource: path.relative(root, target) });
		const upper = (target: string) => path.join(storage, "writes", "upper", target);
		const before = Buffer.from("original");
		const after = Buffer.from("edited");
		try {
			await writeFile(primary, before);
			await link(primary, alias);
			await expect(
				privateReplay(service, storage, [{ ...address(added), after }], view, undefined, [
					{ ...address(primary), before: after },
				]),
			).rejects.toThrow();
			await expect(stat(upper(added))).rejects.toMatchObject({ code: "ENOENT" });
			await privateReplay(
				service,
				storage,
				[
					{ ...address(added), after },
					...[primary, alias].map((target) => ({
						...address(target),
						before,
						after,
						aliases: [primary, alias],
						afterAliases: [primary, alias],
						object: { path: primary, before: true },
						operation: "write_contents" as const,
					})),
				],
				view,
			);
			expect(await readFile(upper(added))).toEqual(after);
			expect(await readFile(upper(primary))).toEqual(after);
			expect((await stat(upper(primary))).ino).toBe((await stat(upper(alias))).ino);
			expect(await readFile(primary)).toEqual(before);
			await expect(stat(added)).rejects.toMatchObject({ code: "ENOENT" });
			await expect(
				privateReplay(service, storage, [{ ...address(primary), before }], view),
			).rejects.toThrow();
			await privateReplay(service, storage, [{ ...address(primary), before: after }], view);
			const log = await readFile(path.join(storage, "writes", "deleted.log"), "utf8");
			expect(log.split("\n")).toContain(primary.slice(1));
			await privateReplay(service, storage, [{ ...address(primary), after: before }], view);
			expect(await readFile(upper(alias))).toEqual(after);
		} finally {
			await service.dispose();
			await rm(root, { recursive: true, force: true });
		}
	},
);
