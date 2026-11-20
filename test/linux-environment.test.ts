import { chmod, link, mkdir, mkdtemp, open, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { expect, test } from "vitest";
import { FilesystemReadView } from "../src/filesystem-evidence.ts";
import {
	directoryEntriesDigest,
	readSandboxDirectoryState,
	WorkspaceSandboxService,
} from "../src/workspace-sandbox.ts";
import { inspectLinuxEnvironment, privateReplay } from "../src/linux-process-backend.ts";
import {
	executionDomainCollector,
	executionDomainProof,
	resolveLinuxExecHelper,
} from "../src/linux-held-exec.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { execHostFile, sha256Digest } from "../src/primitives.ts";
import { speculativeConfig } from "../src/config-schema.ts";

test.skipIf(!process.env.PI_SPEC_DOMAIN_SOCKET || process.platform !== "linux")(
	"continues sealing and detects new writes after the kernel object history fills", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-domain-capacity-"));
		const collector = await executionDomainCollector(await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC));
		let proof: ReturnType<typeof executionDomainProof> | undefined;
		try {
			await execHostFile("python3", ["-c", [
				"import os, sys", "for _ in range(int(sys.argv[1])):",
				" fd = os.memfd_create('pi-history-test')", " os.write(fd, b'x')", " os.close(fd)",
			].join("\n"), String(speculativeConfig().nativeBuild.domainRecords + 1)]);
			const file = path.join(root, "late");
			await writeFile(file, "original");
			proof = executionDomainProof(collector, root);
			const valid = proof.seal([{ kind: "file", path: file, role: "input", contentDigest: sha256Digest("original") }], (p) => p);
			expect(valid(), proof.reason).toBe(true);
			await writeFile(file, "modified");
			expect(valid()).toBe(false);
		} finally {
			proof?.close();
			await rm(root, { recursive: true, force: true });
		}
	}, 30_000,
);

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
			const writer = await open(path.join(root, "input"), "r+");
			try { expect(valid()).toBe(false); }
			finally { await writer.close(); }
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

test.skipIf(process.platform !== "linux").each(["create", "copy-up"])(
	"restores exact private directory permissions and replays its next baseline (%s)",
	async (kind) => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-private-mode-"));
		const target = path.join(root, "dir");
		const storage = path.join(root, "branch");
		const upper = path.join(storage, "writes", "upper", target);
		const service = new WorkspaceSandboxService();
		await using view = FilesystemReadView.ambient();
		try {
			if (kind === "copy-up") {
				await mkdir(target, { mode: 0o750 });
				await writeFile(path.join(target, "inherited"), "host");
			}
			const before = await readSandboxDirectoryState(target);
			const parent = await stat(root);
			const after = {
				entriesDigest: before?.entriesDigest ?? directoryEntriesDigest([]),
				uid: parent.uid,
				gid: parent.gid,
				mode: kind === "create" ? 0o777 : 0o710,
			};
			const change = {
				kind: "directory",
				operation: "mkdir",
				root,
				target,
				resource: "dir",
				before,
				after,
			} as const;
			await privateReplay(service, storage, [change], view);
			expect((await stat(upper)).mode & 0o777).toBe(after.mode);
			await privateReplay(
				service,
				storage,
				[{ ...change, operation: undefined, before: after, after: { ...after, mode: 0o700 } }],
				view,
			);
			expect((await stat(upper)).mode & 0o777).toBe(0o700);
			await expect(privateReplay(service, storage, [{ ...change, before: after }], view)).rejects.toThrow(
				"resource changed",
			);
			expect((await stat(upper)).mode & 0o777).toBe(0o700);
			expect(await readSandboxDirectoryState(target)).toEqual(before);
		} finally {
			await service.dispose();
			await rm(root, { recursive: true, force: true });
		}
	},
);

test.skipIf(process.platform !== "linux").each(["shadowed", "whiteout"])(
	"only seeds visible lower hard links in a private replay (%s)",
	async (kind) => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-private-alias-"));
		const storage = path.join(root, "branch");
		const upper = (name: string) => path.join(storage, "writes", "upper", name);
		const primary = path.join(root, "primary");
		const sibling = path.join(root, "sibling");
		const alias = path.join(root, "alias");
		const before = Buffer.from("host");
		const after = Buffer.from("replayed");
		const shadow = Buffer.from("earlier private change");
		const service = new WorkspaceSandboxService();
		await using view = FilesystemReadView.ambient();
		try {
			await writeFile(primary, before);
			await link(primary, sibling);
			await link(primary, alias);
			await privateReplay(
				service,
				storage,
				[{ root, target: alias, resource: "alias", before, after: kind === "shadowed" ? shadow : undefined }],
				view,
			);
			const previous = kind === "shadowed" ? await stat(upper(alias)) : undefined;
			await privateReplay(
				service,
				storage,
				[
					{
						operation: "write_contents",
						root,
						target: primary,
						resource: "primary",
						before,
						after,
						aliases: [primary, sibling, alias],
					},
				],
				view,
			);
			expect(await readFile(upper(primary))).toEqual(after);
			expect(await readFile(upper(sibling))).toEqual(after);
			expect((await stat(upper(sibling))).ino).toBe((await stat(upper(primary))).ino);
			if (previous) {
				expect((await stat(upper(alias))).ino).toBe(previous.ino);
				expect(await readFile(upper(alias))).toEqual(shadow);
			} else {
				await expect(stat(upper(alias))).rejects.toMatchObject({ code: "ENOENT" });
				const log = await readFile(path.join(storage, "writes", "deleted.log"), "utf8");
				expect(log.split("\n")).toContain(alias.slice(1));
			}
			for (const name of [primary, sibling, alias]) expect(await readFile(name)).toEqual(before);
		} finally {
			await service.dispose();
			await rm(root, { recursive: true, force: true });
		}
	},
);
