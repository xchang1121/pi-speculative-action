import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	createExecPrototype,
	dependencyPathsetKey,
	type DynamicDependency,
	processWeakKey,
	sealProcessCertificate,
	sha256Digest,
} from "../src/provenance-certificate.ts";
import {
	captureAbsenceDependency,
	captureDirectoryDependency,
	captureFileDependency,
	captureSymlinkDependency,
	validateProcessCertificate,
} from "../src/provenance-validation.ts";

const roots: string[] = [];

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("process provenance certificates", () => {
	it("validates positive, directory, negative, symlink, executable, and DSO evidence", async () => {
		const root = await workspace();
		await mkdir(path.join(root, "lib"));
		await writeFile(path.join(root, "input.txt"), "one");
		await writeFile(path.join(root, "tool"), "executable");
		await writeFile(path.join(root, "lib", "runtime.so"), "library");
		let link: Awaited<ReturnType<typeof captureSymlinkDependency>> | undefined;
		try {
			await symlink("input.txt", path.join(root, "input.link"));
			link = await captureSymlinkDependency(path.join(root, "input.link"), "/workspace/input.link");
		} catch (error) {
			if (!(error && typeof error === "object" && "code" in error && error.code === "EPERM")) throw error;
			// Windows without Developer Mode cannot create symlinks; Linux integration covers this path.
		}
		const input = await captureFileDependency(path.join(root, "input.txt"), "/workspace/input.txt");
		const executable = await captureFileDependency(
			path.join(root, "tool"),
			"/workspace/tool",
			"executable",
		);
		const library = await captureFileDependency(
			path.join(root, "lib", "runtime.so"),
			"/workspace/lib/runtime.so",
			"shared_object",
		);
		const directory = await captureDirectoryDependency(path.join(root, "lib"), "/workspace/lib");
		const absent = await captureAbsenceDependency(
			path.join(root, "missing.txt"),
			"/workspace/missing.txt",
		);
		if (!absent) throw new Error("expected negative lookup evidence");
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			dependencyCertificate: {
				complete: true,
				dependencies: [
					input.dependency,
					executable.dependency,
					library.dependency,
					directory,
					absent,
					...(link ? [link] : []),
				],
				taints: [],
			},
			result: { replayProfile: "buffered_noninteractive", observedProcessMs: 1250.5, journal: [], exit: { kind: "code", code: 0 } },
		});
		const validation = await validateProcessCertificate(certificate, {
			resolvePath: (logical) => path.join(root, path.posix.relative("/workspace", logical)),
		});

		expect(validation).toMatchObject({ status: "valid", filesRead: 3 });
		expect(certificate.strongKey).toBe(
			validation.status === "valid" ? validation.strongKey : undefined,
		);
		expect(certificate.result.observedProcessMs).toBe(1250.5);

		await writeFile(path.join(root, "input.txt"), "changed");
		expect(
			await validateProcessCertificate(certificate, {
				resolvePath: (logical) => path.join(root, path.posix.relative("/workspace", logical)),
			}),
		).toMatchObject({ status: "stale", changed: expect.arrayContaining(["/workspace/input.txt"]) });
	});

	it("invalidates negative lookups and directory enumerations and fails closed on taints", async () => {
		const root = await workspace();
		await mkdir(path.join(root, "tree"));
		const directory = await captureDirectoryDependency(path.join(root, "tree"), "/workspace/tree");
		const absent = await captureAbsenceDependency(path.join(root, "missing"), "/workspace/missing");
		if (!absent) throw new Error("expected absence");
		const base = {
			prototype: prototype(),
			result: { replayProfile: "buffered_noninteractive" as const, journal: [], exit: { kind: "code" as const, code: 0 } },
		};
		const certificate = sealProcessCertificate({
			...base,
			dependencyCertificate: { complete: true, dependencies: [directory, absent], taints: [] },
		});
		await writeFile(path.join(root, "tree", "new.txt"), "new");
		await writeFile(path.join(root, "missing"), "appeared");
		const validation = await validateProcessCertificate(certificate, {
			resolvePath: (logical) => path.join(root, path.posix.relative("/workspace", logical)),
		});
		expect(validation).toMatchObject({
			status: "stale",
			changed: expect.arrayContaining(["/workspace/tree", "/workspace/missing"]),
		});

		const tainted = sealProcessCertificate({
			...base,
			dependencyCertificate: { complete: true, dependencies: [], taints: ["clock"] },
		});
		expect(await validateProcessCertificate(tainted)).toMatchObject({ status: "indeterminate", reason: "tainted:clock" });
	});

	it("keys complete environment and process context without persisting raw values", () => {
		const first = prototype({ SECRET: "alpha", MODE: "build" });
		const second = prototype({ SECRET: "beta", MODE: "build" });

		expect(processWeakKey(first)).not.toBe(processWeakKey(second));
		expect(JSON.stringify(first)).not.toContain("alpha");
		expect(JSON.stringify(first)).not.toContain("--compile");
		expect(first).not.toHaveProperty("argv");
		expect(Object.isFrozen(first.environment)).toBe(true);
	});

	it("projects backend-private directory entries consistently during validation", async () => {
		const root = await workspace();
		await writeFile(path.join(root, "tracked.txt"), "base");
		const directory = await captureDirectoryDependency(root, "/workspace", true, [".git"]);
		const absent = await captureAbsenceDependency(path.join(root, "artifact.txt"), "/workspace/artifact.txt", true, [
			".git",
		]);
		if (!absent) throw new Error("expected absence");
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			dependencyCertificate: { complete: true, dependencies: [directory, absent], taints: [] },
			result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } },
		});
		await mkdir(path.join(root, ".git"));
		const resolvePath = (logical: string) => path.join(root, path.posix.relative("/workspace", logical));
		expect(await validateProcessCertificate(certificate, { resolvePath })).toMatchObject({ status: "valid" });

		await writeFile(path.join(root, "other.txt"), "changed");
		expect(await validateProcessCertificate(certificate, { resolvePath })).toMatchObject({
			status: "stale",
			changed: expect.arrayContaining(["/workspace", "/workspace/artifact.txt"]),
		});
	});

	it("keeps dependency capture semantics in the dynamic pathset identity", async () => {
		const root = await workspace();
		const target = path.join(root, "input.txt");
		await writeFile(target, "content");
		const contentOnly = await captureFileDependency(target, "/workspace/input.txt", "input");
		const withMetadata = await captureFileDependency(target, "/workspace/input.txt", "input", {
			includeMetadata: true,
		});
		const executable = await captureFileDependency(target, "/workspace/input.txt", "executable");
		await mkdir(path.join(root, "directory"));
		await mkdir(path.join(root, "directory", ".private"));
		const fullDirectory = await captureDirectoryDependency(path.join(root, "directory"), "/workspace/directory");
		const projectedDirectory = await captureDirectoryDependency(
			path.join(root, "directory"),
			"/workspace/directory",
			false,
			[".private"],
		);
		const absentWithParent = await captureAbsenceDependency(
			path.join(root, "missing"),
			"/workspace/missing",
			true,
		);
		const absentWithoutParent = await captureAbsenceDependency(
			path.join(root, "missing"),
			"/workspace/missing",
			false,
		);
		if (!absentWithParent || !absentWithoutParent) throw new Error("expected absence evidence");
		const key = (dependency: DynamicDependency) =>
			dependencyPathsetKey({ complete: true, dependencies: [dependency], taints: [] });

		expect(key(contentOnly.dependency)).not.toBe(key(withMetadata.dependency));
		expect(key(contentOnly.dependency)).not.toBe(key(executable.dependency));
		expect(key(fullDirectory)).not.toBe(key(projectedDirectory));
		expect(key(absentWithParent)).not.toBe(key(absentWithoutParent));
	});

	it("rejects conflicting sizes for one content-addressed effect", () => {
		const digest = sha256Digest("same digest");
		expect(() =>
			sealProcessCertificate({
				prototype: prototype(),
				dependencyCertificate: { complete: true, dependencies: [], taints: [] },
				result: {
					replayProfile: "buffered_noninteractive",
					journal: [
						{ sequence: 0, kind: "output", fd: 1, data: { digest, size: 11 } },
						{ sequence: 1, kind: "write", path: "/workspace/out", data: { digest, size: 12 }, mode: 0o644 },
					],
					exit: { kind: "code", code: 0 },
				},
			}),
		).toThrow("conflicting effect artifact sizes");
	});

	it("seals exact typed directory state and rejects malformed topology effects", () => {
		const entriesDigest = sha256Digest("empty directory");
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			dependencyCertificate: { complete: true, dependencies: [], taints: [] },
			result: {
				replayProfile: "buffered_noninteractive",
				journal: [
					{ sequence: 0, kind: "mkdir", path: "/workspace/generated", entriesDigest, mode: 0o750, uid: 1000, gid: 1000 },
					{ sequence: 1, kind: "rmdir", path: "/workspace/obsolete" },
				],
				exit: { kind: "code", code: 0 },
			},
		});
		expect(certificate.result.journal[0]).toMatchObject({ kind: "mkdir", entriesDigest, mode: 0o750 });

		expect(() =>
			sealProcessCertificate({
				prototype: prototype(),
				dependencyCertificate: { complete: true, dependencies: [], taints: [] },
				result: {
					replayProfile: "buffered_noninteractive",
					journal: [
						{
							sequence: 0,
							kind: "mkdir",
							path: "/workspace/generated",
							entriesDigest: "not-a-digest",
							mode: 0o750,
							uid: -1,
							gid: 1000,
						},
					] as never,
					exit: { kind: "code", code: 0 },
				},
			}),
		).toThrow("invalid mkdir effect state");
	});
});

function prototype(environment: Readonly<Record<string, string | undefined>> = { MODE: "build" }) {
	const digest = (value: string) => sha256Digest(value);
	return createExecPrototype({
		executablePath: "/workspace/tool",
		executableDigest: digest("executable"),
		argv: ["tool", "--compile"],
		logicalCwd: "/workspace",
		environment,
		umask: 0o22,
		rlimitsDigest: digest("rlimits"),
		signalDispositionsDigest: digest("signals"),
		credentialsDigest: digest("credentials"),
		schedulingDigest: digest("scheduling"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux-x64:kernel",
		monitorEpoch: "monitor-v1",
		policyID: "closed-v1",
	});
}

async function workspace(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-provenance-"));
	roots.push(root);
	return root;
}
