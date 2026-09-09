import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { validateTransferredProcessEvidence } from "../src/linux-process-backend.ts";
import {
	createExecPrototype,
	dependencyPathsetKey,
	type DynamicDependency,
	type OrderedEffectEvent,
	parseProcessCertificate,
	processStrongKey,
	processWeakKey,
	type ProvenanceTaint,
	sealProcessCertificate,
	sha256Digest,
} from "../src/provenance-certificate.ts";
import {
	captureAbsenceDependency,
	captureDirectoryDependency,
	captureFileDependency,
	captureMetadataDependency,
	captureSymlinkDependency,
	validateProcessCertificate,
} from "../src/provenance-validation.ts";

const roots: string[] = [];
const PRODUCER = {
	observer: { provider: "test", fingerprint: sha256Digest("observer-v1") },
	execution: {
		authority: "speculative" as const,
		confinement: { provider: "test", fingerprint: sha256Digest("confinement-v1") },
	},
};

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("process provenance certificates", () => {
	it("validates positive, directory, negative, symlink, executable, and DSO evidence", async () => {
		const root = await workspace();
		await mkdir(path.join(root, "lib"));
		await writeFile(path.join(root, "input.txt"), "one");
		await writeFile(path.join(root, "metadata.txt"), "metadata");
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
		const metadata = await captureMetadataDependency(path.join(root, "metadata.txt"), "/workspace/metadata.txt", true);
		const absent = await captureAbsenceDependency(
			path.join(root, "missing.txt"),
			"/workspace/missing.txt",
		);
		if (!absent) throw new Error("expected negative lookup evidence");
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			producer: PRODUCER,
			dependencyCertificate: {
				complete: true,
				dependencies: [
					input.dependency,
					metadata,
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
			producer: PRODUCER,
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

	it("transfers native one-shot inputs but rejects external effects", async () => {
		const validate = (taint: ProvenanceTaint) =>
			validateTransferredProcessEvidence({ complete: true, dependencies: [], taints: [taint] });
		for (const taint of ["clock", "random", "pid_observation"] satisfies ProvenanceTaint[]) {
			expect(await validate(taint)).toMatchObject({ status: "valid" });
		}
		for (const taint of ["network", "ipc"] satisfies ProvenanceTaint[]) {
			expect(await validate(taint)).toMatchObject({
				status: "indeterminate",
				cause: { detail: `tainted:${taint}` },
			});
		}
	});

	it("keys complete environment and process context without persisting raw values", () => {
		const environment = { SECRET: "alpha", MODE: "build", "\u00e9": "two", "e\u0301": "one", _: "underscore", z: undefined, Z: "upper" };
		const first = prototype(environment);
		const second = prototype({ ...environment, SECRET: "beta" });

		expect(processWeakKey(first)).not.toBe(processWeakKey(second));
		expect(processWeakKey(first)).toBe(processWeakKey(prototype(Object.fromEntries(Object.entries(environment).reverse()))));
		expect(first.environment.map((entry) => entry.name)).toEqual(["MODE", "SECRET", "Z", "_", "e\u0301", "z", "\u00e9"]);
		expect(JSON.stringify(first)).not.toContain("alpha");
		expect(JSON.stringify(first)).not.toContain("--compile");
		expect(first).not.toHaveProperty("argv");
		expect(Object.isFrozen(first.environment)).toBe(true);
		const stdin = Object.freeze({ type: "bytes" as const, digest: sha256Digest("input"), eof: true, metadata: { label: "stdin" } });
		const fd = { fd: 3, type: "regular" as const, flagsDigest: sha256Digest("flags"), offset: 0, metadata: { label: "fd" } };
		const argv = Buffer.from("tool\0--compile\0"), inheritedFDs = [fd];
		const captured = createExecPrototype({ ...first, environment, argv, stdin, inheritedFDs });
		expect([Object.isFrozen(fd), Object.isFrozen(stdin.metadata), Object.isFrozen(fd.metadata)]).toEqual([false, false, false]);
		const key = processWeakKey(captured);
		stdin.metadata.label = fd.metadata.label = "changed";
		fd.offset = 99; inheritedFDs.length = 0; argv.fill(0);
		expect(captured.stdin).toMatchObject({ metadata: { label: "stdin" } });
		expect(captured.inheritedFDs).toMatchObject([{ offset: 0, metadata: { label: "fd" } }]);
		expect(Object.isFrozen(Reflect.get(captured.stdin, "metadata"))).toBe(true);
		expect(Object.isFrozen(Reflect.get(captured.inheritedFDs[0]!, "metadata"))).toBe(true);
		expect(captured.argvDigest).toBe(sha256Digest("tool\0--compile\0"));
		expect(processWeakKey(captured)).toBe(key);
		expect(() => createExecPrototype({ ...first, environment: { "BAD=NAME": "value" }, argv })).toThrow("process prototype environment is incomplete");
		for (const patch of [
			{ environmentComplete: false }, { fileDescriptorTableComplete: false }, { umask: -1 },
			{ executableDigest: "invalid" }, { environment: [...first.environment, first.environment[0]] },
			{ platformFingerprint: { mutable: true } },
			{ stdin: undefined }, { stdin: { type: "bytes", digest: "invalid", eof: true } },
			{ inheritedFDs: [{ fd: 3, type: "regular", flagsDigest: "invalid" }] },
		]) {
			const malformed = Object.freeze({ ...first, ...patch }) as never;
			expect(() => processWeakKey(malformed)).toThrow();
			expect(() => sealProcessCertificate({ prototype: malformed, producer: PRODUCER,
				dependencyCertificate: { complete: true, dependencies: [], taints: [] },
				result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } },
			})).toThrow();
		}
	});

	it.each([
		["a", "b", "sha256:f89e6e11beed7ddb8bc4c8a7a0bb8e5fe192215f54c0e85a6d2073a9245d8e1b"],
		["e\u0301", "\u00e9", "sha256:d0cb9f31cc7f472b0fc83c7e892a03a0685dae8cb85bfd8d35ec5f72d5f83f1e"],
	] as const)("owns an exact dependency set independently of capture order (%s, %s)", (left, right, id) => {
		const a = { kind: "absence" as const, path: `/workspace/${left}`, parentEntriesDigest: sha256Digest("entries"), parentExcludedEntries: [".pi", ".git", ".pi"] };
		const b = { ...a, path: `/workspace/${right}` };
		let semantic!: ReturnType<typeof prototype>;
		const seal = (dependencies: DynamicDependency[]) => sealProcessCertificate({
			prototype: semantic, producer: PRODUCER,
			dependencyCertificate: { complete: true, dependencies, taints: [] },
			result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } }, createdAt: 123,
		});
		let first!: ReturnType<typeof seal>, frozenValues: unknown[] = [];
		const freezing = vi.spyOn(Object, "freeze");
		try {
			semantic = prototype({ [right]: "second", [left]: "first", MODE: "build" });
			processWeakKey(semantic);
			first = seal([a, b, a]); frozenValues = freezing.mock.calls.map(([value]) => value);
		} finally { freezing.mockRestore(); }
		expect({
			prototypeCopies: frozenValues.filter((value) => value && typeof value === "object" && "argvDigest" in value).length,
			exclusionCopies: frozenValues.filter((value) => Array.isArray(value) && value[0] === ".git" && value[1] === ".pi").length,
		}).toEqual({ prototypeCopies: 1, exclusionCopies: 3 });
		const second = seal([b, a]);
		expect(first.id).toBe(id); // Golden v7 identities from the pre-refactor implementation.
		expect(dependencyPathsetKey(first.dependencyCertificate)).toBe(dependencyPathsetKey(second.dependencyCertificate));
		expect(first).toEqual(second);
		expect(first.prototype).toBe(semantic);
		expect(first.weakKey).toBe(processWeakKey(semantic));
		expect(first.strongKey).toBe(processStrongKey(first.weakKey, { complete: true, dependencies: [b, a], taints: [] }));
		expect(parseProcessCertificate(JSON.parse(JSON.stringify(first)))).toEqual(first);
		expect(first.dependencyCertificate.dependencies).toEqual([a, b].map((dependency) => ({ ...dependency, parentExcludedEntries: [".git", ".pi"] })));
		expect(a.parentExcludedEntries).toEqual([".pi", ".git", ".pi"]);
		expect(first.dependencyCertificate.dependencies[0]).not.toBe(a);
		expect(Object.isFrozen(first.dependencyCertificate.dependencies[0])).toBe(true);
		expect(() => seal([a, b, { ...a, parentEntriesDigest: sha256Digest("changed") }])).toThrow("conflicting dependency evidence");
		for (const parentExcludedEntries of [".git", [".git", ".."], [".git", undefined]]) {
			const dependencies = [{ ...a, parentExcludedEntries }] as DynamicDependency[];
			const invalid = { complete: true, dependencies, taints: [] };
			expect(() => seal(dependencies)).toThrow("invalid negative dependency");
			expect(() => processStrongKey(first.weakKey, invalid)).toThrow("invalid negative dependency");
			expect(() => dependencyPathsetKey(invalid)).toThrow("invalid negative dependency");
		}
	});

	it("keeps producer authority out of semantic keys but inside certificate identity", () => {
		const semantic = prototype();
		const result = { replayProfile: "buffered_noninteractive" as const, journal: [], exit: { kind: "code" as const, code: 0 } };
		const dependencyCertificate = { complete: true, dependencies: [], taints: [] };
		const speculative = sealProcessCertificate({
			prototype: semantic,
			producer: PRODUCER,
			dependencyCertificate,
			result,
		});
		const actor = sealProcessCertificate({
			prototype: semantic,
			producer: {
				observer: { provider: "test", fingerprint: sha256Digest("observer-v2") },
				execution: { authority: "actor" },
			},
			dependencyCertificate,
			result,
		});

		expect(actor.weakKey).toBe(speculative.weakKey);
		expect(actor.strongKey).toBe(speculative.strongKey);
		expect(actor.id).not.toBe(speculative.id);
		expect(actor.prototype).not.toHaveProperty("policyID");
		expect(actor.prototype).not.toHaveProperty("monitorEpoch");
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
			producer: PRODUCER,
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
		const followedMetadata = await captureMetadataDependency(target, "/workspace/input.txt", true);
		const linkMetadata = { ...followedMetadata, followSymlinks: false } as const;
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
		expect(key(followedMetadata)).not.toBe(key(linkMetadata));
		expect(key(fullDirectory)).not.toBe(key(projectedDirectory));
		expect(key(absentWithParent)).not.toBe(key(absentWithoutParent));
	});

	it("seals typed effects and rejects malformed topology and conflicting artifact sizes", () => {
		const seal = (journal: readonly OrderedEffectEvent[]) => sealProcessCertificate({
			prototype: prototype(),
			producer: PRODUCER,
			dependencyCertificate: { complete: true, dependencies: [], taints: [] },
			result: { replayProfile: "buffered_noninteractive", journal, exit: { kind: "code", code: 0 } },
		});
		const entriesDigest = sha256Digest("empty directory"), digest = sha256Digest("same digest");
		const certificate = seal([
			{
				sequence: 0, kind: "workspace", path: "/workspace/generated", before: { kind: "absent" },
				after: { kind: "directory", entriesDigest, mode: 0o750, uid: 1000, gid: 1000 },
			},
			{
				sequence: 1, kind: "workspace", path: "/workspace/obsolete",
				before: { kind: "directory", entriesDigest, mode: 0o750, uid: 1000, gid: 1000 }, after: { kind: "absent" },
			},
		]);
		expect(certificate.result.journal[0]).toMatchObject({
			kind: "workspace",
			after: { kind: "directory", entriesDigest, mode: 0o750 },
		});

		expect(() => seal([{
			sequence: 0, kind: "workspace", path: "/workspace/generated", before: { kind: "absent" },
			after: { kind: "directory", entriesDigest: "not-a-digest", mode: 0o750, uid: -1, gid: 1000 },
		}] as never)).toThrow("invalid directory effect state");
		expect(() => seal([
			{ sequence: 0, kind: "output", fd: 1, data: { digest, size: 11 } },
			{
				sequence: 1, kind: "workspace", path: "/workspace/out", before: { kind: "absent" },
				after: { kind: "file", data: { digest, size: 12 }, mode: 0o644 },
			},
		])).toThrow("conflicting effect artifact sizes");
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
		processContextDigest: digest("process-context"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux-x64:kernel",
	});
}

async function workspace(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-provenance-"));
	roots.push(root);
	return root;
}
