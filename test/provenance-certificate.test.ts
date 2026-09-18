import { mkdir, symlink, writeFile } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import { processPrototype, processCertificate } from "./process-fixture.ts";
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
	referencedArtifacts,
	type ProvenanceTaint,
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

const { create: workspace, dispose } = temporaryDirectories("pi-provenance-");

afterEach(dispose);

describe("process provenance certificates", () => {
	it("owns validation inputs and detects filesystem changes", async () => {
		const root = await workspace();
		await mkdir(path.join(root, "lib"));
		for (const [name, content] of [
			["input.txt", "one"], ["metadata.txt", "metadata"], ["tool", "executable"], ["lib/runtime.so", "library"],
		] as const) await writeFile(path.join(root, name), content);
		const location = (name: string) => [path.join(root, name), `/workspace/${name}`] as const;
		let link: Awaited<ReturnType<typeof captureSymlinkDependency>> | undefined;
		try {
			await symlink("input.txt", path.join(root, "input.link"));
			link = await captureSymlinkDependency(...location("input.link"));
		} catch (error) {
			if (!(error && typeof error === "object" && "code" in error && error.code === "EPERM")) throw error;
			// Windows without Developer Mode cannot create symlinks; Linux integration covers this path.
		}
		const files = await Promise.all(([
			["input.txt", "input"], ["tool", "executable"], ["lib/runtime.so", "shared_object"],
		] as const).map(([name, role]) => captureFileDependency(...location(name), role)));
		const directory = await captureDirectoryDependency(...location("lib"));
		const metadata = await captureMetadataDependency(...location("metadata.txt"), true);
		const absent = await captureAbsenceDependency(...location("missing.txt"));
		if (!absent) throw new Error("expected negative lookup evidence");
		const certificate = processCertificate(prototype(), {
			dependencyCertificate: {
				complete: true,
				dependencies: [
					...files.map(({ dependency }) => dependency),
					metadata,
					directory,
					absent,
					...(link ? [link] : []),
				],
				taints: [],
			},
			result: { replayProfile: "buffered_noninteractive", observedProcessMs: 1250.5, journal: [], exit: { kind: "code", code: 0 } },
		});
		const validate = (value = certificate) => validateProcessCertificate(value, {
			resolvePath: (logical) => path.join(root, path.posix.relative("/workspace", logical)),
		});
		const mutable = structuredClone(certificate), validating = validate(mutable);
		Object.assign(mutable, { weakKey: certificate.id, strongKey: certificate.id });
		const validation = await validating;
		expect(validation).toMatchObject({ status: "valid", filesRead: 3 });
		expect(certificate.strongKey).toBe(
			validation.status === "valid" ? validation.strongKey : undefined,
		);
		expect(certificate.result.observedProcessMs).toBe(1250.5);

		await writeFile(path.join(root, "input.txt"), "changed");
		const changed = structuredClone(certificate), stale = validate(changed);
		Object.assign(changed.dependencyCertificate.dependencies.find(d => d.kind === "file")!, { contentDigest: sha256Digest("changed") });
		Object.assign(changed.dependencyCertificate.dependencies, { length: 0 });
		expect(await stale).toMatchObject({ status: "stale", changed: ["/workspace/input.txt"] });
		await writeFile(path.join(root, "lib", "new.txt"), "new");
		await writeFile(path.join(root, "missing.txt"), "appeared");
		expect(await validate()).toMatchObject({
			status: "stale",
			changed: expect.arrayContaining(["/workspace/input.txt", "/workspace/lib", "/workspace/missing.txt"]),
		});
	});

	it.each([
		["clock", true], ["random", true], ["pid_observation", true], ["network", false], ["ipc", false],
	] satisfies ReadonlyArray<readonly [ProvenanceTaint, boolean]>)("distinguishes reuse and one-shot transfer of %s evidence", async (taint, transferable) => {
		const evidence = { complete: true, dependencies: [], taints: [taint] };
		expect(await validateProcessCertificate(processCertificate(prototype(), { dependencyCertificate: evidence })))
			.toMatchObject({ status: "indeterminate", reason: `tainted:${taint}` });
		expect(await validateTransferredProcessEvidence(evidence)).toMatchObject(transferable
			? { status: "valid" }
			: { status: "indeterminate", cause: { detail: `tainted:${taint}` } });
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
			expect(() => processCertificate(malformed)).toThrow();
		}
	});

	it.each([
		["a", "b", "sha256:f89e6e11beed7ddb8bc4c8a7a0bb8e5fe192215f54c0e85a6d2073a9245d8e1b"],
		["e\u0301", "\u00e9", "sha256:d0cb9f31cc7f472b0fc83c7e892a03a0685dae8cb85bfd8d35ec5f72d5f83f1e"],
	] as const)("owns an exact dependency set independently of capture order (%s, %s)", (left, right, id) => {
		const a = { kind: "absence" as const, path: `/workspace/${left}`, parentEntriesDigest: sha256Digest("entries"), parentExcludedEntries: [".pi", ".git", ".pi"] };
		const b = { ...a, path: `/workspace/${right}` };
		let semantic!: ReturnType<typeof prototype>;
		const seal = (dependencies: DynamicDependency[]) => processCertificate(semantic, {
			dependencyCertificate: { complete: true, dependencies, taints: [] },
			createdAt: 123,
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
		expect(first.id).toBe(id); // Recorded certificate identities, including exact Unicode spelling.
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
		const speculative = processCertificate(semantic);
		const actor = processCertificate(semantic, {
			producer: {
				observer: { provider: "test", fingerprint: sha256Digest("observer-other") },
				execution: { authority: "actor" },
			},
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
		const certificate = processCertificate(prototype(), {
			dependencyCertificate: { complete: true, dependencies: [directory, absent], taints: [] },
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
		const seal = (journal: readonly OrderedEffectEvent[]) => processCertificate(prototype(), {
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
		const state = { kind: "file" as const, data: { digest, size: 11 }, mode: 0o600 };
		expect(seal([{ sequence: 0, kind: "workspace", path: "/workspace/file", before: state, after: state, operation: "write_contents" }])
			.result.journal).toHaveLength(1);
		expect(() => seal([{ sequence: 0, kind: "workspace", path: "/workspace/file", before: state,
			after: { ...state, mode: 0o400 }, operation: "write_contents" }])).toThrow("in-place");
	});

	it.each(["regular", "null"] as const)("seals complete OFD transitions and their artifact closure (%s)", type => {
		const before = type === "null" ? 0 : 1;
		const input = processPrototype({ inheritedFDs: [3, 4, 8].map(fd => ({ fd, type, alias: fd === 4 ? 3 : fd,
			offset: before, contentDigest: sha256Digest(type === "null" ? "" : "before"), flagsDigest: sha256Digest(`flags:${fd}`) })) });
		const content = { digest: sha256Digest("after"), size: 5 };
		const positions = [3, 4, 8].map(fd => ({ fd, before, afterFlags: fd === 8 ? 32768 : 35840, after: type === "null" ? 0 : fd === 8 ? 2 : 4, ...(fd === 3 && type === "regular" ? { content } : {}) }));
		const seal = (descriptorOffsets: typeof positions | undefined) => processCertificate(input, { result: {
			replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 }, descriptorOffsets,
		} });
		const certificate = seal(positions);
		expect(parseProcessCertificate(certificate)).toEqual(certificate);
		expect(referencedArtifacts(certificate)).toEqual(type === "null" ? [] : [content]);
		for (const malformed of [undefined, positions.slice(1), [...positions, positions[0]!],
			positions.map(position => ({ ...position, before: before + 1 })), positions.map(position => ({ ...position, after: -1 })),
			positions.map(position => position.fd === 4 ? { ...position, after: 5 } : position),
			positions.map(position => position.fd === 4 ? { ...position, afterFlags: 32768 } : position),
			...[NaN, -1, 0x80000000, 1.5].map(afterFlags => positions.map(position => ({ ...position, afterFlags })))]) expect(() => seal(malformed)).toThrow(/OFD/);
		if (type === "null") expect(() => seal(positions.map(position => ({ ...position, content })))).toThrow(/OFD/);
		positions[0]!.after = 99;
		expect(certificate.result.descriptorOffsets![0]!.after).toBe(type === "null" ? 0 : 4);
		expect(() => processWeakKey({ ...input, inheritedFDs: input.inheritedFDs.map(fd => ({ ...fd, alias: 9 })) })).toThrow(/alias/);
	});
});

function prototype(environment: Readonly<Record<string, string | undefined>> = { MODE: "build" }) {
	return processPrototype({
		executablePath: "/workspace/tool",
		executableDigest: sha256Digest("executable"),
		argv: ["tool", "--compile"],
		environment,
		platformFingerprint: "linux-x64:kernel",
	});
}
