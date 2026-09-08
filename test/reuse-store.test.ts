import { mkdtemp, rm, unlink, utimes } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	createExecPrototype,
	digestObject,
	parseProcessCertificate,
	sealProcessCertificate,
	sha256Digest,
} from "../src/provenance-certificate.ts";
import { ArtifactCAS, ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";

vi.mock("node:fs/promises", { spy: true });

const roots: string[] = [];
const PRODUCER = {
	observer: { provider: "test", fingerprint: sha256Digest("observer-v1") },
	execution: { authority: "actor" as const },
};

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("persistent provenance store", () => {
	it.each(["held", "scan_failure", "delete_failure"] as const)("drains admitted publication and every maintenance sibling (%s)", async (phase) => {
		const root = await temporaryRoot();
		const initial = new ProvenanceCertificateStore(root);
		const first = await initial.artifacts.put("output bytes");
		const duplicateArtifact = await initial.artifacts.put(Buffer.from("output bytes"));
		expect(duplicateArtifact).toEqual(first);
		const certificate = completed(first, 123);
		const duplicate = completed(first, 456, "test", 999);
		expect(duplicate.id).toBe(certificate.id);
		const { id: _id, ...legacyBody } = certificate;
		const legacy = { ...legacyBody, id: digestObject(legacyBody) };
		expect(parseProcessCertificate(legacy)).toBeUndefined();
		for (const version of [2, 6]) expect(parseProcessCertificate({ ...certificate, version })).toBeUndefined();
		expect(await initial.put(certificate)).toBe(true);
		expect(await initial.put(duplicate)).toBe(false);

		const reopened = new ProvenanceCertificateStore(root);
		expect(await reopened.artifacts.get(first)).toEqual(Buffer.from("output bytes"));
		expect(await reopened.get(certificate.id)).toEqual(certificate);
		expect(await reopened.findByWeakKey(certificate.weakKey)).toEqual([certificate]);
		const cachedStats = await reopened.stats();
		expect(cachedStats).toMatchObject({ certificates: 1, artifacts: 1, orphanArtifacts: 0 });
		expect(await reopened.stats()).toBe(cachedStats);
		expect(await reopened.stats(true)).not.toBe(cachedStats);

		const store = new ProvenanceCertificateStore(root, {
			maxCertificates: 1,
			maxBytes: 1024 * 1024,
			gcIntervalMs: 0,
			orphanGraceMs: 0,
		});
		const secondArtifact = await store.artifacts.put("second");
		const orphan = await store.artifacts.put("orphan");
		const artifactPaths = [first, orphan].map((reference) => {
			const hex = reference.digest.slice("sha256:".length);
			return path.join(root, "cas", "sha256", hex.slice(0, 2), hex.slice(2));
		});
		// Explicit ages avoid a zero-grace race between fractional filesystem mtime and integer Date.now().
		await utimes(artifactPaths[0]!, 1, 1);
		const future = new Date(Date.now() + 60_000);
		await utimes(artifactPaths[1]!, future, future);
		const second = completed(secondArtifact, 789, "second");
		const { readdir, rm: remove } = await vi.importActual<typeof filesystem>("node:fs/promises");
		let enter!: () => void, resume!: () => void, rejectEntered!: () => void;
		let armed = true, suspended: Promise<unknown> | undefined;
		const failure = new Error("maintenance IO failure");
		const entered = new Promise<void>((resolve) => { enter = resolve; }), gate = new Promise<void>((resolve) => { resume = resolve; });
		const failureStarted = new Promise<void>((resolve) => { rejectEntered = resolve; });
		const fail = async () => { await entered; rejectEntered(); throw failure; };
		const hold = <Value,>(operation: () => Promise<Value>): Promise<Value> => {
			armed = false; enter(); const task = gate.then(operation); suspended = task; return task;
		};
		const certificateRoot = path.join(root, "certificates"), certificateHex = certificate.id.slice("sha256:".length);
		const enumeration = vi.spyOn(filesystem, "readdir").mockImplementation((...args) => {
			const target = String(args[0]);
			if (armed && phase === "scan_failure" && target === certificateRoot) return readdir(...args).then(fail);
			const blocked = phase === "held" ? certificateRoot : phase === "scan_failure" ? path.join(root, "cas", "sha256") : undefined;
			return armed && target === blocked ? hold(() => readdir(...args)) : readdir(...args);
		});
		const removal = vi.spyOn(filesystem, "rm").mockImplementation((...args) => {
			const target = String(args[0]);
			if (armed && phase === "delete_failure" && target === path.join(certificateRoot, certificateHex.slice(0, 2), `${certificateHex.slice(2)}.json`)) return fail();
			return armed && phase === "delete_failure" && target === artifactPaths[0] ? hold(() => remove(...args)) : remove(...args);
		});
		const collect = vi.spyOn(store, "gc"), publish = vi.fn(() => store.put(second)), closed = vi.fn();
		const gateway = new ToolExecutionGateway<never, boolean>([]);
		const publication = gateway.executeAuthoritative({ tool: "certificate", input: {} }, publish);
		let retirement: Promise<void> | undefined, collection: ReturnType<typeof store.gc> | undefined;
		try {
			await entered; collection = collect.mock.results[0]!.value;
			retirement = Promise.all([gateway.dispose(), gateway.dispose()]).then(() => { closed(); });
			if (phase !== "held") await failureStarted;
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(await store.get(second.id)).toEqual(second);
			expect(publish).toHaveBeenCalledOnce(); expect(collect).toHaveBeenCalledOnce();
			expect(closed).not.toHaveBeenCalled();
		} finally {
			resume(); await Promise.allSettled([publication, retirement ?? gateway.dispose(), suspended]);
			await store.stats().finally(() => { enumeration.mockRestore(); removal.mockRestore(); collect.mockRestore(); });
		}
		expect(await publication).toBe(true); expect(closed).toHaveBeenCalledOnce(); expect(publish).toHaveBeenCalledOnce();

		if (phase !== "held") await expect(collection).rejects.toBe(failure);
		const collected = phase !== "held" ? await store.gc() : await collection;
		expect(collected).toMatchObject({
			removedCertificates: 1,
			removedArtifacts: phase === "delete_failure" ? 0 : 1,
		});
		expect(await store.stats()).toMatchObject({ certificates: 1, artifacts: 2, orphanArtifacts: 1 });
		await utimes(artifactPaths[1]!, 1, 1);
		expect(await store.gc()).toMatchObject({ removedCertificates: 0, removedArtifacts: 1 });
		expect(await store.stats()).toMatchObject({ certificates: 1, artifacts: 1, orphanArtifacts: 0, overBudget: false });
		expect(await store.get(certificate.id)).toBeUndefined();
		expect(await store.get(second.id)).toEqual(second);
		const closure = await store.artifacts.load([secondArtifact]);
		store.configure({ maxCertificates: 2, maxBytes: 2 * 1024 * 1024 });
		expect(store.limits).toEqual({ maxCertificates: 2, maxBytes: 2 * 1024 * 1024 });
		await store.clear();
		expect(await store.stats()).toMatchObject({ certificates: 0, artifacts: 0, totalBytes: 0 });
		expect(closure?.read(secondArtifact).toString("utf8")).toBe("second");
	});

	it("rejects a certificate whose effect bundle is absent from the CAS", async () => {
		const root = await temporaryRoot();
		const store = new ProvenanceCertificateStore(root);
		const missing = { digest: sha256Digest("missing"), size: 7 };
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			producer: PRODUCER,
			dependencyCertificate: { complete: true, dependencies: [], taints: [] },
			result: {
				replayProfile: "buffered_noninteractive",
				journal: [{
					sequence: 0,
					kind: "workspace",
					path: "/workspace/out",
					before: { kind: "absent" },
					after: { kind: "file", data: missing, mode: 0o644 },
				}],
				exit: { kind: "code", code: 0 },
			},
		});

		await expect(store.put(certificate)).rejects.toThrow("missing artifact");
	});

	it("independently leases a verified CAS closure and survives backing-file removal", async () => {
		const root = await temporaryRoot();
		const cas = new ArtifactCAS(root);
		const reference = await cas.put("leased bytes");
		expect(await cas.has(reference)).toBe(true);
		expect((await cas.get(reference))?.toString("utf8")).toBe("leased bytes");
		const closure = await cas.load([reference, reference]);
		if (!closure) throw new Error("expected verified closure");
		expect(closure).toMatchObject({ artifacts: 1, bytes: reference.size });

		expect(closure.read(reference).toString("utf8")).toBe("leased bytes");
		const hex = reference.digest.slice("sha256:".length);
		await unlink(path.join(root, "sha256", hex.slice(0, 2), hex.slice(2)));
		expect(closure.read(reference).toString("utf8")).toBe("leased bytes");
		expect(await cas.load([reference])).toBeUndefined();
	});
});

function completed(
	reference: { readonly digest: `sha256:${string}`; readonly size: number },
	createdAt: number,
	mode = "test",
	observedProcessMs?: number,
) {
	return sealProcessCertificate({
		prototype: prototype(mode),
		producer: PRODUCER,
		dependencyCertificate: { complete: true, dependencies: [], taints: [] },
		result: {
			replayProfile: "buffered_noninteractive",
			...(observedProcessMs !== undefined ? { observedProcessMs } : {}),
			journal: [{ sequence: 0, kind: "output", fd: 1, data: reference }],
			exit: { kind: "code", code: 0 },
		},
		createdAt,
	});
}

function prototype(mode = "test") {
	const digest = (value: string) => sha256Digest(value);
	return createExecPrototype({
		executablePath: "/bin/tool",
		executableDigest: digest("tool"),
		argv: ["tool"],
		logicalCwd: "/workspace",
		environment: { MODE: mode },
		umask: 0o22,
		processContextDigest: digest("process-context"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux",
	});
}

async function temporaryRoot(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-reuse-store-"));
	roots.push(root);
	return root;
}
