import { mkdtemp, rm, unlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	createExecPrototype,
	digestObject,
	parseProcessCertificate,
	sealProcessCertificate,
	sha256Digest,
} from "../src/provenance-certificate.ts";
import { ArtifactCAS, ProvenanceCertificateStore } from "../src/reuse-store.ts";

const roots: string[] = [];
const PRODUCER = {
	observer: { provider: "test", fingerprint: sha256Digest("observer-v1") },
	execution: { authority: "actor" as const },
};

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("persistent provenance store", () => {
	it("deduplicates CAS artifacts and indexes immutable certificates across store instances", async () => {
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
		expect(parseProcessCertificate(legacy)?.id).toBe(legacy.id);
		expect(parseProcessCertificate({ ...certificate, version: 2 })).toBeUndefined();
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
			orphanGraceMs: 0,
		});
		const secondArtifact = await store.artifacts.put("second");
		await store.artifacts.put("orphan");
		const second = completed(secondArtifact, 789, "second");
		await store.put(second);

		const collected = await store.gc();
		expect(collected).toMatchObject({
			removedCertificates: 1,
			removedArtifacts: 2,
		});
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

	it("keeps the artifact CAS usable independently from certificate indexing", async () => {
		const root = await temporaryRoot();
		const cas = new ArtifactCAS(root);
		const reference = await cas.put("standalone");

		expect(await cas.has(reference)).toBe(true);
		expect((await cas.get(reference))?.toString("utf8")).toBe("standalone");
	});

	it("leases a verified artifact closure before replay and survives backing-file removal", async () => {
		const root = await temporaryRoot();
		const cas = new ArtifactCAS(root);
		const reference = await cas.put("leased bytes");
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
		rlimitsDigest: digest("rlimits"),
		signalDispositionsDigest: digest("signals"),
		credentialsDigest: digest("credentials"),
		schedulingDigest: digest("scheduling"),
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
