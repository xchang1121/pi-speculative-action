import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	createExecPrototype,
	sealProcessCertificate,
	sha256Digest,
} from "../src/provenance-certificate.ts";
import { ArtifactCAS, ProvenanceCertificateStore } from "../src/reuse-store.ts";

const roots: string[] = [];

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("persistent provenance store", () => {
	it("deduplicates CAS artifacts and indexes immutable certificates across store instances", async () => {
		const root = await temporaryRoot();
		const store = new ProvenanceCertificateStore(root);
		const first = await store.artifacts.put("output bytes");
		const second = await store.artifacts.put(Buffer.from("output bytes"));
		expect(second).toEqual(first);
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			dependencyCertificate: { complete: true, dependencies: [], taints: [] },
			result: {
				replayProfile: "buffered_noninteractive",
				journal: [{ sequence: 0, kind: "output", fd: 1, data: first }],
				exit: { kind: "code", code: 0 },
			},
			createdAt: 123,
		});
		await store.put(certificate);

		const reopened = new ProvenanceCertificateStore(root);
		expect(await reopened.artifacts.get(first)).toEqual(Buffer.from("output bytes"));
		expect(await reopened.get(certificate.id)).toEqual(certificate);
		expect(await reopened.findByWeakKey(certificate.weakKey)).toEqual([certificate]);
	});

	it("rejects a certificate whose effect bundle is absent from the CAS", async () => {
		const root = await temporaryRoot();
		const store = new ProvenanceCertificateStore(root);
		const missing = { digest: sha256Digest("missing"), size: 7 };
		const certificate = sealProcessCertificate({
			prototype: prototype(),
			dependencyCertificate: { complete: true, dependencies: [], taints: [] },
			result: {
				replayProfile: "buffered_noninteractive",
				journal: [{ sequence: 0, kind: "write", path: "/workspace/out", data: missing, mode: 0o644 }],
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
});

function prototype() {
	const digest = (value: string) => sha256Digest(value);
	return createExecPrototype({
		executablePath: "/bin/tool",
		executableDigest: digest("tool"),
		argv: ["tool"],
		logicalCwd: "/workspace",
		environment: { MODE: "test" },
		umask: 0o22,
		rlimitsDigest: digest("rlimits"),
		signalDispositionsDigest: digest("signals"),
		credentialsDigest: digest("credentials"),
		schedulingDigest: digest("scheduling"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux",
		monitorEpoch: "v1",
		policyID: "closed",
	});
}

async function temporaryRoot(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-reuse-store-"));
	roots.push(root);
	return root;
}
