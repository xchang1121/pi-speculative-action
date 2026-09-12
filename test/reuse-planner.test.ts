import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	createExecPrototype,
	processWeakKey,
	sealProcessCertificate,
	sha256Digest,
	type ProcessProvenanceCertificate,
} from "../src/provenance-certificate.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { captureFileDependency } from "../src/provenance-validation.ts";
import { ProcessReusePlanner } from "../src/reuse-planner.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";

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

describe("ProcessReusePlanner", () => {
	it("reuses the same nested exec across different parent commands after strong validation", async () => {
		const fixture = await fixtureWithCertificate();
		const planner = new ProcessReusePlanner({ store: fixture.store });
		const request = {
			weakKey: fixture.weakKey,
			contract: contract(),
			validation: { resolvePath: () => fixture.input },
		};
		for (const weakKey of [undefined, "invalid", "sha256:ABC", 123]) {
			await expect(planner.plan({ ...request, weakKey: weakKey as never })).rejects.toThrow("invalid process weak key");
		}
		const plan = await planner.plan(request);

		expect(plan).toMatchObject({
			kind: "completed_replay",
			source: "l2",
			certificate: { id: fixture.certificate.id },
		});
		// Parent Bash text is intentionally absent from ExecPrototype/WeakKey.
		expect(JSON.stringify(fixture.prototype)).not.toContain("parent-wrapper");
		expect(await planner.plan({
			...request, contract: { ...request.contract, sink: "pipe" },
		})).toMatchObject({ kind: "miss", reasons: ["observation_contract_incompatible"] });

		await writeFile(fixture.input, "changed");
		expect(await planner.plan(request)).toMatchObject({
			kind: "miss",
			reasons: ["dependency_changed"],
			changedDependencies: ["/workspace/input.txt"],
		});
	});

	it("misses the v6 weak namespace and rewarms v7 without accepting legacy certificate identities", async () => {
		const root = await temporaryRoot(), store = new ProvenanceCertificateStore(root);
		const certificate = sealProcessCertificate({
			prototype: processPrototype(), producer: PRODUCER,
			dependencyCertificate: { complete: true, dependencies: [], taints: [] },
			result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } }, createdAt: 123,
		});
		// Golden v6 hashes from 48bd3b6, with the same prototype, producer, dependencies, result, and timestamp.
		const legacy = { ...certificate, version: 6,
			weakKey: "sha256:063acac52cc249aa186e4f796eb0cb8dc3d1656c13c185912724042764cc325c" as const,
			strongKey: "sha256:80149901d114aae4530bf0dd5e9dedd8d7ca7320f6c5d78ec2829e786c2a4ac7" as const,
			id: "sha256:c525fd6be73f1489f052dafad1414e4ec0802d729e18841b4db21ac165972496" as const,
		};
		const id = legacy.id.slice(7), weak = legacy.weakKey.slice(7);
		const file = path.join(root, "certificates", id.slice(0, 2), `${id.slice(2)}.json`);
		const index = path.join(root, "indexes", "weak", weak.slice(0, 2), weak.slice(2));
		await mkdir(path.dirname(file), { recursive: true }); await mkdir(index, { recursive: true });
		const bytes = JSON.stringify(legacy);
		await writeFile(file, bytes); await writeFile(path.join(index, `${id}.ref`), "");
		const planner = new ProcessReusePlanner({ store }), request = { weakKey: processWeakKey(certificate.prototype), contract: contract() };
		expect(await planner.plan(request)).toMatchObject({ kind: "miss", reasons: ["no_candidate_pathset"], lookup: { candidateCertificates: 0 } });
		expect(await readFile(file, "utf8")).toBe(bytes);
		await expect(store.get(legacy.id)).rejects.toThrow("certificate integrity check failed");
		await store.put(certificate);
		expect(await planner.plan(request)).toMatchObject({ kind: "completed_replay", source: "l2", certificate: { id: certificate.id, version: 7 } });
		await store.stats();
	});

	it("lets the execution authority reject an otherwise matching producer proof", async () => {
		const fixture = await fixtureWithCertificate();
		const plan = await new ProcessReusePlanner({ store: fixture.store }).plan({
			weakKey: fixture.weakKey,
			contract: contract(),
			validation: { resolvePath: () => fixture.input },
			acceptProducer: () => false,
		});

		expect(plan).toMatchObject({ kind: "miss", reasons: ["producer_guarantee_incompatible"] });
	});

	it("reuses confinement observations only when the consumer proves the same domain", async () => {
		const fixture = await fixtureWithCertificate(false, ["confinement_observation"]);
		const planner = new ProcessReusePlanner({ store: fixture.store });
		const request = {
			weakKey: fixture.weakKey,
			contract: contract(),
			validation: { resolvePath: () => fixture.input },
		};
		expect(await planner.plan(request)).toMatchObject({ kind: "miss", reasons: ["certificate_tainted"] });
		expect(await planner.plan({
			...request,
			validation: { ...request.validation, acceptedTaints: ["confinement_observation" as const] },
		})).toMatchObject({ kind: "completed_replay" });
	});

	it("loads each result artifact once into a verified closure before replay", async () => {
		const fixture = await fixtureWithCertificate(true);
		const get = vi.spyOn(fixture.store.artifacts, "get");
		const plan = await new ProcessReusePlanner({ store: fixture.store }).plan({
			weakKey: fixture.weakKey,
			contract: contract(),
			validation: { resolvePath: () => fixture.input },
		});
		expect(plan).toMatchObject({
			kind: "completed_replay",
			lookup: { artifactsLoaded: 2, artifactBytesRead: 14 },
		});
		if (plan.kind !== "completed_replay") throw new Error("expected completed replay");
		const references = plan.certificate.result.journal.flatMap((event) =>
			event.kind === "output"
				? [event.data]
				: [event.before, event.after].flatMap((state) => (state.kind === "file" ? [state.data] : [])),
		);
		expect(get).toHaveBeenCalledTimes(2);
		for (const reference of references) {
			plan.artifacts.read(reference);
			plan.artifacts.read(reference);
		}
		expect(get).toHaveBeenCalledTimes(2);
	});

	it("validates a transferable running result without weakening persistent history", async () => {
		const fixture = await fixtureWithCertificate();
		const find = vi.spyOn(fixture.store, "findByWeakKey");
		const live = sealProcessCertificate({
			prototype: fixture.certificate.prototype,
			producer: fixture.certificate.producer,
			dependencyCertificate: { ...fixture.certificate.dependencyCertificate, taints: ["clock"] },
			result: fixture.certificate.result,
		});
		const planner = new ProcessReusePlanner({ store: fixture.store });
		const unrelatedKey = processWeakKey({ ...fixture.prototype, argvDigest: sha256Digest("unrelated argv") });
		const request = {
			weakKey: fixture.weakKey,
			contract: contract(),
			validation: { resolvePath: () => fixture.input },
		};

		const freezing = vi.spyOn(Object, "freeze");
		try {
			expect(await planner.plan({ ...request, live: { certificate: live, acceptedTaints: [] } })).toMatchObject({
				kind: "miss", reasons: ["certificate_tainted"],
			});
			expect(await planner.plan({ ...request, live: { certificate: live, acceptedTaints: ["clock"] } })).toMatchObject({
				kind: "completed_replay", source: "live", certificate: { id: live.id },
			});
			expect(find).not.toHaveBeenCalled();
			expect(await planner.plan({ ...request, weakKey: unrelatedKey, live: { certificate: live, acceptedTaints: ["clock"] } })).toMatchObject({
				kind: "miss", reasons: ["no_candidate_pathset"], lookup: { candidateCertificates: 0 },
			});
			expect(find).toHaveBeenCalledOnce(); expect(find).toHaveBeenCalledWith(unrelatedKey, undefined);
			await writeFile(fixture.input, "changed");
			expect(await planner.plan({ ...request, live: { certificate: live, acceptedTaints: ["clock"] } })).toMatchObject({
				kind: "miss", reasons: ["dependency_changed"],
			});
			expect(freezing.mock.calls.filter(([value]) => value && typeof value === "object" && "argvDigest" in value)).toHaveLength(0);
		} finally { freezing.mockRestore(); find.mockRestore(); }
	});

	it.each(["l2", "live", "handoff"])("validates batched input states without rereading attempted disk copies (%s)", async (source) => {
		const { input, store, prototype, certificates } = await fixtureWithCertificate(false, [], ["one", "two", "three"]);
		await writeFile(input, "one");

		const request = {
			weakKey: processWeakKey(prototype),
			contract: contract(),
			validation: { resolvePath: () => input },
		};
		const planner = new ProcessReusePlanner({ store }), get = vi.spyOn(store, "get");
		const lookup = async (live?: readonly ProcessProvenanceCertificate[], excludedCertificates?: ReadonlySet<ProcessProvenanceCertificate["id"]>) => {
			const plan = await planner.plan({ ...request, excludedCertificates,
				...(live ? { live: { certificate: live, acceptedTaints: [] } } : {}) });
			return plan.kind === "completed_replay" ? plan : undefined;
		};
		const registry = new ProcessHandoffRegistry(3), scope = { sessionID: "test", turnID: "test" };
		if (source === "handoff") for (const certificate of certificates.slice(0, 2)) {
			const work = await registry.acquire({ key: request.weakKey, scope, role: "producer",
				ownership: new ProcessHandoffOwnership(), lookup: async () => undefined });
			if (work.kind !== "work") throw new Error("expected work");
			await registry.publish(request.weakKey, work.work, certificate, async () => false);
		}
		const acquire = () => registry.acquire({ key: request.weakKey, scope, role: "actor", lookup, waitForRunning: async () => "miss" });
		const acquired = source === "handoff" ? await acquire() : undefined;
		const plan = source === "handoff" ? acquired?.kind === "hit" ? acquired.plan : undefined
			: await lookup(source === "live" ? certificates : undefined);

		expect(plan).toMatchObject({
			kind: "completed_replay",
			source: source === "live" ? "live" : "l2", certificate: { id: certificates[2]!.id },
			lookup: {
				candidateCertificates: source === "handoff" ? 1 : 3,
				eligibleCertificates: source === "handoff" ? 1 : 3,
				pathsetsValidated: 1,
				filesRead: 1,
				bytesRead: 3,
			},
		});
		expect(get).toHaveBeenCalledTimes(source === "live" ? 0 : source === "handoff" ? 1 : 3);
		if (source === "handoff") {
			await writeFile(input, "changed");
			expect(await acquire()).toMatchObject({ kind: "miss" });
			await writeFile(input, "three");
			expect(await acquire()).toMatchObject({ kind: "hit", plan: { source: "live", certificate: { id: certificates[0]!.id } } });
		}
	});
});

async function fixtureWithCertificate(
	withFileEffect = false,
	taints: readonly ("confinement_observation")[] = [],
	versions = ["input"],
) {
	const root = await temporaryRoot();
	const input = path.join(root, "input.txt");
	const store = new ProvenanceCertificateStore(path.join(root, "cache"));
	const artifact = await store.artifacts.put("artifact");
	const prototype = processPrototype();
	const certificates = [];
	for (const [index, value] of versions.entries()) {
		await writeFile(input, value);
		const dependency = await captureFileDependency(input, "/workspace/input.txt");
		const output = await store.artifacts.put(versions.length > 1 ? `result:${value}` : "stdout");
		const certificate = sealProcessCertificate({
			prototype, producer: PRODUCER, createdAt: index + 1,
			dependencyCertificate: { complete: true, dependencies: [dependency.dependency], taints },
			result: {
				replayProfile: "buffered_noninteractive",
				journal: [
					{ sequence: 0, kind: "output", fd: 1, data: output },
					...(withFileEffect ? [{ sequence: 1, kind: "workspace" as const, path: "/workspace/out.bin",
						before: { kind: "absent" as const }, after: { kind: "file" as const, data: artifact, mode: 0o644 } }] : []),
				],
				exit: { kind: "code", code: 0 },
			},
		});
		certificates.unshift(certificate);
		await store.put(certificate);
	}
	return { certificate: certificates[0]!, certificates, input, prototype, weakKey: processWeakKey(prototype), root, store };
}

function processPrototype() {
	const digest = (value: string) => sha256Digest(value);
	return createExecPrototype({
		executablePath: "/usr/bin/compiler",
		executableDigest: digest("compiler"),
		argv: ["compiler", "input.txt"],
		logicalCwd: "/workspace",
		environment: { LANG: "C", PATH: "/usr/bin" },
		umask: 0o22,
		processContextDigest: digest("process-context"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux-x64",
	});
}

function contract() {
	return {
		sink: "buffered" as const,
		orderedJournal: true,
		transactionalEffects: true,
	};
}

async function temporaryRoot(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-reuse-planner-"));
	roots.push(root);
	return root;
}
