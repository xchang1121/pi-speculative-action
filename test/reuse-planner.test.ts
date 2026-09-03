import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	createExecPrototype,
	sealProcessCertificate,
	sha256Digest,
} from "../src/provenance-certificate.ts";
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
		const plan = await planner.plan({
			prototype: fixture.prototype,
			contract: contract("completed_replay"),
			validation: { resolvePath: () => fixture.input },
		});

		expect(plan).toMatchObject({
			kind: "completed_replay",
			source: "l2",
			certificate: { id: fixture.certificate.id },
		});
		// Parent Bash text is intentionally absent from ExecPrototype/WeakKey.
		expect(JSON.stringify(fixture.prototype)).not.toContain("parent-wrapper");

		await writeFile(fixture.input, "changed");
		expect(
			await planner.plan({
				prototype: fixture.prototype,
				contract: contract("completed_replay"),
				validation: { resolvePath: () => fixture.input },
			}),
		).toMatchObject({
			kind: "miss",
			reasons: ["dependency_changed"],
			changedDependencies: ["/workspace/input.txt"],
		});
	});

	it("requires a buffered transactional observation contract and can salvage file artifacts", async () => {
		const fixture = await fixtureWithCertificate(true);
		const planner = new ProcessReusePlanner({ store: fixture.store });
		const validation = { resolvePath: () => fixture.input };

		expect(
			await planner.plan({
				prototype: fixture.prototype,
				contract: { ...contract("completed_replay"), sink: "pipe" },
				validation,
			}),
		).toMatchObject({ kind: "miss", reasons: ["observation_contract_incompatible"] });
		const salvage = await planner.plan({
			prototype: fixture.prototype,
			contract: contract("artifact_seed"),
			validation,
		});
		expect(salvage).toMatchObject({ kind: "artifact_seed", effects: [{ kind: "workspace" }] });
	});

	it("lets the execution authority reject an otherwise matching producer proof", async () => {
		const fixture = await fixtureWithCertificate();
		const plan = await new ProcessReusePlanner({ store: fixture.store }).plan({
			prototype: fixture.prototype,
			contract: contract("completed_replay"),
			validation: { resolvePath: () => fixture.input },
			acceptProducer: () => false,
		});

		expect(plan).toMatchObject({ kind: "miss", reasons: ["producer_guarantee_incompatible"] });
	});

	it("loads each result artifact once into a verified closure before replay", async () => {
		const fixture = await fixtureWithCertificate(true);
		const get = vi.spyOn(fixture.store.artifacts, "get");
		const plan = await new ProcessReusePlanner({ store: fixture.store }).plan({
			prototype: fixture.prototype,
			contract: contract("completed_replay"),
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
		const request = {
			prototype: fixture.prototype,
			contract: contract("completed_replay"),
			validation: { resolvePath: () => fixture.input },
		};

		expect(await planner.plan({ ...request, live: { certificate: live, acceptedTaints: [] } })).toMatchObject({
			kind: "miss", reasons: ["certificate_tainted"],
		});
		expect(await planner.plan({ ...request, live: { certificate: live, acceptedTaints: ["clock"] } })).toMatchObject({
			kind: "completed_replay", source: "live", certificate: { id: live.id },
		});
		expect(find).not.toHaveBeenCalled();
		await writeFile(fixture.input, "changed");
		expect(await planner.plan({ ...request, live: { certificate: live, acceptedTaints: ["clock"] } })).toMatchObject({
			kind: "miss", reasons: ["dependency_changed"],
		});
	});

	it("captures one dynamic pathset once when matching several historical input states", async () => {
		const root = await temporaryRoot();
		const input = path.join(root, "input.txt");
		const store = new ProvenanceCertificateStore(path.join(root, "cache"));
		const prototype = processPrototype();
		let oldestID: string | undefined;
		for (const [index, value] of ["one", "two", "three"].entries()) {
			await writeFile(input, value);
			const dependency = await captureFileDependency(input, "/workspace/input.txt");
			const output = await store.artifacts.put(`result:${value}`);
			const certificate = sealProcessCertificate({
				prototype,
				producer: PRODUCER,
				dependencyCertificate: { complete: true, dependencies: [dependency.dependency], taints: [] },
				result: {
					replayProfile: "buffered_noninteractive",
					journal: [{ sequence: 0, kind: "output", fd: 1, data: output }],
					exit: { kind: "code", code: 0 },
				},
				createdAt: index + 1,
			});
			oldestID ??= certificate.id;
			await store.put(certificate);
		}
		await writeFile(input, "one");

		const plan = await new ProcessReusePlanner({ store }).plan({
			prototype,
			contract: contract("completed_replay"),
			validation: { resolvePath: () => input },
		});

		expect(plan).toMatchObject({
			kind: "completed_replay",
			certificate: { id: oldestID },
			lookup: {
				candidateCertificates: 3,
				eligibleCertificates: 3,
				pathsetsValidated: 1,
				filesRead: 1,
				bytesRead: 3,
			},
		});
	});
});

async function fixtureWithCertificate(withFileEffect = false) {
	const root = await temporaryRoot();
	const input = path.join(root, "input.txt");
	await writeFile(input, "input");
	const dependency = await captureFileDependency(input, "/workspace/input.txt");
	const store = new ProvenanceCertificateStore(path.join(root, "cache"));
	const output = await store.artifacts.put("stdout");
	const artifact = await store.artifacts.put("artifact");
	const prototype = processPrototype();
	const certificate = sealProcessCertificate({
		prototype,
		producer: PRODUCER,
		dependencyCertificate: { complete: true, dependencies: [dependency.dependency], taints: [] },
		result: {
			replayProfile: "buffered_noninteractive",
			journal: [
				{ sequence: 0, kind: "output", fd: 1, data: output },
				...(withFileEffect
					? [{
							sequence: 1,
							kind: "workspace" as const,
							path: "/workspace/out.bin",
							before: { kind: "absent" as const },
							after: { kind: "file" as const, data: artifact, mode: 0o644 },
						}]
					: []),
			],
			exit: { kind: "code", code: 0 },
		},
	});
	await store.put(certificate);
	return { certificate, input, prototype, root, store };
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

function contract(mode: "completed_replay" | "artifact_seed") {
	return {
		mode,
		sink: "buffered" as const,
		orderedJournal: true,
		transactionalEffects: true,
		...(mode === "artifact_seed"
			? { seed: { acceptedPaths: ["/workspace/out.bin"], preconditionsValidated: true as const } }
			: {}),
	};
}

async function temporaryRoot(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-reuse-planner-"));
	roots.push(root);
	return root;
}
