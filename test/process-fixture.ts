import { createExecPrototype, sealProcessCertificate, sha256Digest, type ExecPrototype, type ProcessPrototypeInput, type ProcessProducerProof } from "../src/provenance-certificate.ts";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

export const SPECULATIVE_PRODUCER: ProcessProducerProof = {
	observer: { provider: "test", fingerprint: "sha256:4f3ffc55e1940cebb5c92e16fe7d6e6091f561562fb8b0e044910473356abc2c" },
	execution: { authority: "speculative", confinement: { provider: "test", fingerprint: "sha256:9f1019cefff9d81735701d03105338cf8ef2f7ff0b21b2abedce983d883a8786" } },
};

export function processPrototype(overrides: Partial<ProcessPrototypeInput> = {}) {
	return createExecPrototype({
		executablePath: "/bin/tool",
		executableDigest: sha256Digest("tool"),
		argv: ["tool"],
		logicalCwd: "/workspace",
		environment: { MODE: "test" },
		umask: 0o22,
		processContextDigest: sha256Digest("process-context"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux",
		...overrides,
	});
}

export function processCertificate(
	prototype: ExecPrototype,
	overrides: Partial<Omit<Parameters<typeof sealProcessCertificate>[0], "prototype">> = {},
) {
	return sealProcessCertificate({
		prototype,
		producer: SPECULATIVE_PRODUCER,
		dependencyCertificate: { complete: true, dependencies: [], taints: [] },
		result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } },
		...overrides,
	});
}

export function storeMetadata<T>(root: string, run: (database: DatabaseSync) => T): T {
	const database = new DatabaseSync(path.join(root, "metadata.sqlite"));
	try { return run(database); } finally { database.close(); }
}

export async function legacyCertificate(root: string, certificate: ReturnType<typeof processCertificate>): Promise<void> {
	const id = certificate.id.slice(7), targets = [path.join(root, "certificates", id.slice(0, 2), id.slice(2) + ".json"),
		path.join(root, "indexes", "executable", sha256Digest(certificate.prototype.executablePath).slice(7), certificate.weakKey.slice(7), id + ".ref")];
	for (const [index, target] of targets.entries()) { await mkdir(path.dirname(target), { recursive: true }); await writeFile(target, index ? "" : JSON.stringify(certificate)); }
}

/** A separate owner exercises the on-disk publication protocol, including native lock recovery on process exit. */
export function storePeer(root: string, certificate?: ReturnType<typeof processCertificate>, values: readonly string[] = []) {
	const child = spawn(process.execPath, ["--input-type=module", "--eval", `
		import { DatabaseSync } from 'node:sqlite';
		const { ProvenanceCertificateStore } = await import(${JSON.stringify(new URL("../src/reuse-store.ts", import.meta.url).href)});
		const { root, certificate, values } = await new Promise(resolve => process.once('message', resolve));
		const execute = DatabaseSync.prototype.exec;
		DatabaseSync.prototype.exec = function (sql) { try { return execute.call(this, sql); } catch (error) {
			if ((error.errcode & 0xff) === 5) { process.send('busy'); DatabaseSync.prototype.exec = execute; } throw error; } };
		const store = new ProvenanceCertificateStore(root);
		if (!certificate) await store.exclusive(async () => { process.send('held'); await new Promise(() => {}); });
		for (const value of values) await store.artifacts.put(value);
		await store.put(certificate); await store.drain(); process.send('published'); process.disconnect();
	`], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
	let errors = ""; child.stderr!.on("data", data => { errors += data; });
	const noticed = once(child, "message").then(([message]) => message as string);
	const completed = once(child, "close").then(([code]) => { if (code !== 0) throw new Error(`store peer exited ${code}: ${errors}`); });
	void completed.catch(() => undefined); child.send({ root, certificate, values });
	return { child, noticed, completed };
}
