import { createExecPrototype, sealProcessCertificate, sha256Digest, type ProcessProvenanceCertificate } from "../src/provenance-certificate.ts";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

/** One immutable process identity; scenarios vary its observed inputs, effects and ownership. */
export const PROCESS_CERTIFICATE = sealProcessCertificate({
	prototype: createExecPrototype({
		executablePath: "/bin/tool", executableDigest: sha256Digest("tool"), argv: ["tool"], logicalCwd: "/workspace",
		environment: { MODE: "test" }, umask: 0o22, processContextDigest: sha256Digest("process-context"),
		stdin: { type: "closed", eof: true }, fileDescriptorTableComplete: true, inheritedFDs: [], platformFingerprint: "linux",
	}),
	producer: { observer: { provider: "test", fingerprint: sha256Digest("observer") },
		execution: { authority: "speculative", confinement: { provider: "test", fingerprint: sha256Digest("confinement") } } },
	dependencyCertificate: { complete: true, dependencies: [], taints: [] },
	result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } },
});

export function storeMetadata<T>(root: string, run: (database: DatabaseSync) => T): T {
	const database = new DatabaseSync(path.join(root, "metadata.sqlite"));
	try { return run(database); } finally { database.close(); }
}

export async function legacyCertificate(root: string, certificate: ProcessProvenanceCertificate): Promise<void> {
	const id = certificate.id.slice(7), targets = [path.join(root, "certificates", id.slice(0, 2), id.slice(2) + ".json"),
		path.join(root, "indexes", "executable", sha256Digest(certificate.prototype.executablePath).slice(7), certificate.weakKey.slice(7), id + ".ref")];
	for (const [index, target] of targets.entries()) { await mkdir(path.dirname(target), { recursive: true }); await writeFile(target, index ? "" : JSON.stringify(certificate)); }
}

/** A separate owner exercises the on-disk publication protocol, including native lock recovery on process exit. */
export function storePeer(root: string, certificate?: ProcessProvenanceCertificate, values: readonly string[] = [], crash?: "publish" | "clear" | "legacy-clear" | "collect") {
	const child = spawn(process.execPath, ["--input-type=module", "--eval", `
		import { DatabaseSync } from 'node:sqlite';
		import filesystem from 'node:fs/promises';
		import { syncBuiltinESMExports } from 'node:module';
		import { randomUUID } from 'node:crypto';
		const { ProvenanceCertificateStore } = await import(${JSON.stringify(new URL("../src/reuse-store.ts", import.meta.url).href)});
		const { root, certificate, values, crash } = await new Promise(resolve => process.once('message', resolve));
		const alive = setInterval(() => {}, 1000);
		const execute = DatabaseSync.prototype.exec;
		DatabaseSync.prototype.exec = function (sql) { try { return execute.call(this, sql); } catch (error) {
			if ((error.errcode & 0xff) === 5) { process.send('busy'); DatabaseSync.prototype.exec = execute; } throw error; } };
		const store = new ProvenanceCertificateStore(root);
		if (crash && crash !== 'collect') {
			const operation = crash === 'publish' ? 'rename' : 'rm', original = filesystem[operation];
			filesystem[operation] = async (...args) => {
				if (!String(args[0]).startsWith(root)) return original(...args);
				if (crash === 'clear') await original(...args);
				process.send('crash'); await new Promise(() => {});
			}; syncBuiltinESMExports();
			if (crash === 'publish') await store.artifacts.put('interrupted publication');
			else if (crash === 'clear') await store.clear();
			else { const tomb = root + '/.clear-' + randomUUID(); await filesystem.rename(store.artifacts.root, tomb); await filesystem.rm(tomb, { recursive: true }); }
		}
		if (!certificate && !crash) await store.exclusive(async () => { process.send('held'); await new Promise(() => {}); });
		for (const value of values) await store.artifacts.put(value);
		if (crash === 'collect') await store.gc(); else await store.put(certificate);
		await store.dispose(); clearInterval(alive); process.send('published'); process.disconnect();
	`], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
	let errors = ""; child.stderr!.on("data", data => { errors += data; });
	child.on("message", message => { if (message === "crash") child.kill("SIGKILL"); });
	const completed = once(child, "close").then(([code]) => { if (code !== 0) throw new Error(`store peer exited ${code}: ${errors}`); });
	const noticed = Promise.race([once(child, "message").then(([message]) => message as string), completed.then(() => { throw new Error("store peer exited before its notice"); })]);
	void completed.catch(() => undefined); void noticed.catch(() => undefined); child.send({ root, certificate, values, crash });
	return { child, noticed, completed };
}
