import { createExecPrototype, sealProcessCertificate, type ProcessProvenanceCertificate } from "../src/provenance-certificate.ts";
import { sha256Digest } from "../src/stable-value-hash.ts";
import { spawn } from "node:child_process";
import { once } from "node:events";
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

/** A separate process exercises checkpoint ownership and artifact collection. */
export function storePeer(root: string, certificate?: ProcessProvenanceCertificate,
	checkpoint?: { readonly ticket: string; readonly action: "hold" | "consume" | "release" }) {
	const child = spawn(process.execPath, ["--input-type=module", "--eval", `
		const { ProvenanceCertificateStore } = await import(${JSON.stringify(new URL("../src/reuse-store.ts", import.meta.url).href)});
		const { root, certificate, checkpoint } = await new Promise(resolve => process.once('message', resolve));
		const alive = setInterval(() => {}, 1000), store = new ProvenanceCertificateStore(root);
		let notice = 'collected';
		if (checkpoint) {
			const claim = await store.claimProcessCheckpoint(checkpoint.ticket, certificate.id);
			if (claim && checkpoint.action === 'hold') { process.send('claimed'); await new Promise(() => {}); }
			if (claim) await claim(checkpoint.action === 'consume');
			notice = claim ? 'claimed' : 'miss';
		} else await store.gc();
		await store.dispose(); clearInterval(alive); process.send(notice); process.disconnect();
	`], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
	let errors = ""; child.stderr!.on("data", data => { errors += data; });
	const completed = once(child, "close").then(([code]) => { if (code !== 0) throw new Error(`store peer exited ${code}: ${errors}`); });
	const noticed = Promise.race([once(child, "message").then(([message]) => message as string), completed.then(() => { throw new Error("store peer exited before its notice"); })]);
	void completed.catch(() => undefined); void noticed.catch(() => undefined); child.send({ root, certificate, checkpoint });
	return { child, noticed, completed };
}
