import { chmod, link, mkdir, mkdtemp, rename, rm, symlink, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { expect, test } from "vitest";
import { executionDomainCollector, executionDomainProof, LinuxHeldExecBoundary, resolveLinuxExecHelper } from "../src/linux-held-exec.ts";
import { isPoisonedEffectCommit } from "../src/effect-transaction.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { sha256Digest } from "../src/stable-value-hash.ts";
import { WorkspaceStructureCapture } from "../src/process-observation.ts";

test.skipIf(process.platform !== "linux").each([false, true])("settles the staged native handoff (cancel=%s)", { timeout: 30_000 }, async cancel => {
	const root = await mkdtemp(path.join(os.homedir(), "pi-staged-cancel-"));
	const controller = new AbortController(), settlements: boolean[] = [];
	let validated = false, commits = 0, output = "", failure: unknown;
	const boundary = await LinuxHeldExecBoundary.open({ storeRoot: root, binary: process.env.PI_SPEC_HELD_EXEC,
		observerBinary: process.env.PI_SPEC_HELD_EXEC_OBSERVER });
	try {
		await boundary.observe({ sourceRoot: root, signal: controller.signal, decide: async () => ({
			kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.from("cached") }],
			validate: () => { validated = true; if (cancel) controller.abort(new Error("cancelled after staging")); return true; },
			commit: async () => { commits++; }, settle: consumed => { settlements.push(consumed); },
		}) }, () => new Promise<void>((resolve, reject) => {
			const child = spawn("/bin/printf", ["native"], { cwd: root, signal: controller.signal, stdio: ["ignore", "pipe", "pipe"] });
			child.stdout.on("data", bytes => { output += bytes; }); child.once("error", reject); child.once("close", () => resolve());
		})).catch(error => { failure = error; });
		expect(validated).toBe(true);
		expect(commits).toBe(Number(!cancel));
		if (cancel) expect(output).not.toContain("cached");
		else { expect(output).toBe("cached"); expect(failure).toBeUndefined(); }
		expect(settlements).toEqual([!cancel]);
		expect(isPoisonedEffectCommit(failure)).toBe(false);
	} finally { await boundary.close(); await rm(root, { recursive: true, force: true }); }
});

test.skipIf(process.platform !== "linux").each([1, 1366])("keeps continuous input proof across %s dependencies", { timeout: 30_000 }, async count => {
	const root = await mkdtemp(path.join(os.homedir(), "pi-input-proof-")), directory = path.join(root, "inputs");
	const binary = await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC); nativeProcessLibrary(binary);
	const collector = await executionDomainCollector(binary);
	const dependencies = Array.from({ length: count }, (_, index) => ({ kind: "file" as const, role: "input" as const,
		path: path.join(directory, String(index)), contentDigest: sha256Digest("original") }));
	const proofs: ReturnType<typeof executionDomainProof>[] = [];
	const fresh = (inputs = dependencies) => { const proof = executionDomainProof(collector, root); proofs.push(proof); return proof.seal(inputs, target => target); };
	try {
		await mkdir(directory, { mode: 0o700 });
		for (const input of dependencies) await writeFile(input.path, "original");
		await symlink("inputs", path.join(root, "linked"));
		const linked = fresh(dependencies.map(input => ({ ...input, path: path.join(root, "linked", path.basename(input.path)) })));
		const unchanged = fresh(); expect(unchanged()).toBe(true);
		expect(linked()).toBe(true);
		await writeFile(path.join(directory, "unrelated"), "sibling"); expect(unchanged()).toBe(true);
		const last = dependencies.at(-1)!.path;
		await writeFile(last, "changed"); await writeFile(last, "original"); expect(unchanged()).toBe(false);
		expect(linked()).toBe(false);
		const moved = fresh(); expect(moved()).toBe(true);
		await rename(directory, directory + "-moved"); await rename(directory + "-moved", directory); expect(moved()).toBe(false);
		const permissions = fresh(); expect(permissions()).toBe(true);
		await chmod(directory, 0o500); await chmod(directory, 0o700); expect(permissions()).toBe(false);
		expect(fresh()()).toBe(true);
	} finally { for (const proof of proofs) proof.close(); await rm(root, { recursive: true, force: true }); }
});

test.skipIf(process.platform !== "linux")("discovers new and replaced workspace entries through retained directory versions", { timeout: 30_000 }, async () => {
	const root = await mkdtemp(path.join(os.homedir(), "pi-workspace-proof-")), directory = path.join(root, "inputs");
	const binary = await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC); nativeProcessLibrary(binary);
	const versions = (await executionDomainCollector(binary)).open();
	try {
		await mkdir(directory);
		for (let index = 0; index < 1366; index++) await writeFile(path.join(directory, String(index)), "original");
		versions.watch([root]);
		const capture = new WorkspaceStructureCapture(root, {}, versions), before = await capture.capture();
		expect(before.complete).toBe(true);
		expect((await capture.capture()).entries).toBe(before.entries);
		await writeFile(path.join(directory, "1365"), "changed");
		await link(path.join(directory, "1365"), path.join(root, "alias"));
		const changed = await capture.capture();
		expect(changed.complete).toBe(true);
		expect(changed.entries.get(path.join("inputs", "1365"))).not.toEqual(before.entries.get(path.join("inputs", "1365")));
		expect(changed.entries.get("alias")).toMatchObject({ kind: "file", size: 7 });
		await rename(directory, directory + "-old"); await mkdir(directory); await writeFile(path.join(directory, "new"), "new");
		const replaced = await capture.capture();
		expect(replaced.complete).toBe(true);
		expect(replaced.entries.has(path.join("inputs", "1365"))).toBe(false);
		expect(replaced.entries.get(path.join("inputs", "new"))).toMatchObject({ kind: "file", size: 3 });
		expect(replaced.entries.has(path.join("inputs-old", "1365"))).toBe(true);
	} finally { versions.close(); await rm(root, { recursive: true, force: true }); }
});
