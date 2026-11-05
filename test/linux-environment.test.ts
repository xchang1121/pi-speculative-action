import { chmod, mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { expect, test } from "vitest";
import { inspectLinuxEnvironment } from "../src/linux-environment.ts";
import { executionDomainCollector, executionDomainProof, resolveLinuxExecHelper } from "../src/linux-held-exec.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { sha256Digest } from "../src/stable-value-hash.ts";

test.skipIf(process.platform !== "linux").each([
	["with space 中", ""], ["with\\backslash", ""], ["long/" + "x".repeat(240), ""], ["d/".repeat(18) + "leaf", ""],
	["shared", "root_permissions"], ["d/".repeat(70) + "leaf", "root_depth_limit"],
])("uses the same workspace admission for diagnostics and reuse (%s)", async (suffix, reason) => {
	const base = await mkdtemp(path.join(os.homedir(), "pi-domain-admission-")), root = path.join(base, suffix!);
	const binary = await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC); nativeProcessLibrary(binary);
	const collector = await executionDomainCollector(binary), before = (await readdir("/proc/self/fd")).length;
	let proof: ReturnType<typeof executionDomainProof> | undefined;
	try {
		await mkdir(root, { recursive: true }); await chmod(root, reason === "root_permissions" ? 0o775 : 0o700);
		await writeFile(path.join(root, "input"), "original");
		proof = executionDomainProof(collector, root);
		const valid = proof.seal([{ kind: "file", path: path.join(root, "input"), role: "input", contentDigest: sha256Digest("original") }], target => target);
		expect(valid(), proof.reason).toBe(!reason);
		const report = await inspectLinuxEnvironment(root);
		expect(report.text).toContain(reason || "Workspace admission: available");
		if (reason) { expect(proof.reason).toContain(reason); expect(report.warnings).toBe(true); }
		else { await writeFile(path.join(root, "input"), "changed"); expect(valid()).toBe(false); }
	} finally { proof?.close(); await rm(base, { recursive: true, force: true }); }
	expect((await readdir("/proc/self/fd")).length).toBe(before);
});

test("reports the unavailable execution domain without claiming process qualification", async () => {
	const socket = process.env.PI_SPEC_DOMAIN_SOCKET;
	try {
		delete process.env.PI_SPEC_DOMAIN_SOCKET;
		const report = await inspectLinuxEnvironment();
		expect(report.warnings).toBe(true);
		expect(report.text).toContain(process.platform === "linux" ? "execution_domain_missing" : "Linux or WSL 2");
	} finally { if (socket !== undefined) process.env.PI_SPEC_DOMAIN_SOCKET = socket; }
});
