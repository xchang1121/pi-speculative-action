import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { type ProcessExecutionBinding, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { certificateReplayable, ONE_SHOT_TAINTS, type ProcessProvenanceCertificate, processWeakKey, sha256Digest } from "../src/provenance-certificate.ts";
import { processCertificate, processPrototype, SPECULATIVE_PRODUCER } from "./process-fixture.ts";

const scope = { sessionID: "preparation", turnID: "measured" };
const prototype = processPrototype();
const unusable = processCertificate(prototype, { dependencyCertificate: { complete: true, dependencies: [],
	taints: [...ONE_SHOT_TAINTS, "unsupported_syscall", "mutable_input"] } });

/** Drive the real binding boundary with sealed evidence; no subprocess, filesystem mutation or native policy is mocked in. */
function fixture(certificate: ProcessProvenanceCertificate = unusable, source: "native" | "resources" | "producer" = "native") {
	const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "preparation-evidence-fixture") });
	const internal = backend as unknown as {
		handoffs: ProcessHandoffRegistry<unknown>;
		prototype: (...args: unknown[]) => Promise<unknown>;
		executeRequest: (...args: unknown[]) => Promise<unknown>;
		executeBinding: (session: unknown, binding: ProcessExecutionBinding) => Promise<unknown>;
		recordPreparedResult: (session: unknown, certificate: ProcessProvenanceCertificate) => boolean;
	};
	const invocation = { sourceRoot: "/workspace", executable: "/bin/tool", cwd: "/workspace", argv0: "tool", args: [], environment: { MODE: "test" }, outputRoute: [1, 2],
		...(source === "producer" ? { producer: SPECULATIVE_PRODUCER } : {}),
		...(source === "resources" ? { resources: { handles: [{ fd: 0, description: 0 }], descriptions: { 0: { object: 0, flags: 0 } },
			objects: { 0: { type: "pipe", contentDigest: sha256Digest("prefix"), content: Buffer.from("prefix").toString("base64"),
				queue: { eof: false, bytes: 6, capacity: 4096, producer: "live" } } } } } : {}) };
	const observe = (next = prototype, context = invocation) => internal.handoffs.observe(processWeakKey(next), next.executablePath, scope, context, 6000)!;
	const binding = observe(), controller = new AbortController();
	const session = { sourceRoot: "/workspace", scope, nestedProducer: SPECULATIVE_PRODUCER, projection: { toPhysical: (value: string) => value }, preparedResults: 0,
		workspace: { structure: { capture: vi.fn(async () => ({})) } }, signal: controller.signal, computations: [], metrics: { ...emptyWorldReuseMetrics() } };
	const describe = vi.spyOn(internal, "prototype").mockResolvedValue(prototype);
	const execute = vi.spyOn(internal, "executeRequest").mockImplementation(async () => ({
		kind: certificate.result.continuation ? "suspended" : "executed", exit: certificate.result.exit,
		reusable: internal.recordPreparedResult(session, certificate),
	}));
	return { internal, invocation, observe, binding, controller, session, execute, describe,
		run: (selected = binding) => internal.executeBinding(session, selected),
		close: () => { execute.mockRestore(); describe.mockRestore(); internal.handoffs.dispose(); } };
}

describe("native preparation evidence", () => {
	it("retires repeated tainted preparation without reviving it on identical measured launches", async () => {
		const test = fixture();
		try {
			test.session.metrics.lastError = "tainted:unsupported_syscall,mutable_input; syscalls:fcntl; dependency:mutable:/tmp/jest/input";
			await expect(test.run()).rejects.toThrow("bound process preparation produced no reusable result: tainted:unsupported_syscall,mutable_input; syscalls:fcntl; dependency:mutable:/tmp/jest/input");
			expect(test.binding.available).toBe(false);
			for (let edit = 0; edit < 5; edit++) {
				expect(test.observe()).toBe(test.binding);
				await expect(test.run()).rejects.toThrow("binding is unavailable");
			}
			expect(test.execute).toHaveBeenCalledOnce();
			// A changed launch environment has new proof identity and earns its own probe.
			const next = processPrototype({ environment: { MODE: "changed" } });
			const changed = test.observe(next, { ...test.invocation, environment: { MODE: "changed" } });
			test.describe.mockResolvedValue(next);
			test.execute.mockResolvedValue({ kind: "executed", reusable: true, exit: { kind: "code", code: 0 } });
			await expect(test.run(changed)).resolves.toMatchObject({ exit: { code: 0 } });
			expect(changed.available).toBe(true);
		} finally { test.close(); }
	});

	it.each(["completed", "one-shot", "continuation", "nonzero"] as const)("preserves %s evidence independently of disk publication", async kind => {
		const certificate = processCertificate(prototype, {
			dependencyCertificate: { complete: true, dependencies: [], taints: kind === "one-shot" ? ONE_SHOT_TAINTS : [] },
			result: { replayProfile: "buffered_noninteractive", journal: [], ...(kind === "continuation"
				? { continuation: { imageDigest: sha256Digest("frontier"), imageBytes: 8 } }
				: { exit: { kind: "code", code: kind === "nonzero" ? 2 : 0 } }) },
		});
		const test = fixture(certificate);
		try {
			await expect(test.run()).resolves.toMatchObject(kind === "continuation" ? { suspended: true } : { exit: certificate.result.exit });
			expect(test.binding.available).toBe(true);
			expect(test.session.preparedResults).toBe(1);
			expect(test.session.metrics.published).toBe(0);
		} finally { test.close(); }
	});

	it.each([false, true])("preserves reusable descendants of an unusable enclosing preparation (continuation=%s)", async continuation => {
		const test = fixture();
		test.execute.mockImplementation(async () => {
			test.internal.recordPreparedResult(test.session, processCertificate(prototype, continuation ? {
				result: { replayProfile: "buffered_noninteractive", journal: [], continuation: { imageDigest: sha256Digest("frontier"), imageBytes: 8 } },
			} : {}));
			return { kind: "executed", exit: { kind: "code", code: 0 }, reusable: test.internal.recordPreparedResult(test.session, unusable) };
		});
		try { await expect(test.run()).resolves.toMatchObject({ exit: { code: 0 } }); expect(test.binding.available).toBe(true); }
		finally { test.close(); }
	});

	it.each(["resources", "producer"] as const)("preserves %s bindings whose completed result cannot replay", async source => {
		const test = fixture(unusable, source);
		try {
			await expect(test.run()).resolves.toMatchObject({ exit: { code: 0 } });
			expect(test.binding.available).toBe(true);
			expect(test.session.preparedResults).toBe(0);
		} finally { test.close(); }
	});

	it("preserves a descendant seed with uncovered future stdin for later live preparation", async () => {
		const seed = processCertificate(processPrototype({ stdin: { type: "bytes", eof: false, digest: sha256Digest("prefix") },
			inheritedFDs: [{ fd: 0, type: "pipe", flagsDigest: sha256Digest("read-only"), contentDigest: sha256Digest("prefix"), eof: false, object: 0 }] }), {
			dependencyCertificate: { complete: true, dependencies: [], taints: ONE_SHOT_TAINTS },
		});
		expect(certificateReplayable(seed, ONE_SHOT_TAINTS)).toBe(false);
		const test = fixture();
		test.execute.mockImplementation(async () => {
			test.session.metrics.requests++;
			expect(test.internal.recordPreparedResult(test.session, seed)).toBe(false);
			return { kind: "executed", exit: { kind: "code", code: 0 }, reusable: test.internal.recordPreparedResult(test.session, unusable) };
		});
		try {
			await expect(test.run()).resolves.toMatchObject({ exit: { code: 0 } });
			expect(test.binding.available).toBe(true);
			expect(test.session.preparedResults).toBe(0);
		} finally { test.close(); }
	});

	it.each(["execution error", "capture error", "aborted", "unsealed"] as const)("does not retire a launch after %s", async outcome => {
		const test = fixture();
		test.execute.mockImplementation(async () => {
			if (outcome.endsWith("error")) throw new Error(outcome);
			if (outcome === "aborted") test.controller.abort(new Error("aborted"));
			return { kind: "executed", exit: { kind: "code", code: 0 }, ...(outcome === "aborted" ? { reusable: false } : {}) };
		});
		try {
			if (outcome === "unsealed") await expect(test.run()).resolves.toMatchObject({ exit: { code: 0 } });
			else await expect(test.run()).rejects.toThrow(outcome);
			expect(test.binding.available).toBe(true);
		} finally { test.close(); }
	});
});
