import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { bindResourceOperation } from "../src/agent-execution-world.ts";
import { ResourceInputSession } from "../src/resource-version.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-query-lifetime-");
afterEach(async () => { vi.restoreAllMocks(); await directories.dispose(); });

test.skipIf(process.platform !== "linux")("cancelled query releases an input capture that settles after its waiter", async () => {
	const base = await directories.create(), root = path.join(base, "source"), file = path.join(root, "input.txt");
	await mkdir(root); await writeFile(file, "captured input\n");
	const store = new ProvenanceCertificateStore(path.join(base, "cache"));
	const session = new ResourceInputSession({ store }, 1024 * 1024);
	const acquired = Promise.withResolvers<void>(), resume = Promise.withResolvers<void>();
	const capture = session.capture.bind(session); let captures = 0;
	vi.spyOn(session, "capture").mockImplementation(async (...args) => {
		const token = await capture(...args);
		if (++captures === 2) { acquired.resolve(); await resume.promise; }
		return token;
	});
	const invocation = bindResourceOperation(async (view, request) => {
		await view.prepare!({}, "read", async inputs => ({ value: await inputs.readFile(file), bytes: 32, dispose: () => {} }),
			async value => value, file, request.signal);
		return { result: { content: [{ type: "text", text: "done" }], details: {} }, isError: false };
	}, root);
	const observation = await invocation.captureInputs!(undefined, 1024 * 1024, "cancelled", session);
	const cancellation = new AbortController();
	let finished = false;
	const operation = invocation.authoritative!({ callID: "cancelled", args: {}, signal: cancellation.signal })
		.then(() => { finished = true; }, () => { finished = true; });
	try {
		await acquired.promise; cancellation.abort();
		await new Promise<void>(resolve => setTimeout(resolve, 20));
		expect(finished).toBe(false);
		resume.resolve(); await operation; await observation.dispose();
		expect(captures).toBe(2);
		expect(session.contents.closed).toBe(true);
		expect(session.contents.openChunks).toBe(0);
	} finally { resume.resolve(); await operation; await observation.dispose(); await store.dispose(); }
});
