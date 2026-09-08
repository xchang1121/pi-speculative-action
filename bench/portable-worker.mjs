import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { parentPort } from "node:worker_threads";
import { CLOSED_SEARCH_PROFILE, createClosedSearchKernel, connectClosedSearchWorker } from "../dist/closed-search-kernel.mjs";

// Real process lifetime probe. Only this qualification entry admits an intentionally uncooperative loop.
registerHooks({ resolve(specifier, context, next) {
	if (["wasi-sh", "globby", "ripgrep"].some((name) => specifier === name || specifier.startsWith(name + "/")))
		throw new Error("extra search dependency denied");
	return next(specifier, context);
} });
globalThis.fetch = () => { throw new Error("search download denied"); };
const readChannel = connectClosedSearchWorker(import.meta.url);
if (readChannel) {
	const kernel = await createClosedSearchKernel();
	await assert.rejects(createClosedSearchKernel(), /own worker lifetime/);
	parentPort.on("message", async ({ type, id, input }) => {
		assert.equal(type, "request");
		try {
			parentPort.postMessage({ type: "started", id });
			if (input.kind === "spin") for (;;) {}
			const result = await kernel.execute(input.kind, input.root, input.args, (operation, target) => readChannel(id, operation, target));
			parentPort.postMessage({ type: "result", id, result });
		} catch (error) { parentPort.postMessage({ type: "result", id, error: String(error?.message ?? error) }); }
	});
	parentPort.postMessage({ type: "ready", profile: CLOSED_SEARCH_PROFILE });
}
