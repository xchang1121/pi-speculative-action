import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { createClosedSearchKernel, serveClosedSearchWorker } from "../dist/closed-search-kernel.mjs";

// Real process lifetime probe. Only this qualification entry admits an intentionally uncooperative loop.
registerHooks({ resolve(specifier, context, next) {
	if (["wasi-sh", "globby", "ripgrep"].some((name) => specifier === name || specifier.startsWith(name + "/")))
		throw new Error("extra search dependency denied");
	return next(specifier, context);
} });
globalThis.fetch = () => { throw new Error("search download denied"); };
const kernel = await createClosedSearchKernel();
await assert.rejects(createClosedSearchKernel(), /own bounded process lifetime/);
serveClosedSearchWorker((input, readInput) => {
	if (input.kind === "spin") for (;;) {}
	return kernel.execute(input.kind, input.root, input.args, readInput);
});
