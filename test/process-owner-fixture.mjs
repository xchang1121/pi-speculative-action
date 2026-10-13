import { once } from "node:events";
import fs from "node:fs/promises";
import net from "node:net";
import { syncBuiltinESMExports } from "node:module";
import { mock } from "node:test";
import { parentPort, workerData } from "node:worker_threads";

// Faults and inspection run where production owns the work; no production test outlet is needed.
if (workerData?.nativeProcessObserver) {
	const [{ processTestPort: port }] = await once(parentPort, "message"), state = {
		fs, net,
		deferred: () => Promise.withResolvers(),
		method: (target, key, implementation) => {
			const method = mock.method(target, key, implementation), restore = method.mock.restore.bind(method.mock);
			method.mock.restore = () => { restore(); syncBuiltinESMExports(); }; syncBuiltinESMExports(); return method;
		},
		load: specifier => import(specifier.startsWith("node:") ? specifier : new URL(specifier, import.meta.url).href),
	};
	port.on("message", ({ source, args, reply }) => {
		void Promise.resolve().then(() => Function("state", "args", `return (${source})(state, ...args)`)(state, args))
			.then(value => reply.postMessage({ value }), error => reply.postMessage({ error: String(error?.stack ?? error) }))
			.finally(() => reply.close());
	});
	port.unref();
}
