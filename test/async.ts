export { setImmediate as nextTurn } from "node:timers/promises";

/** Explicit race barriers; tests control admission and completion independently of elapsed time. */
export function deferred<Value = void>() {
	let resolve!: (value: Value | PromiseLike<Value>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<Value>((done, fail) => { resolve = done; reject = fail; });
	return { promise, resolve, reject };
}

export function barrier(expected = 1) {
	const done = deferred();
	return { promise: done.promise, arrive: () => { if (expected > 0 && --expected === 0) done.resolve(); } };
}
