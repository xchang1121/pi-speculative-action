/// <reference lib="es2024.promise" />
export { setImmediate as nextTurn } from "node:timers/promises";

/** Release test resources at their lexical ownership boundary, including assertion failures. */
export function disposable<Value extends { dispose(): unknown } | { close(): unknown }>(value: Value) {
	return Object.assign(value, { [Symbol.asyncDispose]: async () => { await ("dispose" in value ? value.dispose() : value.close()); } });
}
export function onExit(dispose: () => Promise<void>) { return { [Symbol.asyncDispose]: dispose }; }

/** Explicit race barriers; tests control admission and completion independently of elapsed time. */
export const deferred = <Value = void>() => Promise.withResolvers<Value>();

/** Observe callback entry and release it without introducing another promise turn. */
export function gated(expected = 1) {
	const entered = deferred(), released = deferred();
	return { entered: entered.promise, release: () => released.resolve(),
		wait: () => { if (expected > 0 && --expected === 0) entered.resolve(); return released.promise; } };
}
