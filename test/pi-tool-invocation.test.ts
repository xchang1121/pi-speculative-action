import { describe, expect, it } from "vitest";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";

describe("stock Pi invocation identity", () => {
	it("binds exact execution semantics independently of call arguments", () => {
		const options = { cwd: process.cwd(), environment: { PATH: "tools", BENCHMARK: "true" },
			shellPath: process.execPath, shellCommandPrefix: "set -e" };
		const invocation = resolvePiToolInvocation("bash", { command: "printf ok", timeout: 2.5 }, options)!;
		expect(invocation.process).toEqual({
			command: "set -e\nprintf ok", cwd: options.cwd, environment: options.environment,
			shell: process.execPath, shellArgs: ["-c"], commandTransport: "argv", timeout: 2.5,
		});
		const { command: _command, timeout: _timeout, ...processIdentity } = invocation.process!;
		expect(invocation.identity).toEqual({ ...processIdentity, executor: "pi.bash.local.v2", commandPrefix: "set -e" });
		expect(resolvePiToolInvocation("bash", { command: "printf other", timeout: 9 }, options)?.identity).toEqual(invocation.identity);
		const read = resolvePiToolInvocation("read", { path: "a.ts" }, options)!;
		expect(read.resources).toBeTypeOf("function");
		expect(read.process).toBeUndefined();
		expect(resolvePiToolInvocation("read", { path: "b.ts", offset: 2 }, options)?.identity).toEqual(read.identity);
		for (const setting of [{ autoResizeImages: false }, { modelSupportsImages: false }]) {
			expect(resolvePiToolInvocation("read", { path: "a.ts" }, { ...options, ...setting })?.identity).not.toEqual(read.identity);
		}
		expect(resolvePiToolInvocation("bash", { command: 1 }, options)).toBeUndefined();
	});
});
