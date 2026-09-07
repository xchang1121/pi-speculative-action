import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AgentPosixClient, CONTRACT_FINGERPRINT } from "@thinkthread/agent-posix";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { qualifyStockTool, STOCK_TOOL_CASES } from "./stock-tool-qualification.ts";

// Run inside the real installed profile. A local runner or missing Runtime is never a pass.
assert.equal(process.platform, "linux", "ThinkThread qualification requires Linux inside a real tt profile");
assert.ok(process.env.THINKTHREAD_FS, "THINKTHREAD_FS missing: start the installed tt pi-speculative-action profile first");
const cwd = path.resolve(process.env.THINKTHREAD_FS);
assert.equal(path.resolve(process.cwd()), cwd, "Run from the profile's THINKTHREAD_FS root");
const client = AgentPosixClient.fromEnv();
const self = await client.selfView();
assert.ok(self.capabilities.some((capability) => capability.id === "thinkthread.fs.self" && capability.version === 1));
const attachment = await client.fs.stat(); // Prove a live connection before creating any fixture.
const runnerPath = fileURLToPath(new URL("../dist/thinkthread/tool-runner.js", import.meta.url));
await access(runnerPath);
console.log(JSON.stringify({ runtime: "ThinkThread", platform: process.platform, arch: process.arch,
	contract: CONTRACT_FINGERPRINT, attachment: attachment.kind }));
for (const [name, args] of STOCK_TOOL_CASES) {
	console.log(JSON.stringify(await qualifyStockTool(name, args, {
		cwd, world: createThinkThreadExecutionWorld({ runnerPath }),
	})));
}
console.log("Six stock tools passed real execution/adoption comparison; Bash and failure-recovery qualification are separate gates.");
