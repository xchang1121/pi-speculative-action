import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { registerHooks } from "node:module";
import { PassThrough } from "node:stream";
import { serveClosedSearchWorker } from "../src/closed-search-kernel.mjs";

// Feasibility only: the exact Pi tool runs here; native handles remain in the caller.
// The production kernel has no process capability and does not load this module adapter.
let readInput;
const outlet = {
  spawnSync: (name, args) => {
    assert.equal(name, "rg"); assert.deepEqual(args, ["--version"]);
    return { status: 0 }; // Parent already required an installed rg; discovery never downloads or launches here.
  },
  spawn: (file, args, options) => {
    assert.ok(readInput, "no owning invocation");
    const child = new EventEmitter(), controller = new AbortController();
    child.stdout = new PassThrough(); child.stderr = new PassThrough(); child.killed = false;
    child.kill = () => { child.killed = true; controller.abort(); return true; };
    void readInput("process", { file, args, options }, { signal: controller.signal, onChunk: ({ fd, data }) => {
      assert.ok(fd === 1 || fd === 2); (fd === 1 ? child.stdout : child.stderr).write(data);
    } }).then(({ code, signal }) => {
      child.stdout.end(); child.stderr.end(); child.emit("exit", code, signal);
      queueMicrotask(() => child.emit("close", code, signal));
    }, (error) => { child.stdout.end(); child.stderr.end(); child.emit("error", error); child.emit("close", null, null); });
    return child;
  },
};
const exports = Object.keys(await import("node:child_process")).filter((name) => name !== "default");
for (const name of exports) outlet[name] ??= () => { throw new Error(`process capability not granted: ${name}`); };
globalThis.__grepQualificationProcess = outlet;
const module = new URL("./grep-process-capability.mjs", import.meta.url).href;
const source = `export const { ${exports.join(",")} } = globalThis.__grepQualificationProcess; export default globalThis.__grepQualificationProcess;`;
registerHooks({ resolve(specifier, context, next) {
  return ["child_process", "node:child_process"].includes(specifier) ? { url: module, format: "module", shortCircuit: true } : next(specifier, context);
}, load(url, context, next) { return url === module ? { format: "module", source, shortCircuit: true } : next(url, context); } });
process.env.PI_OFFLINE = "1";
globalThis.fetch = () => { throw new Error("no search downloads"); };
const { createGrepToolDefinition } = await import(new URL("./core/tools/grep.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
serveClosedSearchWorker(async (input, requestInput) => {
  readInput = requestInput;
  try { return { result: await createGrepToolDefinition(input.root).execute("grep-qualification", input.args), isError: false }; }
  finally { readInput = undefined; }
});
