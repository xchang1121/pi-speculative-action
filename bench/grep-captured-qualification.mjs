import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createGrepTool } from "@earendil-works/pi-coding-agent";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { makeStructuralSpeculativeActionRuntime } from "../src/runtime-engine.ts";

// Qualification only: complete stock-Pi grep on private, token-owned flat inputs.
// No arbitrary host-tool speculation, downloads, process shim or alternate regexp engine.
const { getToolPath } = await import(new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
if (!getToolPath("rg")) { console.log(JSON.stringify({ qualification: "skipped", reason: "No existing Pi rg; nothing installed" })); process.exit(0); }
process.env.PI_OFFLINE = "1";
const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-grep-evidence-")), report = [];
const configuredAtStart = process.env.RIPGREP_CONFIG_PATH;
const source = await fs.readFile(new URL("../src/runtime-engine.ts", import.meta.url));
const greek = Buffer.from("Αλφα βήτα Ελληνικά κώδικας γράμματα λέξεις μία δύο τρία τέσσερα\n".repeat(50_000));
const rows = [
  { label: "repository", contents: source, files: 32, patterns: ["authoritativeMutationResources", "\\b(?:[A-Za-z_]\\w*\\.){4,}[A-Za-z_]\\w*\\b", "(?:\\p{L}+\\s+){15}\\p{L}+"] },
  { label: "unicode", contents: greek, files: 1, patterns: ["needle", "^\\w{60}$", "(?:\\p{L}+ +){15}\\p{L}+"] },
];
const median = async (run) => {
  const times = []; let output;
  for (let i = 0; i < 5; i++) { const started = performance.now(); output = await run(); times.push(performance.now() - started); }
  return { ms: times.sort((a, b) => a - b)[2], output };
};
try {
  const configuration = path.join(root, "controlled-rg-config"); await fs.writeFile(configuration, "--sort=path\n--no-ignore-global\n");
  for (const row of rows) {
    const cwd = path.join(root, row.label); await fs.mkdir(cwd);
    for (let i = 0; i < row.files; i++) await fs.writeFile(path.join(cwd, `${i}.txt`), row.contents);
    const tool = createGrepTool(cwd);
    for (const pattern of row.patterns) {
      const args = { path: ".", pattern, limit: 100 };
      const native = await median(() => tool.execute("Actor", args));
      process.env.RIPGREP_CONFIG_PATH = configuration;
      let captured;
      try {
        const configured = await median(() => tool.execute("sorted host Actor", args));
        captured = await qualifyCaptured(cwd, args, configured.output);
        captured.sortedHostActorMs = configured.ms;
        captured.nativeOutputEqual = JSON.stringify(configured.output) === JSON.stringify(native.output);
      } finally { if (configuredAtStart === undefined) delete process.env.RIPGREP_CONFIG_PATH; else process.env.RIPGREP_CONFIG_PATH = configuredAtStart; }
      report.push({ fixture: row.label, files: row.files, bytes: row.contents.length * row.files, pattern,
        nativeActorMs: native.ms,
        outputBytes: Buffer.byteLength(JSON.stringify(native.output)), noMatch: native.output.content[0]?.text === "No matches found", captured });
      console.log(JSON.stringify(report.at(-1)));
    }
  }
  console.log(JSON.stringify({ platform: process.platform, node: process.version, report,
    qualification: "Fixed flat fixtures only. Runtime adoption is real; general path/config/ignore/encoding and cancellation qualification are still required." }, null, 2));
} finally {
  assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.ok(path.basename(root).startsWith("pi-grep-evidence-"));
  await fs.rm(root, { recursive: true, force: true });
}

async function qualifyCaptured(cwd, args, expected) {
  const ready = Promise.withResolvers(), started = performance.now(); let executions = 0, copies = 0;
  const invocation = { filesystem: async (view, request) => {
    executions++;
    const privateRoot = await fs.mkdtemp(path.join(root, "retained-"));
    try {
      for (const name of await view.readdir(cwd)) {
        assert.equal(path.basename(name), name); const target = path.join(cwd, name);
        assert.equal((await view.stat(target, "type")).isDirectory(), false, "This probe qualifies flat fixtures, not a general namespace");
        await fs.writeFile(path.join(privateRoot, name), await view.readFile(target), { flag: "wx" }); copies++;
      }
      request.signal.throwIfAborted();
      return { result: await createGrepTool(privateRoot).execute(request.callID, request.args, request.signal), isError: false };
    } finally {
      assert.equal(path.dirname(privateRoot), root); assert.ok(path.basename(privateRoot).startsWith("retained-"));
      await fs.rm(privateRoot, { recursive: true, force: true });
    }
  } };
  const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["grep"], maxBytes: () => 8 * 1024 * 1024 });
  const route = { backend: world.id, scope: "fallback", isolation: world.isolation, reuse: "shared_result", fingerprint: "captured-grep-qualification" };
  const runtime = makeStructuralSpeculativeActionRuntime({
    settings: () => ({ enabled: true, tools: ["grep"], maxConcurrentActions: 1, resourceCacheMaxEntries: 32, resourceCacheMaxBytes: 16 * 1024 * 1024 }),
    sources: [{ id: "probe", enabled: () => true, propose: () => ({ id: "probe", source: "probe", revision: 0, actions: [{ id: "grep", type: "tool_call", tool: "grep", input: args }] }) }],
    definitions: () => [{ name: "grep" }], stateData: () => ({ cwd }), resolveExecution: () => route,
    preflightCandidate: () => ({ ok: true }),
    actionKey: (tool, input) => PI_ACTION_SEMANTICS.buildKey(tool, input, cwd, "", { fingerprint: "captured-grep-qualification", context: invocation }),
    actual: ({ id, tool, input }) => ({ id, tool, input }),
    executeCandidate: (request) => world.speculation.execute({ cwd, tool: createGrepTool(cwd), toolName: request.tool, args: request.concrete,
      action: request.action, callID: request.callID, signal: request.signal }),
    onEvent: (event) => {
      if (event.type === "candidate" && ["succeeded", "failed", "cancelled"].includes(event.state.status)) ready.resolve(event.state);
      if (event.type === "prediction" && event.settlement.observation === "unobserved") ready.reject(new Error(JSON.stringify(event.settlement)));
    },
  });
  const turn = { sessionID: "probe", turnID: "probe" };
  let callID = 0;
  try {
    await runtime.startTurn(turn);
    const completion = await ready.promise; assert.equal(completion.status, "succeeded", JSON.stringify(completion));
    const producerMs = performance.now() - started, materialized = copies;
    const adopted = await median(async () => {
      const result = await runtime.consume({ ...turn, id: `Actor-${++callID}`, tool: "grep", input: args });
      assert.deepEqual(result?.result, expected); return result;
    });
    assert.equal(executions, 1); assert.equal(copies, materialized);
    return { producerMs, hitMs: adopted.ms, producerCalls: executions, filesMaterialized: materialized };
  } finally { await runtime.dispose(); }
}
