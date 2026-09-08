import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { createGrepTool } from "@earendil-works/pi-coding-agent";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { makeStructuralSpeculativeActionRuntime } from "../src/runtime-engine.ts";
import { launchClosedSearchWorker } from "../src/closed-search-process.mjs";

// Qualification only: complete stock-Pi grep on private, token-owned flat inputs.
// Stock Pi runs in a bounded process; the parent owns rg, its output and completion.
const { getToolPath } = await import(new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
const rg = getToolPath("rg");
if (!rg) { console.log(JSON.stringify({ qualification: "skipped", reason: "No existing Pi rg; nothing installed" })); process.exit(0); }
process.env.PI_OFFLINE = "1";
const source = await fs.readFile(new URL("../src/runtime-engine.ts", import.meta.url));
const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-grep-evidence-")), report = [];
let worker;
let nativeProcesses = 0, nativeClosed = 0, nativeCancels = 0;
const configuredAtStart = process.env.RIPGREP_CONFIG_PATH;
const greek = Buffer.from("Αλφα βήτα Ελληνικά κώδικας γράμματα λέξεις μία δύο τρία τέσσερα\n".repeat(50_000));
const rows = [
  { label: "repository", contents: source, files: 32, patterns: ["authoritativeMutationResources", "\\b(?:[A-Za-z_]\\w*\\.){4,}[A-Za-z_]\\w*\\b", "(?:\\p{L}+\\s+){15}\\p{L}+"] },
  { label: "unicode", contents: greek, files: 1, patterns: ["needle", "^\\w{60}$", "(?P<word>Αλφα)", "."] },
];
const median = async (run) => {
  const times = []; let output;
  for (let i = 0; i < 5; i++) { const started = performance.now(); output = await run(); times.push(performance.now() - started); }
  return { ms: times.sort((a, b) => a - b)[2], output };
};
try {
  worker = launchClosedSearchWorker(new URL("./grep-process-worker.mjs", import.meta.url));
  const { preparationMs: workerPreparationMs } = await worker.ready;
  const configuration = path.join(root, "controlled-rg-config"); await fs.writeFile(configuration, "--sort=path\n--no-ignore-global\n");
  for (const row of rows) {
    const cwd = path.join(root, row.label); await fs.mkdir(cwd);
    for (let i = 0; i < row.files; i++) await fs.writeFile(path.join(cwd, `${i}.txt`), row.contents);
    const tool = createGrepTool(cwd);
    for (const pattern of row.patterns) {
      const args = { path: ".", pattern, limit: pattern === "." ? 1 : 100, ...(pattern.startsWith("(?P") ? { context: 1 } : {}) };
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
  const resultLimitCancels = nativeCancels; assert.ok(resultLimitCancels > 0, "Pi's result-limit stop must reach the native process");
  const cancellation = {};
  for (let repeat = 0; repeat < 20; repeat++) {
    const mode = repeat % 2 ? "budget" : "abort"; cancellation[mode] = await qualifyCancellation(path.join(root, "unicode"), mode);
  }
  assert.equal(nativeClosed, nativeProcesses);
  console.log(JSON.stringify({ platform: process.platform, node: process.version, workerPreparationMs, report, cancellation,
    nativeProcesses, nativeClosed, resultLimitCancels, cancellationRepeats: 20,
    qualification: "Fixed flat fixtures only. Original Pi formatting, Runtime adoption and native cancellation are real; general path/config/ignore/encoding and engine identity qualification are still required." }, null, 2));
} finally {
  await worker?.dispose();
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
      return await worker.request({ root: privateRoot, args: request.args }, { signal: request.signal, onInput: runNative });
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

async function runNative(operation, invocation, signal, emit, onSpawn) {
  assert.equal(operation, "process");
  assert.ok(invocation.file === "rg" || invocation.file === rg, "only the previously discovered executable");
  assert.deepEqual(invocation.options, { stdio: ["ignore", "pipe", "pipe"] });
  signal.throwIfAborted();
  const child = spawn(rg, ["--no-config", "--sort=path", "--no-ignore-global", ...invocation.args],
    { stdio: ["ignore", "pipe", "pipe"], windowsHide: true, env: { ...process.env, RIPGREP_CONFIG_PATH: "" } });
  nativeProcesses++;
  return await new Promise((resolve, reject) => {
    let failure;
    const stop = () => { nativeCancels++; child.kill("SIGKILL"); };
    signal.addEventListener("abort", stop, { once: true });
    if (signal.aborted) stop();
    if (onSpawn) child.once("spawn", onSpawn);
    for (const [fd, output] of [[1, child.stdout], [2, child.stderr]]) output.on("data", (data) => emit({ fd, data }));
    child.once("error", (error) => { failure = error; });
    child.once("close", (code, exitSignal) => {
      nativeClosed++;
      signal.removeEventListener("abort", stop);
      if (failure) reject(failure); else resolve({ code, signal: exitSignal });
    });
  });
}

async function qualifyCancellation(cwd, mode) {
  const interrupted = launchClosedSearchWorker(new URL("./grep-process-worker.mjs", import.meta.url));
  const controller = new AbortController(), closed = Promise.withResolvers(), release = Promise.withResolvers();
  const before = nativeClosed; let settled = false;
  try {
    await interrupted.ready;
    const args = mode === "abort" ? { pattern: "^\\w{60}$" } : { pattern: ".", limit: Number.MAX_SAFE_INTEGER };
    const execution = interrupted.request({ root: cwd, args }, {
      signal: controller.signal, onInput: async (...args) => {
        try { return await runNative(...args, mode === "abort" ? () => controller.abort(0) : undefined); }
        finally { closed.resolve(); await release.promise; }
      },
    });
    void execution.then(() => { settled = true; }, () => { settled = true; });
    await Promise.race([closed.promise, execution]);
    assert.equal(nativeClosed, before + 1); assert.equal(settled, false, "request settlement must wait for owned input cleanup");
    release.resolve(); await assert.rejects(execution, mode === "abort" ? (reason) => reason === 0 : /input byte budget/);
    assert.ok(interrupted.closed());
    return { nativeCloseBeforeSettlement: true, borrowedCleanupBeforeSettlement: true };
  } finally { release.resolve(); await interrupted.dispose(); }
}
