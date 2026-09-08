import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn, execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";
import { createGrepTool } from "@earendil-works/pi-coding-agent";
import { createFauxCore, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "../src/effect-model.ts";
import { captureStableFile } from "../src/filesystem-evidence.ts";
import { resolveHostExecutable } from "../src/executable-path.ts";
import { relativeFilesystemPath } from "../src/path-utils.ts";
import { launchClosedSearchWorker } from "../src/closed-search-process.mjs";

// Qualification only: complete stock-Pi grep on private, token-owned inputs.
// Stock Pi runs in a bounded process; the parent owns rg, its output and completion.
const { getToolPath } = await import(new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
const { resolveToCwd } = await import(new URL("./core/tools/path-utils.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
const rg = getToolPath("rg");
if (!rg) { console.log(JSON.stringify({ qualification: "skipped", reason: "No existing Pi rg; nothing installed" })); process.exit(0); }
process.env.PI_OFFLINE = "1";
const linksOnly = process.argv.includes("--links-only"), semanticOnly = linksOnly || process.argv.includes("--semantics-only");
const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-grep-evidence-")), report = [];
let worker, actorWorker, ownedRg, engine;
let nativeProcesses = 0, nativeClosed = 0, nativeCancels = 0;
const configuredAtStart = process.env.RIPGREP_CONFIG_PATH;
const nativeFlags = ["--no-config", "--sort=path", "--no-ignore-global", "--no-ignore-parent", "--no-require-git"];
const rows = semanticOnly ? [] : [
  { label: "repository", contents: await fs.readFile(new URL("../src/runtime-engine.ts", import.meta.url)), files: 32, patterns: ["authoritativeMutationResources", "\\b(?:[A-Za-z_]\\w*\\.){4,}[A-Za-z_]\\w*\\b", "(?:\\p{L}+\\s+){15}\\p{L}+"] },
  { label: "unicode", contents: Buffer.from("Αλφα βήτα Ελληνικά κώδικας γράμματα λέξεις μία δύο τρία τέσσερα\n".repeat(50_000)), files: 1, patterns: ["needle", "^\\w{60}$", "(?P<word>Αλφα)", "."] },
];
const median = async (run) => {
  const times = []; let output;
  for (let i = 0; i < (semanticOnly ? 1 : 5); i++) { const started = performance.now(); output = await run(); times.push(performance.now() - started); }
  return { ms: times.sort((a, b) => a - b)[Math.floor(times.length / 2)], output };
};
try {
  const binary = await captureStableFile(await resolveHostExecutable(rg, "rg"), 32 * 1024 * 1024, true);
  engine = Object.freeze({ sha256: binary.hash, platform: process.platform, arch: process.arch, selectionFlags: nativeFlags, executionFlags: [...nativeFlags, "--no-ignore"] });
  ownedRg = path.join(root, process.platform === "win32" ? "rg-owned.exe" : "rg-owned");
  await fs.writeFile(ownedRg, binary.content, { flag: "wx", mode: 0o500 });
  worker = launchClosedSearchWorker(new URL("./grep-process-worker.mjs", import.meta.url));
  const { preparationMs: workerPreparationMs } = await worker.ready;
  actorWorker = launchClosedSearchWorker(new URL("./grep-process-worker.mjs", import.meta.url));
  await actorWorker.ready;
  const configuration = path.join(root, "controlled-rg-config"); await fs.writeFile(configuration, nativeFlags.slice(1).filter((flag) => flag !== "--no-ignore-parent").join("\n") + "\n");
  if (semanticOnly) {
    process.env.RIPGREP_CONFIG_PATH = configuration;
    report.push(await (linksOnly ? qualifyNativeLinks() : qualifyNamespace()));
  }
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
  const resultLimitCancels = nativeCancels;
  if (!semanticOnly) assert.ok(resultLimitCancels > 0, "Pi's result-limit stop must reach the native process");
  const cancellation = {};
  for (let repeat = 0; repeat < (linksOnly ? 0 : semanticOnly ? 1 : 20); repeat++) {
    const mode = repeat % 2 ? "budget" : "abort";
    cancellation[mode] = await qualifyCancellation(path.join(root, semanticOnly ? "namespace/search" : "unicode"), mode);
  }
  assert.equal(nativeClosed, nativeProcesses);
  console.log(JSON.stringify({ platform: process.platform, node: process.version, engine, workerPreparationMs, report, cancellation,
    nativeProcesses, nativeClosed, resultLimitCancels, cancellationRepeats: linksOnly ? 0 : semanticOnly ? 1 : 20,
    qualification: "Explicit fixed rg flags, pinned existing executable, captured inputs and stock Pi formatting. Not native-default equivalence or production admission; metadata traversal cost, unsupported filesystem entries and benefit gates remain material." }, null, 2));
} finally {
  if (configuredAtStart === undefined) delete process.env.RIPGREP_CONFIG_PATH; else process.env.RIPGREP_CONFIG_PATH = configuredAtStart;
  await worker?.dispose();
  await actorWorker?.dispose();
  assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.ok(path.basename(root).startsWith("pi-grep-evidence-"));
  await fs.rm(root, { recursive: true, force: true });
}

async function qualifyCaptured(cwd, args, expected, changed, rejected = false, stableChange = false) {
  const ready = Promise.withResolvers(), started = performance.now(), reads = new Set(), enumerated = new Set(); let executions = 0, copies = 0, actorCalls = 0;
  const execute = async (view, request) => {
    executions++;
    const privateRoot = await fs.mkdtemp(path.join(root, "retained-"));
    try {
      const relative = relativeFilesystemPath(cwd, resolveToCwd(request.args.path || ".", cwd));
      assert.notEqual(relative, undefined, "captured query must remain in its workspace");
      const query = { ...request.args, path: relative || "." };
      copies += await materializeSelected(view, cwd, privateRoot, query, request.signal, reads, enumerated);
      request.signal.throwIfAborted();
      return await worker.request({ root: privateRoot, args: query }, { signal: request.signal,
        onInput: (operation, invocation, signal, emit) => runNative(operation, { ...invocation, cwd: privateRoot }, signal, emit) });
    } finally {
      assert.equal(path.dirname(privateRoot), root); assert.ok(path.basename(privateRoot).startsWith("retained-"));
      await fs.rm(privateRoot, { recursive: true, force: true });
    }
  };
  const invocation = { executor: "captured-grep-qualification", identity: { engine, cwd },
    semantics: { ...PI_ACTION_SEMANTICS.definition("grep"), epoch: "captured-grep-qualification.v2", effect: "observation",
      requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "captured_inputs" },
    filesystem: execute, authoritative: (request) => {
      actorCalls++;
      return actorWorker.request({ root: cwd, args: request.args }, { signal: request.signal,
        onInput: (_operation, invocation, signal, emit) => runNative("reference", { ...invocation, cwd }, signal, emit) });
    } };
  const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["grep"], maxBytes: () => 8 * 1024 * 1024 });
  const tool = createGrepTool(cwd), tools = [tool], model = createFauxCore({ provider: "qualification", models: [{ id: "qualification", reasoning: false }] }).getModel();
  const host = createSpeculativeActionHost("probe", {
    cwd, getSettings: () => ({ enabled: true, drafterEnabled: true, drafterGateEnabled: false, drafterMaxDepth: 0,
      tools: ["grep"], candidateLimit: 1, maxConcurrentActions: 1, resourceCacheMaxEntries: 32, resourceCacheMaxBytes: 16 * 1024 * 1024, patternAware: { enabled: false } }),
    complete: async () => fauxAssistantMessage(fauxToolCall("grep", args), { stopReason: "toolUse" }),
    resolveInvocation: () => invocation, preflight: () => true, executionWorlds: [world],
    onEvent: (event) => {
      if (event.type === "candidate" && ["succeeded", "failed", "cancelled"].includes(event.state.status)) ready.resolve(event.state);
      if (event.type === "prediction" && event.settlement.observation === "unobserved") ready.reject(new Error(JSON.stringify(event.settlement)));
    },
  });
  const turn = { turnID: "probe", actorModel: model, actorOptions: undefined, tools, context: { systemPrompt: "qualification", messages: [], tools } };
  let callID = 0;
  const actor = () => host.execute({ turnID: turn.turnID, id: `Actor-${++callID}`, tool: "grep", args, tools }, new AbortController().signal,
    async (operation) => (await operation.invocation.authoritative({ args: operation.input, signal: operation.signal, callID: operation.callID })).result);
  try {
    await host.startTurn(turn);
    const completion = await ready.promise;
    assert.equal(completion.status, rejected ? "failed" : "succeeded", JSON.stringify(completion));
    const producerMs = performance.now() - started, materialized = copies;
    if (stableChange) { assert.deepEqual(await changed(), expected); changed = undefined; }
    if (rejected) {
      if (expected instanceof Error) await assert.rejects(actor, { message: expected.message });
      else assert.deepEqual(await actor(), expected);
      assert.equal(executions, 1); assert.equal(actorCalls, 1);
      return { producerMs, producerCalls: executions, actorCalls, reads: [...reads], enumerated: [...enumerated], rejected: true, actorError: expected instanceof Error };
    }
    const adopted = await median(async () => {
      const result = await actor(); assert.deepEqual(result, expected); return result;
    });
    assert.equal(executions, 1); assert.equal(copies, materialized); assert.equal(actorCalls, 0);
    if (changed) { const next = await changed(); assert.deepEqual(await actor(), next); assert.equal(actorCalls, 1); }
    return { producerMs, hitMs: adopted.ms, producerCalls: executions, filesMaterialized: materialized, actorCalls, reads: [...reads], enumerated: [...enumerated] };
  } finally { await host.dispose(); }
}

/** Metadata/config selects files with rg itself; ignored payloads never enter the captured view. */
async function materializeSelected(view, cwd, destination, query, signal, reads, enumerated) {
  const files = new Map(), loaded = new Set(), pending = new Map(), sourceTarget = path.resolve(cwd, query.path);
  const target = path.resolve(destination, query.path), marker = `pi-directory-${randomUUID()}`; let entries = 0;
  assert.notEqual(relativeFilesystemPath(destination, target), undefined, "search target escaped its captured namespace");
  const directory = (await view.stat(sourceTarget, "type")).isDirectory();
  const load = async (source, target) => {
    signal.throwIfAborted(); reads.add(path.relative(cwd, source));
    await fs.writeFile(target, await view.readFile(source)); loaded.add(target);
  };
  const file = async (source, target, configuration) => {
    if (!files.has(target)) {
      await fs.mkdir(path.dirname(target), { recursive: true });
      await fs.writeFile(target, "", { flag: "wx" }); files.set(target, source);
    }
    if (configuration && !loaded.has(target)) await load(source, target);
  };
  const walk = async (source, target) => {
    signal.throwIfAborted(); assert.ok(entries++ < 4096, "metadata entry budget");
    const entry = await view.stat(source, "entry"), configuration = [".gitignore", ".ignore", ".rgignore"].includes(path.basename(source)) ||
      path.relative(cwd, source).split(path.sep).join("/").endsWith(".git/info/exclude");
    assert.ok(path.basename(source) !== ".git" || entry.type === "directory", "git indirection is not qualified");
    if (entry.type === "symlink" && !configuration && relativeFilesystemPath(source, sourceTarget) === undefined) {
      // Windows search opens discovered link metadata even when --files skips it. Unproven targets
      // must reach the original Actor, never silently turn an rg traversal error into a successful hit.
      if (process.platform === "win32" && relativeFilesystemPath(sourceTarget, source) !== undefined) await view.stat(source, "type");
      return;
    }
    if (entry.type === "special") { assert.ok(!configuration && source !== sourceTarget, "special search input"); return; }
    if ((entry.type === "symlink" ? await view.stat(source, "type") : entry).isDirectory()) {
      await fs.mkdir(target, { recursive: true }); pending.set(target, source);
      if (path.basename(source) === ".git" && await view.exists(path.join(source, "info/exclude"))) {
        await file(path.join(source, "info/exclude"), path.join(target, "info/exclude"), true);
      }
    } else await file(source, target, configuration);
  };
  const expand = async (target) => {
    const source = pending.get(target); pending.delete(target);
    enumerated.add(path.relative(cwd, source).split(path.sep).join("/"));
    for (const name of await view.readdir(source)) {
      assert.equal(path.basename(name), name); assert.ok(name !== "." && name !== ".." && name !== marker);
      await walk(path.join(source, name), path.join(target, name));
    }
  };
  const select = async (target, flags = []) => {
    const output = [], diagnostic = []; let bytes = 0;
    const { code } = await runNative("selection", { file: rg, cwd: destination, args: ["--files", "--null", "--hidden", ...flags, "--", target],
      options: { stdio: ["ignore", "pipe", "pipe"] } }, signal,
      ({ fd, data }) => { assert.ok((bytes += data.length) <= 1024 * 1024, "selected name budget"); (fd === 1 ? output : diagnostic).push(data); });
    assert.ok(code === 0 || code === 1, Buffer.concat(diagnostic).toString());
    const raw = Buffer.concat(output), decoded = raw.toString("utf8");
    assert.ok(Buffer.from(decoded).equals(raw), "filename encoding must round-trip without replacement");
    return new Set(decoded.split("\0").filter(Boolean).map((selected) => {
      const resolved = path.resolve(selected);
      assert.notEqual(relativeFilesystemPath(destination, resolved), undefined, "rg selected an unowned input"); return resolved;
    }));
  };
  const selectedFiles = async (flags = []) => {
    const ordinary = await select(destination, flags), selected = query.glob ? await select(destination, ["--glob", query.glob, ...flags]) : ordinary;
    if (query.glob) for (const file of await select(target, ["--no-ignore", "--glob", query.glob, ...flags])) if (ordinary.has(file)) selected.add(file);
    return new Set([...selected].filter((file) => relativeFilesystemPath(target, file) !== undefined));
  };
  await walk(cwd, destination);
  // Open the explicit root/ancestors unconditionally; rg does not filter its supplied root.
  for (;;) {
    const ancestor = [...pending.keys()].find((parent) => relativeFilesystemPath(parent, target) !== undefined);
    if (!ancestor) break;
    await expand(ancestor);
  }
  for (const parent of pending.keys()) if (relativeFilesystemPath(target, parent) === undefined) pending.delete(parent);
  if (directory) for (let parent = path.dirname(target); target !== destination && relativeFilesystemPath(destination, parent) !== undefined; parent = path.dirname(parent)) {
    await fs.appendFile(path.join(parent, ".rgignore"), "\n!/*/\n");
    if (parent === destination) break;
  }
  while (pending.size) {
    const frontier = [...pending.keys()];
    for (const parent of frontier) await fs.writeFile(path.join(parent, marker), "", { flag: "wx" });
    // This last override admits only our synthetic files, not their parent directories. Native
    // ignore and caller-glob decisions still govern directory traversal; no debug-text parser.
    const admitted = await selectedFiles(["--glob", `**/${marker}`]);
    for (const parent of frontier) {
      await fs.unlink(path.join(parent, marker));
      if (admitted.has(path.join(parent, marker))) await expand(parent); else pending.delete(parent);
    }
  }
  const prune = async (selected) => {
    for (const file of files.keys()) if (!selected.has(file)) {
      assert.notEqual(relativeFilesystemPath(destination, file), undefined);
      await fs.unlink(file); files.delete(file);
    }
  };
  if (directory) {
    await prune(await selectedFiles());
  } else await prune(new Set([target])); // An explicit file is not filtered by directory ignore rules in native Pi.
  for (const [target, source] of files) {
    if (!loaded.has(target)) await load(source, target);
  }
  return loaded.size;
}

async function qualifyNativeLinks() {
  const checks = [];
  for (const kind of ["internal", "external", "dangling", "cycle"]) {
    const cwd = path.join(root, `link-${kind}`), search = path.join(cwd, "search"), payload = path.join(cwd, "payload");
    await fs.mkdir(cwd); await fs.mkdir(search); await fs.mkdir(payload);
    await fs.writeFile(path.join(payload, "value.txt"), "needle\n");
    await fs.writeFile(path.join(search, "value.txt"), "needle\n");
    const external = path.join(root, `outside-${kind}`); await fs.mkdir(external); await fs.writeFile(path.join(external, "value.txt"), "needle\n");
    const link = path.join(search, "link"), target = { internal: payload, external, dangling: path.join(cwd, "missing"), cycle: search }[kind];
    await fs.symlink(target, link, process.platform === "win32" ? "junction" : "dir");
    for (const explicit of [false, true]) for (const mode of ["files", "search"]) {
      const chunks = [], signal = AbortSignal.timeout(3000);
      const result = await runNative("reference", { file: rg, cwd, args: [...(mode === "files" ? ["--files", "--null"] : ["--json"]), "--hidden", "--", ...(mode === "search" ? ["needle"] : []), explicit ? link : search],
        options: { stdio: ["ignore", "pipe", "pipe"] } }, signal, ({ fd, data }) => chunks.push({ fd, data: data.toString() }));
      assert.equal(result.code, kind === "dangling" && (explicit || process.platform === "win32" && mode === "search") ? 2 : 0);
      const output = chunks.filter(({ fd }) => fd === 1).map(({ data }) => data).join("");
      const paths = mode === "files" ? output.split("\0").filter(Boolean) : output.split("\n").filter(Boolean).map((line) => JSON.parse(line)).filter((event) => event.type === "match").map((event) => event.data.path.text);
      assert.deepEqual(paths, kind === "dangling" && explicit ? [] : [path.join(explicit ? link : search, "value.txt")]);
      checks.push({ kind, explicit, mode, ...result, paths });
    }
  }
  return { mode: "native-link-characterization", checks };
}

async function qualifyNamespace() {
  const cwd = path.join(root, "namespace"); await fs.mkdir(cwd);
  for (const directory of [".git", ".git/info", "search", "search/nested", "search/empty", "search/blocked", "search/linked-config", "search/file-only", "search/file-only/deep", "rules"]) await fs.mkdir(path.join(cwd, directory));
  const fixtures = {
    ".git/HEAD": "ref: refs/heads/main\n", ".git/info/exclude": "excluded.txt\n",
    ".gitignore": "ignored.*\nsearch/blocked/\n", ".ignore": "*.tmp\n", "search/.gitignore": "*.log\n",
    "search/nested/.gitignore": "*.txt\n!keep.txt\n",
    "search/a.txt": "needle without final newline", "search/z.txt": "before\r\nneedle Z\r\nafter\r\n",
    "search/nested/keep.txt": "needle nested\n", "search/nested/drop.txt": "needle excluded by nested rule\n",
    "search/blocked/inside.txt": "needle explicit ignored directory\n",
    "search/skip.tmp": "needle excluded by dot ignore\n", "search/skip.log": "needle excluded by gitignore\n",
    "search/excluded.txt": "needle excluded by git info\n", "search/ignored.txt": "needle excluded by root\n",
    "search/中文 name.txt": "n.e and NEEDLE unicode\n", "search/é.txt": "needle composed\n", "search/e\u0301.txt": "needle decomposed\n",
    "search/utf16.txt": Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from("before\r\nneedle unicode\r\nafter\r\n", "utf16le")]),
    "search/binary.txt": Buffer.from("before\0needle binary\0after"), "search/long.txt": "needle " + "x".repeat(4096),
    "rules/shared-ignore": "hidden.txt\n", "search/linked-config/hidden.txt": "needle filtered by linked config\n",
    "search/linked-config/visible.txt": "needle visible beside linked config\n",
    "search/file-only/.ignore": "*\n!*/\n", "search/file-only/deep/.ignore": "!value.txt\n",
    "search/file-only/deep/value.txt": "needle reintroduced below excluded files\n",
  };
  for (const [name, contents] of Object.entries(fixtures)) await fs.writeFile(path.join(cwd, name), contents);
  const outside = path.join(root, "outside-namespace"); await fs.mkdir(outside);
  await fs.writeFile(path.join(outside, "external.txt"), "needle outside captured namespace\n");
  const directoryLink = process.platform === "win32" ? "junction" : "dir";
  for (const [name, target] of [["internal-link", path.join(cwd, "search/nested")], ["external-link", outside],
    ["dangling-link", path.join(cwd, "missing")], ["cycle-link", path.join(cwd, "search")]]) {
    const parent = process.platform === "win32" && ["external-link", "dangling-link"].includes(name) ? path.join(cwd, `edge-${name}`) : path.join(cwd, "search");
    await fs.mkdir(parent, { recursive: true }); await fs.symlink(target, path.join(parent, name), directoryLink);
  }
  if (process.platform !== "win32") {
    await fs.symlink(path.join(cwd, "search/a.txt"), path.join(cwd, "search/file-link"));
    await fs.symlink(path.join(cwd, "rules/shared-ignore"), path.join(cwd, "search/linked-config/.ignore"));
  } else await fs.writeFile(path.join(cwd, "search/linked-config/.ignore"), fixtures["rules/shared-ignore"]);
  if (process.platform === "linux") await promisify(execFile)("mkfifo", [path.join(cwd, "search/input.pipe")]);
  const large = await fs.open(path.join(cwd, "search/ignored.bin"), "wx");
  try { await large.truncate(16 * 1024 * 1024); } finally { await large.close(); }
  const checks = [];
  for (const [label, args, rejected] of [
    ["directory-ignore", { path: "search", pattern: "needle", limit: 1000 }],
    ["nested-search", { path: "search/nested", pattern: "needle" }],
    ["glob", { path: "search", pattern: "needle", glob: "**/{a,z}.txt" }],
    ["glob-override", { path: "search", pattern: "needle", glob: "*.tmp" }],
    ["glob-relative", { path: "search", pattern: "needle", glob: "search/nested/keep.txt" }],
    ["glob-anchored", { path: "search", pattern: "needle", glob: "/search/a.txt" }],
    ["glob-negative", { path: "search", pattern: "needle", glob: "!*.txt" }],
    ["glob-root-negative", { path: "search", pattern: "needle", glob: "!search/" }],
    ["glob-positive-directory", { path: "search", pattern: "needle", glob: "**/blocked{,/**}" }],
    ["glob-negative-directory", { path: "search", pattern: "needle", glob: "!**/nested/" }],
    ["mixed-encoding-context", { path: "search", pattern: "(?P<word>needle)", context: 1 }],
    ["literal-case", { path: "search", pattern: "n.e", literal: true, ignoreCase: true }],
    ["negative-query", { path: "search", pattern: "not-present-anywhere" }],
    ["limit", { path: "search", pattern: "needle", limit: 1 }],
    ["file", { path: "search/utf16.txt", pattern: "needle", context: 1 }],
    ["explicit-ignored-directory", { path: "search/blocked", pattern: "needle" }],
    ["ignored-subtree-change", { path: "search", pattern: "needle" }],
    ["explicit-directory-link", { path: "search/internal-link", pattern: "needle" }],
    ["explicit-external-link", { path: process.platform === "win32" ? "edge-external-link/external-link" : "search/external-link", pattern: "needle" }, true],
    ["explicit-dangling-link", { path: process.platform === "win32" ? "edge-dangling-link/dangling-link" : "search/dangling-link", pattern: "needle" }, true],
    ...(process.platform === "win32" ? [
      ["discovered-external-link", { path: "edge-external-link", pattern: "needle" }, true],
      ["discovered-dangling-link", { path: "edge-dangling-link", pattern: "needle" }, true],
    ] : []),
    ...(process.platform === "win32" ? [] : [
      ["explicit-file-link", { path: "search/file-link", pattern: "needle" }],
      ["linked-ignore-file", { path: "search/linked-config", pattern: "needle" }],
    ]),
  ]) {
    const only = process.argv.find((arg) => arg.startsWith("--case="))?.slice(7);
    if (only && label !== only) continue;
    const reference = async () => (await actorWorker.request({ root: cwd, args }, {
      signal: new AbortController().signal,
      onInput: (_operation, invocation, signal, emit) => runNative("reference", { ...invocation, cwd }, signal, emit),
    })).result;
    const expected = await reference().catch((error) => { if (!rejected) throw error; return error; });
    if (label === "limit") assert.equal(expected.details?.matchLimitReached, 1);
    const mutation = {
      "directory-ignore": ["search/z.txt", "needle changed after sealing\n"],
      "nested-search": ["search/nested/.gitignore", "*.txt\n!drop.txt\n"],
      "negative-query": ["search/arrived.txt", "not-present-anywhere\n"],
      "linked-ignore-file": ["rules/shared-ignore", "visible.txt\n"],
      "ignored-subtree-change": ["search/blocked/arrived.txt", "needle arrived inside ignored tree\n"],
    }[label];
    const changed = mutation ? async () => { await fs.writeFile(path.join(cwd, mutation[0]), mutation[1]); return reference(); } : undefined;
    const captured = await qualifyCaptured(cwd, args, expected, changed, rejected, label === "ignored-subtree-change");
    assert.ok(!captured.reads.some((file) => file.endsWith("ignored.bin")), "ignored payload must not consume the input budget");
    if (!["explicit-ignored-directory", "glob-positive-directory"].includes(label)) assert.ok(!captured.enumerated.includes("search/blocked"), "ignored directory must not consume the enumeration budget");
    if (label === "glob-negative-directory") assert.ok(!captured.enumerated.includes("search/nested"), "negative glob directory must not consume the enumeration budget");
    assert.ok(!captured.enumerated.includes(".git"), "repository metadata uses its named configuration, not a recursive walk");
    checks.push({ label, outputBytes: expected instanceof Error ? 0 : Buffer.byteLength(JSON.stringify(expected)), ...captured });
    console.log(JSON.stringify({ semanticCase: checks.at(-1) }));
  }
  return { mode: "small-semantic-fixture", checks, qualification: "Full host key/route/transaction/adoption; content, ignore rules and negative names each invalidate before one Actor fallback. Timings are not performance claims." };
}

async function runNative(operation, invocation, signal, emit, onSpawn) {
  assert.ok(operation === "process" || operation === "selection" || operation === "reference");
  assert.ok(invocation.file === "rg" || invocation.file === rg, "only the previously discovered executable");
  assert.deepEqual(invocation.options, { stdio: ["ignore", "pipe", "pipe"] });
  signal.throwIfAborted();
  const flags = operation === "reference" ? nativeFlags.filter((flag) => flag !== "--no-ignore-parent") : nativeFlags;
  const child = spawn(ownedRg, [...flags, ...(operation === "process" ? ["--no-ignore"] : []), ...invocation.args],
    { stdio: ["ignore", "pipe", "pipe"], windowsHide: true, cwd: invocation.cwd ?? root,
      env: { HOME: root, LC_ALL: "C", ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}) } });
  nativeProcesses++;
  return await new Promise((resolve, reject) => {
    let failure;
    const stop = () => { nativeCancels++; child.kill("SIGKILL"); };
    signal.addEventListener("abort", stop, { once: true });
    if (signal.aborted) stop();
    if (onSpawn) child.once("spawn", onSpawn);
    for (const [fd, output] of [[1, child.stdout], [2, child.stderr]]) output.on("data", (data) => {
      if (!failure) try { emit({ fd, data }); } catch (error) { failure = error; stop(); }
    });
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
