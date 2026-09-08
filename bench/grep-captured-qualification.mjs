import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { createGrepTool } from "@earendil-works/pi-coding-agent";
import { createFauxCore, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { createClosedSearchProfile, runCapturedSearchProcess } from "../src/pi-tool-invocation.ts";
import { captureStableFile } from "../src/filesystem-evidence.ts";
import { resolveHostExecutable } from "../src/executable-path.ts";
import { relativeFilesystemPath } from "../src/path-utils.ts";
import { CLOSED_SEARCH_PROFILE } from "../src/closed-search-kernel.mjs";
import { ClosedSearchProcessPool, launchClosedSearchWorker } from "../src/closed-search-process.mjs";

// Qualification only: complete stock-Pi grep on private, token-owned inputs.
// Stock Pi runs in a bounded process; the parent owns rg, its output and completion.
const { getToolPath } = await import(new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
const pathRules = Object.freeze({ homeDir: os.homedir(), normalizeUnicodeSpaces: true, stripAtPrefix: true });
const nativeEnvironment = Object.freeze({ HOME: pathRules.homeDir, LC_ALL: "C", ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}) });
const rg = getToolPath("rg");
if (!rg) { console.log(JSON.stringify({ qualification: "skipped", reason: "No existing Pi rg; nothing installed" })); process.exit(0); }
process.env.PI_OFFLINE = "1";
const linksOnly = process.argv.includes("--links-only"), semanticOnly = linksOnly || process.argv.includes("--semantics-only");
const costOnly = process.argv.includes("--cost-only");
assert.ok(!costOnly || !semanticOnly, "choose either semantic or cost qualification");
const selectedCases = new Set(process.argv.find((arg) => arg.startsWith("--case="))?.slice(7).split(",") ?? []);
const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-grep-evidence-")), report = [];
let pool, ownedRg, engine;
let referenceProcesses = 0, referenceClosed = 0, referenceCancels = 0;
const configuredAtStart = process.env.RIPGREP_CONFIG_PATH;
const nativeFlags = ["--no-config", "--sort=path", "--no-ignore-global", "--no-ignore-parent"];
const rows = semanticOnly ? [] : [
  { label: "repository", contents: await fs.readFile(new URL("../src/runtime-engine.ts", import.meta.url)), files: costOnly ? 8 : 32, patterns: ["authoritativeMutationResources", "\\b(?:[A-Za-z_]\\w*\\.){4,}[A-Za-z_]\\w*\\b", "(?:\\p{L}+\\s+){15}\\p{L}+"] },
  { label: "unicode", contents: Buffer.from("Αλφα βήτα Ελληνικά κώδικας γράμματα λέξεις μία δύο τρία τέσσερα\n".repeat(costOnly ? 7_000 : 50_000)), files: 1, patterns: ["needle", "^\\w{60}$", "(?P<word>Αλφα)", "."] },
];
const median = async (run) => {
  const times = []; let output;
  for (let i = 0; i < (semanticOnly ? 1 : costOnly ? 3 : 5); i++) { const started = performance.now(); output = await run(); times.push(performance.now() - started); }
  return { ms: times.sort((a, b) => a - b)[Math.floor(times.length / 2)], output };
};
try {
  const binary = await captureStableFile(await resolveHostExecutable(rg, "rg"), 32 * 1024 * 1024, true);
  engine = Object.freeze({ profile: CLOSED_SEARCH_PROFILE, sha256: binary.hash, platform: process.platform, arch: process.arch, pathRules, environment: nativeEnvironment, selectionFlags: nativeFlags, executionFlags: [...nativeFlags, "--no-ignore"] });
  ownedRg = path.join(root, process.platform === "win32" ? "rg-owned.exe" : "rg-owned");
  await fs.writeFile(ownedRg, binary.content, { flag: "wx", mode: 0o500 });
  pool = new ClosedSearchProcessPool();
  const { preparationMs: workerPreparationMs } = await pool.run("actor", (worker) => worker.ready);
  const configuration = path.join(root, "controlled-rg-config"); await fs.writeFile(configuration, nativeFlags.slice(1).filter((flag) => flag !== "--no-ignore-parent").join("\n") + "\n");
  if (semanticOnly) {
    process.env.RIPGREP_CONFIG_PATH = configuration;
    report.push(await (linksOnly ? qualifyNativeLinks() : qualifyNamespace()));
  }
  for (const row of rows) {
    if (selectedCases.size && !selectedCases.has(row.label)) continue;
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
  if (!semanticOnly) for (const label of selectedCases) assert.ok(report.some((row) => row.fixture === label), `unknown cost fixture: ${label}`);
  const cancellation = {};
  if (!linksOnly) cancellation.limit = await qualifyCancellation(path.join(root, semanticOnly ? "namespace/search" : costOnly ? report[0].fixture : "unicode"), "limit");
  const cancellationRepeats = linksOnly ? 0 : semanticOnly || costOnly ? 1 : 20;
  for (let repeat = 0; repeat < cancellationRepeats; repeat++) {
    const mode = repeat % 2 ? "budget" : "abort";
    cancellation[mode] = await qualifyCancellation(path.join(root, semanticOnly ? "namespace/search" : costOnly ? report[0].fixture : "unicode"), mode);
  }
  assert.equal(referenceClosed, referenceProcesses);
  console.log(JSON.stringify({ platform: process.platform, node: process.version, engine, workerPreparationMs, report, cancellation,
    referenceProcesses, referenceClosed, referenceCancels, cancellationRepeats,
    qualification: "Production captured-search profile through Host admission, with explicit fixed rg flags, pinned existing executable, captured inputs and stock Pi formatting. Counters cover instrumented reference workers and cancellation probes only; ordinary native Pi baselines and production-profile processes are not counted. Cost mode primes the same Host with original Actor service before measured admission; fallback is a valid outcome, not a hit. Not native-default equivalence." }, null, 2));
} finally {
  if (configuredAtStart === undefined) delete process.env.RIPGREP_CONFIG_PATH; else process.env.RIPGREP_CONFIG_PATH = configuredAtStart;
  await pool?.dispose();
  assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.ok(path.basename(root).startsWith("pi-grep-evidence-"));
  await fs.rm(root, { recursive: true, force: true });
}

async function qualifyCaptured(cwd, args, expected, changed, rejected = false, stableChange = false) {
  const ready = Promise.withResolvers(), reads = new Set(), enumerated = new Set(), trials = [];
  let started, executions = 0, actorCalls = 0, drafterEnabled = !costOnly, settled;
  const preparation = performance.now(), profile = await createClosedSearchProfile(cwd), profilePreparationMs = performance.now() - preparation;
  const bound = profile.invocations.get("grep");
  if (!bound) { await profile.pool.dispose(); throw new Error("existing rg is not qualified by the production profile"); }
  const invocation = { ...bound, filesystem: (view, request) => {
    executions++;
    return bound.filesystem({ ...view,
      readFile: (target, ...options) => { reads.add(path.relative(cwd, target)); return view.readFile(target, ...options); },
      readdir: (target) => { enumerated.add(path.relative(cwd, target).split(path.sep).join("/")); return view.readdir(target); },
    }, request);
  }, authoritative: (request) => { actorCalls++; return bound.authoritative(request); } };
  const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["grep"], maxBytes: () => 8 * 1024 * 1024 });
  const tool = createGrepTool(cwd), tools = [tool], model = createFauxCore({ provider: "qualification", models: [{ id: "qualification", reasoning: false }] }).getModel();
  const host = createSpeculativeActionHost("probe", {
    cwd, getSettings: () => ({ enabled: true, drafterEnabled, drafterGateEnabled: false, drafterMaxDepth: 0,
      tools: ["grep"], candidateLimit: 1, maxConcurrentActions: 1, resourceCacheMaxEntries: 32, resourceCacheMaxBytes: 16 * 1024 * 1024, patternAware: { enabled: false } }),
    complete: async () => fauxAssistantMessage(fauxToolCall("grep", args), { stopReason: "toolUse" }),
    resolveInvocation: () => invocation, preflight: () => true, executionWorlds: [world],
    onActorActionSettled: ({ settlement }) => settled?.resolve(settlement),
    onEvent: (event) => {
      if (event.type === "candidate" && ["succeeded", "failed", "cancelled"].includes(event.state.status)) ready.resolve(event.state);
      if (event.type === "prediction" && event.settlement.observation === "unobserved") ready.resolve(event.settlement);
    },
  });
  const turn = { turnID: "probe", actorModel: model, actorOptions: undefined, tools, context: { systemPrompt: "qualification", messages: [], tools } };
  let callID = 0;
  const actor = async () => {
    settled = Promise.withResolvers();
    const before = actorCalls, started = performance.now();
    const result = await host.execute({ turnID: turn.turnID, id: `Actor-${++callID}`, tool: "grep", args, tools }, new AbortController().signal,
      async (operation) => (await operation.invocation.authoritative({ args: operation.input, signal: operation.signal, callID: operation.callID })).result);
    if (costOnly) {
      const ms = performance.now() - started, { provider, rejections } = await settled.promise;
      assert.equal(actorCalls - before, provider.kind === "actor" ? 1 : 0, "each fallback executes the original Actor exactly once");
      trials.push({ ms, provider, rejections });
    }
    return result;
  };
  try {
    let hostActorMs;
    if (costOnly) {
      turn.turnID = "baseline"; await host.startTurn(turn);
      hostActorMs = (await median(async () => { const result = await actor(); assert.deepEqual(result, expected); return result; })).ms;
      assert.equal(executions, 0); assert.equal(actorCalls, 3);
      await host.finishTurn(turn.turnID);
      actorCalls = 0; trials.length = 0; drafterEnabled = true; turn.turnID = "probe";
    }
    started = performance.now();
    await host.startTurn(turn);
    const completion = await ready.promise;
    assert.equal(completion.observation === "unobserved" ? completion.cause.code : completion.status,
      rejected === "unkeyable" ? "action_not_keyable" : rejected ? "failed" : "succeeded", JSON.stringify(completion));
    const producerMs = performance.now() - started, materialized = reads.size;
    if (stableChange) { assert.deepEqual(await changed(), expected); changed = undefined; }
    if (rejected) {
      if (expected instanceof Error) await assert.rejects(actor, { message: expected.message });
      else assert.deepEqual(await actor(), expected);
      assert.equal(executions, rejected === "unkeyable" ? 0 : 1); assert.equal(actorCalls, 1);
      return { profilePreparationMs, producerMs, producerCalls: executions, actorCalls, reads: [...reads], enumerated: [...enumerated], rejected, actorError: expected instanceof Error };
    }
    const adopted = await median(async () => {
      const result = await actor(); assert.deepEqual(result, expected); return result;
    });
    assert.equal(executions, 1); assert.equal(reads.size, materialized);
    if (costOnly) for (const trial of trials) {
      if (trial.provider.kind === "actor") assert.ok(trial.rejections.some(({ cause }) => cause.code === "candidate_join_not_profitable"), JSON.stringify(trial));
    } else assert.equal(actorCalls, 0);
    if (changed) { const next = await changed(); assert.deepEqual(await actor(), next); assert.equal(actorCalls, 1); }
    return { profilePreparationMs, producerMs, ...(costOnly ? { hostActorMs, probeMs: adopted.ms, trials } : { hitMs: adopted.ms }),
      producerCalls: executions, inputFilesRead: materialized, actorCalls, reads: [...reads], enumerated: [...enumerated] };
  } finally { try { await host.dispose(); } finally { await profile.pool.dispose(); } }
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
      const result = await runInput("reference", { file: rg, cwd, args: [...(mode === "files" ? ["--files", "--null"] : ["--json"]), "--hidden", "--", ...(mode === "search" ? ["needle"] : []), explicit ? link : search],
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
  for (const directory of [".git", ".git/info", "search", "search/nested", "search/empty", "search/blocked", "search/linked-config", "search/file-only", "search/file-only/deep", "search/syntax", "search/syntax/literal [x]", "search/syntax/!bang#", "rules"]) await fs.mkdir(path.join(cwd, directory));
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
    "search/encoded\u00a0name.txt": "needle encoded nonbreaking name\n",
    "search/utf16.txt": Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from("before\r\nneedle unicode\r\nafter\r\n", "utf16le")]),
    "search/binary.txt": Buffer.from("before\0needle binary\0after"), "search/long.txt": "needle " + "x".repeat(4096),
    "rules/shared-ignore": "hidden.txt\n", "search/linked-config/hidden.txt": "needle filtered by linked config\n",
    "search/linked-config/visible.txt": "needle visible beside linked config\n",
    "search/file-only/.ignore": "*\n!*/\n", "search/file-only/deep/.ignore": "!value.txt\n",
    "search/file-only/deep/value.txt": "needle reintroduced below excluded files\n",
    "search/syntax/.ignore": "*\n", "search/syntax/literal [x]/value.txt": "needle literal directory\n", "search/syntax/!bang#/value.txt": "needle punctuation directory\n",
  };
  for (const [name, contents] of Object.entries(fixtures)) await fs.writeFile(path.join(cwd, name), contents);
  const outside = path.join(root, "outside-namespace"); await fs.mkdir(outside);
  await fs.writeFile(path.join(outside, "external.txt"), "needle outside captured namespace\n");
  const directoryLink = process.platform === "win32" ? "junction" : "dir";
  const cwdAlias = path.join(root, "cwd-alias"); await fs.symlink(cwd, cwdAlias, directoryLink);
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
  const configured = path.join(root, "configured-parent/workspace"), configurationCases = [];
  await fs.mkdir(path.join(configured, "search"), { recursive: true });
  await fs.mkdir(path.join(configured, "../.git"));
  for (const [name, contents] of Object.entries({
    "../.gitignore": "/workspace/search/by-git.txt\n", "../.ignore": "/workspace/search/by-ignore.txt\n", "../.rgignore": "/workspace/search/by-rg.txt\n",
    "search/by-git.txt": "needle parent git\n", "search/by-ignore.txt": "needle parent ignore\n", "search/by-rg.txt": "needle parent rg\n", "search/visible.txt": "needle visible\n",
  })) await fs.writeFile(path.join(configured, name), contents);
  await fs.symlink(path.join(configured, "search"), path.join(configured, "alias"), directoryLink);
  for (const mode of ["absolute", "dot-relative", "bare-relative", "missing-common"]) {
    const workspace = path.join(root, `git-${mode}`), gitdir = path.join(root, `metadata-${mode}`, "worktree");
    const common = mode === "bare-relative" ? path.join(workspace, "local-common") : path.join(gitdir, "../common");
    for (const directory of [path.join(workspace, "search"), gitdir, path.join(common, "info")]) await fs.mkdir(directory, { recursive: true });
    await fs.writeFile(path.join(workspace, ".git"), "gitdir: " + (mode === "absolute" ? gitdir : path.relative(workspace, gitdir)) + "\r\nsecond line ignored\n");
    if (mode !== "missing-common") await fs.writeFile(path.join(gitdir, "commondir"),
      (mode === "absolute" ? common : mode === "dot-relative" ? "../common" : "local-common") + "\r\nsecond line ignored\n");
    await fs.writeFile(path.join(common, "info/exclude"), "/search/excluded.txt\n");
    for (const name of ["visible", "excluded"]) await fs.writeFile(path.join(workspace, `search/${name}.txt`), `needle ${name}\n`);
    configurationCases.push([`git-${mode}`, { path: "search", pattern: "needle" }, false, workspace,
      mode === "missing-common" ? [path.join(gitdir, "commondir"), common + "\n"] : [path.join(common, "info/exclude"), "/search/visible.txt\n"]]);
  }
  for (const mode of ["directory", "file"]) {
    const workspace = path.join(root, `jj-${mode}`); await fs.mkdir(path.join(workspace, "search"), { recursive: true });
    if (mode === "directory") await fs.mkdir(path.join(workspace, ".jj")); else await fs.writeFile(path.join(workspace, ".jj"), "repository marker\n");
    for (const name of ["visible", "excluded"]) await fs.writeFile(path.join(workspace, `search/${name}.txt`), `needle ${name}\n`);
    await fs.writeFile(path.join(workspace, ".gitignore"), "/search/excluded.txt\n");
    configurationCases.push([`jj-${mode}`, { path: "search", pattern: "needle" }, false, workspace, [".gitignore", "/search/visible.txt\n"]]);
  }
  const checks = [];
  for (const [label, args, rejected, workspace = cwd, configurationMutation] of [
    ["directory-ignore", { path: "search", pattern: "needle", limit: 1000 }],
    ["path-at-prefix", { path: "@search", pattern: "needle" }],
    ["path-absolute", { path: path.join(cwd, "search"), pattern: "needle" }],
    ["path-file-url", { path: pathToFileURL(path.join(cwd, "search")).href, pattern: "needle" }],
    ["path-encoded-name", { path: pathToFileURL(path.join(cwd, "search/encoded\u00a0name.txt")).href, pattern: "needle", context: 1 }],
    ["path-home", { path: "~/" + path.relative(pathRules.homeDir, path.join(cwd, "search")).split(path.sep).join("/"), pattern: "needle", glob: "search/a.txt" }],
    ["path-external", { path: outside, pattern: "needle" }, "unkeyable"],
    ["nested-search", { path: "search/nested", pattern: "needle" }],
    ["glob", { path: "search", pattern: "needle", glob: "**/{a,z}.txt" }],
    ["glob-override", { path: "search", pattern: "needle", glob: "*.tmp" }],
    ["glob-relative", { path: "search", pattern: "needle", glob: "search/nested/keep.txt" }],
    ["glob-anchored", { path: "search", pattern: "needle", glob: "/search/a.txt" }],
    ["glob-negative", { path: "search", pattern: "needle", glob: "!*.txt" }],
    ["glob-root-negative", { path: "search", pattern: "needle", glob: "!search/" }],
    ["glob-positive-directory", { path: "search", pattern: "needle", glob: "**/blocked{,/**}" }],
    ["glob-negative-directory", { path: "search", pattern: "needle", glob: "!**/nested/" }],
    ["glob-literal-directory", { path: "search/syntax", pattern: "needle", glob: "**/literal*{,/**}", context: 1 }],
    ["glob-config-data", { path: "search/syntax", pattern: ".", glob: "**" }],
    ["mixed-encoding-context", { path: "search", pattern: "(?P<word>needle)", context: 1 }],
    ["literal-case", { path: "search", pattern: "n.e", literal: true, ignoreCase: true }],
    ["negative-query", { path: "search", pattern: "not-present-anywhere" }],
    ["limit", { path: "search", pattern: "needle", limit: 1 }],
    ["file", { path: "search/utf16.txt", pattern: "needle", context: 1 }],
    ["explicit-ignored-directory", { path: "search/blocked", pattern: "needle" }],
    ["ignored-subtree-change", { path: "search", pattern: "needle" }],
    ["explicit-directory-link", { path: "search/internal-link", pattern: "needle" }],
    ["explicit-external-link", { path: process.platform === "win32" ? "edge-external-link/external-link" : "search/external-link", pattern: "needle" }],
    ["link-glob", { path: "search/internal-link", pattern: "needle", glob: "*.txt" }],
    ["link-glob-relative", { path: "search/internal-link", pattern: "needle", glob: "search/internal-link/keep.txt" }],
    ["link-glob-physical-name", { path: "search/internal-link", pattern: "needle", glob: "search/nested/keep.txt" }],
    ["link-glob-directory", { path: "search/cycle-link", pattern: "needle", glob: "**/blocked{,/**}" }],
    ["link-glob-negative-directory", { path: "search/cycle-link", pattern: "needle", glob: "!**/nested/" }],
    ["unproven-cwd-glob", { path: "search", pattern: "needle", glob: "search/a.txt" }, true, cwdAlias],
    ["explicit-dangling-link", { path: process.platform === "win32" ? "edge-dangling-link/dangling-link" : "search/dangling-link", pattern: "needle" }, true],
    ...(process.platform === "win32" ? [
      ["discovered-external-link", { path: "edge-external-link", pattern: "needle" }],
      ["discovered-dangling-link", { path: "edge-dangling-link", pattern: "needle" }, true],
    ] : []),
    ...(process.platform === "win32" ? [] : [
      ["explicit-file-link", { path: "search/file-link", pattern: "needle" }],
      ["linked-ignore-file", { path: "search/linked-config", pattern: "needle" }],
    ]),
    ["parent-config-alias", { path: "alias", pattern: "needle" }, false, configured],
    ["parent-config", { path: "search", pattern: "needle" }, false, configured, ["../.ignore", "/workspace/search/visible.txt\n"]],
    ...configurationCases,
    ["git-pointer-data", { path: ".", pattern: "gitdir: ", glob: ".git" }, false, path.join(root, "git-absolute")],
    ["explicit-git-directory", { path: ".git", pattern: "ref:" }],
  ]) {
    if (selectedCases.size && !selectedCases.has(label)) continue;
    const reference = async () => (await pool.run("actor", (worker, signal) => worker.request({ kind: "grep", root: workspace, args, home: pathRules.homeDir }, { signal,
      onInput: (operation, invocation, signal, emit) => runInput(operation === "process" ? "reference" : operation, invocation, signal, emit, { cwd: workspace }),
    }))).result;
    const expected = await reference().catch((error) => { if (!rejected) throw error; return error; });
    if (label === "limit") assert.equal(expected.details?.matchLimitReached, 1);
    if (label.startsWith("parent-config") || configurationCases.some(([name]) => name === label)) {
      assert.match(expected.content[0].text, /needle visible/);
      // Existing rg versions differ on .jj recognition; the full result must match that pinned native engine.
      if (!label.startsWith("jj-")) assert.equal(expected.content[0].text.includes("needle excluded"), label === "git-missing-common", JSON.stringify({ label, expected }));
      assert.ok(!expected.content[0].text.includes("needle parent"), "native ancestor rules must actually filter fixture files");
    }
    const mutation = configurationMutation ?? {
      "directory-ignore": ["search/z.txt", "needle changed after sealing\n"],
      "nested-search": ["search/nested/.gitignore", "*.txt\n!drop.txt\n"],
      "negative-query": ["search/arrived.txt", "not-present-anywhere\n"],
      "linked-ignore-file": ["rules/shared-ignore", "visible.txt\n"],
      "ignored-subtree-change": ["search/blocked/arrived.txt", "needle arrived inside ignored tree\n"],
    }[label];
    const changed = mutation ? async () => { await fs.writeFile(path.resolve(workspace, mutation[0]), mutation[1]); return reference(); } : undefined;
    const captured = await qualifyCaptured(workspace, args, expected, changed, rejected, label === "ignored-subtree-change");
    assert.ok(!captured.reads.some((file) => file.endsWith("ignored.bin")), "ignored payload must not consume the input budget");
    if (!["explicit-ignored-directory", "glob-positive-directory", "link-glob-directory"].includes(label)) assert.ok(!captured.enumerated.includes("search/blocked"), "ignored directory must not consume the enumeration budget");
    if (label.endsWith("glob-negative-directory")) assert.ok(!captured.enumerated.includes("search/nested"), "negative glob directory must not consume the enumeration budget");
    if (label !== "explicit-git-directory") assert.ok(!captured.enumerated.includes(".git"), "repository metadata uses its named configuration, not a recursive walk");
    checks.push({ label, outputBytes: expected instanceof Error ? 0 : Buffer.byteLength(JSON.stringify(expected)), ...captured });
    console.log(JSON.stringify({ semanticCase: checks.at(-1) }));
  }
  for (const label of selectedCases) assert.ok(checks.some((check) => check.label === label), `unknown/unavailable semantic case: ${label}`);
  return { mode: "small-semantic-fixture", checks, qualification: "Full host key/route/transaction/adoption; content, ignore rules and negative names each invalidate before one Actor fallback. Timings are not performance claims." };
}

async function runInput(operation, invocation, signal, emit, { cwd, inputRoot, onSpawn } = {}) {
  signal.throwIfAborted();
  if (operation === "stat" || operation === "readFile") {
    assert.equal(typeof invocation, "string");
    if (inputRoot) assert.notEqual(relativeFilesystemPath(inputRoot, invocation), undefined, "worker input escaped its owned tree");
    return operation === "stat" ? { directory: (await fs.stat(invocation)).isDirectory() } : fs.readFile(invocation, { signal });
  }
  assert.ok(operation === "process" || operation === "selection" || operation === "reference");
  assert.ok(invocation.file === "rg" || invocation.file === rg, "only the previously discovered executable");
  assert.deepEqual(invocation.options, { stdio: ["ignore", "pipe", "pipe"] });
  signal.throwIfAborted();
  const flags = operation === "reference" ? nativeFlags.filter((flag) => flag !== "--no-ignore-parent") : nativeFlags;
  referenceProcesses++;
  const stopped = () => { referenceCancels++; }; signal.addEventListener("abort", stopped, { once: true });
  try { return await runCapturedSearchProcess(ownedRg, [...flags, ...(operation === "process" ? ["--no-ignore"] : []), ...invocation.args],
    cwd ?? invocation.cwd ?? root, nativeEnvironment, signal, emit, onSpawn); }
  finally { referenceClosed++; signal.removeEventListener("abort", stopped); }
}

async function qualifyCancellation(cwd, mode) {
  const interrupted = launchClosedSearchWorker();
  const controller = new AbortController(), closed = Promise.withResolvers(), release = Promise.withResolvers();
  const before = referenceClosed, limitCancellation = Promise.withResolvers(); let settled = false;
  try {
    await interrupted.ready;
    const args = mode === "abort" ? { pattern: "^\\w{60}$" } : { pattern: ".", limit: mode === "limit" ? 1 : Number.MAX_SAFE_INTEGER };
    const execution = interrupted.request({ kind: "grep", root: cwd, args, home: pathRules.homeDir }, {
      signal: controller.signal, onInput: async (operation, invocation, signal, emit) => {
        if (operation !== "process") return runInput(operation, invocation, signal, emit, { cwd });
        if (mode === "limit") signal.addEventListener("abort", () => limitCancellation.resolve(), { once: true });
        try {
          const result = await runInput(operation, invocation, signal, emit, { cwd, onSpawn: mode === "abort" ? () => controller.abort(0) : undefined });
          if (mode === "limit") await limitCancellation.promise; // Keep the borrowed operation alive even if this tiny rg already closed.
          return result;
        }
        finally { closed.resolve(); await release.promise; }
      },
    });
    void execution.then(() => { settled = true; }, () => { settled = true; });
    await Promise.race([closed.promise, execution]);
    assert.equal(referenceClosed, before + 1); assert.equal(settled, false, "request settlement must wait for owned input cleanup");
    release.resolve();
    if (mode === "limit") assert.equal((await execution).result.details.matchLimitReached, 1);
    else { await assert.rejects(execution, mode === "abort" ? (reason) => reason === 0 : /input byte budget/); assert.ok(interrupted.closed()); }
    return { nativeCloseBeforeSettlement: true, borrowedCleanupBeforeSettlement: true, ...(mode === "limit" ? { resultLimitCancellationReceived: true } : {}) };
  } finally { limitCancellation.resolve(); release.resolve(); await interrupted.dispose(); }
}
