import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import { createClosedSearchProfile, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { bindResourceOperation, createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { ResourceVersionManager, type ResourceVersionToken, type ToolFilesystemOperations } from "../src/resource-version.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import type { Sha256Digest } from "../src/stable-value-hash.ts";

const [mode, root, cache, saved] = process.argv.slice(2) as [string, string, string, Sha256Digest];
const producing = mode === "produce" || mode === "composed-produce";
if (mode === "environment") process.env.SystemRoot = "pi-alternate-search-environment";
const store = new ProvenanceCertificateStore(cache, { orphanGraceMs: 0 });
const manager = new ResourceVersionManager(root, { watch: false, persistentInputs: { store } });
const profile = await createClosedSearchProfile(root), invocation = profile.invocations.get("grep");
let token: ResourceVersionToken | undefined, matches = 0, materializations = 0, nativeMatches = 0;
const recovered: ResourceVersionToken[] = [];
let selected = saved;
const spawn = childProcess.spawn;
childProcess.spawn = ((...args: Parameters<typeof spawn>) => {
  if (Array.isArray(args[1]) && args[1].includes("--json")) nativeMatches++;
  return Reflect.apply(spawn, childProcess, args);
}) as typeof spawn;
syncBuiltinESMExports();
try {
  assert.ok(invocation, "qualified native grep is required for this contract");
  const file = path.join(root, "input"), controller = new AbortController();
  let query = { pattern: "two", path: file, limit: 20, ignoreCase: false, literal: false };
  const entered = Promise.withResolvers<void>(), resume = Promise.withResolvers<void>();
  const executeView = (inputs: ToolFilesystemOperations) => invocation.filesystem!({ ...inputs,
    prepare: (binding, key, build, consume, target, signal) => inputs.prepare!(binding, key, async (inputs, signal) => {
      if (target === file && JSON.parse(key).length === 4) matches++; else materializations++;
      return build(inputs, signal);
    }, async value => {
      if (mode === "release" && target === file && JSON.parse(key).length === 4) { entered.resolve(); await resume.promise; }
      return consume(value);
    }, target, signal),
  } satisfies ToolFilesystemOperations, { args: query, callID: "cold-search", signal: controller.signal });
  if (mode.startsWith("composed-")) {
    const world = createResourceSnapshotExecutionWorld(undefined, { maxBytes: () => 8 * 1024 * 1024, persistentInputs: { store, internalCache: true } });
    const read = resolvePiToolInvocation("read", { path: file }, { cwd: root, environment: {} })!;
    const reading = { args: { path: file }, callID: "before-search", signal: controller.signal };
    const input = await world.observation!.capture({ ...reading, cwd: root, toolName: "read", executionContext: read,
      tool: { name: "read", label: "read", description: "Read binding", parameters: { type: "object" }, execute: async () => { throw new Error("unbound fixture tool"); } } });
    try {
      const output = await world.observeOperations!({ action: undefined, scope: { sessionID: "cold-search", turnID: "read" }, cwd: root, signal: reading.signal },
        () => read.authoritative!(reading), () => {});
      assert.equal(output.isError, false); await (await input.seal(output)).dispose();
    } finally { await input.dispose(); }
    const bound = bindResourceOperation(executeView, invocation.filesystemRoot!);
    const request = { args: query, callID: "cold-search", signal: controller.signal };
    const active = await world.observation!.capture({ ...request, cwd: root, toolName: "grep", executionContext: { ...invocation, ...bound },
      tool: { name: "grep", label: "grep", description: "Native search binding", parameters: { type: "object" }, execute: async () => { throw new Error("unbound fixture tool"); } } });
    try {
      const output = await world.observeOperations!({ action: undefined, scope: { sessionID: "cold-search", turnID: "search" }, cwd: root, signal: request.signal },
        () => bound.authoritative!(request), () => {});
      const branch = await active.seal(output);
      try {
        let prepared = 0;
        for await (const record of store.candidatesInputVersions(4096)) {
          const values = (record.payload as { preparations?: unknown[] }).preparations;
          if (values?.length) { prepared = values.length; selected = record.id; break; }
        }
        assert.ok(prepared, "ordinary binding must publish the completed matching computation");
        console.log(JSON.stringify({ pid: process.pid, restored: !producing, id: selected, prepared, matches, materializations, nativeMatches, output,
          expired: (await branch.validate!()).status !== "valid", watching: false }));
      } finally { await branch.dispose(); }
    } finally { await active.dispose(); }
  } else {
  if (mode === "discover") {
    for await (const entry of manager.recover(8 * 1024 * 1024)) {
      recovered.push(entry.token); if (!token) { token = entry.token; selected = entry.id; }
      break;
    }
  } else token = producing ? await manager.capture(undefined, 8 * 1024 * 1024) : await manager.restore(saved, 8 * 1024 * 1024);
  if (!token) console.log(JSON.stringify({ pid: process.pid, restored: false, matches, nativeMatches }));
  else {
    const view = token.view!;
    const execute = () => executeView(view);
    if (mode === "clear") await store.clear();
    if (mode === "cancel") {
      const aborted = new AbortController(); aborted.abort();
      await assert.rejects(() => invocation.filesystem!(view, { args: query, callID: "cancelled-search", signal: aborted.signal }), { name: "AbortError" });
    }
    const variants = [];
    if (mode === "variants") for (const change of [{ limit: 1 }, { pattern: "three" }, { ignoreCase: true }, { literal: true }]) {
      query = { pattern: "two", path: file, limit: 20, ignoreCase: false, literal: false, ...change };
      const output = await execute(); variants.push({ change, matches, materializations, nativeMatches, output });
      matches = materializations = nativeMatches = 0;
    }
    let output;
    if (mode === "release") {
      const reading = execute(); await entered.promise;
      const closing = Promise.resolve(token.release());
      await assert.rejects(() => view.readFile(file), /disposed/);
      resume.resolve(); await assert.rejects(() => reading, /disposed/); await closing;
    } else { query = { pattern: "two", path: file, limit: 20, ignoreCase: false, literal: false }; output = await execute(); view.seal(); }
    const id = mode === "produce" || mode === "clear" ? await manager.persist(token) : selected;
    const record = await store.getInputVersion(id), prepared = (record?.payload as { preparations?: readonly unknown[] } | undefined)?.preparations?.length ?? 0;
    console.log(JSON.stringify({ pid: process.pid, restored: !producing, id, prepared, matches, materializations, nativeMatches, output, variants,
      released: mode === "release", expired: (await manager.validate(token)).expired, watching: token.watching }));
  }
  }
} finally {
  childProcess.spawn = spawn; syncBuiltinESMExports();
  await Promise.allSettled(recovered.map(token => token.release()));
  await token?.release(); manager.close(); await profile.pool.dispose(); await store.dispose();
}
