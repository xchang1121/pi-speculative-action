import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import { createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { bindResourceOperation } from "../src/agent-execution-world.ts";
import { ResourceVersionManager, validateResourceVersion, type ResourceVersionToken, type ToolFilesystemOperations } from "../src/resource-version.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import type { Sha256Digest } from "../src/stable-value-hash.ts";

const [mode, root, cache, saved] = process.argv.slice(2) as [string, string, string, Sha256Digest];
const producing = mode === "produce" || mode === "composed-produce";
if (mode === "environment") process.env.SystemRoot = "pi-alternate-search-environment";
const store = new ProvenanceCertificateStore(cache, { orphanGraceMs: 0 });
const manager = new ResourceVersionManager(root, { watch: false, persistentInputs: { store } });
const profile = await createClosedSearchProfile(root), invocation = profile.invocations.get("grep");
let token: ResourceVersionToken | undefined, matches = 0, materializations = 0, nativeMatches = 0;
const composed: ResourceVersionToken[] = [];
const recovered: ResourceVersionToken[] = [];
let releaseComposed: (() => void | Promise<void>) | undefined;
let selected = saved;
const spawn = childProcess.spawn;
childProcess.spawn = ((...args: Parameters<typeof spawn>) => {
  if (Array.isArray(args[1]) && args[1].includes("--json")) nativeMatches++;
  return Reflect.apply(spawn, childProcess, args);
}) as typeof spawn;
syncBuiltinESMExports();
try {
  assert.ok(invocation, "qualified native grep is required for this contract");
  if (mode === "discover" || mode === "composed-discover") {
    for await (const entry of manager.recover(8 * 1024 * 1024)) {
      recovered.push(entry.token); if (!token) { token = entry.token; selected = entry.id; }
      if (mode === "discover") break;
    }
  } else token = producing ? await manager.capture(undefined, 8 * 1024 * 1024) : await manager.restore(saved, 8 * 1024 * 1024);
  if (!token) console.log(JSON.stringify({ pid: process.pid, restored: false, matches, nativeMatches }));
  else {
    const file = path.join(root, "input");
    let query = { pattern: "two", path: file, limit: 20, ignoreCase: false, literal: false };
    const controller = new AbortController();
    let view = token.view!;
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
    const proof = (token: ResourceVersionToken, keys?: ReadonlySet<string>) =>
      [{ ...token, view: undefined, observations: new Map([...token.observations].filter(([key]) => !keys || keys.has(key))) }];
    const execute = () => mode !== "composed-discover" ? executeView(view) : view.evaluate(executeView, keys => proof(token!, keys), root,
      function* () { for (const token of recovered) yield { view: token.view!.readOnly(), observed: (keys?: ReadonlySet<string>) => proof(token, keys) }; },
      undefined, undefined, async proofs => { const result = await validateResourceVersion(proofs); assert.equal(result.expired, false, result.reason); });
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
    if (mode === "composed-produce") {
      await token.release(); token = undefined;
      // Inject storage only: the normal binding still owns query, preparation, proof transfer and sealing.
      const capture = ResourceVersionManager.prototype.capture;
      ResourceVersionManager.prototype.capture = async function (this: ResourceVersionManager, ...args) {
        const token = await capture.apply(this.root === root ? manager : this, args); composed.push(token); return token;
      };
      const bound = bindResourceOperation(inputs => executeView(inputs), root);
      const active = bound.captureInputs!(undefined, 8 * 1024 * 1024, "cold-search");
      try {
        output = await bound.authoritative!({ args: query, callID: "cold-search", signal: controller.signal });
        const branch = await active.seal(output); releaseComposed = () => branch.dispose();
        for (const candidate of composed) if (candidate.view?.retained) {
          const id = await manager.persist(candidate), record = await store.getInputVersion(id);
          if ((record!.payload as { preparations: unknown[] }).preparations.length) {
            assert.equal(token, undefined, "the single-file match has one complete persistent owner"); token = candidate; selected = id;
          }
        }
        assert.ok(token, "normal query must persist its completed matching computation"); view = token.view!;
      } finally { ResourceVersionManager.prototype.capture = capture; await active.dispose(); }
    } else if (mode === "release") {
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
} finally {
  childProcess.spawn = spawn; syncBuiltinESMExports();
  await releaseComposed?.(); await Promise.allSettled([...composed, ...recovered].map(token => token.release()));
  await token?.release(); manager.close(); await profile.pool.dispose(); await store.dispose();
}
