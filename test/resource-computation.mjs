import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { registerHooks } from 'node:module';
import { Worker } from 'node:worker_threads';
import { Session } from 'node:inspector';
import { once } from 'node:events';
import ts from 'typescript';
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from '@earendil-works/pi-ai';
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from '@earendil-works/pi-coding-agent';
const preloadOpaque = process.argv.includes('--preload-opaque'), preload = preloadOpaque || process.argv.includes('--preload'), absolute = Math.abs;
const unscopables = Object.getOwnPropertyDescriptor(Array.prototype, Symbol.unscopables);
let preloadedCalls = 0;
if (preloadOpaque) Object.defineProperty(Array.prototype, Symbol.unscopables, { value: null });
else if (preload) Math.abs = value => { preloadedCalls++; return absolute(value); };
registerHooks({ load(url, context, next) {
  const loaded = next(url, context);
  return url.endsWith('.ts') ? { ...loaded, format: 'module', source: ts.transpileModule(String(loaded.source), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext }, fileName: url,
  }).outputText } : loaded;
} });
const [{ createSpeculativeActionHost }, { createSpeculativeActionExtension }, { ResourceReadView },
  { resourceComputationCompiler, resourceOperationContext }, { TimelineInterval },
  { tool, indexRows, impure, alias, counts, setFactor, cases, replace, reviverCalls, rename }] = await Promise.all([
    import('../src/agent-integration.ts'), import('../src/extension.ts'), import('../src/resource-version.ts'),
    import('../src/resource-computation.ts'), import('../src/task-timing.ts'), import('./resource-computation-tool.mjs'),
  ]);

async function observeCalls() {
  globalThis.__piComputationTest = { indexRows, impure, alias };
  const worker = new Worker(`
    const { parentPort } = require('node:worker_threads'), { Session } = require('node:inspector');
    const session = new Session(); session.connectToMainThread();
    const post = (method, params = {}) => new Promise((resolve, reject) => session.post(method, params,
      (error, result) => error ? reject(error) : resolve(result)));
    const counts = { indexRows: 0, impure: 0, alias: 0 }, breakpoints = new Map();
    session.on('Debugger.paused', ({ params }) => {
      for (const id of params.hitBreakpoints ?? []) { const name = breakpoints.get(id); if (name) counts[name]++; }
      session.post('Debugger.resume');
    });
    parentPort.on('message', async action => {
        if (action === 'stop') {
          await post('Debugger.disable'); await post('Runtime.releaseObjectGroup', { objectGroup: 'counts' });
          session.disconnect(); parentPort.postMessage(counts); parentPort.close();
        } else { parentPort.postMessage({ ...counts }); for (const name of Object.keys(counts)) counts[name] = 0; }
    });
    (async () => {
      await post('Debugger.enable');
      for (const name of Object.keys(counts)) {
        const { result } = await post('Runtime.evaluate', { expression: 'globalThis.__piComputationTest.' + name, objectGroup: 'counts' });
        const { breakpointId } = await post('Debugger.setBreakpointOnFunctionCall', { objectId: result.objectId });
        breakpoints.set(breakpointId, name);
      }
      parentPort.postMessage('ready');
    })().catch(error => { throw error; });
  `, { eval: true });
  await once(worker, 'message');
  return { count: async () => { const reply = once(worker, 'message'); worker.postMessage('count'); return (await reply)[0]; },
    stop: async () => { const reply = once(worker, 'message'); worker.postMessage('stop'); await reply;
      await worker.terminate(); delete globalThis.__piComputationTest; } };
}

async function withChangedCalculation(execute) {
  const session = new Session(); session.connect();
  const post = (method, params = {}) => new Promise((resolve, reject) => session.post(method, params,
    (error, result) => error ? reject(error) : resolve(result)));
  globalThis.__piComputationSourceTest = cases;
  let scriptId, source;
  try {
    await post('Debugger.enable');
    const { result } = await post('Runtime.evaluate', { expression: 'globalThis.__piComputationSourceTest', objectGroup: 'source' });
    const details = await post('Runtime.getProperties', { objectId: result.objectId, ownProperties: true });
    scriptId = details.internalProperties.find(item => item.name === '[[FunctionLocation]]').value.value.scriptId;
    source = (await post('Debugger.getScriptSource', { scriptId })).scriptSource;
    const changed = source.replace('function copy(value) { return { n: value.n }; }', 'function copy(value) { return { n: value.n + 10 }; }');
    assert.notEqual(changed, source);
    assert.equal((await post('Debugger.setScriptSource', { scriptId, scriptSource: changed })).status, 'Ok');
    await execute();
  } finally {
    if (source) assert.equal((await post('Debugger.setScriptSource', { scriptId, scriptSource: source })).status, 'Ok');
    await post('Runtime.releaseObjectGroup', { objectGroup: 'source' }); await post('Debugger.disable'); session.disconnect();
    delete globalThis.__piComputationSourceTest;
  }
}
async function main() {
const directory = await mkdtemp(path.join(os.tmpdir(), 'pi-auto-computation-'));
const file = path.join(directory, 'rows.json'), agentDir = path.join(directory, 'agent');
await writeFile(file, '[{"value":3},{"value":5}]');
await mkdir(agentDir);
await writeFile(path.join(agentDir, 'speculative-action.json'), JSON.stringify({ enabled: false, reuseEnabled: true,
  drafterEnabled: false, prepareObservedOperations: false, patternAware: { enabled: false }, resourceCacheMaxBytes: 8 * 1024 * 1024 }));
const previousAgentDir = process.env.PI_CODING_AGENT_DIR; process.env.PI_CODING_AGENT_DIR = agentDir;
const provider = fauxProvider({ provider: 'automatic-data', models: [{ id: 'actor' }], tokensPerSecond: 4000 });
const tools = ['summarize', 'inspect'].map(name => tool(name, file));
const events = []; let observer, reuseEnabled = true;
const extension = createSpeculativeActionExtension({ createHost: (id, options) => {
  const host = createSpeculativeActionHost(id, { ...options,
    getSettings: () => ({ ...options.getSettings(), reuseEnabled }),
    resources: { initial: { cpuCount: 2, idleCpuCount: 2 }, sample: () => new Promise(() => {}) },
    onEvent: event => { if (event.type === 'actor_action') events.push(event); return options.onEvent?.(event); },
  });
  return { ...host, execute: (call, signal, actor) => host.execute(call, signal, async operation => {
    // Observe the physical calls after the product's automatic preparation; no test prepares these tools.
    observer ??= await observeCalls(); return actor(operation);
  }) };
} });
const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
const resourceLoader = new DefaultResourceLoader({ cwd: directory, agentDir, settingsManager, noExtensions: true,
  extensionFactories: [pi => { for (const tool of tools) pi.registerTool(tool); }, extension] });
await resourceLoader.reload();
const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, 'auth.json'), modelsPath: null, refreshOnCreate: false });
modelRuntime.registerNativeProvider(provider.provider);
const { session } = await createAgentSession({ cwd: directory, agentDir, model: provider.getModel(), modelRuntime, settingsManager,
  resourceLoader, sessionManager: SessionManager.inMemory(directory), tools: tools.map(tool => tool.name) });
const execute = async (tool, id) => {
  provider.setResponses([fauxAssistantMessage(fauxToolCall(tool.name, {}, { id }), { stopReason: 'toolUse' }), fauxAssistantMessage('done')]);
  await session.prompt(`Execute ${tool.name}.`);
  const result = session.messages.find(message => message.role === 'toolResult' && message.toolCallId === id);
  assert.ok(result && !result.isError, JSON.stringify(result)); return result;
};
const original = indexRows.toString(), originalCases = cases.toString(), compiler = resourceComputationCompiler();
let compilerAttached = false;
const countsByCall = [];
const executed = async () => { countsByCall.push(await observer.count()); };
try {
  await session.bindExtensions({ mode: 'print', onError: error => { throw error; } });
  const first = await execute(tools[0], 'first');
  assert.notEqual(cases.toString(), originalCases);
  await executed();
  const second = await execute(tools[1], 'second');
  await executed();
  assert.deepEqual(second.details.result, first.details.result);
  assert.equal(second.details.alias, true);
  assert.deepEqual(counts(), { renders: 2, reads: 2, effects: 2 });
  assert.ok(events[1].computation?.hiddenComputeMs > 0, JSON.stringify({ instrumented: indexRows.toString(), events }));
  second.details.result[0].value = -1;
  assert.deepEqual((await execute(tools[1], 'third')).details.result, first.details.result);
  await executed();
  setFactor(3);
  const changed = await execute(tools[1], 'capture-change');
  await executed();
  assert.notDeepEqual(changed.details.result, first.details.result);
  await writeFile(file, '[{"value":7}]');
  const refreshed = await execute(tools[1], 'file-change');
  await executed();
  assert.equal(refreshed.details.result.length, 1);
  assert.deepEqual(counts(), { renders: 5, reads: 5, effects: 5 });
  assert.deepEqual(countsByCall, [{ indexRows: 1, impure: 1, alias: 1 }, { indexRows: 0, impure: 1, alias: 1 },
    { indexRows: 0, impure: 1, alias: 1 }, { indexRows: 1, impure: 1, alias: 1 }, { indexRows: 1, impure: 1, alias: 1 }]);
  const automatic = { hidden: events.map(event => event.computation?.hiddenComputeMs), counts: counts(),
    indexExecutions: countsByCall.reduce((count, item) => count + item.indexRows, 0) };
  reuseEnabled = false;
  assert.deepEqual((await execute(tools[1], 'disabled')).details.result, refreshed.details.result);
  assert.deepEqual(await observer.count(), { indexRows: 1, impure: 1, alias: 1 });
  assert.equal(events.at(-1).computation?.hiddenComputeMs ?? 0, 0);
  assert.deepEqual(counts(), { renders: 6, reads: 6, effects: 6 });
  await observer.stop(); observer = undefined;
  const view = new ResourceReadView(8 * 1024 * 1024);
  await compiler.prepare(cases); compilerAttached = true;
  const context = { view, sources: () => [] };
  const evaluate = (kind, value, other, scope = context) => TimelineInterval.measure(() =>
    resourceOperationContext.run({ computation: scope, maxBytes: () => 8 * 1024 * 1024 }, async () => cases(kind, value, other)));
  const used = evaluation => evaluation.dependencies.some(item => item.reused);
  try {
    const ordered = { a: 1, b: 2 }, reversed = { b: 2, a: 1 };
    assert.equal((await evaluate('keys', ordered)).output, 'a|b');
    assert.equal((await evaluate('keys', reversed)).output, 'b|a');
    assert.ok(used(await evaluate('keys', ordered)));
    const data = { n: 1 }, fresh = (await evaluate('copy', data)).output;
    const borrowed = await evaluate('copy', data); assert.ok(used(borrowed));
    assert.notEqual(fresh, borrowed.output); borrowed.output.n = 100;
    assert.deepEqual((await evaluate('copy', data)).output, { n: 1 });
    assert.equal((await evaluate('capture')).output.factor, 3); setFactor(4);
    assert.equal((await evaluate('capture')).output.factor, 4);
    for (const mode of ['change', 'alias-change']) {
      const mutable = { n: 0 };
      assert.equal((await evaluate(mode, mutable)).output, 1);
      assert.equal((await evaluate(mode, mutable)).output, 2);
    }
    assert.equal((await evaluate('identity', data)).output, data);
    assert.equal((await evaluate('identity', data)).output, data);
    assert.equal((await evaluate('equal', data, { n: 1 })).output, false);
    assert.equal((await evaluate('equal', data, data)).output, true);
    let reads = 0;
    const getter = { get n() { reads++; return 1; } };
    await evaluate('copy', getter); await evaluate('copy', getter); assert.equal(reads, 2);
    const proxy = new Proxy(data, { get(target, key) { reads++; return target[key]; } });
    await evaluate('copy', proxy); await evaluate('copy', proxy); assert.equal(reads, 4);
    await evaluate('replace', data); let calls = 0;
    replace(value => { calls++; return { n: value.n + 1 }; });
    assert.equal((await evaluate('replace', data)).output.n, 2);
    await evaluate('replace', data); assert.equal(calls, 2);
    await evaluate('abs', -3);
    const absolute = Math.abs; let nativeCalls = 0;
    try { Math.abs = value => { nativeCalls++; return absolute(value); };
      await evaluate('abs', -3); await evaluate('abs', -3); assert.equal(nativeCalls, 2);
    } finally { Math.abs = absolute; }
    await evaluate('parse', '{"n":1}');
    const parse = Object.getOwnPropertyDescriptor(JSON, 'parse'); let propertyReads = 0;
    try { Object.defineProperty(JSON, 'parse', { configurable: true, get() { propertyReads++; return parse.value; } });
      await evaluate('parse', '{"n":1}'); await evaluate('parse', '{"n":1}'); assert.equal(propertyReads, 2);
    } finally { Object.defineProperty(JSON, 'parse', parse); }
    await evaluate('revive', '{"n":1}'); await evaluate('revive', '{"n":1}'); assert.equal(reviverCalls(), 4);
    assert.equal((await evaluate('optional', '{"n":1}')).output, undefined);
    assert.equal((await evaluate('self')).output, 'ownName'); rename('changed');
    assert.equal((await evaluate('self')).output, 'changed');
    const inherited = { n: 1 }; assert.equal((await evaluate('inherited', inherited)).output, 2);
    Object.defineProperty(inherited, 'n', { writable: false });
    await assert.rejects(evaluate('inherited', inherited), TypeError);
    await withChangedCalculation(async () => {
      const changed = await evaluate('copy', data); assert.equal(changed.output.n, 11); assert.equal(used(changed), false);
    });
    assert.equal((await evaluate('copy', data)).output.n, 1);
    const source = 'export function reflected(value) { return { data: JSON.parse(value), source: reflected.toString() }; }';
    const { reflected } = await import('data:text/javascript,' + encodeURIComponent(source));
    const originalSource = reflected.toString(); await compiler.prepare(reflected);
    for (let i = 0; i < 2; i++) {
      const result = await TimelineInterval.measure(() => resourceOperationContext.run({ computation: context, maxBytes: () => 1024 * 1024 },
        async () => reflected('{"n":1}')));
      assert.deepEqual(result.output, { data: { n: 1 }, source: originalSource }); assert.equal(used(result), false);
    }
    assert.equal(used(await evaluate('copy', data, undefined, { ...context, signal: AbortSignal.abort() })), false);
    const empty = new ResourceReadView(0);
    try { for (let i = 0; i < 2; i++) assert.equal(used(await evaluate('copy', data, undefined, { view: empty, sources: () => [] })), false); }
    finally { await empty.dispose(); }
    await view.dispose(); assert.equal(used(await evaluate('copy', data)), false);
  } finally { await view.dispose(); }
  console.log(JSON.stringify(automatic));
} finally {
  try {
    await observer?.stop();
    try { await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' }); } finally { session.dispose(); }
    try { if (compilerAttached) assert.notEqual(cases.toString(), originalCases); } finally { await compiler.dispose(); }
    assert.equal(indexRows.toString(), original); assert.equal(cases.toString(), originalCases);
  } finally {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    await rm(directory, { recursive: true, force: true });
  }
}
}
async function preloaded() {
  const compiler = resourceComputationCompiler(), view = new ResourceReadView(1024 * 1024);
  try {
    await compiler.prepare(cases); preloadedCalls = 0;
    for (let i = 0; i < 2; i++) {
      const result = await TimelineInterval.measure(() => resourceOperationContext.run({ maxBytes: () => 1024 * 1024,
        computation: { view, sources: () => [] } }, async () => cases('abs', -3)));
      assert.equal(result.output, 3); assert.equal(result.dependencies.some(item => item.reused), false);
    }
    assert.equal(preloadedCalls, preloadOpaque ? 0 : 2); console.log(JSON.stringify({ preloadedCalls }));
  } finally { Math.abs = absolute; Object.defineProperty(Array.prototype, Symbol.unscopables, unscopables);
    await view.dispose(); await compiler.dispose(); }
}
(preload ? preloaded() : main()).catch(error => { console.error(error); process.exitCode = 1; });
