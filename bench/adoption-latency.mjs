import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { AsyncLocalStorage } from 'node:async_hooks';
import { registerHooks, syncBuiltinESMExports } from 'node:module';
import { createHash } from 'node:crypto';

const [repository, reportPath, repeatsText = '12', mode = 'ready', selectedText = 'read,ls,write,edit,find,grep,native-find,native-grep'] = process.argv.slice(2);
const repo = path.resolve(repository), repeats = Number(repeatsText), profiled = process.argv.includes('--profile') || mode === 'running';
assert.ok(['ready', 'running'].includes(mode)); assert.ok(repeats > 0);
await assert.rejects(fs.access(reportPath), { code: 'ENOENT' });
process.env.PI_OFFLINE = '1';
const active = new AsyncLocalStorage();
globalThis.__adoptionTrace = (name, operation) => {
  const trace = active.getStore();
  if (!trace) return operation();
  const begin = performance.now(), entry = { name, startMs: begin - trace.begin };
  trace.spans.push(entry);
  try {
    const value = operation();
    if (name === 'waitForCandidate') trace.release?.();
    if (value?.then) return value.then(result => { entry.ms = performance.now() - begin; return result; }, error => { entry.ms = performance.now() - begin; throw error; });
    entry.ms = performance.now() - begin; return value;
  } catch (error) { entry.ms = performance.now() - begin; throw error; }
};
const importRepo = name => import(pathToFileURL(path.join(repo, name)).href);
if (process.argv.includes('--fs-profile')) {
  assert.ok(profiled);
  for (const name of ['lstat', 'stat', 'realpath', 'readFile', 'readdir', 'open', 'access', 'mkdir', 'rename']) {
    const original = fs[name];
    fs[name] = (...args) => globalThis.__adoptionTrace(`fs.${name}`, async () => {
      const value = await original(...args);
      if (name === 'open') for (const method of ['stat', 'read', 'truncate', 'writeFile', 'sync', 'close']) {
        const operation = value[method].bind(value);
        value[method] = (...parameters) => globalThis.__adoptionTrace(`fd.${method}`, () => operation(...parameters));
      }
      return value;
    });
  }
  syncBuiltinESMExports();
}
let hooks;
const workspaceBaseline = process.argv.find(argument => argument.startsWith('--workspace-baseline='))?.slice('--workspace-baseline='.length);
const resourceBaseline = process.argv.find(argument => argument.startsWith('--resource-baseline='))?.slice('--resource-baseline='.length);
let baselineHooks;
if (workspaceBaseline || resourceBaseline) {
  const sources = new Map();
  for (const [name, filename] of [['workspace-sandbox.js', workspaceBaseline], ['resource-version.js', resourceBaseline]])
    if (filename) sources.set(pathToFileURL(path.join(repo, 'dist', name)).href, await fs.readFile(filename, 'utf8'));
  baselineHooks = registerHooks({ load(url, context, nextLoad) {
    const result = nextLoad(url, context); return sources.has(url) ? { ...result, source: sources.get(url) } : result;
  } });
}
if (profiled) {
  const ts = (await importRepo('node_modules/typescript/lib/typescript.js')).default;
  const names = new Set(['actorActionKey', 'resolveBinding', 'predictionMatches', 'promoteForActor', 'rankCandidates', 'authorize',
    'waitForCandidate', 'projectOutput', 'validateCandidate', 'reconcileAdoptedCandidate', 'queueCandidateContinuations',
    'confirmPredictions', 'queueActorSettlement', 'fingerprintDependencies', 'fingerprintBinding', 'fingerprintPath',
    'assertCommitTarget', 'readRegularState', 'createParentDirectories', 'readSandboxDirectoryState']);
  const files = new Set(['runtime-engine.js', 'agent-integration.js', 'resource-version.js', 'workspace-sandbox.js']);
  hooks = registerHooks({ load(url, context, nextLoad) {
    const result = nextLoad(url, context);
    if (!url.startsWith(pathToFileURL(path.join(repo, 'dist') + path.sep).href) || !files.has(path.basename(fileURLToPath(url)))) return result;
    const source = ts.createSourceFile(url, String(result.source), ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
    const transform = ts.transform(source, [context => {
      const visit = node => {
        const containsAwait = value => ts.isAwaitExpression(value) || ts.forEachChild(value, containsAwait);
        const expression = ts.isCallExpression(node) ? node.expression : undefined;
        const label = expression && (ts.isIdentifier(expression) && names.has(expression.text) ? expression.text
          : ts.isPropertyAccessExpression(expression) && ['branch.commit', 'actorAction.settleSelection'].includes(expression.getText(source)) ? expression.getText(source) : undefined);
        if (label && !node.arguments.some(containsAwait)) return ts.factory.createCallExpression(ts.factory.createPropertyAccessExpression(ts.factory.createIdentifier('globalThis'), '__adoptionTrace'), undefined,
          [ts.factory.createStringLiteral(label), ts.factory.createArrowFunction(undefined, undefined, [], undefined, ts.factory.createToken(ts.SyntaxKind.EqualsGreaterThanToken), ts.visitEachChild(node, visit, context))]);
        return ts.visitEachChild(node, visit, context);
      };
      return root => ts.visitNode(root, visit);
    }]);
    const code = ts.createPrinter().printFile(transform.transformed[0]); transform.dispose();
    return { ...result, source: code };
  } });
}
const { createSpeculativeActionHost } = await importRepo('dist/agent-integration.js');
const { PI_ACTION_SEMANTICS } = await importRepo('dist/action-semantics.js');
const { createResourceSnapshotExecutionWorld } = await importRepo('dist/agent-execution-world.js');
const { WorkspaceSandboxService } = await importRepo('dist/workspace-sandbox.js');
const { createPiToolDefinitions, resolvePiToolInvocation, createClosedSearchProfile } = await importRepo('dist/pi-tool-invocation.js');
const model = { id: 'fixture', name: 'fixture', api: 'openai-completions', provider: 'fixture', baseUrl: 'http://fixture.invalid',
  reasoning: false, input: ['text'], contextWindow: 32768, maxTokens: 2048, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const fixtureParent = path.resolve(process.env.THINKTHREAD_FS ?? os.tmpdir());
const owned = await fs.mkdtemp(path.join(fixtureParent, 'pi-adoption-audit-'));
assert.equal(path.dirname(owned), fixtureParent);
const rows = [];
const wire = value => JSON.parse(JSON.stringify(value));
const hash = value => createHash('sha256').update(value).digest('hex');
const percentile = (values, fraction) => [...values].sort((a, b) => a - b)[Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)];
const deadline = async promise => {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Fixture did not reach a terminal state in 60 seconds')), 60000); })]); }
  finally { clearTimeout(timer); }
};
async function state(root) {
  const result = [];
  async function walk(relative = '') {
    for (const name of (await fs.readdir(path.join(root, relative))).sort()) {
      const entry = path.join(relative, name), info = await fs.lstat(path.join(root, entry));
      assert.ok(info.isFile() || info.isDirectory(), `Unexpected fixture entry: ${entry}`);
      result.push({ path: entry, mode: info.mode & 0o777, kind: info.isDirectory() ? 'directory' : 'file',
        ...(info.isFile() ? { hash: hash(await fs.readFile(path.join(root, entry))) } : {}) });
      if (info.isDirectory()) await walk(entry);
    }
  }
  await walk(); return result;
}
try {
  for (const selected of selectedText.split(',')) {
    const nativeSearch = selected.startsWith('native-'), name = selected.replace('native-', '');
    const root = path.join(owned, selected); await fs.mkdir(root);
    const cwd = process.env.THINKTHREAD_FS ? fixtureParent : root;
    const prefix = path.relative(cwd, root).split(path.sep).join('/');
    const at = name => prefix ? `${prefix}/${name}` : name;
    const args = { read: { path: at('notes.txt') }, ls: { path: at('.') },
      write: { path: at('generated.txt'), content: 'generated\n' }, edit: { path: at('notes.txt'), edits: [{ oldText: 'beta', newText: 'gamma' }] },
      find: { path: at('.'), pattern: '*.txt' }, grep: { path: at('.'), pattern: 'alpha' } }[name];
    assert.ok(args, `Unknown tool ${selected}`);
    const reset = async () => {
      await fs.mkdir(path.join(root, 'nested'), { recursive: true });
      await fs.writeFile(path.join(root, 'notes.txt'), 'alpha\nbeta\n' + 'ordinary content\n'.repeat(64));
      await fs.writeFile(path.join(root, 'nested/todo.txt'), 'alpha todo\n');
      await fs.rm(path.join(root, 'generated.txt'), { force: true });
    };
    await reset();
    const baselineState = await state(root);
    let profile;
    if (['find', 'grep'].includes(name) && !nativeSearch) profile = await createClosedSearchProfile(cwd);
    const bound = profile?.invocations.get(name) ?? resolvePiToolInvocation(name, args, { cwd, environment: {} });
    if (profile && !bound) { rows.push({ tool: selected, unavailable: 'Captured search engine is not installed/qualified' }); await profile.pool.dispose(); continue; }
    const definition = createPiToolDefinitions(cwd).get(name);
    const tool = { ...definition, execute: (id, value, signal, onUpdate) => definition.execute(id, value, signal, onUpdate, { model: { input: ['text'] } }) };
    const native = bound?.authoritative ? async () => (await bound.authoritative({ args, callID: 'oracle', signal: new AbortController().signal })).result : () => tool.execute('oracle', args);
    const trials = [];
    try {
      for (let index = 0; index < repeats; index++) {
        await reset();
        const oracleBegin = performance.now(), expected = await native(), nativeMs = performance.now() - oracleBegin;
        const expectedState = await state(root);
        await reset();
        const terminal = Promise.withResolvers(), entered = Promise.withResolvers(), released = Promise.withResolvers();
        let producerCalls = 0, fallbackCalls = 0, readyAt, terminalState, settlement;
        const workspace = new WorkspaceSandboxService();
        const baseWorld = ['write', 'edit'].includes(name) ? workspace.createExecutionWorld({ driver: 'git' })
          : createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: [name], maxBytes: () => 8 * 1024 * 1024 });
        const world = { ...baseWorld, speculation: { ...baseWorld.speculation, execute: async context => {
          producerCalls++; entered.resolve();
          if (mode === 'running') await released.promise;
          return baseWorld.speculation.execute(context);
        } } };
        const host = createSpeculativeActionHost(`adoption-${selected}-${index}`, { cwd, executionWorlds: [world],
          getSettings: () => ({ enabled: true, drafterEnabled: true, drafterGateEnabled: false, drafterMaxDepth: 0,
            candidateLimit: 1, maxConcurrentActions: 1, tools: [name], resourceCacheMaxBytes: 16 * 1024 * 1024,
            patternAware: { enabled: false } }), resolveInvocation: () => bound, preflight: () => true,
          complete: async () => ({ role: 'assistant', api: model.api, provider: model.provider, model: model.id, timestamp: 0,
            content: [{ type: 'toolCall', id: 'fixture-call', name, arguments: args }], stopReason: 'toolUse', usage }),
          onActorActionSettled: ({ settlement: value }) => { settlement = value; },
          onEvent: event => {
            if (event.type === 'candidate' && ['succeeded', 'failed', 'cancelled'].includes(event.state.status)) {
              terminalState = event.state; readyAt = performance.now(); terminal.resolve();
            }
            if (event.type === 'prediction' && event.settlement.observation === 'unobserved') { terminalState = event.settlement; terminal.resolve(); }
          } });
        const turnID = 'turn', tools = [tool];
        try {
          const preparationBegin = performance.now();
          await host.startTurn({ turnID, actorModel: model, actorOptions: undefined, tools,
            context: { systemPrompt: 'Isolated adoption fixture; no model API.', messages: [], tools } });
          if (!nativeSearch) await deadline(mode === 'ready' ? terminal.promise : Promise.race([entered.promise, terminal.promise]));
          const preparationMs = performance.now() - preparationBegin;
          assert.deepEqual(await state(root), baselineState, 'Speculative writes leaked before Actor adoption');
          const trace = { begin: performance.now(), spans: [], release: () => released.resolve() };
          const run = () => host.execute({ turnID, id: 'actor-call', tool: name, args, tools }, new AbortController().signal,
            async operation => { fallbackCalls++; released.resolve();
              return operation.invocation?.authoritative ? (await operation.invocation.authoritative({ args: operation.input, callID: operation.callID, signal: operation.signal })).result
                : tool.execute(operation.callID, operation.input, operation.signal); });
          const result = await deadline(profiled ? active.run(trace, run) : run());
          const returnedAt = performance.now(), adoptionMs = returnedAt - trace.begin;
          await host.finishTurn(turnID);
          assert.deepEqual(wire(result), wire(expected), 'Actor output differs from the native oracle');
          assert.deepEqual(await state(root), expectedState, 'Actor file effects differ from the native oracle');
          assert.ok(settlement, 'Missing authoritative settlement');
          assert.equal(fallbackCalls, settlement.provider.kind === 'actor' ? 1 : 0, 'Duplicate authoritative execution');
          assert.equal(producerCalls, nativeSearch ? 0 : 1, 'Unexpected producer execution count');
          trials.push({ index, nativeMs, preparationMs, adoptionMs, producerCalls, fallbackCalls,
            outcome: settlement.provider.kind, readyAtArrival: readyAt !== undefined && readyAt <= trace.begin,
            ...(readyAt === undefined ? {} : { remainingProducerMs: Math.max(0, readyAt - trace.begin), readyToReturnMs: returnedAt - Math.max(readyAt, trace.begin) }),
            provider: settlement.provider, rejections: settlement.rejections,
            terminal: terminalState?.status ?? terminalState?.cause?.code,
            ...(profiled ? { spans: trace.spans } : {}) });
        } finally { released.resolve(); try { await host.dispose(); } finally { await workspace.dispose(); } }
      }
    } finally { await profile?.pool.dispose(); }
    const row = { tool: selected, mode, profiled, trials,
      p50Ms: percentile(trials.map(trial => trial.adoptionMs), .5), p95Ms: percentile(trials.map(trial => trial.adoptionMs), .95),
      nativeP50Ms: percentile(trials.map(trial => trial.nativeMs), .5), hits: trials.filter(trial => trial.outcome !== 'actor').length };
    rows.push(row); console.log(JSON.stringify({ tool: selected, mode, profiled, p50Ms: row.p50Ms, p95Ms: row.p95Ms, nativeP50Ms: row.nativeP50Ms, hits: row.hits, repeats }));
  }
} finally {
  await fs.writeFile(reportPath, JSON.stringify({ platform: process.platform, node: process.version, mode, profiled, apiRequests: 0,
    scope: 'Full Host.execute entry to resolved Actor result. Producer preparation is separate. A deterministic proposal isolates adoption; this is not natural model/E2E evidence. Running mode releases the controlled producer after Actor joins, and reports remaining execution separately. Native search fallback is reported explicitly.', rows }, null, 2) + '\n', { flag: 'wx' });
  hooks?.deregister(); baselineHooks?.deregister(); delete globalThis.__adoptionTrace;
  assert.equal(path.dirname(owned), fixtureParent); assert.ok(path.basename(owned).startsWith('pi-adoption-audit-'));
  await fs.rm(owned, { recursive: true, force: true });
}
