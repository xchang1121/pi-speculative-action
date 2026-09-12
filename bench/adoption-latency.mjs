import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { AsyncLocalStorage } from 'node:async_hooks';
import { registerHooks, syncBuiltinESMExports } from 'node:module';
import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const [repository, reportPath, repeatsText = '12', mode = 'ready', selectedText = 'read,ls,write,edit,find,grep,native-find,native-grep'] = process.argv.slice(2);
const repo = path.resolve(repository), repeats = Number(repeatsText), profiled = process.argv.includes('--profile') || mode === 'running';
const backend = process.argv.find(argument => argument.startsWith('--backend='))?.slice('--backend='.length) ?? 'local';
assert.ok(['local', 'linux-process', 'thinkthread'].includes(backend));
assert.ok(backend === 'local' || process.platform === 'linux', `${backend} requires Linux`);
assert.ok(['ready', 'running'].includes(mode)); assert.ok(repeats > 0);
await assert.rejects(fs.access(reportPath), { code: 'ENOENT' });
process.env.PI_OFFLINE = '1';
const active = new AsyncLocalStorage();
const inTrace = (trace, run) => profiled ? active.run(trace, run) : run();
let currentActorTrace, currentProducerTrace;
globalThis.__adoptionTrace = (name, operation) => {
  // Socket callbacks re-enter the scope that owns their Actor or producer operation.
  const owner = name === 'decideHeldExec' ? currentActorTrace : name === 'handleWireRequest' ? currentProducerTrace : undefined;
  if (owner && active.getStore() !== owner) return active.run(owner, () => globalThis.__adoptionTrace(name, operation));
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
const handoffBaseline = process.argv.find(argument => argument.startsWith('--handoff-baseline='))?.slice('--handoff-baseline='.length);
let baselineHooks;
if (workspaceBaseline || resourceBaseline || handoffBaseline) {
  const sources = new Map();
  for (const [name, filename] of [['workspace-sandbox.js', workspaceBaseline], ['resource-version.js', resourceBaseline], ['process-handoff.js', handoffBaseline]])
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
    'assertCommitTarget', 'readRegularState', 'createParentDirectories', 'readSandboxDirectoryState',
    'decideHeldExec', 'acquireProcessResult', 'plan', 'lookup', 'findByWeakKey', 'validateDynamicDependencyCertificate', 'replayFilesystemEffects',
    'acquireSandboxRepository', 'acquireSandboxBaseline', 'ensurePreparedSandbox', 'takePreparedSandbox', 'attachSandboxWorkspace',
    'createPrivateSandboxWorkspace', 'createGitWorkspaceTransactionDriver', 'collectSandboxChanges', 'cleanupPrivateSandboxWorkspace',
    'createProcessInterposition', 'probeExecutionContext', 'runSpawn', 'observeStrace', 'captureDependencies', 'sealSessionEvidence', 'handleWireRequest']);
  const files = new Set(['runtime-engine.js', 'agent-integration.js', 'resource-version.js', 'workspace-sandbox.js',
    'linux-process-world.js', 'linux-process-backend.js', 'process-handoff.js', 'reuse-planner.js', 'provenance-validation.js']);
  hooks = registerHooks({ load(url, context, nextLoad) {
    const result = nextLoad(url, context);
    if (!url.startsWith(pathToFileURL(path.join(repo, 'dist') + path.sep).href) || !files.has(path.basename(fileURLToPath(url)))) return result;
    const source = ts.createSourceFile(url, String(result.source), ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
    const transform = ts.transform(source, [context => {
      const visit = node => {
        const containsAwait = value => ts.isAwaitExpression(value) || ts.forEachChild(value, containsAwait);
        const expression = ts.isCallExpression(node) ? node.expression : undefined;
        const label = expression && (ts.isIdentifier(expression) && names.has(expression.text) ? expression.text
          : ts.isPropertyAccessExpression(expression) && names.has(expression.name.text) ? expression.name.text
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
let processApi, thinkThreadApi, runtimeIdentity;
if (backend === 'linux-process') processApi = Object.assign({}, ...await Promise.all([
  importRepo('dist/linux-process-backend.js'), importRepo('dist/linux-process-world.js'),
  importRepo('dist/process-execution.js'), importRepo('node_modules/@earendil-works/pi-coding-agent/dist/index.js'),
]));
if (backend === 'thinkthread') {
  assert.ok(process.env.THINKTHREAD_FS, 'Run inside a real ThinkThread profile');
  assert.equal(path.resolve(process.cwd()), path.resolve(process.env.THINKTHREAD_FS));
  thinkThreadApi = await importRepo('dist/thinkthread/execution-world.js');
  const { createThinkThreadClient } = await importRepo('dist/thinkthread/control-transport.js');
  const client = createThinkThreadClient(), self = await client.selfView();
  assert.ok(self.capabilities.some(value => value.id === 'thinkthread.fs.self' && value.version === 1));
  runtimeIdentity = { attachment: (await client.fs.stat()).kind, liveControlConnection: true };
}
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
    const nativeSearch = selected.startsWith('native-'), child = selected.startsWith('bash-child');
    const name = child ? 'bash' : selected.replace('native-', '');
    const concurrency = Number(process.argv.find(argument => argument.startsWith('--concurrency='))?.slice('--concurrency='.length) ?? (child ? 2 : 1));
    assert.ok(Number.isSafeInteger(concurrency) && concurrency > 0);
    assert.ok(!child || backend === 'linux-process', 'Child handoff requires the Linux process backend');
    assert.ok(backend !== 'linux-process' || name === 'bash', 'The Linux process route in this benchmark binds Bash');
    assert.ok(backend !== 'thinkthread' || nativeSearch || !['find', 'grep'].includes(name), 'ThinkThread does not bind this captured search profile');
    const root = path.join(owned, selected); await fs.mkdir(root);
    const cwd = process.env.THINKTHREAD_FS ? fixtureParent : root;
    const prefix = path.relative(cwd, root).split(path.sep).join('/');
    const at = name => prefix ? `${prefix}/${name}` : name;
    const args = { read: { path: at('notes.txt') }, ls: { path: at('.') },
      write: { path: at('generated.txt'), content: 'generated\n' }, edit: { path: at('notes.txt'), edits: [{ oldText: 'beta', newText: 'gamma' }] },
      find: { path: at('.'), pattern: '*.txt' }, grep: { path: at('.'), pattern: 'alpha' },
      bash: { command: "printf 'completed\\n'" } }[name];
    assert.ok(args, `Unknown tool ${selected}`);
    const reset = async () => {
      await fs.mkdir(path.join(root, 'nested'), { recursive: true });
      await fs.writeFile(path.join(root, 'notes.txt'), 'alpha\nbeta\n' + 'ordinary content\n'.repeat(64));
      await fs.writeFile(path.join(root, 'nested/todo.txt'), 'alpha todo\n');
      await fs.rm(path.join(root, 'generated.txt'), { force: true });
    };
    await reset();
    if (child) {
      await fs.writeFile(path.join(root, 'worker.c'), `
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
int main(void) {
  unsigned char input[4096];
  int fd = open("notes.txt", O_RDONLY);
  if (fd < 0) return 65;
  ssize_t length = read(fd, input, sizeof(input));
  close(fd);
  if (length <= 0) return 66;
  uint64_t digest = 14695981039346656037ULL;
  for (int pass = 0; pass < 100000; pass++)
    for (ssize_t index = 0; index < length; index++) digest = (digest ^ input[index]) * 1099511628211ULL;
  char output[17] = "0000000000000000\\n";
  for (int index = 15; index >= 0; index--, digest >>= 4) output[index] = "0123456789abcdef"[digest & 15];
  fd = open("generated.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0 || write(fd, output, sizeof(output)) != sizeof(output) || close(fd)) return 67;
  return write(1, output, sizeof(output)) == sizeof(output) ? 0 : 68;
}
`);
      await promisify(execFile)('cc', ['-O2', '-Wall', '-Wextra', '-o', 'worker', 'worker.c'], { cwd: root });
      args.command = "printf 'actor-parent\\n'; worker";
    }
    const draftArgs = child ? { command: ': speculative-parent; worker' } : args;
    const baselineState = await state(root);
    let profile;
    if (['find', 'grep'].includes(name) && !nativeSearch) profile = await createClosedSearchProfile(cwd);
    const environment = backend === 'linux-process' ? Object.freeze({ PATH: `${cwd}:/usr/bin:/bin`, HOME: os.homedir(), SHELL: '/bin/bash', LANG: 'C.UTF-8' }) : {};
    const bound = profile?.invocations.get(name) ?? resolvePiToolInvocation(name, args, { cwd, environment, ...(backend === 'linux-process' ? { shellPath: '/bin/bash' } : {}) });
    const bind = value => child ? resolvePiToolInvocation(name, value, { cwd, environment, shellPath: '/bin/bash' }) : bound;
    if (profile && !bound) { rows.push({ tool: selected, unavailable: 'Captured search engine is not installed/qualified' }); await profile.pool.dispose(); continue; }
    const definition = backend === 'linux-process'
      ? processApi.createBashTool(cwd, { shellPath: '/bin/bash', exposeSessionEnvironment: false, spawnHook: context => ({ ...context, env: { ...environment } }) })
      : createPiToolDefinitions(cwd).get(name);
    const originalTool = { ...definition, execute: (id, value, signal, onUpdate) => definition.execute(id, value, signal, onUpdate,
      name === 'bash' ? undefined : { model: { input: ['text'] } }) };
    const native = bound?.authoritative ? async () => (await bound.authoritative({ args, callID: 'oracle', signal: new AbortController().signal })).result : () => originalTool.execute('oracle', args);
    const row = { tool: selected, backend, mode, profiled, concurrency, trials: [],
      ...(backend === 'thinkthread' && ['write', 'edit'].includes(name) ? { limitation: 'Latency and ordinary content/mode comparison only. ThinkThread snapshot writes have known native permission/file-identity differences; these trials do not qualify general write semantics.' } : {}) };
    rows.push(row);
    const trials = row.trials;
    try {
      for (let index = 0; index < repeats; index++) {
        await reset();
        const oracleBegin = performance.now(), expected = await native(), nativeMs = performance.now() - oracleBegin;
        const expectedState = await state(root);
        await reset();
        const preparationBegin = performance.now();
        const producerTrace = { begin: preparationBegin, spans: [] };
        currentProducerTrace = producerTrace;
        const terminal = Promise.withResolvers(), entered = Promise.withResolvers(), released = Promise.withResolvers();
        const hold = async signal => {
          const abort = () => released.reject(signal.reason);
          signal?.addEventListener('abort', abort, { once: true });
          try { signal?.throwIfAborted(); await released.promise; }
          finally { signal?.removeEventListener('abort', abort); }
        };
        let producerCalls = 0, fallbackCalls = 0, readyAt, childReadyAt, terminalState, settlement, trace;
        const workspace = new WorkspaceSandboxService();
        let baseWorld, tool = originalTool, processBackend, coordinator;
        if (backend === 'thinkthread') baseWorld = thinkThreadApi.createThinkThreadExecutionWorld();
        else if (backend === 'linux-process') {
          const storeRoot = await fs.mkdtemp(path.join(owned, 'process-store-'));
          processBackend = new processApi.LinuxProcessReuseBackend({ storeRoot,
            ...(process.env.PI_SPEC_SANDLOCK ? { sandlockBinary: process.env.PI_SPEC_SANDLOCK } : {}),
            ...(process.env.PI_SPEC_HELD_EXEC ? { heldExecBinary: process.env.PI_SPEC_HELD_EXEC } : {}) });
          if (child) {
            const complete = processBackend.handoffs.complete.bind(processBackend.handoffs);
            processBackend.handoffs.complete = (...parameters) => {
              const completed = complete(...parameters);
              if (completed && parameters[2]) childReadyAt = performance.now();
              return completed;
            };
          }
          const originalExecutor = processApi.adaptProcessToolOperations(processApi.createLocalBashOperations({ shellPath: '/bin/bash' }));
          const routeOptions = { sourceRoot: cwd, invocation: request => bind({ command: request.command })?.process };
          const route = child ? await inTrace(producerTrace, () => processBackend.prepareActorReplay(originalExecutor, { ...routeOptions,
            held: { realShell: '/bin/bash', executor: shellPath => processApi.adaptProcessToolOperations(processApi.createLocalBashOperations({ shellPath })),
              scope: () => ({ sessionID: `adoption-${selected}-${index}`, turnID: 'turn' }) } }, true)) : undefined;
          if (route) assert.equal(route.state, 'ready', route.detail);
          coordinator = new processApi.ProcessExecutionCoordinator(route?.executor ?? processBackend.completedReplayExecutor(originalExecutor, routeOptions));
          tool = processApi.createBashTool(cwd, { operations: coordinator.operations, shellPath: '/bin/bash', exposeSessionEnvironment: false,
            spawnHook: context => ({ ...context, env: { ...environment } }) });
          baseWorld = processApi.createLinuxProcessExecutionWorld({ workspaceSandbox: workspace, coordinator, tools: ['bash'],
            backend: processBackend, storeRoot, driver: 'git' });
          if (child && mode === 'running') {
            const execute = processBackend.executeAndPublish.bind(processBackend);
            processBackend.executeAndPublish = async (...parameters) => { entered.resolve(); await hold(parameters[0].signal); return execute(...parameters); };
          }
        } else baseWorld = ['write', 'edit'].includes(name) ? workspace.createExecutionWorld({ driver: 'git' })
          : createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: [name], maxBytes: () => 8 * 1024 * 1024 });
        const eligible = !nativeSearch && !(backend === 'local' && name === 'bash') &&
          (baseWorld.speculation.tools?.includes(name) ?? true);
        const world = { ...baseWorld, speculation: { ...baseWorld.speculation, execute: async context => {
          producerCalls++; if (!child) entered.resolve();
          if (mode === 'running' && !child) await hold(context.signal);
          return baseWorld.speculation.execute(context);
        } } };
        const host = createSpeculativeActionHost(`adoption-${selected}-${index}`, { cwd, executionWorlds: [world],
          getSettings: () => ({ enabled: true, drafterEnabled: true, drafterGateEnabled: false, drafterMaxDepth: 0,
            candidateLimit: 1, maxConcurrentActions: concurrency, tools: [name], resourceCacheMaxBytes: 16 * 1024 * 1024,
            patternAware: { enabled: false } }), resolveInvocation: (_tool, value) => bind(value), preflight: () => true,
          complete: async () => ({ role: 'assistant', api: model.api, provider: model.provider, model: model.id, timestamp: 0,
            content: [{ type: 'toolCall', id: 'fixture-call', name, arguments: draftArgs }], stopReason: 'toolUse', usage }),
          onActorActionSettled: ({ settlement: value }) => { settlement = value; },
          onEvent: event => {
            if (event.type === 'candidate' && ['succeeded', 'failed', 'cancelled'].includes(event.state.status)) {
              terminalState = event.state; readyAt = performance.now(); terminal.resolve();
            }
            if (event.type === 'prediction' && event.settlement.observation === 'unobserved') { terminalState = event.settlement; terminal.resolve(); }
          } });
        const turnID = 'turn', tools = [tool];
        try {
          await inTrace(producerTrace, () => host.startTurn({ turnID, actorModel: model, actorOptions: undefined, tools,
            context: { systemPrompt: 'Isolated adoption fixture; no model API.', messages: [], tools } }));
          if (eligible) await deadline(mode === 'ready' ? terminal.promise : Promise.race([entered.promise, terminal.promise]));
          const preparationMs = performance.now() - preparationBegin;
          assert.deepEqual(await state(root), baselineState, 'Speculative writes leaked before Actor adoption');
          trace = { begin: performance.now(), spans: [], release: () => released.resolve() };
          const run = () => host.execute({ turnID, id: 'actor-call', tool: name, args, tools }, new AbortController().signal,
            async operation => { fallbackCalls++; if (!child) released.resolve();
              return operation.invocation?.authoritative ? (await operation.invocation.authoritative({ args: operation.input, callID: operation.callID, signal: operation.signal })).result
                : tool.execute(operation.callID, operation.input, operation.signal); });
          currentActorTrace = trace;
          const result = await deadline(inTrace(trace, run)).finally(() => { currentActorTrace = undefined; });
          const returnedAt = performance.now(), adoptionMs = returnedAt - trace.begin, resultReadyAt = child ? childReadyAt : readyAt;
          await host.finishTurn(turnID);
          assert.deepEqual(wire(result), wire(expected), 'Actor output differs from the native oracle');
          assert.deepEqual(await state(root), expectedState, 'Actor file effects differ from the native oracle');
          assert.ok(settlement, 'Missing authoritative settlement');
          assert.equal(fallbackCalls, settlement.provider.kind === 'actor' ? 1 : 0, 'Duplicate authoritative execution');
          assert.equal(producerCalls, eligible ? 1 : 0, 'Unexpected producer execution count');
          trials.push({ index, nativeMs, preparationMs, adoptionMs, producerCalls, fallbackCalls,
            outcome: settlement.provider.kind, readyAtArrival: resultReadyAt !== undefined && resultReadyAt <= trace.begin,
            ...(resultReadyAt === undefined ? {} : { remainingProducerMs: Math.max(0, resultReadyAt - trace.begin), readyToReturnMs: returnedAt - Math.max(resultReadyAt, trace.begin) }),
            provider: settlement.provider, rejections: settlement.rejections,
            terminal: terminalState?.status ?? terminalState?.cause?.code,
            ...(processBackend ? { processMetrics: processBackend.metrics(), actorProcessMetrics: processBackend.actorMetrics() } : {}),
            ...(profiled ? { spans: trace.spans, producerSpans: producerTrace.spans } : {}) });
        } catch (error) {
          trials.push({ index, producerCalls, fallbackCalls, terminal: terminalState, error: { name: error.name, message: error.message },
            ...(trace ? { spans: trace.spans } : {}), ...(profiled ? { producerSpans: producerTrace.spans } : {}),
            ...(processBackend ? { processMetrics: processBackend.metrics(), actorProcessMetrics: processBackend.actorMetrics() } : {}) });
          throw error;
        } finally { released.resolve(); try { await host.dispose(); } finally { await coordinator?.dispose(); await workspace.dispose(); currentProducerTrace = undefined; } }
      }
    } finally { await profile?.pool.dispose(); }
    const hit = trial => child ? trial.actorProcessMetrics.hits > 0 : trial.outcome !== 'actor';
    const hitTimes = trials.filter(hit).map(trial => trial.adoptionMs), fallbackTimes = trials.filter(trial => !hit(trial)).map(trial => trial.adoptionMs);
    Object.assign(row, {
      p50Ms: percentile(trials.map(trial => trial.adoptionMs), .5), p95Ms: percentile(trials.map(trial => trial.adoptionMs), .95),
      hitP50Ms: hitTimes.length ? percentile(hitTimes, .5) : null, fallbackP50Ms: fallbackTimes.length ? percentile(fallbackTimes, .5) : null,
      nativeP50Ms: percentile(trials.map(trial => trial.nativeMs), .5), hits: hitTimes.length });
    console.log(JSON.stringify({ tool: selected, mode, profiled, p50Ms: row.p50Ms, p95Ms: row.p95Ms, hitP50Ms: row.hitP50Ms, fallbackP50Ms: row.fallbackP50Ms, nativeP50Ms: row.nativeP50Ms, hits: row.hits, repeats }));
  }
} finally {
  await fs.writeFile(reportPath, JSON.stringify({ platform: process.platform, node: process.version, backend, runtimeIdentity, mode, profiled, apiRequests: 0,
    scope: 'Full Host.execute entry to resolved Actor result. Producer preparation is separate. A deterministic proposal isolates adoption; this is not natural model/E2E evidence. Running mode releases the controlled producer after Actor joins, and reports remaining execution separately. Native search fallback is reported explicitly.', rows }, null, 2) + '\n', { flag: 'wx' });
  hooks?.deregister(); baselineHooks?.deregister(); delete globalThis.__adoptionTrace;
  assert.equal(path.dirname(owned), fixtureParent); assert.ok(path.basename(owned).startsWith('pi-adoption-audit-'));
  await fs.rm(owned, { recursive: true, force: true });
}
