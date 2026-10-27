import assert from 'node:assert/strict';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { appendFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { createStore } from './store.mjs';
import { attachDelivery } from './sdk-delivery.mjs';

const [action, modules, cwd, mode, sessionFile] = process.argv.slice(2);
assert(['start', 'next', 'recover', 'retire', 'direct'].includes(action));
const agentDir = path.join(cwd, 'agent'); process.env.PI_CODING_AGENT_DIR = agentDir;
const checkout = path.dirname(modules), load = name => import(pathToFileURL(`${modules}/${name}.js`));
const sdk = await import(pathToFileURL(`${checkout}/node_modules/@earendil-works/pi-coding-agent/dist/index.js`));
const { fauxProvider, fauxAssistantMessage, fauxToolCall, fauxText } = await import(pathToFileURL(`${checkout}/node_modules/@earendil-works/pi-ai/dist/index.js`));
const { AgentExecutionInterrupted } = await import(pathToFileURL(`${checkout}/node_modules/@earendil-works/pi-agent-core/dist/index.js`));
const { createSpeculativeActionExtension } = await load('extension');
const { createSpeculativeActionHost } = await load('agent-integration');
const { currentExecutionScope } = await load('execution-world');
const { SessionManager, SettingsManager, DefaultResourceLoader, ModelRuntime, createAgentSession } = sdk;
const implementation = { ...JSON.parse(await readFile(new URL('./binding.json', import.meta.url), 'utf8')), toolProfile: mode };
assert.equal(sdk.VERSION, implementation.sdkVersion);
assert.equal(typeof sdk.AgentSession.prototype.resumeTools, 'function');
const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]();
const log = (event, name, data = {}) => console.log(JSON.stringify({ event, name, pid: process.pid, ...data }));
let bridge, session, closeTask, closedAt, duplicate, hostEntries = 0, hostApplications = 0;
const extensionErrors = [], effects = path.join(cwd, 'effects.jsonl');
const readEffects = async () => (await readFile(effects, 'utf8').catch(() => '')).trim().split('\n').filter(Boolean).map(JSON.parse);
const effect = async (name, call) => {
    await appendFile(effects, JSON.stringify({ name, pid: process.pid, ...(call ? { call } : {}) }) + '\n');
    log('trace', name, call ? { call } : {});
};
const phase = async (name, data = {}) => {
    if (mode === 'double-admit' && name === 'run_acquired' && !duplicate) {
        duplicate = session.prompt('Competing request before the first preflight').then(
            () => ({ rejected: false }), error => ({ rejected: true, interrupted: error instanceof AgentExecutionInterrupted, error: error.message }));
        log('trace', 'duplicate_finished', await duplicate);
    }
    if (mode === 'close-preparing' && name === 'publication_committed' && !closeTask) {
        closeTask = bridge.dispose().then(async () => {
            closedAt = { hostEntries, saved: savedCount(), effects: await readEffects() };
            log('trace', 'owner_closed', closedAt);
        });
        await new Promise(setImmediate); log('trace', 'close_observation', { returned: !!closedAt });
    }
    log('phase', name, data);
    const next = await lines.next(); assert(!next.done && next.value === 'continue', 'controller disconnected');
};
const status = readFileSync('/proc/self/status', 'utf8');
log('trace', 'deployment', { uid: process.getuid(), status: status.split('\n').filter(line => /^(Uid|CapPrm|CapEff|NoNewPrivs):/.test(line)) });
await mkdir(agentDir, { recursive: true });
if (action === 'start') await writeFile(path.join(agentDir, 'speculative-action.json'), JSON.stringify({
    enabled: true, drafterEnabled: false, patternAware: { enabled: false }, prepareObservedOperations: false,
}));
const manager = sessionFile ? SessionManager.open(sessionFile) : SessionManager.create(cwd, path.join(cwd, 'sessions'));
const savedCount = () => {
    const file = manager.getSessionFile();
    return file && existsSync(file) ? readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse)
        .filter(entry => entry.type === 'message' && entry.message.role === 'toolResult').length : 0;
};
const actor = fauxProvider({ provider: 'sdk-delivery-actor', models: [{ id: 'actor' }], tokensPerSecond: 4000 });
const toolName = 'qualification_sdk_effect', callID = 'sdk-call-1', batch = mode.startsWith('batch');
const calls = Array.from({ length: batch ? 2 : 1 }, (_, i) => [fauxText(`position:${i}`),
    fauxToolCall(toolName, { text: `call-${i}` }, { id: callID })]).flat();
actor.setResponses(['start', 'next'].includes(action) ? [fauxAssistantMessage(calls, { stopReason: 'toolUse' }),
    fauxAssistantMessage('SDK completed')] : [fauxAssistantMessage('SDK resumed')]);
const extension = createSpeculativeActionExtension({ createExecutionWorlds: () => [], createHost: (id, options) => {
    const host = createSpeculativeActionHost(id, { ...options,
        resources: { initial: { cpuCount: 1, idleCpuCount: 1 }, sample: () => new Promise(() => {}) } });
    return { ...host, execute: (call, signal, apply) => {
        hostEntries++; log('trace', 'host_entry', { callID: call.id });
        return host.execute(call, signal, async (...args) => {
            hostApplications++; log('trace', 'host_apply'); return apply(...args);
        });
    } };
} });
const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } }, { projectTrusted: true });
const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [pi => {
    for (const name of ['input', 'before_agent_start', 'tool_execution_start', 'tool_call'])
        pi.on(name, async event => { await effect(name, event.input?.text); return name === 'input' ? { action: 'continue' } : undefined; });
    pi.registerTool({ name: toolName, label: toolName, description: 'Owned SDK delivery qualification',
        parameters: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] },
        prepareArguments: args => {
            appendFileSync(effects, JSON.stringify({ name: 'prepare_arguments', pid: process.pid, call: args.text }) + '\n');
            log('trace', 'prepare_arguments', { call: args.text }); return args;
        },
        async execute(id, args, _signal, _onUpdate, context) {
            assert.equal(id, callID); assert.equal(currentExecutionScope()?.callID, id);
            assert.equal(currentExecutionScope()?.sessionID, context.sessionManager.getSessionId());
            await effect('tool', args.text);
            const result = { content: [fauxText(`raw:${args.text}:${process.pid}`)], details: { sourcePID: process.pid, call: args.text } };
            await phase('raw_ready', { call: args.text, sessionFile: manager.getSessionFile(), saved: savedCount() });
            if (mode === 'tool-failure') throw new Error('owned tool failure');
            return { ...result, terminate: mode === 'terminate' };
        },
    });
    pi.on('tool_result', async event => {
        if (event.toolName !== toolName) return;
        await effect('tool_result', event.input?.text);
        return { content: [fauxText('hook:' + event.content[0].text)], details: { ...event.details, hookPID: process.pid } };
    });
    pi.on('message_end', async event => {
        if (event.message.role !== 'toolResult') return;
        await effect('message_end', event.message.details?.call);
        return { message: { ...event.message, content: [fauxText('final:' + event.message.content[0].text)],
            details: { ...event.message.details, finalPID: process.pid } } };
    });
}, extension] });
await resourceLoader.reload();
const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, 'auth.json'), modelsPath: null, refreshOnCreate: false });
modelRuntime.registerNativeProvider(actor.provider);
({ session } = await createAgentSession({ cwd, agentDir, model: actor.getModel(), modelRuntime, settingsManager,
    resourceLoader, sessionManager: manager, tools: [toolName] }));
session.agent.toolExecution = mode.includes('parallel') ? 'parallel' : 'sequential';
if (mode !== 'terminate') session.agent.shouldStopAfterTurn = context => context.toolResults?.length > 0;
await session.bindExtensions({ mode: 'print', onError: error => { extensionErrors.push(error); } });
const store = mode === 'baseline' ? undefined : await createStore(modules, path.join(cwd, 'store'), { ticketCapacity: batch ? 2 : 1 });
if (store) bridge = await attachDelivery({ session, store, modules, phase, implementation });
const offSession = session.subscribe(event => {
    if (event.type === 'message_end' && event.message.role === 'toolResult') {
        log('trace', 'session_subscriber', { message: event.message, keys: Reflect.ownKeys(event.message), saved: savedCount() });
        if (mode === 'listener-failure') throw new Error('qualified listener failure before SDK append');
    }
});
const offAgent = session.agent.subscribe(event => {
    if (event.type === 'message_end' && event.message.role === 'toolResult') log('trace', 'agent_subscriber', { message: event.message, saved: savedCount() });
});
const result = {};
try {
    await phase('session_ready', { sessionFile: manager.getSessionFile(), sessionID: manager.getSessionId() });
    try {
        if (action === 'retire') {
            result.retired = [];
            for (const ticket of manager.getEntries().filter(entry => entry.type === 'custom' && entry.customType === 'qualification_sdk_ticket'))
                result.retired.push(await store.retire(ticket.data.execution));
        } else if (action === 'recover') await bridge.recover();
        else if (action === 'direct') await session.agent.prompt('Attempt unscoped direct Agent request.');
        else await session.prompt('Execute the owned SDK qualification tools.');
    } catch (error) {
        result.failure = { name: error.name, message: error.message, interrupted: error instanceof AgentExecutionInterrupted };
    }
    await closeTask;
    if (duplicate) result.duplicate = await duplicate;
    if (closedAt) {
        result.closedAt = closedAt; result.afterClose = [];
        const before = await readFile(manager.getSessionFile(), 'utf8');
        for (const [name, invoke] of [['prompt', () => session.prompt('After close')], ['recover', () => bridge.recover()]]) {
            try { await invoke(); result.afterClose.push({ name, rejected: false }); }
            catch (error) { result.afterClose.push({ name, rejected: true, error: error.message }); }
        }
        assert.equal(await readFile(manager.getSessionFile(), 'utf8'), before);
    }
    Object.assign(result, { sessionFile: manager.getSessionFile(), hostEntries, hostApplications, saved: savedCount(),
        messages: session.messages, effects: await readEffects(), extensionErrors,
        ...(store ? { stats: await store.executionStats() } : {}) });
} finally {
    await bridge?.dispose(); offSession(); offAgent();
    await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' });
    await session.close(); await store?.dispose();
}
log('result', action, result); process.stdin.destroy();
