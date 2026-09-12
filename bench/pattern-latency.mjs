import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { registerHooks } from 'node:module';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';

const [repository, output, baseline, traceFile] = process.argv.slice(2);
assert.ok(repository && output, 'Usage: node bench/pattern-latency.mjs REPO REPORT [OLD_COMPILED_PATTERN_MODULE] [EVENTS_JSON]');
const repo = path.resolve(repository), moduleURL = pathToFileURL(path.join(repo, 'dist/pattern-aware.js')).href;
assert.equal(fs.existsSync(output), false);
const hash = value => createHash('sha256').update(value).digest('hex');
const modules = { current: await import(moduleURL) };
if (baseline) {
  const hook = registerHooks({ load(url, context, next) {
    return url === moduleURL + '?baseline'
      ? { format: 'module', source: fs.readFileSync(baseline, 'utf8'), shortCircuit: true }
      : next(url, context);
  } });
  try { modules.baseline = await import(moduleURL + '?baseline'); } finally { hook.deregister(); }
}
function fixture(width) {
  return Array.from({ length: 12 }, (_, index) => {
    const sessionID = 'session-' + Math.floor(index / 3), turnID = 'turn-' + index;
    const selected = index % Math.max(1, width), paths = Array.from({ length: Math.max(1, width) }, (_, item) => `src/task-${index}/file-${item}.ts`);
    return [
      { sessionID, turnID, tool: 'inspect', input: { query: 'symbol-' + index }, output: width ? { results: paths.map((path, item) => ({ path, line: item + 1 })) } : { nextPath: paths[0] }, outcome: 'success', durationMs: 10 },
      { sessionID, turnID: turnID + ':read', tool: 'read', input: { path: paths[selected], offset: selected + 1 }, output: { nextPath: `tests/task-${index}.test.ts` }, outcome: 'success', durationMs: 40 },
      { sessionID, turnID: turnID + ':test', tool: 'inspect', input: { path: `tests/task-${index}.test.ts` }, output: { passed: true }, outcome: 'success', durationMs: 80 },
    ];
  }).flat();
}
async function run(module, events) {
  const settings = { ...module.PATTERN_AWARE_DEFAULTS, maxPatterns: 128, maxContextLength: 2, maxFutureGap: 1 };
  const store = new module.PatternAwareStore(settings), traces = [];
  let observeMs = 0, predictMs = 0, predictions = 0;
  const started = performance.now();
  for (const event of events) {
    const batch = [event].flat();
    let phase = performance.now();
    if (Array.isArray(event)) store.observeBatch(event); else store.observe(event);
    observeMs += performance.now() - phase;
    phase = performance.now(); const frontier = store.predict(batch[0].sessionID); predictMs += performance.now() - phase;
    predictions += frontier.length; traces.push(frontier);
  }
  const state = store.snapshot();
  for (const sessionID of new Set(events.flat().map(event => event.sessionID))) store.finishSession(sessionID);
  await store.flush();
  return { evidence: { traces, state }, elapsedMs: performance.now() - started, observeMs, predictMs,
    events: events.flat().length, batches: events.length, patterns: state.length, predictions };
}
const fixtures = traceFile ? { recorded: JSON.parse(fs.readFileSync(traceFile, 'utf8')) } : Object.fromEntries([0, 96, 512].map(width => [width, fixture(width)]));
const rows = [], evidence = [];
for (const [name, events] of Object.entries(fixtures)) for (let round = 0; round < 5; round++) {
  const results = {};
  for (const label of round % 2 ? Object.keys(modules).reverse() : Object.keys(modules)) {
    const { evidence: detail, ...metrics } = await run(modules[label], events);
    results[label] = detail; rows.push({ name, round, label, ...metrics });
  }
  if (baseline) assert.deepEqual(results.current, results.baseline, `prediction and learning state: ${name}/${round}`);
  evidence.push({ name, round, sha256: hash(JSON.stringify(results.current)) });
}
const median = values => [...values].sort((left, right) => left - right)[Math.floor(values.length / 2)];
const summary = Object.keys(fixtures).map(name => ({ name, ...Object.fromEntries(Object.keys(modules).map(label => [label,
  Object.fromEntries(['elapsedMs', 'observeMs', 'predictMs'].map(metric => [metric, median(rows.filter(row => row.name === name && row.label === label).map(row => row[metric]))]))])) }));
const result = { method: 'PatternAware component comparison over identical observe/predict histories. Alternating order; all frontiers and learning snapshots must match exactly. Timed lifecycle includes snapshots and flush, excludes module loading and fixture creation. Constructed fixtures unless EVENTS_JSON is supplied; not Agent end-to-end acceleration.',
  repo, platform: process.platform, node: process.version, apiRequests: 0,
  currentSha256: hash(fs.readFileSync(path.join(repo, 'dist/pattern-aware.js'))),
  ...(baseline ? { baselineSha256: hash(fs.readFileSync(baseline)) } : {}), rows, evidence, summary, passed: true };
fs.writeFileSync(output, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
console.log(JSON.stringify({ output, summary, passed: true }));
