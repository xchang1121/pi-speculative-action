// Qualification adapter for the companion SDK's public execution controller.
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { isDeepStrictEqual } from 'node:util';
import { pathToFileURL } from 'node:url';
const ticketType = 'qualification_sdk_ticket';
const deferred = () => {
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    promise.catch(() => {}); return { promise, resolve, reject };
};

export async function attachDelivery({ session, store, modules, phase, implementation }) {
    const { digestObject } = await import(pathToFileURL(`${modules}/stable-value-hash.js`));
    const { immutableSnapshot, isImmutableSnapshot, cloneSharedData } = await import(pathToFileURL(`${modules}/stable-json.js`));
    const { RuntimeLifecycleLane } = await import(pathToFileURL(`${modules}/runtime-lifecycle.js`));
    const lifetime = new RuntimeLifecycleLane();
    const manager = session.sessionManager;
    assert(manager.isPersisted() && manager.getSessionFile() && typeof session.setExecutionController === 'function');
    assert(implementation && isImmutableSnapshot(immutableSnapshot(implementation)), 'explicit implementation binding required');
    let activeLease;
    const own = value => {
        const shared = cloneSharedData(value);
        assert(Reflect.ownKeys(shared).every(key => typeof key === 'string'), 'journal message keys must be strings');
        const message = Object.fromEntries(Object.entries(shared).filter(([, item]) => item !== undefined));
        const snapshot = immutableSnapshot(message);
        assert(isImmutableSnapshot(snapshot) && isDeepStrictEqual(snapshot, message), 'lossless final message required');
        return snapshot;
    };
    const identity = request => {
        const entry = manager.getEntry(request.assistantEntryId);
        assert.equal(request.sessionId, manager.getSessionId());
        assert.equal(entry?.type, 'message'); assert.equal(entry.message.role, 'assistant');
        const call = entry.message.content[request.index];
        assert.equal(call?.type, 'toolCall'); assert(isDeepStrictEqual(call, request.toolCall));
        assert(manager.getBranch().some(item => item.id === entry.id), 'request left the current branch');
        const expected = { session: request.sessionId, entry: entry.id, index: request.index };
        return { ...expected, context: digestObject({ ...expected, call }) };
    };
    const reference = (context, optional = false) => {
        const refs = manager.getEntries().filter(entry => entry.type === 'custom' && entry.customType === ticketType).map(ticket => {
            const ref = ticket.data, entry = manager.getEntry(ref.entry);
            assert.equal(ref.session, manager.getSessionId()); assert.equal(ref.root, store.root);
            assert.equal(ref.protocol, 'sdk-scope-v1'); assert.equal(entry?.type, 'message');
            assert.equal(entry.message.role, 'assistant'); const call = entry.message.content[ref.index];
            assert.equal(call?.type, 'toolCall');
            assert.equal(digestObject({ session: ref.session, entry: ref.entry, index: ref.index, call }), ref.context, 'request reference integrity');
            return ref;
        }).filter(ref => ref.context === context);
        assert(refs.length <= 1, 'duplicate durable request binding');
        if (!refs.length && optional) return;
        assert.equal(refs.length, 1, 'durable request binding missing'); return refs[0];
    };
    async function prepare(request) {
        const expected = identity(request);
        const refresh = () => { assert.equal(identity(request).context, expected.context); return reference(expected.context, true); };
        const execution = await store.issue(expected.context, phase, refresh);
        const record = await store.publish(execution, {
            executionId: execution, requestId: expected.context, profile: 'sdk-scope-v1', implementation,
        }, [], async () => {}, phase);
        const ref = await store.withPublication(async () => {
            let ref = refresh();
            if (!ref) {
                ref = { ...expected, execution, record: record.id, root: store.root, protocol: 'sdk-scope-v1' };
                manager.appendCustomEntry(ticketType, ref);
            }
            assert.equal(ref.execution, execution); assert.equal(ref.record, record.id);
            assert.equal((await store.inspect(execution)).context, expected.context);
            return ref;
        });
        await phase('request_prepared', { ...ref, sessionFile: manager.getSessionFile() });
        return ref;
    }
    const controller = {
        acquire(request) {
            const ready = deferred(), released = deferred();
            const claims = new Map(); let lease, closed = false;
            const job = lifetime.hold(async settlement => {
                try {
                    await mkdir(store.root, { recursive: true });
                    assert.equal(request.sessionId, manager.getSessionId());
                    assert.equal(request.sessionFile, manager.getSessionFile());
                    if (activeLease) {
                        assert.equal(activeLease.session, request.sessionId);
                        assert(!['prompt', 'resume'].includes(request.operation) || session.isStreaming, 'one active SDK admission required');
                        assert.notEqual(request.operation, 'resume', 'cannot resume an active owner');
                        lease = activeLease; lease.users++;
                    } else {
                        const database = new DatabaseSync(path.join(store.root, 'actor-run.sqlite'));
                        try { database.exec('PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE'); }
                        catch (error) { database.close(); throw error; }
                        lease = activeLease = { database, session: request.sessionId, users: 1 };
                    }
                    await store.preflightRun(request.operation);
                    const assertActive = () => {
                        assert(!closed && activeLease === lease && lease.database.isTransaction, 'run authority is closed');
                        assert.equal(request.sessionId, manager.getSessionId());
                        assert.equal(request.sessionFile, manager.getSessionFile());
                    };
                    await phase('run_acquired', { operation: request.operation, session: request.sessionId });
                    ready.resolve({
                        assertActive,
                        async beforeToolCall(call, signal) {
                            assertActive(); signal?.throwIfAborted();
                            const ref = await lifetime.serialize(() => prepare(call), settlement);
                            const receipt = await store.result(ref.execution, ref.record, ref.context);
                            if (receipt) {
                                assert.equal(receipt.result.message.toolCallId, call.toolCall.id);
                                assert.equal(receipt.result.message.toolName, call.toolCall.name);
                                if (call.completed) assert.equal(digestObject(own(call.completed)), digestObject(receipt.result.message));
                                return receipt.result;
                            }
                            assert(!call.completed, 'saved result lacks a durable receipt');
                            const row = await store.inspect(ref.execution);
                            if (row.state === 'committing') {
                                await store.poisonAbandoned(ref.execution, ref.record, ref.context);
                                throw new Error('abandoned committing request is poisoned; effects are unknown');
                            }
                            assert.equal(row.state, 'available', 'request has no resumable outcome');
                            assert(!claims.has(ref.context), 'duplicate live tool claim');
                            const started = deferred(), final = deferred();
                            const pending = { ref, final, job: undefined }; claims.set(ref.context, pending);
                            pending.job = lifetime.track(store.commit(ref.execution, ref.record, ref.context, async () => {
                                assertActive(); started.resolve(); return final.promise;
                            }, phase));
                            pending.job.catch(error => { started.reject(error); final.reject(error); });
                            await started.promise; assertActive();
                        },
                        async persistToolResult(call, result, append) {
                            assertActive();
                            const ref = reference(identity(call).context), pending = claims.get(ref.context);
                            const final = immutableSnapshot({ message: own(result.message), terminate: result.terminate });
                            assert(isImmutableSnapshot(final) && typeof final.terminate === 'boolean');
                            if (pending) {
                                pending.final.resolve({ disposition: final.message.isError ? 'poisoned' : 'consumed', result: final });
                                await pending.job;
                            }
                            const receipt = await store.result(ref.execution, ref.record, ref.context);
                            assert(receipt && digestObject(receipt.result) === digestObject(final), 'final receipt changed');
                            await phase('final_receipt_ready', { execution: ref.execution, index: ref.index, receipt: final });
                            await store.acknowledge(ref.execution, ref.record, ref.context, receipt.digest, async saved => {
                                assertActive(); assert.equal(digestObject(saved), digestObject(final));
                                const entry = append();
                                await phase('sdk_result_saved', { entry, index: ref.index });
                            });
                            await phase('delivery_acknowledged', { execution: ref.execution, index: ref.index });
                        },
                        async close(error) {
                            for (const pending of claims.values()) pending.final.reject(error ?? new Error('SDK run ended before final receipt'));
                            const outcomes = await Promise.allSettled([...claims.values()].map(pending => pending.job));
                            closed = true; released.resolve(); await job;
                            const failure = outcomes.find(outcome => outcome.status === 'rejected');
                            if (!error && failure) throw failure.reason;
                        },
                    });
                    await released.promise;
                } finally {
                    if (lease && --lease.users === 0) {
                        try { lease.database.exec('ROLLBACK'); }
                        finally { lease.database.close(); activeLease = undefined; }
                    }
                }
            });
            job.catch(error => ready.reject(error));
            return ready.promise;
        },
    };
    session.setExecutionController(controller);
    return {
        recover: () => session.resumeTools(),
        dispose: () => lifetime.close(async () => { await lifetime.drain(); assert(!activeLease); }),
    };
}
