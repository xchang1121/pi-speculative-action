// Qualification adapter. It does not enable production continuation certificates.
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { realpath } from 'node:fs/promises';
const table = 'qualification_execution_tickets', namespace = 'qualification_ticket_namespace';

export async function createStore(modules, root, options = {}) {
    const capacity = options.ticketCapacity ?? 1;
    if (!Number.isSafeInteger(capacity) || capacity < 1) throw new Error('positive ticket capacity required');
    const module = name => import(pathToFileURL(`${modules}/${name}.js`));
    const { ProvenanceCertificateStore } = await module('reuse-store');
    const { digestObject } = await module('stable-value-hash');
    const { immutableSnapshot, isImmutableSnapshot, stableStringify } = await module('stable-json');
    const { effectCommitFailure, EffectCommitFailure } = await module('effect-transaction');
    const fail = message => { throw effectCommitFailure(new Error(message), 'poisoned'); };
    const originalExec = DatabaseSync.prototype.exec;
    DatabaseSync.prototype.exec = function (sql) {
        try { return originalExec.call(this, sql); }
        catch (error) {
            if ([5, 6].includes(Number(error.errcode) & 0xff))
                console.log(JSON.stringify({ event: 'observed', name: 'sqlite_busy', pid: process.pid, sql }));
            throw error;
        }
    };
    class TicketStore extends ProvenanceCertificateStore {
        async open(signal) {
            const database = await super.open(signal);
            try {
                database.exec('PRAGMA synchronous=FULL');
                if (database.prepare('PRAGMA synchronous').get().synchronous !== 2) throw new Error('FULL connection required');
                this.authorityOrigin = await realpath(this.root); return database;
            } catch (error) { database.close(); throw error; }
        }
        space(database) {
            const current = database.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(namespace)
                ? database.prepare(`SELECT * FROM ${namespace} WHERE singleton=1`).get() : undefined;
            if (current && current.origin !== this.authorityOrigin) fail('execution issuer context changed');
            return current;
        }
        row(database, execution) {
            const current = this.space(database), parts = typeof execution === 'string' ? execution.split(':') : [];
            const sequence = Number(parts[1]);
            if (!current || parts.length !== 2 || parts[0] !== current.id || !Number.isSafeInteger(sequence) ||
                sequence < 1 || String(sequence) !== parts[1] || sequence > current.last) fail('execution namespace or sequence changed');
            return database.prepare(`SELECT * FROM ${table} WHERE sequence=?`).get(sequence) ?? fail('execution ticket retired or missing');
        }
        issue(context, phase, loadReference = () => undefined) {
            if (typeof context !== 'string' || !context || context.length > 128) fail('invalid execution context');
            return this.exclusive(async database => {
                database.exec(`CREATE TABLE IF NOT EXISTS ${namespace}(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    id TEXT NOT NULL, last INTEGER NOT NULL, capacity INTEGER NOT NULL, origin TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS ${table}(sequence INTEGER PRIMARY KEY, context TEXT NOT NULL UNIQUE, identity TEXT,
                    record TEXT, state TEXT NOT NULL, token TEXT, outcome TEXT, receipt TEXT, acknowledged INTEGER NOT NULL DEFAULT 0,
                    CHECK(state IN ('issued','available','committing','consumed','poisoned')))`);
                database.prepare(`INSERT OR IGNORE INTO ${namespace} VALUES(1, ?, 0, ?, ?)`).run(randomUUID(), capacity, this.authorityOrigin);
                const current = this.space(database), referenced = await loadReference();
                const existing = referenced !== undefined ? this.row(database, referenced?.execution) : database.prepare(`SELECT * FROM ${table} WHERE context=?`).get(context);
                if (existing) {
                    if (existing.context !== context) fail('execution request context changed');
                    const execution = `${current.id}:${existing.sequence}`;
                    await phase('issue_reused', { execution, referenced: !!referenced }); return execution;
                }
                const count = database.prepare(`SELECT count(*) AS count FROM ${table}`).get().count;
                if (count >= current.capacity) fail('execution ticket capacity exhausted');
                const sequence = current.last + 1;
                if (!Number.isSafeInteger(sequence)) fail('execution sequence exhausted');
                database.prepare(`UPDATE ${namespace} SET last=? WHERE singleton=1`).run(sequence);
                database.prepare(`INSERT INTO ${table}(sequence,context,state) VALUES (?, ?, 'issued')`).run(sequence, context);
                const execution = `${current.id}:${sequence}`;
                await phase('issue_staged', { execution }); return execution;
            });
        }
        inspect(execution) { return this.exclusive(async database => this.row(database, execution), [], false); }
        preflightRun(operation) {
            return this.exclusive(async database => {
                const current = this.space(database);
                if (current && current.capacity !== capacity) fail('execution ticket capacity changed');
                if (operation === 'prompt' && current &&
                    database.prepare(`SELECT count(*) AS count FROM ${table}`).get().count >= current.capacity)
                    fail('execution ticket capacity exhausted');
            }, [], false);
        }
        poisonAbandoned(execution, record, context) {
            return this.exclusive(async database => {
                const row = this.row(database, execution);
                if (row.record !== record || row.context !== context) fail('abandoned request binding changed');
                if (row.state === 'committing') database.prepare(`UPDATE ${table} SET state='poisoned', token=NULL WHERE sequence=?`).run(row.sequence);
            });
        }
        executionStats() {
            return this.exclusive(async database => {
                const current = this.space(database);
                return { namespace: current, records: current ? database.prepare(`SELECT count(*) AS count FROM ${table}`).get().count : 0,
                    databaseBytes: database.prepare('PRAGMA page_count').get().page_count * database.prepare('PRAGMA page_size').get().page_size };
            }, [], false);
        }
        retire(execution) {
            return this.exclusive(async database => {
                const row = this.row(database, execution);
                if (!['consumed', 'poisoned'].includes(row.state) ||
                    ((row.state === 'consumed' || row.outcome !== null) && row.acknowledged !== 1))
                    fail(`execution ticket cannot retire: ${row.state}, acknowledged=${row.acknowledged}`);
                database.prepare(`DELETE FROM ${table} WHERE sequence=?`).run(row.sequence);
                return { sequence: row.sequence, state: row.state };
            });
        }
        async publish(execution, payload, artifacts, produce, phase) {
            const owned = immutableSnapshot({ payload, artifacts }), identity = digestObject(owned);
            const record = await this.withPublication(() => this.exclusive(async database => {
                const previous = this.row(database, execution);
                if (previous.context !== payload.requestId || payload.executionId !== execution) fail('execution publication context changed');
                if (previous.state !== 'issued') {
                    if (previous.identity !== identity) fail('checkpoint publication conflict');
                    const saved = await this.getInputVersion(previous.record);
                    if (!saved) fail('checkpoint data retired');
                    if (digestObject({ payload: saved.payload, artifacts: saved.artifacts }) !== identity) fail('checkpoint identity mismatch');
                    await phase('publication_reused', { recordId: saved.id }); return saved;
                }
                await produce(owned.artifacts);
                const saved = await this.putInputVersion(owned.payload, owned.artifacts);
                database.prepare(`UPDATE ${table} SET identity=?, record=?, state='available' WHERE sequence=?`)
                    .run(identity, saved.id, previous.sequence);
                await phase('publication_staged', { recordId: saved.id }); return saved;
            }));
            await phase('publication_committed', { recordId: record.id }); return record;
        }
        receipt(row, execution, record, context) {
            if (row.record !== record || row.context !== context) fail('Actor request binding changed');
            if (row.state !== 'consumed' && !(row.state === 'poisoned' && row.outcome !== null)) return;
            const outcome = immutableSnapshot(JSON.parse(row.outcome));
            if (!outcome || !isImmutableSnapshot(outcome) || row.receipt !== digestObject(outcome) || outcome.schema !== 2 || outcome.disposition !== row.state || outcome.execution !== execution ||
                outcome.record !== record || outcome.context !== context) fail('Actor receipt integrity failed');
            return immutableSnapshot({ ...outcome, digest: row.receipt });
        }
        result(execution, record, context) {
            return this.exclusive(async database => this.receipt(this.row(database, execution), execution, record, context), [], false);
        }
        acknowledge(execution, record, context, digest, persist) {
            return this.exclusive(async database => {
                const row = this.row(database, execution), receipt = this.receipt(row, execution, record, context);
                if (!receipt || receipt.digest !== digest) fail('Actor receipt acknowledgement changed');
                await persist(receipt.result);
                if (database.prepare(`UPDATE ${table} SET acknowledged=1 WHERE sequence=? AND state=?`).run(row.sequence, row.state).changes !== 1)
                    fail('Actor receipt acknowledgement authority changed');
                return { digest, acknowledged: true };
            });
        }
        commit(execution, record, context, apply, phase) {
            return this.lifecycle.hold(async settlement => {
                const token = randomUUID(); let claimed = false, settled = false;
                const settle = async (state, result) => {
                    let outcome = null, receipt = null;
                    if (result !== undefined) {
                        if (!['consumed', 'poisoned'].includes(state)) fail('receipt requires a terminal execution disposition');
                        const owned = immutableSnapshot({ schema: 2, execution, record, context, disposition: state, result });
                        if (!isImmutableSnapshot(owned)) fail('Actor outcome must be lossless pure data');
                        outcome = stableStringify(owned); receipt = digestObject(owned);
                        if (Buffer.byteLength(outcome) > 65536) fail('Actor outcome exceeds receipt capacity');
                    }
                    await this.exclusive(async database => {
                        const row = this.row(database, execution);
                        const changed = database.prepare(`UPDATE ${table} SET state=?, token=NULL, outcome=?, receipt=?
                            WHERE sequence=? AND record=? AND state='committing' AND token=?`).run(state, outcome, receipt, row.sequence, record, token);
                        if (changed.changes !== 1) throw new Error('claim settlement authority changed');
                        await phase('before_settlement_commit', { state, token, receipt });
                    }, [], true, undefined, settlement);
                    settled = true;
                    await phase('after_settlement_commit', { state, token, receipt });
                };
                try {
                    this.signal.throwIfAborted();
                    await this.exclusive(async database => {
                        const row = this.row(database, execution);
                        const receipt = this.receipt(row, execution, record, context);
                        if (receipt) fail('execution already finalized; recover its receipt');
                        if (row.record !== record || row.state !== 'available') fail(`execution unavailable: ${row.state}`);
                        if (database.prepare(`UPDATE ${table} SET state='committing', token=?
                            WHERE sequence=? AND state='available'`).run(token, row.sequence).changes !== 1)
                            throw new Error('claim acquisition authority changed');
                        await phase('before_claim_commit', { token });
                    });
                    claimed = true; await phase('after_claim', { token });
                    const outcome = await apply();
                    if (!outcome || !['consumed', 'poisoned'].includes(outcome.disposition) || outcome.result === undefined)
                        fail('finalized execution outcome required');
                    await phase('after_effect', outcome);
                    await settle(outcome.disposition, outcome.result); return outcome.result;
                } catch (error) {
                    try { if (claimed && !settled) await settle(error instanceof EffectCommitFailure && error.disposition === 'recoverable' ? 'available' : 'poisoned'); }
                    catch (settlementError) { throw effectCommitFailure(settlementError, 'poisoned'); }
                    throw effectCommitFailure(error, 'poisoned');
                }
            }).catch(error => { throw effectCommitFailure(error, 'poisoned'); });
        }
    }
    return new TicketStore(root, { gcIntervalMs: 0, orphanGraceMs: 0, ...options });
}
