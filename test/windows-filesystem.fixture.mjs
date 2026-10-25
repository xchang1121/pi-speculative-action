import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { once } from 'node:events';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

const work = path.dirname(fileURLToPath(import.meta.url));
const binary = path.resolve(work, '../vendor/filesystem-win32-x64.node');
const native = createRequire(import.meta.url)(binary);
if (!isMainThread) {
  const held = Array.from({ length: 32 }, () => native.pin(workerData.file));
  parentPort.on('message', () => parentPort.postMessage(native.identity(held[0])));
  parentPort.postMessage({ held: native.liveCount() });
} else {
  os.setPriority(0, os.constants.priority.PRIORITY_BELOW_NORMAL);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'native-id-lease-'));
  const results = [];
  const leases = new Set();
  const pin = target => { const lease = native.pin(target); leases.add(lease); return lease; };
  const close = lease => { native.close(lease); leases.delete(lease); };
  const snapshot = lease => {
    const bytes = native.identity(lease);
    assert.equal(bytes.length, 28);
    return { id: bytes.subarray(0, 12).toString('hex'), links: bytes.readUInt32LE(12),
      size: (BigInt(bytes.readUInt32LE(16)) << 32n | BigInt(bytes.readUInt32LE(20))).toString(), attributes: bytes.readUInt32LE(24) };
  };
  const run = async (name, body) => {
    try { results.push({ name, status: 'passed', ...await body() }); }
    catch (error) { results.push({ name, status: 'failed', error: { code: error.code, message: error.message, stack: error.stack } }); }
    finally { for (const lease of leases) close(lease); }
  };
  const fixture = name => {
    const directory = path.join(root, name, 'directory');
    fs.mkdirSync(directory, { recursive: true });
    const file = path.join(directory, 'input'); fs.writeFileSync(file, 'base');
    return { directory, file, moved: path.join(root, name, 'moved') };
  };
  let cleaned = false;
  try {
    await run('regular rename replace unlink and close', () => {
      const { directory, file, moved } = fixture('regular'), lease = pin(file), before = snapshot(lease);
      fs.renameSync(directory, moved);
      assert.equal(snapshot(lease).id, before.id);
      const target = path.join(moved, 'input'); fs.unlinkSync(target);
      assert.equal(snapshot(lease).links, 0);
      fs.writeFileSync(target, 'next'); const replacement = pin(target);
      assert.notEqual(snapshot(replacement).id, before.id);
      assert.equal(snapshot(lease).id, before.id);
      close(replacement); close(lease); native.close(lease);
      assert.throws(() => snapshot(lease), { code: 'WIN32_6' });
      return { id: before.id, renamed: true, deletedIdentityRetained: true, replacementDistinct: true, doubleCloseSafe: true };
    });
    await run('directory lease rename and delete', () => {
      const { directory, file, moved } = fixture('directory'), lease = pin(directory), before = snapshot(lease);
      fs.unlinkSync(file); fs.renameSync(directory, moved); fs.rmdirSync(moved);
      assert.equal(snapshot(lease).id, before.id);
      return { renamed: true, deletedDirectoryIdentityRetained: true };
    });
    await run('full hardlink capacity before and after pin', () => {
      const control = fixture('capacity-control'); let capacity = 1, limitCode;
      for (; capacity < 4096; capacity++) {
        try { fs.linkSync(control.file, path.join(control.directory, String(capacity))); }
        catch (error) { limitCode = error.code; break; }
      }
      assert.ok(capacity > 1 && capacity < 4096, JSON.stringify({ capacity, limitCode }));
      const cases = [];
      for (const existing of [true, false]) {
        const { directory, file, moved } = fixture(existing ? 'existing' : 'grow');
        let lease;
        if (!existing) lease = pin(file);
        for (let index = 1; index < capacity; index++) fs.linkSync(file, path.join(directory, String(index)));
        lease ??= pin(file);
        assert.equal(snapshot(lease).links, capacity);
        assert.throws(() => fs.linkSync(file, path.join(directory, 'overflow')), { code: limitCode });
        const before = snapshot(lease); fs.renameSync(directory, moved);
        for (const name of fs.readdirSync(moved)) fs.unlinkSync(path.join(moved, name));
        assert.equal(snapshot(lease).links, 0); assert.equal(snapshot(lease).id, before.id);
        close(lease); cases.push({ existing, capacityPreserved: true, renamed: true, lastNameDeleted: true });
      }
      return { capacity, limitCode, cases };
    });
    await run('invalid input retains no native handles', () => {
      const before = native.liveCount();
      assert.throws(() => native.identity({})); assert.throws(() => native.close({}));
      assert.throws(() => native.pin(path.join(root, 'absent')), { code: 'WIN32_2' });
      assert.throws(() => native.pin('bad\0path'), { code: 'ERR_LEASE_PATH' });
      assert.throws(() => native.pin(123), { code: 'ERR_LEASE_PATH' });
      assert.equal(native.liveCount(), before);
      return { foreignLeaseRejected: true, closedOnFailure: true };
    });
    await run('garbage collection closes owned leases', async () => {
      const { file } = fixture('collection');
      assert.equal(native.liveCount(), 0);
      for (let index = 0; index < 32; index++) native.pin(file);
      for (let attempt = 0; attempt < 20 && native.liveCount(); attempt++) { global.gc(); await nextTurn(); }
      assert.equal(native.liveCount(), 0);
      return { liveAfterCollection: native.liveCount() };
    });
    await run('worker termination closes owned leases', async () => {
      const { directory, file, moved } = fixture('worker');
      const worker = new Worker(new URL(import.meta.url), { workerData: { file } });
      try {
        const [ready] = await once(worker, 'message'); assert.equal(ready.held, 32); assert.equal(native.liveCount(), 32);
        fs.renameSync(directory, moved);
      } finally { await worker.terminate(); }
      assert.equal(native.liveCount(), 0);
      return { renamedWhileWorkerHeld: true, liveAfterTermination: native.liveCount() };
    });
  } finally {
    for (const lease of leases) close(lease);
    const resolved = path.resolve(root), allowed = path.resolve(os.tmpdir()) + path.sep;
    assert.ok(resolved.startsWith(allowed) && path.basename(resolved).startsWith('native-id-lease-'));
    fs.rmSync(resolved, { recursive: true, force: true }); cleaned = !fs.existsSync(resolved);
  }
  const report = { platform: process.platform, node: process.version, napi: process.versions.napi,
    binary, results, cleaned, liveLeases: native.liveCount(), scope: 'Packaged Windows x64 NTFS N-API object leases.' };
  process.stdout.write(JSON.stringify(report) + '\n');
  process.exitCode = results.some(row => row.status !== 'passed') || !cleaned || native.liveCount() !== 0 ? 1 : 0;
}
