import { expect, test } from "vitest";
import { lstat, lutimes, mkdir, mkdtemp, realpath, rm, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Worker } from "node:worker_threads";
import { FILESYSTEM_METADATA_FIELDS } from "../src/filesystem-evidence.ts";

// A fresh isolate cannot inherit a previously qualified callback from another
// test. No backend is constructed, and no backend readiness probe is involved.
const linux = test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC);
linux.for(["environment-directory", "configured-watcher"] as const)("initializes standalone readonly capture ($0)", { timeout: 15_000 }, async mode => {
  const fixture = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-lazy-")), root = path.join(fixture, "source");
  const file = path.join(root, "input"), pointer = path.join(root, "pointer");
  let worker: Worker | undefined;
  try {
    await mkdir(root); await writeFile(file, "source\n"); await symlink("input", pointer);
    await utimes(file, 915148800, 946684800); await utimes(root, 915148800, 946684800); await lutimes(pointer, 915148800, 946684800);
    const snapshot = () => Promise.all([root, file, pointer].map(async target => {
      const stat = await lstat(target, { bigint: true });
      return { target, fields: Object.fromEntries(FILESYSTEM_METADATA_FIELDS.map(field => [field, String(stat[field])])) };
    }));
    const before = await snapshot(), binary = await realpath(process.env.PI_SPEC_HELD_EXEC!);
    worker = new Worker(String.raw`
      const { parentPort, workerData } = require("node:worker_threads");
      (async () => {
        const startup = await import(workerData.startup);
        const filesystem = await import(workerData.filesystem);
        const { ResourceVersionManager } = await import(workerData.resources);
        const initial = Boolean(startup.nativeFilesystem);
        if (workerData.mode === "configured-watcher") startup.configureNativeFilesystem(workerData.binary);
        const manager = new ResourceVersionManager(workerData.root);
        let token, view;
        try {
          token = workerData.mode === "configured-watcher" ? await manager.observeChanges()
            : await manager.capture([{ path: workerData.root, scope: "names" }], 64 * 1024);
          view = await filesystem.openFilesystemReadView();
          const entries = await filesystem.readFilesystemDirectory(workerData.root, view);
          const link = await view.readlink(workerData.pointer);
          token ??= await manager.observeChanges();
          parentPort.postMessage({ initial, loaded: Boolean(startup.nativeFilesystem), binary: startup.nativeFilesystemBinary(),
            names: entries.map(entry => entry.name), link, watching: token.watching });
        } finally { await token?.release(); manager.close(); await view?.close(); }
      })().catch(error => { parentPort.postMessage({ error: String(error.stack ?? error) }); });
    `, { eval: true, workerData: { mode, binary, root, pointer,
      startup: new URL("../src/process-startup.mjs", import.meta.url).href,
      filesystem: new URL("../src/filesystem-evidence.ts", import.meta.url).href,
      resources: new URL("../src/resource-version.ts", import.meta.url).href },
      env: { ...process.env, PI_SPEC_HELD_EXEC: mode === "configured-watcher" ? path.join(fixture, "missing-default-helper") : binary } });
    const result = await new Promise<Record<string, unknown>>((resolve, reject) => {
      worker!.once("message", resolve); worker!.once("error", reject);
      worker!.once("exit", code => { if (code !== 0) reject(new Error(`standalone capture worker exited ${code}`)); });
    });
    expect(result).toEqual({ initial: false, loaded: true, binary, names: ["input", "pointer"], link: "input", watching: true });
    expect(await snapshot()).toEqual(before);
  } finally { await worker?.terminate(); await rm(fixture, { recursive: true, force: true }); }
});
