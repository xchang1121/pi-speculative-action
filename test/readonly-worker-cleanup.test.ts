import { expect, test } from "vitest";
import { fstatSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { Worker } from "node:worker_threads";
import path from "node:path";
import os from "node:os";

test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC).for(["root", "open", "adopted", "returned", "throw"] as const)(
  "releases native and adopted descriptors when a worker terminates after %s", { timeout: 30_000 }, async mode => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-worker-")), target = path.join(directory, "input");
    const waiting = new SharedArrayBuffer(4);
    let worker: Worker | undefined;
    try {
      await writeFile(target, "retained inode\n");
      worker = new Worker(`
        const { parentPort, workerData } = require("node:worker_threads");
        const { openSync } = require("node:fs");
        const native = require(workerData.library), waiting = new Int32Array(workerData.waiting);
        const stop = descriptors => {
          Atomics.store(waiting, 0, 1); parentPort.postMessage(descriptors);
          Atomics.wait(waiting, 0, 1);
        };
        const adopt = fd => openSync("/proc/self/fd/" + fd, 0x200000);
        if (native(-11) !== 2) throw new Error("synchronous descriptor adoption unavailable");
        if (workerData.mode === "throw") {
          let descriptor, caught = false;
          try { native(-5, Buffer.alloc(0), fd => { descriptor = fd; throw new Error("adoption interrupted"); }); }
          catch (error) { caught = error.message === "adoption interrupted"; }
          if (!caught) throw new Error("adoption failure was lost");
          stop([descriptor]);
        } else {
          const root = native(-5, Buffer.alloc(0), fd => workerData.mode === "root" ? stop([fd]) : adopt(fd));
          if (!Number.isInteger(root) || root < 0) throw new Error("read-only root unavailable: " + root);
          const request = Buffer.alloc(Buffer.byteLength(workerData.target) + 9);
          request.writeInt32LE(root); request.writeUInt32LE(0x200000, 4); request.write(workerData.target, 8);
          const opened = native(-6, request, fd => workerData.mode === "returned" ? adopt(fd)
            : stop([root, fd, ...(workerData.mode === "adopted" ? [adopt(fd)] : [])]));
          if (!Number.isInteger(opened) || opened < 0) throw new Error("read-only open unavailable: " + opened);
          stop([root, opened]);
        }
      `, { eval: true, workerData: { mode, target, waiting, library: `${process.env.PI_SPEC_HELD_EXEC}.node` } });
      const descriptors = await new Promise<number[]>((resolve, reject) => {
        worker!.once("message", resolve); worker!.once("error", reject);
      });
      expect(Atomics.load(new Int32Array(waiting), 0)).toBe(1);
      expect(descriptors).toHaveLength(mode === "root" || mode === "throw" ? 1 : mode === "adopted" ? 3 : 2);
      for (const descriptor of descriptors) if (mode === "throw") expect(() => fstatSync(descriptor)).toThrow(/EBADF/);
      else expect(fstatSync(descriptor).isDirectory() || fstatSync(descriptor).isFile()).toBe(true);
      expect(await worker.terminate()).toBe(1);
      for (const descriptor of descriptors) {
        let error: NodeJS.ErrnoException | undefined;
        try { fstatSync(descriptor); } catch (caught) { error = caught as NodeJS.ErrnoException; }
        expect(error?.code, `descriptor ${descriptor} survived worker termination`).toBe("EBADF");
      }
    } finally {
      await worker?.terminate();
      await rm(directory, { recursive: true, force: true });
    }
  },
);
