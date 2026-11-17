import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { isMainThread, parentPort, Worker, workerData } from "node:worker_threads";
import { windowsFilesystem } from "../src/windows-filesystem.ts";
import { resolveSpeculativeConfig as resolveConfig, withSpeculativeConfig } from "../src/config-schema.ts";

const resolveSpeculativeConfig = (input = {}) =>
	resolveConfig({
		...input,
		filesystem: { objectLeaseIdleMs: 0, ...input.filesystem },
	});

const provider = windowsFilesystem();
await withSpeculativeConfig(resolveSpeculativeConfig(), async () => {
	if (process.argv.includes("--idle-child")) {
		await withSpeculativeConfig(
			resolveSpeculativeConfig({ filesystem: { objectLeaseIdleMs: 30000 } }),
			async () => {
				const lease = await provider.pin(process.argv.at(-1));
				await provider.close(lease);
				process.stdout.write(JSON.stringify(provider.processIDs()) + "\n");
			},
		);
	} else if (!isMainThread) {
		const held = await Promise.all(Array.from({ length: 32 }, () => provider.pin(workerData.file)));
		parentPort.on("message", async () => parentPort.postMessage(await provider.identity(held[0])));
		parentPort.postMessage({ held: await provider.liveCount(), processes: provider.processIDs() });
	} else {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-file-id-lease-"));
		const results = [];
		const leases = new Set();
		const pin = async (target) => {
			const lease = await provider.pin(target);
			leases.add(lease);
			return lease;
		};
		const close = async (lease) => {
			await provider.close(lease);
			leases.delete(lease);
		};
		const snapshot = async (lease) => {
			const bytes = await provider.identity(lease);
			assert.equal(bytes.length, 28);
			return {
				id: bytes.subarray(0, 12).toString("hex"),
				links: bytes.readUInt32LE(12),
				size: ((BigInt(bytes.readUInt32LE(16)) << 32n) | BigInt(bytes.readUInt32LE(20))).toString(),
				attributes: bytes.readUInt32LE(24),
			};
		};
		const run = async (name, body) => {
			try {
				results.push({ name, status: "passed", ...(await body()) });
			} catch (error) {
				results.push({ name, status: "failed", error: { code: error.code, message: error.message } });
			} finally {
				await Promise.all([...leases].map(close));
			}
		};
		const fixture = (name) => {
			const directory = path.join(root, name, "directory");
			fs.mkdirSync(directory, { recursive: true });
			const file = path.join(directory, "input");
			fs.writeFileSync(file, "base");
			return { directory, file, moved: path.join(root, name, "moved") };
		};
		const running = (pid) => {
			try {
				process.kill(pid, 0);
				return true;
			} catch (error) {
				if (error.code !== "ESRCH") throw error;
				return false;
			}
		};
		let cleaned = false;
		try {
			await run("idle helpers serve serial captures and retire after the configured timeout", async () => {
				const { file } = fixture("idle");
				const config = resolveSpeculativeConfig({ filesystem: { objectLeaseIdleMs: 200 } });
				await withSpeculativeConfig(config, async () => {
					let processID;
					for (let index = 0; index < 8; index++) {
						const lease = await pin(file);
						await snapshot(lease);
						processID ??= provider.processIDs()[0];
						assert.deepEqual(provider.processIDs(), [processID]);
						await close(lease);
						assert.deepEqual(provider.processIDs(), [processID]);
					}
					for (let attempt = 0; attempt < 200 && provider.processIDs().length; attempt++) await delay(10);
					assert.deepEqual(provider.processIDs(), []);
				});
				return { serialCaptures: 8, helperStarts: 1, idleRetired: true };
			});
			await run("parent exit cleans an idle helper without waiting for the idle timeout", async () => {
				const { file } = fixture("parent-exit");
				const child = spawn(
					process.execPath,
					[fileURLToPath(new URL(import.meta.url)), "--idle-child", file],
					{
						stdio: ["ignore", "pipe", "pipe"],
						windowsHide: true,
					},
				);
				let stdout = "";
				child.stdout.on("data", (bytes) => {
					stdout += bytes;
				});
				const timeout = setTimeout(() => child.kill(), 5000);
				try {
					const [code] = await once(child, "close");
					assert.equal(code, 0);
					const processes = JSON.parse(stdout);
					assert.equal(processes.length, 1);
					for (let attempt = 0; attempt < 100 && processes.some(running); attempt++) await delay(10);
					assert.ok(processes.every((pid) => !running(pid)));
				} finally {
					clearTimeout(timeout);
				}
				return { idleTimeoutSkipped: true, helperExited: true };
			});
			await run("regular rename replace unlink and close", async () => {
				const { directory, file, moved } = fixture("regular");
				const lease = await pin(file);
				const before = await snapshot(lease);
				fs.renameSync(directory, moved);
				assert.equal((await snapshot(lease)).id, before.id);
				const target = path.join(moved, "input");
				fs.unlinkSync(target);
				assert.equal((await snapshot(lease)).links, 0);
				fs.writeFileSync(target, "next");
				const replacement = await pin(target);
				assert.notEqual((await snapshot(replacement)).id, before.id);
				assert.equal((await snapshot(lease)).id, before.id);
				await close(replacement);
				await close(lease);
				await provider.close(lease);
				await assert.rejects(() => snapshot(lease), { code: "WIN32_6" });
				return {
					renamed: true,
					deletedIdentityRetained: true,
					replacementDistinct: true,
					doubleCloseSafe: true,
				};
			});
			await run("directory lease rename and delete", async () => {
				const { directory, file, moved } = fixture("directory");
				const lease = await pin(directory);
				const before = await snapshot(lease);
				fs.unlinkSync(file);
				fs.renameSync(directory, moved);
				fs.rmdirSync(moved);
				assert.equal((await snapshot(lease)).id, before.id);
				return { renamed: true, deletedDirectoryIdentityRetained: true };
			});
			await run("full hardlink capacity before and after pin", async () => {
				const control = fixture("capacity-control");
				let capacity = 1;
				let limitCode;
				for (; capacity < 4096; capacity++) {
					try {
						fs.linkSync(control.file, path.join(control.directory, String(capacity)));
					} catch (error) {
						limitCode = error.code;
						break;
					}
				}
				assert.ok(capacity > 1 && capacity < 4096, JSON.stringify({ capacity, limitCode }));
				for (const existing of [true, false]) {
					const { directory, file, moved } = fixture(existing ? "existing" : "grow");
					let lease;
					if (!existing) lease = await pin(file);
					for (let index = 1; index < capacity; index++)
						fs.linkSync(file, path.join(directory, String(index)));
					lease ??= await pin(file);
					assert.equal((await snapshot(lease)).links, capacity);
					assert.throws(() => fs.linkSync(file, path.join(directory, "overflow")), { code: limitCode });
					const before = await snapshot(lease);
					fs.renameSync(directory, moved);
					for (const name of fs.readdirSync(moved)) fs.unlinkSync(path.join(moved, name));
					assert.equal((await snapshot(lease)).links, 0);
					assert.equal((await snapshot(lease)).id, before.id);
					await close(lease);
				}
				return { capacity, limitCode, capacityPreserved: true };
			});
			await run("invalid input retains no handles", async () => {
				const before = await provider.liveCount();
				await assert.rejects(() => provider.identity({}));
				await assert.rejects(() => provider.close({}));
				await assert.rejects(() => provider.pin(path.join(root, "absent")), { code: "WIN32_2" });
				await assert.rejects(() => provider.pin("bad\0path"), { code: "ERR_LEASE_PATH" });
				await assert.rejects(() => provider.pin(123), { code: "ERR_LEASE_PATH" });
				assert.equal(await provider.liveCount(), before);
				return { foreignLeaseRejected: true, closedOnFailure: true };
			});
			await run("bulk release waits for occupied request capacity", async () => {
				const { file } = fixture("bulk");
				const config = resolveSpeculativeConfig({ filesystem: { maxWindowsLeaseRequests: 2 } });
				await withSpeculativeConfig(config, async () => {
					const held = [];
					for (let index = 0; index < 128; index++) held.push(await pin(file));
					const reading = Promise.all(held.slice(0, 2).map((lease) => provider.identity(lease)));
					await Promise.all([reading, ...held.map(close)]);
				});
				assert.equal(await provider.liveCount(), 0);
				assert.deepEqual(provider.processIDs(), []);
				return { released: 128, requestCapacity: 2 };
			});
			await run("missing interpreter respects its configuration and closes", async () => {
				const { file } = fixture("startup");
				const healthy = await pin(file);
				const config = resolveSpeculativeConfig({
					filesystem: { windowsObjectLeaseCommand: [path.join(root, "missing-python.exe")] },
				});
				await withSpeculativeConfig(config, () =>
					assert.rejects(() => provider.pin(file), { code: "ENOENT" }),
				);
				assert.equal(await provider.liveCount(), 1);
				await snapshot(healthy);
				await close(healthy);
				assert.deepEqual(provider.processIDs(), []);
				return { failedOwnerClosed: true, existingOwnerPreserved: true };
			});
			await run("provider termination releases handles and permits a fresh owner", async () => {
				const { file } = fixture("termination");
				const held = await pin(file);
				const [pid] = provider.processIDs();
				assert.ok(pid && running(pid));
				process.kill(pid);
				for (let attempt = 0; attempt < 100 && provider.processIDs().includes(pid); attempt++)
					await delay(10);
				await assert.rejects(() => provider.identity(held), { code: "WIN32_6" });
				await close(held);
				assert.deepEqual(provider.processIDs(), []);
				const replacement = await pin(file);
				await snapshot(replacement);
				await close(replacement);
				return { terminatedOwnerClosed: true, restarted: true };
			});
			await run("oversized and timed out requests close their processes", async () => {
				const { file } = fixture("request-failure");
				const small = resolveSpeculativeConfig({ processReuse: { maxRequestBytes: 4 } });
				await withSpeculativeConfig(small, () =>
					assert.rejects(() => provider.pin(file), { code: "ERR_LEASE_REQUEST" }),
				);
				assert.equal(await provider.liveCount(), 0);
				const stalled = resolveSpeculativeConfig({
					filesystem: {
						windowsObjectLeaseCommand: [process.execPath, "-e", "setTimeout(() => {}, 10000)", "--"],
						objectLeaseTimeoutMs: 20,
					},
				});
				await withSpeculativeConfig(stalled, () =>
					assert.rejects(() => provider.pin(file), { code: "ERR_LEASE_TIMEOUT" }),
				);
				assert.equal(await provider.liveCount(), 0);
				assert.deepEqual(provider.processIDs(), []);
				return { oversizedOwnerClosed: true, timedOutOwnerClosed: true };
			});
			await run("garbage collection closes owned leases", async () => {
				const { file } = fixture("collection");
				assert.equal(await provider.liveCount(), 0);
				for (let index = 0; index < 32; index++) await provider.pin(file);
				for (let attempt = 0; attempt < 200 && (await provider.liveCount()); attempt++) {
					global.gc();
					await delay(1);
				}
				assert.equal(await provider.liveCount(), 0);
				return { liveAfterCollection: 0 };
			});
			await run("worker termination closes its handle owner process", async () => {
				const { directory, file, moved } = fixture("worker");
				const worker = new Worker(new URL(import.meta.url), { workerData: { file } });
				let processes;
				try {
					const [ready] = await once(worker, "message");
					assert.equal(ready.held, 32);
					processes = ready.processes;
					assert.equal(processes.length, 1);
					assert.ok(processes.every(running));
					fs.renameSync(directory, moved);
				} finally {
					await worker.terminate();
				}
				for (let attempt = 0; attempt < 100 && processes.some(running); attempt++) await delay(10);
				assert.ok(
					processes.every((pid) => !running(pid)),
					"worker left a Windows handle owner process",
				);
				assert.equal(await provider.liveCount(), 0);
				return { renamedWhileWorkerHeld: true, processesExited: true };
			});
		} finally {
			await Promise.all([...leases].map(close));
			const resolved = path.resolve(root);
			assert.ok(
				resolved.startsWith(path.resolve(os.tmpdir()) + path.sep) &&
					path.basename(resolved).startsWith("pi-file-id-lease-"),
			);
			fs.rmSync(resolved, { recursive: true, force: true });
			cleaned = !fs.existsSync(resolved);
		}
		const report = {
			platform: process.platform,
			node: process.version,
			results,
			cleaned,
			liveLeases: await provider.liveCount(),
			processes: provider.processIDs(),
		};
		process.stdout.write(JSON.stringify(report) + "\n");
		process.exitCode =
			results.some((row) => row.status !== "passed") ||
			!cleaned ||
			report.liveLeases !== 0 ||
			report.processes.length
				? 1
				: 0;
	}
});
