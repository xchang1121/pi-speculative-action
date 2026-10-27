import { fork, type ChildProcess } from "node:child_process";
import { closeSync, constants, openSync } from "node:fs";
import { stat } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { getSystemErrorName } from "node:util";
import { serialize } from "node:v8";
import { setTimeout as delay } from "node:timers/promises";
import { withFilesystemReadView } from "./filesystem-evidence.ts";
import { isMissing } from "./error-utils.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { ensureNativeFilesystem, nativeFilesystem, nativeFilesystemBinary, nativeProcessLibrary, startupScope } from "./process-startup.mjs";

type Rows = Record<string, SQLOutputValue>[];
interface Request { readonly root: string; readonly sql: string; readonly parameters: SQLInputValue[]; readonly maxBytes: number; }
interface Reply { readonly id: number; readonly value?: Rows; readonly error?: string; readonly code?: string; readonly errcode?: number; }
interface Peer { readonly child: ChildProcess; readonly closed: Promise<void>; readonly ready: Promise<void>;
	pending?: { readonly id: number; readonly maxBytes: number; resolve(value: Rows | undefined): void; reject(error: unknown): void; }; }
const entry = fileURLToPath(import.meta.url);

/** Reuse an idle process, never a connection or snapshot. Its separate POSIX lock
 * and SHM domain cannot borrow the host publisher's writable mappings. */
export class SQLiteReadOwner {
	private readonly lifetime = new RuntimeLifecycleLane();
	private peer?: Peer;
	private nextID = 0;
	private readonly root: string;
	constructor(root: string) { this.root = root; }
	read(sql: string, parameters: SQLInputValue[], maxBytes: number, signal: AbortSignal): Promise<Rows | undefined> {
		return this.lifetime.serialize(async () => {
			signal.throwIfAborted();
			const request = { root: this.root, sql, parameters, maxBytes };
			if (process.platform !== "linux") return ambientRead(request);
			await ensureNativeFilesystem(); signal.throwIfAborted();
			const peer = this.peer ??= this.launch(nativeFilesystemBinary()!);
			const abort = () => { peer.child.kill("SIGKILL"); };
			signal.addEventListener("abort", abort, { once: true });
			try {
				await peer.ready; signal.throwIfAborted();
				if (serialize(request).byteLength > 65536) throw new Error("SQLite read request budget exceeded");
				const id = ++this.nextID;
				return await new Promise<Rows | undefined>((resolve, reject) => {
					peer.pending = { id, maxBytes, resolve, reject };
					try { peer.child.send({ id, ...request }, error => { if (error) abort(); }); } catch { abort(); }
				});
			} finally {
				signal.removeEventListener("abort", abort);
				if (signal.aborted) { abort(); await peer.closed; }
				signal.throwIfAborted();
			}
		});
	}
	dispose(): Promise<void> {
		return this.lifetime.close(async () => {
			const peer = this.peer; if (peer) { peer.child.kill("SIGKILL"); await peer.closed; }
		});
	}
	private launch(binary: string): Peer {
		// Node does not strip TypeScript inside installed packages. They carry the
		// matching compiled entry; a source checkout can use Node's native TS path.
		const worker = entry.endsWith(".ts") && entry.split(path.sep).includes("node_modules") ? fileURLToPath(new URL("../dist/sqlite-read.js", import.meta.url)) : entry;
		const child = startupScope.exit(() => fork(worker, ["--sqlite-reader", binary], {
			execArgv: [], serialization: "advanced", silent: true, env: { ...process.env, NODE_OPTIONS: "" },
		}));
		const ready = Promise.withResolvers<void>(), closed = Promise.withResolvers<void>();
		const peer: Peer = { child, ready: ready.promise, closed: closed.promise }; let diagnostic = "", received = false;
		const startup = setTimeout(() => child.kill("SIGKILL"), 10000);
		void ready.promise.catch(() => {});
		child.once("error", error => { diagnostic = error.message; child.kill("SIGKILL"); });
		child.once("close", () => {
			clearTimeout(startup); if (this.peer === peer) this.peer = undefined;
			const error = new Error(`SQLite read process closed: ${diagnostic}`);
			ready.reject(error); peer.pending?.reject(error); peer.pending = undefined; closed.resolve();
		});
		for (const stream of [child.stdout, child.stderr]) stream!.on("data", bytes => { diagnostic = (diagnostic + bytes.toString()).slice(-4096); });
		child.on("message", (message: Reply | { ready: true }) => {
			if ("ready" in message) {
				if (received) { child.kill("SIGKILL"); return; }
				received = true; clearTimeout(startup); ready.resolve(); return;
			}
			const pending = peer.pending;
			if (!pending || message.id !== pending.id || serialize(message).byteLength > pending.maxBytes + 65536) { child.kill("SIGKILL"); return; }
			peer.pending = undefined;
			if (message.error) pending.reject(Object.assign(new Error(message.error), { code: message.code, errcode: message.errcode }));
			else pending.resolve(message.value);
		});
		return peer;
	}
}

function query(database: DatabaseSync, request: Request): Rows | undefined {
	const version = Number(database.prepare("PRAGMA user_version").get()?.user_version);
	if (version < 3) return undefined;
	if (version !== 3) throw new Error(`unsupported provenance store version ${version}`);
	database.exec("PRAGMA query_only=ON");
	const rows: Rows = []; let bytes = 0;
	for (const row of database.prepare(request.sql).iterate(...request.parameters)) {
		bytes += serialize(row).byteLength;
		if (bytes > request.maxBytes) throw new Error("SQLite read result budget exceeded");
		rows.push(row);
	}
	return rows;
}
async function ambientRead(request: Request): Promise<Rows | undefined> {
	const target = path.join(request.root, "metadata.sqlite"); let database: DatabaseSync | undefined;
	try { database = new DatabaseSync(target, { readOnly: true }); return query(database, request); }
	catch (error) {
		if (sqliteCode(error) === 14 && !await stat(target).catch(cause => { if (!isMissing(cause)) throw cause; })) return undefined;
		throw error;
	} finally { database?.close(); }
}
function sqliteCode(error: unknown): number { return Number((error as { errcode?: number }).errcode) & 0xff; }

async function isolatedRead(request: Request): Promise<Rows | undefined> {
	return withFilesystemReadView(undefined, async view => {
		const directory = await view.pin(request.root, true).catch(error => { if (!isMissing(error)) throw error; });
		if (!directory) return undefined;
		let database: DatabaseSync | undefined, snapshot: number | undefined;
		try {
			try {
				database = new DatabaseSync(`file:/proc/self/fd/${directory.fd}/metadata.sqlite?mode=ro&vfs=pi-read-view`, { readOnly: true });
				return query(database, request);
			} catch (error) {
				database?.close(); database = undefined;
				if (![8, 14].includes(sqliteCode(error))) throw error;
			}
			const capture = Buffer.alloc(12); capture.writeInt32LE(directory.fd); capture.writeBigUInt64LE(BigInt(request.maxBytes), 4);
			snapshot = nativeFilesystem!(-12, capture, fd => openSync(`/proc/self/fd/${fd}`, constants.O_RDONLY));
			if (!Number.isInteger(snapshot)) { snapshot = undefined; throw new Error("native SQLite snapshot unavailable; rebuild the Linux backend"); }
			if (snapshot < 0) {
				const code = getSystemErrorName(snapshot); snapshot = undefined;
				if (code === "ENOENT") return undefined;
				throw Object.assign(new Error(`native SQLite snapshot failed: ${code}`), { code });
			}
			database = new DatabaseSync(`file:/proc/self/fd/${snapshot}?mode=ro&immutable=1&vfs=pi-sealed-view`, { readOnly: true });
			return query(database, request);
		} finally {
			try { database?.close(); } finally { if (snapshot !== undefined) closeSync(snapshot); await directory.close(); }
		}
	});
}

if (process.argv[1] === entry && process.argv[2] === "--sqlite-reader") {
	if (!process.send || process.platform !== "linux") throw new Error("SQLite reader requires its owning Linux IPC process");
	const binary = process.argv[3]; nativeProcessLibrary(binary);
	const bootstrap = new DatabaseSync(":memory:", { allowExtension: true });
	try { bootstrap.loadExtension(`${binary}.sqlite`); } finally { bootstrap.close(); }
	let active = false;
	process.once("disconnect", () => process.exit(0));
	process.on("message", async (request: Request & { id: number }) => {
		if (active || !Number.isSafeInteger(request.id) || request.id < 1 || !Number.isSafeInteger(request.maxBytes) || request.maxBytes < 1 || serialize(request).byteLength > 65536) process.exit(1);
		active = true;
		let response: Reply;
		try {
			for (let attempt = 0; ; attempt++) {
				try { response = { id: request.id, value: await isolatedRead(request) }; break; }
				catch (error) {
					if (attempt === 2 || !["EAGAIN", "EBUSY", "ESTALE"].includes((error as { code?: string }).code ?? "")) throw error;
					await delay(4);
				}
			}
		}
		catch (error) {
			const failure = error as Error & { code?: string; errcode?: number };
			response = { id: request.id, error: String(failure.message ?? error).slice(0, 4096), code: failure.code, errcode: failure.errcode };
		} finally { active = false; }
		process.send!(response, error => { if (error) process.exit(1); });
	});
	process.send({ ready: true });
}
