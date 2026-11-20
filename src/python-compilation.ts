import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { access, link, mkdir, readFile, realpath, stat, unlink, writeFile } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";
import { crc32 } from "node:zlib";
import { containsFilesystemPath } from "./primitives.ts";
import { TimelineInterval } from "./task-timing.ts";

async function trustedPath(target: string, workspace?: string) {
	for (let current = target; ; current = path.dirname(current)) {
		if (workspace && containsFilesystemPath(workspace, current)) return false;
		const info = await stat(current);
		if ((info.mode & 0o022) || (info.uid !== 0 && info.uid !== process.getuid?.())) return false;
		if (current === path.dirname(current)) return true;
	}
}

// zipimport reads one ordinary, content-verified file. Importing the hook cannot create a pyc in its directory.
async function installBootstrap(): Promise<string> {
	const source = await readFile(new URL("../src/python-runtime/sitecustomize.py", import.meta.url));
	const name = Buffer.from("sitecustomize.py");
	const local = Buffer.alloc(30);
	local.writeUInt32LE(0x04034b50);
	local.writeUInt16LE(20, 4);
	local.writeUInt32LE(crc32(source), 14);
	local.writeUInt32LE(source.length, 18);
	local.writeUInt32LE(source.length, 22);
	local.writeUInt16LE(name.length, 26);
	const central = Buffer.alloc(46);
	central.writeUInt32LE(0x02014b50);
	central.writeUInt16LE(20, 4);
	local.copy(central, 6, 4, 30);
	const end = Buffer.alloc(22);
	end.writeUInt32LE(0x06054b50);
	end.writeUInt16LE(1, 8);
	end.writeUInt16LE(1, 10);
	end.writeUInt32LE(central.length + name.length, 12);
	end.writeUInt32LE(local.length + name.length + source.length, 16);
	const archive = Buffer.concat([local, name, source, central, name, end]);
	const digest = createHash("sha256").update(archive).digest("hex");
	const directory = path.join(homedir(), ".cache", "pi-speculative-action", "python", digest);
	const file = path.join(directory, "bootstrap.zip");
	await mkdir(directory, { recursive: true, mode: 0o700 });
	if (await realpath(directory) !== directory || !await trustedPath(directory)) throw new Error("unsafe Python bootstrap directory");
	if (!await stat(file).catch(() => undefined)) {
		const temporary = path.join(directory, randomUUID());
		await writeFile(temporary, archive, { flag: "wx", mode: 0o444 });
		try {
			await link(temporary, file).catch((error: NodeJS.ErrnoException) => {
				if (error.code !== "EEXIST") throw error;
			});
		} finally {
			await unlink(temporary);
		}
	}
	if (await realpath(file) !== file || !await trustedPath(file) || !(await readFile(file)).equals(archive))
		throw new Error("invalid Python compiler bootstrap");
	return file;
}
let bootstrap: Promise<string | undefined> | undefined;
export function compilerBootstrap() {
	return bootstrap ??= installBootstrap().catch(() => undefined);
}
export function compilerSearchPath(value: string | undefined, archive: string): string {
	if (value?.split(path.delimiter).includes(archive)) return value;
	return value === undefined ? archive : value + path.delimiter + archive;
}
export type PythonCompileRequest = {
	readonly op: "python_compile";
	readonly context: string;
	readonly engine: string;
	readonly filename: string;
	readonly optimize: number;
	readonly digest: string;
};
type Compilation = { bytes: Buffer; computation: TimelineInterval };
type Entry = { result: Promise<Compilation | undefined>; size: number };

/** Only the host's isolated compiler creates executable artifacts. Clients supply source bytes. */
export class PythonCompilationCache {
	private worker?: ReturnType<typeof spawn>;
	private connection?: ReturnType<PythonCompilationCache["connect"]>;
	private queue: Promise<unknown> = Promise.resolve();
	private readonly entries = new Map<string, Entry>();
	private bytes = 0;
	private closed = false;
	private readonly searchPath = process.env.PATH ?? "";
	private readonly workspace: string;
	constructor(workspace: string) {
		this.workspace = workspace;
	}

	private async connect() {
		// Select from the host's configured toolchain, independent of the command's environment and the wire request.
		const root = await realpath(this.workspace);
		let binary: string | undefined;
		for (const directory of this.searchPath.split(path.delimiter)) {
			if (!path.isAbsolute(directory)) continue;
			const target = await realpath(path.join(directory, "python3")).catch(() => undefined);
			if (target && await trustedPath(directory, root).catch(() => false) && await trustedPath(target, root).catch(() => false) &&
				await access(target, constants.X_OK).then(() => true, () => false)) {
				binary = target;
				break;
			}
		}
		const archive = await compilerBootstrap();
		if (this.closed || !binary || !archive || !await trustedPath(archive, root)) return;
		const main = "import sys,zipimport;exec(zipimport.zipimporter(sys.argv[1]).get_code('sitecustomize'))";
		const child = spawn(binary, ["-I", "-S", "-B", "-c", main, archive], {
			cwd: "/", env: {}, stdio: ["pipe", "pipe", "ignore"],
		});
		this.worker = child;
		child.on("error", () => child.stdout.destroy());
		child.stdin.on("error", () => child.stdout.destroy());
		child.once("close", () => {
			if (this.worker === child) {
				this.worker = undefined;
				this.connection = undefined;
			}
		});
		const lines = createInterface({ input: child.stdout, crlfDelay: Infinity })[Symbol.asyncIterator]();
		const receive = async () => {
			const timeout = setTimeout(() => child.kill("SIGKILL"), 5000).unref();
			try {
				const line = await lines.next();
				if (line.done) throw new Error("python_compiler_closed");
				return JSON.parse(line.value) as unknown;
			} finally {
				clearTimeout(timeout);
			}
		};
		const engine = await receive();
		return { child, engine, receive };
	}

	async get(request: PythonCompileRequest, source: () => Promise<string | undefined>,
		capacity: number, byteCapacity: number, resultLimit: number) {
		if (this.closed || capacity < 1 || byteCapacity < 1 || resultLimit < 1 ||
			typeof request.engine !== "string" || !/^[a-f0-9]{64}$/.test(request.engine) ||
			typeof request.filename !== "string" || !path.isAbsolute(request.filename) || request.filename.includes("\0") ||
			typeof request.digest !== "string" || !/^[a-f0-9]{64}$/.test(request.digest) ||
			!Number.isInteger(request.optimize) || request.optimize < 0 || request.optimize > 2)
			return;
		const key = JSON.stringify([request.engine, request.filename, request.optimize, request.digest]);
		let entry = this.entries.get(key);
		const hit = !!entry;
		if (entry) this.entries.delete(key);
		else {
			const build = async (data: string) => {
				const worker = await (this.connection ??= this.connect());
				if (!worker || this.closed || worker.engine !== request.engine) return;
				const start = performance.now();
				worker.child.stdin.write(JSON.stringify({
					data, filename: request.filename, optimize: request.optimize,
					limit: Math.min(resultLimit, byteCapacity),
				}) + "\n");
				const reply: unknown = await worker.receive();
				if (!Array.isArray(reply) || typeof reply[0] !== "string" || !Number.isFinite(reply[1]) || !Number.isFinite(reply[2])) return;
				const end = Math.min(performance.now(), Math.max(start + reply[1], reply[2] - performance.timeOrigin));
				return { bytes: Buffer.from(reply[0], "base64"), computation: new TimelineInterval(Math.max(start, end - reply[1]), end) };
			};
			const result = source().then((data) => {
				if (typeof data !== "string" || data.length > 4 * Math.ceil(Math.min(byteCapacity, 2 * 1024 * 1024) / 3)) return;
				const raw = Buffer.from(data, "base64");
				if (raw.toString("base64") !== data || createHash("sha256").update(raw).digest("hex") !== request.digest) return;
				const compiled = this.queue.then(() => build(data));
				this.queue = compiled.catch(() => {});
				return compiled;
			});
			entry = { result, size: 0 };
		}
		this.entries.set(key, entry);
		try {
			const value = await entry.result;
			if (!value || this.closed) return;
			if (this.entries.get(key) === entry && !entry.size) {
				entry.size = value.bytes.length;
				this.bytes += entry.size;
			}
			while (this.entries.size > capacity || this.bytes > byteCapacity) {
				const [oldest, victim] = this.entries.entries().next().value!;
				this.entries.delete(oldest);
				this.bytes -= victim.size;
			}
			if (value.bytes.length <= Math.min(resultLimit, byteCapacity)) return { ...value, hit };
		} finally {
			if (!entry.size && this.entries.get(key) === entry) this.entries.delete(key);
		}
	}

	close() {
		this.closed = true;
		this.worker?.kill("SIGKILL");
		this.entries.clear();
		this.bytes = 0;
	}
}
