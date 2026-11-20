import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { access, link, mkdir, open, readFile, realpath, stat, unlink, writeFile } from "node:fs/promises";
import { constants, readFileSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";
import { crc32 } from "node:zlib";
import { containsFilesystemPath } from "./primitives.ts";
import { speculativeConfig } from "./config-schema.ts";
import type { TrustedSubcomputeStep, TrustedStepBuild } from "./subcompute.ts";
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
	return bootstrap ??= installBootstrap().catch(() => {
		bootstrap = undefined;
		return undefined;
	});
}
function compilerSearchPath(value: string | undefined, archive: string): string {
	if (value?.split(path.delimiter).includes(archive)) return value;
	return value === undefined ? archive : value + path.delimiter + archive;
}
type CompilerKey = { engine: string; executable: string; filename: string; optimize: number };
type CompilerWorker = {
	readonly child: ReturnType<typeof spawn>;
	readonly engine: string;
	readonly exited: Promise<void>;
	queue: Promise<unknown>;
	readonly receive: () => Promise<unknown>;
};

const ELF_MAGIC = Buffer.from([0x7f, 0x45, 0x4c, 0x46]);
const MAX_COMPILER_WORKERS = 4;

async function executableElf(target: string): Promise<boolean> {
	if (!await access(target, constants.X_OK).then(() => true, () => false)) return false;
	const handle = await open(target, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK).catch(() => undefined);
	if (!handle) return false;
	try {
		if (!(await handle.stat()).isFile()) return false;
		const header = Buffer.alloc(4);
		const { bytesRead } = await handle.read(header, 0, 4, 0);
		return bytesRead === 4 && header.equals(ELF_MAGIC);
	} catch {
		return false;
	} finally {
		await handle.close();
	}
}

/** Only the host's isolated compiler creates executable artifacts. Clients supply source bytes. */
export class PythonImportStep implements TrustedSubcomputeStep {
	readonly namespace = "python.source_to_code";
	readonly implementation = createHash("sha256").update(readFileSync(new URL("../src/python-runtime/sitecustomize.py", import.meta.url))).digest("hex");
	enabled = () => process.platform === "linux" && speculativeConfig().subcomputation.pythonImports;
	async runtime() {
		const archive = await compilerBootstrap();
		if (archive) return {
			readOnlyDirectories: [path.dirname(archive)],
			environment: (base: Readonly<Record<string, string | undefined>>) => ({ PYTHONPATH: compilerSearchPath(base.PYTHONPATH, archive) }),
		};
	}
	private readonly workers = new Map<string, {
		ready: Promise<CompilerWorker | undefined>;
		users: number;
		idle: ReturnType<typeof Promise.withResolvers<void>>;
	}>();
	private closed = false;
	private readonly searchPath = (process.env.PATH ?? "").split(path.delimiter).filter(path.isAbsolute);
	private readonly installations = [
		path.join(process.env.PYENV_ROOT ?? path.join(homedir(), ".pyenv"), "versions"),
		process.env.UV_PYTHON_INSTALL_DIR ?? path.join(process.env.XDG_DATA_HOME ?? path.join(homedir(), ".local/share"), "uv/python"),
	];
	private readonly workspace: string;
	constructor(workspace: string) {
		this.workspace = workspace;
	}

	private async hostInterpreter(binary: string, root: string): Promise<boolean> {
		if (!/^python(?:\d+(?:\.\d+)*t?)?$/.test(path.basename(binary)) ||
			!await trustedPath(binary, root) || !await executableElf(binary)) return false;
		const bin = path.dirname(binary);
		if (path.basename(bin) === "bin") {
			const installationRoot = path.dirname(path.dirname(bin));
			for (const directory of this.installations)
				if (path.isAbsolute(directory) && await realpath(directory).catch(() => undefined) === installationRoot) return true;
		}
		for (const directory of this.searchPath) {
			const hostBin = await realpath(directory).catch(() => undefined);
			if (!hostBin || !await trustedPath(hostBin, root).catch(() => false)) continue;
			if (hostBin === bin) return true;
			for (const name of [path.basename(binary), "python3"])
				if (await realpath(path.join(hostBin, name)).catch(() => undefined) === binary) return true;
		}
		return false;
	}

	private async spawnWorker(binary: string): Promise<CompilerWorker | undefined> {
		const root = await realpath(this.workspace);
		if (this.closed || !await this.hostInterpreter(binary, root)) return;
		const archive = await compilerBootstrap();
		if (this.closed || !archive || !await trustedPath(archive, root)) return;
		const main = "import sys,zipimport;exec(zipimport.zipimporter(sys.argv[1]).get_code('sitecustomize'))";
		const child = spawn(binary, ["-I", "-S", "-B", "-c", main, archive], {
			cwd: "/", env: {}, stdio: ["pipe", "pipe", "ignore"],
		});
		child.on("error", () => child.stdout.destroy());
		child.stdin.on("error", () => child.stdout.destroy());
		const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
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
		try {
			const engine = await receive();
			if (typeof engine !== "string" || !/^[a-f0-9]{64}$/.test(engine) || this.closed)
				throw new Error("invalid compiler identity");
			return { child, engine, queue: Promise.resolve(), receive, exited };
		} catch {
			child.kill("SIGKILL");
			await exited;
		}
	}

	private async stopWorker(ready: Promise<CompilerWorker | undefined>) {
		const worker = await ready;
		if (worker) {
			worker.child.kill("SIGKILL");
			await worker.exited;
		}
	}

	private async withWorker<T>(binary: string, engine: string, use: (worker: CompilerWorker) => Promise<T>) {
		while (!this.closed) {
			let slot = this.workers.get(binary);
			if (!slot) {
				let previous: Promise<void> | undefined;
				if (this.workers.size >= MAX_COMPILER_WORKERS) {
					const idle = [...this.workers].find(([, slot]) => !slot.users);
					if (!idle) {
						await Promise.race([...this.workers.values()].map((slot) => slot.idle.promise));
						continue;
					}
					this.workers.delete(idle[0]);
					previous = this.stopWorker(idle[1].ready);
				}
				const ready = Promise.resolve(previous).then(() => this.spawnWorker(binary)).catch(() => undefined);
				slot = { ready, users: 0, idle: Promise.withResolvers<void>() };
				void ready.then((worker) => worker?.exited).then(() => {
					if (this.workers.get(binary) === slot) this.workers.delete(binary);
				});
			}
			this.workers.delete(binary);
			this.workers.set(binary, slot);
			if (!slot.users++) slot.idle = Promise.withResolvers<void>();
			try {
				const worker = await slot.ready;
				if (worker && worker.engine === engine && !this.closed) return await use(worker);
			} finally {
				if (!--slot.users) slot.idle.resolve();
			}
			return;
		}
	}

	private async compiler(key: unknown) {
		const request = key as Partial<CompilerKey> | null;
		if (!request || typeof request.engine !== "string" || !/^[a-f0-9]{64}$/.test(request.engine) ||
			typeof request.executable !== "string" || !path.isAbsolute(request.executable) || request.executable.includes("\0") ||
			typeof request.filename !== "string" || !path.isAbsolute(request.filename) || request.filename.includes("\0") ||
			typeof request.optimize !== "number" || !Number.isInteger(request.optimize) || request.optimize < 0 || request.optimize > 2) return;
		const binary = await realpath(request.executable).catch(() => undefined);
		if (binary) return { binary, engine: request.engine, filename: request.filename, optimize: request.optimize };
	}

	async acceptShared(key: unknown, signal: AbortSignal) {
		signal.throwIfAborted();
		const request = await this.compiler(key);
		return !!request && !!await this.withWorker(request.binary, request.engine, async () => true);
	}

	async build({ key, input, signal, maxResultBytes }: TrustedStepBuild): Promise<Buffer | undefined> {
		const request = await this.compiler(key);
		if (!request) return;
		const result = await TimelineInterval.outside(() => this.withWorker(request.binary, request.engine, async (worker) => {
			const bytes = await input();
			if (bytes.length > 2 * 1024 * 1024) return;
			const compiled = worker.queue.then(async () => {
				signal.throwIfAborted();
				const data = bytes.toString("base64");
				const start = performance.now();
				worker.child.stdin!.write(JSON.stringify({ data, filename: request.filename, optimize: request.optimize, limit: maxResultBytes }) + "\n");
				const reply = await worker.receive();
				if (!Array.isArray(reply) || typeof reply[0] !== "string" || !Number.isFinite(reply[1]) || !Number.isFinite(reply[2])) return;
				const end = Math.min(performance.now(), Math.max(start + reply[1], reply[2] - performance.timeOrigin));
				return { bytes: Buffer.from(reply[0], "base64"), computation: new TimelineInterval(Math.max(start, end - reply[1]), end) };
			});
			worker.queue = compiled.catch(() => {});
			return compiled;
		}));
		if (result) TimelineInterval.own(result.computation);
		return result?.bytes;
	}

	async dispose() {
		this.closed = true;
		const stopped = [...this.workers.values()].map((slot) => this.stopWorker(slot.ready));
		this.workers.clear();
		await Promise.all(stopped);
	}
}
