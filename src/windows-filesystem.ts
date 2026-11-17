import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { speculativeConfig } from "./config-schema.ts";

type Lease = { readonly connection: ObjectConnection; readonly id: number; closed: boolean };
type Reply = {
	readonly value?: unknown;
	readonly error?: { readonly code: string; readonly message: string };
};
type Pending = {
	readonly operation: string;
	readonly argument: unknown;
	readonly promise: Promise<unknown>;
	readonly resolve: (value: unknown) => void;
	readonly reject: (error: Error) => void;
	readonly timer: NodeJS.Timeout;
};

const connections = new Set<ObjectConnection>();
const owners = new WeakMap<object, Lease>();
const collected = new FinalizationRegistry<Lease>((lease) => {
	void release(lease).catch(() => {});
});
const current = new WeakMap<ReturnType<typeof speculativeConfig>, ObjectConnection>();
const unavailable = new WeakMap<ReturnType<typeof speculativeConfig>, { retryAt: number; error: unknown }>();
process.once("exit", () => {
	for (const connection of connections) connection.terminate();
});

function failure(code: string, message: string): NodeJS.ErrnoException {
	return Object.assign(new Error(message), { code });
}

/** One pipe owns all its file-ID handles. EOF also releases them after worker or parent termination. */
class ObjectConnection {
	private readonly config = speculativeConfig();
	private readonly child;
	private readonly pending = new Map<number, Pending>();
	private readonly leases = new Set<number>();
	private readonly completion = Promise.withResolvers<void>();
	private closing = Promise.resolve();
	private input = Buffer.alloc(0);
	private errors = "";
	private sequence = 0;
	private closed = false;
	private retiring = false;
	private exitTimer?: NodeJS.Timeout;
	private idleTimer?: NodeJS.Timeout;
	private qualified = false;
	readonly finished = this.completion.promise;

	constructor() {
		const [command, ...arguments_] = this.config.filesystem.windowsObjectLeaseCommand;
		this.child = spawn(
			command!,
			[
				...arguments_,
				fileURLToPath(new URL("../src/windows-filesystem.py", import.meta.url)),
				String(this.config.processReuse.maxRequestBytes),
				String(this.config.filesystem.maxTreeEntries),
			],
			{ stdio: ["pipe", "pipe", "pipe"], windowsHide: true },
		);
		connections.add(this);
		this.child.stdout.on("data", (bytes: Buffer) => this.receive(bytes));
		this.child.stderr.on("data", (bytes: Buffer) => {
			this.errors = (this.errors + bytes.toString()).slice(0, this.config.processReuse.diagnosticCharacters);
		});
		this.child.stdin.on("error", (error) => this.fail(error));
		this.child.once("error", (error) => this.fail(error));
		this.child.once("close", () => {
			this.fail(failure("ERR_LEASE_PROVIDER_EXIT", this.errors || "Windows object lease connection closed"));
			clearTimeout(this.exitTimer);
			clearTimeout(this.idleTimer);
			connections.delete(this);
			this.completion.resolve();
		});
	}
	get accepting(): boolean {
		return !this.closed && !this.retiring;
	}
	get pid(): number | undefined {
		return this.child.pid;
	}
	get startupFailed(): boolean {
		return this.closed && !this.qualified;
	}
	terminate(): void {
		this.child.kill();
	}
	private reference(active: boolean): void {
		if (active) this.child.ref();
		else this.child.unref();
		for (const stream of [this.child.stdin, this.child.stdout, this.child.stderr]) {
			const pipe = stream as typeof stream & { ref?(): void; unref?(): void };
			if (active) pipe.ref?.();
			else pipe.unref?.();
		}
	}
	request(operation: string, argument: unknown = null): Promise<unknown> {
		if (!this.accepting) return Promise.reject(failure("WIN32_6", "Windows object lease is closed"));
		if (this.pending.size >= this.config.filesystem.maxWindowsLeaseRequests)
			return Promise.reject(failure("ERR_LEASE_CAPACITY", "Windows object lease request capacity exceeded"));
		const id = ++this.sequence;
		const frame = Buffer.from(JSON.stringify([id, operation, argument]) + "\n");
		if (frame.byteLength > this.config.processReuse.maxRequestBytes) {
			this.retireIfIdle();
			return Promise.reject(failure("ERR_LEASE_REQUEST", "Windows object lease request is too large"));
		}
		const result = Promise.withResolvers<unknown>();
		clearTimeout(this.idleTimer);
		this.idleTimer = undefined;
		this.reference(true);
		const timer = setTimeout(
			() => {
				this.fail(failure("ERR_LEASE_TIMEOUT", "Windows object lease request timed out"));
			},
			Math.min(
				this.config.filesystem.objectLeaseTimeoutMs,
				this.qualified ? Infinity : this.config.filesystem.objectLeaseStartupTimeoutMs,
			),
		);
		this.pending.set(id, { operation, argument, ...result, timer });
		this.child.stdin.write(frame);
		return result.promise;
	}
	close(id: number): Promise<void> {
		const closing = this.closing.then(async () => {
			try {
				while (this.accepting && this.pending.size >= this.config.filesystem.maxWindowsLeaseRequests)
					await this.pending
						.values()
						.next()
						.value!.promise.catch(() => {});
				if (this.accepting) await this.request("close", id);
			} catch (error) {
				this.fail(error as Error);
				throw error;
			} finally {
				if (!this.accepting) await this.finished;
			}
		});
		this.closing = closing.catch(() => {});
		return closing;
	}
	private receive(bytes: Buffer): void {
		this.input = Buffer.concat([this.input, bytes]);
		for (;;) {
			const end = this.input.indexOf(10);
			if ((end < 0 ? this.input.byteLength : end + 1) > this.config.processReuse.maxRequestBytes) {
				this.fail(failure("ERR_LEASE_RESPONSE", "Windows object lease response is too large"));
				return;
			}
			if (end < 0) return;
			const line = this.input.subarray(0, end).toString("utf8");
			this.input = this.input.subarray(end + 1);
			try {
				const [id, reply] = JSON.parse(line) as [number, Reply];
				const pending = this.pending.get(id);
				if (!pending || !reply || typeof reply !== "object") throw new Error("unexpected lease response");
				if (!reply.error && pending.operation === "pin") {
					if (reply.value !== id) throw new Error("invalid lease identity");
					this.leases.add(id);
					this.qualified = true;
				}
				if (!reply.error && pending.operation === "close") this.leases.delete(pending.argument as number);
				clearTimeout(pending.timer);
				this.pending.delete(id);
				if (reply.error) pending.reject(failure(reply.error.code, reply.error.message));
				else pending.resolve(reply.value);
				this.retireIfIdle();
			} catch (error) {
				this.fail(error as Error);
				return;
			}
		}
	}
	private retireIfIdle(): void {
		if (this.pending.size || this.leases.size || !this.accepting) return;
		const retire = () => {
			this.retiring = true;
			this.child.stdin.end();
			this.exitTimer = setTimeout(() => this.terminate(), this.config.processReuse.terminateGraceMs);
			this.exitTimer.unref();
		};
		if (!this.config.filesystem.objectLeaseIdleMs) retire();
		else if (!this.idleTimer) {
			this.idleTimer = setTimeout(retire, this.config.filesystem.objectLeaseIdleMs);
			this.idleTimer.unref();
			this.reference(false);
		}
	}
	private fail(error: Error): void {
		if (this.closed) return;
		this.closed = true;
		clearTimeout(this.idleTimer);
		for (const pending of this.pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(error);
		}
		this.pending.clear();
		this.leases.clear();
		this.child.kill();
	}
}

function owned(value: object): Lease {
	const lease = owners.get(value);
	if (!lease) throw failure("ERR_LEASE_OWNER", "An issued Windows object lease is required");
	return lease;
}
async function release(lease: Lease): Promise<void> {
	if (lease.closed) return;
	lease.closed = true;
	await lease.connection.close(lease.id);
}

const provider = {
	async pin(target: string): Promise<object> {
		if (typeof target !== "string" || !target || target.includes("\0") || target.length >= 32767)
			throw failure("ERR_LEASE_PATH", "A nonempty Windows path is required");
		const config = speculativeConfig();
		const failed = unavailable.get(config);
		if (failed && failed.retryAt > Date.now()) throw failed.error;
		let connection = current.get(config);
		if (!connection?.accepting) {
			connection = new ObjectConnection();
			current.set(config, connection);
		}
		const id = (await connection.request("pin", target).catch(async (error) => {
			if (connection.startupFailed) {
				unavailable.set(config, { retryAt: Date.now() + config.filesystem.objectLeaseRetryMs, error });
				await connection.finished;
			}
			throw error;
		})) as number;
		const value = Object.freeze({});
		const lease = { connection, id, closed: false };
		owners.set(value, lease);
		collected.register(value, lease, value);
		return value;
	},
	async identity(value: object): Promise<Buffer> {
		const lease = owned(value);
		if (lease.closed) throw failure("WIN32_6", "Windows object lease is closed");
		const encoded = await lease.connection.request("identity", lease.id);
		if (typeof encoded !== "string" || !/^[0-9a-f]{56}$/.test(encoded))
			throw failure("ERR_LEASE_IDENTITY", "Invalid Windows object identity");
		return Buffer.from(encoded, "hex");
	},
	async close(value: object): Promise<void> {
		const lease = owned(value);
		collected.unregister(value);
		await release(lease);
	},
	async liveCount(): Promise<number> {
		const counts = await Promise.all(
			[...connections].map(async (connection) => {
				if (connection.accepting) return (await connection.request("count")) as number;
				await connection.finished;
				return 0;
			}),
		);
		return counts.reduce((sum, value) => sum + value, 0);
	},
	processIDs(): number[] {
		return [...connections].flatMap((connection) => connection.pid ?? []);
	},
};

export function windowsFilesystem(): typeof provider {
	if (process.platform !== "win32") throw new Error("Windows object leases require Windows");
	return provider;
}
