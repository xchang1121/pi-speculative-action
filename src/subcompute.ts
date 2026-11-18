import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server, type Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { evaluateResourceQuery } from "./agent-execution-world.ts";
import { currentSpeculativeConfiguration, withSpeculativeConfig } from "./config-schema.ts";
import {
	computeStepInView,
	ResourceInputSession,
	validateResourceVersion,
	type ResourceVersionToken,
	type ComputeStepRequest,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { TimelineInterval } from "./task-timing.ts";
import type { ObservedProcessPath, StraceObservation } from "./strace-observer.ts";
type WireValue =
	| {
			readonly encoding: "json";
			readonly value: unknown;
	  }
	| {
			readonly encoding: "base64";
			readonly value: string;
	  };
type Begin = ComputeStepRequest & {
	readonly protocol: 1;
	readonly op: "compute";
	readonly context: string;
};
type Publication = {
	readonly op: "publish";
	readonly value: WireValue;
	readonly durationMs: number;
};
const endpoint = new AsyncLocalStorage<SubcomputeService>();
type SubcomputeScope = {
	run: ReturnType<typeof AsyncLocalStorage.snapshot>;
	timer: NodeJS.Timeout;
	clients: Set<Socket>;
	socket: string;
	proofs: ResourceVersionToken[];
	busy: number;
	failed: boolean;
};
const channels = new Map<string, SubcomputeScope>();
export function controlledSubcomputeChannel(environment: Readonly<Record<string, string | undefined>>) {
	const token = environment.PI_SUBCOMPUTE_CONTEXT;
	const scope = token ? channels.get(token) : undefined;
	if (!scope || scope.socket !== environment.PI_SUBCOMPUTE_SOCKET) return undefined;
	return {
		socket: scope.socket,
		async validate() {
			if (
				channels.get(token!) !== scope ||
				scope.failed ||
				scope.busy ||
				(scope.proofs.length && (await validateResourceVersion(scope.proofs)).expired)
			)
				throw new Error("subcompute_process_inputs_unproven");
		},
		async observe(observation: StraceObservation): Promise<StraceObservation> {
			await this.validate();
			const paths: ObservedProcessPath[] = [];
			for (const proof of scope.proofs)
				for (const input of proof.observations.values()) {
					if (input.scope === "binding" || input.scope === "resolution") continue;
					paths.push({
						path: path.resolve(proof.root, input.path),
						role: "input",
						...(input.scope === "names" || input.scope === "entries" ? { listed: true } : {}),
					});
				}
			return { ...observation, paths: [...observation.paths, ...paths] };
		},
	};
}
/** Add these variables to an explicitly controlled child launch. */
export async function subcomputeEnvironment(): Promise<Readonly<Record<string, string>>> {
	const service = endpoint.getStore();
	return service ? service.environment() : {};
}
/** A protocol adapter over the same ResourceInputSession and CAS used by computeStep. */
export class SubcomputeService {
	readonly configuration = currentSpeculativeConfiguration();
	private server?: Server;
	private ready?: Promise<string>;
	private directory?: string;
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly clients = new Map<Socket, AbortController>();
	private readonly scopes = new Map<string, SubcomputeScope>();
	private readonly owners = new AsyncLocalStorage<Set<string>>();
	private readonly cwd: string;
	private readonly store: ProvenanceCertificateStore;
	private readonly maxBytes: () => number;
	constructor(cwd: string, store: ProvenanceCertificateStore, maxBytes: () => number) {
		this.cwd = cwd;
		this.store = store;
		this.maxBytes = maxBytes;
		this.configuration.bind(this);
	}
	run<T>(operation: () => T): T {
		const tokens = new Set<string>();
		const finish = () => {
			for (const token of tokens) this.releaseScope(token);
		};
		return endpoint.run(this, () =>
			this.owners.run(tokens, () => {
				try {
					const output = operation();
					if (output instanceof Promise) return output.finally(finish) as T;
					finish();
					return output;
				} catch (error) {
					finish();
					throw error;
				}
			}),
		);
	}
	async environment(): Promise<Readonly<Record<string, string>>> {
		const socket = await this.socket();
		if (this.lifetime.sealed) throw new Error("subcompute_service_closed");
		if (this.scopes.size >= this.configuration.current.subcomputation.maxPendingRequests)
			throw new Error("subcompute_scope_budget_exceeded");
		const token = randomUUID();
		const timer = setTimeout(
			() => this.releaseScope(token),
			this.configuration.current.subcomputation.socketTimeoutMs,
		);
		timer.unref();
		const scope: SubcomputeScope = {
			run: AsyncLocalStorage.snapshot(),
			timer,
			clients: new Set(),
			socket,
			proofs: [],
			busy: 0,
			failed: false,
		};
		this.scopes.set(token, scope);
		channels.set(token, scope);
		this.owners.getStore()?.add(token);
		return { PI_SUBCOMPUTE_SOCKET: socket, PI_SUBCOMPUTE_CONTEXT: token };
	}
	private releaseScope(token: string): void {
		const scope = this.scopes.get(token);
		if (scope) {
			clearTimeout(scope.timer);
			for (const socket of scope.clients) socket.destroy(new Error("subcompute_context_closed"));
			this.lifetime.track(Promise.allSettled(scope.proofs.splice(0).map((proof) => proof.release())));
		}
		this.scopes.delete(token);
		channels.delete(token);
	}
	socket(): Promise<string> {
		if (this.lifetime.sealed) return Promise.reject(new Error("subcompute_service_closed"));
		return (this.ready ??= this.listen());
	}
	private async listen(): Promise<string> {
		const id = randomUUID();
		let address: string;
		if (process.platform === "win32") address = "\\\\.\\pipe\\pi-subcompute-" + id;
		else {
			this.directory = await mkdtemp(path.join(os.tmpdir(), "pi-step-"));
			await chmod(this.directory, 0o700);
			address = path.join(this.directory, "socket");
		}
		const server = (this.server = createServer((socket) => {
			socket.on("error", () => {});
			if (
				this.lifetime.sealed ||
				this.clients.size >= this.configuration.current.subcomputation.maxPendingRequests
			) {
				socket.destroy(new Error("subcompute_connection_budget_exceeded"));
				return;
			}
			const controller = new AbortController();
			this.clients.set(socket, controller);
			const abort = () => controller.abort(new Error("subcompute_client_disconnected"));
			socket.on("error", abort);
			socket.once("end", abort);
			socket.once("close", abort);
			socket.setTimeout(this.configuration.current.subcomputation.socketTimeoutMs, () =>
				socket.destroy(new Error("subcompute_timeout")),
			);
			this.lifetime.track(
				withSpeculativeConfig(this.configuration, () => this.serve(socket, controller.signal))
					.catch((error) => {
						if (!socket.destroyed)
							socket.end(
								JSON.stringify({
									protocol: 1,
									state: "error",
									error: String(error.message ?? error),
								}) + "\n",
							);
					})
					.finally(() => this.clients.delete(socket)),
			);
		}));
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(address, () => {
				server.off("error", reject);
				resolve();
			});
		});
		if (this.lifetime.sealed) throw new Error("subcompute_service_closed");
		return address;
	}
	private async serve(socket: Socket, signal: AbortSignal): Promise<void> {
		const limit = () => this.configuration.current.subcomputation.socketRequestBytes;
		let frameBytes = 0;
		let frames = 0;
		const count = (chunk: Buffer) => {
			for (let start = 0; start < chunk.length;) {
				const newline = chunk.indexOf(10, start);
				const end = newline < 0 ? chunk.length : newline + 1;
				frameBytes += end - start;
				if (frameBytes > limit() || frames >= this.configuration.current.subcomputation.maxPendingRequests) {
					socket.destroy(new Error("subcompute_request_budget_exceeded"));
					return;
				}
				if (newline >= 0) {
					frameBytes = 0;
					frames++;
				}
				start = end;
			}
		};
		socket.on("data", count);
		const lines = createInterface({ input: socket, crlfDelay: Infinity });
		const messages = lines[Symbol.asyncIterator]();
		const read = async () => {
			const next = await messages.next();
			signal.throwIfAborted();
			if (next.done) return undefined;
			frames--;
			if (Buffer.byteLength(next.value) > limit()) throw new Error("subcompute_request_budget_exceeded");
			return JSON.parse(next.value) as unknown;
		};
		const send = async (message: object) => {
			signal.throwIfAborted();
			const frame = JSON.stringify({ protocol: 1, ...message }) + "\n";
			if (Buffer.byteLength(frame) > limit()) throw new Error("subcompute_response_budget_exceeded");
			await new Promise<void>((resolve, reject) =>
				socket.write(frame, (error) => (error ? reject(error) : resolve())),
			);
		};
		const session = new ResourceInputSession({ store: this.store }, this.maxBytes());
		let scope: SubcomputeScope | undefined;
		try {
			for (;;) {
				const request = (await read()) as Begin;
				if (request === undefined) break;
				const requestedScope = request && this.scopes.get(request.context);
				if (scope && scope !== requestedScope) throw new Error("subcompute_context_changed");
				scope = requestedScope;
				if (!scope) throw new Error("subcompute_context_unavailable");
				scope.clients.add(socket);
				if (
					!request ||
					request.protocol !== 1 ||
					request.op !== "compute" ||
					typeof request.namespace !== "string" ||
					typeof request.version !== "string" ||
					(request.includeInputContent !== undefined && typeof request.includeInputContent !== "boolean") ||
					(request.envKeys !== undefined &&
						(!Array.isArray(request.envKeys) ||
							request.envKeys.some((key) => typeof key !== "string" || !key || /[=\0]/u.test(key)))) ||
					(request.environment !== undefined &&
						(!request.environment ||
							typeof request.environment !== "object" ||
							Array.isArray(request.environment) ||
							Object.values(request.environment).some(
								(value) => value !== undefined && (typeof value !== "string" || value.includes("\0")),
							))) ||
					(request.inputResources !== undefined && !Array.isArray(request.inputResources))
				)
					throw new Error("invalid_subcompute_request");
				const contract: ComputeStepRequest = {
					...request,
					inputResources: request.inputResources?.map((resource) => {
						if (
							!resource ||
							typeof resource.path !== "string" ||
							resource.path.includes("\0") ||
							!["content", "entries", "names", "exists", "stat", "entry", "type"].includes(resource.scope)
						)
							throw new Error("invalid_subcompute_resource");
						const target = path.resolve(this.cwd, resource.path);
						return { ...resource, path: target };
					}),
				};
				let produced = false;
				scope.busy++;
				try {
					const query = await scope.run(() =>
						session.run(() =>
							evaluateResourceQuery(
								undefined,
								{ signal },
								this.cwd,
								(view) =>
									computeStepInView(
										view,
										contract,
										async (inputs) => {
											produced = true;
											const captured = await captureDeclaredInputs(inputs, contract);
											const publication = await TimelineInterval.outside(async () => {
												await send({ state: "miss", inputResources: captured });
												return (await read()) as Publication;
											});
											if (
												!publication ||
												publication.op !== "publish" ||
												!Number.isFinite(publication.durationMs) ||
												publication.durationMs < 0
											)
												throw new Error("invalid_subcompute_publication");
											TimelineInterval.own(TimelineInterval.retained(randomUUID(), publication.durationMs));
											return decodeValue(publication.value);
										},
										signal,
									),
								PI_ACTION_SEMANTICS,
								this.maxBytes(),
								true,
								"output",
								undefined,
								session,
							),
						),
					);
					if (!query) throw new Error("subcompute_retention_budget_exceeded");
					try {
						if (this.scopes.get(request.context) !== scope) throw new Error("subcompute_context_closed");
						for (const proof of query.versions)
							scope.proofs.push(session.retain({ ...proof, view: undefined }));
						await send({
							state: produced ? "published" : "hit",
							value: encodeValue(query.output),
						});
					} finally {
						await Promise.allSettled(query.versions.map((token) => token.release()));
					}
				} finally {
					scope.busy--;
				}
			}
		} catch (error) {
			if (scope && !signal.aborted) scope.failed = true;
			throw error;
		} finally {
			scope?.clients.delete(socket);
			lines.close();
			socket.off("data", count);
			await session.dispose();
		}
	}
	dispose(): Promise<void> {
		return this.lifetime.close(async () => {
			for (const token of this.scopes.keys()) this.releaseScope(token);
			for (const [socket, controller] of this.clients) {
				controller.abort(new Error("subcompute_service_closed"));
				socket.destroy();
			}
			await this.ready?.catch(() => undefined);
			if (this.server?.listening)
				await new Promise<void>((resolve, reject) =>
					this.server!.close((error) => (error ? reject(error) : resolve())),
				);
			await this.lifetime.drain();
			if (this.directory) await rm(this.directory, { recursive: true, force: true });
		});
	}
}
function encodeValue(value: unknown): WireValue {
	return value instanceof Uint8Array
		? { encoding: "base64", value: Buffer.from(value).toString("base64") }
		: { encoding: "json", value };
}
function decodeValue(wire: WireValue): unknown {
	if (wire?.encoding === "base64" && typeof wire.value === "string") {
		const result = Buffer.from(wire.value, "base64");
		if (result.toString("base64") !== wire.value) throw new Error("invalid_subcompute_binary");
		return result;
	}
	if (wire?.encoding === "json") return wire.value;
	throw new Error("invalid_subcompute_value");
}
async function captureDeclaredInputs(
	view: ToolFilesystemOperations,
	request: ComputeStepRequest,
): Promise<unknown[]> {
	return Promise.all(
		(request.inputResources ?? []).map(async (resource) => {
			if (resource.scope === "content" && request.includeInputContent)
				return {
					...resource,
					content: encodeValue(await view.readFile(resource.path)),
				};
			if (resource.scope === "content") return { ...resource, size: (await view.stat!(resource.path)).size };
			if (resource.scope === "entries" || resource.scope === "names")
				return { ...resource, names: await view.readdir!(resource.path) };
			if (resource.scope === "exists") return { ...resource, exists: await view.exists!(resource.path) };
			const stat = await view.stat!(
				resource.path,
				resource.scope === "entry" ? "entry" : "type",
				resource.scope !== "type",
			);
			return {
				...resource,
				stat: { ...stat, isDirectory: stat.isDirectory() },
			};
		}),
	);
}
