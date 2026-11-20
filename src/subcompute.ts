import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server, type Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { ContentChunkPool } from "./content-chunks.ts";
import { settledFilesystemStat } from "./filesystem-evidence.ts";
import { evaluateResourceQuery } from "./agent-execution-world.ts";
import { currentSpeculativeConfiguration, withSpeculativeConfig } from "./config-schema.ts";
import {
	computeStepInView,
	dependencyKey,
	observeStepInput,
	ResourceInputSession,
	ResourceReadView,
	ResourceVersionManager,
	resourceProofBytes,
	resourceStatSignature,
	type ResourceDependency,
	type ResourceInputPersistence,
	validateResourceVersion,
	type ResourceVersionToken,
	type ComputeStepRequest,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { containsFilesystemPath, filesystemPathKey, hasErrorCode } from "./primitives.ts";
import { PythonCompilationCache, compilerBootstrap, compilerSearchPath, type PythonCompileRequest } from "./python-compilation.ts";
import { subcomputeTiming, TaskTimeline, TimelineInterval, type SubcomputeCounts } from "./task-timing.ts";
import type { ObservedProcessPath, ExecutionObservation } from "./execution-observer.ts";
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
	readonly op: "compute";
	readonly context: string;
};
type Observation = {
	readonly op: "observe";
	readonly token: string;
	readonly reads: readonly ResourceDependency[];
};
type Publication = {
	readonly op: "publish";
	readonly token: string;
	readonly reads?: readonly ResourceDependency[];
	readonly value: WireValue;
	readonly durationMs: number;
};
const fields = {
	compute: ["op", "context", "namespace", "key", "inputResources", "envKeys", "environment", "includeInputContent"],
	observe: ["op", "token", "reads"],
	publish: ["op", "token", "reads", "durationMs", "value"],
	python_compile: ["op", "context", "engine", "filename", "optimize", "digest"],
	source: ["op", "data"],
	consumed: ["op"],
};
const endpoint = new AsyncLocalStorage<SubcomputeService>();
type SubcomputeScope = {
	run: ReturnType<typeof AsyncLocalStorage.snapshot>;
	timer?: NodeJS.Timeout;
	bootstrap?: string;
	clients: Set<Socket>;
	socket: string;
	proofs: ResourceVersionToken[];
	busy: number;
	failed: boolean;
	general: boolean;
};
const channels = new Map<string, SubcomputeScope>();
export function controlledSubcomputeChannel(environment: Readonly<Record<string, string | undefined>>) {
	const token = environment.PI_SUBCOMPUTE_CONTEXT ?? environment.PI_PYTHON_COMPILE_CONTEXT;
	const scope = token ? channels.get(token) : undefined;
	if (!scope || scope.socket !== (environment.PI_SUBCOMPUTE_SOCKET ?? environment.PI_PYTHON_COMPILE_SOCKET)) return undefined;
	return {
		socket: scope.socket,
		bootstrap: scope.bootstrap,
		async validate() {
			if (
				channels.get(token!) !== scope ||
				scope.failed ||
				scope.busy ||
				(scope.proofs.length && (await validateResourceVersion(scope.proofs)).expired)
			)
				throw new Error("subcompute_process_inputs_unproven");
		},
		async observe(observation: ExecutionObservation): Promise<ExecutionObservation> {
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
export async function controlledRuntimeEnvironment(environment: Readonly<Record<string, string | undefined>>): Promise<Readonly<Record<string, string>>> {
	const service = endpoint.getStore();
	if (!service) return {};
	const config = service.configuration.current;
	const general = config.subcomputation.exposeToControlledProcesses;
	const runtime = service.runtime();
	const archive = process.platform === "linux" && config.subcomputation.pythonImports && runtime.reuseEnabled !== false &&
		runtime.resourceCacheMaxEntries > 0 && (runtime.resourceCacheMaxBytes ?? config.runtime.resourceCacheMaxBytes) > 0
		? await compilerBootstrap() : undefined;
	if (!general && !archive) return {};
	const channel = controlledSubcomputeChannel(environment);
	const transport = channel ? {
		PI_SUBCOMPUTE_SOCKET: channel.socket,
		PI_SUBCOMPUTE_CONTEXT: environment.PI_SUBCOMPUTE_CONTEXT ?? environment.PI_PYTHON_COMPILE_CONTEXT!,
	} : await service.environment(general).catch((error: unknown) => {
		if (general) throw error;
		return undefined;
	});
	if (!transport) return {};
	if (archive) channels.get(transport.PI_SUBCOMPUTE_CONTEXT)!.bootstrap = archive;
	return {
		...(general || (channel && environment.PI_SUBCOMPUTE_CONTEXT) ? transport : {}),
		...(archive ? {
			PI_PYTHON_COMPILE_SOCKET: transport.PI_SUBCOMPUTE_SOCKET,
			PI_PYTHON_COMPILE_CONTEXT: transport.PI_SUBCOMPUTE_CONTEXT,
			PYTHONPATH: compilerSearchPath(environment.PYTHONPATH, archive),
		} : {}),
	};
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
	private readonly inputManager: ResourceVersionManager;
	private readonly inputStorage: { store: ProvenanceCertificateStore; contents: ContentChunkPool };
	private releaseChunks: () => Promise<void>;
	private readonly files = new Map<
		string,
		{ statSig: string; takenAtMs: number; token: Promise<ResourceVersionToken>; bytes: number }
	>();
	private cacheBytes = 0;
	private readonly counts = new Map<string, SubcomputeCounts>();
	private readonly cwd: string;
	private readonly python: PythonCompilationCache;
	private readonly maxBytes: () => number;
	runtime: () => { reuseEnabled?: boolean; resourceCacheMaxEntries: number; resourceCacheMaxBytes?: number } =
		() => this.configuration.current.runtime;
	constructor(cwd: string, store: ProvenanceCertificateStore, maxBytes: () => number) {
		this.cwd = cwd;
		this.python = new PythonCompilationCache(cwd);
		this.inputStorage = { store, contents: new ContentChunkPool(store) };
		this.releaseChunks = this.inputStorage.contents.retain();
		this.inputManager = new ResourceVersionManager(cwd, { watch: false, persistentInputs: this.inputStorage });
		this.maxBytes = maxBytes;
		this.configuration.bind(this);
	}
	drainMetrics() {
		const metrics = Object.fromEntries(this.counts);
		this.counts.clear();
		return metrics;
	}
	private readonly record = (namespace: string, hit: boolean, computation?: TimelineInterval) => {
		const counts = this.counts.get(namespace) ?? { hits: 0, misses: 0, reusedMs: 0 };
		if (hit) {
			counts.hits++;
			if (computation) {
				const now = performance.now();
				counts.reusedMs += new TaskTimeline(now).recordTool(computation, now, true).toolComputeMs ?? 0;
			}
		} else counts.misses++;
		this.counts.set(namespace, counts);
	};
	private evict(key: string): void {
		const entry = this.files.get(key);
		if (!entry) return;
		this.files.delete(key);
		this.cacheBytes -= entry.bytes;
		this.lifetime.track(entry.token.then((token) => token.release(), () => {}));
	}
	private renewChunkPool(): void {
		for (const key of this.files.keys()) this.evict(key);
		this.lifetime.track(this.releaseChunks());
		this.inputStorage.contents = new ContentChunkPool(this.inputStorage.store);
		this.releaseChunks = this.inputStorage.contents.retain();
	}
	invalidate(paths: readonly string[]): void {
		for (const key of this.files.keys())
			if (paths.some((target) => containsFilesystemPath(target, key) || containsFilesystemPath(key, target)))
				this.evict(key);
	}
	private readonly inputs: NonNullable<ResourceInputPersistence["inputs"]> = async (dependency, stat) => {
		const key = filesystemPathKey(dependency.path);
		const budget = this.maxBytes();
		if (stat.isFile() && stat.size > budget) return;
		const statSig = resourceStatSignature(stat, key);
		let entry = this.files.get(key);
		if (entry && (entry.statSig !== statSig || !settledFilesystemStat(stat, entry.takenAtMs, dependency.path))) {
			this.evict(key);
			entry = undefined;
		}
		if (!entry) {
			const takenAtMs = Date.now();
			const capture = async () => {
				const token = await this.inputManager.capture(undefined, this.maxBytes());
				try {
					await observeStepInput(token.view!, dependency);
					token.view!.seal();
					if (token.observations.get(dependencyKey(dependency))?.stamp !== statSig)
						throw new Error("subcompute_inputs_changed");
					return token;
				} catch (error) {
					await token.release();
					throw error;
				}
			};
			entry = { statSig, takenAtMs, token: capture(), bytes: 0 };
		}
		this.files.delete(key);
		this.files.set(key, entry);
		while (this.files.size > 2048) this.evict(this.files.keys().next().value!);
		try {
			const token = await entry.token;
			if (this.files.get(key) !== entry) return;
			if (!entry.bytes) {
				entry.bytes = token.view!.bytes + resourceProofBytes([token]);
				this.cacheBytes += entry.bytes;
			}
			while (this.cacheBytes + this.inputStorage.contents.storedBytes > budget && this.files.size)
				this.evict(this.files.keys().next().value!);
			if (!this.files.size) this.renewChunkPool();
			else if (this.files.get(key) === entry) return token;
		} catch (error) {
			if (hasErrorCode(error, "ERR_ARTIFACT_LEASE_BUDGET")) {
				if (this.files.get(key) === entry) this.renewChunkPool();
				return;
			}
			if (this.files.get(key) === entry) this.evict(key);
			throw error;
		}
	};
	run<T>(operation: () => T): T {
		const tokens = new Set<string>();
		const finish = () => {
			for (const token of tokens) this.releaseScope(token);
		};
		return endpoint.run(this, () =>
			this.owners.run(tokens, () => {
				try {
					const output = subcomputeTiming.run(this.record, operation);
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
	async environment(general = true): Promise<Readonly<Record<string, string>>> {
		const socket = await this.socket();
		if (this.lifetime.sealed) throw new Error("subcompute_service_closed");
		if (this.scopes.size >= this.configuration.current.subcomputation.maxPendingRequests)
			throw new Error("subcompute_scope_budget_exceeded");
		const token = randomUUID();
		const timer = general || !this.owners.getStore() ? setTimeout(
			() => this.releaseScope(token),
			this.configuration.current.subcomputation.socketTimeoutMs,
		).unref() : undefined;
		const scope: SubcomputeScope = {
			run: subcomputeTiming.run(this.record, () => AsyncLocalStorage.snapshot()),
			timer,
			clients: new Set(),
			socket,
			proofs: [],
			busy: 0,
			failed: false,
			general,
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
	private resources(reads?: readonly ResourceDependency[]): ResourceDependency[] | undefined {
		if (reads === undefined) return;
		if (!Array.isArray(reads)) throw new Error("invalid_subcompute_resource");
		return reads.map((resource) => {
			if (!resource || typeof resource.path !== "string" || resource.path.includes("\0") ||
				!["content", "entries", "names", "exists", "stat", "entry", "type"].includes(resource.scope))
				throw new Error("invalid_subcompute_resource");
			const target = path.resolve(this.cwd, resource.path);
			if (!containsFilesystemPath(this.cwd, target)) throw new Error("resource dependency escapes workspace");
			return { path: target, scope: resource.scope };
		});
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
			const frame = JSON.parse(next.value) as Begin | Observation | Publication | PythonCompileRequest |
				{ op: "consumed" } | { op: "source"; data: string };
			if (!frame || !Object.hasOwn(fields, frame.op) || Object.keys(frame).some((key) => !fields[frame.op].includes(key)))
				throw new Error("invalid_subcompute_frame");
			return frame;
		};
		const send = async (message: object) => {
			signal.throwIfAborted();
			const frame = JSON.stringify(message) + "\n";
			if (Buffer.byteLength(frame) > limit()) throw new Error("subcompute_response_budget_exceeded");
			await new Promise<void>((resolve, reject) =>
				socket.write(frame, (error) => (error ? reject(error) : resolve())),
			);
		};
		const publish = async (view: ResourceReadView, contract: ComputeStepRequest) => {
			const token = randomUUID();
			const seen = new Set(contract.inputResources?.map(dependencyKey));
			const inputResources = await captureDeclaredInputs(view, contract);
			const publication = await TimelineInterval.outside(async () => {
				await send({ state: "miss", token, inputResources });
				for (;;) {
					const frame = await read();
					if (!frame || (frame.op !== "observe" && frame.op !== "publish") || frame.token !== token) throw new Error("invalid_subcompute_token");
					if (frame.op === "observe" && !Array.isArray(frame.reads)) throw new Error("invalid_subcompute_resource");
					let repeated = false;
					for (const resource of this.resources(frame.reads) ?? []) {
						const key = dependencyKey(resource);
						if (seen.has(key)) repeated = true;
						else {
							await observeStepInput(view, resource);
							seen.add(key);
						}
					}
					if (repeated || frame.op === "publish") await view.validateObserved();
					if (frame.op === "publish") return frame;
					await send({ state: "observed", token });
				}
			});
			if (!Number.isFinite(publication.durationMs) || publication.durationMs < 0)
				throw new Error("invalid_subcompute_publication");
			TimelineInterval.own(TimelineInterval.retained(randomUUID(), publication.durationMs));
			return decodeValue(publication.value);
		};
		let inputSession: ResourceInputSession | undefined;
		let scope: SubcomputeScope | undefined;
		try {
			for (;;) {
				const request = await read();
				if (request === undefined) break;
				if (request.op !== "compute" && request.op !== "python_compile") throw new Error("invalid_subcompute_request");
				const requestedScope = this.scopes.get(request.context);
				if (scope && scope !== requestedScope) throw new Error("subcompute_context_changed");
				scope = requestedScope;
				if (!scope) throw new Error("subcompute_context_unavailable");
				scope.clients.add(socket);
				if (request.op === "python_compile") {
					if (!scope.bootstrap) throw new Error("subcompute_context_unavailable");
					const { subcomputation } = this.configuration.current;
					const runtime = this.runtime();
					scope.busy++;
					try {
						const source = async () => {
							await send({ state: "source" });
							const frame = await read();
							if (frame?.op === "source") return frame.data;
						};
						const resultLimit = Math.min(subcomputation.maxStepResultBytes, Math.floor((limit() - 128) * 3 / 4));
						const compiled = await scope.run(() =>
							this.python.get(request, source, runtime.reuseEnabled === false ? 0 : runtime.resourceCacheMaxEntries,
								runtime.resourceCacheMaxBytes ?? this.maxBytes(), resultLimit)
						).catch(() => undefined);
						if (!compiled) {
							await send({ state: "unavailable" });
							continue;
						}
						await send({ state: "compiled", data: compiled.bytes.toString("base64") });
						if ((await read())?.op !== "consumed") break;
						compiled.consumed();
						scope.run(() => {
							if (compiled.hit) TimelineInterval.use(compiled.computation);
							this.record("python.source_to_code", compiled.hit, compiled.computation);
						});
						await send({ state: "ready" });
					} finally {
						scope.busy--;
					}
					continue;
				}
				if (!scope.general) throw new Error("subcompute_context_unavailable");
				if (
					!Object.hasOwn(request, "key") ||
					typeof request.namespace !== "string" ||
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
							)))
				)
					throw new Error("invalid_subcompute_request");
				const contract = { ...request, inputResources: this.resources(request.inputResources) };
				const session = inputSession ??= new ResourceInputSession({ store: this.inputStorage.store, inputs: this.inputs }, this.maxBytes());
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
											return publish(inputs as ResourceReadView, contract);
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
			await inputSession?.dispose();
		}
	}
	dispose(): Promise<void> {
		return this.lifetime.close(async () => {
			this.python.close();
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
			for (const key of this.files.keys()) this.evict(key);
			await this.lifetime.drain();
			await this.releaseChunks();
			this.inputManager.close();
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
			if (resource.scope === "content")
				return { ...resource, size: (await view.stat!(resource.path)).size };
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
