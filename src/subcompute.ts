import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server, type Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { createInterface } from "node:readline";
import { isDeepStrictEqual } from "node:util";
import { deserialize } from "node:v8";
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
	snapshotPreparation,
	type ResourceDependency,
	type ResourceInputPersistence,
	validateResourceVersion,
	type ResourceVersionToken,
	type ComputeStepRequest,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import { containsFilesystemPath, digestObject, filesystemPathKey, hasErrorCode, sha256Digest, sha256DigestAsync } from "./primitives.ts";
import { subcomputeTiming, TaskTimeline, TimelineInterval, type SubcomputeCounts } from "./task-timing.ts";
import type { ObservedProcessPath, ExecutionObservation } from "./execution-observer.ts";
/** A host-owned deterministic transform. The key identifies all options and input() supplies verified bytes. */
export interface TrustedSubcomputeStep {
	readonly namespace: string;
	readonly implementation: string;
	readonly enabled?: () => boolean;
	readonly runtime?: () => Promise<TrustedStepRuntime | undefined>;
	build(request: TrustedStepBuild): unknown | Promise<unknown>;
	dispose?(): void | Promise<void>;
}
/** Optional host-supplied files and environment needed to install a caller-side adapter. */
export interface TrustedStepRuntime {
	readonly readOnlyDirectories: readonly string[];
	environment(base: Readonly<Record<string, string | undefined>>): Readonly<Record<string, string>>;
}
export interface TrustedStepBuild {
	readonly key: unknown;
	readonly input: () => Promise<Buffer>;
	readonly signal: AbortSignal;
	readonly maxResultBytes: number;
}
const unavailable = Symbol("step unavailable");
type PreparedBytes = { data: Buffer; computation: TimelineInterval; bytes: number; referenced: boolean };
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
	readonly inputDigest?: string;
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
	compute: ["op", "context", "namespace", "key", "inputResources", "envKeys", "environment", "includeInputContent", "inputDigest"],
	observe: ["op", "token", "reads"],
	publish: ["op", "token", "reads", "durationMs", "value"],
	input: ["op", "token", "value"],
	consumed: ["op", "token"],
};
const endpoint = new AsyncLocalStorage<SubcomputeService>();
type SubcomputeScope = {
	run: ReturnType<typeof AsyncLocalStorage.snapshot>;
	timer?: NodeJS.Timeout;
	runtimes?: readonly TrustedStepRuntime[];
	clients: Set<Socket>;
	socket: string;
	identity: string;
	currentIdentity: () => string;
	proofs: ResourceVersionToken[];
	busy: number;
	failed: boolean;
	general: boolean;
};
const channels = new Map<string, SubcomputeScope>();
function adapterEnvironment(scope: SubcomputeScope, base: Readonly<Record<string, string | undefined>>) {
	const variables: Record<string, string> = {};
	for (const runtime of scope.runtimes ?? []) Object.assign(variables, runtime.environment({ ...base, ...variables }));
	return variables;
}
export function controlledSubcomputeChannel(environment: Readonly<Record<string, string | undefined>>) {
	const token = environment.PI_SUBCOMPUTE_CONTEXT;
	const scope = token ? channels.get(token) : undefined;
	if (!scope || scope.socket !== environment.PI_SUBCOMPUTE_SOCKET) return undefined;
	return {
		socket: scope.socket,
		identity: scope.identity,
		readOnlyDirectories: [...new Set(scope.runtimes?.flatMap((runtime) => runtime.readOnlyDirectories))],
		environment: (base: Readonly<Record<string, string | undefined>>) => adapterEnvironment(scope, base),
		async validate() {
			if (
				channels.get(token!) !== scope || scope.currentIdentity() !== scope.identity ||
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
/** Own temporary controlled endpoints until a background execution has sealed its result. */
export function withSubcomputeScope<T>(execute: () => Promise<T>): Promise<T> {
	return endpoint.getStore()?.run(execute) ?? execute();
}
export async function controlledRuntimeEnvironment(environment: Readonly<Record<string, string | undefined>>): Promise<Readonly<Record<string, string>>> {
	return endpoint.getStore()?.controlledEnvironment(environment) ?? {};
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
	private readonly owners = new AsyncLocalStorage<{ readonly tokens: Set<string>; closed: boolean }>();
	private readonly inputManager: ResourceVersionManager;
	private readonly inputStorage: { store: ProvenanceCertificateStore; contents: ContentChunkPool; internalCache: true };
	private releaseChunks: () => Promise<void>;
	private readonly files = new Map<
		string,
		{ statSig: string; takenAtMs: number; token: Promise<ResourceVersionToken>; bytes: number }
	>();
	private cacheBytes = 0;
	private readonly counts = new Map<string, SubcomputeCounts>();
	private readonly cwd: string;
	private readonly trusted = new Map<string, TrustedSubcomputeStep>();
	private readonly building = new Map<string, Promise<void>>();
	private readonly prepared = new Map<string, PreparedBytes>();
	private preparedBytes = 0;
	private readonly maxBytes: () => number;
	runtime: () => { reuseEnabled?: boolean; resourceCacheMaxEntries: number; resourceCacheMaxBytes?: number } =
		() => this.configuration.current.runtime;
	constructor(cwd: string, store: ProvenanceCertificateStore, maxBytes: () => number, steps: readonly TrustedSubcomputeStep[] = []) {
		this.cwd = cwd;
		for (const step of steps) {
			if (!step.namespace.trim() || !step.implementation.trim() || this.trusted.has(step.namespace))
				throw new Error("invalid_trusted_step_registration");
			this.trusted.set(step.namespace, step);
		}
		this.inputStorage = { store, contents: new ContentChunkPool(store), internalCache: true };
		this.releaseChunks = this.inputStorage.contents.retain();
		this.inputManager = new ResourceVersionManager(cwd, { watch: false, persistentInputs: this.inputStorage });
		this.maxBytes = maxBytes;
		this.configuration.bind(this);
	}
	async controlledEnvironment(base: Readonly<Record<string, string | undefined>>): Promise<Readonly<Record<string, string>>> {
		const general = this.configuration.current.subcomputation.exposeToControlledProcesses;
		const steps = [...this.trusted.values()].filter((step) => this.trustedEnabled(step));
		if (!general && !steps.length) return {};
		const existing = base.PI_SUBCOMPUTE_CONTEXT && this.scopes.get(base.PI_SUBCOMPUTE_CONTEXT);
		const transport = existing && existing.socket === base.PI_SUBCOMPUTE_SOCKET && existing.currentIdentity() === existing.identity ? {
			PI_SUBCOMPUTE_SOCKET: existing.socket, PI_SUBCOMPUTE_CONTEXT: base.PI_SUBCOMPUTE_CONTEXT!,
		} : await this.environment(general).catch((error: unknown) => {
			if (general) throw error;
			return undefined;
		});
		if (!transport) return {};
		const scope = this.scopes.get(transport.PI_SUBCOMPUTE_CONTEXT)!;
		scope.runtimes ??= (await Promise.all(steps.map((step) => step.runtime?.()))).filter((runtime) => runtime !== undefined);
		return { ...adapterEnvironment(scope, base), ...transport };
	}
	private trustedEnabled(step: TrustedSubcomputeStep): boolean {
		const runtime = this.runtime();
		if (runtime.reuseEnabled === false || runtime.resourceCacheMaxEntries <= 0 || (runtime.resourceCacheMaxBytes ?? this.maxBytes()) <= 0) return false;
		return step.enabled?.() ?? true;
	}
	withMetrics<T>(counts: Map<string, SubcomputeCounts>, execute: () => T): T {
		return subcomputeTiming.run((namespace, hit, receipt) => this.record(namespace, hit, receipt, counts), execute);
	}
	drainMetrics(counts = this.counts) {
		const metrics = Object.fromEntries(counts);
		counts.clear();
		return metrics;
	}
	private readonly record = (namespace: string, hit: boolean, computation?: TimelineInterval, totals = this.counts) => {
		const counts = totals.get(namespace) ?? { hits: 0, misses: 0, reusedMs: 0 };
		if (hit) {
			counts.hits++;
			if (computation) {
				const now = performance.now();
				counts.reusedMs += new TaskTimeline(now).recordTool(computation, now, true).toolComputeMs ?? 0;
			}
		} else counts.misses++;
		totals.set(namespace, counts);
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
				this.trimPrepared();
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
		const owner = { tokens: new Set<string>(), closed: false };
		const finish = () => {
			owner.closed = true;
			for (const token of owner.tokens) this.releaseScope(token);
			owner.tokens.clear();
		};
		return endpoint.run(this, () =>
			this.owners.run(owner, () => {
				try {
					const output = subcomputeTiming.run(subcomputeTiming.getStore() ?? this.record, operation);
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
		const owner = this.owners.getStore();
		const activeOwner = owner && !owner.closed ? owner : undefined;
		const timer = general || !activeOwner ? setTimeout(
			() => this.releaseScope(token),
			this.configuration.current.subcomputation.socketTimeoutMs,
		).unref() : undefined;
		const currentIdentity = () => digestObject({
			general,
			steps: Object.fromEntries([...this.trusted.values()].map((step) =>
				[step.namespace, this.trustedEnabled(step) ? step.implementation : null])),
		});
		const scope: SubcomputeScope = {
			identity: currentIdentity(),
			currentIdentity,
			run: subcomputeTiming.run(subcomputeTiming.getStore() ?? this.record, () => AsyncLocalStorage.snapshot()),
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
		activeOwner?.tokens.add(token);
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
			const frame = JSON.parse(next.value) as Begin | Observation | Publication |
				{ op: "consumed"; token: string } | { op: "input"; token: string; value: WireValue };
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
				if (request.op !== "compute") throw new Error("invalid_subcompute_request");
				const active = this.scopes.get(request.context);
				if (scope && scope !== active) throw new Error("subcompute_context_changed");
				if (!active) throw new Error("subcompute_context_unavailable");
				scope = active;
				scope.clients.add(socket);
				const step = this.trusted.get(request.namespace);
				if (!scope.general && !step) throw new Error("subcompute_context_unavailable");
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
				if (step ? typeof request.inputDigest !== "string" || !/^[a-f0-9]{64}$/.test(request.inputDigest) || request.inputResources !== undefined ||
					request.envKeys !== undefined || request.environment !== undefined || request.includeInputContent !== undefined : request.inputDigest !== undefined)
					throw new Error("invalid_subcompute_request");
				if (step && !this.trustedEnabled(step)) {
					await send({ state: "unavailable" });
					continue;
				}
				let busy = true;
				active.busy++;
				const releaseBusy = () => {
					if (!busy) return;
					busy = false;
					active.busy--;
				};
				try {
					if (step) {
						const identity = snapshotPreparation([step.namespace, step.implementation, request.key, request.inputDigest]);
						if (!identity) throw new Error("compute_step_input_budget_exceeded");
						const key = sha256Digest(identity.data);
						const token = randomUUID();
						let input: Promise<Buffer> | undefined;
						const receiveInput = async () => {
							await send({ state: "input", token });
							const frame = await read();
							if (frame?.op !== "input" || frame.token !== token || frame.value?.encoding !== "base64")
								throw new Error("invalid_subcompute_input");
							const bytes = decodeValue(frame.value) as Buffer;
							if (await sha256DigestAsync(bytes) !== `sha256:${request.inputDigest}`) throw new Error("subcompute_input_digest_mismatch");
							return bytes;
						};
						const maxResultBytes = Math.min(this.configuration.current.subcomputation.maxStepResultBytes, this.maxBytes(),
							this.runtime().resourceCacheMaxBytes ?? Infinity, Math.floor((limit() - 160) * 3 / 4));
						try {
							await active.run(async () => {
								let produced = false;
								const result = await this.sharedBuild(key, () => this.prepareBytes(key, signal, async () => {
									produced = true;
									const value = await step.build({ key: request.key, input: () => input ??= TimelineInterval.outside(receiveInput), signal, maxResultBytes });
									if (value === undefined) throw unavailable;
									return value;
								}, maxResultBytes), signal);
								if (this.scopes.get(request.context) !== active) throw new Error("subcompute_context_closed");
								const value = deserialize(result.data);
								if (wireBytes(value) > maxResultBytes) {
									releaseBusy();
									await send({ state: "local" });
									return;
								}
								await send({ state: produced ? "published" : "hit", value: encodeValue(value), token });
								const ack = await read();
								if (ack?.op !== "consumed" || ack.token !== token) throw new Error("invalid_subcompute_receipt");
								if (!produced) {
									const retained = this.prepared.get(key);
									if (retained?.data === result.data) retained.referenced = true;
									TimelineInterval.use(result.computation);
								}
								subcomputeTiming.getStore()?.(step.namespace, !produced, result.computation);
								releaseBusy();
								await send({ state: "ready" });
							});
						} catch (error) {
							if (error !== unavailable) throw error;
							releaseBusy();
							await send({ state: input ? "local" : "unavailable" });
						}
					} else {
						const contract = { ...request, inputResources: this.resources(request.inputResources) };
						const session = inputSession ??= new ResourceInputSession(
							{ store: this.inputStorage.store, inputs: this.inputs, internalCache: true },
							this.maxBytes(),
						);
						await active.run(async () => {
							let produced = false;
							const query = await session.run(() => evaluateResourceQuery(
								undefined, { signal }, this.cwd, (view) => computeStepInView(view, contract, (view) => {
									produced = true;
									return publish(view as ResourceReadView, contract);
								}, signal), PI_ACTION_SEMANTICS, this.maxBytes(), true, "output", undefined, session,
							));
							if (!query) throw new Error("subcompute_retention_budget_exceeded");
							try {
								if (this.scopes.get(request.context) !== active) throw new Error("subcompute_context_closed");
								for (const proof of query.versions) active.proofs.push(session.retain({ ...proof, view: undefined }));
								releaseBusy();
								await send({ state: produced ? "published" : "hit", value: encodeValue(query.output) });
							} finally {
								await Promise.allSettled(query.versions.map((version) => version.release()));
							}
						});
					}
				} finally {
					releaseBusy();
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
	/** Inline inputs are completely identified by the host binding, exact key and verified content digest. */
	private async prepareBytes(key: string, signal: AbortSignal, build: () => Promise<unknown>, limit: number): Promise<PreparedBytes> {
		const store = this.inputStorage.store;
		const root = `content:${this.inputManager.root}`;
		const binding = "host.content";
		const hot = this.prepared.get(key);
		if (hot) {
			this.prepared.delete(key);
			this.prepared.set(key, hot);
			this.trimPrepared();
			if (this.prepared.has(key)) return hot;
		}
		const [record] = await store.findPreparedInputCandidates({ root, binding, prepKey: digestObject(key), limit: 1 }, signal);
		const payload = record?.payload as { root?: string; preparations?: { binding: string; key: string }[]; data?: string; computation?: unknown } | undefined;
		let data: Buffer | undefined;
		let computation: TimelineInterval | undefined;
		if (record && payload?.root === root && payload.preparations?.length === 1 &&
			payload.preparations[0]?.binding === binding && payload.preparations[0].key === key) {
			try {
				if (!record.artifacts.length && typeof payload.data === "string") data = Buffer.from(payload.data, "base64");
				else if (record.artifacts.length === 1 && payload.data === undefined)
					data = await store.artifacts.get(record.artifacts[0]!, signal);
				if (data && !snapshotTrusted(deserialize(data))) data = undefined;
				if (data) computation = TimelineInterval.restore(payload.computation) ?? TimelineInterval.unknownReuse(record.id);
			} catch (error) {
				if (hasErrorCode(error, "ERR_PROVENANCE_SCHEMA")) throw error;
				signal.throwIfAborted();
				store.signal.throwIfAborted();
				data = undefined;
				computation = undefined;
			}
		}
		if (!data || !computation) {
			signal.throwIfAborted();
			const measured = await TimelineInterval.measure(build);
			computation = TimelineInterval.own(measured.computation);
			signal.throwIfAborted();
			const owned = snapshotTrusted(measured.output);
			if (!owned || wireBytes(owned.value, true) > limit) throw unavailable;
			data = owned.data;
			const metadata = { root, preparations: [{ binding, key }], computation: this.configuration.current.subcomputation.persistTimelineIntervals
				? TimelineInterval.serialize(computation) : undefined };
			const envelope = { id: key, createdAt: Date.now(), artifacts: [], payload: { ...metadata, data: "" } };
			if (Buffer.byteLength(JSON.stringify(envelope)) + 4 * Math.ceil(data.length / 3) <= this.configuration.current.storage.maxRecordBytes)
				await store.putInputVersion({ ...metadata, data: data.toString("base64") }, []);
			else {
				const bytes = data;
				await store.withPublication(async () => store.putInputVersion(metadata, [await store.artifacts.put(bytes)]));
			}
		}
		const receipt = TimelineInterval.serialize(computation);
		const result = { data, computation, referenced: false, bytes: data.byteLength + key.length + Buffer.byteLength(JSON.stringify(receipt ?? null)) };
		const prior = this.prepared.get(key);
		if (prior) {
			this.prepared.delete(key);
			this.preparedBytes -= prior.bytes;
		}
		this.prepared.set(key, { ...result, computation: TimelineInterval.restore(receipt) ?? TimelineInterval.unknownReuse(key) });
		this.preparedBytes += result.bytes;
		this.trimPrepared();
		return result;
	}
	private trimPrepared(): void {
		const runtime = this.runtime();
		const budget = Math.min(this.maxBytes(), runtime.resourceCacheMaxBytes ?? Infinity);
		while (this.prepared.size && (this.prepared.size > runtime.resourceCacheMaxEntries ||
			this.cacheBytes + this.inputStorage.contents.storedBytes + this.preparedBytes > budget)) {
			const [key, value] = this.prepared.entries().next().value!;
			this.prepared.delete(key);
			if (value.referenced) {
				value.referenced = false;
				this.prepared.set(key, value);
			} else this.preparedBytes -= value.bytes;
		}
	}
	/** Followers can retry immediately after owner cancellation. */
	private async sharedBuild<T>(key: string, evaluate: () => Promise<T>, signal: AbortSignal): Promise<T> {
		while (this.building.has(key)) {
			await waitForCompletion(this.building.get(key)!, signal);
			signal.throwIfAborted();
		}
		const done = Promise.withResolvers<void>();
		this.building.set(key, done.promise);
		try {
			return await evaluate();
		} finally {
			this.building.delete(key);
			done.resolve();
		}
	}
	dispose(): Promise<void> {
		return this.lifetime.close(async () => {
			for (const token of this.scopes.keys()) this.releaseScope(token);
			for (const [socket, controller] of this.clients) {
				controller.abort(new Error("subcompute_service_closed"));
				socket.destroy();
			}
			await Promise.all([...this.trusted.values()].map((step) => step.dispose?.()));
			await this.ready?.catch(() => undefined);
			if (this.server?.listening)
				await new Promise<void>((resolve, reject) =>
					this.server!.close((error) => (error ? reject(error) : resolve())),
				);
			await this.lifetime.drain();
			for (const key of this.files.keys()) this.evict(key);
			await this.lifetime.drain();
			await this.releaseChunks();
			this.prepared.clear();
			this.preparedBytes = 0;
			this.inputManager.close();
			if (this.directory) await rm(this.directory, { recursive: true, force: true });
		});
	}
}
function snapshotTrusted(value: unknown) {
	try {
		return snapshotPreparation(value);
	} catch {
		return undefined;
	}
}
function wireBytes(value: unknown, checkJson = false): number {
	if (value instanceof Uint8Array) return value.byteLength;
	const json = JSON.stringify(value);
	if (json === undefined || (checkJson && !isDeepStrictEqual(JSON.parse(json), value))) throw unavailable;
	return Buffer.byteLength(json);
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
